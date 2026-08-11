"""seed_demo.py — 產生 BI 展示用的訂單資料，走真實攝入路徑。

與 load_test.py 的關係
──────────────────────
兩支腳本【目的不同、刻意不共用】：

    load_test.py   壓吞吐：高併發、短時間、只關心 latency 與 status code 分佈
    seed_demo.py   造資料：低速率、可重現、關心【落地後的資料長什麼樣】

不 import load_test 的產生器，是因為兩者的取樣需求已經分岔——本檔要控制
分類權重、日期分佈、以及刻意注入的髒資料比例，這些都是壓測不需要的。
共用會讓「調壓測參數」與「調 demo 資料形狀」互相干擾。
payload 的【結構】真相仍然只有一份，在 schema.py（OrderIN），不因此重複。

兩個【正交】的資料形狀旋鈕 ⭐
────────────────────────────
    --dirty-rate           上游給了【違規的值】 → 觸發 DQCode、產生 quality_events
    --missing-cost-rate    上游給的資料【本來就不完整】 → 一個 code 都不產生

刻意分成兩個參數而不是一個「髒資料比例」，因為它們在 DQ 架構裡是不同層級的東西：
前者是品質規則攔下來的違規，後者是選填欄位沒給值（schema.py 的 cost_price /
shipping_fee 是 Optional，business_clean 對 None 完全不檢查）。把兩者混成一個
旋鈕，就沒辦法造出「乾淨但不完整」與「髒但完整」這兩種資料——而它們在下游
是完全不同的問題：前者讓 rpt_sales 的加總靜默少算，後者讓訂單落進 quarantine。
細節見下方〈選填欄位缺漏〉一節。

為什麼走 POST /orders 而不是直接 INSERT 進 ODS
─────────────────────────────────────────────
資料必須走完整條真實路徑才有展示價值：
    Pydantic 驗證 → format_clean → business_clean → CAS claim → idempotency
    → ODS + quality_events
直接 INSERT 進 ODS 等於【繞過品質架構去展示品質架構】，產出的 quality_events
是假的，rpt_quality_* 上的每個數字都失去意義。

⚠️ 限流：POST /orders 是 60/minute（main.py 的 @limiter.limit，key 為 client_id）
─────────────────────────────────────────────────────────────────────────
所以 --rps 預設 0.8（48/min）留安全邊際。灌 n 筆約需 n / (rps * 60) 分鐘，
例如 800 筆 ≈ 17 分鐘。這是刻意的：seeding 不是壓測，跑快沒有好處，
反而會把 CAS 競爭與背景任務佇列攪進來，讓「這批資料為什麼長這樣」變得難以解釋。
腳本會自動處理 429（指數退避重送），不會靜默掉單。

> 若要更快：在 .env 的 API_KEYS 加一組獨立的 seeding 身分（例如
>   demo_seed_key:demo-seeder），限流是 per client_id，新身分有自己的配額。
> 附帶好處是 ODS.source_client_id 從此能區分「demo 資料」與「上游真實資料」，
> 資料血緣多一個維度。本腳本不自動改 .env——那是你的環境設定。

⚠️ received_at 無法回填
──────────────────────
order_date 在 payload 裡，本腳本可以自由灑；但 received_at / event_at 是
伺服器在攝入當下產生的。也就是說：

    今天灌一萬筆，rpt_quality_events_daily 的時間軸上仍然只有【一個點】。

品質趨勢圖需要跨日的資料，唯一的做法是【分幾天各跑一次】（或掛排程）。
這同時也是 CLOUD_LAYER §1.7.7 說的「持續攝入」——一併把 source freshness
從「預期紅」變成有意義的 gate。
"""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import os
import random
import time
from collections import Counter
from datetime import date, datetime, timedelta

import httpx

# ── 值域 ─────────────────────────────────────────────────────────────────────
# 分類刻意給不均勻權重：真實電商的分類分佈是長尾的，均勻分佈會讓 BI 上的
# 堆疊圖每條一樣高，看起來像假資料（事實上也就是假資料）。
CATEGORIES = [
    ("Electronics", 30), ("Clothing", 25), ("Home & Kitchen", 18),
    ("Books", 14), ("Sports", 8), ("Beauty", 5),
]
SUB_CATEGORIES = ["General", "Premium", "Budget", "Seasonal"]
BRANDS         = ["Acme", "Globex", "Initech", "Umbrella", "Soylent"]
CONDITIONS     = ["New", "Refurbished", "Open Box"]

GENDERS   = ["male", "female", "other"]
TIERS     = ["bronze", "silver", "gold", "platinum"]
CHANNELS  = ["organic", "paid_search", "referral", "social_media", "email"]
DEVICES   = ["mobile", "desktop", "tablet"]
PAYMENTS  = ["credit_card", "paypal", "bank_transfer", "cash_on_delivery"]
SHIP_MODES = ["Standard Class", "Second Class", "First Class", "Same Day"]
STATUSES   = ["Shipped", "Pending", "Delivered", "Cancelled"]

COUNTRY = "Taiwan"
REGIONS = ["North", "Central", "South", "East"]
CITIES  = ["Taipei", "Kaohsiung", "Taichung", "Tainan", "Hsinchu", "Taoyuan"]


def _weighted(rng: random.Random, pairs: list[tuple[str, int]]) -> str:
    vals, weights = zip(*pairs)
    return rng.choices(vals, weights=weights, k=1)[0]


def make_product(pid_num: int) -> dict:
    """商品屬性一律【由 product_id 決定】，不吃訂單的 rng。

    同一個 product_id 在不同訂單必須帶相同的 name/category/brand，否則
    dim_product 無法以 product_id 為 grain（load_test.py 早期就踩過這個坑：
    342 個 product_id 裡 163 個屬性衝突）。用 product_id 當 seed 另開一個
    Random，就結構性地保證了這件事。
    """
    prng = random.Random(pid_num)
    return {
        "product_id":   f"P{pid_num:04d}",
        "product_name": f"Product {pid_num:04d}",
        "category":     _weighted(prng, CATEGORIES),
        "sub_category": prng.choice(SUB_CATEGORIES),
        "brand":        prng.choice(BRANDS),
        "condition":    prng.choice(CONDITIONS),
    }


def make_order(rng: random.Random, order_id: str, lookback_days: int) -> dict:
    """產生一筆【乾淨】的訂單 payload。髒資料由 inject_dirty() 事後注入。

    order_date 貼近今天（預設回看 45 天）：Gold 的 fct_* 按 order_date 分區，
    而 BQ sandbox 對每張分區表無條件套 60 天過期（見 CLOUD_LAYER §1.7.8）。
    order_date 離今天太遠的訂單【一進 Gold 就被回收】，等於白灌。
    """
    order_dt = date.today() - timedelta(days=rng.randint(0, lookback_days))
    delivery_dt = order_dt + timedelta(days=rng.randint(1, 14))
    cust_num = rng.randint(1, 400)

    return {
        "order_id":      order_id,
        "order_date":    order_dt.isoformat(),
        "ship_mode":     rng.choice(SHIP_MODES),
        "order_status":  rng.choice(STATUSES),
        "delivery_date": delivery_dt.isoformat(),
        "delivery_days": (delivery_dt - order_dt).days,
        # 退貨率 ~12%：is_returned 進了 rpt_sales 的 grain，全 false 會讓那個
        # 維度失去意義；給一個像樣的比例，BI 上「扣不扣退貨」才是個真問題。
        "returned":      rng.random() < 0.12,
        "customer": {
            "customer_id":              f"C{cust_num:04d}",
            "customer_name":            f"Customer {cust_num:04d}",
            "age":                      rng.randint(18, 75),
            "gender":                   rng.choice(GENDERS),
            "membership_tier":          rng.choice(TIERS),
            "registration_date":        (date.today() - timedelta(days=rng.randint(30, 1500))).isoformat(),
            "acquisition_channel":      rng.choice(CHANNELS),
            "newsletter_subscribed":    rng.random() < 0.4,
            "preferred_payment_method": rng.choice(PAYMENTS),
            "preferred_device":         rng.choice(DEVICES),
        },
        "address": {
            "country":     COUNTRY,
            "region":      rng.choice(REGIONS),
            "state":       rng.choice(CITIES),
            "city":        rng.choice(CITIES),
            "postal_code": f"{rng.randint(10000, 99999)}",
        },
        "payment": {
            "payment_method": rng.choice(PAYMENTS),
            "tax_pct":        rng.choice([0, 5, 5, 5, 8, 10]),
        },
        "behavior": {
            "device_used":        rng.choice(DEVICES),
            "is_repeat_customer": rng.random() < 0.35,
            "customer_rating":    rng.choice([None, 1, 2, 3, 4, 4, 5, 5]),
        },
        "items": [
            {
                "product":      make_product(rng.randint(1, 300)),
                "quantity":     rng.randint(1, 8),
                "unit_price":   round(rng.uniform(10.0, 500.0), 2),
                "cost_price":   round(rng.uniform(5.0, 200.0), 2),
                "discount_pct": rng.choice([0, 0, 0, 5, 10, 15, 20, 30]),
                "shipping_fee": round(rng.uniform(0.0, 25.0), 2),
            }
            for _ in range(rng.randint(1, 4))
        ],
    }


# ── 髒資料注入 ───────────────────────────────────────────────────────────────
#
# 每個注入器對應 clean.py 的一個 DQCode，回傳它預期會觸發的 code 名稱
# （回傳值僅供本腳本統計與事後對帳；真正的判定永遠是 business_clean 說了算）。
#
# 為什麼要刻意注入：現有 554 筆裡只有 14 筆髒（~2.5%），而且是隨機撞出來的，
# 10 個 DQCode 實際只碰得到一兩個 → rpt_quality_backlog 的「Top N 錯誤碼」
# 那張圖近乎空白，而那正是最能展示 RCA 能力的一張。

def _dirty_quantity_non_positive(p: dict, rng: random.Random) -> str:
    rng.choice(p["items"])["quantity"] = rng.choice([0, -1, -5])
    return "quantity_non_positive"


def _dirty_unit_price_negative(p: dict, rng: random.Random) -> str:
    rng.choice(p["items"])["unit_price"] = -round(rng.uniform(1.0, 200.0), 2)
    return "unit_price_negative"


def _dirty_discount_pct_out_of_range(p: dict, rng: random.Random) -> str:
    rng.choice(p["items"])["discount_pct"] = rng.choice([-10.0, 120.0, 150.5])
    return "discount_pct_out_of_range"


def _dirty_non_finite_number(p: dict, rng: random.Random) -> str:
    """NaN / Infinity 注入 items（不是訂單層欄位）。

    items 的值不經 Pydantic 型別強轉（ODSOrder.items 是 Any，直接吃 payload
    原始 dict），所以非有限值進得來、由 business_clean 攔下並正規化為 None。
    訂單層的 tax_pct/customer_rating 走 Pydantic float 欄位，行為不同，
    這裡刻意只打 items，讓觸發路徑單一可預期。

    json.dumps 預設 allow_nan=True 會輸出 NaN / Infinity 字面值，
    伺服器端 json.loads 同樣預設接受——這條路徑是通的（clean.py 的
    NON_FINITE_NUMBER 規則本來就是為它而寫）。
    """
    item = rng.choice(p["items"])
    field = rng.choice(["unit_price", "quantity", "discount_pct"])
    item[field] = rng.choice([float("nan"), float("inf"), float("-inf")])
    return "non_finite_number"


def _dirty_tax_pct_out_of_range(p: dict, rng: random.Random) -> str:
    p["payment"]["tax_pct"] = rng.choice([-5.0, 120.0, 250.0])
    return "tax_pct_out_of_range"


def _dirty_delivery_before_order(p: dict, rng: random.Random) -> str:
    od = date.fromisoformat(p["order_date"])
    p["delivery_date"] = (od - timedelta(days=rng.randint(1, 10))).isoformat()
    return "delivery_before_order"


def _dirty_customer_rating_out_of_range(p: dict, rng: random.Random) -> str:
    p["behavior"]["customer_rating"] = rng.choice([0, -2, 7.5, 11])
    return "customer_rating_out_of_range"


def _dirty_age_out_of_range(p: dict, rng: random.Random) -> str:
    # 125 是刻意加的【邊界附近】值：原本只注入 -3 / 150 / 999 這種離譜值，
    # 產出的資料永遠只落在規則的遠端，測不到閾值本身合不合理。
    # 而閾值正是最可能被調整的東西（見 DQ 文件〈Hard Gate 閾值為業務判斷〉），
    # 所以資料裡要有「調一下上限就會改變判定」的樣本——它同時讓 Proposal B 的
    # 回溯重評估有真實可 promote 的對象（見 ORCHESTRATION-TW §3.3 的 demo 劇本）。
    p["customer"]["age"] = rng.choice([-3, 125, 150, 999])
    return "age_out_of_range"


def _dirty_field_too_long(p: dict, rng: random.Random) -> str:
    """長度必須落在【軟性上限與 DB 硬牆之間】。

    clean.py 的 SOFT_MAX_LENGTHS 是 customer_name 100 / city 80；
    models.py 的欄位硬牆是 customer_name 255 / city 128。
    超過硬牆會撞 DataError 被 process.py fast-fail 成終態 error——那筆資料
    【不會進 quarantine】，也就不會出現在品質報表裡，注入就白做了。
    """
    if rng.random() < 0.5:
        p["customer"]["customer_name"] = "Customer " + "x" * rng.randint(110, 200)   # 100 < len < 255
    else:
        p["address"]["city"] = "Taipei " + "y" * rng.randint(80, 110)                # 80 < len < 128
    return "field_too_long"


def _dirty_order_date_in_future(p: dict, rng: random.Random) -> str:
    """⚠️ 權重刻意壓到最低（見 DIRTY_WEIGHTS）。

    未來日期超出 BQ 分區合法區間時會靜默落進 __UNPARTITIONED__
    （CLOUD_LAYER §1.7.3），在 BI 的日期軸上會變成一個突兀的空桶。
    保留極小比例是為了讓這條規則在 demo 裡「存在且可被查到」，
    而不是為了讓它在圖上被看見。
    """
    p["order_date"] = (date.today() + timedelta(days=rng.randint(2, 20))).isoformat()
    return "order_date_in_future"


# 權重刻意做成長尾：真實系統的錯誤分佈從來不是均勻的，均勻分佈會讓
# 「Top N 錯誤碼」那張圖變成一排等高的長條，反而不像真的。
DIRTY_WEIGHTS: list[tuple] = [
    (_dirty_quantity_non_positive,        22),
    (_dirty_discount_pct_out_of_range,    18),
    (_dirty_unit_price_negative,          15),
    (_dirty_customer_rating_out_of_range, 12),
    (_dirty_delivery_before_order,        10),
    (_dirty_tax_pct_out_of_range,          8),
    (_dirty_age_out_of_range,              6),
    (_dirty_field_too_long,                5),
    (_dirty_non_finite_number,             3),
    (_dirty_order_date_in_future,          1),   # ⚠️ 見該函式 docstring
]

# 一筆訂單同時帶多個錯誤的機率。這不只是為了擬真——rpt_quality_backlog 的
# orders_with_code（不可加）與 orders_primary_code（可加）之所以要分成兩個
# 度量，前提就是「一張訂單可能有多個 code」。全部單碼的話這個設計在 demo 上
# 看不出差別，assert_rpt_backlog_primary_code_balances 也驗不到有意義的東西。
MULTI_ERROR_RATE = 0.25


def inject_dirty(payload: dict, rng: random.Random) -> list[str]:
    """對 payload 就地注入 1~2 個違規，回傳預期觸發的 code 清單。"""
    funcs, weights = zip(*DIRTY_WEIGHTS)
    codes = [rng.choices(funcs, weights=weights, k=1)[0](payload, rng)]
    if rng.random() < MULTI_ERROR_RATE:
        codes.append(rng.choices(funcs, weights=weights, k=1)[0](payload, rng))
    return codes


# ── 選填欄位缺漏（上游不完整，【不是】DQ 違規）───────────────────────────────
#
# ⚠️ 這一節刻意【不】放進 DIRTY_WEIGHTS，理由不是風格而是正確性：
#
#   1. 它沒有對應的 DQCode。schema.py 的 cost_price / shipping_fee 是
#      `Optional[float] = None`，business_clean 對 None 完全不標記（數值檢查都在
#      `_is_number()` 守衛之後）。它描述的是【上游給的資料本來就不完整】，
#      而不是【上游給了違規的值】——兩者在 DQ 架構裡是不同層級的東西。
#   2. DIRTY_WEIGHTS 的每一項都回傳一個 code 名稱，run() 用它累計 expected_codes
#      去和 business_clean 的實判對帳。混進一個「不會產生任何 code」的注入器，
#      那份對帳表就永遠差一項，而且差的原因無法從輸出看出來。
#
# 為什麼要造這種資料：rpt_sales_daily_by_category 有三個 counter——
# items_missing_amount / items_missing_cost / items_missing_shipping。它們存在的
# 理由寫在該檔檔頭：整格 sum 為 NULL 時 BI 只顯示一個空白，而【部分】缺失更危險，
# sum 會回一個看起來完全正常的數字，只是少算幾筆。沒有這種資料，那三個 counter
# 恆為 0，等於一個防呆設計沒有任何證據能證明它有用。
#
# ⭐ 兩個欄位【各自獨立】抽，而不是一起拿掉。這是刻意要修正舊 demo 資料的形狀：
#    2026-07-08 手動灌的那 250 筆是 cost 與 shipping 永遠同時缺，於是
#    items_missing_cost 與 items_missing_shipping 在報表上【恆等】——兩個 counter
#    看起來像同一個訊號的兩份複本，分開設計的意義完全看不出來。獨立抽才會讓它們
#    真的分岔。
#
# ⭐ 粒度是 item 而非 order，同理：舊資料是「整張訂單全缺」（order 粒度 251 筆
#    全缺、0 筆部分缺），產不出「同一張訂單裡有些品項有成本、有些沒有」這種
#    最貼近真實上游、也最容易讓下游加總靜默少算的形狀。
OPTIONAL_COST_FIELDS = ("cost_price", "shipping_fee")


def strip_optional_costs(payload: dict, rng: random.Random,
                         rate: float) -> Counter:
    """以 item 為粒度、逐欄位獨立地把選填成本欄位拿掉。回傳各欄位的缺漏筆數。

    就地修改 payload。與 inject_dirty 正交——DIRTY_WEIGHTS 裡沒有任何注入器會碰
    cost_price / shipping_fee（_dirty_non_finite_number 只打 unit_price /
    quantity / discount_pct），所以兩者誰先誰後都不影響結果。
    """
    stripped: Counter = Counter()
    if rate <= 0:
        return stripped
    for item in payload["items"]:
        for field in OPTIONAL_COST_FIELDS:
            if rng.random() < rate:
                item[field] = None
                stripped[field] += 1
    return stripped


# ── 送出 ─────────────────────────────────────────────────────────────────────

def resolve_api_key(explicit: str | None) -> str:
    """優先用 --api-key；否則取 .env 的 API_KEYS 第一組的 key 部分。"""
    if explicit:
        return explicit
    raw = os.getenv("API_KEYS")
    if not raw:
        try:
            from dotenv import load_dotenv
            load_dotenv(".env")
            raw = os.getenv("API_KEYS")
        except ImportError:
            pass
    if not raw:
        raise SystemExit("找不到 API 金鑰：請用 --api-key 指定，或在 .env 設定 API_KEYS")
    return raw.split(",")[0].split(":")[0].strip()


async def post_one(client: httpx.AsyncClient, url: str, headers: dict,
                   payload: dict, max_retries: int = 5) -> tuple[str, int | None]:
    """送一筆；429 指數退避重送。回傳 (結果標籤, raw_id)。

    429 必須重送而不是放棄——靜默掉單會讓「我灌了 n 筆」與實際落地筆數對不上，
    而這個腳本存在的意義就是產出一批【可解釋的】資料。
    """
    # allow_nan=True（預設）讓 NaN/Infinity 以字面值送出，供 non_finite_number 注入。
    body = json.dumps(payload)
    delay = 2.0
    for attempt in range(max_retries):
        try:
            r = await client.post(url, content=body, headers=headers)
        except httpx.RequestError as e:
            return f"neterr:{type(e).__name__}", None
        if r.status_code == 429:
            await asyncio.sleep(delay)
            delay = min(delay * 2, 30.0)
            continue
        if r.status_code in (200, 201, 202):
            try:
                return "ok", r.json().get("raw_id")
            except Exception:
                return "ok", None
        return f"http:{r.status_code}", None
    return "http:429_giveup", None


async def run(args) -> None:
    rng = random.Random(args.seed)
    api_key = resolve_api_key(args.api_key)
    headers = {"Content-Type": "application/json", "X-API-Key": api_key}
    batch = datetime.now().strftime("%Y%m%d-%H%M%S")

    if args.dry_run:
        p = make_order(rng, f"SEED-{batch}-0001", args.order_date_days)
        codes = inject_dirty(p, rng) if args.dirty_rate > 0 else []
        stripped = strip_optional_costs(p, rng, args.missing_cost_rate)
        print(f"[dry-run] 預期觸發 code: {codes or '(clean)'}")
        print(f"[dry-run] 留空的選填成本欄位: {dict(stripped) or '(無)'}")
        print(json.dumps(p, indent=2, ensure_ascii=False))
        return

    interval = 1.0 / args.rps
    eta_min = args.n * interval / 60
    print(f"目標 {args.n} 筆 → {args.url}")
    print(f"髒資料比例 {args.dirty_rate:.0%}｜速率 {args.rps}/s（{args.rps*60:.0f}/min，上限 60/min）"
          f"｜預估 {eta_min:.1f} 分鐘")
    print(f"選填成本欄位缺漏率 {args.missing_cost_rate:.0%}（per item、逐欄位獨立）")
    print(f"批次標記 SEED-{batch}-*｜order_date 回看 {args.order_date_days} 天\n")

    results, expected_codes, missing_fields = Counter(), Counter(), Counter()
    raw_ids: list[int] = []
    total_items = 0
    started = time.monotonic()

    async with httpx.AsyncClient(timeout=30.0) as client:
        for i in range(1, args.n + 1):
            payload = make_order(rng, f"SEED-{batch}-{i:04d}", args.order_date_days)
            if rng.random() < args.dirty_rate:
                for c in inject_dirty(payload, rng):
                    expected_codes[c] += 1
            # 與 dirty 注入正交：髒訂單一樣可能缺成本欄位，真實上游本來就這樣。
            missing_fields.update(strip_optional_costs(payload, rng, args.missing_cost_rate))
            total_items += len(payload["items"])

            tag, raw_id = await post_one(client, args.url, headers, payload)
            results[tag] += 1
            if raw_id is not None:
                raw_ids.append(raw_id)

            if i % 25 == 0 or i == args.n:
                elapsed = time.monotonic() - started
                print(f"  {i}/{args.n}  ok={results['ok']}  "
                      f"其他={sum(v for k, v in results.items() if k != 'ok')}  "
                      f"({elapsed/60:.1f} 分)")

            if i < args.n:
                # 依「應該送到第幾筆」對齊，避免每筆的處理時間累積成漂移
                target = started + i * interval
                drift = target - time.monotonic()
                if drift > 0:
                    await asyncio.sleep(drift)

    print(f"\n送出結果：{dict(results)}")
    if raw_ids:
        print(f"raw_id 範圍：{min(raw_ids)} .. {max(raw_ids)}")
    if expected_codes:
        print("\n預期觸發的錯誤碼（腳本端統計，非權威——以 business_clean 實判為準）：")
        for code, n in expected_codes.most_common():
            print(f"  {code:32s} {n}")

    if missing_fields:
        # 與上面那份統計分開列，因為它們是不同性質的東西：上面是【違規】、
        # 會產生 quality_events；這裡是【上游不完整】，一個 code 都不會產生，
        # 只會在 rpt_sales_daily_by_category 的 items_missing_* 上現形。
        print(f"\n留空的選填成本欄位（不產生任何 DQCode；共 {total_items} 個 item）：")
        for field, n in missing_fields.most_common():
            print(f"  {field:32s} {n}  ({n/total_items:.1%})")

    print("\n⚠️ POST 回傳 pending 只代表 Raw 已落地，ODS 由背景任務非同步寫入。")
    print("   請等背景處理完再抽取；用 --verify 可直接查 DB 確認。")
    if args.verify:
        await asyncio.sleep(args.verify_wait)
        landed = verify(batch, expected=results["ok"])
        enforce_landed(landed, results["ok"], args.require_landed_pct)


def enforce_landed(landed: int | None, expected: int, threshold: float | None) -> None:
    """落地率閘門。低於門檻就非零 exit——這是排程化之後唯一擋得住「靜默不落地」的東西。

    為什麼需要它 ⭐
    ──────────────
    `POST /orders` 回 202 只代表 Raw 落地；ODS 由 Celery worker 非同步寫入，而
    `main._enqueue` **刻意吞掉 broker 故障**（照樣回 200 pending）。所以 worker 或
    Redis 掛掉時，這支腳本會印出滿版的 ok、exit code 0、DAG 一片綠，而 ODS 一筆都沒有。
    補派的 scan_and_dispatch 在 Celery Beat 裡，一起沒了的話也不會自癒。

    無人值守的日排程正是最不可能有人察覺這件事的場景——它會一路綠到有人問
    「為什麼報表沒更新」為止。

    預設 None＝不啟用，手動跑的行為完全不變（那時有人盯著輸出）。
    """
    if threshold is None:
        return
    if landed is None:
        raise SystemExit(
            "落地閘門：查不到落地筆數（verify 失敗或連錯資料庫），無法判定 → 視為失敗。\n"
            "  查不到與真的沒落地在後果上相同，都代表『我不知道這批資料在哪』。"
        )
    pct = landed / expected if expected else 0.0
    if pct < threshold:
        raise SystemExit(
            f"落地閘門未通過：送出 {expected} 筆，ODS 只有 {landed} 筆（{pct:.1%} < {threshold:.0%}）。\n"
            f"  最常見原因是 Celery worker 或 Redis 沒起——Raw 落地了但沒人處理。\n"
            f"  檢查：docker compose ps worker redis / docker compose logs worker"
        )
    print(f"\n落地閘門通過：{landed}/{expected}（{pct:.1%} >= {threshold:.0%}）")


def verify(batch: str, expected: int | None = None) -> int | None:
    """查 DB 確認這批資料的落地狀況，回傳進到 ODS 的筆數（查不到回 None）。

    失敗不影響已灌的資料，只是看不到報告——但**回傳 None 是有意義的訊號**，
    由 enforce_landed 決定要不要當成失敗。
    """
    try:
        from dotenv import load_dotenv
        from sqlalchemy import create_engine, text
    except ImportError:
        print("（跳過 verify：缺 sqlalchemy / python-dotenv）")
        return None

    # ⚠️ override=False（預設）：**環境裡既有的 DB_URL 會蓋過 .env**。
    #    這不是 bug 而是 dotenv 的設計（環境優先於檔案），但它讓「我改了 .env
    #    卻連到舊資料庫」變成一個完全無聲的故障——實測踩過兩次，兩次都只看到
    #    一片空白的報告而沒有任何錯誤。故下方一定要印出實際連到哪裡。
    load_dotenv(".env")
    db_url = os.getenv("DB_URL")
    if not db_url:
        print("（跳過 verify：找不到 DB_URL）")
        return None

    prefix = f"SEED-{batch}-%"
    try:
        with create_engine(db_url).connect() as c:
            # ⭐ 印出實際連到的資料庫。一行字，但它是「灌 A 庫、查 B 庫」這類
            #    問題唯一會自己現形的地方——沒有它，錯連只會表現為空白報告。
            who = c.execute(text(
                "select current_user || '@' || inet_server_addr() || ':' "
                "|| inet_server_port() || '/' || current_database()"
            )).scalar()
            print(f"\n── 落地確認（order_id LIKE '{prefix}'）──")
            print(f"連線：{who}")
            print("raw.status：")
            for status, n in c.execute(text(
                "select status, count(*) from raw where order_id like :p group by status order by 1"
            ), {"p": prefix}):
                print(f"  {status:14s} {n}")

            row = c.execute(text(
                "select count(*), count(*) filter (where has_clean_error), "
                "       min(raw_id), max(raw_id) "
                "from ods where order_id like :p"
            ), {"p": prefix}).one()
            total, dirty, lo, hi = row
            if total:
                line = (f"ods：{total} 筆，其中髒 {dirty} 筆（{dirty/total:.1%}）"
                        f"，raw_id {lo}..{hi}")
                if expected:
                    line += f"｜落地率 {total/expected:.1%}"
                print(line)
            else:
                # 明說「零筆」而不是靜靜跳過。舊版在這裡什麼都不印，於是「連錯庫」
                # 與「資料還沒處理完」看起來一模一樣——都是一片空白。
                print(f"ods：0 筆 ⚠️ 這批資料在上面那個連線裡【找不到】")

            # 選填欄位缺漏的落地確認。用 jsonb_typeof(...) = 'null' 而不是
            # `it->'cost_price' IS NULL`：前者是「欄位存在、值是 JSON null」
            # （我們寫進去的形狀），後者是「欄位根本不存在」。兩者在 ODS 都可能
            # 出現，而下游 int_order_items 的 json_value() 對兩者都回 SQL NULL，
            # 所以這裡兩種都要算進去，否則對帳數字會比報表少。
            row = c.execute(text(
                "select count(*), "
                "  count(*) filter (where coalesce(jsonb_typeof(it->'cost_price'), 'null') = 'null'), "
                "  count(*) filter (where coalesce(jsonb_typeof(it->'shipping_fee'), 'null') = 'null') "
                "from ods, lateral jsonb_array_elements(items) it "
                "where order_id like :p"
            ), {"p": prefix}).one()
            items_n, miss_cost, miss_ship = row
            if items_n:
                print(f"選填成本欄位缺漏（item 粒度，共 {items_n} 個）："
                      f"cost_price {miss_cost}（{miss_cost/items_n:.1%}）、"
                      f"shipping_fee {miss_ship}（{miss_ship/items_n:.1%}）")

            print("實際觸發的錯誤碼：")
            # jsonb_typeof 守衛不可省：乾淨列的 clean_error_message 存的是 JSON null
            # （scalar，不是 SQL NULL 也不是空陣列），jsonb_array_elements 對 scalar
            # 會直接報 "cannot extract elements from a scalar" 而不是回零列。
            for code, n in c.execute(text(
                "select e->>'code' code, count(*) "
                "from ods, lateral jsonb_array_elements(clean_error_message) e "
                "where order_id like :p and jsonb_typeof(clean_error_message) = 'array' "
                "group by code order by 2 desc"
            ), {"p": prefix}):
                print(f"  {code:32s} {n}")
            return total
    except Exception as e:
        print(f"（verify 失敗，不影響已灌資料）{type(e).__name__}: {e}")
        return None


def main() -> None:
    ap = argparse.ArgumentParser(
        description="產生 BI 展示用訂單資料（走 POST /orders 真實攝入路徑）")
    ap.add_argument("--url", default="http://localhost:8000/orders")
    ap.add_argument("--n", type=int, default=200, help="訂單筆數（預設 200）")
    ap.add_argument("--dirty-rate", type=float, default=0.12,
                    help="髒資料比例（預設 0.12——刻意壓在 Hard Gate 的 "
                         "warn@5%%/error@10%% 附近，讓門檻線在 BI 上真的有意義）")
    # 預設 0.06 的取法：報表的 grain 是 (order_date, category, is_returned)，
    # 太高會讓整格都缺（sum 變 NULL，BI 上是空白，反而不危險也不真實）；
    # 太低則多數格子一筆都不缺，測不到「sum 靜默少算」那個真正危險的狀態。
    # 6% 讓典型格子落在「0~2 個品項缺」的區間，正是要展示的形狀。
    ap.add_argument("--missing-cost-rate", type=float, default=0.06,
                    help="選填成本欄位（cost_price / shipping_fee）的缺漏率，"
                         "per item 且兩欄位獨立抽（預設 0.06）。這【不是】髒資料——"
                         "它不觸發任何 DQCode，只餵 rpt_sales_daily_by_category 的 "
                         "items_missing_* counter。設 0 可完全關閉。")
    ap.add_argument("--rps", type=float, default=0.8,
                    help="每秒送出筆數（預設 0.8＝48/min；API 限流上限 60/min）")
    ap.add_argument("--order-date-days", type=int, default=45,
                    help="order_date 往回散佈的天數（預設 45；須 < BQ 的 60 天分區過期）")
    ap.add_argument("--api-key", default=None, help="X-API-Key；預設取 .env 的 API_KEYS 第一組")
    ap.add_argument("--seed", type=int, default=None, help="亂數種子（給定則可重現）")
    ap.add_argument("--dry-run", action="store_true", help="只印一筆樣本 payload，不送出")
    ap.add_argument("--verify", action=argparse.BooleanOptionalAction, default=True,
                    help="灌完查 DB report 落地狀況（預設開啟）")
    ap.add_argument("--verify-wait", type=float, default=10.0,
                    help="verify 前等待背景任務的秒數（預設 10）")
    ap.add_argument("--require-landed-pct", type=float, default=None,
                    help="落地率閘門：ODS 筆數／送出筆數低於此值就非零 exit（例 0.9）。"
                         "預設不啟用——手動跑時有人盯著輸出，排程跑則必設。"
                         "見 enforce_landed() 的說明。")
    ap.add_argument("--dirty-rate-choices", default=None,
                    help="逗號分隔的候選髒率（例 0.03,0.05,0.08,0.10,0.12）。給定時"
                         "覆蓋 --dirty-rate，並以 --dirty-rate-seed 決定性地選一個。")
    ap.add_argument("--dirty-rate-seed", default=None,
                    help="挑選 --dirty-rate-choices 用的種子，通常餵 Airflow 的 "
                         "{{ ds_nodash }}。同一天必得同一個值。")
    args = ap.parse_args()

    if args.rps > 1.0:
        print(f"⚠️ --rps {args.rps} 超過 API 限流 60/min，將大量觸發 429 退避，"
              f"實際只會更慢。建議 ≤ 1.0。\n")

    args.dirty_rate = resolve_dirty_rate(
        args.dirty_rate, args.dirty_rate_choices, args.dirty_rate_seed)

    asyncio.run(run(args))


def resolve_dirty_rate(fixed: float, choices: str | None, seed: str | None) -> float:
    """從候選清單裡【決定性地】挑一個髒率。沒給候選就原樣回傳 fixed。

    為什麼是「由日期決定的隨機」而不是固定循環，也不是真隨機 ⭐
    ──────────────────────────────────────────────────────────
    · 固定循環（例如 5 天一輪）會在品質趨勢圖上畫出規律的鋸齒——比隨機更像假資料。
    · 真 random.choice() 則讓補跑同一天得到不同的髒率，那天的資料就再也解釋不了；
      而「同一個 logical date 產生同一批資料」是整個排程能被推理的前提。
    以日期為種子同時滿足兩邊：圖上看不出週期，重跑卻完全可重現。

    ⚠️ 一天多個 slot 必須共用同一個 seed（都餵 ds_nodash）。Hard Gate 的口徑是
       【最新的一個 UTC 日分區】而非單一批次（見 macros/error_rate_below.sql），
       所以它判的是全天加權平均——各 slot 髒率不同的話，你選的那個值根本不是
       gate 看到的值。payload 的種子則必須逐 slot 不同，否則會灌出四批一模一樣
       的訂單（見 seed_demo_daily.py）。
    """
    if not choices:
        return fixed
    values = [float(x) for x in choices.split(",") if x.strip()]
    if not values:
        raise SystemExit("--dirty-rate-choices 解析後是空的")
    picked = random.Random(seed).choice(values)
    print(f"髒率由 --dirty-rate-choices 決定：seed={seed!r} → {picked:.0%}"
          f"（候選 {', '.join(f'{v:.0%}' for v in values)}）")
    return picked


if __name__ == "__main__":
    main()
