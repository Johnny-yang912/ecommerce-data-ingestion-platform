"""
負載測試：1000 筆不同訂單同時打 POST /orders
用法：python load_test.py [--url URL] [--n N] [--concurrency C]
"""
import asyncio
import random
import time
import argparse
import statistics
from collections import Counter
from datetime import date, timedelta

import httpx


# ── 產生假資料 ────────────────────────────────────────────────────────────────

GENDERS       = ["male", "female", "other"]
TIERS         = ["bronze", "silver", "gold", "platinum"]
CHANNELS      = ["organic", "paid_search", "referral", "social_media", "email"]
DEVICES       = ["mobile", "desktop", "tablet"]
PAYMENTS      = ["credit_card", "paypal", "bank_transfer", "cash_on_delivery"]
SHIP_MODES    = ["Standard Class", "Second Class", "First Class", "Same Day"]
STATUSES      = ["Shipped", "Pending", "Delivered", "Cancelled"]
CATEGORIES    = ["Electronics", "Clothing", "Food", "Books", "Sports"]
SUB_CATEGORIES = ["General", "Premium", "Budget", "Seasonal"]
BRANDS        = ["LoadTestBrand", "Acme", "Globex", "Initech", "Umbrella"]
CONDITIONS    = ["New", "Refurbished", "Open Box"]
STATES        = ["Taipei", "Kaohsiung", "Taichung", "Tainan", "Hsinchu"]

# order_date 的回溯窗。必須 < BQ sandbox 的 60 天分區過期，且要讓 order_date 貼近
# received_at——Gold 的 fct_* 按 order_date 分區、staging/stg_ 按 received_at 分區，
# 兩個 60 天時鐘掛在不同軸上，只有兩軸對齊時「fct 與 int 內容一致」才成立。
# 舊版寫死 date(2024, 1, 1)，與 received_at 相差平均 410 天 → 新灌的資料一進 Gold
# 就被分區過期回收（見 CLOUD_LAYER-TW §1.7）。
ORDER_DATE_LOOKBACK_DAYS = 45


def make_product(pid_num: int) -> dict:
    """商品屬性一律由 product_id 決定（以 product_id 為 seed），不吃訂單的 rng。

    舊版對 product_id 與 product_name/category 各抽一次亂數，同一個 product_id 會
    在不同訂單帶著不同的名稱與分類（實測 342 個 id 裡 163 個衝突）——那讓
    dim_product 無法以 product_id 為 grain 建立。商品屬性屬於商品本身，不屬於訂單。
    """
    prng = random.Random(f"product-{pid_num}")
    return {
        "product_id":   f"PROD-{pid_num:04d}",
        "product_name": f"Product {pid_num}",
        "category":     prng.choice(CATEGORIES),
        "sub_category": prng.choice(SUB_CATEGORIES),
        "brand":        prng.choice(BRANDS),
        "condition":    prng.choice(CONDITIONS),
    }


def make_payload(i: int) -> dict:
    rng = random.Random(i)  # 固定 seed 讓每次跑結果可重現

    # 貼近 received_at（見 ORDER_DATE_LOOKBACK_DAYS）
    order_date    = date.today() - timedelta(days=rng.randint(0, ORDER_DATE_LOOKBACK_DAYS))
    delivery_date = order_date + timedelta(days=rng.randint(1, 14))

    return {
        "order_id":      f"LOAD-{i:06d}",
        "order_date":    str(order_date),
        "ship_mode":     rng.choice(SHIP_MODES),
        "order_status":  rng.choice(STATUSES),
        "delivery_date": str(delivery_date),
        "delivery_days": (delivery_date - order_date).days,
        "returned":      rng.choice([True, False]),
        "customer": {
            "customer_id":              f"CUST-{i:06d}",
            "customer_name":            f"TestUser {i}",
            "age":                      rng.randint(18, 75),
            "gender":                   rng.choice(GENDERS),
            "membership_tier":          rng.choice(TIERS),
            "registration_date":        "2020-06-15",
            "acquisition_channel":      rng.choice(CHANNELS),
            "newsletter_subscribed":    rng.choice([True, False]),
            "preferred_payment_method": rng.choice(PAYMENTS),
            "preferred_device":         rng.choice(DEVICES),
        },
        "address": {
            "country":     "Taiwan",
            "region":      "East Asia",
            "state":       rng.choice(STATES),
            "city":        "Test City",
            "postal_code": f"{rng.randint(10000, 99999)}",
        },
        "items": [
            {
                "product":      make_product(rng.randint(1, 500)),
                "quantity":     rng.randint(1, 10),
                "unit_price":   round(rng.uniform(10.0, 500.0), 2),
                "cost_price":   round(rng.uniform(5.0, 200.0), 2),
                "discount_pct": round(rng.uniform(0.0, 30.0), 2),
                "shipping_fee": round(rng.uniform(0.0, 25.0), 2),
            }
            for _ in range(rng.randint(1, 3))
        ],
        "payment": {
            "payment_method": rng.choice(PAYMENTS),
            "tax_pct":        round(rng.uniform(0.0, 10.0), 2),
        },
        "behavior": {
            "device_used":       rng.choice(DEVICES),
            "is_repeat_customer": rng.choice([True, False]),
            "customer_rating":   round(rng.uniform(1.0, 5.0), 1),
        },
    }


# ── 單一請求 ──────────────────────────────────────────────────────────────────

async def send_order(
    client: httpx.AsyncClient,
    sem: asyncio.Semaphore,
    i: int,
    url: str,
    results: list,
    progress_counter: list,
    total: int,
    duplicate: bool = False,
) -> None:
    payload = make_payload(0) if duplicate else make_payload(i)
    t0 = time.perf_counter()

    async with sem:
        try:
            resp = await client.post(url, json=payload, timeout=30.0)
            elapsed = time.perf_counter() - t0
            results.append({"status": resp.status_code, "elapsed": elapsed})
        except httpx.ConnectError as e:
            elapsed = time.perf_counter() - t0
            results.append({"status": "ConnectError", "elapsed": elapsed, "err": str(e)})
        except httpx.TimeoutException:
            elapsed = time.perf_counter() - t0
            results.append({"status": "Timeout", "elapsed": elapsed})
        except Exception as e:
            elapsed = time.perf_counter() - t0
            results.append({"status": f"Exception:{type(e).__name__}", "elapsed": elapsed, "err": str(e)})

    progress_counter[0] += 1
    done = progress_counter[0]
    if done % 100 == 0 or done == total:
        print(f"  進度：{done}/{total}", flush=True)


# ── 統計輸出 ──────────────────────────────────────────────────────────────────

def print_report(results: list, total_elapsed: float) -> None:
    n = len(results)
    latencies = [r["elapsed"] for r in results]
    status_counts = Counter(r["status"] for r in results)
    success = sum(v for k, v in status_counts.items() if isinstance(k, int) and 200 <= k < 300)
    errors  = n - success

    latencies_sorted = sorted(latencies)
    p50 = statistics.median(latencies_sorted)
    p95 = latencies_sorted[int(n * 0.95)]
    p99 = latencies_sorted[int(n * 0.99)]

    print()
    print("=" * 52)
    print("  負載測試結果")
    print("=" * 52)
    print(f"  總請求數      : {n}")
    print(f"  成功 (2xx)   : {success}")
    print(f"  失敗          : {errors}")
    print(f"  總耗時        : {total_elapsed:.2f} 秒")
    print(f"  RPS           : {n / total_elapsed:.1f}")
    print()
    print("  延遲分布 (秒)")
    print(f"    min    : {min(latencies):.3f}")
    print(f"    median : {p50:.3f}")
    print(f"    P95    : {p95:.3f}")
    print(f"    P99    : {p99:.3f}")
    print(f"    max    : {max(latencies):.3f}")
    print()
    print("  狀態碼分布")
    for status, count in sorted(status_counts.items(), key=lambda x: -x[1]):
        print(f"    {status}: {count}")

    errors_with_msg = [r for r in results if "err" in r]
    if errors_with_msg:
        print()
        print("  錯誤樣本 (最多顯示 5 筆)")
        for r in errors_with_msg[:5]:
            print(f"    [{r['status']}] {r.get('err', '')[:120]}")

    print("=" * 52)


# ── 主程式（吞吐量測試）────────────────────────────────────────────────────────

async def main(url: str, n: int, concurrency: int, duplicate: bool, api_key: str | None = None) -> None:
    print(f"目標：{url}")
    print(f"請求數：{n}　並發上限：{concurrency}　重複資料：{'是' if duplicate else '否'}")
    if duplicate:
        print(f"  order_id 全部使用：{make_payload(0)['order_id']}")
    print()

    sem = asyncio.Semaphore(concurrency)
    results = []
    progress_counter = [0]

    # /orders 需 X-API-Key 認證；帶了才不會被 401 擋下（見 auth.py）。
    headers = {"X-API-Key": api_key} if api_key else {}
    limits = httpx.Limits(max_connections=concurrency, max_keepalive_connections=concurrency)
    async with httpx.AsyncClient(limits=limits, headers=headers) as client:
        t0 = time.perf_counter()
        await asyncio.gather(*[
            send_order(client, sem, i, url, results, progress_counter, n, duplicate)
            for i in range(1, n + 1)
        ])
        total_elapsed = time.perf_counter() - t0

    print_report(results, total_elapsed)


# ── CAS Lock 測試 ──────────────────────────────────────────────────────────────

async def _replay(
    client: httpx.AsyncClient,
    sem: asyncio.Semaphore,
    url: str,
    results: list,
) -> None:
    async with sem:
        t0 = time.perf_counter()
        try:
            resp = await client.post(url, timeout=30.0)
            results.append({"status": resp.status_code, "elapsed": time.perf_counter() - t0})
        except Exception as e:
            results.append({"status": f"Exception:{type(e).__name__}", "elapsed": time.perf_counter() - t0})


async def run_cas_test(base_url: str, n: int) -> None:
    print(f"目標：{base_url}")
    print(f"CAS 並發數：{n}（對同一個 raw_id 同時搶 claim）")
    print()

    # Step 1: 建立一筆測試訂單，取得 raw_id
    orders_url = base_url.replace("/process_raw", "").rstrip("/")
    if not orders_url.endswith("/orders"):
        orders_url = orders_url.rstrip("/") + "/orders"

    async with httpx.AsyncClient() as client:
        payload = make_payload(99999)
        resp = await client.post(orders_url, json=payload, timeout=10.0)
        resp.raise_for_status()
        raw_id = resp.json()["raw_id"]
        order_id = payload["order_id"]
        print(f"  建立訂單   raw_id={raw_id}  order_id={order_id}")

    # Step 2: n 個 worker 立刻同時對同一個 raw_id 發 process_raw
    #         background task 也在搶，這是刻意的——模擬真實競爭
    replay_url = orders_url.replace("/orders", f"/process_raw/{raw_id}")
    sem = asyncio.Semaphore(n)
    results = []

    async with httpx.AsyncClient(limits=httpx.Limits(max_connections=n)) as client:
        t0 = time.perf_counter()
        await asyncio.gather(*[_replay(client, sem, replay_url, results) for _ in range(n)])
        elapsed = time.perf_counter() - t0

    http_ok = sum(1 for r in results if isinstance(r["status"], int) and 200 <= r["status"] < 300)
    print(f"  replay 請求  總={len(results)}  HTTP 200={http_ok}  失敗={len(results)-http_ok}  耗時={elapsed:.2f}s")
    print()
    print("  HTTP 200 只代表請求送達，不代表 claim 成功。")
    print("  CAS 結果要看 DB（uvicorn log 會有 'claim 失敗' 訊息）：")
    print()
    print(f"    SELECT status FROM raw WHERE id={raw_id};")
    print(f"    SELECT COUNT(*) FROM ods WHERE order_id='{order_id}';")
    print()
    print("  CAS 正確：raw.status='processed'，ODS COUNT=1")


# ── 進入點 ────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="POST /orders 負載測試")
    parser.add_argument("--url",         default="http://localhost:8000/orders", help="API base URL（吞吐量測試用）")
    parser.add_argument("--base-url",    default="http://localhost:8000",        help="API base URL（CAS 測試用）")
    parser.add_argument("--n",           type=int, default=1000,                 help="總請求數")
    parser.add_argument("--concurrency", type=int, default=50,                   help="同時發出的最大請求數")
    parser.add_argument("--duplicate",   action="store_true",                    help="所有請求送同一份 payload（相同 order_id）")
    parser.add_argument("--cas-test",    action="store_true",                    help="CAS lock 測試：n 個 worker 同時搶同一個 raw_id")
    parser.add_argument("--api-key",     default=None,                           help="X-API-Key header 值（/orders 需認證）")
    args = parser.parse_args()

    if args.cas_test:
        asyncio.run(run_cas_test(args.base_url, args.n))
    else:
        asyncio.run(main(args.url, args.n, args.concurrency, args.duplicate, args.api_key))
