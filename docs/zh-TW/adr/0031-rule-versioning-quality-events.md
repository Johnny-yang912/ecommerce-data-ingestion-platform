# ADR-0031：規則版本化，加上 append-only 的 `quality_events` 狀態機

[English](../../en/adr/0031-rule-versioning-quality-events.md) | **繁體中文**

| | |
|---|---|
| **狀態** | Accepted |
| **日期** | 2026-07 |
| **層** | 資料品質 — 稽核 |

---

## 背景

`has_clean_error` 記錄了一筆記錄**曾經**失敗。它沒有記錄**在哪一套規則之下**，也沒有記錄**判定何時改變**。

沒有那些，ODS 與倉庫之間的分歧就無法回答：

```
ODS：has_clean_error = TRUE      Gold：那筆記錄存在且乾淨
→ 「為什麼這兩者不同？」          → 沒有答案
```

**一個布林值沒有歷史。** 任何需要解釋「判斷如何演進」的東西，都需要別的機制。

## 決策

兩個機制，合起來運作：

**一個規則版本常數。** `clean.py` 裡的 `DQ_RULE_VERSION`（目前是 `v4`），每次規則變更就 bump，並配一個 git tag 記錄改了什麼。每一列 ODS 存下它被評估時所用的版本，在 `dq_rule_version` 欄——**攝入時寫入一次，之後永不觸碰**。

**一份 append-only 的事件日誌。** `quality_events` 記錄每一次狀態轉移：

```
initial_evaluation
  ├── 通過所有規則              → clean
  └── has_clean_error = TRUE    → quarantined

quarantined / re_quarantined
  ├── 再評估通過                → promoted               (promotion)
  ├── 再評估未通過              → 不寫任何事件
  └── 人工註銷                  → permanently_rejected   (rejection)

promoted
  ├── 更嚴的規則下不通過        → re_quarantined         (re_quarantination)
  └── 仍然通過                  → 不寫任何事件

permanently_rejected            ← 終端；沒有出邊
```

這台狀態機有三個性質，本身就是決策：

**「不寫事件」是刻意的。** 只在確實改變時 append，正是讓這份日誌成為它自己的冪等閘門的原因（ADR-0030）。

**`permanently_rejected` 只能來自人。** 自動任務永不寫入、永不覆蓋它，在 PostgreSQL 的寫入目標上強制，而非靠下游過濾。

**`re_quarantination` 是後來才加的**，而加它沒有弄壞任何東西——因為 `rpt_quality_events_daily` 是按 `to_state` 而非 `event_type` 計數的，而 `int_` 層的 `CASE` 把 `re_quarantined` 摺進 `else 'quarantined'`。**這次擴充之所以安全，是消費端寫法的性質，不是運氣。**

## 後果

**分歧變得可稽核而非神祕：**

```
ODS：has_clean_error = TRUE, dq_rule_version = 'v1'    ← 攝入當下的真相
quality_events：2026-03-01 在 'v2' 下被 promoted        ← 演進之後的真相
→ 有時戳、有規則版本，解釋完成
```

**一筆記錄的完整品質歷史是可查詢的**，不只是它當前的狀態。

**改一條清洗規則不是小變更。** 它需要 bump `DQ_RULE_VERSION` 並跑一次再評估——**那正是 ADR-0006 為 NUL 毒藥丸選擇較小修法、而非哲學上一致的那個修法的原因。**

**代價是一個欄位、一張表，以及一份紀律。** 紀律是脆弱的那部分：沒有任何東西自動偵測「改了規則卻忘記 bump 版本」。它由 review 與部署 runbook 持有。

## 考慮過的替代方案

**一個可變的 `quality_status` 欄位。** 每筆記錄一列、變更時更新。更小、查詢更快——**而它摧毀歷史，那正是整件事的重點。**

**靠 git history 記錄規則版本。** Git 記錄的是**程式碼**何時改變。它無法告訴你**某一列**是在哪個版本下被評估的，而那才是真正的問題。

**逐規則版本化而非全域版本化。** 粒度更細、原則上也更誠實。以過早為由否決：全域版本足以解釋任何分歧，而逐規則版本化在沒有實證需求的情況下讓帳目倍增。

## 相關

- [ADR-0030](./0030-proposal-b-event-driven-reevaluation.md) — 寫入這份日誌的機制
- [ADR-0029](./0029-effective-quality-state.md) — 讀取它的機制
- [ADR-0033](./0033-historical-metrics-never-rewritten.md) — append-only 在下游買到什麼
- [ADR-0006](./0006-nul-byte-fast-fail.md) — 被「版本 bump 的代價」所形塑的一個決策
