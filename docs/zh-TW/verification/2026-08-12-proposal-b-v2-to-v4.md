# 2026-08-12 — 重建 fixture 並背靠背走過 v2→v3→v4

[English](../../en/verification/2026-08-12-proposal-b-v2-to-v4.md) | **繁體中文**

---

## 驗證的假設

遷移到原生 Docker Engine 會置換 Docker 的 DataRoot，因而讓業務 DB 拿到一個全新的 volume。**那個代價在遷移之前就被權衡並接受了**——fixture 是可以直接重新灌的模擬資料，而為它保留備份等於為一個沒有價值的東西付維護成本。

既然反正得重灌，這次重建就當成一次**完整的 SOP 演練**：一批**刻意塑形**的資料從 v2 一路走到 v4，補上先前幾次覆蓋不到的分支。

## 唯一無法迴避的取捨

**重放的是「規則狀態」，不是「commit 狀態」。**

`git checkout dq-rules-v2` 行不通：`business_clean` 的 `(ods, as_of)` 簽章、`NON_REPRODUCIBLE_CODES`、以及 `AGE_MIN`／`AGE_MAX` 常數**全都是在 v3 才引入的**，所以真正的回滾會讓 `reevaluate_quality.py` 在 import 就失敗。

改採的做法是：**HEAD 的程式碼 + 那個版本的閾值 + 那個版本的標籤。**

對這次演練的兩個違規 code（`age_out_of_range`、`field_too_long`）而言，那在行為上與真正的舊版本完全相同：

- v2 缺少的 `as_of` 參數，在攝入路徑省略它時行為相同；
- `NON_REPRODUCIBLE_CODES` 只在再評估時才有作用——而 v2 那批只被**攝入**，從來不會**以 v2 的身分**被再評估。

**v4 那一格則是完全相同的**：`git diff dq-rules-v4 HEAD -- clean.py` 是空的，所以結束狀態就是 `git checkout clean.py`，整個重放**零程式碼殘留**。

> **基於同樣的理由，沒有動任何 git tag。** `dq-rules-v2/v3/v4` 指向真實的歷史 commit；為了一次資料重放而新增或移動它們，等於用一個合成的紀錄汙染一個真實的紀錄。

## 觀測

| 循環 | Promote |
|---|---|
| v2 → v3 | **16** |
| v3 → v4 | **15** |

兩個循環：重跑皆冪等、**ODS 從未被修改**、對照組留在隔離區。

加上先前兩次演練，這份 SOP 至今已被執行**四次**：

| 日期 | 循環 | Promote |
|---|---|---|
| 2026-08-05 | v2 → v3 | 15 |
| 2026-08-11 | v3 → v4 | 3 |
| 2026-08-12 | v2 → v3（重建） | 16 |
| 2026-08-12 | v3 → v4（重建） | 15 |

## 結論

這份 SOP 跨規則版本、跨一次完整環境重建都可重複。**背靠背那一次才是關鍵**：它在「已經被 promote 過一次的資料」上演練 v3 → v4，而那是唯一會碰到 `promoted → re_quarantined` 這條邊鄰域的路徑。

## 關於這則記錄不是什麼

這**不是**一次災難復原測試。volume 的遺失是一次遷移決策的**計畫內後果**，事前基於「資料可重現」而被接受。把它記成復原會誇大它。

它確實展示的，是讓那個決策變便宜的那個性質：**一份可以由 seeding 腳本重新產生的 fixture，不是你需要備份的東西。** 那是模擬上游的一個設計性質，不是一次僥倖。

## 相關

- [2026-08-05-proposal-b-v3](./2026-08-05-proposal-b-v3.md) · [2026-08-11-full-compose-rebuild-v4](./2026-08-11-full-compose-rebuild-v4.md)
- [incidents/2026-08-silent-scheduling-stalls](../incidents/2026-08-silent-scheduling-stalls.md) — 促成這次重建的那次遷移
- [runbooks/proposal-b-rollout](../runbooks/proposal-b-rollout.md)
