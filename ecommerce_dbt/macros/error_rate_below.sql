{#
    Hard Gate（DQ 機制一，run-level）：has_clean_error 比率 >= threshold 時，
    測試回傳一列 → dbt test 失敗 → 整個 dbt run 中止，下游保留上次乾淨狀態。

    為何自訂 generic test，而非 dbt_utils.expression_is_true：
    後者是逐列測試（把條件塞進 WHERE NOT(...)），而 countif()/count(*) 是整表聚合，
    不能放進 WHERE（BQ 報 "Aggregate function COUNTIF not allowed in WHERE clause"）。
    比率斷言必須在聚合層級做，故用 HAVING（無 GROUP BY = 對選定範圍求單一值）。

    scope：斷言的口徑，兩者守的東西不同 ⭐
    ────────────────────────────────────────────────────────────────────────
      'table'（預設）
          全表聚合＝【資料集整體健康度】。只適合當儀表，**不可當閘門**，理由見下。

      'latest_partition'
          只看最新一個 partition_column 分區＝【最近一次攝入的那批】。這才是
          「上游是不是壞了」該問的問題：常態 3% 突然變 40%，代表 source 出事，
          管線該停下來等人看。阻斷權放這裡。

    為什麼阻斷權必須在逐批那支 ⭐
    ────────────────────────────────────────────────────────────────────────
    ① 靈敏度：全表比率的分母是累積歷史，日流量越大、歷史越長，單批異常能推動它的
       幅度越小。一個隨資料成長而越來越遲鈍的閘門，最需要它的時候正是它最沒用的時候。
    ② 可自癒：上游修好後新資料是乾淨的，但歷史髒資料永遠留在分母裡——全表比率不會
       因為問題解決而回落，閘門會一直擋著。屆時人的反應必然是調高門檻或關掉測試。
       **只能靠放寬自己來解除的閘門已經失效，且是以最糟的方式失效：它訓練人忽略它。**
       更一般地說，全表口徑的分母是「歷史保留與回填策略」的函數——調保留期、補跑
       backfill、--full-refresh 重建都會讓它變動，而這些與資料品質無關。
    ③ 職責：逐筆攔髒資料是 int_ 層 Row Filter 的事。這裡做的是突變偵測，不是清潔度
       檢查。讓同一個指標兼任兩種角色，是舊設計真正的錯誤。

    ⚠️ 'latest_partition' 不等於「這一次 extract」：staging 沒有 load batch id，
       日分區是最接近的代理。一天跑多次 extract 會併成一個判斷；一次 extract 跨兩個
       分區（`>=` watermark 重抓前一天）時只有最新那個會被斷言。
       以日批節奏兩者幾乎等價；要精確到批次，得先讓 extract 寫入批次欄位——
       但那會讓 stg_ 的 raw_id 去重決勝變成非決定性，是另一個獨立決策。

    ⚠️ 升到小時批時分區粒度必須一起改：排程改小時級而分區仍是 DAY 的話，
       「最新分區」會退化成「今天到目前為止」，稀釋問題在一天之內重演一次。

    ⚠️ 分區邊界是 UTC：received_at 為 TIMESTAMP，date() 在 UTC 換日。

    用法（severity 由呼叫端指定）：
      tests:
        - error_rate_below: {threshold: 0.1, config: {severity: warn}}
        - error_rate_below: {threshold: 0.15, scope: latest_partition,
                             config: {severity: error}}
#}
{% test error_rate_below(model, threshold, column_name='has_clean_error',
                         scope='table', partition_column='received_at') %}

{%- if scope not in ['table', 'latest_partition'] -%}
    {{ exceptions.raise_compiler_error(
        "error_rate_below: scope 必須是 'table' 或 'latest_partition'，收到 '" ~ scope ~ "'"
    ) }}
{%- endif -%}

with scoped as (

    select {{ column_name }}
    from {{ model }}
    {%- if scope == 'latest_partition' %}
    where date({{ partition_column }}) = (
        select max(date({{ partition_column }})) from {{ model }}
    )
    {%- endif %}

)

select
    countif({{ column_name }}) as error_count,
    count(*) as total_count,
    safe_divide(countif({{ column_name }}), count(*)) as error_rate
from scoped
having safe_divide(countif({{ column_name }}), count(*)) >= {{ threshold }}

{% endtest %}
