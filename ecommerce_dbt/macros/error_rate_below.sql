{#
    Hard Gate（DQ 機制一，run-level）：整批 has_clean_error 比率 >= threshold 時，
    測試回傳一列 → dbt test 失敗 → 整個 dbt run 中止，下游保留上次乾淨狀態。

    為何自訂 generic test，而非 dbt_utils.expression_is_true：
    後者是逐列測試（把條件塞進 WHERE NOT(...)），而 countif()/count(*) 是整表聚合，
    不能放進 WHERE（BQ 報 "Aggregate function COUNTIF not allowed in WHERE clause"）。
    整批比率斷言必須在聚合層級做，故用 HAVING（無 GROUP BY = 對全表求值）。

    用法（severity 由呼叫端指定）：
      tests:
        - error_rate_below: {threshold: 0.1, config: {severity: error}}
#}
{% test error_rate_below(model, threshold, column_name='has_clean_error') %}

select
    countif({{ column_name }}) as error_count,
    count(*) as total_count,
    safe_divide(countif({{ column_name }}), count(*)) as error_rate
from {{ model }}
having safe_divide(countif({{ column_name }}), count(*)) >= {{ threshold }}

{% endtest %}
