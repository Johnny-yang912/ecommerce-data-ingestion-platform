{#
    DQ 錯誤碼工具。

    clean_error_message 是 JSON 物件陣列 [{code, field, value, ...}]（docs/zh-TW/design/cloud-layer.md
    已實機驗證落地為 JSON_TYPE=array）。下游一律以【穩定的 code】比對，不比對人類可讀
    措辭——措辭可調整、code 不變（見 clean.py 的 DQCode 與 docs/zh-TW/design/data-quality.md 機制三）。
#}

{#
    把 clean_error_message 攤平成 ARRAY<STRING> 的 code 清單。
    JSON null / 空陣列 → 空陣列（UNNEST(NULL) 產生零列）。
#}
{% macro dq_error_codes(json_col) -%}
array(
    select json_value(_e, '$.code')
    from unnest(json_query_array({{ json_col }})) as _e
)
{%- endmacro %}
