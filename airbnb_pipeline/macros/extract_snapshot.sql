{% macro extract_snapshot_date(filename_col) %}
    TO_DATE(REGEXP_SUBSTR({{ filename_col }}, 'dt=(\\d{4}-\\d{2}-\\d{2})', 1, 1, 'e', 1))
{% endmacro %}