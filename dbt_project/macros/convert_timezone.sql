{% macro to_amsterdam(column) %}
    ({{ column }} at time zone 'Europe/Amsterdam')
{% endmacro %}