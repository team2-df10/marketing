{#
    This macro returns the encoded value of the loan status
#}

{% macro encode_LOAN(column_name) -%}
    case {{ column_name }}
       when 'no' then 1
       when 'yes' then 2
       when 'unknown' then 3
   end
{%- endmacro %}
