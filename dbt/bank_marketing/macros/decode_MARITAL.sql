{#
    This macro returns the encoded value of the marital status
#}

{% macro encode_MARITAL(column_name) -%}
    case {{ column_name }}
       when 'divorced' then 1
       when 'married' then 2
       when 'single' then 3
       when 'unknown' then 4
   end
{%- endmacro %}