{#
    This macro returns the encoded value of the default credit
#}

{% macro encode_CREDIT(column_name) -%}
    case {{ column_name }}
       when 'no' then 1
       when 'yes' then 2
       when 'unknown' then 3
   end
{%- endmacro %}
