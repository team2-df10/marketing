{#
    This macro returns the encoded value of the housing loan
#}

{% macro encode_HOUSING(column_name) -%}
    case {{ column_name }}
       when 'no' then 1
       when 'yes' then 2
       when 'unknown' then 3
   end
{%- endmacro %}
