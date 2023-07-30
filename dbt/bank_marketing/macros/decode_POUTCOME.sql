{#
    This macro returns the encoded value of the poutcome column
#}

{% macro encode_POUTCOME(column_name) -%}
    case {{ column_name }}
       when 'failure' then 1
       when 'nonexistent' then 2
       when 'success' then 3
   end
{%- endmacro %}