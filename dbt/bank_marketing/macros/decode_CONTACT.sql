{#
    This macro returns the encoded value of the contact type
#}

{% macro encode_CONTACT(column_name) -%}
    case {{ column_name }}
       when 'cellular' then 1
       when 'telephone' then 2
   end
{%- endmacro %}