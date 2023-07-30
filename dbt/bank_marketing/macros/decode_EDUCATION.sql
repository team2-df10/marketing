{#
    This macro returns the encoded value of the education 
#}

{% macro encode_EDUCATION(column_name) -%}
    case {{ column_name }}
       when 'basic' then 1
       when 'high.school' then 2
       when 'illiterate' then 3
       when 'professional.course' then 4
       when 'university.degree' then 5
       when 'unknown' then 6
   end
{%- endmacro %}