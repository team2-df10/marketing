{#
    This macro returns the encoded value of the job
#}

{% macro encode_JOB(column_name) -%}
    case {{ column_name }}
       when 'admin.' then 1
       when 'blue-collar' then 2
       when 'entrepreneur' then 3
       when 'housemaid' then 4
       when 'management' then 5
       when 'retired' then 6
       when 'self-employed' then 7
       when 'services' then 8
       when 'student' then 9
       when 'technician' then 10
       when 'unemployed' then 11
       when 'unknown' then 12
   end
{%- endmacro %}