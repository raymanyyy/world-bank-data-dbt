select 
    country, 
    country_code,
    2010        AS as_of_year,
    year_2010   AS population,
from {{ source("population", "population_by_country") }}

{% for year_n in range(2011, 2020) %}
UNION ALL
select 
    country, 
    country_code,
    {{year_n}}      AS as_of_year,
    year_{{year_n}} AS population,
from {{ source("population", "population_by_country") }}
{% endfor %}