WITH 
    nutrition as ( select * from {{ ref('stg_health_nutrition_population') }} ),
    population as ( select * from {{ ref('stg_population_by_country') }} )


SELECT n.country_name
  , n.country_code
  , n.year
  , p.population
  , p.population / LAG(p.population)
      over (partition by p.country_code order by p.as_of_year)  AS pct_change_in_population
  , n.country_value                                             AS health_expenditure_per_capita
  , n.country_value / LAG(n.country_value) 
      over (partition by n.country_code order by n.year)        AS pct_change_in_health_expenditure_per_capita

FROM nutrition n
  JOIN population p
      ON n.country_code = p.country_code
        and n.year = p.as_of_year