WITH 
src AS (
  SELECT country_name, country_code, indicator_name, value, year
  FROM {{ source("nutrition", "health_nutrition_population") }}
  WHERE indicator_code = 'SH.XPD.CHEX.PP.CD'
),

result AS (
  SELECT country_name
  , country_code
  , indicator_name
  , max(value)
      over (partition by year)  AS highest_value
  , value                       AS country_value
  , year
  FROM src
)

SELECT country_name
  , country_code
  , indicator_name
  , highest_value
  , country_value
  , country_value - highest_value AS gap
  , year
FROM result