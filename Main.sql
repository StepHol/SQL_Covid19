
DROP TABLE IF EXISTS t_stepan_holub_tests;
DROP TABLE IF EXISTS t_stepan_holub_covid;
DROP TABLE IF EXISTS t_stepan_holub_gini;
DROP TABLE IF EXISTS t_stepan_holub_economies;
DROP TABLE IF EXISTS t_stepan_holub_economies_2019;
DROP TABLE IF EXISTS t_stepan_holub_religions;
DROP TABLE IF EXISTS t_stepan_holub_le_diff;
DROP TABLE IF EXISTS t_stepan_holub_weather;
DROP TABLE IF EXISTS t_stepan_holub_projekt_SQL_final;

--  Table t_stepan_holub_tests
CREATE TABLE IF NOT EXISTS t_stepan_holub_tests AS
SELECT
	ctp.`date`,
	CASE WHEN ctp.country = 'Myanmar' THEN 'Burma'
	WHEN ctp.country = 'Cape Verde' THEN 'Cabo Verde'
	WHEN ctp.country = 'Congo' THEN 'Congo (Brazzaville)'
	WHEN ctp.country = 'The Democratic Republic of Congo' THEN 'Congo (Kinshasa)'
	WHEN ctp.country = 'Swaziland' THEN 'Eswatini'
	WHEN ctp.country = 'Fiji Islands' THEN 'Fiji'
	WHEN ctp.country = 'Holy See (Vatican City State)' THEN 'Holy See'
	WHEN ctp.country = 'South Korea' THEN 'Korea, South'
	WHEN ctp.country = 'Libyan Arab Jamahiriya' THEN 'Libya'
	WHEN ctp.country = 'Micronesia, Federated States of' THEN 'Micronesia'
	WHEN ctp.country = 'Russian Federation' THEN 'Russia'
	WHEN ctp.country = 'United States' THEN 'US'
	ELSE ctp.country
	END AS country,
	tests_performed 
FROM Covid19_tests_performed ctp;

CREATE TABLE IF NOT EXISTS t_stepan_holub_covid AS 
SELECT 
	cbd.`date`,
	CASE WHEN cbd.country = 'Czechia' THEN 'Czech Republic' ELSE cbd.country END AS country,
	ROUND(cbd.confirmed / t.tests_performed, 2) AS positivity_rate,
	ROUND(cbd.confirmed / lt.population * 1000000, 2) AS positive_per_mil
-- 	ROUND(t.tests_performed / lt.population * 1000000, 2) AS tests_per_mil
FROM covid19_basic_differences cbd 
INNER JOIN t_stepan_holub_tests t
	ON cbd.`date` = t.`date` 
	AND cbd.country = t.country
LEFT JOIN lookup_table lt 
	ON cbd.country = lt.country
WHERE 1=1
	AND lt.province IS NULL
	AND cbd.confirmed IS NOT NULL
	AND t.tests_performed IS NOT NULL 
ORDER BY `date`, country;



CREATE TABLE IF NOT EXISTS t_stepan_holub_economies AS
SELECT 
	CASE WHEN country = 'Myanmar' THEN 'Burma'
	WHEN country = 'Congo' THEN 'Congo (Brazzaville)'
	WHEN country = 'The Democratic Republic of Congo' THEN 'Congo (Kinshasa)'
	WHEN country = 'Ivory Coast' THEN "Cote d'Ivoire"
	WHEN country = 'Swaziland' THEN 'Eswatini'
	WHEN country = 'South Korea' THEN 'Korea, South'
	WHEN country = 'Micronesia, Fed. Sts.' THEN 'Micronesia'
	WHEN country = 'Russian Federation' THEN 'Russia'
	WHEN country = 'United States' THEN 'US'
	ELSE country
	END AS country,
	`year`,
	GDP,
	population,
	gini,
	mortaliy_under5 
FROM economies e;

CREATE TABLE IF NOT EXISTS t_stepan_holub_gini as
WITH base AS(
SELECT 
	country,
	gini,
	`year` ,
	max(`year`) OVER(PARTITION BY country) last_gini_year
	FROM t_stepan_holub_economies
	where gini IS NOT NULL
)
SELECT 
	country,
	gini
FROM base
WHERE `year` = last_gini_year;

CREATE TABLE  IF NOT EXISTS t_stepan_holub_economies_2019 AS
SELECT country, GDP, population, mortaliy_under5 
FROM t_stepan_holub_economies
WHERE `year` = '2019'




CREATE TABLE IF NOT EXISTS t_stepan_holub_religions
WITH base AS (
SELECT
	DISTINCT r.country,
	sum(r.population) OVER(PARTITION BY country) AS pop
FROM religions r 
WHERE r.`year` = 2020
)
SELECT
	CASE WHEN b.country = 'Myanmar' THEN 'Burma'
	WHEN b.country = 'Congo' THEN 'Congo (Brazzaville)'
	WHEN b.country = 'The Democratic Republic of Congo' THEN 'Congo (Kinshasa)'
	WHEN b.country = 'Ivory Coast' THEN "Cote d'Ivoire"
	WHEN b.country = 'Swaziland' THEN 'Eswatini'
	WHEN b.country = 'South Korea' THEN 'Korea, South'
	WHEN b.country = 'Micronesia, Fed. Sts.' THEN 'Micronesia'
	WHEN b.country = 'Russian Federation' THEN 'Russia'
	WHEN b.country = 'United States' THEN 'US'
	ELSE b.country
	END AS country,
	ROUND(r2.population / pop * 100, 2)  AS  christianity_pct,
	ROUND(r3.population / pop * 100, 2)  AS  islam_pct,
	ROUND(r4.population / pop * 100, 2)  AS  unaffiliated_religions_pct,
	ROUND(r5.population / pop * 100, 2)  AS  hinduism_pct,
	ROUND(r6.population / pop * 100, 2)  AS  buddhism_pct,
	ROUND(r7.population / pop * 100, 2)  AS  folk_religions_pct,
	ROUND(r8.population / pop * 100, 2)  AS  other_religions_pct,
	ROUND(r9.population / pop * 100, 2)  AS  judaism_pct
FROM base b
LEFT JOIN religions r2
	ON b.country = r2.country
LEFT JOIN religions r3
	ON b.country = r3.country
LEFT JOIN religions r4
	ON b.country = r4.country
LEFT JOIN religions r5
	ON b.country = r5.country
LEFT JOIN religions r6
	ON b.country = r6.country
LEFT JOIN religions r7
	ON b.country = r7.country
LEFT JOIN religions r8
	ON b.country = r8.country
LEFT JOIN religions r9
	ON b.country = r9.country
WHERE 1=1
AND r2.`year` = 2020
AND r3.`year` = 2020
AND r4.`year` = 2020
AND r5.`year` = 2020
AND r6.`year` = 2020
AND r7.`year` = 2020
AND r8.`year` = 2020
AND r9.`year` = 2020
AND r2.religion  = 'Christianity'
AND r3.religion  = 'Islam'
AND r4.religion  = 'Unaffiliated Religions'
AND r5.religion  = 'Hinduism'
AND r6.religion  = 'Buddhism'
AND r7.religion  = 'Folk Religions'
AND r8.religion  = 'Other Religions'
AND r9.religion  = 'Judaism';

CREATE TABLE IF NOT EXISTS t_stepan_holub_le_diff AS 
SELECT 	
	CASE WHEN le.country = 'Myanmar' THEN 'Burma'
	WHEN le.country = 'Congo' THEN 'Congo (Brazzaville)'
	WHEN le.country = 'The Democratic Republic of Congo' THEN 'Congo (Kinshasa)'
	WHEN le.country = 'Ivory Coast' THEN "Cote d'Ivoire"
	WHEN le.country = 'Swaziland' THEN 'Eswatini'
	WHEN le.country = 'South Korea' THEN 'Korea, South'
	WHEN le.country = 'Micronesia, Fed. Sts.' THEN 'Micronesia'
	WHEN le.country = 'Russian Federation' THEN 'Russia'
	WHEN le.country = 'United States' THEN 'US'
	ELSE le.country
	END AS country,
	ROUND(le2.life_expectancy - le.life_expectancy, 2) AS le_diff_1965_2015
FROM life_expectancy le 
JOIN life_expectancy le2 
	ON le.country = le2.country 
WHERE le.`year` = 1965
AND le2.`year` = 2015;

CREATE TABLE IF NOT EXISTS t_stepan_holub_weather AS
WITH rain_gust AS 
(
SELECT
	w.city,
	date(w.`date`) AS `date`,
	SUM(CAST(TRIM(TRAILING ' mm' FROM w.rain) AS float) >= 0.3) * 3 AS rainy_hours,
	CAST(TRIM(TRAILING ' km/h' FROM w.gust) AS float) AS gust_km_h
FROM weather w 
WHERE 1=1
AND city IS NOT NULL 
GROUP BY w.`date`, w.city
),
avg_day_temperature AS 
(
SELECT 
	w.city,
	date(w.`date`) AS `date`,
	AVG(CAST(TRIM(TRAILING ' °c' FROM w.temp) AS int)) AS day_temp_cls
FROM weather w 
WHERE 1 = 1 
AND w.city IS NOT NULL 
AND w.`time` IN ('06:00', '09:00', '12:00', '15:00', '18:00')
GROUP BY w.`date`, w.city
)
SELECT 
	CASE WHEN rg.city = 'Moscow' THEN 'Russia'
	WHEN c.country IS NOT NULL THEN c.country
	WHEN rg.city = 'Athens' THEN 'Greece'
	WHEN rg.city = 'Athens' THEN 'Greece'
	WHEN rg.city = 'Brussels' THEN 'Belgium'
	WHEN rg.city = 'Bucharest' THEN 'Romania'
	WHEN rg.city = 'Helsinki' THEN 'Finland'
	WHEN rg.city = 'Kiev' THEN 'Ukraine'
	WHEN rg.city = 'Lisbon' THEN 'Portugal'
	WHEN rg.city = 'Luxembourg' THEN 'Luxembourg'
	WHEN rg.city = 'Prague' THEN 'Czech Republic'
	WHEN rg.city = 'Rome' THEN 'Italy'
	WHEN rg.city = 'Vienna' THEN 'Austria'
	WHEN rg.city = 'Warsaw' THEN 'Poland'
	END AS country,
	rg.`date`,
	adt.day_temp_cls,
	rg.rainy_hours,
	rg.gust_km_h
FROM rain_gust rg
JOIN avg_day_temperature adt
	ON rg.city = adt.city
	AND rg.`date` = adt.`date`
LEFT JOIN countries c
	ON rg.city = c.capital_city 
	


CREATE TABLE IF NOT EXISTS t_stepan_holub_projekt_SQL_final AS
SELECT 
	cov.`date`,
	cov.country,
	cov.positivity_rate,
	cov.positive_per_mil,
		-- 	weekend
	CASE WHEN WEEKDAY(cov.`date`) IN (5,6) THEN 1 ELSE 0 END AS weekend,
	-- 	season
	CASE WHEN DAYOFYEAR(cov.`date`) BETWEEN 81 AND 172 THEN 0
		WHEN DAYOFYEAR(cov.`date`) BETWEEN 173 AND 266 THEN 1
		WHEN DAYOFYEAR(cov.`date`) BETWEEN 267 AND 356 THEN 2
		WHEN DAYOFYEAR(cov.`date`) BETWEEN 357 AND 366 THEN 3
		WHEN DAYOFYEAR(cov.`date`) BETWEEN 0 AND 80 THEN 3
	END AS season,
	ROUND(c.population_density, 2) AS population_density,
	ROUND(te.GDP / te.population, 2) AS GDP_per_capita,
	tg.gini,
	te.mortaliy_under5,
	c.median_age_2018,
	tr.christianity_pct,	tr.islam_pct,	tr.unaffiliated_religions_pct,	tr.hinduism_pct,	tr.buddhism_pct,	tr.folk_religions_pct,	tr.other_religions_pct,	tr.judaism_pct,
	tle.le_diff_1965_2015,
	tw.day_temp_cls,
	tw.rainy_hours,
	tw.gust_km_h
FROM t_stepan_holub_covid cov
LEFT JOIN countries c
	ON cov.country = c.country
LEFT JOIN t_stepan_holub_gini tg
	ON cov.country = tg.country
LEFT JOIN t_stepan_holub_economies_2019 te
	ON cov.country = te.country
LEFT JOIN t_stepan_holub_religions tr
	ON cov.country = tr.country
LEFT JOIN t_stepan_holub_le_diff tle
	ON cov.country = tle.country
INNER JOIN t_stepan_holub_weather tw 
	ON cov.`date` = tw.`date`
	AND cov.country = tw.country
ORDER BY cov.`date`, cov.country 


SELECT * FROM t_stepan_holub_projekt_SQL_final


SELECT * FROM countries




SELECT DISTINCT country FROM v_stepan_holub_rain_gust
EXCEPT
SELECT DISTINCT country FROM covid19_basic_differences cbd 


(SELECT DISTINCT country FROM covid19_basic_differences cbd 
EXCEPT
SELECT DISTINCT country FROM v_stepan_holub_rain_gust)






SELECT DISTINCT country FROM life_expectancy le 
EXCEPT
SELECT DISTINCT country FROM covid19_basic_differences cbd 


(SELECT DISTINCT country FROM covid19_basic_differences cbd 
EXCEPT
SELECT DISTINCT country FROM life_expectancy le)


(SELECT DISTINCT country FROM Covid19_tests_performed
EXCEPT
SELECT DISTINCT country FROM covid19_basic_differences cbd )



SELECT
min(date),
max(date)
FROM Covid19_tests_performed ctp 

SELECT
min(date),
max(date)
FROM covid19_basic_differences
2020-01-22	2021-05-23


SELECT
min(date),
max(date)
FROM Covid19_tests_performed ctp
2020-01-22	2021-05-23