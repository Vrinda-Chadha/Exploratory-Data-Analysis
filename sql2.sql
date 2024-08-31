-- CLEANING DATA FROM LAYOFF DATABASE
SELECT * FROM layoffs;

-- create a staging table. 
-- the one to work  and clean the data.

CREATE TABLE layoff LIKE layoffs;
-- check the staging table
SELECT * FROM layoff;

INSERT layoff
SELECT * FROM layoffs;

SELECT * FROM layoff;
-- steps for data cleaning
-- 1. check for duplicates and remove any
-- 2. standardize data and fix errors
-- 3. Look at null values and see what 
-- 4. remove any columns and rows that are not necessary - few ways

-- 1. Remove Duplicates
CREATE TABLE layoff_staging LIKE layoff;

INSERT layoff_staging
SELECT DISTINCT * FROM layoffs
;

SELECT *
FROM layoff_staging;
-- ALTERNATIVE WAY
SELECT *
FROM (
	SELECT company, location, industry, total_laid_off,percentage_laid_off,'date', stage, country, funds_raised_millions,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,'date', stage, country, funds_raised_millions
			order by company 
			) AS row_num
	FROM 
		layoffs
) AS A
WHERE 
	row_num > 1;

-- these are the ones we want to delete where the row number is > 1 or 2or greater essentially

WITH DELETE_CTE AS 
(
SELECT *
FROM (
	SELECT company, location, industry, total_laid_off,percentage_laid_off,'date', stage, country, funds_raised_millions,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,'date', stage, country, funds_raised_millions order by company
			) AS row_num
	FROM 
		layoffs
) duplicates
WHERE 
	row_num > 1
)
DELETE
FROM DELETE_CTE
;

ALTER TABLE world_layoffs.layoffs_staging ADD row_num INT;

SELECT *
FROM world_layoffs.layoffs_staging
;

CREATE TABLE world_layoffs.layoffs_staging2 (
company text,
`location`text,
`industry`text,
total_laid_off INT,
percentage_laid_off text,
date text,
`stage`text,
country text,
funds_raised_millions int,
row_num INT
);

INSERT INTO world_layoffs.layoffs_staging2
(company,
location,
industry,
total_laid_off,
percentage_laid_off,
date,
stage,
country,
funds_raised_millions,
row_num)
SELECT company,
location,
industry,
total_laid_off,
percentage_laid_off,
date,
stage,
country,
funds_raised_millions,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,date, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		world_layoffs.layoffs_staging;

-- now that we have this we can delete rows were row_num is greater than 2

DELETE FROM world_layoffs.layoffs_staging2
WHERE row_num >= 2;




-- 2. Standardize Data
-- Start with company column 
SELECT * 
FROM layoff_staging;

-- removing blank spaces 
SELECT company , TRIM(company) 
FROM layoff_staging;

UPDATE layoff_staging 
SET company = TRIM(company);

-- Industry 
SELECT DISTINCT industry 
FROM layoff_staging ORDER BY industry; 

UPDATE layoff_staging 
SET industry = 'Crypto' WHERE industry LIKE 'Crypto%'
;

SELECT DISTINCT industry 
FROM layoff_staging ORDER BY industry; 

-- location 
SELECT DISTINCT country
FROM layoff_staging
ORDER BY country;

UPDATE layoff_staging 
SET country = 'United States' WHERE country LIKE 'United States.'
;

-- date

SELECT * FROM layoff_staging;

UPDATE layoff_staging
SET `DATE` =
str_to_date(`DATE` , '%m/%d/%Y');

-- total_laid_off
SELECT *
FROM layoff_staging
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL
;

SELECT *
FROM layoff_staging
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

SELECT *
FROM layoff_staging
WHERE company LIKE 'Bally%';

SELECT *
FROM layoff_staging
WHERE company LIKE 'airbnb%';

-- airbnb is a travel, but this one just isn't populated.
-- if there is another row with the same company name, it will update it to the non-null industry values

-- we should set the blanks to nulls since those are typically easier to work with
UPDATE layoff_staging
SET industry = NULL
WHERE industry = '';

SELECT *
FROM layoff_staging
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

SELECT t1.industry , t2.industry
from layoff_staging t1
JOIN layoff_staging t2
ON t1.company = t2.company
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

UPDATE layoff_staging t1
JOIN layoff_staging t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

SELECT * FROM layoff_staging;

DELETE
FROM layoff_staging
WHERE total_laid_off is null AND percentage_laid_off is null;

-- EXPORATORY DATA ANALYSIS
SELECT *
FROM layoff_staging;

SELECT MAX(TOTAL_LAID_OFF) 
FROM layoff_staging;

SELECT MAX(percentage_laid_off)
FROM layoff_staging;

-- Looking at Percentage to see how big these layoffs were
SELECT MAX(percentage_laid_off),  MIN(percentage_laid_off)
FROM layoff_staging
WHERE  percentage_laid_off IS NOT NULL;

-- 1 implies 100% company layoff -- company closed
SELECT *
FROM layoff_staging
WHERE percentage_laid_off = 1;

-- these are mostly startups it looks like who all went out of business during this time

-- if we order by funs_raised_millions we can see how big some of these companies were
SELECT *
FROM layoff_staging
WHERE  percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;
-- BritishVolt looks like an EV company

SELECT company, total_laid_off
FROM layoff_staging
ORDER BY 2 DESC
LIMIT 5; 

-- Companies with the most Total Layoffs
SELECT company, SUM(total_laid_off)
FROM layoff_staging
GROUP BY company
ORDER BY 2 DESC
LIMIT 10;

-- by location
SELECT location, SUM(total_laid_off)
FROM layoff_staging
GROUP BY location
ORDER BY 2 DESC
LIMIT 10;

-- by INDUSTRY
SELECT INDUSTRY , SUM(total_laid_off)
FROM layoff_staging
GROUP BY INDUSTRY
ORDER BY 2 DESC
LIMIT 10;

-- by DATE -- DATE ON WHICH HIGHEST LAID OFF 
SELECT`DATE`,SUM(total_laid_off)
FROM layoff_staging
GROUP BY `DATE`
ORDER BY 1 DESC
LIMIT 10;

SELECT MIN(date) , MAX(DATE)
FROM layoff_staging;

-- this it total in the past 3 years or in the dataset

SELECT country, SUM(total_laid_off)
FROM layoff_staging
GROUP BY country
ORDER BY 2 DESC;

SELECT YEAR(date), SUM(total_laid_off)
FROM layoff_staging
GROUP BY YEAR(date)
ORDER BY 1 ASC;

SELECT stage, SUM(total_laid_off)
FROM layoff_staging
GROUP BY stage
ORDER BY 2 DESC;

SELECT * FROM layoff_staging;

-- TOTAL WORKERS LAID OFF MONTH WISE
SELECT SUBSTRING(`DATE` , 6, 2)AS 'MONTH' , SUM(total_laid_off)
FROM layoff_staging
GROUP BY SUBSTRING(`DATE` , 6, 2) 
; 

-- TOTAL WORKERS LAID OFF YEAR WISE
SELECT SUBSTRING(`DATE` , 1, 7)AS `MONTH` , SUM(total_laid_off)
FROM layoff_staging
WHERE SUBSTRING(`DATE` , 1, 7) IS NOT NULL
GROUP BY SUBSTRING(`DATE` , 1, 7) 
ORDER BY 1 ASC
; 

-- ROLLING TOTAL 
WITH ROLLING_TOTAL AS
(
SELECT SUBSTRING(`DATE` , 1, 7)AS `MONTH` , SUM(total_laid_off) AS TOTAL_EMP_LAID_OFF
FROM layoff_staging
WHERE SUBSTRING(`DATE` , 1, 7) IS NOT NULL
GROUP BY SUBSTRING(`DATE` , 1, 7) 
ORDER BY 1 ASC
)
SELECT `MONTH`, 
TOTAL_EMP_LAID_OFF,
SUM(TOTAL_EMP_LAID_OFF) OVER(ORDER BY `MONTH` ) AS ROLLING_TOTAL
FROM ROLLING_TOTAL
;

-- HIGHEST LAYOFF COMPANY WISE (WITH YEARS)
SELECT company, YEAR(date) AS years, SUM(total_laid_off) AS total_laid_off
FROM layoff_staging
GROUP BY company, YEAR(date)
ORDER BY 3 DESC
;

WITH Company_Year AS 
(
  SELECT company, YEAR(date) AS years, SUM(total_laid_off) AS total_laid_off
  FROM layoff_staging
  GROUP BY company, YEAR(date)
)
, Company_Year_Rank AS (
  SELECT company, years, total_laid_off, DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS ranking
  FROM Company_Year
)
SELECT company, years, total_laid_off, ranking
FROM Company_Year_Rank
WHERE ranking <= 4
AND years IS NOT NULL
ORDER BY years ASC, total_laid_off DESC;
