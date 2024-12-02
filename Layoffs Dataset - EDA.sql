-- Exploratory Data Analysis (This process is continuation of "Layoffs Dataset - Data Cleaning" file)
-- Checking the table

SELECT *
FROM world_layoffs.layoffs_staging2;

-- Looking at both max and min values of percentage and number values of lay offs to see how big these layoffs were

SELECT MAX(total_laid_off), MIN(total_laid_off)
FROM world_layoffs.layoffs_staging2;

SELECT MAX(percentage_laid_off), MIN(percentage_laid_off)
FROM world_layoffs.layoffs_staging2
WHERE percentage_laid_off IS NOT NULL;

-- Looking at which companies laid off 100 percent of their employees
-- When we check the funds_raised_million we can say some of them are startups

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE percentage_laid_off = 1;

-- By using ORDER BY function for funds_raised_million column we can see how big those companies
-- Some of them even raised more than a billion dollars

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;


-- Top 5 companies with the most single layoffs

SELECT company, total_laid_off
FROM world_layoffs.layoffs_staging
ORDER BY 2 DESC
LIMIT 10;

-- Top 10 companies with the most total lay offs
-- Most of them are big companies with a lot of employees

SELECT company, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY company
ORDER BY 2 DESC
LIMIT 10;

-- Top 10 total lay offs by locations

SELECT location, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY location
ORDER BY 2 DESC
LIMIT 10;

-- Top 10 total lay offs by countries

SELECT country, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY country
ORDER BY 2 DESC
LIMIT 10;

-- Total lay offs by industry

SELECT industry, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;

-- Total lay offs by stage
-- More than 200 thousands of the layoffs were the companies which were in the Post-IPO stage

SELECT stage, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY stage
ORDER BY 2 DESC;

-- Looking at the time interval of those layoffs

SELECT MIN(`date`), MAX(`date`)
FROM world_layoffs.layoffs_staging2;

-- Looking at the yearly total lay offs
-- Most of them were in the 2022. However as this data inclued first 3 months of the 2023, the number of 2023 is so high

SELECT YEAR(`date`), SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY 1 DESC;

-- Checking the top 5 companies of each year based on the total number of total_laid_off

WITH Company_Year (company, years, total_laid_off) AS
(
SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY company, YEAR(`date`)
), Company_Year_Rank AS
(SELECT *,
DENSE_RANK() OVER(PARTITION BY years ORDER BY total_laid_off DESC) AS Ranking
FROM Company_Year
WHERE years IS NOT NULL)
SELECT *
FROM Company_Year_Rank
WHERE Ranking <=5;

-- Checking total number of layoffs for each month of each year

SELECT SUBSTRING(`date`,1,7) as dates, SUM(total_laid_off) AS total_laid_off
FROM world_layoffs.layoffs_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY dates
ORDER BY dates ASC;

-- Writing it as CTE for writing query
-- We can see it as rolling total

WITH Rolling_Total AS
(
SELECT SUBSTRING(`date`, 1, 7) AS `MONTH`, SUM(total_laid_off) AS total_off
FROM world_layoffs.layoffs_staging2
WHERE SUBSTRING(`date`, 1, 7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC
)
SELECT `MONTH`, total_off
, SUM(total_off) OVER(ORDER BY `MONTH`) AS rolling_total
FROM Rolling_Total;







