-- =========================================
-- SQL DATA CLEANING PROJECT
-- Dataset: Tech Layoffs
-- =========================================

-- Step 1: Create staging table
CREATE TABLE layoffs_staging AS
SELECT *
FROM layoffs;

-- Step 2: Identify duplicate records
WITH duplicate_cte AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY company, location, industry, total_laid_off,
                            percentage_laid_off, date, stage, country,
                            funds_raised_millions
           ) AS row_num
    FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

-- Step 3: Remove duplicates safely
DELETE FROM layoffs_staging
WHERE (company, location, industry, total_laid_off,
       percentage_laid_off, date, stage, country,
       funds_raised_millions) IN (
    SELECT company, location, industry, total_laid_off,
           percentage_laid_off, date, stage, country,
           funds_raised_millions
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY company, location, industry, total_laid_off,
                                percentage_laid_off, date, stage, country,
                                funds_raised_millions
               ) AS row_num
        FROM layoffs_staging
    ) t
    WHERE row_num > 1
);

-- Step 4: Standardize text fields
UPDATE layoffs_staging
SET industry = TRIM(industry);

UPDATE layoffs_staging
SET country = TRIM(country);

-- Step 5: Handle NULL industries using company match
UPDATE layoffs_staging t1
JOIN layoffs_staging t2
  ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
  AND t2.industry IS NOT NULL;

-- Step 6: Remove rows where both layoff values are NULL
DELETE FROM layoffs_staging
WHERE total_laid_off IS NULL
  AND percentage_laid_off IS NULL;

-- Step 7: Convert date column to DATE format
UPDATE layoffs_staging
SET date = STR_TO_DATE(date, '%m/%d/%Y');

ALTER TABLE layoffs_staging
MODIFY COLUMN date DATE;

-- Step 8: Final cleaned dataset
SELECT *
FROM layoffs_staging;
