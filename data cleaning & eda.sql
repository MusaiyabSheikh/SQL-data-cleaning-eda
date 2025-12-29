--- Data cleaning

SELECT *
FROM layoffs;

-- 1.remove duplicates
-- 2.standarize the data
-- 3.null values or blank values
-- 4.remove any columns

Create Table layoffs_staging
Like layoffs;

Select *
from layoffs_staging;

INSERT layoffs_staging
Select *
from layoffs;



Select *
from layoffs;

Select *,
Row_Number() Over(
Partition by company,industry,total_laid_off,percentage_laid_off,`date`) AS row_num
from layoffs_staging;

WITH duplicate_cte As
(Select *,
Row_Number() Over(
Partition by company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) AS row_num
from layoffs_staging
)
select *
from duplicate_cte
where row_num > 1;

Select *
from layoffs_staging
where company = 'Casper';

WITH duplicate_cte As
(Select *,
Row_Number() Over(
Partition by company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) AS row_num
from layoffs_staging
)
select *
from duplicate_cte
where row_num > 1;

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


select *
from layoffs_staging2;

Insert into layoffs_staging2
Select *,
Row_Number() Over(
Partition by company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) AS row_num
from layoffs_staging;



select *
from layoffs_staging2
where row_num > 1;

delete
from layoffs_staging2
where row_num > 1;

select *
from layoffs_staging2
;

-- standardize the data

select company, (Trim(company))
from layoffs_staging2;

update layoffs_staging2
set company = (Trim(company));

select distinct industry
from layoffs_staging2
Order by 1;

select * from layoffs_staging2
where industry like 'Crypto%';

update layoffs_staging2
set industry = 'Crypto'
where industry like 'Crypto%';

select distinct industry
from layoffs_staging2;

select distinct location 
from layoffs_staging2
order by 1;

select distinct country 
from layoffs_staging2;


select distinct country, trim(TRAILING '.' FROM country)
from layoffs_staging2
order by 1;

update layoffs_staging2
set country = trim(TRAILING '.' FROM country)
where country like 'United States%';

select `date`
from layoffs_staging2;


update layoffs_staging2
set `date`= str_to_date(`date`,'%m/%d/%Y');

alter table layoffs_staging2
modify column `date` DATE;


select * 
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

update layoffs_staging2
set industry = null
where industry = '';


select *
from layoffs_staging2
where industry is null
or industry = '';

select t1.industry,t2.industry
from layoffs_staging2 t1
join layoffs_staging2 t2
    on t1.company = t2.company
    and t1.location = t2.location
where (t1.industry is null or t1.industry ='')
and t2.industry is not null; 

update layoffs_staging2 t1
join layoffs_staging2 t2
    on t1.company = t2.company
set   t1.industry = t2.industry  
where t1.industry is null 
and t2.industry is not null; 

select * 
from layoffs_staging2
where company = 'Airbnb';

select * from layoffs_staging2;

select * 
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;


Delete
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

Alter table layoffs_staging2
drop column row_num;

select * from layoffs_staging2;

select Max(total_laid_off), Max(percentage_laid_off)
from layoffs_staging2;

select * from layoffs_staging2
where percentage_laid_off = 1
order by total_laid_off desc;

select * from layoffs_staging2
where percentage_laid_off = 1
order by funds_raised_millions desc;

select company, sum(total_laid_off)
from layoffs_staging2
group by company
order by 2 desc;

select min(`date`),max(`date`)
from layoffs_staging2;

select industry, sum(total_laid_off)
from layoffs_staging2
group by industry
order by 2 desc;


select * 
from layoffs_staging2;

select country, sum(total_laid_off)
from layoffs_staging2
group by country
order by 2 desc;

select YEAR(`date`), sum(total_laid_off)
from layoffs_staging2
group by YEAR(`date`)
order by 1 desc;

select stage, sum(total_laid_off)
from layoffs_staging2
group by stage
order by 2 desc;

select company, sum(total_laid_off)
from layoffs_staging2
group by company
order by 2 desc;

select substring(`date`,1,7) as `MONTH`,sum(total_laid_off)
from layoffs_staging2
where substring(`date`,1,7) is not null
group by `month`
order by 1 ASC;


with rolling_total as 
(select substring(`date`,1,7) as `MONTH`,sum(total_laid_off) as total_off
from layoffs_staging2
where substring(`date`,1,7) is not null
group by `month`
order by 1 ASC
)
select `MONTH`,total_off
,sum(total_off) over(order by `MONTH`) as rolling_total
from rolling_total;


select company, sum(total_laid_off)
from layoffs_staging2
group by company
order by 2 desc;

select company,YEAR(`date`), sum(total_laid_off)
from layoffs_staging2
group by company,YEAR(`date`)
order by 3 desc;


With company_year (company,years,total_laid_off) as
(select company,YEAR(`date`), sum(total_laid_off)
from layoffs_staging2
group by company,YEAR(`date`)
),Company_year_rank as
(select *,dense_rank() over (partition by years order by total_laid_off desc) as Ranking
from company_year
where years is not null
)
select *
from Company_year_rank
where Ranking<=5;






