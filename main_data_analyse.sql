#Create a database named "recruitment"
create database if not exists recruitment charset utf8;


#Use of the database for this analysis
use recruitment;


#Data clean to remove the presence of non-null items. Creating a view
create view v_data_clean_null as
select *
from data
where
    /* Fields are not empty nor null */
    job_href is not null
  and job_href != ''
  and job_name is not null
  and job_name != ''
  and company_href is not null
  and company_href != ''
  and company_name is not null
  and company_name != ''
  and providesalary_text is not null
  and providesalary_text != ''
  and workarea is not null
  and workarea != ''
  and workarea_text is not null
  and workarea_text != ''
  and companytype_text is not null
  and companytype_text != ''
  and degreefrom is not null
  and degreefrom != ''
  and workyear is not null
  and workyear != ''
  and updatedate is not null
  and updatedate != ''
  and issuedate is not null
  and issuedate != ''
  and parse2_job_detail is not null
  and parse2_job_detail != '';

#How many columns are there in the original
select count(*)
from data;

#How many columns were cleared
select count(*)
from v_data_clean_null;


#Use the appropriate columns for de-duplication, and consider the data to be the same if the company and position are duplicated.
#If the company name and position are the same, save the latest data according to the time and process the latest data.
#Because of the use of window functions, MySQL 8 or above is needed.
#The role of row1: to avoid data that cannot be deleted if the company name and the position are identical.
create view v_clean_data_distinct as
with process as (select *,
                        row_number() over (partition by company_name,job_name order by
                            issuedate desc) as rank_n
                 from v_data_clean_null)
select id,
       job_href,
       job_name,
       company_href,
       company_name,
       providesalary_text,
       workarea,
       workarea_text,
       updatedate,
       companytype_text,
       degreefrom,
       workyear,
       issuedate,
       parse2_job_detail
from process
where rank_n = 1;

#View post-cleaning data.
select *
from v_clean_data_distinct;

#View the number of data after cleaning,80859.
select count(*)
from v_clean_data_distinct;


#Limit the area to be analysed.
#First change Shanghai-Pudong to Shanghai, normalisation of data.
create view v_data_clean_workplace as
with process as (select *,
                        (case
                             when workarea_text like '%北京%' then '北京'
                             when workarea_text like '%上海%' then '上海'
                             when workarea_text like '%广州%' then '广州'
                             when workarea_text like '%深圳%' then '深圳'
                            end) as workplace
                 from v_clean_data_distinct)
select *
from process
where workplace is not null;

#View the amount of data after qualifying the area, 78719.
select count(*)
from v_data_clean_workplace;


#Filtering peripheral positions such as PR, integrated sales manager.
#The data obtained by the crawler is searched twice and the obtained data is filtered once more.
#Job information must contain keywords, different data have different strategies, job_name must contain 'data'.
#Have to try, such as 'analysis', such as 'data'.
create view v_temp1 as
select *
from v_data_clean_workplace
where job_name like '%数据%'
   or job_name like '%分析%';

create view v_temp2 as
select *
from v_data_clean_workplace
where job_name like '%数据%';

#Compare and contrast v1 data or analysis and v2 using data alone.
select *
from v_temp1; #6945
select *
from v_temp2;#4520

#See if the results obtained fulfil the requirements.
select *
from v_temp1
where v_temp1.id not in (select id from v_temp2);

#Processing job name using the limitation 'data'.
create view v_data_clean_jobname as
select *
from v_data_clean_workplace
where job_name like '%数据%';

#Query the amount of data left after this processing, 5420.
select count(*)
from v_data_clean_jobname;


#Naming the results of data cleansing.
create view v_data_clean as
(
select *
from v_data_clean_jobname);



#Market demand according to the number of recruitments and jobs received by city.
create view v_data_market_demand as
select workplace       as '城市',
       sum(degreefrom) as '招聘总量',
       count(*)        as '职位数目'
from v_data_clean
group by workplace;

#Enquiry results.
select *
from v_data_market_demand;

#Total number of recruits, 30929.
select sum(degreefrom) as sum_degreefrom
from v_data_clean;


#Getting the total number of people needed for different company types.
select companytype_text,
       sum(degreefrom) as degreefrom
from v_data_clean
group by companytype_text
order by degreefrom desc;


#Distribution of types of employment enterprises.
create view v_data_companytype_degree as
(
select companytype_text                                                      as '企业类型',
       degreefrom                                                            as '招聘量',
       concat(cast(degreefrom / sum_degreefrom * 100 as decimal(4, 2)), '%') as '百分比'
from (select companytype_text,
             sum(degreefrom) as degreefrom
      from v_data_clean
      group by companytype_text
      order by degreefrom desc) f1,
     (select sum(degreefrom) as sum_degreefrom from v_data_clean) f2);

#查询结果
select *
from v_data_companytype_degree;


#Regulated salary, unit.
#thousand/month -> 1000.
#million/month -> 10,000.
#million/year -> 833.
create view v_data_salary_unit as
select *,
       (case
            when providesalary_text like '%万/月' then 10000
            when providesalary_text like '%千/月' then 1000
            when providesalary_text like '%万/年' then 833
           end
           ) as unit
from v_data_clean;

select *
from v_data_salary_unit;


#Extracting Salary from the String, a test.
select cast(substring_index(
        substring_index(providesalary_text, '千/月', 1), '-', 1
    ) as decimal(6, 2)) * unit        as salary_min,
       cast(substring_index(
               substring_index(providesalary_text, '千/月', 1), '-', -1
           ) as decimal(6, 2)) * unit as salary_max
from v_data_salary_unit
limit 1;

#Extracting Salary from the String.
create view v_data_salary_min_max_mean as
with process as (select *,
                        (case
                             when unit = 1000 then
                                     cast(substring_index(
                                             substring_index(providesalary_text, '千/月', 1), '-', 1
                                         ) as decimal(6, 2)) * unit
                             when unit = 10000 then
                                     cast(substring_index(
                                             substring_index(providesalary_text, '万/月', 1), '-', 1
                                         ) as decimal(6, 2)) * unit
                             when unit = 833 then
                                     cast(substring_index(
                                             substring_index(providesalary_text, '万/年', 1), '-', 1
                                         ) as decimal(6, 2)) * unit
                            end
                            ) as salary_min,
                        (case
                             when unit = 1000 then
                                     cast(substring_index(
                                             substring_index(providesalary_text, '千/月', 1), '-', -1
                                         ) as decimal(6, 2)) * unit
                             when unit = 10000 then
                                     cast(substring_index(
                                             substring_index(providesalary_text, '万/月', 1), '-', -1
                                         ) as decimal(6, 2)) * unit
                             when unit = 833 then
                                     cast(substring_index(
                                             substring_index(providesalary_text, '万/年', 1), '-', -1
                                         ) as decimal(6, 2)) * unit
                            end
                            ) as salary_max
                 from v_data_salary_unit)
select *,
       cast((salary_max + salary_min) / 2 as decimal(10, 2)) as salary_mean
from process;


#Wage classification.
with process as (select *,
                        (case
                             when salary_mean >= 50000 then 1
                             when salary_mean < 50000 and salary_mean >= 40000 then 2
                             when salary_mean < 40000 and salary_mean >= 30000 then 3
                             when salary_mean < 30000 and salary_mean >= 20000 then 4
                             when salary_mean < 20000 and salary_mean >= 10000 then 5
                             when salary_mean < 10000 and salary_mean >= 5000 then 6
                             when salary_mean < 5000 and salary_mean >= 3000 then 7
                             when salary_mean < 3000 then 8
                            end
                            ) as label
                 from v_data_salary_min_max_mean)
select count(*), label
from process
group by label;


#Find the average salary of each group, grouped by years of service.
create view v_data_workyear_salary as
select workyear         as '工作年限',
       avg(salary_mean) as '平均薪资'
from v_data_salary_min_max_mean
group by workyear
order by length(workyear), workyear;

#Enquiry results.
select *
from v_data_workyear_salary;


#Grouped by business type to calculate average salary.
create view v_data_companytype_salary as
select companytype_text as '企业类型',
       avg(salary_mean) as '平均薪资'
from v_data_salary_min_max_mean
group by companytype_text
order by avg(salary_mean) desc;

select *
from v_data_companytype_salary;

select *
from v_data_clean;


#Getting the skills needed for hiring.
create view v_data_skill_quantity as
select skill,
       count(*) as quantity
from skill_table
         inner join v_data_clean
                    on v_data_clean.parse2_job_detail like concat('%', skill_table.skill, '%')
group by skill_table.skill
order by quantity desc
limit 30
;


#Total number of recruitments, 5428.
select *
from v_data_clean;

#Frequency of occurrence of skills.
create view v_data_skill as
select skill                                                               as '技能',
       quantity                                                            as '出现频数',
       concat(cast(quantity / total_quantity * 100 as decimal(4, 2)), '%') as '出现率'
from v_data_skill_quantity as f1,
     (select count(*) as total_quantity from v_data_clean) f2;
select *
from v_data_skill;