use recruitment;
create view v_operator_clean_null as
select *
from operator
where job_href is not null
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

select count(*)
from operator;

select count(*)
from v_operator_clean_null;

create view v_clean_operator_distinct as
with process as (select *,
                        row_number() over (partition by company_name,job_name order by
                            issuedate desc) as rank_n
                 from v_operator_clean_null)
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

select *
from v_clean_operator_distinct;
select count(*)
from v_clean_operator_distinct;

create view v_operator_clean_workplace as
with process as (select *,
                        (case
                             when workarea_text like '%北京%' then '北京'
                             when workarea_text like '%上海%' then '上海'
                             when workarea_text like '%广州%' then '广州'
                             when workarea_text like '%深圳%' then '深圳'
                            end) as workplace
                 from v_clean_operator_distinct)
select *
from process
where workplace is not null;

select count(*)
from v_operator_clean_workplace;

create view v_operator_clean_jobname as
select *
from v_operator_clean_workplace
where job_name like '%运维%';

select count(*)
from v_operator_clean_jobname;

create view v_operator_clean as
(
select *
from v_operator_clean_jobname);

create view v_operator_market_demand as
select workplace       as '城市',
       sum(degreefrom) as '招聘总量',
       count(*)        as '职位数目'
from v_operator_clean
group by workplace;

select *
from v_operator_market_demand;

select sum(degreefrom) as sum_degreefrom
from v_operator_clean;

select companytype_text,
       sum(degreefrom) as degreefrom
from v_operator_clean
group by companytype_text
order by degreefrom desc;

create view v_operator_companytype_degree as
(
select companytype_text                                                      as '企业类型',
       degreefrom                                                            as '招聘量',
       concat(cast(degreefrom / sum_degreefrom * 100 as decimal(4, 2)), '%') as '百分比'
from (select companytype_text,
             sum(degreefrom) as degreefrom
      from v_operator_clean
      group by companytype_text
      order by degreefrom desc) f1,
     (select sum(degreefrom) as sum_degreefrom from v_operator_clean) f2);

select *
from v_operator_companytype_degree;


create view v_operator_salary_unit as
select *,
       (case
            when providesalary_text like '%万/月' then 10000
            when providesalary_text like '%千/月' then 1000
            when providesalary_text like '%万/年' then 833
           end
           ) as unit
from v_operator_clean;

select *
from v_operator_salary_unit;

select cast(substring_index(
        substring_index(providesalary_text, '千/月', 1), '-', 1
    ) as decimal(6, 2)) * unit        as salary_min,
       cast(substring_index(
               substring_index(providesalary_text, '千/月', 1), '-', -1
           ) as decimal(6, 2)) * unit as salary_max
from v_operator_salary_unit
limit 1;

create view v_operator_salary_min_max_mean as
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
                 from v_operator_salary_unit)
select *,
       cast((salary_max + salary_min) / 2 as decimal(10, 2)) as salary_mean
from process;

create view v_operator_workyear_salary as
select workyear         as '工作年限',
       avg(salary_mean) as '平均薪资'
from v_operator_salary_min_max_mean
group by workyear
order by length(workyear), workyear;

select *
from v_operator_workyear_salary;

create view v_operator_companytype_salary as
select companytype_text as '企业类型',
       avg(salary_mean) as '平均薪资'
from v_operator_salary_min_max_mean
group by companytype_text
order by avg(salary_mean) desc;

select *
from v_operator_companytype_salary;

select *
from v_operator_clean;

create view v_operator_skill_quantity as
select skill,
       count(*) as quantity
from skill_table
         inner join v_operator_clean
                    on v_operator_clean.parse2_job_detail like concat('%', skill_table.skill, '%')
group by skill_table.skill
order by quantity desc
limit 30
;

select *
from v_operator_clean;

create view v_operator_skill as
select skill                                                               as '技能',
       quantity                                                            as '出现频数',
       concat(cast(quantity / total_quantity * 100 as decimal(4, 2)), '%') as '出现率'
from v_operator_skill_quantity as f1,
     (select count(*) as total_quantity from v_operator_clean) f2;
select *
from v_operator_skill;