#create a database named "recruitment"
create database if not exists recruitment charset utf8;

#使用本次分析的数据库
use recruitment;

#数据清洗，存在非空项都不要。创建视图
create view v_data_clean_null as
select *
from data
where
    /* 字段不为空也不为null */
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

#原始有多少列
select count(*)
from data;

#看清除了多少个列
select count(*)
from v_data_clean_null;

#使用合适的列进行去重，公司和职位重复就认为是相同的。
#遇到公司名称和职位相同的，按照时间最新的数据保存，及处理最新的数据
#因为要使用窗口函数，所以需要使用MySQL8以上的版本
#row1的作用：避免公司名称和职位完全一致的数据无法删除
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

select *
from v_clean_data_distinct;
#80859
select count(*)
from v_clean_data_distinct;

#限定区域
#首先改变上海-浦东 为上海,数据的规范化
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

#78719
select count(*)
from v_data_clean_workplace;

#过滤周边的岗位，比如公关、整合销售经理
#对爬虫得到的数据二次检索，对得到的数据再筛选一次
#职位信息必须包含关键词，不同的数据有不同的策略，job_name必须包含数据,得尝试，比如分析，比如数据
create view v_temp1 as
select *
from v_data_clean_workplace
where job_name like '%数据%'
   or job_name like '%分析%';


create view v_temp2 as
select *
from v_data_clean_workplace
where job_name like '%数据%';


#对比v1数据或分析和v2单纯用数据
select *
from v_temp1; #6945
select *
from v_temp2;
#4520

#看得到的结果是否满足要求。
select *
from v_temp1
where v_temp1.id not in (select id from v_temp2);


#处理职位名称
create view v_data_clean_jobname as
select *
from v_data_clean_workplace
where job_name like '%数据%';


#5420
select count(*)
from v_data_clean_jobname;

#对清理结果命名

create view v_data_clean as
(
select *
from v_data_clean_jobname);



create view v_data_market_demand as
select workplace       as '城市',
       sum(degreefrom) as '招聘总量',
       count(*)        as '职位数目'
from v_data_clean
group by workplace;

#按照城市得到招聘数量和职位数目，市场需求量
select *
from v_data_market_demand;

#总招聘人数,30929
select sum(degreefrom) as sum_degreefrom
from v_data_clean;

#得到不同公司类型需要的总人数
select companytype_text,
       sum(degreefrom) as degreefrom
from v_data_clean
group by companytype_text
order by degreefrom desc;


#就业企业类型分布
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

#规范薪资，单位
#千/月->1000
#万/月->10000
#万/年->833
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

#提取薪资,test
select cast(substring_index(
        substring_index(providesalary_text, '千/月', 1), '-', 1
    ) as decimal(6, 2)) * unit        as salary_min,
       cast(substring_index(
               substring_index(providesalary_text, '千/月', 1), '-', -1
           ) as decimal(6, 2)) * unit as salary_max
from v_data_salary_unit
limit 1;



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


#按照工作年限分组，求各组平均薪资
create view v_data_workyear_salary as
select workyear         as '工作年限',
       avg(salary_mean) as '平均薪资'
from v_data_salary_min_max_mean
group by workyear
order by length(workyear), workyear;

#查询结果
select *
from v_data_workyear_salary;

#按企业类型分组，计算平均薪资
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


#得到招聘对应的技能需求
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

#5428,总招聘数目
select *
from v_data_clean;

#技能出现的频数
create view v_data_skill as
select skill                                                               as '技能',
       quantity                                                            as '出现频数',
       concat(cast(quantity / total_quantity * 100 as decimal(4, 2)), '%') as '出现率'
from v_data_skill_quantity as f1,
     (select count(*) as total_quantity from v_data_clean) f2;
select *
from v_data_skill;



