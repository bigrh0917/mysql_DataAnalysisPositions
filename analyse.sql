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

select count(*) from v_clean_data_distinct;



