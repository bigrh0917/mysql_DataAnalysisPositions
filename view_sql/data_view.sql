create or replace definer = root@localhost view v_clean_data_distinct as
with `process` as (select `recruitment`.`v_data_clean_null`.`id`                                                                                                                                                      AS `id`,
                          `recruitment`.`v_data_clean_null`.`job_href`                                                                                                                                                AS `job_href`,
                          `recruitment`.`v_data_clean_null`.`job_name`                                                                                                                                                AS `job_name`,
                          `recruitment`.`v_data_clean_null`.`company_href`                                                                                                                                            AS `company_href`,
                          `recruitment`.`v_data_clean_null`.`company_name`                                                                                                                                            AS `company_name`,
                          `recruitment`.`v_data_clean_null`.`providesalary_text`                                                                                                                                      AS `providesalary_text`,
                          `recruitment`.`v_data_clean_null`.`workarea`                                                                                                                                                AS `workarea`,
                          `recruitment`.`v_data_clean_null`.`workarea_text`                                                                                                                                           AS `workarea_text`,
                          `recruitment`.`v_data_clean_null`.`updatedate`                                                                                                                                              AS `updatedate`,
                          `recruitment`.`v_data_clean_null`.`companytype_text`                                                                                                                                        AS `companytype_text`,
                          `recruitment`.`v_data_clean_null`.`degreefrom`                                                                                                                                              AS `degreefrom`,
                          `recruitment`.`v_data_clean_null`.`workyear`                                                                                                                                                AS `workyear`,
                          `recruitment`.`v_data_clean_null`.`issuedate`                                                                                                                                               AS `issuedate`,
                          `recruitment`.`v_data_clean_null`.`parse2_job_detail`                                                                                                                                       AS `parse2_job_detail`,
                          row_number() OVER (PARTITION BY `recruitment`.`v_data_clean_null`.`company_name`,`recruitment`.`v_data_clean_null`.`job_name` ORDER BY `recruitment`.`v_data_clean_null`.`issuedate` desc ) AS `rank_n`
                   from `recruitment`.`v_data_clean_null`)
select `process`.`id`                 AS `id`,
       `process`.`job_href`           AS `job_href`,
       `process`.`job_name`           AS `job_name`,
       `process`.`company_href`       AS `company_href`,
       `process`.`company_name`       AS `company_name`,
       `process`.`providesalary_text` AS `providesalary_text`,
       `process`.`workarea`           AS `workarea`,
       `process`.`workarea_text`      AS `workarea_text`,
       `process`.`updatedate`         AS `updatedate`,
       `process`.`companytype_text`   AS `companytype_text`,
       `process`.`degreefrom`         AS `degreefrom`,
       `process`.`workyear`           AS `workyear`,
       `process`.`issuedate`          AS `issuedate`,
       `process`.`parse2_job_detail`  AS `parse2_job_detail`
from `process`
where (`process`.`rank_n` = 1);

create or replace definer = root@localhost view v_data_clean as
select `recruitment`.`v_data_clean_jobname`.`id`                 AS `id`,
       `recruitment`.`v_data_clean_jobname`.`job_href`           AS `job_href`,
       `recruitment`.`v_data_clean_jobname`.`job_name`           AS `job_name`,
       `recruitment`.`v_data_clean_jobname`.`company_href`       AS `company_href`,
       `recruitment`.`v_data_clean_jobname`.`company_name`       AS `company_name`,
       `recruitment`.`v_data_clean_jobname`.`providesalary_text` AS `providesalary_text`,
       `recruitment`.`v_data_clean_jobname`.`workarea`           AS `workarea`,
       `recruitment`.`v_data_clean_jobname`.`workarea_text`      AS `workarea_text`,
       `recruitment`.`v_data_clean_jobname`.`updatedate`         AS `updatedate`,
       `recruitment`.`v_data_clean_jobname`.`companytype_text`   AS `companytype_text`,
       `recruitment`.`v_data_clean_jobname`.`degreefrom`         AS `degreefrom`,
       `recruitment`.`v_data_clean_jobname`.`workyear`           AS `workyear`,
       `recruitment`.`v_data_clean_jobname`.`issuedate`          AS `issuedate`,
       `recruitment`.`v_data_clean_jobname`.`parse2_job_detail`  AS `parse2_job_detail`,
       `recruitment`.`v_data_clean_jobname`.`workplace`          AS `workplace`
from `recruitment`.`v_data_clean_jobname`;

create or replace definer = root@localhost view v_data_clean_jobname as
select `recruitment`.`v_data_clean_workplace`.`id`                 AS `id`,
       `recruitment`.`v_data_clean_workplace`.`job_href`           AS `job_href`,
       `recruitment`.`v_data_clean_workplace`.`job_name`           AS `job_name`,
       `recruitment`.`v_data_clean_workplace`.`company_href`       AS `company_href`,
       `recruitment`.`v_data_clean_workplace`.`company_name`       AS `company_name`,
       `recruitment`.`v_data_clean_workplace`.`providesalary_text` AS `providesalary_text`,
       `recruitment`.`v_data_clean_workplace`.`workarea`           AS `workarea`,
       `recruitment`.`v_data_clean_workplace`.`workarea_text`      AS `workarea_text`,
       `recruitment`.`v_data_clean_workplace`.`updatedate`         AS `updatedate`,
       `recruitment`.`v_data_clean_workplace`.`companytype_text`   AS `companytype_text`,
       `recruitment`.`v_data_clean_workplace`.`degreefrom`         AS `degreefrom`,
       `recruitment`.`v_data_clean_workplace`.`workyear`           AS `workyear`,
       `recruitment`.`v_data_clean_workplace`.`issuedate`          AS `issuedate`,
       `recruitment`.`v_data_clean_workplace`.`parse2_job_detail`  AS `parse2_job_detail`,
       `recruitment`.`v_data_clean_workplace`.`workplace`          AS `workplace`
from `recruitment`.`v_data_clean_workplace`
where (`recruitment`.`v_data_clean_workplace`.`job_name` like '%数据%');

create or replace definer = root@localhost view v_data_clean_null as
select `recruitment`.`data`.`id`                 AS `id`,
       `recruitment`.`data`.`job_href`           AS `job_href`,
       `recruitment`.`data`.`job_name`           AS `job_name`,
       `recruitment`.`data`.`company_href`       AS `company_href`,
       `recruitment`.`data`.`company_name`       AS `company_name`,
       `recruitment`.`data`.`providesalary_text` AS `providesalary_text`,
       `recruitment`.`data`.`workarea`           AS `workarea`,
       `recruitment`.`data`.`workarea_text`      AS `workarea_text`,
       `recruitment`.`data`.`updatedate`         AS `updatedate`,
       `recruitment`.`data`.`companytype_text`   AS `companytype_text`,
       `recruitment`.`data`.`degreefrom`         AS `degreefrom`,
       `recruitment`.`data`.`workyear`           AS `workyear`,
       `recruitment`.`data`.`issuedate`          AS `issuedate`,
       `recruitment`.`data`.`parse2_job_detail`  AS `parse2_job_detail`
from `recruitment`.`data`
where ((`recruitment`.`data`.`job_href` is not null) and (`recruitment`.`data`.`job_href` <> '') and
       (`recruitment`.`data`.`job_name` is not null) and (`recruitment`.`data`.`job_name` <> '') and
       (`recruitment`.`data`.`company_href` is not null) and (`recruitment`.`data`.`company_href` <> '') and
       (`recruitment`.`data`.`company_name` is not null) and (`recruitment`.`data`.`company_name` <> '') and
       (`recruitment`.`data`.`providesalary_text` is not null) and (`recruitment`.`data`.`providesalary_text` <> '') and
       (`recruitment`.`data`.`workarea` is not null) and (`recruitment`.`data`.`workarea` <> '') and
       (`recruitment`.`data`.`workarea_text` is not null) and (`recruitment`.`data`.`workarea_text` <> '') and
       (`recruitment`.`data`.`companytype_text` is not null) and (`recruitment`.`data`.`companytype_text` <> '') and
       (`recruitment`.`data`.`degreefrom` is not null) and (`recruitment`.`data`.`degreefrom` <> '') and
       (`recruitment`.`data`.`workyear` is not null) and (`recruitment`.`data`.`workyear` <> '') and
       (`recruitment`.`data`.`updatedate` is not null) and (`recruitment`.`data`.`updatedate` <> '') and
       (`recruitment`.`data`.`issuedate` is not null) and (`recruitment`.`data`.`issuedate` <> '') and
       (`recruitment`.`data`.`parse2_job_detail` is not null) and (`recruitment`.`data`.`parse2_job_detail` <> ''));

create or replace definer = root@localhost view v_data_clean_workplace as
with `process` as (select `recruitment`.`v_clean_data_distinct`.`id`                 AS `id`,
                          `recruitment`.`v_clean_data_distinct`.`job_href`           AS `job_href`,
                          `recruitment`.`v_clean_data_distinct`.`job_name`           AS `job_name`,
                          `recruitment`.`v_clean_data_distinct`.`company_href`       AS `company_href`,
                          `recruitment`.`v_clean_data_distinct`.`company_name`       AS `company_name`,
                          `recruitment`.`v_clean_data_distinct`.`providesalary_text` AS `providesalary_text`,
                          `recruitment`.`v_clean_data_distinct`.`workarea`           AS `workarea`,
                          `recruitment`.`v_clean_data_distinct`.`workarea_text`      AS `workarea_text`,
                          `recruitment`.`v_clean_data_distinct`.`updatedate`         AS `updatedate`,
                          `recruitment`.`v_clean_data_distinct`.`companytype_text`   AS `companytype_text`,
                          `recruitment`.`v_clean_data_distinct`.`degreefrom`         AS `degreefrom`,
                          `recruitment`.`v_clean_data_distinct`.`workyear`           AS `workyear`,
                          `recruitment`.`v_clean_data_distinct`.`issuedate`          AS `issuedate`,
                          `recruitment`.`v_clean_data_distinct`.`parse2_job_detail`  AS `parse2_job_detail`,
                          (case
                               when (`recruitment`.`v_clean_data_distinct`.`workarea_text` like '%北京%') then '北京'
                               when (`recruitment`.`v_clean_data_distinct`.`workarea_text` like '%上海%') then '上海'
                               when (`recruitment`.`v_clean_data_distinct`.`workarea_text` like '%广州%') then '广州'
                               when (`recruitment`.`v_clean_data_distinct`.`workarea_text` like '%深圳%')
                                   then '深圳' end)                                  AS `workplace`
                   from `recruitment`.`v_clean_data_distinct`)
select `process`.`id`                 AS `id`,
       `process`.`job_href`           AS `job_href`,
       `process`.`job_name`           AS `job_name`,
       `process`.`company_href`       AS `company_href`,
       `process`.`company_name`       AS `company_name`,
       `process`.`providesalary_text` AS `providesalary_text`,
       `process`.`workarea`           AS `workarea`,
       `process`.`workarea_text`      AS `workarea_text`,
       `process`.`updatedate`         AS `updatedate`,
       `process`.`companytype_text`   AS `companytype_text`,
       `process`.`degreefrom`         AS `degreefrom`,
       `process`.`workyear`           AS `workyear`,
       `process`.`issuedate`          AS `issuedate`,
       `process`.`parse2_job_detail`  AS `parse2_job_detail`,
       `process`.`workplace`          AS `workplace`
from `process`
where (`process`.`workplace` is not null);

create or replace definer = root@localhost view v_data_companytype_degree as
select `f1`.`companytype_text`                                                                 AS `企业类型`,
       `f1`.`degreefrom`                                                                       AS `招聘量`,
       concat(cast(((`f1`.`degreefrom` / `f2`.`sum_degreefrom`) * 100) as decimal(4, 2)), '%') AS `百分比`
from (select `recruitment`.`v_data_clean`.`companytype_text` AS `companytype_text`,
             sum(`recruitment`.`v_data_clean`.`degreefrom`)  AS `degreefrom`
      from `recruitment`.`v_data_clean`
      group by `recruitment`.`v_data_clean`.`companytype_text`
      order by `degreefrom` desc) `f1`
         join (select sum(`recruitment`.`v_data_clean`.`degreefrom`) AS `sum_degreefrom`
               from `recruitment`.`v_data_clean`) `f2`;

create or replace definer = root@localhost view v_data_companytype_salary as
select `recruitment`.`v_data_salary_min_max_mean`.`companytype_text` AS `企业类型`,
       avg(`recruitment`.`v_data_salary_min_max_mean`.`salary_mean`) AS `平均薪资`
from `recruitment`.`v_data_salary_min_max_mean`
group by `recruitment`.`v_data_salary_min_max_mean`.`companytype_text`
order by avg(`recruitment`.`v_data_salary_min_max_mean`.`salary_mean`) desc;

create or replace definer = root@localhost view v_data_market_demand as
select `recruitment`.`v_data_clean`.`workplace`       AS `城市`,
       sum(`recruitment`.`v_data_clean`.`degreefrom`) AS `招聘总量`,
       count(0)                                       AS `职位数目`
from `recruitment`.`v_data_clean`
group by `recruitment`.`v_data_clean`.`workplace`;

create or replace definer = root@localhost view v_data_salary_min_max_mean as
with `process` as (select `recruitment`.`v_data_salary_unit`.`id`                                                                           AS `id`,
                          `recruitment`.`v_data_salary_unit`.`job_href`                                                                     AS `job_href`,
                          `recruitment`.`v_data_salary_unit`.`job_name`                                                                     AS `job_name`,
                          `recruitment`.`v_data_salary_unit`.`company_href`                                                                 AS `company_href`,
                          `recruitment`.`v_data_salary_unit`.`company_name`                                                                 AS `company_name`,
                          `recruitment`.`v_data_salary_unit`.`providesalary_text`                                                           AS `providesalary_text`,
                          `recruitment`.`v_data_salary_unit`.`workarea`                                                                     AS `workarea`,
                          `recruitment`.`v_data_salary_unit`.`workarea_text`                                                                AS `workarea_text`,
                          `recruitment`.`v_data_salary_unit`.`updatedate`                                                                   AS `updatedate`,
                          `recruitment`.`v_data_salary_unit`.`companytype_text`                                                             AS `companytype_text`,
                          `recruitment`.`v_data_salary_unit`.`degreefrom`                                                                   AS `degreefrom`,
                          `recruitment`.`v_data_salary_unit`.`workyear`                                                                     AS `workyear`,
                          `recruitment`.`v_data_salary_unit`.`issuedate`                                                                    AS `issuedate`,
                          `recruitment`.`v_data_salary_unit`.`parse2_job_detail`                                                            AS `parse2_job_detail`,
                          `recruitment`.`v_data_salary_unit`.`workplace`                                                                    AS `workplace`,
                          `recruitment`.`v_data_salary_unit`.`unit`                                                                         AS `unit`,
                          (case
                               when (`recruitment`.`v_data_salary_unit`.`unit` = 1000) then (cast(substring_index(
                                       substring_index(`recruitment`.`v_data_salary_unit`.`providesalary_text`, '千/月',
                                                       1), '-', 1) as decimal(6, 2)) *
                                                                                             `recruitment`.`v_data_salary_unit`.`unit`)
                               when (`recruitment`.`v_data_salary_unit`.`unit` = 10000) then (cast(substring_index(
                                       substring_index(`recruitment`.`v_data_salary_unit`.`providesalary_text`, '万/月',
                                                       1), '-', 1) as decimal(6, 2)) *
                                                                                              `recruitment`.`v_data_salary_unit`.`unit`)
                               when (`recruitment`.`v_data_salary_unit`.`unit` = 833) then (cast(substring_index(
                                       substring_index(`recruitment`.`v_data_salary_unit`.`providesalary_text`, '万/年',
                                                       1), '-', 1) as decimal(6, 2)) *
                                                                                            `recruitment`.`v_data_salary_unit`.`unit`) end) AS `salary_min`,
                          (case
                               when (`recruitment`.`v_data_salary_unit`.`unit` = 1000) then (cast(substring_index(
                                       substring_index(`recruitment`.`v_data_salary_unit`.`providesalary_text`, '千/月',
                                                       1), '-', -(1)) as decimal(6, 2)) *
                                                                                             `recruitment`.`v_data_salary_unit`.`unit`)
                               when (`recruitment`.`v_data_salary_unit`.`unit` = 10000) then (cast(substring_index(
                                       substring_index(`recruitment`.`v_data_salary_unit`.`providesalary_text`, '万/月',
                                                       1), '-', -(1)) as decimal(6, 2)) *
                                                                                              `recruitment`.`v_data_salary_unit`.`unit`)
                               when (`recruitment`.`v_data_salary_unit`.`unit` = 833) then (cast(substring_index(
                                       substring_index(`recruitment`.`v_data_salary_unit`.`providesalary_text`, '万/年',
                                                       1), '-', -(1)) as decimal(6, 2)) *
                                                                                            `recruitment`.`v_data_salary_unit`.`unit`) end) AS `salary_max`
                   from `recruitment`.`v_data_salary_unit`)
select `process`.`id`                                                                  AS `id`,
       `process`.`job_href`                                                            AS `job_href`,
       `process`.`job_name`                                                            AS `job_name`,
       `process`.`company_href`                                                        AS `company_href`,
       `process`.`company_name`                                                        AS `company_name`,
       `process`.`providesalary_text`                                                  AS `providesalary_text`,
       `process`.`workarea`                                                            AS `workarea`,
       `process`.`workarea_text`                                                       AS `workarea_text`,
       `process`.`updatedate`                                                          AS `updatedate`,
       `process`.`companytype_text`                                                    AS `companytype_text`,
       `process`.`degreefrom`                                                          AS `degreefrom`,
       `process`.`workyear`                                                            AS `workyear`,
       `process`.`issuedate`                                                           AS `issuedate`,
       `process`.`parse2_job_detail`                                                   AS `parse2_job_detail`,
       `process`.`workplace`                                                           AS `workplace`,
       `process`.`unit`                                                                AS `unit`,
       `process`.`salary_min`                                                          AS `salary_min`,
       `process`.`salary_max`                                                          AS `salary_max`,
       cast(((`process`.`salary_max` + `process`.`salary_min`) / 2) as decimal(10, 2)) AS `salary_mean`
from `process`;

create or replace definer = root@localhost view v_data_salary_unit as
select `recruitment`.`v_data_clean`.`id`                                                         AS `id`,
       `recruitment`.`v_data_clean`.`job_href`                                                   AS `job_href`,
       `recruitment`.`v_data_clean`.`job_name`                                                   AS `job_name`,
       `recruitment`.`v_data_clean`.`company_href`                                               AS `company_href`,
       `recruitment`.`v_data_clean`.`company_name`                                               AS `company_name`,
       `recruitment`.`v_data_clean`.`providesalary_text`                                         AS `providesalary_text`,
       `recruitment`.`v_data_clean`.`workarea`                                                   AS `workarea`,
       `recruitment`.`v_data_clean`.`workarea_text`                                              AS `workarea_text`,
       `recruitment`.`v_data_clean`.`updatedate`                                                 AS `updatedate`,
       `recruitment`.`v_data_clean`.`companytype_text`                                           AS `companytype_text`,
       `recruitment`.`v_data_clean`.`degreefrom`                                                 AS `degreefrom`,
       `recruitment`.`v_data_clean`.`workyear`                                                   AS `workyear`,
       `recruitment`.`v_data_clean`.`issuedate`                                                  AS `issuedate`,
       `recruitment`.`v_data_clean`.`parse2_job_detail`                                          AS `parse2_job_detail`,
       `recruitment`.`v_data_clean`.`workplace`                                                  AS `workplace`,
       (case
            when (`recruitment`.`v_data_clean`.`providesalary_text` like '%万/月') then 10000
            when (`recruitment`.`v_data_clean`.`providesalary_text` like '%千/月') then 1000
            when (`recruitment`.`v_data_clean`.`providesalary_text` like '%万/年') then 833 end) AS `unit`
from `recruitment`.`v_data_clean`;

create or replace definer = root@localhost view v_data_skill as
select `recruitment`.`f1`.`skill`                                                                          AS `技能`,
       `recruitment`.`f1`.`quantity`                                                                       AS `出现频数`,
       concat(cast(((`recruitment`.`f1`.`quantity` / `f2`.`total_quantity`) * 100) as decimal(4, 2)), '%') AS `出现率`
from `recruitment`.`v_data_skill_quantity` `f1`
         join (select count(0) AS `total_quantity` from `recruitment`.`v_data_clean`) `f2`;

create or replace definer = root@localhost view v_data_skill_quantity as
select `recruitment`.`skill_table`.`skill` AS `skill`, count(0) AS `quantity`
from (`recruitment`.`skill_table` join `recruitment`.`v_data_clean`
      on ((`recruitment`.`v_data_clean`.`parse2_job_detail` like
           concat('%', `recruitment`.`skill_table`.`skill`, '%'))))
group by `recruitment`.`skill_table`.`skill`
order by `quantity` desc
limit 30;

create or replace definer = root@localhost view v_data_workyear_salary as
select `recruitment`.`v_data_salary_min_max_mean`.`workyear`         AS `工作年限`,
       avg(`recruitment`.`v_data_salary_min_max_mean`.`salary_mean`) AS `平均薪资`
from `recruitment`.`v_data_salary_min_max_mean`
group by `recruitment`.`v_data_salary_min_max_mean`.`workyear`
order by length(`recruitment`.`v_data_salary_min_max_mean`.`workyear`),
         `recruitment`.`v_data_salary_min_max_mean`.`workyear`;


