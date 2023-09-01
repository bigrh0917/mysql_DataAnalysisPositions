create or replace definer = root@localhost view v_clean_game_distinct as
with `process` as (select `recruitment`.`v_game_clean_null`.`id`                                                                                                                                                      AS `id`,
                          `recruitment`.`v_game_clean_null`.`job_href`                                                                                                                                                AS `job_href`,
                          `recruitment`.`v_game_clean_null`.`job_name`                                                                                                                                                AS `job_name`,
                          `recruitment`.`v_game_clean_null`.`company_href`                                                                                                                                            AS `company_href`,
                          `recruitment`.`v_game_clean_null`.`company_name`                                                                                                                                            AS `company_name`,
                          `recruitment`.`v_game_clean_null`.`providesalary_text`                                                                                                                                      AS `providesalary_text`,
                          `recruitment`.`v_game_clean_null`.`workarea`                                                                                                                                                AS `workarea`,
                          `recruitment`.`v_game_clean_null`.`workarea_text`                                                                                                                                           AS `workarea_text`,
                          `recruitment`.`v_game_clean_null`.`updatedate`                                                                                                                                              AS `updatedate`,
                          `recruitment`.`v_game_clean_null`.`companytype_text`                                                                                                                                        AS `companytype_text`,
                          `recruitment`.`v_game_clean_null`.`degreefrom`                                                                                                                                              AS `degreefrom`,
                          `recruitment`.`v_game_clean_null`.`workyear`                                                                                                                                                AS `workyear`,
                          `recruitment`.`v_game_clean_null`.`issuedate`                                                                                                                                               AS `issuedate`,
                          `recruitment`.`v_game_clean_null`.`parse2_job_detail`                                                                                                                                       AS `parse2_job_detail`,
                          row_number() OVER (PARTITION BY `recruitment`.`v_game_clean_null`.`company_name`,`recruitment`.`v_game_clean_null`.`job_name` ORDER BY `recruitment`.`v_game_clean_null`.`issuedate` desc ) AS `rank_n`
                   from `recruitment`.`v_game_clean_null`)
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

create or replace definer = root@localhost view v_game_clean as
select `recruitment`.`v_game_clean_jobname`.`id`                 AS `id`,
       `recruitment`.`v_game_clean_jobname`.`job_href`           AS `job_href`,
       `recruitment`.`v_game_clean_jobname`.`job_name`           AS `job_name`,
       `recruitment`.`v_game_clean_jobname`.`company_href`       AS `company_href`,
       `recruitment`.`v_game_clean_jobname`.`company_name`       AS `company_name`,
       `recruitment`.`v_game_clean_jobname`.`providesalary_text` AS `providesalary_text`,
       `recruitment`.`v_game_clean_jobname`.`workarea`           AS `workarea`,
       `recruitment`.`v_game_clean_jobname`.`workarea_text`      AS `workarea_text`,
       `recruitment`.`v_game_clean_jobname`.`updatedate`         AS `updatedate`,
       `recruitment`.`v_game_clean_jobname`.`companytype_text`   AS `companytype_text`,
       `recruitment`.`v_game_clean_jobname`.`degreefrom`         AS `degreefrom`,
       `recruitment`.`v_game_clean_jobname`.`workyear`           AS `workyear`,
       `recruitment`.`v_game_clean_jobname`.`issuedate`          AS `issuedate`,
       `recruitment`.`v_game_clean_jobname`.`parse2_job_detail`  AS `parse2_job_detail`,
       `recruitment`.`v_game_clean_jobname`.`workplace`          AS `workplace`
from `recruitment`.`v_game_clean_jobname`;

create or replace definer = root@localhost view v_game_clean_jobname as
select `recruitment`.`v_game_clean_workplace`.`id`                 AS `id`,
       `recruitment`.`v_game_clean_workplace`.`job_href`           AS `job_href`,
       `recruitment`.`v_game_clean_workplace`.`job_name`           AS `job_name`,
       `recruitment`.`v_game_clean_workplace`.`company_href`       AS `company_href`,
       `recruitment`.`v_game_clean_workplace`.`company_name`       AS `company_name`,
       `recruitment`.`v_game_clean_workplace`.`providesalary_text` AS `providesalary_text`,
       `recruitment`.`v_game_clean_workplace`.`workarea`           AS `workarea`,
       `recruitment`.`v_game_clean_workplace`.`workarea_text`      AS `workarea_text`,
       `recruitment`.`v_game_clean_workplace`.`updatedate`         AS `updatedate`,
       `recruitment`.`v_game_clean_workplace`.`companytype_text`   AS `companytype_text`,
       `recruitment`.`v_game_clean_workplace`.`degreefrom`         AS `degreefrom`,
       `recruitment`.`v_game_clean_workplace`.`workyear`           AS `workyear`,
       `recruitment`.`v_game_clean_workplace`.`issuedate`          AS `issuedate`,
       `recruitment`.`v_game_clean_workplace`.`parse2_job_detail`  AS `parse2_job_detail`,
       `recruitment`.`v_game_clean_workplace`.`workplace`          AS `workplace`
from `recruitment`.`v_game_clean_workplace`
where (`recruitment`.`v_game_clean_workplace`.`job_name` like '%游戏%');

create or replace definer = root@localhost view v_game_clean_null as
select `recruitment`.`game`.`id`                 AS `id`,
       `recruitment`.`game`.`job_href`           AS `job_href`,
       `recruitment`.`game`.`job_name`           AS `job_name`,
       `recruitment`.`game`.`company_href`       AS `company_href`,
       `recruitment`.`game`.`company_name`       AS `company_name`,
       `recruitment`.`game`.`providesalary_text` AS `providesalary_text`,
       `recruitment`.`game`.`workarea`           AS `workarea`,
       `recruitment`.`game`.`workarea_text`      AS `workarea_text`,
       `recruitment`.`game`.`updatedate`         AS `updatedate`,
       `recruitment`.`game`.`companytype_text`   AS `companytype_text`,
       `recruitment`.`game`.`degreefrom`         AS `degreefrom`,
       `recruitment`.`game`.`workyear`           AS `workyear`,
       `recruitment`.`game`.`issuedate`          AS `issuedate`,
       `recruitment`.`game`.`parse2_job_detail`  AS `parse2_job_detail`
from `recruitment`.`game`
where ((`recruitment`.`game`.`job_href` is not null) and (`recruitment`.`game`.`job_href` <> '') and
       (`recruitment`.`game`.`job_name` is not null) and (`recruitment`.`game`.`job_name` <> '') and
       (`recruitment`.`game`.`company_href` is not null) and (`recruitment`.`game`.`company_href` <> '') and
       (`recruitment`.`game`.`company_name` is not null) and (`recruitment`.`game`.`company_name` <> '') and
       (`recruitment`.`game`.`providesalary_text` is not null) and (`recruitment`.`game`.`providesalary_text` <> '') and
       (`recruitment`.`game`.`workarea` is not null) and (`recruitment`.`game`.`workarea` <> '') and
       (`recruitment`.`game`.`workarea_text` is not null) and (`recruitment`.`game`.`workarea_text` <> '') and
       (`recruitment`.`game`.`companytype_text` is not null) and (`recruitment`.`game`.`companytype_text` <> '') and
       (`recruitment`.`game`.`degreefrom` is not null) and (`recruitment`.`game`.`degreefrom` <> '') and
       (`recruitment`.`game`.`workyear` is not null) and (`recruitment`.`game`.`workyear` <> '') and
       (`recruitment`.`game`.`updatedate` is not null) and (`recruitment`.`game`.`updatedate` <> '') and
       (`recruitment`.`game`.`issuedate` is not null) and (`recruitment`.`game`.`issuedate` <> '') and
       (`recruitment`.`game`.`parse2_job_detail` is not null) and (`recruitment`.`game`.`parse2_job_detail` <> ''));

create or replace definer = root@localhost view v_game_clean_workplace as
with `process` as (select `recruitment`.`v_clean_game_distinct`.`id`                 AS `id`,
                          `recruitment`.`v_clean_game_distinct`.`job_href`           AS `job_href`,
                          `recruitment`.`v_clean_game_distinct`.`job_name`           AS `job_name`,
                          `recruitment`.`v_clean_game_distinct`.`company_href`       AS `company_href`,
                          `recruitment`.`v_clean_game_distinct`.`company_name`       AS `company_name`,
                          `recruitment`.`v_clean_game_distinct`.`providesalary_text` AS `providesalary_text`,
                          `recruitment`.`v_clean_game_distinct`.`workarea`           AS `workarea`,
                          `recruitment`.`v_clean_game_distinct`.`workarea_text`      AS `workarea_text`,
                          `recruitment`.`v_clean_game_distinct`.`updatedate`         AS `updatedate`,
                          `recruitment`.`v_clean_game_distinct`.`companytype_text`   AS `companytype_text`,
                          `recruitment`.`v_clean_game_distinct`.`degreefrom`         AS `degreefrom`,
                          `recruitment`.`v_clean_game_distinct`.`workyear`           AS `workyear`,
                          `recruitment`.`v_clean_game_distinct`.`issuedate`          AS `issuedate`,
                          `recruitment`.`v_clean_game_distinct`.`parse2_job_detail`  AS `parse2_job_detail`,
                          (case
                               when (`recruitment`.`v_clean_game_distinct`.`workarea_text` like '%北京%') then '北京'
                               when (`recruitment`.`v_clean_game_distinct`.`workarea_text` like '%上海%') then '上海'
                               when (`recruitment`.`v_clean_game_distinct`.`workarea_text` like '%广州%') then '广州'
                               when (`recruitment`.`v_clean_game_distinct`.`workarea_text` like '%深圳%')
                                   then '深圳' end)                                  AS `workplace`
                   from `recruitment`.`v_clean_game_distinct`)
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

create or replace definer = root@localhost view v_game_companytype_degree as
select `f1`.`companytype_text`                                                                 AS `企业类型`,
       `f1`.`degreefrom`                                                                       AS `招聘量`,
       concat(cast(((`f1`.`degreefrom` / `f2`.`sum_degreefrom`) * 100) as decimal(4, 2)), '%') AS `百分比`
from (select `recruitment`.`v_game_clean`.`companytype_text` AS `companytype_text`,
             sum(`recruitment`.`v_game_clean`.`degreefrom`)  AS `degreefrom`
      from `recruitment`.`v_game_clean`
      group by `recruitment`.`v_game_clean`.`companytype_text`
      order by `degreefrom` desc) `f1`
         join (select sum(`recruitment`.`v_game_clean`.`degreefrom`) AS `sum_degreefrom`
               from `recruitment`.`v_game_clean`) `f2`;

create or replace definer = root@localhost view v_game_companytype_salary as
select `recruitment`.`v_game_salary_min_max_mean`.`companytype_text` AS `企业类型`,
       avg(`recruitment`.`v_game_salary_min_max_mean`.`salary_mean`) AS `平均薪资`
from `recruitment`.`v_game_salary_min_max_mean`
group by `recruitment`.`v_game_salary_min_max_mean`.`companytype_text`
order by avg(`recruitment`.`v_game_salary_min_max_mean`.`salary_mean`) desc;

create or replace definer = root@localhost view v_game_market_demand as
select `recruitment`.`v_game_clean`.`workplace`       AS `城市`,
       sum(`recruitment`.`v_game_clean`.`degreefrom`) AS `招聘总量`,
       count(0)                                       AS `职位数目`
from `recruitment`.`v_game_clean`
group by `recruitment`.`v_game_clean`.`workplace`;

create or replace definer = root@localhost view v_game_salary_min_max_mean as
with `process` as (select `recruitment`.`v_game_salary_unit`.`id`                                                                           AS `id`,
                          `recruitment`.`v_game_salary_unit`.`job_href`                                                                     AS `job_href`,
                          `recruitment`.`v_game_salary_unit`.`job_name`                                                                     AS `job_name`,
                          `recruitment`.`v_game_salary_unit`.`company_href`                                                                 AS `company_href`,
                          `recruitment`.`v_game_salary_unit`.`company_name`                                                                 AS `company_name`,
                          `recruitment`.`v_game_salary_unit`.`providesalary_text`                                                           AS `providesalary_text`,
                          `recruitment`.`v_game_salary_unit`.`workarea`                                                                     AS `workarea`,
                          `recruitment`.`v_game_salary_unit`.`workarea_text`                                                                AS `workarea_text`,
                          `recruitment`.`v_game_salary_unit`.`updatedate`                                                                   AS `updatedate`,
                          `recruitment`.`v_game_salary_unit`.`companytype_text`                                                             AS `companytype_text`,
                          `recruitment`.`v_game_salary_unit`.`degreefrom`                                                                   AS `degreefrom`,
                          `recruitment`.`v_game_salary_unit`.`workyear`                                                                     AS `workyear`,
                          `recruitment`.`v_game_salary_unit`.`issuedate`                                                                    AS `issuedate`,
                          `recruitment`.`v_game_salary_unit`.`parse2_job_detail`                                                            AS `parse2_job_detail`,
                          `recruitment`.`v_game_salary_unit`.`workplace`                                                                    AS `workplace`,
                          `recruitment`.`v_game_salary_unit`.`unit`                                                                         AS `unit`,
                          (case
                               when (`recruitment`.`v_game_salary_unit`.`unit` = 1000) then (cast(substring_index(
                                       substring_index(`recruitment`.`v_game_salary_unit`.`providesalary_text`, '千/月',
                                                       1), '-', 1) as decimal(6, 2)) *
                                                                                             `recruitment`.`v_game_salary_unit`.`unit`)
                               when (`recruitment`.`v_game_salary_unit`.`unit` = 10000) then (cast(substring_index(
                                       substring_index(`recruitment`.`v_game_salary_unit`.`providesalary_text`, '万/月',
                                                       1), '-', 1) as decimal(6, 2)) *
                                                                                              `recruitment`.`v_game_salary_unit`.`unit`)
                               when (`recruitment`.`v_game_salary_unit`.`unit` = 833) then (cast(substring_index(
                                       substring_index(`recruitment`.`v_game_salary_unit`.`providesalary_text`, '万/年',
                                                       1), '-', 1) as decimal(6, 2)) *
                                                                                            `recruitment`.`v_game_salary_unit`.`unit`) end) AS `salary_min`,
                          (case
                               when (`recruitment`.`v_game_salary_unit`.`unit` = 1000) then (cast(substring_index(
                                       substring_index(`recruitment`.`v_game_salary_unit`.`providesalary_text`, '千/月',
                                                       1), '-', -(1)) as decimal(6, 2)) *
                                                                                             `recruitment`.`v_game_salary_unit`.`unit`)
                               when (`recruitment`.`v_game_salary_unit`.`unit` = 10000) then (cast(substring_index(
                                       substring_index(`recruitment`.`v_game_salary_unit`.`providesalary_text`, '万/月',
                                                       1), '-', -(1)) as decimal(6, 2)) *
                                                                                              `recruitment`.`v_game_salary_unit`.`unit`)
                               when (`recruitment`.`v_game_salary_unit`.`unit` = 833) then (cast(substring_index(
                                       substring_index(`recruitment`.`v_game_salary_unit`.`providesalary_text`, '万/年',
                                                       1), '-', -(1)) as decimal(6, 2)) *
                                                                                            `recruitment`.`v_game_salary_unit`.`unit`) end) AS `salary_max`
                   from `recruitment`.`v_game_salary_unit`)
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

create or replace definer = root@localhost view v_game_salary_unit as
select `recruitment`.`v_game_clean`.`id`                                                         AS `id`,
       `recruitment`.`v_game_clean`.`job_href`                                                   AS `job_href`,
       `recruitment`.`v_game_clean`.`job_name`                                                   AS `job_name`,
       `recruitment`.`v_game_clean`.`company_href`                                               AS `company_href`,
       `recruitment`.`v_game_clean`.`company_name`                                               AS `company_name`,
       `recruitment`.`v_game_clean`.`providesalary_text`                                         AS `providesalary_text`,
       `recruitment`.`v_game_clean`.`workarea`                                                   AS `workarea`,
       `recruitment`.`v_game_clean`.`workarea_text`                                              AS `workarea_text`,
       `recruitment`.`v_game_clean`.`updatedate`                                                 AS `updatedate`,
       `recruitment`.`v_game_clean`.`companytype_text`                                           AS `companytype_text`,
       `recruitment`.`v_game_clean`.`degreefrom`                                                 AS `degreefrom`,
       `recruitment`.`v_game_clean`.`workyear`                                                   AS `workyear`,
       `recruitment`.`v_game_clean`.`issuedate`                                                  AS `issuedate`,
       `recruitment`.`v_game_clean`.`parse2_job_detail`                                          AS `parse2_job_detail`,
       `recruitment`.`v_game_clean`.`workplace`                                                  AS `workplace`,
       (case
            when (`recruitment`.`v_game_clean`.`providesalary_text` like '%万/月') then 10000
            when (`recruitment`.`v_game_clean`.`providesalary_text` like '%千/月') then 1000
            when (`recruitment`.`v_game_clean`.`providesalary_text` like '%万/年') then 833 end) AS `unit`
from `recruitment`.`v_game_clean`;

create or replace definer = root@localhost view v_game_skill as
select `recruitment`.`f1`.`skill`                                                                          AS `技能`,
       `recruitment`.`f1`.`quantity`                                                                       AS `出现频数`,
       concat(cast(((`recruitment`.`f1`.`quantity` / `f2`.`total_quantity`) * 100) as decimal(4, 2)), '%') AS `出现率`
from `recruitment`.`v_game_skill_quantity` `f1`
         join (select count(0) AS `total_quantity` from `recruitment`.`v_game_clean`) `f2`;

create or replace definer = root@localhost view v_game_skill_quantity as
select `recruitment`.`skill_table`.`skill` AS `skill`, count(0) AS `quantity`
from (`recruitment`.`skill_table` join `recruitment`.`v_game_clean`
      on ((`recruitment`.`v_game_clean`.`parse2_job_detail` like
           concat('%', `recruitment`.`skill_table`.`skill`, '%'))))
group by `recruitment`.`skill_table`.`skill`
order by `quantity` desc
limit 30;

create or replace definer = root@localhost view v_game_workyear_salary as
select `recruitment`.`v_game_salary_min_max_mean`.`workyear`         AS `工作年限`,
       avg(`recruitment`.`v_game_salary_min_max_mean`.`salary_mean`) AS `平均薪资`
from `recruitment`.`v_game_salary_min_max_mean`
group by `recruitment`.`v_game_salary_min_max_mean`.`workyear`
order by length(`recruitment`.`v_game_salary_min_max_mean`.`workyear`),
         `recruitment`.`v_game_salary_min_max_mean`.`workyear`;


