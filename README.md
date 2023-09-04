# Learn more about **data analytics jobs** with **Data Analytics Methods**!  🥰

🤔--**What should I do if I am interested in a data analytics position?** 

🧐--**Understanding Data Analytics Positions with Data Analytics！**



🧠 **By analyzing the data, we are going to get:** 

- **which companies need data analytics jobs?** 
- **What is the number of need?**
-  **What are the salary levels for data analytics jobs?** 
- **What do you need to learn to get a data analysis job?**
- **......**



### Preliminary

#### Data Sources

Recruitment data obtained from job website by searching for the keywords "data analysis", "game" and "operation and maintenance".

- "Data analysis" as the main object of analysis.

- Operations and games are used as references.

#### Some notes for data processing

- **Missing data processing:** The presence of any item that is empty is considered invalid data.

- **Duplicate data handling:** Only the latest posting is retained in the recruitment data of multiple identical jobs posted by the same company.

- **Recruitment is limited to first-tier cities in China**: Be able to demonstrate trends in the industry and be regionally representative.

- **Secondary search criteria:** Job names need to contain certain keywords respectively.

- For more details you can see ..... : 

  [data_analyse](./main_data_analyse.sql)

  

**目标**：处理并且分析数据

**数据怎么来？**：日志，爬虫

**怎么对比？** 与运维和游戏，对比数据分析岗位怎么样。对比薪资，招聘需求量

**限定一线城市**，代表大的趋势

**数据来源：** 51-job 

**怎么分析岗位**：市场需求（招募数量），就业企业类型（城市、行业、就业企业类型数目与占比），岗位薪资（工作年限与平均薪资的关系，代表个人发展水平。不同企业的平均薪资发布，代表行业发展水平）。核心技能（我要找到一份这样的工作，需要学会什么技能才能胜任）

**数据展示：** excel、pandas

数据源：recruitment.sql
Database: recruitment

判断哪些缺失值可以缺少。字段为空都不要了。
