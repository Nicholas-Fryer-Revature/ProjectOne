use project1v2;

show tables;

--CREATE Tables
Create table count(beverage STRING, orders INT) 
row format delimited fields terminated by ',' stored as textfile; 

Create table branch(beverage STRING, branch STRING) 
row format delimited fields terminated by ',' stored as textfile; 

--POPULATE tables
Load data inpath '/user/nick/project1dataset/Bev_ConscountA.txt' into table count;
Load data inpath '/user/nick/project1dataset/Bev_ConscountB.txt' into table count;
Load data inpath '/user/nick/project1dataset/Bev_ConscountC.txt' into table count;

Load data inpath '/user/nick/project1dataset/Bev_BranchA.txt' into table branch;
Load data inpath '/user/nick/project1dataset/Bev_BranchB.txt' into table branch;
Load data inpath '/user/nick/project1dataset/Bev_BranchC.txt' into table branch;

describe count;
describe branch;

select * from count;
select * from branch;


--SCENARIO 1
--What is the total number of consumers for Branch1?
select sum(c.orders) as branch1_consumers 
from branch b JOIN count c on (b.beverage = c.beverage) 
WHERE b.branch = "Branch1";
--1,115,974

--What is the number of consumers for the Branch2?
select sum(c.orders) as branch2_consumers 
from branch b JOIN count c on (b.beverage = c.beverage) 
WHERE b.branch = "Branch2";
--5,099,141


--SCENARIO 2
--What is the most consumed beverage on Branch1
SELECT b.beverage AS beverages, SUM(c.orders) AS total 
FROM branch b FULL OUTER JOIN count c ON (b.beverage = c.beverage) 
WHERE b.branch = "Branch1" 
GROUP BY b.beverage ORDER BY total DESC LIMIT 1;
--Special_cappuccino @ 108,163

--What is the least consumed beverage on Branch2
select b.beverage as beverages, sum(c.orders) as total 
from branch b FULL OUTER JOIN count c on (b.beverage = c.beverage) 
WHERE b.branch = "Branch2" 
group by b.beverage order by total limit 1;
--Cold_MOCHA @ 47,524


--SCENARIO 3
--What are the beverages available on Branch10, Branch8, and Branch1?
select distinct beverage 
from branch 
where branch = 'Branch10' or branch = 'Branch8' or branch = 'Branch1';
--44 Rows

--What are the comman beverages available in Branch4,Branch7?

--Method 1 Make 2 tables with branch7 and branch 4 beverages then join on beverages 
create table branch4 as select distinct beverage, branch 
from branch where branch = 'Branch4';
select * from branch4 order by beverage; 

create table branch7 as select distinct beverage, branch 
from branch where branch = 'Branch7';
select * from branch7 order by beverage;

select * from branch4 b4 inner join branch7 b7 on (b4.beverage = b7.beverage);

--Method 2 With Intersect
select beverage 
from branch 
where branch = 'Branch4' 
INTERSECT 
select beverage 
from branch 
where branch = 'Branch7';
--51 Rows

--SCENARIO 4
--create a partition, index, View for the scenario 3.

--Partition
create table branchpart (beverage STRING) partitioned by (branch STRING);
set hive.exec.dynamic.partition.mode = nonstrict;
set hive.exec.dynamic.partition=true;
insert into branchpart partition (branch) select * from branch;
select * from branchpart; 

--Index
create table beverages4and7  as select beverage from branch where branch = 'Branch4' 
INTERSECT 
select beverage from branch where branch = 'Branch7';

select * from beverages4and7;

create index ibeverages4and7 on table beverages4and7(beverage) as 'COMPACT' WITH DEFERRED REBUILD;
ALTER INDEX ibeverages4and7 ON beverages4and7 REBUILD;
Show index on beverages4and7; 

--View
create view vbeverages4and7 as select beverage from branch where branch = 'Branch4' 
INTERSECT 
select beverage from branch where branch = 'Branch7';
select * from vbeverages4and7; 
describe vbeverages4and7;
drop view vbeverages4and7;


--SCENARIO 5
--Alter the table properties to add "note","comment"
alter table branch set tblproperties('TableFor'='Branch A,B,C');
alter table branch set tblproperties('Note'='Branch Locations And Beverages');
SHOW tblproperties branch;

alter table count set tblproperties('Note'='Beverages And Order Numbers');
SHOW tblproperties count;


--SCENARIO 6
--Remove the row 5 from the output of Scenario 1 

select branch, beverage, ROW_NUMBER() over () as row_num from branch;

--Method 1 with intermediary table
create table branch_with_rows as select branch, beverage, ROW_NUMBER() over () as row_num from branch;
create table branch_without_5 as select branch, beverage from branch_with_rows where row_num != 5;

drop table branch_with_rows; 

--Method 2 without intermediary table
create table branch_without_5 as 
select branch, beverage 
from (select branch, beverage, ROW_NUMBER() OVER() as row_num from branch) as temp 
where row_num != 5;

drop table branch_with_rows;

--Method 3 Insert Overwrite
CREATE TABLE branch2 as SELECT * FROM branch; 
--Row 5  SMALL_ESPRESSO  Branch1
INSERT overwrite table branch2 
select beverage, branch 
from (select ROW_NUMBER() OVER() as row_num, beverage, branch from branch2) as temp 
where row_num != 5;

select * from branch2;
Select * from branch2 LIMIT 6; 
Select * from branch2 where beverage = 'SMALL_Espresso';
select * from branch2 where beverage = 'SMALL_Espresso' AND branch = 'Branch1';
select count(*) as row_num from branch2; 
drop table branch2;
show tables; 

--Method 4 Select And Filter out Row 5
select * from (select *, ROW_NUMBER() over() as row_num from branch) as temp where row_num != 5;

select * from branch_without_5;
drop table branch_without_5;
