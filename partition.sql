create table tab1(id int);
load data local inpath '/config/workspace/tab1.txt' into table tab1;
ALTER TABLE tab1 RENAME TO tab1_renamed;
describe tablename;
describe databasename;
truncate table tablename;
drop table tablename;
drop database databasename;


create table sales_data_non_partitioned(
    ORDERNUMBER int,
    QUANTITYORDERED int,
    PRICEEACH float,
    ORDERLINENUMBER int,
    SALES int,
    STATUS string,
    QTR_ID int,
    MONTH_ID int,
    YEAR_ID int,
    PRODUCTLINE string,
    MSRP string,
    PRODUCTCODE string,
    PHONE string,
    CITY string,
    STATE string,
    POSTALCODE string,
    COUNTRY string,
    TERRITORY string,
    CONTACTLASTNAME string,
    CONTACTFIRSTNAME string,
    DEALSIZE string
)
row format delimited
fields terminated by ','
tblproperties("skip.header.line.count"="1")

--loading data into non partitioned table

load data local inpath '/config/workspace/sales_data.csv' into table sales_data_non_partitioned;

select * from sales_data_non_partitioned limit 10;

--crating static partitioned table

create table sales_data_partitioned_static(
    ORDERNUMBER int,
    QUANTITYORDERED int,
    PRICEEACH float,
    ORDERLINENUMBER int,
    SALES int,
    STATUS string,
    QTR_ID int,
    MONTH_ID int,
    YEAR_ID int,
    MSRP string,
    PRODUCTCODE string,
    PHONE string,
    CITY string,
    STATE string,
    POSTALCODE string,
    COUNTRY string,
    TERRITORY string,
    CONTACTLASTNAME string,
    CONTACTFIRSTNAME string,
    DEALSIZE string
)
partitioned by(PRODUCTLINE string)
row format delimited
fields terminated by ',';

static partitioning example

insert into table sales_data_partitioned_static 
partition(PRODUCTLINE="Motorcycles")
select ORDERNUMBER,QUANTITYORDERED,PRICEEACH,ORDERLINENUMBER,SALES,STATUS,QTR_ID,MONTH_ID,YEAR_ID,MSRP,PRODUCTCODE,PHONE,CITY,STATE,POSTALCODE,COUNTRY,TERRITORY,CONTACTLASTNAME,CONTACTFIRSTNAME,DEALSIZE
from sales_data_non_partitioned
where PRODUCTLINE = 'Motorcycles';

select * from sales_data_partitioned_static limit 1;

insert into table sales_data_partitioned_static
partition(PRODUCTLINE="Classic Cars")
select ORDERNUMBER,QUANTITYORDERED,PRICEEACH,ORDERLINENUMBER,SALES,STATUS,QTR_ID,MONTH_ID,YEAR_ID,MSRP,PRODUCTCODE,PHONE,CITY,STATE,POSTALCODE,COUNTRY,TERRITORY,CONTACTLASTNAME,CONTACTFIRSTNAME,DEALSIZE
from sales_data_non_partitioned
where PRODUCTLINE = 'Classic Cars';

select * from sales_data_partitioned_static where PRODUCTLINE = 'Classic Cars' limit 1;

show partitions sales_data_partitioned_static;

--dynamic partitioning

set hive.exec.dynamic.partition.mode = nonstrict;

create table sales_data_partitioned_dynamic(
    ORDERNUMBER int,
    QUANTITYORDERED int,
    PRICEEACH float,
    ORDERLINENUMBER int,
    SALES int,
    STATUS string,
    QTR_ID int,
    MONTH_ID int,
    YEAR_ID int,
    MSRP string,
    PRODUCTCODE string,
    PHONE string,
    CITY string,
    STATE string,
    POSTALCODE string,
    COUNTRY string,
    TERRITORY string,
    CONTACTLASTNAME string,
    CONTACTFIRSTNAME string,
    DEALSIZE string
)
partitioned by(PRODUCTLINE string)
row format delimited
fields terminated by ',';

load data into table with partition dynamically

set hive.exec.dynamic.partition.mode = nonstrict;

insert into table sales_data_partitioned_dynamic
partition(PRODUCTLINE)
select ORDERNUMBER,QUANTITYORDERED,PRICEEACH,ORDERLINENUMBER,SALES,STATUS,QTR_ID,MONTH_ID,YEAR_ID,PRODUCTLINE,MSRP,PRODUCTCODE,PHONE,CITY,STATE,POSTALCODE,COUNTRY,TERRITORY,CONTACTLASTNAME,CONTACTFIRSTNAME,DEALSIZE
from sales_data_non_partitioned
where PRODUCTLINE = 'Motorcycles';

select * from sales_data_partitioned_dynamic limit 10;
