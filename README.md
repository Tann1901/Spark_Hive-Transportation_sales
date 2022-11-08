# spark-hive-transportation-sales
Using Hive and Spark and SQL to analyze the data regarding transportation

Using three datasets Sales_data.csv, Company_data.csv and company __Emp_data1.csv in Hadoop environment and SQL to create different reports.
# CREATE SALES TABLE IN HIVE 2.0
Schema of columns for the external table:
```
CREATE TABLE IF NOT EXISTS sales 
(
ORDERNUMBER INT,
QUANTITYORDERED INT,
PRICEEACH INT,
ORDERLINENUMBER INT,
SALES INT,
REVENUE INT,
ORDERDATE INT,
STATUS STRING,
QTR_ID INT,
MONTH_ID INT,
YEAR_ID INT,
PRODUCTLINE STRING,
MSRP INT,
PRODUCTCODE STRING,
CUSTOMERNAME STRING,
PHONE INT,
ADDRESSLINE1 STRING,
ADDRESSLINE2 STRING,
CITY STRING,
STATE STRING,
POSTALCODE STRING,
COUNTRY STRING,
TERRITORY STRING,
CONTACTLASTNAME STRING,
CONTACTFIRSTNAME STRING,
DEALSIZE STRING,
CompanyID INT
)
COMMENT 'public database'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE ;

LOAD DATA INPATH '/user/maria_dev/drivers.csv' OVERWRITE INTO TABLE drivers
```
# PROCESS DATA IN ZEPPELIN

Go to Zeppelin Notebook

Create a Company_Emp_data1 DataFrame from CSV file
```
val Company_Emp_data1 = (spark.read
.option("header", "true") // Use first line as header
.option("inferSchema", "true") // Infer schema
.csv("/tmp/Company_Emp_data1.csv"))
```

Create a company_data DataFrame from CSV file
```
val company_data = (spark.read
.option("header", "true") // Use first line as header
.option("inferSchema", "true") // Infer schema
.csv("/tmp/company_data.csv"))
```
Print Schema Company_Emp_data1
```
Company_Emp_data1.printSchema()
```
PrintSchema company_data
```
company_data.printSchema()
```

Create Tempview Company_Emp_data1
```
Company_Emp_data1.createOrReplaceTempView(“EmpView”)
```
Create Tempview Compview
```
Company_data.createOrReplaceTempView(“CompView”)
```

# ANALYSIS

I. Produce a pie chart with the Cumulation of revenue by country from Sales  for the following countries : USA, UK, France, Austria, Canada and Denmark.
```
%spark2.sql
SELECT sum(revenue) AS Cumulative_Revenue, country AS Country FROM sales
WHERE country=’USA’ GROUP BY Country
UNION
SELECT sum(revenue) AS Cumulative_Revenue, country AS Country FROM sales
WHERE country=’UK’ GROUP BY Country
UNION
SELECT sum(revenue) AS Cumulative_Revenue, country AS Country FROM sales
WHERE country=’France’ GROUP BY Country
UNION
SELECT sum(revenue) AS Cumulative_Revenue, country AS Country FROM sales
WHERE country=’Austria’ GROUP BY Country
UNION
SELECT sum(revenue) AS Cumulative_Revenue, country AS Country FROM sales
WHERE country='Canada' GROUP BY Country
UNION
SELECT sum(revenue) AS Cumulative_Revenue, country AS Country FROM sales
WHERE country='Denmark' GROUP BY Country
```
Or
```
%spark2.sql
SELECT sum(Revenue), country
FROM Salesview
WHERE country in ("USA","UK","France","Austria","Canada","Denmark")
GROUP BY country
```
RESULT

<img src="https://github.com/Tann1901/spark-hive-transportation-sales/blob/main/photos/I.%20Country%20pie%20chart%201.jpg" height="400">
<img src="https://github.com/Tann1901/spark-hive-transportation-sales/blob/main/photos/I.%20Country%20pie%20chart%202.jpg" height="200">
<img src="https://github.com/Tann1901/spark-hive-transportation-sales/blob/main/photos/I.%20Country%20pie%20chart%203.jpg" height="350">

II. Line chart of all sales grouped by productline , each productline will be represented in the chart by a different line and color, within 8 company ID

```
%spark2.sql
SELECT sum(s.revenue), s.productline, c.name
FROM sales s
JOIN compview as c on c.companyID = s.companyID
WHERE c.companyID < 8
GROUP BY s.productline, c.name
```
RESULT

<img src="https://github.com/Tann1901/spark-hive-transportation-sales/blob/main/photos/II.Transportation_Linechart1.jpg" height="400">
<img src="https://github.com/Tann1901/spark-hive-transportation-sales/blob/main/photos/II.Transportation_Linechart2.jpg" height="350">


III. Present Revenue of vehicles like Trucks & Planes as an acquisition strategy with a bar chart with the sum of revenue by company name in order of ascending revenue filtered for country USA and for product line “Trucks and Buses” and “Planes”.
```
%spark2.sql
SELECT sum(revenue), productline, country
FROM sales
WHERE
(
(productline = ‘Trucks and Buses’)
OR
(productline = ‘ Planes’)
)
AND
(country = ‘USA’)
GROUP BY productline, country ORDER BY sum(revenue) DESC
```
RESULT

<img src="https://github.com/Tann1901/spark-hive-transportation-sales/blob/main/photos/III.Plane%26Truck%20Bar%20chart.jpg" height="250">
<img src="https://github.com/Tann1901/spark-hive-transportation-sales/blob/main/photos/III.Plane%26Truck%20Bar%20chart2.jpg" height="400">

IV. Present average salary for each company by name – present the average salary in a bar chart with the highest average salary first.

```
%spark2.sql
SELECT avg(e.salary), e.companyID, c.Name
FROM empview e, compview c
WHERE e.companyID = c.companyID
GROUP BY e.companyID, c.name
ORDER BY avg(e.salary) DESC
```
RESULT
<img src="https://github.com/Tann1901/spark-hive-transportation-sales/blob/main/photos/IV.%20Average%20salary%20with%20ranking.jpg" height="400">
