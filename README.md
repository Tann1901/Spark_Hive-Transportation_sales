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
<<<<<<< HEAD

# PROCESS DATA IN ZEPPELIN
=======
>>>>>>> 4ad9a903123523458824dd58da752ad6eb4c4fe8

Go to Zeppelin Notebook
// Create a Company_Emp_data1 DataFrame from CSV file
```
val Company_Emp_data1 = (spark.read
.option("header", "true") // Use first line as header
.option("inferSchema", "true") // Infer schema
.csv("/tmp/Company_Emp_data1.csv"))
```
// Create a company_data DataFrame from CSV file
```
val company_data = (spark.read
.option("header", "true") // Use first line as header
.option("inferSchema", "true") // Infer schema
.csv("/tmp/company_data.csv"))
```
