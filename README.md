# Cohort-Customer-Retention-Analysis-using-SSIS-SQL-and-PBI
Understand how different customer groups of users or cohorts behave for a retail dataset, and see their engagement with the business overtime.


## About the dataset:
This is a transnational data set which contains all the transactions occurring between 01/12/2010 and 09/12/2011 for a UK-based and registered non-store online retail. 

Downloaded from [here](https://archive.ics.uci.edu/dataset/352/online+retail)
| **Variable Name** | **Role** | **Type** | **Units** | **Missing Values** | **Description** |
| --- | --- | --- | --- | --- | --- |
| InvoiceNo | ID | Categorical |  | no | a 6-digit integral number uniquely assigned to each transaction. If this code starts with letter 'c', it indicates a cancellation |
| StockCode | ID | Categorical |  | no | a 5-digit integral number uniquely assigned to each distinct product |
| Description | Feature | Categorical |  | no | product name |
| Quantity | Feature | Integer |  | no | the quantities of each product (item) per transaction |
| InvoiceDate | Feature | Date |  | no | the day and time when each transaction was generated |
| UnitPrice | Feature | Continuous | sterling | no | product price per unit |
| CustomerID | Feature | Categorical |  | no | a 5-digit integral number uniquely assigned to each customer |
| Country | Feature | Categorical |  | no | the name of the country where each customer resides |


## Importing the dataset into a Database with SSIS:
In this process I used SSIS tool which is a powerful ETL tool, designed to handle a large data migration tasks, and can be used to automate dataflows easily. 
  
So I created a package that executes an SQL task to delete any old data, then uses a dataflow task to import data from an EXCEL file to SQL Server database

![image](https://github.com/user-attachments/assets/419c6fcc-9662-4953-8bc8-de61739ecbba)


## Data Cleaning in SQL Sever:
```sql
-- Total Records = 541,909
-- 135,080 records have no CustomerId
-- 406,829 records have CustomerId

----------------------------------------------------------------------------------------------------------------------------------------
-- ---------Create CTEs that focuses on records with "customerid" and with both Quantity and UnitPrice > 0 and clear duplicated records
---------------------------------------------------------------------------------------------------------------------------------------

with 
Online_Retail as        -- 406,829 records have CustomerId
	(
		SELECT [InvoiceNo]
			  ,[StockCode]
			  ,[Description]
			  ,[Quantity]
			  ,[InvoiceDate]
			  ,[UnitPrice]
			  ,[CustomerID]
			  ,[Country]
		 FROM [Portfolio DBs].[dbo].[Online_Retail]
		 WHERE CustomerID IS NOT NULL
	)
,Quantity_UnitPrice AS   -- 397,884 records with Quantity and UnitPrice > 0
	(
		SELECT * 
		FROM Online_Retail
		WHERE Quantity > 0 AND UnitPrice > 0
	)
,Duplicates AS
	(
		SELECT 
			*, 
			--for deleting all but the first occurrence and not just counting duplicates!
			ROW_NUMBER() OVER (PARTITION BY InvoiceNo, StockCode, Quantity ORDER BY InvoiceDate) Duplicated
		FROM Quantity_UnitPrice
	)
-----------------------------------------------------------------------------------------------------------
-------------Save ALL created CTEs into a temp table in order to not run them all each time----------------
-----------------------------------------------------------------------------------------------------------
SELECT *
INTO #Temp_Online_Retail
FROM Duplicates
WHERE Duplicated = 1      --> clean data = 392,669
-- WHERE Duplicated > 1    --> duplicated records = 5,215

```

## Cohort Analysis:
To begin we need to identify 3 things…

1. Unique Identifier *(Customer ID)*
2. Initial Start Date *(Invoice Date)*
3. Revenue Data

```sql
-----------------------------------------------------------------------------------------------------------
----------------------------------------- COHORT ANALYSIS -------------------------------------------------
-----------------------------------------------------------------------------------------------------------
-- 1.  Unique Identifier (CustomerID)
-- 2.  Initial Start Date (InvoiceDate)
-- 3.  Revenue Data

SELECT
	CustomerID,
	MIN(InvoiceDate) AS First_Purchase_Date,
	DATEFROMPARTS( YEAR(MIN(InvoiceDate)), MONTH(MIN(InvoiceDate)), 1) AS CohortDate  -- day=1 because I'm not doing analysis on the day just on Year & Month 
INTO #Retention_Cohort
FROM #Temp_Online_Retail
GROUP BY CustomerID;

SELECT * FROM #Retention_Cohort;

-------------------------------------------------------------------------------------------------------------------------------
----------------- Create a Cohort Index that represents the number of months passed sice their first purchase -----------------
-----------------         when cohort_index = 0 , this means they made a purchase in the same month           -----------------
-------------------------------------------------------------------------------------------------------------------------------

SELECT
	q2.*,
	cohort_month_index = (year_diff * 12) + month_diff
FROM(
	-- after extracting year & month, we calculate the year and month differences between the InvoiceDate and the CohortDate aka first purchase
	SELECT
		q1.*,
		year_diff = q1.Invoice_Year - q1.Cohort_Year,
		month_diff = q1.Invoice_Month - q1.Cohort_Month  
	FROM(
		-- first extract both year & month from both temp tables #Temp_Online_Retail & #Retention_Cohort
		SELECT
			T.*,
			R.CohortDate,
			YEAR(T.InvoiceDate) AS Invoice_Year,
			MONTH(T.InvoiceDate) AS Invoice_Month,
			YEAR(R.CohortDate) AS Cohort_Year,    -- 1st year_purchase
			MONTH(R.CohortDate) AS Cohort_Month   -- 1st month_purchase
		FROM #Temp_Online_Retail AS T
		LEFT JOIN #Retention_Cohort AS R
			ON T.CustomerID = R.CustomerID
	) AS q1 
)AS q2;
```


After that I stored them in a VIEW to import them for vizualization:
```sql
--------------------------------------Bringing them all together------------------------------------------------------
-------------------------------------------  Data Cleaning  -----------------------------------------------------
-------------------------------------------------------------------------------------------------

CREATE OR ALTER VIEW CleanedData
AS
	WITH 
	Online_Retail AS (
		SELECT 
			[InvoiceNo], [StockCode], [Description], [Quantity],[InvoiceDate], [UnitPrice], [CustomerID], [Country]
		FROM [Portfolio DBs].[dbo].[Online_Retail]
		WHERE CustomerID IS NOT NULL
	),
	Quantity_UnitPrice AS (
		SELECT * 
		FROM Online_Retail
		WHERE Quantity > 0 AND UnitPrice > 0
	),
	Duplicates AS (
		SELECT 
			*, 
			ROW_NUMBER() OVER (PARTITION BY InvoiceNo, StockCode, Quantity ORDER BY InvoiceDate) AS Duplicated
		FROM Quantity_UnitPrice
	),
	CleanedData AS (
		SELECT *
		FROM Duplicates
		WHERE Duplicated = 1
	),
	FirstPurchase AS (
		SELECT
			CustomerID,
			MIN(InvoiceDate) AS First_Purchase_Date,
			DATEFROMPARTS(YEAR(MIN(InvoiceDate)), MONTH(MIN(InvoiceDate)), 1) AS CohortDate
		FROM CleanedData
		GROUP BY CustomerID
	),
	SUB1 AS (
			SELECT
				c.*,
				f.CohortDate,
				YEAR(c.InvoiceDate) AS Invoice_Year,
				MONTH(c.InvoiceDate) AS Invoice_Month,
				YEAR(f.CohortDate) AS Cohort_Year,    -- 1st year_purchase
				MONTH(f.CohortDate) AS Cohort_Month   -- 1st month_purchase
			FROM CleanedData AS c
			LEFT JOIN FirstPurchase AS f
				ON c.CustomerID = f.CustomerID
	),
	SUB2 AS (
		SELECT
			SUB1.*,
			year_diff = Invoice_Year - Cohort_Year,
			month_diff = Invoice_Month - Cohort_Month  
		FROM SUB1
	),
	CohortTable AS(
		SELECT
			SUB2.*,
			cohort_month_index = (year_diff * 12) + month_diff
		FROM SUB2
	)

	select * from CohortTable
```



I also created a dynamic pivot table for the cohort analysis with variables and saved it in a stored procedure.. if needed in the future.
```sql
-------------------------------------------------------------------------------------------------
---------------------------  DYNAMIC PIVOT TABLE  -----------------------------------------------
-------------------------------------------------------------------------------------------------

GO
CREATE OR ALTER PROCEDURE sp_CohortPivot
AS
BEGIN

-- Dynamic Pivot Table
	IF OBJECT_ID('tempdb..#PivotTemp') IS NOT NULL
	DROP TABLE #PivotTemp;

	DECLARE 
		@pvt AS NVARCHAR(MAX),  
		@pvt_cols AS NVARCHAR(MAX);

	-- Generate the dynamic columns
	SELECT @pvt_cols = COALESCE(@pvt_cols + ',' , '') + QUOTENAME(cohort_month_index)
	FROM (SELECT DISTINCT cohort_month_index FROM ##CTEs) AS sub  
	ORDER BY cohort_month_index;

	-- Generate the pivot query
	SET @pvt = 
		N'
		SELECT *
		INTO #PivotTemp
		FROM (
			SELECT
				DISTINCT CustomerID,
				CohortDate,            
				cohort_month_index     
			FROM ##CTEs
		) AS q1
		PIVOT 
		(
			COUNT(CustomerID) 
			FOR cohort_month_index IN (' + @pvt_cols + ')
		) AS dynamic_pvt;
		SELECT * FROM #PivotTemp ORDER BY CohortDate;';

	EXEC sp_executesql @pvt;
END
GO

EXEC sp_CohortPivot
```


## Creating the Data Model:
The dataset consists of 1 Fact Table and 2 Dimesnsion Tables 
1. Retail Data as the Fact Table
2. Date Data as Dimension Table
3. Customer Dimension Table --> created the customer table using DAX

![image](https://github.com/user-attachments/assets/a1d309eb-e20b-426f-8b67-62caa437a37b)


## Visualizations:

In this dashboard I used lots of measures and DAX functions to calculate different types of customers like:
  
- `New customers` are individuals who make their first purchase from the business. They have no prior purchase history with the company.
- `Returned customers` are those who have made at least one previous purchase and come back to make additional purchases. They may not have a consistent buying pattern but choose to return based on satisfaction with previous experiences or products
- `Retained customers` are those who continue to make purchases from a business over a longer period, indicating a higher level of loyalty and satisfaction. Retention is often measured over a specific time period, such as a year
- `Recovered customers` are those who were previously considered lost (i.e., they had stopped purchasing for a significant period) but have returned to make a purchase again


https://github.com/user-attachments/assets/a66149d2-9049-4a56-a238-c6c8a600201f



### 1st: Cohort Analysis Matrix
- In Dec 2010, a total no. of 855 customers made their 1st purchase with the company, these new customers spent 407.26 sterling on average, which is 3.4% less than the overall spend average
- **1 month later**
  - only 324 (36.6%) customers were retained and made another purchase with the company, and they spent 533.3 sterling on average which is 29% more than the overall spend average.
  - and 417 new customers made their purchase with us, and spend 622.06 sterling on average

  
### 2nd: Customer Retention Breakdown and by Month
- On the right-hand side of the dashboard, we have “Customer Retention by Month Stacked Area chart” and “Customer Retention Breakdown of a 100% stacked bar chart”
  
  both of them are pretty much the same concept and they both corresponds 1 to 1 with the matrix table here of the cohort analysis.


  
### 3rd: Customers' spending by Month
- There is a peak in the bar chart around November 2011, where it may be associated with Christmas time or another holiday periods, and this is why we have a peak with a total spending of ~1.1M
- Looking at the line chart, I noticed that even though we have a high peak in the line chart around December representing our highest average spend, we have the lowest number of customers in that time. and those customers spend more than average per transaction, So this is maybe because all other retailers have run out of stock and customers were trying to purchase as much as they can 






  
