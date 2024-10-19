-------------------------------------------------------------------------------------------------
---------------------------  Data Cleaning  -----------------------------------------------------
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

	select * from CleanedData


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
	FROM (SELECT DISTINCT cohort_month_index FROM [Portfolio DBs].[dbo].[CleanedData]) AS sub  
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
			FROM [CleanedData]
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