--Listing all countries and number of stores in each country.

SELECT country_code AS country,
	COUNT(country_code) AS total_no_stores
FROM 
	dim_store_details
GROUP BY
	country_code
ORDER BY
	total_no_stores DESC;
------------------------------------------------------------------------

--Listing Number of stores in each location.

SELECT locality,
	COUNT(locality) AS total_no_stores
FROM 
	dim_store_details
GROUP BY
	locality
ORDER BY
	total_no_stores DESC;
------------------------------------------------------------------------

--Which months have produced the most sales?

WITH total_sales_table AS(
	SELECT
		dim_date_times.month,
		dim_products.product_price,
		order_table.product_quantity
	FROM order_table
	JOIN dim_products ON order_table.product_code = dim_products.product_code
	JOIN dim_date_times ON order_table.date_uuid = dim_date_times.date_uuid)
SELECT SUM(product_price * product_quantity) AS total_sales, "month"
FROM total_sales_table
GROUP BY
	"month"
ORDER BY
	total_sales DESC;
----------------------------------------------------------------------

--Calculating all sales and categorising them into Online and Offline sales.

WITH total_sales_table AS(
	SELECT s.store_type,
		   o.product_quantity
	FROM order_table o
	JOIN dim_store_details s ON o.store_code = s.store_code)
SELECT CASE
		WHEN store_type = 'Web Portal' THEN 'Web'
		ELSE 'Offline'
	END AS location,
	COUNT(product_quantity) AS number_of_sales,
	SUM(product_quantity) AS product_quantity_count
FROM total_sales_table
GROUP BY CASE
		WHEN store_type = 'Web Portal' THEN 'Web'
		ELSE 'Offline'
	END;
-----------------------------------------------------------------------

--Calculating all sales and categorising according to store type.

WITH total_sales_table AS (
    SELECT s.store_type,
           o.product_quantity,
           p.product_price
    FROM order_table o
    JOIN dim_store_details s ON o.store_code = s.store_code
    JOIN dim_products p ON o.product_code = p.product_code
	),
	sales_summary AS (
	    SELECT store_type,
	           SUM(product_price * product_quantity) AS total_sales,
	           SUM(SUM(product_price * product_quantity)) OVER () AS grand_total
	    FROM total_sales_table
	    GROUP BY store_type
)
SELECT store_type,
       total_sales,
       ROUND((total_sales / grand_total) * 100, 2) AS sales_percentage
FROM sales_summary
ORDER BY total_sales DESC;
-----------------------------------------------------------------------

--Which month in each year produced the highest cost of sales.

WITH total_sales_table  AS (
    SELECT d.month,
		   d.year,
           o.product_quantity,
           p.product_price
    FROM order_table o
    JOIN dim_date_times d ON o.date_uuid = d.date_uuid
    JOIN dim_products p ON o.product_code = p.product_code
	)
SELECT
	SUM(product_price * product_quantity) AS total_sales,
	total_sales_table.year,
	total_sales_table.month 
FROM total_sales_table
GROUP BY 
	total_sales_table.month, total_sales_table.year
ORDER BY 
	total_sales DESC
LIMIT 10;
----------------------------------------------------------------------

--Staff headcount by country.

SELECT
	SUM(staff_numbers) AS total_staff_numbers, country_code
FROM dim_store_details
GROUP BY
	country_code
ORDER BY 
	total_staff_numbers DESC
----------------------------------------------------------------------

--Which German Store is selling the most?

WITH total_sales_table  AS (
    SELECT s.store_type,
		   s.country_code,
           o.product_quantity,
           p.product_price
    FROM order_table o
    JOIN dim_store_details s ON o.store_code = s.store_code
    JOIN dim_products p ON o.product_code = p.product_code
	)
SELECT
	SUM(product_price * product_quantity) AS total_sales,
	store_type,
	country_code
FROM
	total_sales_table 
WHERE
	country_code = 'DE'
GROUP BY 
	store_type, country_code
ORDER BY 
	total_sales DESC;
---------------------------------------------------------------------
WITH sale_times AS(
	SELECT 
	    year,
	    month,
	    day,
	    timestamp,
	    (year || '-' || month || '-' || day || ' ' || timestamp)::TIMESTAMP
			AS full_timestamp
	FROM dim_date_times
	),
	sale_delta_times AS(
	SELECT
		year,
		full_timestamp,	
		LEAD (full_timestamp) OVER (ORDER BY full_timestamp) - full_timestamp 
			AS sale_difference_time
	FROM sale_times
	)
SELECT 
	year,
	AVG(sale_difference_time) AS actual_time_taken
FROM sale_delta_times
GROUP BY
	year
ORDER BY 
	actual_time_taken DESC;
	
	

