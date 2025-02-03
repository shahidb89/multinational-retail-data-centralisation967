--Altering data types of some cloumns in order_table.

ALTER TABLE order_table
	ALTER COLUMN date_uuid TYPE UUID USING date_uuid::uuid,
	ALTER COLUMN user_uuid TYPE UUID USING user_uuid::uuid,
	ALTER COLUMN card_number TYPE VARCHAR(19),
	ALTER COLUMN store_code TYPE VARCHAR(12),
	ALTER COLUMN product_code TYPE VARCHAR(11),
	ALTER COLUMN product_quantity TYPE SMALLINT;
-----------------------------------------------------------------------
-- Altering data types of some columns in dim_users table.
ALTER TABLE dim_users
	ALTER COLUMN first_name TYPE VARCHAR(255),
	ALTER COLUMN last_name TYPE VARCHAR(255),
	ALTER COLUMN date_of_birth TYPE DATE USING date_of_birth::date,
	ALTER COLUMN country_code TYPE VARCHAR(3),
	ALTER COLUMN user_uuid TYPE UUID USING user_uuid::uuid,
	ALTER COLUMN join_date TYPE DATE;
------------------------------------------------------------------------
--Altering data types of some columns in dim_sroe_details.
ALTER TABLE dim_store_details
	ALTER COLUMN longitude TYPE NUMERIC USING longitude::numeric,
	ALTER COLUMN locality TYPE VARCHAR(255),
	ALTER COLUMN store_code TYPE VARCHAR(12),
	ALTER COLUMN staff_numbers TYPE SMALLINT USING staff_numbers::smallint,
	ALTER COLUMN opening_date TYPE DATE,
	ALTER COLUMN store_type TYPE VARCHAR(255),
	ALTER COLUMN store_type DROP NOT NULL,
	ALTER COLUMN latitude TYPE NUMERIC USING latitude::numeric, 
	ALTER COLUMN country_code TYPE VARCHAR(3),
	ALTER COLUMN continent TYPE VARCHAR(255);
-----------------------------------------------------------------------

-- Adding weight_class column to dim_products table
ALTER TABLE dim_products
	ADD COLUMN weight_class TEXT;
	
--Removing kg unit from weight column.
UPDATE dim_products
	SET weight = regexp_replace(weight, 'kg', '', 'g')::NUMERIC;

--Removing £ sign from prices column.
UPDATE dim_products
	SET product_price = regexp_replace(product_price, '£', '', 'g')::NUMERIC;

--Renaming removed column name to still_available in dim_products table.
ALTER TABLE dim_products
	RENAME COLUMN removed TO still_available;
--Changing stll_avilable column type to boolean in dim_products table.
UPDATE dim_products
SET still_available = CASE
                		WHEN still_available = 'Still_avaliable' THEN TRUE
                		WHEN still_available = 'Removed' THEN FALSE
             		  END;
	
-- Altering data types of some columns in dim_products table.
ALTER TABLE dim_products
	ALTER COLUMN weight TYPE NUMERIC USING weight::numeric,
	ALTER COLUMN product_price TYPE NUMERIC USING product_price::numeric,
	ALTER COLUMN "EAN" TYPE VARCHAR(17),
	ALTER COLUMN product_code TYPE VARCHAR(11),
	ALTER COLUMN date_added TYPE DATE USING date_added::date,
	ALTER COLUMN uuid TYPE UUID USING uuid::uuid,
	ALTER COLUMN still_available TYPE BOOL USING still_available::boolean,
	ALTER COLUMN weight_class TYPE VARCHAR(14);
	
--Setting values of weight_class column.
UPDATE dim_products
SET weight_class = CASE
    WHEN weight < 2 THEN 'Light'
    WHEN weight >= 2 AND weight < 40 THEN 'Mid_Sized'
    WHEN weight >= 40 AND weight < 140 THEN 'Heavy'
    WHEN weight >= 140 THEN 'Truck_Required'
END;
-----------------------------------------------------------------------

--Altering column data types of dim_date_times table

ALTER TABLE dim_date_times
	ALTER COLUMN "month" TYPE VARCHAR(2),
	ALTER COLUMN "year" TYPE VARCHAR(4),
	ALTER COLUMN "day" TYPE VARCHAR(2),
	ALTER COLUMN time_period TYPE VARCHAR(10),
	ALTER COLUMN date_uuid TYPE UUID USING date_uuid::uuid;
-----------------------------------------------------------------------

--Alterind column data types of dim_card_details table.

ALTER TABLE dim_card_details
	ALTER COLUMN card_number TYPE VARCHAR(22),
	ALTER COLUMN expiry_date TYPE VARCHAR(5),
	ALTER COLUMN date_payment_confirmed TYPE DATE;
-----------------------------------------------------------------------	

--Adding Primary Key to dim tables

ALTER TABLE dim_card_details 
	ADD CONSTRAINT dim_card_details_pkey PRIMARY KEY (card_number);

ALTER TABLE dim_date_times 
	ADD CONSTRAINT dim_date_times_pkey PRIMARY KEY (date_uuid);

ALTER TABLE dim_products 
	ADD CONSTRAINT dim_products_pkey PRIMARY KEY (product_code);

ALTER TABLE dim_store_details 
	ADD CONSTRAINT dim_store_details_pkey PRIMARY KEY (store_code);

ALTER TABLE dim_users 
	ADD CONSTRAINT dim_users_pkey PRIMARY KEY (user_uuid);


----------------------------------------------------------------------

--Adding Foreign Keys to order_table
--Deleting card_number rows in order_tbale that does not exist in dim tables.

DELETE FROM order_table
	WHERE card_number NOT IN (SELECT card_number FROM dim_card_details);

ALTER TABLE order_table
	ADD CONSTRAINT card_fk FOREIGN KEY (card_number)
	REFERENCES dim_card_details(card_number);

ALTER TABLE order_table
	ADD CONSTRAINT date_fk FOREIGN KEY (date_uuid)
	REFERENCES dim_date_times(date_uuid);

ALTER TABLE order_table
	ADD CONSTRAINT product_fk FOREIGN KEY (product_code)
	REFERENCES dim_products(product_code);

DELETE FROM order_table
	WHERE store_code NOT IN (SELECT store_code FROM dim_store_details);

ALTER TABLE order_table
	ADD CONSTRAINT store_fk FOREIGN KEY (store_code)
	REFERENCES dim_store_details(store_code);

ALTER TABLE order_table
	ADD CONSTRAINT user_fk FOREIGN KEY (user_uuid)
	REFERENCES dim_users(user_uuid);

SELECT DISTINCT card_number 
FROM order_table 
WHERE card_number NOT IN (SELECT card_number FROM dim_card_details);


SELECT column_name, data_type
    FROM information_schema.columns
	WHERE table_name = 'dim_date_times';