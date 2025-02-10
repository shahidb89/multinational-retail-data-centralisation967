# Multinational Retail Business Data Pipeline

## Project Overview
This project is designed to extract, clean, and load retail business data into a PostgreSQL database. The system processes data from multiple sources, including RDS databases, APIs, PDFs, and AWS S3, transforming it into a structured format suitable for business analysis.

## Project Structure
The project consists of the following key files:

### 1. `database_utils.py`
This file contains the `DatabaseConnector` class, which is responsible for managing database connections and executing SQL queries.

- Reads database credentials from YAML files.
- Initializes database connections for reading and writing.
- Uploads cleaned data to the target database.
- Executes SQL queries (SELECT, INSERT, UPDATE, DELETE, ALTER).

### 2. `data_extraction.py`
This file includes the `DataExtractor` class, which extracts data from various sources:

- Lists all tables in the RDS database.
- Reads specific tables into Pandas DataFrames.
- Extracts tabular data from PDFs.
- Retrieves data from APIs (store details, number of stores).
- Downloads and reads files from an AWS S3 bucket.

### 3. `data_cleaning.py`
This module contains the `DataCleaning` class, which is responsible for cleaning and preprocessing extracted data. The cleaning functions include:

- Removing duplicates and handling missing values.
- Converting data types for consistency.
- Standardizing data formats.
- Cleaning specific datasets, such as users, cards, stores, products, orders, and events.

### 4. `db_schema.sql`
This SQL script defines the database schema for the retail business project. It includes:

- Table structures for users, cards, stores, products, orders, and events.
- Constraints such as primary keys, foreign keys, and indexes.
- Data types and relationships between tables.

### 5. `query.sql`
This file contains SQL queries for data retrieval and analysis. Some examples include:

- Extracting key business insights.
- Performing joins between tables.
- Aggregating sales data.
- Filtering data based on specific conditions.

## Setup Instructions
### Prerequisites
Ensure you have the following dependencies installed:
- Python 3.8+
- PostgreSQL
- Required dependencies listed in `environment.yml` (install via `conda env create -f environment.yml`)

### Running the Project
The project has a robust pipeline. Only running database_utils.py is necessary to load all the required data to the sales_data database. Then run db_schema.sql commands in PostgreSQL to build the fully functioning database. Here is step by step instructions to build the fully functioning database:
1. **Set Up Database Credentials**
   - Create `db_creds.yaml` and `sales_db_creds.yaml` with the necessary database connection details.

2. **Load Data into the Database**
   ```sh
   python database_utils.py
   ```

3. **Run SQL Commands**
    Execute `db_schema.sql` in PostgreSQL to build the database schema.

4. **Run SQL Queries**
   Execute `query.sql` in PostgreSQL to analyze the data.

## Author
This project was developed for retail business data processing and analysis. Feel free to contribute or extend the functionalities as needed.

## License
This project is licensed under the MIT License.


