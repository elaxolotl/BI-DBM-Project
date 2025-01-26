# Glovo Data Processing and Database Integration

This project is designed to process and analyze data related to Glovo orders and feedback. It involves data extraction, cleaning, aggregation, and loading into a MySQL database. Additionally, the processed data is exported to Excel files for further analysis.

## Project Overview {#project-overview}

The project processes two datasets from an Excel file (`dataset-glovo.xlsx`):
1. **Orders Data**: Contains information about individual orders, including store code, city, date, basket size, delivery fee, cost per order, distance, and courier waiting time.
2. **Feedback Data**: Contains feedback and cancellation reasons grouped by store for each week, including issues like wrong/missing products, packaging issues, and more.

The script performs the following steps:
1. **Extraction**: Reads data from the Excel file.
2. **Cleaning**: Cleans the data by removing duplicates, handling missing values, and standardizing column names.
3. **Aggregation**: Aggregates order data by week and store code.
4. **Database Integration**: Creates and populates a MySQL database with dimension and fact tables.
5. **Exporting**: Exports the processed data to Excel files.

## Requirements

- Python 3.x
- Libraries:
  - `pandas`
  - `mysql-connector-python`
  - `sqlite3`
  - `openpyxl` (for Excel file handling)
- MySQL Server

## Installation

1. **Clone the repository**:
   ```bash
   git clone [https://github.com/yourusername/glovo-data-processing.git
   cd glovo-data-processing
 2. **Install the required Python libraries**:
    ```bash
    pip install pandas mysql-connector-python openpyxl
 3. **Set up MySQL**:
    + Ensure MySQL Server is installed and running.
    + Create a database named glovo.
    + Set the MYSQL_PASSWORD environment variable with your MySQL root password.

# Usage
 1. Place the dataset:
    + Ensure the dataset dataset-glovo.xlsx is placed in the data/ directory.

 2. Run the script:
    ```bash
    python main.py
 3. Check the output:
    + The script will create two Excel files:
      + fact_tables.xlsx: Contains fact tables (fact_cancellations, fact_orders, fact_ratings).
      + dim_tables.xlsx: Contains dimension tables (dim_store, dim_time, dim_date, dim_order).
    + The data will also be loaded into the MySQL database.
# Data Processing
## Extraction
  + The script reads data from the Excel file dataset-glovo.xlsx using pandas.
  + Two sheets are extracted: orders and feedback.
## Cleaning
  + Column names are standardized to lowercase with underscores.
  + Duplicates are removed, and missing values are filled with 0.
  + Specific columns are converted to appropriate data types (e.g., city to category, date to datetime).
## Aggregation
  + Orders data is aggregated by week and store_code to calculate metrics like total basket size, average delivery fee, and more.
## Merging
  + The aggregated orders data is merged with the feedback data on week and store_code.
# Database Schema
The MySQL database contains the following tables:
## Dimension Tables
  + dim_store: Stores information about stores (store code and city).
  + dim_time: Stores time-related information (week, month, year).
  + dim_date: Stores date-related information (date and week).
  + dim_order: Stores order-related information (order ID and store code).
## Fact Tables
  + fact_orders: Stores detailed information about each order (order ID, date, basket size, delivery fee, etc.).
  + fact_cancellations: Stores information about order cancellations and refunds.
  + fact_ratings: Stores information about order ratings feedback and refunds.
# Exporting Data
The processed data is exported to Excel files:
  + fact_tables.xlsx: Contains all fact tables.
  + dim_tables.xlsx: Contains all dimension tables.
