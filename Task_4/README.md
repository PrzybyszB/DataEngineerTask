# High Value Orders ETL with Pandas & PostgreSQL

## Project Overview

This project implements a simple ETL (Extract-Transform-Load) pipeline that:

1. Loads data from a CSV file into a Pandas DataFrame.
2. Transforms the data by:
   - Calculating the total sales value for each order (`quantity * price`),
   - Creating a new column `total_value`,
   - Filtering only the orders where `total_value > 1000`,
   - Sorting the results in descending order by `total_value`.
3. Loads the transformed data into a PostgreSQL database table called `high_value_orders`.

---