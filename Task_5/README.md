# ETL: User Transaction Analysis

## Project Goal

Build a simple and clear ETL (Extract → Transform → Load) pipeline using **Pandas** to analyze user transaction data.

---

## Extract

- Load data from a **CSV file** into a **Pandas DataFrame**.

---

## Transform

### Date Parsing:
- Parse the `timestamp` column into `datetime` format.

### Add New Columns:
- `day_of_week` – day of the week (e.g., Monday, Tuesday),
- `hour_of_day` – hour extracted from the `timestamp`.

### Aggregations:
For each `user_id`:
- `total_spent` – total sum of `amount`,
- `avg_transaction_value` – average transaction amount,
- `transaction_count` – number of transactions.

### Final Table Structure:

| user_id | total_spent | avg_transaction_value | transaction_count |
|---------|-------------|------------------------|--------------------|

---

## Load

- Load the final processed data into a database table called **`user_stats`**.

---

## Additional Requirements

- Organize code clearly into ETL sections (or even separate functions).
- Use proper logging (`logging.info`, `logging.debug`) at key processing steps.
- (Optional) Include a `requirements.txt` file if you plan to publish the project on GitHub.

---

## Educational Purpose

- Practice working with `datetime` in Pandas.
- Apply data grouping and aggregation techniques.
- Build a clean, repeatable, and well-structured ETL process.
- Strengthen Python best practices in a real-world data task.

---

