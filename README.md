## Description
This module provides a class `OracleDB` for interacting with an Oracle database. It allows various operations such as connecting to the database, inserting, updating, and deleting data, as well as executing custom SQL queries.

## Requirements
- Oracle Instant Client installed and correctly configured.
- Python 3.x installed.
- `oracledb` library installed (`pip install oracledb`).
- `pandas` library installed (`pip install pandas`).

## Usage

### 1. Connection to Oracle Database
```python
from utility.oracledb import OracleDB
```
# Create an instance of OracleDB
```python
db = OracleDB(
    user='username',
    password='password',
    host='localhost',
    port=1521,
    service_name='service_name'
)
```
# Connect to the database
```python
db.connect()
```

## Basic Operations

# Insert a record into a specific table
```python
db.add_data(table_name='table_name', values={'column_1': 'value_1', 'column_2': 'value_2'})
```
# Update records in a specific table
```python
db.update_data(table_name='table_name', value_to_update={'column': 'new_value'}, condition_column='value')
```
# Delete records from a specific table
```python
db.remove_data(table_name='table_name', condition_column='value')
```
# Execute a custom SQL query
```python
result = db.execute_sql(query="SELECT * FROM table_name WHERE column = 'value'")
print(result)
```
# Select data from a specific table
```python
rows = db.select(
    table_name='table_name',
    condition={"key": "column_name", "value": "desired_value"},
    column_name=["column_1", "column_2"]
)
```
# Select data from a specific table based on a query
```python
rows = db.select_by_query(
    table_name='table_name',
    condition_query="column_name LIKE '%desired_value%'",
    column_name=["column_1", "column_2"]
)
```
## Other Operations

# Create a new table with specified columns
```python
db.create_table(table_name='table_name', columns=[{"key": "column_name", "value": "data_type"}])
```
# Check if a table exists in the database
```python
exists = db.table_exists(table_name='table_name')
```
# Remove all data from a specific table
```python
db.remove_all_data(table_name='table_name')
```
# Close the connection to the database
```python
db.close()
```
