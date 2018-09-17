# Sql Routine Test Utility

By providing the names of two SQL routines and a query that can get a data table as the source of the input parameter values, the application can execute both SQL routines with the data rows from the data table and compare the result sets returned by the two SQL routines.

In order to achieve the aforementioned goal, the following conditions must be met:
* The parameters of the two SQL routines must have the matching names and types.
* Use column aliases in the query that gets the data table, such that the column aliases match the parameter names of the SQL routines.

For example, assume that there are two stored procedures with the following parameters:
<br/>@p1 int, 
<br/>@p2 decimal(9,2) = 2, 
<br/>@p3 varchar(7) OUTPUT.

The query that gets the data table as input parameter values could look like this: 
<br/>SELECT 123 AS [@p1], NULL AS [@p3]
