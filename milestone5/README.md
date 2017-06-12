# Milestone 5: Insert, Delete, Simple Queries

We'll implement certain INSERT, DELETE, and very simple SELECT statements.

INSERT INTO table (col_1, col_2, col_n) VALUES (1, 2, "three");

DELETE FROM table WHERE col_1 = 1;

SELECT * FROM table WHERE col_1 = 1 AND col_n = "three";

SELECT col_1, col_2 FROM table;


The WHERE clauses in DELETE and SELECT can at most have equality conditions and the only operator allowed is the AND operator.
Further the equality conditions must be of the specific form, column = literal. 
The values in INSERT are limited to one row and must be composed of all literals.

