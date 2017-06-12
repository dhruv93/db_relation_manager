# Milestone 4: Indexing Setup

We'll model our index commands after the syntax of MySQL.

CREATE INDEX index_name ON table_name [USING {BTREE | HASH}] (col1, col2, ...)

SHOW INDEX FROM table_name

DROP INDEX index_name ON table_name

### Creating a new index:

1. Get the underlying table. 
2. Check that all the index columns exist in the table.
3. Insert a row for each column in index key into _indices. 
4. Call get_index to get a reference to the new index and then invoke the create method on it.

###  Dropping an index:

1. Call get_index to get a reference to the index and then invoke the drop method on it.
2. Remove all the rows from _indices for this index.

### Showing index:

1. Do a query (using the select method) on _indices for the given table name.

### Dropping a table:

1. Before dropping the table, drop each index on the table.
