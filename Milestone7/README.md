# Milestone 7: BTree Table

This week we will be creating a new storage engine using BTree. We'll use this to implement the PRIMARY KEY constraint in SQL. For example:

CREATE TABLE foo (id INT, x INT, y INT, data TEXT, PRIMARY KEY(id, x))

We'll expand on BTreeIndex and instead of storing a (block, record) handle in the BTree leaf nodes, we'll store the entire row. The primary key columns are stored in the BTree leaf nodes in the usual way, but instead of mapping each to a handle pointing into a relation, we have all the remaining columns marshaled into where the handle was stored when we just had an index.

