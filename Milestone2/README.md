# Milestone 2: Rudimentary Storage Engine

In a fully fledged system, we might have multiple (Heap Storage, BTree, etc) storage engines to choose from. We're going to start with a Heap Storage Engine. This one will use a heap file organization, i.e., records go in any block in any order.

For the Heap Storage Engine, SlottedPage is used as the block architecture and will also use Berkeley DB using a RecNo file type. 

Each of our blocks will correspond to one numbered record in the Berkeley DB file. 

We'll thus be using Berkeley DB's buffer manager for handling reading from and writing to the disk.

For this milestone we'll need : 
### create, create_if_not_exists, insert, select, open, close, and drop.

