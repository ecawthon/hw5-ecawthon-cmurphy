Distributed Systems 
Assignment 5
Eleanor Cawthon & Claire Murphy

Usage Example:

erl -noshell -run key_value_node main 2 node1
erl -noshell -run key_value_node main 1 node2 node1@honeysuckle

If a more verbose output is desired, the code
contains a debug print function of varying verbosities, which we found useful
while tracking the progress of our processes. 

We have run the following tests:
M=2, 3 nodes:
    join 1, 2, 3, store keys, kill in any order, all data is preserved
M=2, 4 nodes:
    join 1, 2, 3, 4 works and distributes processes correctly
