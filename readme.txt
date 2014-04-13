Distributed Systems 
Assignment 5
Eleanor Cawthon & Claire Murphy

Usage Example:

erl -noshell -run key_value_node main 2 node1
erl -noshell -run key_value_node main 1 node2 node1@honeysuckle

If a more verbose output is desired, the code
contains a debug print function of varying verbosities, which we found useful
while tracking the progress of our processes. 

