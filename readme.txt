Distributed Systems 
Assignment 5
Eleanor Cawthon & Claire Murphy

Usage Example:

erl -noshell -run key_value_node main 2 node1
erl -noshell -run key_value_node main 1 node2 node1@honeysuckle

If a more verbose output is desired, the code
contains a debug print function of varying verbosities, which we found useful
while tracking the progress of our processes. 

Sometimes there will be a few seconds' delay without output even though it is
still making progress - in general, when the output indicates that the storage 
processes are in storage_serve and node processes are in node_monitor, the system 
is stable.

Similarly to the last assignment, we spend a significant amount of time
brainstorming an algorithm that would allow us a fault-tolerant system where
communication was not excessively difficult. Once we decided on our algorithm, 
we implemented it gradually while simultaneously updating the writeup, although
ended up changing many details as we progressed and ran into roadblocks. We 
wrote a testing program to test our (extensive) code, but had more success 
testing with the command line. The algorithm design, learning about parts of
Erlang we had never used before, and debugging nondeterministic behavior all
contributed to making this a very difficult but also rewarding assignment.

We tested by starting and connecting various numbers of nodes with various
values of M on different lab macs. We also tested the outside-facing functions
by sending messages from an erlang shell on another computer. We have run the 
following specific tests:
M=2, 3 nodes:
    join 1, 2, 3, store keys, kill in any order, all data is preserved
M=2, 4 nodes:
    join 1, 2, 3, 4 works and distributes processes correctly
M=3, 3-4 nodes:
    Everything tested works consistently

