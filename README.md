# Raft

Raft is a consensus algorithm that is designed to be easy to understand. 
It's equivalent to Paxos in fault-tolerance and performance. 
The difference is that it's decomposed into relatively independent subproblems, and it cleanly addresses all major pieces needed for practical systems. We hope Raft will make consensus available to a wider audience, and that this wider audience will be able to develop a variety of higher quality consensus-based systems than are available today.


## Description 

Minimal implementation of the raft protocol in go, the client part is incomplet while the node one should be good (needs proper testing). 
The node is implemented using the actor model to simplify concurrency.



