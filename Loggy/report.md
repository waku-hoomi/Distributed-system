# HW1: Loggy - a logical time logger
## Introduction
In distributed systems, establishing a global, total order of events is notoriously challenging due to the absence of a synchronized global clock and unpredictable network delays. To trace, debug, and reason about execution order across asynchronous nodes, logical clocks are used instead of physical wall-clock time.

This report evaluates three implementations of a centralized logging system (Loggy) in Erlang:

1. Unsynchronized Local Clocks (No Logical Time)

2. Lamport Timestamps (Scalar Logical Time)

3. Vector Clocks (Causality Tracking)
## First attempt - without Lamport time
### Description & Execution Results

In this initial implementation, nodes send log messages to the central logger without any logical time mechanism (na / Not Available). The logger simply prints the events as they arrive via Erlang process messaging.

**result**:
```
test:run(1000, 100).
log: na paul {received,{hello,50}}
log: na john {sending,{hello,50}}
log: na george {received,{hello,61}}
log: na paul {sending,{hello,61}}
log: na ringo {sending,{hello,31}}
log: na ringo {received,{hello,40}}
log: na george {sending,{hello,40}}
log: na george {received,{hello,31}}
log: na ringo {received,{hello,1}}
log: na john {sending,{hello,1}}
log: na ringo {received,{hello,68}}
log: na paul {sending,{hello,68}}
log: na john {received,{hello,76}}
log: na george {sending,{hello,76}}
log: na george {received,{hello,80}}
log: na paul {sending,{hello,80}}
log: na john {received,{hello,7}}
log: na paul {sending,{hello,7}}
log: na ringo {received,{hello,25}}
log: na george {sending,{hello,25}}
log: na george {received,{hello,93}}
log: na paul {received,{hello,72}}
log: na george {sending,{hello,72}}
log: na john {sending,{hello,93}}
log: na john {received,{hello,74}}
log: na paul {sending,{hello,5}}
log: na ringo {sending,{hello,74}}
log: na ringo {received,{hello,5}}
log: na ringo {received,{hello,19}}
log: na john {sending,{hello,19}}
log: na john {received,{hello,57}}
log: na george {sending,{hello,57}}
log: na paul {received,{hello,4}}
log: na george {sending,{hello,4}}
log: na paul {received,{hello,84}}
log: na ringo {sending,{hello,84}}
log: na john {received,{hello,71}}
log: na george {received,{hello,28}}
log: na paul {sending,{hello,71}}
log: na john {sending,{hello,28}}
log: na george {received,{hello,73}}
log: na ringo {received,{hello,48}}
log: na paul {sending,{hello,73}}
log: na john {sending,{hello,48}}
log: na paul {received,{hello,38}}
log: na george {sending,{hello,38}}
log: na john {received,{hello,52}}
log: na george {sending,{hello,52}}
log: na ringo {received,{hello,16}}
log: na george {sending,{hello,16}}
log: na paul {received,{hello,90}}
log: na george {sending,{hello,90}}
log: na george {received,{hello,27}}
log: na ringo {sending,{hello,27}}
log: na george {received,{hello,70}}
log: na ringo {sending,{hello,70}}
log: na george {received,{hello,23}}
log: na john {sending,{hello,23}}
log: na ringo {received,{hello,40}}
log: na george {sending,{hello,40}}
log: na paul {received,{hello,8}}
log: na ringo {sending,{hello,63}}
log: na john {sending,{hello,8}}
log: na john {received,{hello,63}}
log: na paul {received,{hello,72}}
log: na george {sending,{hello,72}}
log: na paul {received,{hello,69}}
log: na george {received,{hello,16}}
log: na john {sending,{hello,69}}
stop
```
### Analysis
As seen in lines 1–2 of the output, paul logs the reception of {hello,50} before john logs the sending of {hello,50}. This violates basic causality: an effect cannot precede its cause.

- Pros:

    - Zero Overhead: No clock synchronization, metadata management, or extra computation required.

    - Simplicity: Trivial to write and execute.

- Cons:

    - Violates Causality: Messages appear received before they are sent due to asynchronous logging message delivery.

    - No Event Ordering: Impossible to reconstruct the chronological or causal sequence of distributed events.

## Sceond attempt - with lamport time
### Description & Execution Results
Each node maintains a scalar logical counter.
- Internal/Send Rule: $L_{node} = L_{node} + 1$
- Receive Rule: $L_{node} = \max(L_{node}, L_{msg}) + 1$
```
test:run(1000, 100).
log: 1 ringo {sending,{hello,31}}
log: 1 john {sending,{hello,50}}
log: 2 john {sending,{hello,1}}
log: 2 paul {received,{hello,50}}
log: 3 paul {sending,{hello,61}}
log: 4 paul {sending,{hello,68}}
log: 4 george {received,{hello,61}}
log: 5 paul {sending,{hello,80}}
log: 5 george {sending,{hello,40}}
log: 6 paul {sending,{hello,7}}
log: 6 george {received,{hello,31}}
log: 6 ringo {received,{hello,40}}
log: 7 george {sending,{hello,76}}
log: 7 ringo {received,{hello,1}}
log: 8 george {received,{hello,80}}
log: 8 john {received,{hello,76}}
log: 8 ringo {received,{hello,68}}
log: 9 george {sending,{hello,25}}
log: 9 john {received,{hello,7}}
log: 10 john {sending,{hello,93}}
log: 10 ringo {received,{hello,25}}
log: 11 ringo {sending,{hello,74}}
log: 11 george {received,{hello,93}}
log: 12 john {received,{hello,74}}
log: 12 george {sending,{hello,72}}
log: 13 george {sending,{hello,57}}
log: 13 john {sending,{hello,19}}
log: 13 paul {received,{hello,72}}
log: 14 george {sending,{hello,4}}
log: 14 john {received,{hello,57}}
log: 14 paul {sending,{hello,5}}
log: 15 paul {received,{hello,4}}
log: 15 ringo {received,{hello,5}}
log: 16 ringo {received,{hello,19}}
log: 17 ringo {sending,{hello,84}}
log: 18 paul {received,{hello,84}}
log: 19 paul {sending,{hello,71}}
log: 20 paul {sending,{hello,73}}
log: 20 john {received,{hello,71}}
log: 21 john {sending,{hello,28}}
log: 22 john {sending,{hello,48}}
log: 22 george {received,{hello,28}}
log: 23 ringo {received,{hello,48}}
log: 23 george {received,{hello,73}}
log: 24 george {sending,{hello,38}}
log: 25 george {sending,{hello,52}}
log: 25 paul {received,{hello,38}}
log: 26 george {sending,{hello,16}}
log: 26 john {received,{hello,52}}
log: 27 john {sending,{hello,23}}
log: 27 george {sending,{hello,90}}
log: 27 ringo {received,{hello,16}}
log: 28 john {sending,{hello,8}}
log: 28 ringo {sending,{hello,27}}
log: 28 paul {received,{hello,90}}
log: 29 paul {received,{hello,8}}
log: 29 ringo {sending,{hello,70}}
log: 29 george {received,{hello,27}}
log: 30 george {received,{hello,70}}
log: 31 george {received,{hello,23}}
log: 32 george {sending,{hello,40}}
log: 33 george {sending,{hello,72}}
log: 33 ringo {received,{hello,40}}
log: 34 paul {received,{hello,72}}
log: 34 ringo {sending,{hello,63}}
log: 35 john {received,{hello,63}}
stop
log: 36 john {sending,{hello,69}}
log: 36 george {received,{hello,16}}
log: 37 paul {received,{hello,69}}
```
### Analysis
Lamport time imposes a partial order on events ($e_1 \to e_2 \implies L(e_1) < L(e_2)$).Notice that {hello,50} is sent by john at $L=1$ and received by paul at $L=2$, preserving the causal send-receive constraint.
- Pros:
    - Ensures Causal Consistency: Guarantees $L(send) < L(receive)$ for any message.
    - Low Metadata Overhead: Adds only a single integer timestamp to messages and state.
- Cons:
    - Cannot Characterize Concurrency: If $L(a) < L(b)$, it does not imply $a \to b$. $a$ and $b$ could be independent (concurrent) events.
    - Clock Jumps: A process receiving a message from a node with a high clock value experiences artificial "jumps" in its local logical time, losing granular event pacing.

## Final attempt - Vector Clocks
### Description & Execution Results
Each node maintains a vector clock $V$, where $V[i]$ represents the local clock value of process $i$.
- Local/Send Event: $V_{node}[node] = V_{node}[node] + 1$
- Receive Event: $V_{node}[k] = \max(V_{node}[k], V_{msg}[k])$ for all $k$, then increment $V_{node}[node]$.
```
test:run(1000, 100).
log: [{john,1}] john {sending,{hello,50}}
log: [{paul,1},{john,1}] paul {received,{hello,50}}
log: [{paul,2},{john,1}] paul {sending,{hello,61}}
log: [{george,1},{paul,2},{john,1}] george {received,{hello,61}}
log: [{george,2},{paul,2},{john,1}] george {sending,{hello,40}}
log: [{john,2}] john {sending,{hello,1}}
log: [{ringo,1}] ringo {sending,{hello,31}}
log: [{john,1},{paul,2},{george,2},{ringo,2}] ringo {received,{hello,40}}
log: [{john,2},{paul,2},{george,2},{ringo,3}] ringo {received,{hello,1}}
log: [{paul,3},{john,1}] paul {sending,{hello,68}}
log: [{john,2},{paul,3},{george,2},{ringo,4}] ringo {received,{hello,68}}
log: [{ringo,1},{george,3},{paul,2},{john,1}] george {received,{hello,31}}
log: [{ringo,1},{george,4},{paul,2},{john,1}] george {sending,{hello,76}}
log: [{paul,4},{john,1}] paul {sending,{hello,80}}
log: [{ringo,1},{george,5},{paul,4},{john,1}] george {received,{hello,80}}
log: [{john,2},{paul,4},{george,6},{ringo,5}] ringo {received,{hello,25}}
log: [{ringo,1},{george,6},{paul,4},{john,1}] george {sending,{hello,25}}
log: [{paul,2},{george,4},{ringo,1},{john,3}] john {received,{hello,76}}
log: [{paul,5},{john,1}] paul {sending,{hello,7}}
log: [{paul,5},{george,4},{ringo,1},{john,4}] john {received,{hello,7}}
log: [{ringo,1},{george,7},{paul,5},{john,5}] george {received,{hello,93}}
log: [{george,8},{ringo,1},{paul,6},{john,5}] paul {received,{hello,72}}
log: [{ringo,1},{george,8},{paul,5},{john,5}] george {sending,{hello,72}}
log: [{paul,5},{george,4},{ringo,1},{john,5}] john {sending,{hello,93}}
log: [{george,8},{ringo,1},{paul,7},{john,5}] paul {sending,{hello,5}}
log: [{ringo,1},{george,9},{paul,5},{john,5}] george {sending,{hello,57}}
log: [{george,10},{ringo,1},{paul,8},{john,5}] paul {received,{hello,4}}
log: [{ringo,1},{george,10},{paul,5},{john,5}] george {sending,{hello,4}}
log: [{john,2},{paul,4},{george,6},{ringo,6}] ringo {sending,{hello,74}}
log: [{paul,5},{george,6},{ringo,6},{john,6}] john {received,{hello,74}}
log: [{john,5},{paul,7},{george,8},{ringo,7}] ringo {received,{hello,5}}
log: [{paul,5},{george,6},{ringo,6},{john,7}] john {sending,{hello,19}}
log: [{john,7},{paul,7},{george,8},{ringo,8}] ringo {received,{hello,19}}
log: [{george,10},{ringo,9},{paul,9},{john,7}] paul {received,{hello,84}}
log: [{john,7},{paul,7},{george,8},{ringo,9}] ringo {sending,{hello,84}}
log: [{george,10},{ringo,9},{paul,10},{john,7}] paul {sending,{hello,71}}
log: [{george,10},{ringo,9},{paul,11},{john,7}] paul {sending,{hello,73}}
log: [{paul,5},{george,9},{ringo,6},{john,8}] john {received,{hello,57}}
log: [{paul,10},{george,10},{ringo,9},{john,9}] john {received,{hello,71}}
log: [{paul,10},{george,10},{ringo,9},{john,10}] john {sending,{hello,28}}
log: [{ringo,9},{george,11},{paul,10},{john,10}] george {received,{hello,28}}
log: [{ringo,9},{george,12},{paul,11},{john,10}] george {received,{hello,73}}
log: [{george,13},{ringo,9},{paul,12},{john,10}] paul {received,{hello,38}} 
log: [{ringo,9},{george,13},{paul,11},{john,10}] george {sending,{hello,38}}
log: [{ringo,9},{george,14},{paul,11},{john,10}] george {sending,{hello,52}}
log: [{ringo,9},{george,15},{paul,11},{john,10}] george {sending,{hello,16}}
log: [{george,16},{ringo,9},{paul,13},{john,10}] paul {received,{hello,90}} 
log: [{ringo,9},{george,16},{paul,11},{john,10}] george {sending,{hello,90}}
log: [{paul,10},{george,10},{ringo,9},{john,11}] john {sending,{hello,48}}
log: [{paul,11},{george,14},{ringo,9},{john,12}] john {received,{hello,52}} 
log: [{paul,11},{george,14},{ringo,9},{john,13}] john {sending,{hello,23}}
log: [{george,16},{ringo,9},{paul,14},{john,14}] paul {received,{hello,8}}
log: [{paul,11},{george,14},{ringo,9},{john,14}] john {sending,{hello,8}}
log: [{john,11},{paul,10},{george,10},{ringo,10}] ringo {received,{hello,48}}
log: [{john,11},{paul,11},{george,15},{ringo,11}] ringo {received,{hello,16}}
log: [{john,11},{paul,11},{george,15},{ringo,12}] ringo {sending,{hello,27}}
log: [{ringo,12},{george,17},{paul,11},{john,11}] george {received,{hello,27}}
log: [{john,11},{paul,11},{george,15},{ringo,13}] ringo {sending,{hello,70}}
log: [{ringo,13},{george,18},{paul,11},{john,11}] george {received,{hello,70}}
log: [{ringo,13},{george,19},{paul,11},{john,13}] george {received,{hello,23}}
log: [{ringo,13},{george,20},{paul,11},{john,13}] george {sending,{hello,40}}
log: [{george,21},{ringo,13},{paul,15},{john,14}] paul {received,{hello,72}}
log: [{ringo,13},{george,21},{paul,11},{john,13}] george {sending,{hello,72}}
log: [{john,13},{paul,11},{george,20},{ringo,14}] ringo {received,{hello,40}}
log: [{john,13},{paul,11},{george,20},{ringo,15}] ringo {sending,{hello,63}}
log: [{paul,11},{george,20},{ringo,15},{john,15}] john {received,{hello,63}}
log: [{george,21},{ringo,15},{paul,16},{john,16}] paul {received,{hello,69}}
log: [{paul,11},{george,20},{ringo,15},{john,16}] john {sending,{hello,69}} 
log: [{ringo,16},{george,22},{paul,11},{john,13}] george {received,{hello,16}}
stop
```
### Analysis
Vector clocks capture full causal history. Comparing two vectors allows us to determine if event $A$ happened before event $B$ ($A \to B$), if $B \to A$, or if $A \parallel B$ (concurrent).
For instance:
- john sends {hello,50} at [{john,1}].
- paul receives it at [{paul,1},{john,1}], showing explicit causal dependency on john's state at 1.
- Pros:
    - Exact Causality Tracking: $V(a) < V(b) \iff a \to b$. Perfectly distinguishes between causal dependency and concurrent/independent events.
    - No False Dependencies: Eliminates ambiguity in distributed state reconstruction.
- Cons:
    - High Message & Memory Overhead: Vector size grows linearly $O(N)$ with the number of nodes $N$.
    - Dynamism Limitations: Requires knowing or dynamically updating the set of all active node identities.

## Future Improvements
To transition this logging implementation into a production-grade distributed tracer (similar to Jaeger or Zipkin), several enhancements can be introduced:
1. Logger Safe-Delivery Buffer (Holdback Queue):
    - Problem: Currently, the logger prints messages as soon as they arrive in its mailbox, meaning log lines on screen can still appear out-of-order due to network jitter.
    - Solution: Implement a holdback queue using a safe clock condition. The logger holds logs in a buffer and only flushes/prints an event at time $T$ once it has received messages with timestamp $\ge T$ from all worker nodes.
2. Dotted Version Vectors / Dynamic Vector Clocks:
    - Problem: Standard vector clocks ($O(N)$) scale poorly when nodes dynamically join or leave the system.
    - Solution: Adopt Dotted Version Vectors or Interval Tree Clocks (ITC) to support dynamic membership without unbounded vector growth.
3. Total Order Multicast Integration:
    - Problem: Lamport timestamps establish a partial order; concurrent events with equal logical timestamps are arbitrarily ordered.
    - Solution: Break ties deterministically using node IDs (e.g., $(L, Node\_ID)$) to establish a deterministic total order across all nodes.