# HW2: Routy - a routing network
## Introduction
The Routy project implements a distributed Link-State routing protocol in Erlang. The architecture decouples key routing components: interface management (`intf`), topology mapping (`map`), history sequence tracking (`hist`), shortest path algorithm (`dijkstra`), and the core state machine (`routy`). This modularity provides high maintainability, fault tolerance, and dynamic path recalculation upon network state changes.
## Difficulties and Solutions
1. **Maintaining Sorted Order in Dijkstra’s `replace/4` Function** 
- **Challenge**: During Dijkstra’s shortest-path search, updating a node's shortest distance requires re-sorting the path list. Naively replacing the tuple or appending it breaks the sorted invariant, leading to suboptimal or incorrect path selection.  

- **Solution**: Implemented an ordered insertion function (`insert/2`) alongside `replace/4`. When a shorter path to a node $N$ is found with length $D$ via gateway $G$, the old entry `{node, N}` is removed from the sorted list, and the updated tuple `{N, D, G}` is recursively inserted into its correct position to maintain ascending distance order ($D$).
2. **Global Routing Logic & Event-Driven Architecture**
- **Challenge**: Comprehending how message passing between concurrent Erlang processes coordinates global routing tables was complex. Specifically, managing race conditions between Link-State Packet (LSP) propagation, history checks, and localized routing table updates presented initial design hurdles.  

- **Solution**: Mapped out the explicit state transitions and message dependencies:  

1. Local interface changes (`{add, ...}`, `{remove, ...}`, `{'DOWN', ...}`) update Intf, recompute the local Table, and issue a `broadcast`.  

2. Flooding notifications (`{links, ...}`) verify sequence numbers via `hist:update/3`. Only novel sequence updates trigger map updates, local Dijkstra recalculations, and outward forwards.  

3. Packet routing (`{route, ...}`) decouples matching logic using guards (`when To =:= Name`) and routes asynchronously through intermediate gateways.

## Potential Improvements
1. **Incremental Dijkstra Algorithm**: Currently, any link change recalculates the entire topology from scratch ($O(V^2)$ or $O(E \log V)$). Implementing incremental route updates would substantially reduce CPU overhead in large networks.  

2. **Heartbeat Mechanisms & Keep-Alive Pings**: Rather than relying strictly on process monitors (`erlang:monitor/2`), incorporating periodic heartbeat pings across interfaces would enable detection of network partition events or process silent freezes.  

3. **Link Metrics (Weighted Shortest Path)**: The current Dijkstra implementation assumes uniform hop costs (weight = 1). Extending the protocol to support dynamic link costs (latency, bandwidth) would allow traffic engineering and path optimization.  

4. **LSP Ageing and Sequence Rollover Handling**: Sequence numbers ($N$) increment indefinitely. Adding sequence counter wraparound or message Time-To-Live (TTL) field would prevent infinite loop propagation and sequence overflow in long-running networks.
## Automated Test Script  
Below is a complete test module `test_routy.erl` that sets up a 5-node topology (`r1` through `r5`), triggers automatic Link-State flooding, verifies routing table convergence, sends end-to-end packets, and simulates node failure with dynamic rerouting.
```
-module(test_routy).
-export([run/0]).

run() ->
    io:format("~n=== STARTING ROUTY AUTOMATED TEST ===~n"),

    %% 1. Start 5 Routers
    io:format("[1] Initializing 5 Routers...~n"),
    routy:start(r1, r1),
    routy:start(r2, r2),
    routy:start(r3, r3),
    routy:start(r4, r4),
    routy:start(r5, r5),
    timer:sleep(200),

    %% 2. Setup Linear Topology: r1 - r2 - r3 - r4 - r5
    io:format("[2] Connecting Routers into Linear Topology...~n"),
    connect(r1, r2),
    connect(r2, r3),
    connect(r3, r4),
    connect(r4, r5),

    %% 3. Wait for Link-State Flooding Convergence
    io:format("[3] Waiting for Network Convergence...~n"),
    timer:sleep(1000),

    %% 4. Print Status of End Nodes
    io:format("~n--- Initial Convergence Status ---~n"),
    routy:status(r1),
    routy:status(r5),

    %% 5. Test Message Routing from r1 to r5 (Multi-hop)
    io:format("~n[4] Sending Message from r1 -> r5...~n"),
    r1 ! {send, r5, "Hello from r1 to r5!"},
    timer:sleep(500),

    %% 6. Test Node Failure Dynamic Rerouting
    io:format("~n[5] Simulating Failure: Stopping Intermediate Node r3...~n"),
    routy:stop(r3),
    timer:sleep(1000),

    %% 7. Verify Network Adaptation
    io:format("~n--- Status of r1 After r3 Failure ---~n"),
    routy:status(r1),

    io:format("~n[6] Attempting to Send Message from r1 -> r5 (Should Fail safely)...~n"),
    r1 ! {send, r5, "Unreachable message test"},
    timer:sleep(500),

    %% Cleanup remaining processes
    routy:stop(r1),
    routy:stop(r2),
    routy:stop(r4),
    routy:stop(r5),
    io:format("~n=== AUTOMATED TEST COMPLETED SUCCESSFULLY ===~n"),
    ok.

connect(RouterA, RouterB) ->
    PidA = whereis(RouterA),
    PidB = whereis(RouterB),
    RouterA ! {add, RouterB, PidB},
    RouterB ! {add, RouterA, PidA}.
```

```
c(map), c(intf), c(hist), c(dijkstra), c(routy), c(test_routy).
test_routy:run().
```