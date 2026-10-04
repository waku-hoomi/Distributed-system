# Report: Chordy — A Distributed Hash Table

## 1. Introduction

This assignment implements **Chordy**, a distributed hash table developed through four versions, from `node1` to `node4`. Each Erlang process represents a ring member. Nodes exchange asynchronous messages to maintain their neighbors, locate keys, transfer data during joins, and recover from failures.

A node owns the circular interval **(PredecessorId, Id]**. The implementation uses successor pointers for routing rather than a finger table. The main objective is to understand the trade-offs between simplicity, performance, and fault tolerance.

**Evidence:** This report analyzes the supplied code. One single-node probe log is available; performance and failure-test results have not yet been supplied. Expected behavior below is not presented as a measured result.

---

## 2. Evolution of Modules (`node1`–`node4`)

### 2.1 `node1`: Building a Ring

- **Design:** Each node maintains its identifier, predecessor, and successor. Periodic `request/status` exchanges discover better successors, while `notify` proposes predecessor changes. A probe follows successor pointers to inspect ring membership.
- **Key implementation:** Circular intervals handle ordinary ranges, wrap-around, and the single-node case:

```erlang
between(Key, From, To) when From < To ->
    Key > From andalso Key =< To;
between(Key, From, To) when From > To ->
    Key > From orelse Key =< To;
between(_Key, From, To) when From =:= To ->
    true.
```

- **Strengths & Limitations:** The message protocol is small and works with self-directed messages. However, joins require stabilization, random identifiers may collide, and a probe alone cannot prove correct ownership.
- **Reflection:** Every stabilization branch must return a valid successor. Returning a sent message or `true` previously corrupted the node state.

**Assignment questions:** Equal interval endpoints represent the whole ring, so `notify` needs no separate self-predecessor case. The explicit `{Skey, _}` stabilization branch handles a successor pointing to itself before the general interval test. More frequent stabilization reduces convergence delays but increases control traffic. Without stabilization, joins do not reliably converge. Basic notifications need no acknowledgement because later status exchanges reveal the decision; data transfer requires stronger coordination.

### 2.2 `node2`: Distributed Storage

- **Design:** A local Store holds `{Key, Value}` entries. Add and lookup requests are handled locally for owned keys or forwarded to the successor. Qref associates each response with its request.
- **Key implementation:** Entries are replaced by key, and ownership changes partition the store:

```erlang
add(Key, Value, Store) ->
    lists:keystore(Key, 1, Store, {Key, Value}).

split(From, To, Store) ->
    lists:partition(
        fun({Key, _}) -> key:between(Key, From, To) end,
        Store).
```

For `storage:split(Id, Nkey, Store)`, the first list is handed to the new predecessor; the second is retained. The receiving node merges transferred entries into its Store.

- **Strengths & Limitations:** Clients need only one contact, and the list representation is easy to inspect. Lookup and key replacement take `O(m)` local work; successor-only routing takes up to `O(n)` hops. Requests received with a nil predecessor require explicit handling.
- **Reflection:** Routing and migration must use the same interval convention. A sent handover is not proof that the destination has installed the data.

### 2.3 `node3`: Failure Detection and Ring Repair

- **Design:** Neighbors become `{Key, Ref, Pid}`, where Ref identifies an Erlang monitor. Next stores the successor's successor. A predecessor failure clears the predecessor; a successor failure promotes Next and restarts stabilization.

```erlang
down(Ref, Predecessor, {_, Ref, _}, {Nkey, Npid}) ->
    Nref = monitor(Npid),
    self() ! stabilize,
    {Predecessor, {Nkey, Nref, Npid}, nil}.
```

- **Strengths & Limitations:** Monitoring and one backup successor provide a simple repair mechanism. Two consecutive failures or a missing Next may defeat recovery. **Ring repair does not restore the failed node's Store.**
- **Reflection:** When an intermediate node becomes the successor, the former successor becomes Next. Status replies must contain the sender's immediate successor, not its Next.

### 2.4 `node4`: Successor Replication

- **Design:** Store contains primary data; Replica contains the predecessor's data. The responsible node writes locally and forwards the update to its successor. The successor acknowledges after updating Replica:

```erlang
{replicate, Key, Value, Qref, Client} ->
    Replicated = storage:add(Key, Value, Replica),
    Client ! {Qref, ok},
    node(Id, Predecessor, Successor, Next, Store, Replicated);
```

- **Strengths & Limitations:** The replica is already at the node that should take over after a failure. Replication adds memory, messages, and write latency. Two in-memory copies do not provide disk durability or protection against a shared-machine failure.
- **Reflection:** An acknowledgement should represent both primary insertion and replication. Forwarding nodes must not replicate requests they do not own.

---

## 3. Corner Cases, Performance, and Analysis

### Task 1: Failure Detection and Recovery

**Problem:** A remote `DOWN` may indicate lost connectivity rather than permanent process failure. A partition can produce conflicting ring views; reconnection needs reconciliation.

**Implementation:** The supplied `recover` message coordinates promotion of the failed predecessor's Replica before installing the replacement predecessor's data. This avoids overwriting the only surviving copy.

**Remaining limitation:** Ordinary `notify` can still race with recovery. Calling `demonitor(Ref, [flush])` before preserving the old Replica may discard the event needed to trigger promotion. Recovery and normal membership changes need a shared transition protocol. [Erlang monitor documentation](https://www.erlang.org/doc/apps/erts/erlang.html#monitor/2)

### Task 2: Replication, Retries, and Joining

**Acknowledgements and duplicates:** Confirming before replication allows acknowledged data to disappear if the primary fails. The successor can acknowledge directly after saving its copy. Key replacement makes identical retries idempotent, but a delayed old update can overwrite a newer value. Stable request IDs and versions address different aspects of this problem.

**Joining:** When `P → A → B` becomes `P → N → A → B`:

| Data | New location |
|---|---|
| A's transferred primary entries | N's Store and A's Replica |
| A's old Replica of P | N's Replica |
| A's retained Store | A's Store and B's Replica |

**Concurrency problem:** After a replica snapshot is transferred, P may still replicate a new write to A instead of N. That write can be acknowledged but absent from N when P fails.

**Possible solution:** Coordinate handover with an identifier, buffer affected writes while continuing to process control messages, acknowledge data installation, switch replication targets, and then resume writes. Snapshot-plus-update-log transfer is another option. These mechanisms are proposed improvements, not verified features of the current code.

**Reflection:** The implementation has a basic replication strategy, but cannot claim to be “obviously free of faults.” Delayed snapshots, overlapping recovery, and partitions remain important cases.

### Task 3: Performance Testing

Compare one client making 4000 requests with four clients making 1000 each. Repeat with 1, 2, and 4 storage nodes, shared and distributed contacts, and 10000 total requests.

| Configuration | Add time | Lookup time | Errors |
|---|---|---|---|
| 1 node, 4 clients, 4000 total | Not measured | Not measured | Not measured |
| 2 nodes, shared contacts | Not measured | Not measured | Not measured |
| 2 nodes, distributed contacts | Not measured | Not measured | Not measured |
| 4 nodes, distributed contacts | Not measured | Not measured | Not measured |
| 4 nodes, 10000 total | Not measured | Not measured | Not measured |

**Answers:** Four clients contacting one storage process do not create fourfold storage parallelism. More storage nodes reduce local list sizes and enable parallel work, but may increase forwarding. A common entry node can become a bottleneck. Larger datasets increase linear scan costs. The limiting factor may be storage scans, mailbox queueing, or network latency; actual results are needed to identify it.

**Method:** Start every configuration with empty stores, keep value sizes fixed, wait for stabilization, and repeat runs. Measure successful operations and errors. Concurrent batch durations must not be summed as elapsed time. The supplied batch `test:add/2` ignores failures, and `test:check/2` does not verify values; the commands below compensate for both.

### Task 4: Further Extensions

Finger tables can reduce routing hops; successor-only routing does not inherit original Chord's logarithmic lookup bound. Network-aware choices should preserve routing progress.

Replica count depends on reliability targets, failure correlation, and repair time. Copies on one machine share a failure domain. Replica reads can distribute load, but need defined stale-read semantics.

Mutable values need ordering or conflict rules. Two-phase commit is not mandatory for all replicated writes. Shopping carts may tolerate temporary divergence, but merging must correctly represent removals and quantities; payments require stricter guarantees.

---

## 4. Conclusion

The progression from `node1` to `node4` separates topology maintenance, data placement, failure detection, and replication. The main lesson is that **a repaired ring is not necessarily a recovered or consistent store**.

Distribution can improve performance and fault tolerance, but neither benefit is automatic. Correctness requirements should be defined before optimizing latency. Further work should prioritize coordinated handover and recovery, complete failure tests, and then better storage and routing. The current evidence does not establish unconditional preservation of acknowledged data.

---

## 5. Running the Tests

First fix the supplied key generator: `rand:seed/1` does not accept an integer timestamp. Remove that call and seed once per test process when reproducibility is needed. [Erlang rand documentation](https://www.erlang.org/doc/apps/stdlib/rand.html#seed/1)

```erlang
generate() ->
    rand:uniform(1000000000).
```

Save each module in a separate .erl file. Run the following startup commands in PowerShell; subsequent commands belong in the Erlang shell.

```powershell
cd D:\Homework\DS
erl
```

### 5.1 Functional and Replication Tests

```erlang
c(key).
c(storage).
c(test).
c(node4).

A = node4:start(10).
B = node4:start(20, A).
C = node4:start(30, A).
D = node4:start(40, A).
timer:sleep(5000).
A ! probe.

%% Validate several acknowledged writes.
Pairs = [{12, apple}, {18, pear}, {25, banana}].
[ok, ok, ok] = [test:add(K, V, A) || {K, V} <- Pairs].
Pairs = [test:lookup(K, D) || {K, _} <- Pairs].

%% B owns keys 12 and 18; C should recover its replicas.
exit(B, kill).
timer:sleep(5000).
A ! probe.
Pairs = [test:lookup(K, A) || {K, _} <- Pairs].

%% Join node 15, then check recovery of transferred key 12.
N = node4:start(15, A).
timer:sleep(5000).
Pairs = [test:lookup(K, A) || {K, _} <- Pairs].
exit(N, kill).
timer:sleep(5000).
Pairs = [test:lookup(K, A) || {K, _} <- Pairs].

[exit(P, kill) || P <- [A, C, D]].
```

Pattern matching checks the expected values. A failed assertion or timeout must be recorded, not treated as a pass. Fixed sleeps are convenient but do not prove stabilization. Repeat while writing during joining to investigate the concurrency race.

For earlier versions, use their own source files: probe node1, test insertion and joining in node2, and test ring repair in node3. Unlike node4, node3 is expected to lose data held only by the failed node.

### 5.2 Performance Test

Use a fresh Erlang session and an empty ring. The example has four evenly spaced storage nodes and one client.

```erlang
c(key).
c(storage).
c(test).
c(node2).

A = node2:start(250000000).
B = node2:start(500000000, A).
C = node2:start(750000000, A).
D = node2:start(1000000000, A).
timer:sleep(5000).

rand:seed(exsplus, {101, 202, 303}).
Keys = test:keys(1000).

{AddUs, Added} = timer:tc(fun() ->
    [test:add(K, gurka, A) || K <- Keys]
end).
{LookupUs, Results} = timer:tc(fun() ->
    [test:lookup(K, D) || K <- Keys]
end).
AddErrors = length([R || R <- Added, R =/= ok]).
LookupErrors = length([R || {K, R} <- lists:zip(Keys, Results),
                           R =/= {K, gurka}]).
io:format("add: ~p ms; lookup: ~p ms; errors: ~p/~p~n",
          [AddUs / 1000, LookupUs / 1000, AddErrors, LookupErrors]).

[exit(P, kill) || P <- [A, B, C, D]].
```

For 10000 requests, rebuild the ring before changing the batch size. To measure replication overhead, benchmark node4 separately.

For distributed tests, start storage VMs with distinct names and the same cookie, for example:

```powershell
erl -name ring1@192.168.1.101 -setcookie ds_test
```

Register the storage process with `register(entry, A)`. Other nodes can join through `{entry, 'ring1@192.168.1.101'}`; clients use that address as their contact. Run four client machines with different seeds, each making 1000 requests—or 2500 for 10000 total—and coordinate add/lookup phases. Test both common and different contacts. Multiple processes on one machine are not a substitute for the multi-machine experiment.

### References and Evidence Note

The report follows *Chordy: A Distributed Hash Table* (Montelius, Vlassov, and Segeljakt, 2024). Routing background: [original Chord paper](https://pdos.csail.mit.edu/papers/chord:sigcomm01/chord_sigcomm.pdf).

This report was prepared with AI assistance from the supplied code and handout. Complete the measurements and verify the final implementation before submission. The commands have not been executed in the report-writing environment.
