# Report: Groupy — A Group Membership Service

## 1. Introduction

In this assignment, we implemented **Groupy**, a Group Membership Service (GMS) providing **Atomic Multicast in View Synchrony**. The core goal is to maintain state synchronization across a set of distributed processes despite node dynamic joins and crashes. Groupy adopts a single-leader architecture: nodes send state-changing requests to the leader, which tags each message and multicasts it to all group members (slaves).

Through four iterations (`gms1` to `gms4`), we systematically built, diagnosed, and improved the system's fault tolerance, addressing issues such as missing messages, network loss, reordering, and leader failure cascades.

---

## 2. Evolution of Modules (`gms1` – `gms4`)

### 2.1 `gms1`: The Baseline (No Fault Tolerance)

* **Design**: Implemented basic group joining and atomic multicast using a single leader. The leader maintains ordered lists of slaves (`Slaves`) and application processes (`Group`).
* **Performance & Limitations**: Works reliably under ideal conditions on a single machine where process crashes do not occur. However, if the leader crashes, the entire cluster halts permanently as there is no election mechanism or failure detection.

### 2.2 `gms2`: Introducing Failure Detection & Leader Election

* **Design**: Integrated Erlang's `erlang:monitor/2` to monitor the leader process. When a slave receives a `{'DOWN', ...}` notification, it triggers an `election/4` process where the first surviving slave in the ordered `Slaves` list ascends to become the new leader.
* **Flaws Identified (`crash/1` Simulation)**:
By introducing a artificial crash probability in `bcast/3` (`crash(Id)`), we observed that when a leader crashes *mid-broadcast* (after sending a message to Slave $A$ but before Slave $B$), Slave $A$ applies the state change while Slave $B$ never sees it. After election, the cluster enters an **out-of-sync state**, violating atomic multicast requirements.

### 2.3 `gms3`: Reliable Multicast with Sequence Numbers & `Last` State

* **Design**:
1. Added explicit sequence numbers ($N$) to every `{msg, N, Msg}` and `{view, N, ...}` message.
2. Each slave tracks `N` (expected sequence number) and `Last` (the last received message).
3. Upon electing a new leader, the new leader immediately re-broadcasts `Last` to all remaining slaves before processing new requests.


* **Limitations**: While `gms3` handles single leader crashes during FIFO broadcasts, it inherently assumes **reliable network transport** and **perfect failure detectors**. Under lossy networks or heavy load, slaves can still fall out of sync or miss gaps.

### 2.4 `gms4`: NACK, Resend Buffering & Cascading Prevention

* **Design**:
* **Leader History Buffer**: Leader maintains a bounded history (`History`, constrained by `?history_limit = 100` to avoid OOM) containing recent `{N, Msg}` pairs.
* **Slave Pending Buffer (`maps`)**: Slaves store out-of-order/future messages ($I > N$) in a `Pending` map. When a missing message ($N$) is recovered via NACK (`{request_resend, N, self()}`), a recursive `flush_pending/4` helper sequentially delivers all buffered contiguous messages.
* **NACK Debouncing**: To prevent "Resend Storms" (where multiple out-of-order messages trigger a flood of duplicate NACKs that overwhelm the leader and cause cascading crashes), slaves only transmit a NACK if `maps:size(Pending) == 0`.



---

## 3. Bonus Tasks: Distributed System Corner Cases & Analysis

### Task 1: Handling Unreliable Message Delivery (Lost Messages)

#### Problem Analysis

Erlang message passing across distributed nodes relies on TCP/IP, guaranteeing FIFO order per pair of processes but **not guaranteeing message delivery** in the presence of dropped connections, network partitions, or transient socket failures. In `gms3`, if a message is dropped, the affected slave remains stuck waiting for $N$, ignoring subsequent messages $N+1, N+2, \dots$.

#### Implementation & Solution (`gms4`)

We implemented an **asynchronous NACK (Negative Acknowledgment) + Selective Resend** mechanism with a local `Pending` buffer:

1. **Detection & Buffer**: When a slave receives a message with index $I > N$, it recognizes a gap. It buffers $I$ inside `Pending` map (`maps:put(I, Msg, Pending)`) and requests the leader to retransmit $N$.
2. **Leader History**: The leader buffers the last $K$ sent messages (`?history_limit`). Upon receiving `{request_resend, ReqN, Peer}`, it looks up $ReqN$ in `History` and resends it directly to the requesting peer.
3. **Catch-Up & Unbuffering**: Once $N$ arrives, the slave processes $N$ and executes `flush_pending`, resolving consecutive cached entries ($N+1, N+2, \dots$) instantly.

#### Performance Impact Discussion

* **Latency & Throughput in Normal Operations**: Zero performance overhead. Unlike Stop-and-Wait ACK or synchronous ACK protocols, NACK is purely **reactive**. Under healthy network conditions, messages flow asynchronously with zero additional RTT (Round Trip Time).
* **Memory Cost**: Minor memory overhead on the Leader (holding $K$ historical messages) and Slaves (temporary holding of out-of-order frames in `Pending`).
* **Bandwidth Under High Loss**: In lossy environments, NACK resends consume extra bandwidth. Without NACK debouncing (`maps:size(Pending) == 0`), duplicate NACKs can overload the leader.

---

### Task 2: Impact of Imperfect Failure Detectors (Unsuspected Crashes / False Positives)

#### Problem Analysis

Erlang's built-in monitor uses heartbeats and process links. In real distributed environments (or high CPU load / Garbage Collection pauses), network delays or scheduling lags can cause the monitor to emit a premature `{'DOWN', ...}` signal for a **correct node that has NOT crashed** (a False Positive).

#### What Breaks in `gms3` / `gms4`?

1. **Split-Brain / Dual Leaders**: If Slaves falsely suspect Leader $L_1$ as dead, the next slave $L_2$ will run `election/4` and proclaim itself as Leader $L_2$. If $L_1$ is still alive, two active leaders will concurrently assign sequence numbers and broadcast conflicting views/messages, destroying total order.
2. **Stale View Deliveries**: $L_1$ might attempt to send messages to slaves that have already migrated to $L_2$'s view.

#### Architectural Solutions

To safely handle imperfect failure detectors, the system must transition to a **Term-based Consensus Protocol** (e.g., Raft / Paxos / Multi-Paxos):

* **Logical Terms / Epochs**: Every view update increment an `Epoch` number. Messages tagged with an older `Epoch` from a deposed leader are discarded by slaves.
* **Quorum Majority Validation**: A node cannot unilaterally declare itself leader just because its local monitor timed out; it must collect votes from a majority ($\lfloor N/2 \rfloor + 1$) of nodes before accepting `mcast` requests.

---

### Task 3: Unreliable Delivery by Non-Correct (Crashing) Nodes

#### Problem Analysis

A non-correct (faulty) node is a node that crashes during a view execution. A critical violation of **Uniform Atomic Multicast** occurs if a crashing node delivers a message locally (or to a subset of application peers) that no correct (surviving) node ever delivers.

#### How It Happens in Groupy

Consider the following scenario in `gms3`/`gms4`:

1. Leader $L_1$ receives message $M$ with sequence $N$.
2. $L_1$ broadcasts $M$ to Slave $1$, and then $L_1$ **crashes immediately**.
3. Slave $1$ receives $M$, delivers it to its local `Master` application, and then **Slave 1 also crashes**.
4. The remaining correct slaves ($Slave_2, Slave_3$) detect $L_1$'s death and run election. Slave $2$ becomes the new leader.
5. However, neither $Slave_2$ nor $Slave_3$ ever received $M$. The last message they saw was $N-1$.
6. **Result**: The application state on Slave $1$ diverged before it died (e.g., executing a bank withdrawal or a state transition), while no surviving node ever saw $N$.

#### Solution & Mitigation

To enforce **Uniformity** (If *any* node—even one that subsequently crashes—delivers $M$, then all correct nodes must deliver $M$):

* **Two-Phase Commit / ACK-before-Deliver (Virtual Synchrony)**: The leader cannot deliver $M$ to its local master or allow slaves to deliver $M$ upon first receipt. Instead, nodes must issue an ACK for $M$. $M$ is only *delivered to the application layer* once a quorum (or all correct members) has acknowledged receipt.
* **Forward-Before-Deliver**: Alternatively, when a slave receives $M$, it must re-transmit $M$ to its peers before delivering it to its application layer, ensuring that if it crashes right after local delivery, the message has already been propagated to surviving nodes.

---

## 4. Conclusion

Through the iterative development of `gms1` to `gms4`, we demonstrated the progression from a fragile single-leader multicast service to a fault-tolerant view-synchronous protocol. By introducing sequence numbers, last-message retransmission, NACK request debouncing, and pending buffer flushing, Groupy successfully maintains total order and state consistency under simulated process crashes and packet loss.
