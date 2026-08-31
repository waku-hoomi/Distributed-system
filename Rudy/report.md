# HW1: Rudy - a small web server
## First part
After adding artificial delay, Web server needs 4485730 μs(**4.485s**) to handle 100 requests. The time of artificial  delay is $40 \times 100 = 4000ms$ (**4s**).  

**Percentage of Delay Time** is $$\frac{4}{4.485} \approx 89.18\% $$  
**Requests Per Second(RPS)** is $$\frac{100 \times 1,000,000}{4485730} \approx 22.29 \text{ requests/s}$$
SO the aritificial delay is significant in the pasrsing overhead.

### Benchmark on two machines
```
Nodes = ['client1@192.168.x.xxx','client2@192.168.x.xxx'],           
Results = rpc:multicall(Nodes, test, bench, ["192.168.x.xxx", 3192]).
```
**Result = [8204388(8.02s),8185140(8.18s)]** 

Because the logic of rudy_server is sequential single process, the server need to queue the requests and handle them in order.  
```
-module(rudy_server).

-export([start/1, stop/0]).

start(Port) ->
    register(rudy, spawn(fun() -> init(Port) end)).
stop() ->
    exit(whereis(rudy), "time to die").

init(Port) ->
    Opt = [list, {active, false}, {reuseaddr, true}],
    case gen_tcp:listen(Port, Opt) of
        {ok, Listen} ->
            handler(Listen),
            gen_tcp:close(Listen),
            ok;
        {error, Error} ->
            error
    end.

handler(Listen) ->
    case gen_tcp:accept(Listen) of
        {ok, Client} ->
            request(Client),
            handler(Listen);
        {error, Error} ->
            error
    end.

request(Client) ->
    Recv = gen_tcp:recv(Client, 0),
    case Recv of
        {ok, Str} ->
            Request = http:parse_request(Str),
            Response = reply(Request),
            gen_tcp:send(Client, Response);
        {error, Error} ->
            io:format("rudy: error: ~w~n", [Error])
    end,
    gen_tcp:close(Client).

reply({{get, URI, _}, _, _}) ->
    timer:sleep(40),
    Body = "Hello World! You requested: " ++ URI,
    http:ok(Body).

```  

## Second part  

### Spawn per Request
By changing the `request(Client)` in the `handler(Listen)` into `spawn(fun() -> request(Client) end),`, I instantly transformed Rudy from a single-threaded sequential server to a high-concurrency asynchronous server. 

### Two machines
```
Nodes = ['client1@192.168.x.xxx','client2@192.168.x.xxx'],           
Results = rpc:multicall(Nodes, test, bench, ["192.168.x.xxx", 3192]).
```
**Result = [4217774,4220608]**   
**Percentage of Delay Time** is $$\frac{4}{4.220} \approx 94.78\% $$  
**Requests Per Second(RPS)** is $$\frac{100 \times 1,000,000}{4485730} \approx 23.69 \text{ requests/s}$$

- The time comsuming is less than single thread, which means the computer make full use of the cores of cpu. 
- The cost of spawn a thread is small in erlang.
- The blocking time can hide the spawn cost.

Two Advantages:  

1. Zero-blocking decoupling:  
The main process only does one thing - gen_tcp:accept(Listen). Once it gets the client Socket, it only spends a microsecond cost to spawn a child process, handing over the control of the Client to the child process. The main process immediately recursively calls handler(Listen) to prepare to receive the next connection.

2. Connection independence and isolation:  
Each incoming HTTP request has an independent Erlang process serving it. Even if request/1 contains timer:sleep(40) or a certain request crashes due to malformed messages, it will not affect the main process and other client connections!  

Four Disadvantages:
1. Uncontrolled consumption of resources (lack of flow backpressure mechanism / Backpressure)
Although the lightweight processes in Erlang have extremely low overhead (an empty process occupies approximately 2.6 KB of memory), they are not zero overhead.

    Problem: If 100,000 HTTP requests flood in simultaneously, the server will spawn 100,000 Erlang processes without condition.

    Consequences:

    Memory exhaustion (OOM): 100,000 processes along with the memory allocated for parsing HTTP messages will instantly consume all the server's memory, causing the Erlang virtual machine (BEAM) to crash and be killed by the OS.

    File descriptors (FD) exhaustion: The operating system has a limit on the number of Socket handles that a single process can open (such as ulimit -n). Once this limit is exceeded, gen_tcp:accept will directly throw {error, emfile} and abandon the service.

2. Context Switch Overload and Scheduling Overhead  

    Problem: 
    My CPU core count (such as 4 cores and 8 threads) is fixed and can only truly parallel process 8 threads at the same time.

    Consequences:  
    When there are tens of thousands of active processes simultaneously, the scheduler of the BEAM virtual machine (Scheduler) needs to constantly perform context switches between these processes. Frequent process switching and preemptive scheduling will consume a lot of CPU power, resulting in a decrease in overall system throughput and a reduction in effective CPU utilization.

3. Lack of protection for downstream dependencies (database/file system)  

    Problem: 
    In real web development, HTTP requests often need to query the database or read files.

    Consequences:  
    If 10,000 HTTP requests arrive, Spawn per Request will cause 10,000 database query connections to be generated simultaneously. The downstream PostgreSQL/MySQL or Redis will be instantly overwhelmed by thousands of concurrent connections (connection pools will be full, timeouts will occur, and connections will be disconnected), leading to an avalanche effect.

4. Vulnerability to DDoS attacks (Denial of Service)  

    Problem: 
    Attackers only need to write a simple script to send thousands of malformed requests without sending the complete Header to the server.

    Consequences:  
    The server will spawn a process for each malicious connection and suspend it until it is completed, making it extremely vulnerable to being exhausted of server resources by the attacker at a very low cost.

### Worker pool
To solve the disadvantages in Spawn per Request, I create a Worker pool for the rudy_server.  
- Deleting the gen_tcp:close(Listen) to keep the worker pool running.  
- Using spawn_monitor and supervise to monitor the process,spawn a new process when the resource is available.  

### Two machines
```
Nodes = ['client1@192.168.x.xxx','client2@192.168.x.xxx'],           
Results = rpc:multicall(Nodes, test, bench, ["192.168.x.xxx", 3192]).
rudy_server:start(3192,100).
Results = rpc:multicall(Nodes, test, bench, ["192.168.x.xxx", 3192]).
```
**Result = [4213898,4213518]**   

Two Advantages:  

Robustness (Self-healing / Self-repairing Capability):  
This is the well-known "Let it crash" philosophy of Erlang. No matter how abnormal the request received by the Worker process causes a crash, the main process can immediately capture it and instantly start a brand new Worker to replace it. The total number of process pools will always remain at PoolSize.

Flow Control (Backpressure):  
At any given moment, the upper limit of active Workers is PoolSize (such as 8 or 100). No matter how many client requests come in, they can only queue up in the TCP queue of the operating system, completely eliminating the risk of OOM (Out of Memory) and excessive memory usage.

```
-module(rudy_server).

-export([start/2, stop/0]).

start(Port,PoolSize) ->
    register(rudy, spawn(fun() -> init(Port,PoolSize) end)).
stop() ->
    exit(whereis(rudy), "time to die").

init(Port,PoolSize) ->
    Opt = [list, {active, false}, {reuseaddr, true}],
    case gen_tcp:listen(Port, Opt) of
        {ok, Listen} ->
            start_pool(PoolSize,Listen),
            supervise(Listen),
            ok;
        {error, Error} ->
            error
    end.

start_pool(PoolSize,Listen) when PoolSize > 0 ->
    spawn_monitor(fun() -> handler(Listen) end),
    start_pool(PoolSize - 1, Listen);
start_pool(0, _) ->
    ok.

supervise(Listen) ->
    receive
        {'DOWN', _Ref, process, _Pid, _Reason} ->
            spawn_monitor(fun() -> handler(Listen) end),
            supervise(Listen)
    end.

handler(Listen) ->
    case gen_tcp:accept(Listen) of
        {ok, Client} ->
            request(Client),
            handler(Listen);
        {error, Error} ->
            error
    end.

request(Client) ->
    Recv = gen_tcp:recv(Client, 0),
    case Recv of
        {ok, Str} ->
            Request = http:parse_request(Str),
            Response = reply(Request),
            gen_tcp:send(Client, Response);
        {error, Error} ->
            io:format("rudy: error: ~w~n", [Error])
    end,
    gen_tcp:close(Client).

reply({{get, URI, _}, _, _}) ->
    timer:sleep(40),
    Body = "Hello World! You requested: " ++ URI,
    http:ok(Body).
```

Existing problems in the rudy server  
1. When exiting, the Socket leakage occurred, resulting in the inability to release the port.  
2. When a Worker crashes, it will cause the Socket to remain in an open state.  

Advice:
- Add more code for handling abnormal states  
- The way of exiting can be more elegant.