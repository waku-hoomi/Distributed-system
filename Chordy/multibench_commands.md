# Multi-machine Chordy Benchmark

Use four storage machines and four client machines. The shell on client1 is
also the coordinator; no ninth machine is required. Replace all example IP
addresses with the real addresses, and use the same cookie on every VM.

## 1. Start the storage VMs

Run the appropriate command in PowerShell on each storage machine:

```powershell
# Storage machine 1
erl -name ring1@192.168.1.101 -setcookie ds_test
# Storage machine 2
erl -name ring2@192.168.1.102 -setcookie ds_test
# Storage machine 3
erl -name ring3@192.168.1.103 -setcookie ds_test
# Storage machine 4
erl -name ring4@192.168.1.104 -setcookie ds_test
```

Place key.erl, storage.erl, and node2.erl in each VM's working directory.
In EACH storage Erlang shell, compile:

```erlang
c(key).
c(storage).
c(node2).
```

Do not create ring processes manually: the coordinator will create the exact
ring required for each experiment. All four storage VMs remain running even
when only one or two participate in the ring.

## 2. Start the client VMs

Copy multibench.erl to all four client machines. Run the appropriate PowerShell
command on each machine:

```powershell
# Client machine 1 (also coordinator)
erl -name client1@192.168.1.105 -setcookie ds_test
# Client machine 2
erl -name client2@192.168.1.106 -setcookie ds_test
# Client machine 3
erl -name client3@192.168.1.107 -setcookie ds_test
# Client machine 4
erl -name client4@192.168.1.108 -setcookie ds_test
```

In EACH client Erlang shell:

```erlang
c(multibench).
```

The benchmark does not depend on test.erl or key:generate/0. It generates keys
inside each client using a distinct fixed seed, verifies returned values,
counts errors, and coordinates the two timed phases.

## 3. Define the environment on client1

Execute everything below in the client1 Erlang shell only. Other client shells
stay open; remote workers will run there automatically.

```erlang
Clients = [
    'client1@192.168.1.105',
    'client2@192.168.1.106',
    'client3@192.168.1.107',
    'client4@192.168.1.108'
].
Rings = [
    'ring1@192.168.1.101',
    'ring2@192.168.1.102',
    'ring3@192.168.1.103',
    'ring4@192.168.1.104'
].

%% Every remote connection should return pong.
[{VM, net_adm:ping(VM)} || VM <- Clients ++ Rings, VM =/= node()].

[Ring1, Ring2, Ring3, Ring4] = Rings.
E1 = {entry, Ring1}.
E2 = {entry, Ring2}.
E3 = {entry, Ring3}.
E4 = {entry, Ring4}.
```

Resolve pang results before testing. VM names must match the actual startup
names; cookie and distribution connectivity must be consistent.

The following helper removes previous benchmark entry processes and creates
fresh empty stores. Reserve the registered name entry for this experiment.

```erlang
Setup = fun(Specs) ->
    lists:foreach(fun(VM) ->
        case rpc:call(VM, erlang, whereis, [entry]) of
            undefined -> ok;
            Old when is_pid(Old) -> exit(Old, kill);
            Other -> error({cannot_reset_ring, VM, Other})
        end
    end, Rings),
    timer:sleep(1000),

    [{FirstVM, FirstId} | Rest] = Specs,
    FirstPid = rpc:call(FirstVM, node2, start, [FirstId]),
    true = is_pid(FirstPid),
    true = rpc:call(FirstVM, erlang, register, [entry, FirstPid]),
    Others = [
        begin
            Pid = rpc:call(VM, node2, start, [Id, FirstPid]),
            true = is_pid(Pid),
            true = rpc:call(VM, erlang, register, [entry, Pid]),
            Pid
        end || {VM, Id} <- Rest
    ],
    timer:sleep(5000),
    FirstPid ! probe,
    [FirstPid | Others]
end.
```

Setup uses actual remote PIDs to connect storage nodes. The probe output
appears on the first storage machine. Check the expected member count before
proceeding; fixed waiting alone is not proof of convergence.

## 4. Run the five experiments on client1

Run each Setup first, check its probe output, then run the benchmark.
Count is the number of requests PER client.

```erlang
%% 1 node, four clients, 4000 requests, common contact
Setup([{Ring1, 250000000}]).
M1 = multibench:run(Clients, [E1, E1, E1, E1], 1000).

%% 2 nodes, four clients, 4000 requests, common contact
Setup([{Ring1, 250000000}, {Ring2, 750000000}]).
M2 = multibench:run(Clients, [E1, E1, E1, E1], 1000).

%% 2 nodes, four clients, 4000 requests, distributed contacts
Setup([{Ring1, 250000000}, {Ring2, 750000000}]).
M3 = multibench:run(Clients, [E1, E1, E2, E2], 1000).

%% 4 nodes, four clients, 4000 requests, distributed contacts
Setup([{Ring1, 250000000}, {Ring2, 500000000},
       {Ring3, 750000000}, {Ring4, 1000000000}]).
M4 = multibench:run(Clients, [E1, E2, E3, E4], 1000).

%% 4 nodes, four clients, 10000 requests, distributed contacts
Setup([{Ring1, 250000000}, {Ring2, 500000000},
       {Ring3, 750000000}, {Ring4, 1000000000}]).
M5 = multibench:run(Clients, [E1, E2, E3, E4], 2500).
```

Display concise table rows:

```erlang
[{maps:get(total_requests, R),
  maps:get(add_ms, R), maps:get(lookup_ms, R),
  maps:get(add_errors, R), maps:get(lookup_errors, R)}
 || R <- [M1, M2, M3, M4, M5]].
```

Each tuple contains total requests, add milliseconds, lookup milliseconds,
add errors, and lookup errors. The summary also includes per-client timings,
timeouts, missing values, and incorrect values. Inspect details with:

```erlang
maps:get(clients, M4).
```

The coordinator times each complete phase using its own monotonic clock; clock
synchronization between machines is unnecessary. This includes dispatching
commands and collecting completion reports. Every client still sends requests
sequentially, while all four clients operate concurrently.

Repeat each configuration from empty stores at least three times and report
the median and range. Keep multi-machine results separate from the previous
single-VM measurements. Deterministic keys can collide; reported totals are
request counts, not a claim of globally unique entries.

## 5. Timeouts and cleanup

Default per-request timeout: 5000 ms. Default per-phase deadline: 120000 ms.
For a slower environment, explicitly change and record them:

```erlang
Options = #{request_timeout => 10000, batch_timeout => 300000}.
multibench:run(Clients, [E1, E2, E3, E4], 2500, Options).
```

An unreachable client, missing module, worker failure, or phase timeout raises
an error. Such a run is incomplete and should not be recorded as normal
performance. The coordinator cleans up the workers when it returns or fails.

After all experiments, stop the benchmark ring processes from client1:

```erlang
[case rpc:call(VM, erlang, whereis, [entry]) of
     Pid when is_pid(Pid) -> exit(Pid, kill);
     undefined -> ok;
     Other -> {error, VM, Other}
 end || VM <- Rings].
```

Exit the individual shells with q(). The files have been inspected but not
compiled or executed in the report-writing environment, where Erlang was not
available.
