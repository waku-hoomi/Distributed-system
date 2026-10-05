-module(node4).
-export([start/1,start/2]).
-define(Stabilize, 1000).
-define(Timeout, 10000).

start(Id) ->
    start(Id, nil).

start(Id, Peer) ->
    timer:start(),
    spawn(fun() -> init(Id, Peer) end).

init(Id, Peer) ->
    Predecessor = nil,
    {ok, Successor} = connect(Id, Peer),
    Store = storage:create(),
    Replica = storage:create(),
    Next = nil,
    schedule_stabilize(),
    node(Id, Predecessor, Successor,Next,Store,Replica).

connect(Id, nil) ->
    Ref = monitor(self()),
    {ok, {Id, Ref, self()}};
connect(_Id, Peer) ->
    Qref = make_ref(),
    Peer ! {key, Qref, self()},
    receive
        {Qref, Skey} ->
            Ref = monitor(Peer),
            {ok, {Skey, Ref, Peer}}
        after ?Timeout ->
            io:format("Time out: no response~n",[])
    end.

monitor(Pid) ->
    erlang:monitor(process, Pid).

drop(nil) ->
    ok;
drop(Ref) ->
    erlang:demonitor(Ref, [flush]).

create_probe(Id, {_, _, Spid}) ->
    Time = erlang:monotonic_time(microsecond),
    Spid ! {probe, Id, [self()], Time}.

remove_probe(Time, Nodes) ->
    Now = erlang:monotonic_time(microsecond),
    Elapsed = Now - Time,
    io:format("Probe returned in ~p us. Ring nodes count: ~p~nNodes: ~p~n", 
              [Elapsed, length(Nodes), Nodes]).

forward_probe(Ref, Time, Nodes, _Id, {_, _, Spid}) ->
    Spid ! {probe, Ref, Nodes ++ [self()], Time}.

node(Id, Predecessor, Successor,Next,Store,Replica) ->
    receive
        probe ->
            create_probe(Id, Successor),
            node(Id, Predecessor, Successor, Next, Store, Replica);

        {probe, Id, Nodes, Time} ->
            remove_probe(Time, Nodes),
            node(Id, Predecessor, Successor, Next, Store, Replica);

        {probe, Ref, Nodes, Time} ->
            forward_probe(Ref, Time, Nodes, Id, Successor),
            node(Id, Predecessor, Successor, Next, Store, Replica);

        {'DOWN', Ref, process, _, _} ->
            {Pred, Succ, Nxt,Nstore,Nreplica} = down(Ref, Predecessor, Successor, Next, Id, Store, Replica),
            node(Id, Pred, Succ, Nxt, Nstore, Nreplica);
        
        {recover, DeadKey, {Nkey, Npid}, Entries} ->
            case Predecessor of
                {DeadKey, Pref, _} ->
                    Merged = storage:merge(Replica, Store),
                    drop(Pref),
                    Nref = monitor(Npid),

                    {_, _, Spid} = Successor,
                    Spid ! {replica_replace, Merged},

                    Npid ! {recovered, self()},
                    node(Id, {Nkey, Nref, Npid}, Successor,
                        Next, Merged, Entries);

                nil ->
                    Nref = monitor(Npid),
                    Npid ! {recovered, self()},
                    node(Id, {Nkey, Nref, Npid}, Successor,
                        Next, Store, Entries);

                _ ->
                    node(Id, Predecessor, Successor,
                        Next, Store, Replica)
            end;

        {recovered, Peer} ->
            case Successor of
                {_, _, Peer} ->
                    self() ! stabilize;
                _ ->
                    ok
            end,
            node(Id, Predecessor, Successor, Next, Store, Replica);

        {replica_replace, Entries} ->
            node(Id, Predecessor, Successor, Next, Store, Entries);

        {replicate, Key, Value, Qref, Client} ->
            Replicated = storage:add(Key, Value, Replica),
            Client ! {Qref, ok},
            node(Id, Predecessor, Successor, Next, Store, Replicated);

        {add, Key, Value, Qref, Client} ->
            Added = add(Key, Value, Qref, Client, Id, Predecessor, Successor, Store),
            node(Id, Predecessor, Successor, Next, Added,Replica);

        {lookup, Key, Qref, Client} ->
            lookup(Key, Qref, Client, Id, Predecessor, Successor, Store),
            node(Id, Predecessor, Successor, Next, Store, Replica);

        {handover, Elements} ->
            Merged = storage:merge(Elements,Store),
            node(Id, Predecessor, Successor, Next, Merged, Replica);

        stabilize ->
            stabilize(Successor),
            node(Id, Predecessor, Successor, Next, Store, Replica);

        {key, Qref, Peer} ->
            Peer ! {Qref, Id},
            node(Id, Predecessor, Successor, Next, Store, Replica);

        {notify, New} ->
            {Pred,Keep,Nreplica} = notify(New, Id, Predecessor,Store,Replica),
            {_, _, Spid} = Successor,
            Spid ! {replica_replace, Keep},
            node(Id, Pred, Successor, Next, Keep, Nreplica);

        {request, Peer} ->
            request(Peer, Predecessor,Successor),
            node(Id, Predecessor, Successor,Next, Store, Replica);

        {status, Pred, Nx} ->
            {Succ,Nxt} = stabilize(Pred,Nx, Id, Successor),
            node(Id, Predecessor, Succ, Nxt, Store, Replica)
    end.

stabilize(Pred,Nx, Id, Successor) ->
    {Skey, Sref, Spid} = Successor,
    case Pred of
        nil ->
            Spid ! {notify, {Id, self()}},
            {Successor,Nx};
        {Id, _} ->
            {Successor,Nx};
        {Skey, _} ->
            Spid ! {notify, {Id, self()}},
            {Successor,Nx};
        {Xkey, Xpid} ->
            case key:between(Xkey, Id, Skey) of
                true ->
                    drop(Sref),
                    Xref = monitor(Xpid),
                    Xpid ! {notify, {Id,self()}},
                    {{Xkey, Xref, Xpid},{Skey,Spid}};
                false ->
                    Spid ! {notify, {Id, self()}},
                    {Successor,Nx}
            end
    end.

schedule_stabilize() ->
    timer:send_interval(?Stabilize, self(), stabilize).

stabilize({_, _, Spid}) ->
    Spid ! {request, self()}.


request(Peer, Predecessor, {Skey, _Sref, Spid}) ->
    Pred =
        case Predecessor of
            nil ->
                nil;
            {Pkey, _Pref, Ppid} ->
                {Pkey, Ppid}
        end,
    Peer ! {status, Pred, {Skey, Spid}}.

notify({Nkey, Npid}, Id, Predecessor,Store,Replica) ->
    case Predecessor of
        nil ->
            Nref = monitor(Npid),
            {Rest,Keep} = handover(Id,Store, Nkey, Npid,Replica),
            {{Nkey, Nref, Npid}, Keep,Rest};
        {Pkey, Pref, _Ppid} ->
            case key:between(Nkey, Pkey, Id) of
                true ->
                    drop(Pref),
                    Nref = monitor(Npid),
                    {Rest,Keep} = handover(Id,Store, Nkey, Npid,Replica),
                    {{Nkey, Nref, Npid}, Keep,Rest};
                false ->
                    {Predecessor, Store, Replica}
        end
    end.

handover(Id, Store, Nkey, Npid,Replica) ->
    {Rest,Keep} = storage:split(Id, Nkey, Store),
    Npid ! {replica_replace, Replica},
    Npid ! {handover, Rest},
    {Rest,Keep}.


add(Key, Value, Qref, Client, Id, {Pkey, _, _}, { _, _, Spid}, Store) ->
    case key:between(Key,Pkey,Id) of
        true ->
            Added = storage:add(Key, Value, Store),
            Spid ! {replicate, Key, Value, Qref, Client},
            Added;
        false ->
            Spid ! {add, Key, Value, Qref, Client},
            Store
    end.

lookup(Key, Qref, Client, Id, {Pkey, _ , _}, {_,_,Spid}, Store) ->
    case key:between(Key,Pkey,Id) of
        true ->
            Result = storage:lookup(Key, Store),
            Client ! {Qref, Result};
        false ->
            Spid ! {lookup, Key, Qref, Client}
    end.

down(Ref, {_, Ref, _}, Successor, Next, _Id, Store, Replica) ->
    Merged = storage:merge( Replica,Store),
    {_, _, Spid} = Successor,
    Spid ! {replica_replace, Merged},
    {nil, Successor, Next,Merged, storage:create()};
down(Ref, Predecessor, {Skey, Ref, _}, {Nkey, Npid}, Id, Store, Replica) ->
    Nref = monitor(Npid),
    Npid ! {recover, Skey, {Id, self()}, Store},
    {Predecessor, {Nkey, Nref, Npid}, nil,Store,Replica}.