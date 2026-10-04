-module(node3).
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
    Next = nil,
    schedule_stabilize(),
    node(Id, Predecessor, Successor,Next,Store).

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

node(Id, Predecessor, Successor,Next,Store) ->
    receive
        probe ->
            create_probe(Id, Successor),
            node(Id, Predecessor, Successor, Next, Store);

        {probe, Id, Nodes, Time} ->
            remove_probe(Time, Nodes),
            node(Id, Predecessor, Successor, Next, Store);

        {probe, Ref, Nodes, Time} ->
            forward_probe(Ref, Time, Nodes, Id, Successor),
            node(Id, Predecessor, Successor, Next, Store);

        {'DOWN', Ref, process, _, _} ->
            {Pred, Succ, Nxt} = down(Ref, Predecessor, Successor, Next),
            node(Id, Pred, Succ, Nxt, Store);


        {add, Key, Value, Qref, Client} ->
            Added = add(Key, Value, Qref, Client, Id, Predecessor, Successor, Store),
            node(Id, Predecessor, Successor, Next, Added);

        {lookup, Key, Qref, Client} ->
            lookup(Key, Qref, Client, Id, Predecessor, Successor, Store),
            node(Id, Predecessor, Successor, Next, Store);

        {handover, Elements} ->
            Merged = storage:merge(Elements,Store),
            node(Id, Predecessor, Successor, Next, Merged);

        stabilize ->
            stabilize(Successor),
            node(Id, Predecessor, Successor, Next, Store);

        {key, Qref, Peer} ->
            Peer ! {Qref, Id},
            node(Id, Predecessor, Successor, Next, Store);

        {notify, New} ->
            {Pred,Keep} = notify(New, Id, Predecessor,Store),
            node(Id, Pred, Successor, Next, Keep);

        {request, Peer} ->
            request(Peer, Predecessor,Successor),
            node(Id, Predecessor, Successor,Next, Store);

        {status, Pred, Nx} ->
            {Succ,Nxt} = stabilize(Pred,Nx, Id, Successor),
            node(Id, Predecessor, Succ, Nxt, Store)
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

notify({Nkey, Npid}, Id, Predecessor,Store) ->
    case Predecessor of
        nil ->
            Nref = monitor(Npid),
            Keep = handover(Id,Store, Nkey, Npid),
            {{Nkey, Nref, Npid}, Keep};
        {Pkey, Pref, _Ppid} ->
            case key:between(Nkey, Pkey, Id) of
                true ->
                    drop(Pref),
                    Nref = monitor(Npid),
                    Keep = handover(Id,Store, Nkey, Npid),
                    {{Nkey, Nref, Npid}, Keep};
                false ->
                    {predecessor, Store}
        end
    end.

handover(Id, Store, Nkey, Npid) ->
    {Rest,Keep} = storage:split(Id, Nkey, Store),
    Npid ! {handover, Rest},
    Keep.


add(Key, Value, Qref, Client, Id, {Pkey, _, _}, { _, _, Spid}, Store) ->
    case key:between(Key,Pkey,Id) of
        true ->
            Client ! {Qref, ok},
            storage:add(Key, Value, Store);
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

down(Ref, {_, Ref, _}, Successor, Next) ->
    {nil, Successor, Next};
down(Ref, Predecessor, {_, Ref, _}, {Nkey, Npid}) ->
    Nref = monitor(Npid),
    self() ! stabilize,
    {Predecessor, {Nkey, Nref, Npid}, nil}.