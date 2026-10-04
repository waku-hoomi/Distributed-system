-module(node1).
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
    schedule_stabilize(),
    node(Id, Predecessor, Successor).

connect(Id, nil) ->
    {ok, {Id, self()}};
connect(_Id, Peer) ->
    Qref = make_ref(),
    Peer ! {key, Qref, self()},
    receive
        {Qref, Skey} ->
            {ok, {Skey, Peer}}
        after ?Timeout ->
            io:format("Time out: no response~n",[])
    end.

create_probe(Id, {_, Spid}) ->
    Time = erlang:monotonic_time(microsecond),
    Spid ! {probe, Id, [self()], Time}.

remove_probe(Time, Nodes) ->
    Now = erlang:monotonic_time(microsecond),
    Elapsed = Now - Time,
    io:format("Probe returned in ~p us. Ring nodes count: ~p~nNodes: ~p~n", 
              [Elapsed, length(Nodes), Nodes]).

forward_probe(Ref, Time, Nodes, _Id, {_, Spid}) ->
    Spid ! {probe, Ref, Nodes ++ [self()], Time}.

node(Id, Predecessor, Successor) ->
    receive
        probe ->
            create_probe(Id, Successor),
            node(Id, Predecessor, Successor);

        {probe, Id, Nodes, Time} ->
            remove_probe(Time, Nodes),
            node(Id, Predecessor, Successor);

        {probe, Ref, Nodes, Time} ->
            forward_probe(Ref, Time, Nodes, Id, Successor),
            node(Id, Predecessor, Successor);
        stabilize ->
            stabilize(Successor),
            node(Id, Predecessor, Successor);

        {key, Qref, Peer} ->
            Peer ! {Qref, Id},
            node(Id, Predecessor, Successor);

        {notify, New} ->
            Pred = notify(New, Id, Predecessor),
            node(Id, Pred, Successor);

        {request, Peer} ->
            request(Peer, Predecessor),
            node(Id, Predecessor, Successor);

        {status, Pred} ->
            Succ = stabilize(Pred, Id, Successor),
            node(Id, Predecessor, Succ)
    end.

stabilize(Pred, Id, Successor) ->
    {Skey, Spid} = Successor,
    case Pred of
        nil ->
            Spid ! {notify, {Id, self()}},
            Successor;
        {Id, _} ->
            Successor;
        {Skey, _} ->
            Spid ! {notify, {Id, self()}},
            Successor;
        {Xkey, Xpid} ->
            case key:between(Xkey, Id, Skey) of
                true ->
                    Xpid ! {notify, {Id,self()}},
                    {Xkey, Xpid};
                false ->
                    Spid ! {notify, {Id, self()}},
                    Successor
            end
    end.

schedule_stabilize() ->
    timer:send_interval(?Stabilize, self(), stabilize).

stabilize({_, Spid}) ->
    Spid ! {request, self()}.


request(Peer, Predecessor) ->
    case Predecessor of
        nil ->
            Peer ! {status, nil};
        {Pkey, Ppid} ->
            Peer ! {status, {Pkey, Ppid}}
    end.

notify({Nkey, Npid}, Id, Predecessor) ->
    case Predecessor of
        nil ->
            {Nkey, Npid};
        {Pkey, _} ->
            case key:between(Nkey, Pkey, Id) of
                true ->
                    {Nkey, Npid};
                false ->
                    Predecessor
        end
    end.
