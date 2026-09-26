-module(gms2).
-export([start/1,start/2]).
-define(arghh,1000).
-define(timeout, 1000).

start(Id) ->
    Rnd = rand:uniform(1000),
    Self = self(),
    {ok, spawn_link(fun()-> init(Id,Rnd, Self) end)}.

start(Id, Grp) ->
    Rnd = rand:uniform(1000),
    Self = self(),
    {ok, spawn_link(fun()-> init(Id,Rnd, Grp, Self) end)}.


init(Id,Rnd, Master) ->
    rand:seed(exsss,{Rnd, Rnd, Rnd}),
    leader(Id, Master, [], [Master]).

init(Id,Rnd, Grp, Master) ->
    rand:seed(exsss,{Rnd, Rnd, Rnd}),
    Self = self(),
    Grp ! {join, Master, Self},
    receive
        {view, [Leader|Slaves], Group} ->
            Master ! {view, Group},
            Ref = erlang:monitor(process, Leader),
            slave(Id, Master, Leader, Slaves, Group,Ref)
        after ?timeout ->
            Master ! {error, "no reply from leader"}
    end.


bcast(Id, Msg, Nodes) ->
    lists:foreach(fun(Node) -> Node ! Msg, crash(Id) end, Nodes).

crash(Id) ->
    case rand:uniform(?arghh) of
    ?arghh ->
        io:format("leader ~w: crash~n", [Id]),
        exit(no_luck);
    _ ->
        ok
    end.


leader(Id, Master, Slaves, Group) ->
    receive
        {mcast, Msg} ->
            bcast(Id, {msg, Msg}, Slaves),
            Master ! Msg,
            leader(Id, Master, Slaves, Group);
        {join, Wrk, Peer} ->
            Slaves2 = lists:append(Slaves, [Peer]),
            Group2 = lists:append(Group, [Wrk]),
            bcast(Id, {view, [self()|Slaves2], Group2}, Slaves2),
            Master ! {view, Group2},
            leader(Id, Master, Slaves2, Group2);
        stop ->
            ok
    end.


slave(Id, Master, Leader, Slaves, Group,Ref) ->
    receive
        {mcast, Msg} ->
            Leader ! {mcast, Msg},
            slave(Id, Master, Leader, Slaves, Group,Ref);
        {join, Wrk, Peer} ->
            Leader ! {join, Wrk, Peer},
            slave(Id, Master, Leader, Slaves, Group,Ref);
        {msg, Msg} ->
            Master ! Msg,
            slave(Id, Master, Leader, Slaves, Group,Ref);
        {view, [Leader|Slaves2], Group2} ->
            erlang:demonitor(Ref, [flush]),
            RefNew = erlang:monitor(process, Leader),
            Master ! {view, Group2},
            slave(Id, Master, Leader, Slaves2, Group2,RefNew);
        {'DOWN', Ref, process, Leader, _Reason} ->
            election(Id,Master,Slaves,Group);
        stop ->
        ok
    end.

election(Id, Master, Slaves, [_|Group]) ->
    Self = self(),
    case Slaves of
        [Self|Rest] ->
            bcast(Id, {view, Slaves, Group}, Rest),
            Master ! {view, Group},
            leader(Id, Master, Rest, Group);
        [Leader|Rest] ->
            Ref = erlang:monitor(process, Leader),    
            slave(Id, Master, Leader, Rest, Group,Ref)
    end.
