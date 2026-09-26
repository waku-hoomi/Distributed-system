-module(gms3).
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
    leader(Id, Master,1, [], [Master]).

init(Id,Rnd, Grp, Master) ->
    rand:seed(exsss,{Rnd, Rnd, Rnd}),
    Self = self(),
    Grp ! {join, Master, Self},
    receive
        {view,N, [Leader|Slaves], Group} ->
            Master ! {view, Group},
            Ref = erlang:monitor(process, Leader),
            Last = {view,N, [Leader|Slaves], Group},
            slave(Id, Master, Leader,N+1,Last, Slaves, Group,Ref)
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


leader(Id, Master,N, Slaves, Group) ->
    receive
        {mcast, Msg} ->
            bcast(Id, {msg, N, Msg}, Slaves),
            Master ! Msg,
            leader(Id, Master, N+1, Slaves, Group);
        {join, Wrk, Peer} ->
            Slaves2 = lists:append(Slaves, [Peer]),
            Group2 = lists:append(Group, [Wrk]),
            bcast(Id, {view, N, [self()|Slaves2], Group2}, Slaves2),
            Master ! {view, Group2},
            leader(Id, Master,N+1, Slaves2, Group2);
        stop ->
            ok
    end.


slave(Id, Master, Leader, N , Last , Slaves, Group,Ref) ->
    receive
        {mcast, Msg} ->
            Leader ! {mcast, Msg},
            slave(Id, Master, Leader, N, Last, Slaves, Group,Ref);
        {join, Wrk, Peer} ->
            Leader ! {join, Wrk, Peer},
            slave(Id, Master, Leader, N, Last, Slaves, Group,Ref);
        {msg, I, _} when I < N ->
            slave(Id, Master, Leader, N, Last, Slaves, Group, Ref);
        {view, I, _, _} when I < N ->
            slave(Id, Master, Leader, N, Last, Slaves, Group, Ref);
        {msg, N, Msg} ->
            Master ! Msg,
            NewLast = {msg, N, Msg},
            slave(Id, Master, Leader, N+1, NewLast, Slaves, Group,Ref);
        {view, N, [Leader|Slaves2], Group2} ->
            erlang:demonitor(Ref, [flush]),
            RefNew = erlang:monitor(process, Leader),
            Master ! {view, Group2},
            NewLast = {view, N, [Leader|Slaves2], Group2},
            slave(Id, Master, Leader, N+1, NewLast, Slaves2, Group2,RefNew);
        {'DOWN', Ref, process, Leader, _Reason} ->
            election(Id,Master, N, Last, Slaves,Group);
        stop ->
        ok
    end.

election(Id, Master, N, Last, Slaves, [_|Group]) ->
    Self = self(),
    case Slaves of
        [Self|Rest] ->
            bcast(Id, Last, Rest),
            Master ! {view, Group},
            leader(Id, Master,N, Rest, Group);
        [Leader|Rest] ->
            Ref = erlang:monitor(process, Leader),    
            slave(Id, Master, Leader, N, Last, Rest, Group,Ref)
    end.
