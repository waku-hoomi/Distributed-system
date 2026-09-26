-module(gms4).
-export([start/1, start/2]).
-define(arghh, 1000).
-define(timeout, 1000).
-define(history_limit, 100).

start(Id) ->
    Rnd = rand:uniform(1000),
    Self = self(),
    {ok, spawn_link(fun() -> init(Id, Rnd, Self) end)}.

start(Id, Grp) ->
    Rnd = rand:uniform(1000),
    Self = self(),
    {ok, spawn_link(fun() -> init(Id, Rnd, Grp, Self) end)}.

init(Id, Rnd, Master) ->
    rand:seed(exsss, {Rnd, Rnd, Rnd}),
    N = 1,
    History = [],
    leader(Id, Master, N, [], [Master], History).

init(Id, Rnd, Grp, Master) ->
    rand:seed(exsss, {Rnd, Rnd, Rnd}),
    Self = self(),
    Grp ! {join, Master, Self},
    receive
        {view, N, [Leader|Slaves], Group} ->
            Master ! {view, Group},
            Ref = erlang:monitor(process, Leader),
            Last = {view, N, [Leader|Slaves], Group},
            Pending = maps:new(),
            slave(Id, Master, Leader, N + 1, Last, Slaves, Group, Ref, Pending)
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

add_to_history(Msg, History) ->
    lists:sublist([Msg | History], ?history_limit).

leader(Id, Master, N, Slaves, Group, History) ->
    receive
        {mcast, Msg} ->
            MsgTag = {msg, N, Msg},
            bcast(Id, MsgTag, Slaves),
            Master ! Msg,
            NewHistory = add_to_history(MsgTag, History),
            leader(Id, Master, N + 1, Slaves, Group, NewHistory);
            
        {join, Wrk, Peer} ->
            Slaves2 = lists:append(Slaves, [Peer]),
            Group2 = lists:append(Group, [Wrk]),
            ViewTag = {view, N, [self()|Slaves2], Group2},
            bcast(Id, ViewTag, Slaves2),
            Master ! {view, Group2},
            NewHistory = add_to_history(ViewTag, History),
            leader(Id, Master, N + 1, Slaves2, Group2, NewHistory);
            
        {request_resend, ReqN, Peer} ->
            case lists:keyfind(ReqN, 2, History) of
                false ->
                    ok;
                FoundMsg ->
                    Peer ! FoundMsg
            end,
            leader(Id, Master, N, Slaves, Group, History);
            
        stop ->
            ok
    end.

slave(Id, Master, Leader, N, Last, Slaves, Group, Ref, Pending) ->
    receive
        {mcast, Msg} ->
            Leader ! {mcast, Msg},
            slave(Id, Master, Leader, N, Last, Slaves, Group, Ref, Pending);
            
        {join, Wrk, Peer} ->
            Leader ! {join, Wrk, Peer},
            slave(Id, Master, Leader, N, Last, Slaves, Group, Ref, Pending);

        {msg, I, _} when I < N ->
            slave(Id, Master, Leader, N, Last, Slaves, Group, Ref, Pending);
        {view, I, _, _} when I < N ->
            slave(Id, Master, Leader, N, Last, Slaves, Group, Ref, Pending);

        {msg, N, Msg} ->
            Master ! Msg,
            NewLast = {msg, N, Msg},
            {NextN, FinalLast, NewPending} = flush_pending(N + 1, NewLast, Master, Pending),
            slave(Id, Master, Leader, NextN, FinalLast, Slaves, Group, Ref, NewPending);

        {view, N, [Leader|Slaves2], Group2} ->
            erlang:demonitor(Ref, [flush]),
            RefNew = erlang:monitor(process, Leader),
            Master ! {view, Group2},
            NewLast = {view, N, [Leader|Slaves2], Group2},
            {NextN, FinalLast, NewPending} = flush_pending(N + 1, NewLast, Master, Pending),
            slave(Id, Master, Leader, NextN, FinalLast, Slaves2, Group2, RefNew, NewPending);

        MsgTag = {msg, I, _} when I > N ->
            Leader ! {request_resend, N, self()},
            NewPending = maps:put(I, MsgTag, Pending),
            slave(Id, Master, Leader, N, Last, Slaves, Group, Ref, NewPending);

        ViewTag = {view, I, _, _} when I > N ->
            Leader ! {request_resend, N, self()},
            NewPending = maps:put(I, ViewTag, Pending),
            slave(Id, Master, Leader, N, Last, Slaves, Group, Ref, NewPending);

        {'DOWN', Ref, process, Leader, _Reason} ->
            election(Id, Master, N, Last, Slaves, Group, [Last]);
            
        stop ->
            ok
    end.


flush_pending(N, Last, Master, Pending) ->
    case maps:find(N, Pending) of
        {ok, {msg, N, Msg}} ->
            Master ! Msg,
            NewLast = {msg, N, Msg},
            NewPending = maps:remove(N, Pending),
            flush_pending(N + 1, NewLast, Master, NewPending);
            
        {ok, {view, N, [Leader|Slaves2], Group2}} ->
            Master ! {view, Group2},
            NewLast = {view, N, [Leader|Slaves2], Group2},
            NewPending = maps:remove(N, Pending),
            flush_pending(N + 1, NewLast, Master, NewPending);
            
        error ->
            {N, Last, Pending}
    end.

election(Id, Master, N, Last, Slaves, [_|Group], History) ->
    Self = self(),
    case Slaves of
        [Self|Rest] ->
            bcast(Id, Last, Rest),
            Master ! {view, Group},
            leader(Id, Master, N, Rest, Group, History);
            
        [Leader|Rest] ->
            Ref = erlang:monitor(process, Leader),
            Pending = maps:new(),
            slave(Id, Master, Leader, N, Last, Rest, Group, Ref, Pending)
    end.