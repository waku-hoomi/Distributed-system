-module(my_logger).
-export([start/1, stop/1]).

start(Nodes) ->
    spawn_link(fun() -> init(Nodes) end).

stop(Logger) ->
    Logger ! stop.

init(Nodes) ->
    Clock = time:clock(Nodes),
    Queue = [],
    loop(Clock,Queue).

loop(Clock,Queue) ->
    receive
        {log,From,Time,Msg} ->
            NewClock = time:update(From,Time,Clock),
            NewQueue = [{From,Time,Msg}|Queue],
            RemainingQueue = flush(NewClock,NewQueue),
            loop(NewClock,RemainingQueue);
        stop ->
            SortedQueue = lists:sort(fun({_,T1,_},{_,T2,_}) -> time:leq(T1,T2) end,Queue),
            lists:foreach(fun({From,Time,Msg}) -> log(From,Time,Msg) end,SortedQueue),
            ok
    end.


log(From,Time,Msg) ->
    io:format("log: ~w ~w ~p~n", [Time, From,Msg]).

flush(Clock,Queue) ->
    {SafeMsgs, UnsafeMsgs} = lists:partition(fun({_,Time,_}) -> time:safe(Time,Clock) end,Queue),
    SortedSafe = lists:sort(fun({_,T1,_},{_,T2,_}) -> time:leq(T1,T2) end,SafeMsgs),
    lists:foreach(fun({From,Time,Msg}) -> log(From,Time,Msg) end,SortedSafe),
    UnsafeMsgs.