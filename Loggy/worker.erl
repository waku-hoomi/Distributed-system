-module(worker).
-export([start/5, stop/1, peers/2]).

start(Name, Logger, Seed, Sleep, Jitter) ->
    spawn_link(fun() -> init(Name, Logger, Seed, Sleep, Jitter) end).
stop(Worker) ->
    Worker ! stop.
init(Name, Log, Seed, Sleep, Jitter) ->
    rand:seed(exsss,{Seed, Seed, Seed}),
    Local_Time = time:zero(),
    receive
        {peers, Peers} ->
            loop(Name, Log, Peers, Sleep, Jitter, Local_Time);
        stop ->
            ok
    end.

peers(Wrk, Peers) ->
    Wrk ! {peers, Peers}.

loop(Name, Log, Peers, Sleep, Jitter,Local_Time)->
    Wait = rand:uniform(Sleep),
    receive
        {msg, Time, Msg} ->
            Merged_Time = time:merge(Time, Local_Time),
            NewTime = time:inc(Name,Merged_Time),
            Log ! {log, Name, NewTime, {received, Msg}},
            loop(Name, Log, Peers, Sleep, Jitter, NewTime);
        stop ->
            ok;
        Error ->
            Log ! {log, Name, time:inc(Name,Local_Time), {error, Error}}
        after Wait ->
            NewTime = time:inc(Name,Local_Time),
            Selected = select(Peers),
            Message = {hello, rand:uniform(100)},
            Selected ! {msg, NewTime, Message},
            jitter(Jitter),
            Log ! {log, Name, NewTime, {sending, Message}},
            loop(Name, Log, Peers, Sleep, Jitter, NewTime)
    end.

select(Peers) ->
    lists:nth(rand:uniform(length(Peers)), Peers).
jitter(0) -> ok;
jitter(Jitter) -> timer:sleep(rand:uniform(Jitter)).