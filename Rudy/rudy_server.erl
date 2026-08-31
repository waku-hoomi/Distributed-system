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
        {error, _Error} ->
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
        {error, _Error} ->
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


