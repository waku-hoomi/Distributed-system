-module(routy).
-export([start/2, stop/1,status/1]).

start(Reg, Name) ->
    register(Reg, spawn(fun() -> init(Name) end)).

stop(Node) ->
    Node ! stop,
    unregister(Node).

init(Name) ->
    Intf = intf:new(),
    Map = map:new(),
    Gateways = intf:list(Intf),
    Table = dijkstra:table(Gateways, Map),
    Hist = hist:new(Name),
    router(Name, 0, Hist, Intf, Table, Map).

router(Name, N, Hist, Intf, Table, Map) ->
    receive
        {add, Node, Pid} ->
            Ref = erlang:monitor(process,Pid),
            Intf1 = intf:add(Node, Ref, Pid, Intf),
            Message = {links, Name, N, intf:list(Intf1)},
            intf:broadcast(Message, Intf1),
            Table1 = dijkstra:table(intf:list(Intf1), Map),
            router(Name, N+1, Hist, Intf1, Table1, Map);
        {remove, Node} ->
            {ok, Ref} = intf:ref(Node, Intf),
            erlang:demonitor(Ref),
            Intf1 = intf:remove(Node, Intf),
            Message = {links, Name, N, intf:list(Intf1)},
            intf:broadcast(Message, Intf1),
            Table1 = dijkstra:table(intf:list(Intf1), Map),
            router(Name, N+1, Hist, Intf1, Table1, Map);
        {'DOWN', Ref, process, _, _} ->
            {ok, Down} = intf:name(Ref, Intf),
            io:format("~w: exit recived from ~w~n", [Name, Down]),
            Intf1 = intf:remove(Down, Intf),
            Message = {links, Name, N, intf:list(Intf1)},
            intf:broadcast(Message, Intf1),
            Table1 = dijkstra:table(intf:list(Intf1), Map),
            router(Name, N + 1, Hist, Intf1, Table1, Map);
        {links, Node, R, Links} ->
            case hist:update(Node, R, Hist) of
                {new, Hist1} ->
                    Message = {links, Node, R, Links},
                    intf:broadcast(Message, Intf),
                    Map1 = map:update(Node, Links, Map),
                    Table1 = dijkstra:table(intf:list(Intf), Map1),
                    router(Name, N, Hist1, Intf, Table1, Map1);
                old ->
                    router(Name, N, Hist, Intf, Table, Map)
            end;
        {status, From} ->
            From ! {status, {Name, N, Hist, Intf, Table, Map}},
            router(Name, N, Hist, Intf, Table, Map);
        {route, To, _From, Message} when To =:= Name ->
            io:format("~w: received message ~w ~n", [Name, Message]),
            router(Name, N, Hist, Intf, Table, Map);
        {route, To, From, Message} ->
            io:format("~w: routing message (~w)", [Name, Message]),
            case dijkstra:route(To, Table) of
                {ok, Gw} ->
                    case intf:lookup(Gw, Intf) of
                        {ok, Pid} ->
                            Pid ! {route, To, From, Message};
                        notfound ->
                            ok
                    end;
                notfound ->
                ok
            end,
            router(Name, N, Hist, Intf, Table, Map);
        {send, To, Message} ->
            self() ! {route, To, Name, Message},
            router(Name, N, Hist, Intf, Table, Map);
        stop ->
            ok
    end.

status(Router) ->
    Router ! {status, self()},
    receive
        {status, {Name, N, Hist, Intf, Table, Map}} ->
            io:format("~n=== Router Status: ~w ===~n", [Name]),
            io:format("Msg Counter: ~p~n", [N]),
            io:format("History    : ~p~n", [Hist]),
            io:format("Interfaces : ~p~n", [Intf]),
            io:format("Table      : ~p~n", [Table]),
            io:format("Map        : ~p~n", [Map]),
            io:format("==========================~n")
    end.

