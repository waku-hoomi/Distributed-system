-module(dijkstra).
-export([update/4,table/2,route/2]).

entry(Node,Sorted) ->
    case lists:keyfind(Node, 1, Sorted) of
        false -> 0;
        {_, Length, _} -> Length
    end.

replace(Node,N,Gateway,Sorted) ->
    Rest = lists:keydelete(Node, 1, Sorted),
    insert({Node,N,Gateway},Rest).

insert(Entry = {_, _, _}, []) -> 
    [Entry];
insert(Entry = {_, N, _}, [H = {_, HN, _} | T]) when N =< HN -> 
    [Entry, H | T];
insert(Entry, [H | T]) -> 
    [H | insert(Entry, T)].



update(Node,N,Gateway,Sorted) ->
    Length = entry(Node,Sorted),
    if
        Length >0, N<Length ->
            replace(Node,N,Gateway,Sorted);
        true ->
            Sorted
    end.
 
    
table(Gateways,Map) ->
    AllNodes = map:all_nodes(Map),
    NonGateways = lists:filter(fun(Node) -> not lists:member(Node,Gateways) end, AllNodes),
    GatewayEntries = lists:map(fun(Node) -> {Node,0,Node} end, Gateways),
    NodeEntries = lists:map(fun(Node) -> {Node,inf,unknown} end, NonGateways),
    Sorted = GatewayEntries ++ NodeEntries,
    iterate(Sorted,Map,[]).

iterate(Sorted,Map,Table) -> 
    case Sorted of
        [] -> Table;
        [{_,inf,_} | _] -> Table;
        [{Node,Length,Gateway} | Rest] ->
            Links = map:reachable(Node,Map),
            Updated = lists:foldl(fun(Neighbor,Acc) ->
                update(Neighbor,Length + 1,Gateway,Acc)
            end, Rest, Links),
            iterate(Updated,Map,[{Node,Gateway} | Table])
    end.
            
route(Node,Table) ->
    case lists:keyfind(Node, 1, Table) of
        false -> notfound;
        {_,Gateway} -> {ok,Gateway}
    end.

