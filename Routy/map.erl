-module(map).
-export([new/0,update/3,reachable/2,all_nodes/1]).

new() ->
    [].

update(Node, Links, Map) ->
    CleanMap = lists:keydelete(Node, 1, Map),
    [{Node, Links} | CleanMap].

reachable(Node, Map) ->
    case lists:keyfind(Node, 1, Map) of
        {_, Links} -> Links;
        false -> []
    end.

all_nodes(Map) ->
    Allnode = lists:foldl(fun({Node, _Links}, Acc) -> [Node | _Links] ++ Acc end, [], Map),
    lists:usort(Allnode).