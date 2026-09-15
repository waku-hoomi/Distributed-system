-module(time).
-export([zero/0,inc/2,merge/2,leq/2,clock/1,update/3,safe/2]).

zero() ->
    [].

inc(Name,Vector) ->
    case lists:keyfind(Name,1,Vector) of
        {Name,Val} ->
            lists:keyreplace(Name,1,Vector,{Name,Val+1});
        false ->
            [{Name,1}|Vector]
    end.


merge([],V2) -> V2;
merge(V1,[]) -> V1;
merge(V1,V2) ->
    Merged = lists:foldl(
        fun({Node,Val1},Acc)->
            case lists:keyfind(Node,1,Acc) of
                {Node,Val2}->
                    lists:keyreplace(Node,1,Acc,{Node,max(Val1,Val2)});
                false ->
                    [{Node,Val1}|Acc]
            end
        end,
        V2,
        V1
    ),
    Merged.

leq([],_v2) -> true;
leq([{Node,Val1}|Rest1],V2) ->
    Val2 = case lists:keyfind(Node,1,V2) of
        {Node,V} -> V;
        false -> 0
    end,
    if
        Val1 =< Val2 -> leq(Rest1,V2);
        true -> false
    end.


clock(Nodes) ->
    lists:map(fun(Node) -> {Node,zero()} end, Nodes).

update(Node,Time,Clock) ->
    lists:keystore(Node, 1, Clock, {Node,Time}).

safe(Time,Clock) ->
    Times = [T || {_,T} <- Clock],
    MinTime = lists:min(Times),
    leq(Time,MinTime).
