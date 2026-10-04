-module(storage).
-export([create/0,add/3,lookup/2,split/3,merge/2]).

create() ->
    [].

add(Key, Value, Store) ->
    lists:keystore(Key, 1, Store, {Key, Value}). 

lookup(Key, Store) ->
    case lists:keyfind(Key, 1, Store) of
        {Key, Value} ->
            {Key, Value};
        false ->
            false
    end.

split(From, To, Store) ->
    lists:partition(
        fun({Key, _Value}) ->
            key:between(Key, From, To)
        end,
        Store
    ).

merge(Entries, Store) ->
    lists:foldl(fun({Key, Value}, Acc) -> add(Key, Value, Acc) end, Store, Entries).