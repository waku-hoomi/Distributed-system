-module(hist).
-export([new/1,update/3]).

new(Name) ->
    [{Name,inf}].
update(Node,N,History) ->
    case lists:keyfind(Node,1,History) of
        false -> 
            UpdatedHistory = [{Node,N}|History],
            {new,UpdatedHistory};
        {_,OldN} when N > OldN ->
            UpdatedHistory = lists:keyreplace(Node,1,History,{Node,N}),
            {new,UpdatedHistory};
        {_,_OldN} ->
            old
    end.
            