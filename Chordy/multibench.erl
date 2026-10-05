-module(multibench).
-export([run/3, run/4, worker/6]).

%% Run this on a named VM. Clients contains four VM names; Contacts
%% contains one ring PID or {RegisteredName, Node} address per client.
run(Clients, Contacts, Count) ->
    run(Clients, Contacts, Count, #{}).

run(Clients, Contacts, Count, Options)
  when is_list(Clients), is_list(Contacts),
       is_integer(Count), Count > 0, is_map(Options) ->
    4 = length(Clients),
    4 = length(Contacts),
    4 = length(lists:usort(Clients)),
    RequestTimeout = maps:get(request_timeout, Options, 5000),
    BatchTimeout = maps:get(batch_timeout, Options, 120000),
    true = is_integer(RequestTimeout) andalso RequestTimeout > 0,
    true = is_integer(BatchTimeout) andalso BatchTimeout > 0,
    lists:foreach(fun check_client/1, Clients),
    Parent = self(),
    Run = make_ref(),
    Workers = [
        begin
            {Pid, Monitor} = spawn_monitor(
                Client, ?MODULE, worker,
                [Parent, Run, Contact, Count, Index, RequestTimeout]),
            {Index, Client, Pid, Monitor}
        end
        || {Index, {Client, Contact}} <-
               lists:zip(lists:seq(1, 4), lists:zip(Clients, Contacts))
    ],
    try
        Ready = gather(ready, Run, Workers, 30000),
        {AddUs, AddReports} = phase(add, add_done, Run, Workers, BatchTimeout),
        {LookupUs, LookupReports} =
            phase(lookup, lookup_done, Run, Workers, BatchTimeout),
        AddErrors = sum_field(errors, AddReports),
        LookupErrors = sum_field(errors, LookupReports),
        Total = 4 * Count,
        Result = #{
            total_requests => Total,
            add_ms => AddUs / 1000,
            lookup_ms => LookupUs / 1000,
            add_errors => AddErrors,
            lookup_errors => LookupErrors,
            add_success_per_second => rate(Total - AddErrors, AddUs),
            lookup_success_per_second => rate(Total - LookupErrors, LookupUs),
            unique_keys_per_client =>
                [maps:get(unique_keys, maps:get(P, Ready))
                 || {_, _, P, _} <- Workers],
            clients => [
                #{vm => Client, contact => Contact,
                  add => maps:get(Pid, AddReports),
                  lookup => maps:get(Pid, LookupReports)}
                || {{_, Client, Pid, _}, Contact} <-
                       lists:zip(Workers, Contacts)
            ]
        },
        io:format("total=~p, add=~.3f ms, lookup=~.3f ms, errors=~p/~p~n",
                  [Total, AddUs / 1000, LookupUs / 1000,
                   AddErrors, LookupErrors]),
        Result
    after
        lists:foreach(fun({_, _, Pid, Monitor}) ->
            exit(Pid, kill),
            erlang:demonitor(Monitor, [flush])
        end, Workers)
    end.

check_client(Client) ->
    case Client =:= node() orelse net_adm:ping(Client) =:= pong of
        false -> error({client_unreachable, Client});
        true ->
            case rpc:call(Client, code, ensure_loaded, [?MODULE]) of
                {module, ?MODULE} -> ok;
                Other -> error({client_module_unavailable, Client, Other})
            end
    end.

phase(Command, ReplyTag, Run, Workers, Timeout) ->
    timer:tc(fun() ->
        lists:foreach(fun({_, _, Pid, _}) ->
            Pid ! {Command, Run}
        end, Workers),
        gather(ReplyTag, Run, Workers, Timeout)
    end).

gather(Tag, Run, Workers, Timeout) ->
    Pending = maps:from_list([{Pid, Monitor} || {_, _, Pid, Monitor} <- Workers]),
    Deadline = erlang:monotonic_time(millisecond) + Timeout,
    gather(Tag, Run, Pending, #{}, Deadline).

gather(_Tag, _Run, Pending, Reports, _Deadline) when map_size(Pending) =:= 0 ->
    Reports;
gather(Tag, Run, Pending, Reports, Deadline) ->
    Remaining = erlang:max(0, Deadline - erlang:monotonic_time(millisecond)),
    receive
        {Tag, Run, Pid, Report} when is_map_key(Pid, Pending) ->
            gather(Tag, Run, maps:remove(Pid, Pending),
                   maps:put(Pid, Report, Reports), Deadline);
        {'DOWN', Monitor, process, Pid, Reason} when is_map_key(Pid, Pending) ->
            case maps:get(Pid, Pending) of
                Monitor -> error({worker_failed, node(Pid), Reason});
                _ -> gather(Tag, Run, Pending, Reports, Deadline)
            end
    after Remaining ->
        error({phase_timeout, Tag, maps:keys(Pending)})
    end.

sum_field(Field, Reports) ->
    lists:sum([maps:get(Field, Report) || Report <- maps:values(Reports)]).

rate(Success, Us) -> Success * 1000000 / erlang:max(Us, 1).

worker(Parent, Run, Contact, Count, Index, Timeout) ->
    ParentMonitor = erlang:monitor(process, Parent),
    rand:seed(exsplus, {Index, Index + 100, Index + 200}),
    %% Generate keys here rather than calling the supplied key:generate/0.
    Keys = [rand:uniform(1000000000) || _ <- lists:seq(1, Count)],
    Parent ! {ready, Run, self(), #{unique_keys => length(lists:usort(Keys))}},
    wait_for(add, Run, ParentMonitor),
    {AddUs, AddCounts} = timer:tc(fun() ->
        lists:foldl(fun(Key, Counts) ->
            Qref = make_ref(),
            Contact ! {add, Key, gurka, Qref, self()},
            Outcome = reply(Qref, ParentMonitor, Timeout),
            count_add(Outcome, Counts)
        end, #{errors => 0, timeouts => 0}, Keys)
    end),
    Parent ! {add_done, Run, self(), AddCounts#{elapsed_us => AddUs}},
    wait_for(lookup, Run, ParentMonitor),
    {LookupUs, LookupCounts} = timer:tc(fun() ->
        lists:foldl(fun(Key, Counts) ->
            Qref = make_ref(),
            Contact ! {lookup, Key, Qref, self()},
            Outcome = reply(Qref, ParentMonitor, Timeout),
            count_lookup(Key, Outcome, Counts)
        end, #{errors => 0, timeouts => 0, missing => 0, wrong => 0}, Keys)
    end),
    Parent ! {lookup_done, Run, self(), LookupCounts#{elapsed_us => LookupUs}},
    %% Remain alive until the coordinator cleans up, avoiding normal DOWN
    %% events racing with collection of the final reports.
    receive
        {'DOWN', ParentMonitor, process, _, _} -> ok
    end.

wait_for(Command, Run, ParentMonitor) ->
    receive
        {Command, Run} -> ok;
        {'DOWN', ParentMonitor, process, _, Reason} -> exit({coordinator_down, Reason})
    end.

reply(Qref, ParentMonitor, Timeout) ->
    receive
        {Qref, Value} -> {reply, Value};
        {'DOWN', ParentMonitor, process, _, Reason} -> exit({coordinator_down, Reason})
    after Timeout ->
        timeout
    end.

count_add({reply, ok}, Counts) -> Counts;
count_add(timeout, Counts) -> increment(timeouts, increment(errors, Counts));
count_add(_, Counts) -> increment(errors, Counts).

count_lookup(Key, {reply, {Key, gurka}}, Counts) -> Counts;
count_lookup(_Key, timeout, Counts) -> increment(timeouts, increment(errors, Counts));
count_lookup(_Key, {reply, false}, Counts) -> increment(missing, increment(errors, Counts));
count_lookup(_Key, _, Counts) -> increment(wrong, increment(errors, Counts)).

increment(Field, Counts) -> maps:update_with(Field, fun(N) -> N + 1 end, Counts).
