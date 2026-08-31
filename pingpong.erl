-module(pingpong).
-export([start_pong/0, start_ping/1, pong_loop/0, ping_loop/1]).

%% ==================== Pong (接收方) ====================
start_pong() ->
    Pid = spawn(?MODULE, pong_loop, []),
    register(pong_server, Pid),
    io:format("Pong server started and registered as 'pong_server' (~p)~n", [Pid]).

pong_loop() ->
    receive
        {ping, FromPid, Msg} ->
            io:format("Pong received: ~p from ~p~n", [Msg, FromPid]),
            %% 使用拿到对方的 FromPid 进行直接回复（不需要知道对方节点名）
            FromPid ! {pong, "Hello from Pong!"},
            pong_loop();
        stop ->
            io:format("Pong server stopping.~n")
    end.

%% ==================== Ping (发起方) ====================
start_ping(RemoteNode) ->
    spawn(?MODULE, ping_loop, [RemoteNode]).

ping_loop(RemoteNode) ->
    MyPid = self(), %% 获取当前进程本身的真实 Pid
    io:format("Ping sending message from ~p to node ~p~n", [MyPid, RemoteNode]),
    
    %% 第一步：向远程节点的注册名 pong_server 发送自己的 MyPid
    {pong_server, RemoteNode} ! {ping, MyPid, "Hello from Ping!"},
    
    %% 第二步：挂起等待对方通过 MyPid 给自己回复
    receive
        {pong, Reply} ->
            io:format("Ping got reply: ~p~n", [Reply])
    after 5000 ->
        io:format("Ping timeout waiting for reply!~n")
    end.