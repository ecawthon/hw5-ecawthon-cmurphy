%% @author Eleanor Cawthon & Claire Murphy
%% @doc
%% Distributed Systems Assignment 5:
%% Distributed, Fault-Tolerant Key-Value Storage.

-module(key_value_node).

%% ==========================================================================
%% API functions
%% ==========================================================================
-export([main/1, node_monitor/3, storage_join/2, backup/3, is_node/1, 
        storage_serve/2, storage_serve/3]).
-define(DEBUG, 0).

main(Params) ->
    try
        M = list_to_integer(hd(Params)),
        MyName = hd(tl(Params)),
        _ = os:cmd("epmd -daemon"),
        NodeName = [list_to_atom(MyName), shortnames],
        net_kernel:start(NodeName),
        debug(1, "My self: ~p. Node name: ~p.~n",
            [self(), node()]),
        OtherNodes = tl(tl(Params)),
        print("new node: Entering join...~n"),
        node_join(M, OtherNodes)
    catch
        Anything -> print("Error, ~p. exiting.~n",[Anything])
    end,
    debug(1, "(main) Leaving now"),
    erlang:halt().

%%===========================================================================
%% Node Process States
%%===========================================================================

%% The node process's joining state: initialize the necessary storage 
%% processes,
%% M: Power of total storage processes in the system (NumProcesses = 2^M)
%% OtherNode: The other node which node_join was given to connect to. Note 
%% that
%% this is an erlang node, not a node process.
node_join(M, OtherNodes) ->
    print("(node_join) Entered node_join(~p, ~p)~n", [M, OtherNodes]),
    case OtherNodes of
        [] ->
            glocally_register_name('Node0', self()),
            % we're first--tell storage processes to go straight to serve
            node_spawn_and_register_processes(storage_serve,
                lists:seq(0, trunc(math:pow(2, M)) - 1), M),
            print("(node_join) Entering (node_monitor)~n"),
            node_monitor(0, 0, M);
        [OtherNode] ->
            net_kernel:connect(list_to_atom(OtherNode)),
            global:sync(),
            Registered = global:registered_names(),
            NodeNames = lists:filter(fun(X) -> is_node(X) end, Registered),
            global_send(hd(NodeNames), {join_request, self()}), 
            receive
                {join_ack, MyNum, SuccNum, Range} ->
                    print("(node_join) got {~p, ~p, ~p, ~p}~n",
                        [join_ack, MyNum, SuccNum, Range]),
                    glocally_register_name(node_name(MyNum), self()), 
                    global:sync(),
                    SuccName = node_name(SuccNum),
                    debug(3, "(node_join) taking responsibility~n"),
                    % not first--take responsibility for some existing 
                    % processes
                    node_take_responsibility(MyNum, Range, M), % won't finish
                    % until we have all maps
                    Node = node(global:whereis_name(SuccName)), 
                    monitor_node(Node, true),
                    print("(node_join) Entering node_monitor(~p, ~p, ~p)~n", 
                        [MyNum, SuccNum, M]),
                    node_monitor(MyNum, SuccNum, M)
            end
    end.

%% The node process's neutral state: waiting and responding to messages
%% MyNum: My node number
%% SuccNum: My successor's number
%% NumProcesses: Total processes in the system
node_monitor(MyNum, SuccNum, M) ->
    NumProcesses = trunc(math:pow(2,M)),
    debug(1, "(node_monitor) Entered node_monitor.~n" ++
        "\tMy processes:~p~n"++
        "\tGlobal names:~p~n", 
        [node_get_my_processes(), global:registered_names()]),
    SuccName = node_name(SuccNum),
    receive
        {nodedown, _} ->
            print("(node_monitor) Got {~p, ~p}~n", 
                [nodedown, SuccName]),
            node_rebalance(MyNum, SuccNum, M);
        {take_processes, RecipientNum, Range} ->
            print("(node_monitor) Got {~p, ~p, ~p}~n",
                [take_processes, RecipientNum, Range]),
            case RecipientNum of
                MyNum ->
                    NewSuccNum = node_get_num(node_get_successor(MyNum)),
                    node_take_responsibility(MyNum, Range, M),
                    debug(2, "(node_monitor) Done taking responsibility.~n"),
                    node_send_to_node_proc(MyNum, NewSuccNum, 
                        {monitoring}, M),
                    node_monitor(MyNum, NewSuccNum, M);
                _ ->
                    node_send_to_node_proc(MyNum, RecipientNum, 
                        {take_processes, RecipientNum, Range}, M),
                    node_monitor(MyNum, SuccNum, M)
            end;
        %% There are two kinds of join requests because the first node that 
        %% receives the join request from the outside has to calculate the 
        %% eventual successor/predecessor anyway, and forwarding it saves a 
        %% computation.
        {join_request, Pid} ->
            print("(node_monitor) Got {~p, ~p}~n", [join_request, Pid]),
            {ItsPredNum, ItsNum, ItsSuccNum} = node_pick_num(NumProcesses),
            node_handle_join(Pid, MyNum, SuccNum, M, ItsPredNum, ItsNum,
                ItsSuccNum);
        {join_request, Pid, ItsPredNum, ItsNum, ItsSuccNum} ->
            print("(node_monitor) Got {~p, ~p, ~p, ~p, ~p}~n",
                [join_request, Pid, ItsPredNum, ItsNum, ItsSuccNum]),
            node_handle_join(Pid, MyNum, SuccNum, M, ItsPredNum, ItsNum,
                ItsSuccNum);
        {Pid, Ref, leave} ->
            print("(node_monitor) Got {~p, ~p, ~p}~n", [Pid, Ref, leave]),
            node_leave();
        {Pid, Ref, node_list, NodeList} ->
            print("(node_monitor) Got {~p, ~p, ~p, ~p}~n",
                [Pid, Ref, node_list, NodeList]),
            MyName = node_name(MyNum),
            case lists:any(fun(X) -> X == MyName end, NodeList) of
                true ->
                    Pid ! {Ref, result, NodeList},
                    print("Sent {~p, ~p, ~p} to ~p~n",
                        [Ref, result, NodeList, Pid]);
                false ->
                    Message = {Pid, Ref, node_list, [MyName|NodeList]},
                    global_send(SuccName, Message)
            end,
            node_monitor(MyNum, SuccNum, M);
        Message -> 
            print("Unexpected message received in node_monitor: ~p~n",
                [Message]),
            node_monitor(MyNum, SuccName, M)
    end.

%% The node process's rebalancing state: After a node dies
%% MyNum: My node number
%% DepartedNum: The number of the node that died
%% NumProcesses: Total storage processes in the system. (2^M)
node_rebalance(MyNum, DepartedNum, M) ->
    print("(node_rebalance) Entered node_rebalance(~p, ~p, ~p)~n",
        [MyNum, DepartedNum, M]),
    NumProcesses = trunc(math:pow(2,M)),
    global:sync(),
    NewSuccessor = node_get_num(node_get_successor(DepartedNum)),
    %% Monitor our NewSucessor now.
    SNode = node(global:whereis_name(node_name(NewSuccessor))),
    monitor_node(SNode, true),
    NewRange = node_get_responsibilities(DepartedNum, NewSuccessor, 
        NumProcesses),
    OldRange = node_get_responsibilities(MyNum, DepartedNum, NumProcesses),
    case node_can_take_all_processes(NewRange ++ OldRange, NumProcesses) of
        true ->
            node_take_responsibility(MyNum, NewRange, M), 
            print("(node_rebalance) Entering (node_monitor)~n"),
            node_monitor(MyNum, NewSuccessor, M);
        false ->
            MyNewNum = get_legal_num(lists:last(NewRange), 
                (NumProcesses / 2) + 1, NumProcesses),
            MyBackupNums = node_get_my_backup_proc_nums(),
            {BackupsToSwitch, ToRequest} = lists:partition(
                fun(X) -> lists:any(fun(Y) -> Y == X end, MyBackupNums) end,
                NewRange),
            PrimariesToSwitch = lists:map(fun(X) -> 
                        backup_get_num(storage_assoc_backup_name(X, M)) end,
                BackupsToSwitch),
            debug(2, "(node_rebalance) MyBackupNums:~p NewRange:~p"++
                "BackupsToSwitch:~p, ToReauest:~p~n",
                [MyBackupNums, NewRange, BackupsToSwitch, ToRequest]),
            node_switch_processes(PrimariesToSwitch, M),
            debug(2, "(node_rebalance) Switched: ~p. My procs: ~p~n",
                [PrimariesToSwitch, node_get_my_processes()]),
            glocally_unregister_name(node_name(MyNum)),
            glocally_register_name(node_name(MyNewNum), self()),
            Recipient = node_get_predecessor(MyNum),
            debug(1,"(node_rebalance) Re-registered as ~p~n",
                [node_name(MyNewNum)]),
            node_send_to_node_proc(MyNewNum, node_get_num(Recipient),
                {take_processes, 
                    node_get_num(Recipient), PrimariesToSwitch}, M),
            node_wait_for_monitoring(MyNewNum, M),
            node_take_responsibility(MyNewNum, ToRequest, M),
            debug(2, "(node_rebalance) Took ~p. MyProcs: ~p~n",
                [ToRequest, node_get_my_processes()]),
            print("(node_rebalance) Entering (node_monitor)~n"),
            node_monitor(MyNewNum, NewSuccessor, M)
    end.

node_wait_for_monitoring(MyNum, M) ->
    receive
        {monitoring} ->
            print("(node_rebalance) Got {monitoring}~n");
        {take_processes, RecipientNum, Range} ->
            print("(node_rebalance) Got {~p, ~p, ~p}~n",
                [take_processes, RecipientNum, Range]),
            case RecipientNum of
                MyNum ->
                    ok;
                _ ->
                    node_send_to_node_proc(MyNum, RecipientNum, 
                        {take_processes, RecipientNum, Range}, M)
            end,
            node_wait_for_monitoring(MyNum, M)
    end.


node_leave() ->
    print("(node_leave) Entered node_leave()~n"),
    erlang:disconnect_node(node()).
%% ===========================================================================
%% Storage Process States
%% ===========================================================================

%% The joining state ofa storage process. It requests maps, and then calls
%% storage_await_maps.
%% The first node should call serve directly.
%% MyNum: the number of this storage process
%% M: where 2^M is the total number of storage processes.
storage_join(MyNum, M) ->
    global:sync(),
    print("(storage_join) Entered storage_join(~p, ~p)~n", [MyNum, M]),
    %MyName = storage_name(MyNum),
    BackupToSpawnName = storage_assoc_backup_name(MyNum, M),
    BackupToConsult = backup_name(MyNum),
    StorageToConsult = storage_name(backup_get_num(BackupToSpawnName)),
    debug(2,"(storage_join) nodes:~p global procs:~p~n", [nodes(), 
            global:registered_names()]),
    print("(storage_join) sending {~p, request_map} to ~p~n", 
        [self(), BackupToConsult]),
    global_send(BackupToConsult, {self(), request_map}),
    global_send(StorageToConsult, {self(), request_backup}),
    storage_await_maps(MyNum, none, none, M).

%% Waits until both maps needed by a new process arrive, then switches to
%% storate_serve state.
%% MyNum: the number ofthis storage process
%% Map: the map I use to service requests
%% BackupMap: the map I use to backup my neighbor
%% M: where 2^M is the total number of storage processes.
storage_await_maps(MyNum, Map, BackupMap, M) ->
    debug(1,"(await_maps) Entered storage_await_maps(~p, ~p, ~p, ~p)~n",
        [MyNum, Map, BackupMap, M]),
    global:sync(),
    receive
        {map, NewMap} ->
            %% I came from your backup process
            print("(storage_await_maps) got {map, ~p}~n", [NewMap]),
            case BackupMap of
                none ->
                    storage_await_maps(MyNum, NewMap, BackupMap, M);
                _ ->
                    NodeName = storage_get_expected_node_name_of(MyNum),
                    global_send(NodeName, {serving, MyNum}),
                    storage_serve(NewMap, MyNum, M)
            end;
        {backup, NewBackupMap} ->
            print("(storage_await_maps) got {~p, ~p}~n", [backup, NewBackupMap]),
            BackupName = storage_assoc_backup_name(MyNum, M),
            debug(1,"(storage_join) spawning and registering ~p~n",
                [BackupName]),
            glocally_register_name(BackupName, spawn(key_value_node, backup, 
                    [backup_get_num(BackupName), NewBackupMap, M])),
            debug(1,"(storage_join) spawned and registered ~p~n",
                [BackupName]),
            case Map of
                none ->
                    storage_await_maps(MyNum, Map, NewBackupMap, M);
                _ ->
                    NodeName = storage_get_expected_node_name_of(MyNum),
                    global_send(NodeName, {serving, MyNum}),
                    storage_serve(Map, MyNum, M)
            end
    end.

%% The main state of a storage process; responds to messages.
%% MyNum: The identification number of this storage process
%% Map: A tuple list of {Key, Value}
%% M: Where there are 2^M storage processes.
storage_serve(MyNum, M) ->
    print("(storage_serve) Entered storage_serve.~n"),
    storage_serve([], MyNum, M).
storage_serve(Map, MyNum, M) ->
    debug(1, "(storage_serve) Waiting for messages.~n"
        ++"\tMy map:~p.~n\tGlobal names:~p~n",
        [Map, global:registered_names()]),
    receive
    Message ->
        print("(storage_serve) Got ~p.~n", [Message]),
        case Message of

    %%%%%% Outside world messages %%%%%%
        %%%%%%% Key storage/retrieval %%%%%%%%
        {Pid, Ref, store, Key, Value} ->
            ProcNum = hash(Key, M),
            debug(1, "(storage_serve ~p) ~p key goes on ~p. I'll...~n", 
                [MyNum, Key, ProcNum]),
            case ProcNum of
                MyNum ->
                    debug(1, "Reply to it!~n"),
                    OldValue =
                    case lists:keyfind(Key, 1, Map) of
                        {Key, Val} -> Val;
                        false -> no_value
                    end,
                    NewMap = lists:keystore(Key, 1, Map, {Key, Value}),
                    global_send(backup_name(MyNum), {self(), Ref, 
                            write_backup, Key, Value}),
                    receive
                        {bACKup, Ref} ->
                            print("(storage_serve) Got {~p, ~p}~n", [bACKup, 
                                    Ref])
                    end,
                    Pid ! {Ref, stored, OldValue},
                    print("(storage_serve) Sent {~p, ~p, ~p}~n",
                        [Ref, stored, OldValue]),
                    storage_serve(NewMap, MyNum, M);
                Other ->
                    Forward = storage_get_forwarding_neighbor(Other, MyNum, M),
                    debug(1,"Forward it to ~p!~n", [Forward]),
                    global_send(storage_name(Forward) , {Pid, Ref, store, 
                            Key, Value}),
                    storage_serve(Map, MyNum, M)
            end;
        {Pid, Ref, retrieve, Key} ->
            ProcNum = hash(Key, M),
            case ProcNum of
                MyNum ->
                    Value = case lists:keyfind(Key, 1, Map) of
                        {Key, Val} -> Val;
                        false -> no_value
                    end,
                    Pid ! {Ref, retrieved, Value},
                    print("(storage_serve) Sent {~p, ~p, ~p}~n", [Ref, 
                            retrieved, Value]);
                Other ->
                    Forward = storage_get_forwarding_neighbor(Other, MyNum, M),
                    global_send(storage_name(Forward), 
                        {Pid, Ref, retrieve, Key})
            end,
            storage_serve(Map, MyNum, M);

        %%%%%%% Snapshot-like requests %%%%%%%%
        {Pid, Ref, first_key} ->
            KeyList = lists:map(fun(X) -> element(1, X) end, Map),
            storage_get_requested_info(MyNum, Map, 
                {Pid, Ref, first_key, MyNum, lists:min(KeyList)}),
            storage_serve(Map, MyNum, M);
        {Pid, Ref, last_key} ->
            KeyList = lists:map(fun(X) -> element(1, X) end, Map),
            storage_get_requested_info(MyNum, Map, 
                {Pid, Ref, last_key, MyNum, lists:max(KeyList)}),
            storage_serve(Map, MyNum, M);
        {Pid, Ref, num_keys} ->
            storage_get_requested_info(MyNum, Map,
                {Pid, Ref, num_keys, MyNum, 0}),
            storage_serve(Map, MyNum, M);
        {Pid, Ref, Atom, Starter, Running} ->
            storage_get_requested_info(MyNum, Map,
                        {Pid, Ref, Atom, Starter, Running}),
            storage_serve(Map, MyNum, M);

        %%%%%%%% Forward to my node process %%%%%%
        {Pid, Ref, node_list} ->
            MyNode = storage_get_expected_node_name_of(MyNum),
            Message = {Pid, Ref, node_list, []},
            global_send(MyNode, Message),
            storage_serve(Map, MyNum, M);
        {Pid, Ref, leave} ->
            %% Tell my node to leave.
            global_send(storage_get_expected_node_name_of(MyNum), 
                {Pid, Ref, leave}),
            storage_serve(Map, MyNum, M);

    %%%%%%%% Internal messages
        {Pid, request_backup} ->
            %% Someone is trying to restore my backup copy.
            Pid ! {backup, Map},
            print("sent {backup, ~p} to ~p~n", [Map, Pid]),
            storage_serve(Map, MyNum, M);
        {SwitcherPid, switch} ->
            NewName = backup_name(MyNum),
            glocally_unregister_name(storage_name(MyNum)),
            glocally_register_name(NewName, self()),
            SwitcherPid ! {switched, MyNum},
            print("Sent {switched, ~p} to ~p~n", [MyNum, SwitcherPid]),
            backup(MyNum, Map, M);
        _ ->
            storage_serve(Map, MyNum, M)
    end
end.

%% ===========================================================================
%% Backup Process States
%% ===========================================================================
backup(MyNum, Map, M) ->
    print("(backup) Entered backup(~p, ~p)~n", [MyNum, Map]),
    receive
        {Pid, Ref, write_backup, Key, Value} ->
            print("(backup) Received {~p, ~p, ~p, ~p, ~p}~n",
                [Pid, Ref, write_backup, Key, Value]),
            NewMap = lists:keystore(Key, 1, Map, {Key, Value}),
            Pid ! {bACKup, Ref},
            print("(backup) Sent {~p, ~p} to ~p~n", [bACKup, Ref, Pid]),
            backup(MyNum, NewMap, M);
        {Pid, request_map} ->
            print("(backup) Received {~p, ~p}~n",
                 [Pid, request_map]),
            %% Got request from the process you back up
            Pid ! {map, Map},
            print("(backup) Sent {~p, ~p} to ~p~n", [map, Map, Pid]),
            backup(MyNum, Map, M);
        {SwitcherPid, switch} ->
            print("(backup) Received {~p, ~p}~n", [SwitcherPid, switch]),
            glocally_unregister_name(backup_name(MyNum)),
            glocally_register_name(storage_name(MyNum), self()),
            SwitcherPid ! {switched, MyNum},
            print("(backup) Sent {switched, ~p} to ~p~n", 
                [MyNum, SwitcherPid]),
            print("(backup) Entering (storage_serve)~n"),
            storage_serve(Map, MyNum, M);
        Message ->
            print("(backup) Got unexpected Message: ~p~n",[Message]),
            backup(MyNum, Map, M)
    end.

%%============================================================================
%% Helper functions
%%============================================================================

%% Key-Value-Related Functions
%% ---------------------------------------------------------------------------

hash(Key, M) ->
    debug(1,"Hashing key ~p~n", [Key]),
    erlang:phash(Key, trunc(math:pow(2,M)))-1.           


%% Messaging-Related 
%% Functions 
%% ---------------------------------------------------------------------------

%% Send Message to the process with number 1 higher (mod 2^M) than SenderNum.
%% This is legal because the next process num is always a neighbor.
storage_send_to_next_process(SenderNum, Message) ->
    StorNums = lists:sort(lists:filter(fun(X) -> is_node(X) end, 
            global:registered_names())),
    Name = storage_name(get_next_num(SenderNum, StorNums)),
    global_send(Name, Message).

%% Given my node name, an intended node recipient (both node nums), and a 
%% Message, send the message on an acceptable legal path to the recipient.
node_send_to_node_proc(SenderNum, RecipientNum, Message, M) ->
    debug(1,"(send_to_node) Routing message from nodes ~p to ~p:~n\t~p~n", 
        [SenderNum, RecipientNum, Message]),
    NumProcesses = trunc(math:pow(2,M)),
    %% New plan: Take the difference mod 2, forward to that node.
    Diff = abs(RecipientNum - SenderNum),
    Rem = Diff rem NumProcesses,
    debug(1, "Diff:~p. Rem:~p.~n", [Diff, Rem]),
    Ten = math:log(Rem),
    Raw = Ten / math:log(2),
    Steps = trunc(Ten / math:log(2)) + 1,
    debug(1,"Ten:~p, Raw:~p, Steps: ~p~n", [Ten, Raw, Steps]),
    TargetProc = (Steps + SenderNum) rem NumProcesses,
    Target = storage_get_expected_node_name_of(TargetProc),
    debug(1,"TargetProc:~p Target:~p case? ~p~n", 
        [TargetProc, node_get_num(Target), node_get_num(Target) == SenderNum]),
    case node_get_num(Target) == SenderNum of
        true -> node_send_to_node_proc(TargetProc, RecipientNum, Message, M);
        false -> global_send(Target, Message)
    end.

%% Given an intended recipient number and a list of neighbor numbers, which
%% neighbor should I send to?
storage_get_forwarding_neighbor(RecipientNum, SenderNum, M)->
    Neighbors = [SenderNum | storage_get_neighbors(SenderNum, M)],
    debug(3,"Entering storage_get_forwarding_neighbor(~p, ~p, ~p)~n",
        [RecipientNum, SenderNum, Neighbors]),
    Neighbor = case lists:any(fun(Y) -> Y == RecipientNum end, Neighbors) of
        true ->
            RecipientNum; % If we can send directly to RecipientNum...
        false ->
            % Otherwise, find the closest neighbor to RecipientNum.
            Lower = lists:sort(lists:filter(fun(X) -> X < RecipientNum end, 
                    Neighbors)),
            case Lower of
                [] -> lists:last(Neighbors);
                _-> hd(Lower)
            end
        end,
    debug(2, "(get_forward) Recipient:~p Neighbors: ~p, Neighbor:~p~n",
        [RecipientNum, Neighbors, Neighbor]),
    Neighbor.

%% Joining-Related Functions
%% ---------------------------------------------------------------------------

node_handle_join(Pid, MyNum, MySuccNum, M, ItsPredNum, ItsNum, ItsSuccNum) ->
    NumProcesses = trunc(math:pow(2,M)),
    debug(1, "(node_handle_join) Entering "
            ++ "node_handle_join(~p, ~p, ~p, ~p, ~p, ~p, ~p)~n",
        [Pid, MyNum, MySuccNum, M, ItsPredNum, ItsNum, ItsSuccNum]),
    case ItsPredNum of
        MyNum ->
            global:sync(),
            debug(1,"(node_handle_join) I'm its predecessor!~n"),
            Range = node_get_responsibilities(ItsNum, 
                ItsSuccNum, NumProcesses),
            debug(3, "Got responsibilities~n"),
            node_kill_processes(Range, M),
            debug(3, "(node_handle_join) Killed processes ~p~n", [Range]),
            Pid ! {join_ack, ItsNum, ItsSuccNum, Range},
            print("(node_handle_join) Sent {~p, ~p, ~p, ~p} to Pid ~p~n",
                [join_ack, ItsNum, ItsSuccNum, Range, Pid]),
            %% erlang:demonitor(process, global:whereis(node_name(MySuccNum)))
            ItsName = node_name(ItsNum),
            debug(3, "(node_handle_join) Globally Registered names: ~n~p~n", 
                [global:registered_names()]),
            debug(3, "(node_handle_join) I want to monitor ~p at Pid: ~p~n",
                [ItsName, global:whereis_name(ItsName)]),
            % TODO: unmonitor your old one. USE PID!!!!!!! IT'S PASSED!
            OldNode = node(global:whereis_name(node_name(MySuccNum))),
            NewNode = node(Pid),
            monitor_node(OldNode, false),
            monitor_node(NewNode, true),
            print("(node_handle_join) Entering (node_monitor)~n"),
            node_monitor(MyNum, ItsNum, M)
            ;
        _ ->
            debug(2, "(node_handle_join) ItsPredNum: ~p, MyNum: ~p~n",
                [ItsPredNum, MyNum]),
            node_send_to_node_proc(MyNum, ItsPredNum,
                {join_request, Pid, ItsPredNum, ItsNum, ItsSuccNum}, M),
            print(
                "(node_handle_join) Sent {join_request, ~p, ~p, ~p, ~p} to ~p~n",
                [Pid, ItsPredNum, ItsNum, ItsSuccNum, ItsPredNum]),
            print("(node_handle_join) Entering (node_monitor)~n"),
            node_monitor(MyNum, MySuccNum, M)
    end.

%% Find the biggest split between node names.
%% Returns tuple of {PrevNodeNum, Split}
node_find_biggest_split(TakenNums, NumProcesses) ->
    debug(1, "Entering node_find_biggest_split(~p,~p)~n",
        [TakenNums, NumProcesses]),
    Wrap = hd(TakenNums)+NumProcesses-(lists:last(TakenNums)),
    debug(3, "(node_find_biggest_split) calling recursive with~p, ~p, ~p~n",
        [TakenNums, Wrap, lists:last(TakenNums)]),
    biggest_split_recursive(TakenNums, Wrap, 
        lists:last(TakenNums)).
biggest_split_recursive([ _ | []], MaxSplit, PrevNodeNum) ->
    debug(2,"(node_find_biggest_split) End rec. MaxSplit: ~p, PrevNodeNum~p~n",
        [MaxSplit, PrevNodeNum]),
    {PrevNodeNum, MaxSplit};
biggest_split_recursive([FNum | [SNum | Nums]], MaxSplit, PrevNodeNum) ->
        debug(4, "TakenNums: ~p, MaxSplit: ~p, PrevNodeNum~p~n", 
            [[FNum | [SNum | Nums]], MaxSplit, PrevNodeNum]),
        CurrentSplit = SNum - FNum,
        case CurrentSplit > MaxSplit of
            true ->
                biggest_split_recursive([SNum | Nums], 
                    CurrentSplit, FNum);
            false ->
                biggest_split_recursive([SNum | Nums], 
                    MaxSplit, PrevNodeNum)
        end.

%% Joining and Rebalancing-Related Functions
%% ---------------------------------------------------------------------------

%% spawns and registers all processes in Range
node_spawn_and_register_processes(storage_join, [], _) ->
    debug(1, "Finished spawning and registering joining processes!~n"),
    global:sync(),
    ok;
node_spawn_and_register_processes(storage_join, [P | Range], M) ->
    debug(4, "Entering node_spawn_and_register_processes with 
        storage_join.~n"),
    Name = storage_name(P),
    glocally_register_name(Name, spawn(key_value_node, 
            storage_join, [P, M])),
    debug(4, "Registered ~p. Registered_names:~p~n", [Name, 
            global:registered_names()]),
    node_spawn_and_register_processes(storage_join, Range, M);
node_spawn_and_register_processes(storage_serve, [], _) ->
    debug(1, "Finished initial spawning and registering processes!~n"),
    global:sync(),
    ok;
node_spawn_and_register_processes(storage_serve, [P | 
        Range], M) ->
    debug(4, "(storage_serve) Entering node_spawn_and_register_processes.~n"),
    Name = storage_name(P),
    BackupName = storage_assoc_backup_name(P, M),
    glocally_register_name(BackupName, spawn(key_value_node, 
            backup, [backup_get_num(BackupName), [], M])),
    glocally_register_name(Name, spawn(key_value_node, 
            storage_serve, [P, M])),
    debug(4, "Registered ~p and ~p. Registered_names:~p~n", 
        [Name, BackupName, global:registered_names()]),
    node_spawn_and_register_processes(storage_serve, Range, M).


%% Produces a list of numbers for the processes that this 
%% node is responsible for.
%% Start: Num of first storage process to take 
%% responsibility for
%% Sucessor: Num of Sucessor node--the end of your 
%% responsibilities
node_get_responsibilities(Start, Successor, NumProcesses) ->
    debug(3, "Entering node_get_responsibilities(~p, ~p, ~p)~n", 
        [Start, Successor, NumProcesses]),
    End = get_legal_num(Successor, -1, NumProcesses),
    debug(2, "(node_get_responsibilities) Start: ~p, End: ~p~n",[Start, End]),
    case (Start =< End) of
        true ->
            lists:seq(Start, End);
        false ->
            lists:seq(Start, NumProcesses-1) ++ 
            lists:seq(0, End)
    end.

%% This corresponds to the overflow state in the description.
node_take_responsibility(MyNum, Range, M) -> 
    %% should be all because we should never tell it to take responsibility 
    %% for something it already has.
    node_spawn_and_register_processes(storage_join, Range, M),
    debug(1, "(take_responsibility) Node~p is waiting for children ~p~n",
        [MyNum, Range]),
    node_wait_for_prodigal_children(Range).


%% This should kill all the storage processes with numbers in 
%% range, as well as their counterparts which backup their opposites.
%% Note: It is only possible to kill processes on your own node.
node_kill_processes(PrimaryRange, M) ->
    debug(3,"(node_handle_join) Entering node_kill_processes(~p, ~p)~n", 
        [PrimaryRange, M]),
    BackupRange = lists:map(fun(X) -> storage_assoc_backup_name(X, M) end,
        PrimaryRange),
    StorageNodeRange = lists:map(fun(X) -> storage_name(X) end,
        PrimaryRange),
    lists:map(fun(X) -> erlang:exit(global:whereis_name(X), system_limit) end,
        StorageNodeRange ++ BackupRange),
    debug(1,"(node_handle_join) Killed processes ~p~n",
        [StorageNodeRange ++ BackupRange]).

%% Prodigals is a list of numbers
node_wait_for_prodigal_children([]) ->
    debug(3,"Received all 'serving's!~n");
node_wait_for_prodigal_children(Prodigals) ->
    debug(4, "Waiting for prodigal children ~p~n", [Prodigals]),
    receive
        {serving, Prodigal} ->
            print("(take_responsibility) Received {serving, ~p}~n",[Prodigal]),
            node_wait_for_prodigal_children(lists:delete(Prodigal, 
                    Prodigals))
    end.

%% Given the total number of processes, pick the best 
%% name(and therefore set of
%% processes) for a joining node.
node_pick_num(NumProcesses)->
    debug(1,"Entering node_pick_num(~p)~n", [NumProcesses]),
    TakenNames = lists:filter(fun(X) -> is_node(X) end, 
        global:registered_names()),
    debug(3,"got takenames:~p~n", [TakenNames]),
    TakenNums = lists:sort(lists:map(
            fun(X) -> node_get_num(X) end,
            TakenNames)),
    debug(3,"got takenums~n"),
    case node_find_biggest_split(TakenNums, NumProcesses) of
        {PrevNodeNum, Split} ->
            Number = ((PrevNodeNum+(Split div 2)) rem NumProcesses),
            Higher = lists:filter(fun(X) -> X > Number end, TakenNums),
            Lower = lists:filter(fun(X) -> X < Number end, TakenNums),
            case Higher of
                [] ->
                    case Lower of
                        [] -> {lists:last(TakenNums), Number, hd(TakenNums)};
                        _ -> {lists:last(Lower), Number, hd(TakenNums)}
                    end;
                _ ->
                    case Lower of
                        [] -> {lists:last(TakenNums), Number, hd(Higher)};
                        _ -> {lists:last(Lower), Number, hd(Higher)}
                    end
            end
    end.

%% Returns a list of the numbers of a node's neighbors.
storage_get_neighbors(MyNum, M)->
    NumProcesses = trunc(math:pow(2, M)),
    lists:map(
        fun(X) -> get_legal_num(MyNum, trunc(math:pow(2, X)), NumProcesses) end,
        lists:seq(0, M-1)).
%%
%% Rebalancing-Related Functions
%% ---------------------------------------------------------------------------

%% Given a range of process numbers, can one node have them 
%% all?
node_can_take_all_processes(NumRange, NumProcesses) ->
    TooMany = (length(NumRange) > (NumProcesses / 2)) ,
    Alone = length(all_node_procs()) == 1,
    Result = (not TooMany) or Alone,
    debug(1, "(can_take_all) Range: ~p. TooMany: ~p. Alone: ~p. "++
        "Result: ~p. Globally registered:~n\t~p~n",
        [NumRange, TooMany, Alone, Result, global:registered_names()]),
    Result.

node_switch_processes(Range, M) ->
    BackupRange = 
    lists:map(fun(X) ->
                StorageName = storage_name(X),
                BackupName = storage_assoc_backup_name(X, M),
                StorageName ! {self(), switch},
                print("(node_rebalance) Sent {~p, ~p} to {~p, ~p}~n",
                    [self(), switch, StorageName, node()]),
                BackupName ! {self(), switch},
                print("(node_rebalance) Sent {~p, ~p} to {~p, ~p}~n",
                    [self(), switch, BackupName, node()]),
                backup_get_num(BackupName)
                end,
                Range),
    debug(2, "(switch_processes) Range:~p. BackupRange:~p.~n",
        [Range, BackupRange]),
    wait_for_switched(Range ++ BackupRange).

wait_for_switched([]) -> 
    debug(3,"(wait_for_switch) All processes have switched!~n"),
    ok;
wait_for_switched(Range) -> 
    receive
        {switched, Num} -> 
            print("(switch_processes) Got {switched, ~p}~n", [Num]),
            wait_for_switched(lists:delete(Num, Range))
    end.
%%
%% General Helper Functions
%% ---------------------------------------------------------------------------

glocally_register_name(Atom, Pid) ->
    register(Atom, Pid),
    global:register_name(Atom, Pid).

glocally_unregister_name(Atom) ->
    unregister(Atom),
    global:unregister_name(Atom).

global_send(Recipient, Message) ->
    global:send(Recipient, Message),
    print("Sent ~p to ~p~n", [Message, Recipient]).

node_get_successor(MyNum) ->
    debug(3,"Entering node_get_successor(~p)~n", [MyNum]),
    NodeNums = 
    lists:sort(lists:map(fun(Y) -> node_get_num(Y) end, 
            lists:filter(fun(X) -> is_node(X) end, 
                global:registered_names()))),
    % We assume registered_names() is in alphabetical order
    node_name(get_next_num(MyNum, NodeNums)).

get_next_num(MyNum, NumList) ->
    HigherNums = 
        lists:sort(lists:filter(fun(Y) -> Y > MyNum end, 
                NumList)),
    case HigherNums of
        [] -> hd(NumList);
        _ -> hd(HigherNums)
    end.

node_get_predecessor(MyNum) ->
    debug(1,"Entering node_get_predecssor(~p)~n", [MyNum]),
    NodeNames = 
        lists:sort(lists:filter(fun(X) -> is_node(X) end, 
                global:registered_names())),
    % We assume registered_names() is in alphabetical order
    LowerNodeNames = 
        lists:sort(
            lists:filter(fun(Y) -> node_get_num(Y) < MyNum end, NodeNames)),
    case LowerNodeNames of
        [] ->
            lists:last(NodeNames);
        _ ->
            lists:last(LowerNodeNames)
    end.

node_name(Number) ->
    list_to_atom("Node"++integer_to_list(Number)).

storage_name(Number) ->
    list_to_atom("StorageProcess"++integer_to_list(Number)).

backup_name(Number) ->
    list_to_atom("Backup"++integer_to_list(Number)).

node_get_num([P | Name])->
    debug(4,"Entering node_get_num(String)"),
    list_to_integer(lists:sublist([P | Name], 5, length([P | Name])));
node_get_num(Atom) ->
    debug(4,"Entering node_get_num(~p)~n", [Atom]),
    node_get_num(atom_to_list(Atom)).

backup_get_num([P | Name])->
    list_to_integer(lists:sublist([P | Name], 7, length([P | Name])));
backup_get_num(Atom) ->
    backup_get_num(atom_to_list(Atom)).

%% Given a starting number, an offset, and a number of

%% processes,
%% return the correct process number.
get_legal_num(Start, Offset, NumProcesses)->
    Ret = (Start+trunc(Offset)+trunc(NumProcesses)) rem 
    trunc(NumProcesses),
    debug(4, "get_legal_num(~p, ~p, ~p) returning ~p~n",
        [Start, Offset, NumProcesses, Ret]),
    Ret.

all_node_procs() ->
    lists:filter(fun(X) -> is_node(X) end, global:registered_names()).

%% Given a process number, get the name of the node process it's supposed to be 
%% on
storage_get_expected_node_name_of(MyNum) ->
    global:sync(),
    NodeNames = lists:sort(lists:filter(fun(Y) ->
                    is_node(Y) 
            end, global:registered_names())),
    % We assume registered_names() is in alphabetical order
    LowerNodeNames = lists:sort(lists:filter(fun(X) ->
                node_get_num(X) =< 
                MyNum end, NodeNames)),
    debug(4,"(get_expected) Lower Nodes: ~p~n", [LowerNodeNames]),
    NodeName = case LowerNodeNames of
        [] -> lists:last(NodeNames);
        _ -> lists:last(LowerNodeNames)
    end,
    debug(3, "(get_expected) StorageProcess~p should be on ~p.~n",
        [MyNum, NodeName]),
    NodeName.

%% Returns a nodeName atom
storage_assoc_backup_name(MyNum, M) ->
    debug(4,"entered storage_assoc_backup_name(~p,~p)~n",[MyNum, M]),
    BackupNum = 
        get_legal_num(MyNum, trunc(math:pow(2, M-1)), trunc(math:pow(2, M))),
    backup_name(BackupNum).

%% We assume that all nodes register a global process named 
%% "Node__" where
%% __ is the node number which specifies its storage 
%% processes.
is_node(Atom) ->
    debug(4,"is it a node? ~p~n", [Atom]),
    [hd(atom_to_list(Atom))] == "N".

%% We assume that all storage processes register a global 
%% process named "StorageProcess__" where __ is the storage 
%% process number.
is_storage(Atom) ->
    debug(4,"is it a storage process? ~p~n", [Atom]),
    [hd(atom_to_list(Atom))] == "S".

%% We assume that all backup processes register a global process named
%% "Backup__" where __ is the process number of the storage process it backs up
is_backup(Atom) ->
    debug(4,"is it a backup process? ~p~n", [Atom]),
    [hd(atom_to_list(Atom))] == "B".

%% Forward "snapshot" infor requests atrunc ring & aggregate 
%% information
storage_get_requested_info(MyNum, MyMap, {Pid, Ref, Atom, Starter, Running})->
    debug(1,"Entering storage_get_requested_info"),
    case Starter of
        MyNum ->
            Pid ! {Ref, result, Running},
            print("(storage_serve) Sent {~p, ~p, ~p} to ~p~n",
                [Ref, result, Running, Pid]);
        _ ->
            Message = 
            case Atom of
                first_key ->
                    KeyList = lists:map(fun(X) ->
                                element(1, X) end, 
                        MyMap),
                    NewFirst = lists:min([Running | KeyList]),
                    {Pid, Ref, first_key, Starter, NewFirst};
                last_key ->
                    KeyList = lists:map(fun(X) ->
                                element(1, X) end, 
                        MyMap),
                    NewLast = lists:max([Running | KeyList]),
                    {Pid, Ref, last_key, Starter, NewLast};
                num_keys ->
                    NewTotal = Running + length(MyMap),
                    {Pid, Ref, num_keys, Starter, NewTotal}
            end,
            storage_send_to_next_process(MyNum, Message)
    end.

node_get_my_processes() ->
    Registered = registered(),
    lists:filter(fun(X) -> is_node(X) or is_storage(X) or is_backup(X) end,
		 Registered).

node_get_my_backup_proc_nums() ->
    Registered = registered(),
    lists:map(fun(Y) -> backup_get_num(Y) end,
              lists:filter(fun(X) -> is_backup(X) end,
                           Registered)).

%%
%% Debug Output
%% ---------------------------------------------------------------------------

%% Returns a timestamp formatted string with year, month, 
%% day, hour, minute,
%% second, and milisecond. Takes an erlang timestamp.
date_time_prefix({Mega, Sec, Micro}) ->
    {{Year, Month, Day}, {Hour, Minute, Second}} = 
        calendar:now_to_local_time({Mega, Sec, Micro}),
    Milliseconds = Micro div 1000,
    io_lib:format("~p-~p-~p ~p:~p:~p.~p:", 
        [Year, Month, Day, Hour, Minute, Second, Milliseconds]).

proc_node_prefix() ->
    Pname = case process_info(self(), registered_name) of
                {registered_name, Atom} -> atom_to_list(Atom);
                _ -> pid_to_list(self())
            end,
            io_lib:format(" ~-10s:~18s: ", [atom_to_list(node()), Pname]).
%% Prints String to console with timestamp prefix and format 
%% Data.
print(String) -> print(String, []).
print(String, Data) ->
    io:format(date_time_prefix(erlang:now()) ++ proc_node_prefix() ++ String,
        Data).

debug(Level, String) -> debug(Level, String, []).
debug(Level, String, Data) ->
    case ?DEBUG >= Level of
        true -> print(String, Data);
        false -> ok
    end.
