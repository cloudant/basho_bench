%% -------------------------------------------------------------------
%%
%% basho_bench: Benchmarking Suite
%%
%% Copyright (c) 2009-2010 Basho Techonologies
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------
-module(basho_bench_worker).

-behaviour(gen_server).

%% API
-export([start_link/2,
         start_link_local/2,
         run/1,
         stop/1,
         config_get/1, config_get/2, config_get/3, 
         lookup_value/2, lookup_value/3
]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, { id,
                 keygen,
                 valgen,
                 driver,
                 driver_state,
                 worker_type,
                 local_config,
                 shutdown_on_error,
                 ops,
                 ops_len,
                 ops_configs,
                 rng_seed,
                 parent_pid,
                 worker_pid,
                 sup_id}).

-include("basho_bench.hrl").

%% ====================================================================
%% API
%% ====================================================================

start_link(SupChild, Id) ->
    case basho_bench_config:get(distribute_work, false) of 
        true -> 
            start_link_distributed(SupChild, Id);
        false -> 
            start_link_local(SupChild, Id)
    end.

start_link_distributed(SupChild, Id) ->
    Node = pool:get_node(),
    rpc:block_call(Node, ?MODULE, start_link_local, [SupChild, Id]).

start_link_local(SupChild, Id) ->
    gen_server:start_link(?MODULE, [SupChild, Id], []).

run(Pids) ->
    [ok = gen_server:call(Pid, run, infinity) || Pid <- Pids],
    ok.

stop(Pids) ->
    [ok = gen_server:call(Pid, stop, infinity) || Pid <- Pids],
    ok.

%% ====================================================================
%% gen_server callbacks
%% ====================================================================

init([SupChild, Id]) ->
    %% If workers are being used, WorkerType will be set to next needed type
    %% and LocalConfig will be set from the associated config in worker_types
    WorkerType = basho_bench_config:next_worker(),
    ?DEBUG("init ID ~p WorkerType=~p~n", [Id, WorkerType]),
    LocalConfig = 
        case WorkerType of 
            no_workers ->
                [];
            _ ->
                % TODO: Improve error reporting, 
                %    but with no worker_types or specific worker defined should complain
                WorkerTypes = basho_bench_config:get(worker_types),
                lookup_value(WorkerType, WorkerTypes)
        end,

    %% Setup RNG seed for worker sub-process to use; incorporate the ID of
    %% the worker to ensure consistency in load-gen
    %%
    %% NOTE: If the worker process dies, this obviously introduces some entroy
    %% into the equation since you'd be restarting the RNG all over.
    %%
    %% The RNG_SEED is static by default for replicability of key size
    %% and value size generation between test runs.
    process_flag(trap_exit, true),
    {A1, A2, A3} =
        case config_get(rng_seed, {42, 23, 12}, LocalConfig) of
            {Aa, Ab, Ac} -> {Aa, Ab, Ac};
            now -> now()
        end,

    RngSeed = {A1+Id, A2+Id, A3+Id},

    %% Pull all config settings from environment
    Driver  = config_get(driver, LocalConfig),
    Operations = config_get(operations, LocalConfig),
    Ops     = ops_tuple(Operations), 
    ShutdownOnError = config_get(shutdown_on_error, false, LocalConfig),

    %% Finally, initialize key and value generation. We pass in our ID to the
    %% initialization to enable (optional) key/value space partitioning
    KeyGen = basho_bench_keygen:new(config_get(key_generator, LocalConfig), Id),
    ValGen = basho_bench_valgen:new(config_get(value_generator, LocalConfig), Id),

    OptionsConfigs = ops_configs(Id, Operations),
    State = #state { id = Id, keygen = KeyGen, valgen = ValGen,
                     driver = Driver,
                     local_config = LocalConfig,
                     worker_type = WorkerType,
                     shutdown_on_error = ShutdownOnError,
                     ops = Ops, ops_len = size(Ops),
                     ops_configs = OptionsConfigs,
                     rng_seed = RngSeed,
                     parent_pid = self(),
                     sup_id = SupChild},

    %% Use a dedicated sub-process to do the actual work. The work loop may need
    %% to sleep or otherwise delay in a way that would be inappropriate and/or
    %% inefficient for a gen_server. Furthermore, we want the loop to be as
    %% tight as possible for peak load generation and avoid unnecessary polling
    %% of the message queue.
    %%
    %% Link the worker and the sub-process to ensure that if either exits, the
    %% other goes with it.
    WorkerPid = spawn_link(fun() -> worker_init(State) end),
    WorkerPid ! {init_driver, self()},
    receive
        driver_ready ->
            ok;
        {driver_failed, Why} ->
            exit({init_driver_failed, Why})
    end,

    %% If the system is marked as running this is a restart; queue up the run
    %% message for this worker
    case basho_bench_app:is_running() of
        true ->
            ?WARN("Restarting crashed worker.\n", []),
            gen_server:cast(self(), run);
        false ->
            ok
    end,

    {ok, State#state { worker_pid = WorkerPid }}.

handle_call(run, _From, State) ->
    State#state.worker_pid ! run,
    {reply, ok, State}.

handle_cast(run, State) ->
    State#state.worker_pid ! run,
    {noreply, State}.

handle_info({'EXIT', Pid, Reason}, State) ->
    #state{worker_pid=WorkerPid} = State,
    case {Reason, Pid} of
        {normal, _} ->
            {stop, normal, State};
        {_, WorkerPid} ->
            ?ERROR("Worker ~p exited with ~p~n", [Pid, Reason]),
            %% Worker process exited for some other reason; stop this process
            %% as well so that everything gets restarted by the sup
            {stop, worker_died, State}
    end.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ====================================================================
%% Additional Exported Functions
%% ====================================================================

%% get works with either LocalConfig list or #state.local_config in provided State
%% if value is not found in LocalConfig list then basho_bench_config:get is called 
%% if no value is found and no default is provided, returns an error

config_get(Key) ->
    config_get(Key, undefined, []).    

config_get(Key, #state{local_config=LocalConfig}) ->
    config_get(Key, undefined, LocalConfig);
config_get(Key, LocalConfig) ->
    config_get(Key, undefined, LocalConfig).

config_get(Key, Default, #state{local_config=LocalConfig}) ->
   config_get(Key, Default, LocalConfig);
config_get(Key, Default, LocalConfig) ->
    Value = 
        case V = erlang:get(Key) of 
            undefined ->
                lookup_value(Key, LocalConfig);
            _ -> V
        end,
    case Value of 
        undefined -> 
            basho_bench_config:get(Key,Default);
        _ ->
            Value
    end.

%% lookup_value finds a given {Key,Value} pair in a provided list or returns undefined
lookup_value(Key, Default, LocalConfig) ->
    case Value = lookup_value(Key, LocalConfig) of
        undefined ->
            Default;
         _ ->
            Value
    end.

lookup_value(_Key, undefined) ->
    undefined;
lookup_value(_Key, []) ->
    undefined;
lookup_value(Key, [Term | Tail]) ->
    {K,V} = Term,
    case K =:= Key of
        true ->
            V;
        _ ->
            lookup_value(Key,Tail)
    end.

publish_config([]) ->
    ok;
publish_config([{Key,Value} | RestConfig]) ->
    erlang:put(Key,Value),
    publish_config(RestConfig).


%% ====================================================================
%% Internal functions
%% ====================================================================

%%
%% Expand operations list into tuple suitable for weighted, random draw
%%
ops_tuple(Operations) ->
    F =
        fun({OpTag, Count}) ->
                lists:duplicate(Count, {OpTag, OpTag});
           ({Label, OpTag, Count}) ->
                lists:duplicate(Count, {Label, OpTag});
           ({Label, OpTag, Count, _OptionsList}) ->
                lists:duplicate(Count, {Label, OpTag})
        end,
    Ops = [F(X) || X <- Operations],
    list_to_tuple(lists:flatten(Ops)).

%%
%% Expand operations list into initialized operations configuration information where used
%%

ops_configs(Id, Operations) ->
    ops_configs(Id, Operations, []).
    
%% Process individual operation entries in Operations list
ops_configs(_Id, [], ACC) ->
    ACC;
ops_configs(Id, [{_Label, OpTag, _Count, OptionsList}|RestOps], ACC) ->
    ops_configs(Id, RestOps, [ {OpTag, ops_configs_list(Id, OptionsList, [])} | ACC]);
ops_configs(Id, [_Op|RestOps], ACC) ->
    ops_configs(Id, RestOps, ACC).

% Process individual config parameter entries within an Operations OptionsList
ops_configs_list(_Id, [], ACC) ->
    ACC;
% Treat keygen and valuegen specially, converting keygen/valgen specification into actual object for later use
ops_configs_list(Id, [{key_generator, KeyGenOption} | RestOptions], ACC) ->
    ops_configs_list(Id, RestOptions, [ {keygen, basho_bench_keygen:new(KeyGenOption, Id)} | ACC]);
ops_configs_list(Id, [{value_generator, ValGenOption} | RestOptions], ACC) ->
    ops_configs_list(Id, RestOptions, [ {valgen, basho_bench_valgen:new(ValGenOption, Id)} | ACC]);
% Allow remaining options to pass through
ops_configs_list(Id, [KeyValuePair | RestOptions], ACC) ->
    ops_configs(Id, RestOptions, [ KeyValuePair | ACC]).

worker_init(State) ->
    %% Trap exits from linked parent process; use this to ensure the driver
    %% gets a chance to cleanup
    process_flag(trap_exit, true),
    publish_config(State#state.local_config),
    random:seed(State#state.rng_seed),
    worker_idle_loop(State).

worker_idle_loop(State) ->
    Driver = State#state.driver,
    receive
        {init_driver, Caller} ->
            %% Spin up the driver implementation
            case catch(Driver:new(State#state.id)) of
                {ok, DriverState} ->
                    Caller ! driver_ready,
                    ok;
                Error ->
                    Caller ! {init_driver_failed, Error},
                    DriverState = undefined, % Make erlc happy
                    ?FAIL_MSG("Failed to initialize driver ~p: ~p\n", [Driver, Error])
            end,
            worker_idle_loop(State#state { driver_state = DriverState });
        run ->
            case config_get(mode, State) of
                max ->
                    ?INFO("Starting max worker: ~p on ~p~n", [self(), node()]),
                    max_worker_run_loop(State);
                {rate, max} ->
                    ?INFO("Starting max worker: ~p on ~p~n", [self(), node()]),
                    max_worker_run_loop(State);
                {rate, Rate} ->
                    %% Calculate mean interarrival time in in milliseconds. A
                    %% fixed rate worker can generate (at max) only 1k req/sec.
                    MeanArrival = 1000 / Rate,
                    ?INFO("Starting ~w ms/req fixed rate worker: ~p on ~p\n", [MeanArrival, self(), node()]),
                    rate_worker_run_loop(State, 1 / MeanArrival)
            end
    end.

worker_next_op2(State, OpTag) ->
   OpConfig = lookup_value(OpTag, State#state.ops_configs),
   KeyGen = lookup_value(keygen, State#state.keygen, OpConfig),
   ValueGen = lookup_value(valuegen, State#state.valgen, OpConfig),
   catch (State#state.driver):run(OpTag, KeyGen, ValueGen, 
                                  State#state.driver_state).
worker_next_op(State) ->
    Next = element(random:uniform(State#state.ops_len), State#state.ops),
    {Label, OpTag} = Next,
    Start = os:timestamp(),
    Result = worker_next_op2(State, OpTag),
    ElapsedUs = erlang:max(0, timer:now_diff(os:timestamp(), Start)),

    %%TODO: May WORK but ugly/horrible way to apply worker_type name so revisit
    OpName = { basho_bench_stats:worker_op_name(State#state.worker_type, Label),
               basho_bench_stats:worker_op_name(State#state.worker_type, OpTag)},
    case Result of
        {Res, DriverState} when Res == ok orelse element(1, Res) == ok ->
            basho_bench_stats:op_complete(OpName, Res, ElapsedUs),
            {ok, State#state { driver_state = DriverState}};

        {Res, DriverState} when Res == silent orelse element(1, Res) == silent ->
            {ok, State#state { driver_state = DriverState}};

        {ok, ElapsedT, DriverState} ->
            %% time is measured by external system
            basho_bench_stats:op_complete(OpName, ok, ElapsedT),
            {ok, State#state { driver_state = DriverState}};

        {error, Reason, DriverState} ->
            %% Driver encountered a recoverable error
            basho_bench_stats:op_complete(OpName, {error, Reason}, ElapsedUs),
            State#state.shutdown_on_error andalso
                erlang:send_after(500, basho_bench,
                                  {shutdown, "Shutdown on errors requested", 1}),
            {ok, State#state { driver_state = DriverState}};

        {'EXIT', Reason} ->
            %% Driver crashed, generate a crash error and terminate. This will take down
            %% the corresponding worker which will get restarted by the appropriate supervisor.
            basho_bench_stats:op_complete(OpName, {error, crash}, ElapsedUs),

            %% Give the driver a chance to cleanup
            (catch (State#state.driver):terminate({'EXIT', Reason}, State#state.driver_state)),

            ?DEBUG("Driver ~p crashed: ~p\n", [State#state.driver, Reason]),
            case State#state.shutdown_on_error of
                true ->
                    %% Yes, I know this is weird, but currently this
                    %% is how you tell Basho Bench to return a
                    %% non-zero exit status.  Ideally this would all
                    %% be done in the `handle_info' callback where it
                    %% would check `Reason' and `shutdown_on_error'.
                    %% Then I wouldn't have to return a bullshit "ok"
                    %% here.
                    erlang:send_after(500, basho_bench,
                                      {shutdown, "Shutdown on errors requested", 2}),
                    {ok, State};
                false ->
                    crash
            end;

        {stop, Reason} ->
            %% Driver (or something within it) has requested that this worker
            %% terminate cleanly.
            ?INFO("Driver ~p (~p) has requested stop: ~p\n", [State#state.driver, self(), Reason]),

            %% Give the driver a chance to cleanup
            (catch (State#state.driver):terminate(normal, State#state.driver_state)),

            normal
    end.

needs_shutdown(State) ->
    receive
        {'EXIT', _Pid, _Reason} ->
            (catch (State#state.driver):terminate(normal,
                                                  State#state.driver_state)),
            true
    after 0 ->
            false
    end.


max_worker_run_loop(State) ->
    case worker_next_op(State) of
        {ok, State2} ->
            case needs_shutdown(State2) of
                true ->
                    ok;
                false ->
                    max_worker_run_loop(State2)
            end;
        ExitReason ->
            exit(ExitReason)
    end.

rate_worker_run_loop(State, Lambda) ->
    %% Delay between runs using exponentially distributed delays to mimic
    %% queue.
    timer:sleep(trunc(basho_bench_stats:exponential(Lambda))),
    case worker_next_op(State) of
        {ok, State2} ->
            case needs_shutdown(State2) of
                true ->
                    ok;
                false ->
                    rate_worker_run_loop(State2, Lambda)
            end;
        ExitReason ->
            exit(ExitReason)
    end.



