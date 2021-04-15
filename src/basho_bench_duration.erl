%% -------------------------------------------------------------------
%% Check whether to exit besho_bench, exit conditions
%%     1. run out of duration time
%%     2. run out of operations even if the duration time had not up
%% -------------------------------------------------------------------

-module(basho_bench_duration).

-behavior(gen_server).

-export([
    run/0,
    remaining/0,
    abort/1
]).

-export([start_link/0]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3,
    worker_stopping/1
]).

-record(state, {
    ref,
    duration,
    start,
    rampup_interval
}).

-include("basho_bench.hrl").


run() ->
    Timeout = basho_bench_config:get(duration_call_run_timeout),
    gen_server:call(?MODULE, run, Timeout).


remaining() ->
    gen_server:call(?MODULE, remaining).


abort(Reason) ->
    gen_server:cast(?MODULE, {abort, Reason}).


start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


init([]) ->
    run_hook(basho_bench_config:get(pre_hook, no_op)),
    Ref = erlang:monitor(process, whereis(basho_bench_run_sup)),
    {ok, maybe_add_rampup(#state{ref=Ref})}.


handle_call(run, _From, State) ->
    DurationMins = basho_bench_config:get(duration, 1),
    ?INFO("Starting with duration: ~p", [DurationMins]),
    NewState = State#state{
        start=os:timestamp(),
        duration=DurationMins
    },
    if NewState#state.rampup_interval =:= undefined -> ok; true ->
        timer:send_interval(NewState#state.rampup_interval, rampup)
    end,
    maybe_end({reply, ok, NewState});

handle_call(remaining, _From, State) ->
    #state{start=Start, duration=Duration} = State,
    Remaining = (Duration*60) - timer:now_diff(os:timestamp(), Start),
    maybe_end({reply, Remaining, State}).


%% WorkerPid is basho_bench_worker's id, not the pid of actual driver
handle_cast({worker_stopping, WorkerPid}, State) ->
    case basho_bench_worker_sup:active_workers() -- [WorkerPid] of
        [] ->
            ?INFO("The application has stopped early!", []),
            #state{start=Start} = State,
            Elapsed = timer:now_diff(os:timestamp(), Start) / 60000000,
            {stop, {shutdown, normal}, State#state{duration=Elapsed}};
        _ ->
            maybe_end({noreply, State}) 
    end;

handle_cast({abort, Reason}, State) ->
    {stop, {abort, Reason}, State};

handle_cast(_Msg, State) ->
    maybe_end({noreply, State}).


handle_info({'DOWN', Ref, process, _Object, Info}, #state{ref=Ref}=State) ->
    {stop, {shutdown, Info}, State};

handle_info(timeout, State) ->
    {stop, {shutdown, normal}, State};

handle_info(rampup, State) ->
    ?INFO("Triggering worker rampup", []),
    add_worker(),
    maybe_end({noreply, State});

handle_info(Msg, State) ->
    ?WARN("basho_bench_duration handled unexpected info message: ~p", [Msg]),
    maybe_end({noreply, State}).


terminate(Reason, #state{duration=DurationMins}) ->
    Abort = case Reason of
        normal ->
            ?CONSOLE("Test completed after ~p mins.\n", [DurationMins]),
            false;
        {shutdown, normal} ->
            ?CONSOLE("Test completed after ~p mins.\n", [DurationMins]),
            false;
        {shutdown, Reason1} ->
            ?CONSOLE("Test stopped: ~p\n", [Reason1]),
            false;
        {abort, Reason1} ->
            ?CONSOLE("!! Test aborted: ~p\n", [Reason1]),
            true
    end,
    case whereis(basho_bench_worker_sup) of
        undefined ->
            ok;
        WSup ->
            WRef = erlang:monitor(process, WSup),
            supervisor:terminate_child(basho_bench_run_sup, basho_bench_worker_sup),
            receive
                {'DOWN', WRef, process, _Object, _Info} ->
                    ok
            end
    end,
    run_hook(basho_bench_config:get(post_hook, no_op)),
    basho_bench_profiler:maybe_terminate_profiler(basho_bench_config:get(enable_profiler, false)),
    if Abort =/= true -> ok; true ->
           ?CONSOLE("Aborting benchmark run...", []),
           timer:sleep(1000),
           erlang:halt(1)
    end,
    supervisor:terminate_child(basho_bench_sup, basho_bench_run_sup),
    application:set_env(basho_bench_app, is_running, false),
    application:stop(basho_bench),
    ok.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


maybe_end(Return) ->
    {Reply, Message, State} = case Return of
        {Reply0, Message0, State0} ->
            {Reply0, Message0, State0};
        {Reply0, State0} ->
            {Reply0, ok, State0}
    end,
    #state{start=Start} = State,
    case State#state.duration of
        infinity ->
            Return;
        Duration ->
            if Start /= undefined ->
                case timer:now_diff(os:timestamp(), Start) of
                    Elapsed when Elapsed / 60000000 >= Duration ->
                        ?CONSOLE("Stopping: ~p", [Elapsed]),
                        {stop, normal, State};
                    Elapsed ->
                        Timeout = round(Duration*60000 - Elapsed/1000),
                        case tuple_size(Return) of
                            2 -> {Reply, State, Timeout};
                            3 -> {Reply, Message, State, Timeout}
                        end
                end;
            true ->
               {Reply, State}
            end
    end.


run_hook({Module, Function}) ->
    Module:Function();

run_hook(no_op) ->
    no_op.

worker_stopping(WorkerPid) ->
    %% WorkerPid is basho_bench_worker's id, not the pid of actual driver 
    gen_server:cast(?MODULE, {worker_stopping, WorkerPid}),
    ok.

maybe_add_rampup(State) ->
    case basho_bench_config:get(workers_rampup, undefined) of
        undefined ->
            State;
        Interval when is_integer(Interval) ->
            State#state{rampup_interval=Interval};
        %% TODO: should we support per type intervals?
        [{_Type, Interval} | _ ] when is_integer(Interval) ->
            State#state{rampup_interval=Interval};
        Else ->
            throw({unexpected_rampup, Else})
    end.

add_worker() ->
    case basho_bench_config:get(workers, undefined) of
        undefined ->
            basho_bench_worker_sup:add_worker();
        [_|_] = Workers ->
            WorkerTypes = [WT || {WT, _C} <- Workers],
            basho_bench_worker_sup:add_workers(WorkerTypes)
    end.
