-module(basho_bench_duration).

-behavior(gen_server).

-export([start_link/0]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-record(state, {
    ref,
    duration,
    start
}).

-include("basho_bench.hrl").


start_link() ->
    gen_server:start_link({global, ?MODULE}, ?MODULE, [], []).


init([]) ->
    run_hook(basho_bench_config:get(pre_hook, no_op)),
    Ref = erlang:monitor(process, whereis(basho_bench_run_sup)),
    DurationMins = basho_bench_config:get(duration),
    {ok, #state{ref=Ref, duration=DurationMins}}.


handle_call(_Msg, _From, State) ->
    maybe_end({reply, ok, State}).


handle_cast(_Msg, State) ->
    maybe_end({noreply, State}).


handle_info({'DOWN', Ref, process, _Object, Info}, #state{ref=Ref}=State) ->
    run_hook(basho_bench_config:get(post_hook, no_op)),
    ?CONSOLE("Test stopped: ~p\n", [Info]),
    {stop, State};

handle_info(timeout, #state{duration=DurationMins}=State) ->
    run_hook(basho_bench_config:get(post_hook, no_op)),
    ?CONSOLE("Test completed after ~p mins.\n", [DurationMins]),
    {stop, State}.


terminate(_Reason, _State) ->
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
    Now = os:timestamp(),
    case State#state.duration of
        infinity ->
            Return;
        Duration when Start + (Duration * 60) < Now ->
            {stop, State};
        Duration ->
            End = Start + (Duration * 60),
            case tuple_size(Return) of
                2 -> {Reply, State, End - Now};
                3 -> {Reply, Message, State, End - Now}
            end
    end.


run_hook({Module, Function}) ->
    Module:Function();

run_hook(no_op) ->
    no_op.
