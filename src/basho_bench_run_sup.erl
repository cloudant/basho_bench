-module(basho_bench_run_sup).

-behavior(supervisor).

-export([start_link/0]).

-export([init/1]).

-define(CHILD(I, Type, Timeout), {I, {I, start_link, []}, transient, Timeout, Type, [I]}).


start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).


init([]) ->
    Timeout = application:get_env(basho_bench, shutdown_timeout, 30000),
    MeasurementDriver = case basho_bench_config:get(measurement_driver, []) of
        [] -> [];
        _Driver -> [?CHILD(basho_bench_measurement, worker, Timeout)]
    end,
    Spec0 = [?CHILD(basho_bench_worker_sup, supervisor, Timeout)],
    Spec1 = case basho_bench:is_master() of
        true ->
            [?CHILD(basho_bench_stats, worker, Timeout) | Spec0];
        false ->
            Spec0
    end,
    Spec2 = [?CHILD(basho_bench_duration, worker, Timeout) | Spec1],
    {ok, {{one_for_all, 0, 1}, Spec2 ++ MeasurementDriver}}.
