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
-module(basho_bench_config).
-behaviour(gen_server).


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-compile(export_all).
-endif.

-export([load/1,
         normalize_ips/2,
         set/2,
         get/1, get/2,
         next_worker/0]).

-export([start_link/0]).

% Gen server callbacks
-export([code_change/3, init/1, terminate/2, handle_call/3, handle_cast/2, handle_info/2]).

-include("basho_bench.hrl").

-record(basho_bench_config_state, {
    workers
}).

-type state() :: #basho_bench_config_state{}.
%% ===================================================================
%% Public API
%% ===================================================================

%% Todo: ensure_started before calling on any gen_server APIs.
ensure_started() -> 
    start_link().

start_link() ->
    gen_server:start_link({global, ?MODULE}, ?MODULE, [], []).


load(Files) ->
    ensure_started(),
    gen_server:call({global, ?MODULE}, {load_files, Files}). 
    
set(Key, Value) ->
    gen_server:call({global, ?MODULE}, {set, Key, Value}).

get(Key) ->
    case gen_server:call({global, ?MODULE}, {get, Key}) of
        {ok, Value} ->
            Value;
        undefined ->
            erlang:error("Missing configuration key", [Key])
    end.

get(Key, Default) ->
    case gen_server:call({global, ?MODULE}, {get, Key}) of
        {ok, Value} ->
            Value;
        undefined ->
            Default
    end.

next_worker() ->
    gen_server:call({global, ?MODULE}, {next_worker}).

%% @doc Normalize the list of IPs and Ports.
%%
%% E.g.
%%
%% ["127.0.0.1", {"127.0.0.1", 8091}, {"127.0.0.1", [8092,8093]}]
%%
%% => [{"127.0.0.1", DefaultPort},
%%     {"127.0.0.1", 8091},
%%     {"127.0.0.1", 8092},
%%     {"127.0.0.1", 8093}]
normalize_ips(IPs, DefultPort) ->
    F = fun(Entry, Acc) ->
                normalize_ip_entry(Entry, Acc, DefultPort)
        end,
    lists:foldl(F, [], IPs).





%% ===================================================================
%% Internal functions
%% ===================================================================


normalize_ip_entry({IP, Ports}, Normalized, _) when is_list(Ports) ->
    [{IP, Port} || Port <- Ports] ++ Normalized;
normalize_ip_entry({IP, Port}, Normalized, _) ->
    [{IP, Port}|Normalized];
normalize_ip_entry(IP, Normalized, DefaultPort) ->
    [{IP, DefaultPort}|Normalized].


%% ===
%% Gen_server Functions
%% ===

-spec init(term()) -> {ok, state()}.  
init(_Args) ->
    State = #basho_bench_config_state{ workers=[]},
    {ok, State}.

-spec code_change(term(), state(), term()) -> {ok, state()}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.                                

-spec terminate(term(), state()) -> 'ok'.
terminate(_Reason, _State) ->
    ok.

handle_call({load_files, FileNames}, _From, State) ->
    set_keys_from_files(FileNames),
    {reply, ok, State};

handle_call({set, Key, Value}, _From, State) ->
    application:set_env(basho_bench, Key, Value), 
    {reply, ok, State};
handle_call({get, Key}, _From, State) ->
    Value = application:get_env(basho_bench, Key),
    {reply, Value, State};

handle_call({next_worker}, From, #basho_bench_config_state{workers=Workers}=State) ->
    case Workers of 
        %% Load Workers the first time or when we've exhausted the list and need to refill it
        [] -> 
            NewWorkers = workers_tuple(),
            ?DEBUG("next_worker reload ~p~n", [NewWorkers]),
            handle_call({next_worker}, From, State#basho_bench_config_state{workers=NewWorkers});

        %% Failed first load will set state to no_workers to avoid trying to load the list again
        no_workers -> 
            {reply, no_workers, State};

        %% Pop first element of list 
        _ -> 
            [ Worker | NewWorkers ] = Workers,
            ?DEBUG("next_worker Worker ~p~n", [Worker]),
            {reply, Worker, State#basho_bench_config_state{workers=NewWorkers}}
    end.

handle_cast(_Cast, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

set_keys_from_files(Files) ->
    KVs = [ 
    case file:consult(File) of
        {ok, Terms} ->
            Terms;
        {error, Reason} ->
            ?FAIL_MSG("Failed to parse config file ~s: ~p\n", [File, Reason]),
            throw(invalid_config),
            notokay
    end || File <- Files ],
    FlatKVs = lists:flatten(KVs),
    [application:set_env(basho_bench, Key, Value) || {Key, Value} <- FlatKVs].

%%
%% Expand worker types list into tuple suitable for weighted, random draw
%% TODO: better later if randomized order or randomized draw
%%

workers_tuple() ->
    %% WorkersConfig = basho_bench_config:get(workers, []),
    %% get isn't working at this point in initializaton for some reason but raw get_env does

    WorkersConfig = application:get_env(basho_bench, workers, []),
    case WorkersConfig of
        [] ->
             no_workers;

        _  -> 
            F = fun({WorkerTag, Count}) ->
               lists:duplicate(Count, WorkerTag)
            end,
            Workers = [F(X) || X <- WorkersConfig],
            lists:flatten(Workers)
    end.
