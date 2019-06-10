%% -------------------------------------------------------------------
%%
%% cloudant_bench:basho_value_source_cache
%%
%% Copyright (c) 2019 IBM/Cloudant
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
%%


-module(basho_bench_value_source_cache).


-behavior(gen_server).


-include_lib("basho_bench/include/basho_bench.hrl").


%% Public key generator interface
-export([
    insert_key/1,
    lookup_key/1,
    start_link/0,
    start_link/1,
    stop/0
]).


%% gen_server callbacks
-export([
    code_change/3,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    init/1,
    terminate/2
]).


-spec insert_key({binary(), binary()} | [{binary(), binary()}]) -> ok.
%% This function can insert either a single {Id, Key} or a list of tuples of
%% that format.
insert_key(IdKey) ->
    ok = gen_server:call({global, ?MODULE}, {insert_key, IdKey}).


lookup_key(Key) ->
    {ok, Found} = gen_server:call({global, ?MODULE}, {lookup_key, Key}),
    Found.


start_link() ->
    TableState = ets:new(?MODULE, [set, named_table, public]),
    start_link(TableState).


start_link(TableState) ->
    gen_server:start_link({global, ?MODULE}, ?MODULE, [TableState], []).


stop() ->
    % The server owns the tables, so when we stop the server the
    % tables will be cleaned up.
    gen_server:call({global, ?MODULE}, stop).


init([TableState]) ->
    % The state is a list of table_state records, empty to start.
    SourceSz = basho_bench_config:get(?VAL_GEN_SRC_SIZE, 96*1048576),
    ?INFO("Random source: calling crypto:rand_bytes(~w) (override with the '~w' config option\n", [SourceSz, ?VAL_GEN_SRC_SIZE]),
    Bytes = crypto:strong_rand_bytes(SourceSz),
    try
        insert_key({x, Bytes})
    catch _:_ -> rerunning_id_1_init_source_table_already_exists
    end,
    ?INFO("Random source: finished crypto:rand_bytes(~w)\n", [SourceSz]),
    {ok, TableState}.


handle_call({insert_key, Key}, _From, TableState) ->
    ets:insert(TableState, Key),
    {reply, ok, TableState};
handle_call({lookup_key, Key}, _From, TableState) ->
    [{_, Found}] = ets:lookup(TableState, Key),
    {reply, {ok, Found}, TableState};
handle_call(stop, _From, State) ->
        {stop, normal, shutdown_ok, State}.


handle_cast(Msg, State) ->
    ?ERROR("basho_bench_value_source_cache received cast ~p", [Msg]),
    {noreply, State}.


handle_info(Msg, State) ->
    ?ERROR("basho_bench_value_source_cache received info ~p", [Msg]),
    {noreply, State}.


code_change(_OldVsn, State, _Extra) ->
    ?INFO("basho_bench_value_source_cache code_change called", []),
    {ok, State}.


terminate(_Reason, _State) ->
    ok.
