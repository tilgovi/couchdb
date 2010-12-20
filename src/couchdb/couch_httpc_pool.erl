% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.

-module(couch_httpc_pool).
-behaviour(gen_server).

% public API
-export([start_link/4, stop/1]).
-export([get_worker/1]).

% gen_server API
-export([init/1, handle_call/3, handle_info/2, handle_cast/2]).
-export([code_change/3, terminate/2]).

-include("couch_db.hrl").

-record(state, {
    url,
    ssl_options,
    max_connections,
    pipeline_size,
    connections,
    used_connections = 0
}).


start_link(Url, SslOptions, MaxConnections, PipelineSize) ->
    gen_server:start_link(
        ?MODULE, {Url, SslOptions, MaxConnections, PipelineSize}, []).


stop(Pool) ->
    ok = gen_server:call(Pool, stop, infinity).


get_worker(Pool) ->
    gen_server:call(Pool, get_worker, infinity).


init({Url, SslOptions, MaxConnections, PipelineSize}) ->
    process_flag(trap_exit, true),
    State = #state{
        url = Url,
        ssl_options = SslOptions,
        pipeline_size = PipelineSize,
        max_connections = MaxConnections,
        connections = ets:new(httpc_pool, [ordered_set, public])
    },
    {ok, State}.


handle_call(get_worker, _From,
    #state{connections = Conns, max_connections = Max, used_connections = Used,
        url = Url, ssl_options = SslOptions} = State) when Used < Max ->
    {ok, Worker} = ibrowse_http_client:start_link({Conns, Url,
        {SslOptions, SslOptions =/= []}}),
    {reply, {ok, Worker}, State#state{used_connections = Used + 1}};

handle_call(get_worker, _From,
    #state{connections = Conns, pipeline_size = PipeSize} = State) ->
    case ets:first(Conns) of
	{NumSessions, Worker} when NumSessions < PipeSize ->
	    true = ets:delete(Conns, {NumSessions, Worker}),
	    true = ets:insert(Conns, {{NumSessions + 1, Worker}, []}),
        {reply, {ok, Worker}, State};
	_ ->
	    {reply, retry_later, State}
    end;

handle_call(stop, _From, #state{connections = Conns} = State) ->
    ets:foldl(
        fun({{_, W}, _}, _) -> ibrowse_http_client:stop(W) end, ok, Conns),
    {stop, normal, ok, State}.


handle_cast(Msg, State) ->
    {stop, {unexpected_cast, Msg}, State}.


handle_info({'EXIT', Pid, _Reason}, #state{connections = Conns,
        used_connections = Used} = State) ->
    true = ets:match_delete(Conns, {{'_', Pid}, '_'}),
    {noreply, State#state{used_connections = Used - 1}}.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


terminate(_Reason, _State) ->
    ok.
