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

-module(couch_ref_counter).
-behaviour(gen_server).

-include("couch_db.hrl").

-export([start/1, init/1, terminate/2, handle_call/3, handle_cast/2, code_change/3, handle_info/2]).
-export([drop/1,drop/2,add/1,add/2,count/1]).

start(ChildProcs) ->
    gen_server:start(couch_ref_counter, {self(), ChildProcs}, []).

drop(RefCounterPid) ->
    drop(RefCounterPid, self()).

drop(RefCounterPid, Pid) ->
    case
    catch ets:update_counter(couch_ref_counter, {RefCounterPid, Pid}, -1) of
    0 ->
        ets:delete_object(couch_ref_counter, {{RefCounterPid, Pid}, 0}),
        ets:update_counter(couch_ref_counter, {total, RefCounterPid}, -1),
        gen_server:cast(RefCounterPid, {drop, Pid});
    _ ->
        ok
    end.

add(RefCounterPid) ->
    add(RefCounterPid, self()).

add(RefCounterPid, Pid) ->
    case
    catch ets:update_counter(couch_ref_counter, {RefCounterPid, Pid}, 1) of
    1 ->
        gen_server:cast(RefCounterPid, {add, Pid});
    {'EXIT', _} ->
        ets:update_counter(couch_ref_counter, {total, RefCounterPid}, 1),
        ets:insert_new(couch_ref_counter, {{RefCounterPid, Pid}, 0}),
        add(RefCounterPid, Pid);
    _ ->
        ok
    end.

count(RefCounterPid) ->
    ets:lookup_element(couch_ref_counter, {total, RefCounterPid}, 2).

% server functions

-record(srv,
    {
    child_procs=[]
    }).

init({Pid, ChildProcs}) ->
    [link(ChildProc) || ChildProc <- ChildProcs],
    maybe_create_ets_table(),
    ets:insert(couch_ref_counter, {{total, self()}, 1}),
    ets:insert(couch_ref_counter, {{self(), Pid}, 1}),
    ets:insert(couch_ref_counter, {{monitor, self(), Pid},
                                   erlang:monitor(process, Pid)}), 
    {ok, #srv{child_procs=ChildProcs}}.


terminate(_Reason, #srv{child_procs=ChildProcs}) ->
    [couch_util:shutdown_sync(Pid) || Pid <- ChildProcs],
    ets:match_delete(couch_ref_counter, {{total, self()}, '$1'}),
    ets:match_delete(couch_ref_counter, {{self(), '$1'}, '$2'}),
    ets:match_delete(couch_ref_counter, {{monitor, self(), '$1'}, '$2'}),
    ok.

handle_call(Msg, _From, Srv) ->
    exit({unknown_msg,Msg}).

handle_cast({add, Pid}, Srv) ->
    case ets:lookup(couch_ref_counter, {self(), Pid}) of
    [0] ->
        ok;
    _ ->
        case ets:lookup(couch_ref_counter, {monitor, self(), Pid}) of
        [MonRef] ->
            ok;
        _ ->
            ets:insert_new(couch_ref_counter, {{monitor, self(), Pid},
                                               erlang:monitor(process, Pid)})
        end
    end,
    {noreply, Srv};
handle_cast({drop, Pid}, Srv) ->
    case ets:lookup(couch_ref_counter, {self(), Pid}) of
    [0] ->
        MonRef =
            ets:lookup_element(couch_ref_counter, {monitor, self(), Pid}, 2),
        erlang:demonitor(MonRef, [flush]),
        ets:delete(couch_ref_counter, {monitor, self(), Pid});
    _ ->
        ok
    end,

    case should_close() of
    true ->
        {stop, normal, Srv};
    false ->
        {noreply, Srv}
    end;
handle_cast(Msg, _Srv)->
    exit({unknown_msg,Msg}).


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

handle_info({'DOWN', MonRef, _, Pid, _}, Srv) ->
    ets:delete_object(couch_ref_counter, {{monitor, self(), Pid}, MonRef}),
    ets:update_counter(couch_ref_counter, {total, self()}, -1),
    ets:delete(couch_ref_counter, {self(), Pid}),

    case should_close() of
    true ->
        {stop, normal, Srv};
    false ->
        {noreply, Srv}
    end.

maybe_create_ets_table() ->
    case catch ets:new(couch_ref_counter,
                       [public, named_table, {write_concurrency, true}]) of
    couch_ref_counter ->
        catch ets:give_away(couch_ref_counter, whereis(couch_server), ok);
    _ -> ok
    end.

should_close() ->
    % Use ets:delete_object/2 to erase the total count if it is exactly
    % zero. This will cause add/2 and add/3 to fail with badarg.
    % At this point, we can shut down the server safely.
    ets:delete_object(couch_ref_counter, {{total, self()}, 0}),
    case ets:lookup(couch_ref_counter, {total, self()}) of
    [] -> true;
    _ -> false
    end.
