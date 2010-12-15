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

-module(couch_replicator_rev_finders).

-export([spawn_missing_rev_finders/5]).

-include("couch_db.hrl").



spawn_missing_rev_finders(Target, ChangesQueue, MissingRevsQueue,
    RevFindersCount, BatchSize) ->
    lists:map(
        fun(_) ->
            spawn_link(fun() ->
                missing_revs_finder_loop(
                    Target, ChangesQueue, MissingRevsQueue, BatchSize)
            end)
        end, lists:seq(1, RevFindersCount)).


missing_revs_finder_loop(Target, ChangesQueue, RevsQueue, BatchSize) ->
    case couch_work_queue:dequeue(ChangesQueue, BatchSize) of
    closed ->
        ok;
    {ok, DocInfos} ->
        IdRevs = [{Id, [Rev || #rev_info{rev = Rev} <- RevsInfo]} ||
                    #doc_info{id = Id, revs = RevsInfo} <- DocInfos],
        Target2 = reopen_db(Target),
        {ok, Missing} = couch_api_wrap:get_missing_revs(Target2, IdRevs),
        queue_missing_revs(Missing, DocInfos, RevsQueue),
        missing_revs_finder_loop(Target2, ChangesQueue, RevsQueue, BatchSize)
    end.


queue_missing_revs(Missing, DocInfos, Queue) ->
    IdRevsSeqDict = dict:from_list(
        [{Id, {[Rev || #rev_info{rev = Rev} <- RevsInfo], Seq}} ||
            #doc_info{id = Id, revs = RevsInfo, high_seq = Seq} <- DocInfos]),
    AllDict = lists:foldl(
        fun({Id, MissingRevs, PAs}, Acc) ->
            {_, Seq} = dict:fetch(Id, IdRevsSeqDict),
            dict:store(Seq, {Id, MissingRevs, 0, PAs}, Acc)
        end,
        dict:new(), Missing),
    AllDict2 = dict:fold(
        fun(Id, {NotMissingRevs, Seq}, Acc) ->
            case dict:find(Seq, Acc) of
            error ->
                dict:store(Seq, {Id, [], length(NotMissingRevs), []}, Acc);
            {ok, {Id, MissingRevs, NotMissingCount, PAs}} ->
                NotMissingCount2 = NotMissingCount + length(NotMissingRevs),
                dict:store(Seq, {Id, MissingRevs, NotMissingCount2, PAs}, Acc)
            end
        end,
        AllDict, non_missing(IdRevsSeqDict, Missing)),
    ok = couch_work_queue:queue(Queue, lists:keysort(1, dict:to_list(AllDict2))).


non_missing(NonMissingDict, []) ->
    NonMissingDict;
non_missing(IdRevsSeqDict, [{MissingId, MissingRevs, _} | Rest]) ->
    {AllRevs, Seq} = dict:fetch(MissingId, IdRevsSeqDict),
    case AllRevs -- MissingRevs of
    [] ->
        non_missing(dict:erase(MissingId, IdRevsSeqDict), Rest);
    NotMissing ->
        non_missing(
            dict:store(MissingId, {NotMissing, Seq}, IdRevsSeqDict),
            Rest)
    end.


reopen_db(#db{main_pid = Pid, user_ctx = UserCtx}) ->
    {ok, NewDb} = gen_server:call(Pid, get_db, infinity),
    NewDb#db{user_ctx = UserCtx};
reopen_db(HttpDb) ->
    HttpDb.
