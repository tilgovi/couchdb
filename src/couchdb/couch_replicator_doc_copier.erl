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

-module(couch_replicator_doc_copier).
-behaviour(gen_server).

% public API
-export([start_link/5]).

% gen_server callbacks
-export([init/1, terminate/2, code_change/3]).
-export([handle_call/3, handle_cast/2, handle_info/2]).

-include("couch_db.hrl").
-include("couch_api_wrap.hrl").

-define(DOC_BUFFER_BYTE_SIZE, 1024 * 1024). % for remote targets
-define(DOC_BUFFER_LEN, 100).               % for local targets, # of documents
-define(MAX_BULK_ATT_SIZE, 64 * 1024).
-define(MAX_BULK_ATTS_PER_DOC, 8).

-record(state, {
    loop,
    cp,
    max_parallel_conns,
    source,
    target,
    docs = [],
    size_docs = 0,
    readers = [],
    writer = nil,
    pending_fetch = nil,
    flush_waiter = nil,
    highest_seq_seen = ?LOWEST_SEQ,
    stats = #rep_stats{}
}).



start_link(Cp, Source, Target, MissingRevsQueue, MaxConns) ->
    gen_server:start_link(
        ?MODULE, {Cp, Source, Target, MissingRevsQueue, MaxConns}, []).


init({Cp, Source, Target, MissingRevsQueue, MaxConns}) ->
    process_flag(trap_exit, true),
    Parent = self(),
    LoopPid = spawn_link(
        fun() -> queue_fetch_loop(Parent, MissingRevsQueue) end
    ),
    State = #state{
        cp = Cp,
        max_parallel_conns = MaxConns,
        loop = LoopPid,
        source = Source,
        target = Target
    },
    {ok, State}.


handle_call({seq_done, Seq, RevCount}, {Pid, _},
    #state{loop = Pid, highest_seq_seen = HighSeq, stats = Stats} = State) ->
    NewState = State#state{
        highest_seq_seen = lists:max([Seq, HighSeq]),
        stats = Stats#rep_stats{
            missing_checked = Stats#rep_stats.missing_checked + RevCount
        }
    },
    {reply, ok, NewState};

handle_call({fetch_doc, {_Id, Revs, _PAs, Seq} = Params}, {Pid, _} = From,
    #state{loop = Pid, readers = Readers, pending_fetch = nil,
        highest_seq_seen = HighSeq, stats = Stats,
        max_parallel_conns = MaxConns} = State) ->
    Stats2 = Stats#rep_stats{
        missing_checked = Stats#rep_stats.missing_checked + length(Revs),
        missing_found = Stats#rep_stats.missing_found + length(Revs)
    },
    case length(Readers) of
    Size when Size < MaxConns ->
        {Fetcher, Source2} = spawn_doc_reader(State#state.source, Params),
        NewState = State#state{
            highest_seq_seen = lists:max([Seq, HighSeq]),
            stats = Stats2,
            source = Source2,
            readers = [Fetcher | Readers]
        },
        {reply, ok, NewState};
    _ ->
        NewState = State#state{
            highest_seq_seen = lists:max([Seq, HighSeq]),
            stats = Stats2,
            pending_fetch = {From, Params}
        },
        {noreply, NewState}
    end;

handle_call({add_doc, Doc}, _From, State) ->
    {reply, ok, maybe_flush_docs(Doc, State)};

handle_call({add_write_stats, Written, Failed}, _From,
    #state{stats = Stats} = State) ->
    NewStats = Stats#rep_stats{
        docs_written = Stats#rep_stats.docs_written + Written,
        doc_write_failures = Stats#rep_stats.doc_write_failures + Failed
    },
    {reply, ok, State#state{stats = NewStats}};

handle_call(flush, {Pid, _} = From,
    #state{loop = Pid, writer = nil, flush_waiter = nil,
        target = Target, docs = DocAcc} = State) ->
    State2 = case State#state.readers of
    [] ->
        {Target2, Writer} = spawn_writer(Target, DocAcc),
        State#state{writer = Writer, target = Target2};
    _ ->
        State
    end,
    {noreply, State2#state{flush_waiter = From}}.


handle_cast(Msg, State) ->
    {stop, {unexpected_async_call, Msg}, State}.


handle_info({'EXIT', Pid, normal}, #state{loop = Pid} = State) ->
    #state{
        docs = [], readers = [], writer = nil,
        pending_fetch = nil, flush_waiter = nil
    } = State,
    {stop, normal, State};

handle_info({'EXIT', Pid, normal},
    #state{writer = Pid, stats = Stats} = State) ->
    NewState = after_full_flush(State),
    ?LOG_DEBUG("Replication copy process, read ~p documents, wrote ~p documents",
        [Stats#rep_stats.docs_read, Stats#rep_stats.docs_written]),
    {noreply, NewState};

handle_info({'EXIT', Pid, normal}, #state{writer = nil} = State) ->
    #state{
        readers = Readers, writer = Writer, docs = Docs,
        source = Source, target = Target,
        pending_fetch = Fetch, flush_waiter = FlushWaiter
    } = State,
    case Readers -- [Pid] of
    Readers ->
        {noreply, State};
    Readers2 ->
        State2 = case Fetch of
        nil ->
            case (FlushWaiter =/= nil) andalso (Writer =:= nil) andalso
                (Readers2 =:= [])  of
            true ->
                {Target2, Writer2} = spawn_writer(Target, Docs),
                State#state{
                    readers = Readers2,
                    writer = Writer2,
                    target = Target2
                };
            false ->
                State#state{readers = Readers2}
            end;
        {From, FetchParams} ->
            {Fetcher, Source2} = spawn_doc_reader(Source, FetchParams),
            gen_server:reply(From, ok),
            State#state{
                source = Source2,
                readers = [Fetcher | Readers2],
                pending_fetch = nil
            }
        end,
        {noreply, State2}
    end;

handle_info({'EXIT', Pid, Reason}, State) ->
   {stop, {process_died, Pid, Reason}, State}.


terminate(_Reason, _State) ->
    ok.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


queue_fetch_loop(Parent, MissingRevsQueue) ->
    case couch_work_queue:dequeue(MissingRevsQueue, 1) of
    closed ->
        ok;
    {ok, [IdRevs]} ->
        lists:foreach(
            fun({Seq, {Id, Revs, NotMissingCount, PAs}}) ->
                case NotMissingCount > 0 of
                true ->
                    ok = gen_server:call(
                        Parent, {seq_done, Seq, NotMissingCount}, infinity);
                false ->
                    ok
                end,
                lists:foreach(
                    fun(R) ->
                        ok = gen_server:call(
                            Parent, {fetch_doc, {Id, [R], PAs, Seq}}, infinity)
                    end,
                    Revs)
            end,
            IdRevs),
        ok = gen_server:call(Parent, flush, infinity),
        queue_fetch_loop(Parent, MissingRevsQueue)
    end.


spawn_doc_reader(Source, FetchParams) ->
    Source2 = reopen_db(Source),
    Parent = self(),
    Pid = spawn_link(fun() -> fetch_doc(Parent, Source2, FetchParams) end),
    {Pid, Source2}.


fetch_doc(Parent, Source, {Id, Revs, PAs, _Seq}) ->
    couch_api_wrap:open_doc_revs(
        Source, Id, Revs, [{atts_since, PAs}], fun doc_handler/2, Parent).


doc_handler({ok, Doc}, Parent) ->
    ok = gen_server:call(Parent, {add_doc, Doc}, infinity),
    Parent;
doc_handler(_, Parent) ->
    Parent.


spawn_writer(Target, DocList) ->
    Target2 = reopen_db(Target),
    Parent = self(),
    Pid = spawn_link(
        fun() ->
            {Written, Failed} = flush_docs(Target2, DocList),
            ok = gen_server:call(
                Parent, {add_write_stats, Written, Failed}, infinity)
        end),
    {Target2, Pid}.


after_full_flush(#state{cp = Cp, stats = Stats, flush_waiter = Waiter,
        highest_seq_seen = HighSeqDone} = State) ->
    ok = gen_server:cast(Cp, {report_seq_done, HighSeqDone, Stats}),
    gen_server:reply(Waiter, ok),
    State#state{
        stats = #rep_stats{},
        flush_waiter = nil,
        writer = nil,
        docs = [],
        size_docs = 0,
        highest_seq_seen = ?LOWEST_SEQ
    }.


maybe_flush_docs(Doc, #state{target = Target, docs = DocAcc,
        size_docs = SizeAcc, stats = Stats} = State) ->
    Target2 = reopen_db(Target),
    {DocAcc2, SizeAcc2, W, F} = maybe_flush_docs(Target2, DocAcc, SizeAcc, Doc),
    Stats2 = Stats#rep_stats{
        docs_read = Stats#rep_stats.docs_read + 1,
        docs_written = Stats#rep_stats.docs_written + W,
        doc_write_failures = Stats#rep_stats.doc_write_failures + F
    },
    State#state{
        target = Target2,
        stats = Stats2,
        docs = DocAcc2,
        size_docs = SizeAcc2
    }.


maybe_flush_docs(#httpdb{} = Target, DocAcc, SizeAcc, #doc{atts = Atts} = Doc) ->
    case (length(Atts) > ?MAX_BULK_ATTS_PER_DOC) orelse
        lists:any(
            fun(A) -> A#att.disk_len > ?MAX_BULK_ATT_SIZE end, Atts) of
    true ->
        {Written, Failed} = flush_docs(Target, Doc),
        {DocAcc, SizeAcc, Written, Failed};
    false ->
        JsonDoc = iolist_to_binary(
            ?JSON_ENCODE(couch_doc:to_json_obj(Doc, [revs, attachments]))),
        case SizeAcc + byte_size(JsonDoc) of
        SizeAcc2 when SizeAcc2 > ?DOC_BUFFER_BYTE_SIZE ->
            {Written, Failed} = flush_docs(Target, [JsonDoc | DocAcc]),
            {[], 0, Written, Failed};
        SizeAcc2 ->
            {[JsonDoc | DocAcc], SizeAcc2, 0, 0}
        end
    end;

maybe_flush_docs(#db{} = Target, DocAcc, SizeAcc, #doc{atts = []} = Doc) ->
    case SizeAcc + 1 of
    SizeAcc2 when SizeAcc2 >= ?DOC_BUFFER_LEN ->
        {Written, Failed} = flush_docs(Target, [Doc | DocAcc]),
        {[], 0, Written, Failed};
    SizeAcc2 ->
        {[Doc | DocAcc], SizeAcc2, 0, 0}
    end;

maybe_flush_docs(#db{} = Target, DocAcc, SizeAcc, Doc) ->
    {Written, Failed} = flush_docs(Target, Doc),
    {DocAcc, SizeAcc, Written, Failed}.


flush_docs(Target, DocList) when is_list(DocList) ->
    {ok, Errors} = couch_api_wrap:update_docs(
        Target, DocList, [delay_commit], replicated_changes),
    DbUri = couch_api_wrap:db_uri(Target),
    lists:foreach(
        fun({[ {<<"id">>, Id}, {<<"error">>, <<"unauthorized">>} ]}) ->
                ?LOG_ERROR("Replicator: unauthorized to write document"
                    " `~s` to `~s`", [Id, DbUri]);
            (_) ->
                ok
        end, Errors),
    {length(DocList) - length(Errors), length(Errors)};

flush_docs(Target, Doc) ->
    case couch_api_wrap:update_doc(Target, Doc, [], replicated_changes) of
    {ok, _} ->
        {1, 0};
    {error, <<"unauthorized">>} ->
        ?LOG_ERROR("Replicator: unauthorized to write document `~s` to `~s`",
            [Doc#doc.id, couch_api_wrap:db_uri(Target)]),
        {0, 1};
    _ ->
        {0, 1}
    end.


reopen_db(#db{main_pid = Pid, user_ctx = UserCtx}) ->
    {ok, NewDb} = gen_server:call(Pid, get_db, infinity),
    NewDb#db{user_ctx = UserCtx};
reopen_db(HttpDb) ->
    HttpDb.
