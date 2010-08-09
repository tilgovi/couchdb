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

-module(couch_db_repair).

-compile(export_all).
-export([repair/1, merge_to_file/2]).

-include("couch_db.hrl").


repair(DbName) ->
    RootDir = couch_config:get("couchdb", "database_dir", "."),
    FullPath = filename:join([RootDir, "./" ++ DbName ++ ".couch"]),
    {ok, Fd} = couch_file:open(FullPath, []),
    Ret = maybe_add_missing_header(Fd),
    couch_file:close(Fd),
    % TODO: log if return value matches {ok, repaired, _BTreeInfos}
    Ret.


maybe_add_missing_header(Fd) ->
    case couch_file:read_header(Fd, [return_pos]) of
    no_valid_header ->
        no_header; % TODO: ignore?
    {ok, Header, HeaderPos} ->
        case find_last_btree_root(Fd, by_seq) of
        {nil, _} ->
            ok;
        {_, NodePos, _, _} = BySeqInfo when NodePos > HeaderPos ->
            ByIdInfo = find_last_btree_root(Fd, by_id),
            add_missing_header(
                Fd, Header,
                sort_btree_infos(ByIdInfo, BySeqInfo)
            );
        _ ->
            ok
        end
     end.


sort_btree_infos({Root1Node, Root1Pos, BTree1Type, LastKey1},
    {Root2Node, Root2Pos, BTree2Type, LastKey2}) ->
    case {BTree1Type, BTree2Type} of
    {by_seq, by_id} ->
        {by_seq, LastKey1, Root1Pos, Root1Node,
            by_id, LastKey2, Root2Pos, Root2Node};
    {by_id, by_seq} ->
        {by_seq, LastKey2, Root2Pos, Root2Node,
            by_id, LastKey1, Root1Pos, Root1Node}
    end.


add_missing_header(Fd, LastHeader, BTreeInfos) ->
    {by_seq, BySeqLastKey, BySeqRootPos, _BySeqRootNode,
        by_id, _ByIdLastKey, ByIdRootPos, _ByIdRootNode} = BTreeInfos,
    {_OldBySeqOffset, OldBySeqRed} =
        LastHeader#db_header.docinfo_by_seq_btree_state,
    {_OldByIdOffset, OldByIdRed} =
        LastHeader#db_header.fulldocinfo_by_id_btree_state,
    NewHeader = LastHeader#db_header{
        update_seq = BySeqLastKey,
        fulldocinfo_by_id_btree_state = {ByIdRootPos, OldByIdRed},
        docinfo_by_seq_btree_state = {BySeqRootPos, OldBySeqRed}
    },
    ok = couch_file:write_header(Fd, NewHeader),
    ok = couch_file:sync(Fd),
    {ok, repaired, BTreeInfos}.


find_last_btree_root(Fd, Type) ->
    {ok, StartPos} = couch_file:bytes(Fd),
    find_last_btree_root(Fd, Type, StartPos).


find_last_btree_root(_Fd, _Type, Pos) when Pos < 0 ->
    {nil, -1};
find_last_btree_root(Fd, BTreeType, Pos) ->
    case couch_file:pread_term(Fd, Pos) of
    {ok, {kv_node, _} = Node} ->
        {Type, LastKey} = btree_type(Pos, Fd),
        case Type of
        BTreeType ->
            {Node, Pos, Type, LastKey};
        _Other ->
            find_last_btree_root(Fd, BTreeType, Pos - 1)
        end;
    {ok, {kp_node, _} = Node} ->
        {Type, LastKey} = btree_type(Pos, Fd),
        case Type of
        BTreeType ->
            {Node, Pos, Type, LastKey};
        _Other ->
            find_last_btree_root(Fd, BTreeType, Pos - 1)
        end;
    _ ->
        find_last_btree_root(Fd, BTreeType, Pos - 1)
    end.


btree_type(NodePos, _Fd) when NodePos < 0 ->
    nil;
btree_type(NodePos, Fd) ->
    {ok, Btree} = couch_btree:open({NodePos, 0}, Fd, []),
    {ok, _, {LastKey, _}} = couch_btree:fold(
        Btree,
        fun(KV, _, _) -> {stop, KV} end,
        ok,
        [{dir, rev}]
    ),
    case key_type(LastKey) of
    integer ->
        {by_seq, LastKey};
    binary ->
        {by_id, LastKey}
    end.


key_type(K) when is_integer(K) ->
    integer;
key_type(K) when is_binary(K) ->
    binary.

merge_to_file(Db, TargetName) ->
    case couch_db:open(TargetName, []) of
    {ok, TargetDb0} ->
        ok;
    {not_found, no_db_file} ->
        {ok, TargetDb0} = couch_db:create(TargetName, [])
    end,
    TargetDb = TargetDb0#db{fsync_options = [before_header]},

    {ok, _, {_, FinalDocs}} =
    couch_btree:fold(Db#db.docinfo_by_seq_btree, fun(DocInfo, _, {I, Acc}) ->
        #doc_info{id=Id, revs = RevsInfo} = DocInfo,
        LeafRevs = [Rev || #rev_info{rev=Rev} <- RevsInfo],
        {ok, Docs} = couch_db:open_doc_revs(Db, Id, LeafRevs, [latest]),
        if I > 1000 ->
            couch_db:update_docs(TargetDb, [Doc || {ok, Doc} <- Acc],
                [full_commit], replicated_changes),
            {ok, {length(Docs), Docs}};
        true ->
            {ok, {I+length(Docs), Docs ++ Acc}}
        end
    end, {0, []}, []),
    couch_db:update_docs(TargetDb, [Doc || {ok, Doc} <- FinalDocs],
        [full_commit], replicated_changes),
    couch_db:close(TargetDb).
