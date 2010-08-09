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

-module('couch_db_repair_b').

-compile(export_all).
-export([repair/1]).

-include("couch_db.hrl").

% couch_db_repair_b:repair("gnis").


repair(DbName) ->
    ?LOG_DEBUG("Hello World!~p", [DbName]),
    RootDir = couch_config:get("couchdb", "database_dir", "."),
    FullPath = filename:join([RootDir, "./" ++ DbName ++ ".couch"]),
    {ok, Fd} = couch_file:open(FullPath, []),
    RootSet = go_backwards(Fd, by_id),
    couch_file:close(Fd),
    sets:to_list(RootSet).

go_backwards(Fd, Type) ->
    {ok, EOF} = couch_file:bytes(Fd),
    go_backwards(Fd, Type, EOF, sets:new()).

go_backwards(_Fd, _Type, Pos, Acc) when Pos < 0->
    Acc;

go_backwards(Fd, Type, Pos, Acc) ->
    ?LOG_DEBUG("go_backwards: ~p", [Pos]),
    case find_last_btree_root(Fd, Type, Pos) of
    {nil, -1} ->
        Acc;
    % Key-Value nodes might also be root nodes, but don't contain any pointers
    % to other nodes => we can't eliminate positions from the Acc
    {{kv_node, Contents}, Pos2, _Type, _LastKey} ->
        %?LOG_DEBUG("go_backwards: Node: ~p", [Node]),
        go_backwards(Fd, Type, Pos2-1, sets:add_element(Pos2, Acc));
    {{kp_node, Children}, Pos2, _Type, _LastKey} ->
        %?LOG_DEBUG("go_backwards: Children: ~p", [Children]),
        ChildrenPos = sets:from_list([P || {_DocId, {P, _}} <- Children]),
        %?LOG_DEBUG("go_backwards: ChildrenPos: ~p", [ChildrenPos]),
        % Remove all positions from Acc that have pointers (Children) in the
        % current node
        Acc2 = sets:subtract(Acc, ChildrenPos),
        go_backwards(Fd, Type, Pos2-1, Acc2)
    end.


find_last_btree_root(Fd, Type) ->
    {ok, StartPos} = couch_file:bytes(Fd),
    find_last_btree_root(Fd, Type, StartPos).


find_last_btree_root(_Fd, _Type, Pos) when Pos < 0 ->
    {nil, -1};
find_last_btree_root(Fd, BTreeType, Pos) ->
    %?LOG_DEBUG("find_last_btree_root: ~p", [Pos]),
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
    {error, invalid_iolist} ->
        find_last_btree_root(Fd, BTreeType, Pos - 1);
    {error, invalid_term} ->
        find_last_btree_root(Fd, BTreeType, Pos - 1);
    Otherwise ->
        %?LOG_DEBUG("find_last_btree_root: I just pread_term: ~p", [Otherwise]),
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

