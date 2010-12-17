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

-module(couch_replicator_utils).

-export([parse_rep_doc/2]).
-export([update_rep_doc/2]).
-export([ensure_rep_db_exists/0]).

-include("couch_db.hrl").
-include("couch_api_wrap.hrl").
-include("couch_js_functions.hrl").
-include("../ibrowse/ibrowse.hrl").

-import(couch_util, [
    get_value/2,
    get_value/3
]).


parse_rep_doc({Props} = RepObj, UserCtx) ->
    ProxyParams = parse_proxy_params(get_value(<<"proxy">>, Props, <<>>)),
    Source = parse_rep_db(get_value(<<"source">>, Props), ProxyParams),
    Target = parse_rep_db(get_value(<<"target">>, Props), ProxyParams),
    Options = make_options(Props),
    Rep = #rep{
        id = make_replication_id(Source, Target, UserCtx, Options),
        source = Source,
        target = Target,
        options = Options,
        user_ctx = UserCtx,
        doc = RepObj
    },
    {ok, Rep}.


update_rep_doc({Props} = _RepDoc, KVs) ->
    case get_value(<<"_id">>, Props) of
    undefined ->
        ok;
    RepDocId ->
        {ok, RepDb} = ensure_rep_db_exists(),
        case couch_db:open_doc(RepDb, RepDocId, []) of
        {ok, LatestRepDoc} ->
            update_rep_doc(RepDb, LatestRepDoc, KVs);
        _ ->
            ok
        end,
        couch_db:close(RepDb)
    end.

update_rep_doc(RepDb, #doc{body = {RepDocBody}} = RepDoc, KVs) ->
    NewRepDocBody = lists:foldl(
        fun({<<"_replication_state">> = K, _V} = KV, Body) ->
                Body1 = lists:keystore(K, 1, Body, KV),
                {Mega, Secs, _} = erlang:now(),
                UnixTime = Mega * 1000000 + Secs,
                lists:keystore(
                    <<"_replication_state_time">>, 1,
                    Body1, {<<"_replication_state_time">>, UnixTime});
            ({K, _V} = KV, Body) ->
                lists:keystore(K, 1, Body, KV)
        end,
        RepDocBody,
        KVs
    ),
    % might not succeed - when the replication doc is deleted right
    % before this update (not an error)
    couch_db:update_doc(
        RepDb,
        RepDoc#doc{body = {NewRepDocBody}},
        []).


ensure_rep_db_exists() ->
    DbName = ?l2b(couch_config:get("replicator", "db", "_replicator")),
    Opts = [
        {user_ctx, #user_ctx{roles=[<<"_admin">>, <<"_replicator">>]}},
        sys_db
    ],
    case couch_db:open(DbName, Opts) of
    {ok, Db} ->
        Db;
    _Error ->
        {ok, Db} = couch_db:create(DbName, Opts)
    end,
    ok = ensure_rep_ddoc_exists(Db, <<"_design/_replicator">>),
    {ok, Db}.


ensure_rep_ddoc_exists(RepDb, DDocID) ->
    case couch_db:open_doc(RepDb, DDocID, []) of
    {ok, _Doc} ->
        ok;
    _ ->
        DDoc = couch_doc:from_json_obj({[
            {<<"_id">>, DDocID},
            {<<"language">>, <<"javascript">>},
            {<<"validate_doc_update">>, ?REP_DB_DOC_VALIDATE_FUN}
        ]}),
        {ok, _Rev} = couch_db:update_doc(RepDb, DDoc, [])
    end,
    ok.


make_replication_id(Source, Target, UserCtx, Options) ->
    %% funky algorithm to preserve backwards compatibility
    {ok, HostName} = inet:gethostname(),
    % Port = mochiweb_socket_server:get(couch_httpd, port),
    Src = get_rep_endpoint(UserCtx, Source),
    Tgt = get_rep_endpoint(UserCtx, Target),
    Base = [HostName, Src, Tgt] ++
        case get_value(filter, Options) of
        undefined ->
            case get_value(doc_ids, Options) of
            undefined ->
                [];
            DocIds ->
                [DocIds]
            end;
        Filter ->
            [filter_code(Filter, Source, UserCtx),
                get_value(query_params, Options, {[]})]
        end,
    Extension = maybe_append_options([continuous, create_target], Options),
    {couch_util:to_hex(couch_util:md5(term_to_binary(Base))), Extension}.


filter_code(Filter, Source, UserCtx) ->
    {match, [DDocName, FilterName]} =
        re:run(Filter, "(.*?)/(.*)", [{capture, [1, 2], binary}]),
    {ok, Db} = couch_api_wrap:db_open(Source, [{user_ctx, UserCtx}]),
    try
        {ok, #doc{body = Body}} =
            couch_api_wrap:open_doc(Db, <<"_design/", DDocName/binary>>, []),
        Code = couch_util:get_nested_json_value(
            Body, [<<"filters">>, FilterName]),
        re:replace(Code, [$^, "\s*(.*?)\s*", $$], "\\1", [{return, binary}])
    after
        couch_api_wrap:db_close(Db)
    end.


maybe_append_options(Options, RepOptions) ->
    lists:foldl(fun(Option, Acc) ->
        Acc ++
        case get_value(Option, RepOptions, false) of
        true ->
            "+" ++ atom_to_list(Option);
        false ->
            ""
        end
    end, [], Options).


get_rep_endpoint(_UserCtx, #httpdb{url=Url, headers=Headers, oauth=OAuth}) ->
    case OAuth of
    nil ->
        {remote, Url, Headers};
    #oauth{} ->
        {remote, Url, Headers, OAuth}
    end;
get_rep_endpoint(UserCtx, <<DbName/binary>>) ->
    {local, DbName, UserCtx}.


parse_rep_db({Props}, ProxyParams) ->
    Url = maybe_add_trailing_slash(get_value(<<"url">>, Props)),
    {AuthProps} = get_value(<<"auth">>, Props, {[]}),
    {BinHeaders} = get_value(<<"headers">>, Props, {[]}),
    Headers = lists:ukeysort(1, [{?b2l(K), ?b2l(V)} || {K, V} <- BinHeaders]),
    DefaultHeaders = (#httpdb{})#httpdb.headers,
    OAuth = case get_value(<<"oauth">>, AuthProps) of
    undefined ->
        nil;
    {OauthProps} ->
        #oauth{
            consumer_key = ?b2l(get_value(<<"consumer_key">>, OauthProps)),
            token = ?b2l(get_value(<<"token">>, OauthProps)),
            token_secret = ?b2l(get_value(<<"token_secret">>, OauthProps)),
            consumer_secret = ?b2l(get_value(<<"consumer_secret">>, OauthProps)),
            signature_method =
                case get_value(<<"signature_method">>, OauthProps) of
                undefined ->        hmac_sha1;
                <<"PLAINTEXT">> ->  plaintext;
                <<"HMAC-SHA1">> ->  hmac_sha1;
                <<"RSA-SHA1">> ->   rsa_sha1
                end
        }
    end,
    #httpdb{
        url = Url,
        oauth = OAuth,
        headers = lists:ukeymerge(1, Headers, DefaultHeaders),
        proxy_options = ProxyParams,
        ssl_options = ssl_params(Url)
    };
parse_rep_db(<<"http://", _/binary>> = Url, ProxyParams) ->
    parse_rep_db({[{<<"url">>, Url}]}, ProxyParams);
parse_rep_db(<<"https://", _/binary>> = Url, ProxyParams) ->
    parse_rep_db({[{<<"url">>, Url}]}, ProxyParams);
parse_rep_db(<<DbName/binary>>, _ProxyParams) ->
    DbName.


maybe_add_trailing_slash(Url) when is_binary(Url) ->
    maybe_add_trailing_slash(?b2l(Url));
maybe_add_trailing_slash(Url) ->
    case lists:last(Url) of
    $/ ->
        Url;
    _ ->
        Url ++ "/"
    end.


make_options(Props) ->
    Options = lists:ukeysort(1, convert_options(Props)),
    DefWorkers = couch_config:get("replicator", "worker_processes", "5"),
    DefBatchSize = couch_config:get("replicator", "worker_batch_size", "1000"),
    DefConns = couch_config:get("replicator", "worker_max_connections", "25"),
    lists:ukeymerge(1, Options, [
        {worker_batch_size, list_to_integer(DefBatchSize)},
        {worker_max_connections, list_to_integer(DefConns)},
        {worker_processes, list_to_integer(DefWorkers)}
    ]).


convert_options([])->
    [];
convert_options([{<<"cancel">>, V} | R]) ->
    [{cancel, V} | convert_options(R)];
convert_options([{<<"create_target">>, V} | R]) ->
    [{create_target, V} | convert_options(R)];
convert_options([{<<"continuous">>, V} | R]) ->
    [{continuous, V} | convert_options(R)];
convert_options([{<<"filter">>, V} | R]) ->
    [{filter, V} | convert_options(R)];
convert_options([{<<"query_params">>, V} | R]) ->
    [{query_params, V} | convert_options(R)];
convert_options([{<<"doc_ids">>, V} | R]) ->
    % Ensure same behaviour as old replicator: accept a list of percent
    % encoded doc IDs.
    DocIds = [?l2b(couch_httpd:unquote(Id)) || Id <- V],
    [{doc_ids, DocIds} | convert_options(R)];
convert_options([{<<"worker_processes">>, V} | R]) ->
    [{worker_processes, couch_util:to_integer(V)} | convert_options(R)];
convert_options([{<<"worker_batch_size">>, V} | R]) ->
    [{worker_batch_size, couch_util:to_integer(V)} | convert_options(R)];
convert_options([{<<"worker_max_connections">>, V} | R]) ->
    [{worker_max_connections, couch_util:to_integer(V)} | convert_options(R)];
convert_options([_ | R]) -> % skip unknown option
    convert_options(R).


parse_proxy_params(ProxyUrl) when is_binary(ProxyUrl) ->
    parse_proxy_params(?b2l(ProxyUrl));
parse_proxy_params([]) ->
    [];
parse_proxy_params(ProxyUrl) ->
    #url{
        host = Host,
        port = Port,
        username = User,
        password = Passwd
    } = ibrowse_lib:parse_url(ProxyUrl),
    [{proxy_host, Host}, {proxy_port, Port}] ++
        case is_list(User) andalso is_list(Passwd) of
        false ->
            [];
        true ->
            [{proxy_user, User}, {proxy_password, Passwd}]
        end.


ssl_params(Url) ->
    case ibrowse_lib:parse_url(Url) of
    #url{protocol = https} ->
        Depth = list_to_integer(
            couch_config:get("replicator", "ssl_certificate_max_depth", "3")
        ),
        VerifyCerts = couch_config:get("replicator", "verify_ssl_certificates"),
        SslOpts = [{depth, Depth} | ssl_verify_options(VerifyCerts =:= "true")],
        [{is_ssl, true}, {ssl_options, SslOpts}];
    #url{protocol = http} ->
        []
    end.

ssl_verify_options(Value) ->
    ssl_verify_options(Value, erlang:system_info(otp_release)).

ssl_verify_options(true, OTPVersion) when OTPVersion >= "R14" ->
    CAFile = couch_config:get("replicator", "ssl_trusted_certificates_file"),
    [{verify, verify_peer}, {cacertfile, CAFile}];
ssl_verify_options(false, OTPVersion) when OTPVersion >= "R14" ->
    [{verify, verify_none}];
ssl_verify_options(true, _OTPVersion) ->
    CAFile = couch_config:get("replicator", "ssl_trusted_certificates_file"),
    [{verify, 2}, {cacertfile, CAFile}];
ssl_verify_options(false, _OTPVersion) ->
    [{verify, 0}].
