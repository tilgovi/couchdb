#! /usr/bin/env escript

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

filename() ->
    list_to_binary(test_util:source_file("test/etap/171-os-daemons-config.es")).

read() ->
    case io:get_line('') of
        eof ->
            stop;
        Data ->
            couch_util:json_decode(Data)
    end.

write(Mesg) ->
    Data = iolist_to_binary(couch_util:json_encode(Mesg)),
    io:format(binary_to_list(Data) ++ "\n", []).

get_cfg(Section) ->
    write([<<"get">>, Section]),
    read().

get_cfg(Section, Name) ->
    write([<<"get">>, Section, Name]),
    read().

log(Mesg) ->
    write([<<"log">>, Mesg]).

test_get_cfg1() ->
    FileName = filename(),
    {[{<<"foo">>, FileName}]} = get_cfg(<<"os_daemons">>).

test_get_cfg2() ->
    FileName = filename(),
    FileName = get_cfg(<<"os_daemons">>, <<"foo">>),
    <<"sequential">> = get_cfg(<<"uuids">>, <<"algorithm">>).

test_log() ->
    log(<<"foobar!">>).

loop() ->
    timer:sleep(5000),
    loop().

do_tests() ->
    test_get_cfg1(),
    test_get_cfg2(),
    test_log(),
    loop().

main([]) ->
    test_util:init_code_path(),
    do_tests().
