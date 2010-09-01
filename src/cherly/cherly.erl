%%%-------------------------------------------------------------------
%%% File:      cherly.erl
%%% @author    Cliff Moon <cliff@moonpolysoft.com> []
%%% @copyright 2009 Cliff Moon See LICENSE file
%%% @doc  
%%%
%%% @end  
%%%
%%% @since 2009-02-22 by Cliff Moon
%%%-------------------------------------------------------------------
-module(cherly).
-author('cliff@moonpolysoft.com').

-export([start/1, put/3, get/2, remove/2, size/1, items/1, stop/1]).

-define(INIT, $i).
-define(GET, $g).
-define(PUT, $p).
-define(REMOVE, $r).
-define(SIZE, $s).
-define(ITEMS, $t).

-ifdef(TEST).
-include("test_cherly.erl").
-endif.

%% api bitches

start(Size) ->
  case load_driver() of
    ok -> 
      P = open_port({spawn, 'cherly_drv'}, [binary]),
      port_command(P, [?INIT, term_to_binary({0, Size})]),
      {ok, {cherly, P}};
    {error, Err} ->
      Msg = erl_ddll:format_error(Err),
      {error, Msg}
  end.
  
put({cherly, P}, Key, Value) when is_list(Value) ->
  % error_logger:info_msg("key ~p value ~p~n", [Key, Value]),
  Values = lists:flatten(Value),
  Len = length(Key),
  ValLen = length(Values),
  SizeBin = << <<S:32>> || S <- [byte_size(Bin) || Bin <- Values] >>,
  Preamble = <<ValLen:32, SizeBin/binary>>,
  port_command(P, [?PUT, <<Len:32>>, Key, Preamble, Values]);
  
put({cherly, P}, Key, Value) ->
  Len = length(Key),
  Preamble = <<0:32>>,
  % error_logger:info_msg("key ~p value ~p", [Key, Value]),
  port_command(P, [?PUT, <<Len:32>>, Key, Preamble, Value]).
  
get({cherly, P}, Key) ->
  Len = length(Key),
  port_command(P, [?GET, <<Len:32>>, Key]),
  receive
    {P, {data, BinList}} -> 
      % error_logger:info_msg("key ~p BinList ~p", [Key, BinList]),
      unpack(BinList, length(Key)+5);
    {P, So} -> So
  end.
  
remove({cherly, P}, Key) ->
  Len = length(Key),
  port_command(P, [?REMOVE, <<Len:32>>, Key]).
  
size({cherly, P}) ->
  port_command(P, [?SIZE]),
  receive
    {P, {data, Bin}} -> binary_to_term(Bin)
  end.
  
items({cherly, P}) ->
  port_command(P, [?ITEMS]),
  receive
    {P, {data, Bin}} -> binary_to_term(Bin)
  end.
  
stop({cherly, P}) ->
  unlink(P),
  port_close(P).
%%====================================================================
%% Internal functions
%%====================================================================
  
load_driver() ->
  Dir = filename:join([code:priv_dir(cherly), ".libs"]),
  erl_ddll:load(Dir, "cherly_drv").
  
% thanks erlang for fucking with the binaries passed into outputv
unpack(Bin, SkipSize) ->
  % ?debugFmt("bin ~p", [Bin]),
  [PreFirst|BinList] = normalize_tarded_binlist(Bin),
  <<_:SkipSize/binary, First/binary>> = PreFirst, %we need to do the skip here because outputv modifies the iovec
  <<ValLen:32, RestFirst/binary>> = First,
  if
    ValLen > 0 -> 
      SizeLen = ValLen * 4,
      <<SizesBin:SizeLen/binary, LeftOver/binary>> = RestFirst,
      Sizes = [ Size || <<Size:32>> <= SizesBin ],
      {ok, unpack(Sizes, [LeftOver|BinList], [])};
    byte_size(RestFirst) == 0 ->
      [B] = BinList,
      {ok, B};
    true ->
      {ok, RestFirst}
  end.
  
% unpack([], _, [Acc]) -> Acc;
  
unpack([], _, Acc) -> lists:reverse(Acc);

unpack(Sizes, [Bin|BinList], Acc) when byte_size(Bin) == 0 -> %discard your empties
  unpack(Sizes, BinList, Acc);

unpack([Size|Sizes], [Bin|BinList], Acc) ->
  if
    Size < byte_size(Bin) ->
      <<Ext:Size/binary, Rest/binary>> = Bin,
      unpack(Sizes, [Rest|BinList], [Ext|Acc]);
    true ->
      unpack(Sizes, BinList, [Bin|Acc])
  end.
  
normalize_tarded_binlist(List) ->
  normalize_tarded_binlist(List, []).
  
normalize_tarded_binlist(Bin, Acc) when not is_list(Bin) -> lists:reverse([Bin|Acc]);
  
normalize_tarded_binlist([Bin|Rest], Acc) ->
  normalize_tarded_binlist(Rest, [Bin|Acc]).
