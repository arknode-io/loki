-module(loki_utils).

-define(FRAG_FACTOR, 86400).
-define(SLOT_FACTOR, 300).

-export([timestamp/0,
         frag/1,
         slot/1]).

timestamp() ->
  erlang:system_time(second).

frag(Timestamp) ->
  Timestamp div ?FRAG_FACTOR.

slot(Timestamp) ->
  Timestamp div ?SLOT_FACTOR.
