loki
=====

An OTP application

Build
-----

    $ rebar3 compile

Frag
{Uuid, Slot} -> <<Mark:16, Event:16, Size:8, Type:8, Data:Custom>>
                <<30:16, 4:16, 0:16>>
                <<30:16, 10:16, 3:8, 2:8, "123"/binary>>

Slot -> erlang:system_time(second) div ?SLOT
Frag -> erlang:system_time(second) div ?FRAG
SumBegin -> erlang:system_time(second) div ?SUM
SumEnd -> erlang:system_time(second) div ?SUM + ?SUM
