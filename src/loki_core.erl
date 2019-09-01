%%%-------------------------------------------------------------------
%%% @author danny
%%% @copyright (C) 2019, danny
%%% @doc
%%%
%%% @end
%%% Created : 2019-07-28 10:39:04.843816
%%%-------------------------------------------------------------------
-module(loki_core).

-behaviour(gen_server).

-define(FRAG_PREFIX, "loki_").
-define(FRAG_SIZE, 604800).
-define(SLOT_SIZE, 3600).

%% API
-export([start_link/0
        ,put/1
        ,get/1
        ,apply/1]).

%% gen_server callbacks
-export([init/1
        ,handle_call/3
        ,handle_cast/2
        ,handle_info/2
        ,terminate/2
        ,code_change/3]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

put(#{entity := _Entity, event := _Event} = EventMap) ->
  EventMapU = case maps:find(time, EventMap) of
                error -> EventMap#{time => erlang:system_time(second)};
                _ -> EventMap
              end,
  gen_server:cast(?MODULE, {put, EventMapU});
put(EventMap) ->
  lager:info("EventMap ~p missing essential info", [EventMap]),
  error.

get(#{entity := _Entity} = Request) ->
  GetFunction = fun({S, E}, A) -> A#{S => E} end,
  apply(Request#{function => GetFunction, init => #{}}).

apply(#{entity := _Entity, function := Function} = Request) when is_function(Function, 2) ->
  do_apply(Request).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([]) ->
  FragPrefix = application:get_env(loki, frag_prefix, ?FRAG_PREFIX),
  FragSize = application:get_env(loki, frag_size, ?FRAG_SIZE),
  SlotSize = application:get_env(loki, slot_size, ?SLOT_SIZE),
  {ok, #{frag_prefix => FragPrefix, frag_size => FragSize, slot_size => SlotSize}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call(_Request, _From, State) ->
  Reply = ok,
  {reply, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast({put, EventMap}, State) ->
  do_put(EventMap, State),
  % lager:info("do_put ~p result is ~p", [{EventMap, State}, R]),
  {noreply, State};
handle_cast(_Msg, State) ->
  {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info(_Info, State) ->
  {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
  ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

frag_table(FragPrefix, FragNo) ->
  list_to_atom(FragPrefix ++ integer_to_list(FragNo)).

do_put(#{entity := Entity, event := Event, time := TimeInSeconds} = EventMap
      ,#{frag_prefix := FragPrefix, frag_size := FragSize, slot_size := SlotSize}) ->
  Mark = TimeInSeconds rem SlotSize,
  FragNo = TimeInSeconds div FragSize,
  SlotNo = TimeInSeconds div SlotSize,
  EventId = loki_tivan:get_id_for_event(Event),
  EventData = case maps:get(data, EventMap, <<>>) of
                <<>> ->
                  <<Mark:16, EventId:16, 0:16>>;
                Data ->
                  {DataU, Size} = case size(Data) of
                                    S when S > 1023 -> {binary_part(Data, 0, 1023), 1023};
                                    S -> {Data, S}
                                  end,
                  % lager:info("Putting ~p", [{Mark, EventId, Size, DataU}]),
                  <<Mark:16, EventId:16, Size:16, DataU/binary>>
              end,
  Table = frag_table(FragPrefix, FragNo),
  loki_tivan:ensure_loki_frag(Table),
  EventsData = loki_tivan:get(Table, {Entity, SlotNo}),
  % lager:info("EventsData is ~p", [{EventsData, EventData}]),
  loki_tivan:put(Table, {Entity, SlotNo}, <<EventsData/binary, EventData/binary>>).

do_apply(#{entity := Entity, function := Function} = Request) ->
  InitValue = maps:get(init, Request, #{}),
  FragPrefix = application:get_env(loki, frag_prefix, ?FRAG_PREFIX),
  FragSize = application:get_env(loki, frag_size, ?FRAG_SIZE),
  SlotSize = application:get_env(loki, slot_size, ?SLOT_SIZE),
  Now = erlang:system_time(second),
  ToFrag = case maps:find(to_time, Request) of
             error ->
               Now div FragSize;
             ToTimeInSeconds ->
               ToTimeInSeconds div FragSize
           end,
  FromFrag = case maps:find(from_time, Request) of
               error ->
                 Now div FragSize;
               FromTimeInSeconds ->
                 FromTimeInSeconds div FragSize
             end,
  EventsReq = maps:get(events, Request, []),
  lists:foldl(
    fun(FragNo, EventsAcc) ->
        Table = frag_table(FragPrefix, FragNo),
        lists:foldl(
          fun(#{entity_slot := {_, SlotNo}, data := Data}, EventsAccInner) ->
              SlotSeconds = SlotSize * SlotNo,
              parse_event_data(Data, SlotSeconds, EventsReq, Function, EventsAccInner)
          end,
          EventsAcc,
          loki_tivan:get(Table, Entity)
         )
    end,
    InitValue,
    lists:seq(FromFrag, ToFrag)
   ).

parse_event_data(<<Mark:16, EventId:16, 0:16, Rest/binary>>
                 ,SlotSeconds, EventsReq, Function, EventsAcc) ->
  % lager:info("Parsing ~p from ~p and fitering from ~p adding to ~p"
  %            ,[Data, SlotSeconds, EventsReq, EventsAcc]),
  case get_requested_event_for_id(EventId, EventsReq) of
    error ->
      % lager:info("Event for ~p missing so ignoring", [EventId]),
      parse_event_data(Rest, SlotSeconds, EventsReq, Function, EventsAcc);
    not_requested ->
      % lager:info("Event for ~p not requested so ignoring", [EventId]),
      parse_event_data(Rest, SlotSeconds, EventsReq, Function, EventsAcc);
    {ok, Event} ->
      EventsAccU = apply(Function, [{SlotSeconds + Mark, #{event => Event}}, EventsAcc]),
      parse_event_data(Rest, SlotSeconds, EventsReq, Function, EventsAccU)
  end;
parse_event_data(<<Mark:16, EventId:16, Size:16, EData:Size/binary, Rest/binary>>
                 ,SlotSeconds, EventsReq, Function, EventsAcc) ->
  % lager:info("Parsing ~p from ~p and fitering from ~p adding to ~p"
  %            ,[Data, SlotSeconds, EventsReq, EventsAcc]),
  case get_requested_event_for_id(EventId, EventsReq) of
    error ->
      % lager:info("Event for ~p missing so ignoring", [EventId]),
      parse_event_data(Rest, SlotSeconds, EventsReq, Function, EventsAcc);
    not_requested ->
      % lager:info("Event for ~p not requested so ignoring", [EventId]),
      parse_event_data(Rest, SlotSeconds, EventsReq, Function, EventsAcc);
    {ok, Event} ->
      EventsAccU = apply(Function, [{SlotSeconds + Mark, #{event => Event
                                                          ,data => EData}}, EventsAcc]),
      parse_event_data(Rest, SlotSeconds, EventsReq, Function, EventsAccU)
  end;
parse_event_data(<<>>, _SlotSeconds, _EventsReq, _Function, EventsAcc) -> EventsAcc.

get_requested_event_for_id(EventId, EventsReq) ->
  case loki_tivan:get_event_for_id(EventId) of
    error ->
      error;
    {ok, Event} when EventsReq == [] ->
      {ok, Event};
    {ok, Event} ->
      case lists:member(Event, EventsReq) of
        true ->
          {ok, Event};
        false ->
          not_requested
      end
  end.

