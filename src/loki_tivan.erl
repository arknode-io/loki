%%%-------------------------------------------------------------------
%%% @author danny
%%% @copyright (C) 2019, danny
%%% @doc
%%%
%%% @end
%%% Created : 2019-04-13 21:24:22.860650
%%%-------------------------------------------------------------------
-module(loki_tivan).

-behaviour(gen_server).

%% API
-export([start_link/0
        ,ensure_loki_frag/1
        ,get_id_for_event/1
        ,get_event_for_id/1
        ,put/3
        ,get/2]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

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

ensure_loki_frag(Table) ->
  case tivan:is_local(Table) of
    false ->
      gen_server:call(?MODULE, {ensure_loki_frag, Table});
    true ->
      ok
  end.

get_id_for_event(Event) ->
  case tivan:get(loki_events, Event) of
    [] ->
      gen_server:call(?MODULE, {get_id_for_event, Event});
    [#{id := EventId}] ->
      EventId
  end.

get_event_for_id(Id) ->
  case tivan:get(loki_events, #{match => #{id => Id}
                               ,select => [event]}) of
    [] -> error;
    [#{event := Event}] -> {ok, Event}
  end.

put(Table, EntitySlot, Data) ->
  lager:info("Putting ~p in ~p", [{EntitySlot, Data}, Table]),
  tivan:put(Table, #{entity_slot => EntitySlot, data => Data}).

get(Table, {Entity, SlotNo}) when is_integer(SlotNo) ->
  case tivan:get(Table, {Entity, SlotNo}) of
    [] -> <<>>;
    [#{data := Data}] -> Data
  end;
get(Table, Entity) ->
  tivan:get(Table, #{match => #{entity_slot => {1, 2, Entity}}}).

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
  tivan:create(loki_events, #{columns => [id, {event}]
                             ,type => ordered_set}),
  {ok, #{}}.

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
handle_call({ensure_loki_frag, Table}, _From, State) ->
  Reply = do_ensure_loki_frag(Table),
  {reply, Reply, State};
handle_call({get_id_for_event, Event}, _From, State) ->
  Reply = do_get_id_for_event(Event),
  lager:info("Got the reply for the id for event ~p", [Reply]),
  {reply, Reply, State};
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

do_ensure_loki_frag(Table) ->
  case tivan:is_local(Table) of
    false ->
      R = tivan:create(Table, #{columns => [entity_slot, data]
                                ,type => ordered_set}),
      lager:info("Loki frag ~p creation status ~p", [Table, R]),
      ok;
    true ->
      ok
  end.

do_get_id_for_event(Event) ->
  lager:info("Getting Id for event ~p", [Event]),
  case tivan:get(loki_events, #{match => #{event => Event}}) of
    [] ->
      Id = case tivan:get_last_key(loki_events) of
             '$end_of_table' -> 1;
             LastId -> LastId + 1
           end,
      tivan:put(loki_events, #{id => Id, event => Event});
    [#{id := EventId}] ->
      lager:info("Event already exists ~p", [EventId]),
      EventId
  end.
