%%%-------------------------------------------------------------------
%%% @author danny
%%% @copyright (C) 2019, danny
%%% @doc
%%%
%%% @end
%%% Created : 2019-08-03 12:54:41.613509
%%%-------------------------------------------------------------------
-module(loki_events).

-behaviour(gen_server).

-callback init(Args :: list()) -> {'ok', State :: map()}.
-callback handle_event(EventMap :: map(), State :: map()) ->
  {'ok', EventMapList :: list(), NewState :: map()} |
  {'noreply', NewState :: map()} |
  {'stop', Reason :: term(), NewState :: map()}.

%% API
-export([start_link/4
        ,notify/1]).

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
%% @spec start_link(Registration, Callback, Arguments, Options) -> {ok, Pid}
%%                                                               | ignore
%%                                                               | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(Registration, Callback, Arguments, Options) ->
  gen_server:start_link(Registration, ?MODULE, [Callback|Arguments], Options).

notify(EventMap) ->
  gen_server:cast(?MODULE, {notify, EventMap}).

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
init([Callback|Arguments]) ->
  case Callback:init(Arguments) of
    {ok, State} ->
      {ok, #{callback => Callback, state => State}};
    Error ->
      Error
  end.

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
handle_cast({notify, EventMap}, #{callback := Callback, state := State}) ->
  case Callback:handle_notify(EventMap, State) of
    {ok, EventMapList, NewState} ->
      do_notify(EventMapList),
      {noreply, #{callback => Callback, state => NewState}};
    {noreply, NewState} ->
      {noreply, #{callback => Callback, state => NewState}};
    {stop, Reason, NewState} ->
      {stop, Reason, #{callback => Callback, state => NewState}}
  end;
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

do_notify(EventMapList) ->
  lists:foreach(
    fun(EventMap) ->
        lager:info("Sending the event ~p to loki_core", [EventMap]),
        loki_core:put(EventMap)
    end,
    EventMapList
   ).
