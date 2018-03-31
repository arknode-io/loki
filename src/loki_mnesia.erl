%%%-------------------------------------------------------------------
%%% @author danny
%%% @copyright (C) 2017, danny
%%% @doc
%%%
%%% @end
%%% Created : 2017-12-07 16:49:37.506082
%%%-------------------------------------------------------------------
-module(loki_mnesia).

-behaviour(gen_server).

-include("loki.hrl").

%% API
-export([start_link/0,
         ensure_table/1]).

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

ensure_table(FragNo) ->
  gen_server:call(?MODULE, {ensure_table, FragNo}).

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
  init_tables(),
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
handle_call({ensure_table, FragNo}, _From, State) ->
  Reply = handle_ensure_table(FragNo),
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

init_tables() ->
  StorageType = mnesia:table_info(schema, storage_type),
  mnesia:create_table(loki_counter, [{attributes, record_info(fields, loki_counter)},
                                     {StorageType, [node()]}]),
  mnesia:create_table(loki_event, [{attributes, record_info(fields, loki_event)},
                                   {StorageType, [node()]},
                                   {index, [event]}]).

handle_ensure_table(FragNo) ->
  Tname = list_to_atom("loki_" ++ integer_to_list(FragNo)),
  case does_table_exist(Tname) of
    true ->
      {ok, Tname};
    false ->
      create_table(Tname)
  end.

does_table_exist(Tname) ->
  lists:member(Tname, mnesia:system_info(local_tables)).

create_table(Tname) ->
  StorageType = mnesia:table_info(schema, storage_type),
  case mnesia:create_table(Tname, [{attributes, record_info(fields, loki)},
                                   {StorageType, [node()]},
                                   {record_name, loki}]) of
    {atomic, ok} ->
      {ok, Tname};
    {aborted,{already_exists,Tname}} ->
      {ok, Tname};
    Error ->
      Error
  end.
