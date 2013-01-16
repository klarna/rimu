%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc Distributed counters.
%%% Serialize updates to node-indexed PN-counters.
%%% @copyright 2012 Klarna AB
%%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%%
%%%   Copyright 2011-2013 Klarna AB
%%%
%%%   Licensed under the Apache License, Version 2.0 (the "License");
%%%   you may not use this file except in compliance with the License.
%%%   You may obtain a copy of the License at
%%%
%%%       http://www.apache.org/licenses/LICENSE-2.0
%%%
%%%   Unless required by applicable law or agreed to in writing, software
%%%   distributed under the License is distributed on an "AS IS" BASIS,
%%%   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%%   See the License for the specific language governing permissions and
%%%   limitations under the License.
%%%

%%%_* Module declaration ===============================================
-module(rimu_counters).
-behaviour(meshup_store).
-behaviour(gen_server).
-compile({no_auto_import, [get/1, put/2]}).

%%%_* Exports ==========================================================
%% API
-export([ dec/1
        , inc/1
        , val/1
        ]).

-export([ start_link/0
        , stop/0
        ]).

%% meshup_store callbacks
-export([ del/1
        , del/2
        , get/1
        , get/2
        , merge/3
        , put/2
        , put/3
        , bind/2
        , return/2
        , return/3
        ]).

%% gen_server callbacks
-export([ code_change/3
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , init/1
        , terminate/2
        ]).

%%%_* Includes =========================================================
-include("rimu.hrl").

-include_lib("tulib/include/assert.hrl").
-include_lib("tulib/include/logging.hrl").
-include_lib("tulib/include/metrics.hrl").
-include_lib("tulib/include/prelude.hrl").

%%%_* Code =============================================================
%%%_ * API -------------------------------------------------------------
start_link() -> gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).
stop()       -> gen_server:call(?MODULE, stop, infinity).

-spec dec(atom()) -> integer().
dec(Name)         -> gen_server:call(?MODULE, {dec, Name}).

-spec inc(atom()) -> integer().
inc(Name)         -> gen_server:call(?MODULE, {inc, Name}).

-spec val(atom()) -> integer().
val(Name)         -> gen_server:call(?MODULE, {val, Name}).

%%%_ * meshup_store callbacks ------------------------------------------
%% Services can increment and decrement counters by writing `inc' or
%% `dec' to this store. Currently, we support at most one update per
%% counter per flow.
%% Note that, since inc and dec are _NOT_ idempotent, reported counts
%% will not be accurate in the presence of failures.
del(Key)              -> del(Key, []).
del(Key, Opts)        -> throw({?MODULE, del, [Key, Opts]}).

get(Key)              -> get(Key, []).
get(Key, [])          -> {ok, val(Key)}.

put(Key, Val)         -> put(Key, Val, []).
put(Key, inc, _)      -> _ = inc(Key), ok;
put(Key, dec, _)      -> _ = dec(Key), ok.

%% Identity.
bind(_Key, Val)       -> {Val, ''}.
return(_Key, Val)     -> Val.
return(_Key, Val, '') -> Val.

merge(_Key, V1, V2)   -> {ok, [V1, V2]}.

%%%_ * gen_server callbacks --------------------------------------------
init([])                -> {ok, undefined}.
terminate(_, _)         -> ok.
code_change(_, S, _)    -> {ok, S}.
handle_call(stop, _, S) -> {stop, normal, ok, S};
handle_call(Cmd, _, S)  -> {reply, do_call(Cmd), S}.
handle_cast(_, S)       -> {stop, bad_cast, S}.
handle_info(Msg, S)     -> ?warning("~p", [Msg]), {noreply, S}.

%%%_ * Internals -------------------------------------------------------
do_call({dec, Name}) ->
  case patch_(Name, fun(C) -> tulib_pn_counters:dec(C, 1) end) of
    {ok, Counter} ->
      tulib_pn_counters:val(Counter);
    {error, notfound} ->
      ok = put_(Name, tulib_pn_counters:dec(tulib_pn_counters:new(), 1)),
      -1
  end;
do_call({inc, Name}) ->
  case patch_(Name, fun(C) -> tulib_pn_counters:inc(C, 1) end) of
    {ok, Counter} ->
      tulib_pn_counters:val(Counter);
    {error, notfound} ->
      ok = put_(Name, tulib_pn_counters:inc(tulib_pn_counters:new(), 1)),
      1
  end;
do_call({val, Name}) ->
  case get_(Name) of
    {ok, Counter} ->
      tulib_pn_counters:val(Counter);
    {error, notfound} ->
      0
  end.

patch_(Name, Fun) -> meshup_store:patch_(rimu_store, Name, Fun, opts()).
get_(Name)        -> meshup_store:get_(rimu_store, Name, opts()).
put_(Name, Val)   -> meshup_store:put_(rimu_store, Name, Val).

opts()            -> [{resolver, fun tulib_pn_counters:merge/2}].

%%%_* Tests ============================================================
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-include_lib("meshup/include/test.hrl").

api_test() ->
  rimu_test:with_rimu(fun() ->
    [{K, _}] = rimu_test:fresh_kvs(1),
    Ops      = [tulib_random:pick([inc, dec]) || _ <- lists:seq(1, 1000)],
    {ok, _}  = tulib_par:eval(fun(Op) -> ?MODULE:Op(K) end,
                              Ops,
                              [{workers, 10}]),
    Expected = lists:sum([case Op of
                            inc ->  1;
                            dec -> -1
                          end || Op <- Ops]),
    Expected = val(K),
    ok
  end).

meshup_store_test() ->
  rimu_test:with_rimu(fun() ->
    [{K, _}] = rimu_test:fresh_kvs(1),
    %% Once per flow
    Inc      = meshup_test_services:write(K, inc, rimu_counters),
    Dec      = meshup_test_services:write(K, dec, rimu_counters),
    Flow     = [ {Inc, write}
               , {Dec, write}
               , {Dec, write}
               , {Inc, write}
               ],
    {ok, _}  = meshup:start([ {endpoint, meshup_endpoint:make(fun() -> Flow end)}
                            , {mode, in_process}
                            ]),
    1        = val(K),
    ok
  end).

cover_test() ->
  {ok, _} = start_link(),
  stop(),
  tulib_processes:sync_unregistered(?MODULE),
  process_flag(trap_exit, true),
  {ok, Pid} = start_link(),
  ok = terminate(foo, bar),
  {ok, bar} = code_change(foo, bar, baz),
  Pid ! msg,
  gen_server:cast(Pid, msg),
  ok.

-endif.

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
