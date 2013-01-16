%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc Session monitor.
%%% Periodically redo sessions which failed to commit and clean up stale
%%% sessions.
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
-module(rimu_session_monitor).
-behaviour(gen_server).

%%%_* Exports ==========================================================
%% API
-export([ start_link/0
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
-include_lib("tulib/include/types.hrl").

%%%_* Code =============================================================
%%%_ * API -------------------------------------------------------------
start_link() -> gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%%_ * gen_server callbacks --------------------------------------------
init([])                   -> {ok, do_init()}.
terminate(_, _)            -> ok.
code_change(_, S, _)       -> {ok, S}.
handle_call(Msg, _, S)     -> ?warning("~p", [Msg]), {reply, error, S}.
handle_cast(Msg, S)        -> ?warning("~p", [Msg]), {noreply, S}.
handle_info(redo, S)       -> do_redo(),             {noreply, S};
handle_info(session_gc, S) -> do_session_gc(),       {noreply, S};
handle_info(Msg, S)        -> ?warning("~p", [Msg]), {noreply, S}.

%%%_ * Internals -------------------------------------------------------
do_init() ->
  do_redo(),
  RedoInterval      = tulib_util:get_env(redo_interval, 1000*60*10, ?APP),
  SessionGcInterval = tulib_util:get_env(session_gc_interval, 1000*60*10,?APP),
  Self              = self(),
  spawn_link(?thunk(timer(RedoInterval,      Self, redo))),
  spawn_link(?thunk(timer(SessionGcInterval, Self, session_gc))),
  undefined.


timer(Interval, Pid, Msg) ->
  receive
  after Interval -> Pid ! Msg, timer(Interval, Pid, Msg)
  end.

flush(Msg) ->
  receive Msg -> flush(Msg)
  after   0   -> ok
  end.


do_redo() ->
  ?info("redo"),
  ?increment([redo]),
  flush(redo),
  Ops = rimu_session_logger:redo(),
  ?debug("Ops = ~p", [Ops]),
  ?info("redoing ~p sessions", [length(Ops)]),
  case
    tulib_par:eval(fun({ID, State}) -> meshup_sessions:redo(State, ID) end,
                   Ops,
                   [{errors, true}])
  of
    {ok, Rets} ->
      {Oks, Errs} = tulib_maybe:partition(Rets),
      ?info("redo success: ~p session oks, ~p session errors: ~p",
            [length(Oks), length(Errs), Errs]);
    {error, Rsn} ->
      ?info("redo fail: ~p", [Rsn])
  end.

do_session_gc() ->
  ?info("session_gc"),
  ?increment([session_gc]),
  flush(session_gc),
  case ?lift(meshup_sessions:collect()) of
    {ok, _}      -> ?info("session_gc success");
    {error, Rsn} -> ?info("session_gc fail: ~p", [Rsn])
  end.

%%%_* Tests ============================================================
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

%%%_ * Basics ----------------------------------------------------------
basic_test() ->
  rimu_test:with_rimu([ {rimu, redo_interval, 2000}
                      , {rimu, state_timeout, 100}
                      ], fun() ->
    [ {K1, V1}
    , {K2, V2}
    , {K3, V3}
    ] = rimu_test:fresh_kvs(3),

    MFA = {rimu_store, put, [K2, '_', '_']},
    tulib_meck:with_faulty(MFA, fun() ->
      ok = rimu:transaction(
             fun(_Ctx) ->
               [K1, V1, K2, V2, K3, V3]
             end, [], [K1, K2, K3]),
      timer:sleep(100) %wait for async commit
    end),

    ok   = rimu_test:expect_value(K1, V1),
    ok   = rimu_test:expect_notfound(K2),
    %write-set committed in parallel
    ok   = rimu_test:expect_value(K3, V3),

    ok   = timer:sleep(2000), %wait for redo

    %% Redo replays the entire write-set, creating trivially resolvable
    %% siblings for K1 and K3.
    Opts = [{resolver, meshup_resolver:to_fun(rimu_resolver_eq)}],
    ok   = rimu_test:expect_value(K1, V1, Opts),
    ok   = rimu_test:expect_value(K2, V2),
    ok   = rimu_test:expect_value(K3, V3, Opts),
    ok
  end).

replay_test() ->
  rimu_test:with_rimu([{rimu, redo_interval, 1000}, {rimu, state_timeout, 100}], fun() ->
    [ {K1, V1}
    , {K2, V2}
    , {_K3, V3}
    ] = rimu_test:fresh_kvs(3),

    ok       = rimu:create(K1, V1),
    ok       = rimu_test:expect_value(K1, V1),
    %% Note that we must have K1 in our read-set so that MeshUp can get a
    %% hold of its vector clock!
    ok       = rimu:transaction(fun(_Ctx) -> [K1, V2] end, [K1], [K1], in_process),
    ok       = rimu_test:expect_value(K1, V2),

    %% Replaying updates works in general...
    ok       = rimu:create(K2, V2),

    MFA = {rimu_store, put, [K2, '_', '_']},
    tulib_meck:with_faulty(MFA, fun() ->
      ok = rimu:transaction(fun(_Ctx) -> [K1, V1, K2, V3] end,
                            [K1, K2],
                            [K1, K2]),
      timer:sleep(100) %wait for async commit
     end),

    %% ... but not in the presence of concurrency!
    ok       = rimu_test:expect_value(K1, V1),
    {ok, V3} = rimu:update(K1, fun(V) when V =:= V1 -> V3 end),
    ok       = rimu_test:expect_value(K1, V3),

    ok       = timer:sleep(1000), %wait for redo

    ok       = rimu_test:expect_conflict(K1),
    ok       = rimu_test:expect_value(K2, V3),

    ok
  end).

in_process_test() ->
  rimu_test:with_rimu([{rimu, redo_interval, 1000}, {rimu, state_timeout, 100}], fun() ->
    [ {K1, V1}
    , {K2, V2}
    ]   = rimu_test:fresh_kvs(2),
    MFA = {rimu_store, put, [K2, '_', '_']},
    tulib_meck:with_faulty(MFA, fun() ->
      {error, _} = rimu:transaction(fun(_Ctx) -> [K1, V1, K2, V2] end,
                                    [],
                                    [K1, K2],
                                    in_process)
    end),
    ok       = timer:sleep(1000),   %wait for redo
    Opts     = [{resolver, meshup_resolver:to_fun(rimu_resolver_eq)}],
    ok       = rimu_test:expect_value(K1, V1, Opts), %\ no isolation
    ok       = rimu_test:expect_value(K2, V2),       %/ once it's in the WAL
    ok
  end).

cover_test() ->
  rimu_test:with_rimu(fun() ->
    %% Not covered by previous test cases due to brutal_kill.
    ok        = terminate(foo, bar),
    {ok, bar} = code_change(foo, bar, baz),
    gen_server:call(rimu_session_monitor, foo),
    gen_server:cast(rimu_session_monitor, foo),
    rimu_session_monitor ! foo
  end).

%% TODO
session_gc_test() ->
  ok.

%%%_ * Overlay stores --------------------------------------------------
%% Pseudo stores implemented on top of rimu_store may map a single put
%% to several rimu_store:puts.
multi_obj_write_test() ->
  rimu_test:with_rimu(fun() ->
    [ {K1, V1}
    , {K2, V2}
    , {K3, V3}
    , {K4, V4}
    ] = KVs = rimu_test:fresh_kvs(4),

    {ok, _} = meshup:start([{endpoint, endpoint(KVs)}]),
    timer:sleep(100), %wait for async commit

    ok       = rimu_test:expect_value(K1, V1),
    ok       = rimu_test:expect_value(K2, V2),
    ok       = rimu_test:expect_value(K3, V3),
    ok       = rimu_test:expect_value(K4, V4),
    ok
  end).

multi_obj_replay_test() ->
  rimu_test:with_rimu([{rimu, redo_interval, 1000}, {rimu, state_timeout, 100}], fun() ->
    [ {K1, V1}
    , {K2, V2}
    , {K3, V3}
    , {K4, V4}
    ] = KVs = rimu_test:fresh_kvs(4),

    MFA = {rimu_store, put, [K2, '_']},
    tulib_meck:with_faulty(MFA, fun() ->
      {ok, _} = meshup:start([{endpoint, endpoint(KVs)}]),
      timer:sleep(100) %wait for async commit
     end),

    ok   = rimu_test:expect_value(K1, V1),
    ok   = rimu_test:expect_notfound(K2),

    ok   = timer:sleep(1000), %wait for redo

    Opts = [{resolver, meshup_resolver:to_fun(rimu_resolver_eq)}],
    ok   = rimu_test:expect_value(K1, V1, Opts),
    ok   = rimu_test:expect_value(K2, V2),
    ok   = rimu_test:expect_value(K3, V3),
    ok   = rimu_test:expect_value(K4, V4),
    ok
  end).


endpoint(KVs) ->
  meshup_endpoint:make(
    fun() -> [{service(KVs), method}] end).

service(KVs) ->
  meshup_service:make(
    fun(method, _Ctx)   -> meshup:ok([ [service, objs], KVs ]) end,
    fun(method, input)  -> [];
       (method, output) -> [ {[service, objs], [{store, store()}]} ]
    end,
    fun() -> service end).

store() ->
  meshup_store:make(
    fun(_, _)    -> throw(bind) end,
    fun(_)       -> throw(del) end,
    fun(_)       -> throw(get) end,
    fun(_, _, _) -> throw(merge) end,
    fun(_, KVs)  ->
        [meshup_store:put_(rimu_store, K, V) || {K, V} <- KVs],
        ok
    end,
    fun(_, X)    -> X end,
    fun(_, _, _) -> throw(return3) end).

-endif.

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
