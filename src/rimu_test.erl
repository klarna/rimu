%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc Test support library.
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
-module(rimu_test).

%%%_* Exports ==========================================================
-export([ defaults/0
        , fresh_kvs/1
        , gen_id/0
        , gen_idxs/1
        , gen_kvs/1
        , with_rimu/1
        , with_rimu/2
        , expect/1
        , expect/3
        , expect_value/2
        , expect_value/3
        , expect_siblings/2
        , expect_siblings/3
        , expect_conflict/1
        , expect_conflict/2
        , expect_notfound/1
        , expect_notfound/2
        ]).

%%%_* Includes =========================================================
-include("rimu.hrl").

-include_lib("tulib/include/logging.hrl").
-include_lib("tulib/include/prelude.hrl").
-include_lib("tulib/include/types.hrl").

%%%_* Code =============================================================
%%%_ * Name generation -------------------------------------------------
fresh_kvs(N)    -> delete_kvs(gen_kvs(N)).

gen_kvs(N)      -> [{gen_k(), gen_v()} || _ <- lists:seq(1, N)].
gen_k()         -> [ gensym(rimu_namespace)
                   , gensym(rimu_bucket)
                   , gensym(rimu_key)
                   ].
gen_v()         -> rand().

delete_kvs(KVs) -> [begin ok = rimu:delete(K), KV end || {K, _V} = KV <- KVs].

gen_idxs(N)     -> [{gen_idx(), gen_idx_key()} || _ <- lists:seq(1, N)].
gen_idx()       -> gensym(rimu_idx).
gen_idx_key()   -> gensym(rimu_idx_key).

gen_id()        -> gensym(rimu_id).

gensym(Stem)    -> tulib_atoms:catenate([Stem, rand()]).
rand()          -> crypto:rand_uniform(0, 1 bsl 159).

%%%_ * Environment -----------------------------------------------------
with_rimu(Thunk) ->
  with_rimu([], Thunk).
with_rimu(Settings, Thunk) ->
  tulib_sh:rm_rf("logs"),
  tulib_util:with_sys(Settings ++ defaults(), [rimu], Thunk).

defaults() ->
  [ {meshup, logger,  rimu_session_logger}
  , {meshup, store,   rimu_session_store}
  , {rimu,   log_dir, "logs"}
  ].


%%%_ * Read Validation -------------------------------------------------
expect(F) ->
  expect(F, 10, 100).

expect(F, Tries, Sleep) ->
  case ?lift(tulib_loops:retry(F, Sleep, Tries)) of
    {ok, _}      -> ok;
    {error, Err} -> {error, Err}
  end.

expect_value(K, Value) ->
  expect_value(K, Value, []).

expect_value(K, Value, Opts) ->
  expect(?thunk(?lift({ok, Value} = rimu:read(K, Opts)))).

expect_siblings(K, Siblings) ->
  expect_siblings(K, Siblings, []).

expect_siblings(K, Siblings, Opts) ->
  F = fun () ->
          {error, {conflict, V1 ,V2, _}} = rimu:read(K, Opts),
          true = tulib_predicates:is_permutation([V1, V2], Siblings)
      end,

  expect(?thunk(?lift(F()))).

expect_conflict(K) ->
  expect_conflict(K, []).

expect_conflict(K, Opts) ->
  expect(?thunk(?lift({ok, {error, {conflict, _, _, _}}} = {ok, rimu:read(K, Opts)}))).

expect_notfound(K) ->
  expect_notfound(K, []).

expect_notfound(K, Opts) ->
  expect(?thunk(?lift({ok, {error, notfound}} = {ok, rimu:read(K, Opts)}))).

%%%_* Tests ============================================================
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

basic_test() -> with_rimu(fun() -> io:format(user, "OHAI~n", []) end).

-endif.

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
