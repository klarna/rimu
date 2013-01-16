%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc DB API.
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
-module(rimu).

%%%_* Exports ==========================================================
-export([ create/2
        , create/3
        , read/1
        , read/2
        , update/2
        , update/3
        , delete/1
        , delete/2
        ]).

-export([ transaction/3
        , transaction/4
        ]).

-export([ start_link/0
        , stop/0
        ]).

%%%_* Includes =========================================================
-include("rimu.hrl").

-include_lib("krc/include/krc.hrl").
-include_lib("tulib/include/logging.hrl").
-include_lib("tulib/include/prelude.hrl").

%%%_* Code =============================================================
start_link()              -> krc_server:start_link(?KRC, []).
stop()                    -> krc_server:stop(?KRC).

create(K, V)              -> meshup_store:put_(rimu_store, K, V).
create(_K, _V, _Opts)     -> throw(nyi).

read(K)                   -> read(K, []).
read(K, Opts)             -> meshup_store:get_(rimu_store, K, Opts).

update(K, F)              -> update(K, F, []).
update(K, F, Opts)        -> meshup_store:patch_(rimu_store, K, F, Opts).

delete(K)                 -> delete(K, []).
delete(K, Opts)           -> meshup_store:del(rimu_store, K, Opts).

transaction(F, RS, WS)    -> transaction(F, RS, WS, async).
transaction(F, RS, WS, M) -> meshup_txn:txn(F, annotate(RS), annotate(WS), M).
annotate(Xs)              -> [{X, [{store, rimu_store}]} || X <- Xs].

%%%_* Tests ============================================================
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

%%%_ * Basics ----------------------------------------------------------
basic_test() ->
  rimu_test:with_rimu(fun() ->
    [ {K1, V1}
    , {K2, V2}
    , {K3, V3}
    ]         = rimu_test:fresh_kvs(3),

    ok        = create(K1, V1),
    ok        = rimu_test:expect_value(K1, V1),
    {ok, V1_} = update(K1, fun(V1) -> V1 + 1 end),
    {ok, V1_} = read(K1),
    ok        = delete(K1),
    ok        = rimu_test:expect_notfound(K1),

    ok        = create(K2, V2),
    ok        = transaction(fun(Ctx) ->
                                V2 = meshup_contexts:get(Ctx, K2),
                                [K2, V3, K3, V3]
                            end, [K2], [K2, K3]),
    ok        = rimu_test:expect_value(K2, V2),
    ok        = rimu_test:expect_value(K2, V3),
    ok        = rimu_test:expect_value(K3, V3),

    ok
  end).

transaction_test() ->
  rimu_test:with_rimu(fun() ->
    [ {K1, V1}
    , {_K2, V2}
    ]  = rimu_test:fresh_kvs(2),
    spawn(?thunk(transaction(fun(_Ctx) -> [K1, V1] end, [], [K1]))),
    spawn(?thunk(transaction(fun(_Ctx) -> [K1, V2] end, [], [K1]))),
    ok = rimu_test:expect_siblings(K1, [V1, V2]),
    ok
  end).

%%%_ * Built-in resolution ---------------------------------------------
conflict_test() ->
  rimu_test:with_rimu(fun() ->
    [ {K1, V1}
    , {K2, V2}
    , {K3, V3}
    , {K4, V4}
    ]          = rimu_test:gen_kvs(4),

    %% delete/create
    ok         = create(K1, V1),
    ok         = delete(K1),
    ok         = create(K1, V1),
    ok         = rimu_test:expect_siblings(K1, [?TOMBSTONE, V1]),

    %% create/create
    ok         = create(K2, V2),
    ok         = create(K2, V2),
    ok         = rimu_test:expect_siblings(K2, [V2, V2]),

    %% create/update
    ok         = create(K3, V3),
    {ok, V3_}  = update(K3, fun(V3) -> V3 + 1 end),
    ok         = create(K3, V4),
    ok         = rimu_test:expect_siblings(K3, [V3_, V4]),

    %% update/update
    ok         = create(K4, V4),
    {ok, Rets} = tulib_par:eval(fun(F) -> update(K4, F) end,
                                [ fun(V4) -> V4 + 1 end
                                , fun(V4) -> V4 * 2 end
                                ],
                                [{errors, false}]),
    ok         = rimu_test:expect_siblings(K4, Rets),

    ok
  end).

resolver_test() ->
  rimu_test:with_rimu(fun() ->
    [ {K1, V1}
    , {K2, V2}
    , {K3, V3}
    ]          = rimu_test:gen_kvs(3),

    %% Tombstone
    ok       = create(K1, V1),
    ok       = delete(K1),
    ok       = create(K1, V1),
    ok       = rimu_test:expect_value(K1,
                                      V1,
                                      [{ resolver
                                       , meshup_resolver:to_fun(rimu_resolver_tombstone)
                                       }]),

    %% Equality
    ok       = create(K2, V2),
    ok       = create(K2, V2),
    ok       = rimu_test:expect_value(K2,
                                      V2,
                                      [{ resolver
                                       , meshup_resolver:to_fun(rimu_resolver_eq)
                                       }]),

    %% Compose
    {ok, _}  = tulib_par:eval(fun(Thunk) -> Thunk() end,
                              [ ?thunk(create(K3, V3))
                              , ?thunk(create(K3, ?TOMBSTONE))
                              , ?thunk(create(K3, V3))
                              ],
                              [{errors, false}]),
    ok       = rimu_test:expect_value(K3,
                                      V3,
                                      [{ resolver
                                       , meshup_resolver:to_fun(
                                           meshup_resolver:compose([ rimu_resolver_tombstone
                                                                   , rimu_resolver_eq
                                                                   ]))
                                       }]),

    ok
  end).

%%%_ * Custom resolution -----------------------------------------------
resolve(Siblings) -> ?unlift(tulib_maybe:reduce(fun merge/2, Siblings)).
merge(Xs, Ys)     -> lists:sort(Xs ++ Ys).

update_update_resolution_test() ->
  rimu_test:with_rimu(fun() ->
    [ {K, V0}
    , {_, V1_}
    , {_, V1__}
    , {_, V1___}
    ] = [{Key, [Val]}  || {Key, Val} <- rimu_test:gen_kvs(4)],

    %%%
    %%%     v0
    %%%     /|\
    %%%    / | \
    %%%   /  |  \
    %%%  /   |   \
    %%% /    |    \
    %%% v1_ v1__ v1___
    %%%  \   |   /
    %%%   \  |  /
    %%%    \ | /
    %%%     \|/
    %%%      v
    %%%
    V       = resolve([V1_, V1__, V1___]),
    ok      = create(K, V0),
    {ok, _} = tulib_par:eval(
                fun(Y) ->
                    update(K, fun(X) when X =:= V0 -> Y end)
                end,
                [V1_, V1__, V1___],
                [{errors, false}]),
    ok      = rimu_test:expect_conflict(K),
    ok      = rimu_test:expect_value(K,
                                     V,
                                     [{resolver, fun merge/2}]),
    ok      = rimu_test:expect_conflict(K),
    {ok, V} = update(K,
                     fun(X) -> X end,
                     [{resolver, fun merge/2}]),
    ok      = rimu_test:expect_value(K, V),
    ok
  end).

create_create_resolution_test() ->
  rimu_test:with_rimu(fun() ->
    [ {K, V0_}
    , {_, V0__}
    , {_, V0___}
    ] = [{Key, [Val]}  || {Key, Val} <- rimu_test:gen_kvs(3)],

    %%%
    %%% v0_ v0__ v0___
    %%%  \   |   /
    %%%   \  |  /
    %%%    \ | /
    %%%     \|/
    %%%      v
    %%%
    V       = resolve([V0_, V0__, V0___]),
    {ok, _} = tulib_par:eval(
                fun(Y) -> create(K, Y) end,
                [V0_, V0__, V0___],
                [{errors, false}]),
    ok      = rimu_test:expect_conflict(K),
    {ok, V} = update(K,
                     fun(X) -> X end,
                     [{resolver, fun merge/2}]),
    ok      = rimu_test:expect_value(K, V),
    ok
  end).

update_create_resolution_test() ->
  rimu_test:with_rimu(fun() ->
    [ {K, V0}
    , {_, V1}
    , {_, V2}
    ] = [{Key, [Val]}  || {Key, Val} <- rimu_test:gen_kvs(3)],

    %%%
    %%% v0
    %%% |
    %%% |
    %%% v1   v2
    %%%  \   |
    %%%   \  |
    %%%    \ |
    %%%     \|
    %%%      v
    %%%
    V       = resolve([V1, V2]),
    ok      = create(K, V0),
    {ok, _} = update(K, fun(X) when X =:= V0 -> V1 end),
    ok      = create(K, V2),
    ok      = rimu_test:expect_siblings(K, [V1, V2]),
    {ok, V} = update(K,
                     fun(X) -> X end,
                     [{resolver, fun merge/2}]),
    ok      = rimu_test:expect_value(K, V),
    ok
  end).

-ifdef(NOJENKINS).
par_update_create_resolution_test() ->
  rimu_test:with_rimu(fun() ->
    [ {K, V0}
    , {_, V1}
    , {_, V2}
    ] = [{Key, [Val]}  || {Key, Val} <- rimu_test:gen_kvs(3)],

    %%%
    %%% v0
    %%% |
    %%% |
    %%% v1   v2
    %%%  \   |
    %%%   \  |
    %%%    \ |
    %%%     \|
    %%%      v
    %%%
    V       = resolve([V0, V1, V2]), %V0!
    ok      = create(K, V0),
    {ok, _} = tulib_par:eval(
                fun(Thunk) -> Thunk() end,
                %% Note that this can fail if the
                %% create happens to be executed
                %% first.
                [ ?thunk(update(K, fun(X) when X =:= V0 ->
                                       V1
                                   end))
                , ?thunk(create(K, V2))
                ],
                [{errors, false}]),
    ok      = rimu_test:expect_siblings(K, [V1, V2]),
    {ok, V} = update(K, fun(X) -> X end, [{resolver, fun merge/2}]),
    ok      = rimu_test:expect_value(K, V),
    ok
  end).
-endif.

complex_conflict_resolution_test() ->
  rimu_test:with_rimu(fun() ->
    [ {K, V0}
    , {_, V1_}
    , {_, V1__}
    , {_, V1___}
    , {_, V2_}
    , {_, V2__}
    , {_, V3}
    , {_, V4}
    ] = [{Key, [Val]}  || {Key, Val} <- rimu_test:gen_kvs(8)],

    %%%
    %%%     v0
    %%%    /|\
    %%%   / | \             v0
    %%%  /  |  \           / \
    %%% /   |   \         /   \
    %%% v1_ v1__ v1___    v2_ v2__    v3    v4
    %%%  \  |   /          \   /      /     /
    %%%   \ |  /            \ /      /     /
    %%%    \| /              v2     /     /
    %%%     v1                \    /     /
    %%%      \                 \  /     /
    %%%       \                 v5     /
    %%%        \                /     /
    %%%         \              /     /
    %%%          \            /     /
    %%%           \          /     /
    %%%            \        /     /
    %%%             \      /     /
    %%%              \    /     /
    %%%               \  /     /
    %%%                v6     /
    %%%                 \    /
    %%%                  \  /
    %%%                   v
    %%%
    V1         = resolve([V1_, V1__, V1___]),
    V2         = resolve([V2_, V2__]),
    V5         = resolve([V2, V3]),
    V6         = resolve([V1, V5]),
    V          = resolve([V6, V4]),

    %% v0 -> v1
    ok         = create(K, V0),
    {ok, _}    = tulib_par:eval(fun(Y) ->
                                  update(K, fun(X) when X =:= V0 -> Y end)
                                end,
                                [V1_, V1__, V1___],
                                [{errors, false}]),
    {error, {conflict,_,_,_}} = read(K),
    {ok, Blob} = meshup_store:get(rimu_store, K, [{resolver, fun merge/2}]),
    {V1, Meta} = meshup_store:bind(rimu_store, K, Blob),

    %% v0 -> v2 (ignore merged v1s)
    {ok, _}    = tulib_par:eval(fun(Y) ->
                                  update(K,
                                         fun(_) -> Y end,
                                         [{resolver, fun merge/2}])
                                end,
                                [V2_, V2__],
                                [{errors, false}]),
    {error, {conflict,_,_,_}} = read(K),
    {ok, V2}   = update(K, fun(X) -> X end, [{resolver, fun merge/2}]),

    %% create v3
    ok         = create(K, V3),
    ok         = rimu_test:expect_conflict(K),
    {ok, V5}   = update(K, fun(X) -> X end, [{resolver, fun merge/2}]),

    %% write v1 (with old vector clock)
    ok         = meshup_store:put(rimu_store, K, meshup_store:return(rimu_store, K, V1, Meta)),
    ok         = rimu_test:expect_conflict(K),
    {ok, V6}   = update(K, fun(X) -> X end, [{resolver, fun merge/2}]),

    %% v4
    ok         = create(K, V4),
    ok         = rimu_test:expect_conflict(K),
    {ok, V}    = update(K, fun(X) -> X end, [{resolver, fun merge/2}]),

    ok
  end).

-endif.

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
