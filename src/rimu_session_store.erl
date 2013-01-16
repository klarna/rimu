%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc Session storage in Riak (c.f. meshup_sessions.erl).
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
-module(rimu_session_store).
-behaviour(meshup_store).
-compile({no_auto_import, [get/1, put/2]}).

%%%_* Exports ==========================================================
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

%%%_* Includes =========================================================
-include("rimu.hrl").

-include_lib("tulib/include/prelude.hrl").

%%%_* Macros ===========================================================
-define(is_bucket(X),
        (X =:= saved     orelse
         X =:= stale     orelse
         X =:= wal       orelse
         X =:= committed orelse
         X =:= cancelled)).

%%%_* Code =============================================================
%% Typechecking.
del([saved, K])                    -> do_del(saved, K);
del([wal, K])                      -> do_del(wal, K).
del(Key, Opts)                     -> throw({error, {del, Key, Opts}}).

get(B)          when ?is_bucket(B) -> do_get(B);
get([B, K])     when ?is_bucket(B) -> do_get(B, K).
get(B, [])      when ?is_bucket(B) -> do_get(B);
get([B, K], []) when ?is_bucket(B) -> do_get(B, K);
get(Key, Opts)                     -> throw({error, {get, Key, Opts}}).

put([B, K], V) when ?is_bucket(B)  -> case
                                        meshup_sessions:is_session(
                                          krc_obj:val(V))
                                      of
                                        true  -> do_put(B, K, V);
                                        false -> throw({error, {put, [B, K], V}})
                                      end.
put(Key, Val, Opts)                -> throw({error, {put, Key, Val, Opts}}).

%% Primitives.
do_del(B, K)                       -> krc:delete(?KRC, B, K).

do_get(B)                          -> krc:get_index(?KRC, B, B, '_', merge()).
do_get(B, K)                       -> krc:get(?KRC, B, K, merge()).

do_put(B, _K, Obj)                 -> krc:put_index(?KRC, Obj, [{B, '_'}]).

%% Representation.
bind(K, Objs) when is_atom(K)      -> {[krc_obj:val(Obj) || Obj <- Objs], ''};
bind(K, Obj)  when is_list(K)      -> {krc_obj:val(Obj),                  ''}.

return([B, K], V)                  -> krc_obj:new(B, K, V).
return(K, V, M)                    -> throw({error, {return, K, V, M}}).

%% Conflicts.
merge()                            -> fun(V1, V2) -> merge([], V1, V2) end.
merge(_, V1, V2)                   -> (meshup_resolver:to_fun(
                                         meshup_resolver:compose(
                                           [ rimu_resolver_tombstone
                                           , rimu_resolver_session
                                           ])))(V1, V2).

%%%_* Tests ============================================================
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

basic_test() ->
  rimu_test:with_rimu(fun() ->
    Flow                     = [ {meshup_test_services:block(), block},
                                 [block, '=>', {}]
                               ],
    Endpoint                 = meshup_endpoint:make(fun() -> Flow end),
    ID                       = rimu_test:gen_id(),
    {ok, {suspended, ID, _}} = meshup:start([ {endpoint,   Endpoint}
                                            , {session_id, ID}
                                            ]),
    {ok, {committed, _}}     = meshup:resume([{session_id, ID}]),
    {ok, Objs}               = get(committed),
    {[_|_] = _Vals, ''}      = bind(committed, Objs),
    ok
  end).

error_test() ->
  rimu_test:with_rimu(fun() ->
    {error, _}         = ?lift(meshup_store:del(?MODULE, [committed, 42])),
    {error, {del,_,_}} = ?lift(meshup_store:del(?MODULE, [committed, 42], [])),

    {error, _}         = ?lift(meshup_store:get(?MODULE, [b, k, ik])),
    {error, {get,_,_}} = ?lift(meshup_store:get(?MODULE, [b, k, ik], [])),

    {error, _}         = ?lift(put([wal, 42], krc_obj:new(foo, bar, baz))),
    {error, _}         = ?lift(put([wal, 42], krc_obj:new(foo, bar, baz), [])),

    {error, _}         = ?lift(return(foo, bar, baz)),
    ok
  end).

merge_test() ->
  rimu_test:with_rimu(fun() ->
    [ {K1, V1}
    , {K2, V2}
    ]            = rimu_test:gen_kvs(2),
    Service1     = meshup_test_services:write(K1, V1, rimu_store),
    Service2     = meshup_test_services:write(K2, V2, rimu_store),
    Endpoint1    = meshup_endpoint:make(fun() ->
      [ actor1
      , [ {Service1, write}
        ]
      , actor2
      , [ {Service2, write}
        ]
      ] end),
    ID1          = rimu_test:gen_id(),
    Label1       = meshup_flow:annotate(meshup_contexts:new(), actor1),
    Label2       = meshup_flow:annotate(meshup_contexts:new(), actor2),
    %% Safe
    Computation1 = meshup_endpoint:eval(Endpoint1, Label1),
    Computation2 = meshup_endpoint:eval(Endpoint1, Label2),
    Session1     = meshup_sessions:new_session(ID1, Computation1, 0),
    Session2     = meshup_sessions:new_session(ID1, Computation2, 0),
    ok           = put([saved, ID1], return([saved, ID1], Session1)),
    ok           = put([saved, ID1], return([saved, ID1], Session2)),
    {ok, _}      = get([saved, ID1]),
    %% Unsafe
    Service3     = meshup_test_services:write(K1, V2, rimu_store),
    Endpoint2    = meshup_endpoint:make(fun() ->
      [ actor1
      , [ {Service1, write}
        ]
      , actor2
      , [ {Service3, write}
        ]
      ] end),
    ID2          = rimu_test:gen_id(),
    Computation3 = meshup_endpoint:eval(Endpoint2, Label1),
    Computation4 = meshup_endpoint:eval(Endpoint2, Label2),
    Session3     = meshup_sessions:new_session(ID2, Computation3, 0),
    Session4     = meshup_sessions:new_session(ID2, Computation4, 0),
    ok           = put([saved, ID2], return([saved, ID2], Session3)),
    ok           = put([saved, ID2], return([saved, ID2], Session4)),

    {error, {conflict, _, _, {session, {vals, _, _}}}} =
      get([saved, ID2]),
    ok
  end).

-endif.

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
