%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc MeshUp-compatible Riak client.
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
-module(rimu_store).
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

-export([ virtualize/1
        , virtualize/2
        ]).

%%%_* Includes =========================================================
-include("rimu.hrl").

-include_lib("tulib/include/logging.hrl").

%%%_* Code =============================================================
%% vBuckets.
del(Key)                         -> del(Key, []).
del(Key, Opts)                   -> do_del(virtualize(Key), Opts).

get(Key)                         -> get(Key, []).
get(Key, Opts)                   -> do_get(virtualize(Key), Opts).

put(Key, Val)                    -> put(Key, Val, []).
put(Key, Val, Opts)              -> do_put(virtualize(Key), Val, Opts).

virtualize([NS, B, K])           -> [tulib_atoms:catenate([NS, '_', B]), K];
virtualize([NS, B, I, IK])       -> [tulib_atoms:catenate([NS, '_', B]), I, IK];
virtualize([NS, B, K, I, IK])    -> [tulib_atoms:catenate([NS, '_', B]), K, I, IK].
virtualize(NS, B)                -> tulib_atoms:catenate([NS, '_', B]).

%% Primitives.
%% Note the asymmetry of index operations: we read a set of objects from
%% an index but add indexing information only to a single object at a
%% time.
do_del([B, K], [])               -> krc:delete(?KRC, B, K).

do_get([B, K],    Opts)          -> krc:get(?KRC, B, K, resolver(Opts));
do_get([B, I, K], Opts)          -> krc:get_index(?KRC,
                                                  B,
                                                  I,
                                                  K,
                                                  resolver(Opts)).

do_put([_, _],        Obj, Opts) -> krc:put_index(?KRC, Obj, indices(Opts));
do_put([_, _, I, IK], Obj, Opts) -> krc:put_index(?KRC,
                                                  Obj,
                                                  indices(Opts) ++ [{I, IK}]).

resolver(Opts)                   -> tulib_lists:assoc(resolver, Opts, merge()).
indices(Opts)                    -> tulib_lists:assoc(indices, Opts, []).

%% Representation.
bind([_, _, _],    O)            -> {krc_obj:val(O), O};
bind([_, _, _, _], Os)           -> {[krc_obj:val(O) || O <- Os], index_read}.

return([NS, B, K],       V)      -> krc_obj:new(virtualize(NS, B), K, V);
return([NS, B, K, _, _], V)      -> krc_obj:new(virtualize(NS, B), K, V).
return([_, _, _], V, O)          -> krc_obj:set_val(O, V).

%% Conflicts (no resolution).
merge()                          -> fun(V1, V2) -> merge('', V1, V2) end.
merge(_K, V1, V2)                -> meshup_resolver:resolve(rimu_resolver_defaulty,
                                                            V1,
                                                            V2).

%%%_* Tests ============================================================
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

basic_test() ->
  rimu_test:with_rimu(fun() ->
    [ {[NS, B, K1], V1}
    , {[_,  _, K2], V2}
    ]          = rimu_test:fresh_kvs(2),
    [ {I, IK}
    ]          = rimu_test:gen_idxs(1),
    ok         = meshup_store:put_(?MODULE, [NS, B, K1, I, IK], V1),
    ok         = meshup_store:put_(?MODULE, [NS, B, K2, I, IK], V2),
    {ok, V1}   = meshup_store:get_(?MODULE, [NS, B, K1]),
    {ok, V2}   = meshup_store:get_(?MODULE, [NS, B, K2]),
    {ok, Vs}   = meshup_store:get_(?MODULE, [NS, B, I, IK]),
    true       = tulib_predicates:is_permutation(Vs, [V1, V2]),
    ok         = meshup_store:del(?MODULE, [NS, B, K1]),
    {ok, [V2]} = meshup_store:get_(?MODULE, [NS, B, I, IK]),
    ok
  end).

-endif.

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
