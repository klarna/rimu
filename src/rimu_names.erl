%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc RiMU naming conventions / query language syntax.
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
-module(rimu_names).

%%%_* Exports ==========================================================
-export([ is_index_read/1
        , is_index_write/1
        , is_name/1
        , is_primary_key/1
        , is_transient/1
        ]).

-export_type([ index_read/0
             , index_write/0
             , name/0
             , primary_key/0
             , transient/0
             ]).

%%%_* Includes =========================================================
-include_lib("tulib/include/assert.hrl").

%%%_* Code =============================================================
-type name()        :: primary_key()
                     | index_read()
                     | index_write()
                     | transient().
-type primary_key() :: [atom()|_].
-type index_read()  :: [atom()|_].
-type index_write() :: [atom()|_].
-type transient()   :: [atom()|_].


is_name(X) ->
  is_primary_key(X) orelse
  is_index_read(X)  orelse
  is_index_write(X) orelse
  is_transient(X).

is_primary_key([NS, B, K] = Name) ->
  ?hence(meshup_contracts:is_name(Name)),
  is_namespace(NS) andalso
  is_bucket(B)     andalso
  is_key(K);
is_primary_key(_) -> false.


is_index_read([NS, B, I, IK] = Name) ->
  ?hence(meshup_contracts:is_name(Name)),
  is_namespace(NS) andalso
  is_bucket(B)     andalso
  is_index(I)      andalso
  is_index_key(IK);
is_index_read(_) -> false.


is_index_write([NS, B, K, I, IK] = Name) ->
  ?hence(meshup_contracts:is_name(Name)),
  is_namespace(NS) andalso
  is_bucket(B)     andalso
  is_key(K)        andalso
  is_index(I)      andalso
  is_index_key(IK);
is_index_write(_) -> false.


is_transient([NS|_] = Name) ->
  ?hence(meshup_contracts:is_name(Name)),
  is_namespace(NS);
is_transient(_) -> false.


is_namespace(NS) -> is_atom(NS).
is_bucket(_)     -> true.
is_key(_)        -> true.
is_index(_)      -> true.
is_index_key(_)  -> true.

%%%_* Tests ============================================================
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

basic_test() ->
  true  = is_primary_key([service, bucket, key]),
  true  = is_index_read([service, bucket, index, index_key]),
  true  = is_index_write([service, bucket, key, index, index_key]),
  true  = is_transient([service, x]),
  false = is_name({service, bucket, key}),
  ok.

-endif.

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
