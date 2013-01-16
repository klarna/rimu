%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc RiMU supervisor.
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
-module(rimu_sup).
-behaviour(supervisor).

%%%_* Exports ==========================================================
-export([ start_link/0
        ]).

-export([ init/1
        ]).

%%%_* Include ==========================================================
%%-include_lib("").

%%%_* Code =============================================================
start_link() -> supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
  {ok, tulib_strats:worker_supervisor_strat(
         [ tulib_strats:permanent_worker_spec(rimu)
         , tulib_strats:permanent_worker_spec(rimu_session_logger)
         , tulib_strats:permanent_worker_spec(rimu_session_monitor)
         , tulib_strats:permanent_worker_spec(rimu_counters)
         ])}.

%%%_* Tests ============================================================
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

basic_test() ->
  rimu_test:with_rimu(fun() ->
    tulib_processes:kill(rimu),
    tulib_processes:kill(rimu_session_logger),
    tulib_processes:kill(rimu_session_monitor),
    tulib_processes:kill(rimu_counters),
    timer:sleep(100)
  end).

-endif.

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
