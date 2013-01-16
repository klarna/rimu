%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc Session logger (c.f. meshup_sessions.erl).
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
-module(rimu_session_logger).
-behaviour(meshup_logger).
-behaviour(gen_fsm).

%%%_* Exports ==========================================================
%% API
-export([ start_link/0
        , stop/0
        ]).

%% meshup_logger callbacks
-export([ log/2
        , redo/0
        ]).

%% gen_fsm callbacks: states
-export([ compact/3
        , do_redo/3
        , idle/3
        , redo/3
        ]).

%% gen_fsm callbacks: other
-export([ code_change/4
        , handle_event/3
        , handle_sync_event/4
        , handle_info/3
        , init/1
        , terminate/3
        ]).

%% internal exports
-export([ compact/0
        ]).

%%%_* Includes =========================================================
-include("rimu.hrl").

-include_lib("tulib/include/assert.hrl").
-include_lib("tulib/include/logging.hrl").
-include_lib("tulib/include/metrics.hrl").
-include_lib("tulib/include/prelude.hrl").
-include_lib("tulib/include/types.hrl").

%%%_* Code =============================================================
%%%_ * Types -----------------------------------------------------------
%% Once a MeshUp session has computed a result, it goes through a commit
%% protocol to persist its state to permanent storage. Whenever it has
%% completed a transition in the FSM associated with this protocol, it
%% sends the name of the new state to the gen_fsm defined in this file,
%% which maintains the per-node session log.
-type id() :: meshup_sessions:id().

%% Commit protocol state-machine
%% =============================
%%
%% PREPARE --> LOG --> COMMIT --> CLEANUP
%%    |         |        /\
%%    |         |        |
%%   \/        \/        |
%% ABORT      RETRY -----+
%%            /\  |
%%            |   |
%%            +---+
%%
%% ABORT and RETRY are implicit states, induced by timeouts.
%%
%% We initiate a commit by issuing a PREPARE. If writing to the
%% replicated redo-log fails, we ABORT - otherwise, we LOG. Once we've
%% pushed all data items to their stores, we COMMIT. If there are any
%% failures, we RETRY until we manage to COMMIT. Finally, we CLEANUP.
-type commit_state() :: prepare
                      | log
                      | commit
                      | cleanup.

-define(is_commit_state(X),
        (X =:= prepare orelse
         X =:= log     orelse
         X =:= commit  orelse
         X =:= cleanup)).

%% Some further terminology:
%% The `session log' contains the commit history for all sessions which
%% have been executed on the local node.
%% The `compacted session log' contains only the current state for each
%% session which has been executed on the local node and hasn't reached
%% a halting state yet (i.e. CLEANUP and ABORT sessions are dropped).
-type session_log() :: tulib_dlogs:dlog().

%%%_ * API -------------------------------------------------------------
start_link() -> gen_fsm:start_link({local, ?MODULE}, ?MODULE, [], []).
stop()       -> send_all_state_event(stop).


sync_send_event(E)           -> gen_fsm:sync_send_event(?MODULE, E).
send_all_state_event(E)      -> gen_fsm:send_all_state_event(?MODULE, E).
sync_send_all_state_event(E) -> gen_fsm:sync_send_all_state_event(?MODULE, E).

%%%_ * meshup_logger callbacks -----------------------------------------
%% api
log(ID, State) -> sync_send_all_state_event({timestamp(), ID, State}).
redo()         -> sync_send_event(redo).

%% internal
compact()      -> sync_send_event(compact).
compact_ok()   -> sync_send_event(compact_ok).
redo_ok()      -> sync_send_event(redo_ok).

%%%_ * gen_fsm callbacks -----------------------------------------------
%% The state consists of a handle to the current session log, the PID of
%% the process currently performing a background operation (if any), and
%% the name of the client currently blocking on an asynchronous
%% operation (if any).
-record(s,
        { log=throw('#s.log') :: session_log()
        , from=''             :: '' | {_, _}
        , pid=''              :: '' | pid()
        }).

init([]) ->
  process_flag(trap_exit, true),
  _ = init_gc(),
  {ok, idle, #s{log=do_init()}}.

init_gc() ->
  Time = tulib_util:get_env(log_gc_interval, 1000 * 60 * 10, ?APP),
  {ok, TRef} = timer:apply_interval(Time, ?MODULE, compact, []),
  TRef.

terminate(_, _, #s{log=Log}) ->
  tulib_dlogs:close(Log),
  ok.

code_change(_, _, S, _) -> {ok, S}.

%% The logger serializes asynchronous operations (compaction and
%% computing redo-ops from the compacted log). The legal
%% transition-sequences are depicted below.
%%
%%           compact             redo       compact_ok
%% --> IDLE <----------> COMPACT ----> REDO ----------> DO_REDO
%%     /\ |  compact_ok                 /\                 |
%%     |  |redo                         |                  |
%%     |  +-----------------------------+           redo_ok|
%%     +---------------------------------------------------+
%%
-define(T(State, S),        {next_state, State, S}).
-define(T(Reply, State, S), {reply, Reply, State, S}).

idle(compact,       _, S) -> ?T(ok,    compact, do_compact(S)            );
idle(redo,          F, S) -> ?T(       redo,    (do_compact(S))#s{from=F});
idle(_,             _, S) -> ?T(error, idle,    S                        ).

compact(compact_ok, _, S) -> ?T(ok,    idle,    S#s{pid=''}              );
compact(redo,       F, S) -> ?T(       redo,    S#s{from=F}              );
compact(_,          _, S) -> ?T(error, compact, S                        ).

redo(compact_ok,    _, S) -> ?T(ok,    do_redo, do_redo(S)               );
redo(_,             _, S) -> ?T(error, redo,    S                        ).

do_redo(redo_ok,    _, S) -> ?T(ok,    idle,    S#s{pid='', from=''}     );
do_redo(_,          _, S) -> ?T(error, do_redo, S                        ).

%% We always accept appends to the session log.
handle_sync_event({_TS, _ID, CommitState} = Op, _, State, #s{log=Log} = S)
  when ?is_commit_state(CommitState) ->
  tulib_dlogs:log(Log, Op),
  ?T(ok, State, S).

handle_event(stop, _, S) -> {stop, normal, S}.

handle_info({'EXIT', Pid, Rsn}, State, S) ->
  case Rsn of
    normal ->
      ?debug("EXIT ~p: ~p", [Pid, Rsn]),
      ?T(State, S);
    _ ->
      ?error("EXIT ~p: ~p", [Pid, Rsn]),
      {stop, Rsn, S}
  end;
handle_info(Msg, State, S) ->
  ?warning("~p", [Msg]),
  ?T(State, S).

%%%_ * Internals -------------------------------------------------------
%%%_  * Log replay -----------------------------------------------------
%% We figure out which commit-state each session is in by going through
%% the session log in chronological order and simulating the commit
%% state-machine for each entry.
-spec replay(file() | [file()]) -> [{id(), commit_state()}].
replay(Log) ->
  replay(Log, dirty).
replay(Log, LogState) ->
  Insert  = insert(LogState),
  {ok, T} =
    tulib_dlogs:foldterms(
      fun({TS, ID, S}, Tree) ->
        Insert(ID, {TS, fsm(gb_trees:lookup(ID, Tree), S)}, Tree)
      end, gb_trees:empty(), Log),
  gb_trees:to_list(T).

%% Compacted log should have one entry per ID.
insert(dirty)     -> fun insert/3;
insert(compacted) -> fun gb_trees:insert/3.

insert(ID, State, Tree) ->
  try gb_trees:update(ID, State, Tree)
  catch _:_ -> gb_trees:insert(ID, State, Tree)
  end.

%%  old                 new     current
fsm(none,               S)   -> S; %previous states compacted away
fsm({value, {_TS, S0}}, S)   -> meshup_sessions:state(S0, S);
fsm(Old,                New) -> throw({fsm, Old, New}).

%%%_  * Compaction -----------------------------------------------------
%% The session log is represented on disk by two files: ?NEXT, which new
%% entries are appended to and ?PREV, the compacted collection of old
%% entries. These two files must always exist.
-define(NEXT, path("NEXT.dlog")).
-define(PREV, path("PREV.dlog")).

path(File) ->
  {ok, Dir} = application:get_env(?APP, log_dir),
  filename:join(Dir, File).

ensure_env() ->
  ensure(?PREV),
  ensure(?NEXT).

%% @doc Create File if it doesn't exist. Make sure that it's not
%% corrupted if it does.
ensure(File) ->
  tulib_sh:mkdir_p(filename:dirname(File)),
  {ok, Log} = tulib_dlogs:open(File),
  tulib_dlogs:close(Log).

%% Two temporary files are used during compaction, which works as
%% follows:
%%   1)    Close ?NEXT
%%   2)    Rename ?NEXT to ?CUR
%%   3a)   Open ?NEXT
%%   3b.1) Compact ?PREV and ?CUR into ?TMP
%%   3b.2) Delete ?CUR
%%   3b.3) Rename ?TMP to ?PREV
%% The two branches of 3) are executed concurrently.
%% We assume that `rename' and `delete' are atomic operations.
-define(CUR, path("CUR.dlog")).
-define(TMP, path("TMP.dlog")).

-spec do_compact(#s{}) -> #s{}.
do_compact(#s{log=Old} = S) ->
  ?info("begin compacting"),
  ?increment([?MODULE, compactions]),
  {New, Pid} = do_compact1(Old),
  S#s{log=New, pid=Pid}.

do_compact1(Old) ->
  eval([{'1', Old}, '2']),
  Pid =
    proc_lib:spawn_link(?thunk(
      eval(['3b.1', '3b.2', '3b.3']),
      ?info("end compacting", []),
      compact_ok())),
  New = eval('3a'),
  {New, Pid}.

eval(Ops) when is_list(Ops) ->
  [eval(Op) || Op <- Ops];
eval({'1', Next}) ->
  tulib_dlogs:close(Next);
eval('2') ->
  tulib_sh:mv(?NEXT, ?CUR);
eval('3a') ->
  {ok, Next} = tulib_dlogs:open(?NEXT),
  Next;
eval('3b.1') ->
  {ok, _} =
    tulib_dlogs:with_dlog(?TMP,
      fun(Log) ->
        [case meshup_sessions:is_halting_state(CommitState) of
           true  -> ok;
           false -> tulib_dlogs:log(Log, {TS, ID, CommitState})
         end || {ID, {TS, CommitState}}  <- replay([?PREV, ?CUR])]
      end);
eval('3b.2') ->
  tulib_sh:rm_rf(?CUR);
eval('3b.3') ->
  tulib_sh:mv(?TMP, ?PREV).

%% We identify intermediate states with combinations of files currently
%% on disk and provide a recovery strategy for each case (recall that
%% ?NEXT and ?PREV are assumed to always exist).
state() ->
  { filelib:is_file(?NEXT)
  , filelib:is_file(?CUR)
  , filelib:is_file(?PREV)
  , filelib:is_file(?TMP)
  }.

%%       NEXT  CUR    PREV  TMP
recover({true, false, true, false}) -> eval(['2', '3b.1', '3b.2', '3b.3']);
recover({true, true,  true, false}) -> eval([     '3b.1', '3b.2', '3b.3']);
recover({true, true,  true, true})  -> eval([     '3b.1', '3b.2', '3b.3']);
recover({true, false, true, true})  -> eval([                     '3b.3']);
recover(S)                          -> throw({illegal_state, S}).

%% We have:
do_init() ->
  ensure_env(),
  recover(state()),
  {ok, Log} = tulib_dlogs:open(?NEXT),
  Log.

%%%_  * Redo Ops -------------------------------------------------------
%% Periodically, we extract a sequence of actions which will move
%% unfinished sessions towards a halting state from the compacted part
%% of the session log.
%% We assume that the returned ops are evaluated before redo is called
%% again - in which case doing a compaction before computing the redo
%% ops ensures that we don't redo sessions which have already been
%% committed successfully by a previous redo.
-spec do_redo(#s{}) -> #s{}.
do_redo(#s{from=From} = S) ->
  ?info("begin redo"),
  Pid =
    proc_lib:spawn_link(?thunk(
      Ops = [{ID, State} || {ID, {TS, State}} <- replay(?PREV, compacted),
                            eligible(TS)],
      gen_fsm:reply(From, Ops),
      ?debug("end redo"),
      redo_ok())),
  S#s{pid=Pid}.

%% Exclude commits which are still in progress and might have entries
%% in the non-compacted part of the log.
eligible(TS) ->
  Timeout = tulib_util:get_env(state_timeout, 1000 * 60 * 10, ?APP),
  (timestamp() - TS) > Timeout.

timestamp() -> tulib_util:timestamp() div 1000. %ms

%%%_* Tests ============================================================
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

simulate(Ops)     -> simulate(Ops, now()).
simulate(Ops, ID) -> [ok = log(ID, Op) || Op <- Ops], ID.


wait_for_monitor() ->
  case sys:get_status(rimu_session_monitor) of
    {status, _, _, [_, running, _, _, _]} -> ok;
    _                                     -> wait_for_monitor()
  end.

redo_test() ->
  rimu_test:with_rimu([{rimu, state_timeout, 0}], fun() ->
    wait_for_monitor(), %avoid redo race
    ID1  = simulate([prepare]),
    ID2  = simulate([prepare, log]),
    ID3  = simulate([prepare, log, commit]),
    _    = simulate([prepare, log, commit, cleanup]),
    Ops  = redo(),
    true = lists:member({ID1, prepare}, Ops),
    true = lists:member({ID2, log},     Ops),
    true = lists:member({ID3, commit},  Ops),
    ID1  = simulate([cleanup], ID1),
    ID2  = simulate([commit, cleanup], ID2),
    ID3  = simulate([cleanup], ID3),
    []   = redo(),
    ok
  end).

compact_test() ->
  rimu_test:with_rimu(fun() ->
    _     = simulate([prepare, log, commit, cleanup]),
    ok    = compact(),
    error = compact(),
    ok    = timer:sleep(100), %wait for compaction to finish
    []    = replay(?PREV),
    []    = replay(?NEXT)
  end).

-ifdef(NOJENKINS).
recover_test() ->
  rimu_test:with_rimu(fun() ->
    Pid                   = whereis(?MODULE),
    ID                    = simulate([prepare, log]),
    MFA                   = {meshup_sessions, is_halting_state, [log]},
    ok                    = tulib_meck:faultify(MFA),
    ok                    = compact(),

    tulib_loops:retry(fun() -> not tulib_processes:is_up(Pid) end, 1),

    ok                    = tulib_meck:defaultify(MFA),
    {true,true,true,true} = state(),

    tulib_loops:retry(fun() -> tulib_processes:is_up(?MODULE) end, 1),

    ok                    = compact(),
    ok                    = timer:sleep(100),
    [{ID, {_, log}}]      = replay(?PREV),
    ok
  end).
-endif.

cover_test() ->
  tulib_util:with_env(rimu_test:defaults(), fun() ->
    {ok, Pid} = start_link(),
    {ok, baz} = code_change(foo, bar, baz, quux),
    Pid ! snarf,
    error = sync_send_event(snarf),
    ok    = sync_send_event(compact),
    error = sync_send_event(snarf),
    stop(),
    timer:sleep(100),
    catch fsm(foo, bar),
    catch recover({false, false, false, false})
  end).

-endif.

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
