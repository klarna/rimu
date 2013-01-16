
                        8"""8     8""8""8 8   8
                        8   8  e  8  8  8 8   8
                        8eee8e 8  8e 8  8 8e  8
                        88   8 8e 88 8  8 88  8
                        88   8 88 88 8  8 88  8
                        88   8 88 88 8  8 88ee8

## Overview
RiMU is a system for executing MeshUp workflows on top of Riak.

It does this by:
* Establishing a naming convention for use in MeshUp contracts (which
  corresponds roughly to a query language).
* Providing meshup_store implementations for use by MeshUp internally
  (rimu_session_store) and by workflows running in MeshUp
  (rimu_store).
* Starting servers which handle recovery (rimu_session_logger,
  rimu_session_monitor).
* Defining some basic resolvers for common conflict scenarios.

Additionaly, you get:
* A nice CRUD-style API to Riak (rimu). See in particular
  rimu:transaction/3.
* An implementation of distributed counters for Riak (rimu_counters).

RiMU relies on KRC for the actual communication with a Riak cluster.

## Installation
jakob@fluffy.primat.es:~/git/klarna/rimu$ gmake

jakob@fluffy.primat.es:~/git/klarna/rimu$ gmake test #needs Riak cluster

## Manifest
* rimu.app.src                -- application resource file
* rimu.erl                    -- Direct DB access
* rimu.hrl                    -- shared macros and such
* rimu_app.erl                -- application
* rimu_counters.erl           -- Distributed counters
* rimu_names.erl              -- RiMU contract format
* rimu_resolver_defaulty.erl  -- Fails to merge anything
* rimu_resolver_eq.erl        -- Merges equal things
* rimu_resolver_session.erl   -- Merges sessions
* rimu_resolver_tombstone.erl -- Resolves conflicts caused by tombstones
* rimu_session_logger.erl     -- Meshup session logger
* rimu_session_monitor.erl    -- Recovery
* rimu_session_store.erl      -- MeshUp session store
* rimu_store.erl              -- DB access from MeshUp
* rimu_sup.erl                -- supervisor
* rimu_test.erl               -- test helpers
