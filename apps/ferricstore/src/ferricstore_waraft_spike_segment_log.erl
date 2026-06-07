%%% Copyright (c) FerricStore contributors.
%%%
%%% Durable segmented WARaft log provider for the migration spike.
%%% Each append batch is written as CRC-framed records and data-synced before
%%% the Raft append is acknowledged. Same-segment batches pay one data sync for
%%% the group, new segments can reserve disk through keep-size preallocation,
%%% and recovery fails closed on corrupt or ambiguous records.

-module(ferricstore_waraft_spike_segment_log).

-behaviour(wa_raft_log).

-export([
    first_index/1,
    last_index/1,
    fold/6,
    fold_binary/6,
    fold_terms/5,
    get/2,
    term/2,
    config/1,
    fold_disk/3,
    location_for_index/2,
    read_disk/2,
    read_disk_at/4,
    reset_disk_to_position/2,
    memory_status/1,
    ensure_segment_config/1,
    write_projection/3,
    write_projection_batch/3,
    write_projection_batches/2,
    write_projection_batches_sync/2,
    close_process_writers/1
]).

-export([
    append/4
]).

-export([
    init/1,
    open/1,
    close/2,
    reset/3,
    truncate/3,
    trim/3,
    flush/1
]).

-include_lib("wa_raft/include/wa_raft.hrl").
-include_lib("kernel/include/file.hrl").

-define(DEFAULT_SEGMENT_RECORDS, 65536).
-define(SEGMENT_EXT, ".seg").
-define(SEGMENT_CONFIG_FILE, "segment_config.term").
-define(TRIM_FLOOR_FILE, "trim_floor.term").
-define(RECORD_HEADER_SIZE, 8).
-define(MAX_RECORD_BYTES, 1073741824).
-define(MAX_SEGMENT_METADATA_BYTES, 1048576).
-define(REWRITE_MARKER_EXT, ".rewrite.term").
-define(REWRITE_STAGING_PREFIX, ".rewrite.staging.").
-define(REWRITE_BACKUP_PREFIX, ".rewrite.backup.").
-define(REWRITE_GROUP_MAX_RECORDS, 128).
-define(APPEND_FAILURE_MARKER, "segment_log.append_failed.term").
-define(DEFAULT_PREALLOCATE_BYTES, 0).
-define(WRITER_REGISTRY, ferricstore_waraft_segment_writer_registry).
-define(OFFSET_REGISTRY, ferricstore_waraft_segment_offset_registry).
-define(MEMORY_REGISTRY, ferricstore_waraft_segment_log_memory_registry).
-define(LOAD_CONTEXT, ferricstore_waraft_segment_log_load_context).
-define(FOLD_CONTEXT, ferricstore_waraft_segment_log_fold_context).
-define(DEFAULT_MAX_ETS_BYTES, 536870912).
-define(DEFAULT_MAX_ETS_ENTRIES, 65536).
-define(DEFAULT_MIN_ETS_ENTRIES, 4096).


-include("ferricstore_waraft_spike_segment_log/sections/part_01.hrl").
-include("ferricstore_waraft_spike_segment_log/sections/part_02.hrl").
-include("ferricstore_waraft_spike_segment_log/sections/part_03.hrl").
-include("ferricstore_waraft_spike_segment_log/sections/part_04.hrl").
-include("ferricstore_waraft_spike_segment_log/sections/part_05.hrl").
-include("ferricstore_waraft_spike_segment_log/sections/part_06.hrl").
