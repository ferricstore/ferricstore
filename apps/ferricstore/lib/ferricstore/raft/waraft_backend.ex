defmodule Ferricstore.Raft.WARaftBackend do
  @moduledoc """
  Production WARaft backend boundary.

  WARaft is the only default-instance runtime backend.
  """

  alias Ferricstore.ErrorReasons
  alias Ferricstore.NamespaceConfig
  alias Ferricstore.Raft.BlobCommand
  alias Ferricstore.Raft.CommandStamp
  alias Ferricstore.Raft.WARaftBackend.Batcher, as: NamespaceBatcher
  alias Ferricstore.Raft.WARaftBackend.BatcherSupervisor, as: NamespaceBatcherSupervisor
  alias Ferricstore.Raft.WARaftBackend.SyncGate

  @app :ferricstore_waraft_backend
  @table :ferricstore_waraft_backend
  @sup_id :ferricstore_waraft_backend_sup
  @timeout 10_000
  @context_key {__MODULE__, :context}
  @inflight_bytes_key {__MODULE__, :inflight_commit_bytes}
  @max_inflight_bytes_key {__MODULE__, :max_inflight_commit_bytes}
  @shard_count_key {__MODULE__, :shard_count}
  @voter_nodes_key {__MODULE__, :voter_nodes}
  @default_log_module :ferricstore_waraft_spike_segment_log
  @default_commit_batch_max 10_000
  @config_apply_poll_ms 10
  @config_redirects 2
  @log_module_callbacks [
    first_index: 1,
    last_index: 1,
    fold: 6,
    fold_terms: 5,
    get: 2,
    term: 2,
    config: 1,
    append: 4,
    init: 1,
    open: 1,
    close: 2,
    reset: 3,
    truncate: 3,
    trim: 3,
    flush: 1
  ]
  @label_module_callbacks [new_label: 2]
  @membership_actions [
    :add,
    :add_witness,
    :remove,
    :remove_witness,
    :add_participant,
    :promote_participant_if_ready,
    :remove_membership,
    :demote_to_witness
  ]


  use Ferricstore.Raft.WARaftBackend.Sections.Part01
  use Ferricstore.Raft.WARaftBackend.Sections.Part02
  use Ferricstore.Raft.WARaftBackend.Sections.Part03
  use Ferricstore.Raft.WARaftBackend.Sections.Part04
  use Ferricstore.Raft.WARaftBackend.Sections.Part05
end
