%%% Copyright (c) FerricStore contributors.
%%%
%%% WARaft storage callback shim for the replacement backend.  The record
%%% definitions live in WARaft's Erlang headers, while the actual storage logic
%%% lives in Elixir so it can reuse FerricStore's StateMachine and Bitcask
%%% modules directly.

-module(ferricstore_waraft_backend_storage).

-behaviour(wa_raft_storage).

-export([
    storage_open/2,
    storage_close/1,
    storage_label/1,
    storage_position/1,
    storage_status/1,
    storage_config/1,
    storage_apply/3,
    storage_apply/4,
    storage_apply_config/3,
    storage_read/3,
    storage_create_snapshot/2,
    storage_create_witness_snapshot/2,
    storage_open_snapshot/3,
    storage_make_empty_snapshot/5
]).

-include_lib("wa_raft/include/wa_raft.hrl").

storage_open(#raft_options{} = Options, RootDir) ->
    'Elixir.Ferricstore.Raft.WARaftStorage':open(
        #{
            application => Options#raft_options.application,
            table => Options#raft_options.table,
            partition => Options#raft_options.partition,
            self => Options#raft_options.self,
            database => Options#raft_options.database,
            storage_name => Options#raft_options.storage_name
        },
        RootDir
    ).

storage_close(Handle) ->
    'Elixir.Ferricstore.Raft.WARaftStorage':close(Handle).

storage_position(Handle) ->
    'Elixir.Ferricstore.Raft.WARaftStorage':position(Handle).

storage_status(Handle) ->
    'Elixir.Ferricstore.Raft.WARaftStorage':status(Handle).

storage_label(Handle) ->
    'Elixir.Ferricstore.Raft.WARaftStorage':label(Handle).

storage_config(Handle) ->
    'Elixir.Ferricstore.Raft.WARaftStorage':config(Handle).

storage_apply(Command, Position, Handle) ->
    'Elixir.Ferricstore.Raft.WARaftStorage':apply(Command, Position, Handle).

storage_apply(Command, Position, Label, Handle) ->
    'Elixir.Ferricstore.Raft.WARaftStorage':apply(Command, Position, Label, Handle).

storage_apply_config(Config, Position, Handle) ->
    'Elixir.Ferricstore.Raft.WARaftStorage':apply_config(Config, Position, Handle).

storage_read(Command, Position, Handle) ->
    'Elixir.Ferricstore.Raft.WARaftStorage':read(Command, Position, Handle).

storage_create_snapshot(SnapshotPath, Handle) ->
    'Elixir.Ferricstore.Raft.WARaftStorage':create_snapshot(SnapshotPath, Handle).

storage_create_witness_snapshot(SnapshotPath, Handle) ->
    'Elixir.Ferricstore.Raft.WARaftStorage':create_witness_snapshot(SnapshotPath, Handle).

storage_open_snapshot(SnapshotPath, ExpectedPosition, Handle) ->
    'Elixir.Ferricstore.Raft.WARaftStorage':open_snapshot(
        SnapshotPath,
        ExpectedPosition,
        Handle
    ).

storage_make_empty_snapshot(#raft_options{} = Options, SnapshotPath, Position, Config, Data) ->
    'Elixir.Ferricstore.Raft.WARaftStorage':make_empty_snapshot(
        #{
            application => Options#raft_options.application,
            table => Options#raft_options.table,
            partition => Options#raft_options.partition,
            self => Options#raft_options.self,
            database => Options#raft_options.database,
            storage_name => Options#raft_options.storage_name
        },
        SnapshotPath,
        Position,
        Config,
        Data
    ).
