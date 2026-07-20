defmodule Ferricstore.Flow.Query.MandatoryScope do
  @moduledoc false

  alias FerricStore.Flow.MetadataExtension
  alias FerricStore.Flow.MetadataExtension.Snapshot
  alias Ferricstore.Flow.{StorageScope, SystemMetadata}
  alias Ferricstore.TermCodec

  @max_branches 4

  @derive {Inspect, only: [:mode, :generation, :schema_digest, :digest]}
  @enforce_keys [:mode, :generation, :schema_digest, :branches, :prefixes, :digest]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          mode: :dedicated | :shared,
          generation: non_neg_integer(),
          schema_digest: <<_::256>>,
          branches: [SystemMetadata.t()],
          prefixes: [binary() | nil],
          digest: <<_::256>>
        }

  @spec dedicated() :: t()
  def dedicated do
    schema_digest = <<0::256>>
    branches = [%{}]
    digest = scope_digest(:dedicated, 0, schema_digest, branches)

    %__MODULE__{
      mode: :dedicated,
      generation: 0,
      schema_digest: schema_digest,
      branches: branches,
      prefixes: [nil],
      digest: digest
    }
  end

  @spec bind(map(), atom()) :: {:ok, t()} | {:error, atom()}
  def bind(ctx, source) when is_map(ctx) and is_atom(source) do
    with {:ok, %Snapshot{} = snapshot} <- MetadataExtension.snapshot(ctx),
         {:ok, branches} <- MetadataExtension.bind_query_metadata(ctx, source),
         :ok <- validate_branches(snapshot, branches),
         {:ok, prefixes} <- scope_prefixes(branches) do
      canonical_branches = Enum.map(branches, &canonical_metadata/1)

      digest =
        scope_digest(
          snapshot.mode,
          snapshot.generation,
          snapshot.schema_digest,
          canonical_branches
        )

      {:ok,
       %__MODULE__{
         mode: snapshot.mode,
         generation: snapshot.generation,
         schema_digest: snapshot.schema_digest,
         branches: branches,
         prefixes: prefixes,
         digest: digest
       }}
    end
  end

  def bind(_ctx, _source), do: {:error, :flow_metadata_extension_unavailable}

  @spec validate(t()) :: :ok | {:error, :invalid_flow_mandatory_scope}
  def validate(%__MODULE__{
        mode: mode,
        generation: generation,
        schema_digest: schema_digest,
        branches: branches,
        prefixes: prefixes,
        digest: digest
      })
      when mode in [:dedicated, :shared] and is_integer(generation) and generation >= 0 and
             is_binary(schema_digest) and byte_size(schema_digest) == 32 and is_list(branches) and
             is_list(prefixes) and is_binary(digest) and byte_size(digest) == 32 do
    with :ok <- validate_bounded_branches(mode, branches),
         {:ok, expected_prefixes} <- scope_prefixes(branches),
         true <- expected_prefixes == prefixes,
         expected_digest <- scope_digest(mode, generation, schema_digest, branches),
         true <- :crypto.hash_equals(expected_digest, digest) do
      :ok
    else
      _invalid -> {:error, :invalid_flow_mandatory_scope}
    end
  end

  def validate(_scope), do: {:error, :invalid_flow_mandatory_scope}

  @spec validate_against(t(), Snapshot.t()) :: :ok | {:error, :invalid_flow_mandatory_scope}
  def validate_against(
        %__MODULE__{} = scope,
        %Snapshot{
          mode: mode,
          generation: generation,
          schema_digest: schema_digest
        } = snapshot
      ) do
    with :ok <- validate(scope),
         true <- scope.mode == mode,
         true <- scope.generation == generation,
         true <- :crypto.hash_equals(scope.schema_digest, schema_digest),
         :ok <- validate_branches(snapshot, scope.branches) do
      :ok
    else
      _invalid -> {:error, :invalid_flow_mandatory_scope}
    end
  end

  def validate_against(_scope, _snapshot), do: {:error, :invalid_flow_mandatory_scope}

  @spec branch_count(t()) :: pos_integer()
  def branch_count(%__MODULE__{branches: branches}), do: length(branches)

  @spec single_prefix(t()) :: {:ok, binary() | nil} | {:error, :flow_scope_union}
  def single_prefix(%__MODULE__{prefixes: [prefix]}), do: {:ok, prefix}
  def single_prefix(%__MODULE__{}), do: {:error, :flow_scope_union}

  @spec single_metadata(t()) :: {:ok, SystemMetadata.t()} | {:error, :flow_scope_union}
  def single_metadata(%__MODULE__{branches: [metadata]}), do: {:ok, metadata}
  def single_metadata(%__MODULE__{}), do: {:error, :flow_scope_union}

  @spec physical_partition_key(t(), binary()) ::
          {:ok, binary()} | {:error, :flow_scope_union | :invalid_flow_system_metadata}
  def physical_partition_key(%__MODULE__{} = scope, logical_partition_key)
      when is_binary(logical_partition_key) and logical_partition_key != "" do
    with :ok <- validate(scope),
         do: derive_physical_partition_key(scope, logical_partition_key)
  end

  def physical_partition_key(_scope, _logical_partition_key),
    do: {:error, :invalid_flow_mandatory_scope}

  @spec derive_keys(t(), binary()) ::
          {:ok,
           %{
             physical_partition_key: binary(),
             admission_key: binary(),
             statistics_key: binary(),
             query_binding: <<_::256>>
           }}
          | {:error, atom()}
  def derive_keys(%__MODULE__{} = scope, logical_partition_key)
      when is_binary(logical_partition_key) and logical_partition_key != "" do
    with :ok <- validate(scope),
         {:ok, physical_partition_key} <-
           derive_physical_partition_key(scope, logical_partition_key) do
      tenant_key = if scope.mode == :dedicated, do: logical_partition_key, else: scope.digest

      {:ok,
       %{
         physical_partition_key: physical_partition_key,
         admission_key: tenant_key,
         statistics_key: tenant_key,
         query_binding: query_binding_digest(scope, logical_partition_key)
       }}
    end
  end

  def derive_keys(_scope, _logical_partition_key),
    do: {:error, :invalid_flow_mandatory_scope}

  @spec admission_key(t(), binary()) ::
          {:ok, binary()} | {:error, :invalid_flow_mandatory_scope}
  def admission_key(%__MODULE__{} = scope, logical_partition_key)
      when is_binary(logical_partition_key) and logical_partition_key != "" do
    with {:ok, keys} <- derive_keys(scope, logical_partition_key),
         do: {:ok, keys.admission_key}
  end

  def admission_key(_scope, _logical_partition_key),
    do: {:error, :invalid_flow_mandatory_scope}

  @spec statistics_key(t(), binary()) ::
          {:ok, binary()} | {:error, :invalid_flow_mandatory_scope}
  def statistics_key(%__MODULE__{} = scope, logical_partition_key),
    do:
      with(
        {:ok, keys} <- derive_keys(scope, logical_partition_key),
        do: {:ok, keys.statistics_key}
      )

  def statistics_key(_scope, _logical_partition_key),
    do: {:error, :invalid_flow_mandatory_scope}

  @spec query_binding(t(), binary()) ::
          {:ok, <<_::256>>} | {:error, :invalid_flow_mandatory_scope}
  def query_binding(%__MODULE__{} = scope, logical_partition_key)
      when is_binary(logical_partition_key) and logical_partition_key != "" do
    with {:ok, keys} <- derive_keys(scope, logical_partition_key),
         do: {:ok, keys.query_binding}
  end

  def query_binding(_scope, _logical_partition_key),
    do: {:error, :invalid_flow_mandatory_scope}

  @spec verify_record(t(), map()) :: :ok | {:error, :flow_scope_mismatch}
  def verify_record(%__MODULE__{branches: branches}, record) when is_map(record) do
    metadata = Map.get(record, :system_metadata, %{})

    if Enum.any?(branches, &(&1 == metadata)),
      do: :ok,
      else: {:error, :flow_scope_mismatch}
  end

  def verify_record(%__MODULE__{}, _record), do: {:error, :flow_scope_mismatch}

  defp validate_branches(%Snapshot{} = snapshot, branches)
       when is_list(branches) and branches != [] and length(branches) <= @max_branches do
    with true <-
           Enum.all?(branches, &(SystemMetadata.validate_against(&1, snapshot.fields) == :ok)),
         true <- length(Enum.uniq(branches)) == length(branches),
         true <- valid_mode_branches?(snapshot.mode, branches) do
      :ok
    else
      _invalid -> {:error, :invalid_flow_system_metadata}
    end
  end

  defp validate_branches(%Snapshot{}, _branches),
    do: {:error, :invalid_flow_system_metadata}

  defp validate_bounded_branches(mode, branches) do
    with true <- bounded_nonempty_list?(branches, @max_branches),
         true <- Enum.all?(branches, &(SystemMetadata.validate(&1) == :ok)),
         true <- length(Enum.uniq(branches)) == length(branches),
         true <- valid_mode_branches?(mode, branches) do
      :ok
    else
      _invalid -> {:error, :invalid_flow_mandatory_scope}
    end
  end

  defp bounded_nonempty_list?([], _remaining), do: false
  defp bounded_nonempty_list?([_head], remaining) when remaining > 0, do: true

  defp bounded_nonempty_list?([_head | tail], remaining) when remaining > 1,
    do: bounded_nonempty_list?(tail, remaining - 1)

  defp bounded_nonempty_list?(_values, _remaining), do: false

  defp valid_mode_branches?(:dedicated, [%{} = metadata]), do: map_size(metadata) == 0

  defp valid_mode_branches?(:shared, branches),
    do: Enum.all?(branches, &shared_branch?/1)

  defp shared_branch?(metadata) do
    Enum.any?(metadata, fn {_id, {_version, _type, role, _value}} ->
      role == :isolation_scope
    end)
  end

  defp scope_prefixes(branches) do
    Enum.reduce_while(branches, {:ok, []}, fn metadata, {:ok, acc} ->
      case SystemMetadata.scope_prefix(metadata) do
        {:ok, prefix} -> {:cont, {:ok, [prefix | acc]}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, reversed} -> {:ok, Enum.reverse(reversed)}
      {:error, _reason} = error -> error
    end
  end

  defp canonical_metadata(metadata), do: metadata |> Enum.sort_by(&elem(&1, 0))

  defp derive_physical_partition_key(scope, logical_partition_key) do
    with {:ok, scope_prefix} <- single_prefix(scope),
         do: StorageScope.physical_partition_key(logical_partition_key, scope_prefix)
  end

  defp query_binding_digest(scope, logical_partition_key) do
    {:ferric_flow_query_scope_binding, 1, scope.digest, logical_partition_key}
    |> TermCodec.encode()
    |> then(&:crypto.hash(:sha256, &1))
  end

  defp scope_digest(mode, generation, schema_digest, branches) do
    canonical_branches = Enum.map(branches, &canonical_metadata/1)

    {:ferric_flow_mandatory_scope, 1, mode, generation, schema_digest, canonical_branches}
    |> TermCodec.encode()
    |> then(&:crypto.hash(:sha256, &1))
  end
end
