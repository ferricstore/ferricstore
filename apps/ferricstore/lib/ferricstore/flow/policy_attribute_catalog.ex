defmodule Ferricstore.Flow.PolicyAttributeCatalog do
  @moduledoc false

  alias Ferricstore.Flow.Keys
  alias Ferricstore.Flow.LMDB
  alias Ferricstore.Flow.LMDBMirror
  alias Ferricstore.Flow.LMDBWriter
  alias Ferricstore.Flow.PolicyMigrationWorker
  alias Ferricstore.Store.Router

  @repair_magic <<"FPAR", 1>>
  @max_count 0xFFFFFFFFFFFFFFFF

  @spec encode_repair_request(binary()) :: binary()
  def encode_repair_request(name) when is_binary(name) and name != "" do
    <<@repair_magic::binary, byte_size(name)::unsigned-big-32, name::binary>>
  end

  @spec decode_repair_request(term()) :: {:ok, binary()} | :error
  def decode_repair_request(<<@repair_magic::binary, name_size::unsigned-big-32, name::binary>>)
      when name_size > 0 and byte_size(name) == name_size,
      do: {:ok, name}

  def decode_repair_request(_invalid), do: :error

  @spec indexed_member_exists?(FerricStore.Instance.t(), binary()) :: boolean()
  def indexed_member_exists?(ctx, name) when is_binary(name) and name != "" do
    count_key = Keys.policy_indexed_attribute_count_key(name)
    shard_index = Router.shard_for(ctx, count_key)
    path = lmdb_path(ctx, shard_index)
    prefix = Keys.policy_indexed_attribute_member_prefix(name)
    revision_key = Keys.policy_indexed_attribute_revision_key(name)

    with :ok <- flush_projection(ctx, shard_index),
         {:ok, revision_before} <- read_revision(ctx, shard_index, revision_key),
         {:ok, ^revision_before} <- read_projected_revision(path, revision_key),
         {:ok, [_member]} <- LMDB.prefix_entries(path, prefix, 1),
         {:ok, ^revision_before} <- read_projected_revision(path, revision_key),
         {:ok, ^revision_before} <- read_revision(ctx, shard_index, revision_key) do
      true
    else
      _missing_stale_or_unavailable -> false
    end
  end

  @spec request_repair(FerricStore.Instance.t(), binary()) :: :ok
  def request_repair(ctx, name) when is_binary(name) and name != "" do
    PolicyMigrationWorker.request_attribute_repair(ctx, name)
  end

  @spec repair_next(FerricStore.Instance.t(), non_neg_integer()) ::
          {:ok, :idle | map()} | {:retry, term()} | {:error, term()}
  def repair_next(ctx, shard_index) do
    cond do
      LMDBMirror.degraded_shard?(ctx, shard_index) ->
        {:retry, :policy_attribute_catalog_projection_degraded}

      true ->
        with :ok <- flush_projection(ctx, shard_index),
             {:ok, repair} <- next_repair(ctx, shard_index) do
          repair_attribute(ctx, shard_index, repair)
        end
    end
  rescue
    error -> {:error, {:policy_attribute_catalog_repair_exception, error}}
  catch
    :exit, reason -> {:error, {:policy_attribute_catalog_repair_exit, reason}}
  end

  defp next_repair(ctx, shard_index) do
    path = lmdb_path(ctx, shard_index)
    prefix = Keys.policy_indexed_attribute_repair_prefix()

    case LMDB.prefix_entries(path, prefix, 1) do
      {:ok, []} ->
        {:ok, :idle}

      {:ok, [{key, encoded_value}]} ->
        with {:ok, value} <- decode_mirror_value(encoded_value),
             {:ok, name} <- decode_repair_request(value),
             true <- key == Keys.policy_indexed_attribute_repair_key(name) do
          {:ok, %{name: name}}
        else
          _invalid -> {:error, :corrupt_policy_attribute_catalog_repair}
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp repair_attribute(_ctx, _shard_index, :idle), do: {:ok, :idle}

  defp repair_attribute(ctx, shard_index, %{name: name}) do
    revision_key = Keys.policy_indexed_attribute_revision_key(name)
    path = lmdb_path(ctx, shard_index)

    with {:ok, revision_before} <- read_revision(ctx, shard_index, revision_key),
         {:ok, projected_revision_before} <- read_projected_revision(path, revision_key),
         true <- projected_revision_before == revision_before,
         {:ok, count} <-
           LMDB.prefix_count(
             path,
             Keys.policy_indexed_attribute_member_prefix(name)
           ),
         true <- is_integer(count) and count >= 0 and count <= @max_count,
         {:ok, projected_revision_after} <- read_projected_revision(path, revision_key),
         {:ok, revision_after} <- read_revision(ctx, shard_index, revision_key) do
      if revision_before == projected_revision_after and revision_before == revision_after do
        Router.flow_policy_attribute_catalog_repair(ctx, shard_index, %{
          name: name,
          expected_revision: revision_after,
          count: count
        })
      else
        {:retry, :policy_attribute_catalog_revision_changed}
      end
    else
      false -> {:retry, :policy_attribute_catalog_revision_changed}
      :unavailable -> {:retry, :policy_attribute_catalog_revision_unavailable}
      {:error, _reason} = error -> error
    end
  end

  defp read_revision(ctx, shard_index, revision_key) do
    case Router.read_shard_value(ctx, shard_index, revision_key) do
      {:ok, <<revision::unsigned-big-64>>} -> {:ok, revision}
      {:ok, _missing_or_invalid} -> {:error, :invalid_policy_attribute_catalog_revision}
      :unavailable -> :unavailable
    end
  end

  defp read_projected_revision(path, revision_key) do
    with {:ok, encoded_value} <- LMDB.get(path, revision_key),
         {:ok, <<revision::unsigned-big-64>>} <- decode_mirror_value(encoded_value) do
      {:ok, revision}
    else
      :not_found -> {:error, :missing_projected_policy_attribute_catalog_revision}
      {:error, _reason} = error -> error
      _invalid -> {:error, :invalid_projected_policy_attribute_catalog_revision}
    end
  end

  defp flush_projection(ctx, shard_index) do
    case LMDBWriter.flush(ctx.name, shard_index, 30_000) do
      :ok -> :ok
      {:error, :writer_not_started} -> {:retry, :policy_attribute_catalog_writer_not_started}
      {:error, {:noproc, _reason}} -> {:retry, :policy_attribute_catalog_writer_not_started}
      {:error, _reason} = error -> error
    end
  end

  defp decode_mirror_value(encoded_value) when is_binary(encoded_value) do
    case LMDB.decode_value(encoded_value, 0) do
      {:ok, value} when is_binary(value) -> {:ok, value}
      _invalid -> :error
    end
  end

  defp lmdb_path(ctx, shard_index) do
    ctx.data_dir
    |> Ferricstore.DataDir.shard_data_path(shard_index)
    |> LMDB.path()
  end
end
