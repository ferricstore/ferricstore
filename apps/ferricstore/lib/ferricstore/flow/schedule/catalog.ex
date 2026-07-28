defmodule Ferricstore.Flow.Schedule.Catalog do
  @moduledoc false

  alias Ferricstore.Store.Router

  @schedule_type "__ferricstore_schedule"
  @schedule_id_prefix "__ferricstore_schedule__:"
  @partition_prefix "__ferricstore_schedule__:"
  @partition_buckets 256
  @bitmap_key <<0, "flow-schedule-partitions:1">>
  @count_prefix <<0, "flow-schedule-partition-count:1:">>
  @max_u64 0xFFFF_FFFF_FFFF_FFFF

  @spec bitmap_key() :: binary()
  def bitmap_key, do: @bitmap_key

  @spec count_key(non_neg_integer()) :: binary()
  def count_key(bucket) when is_integer(bucket) and bucket >= 0 and bucket < @partition_buckets,
    do: <<@count_prefix::binary, bucket::unsigned-big-16>>

  @spec bucket(map()) :: {:ok, non_neg_integer()} | :error
  def bucket(%{id: @schedule_id_prefix <> id, type: @schedule_type}) when id != "",
    do: {:ok, :erlang.phash2(id, @partition_buckets)}

  def bucket(_record), do: :error

  @spec encode_bitmap(non_neg_integer()) :: binary()
  def encode_bitmap(bitmap) when is_integer(bitmap) and bitmap >= 0,
    do: <<bitmap::unsigned-big-size(@partition_buckets)>>

  @spec decode_bitmap(term()) :: {:ok, non_neg_integer()} | :error
  def decode_bitmap(nil), do: {:ok, 0}

  def decode_bitmap(<<bitmap::unsigned-big-size(@partition_buckets)>>),
    do: {:ok, bitmap}

  def decode_bitmap(_value), do: :error

  @spec encode_count(pos_integer()) :: binary()
  def encode_count(count) when is_integer(count) and count > 0 and count <= @max_u64,
    do: <<count::unsigned-big-64>>

  @spec decode_count(term()) :: {:ok, non_neg_integer()} | :error
  def decode_count(nil), do: {:ok, 0}
  def decode_count(<<count::unsigned-big-64>>), do: {:ok, count}
  def decode_count(_value), do: :error

  @spec put_bucket(non_neg_integer(), non_neg_integer()) :: non_neg_integer()
  def put_bucket(bitmap, bucket)
      when is_integer(bucket) and bucket >= 0 and bucket < @partition_buckets,
      do: Bitwise.bor(bitmap, Bitwise.bsl(1, bucket))

  @spec delete_bucket(non_neg_integer(), non_neg_integer()) :: non_neg_integer()
  def delete_bucket(bitmap, bucket)
      when is_integer(bucket) and bucket >= 0 and bucket < @partition_buckets,
      do: Bitwise.band(bitmap, Bitwise.bnot(Bitwise.bsl(1, bucket)))

  @spec partition_keys(FerricStore.Instance.t()) :: {:ok, [binary()]} | {:error, binary()}
  def partition_keys(%{shard_count: shard_count, keydir_refs: keydir_refs} = ctx)
      when is_integer(shard_count) and shard_count > 0 and is_tuple(keydir_refs) and
             tuple_size(keydir_refs) >= shard_count do
    0..(shard_count - 1)
    |> Enum.reduce_while({:ok, 0}, fn shard_index, {:ok, bitmap} ->
      case Router.read_shard_value(ctx, shard_index, @bitmap_key) do
        {:ok, value} ->
          case decode_bitmap(value) do
            {:ok, shard_bitmap} -> {:cont, {:ok, Bitwise.bor(bitmap, shard_bitmap)}}
            :error -> {:halt, {:error, "ERR flow schedule catalog is corrupt"}}
          end

        :unavailable ->
          {:halt, {:error, "ERR flow schedule catalog is unavailable"}}

        _invalid ->
          {:halt, {:error, "ERR flow schedule catalog is corrupt"}}
      end
    end)
    |> case do
      {:ok, bitmap} -> {:ok, decode_partition_keys(bitmap)}
      {:error, _reason} = error -> error
    end
  end

  def partition_keys(_ctx), do: {:error, "ERR flow schedule catalog is unavailable"}

  defp decode_partition_keys(bitmap) do
    0..(@partition_buckets - 1)
    |> Enum.reduce([], fn bucket, acc ->
      if Bitwise.band(bitmap, Bitwise.bsl(1, bucket)) == 0,
        do: acc,
        else: [@partition_prefix <> Integer.to_string(bucket) | acc]
    end)
    |> Enum.reverse()
  end
end
