defmodule Ferricstore.Commands.ProbParameters do
  @moduledoc false

  @max_bloom_bits 8_589_934_592
  @max_bloom_hashes 1_024
  @max_cms_depth 1_024
  @max_cms_counters 16_777_216
  @max_cms_merge_sources 128
  @max_cms_merge_counter_visits @max_cms_counters
  @cuckoo_bucket_size 4
  @max_cuckoo_capacity 1_073_741_824
  @max_topk_k 100_000
  @max_topk_counters 1_048_576

  @type validation_error ::
          :invalid_bloom_dimensions
          | :bloom_bit_limit_exceeded
          | :bloom_hash_limit_exceeded
          | :invalid_cms_dimensions
          | :cms_depth_exceeded
          | :cms_counter_limit_exceeded
          | :cms_merge_source_limit_exceeded
          | :cms_merge_work_limit_exceeded
          | :invalid_cuckoo_parameters
          | :cuckoo_capacity_limit_exceeded
          | :unsupported_cuckoo_bucket_size
          | :invalid_topk_parameters
          | :topk_k_limit_exceeded
          | :topk_counter_limit_exceeded

  @spec validate_bloom_dimensions(term(), term()) :: :ok | {:error, validation_error()}
  def validate_bloom_dimensions(num_bits, num_hashes)
      when is_integer(num_bits) and num_bits > 0 and is_integer(num_hashes) and num_hashes > 0 do
    cond do
      num_bits > @max_bloom_bits -> {:error, :bloom_bit_limit_exceeded}
      num_hashes > @max_bloom_hashes -> {:error, :bloom_hash_limit_exceeded}
      true -> :ok
    end
  end

  def validate_bloom_dimensions(_num_bits, _num_hashes),
    do: {:error, :invalid_bloom_dimensions}

  @spec validate_cms_dimensions(term(), term()) :: :ok | {:error, validation_error()}
  def validate_cms_dimensions(width, depth)
      when is_integer(width) and width > 0 and is_integer(depth) and depth > 0 do
    cond do
      depth > @max_cms_depth ->
        {:error, :cms_depth_exceeded}

      width > div(@max_cms_counters, depth) ->
        {:error, :cms_counter_limit_exceeded}

      true ->
        :ok
    end
  end

  def validate_cms_dimensions(_width, _depth),
    do: {:error, :invalid_cms_dimensions}

  @spec cms_merge_source_limit() :: pos_integer()
  def cms_merge_source_limit, do: @max_cms_merge_sources

  @spec validate_cms_merge_source_count(term()) :: :ok | {:error, validation_error()}
  def validate_cms_merge_source_count(count)
      when is_integer(count) and count > 0 and count <= @max_cms_merge_sources,
      do: :ok

  def validate_cms_merge_source_count(_count),
    do: {:error, :cms_merge_source_limit_exceeded}

  @spec validate_cms_merge_work(term(), term(), term()) ::
          :ok | {:error, validation_error()}
  def validate_cms_merge_work(source_count, width, depth) do
    with :ok <- validate_cms_merge_source_count(source_count),
         :ok <- validate_cms_dimensions(width, depth) do
      counter_count = width * depth

      if source_count <= div(@max_cms_merge_counter_visits, counter_count) do
        :ok
      else
        {:error, :cms_merge_work_limit_exceeded}
      end
    end
  end

  @spec validate_cuckoo_parameters(term(), term()) :: :ok | {:error, validation_error()}
  def validate_cuckoo_parameters(capacity, bucket_size)
      when is_integer(capacity) and capacity > 0 and is_integer(bucket_size) do
    cond do
      capacity > @max_cuckoo_capacity -> {:error, :cuckoo_capacity_limit_exceeded}
      bucket_size != @cuckoo_bucket_size -> {:error, :unsupported_cuckoo_bucket_size}
      true -> :ok
    end
  end

  def validate_cuckoo_parameters(_capacity, _bucket_size),
    do: {:error, :invalid_cuckoo_parameters}

  @spec validate_topk_parameters(term(), term(), term()) ::
          :ok | {:error, validation_error()}
  def validate_topk_parameters(k, width, depth)
      when is_integer(k) and k > 0 and is_integer(width) and width > 0 and is_integer(depth) and
             depth > 0 do
    cond do
      k > @max_topk_k -> {:error, :topk_k_limit_exceeded}
      width > div(@max_topk_counters, depth) -> {:error, :topk_counter_limit_exceeded}
      true -> :ok
    end
  end

  def validate_topk_parameters(_k, _width, _depth),
    do: {:error, :invalid_topk_parameters}
end
