defmodule Ferricstore.MemoryBudget do
  @moduledoc """
  Central sizing policy for memory-sensitive queues and caches.

  The limits here are startup/default guardrails. Live pressure response still
  belongs to `Ferricstore.MemoryGuard`; this module decides the safe default
  size for structures that otherwise grow independently of MemoryGuard:

    * WARaft segment-log ETS tail cache
    * WARaft apply-projection cache before segment projection compaction
    * Flow history projector pending queue
    * Flow LMDB writer mailbox and per-enqueue batch size

  Explicit application env always wins. When a key is unset, defaults are
  derived from RAM, disk free space, CPU count, and shard count.
  """

  @gib 1024 * 1024 * 1024
  @mib 1024 * 1024

  @default_memory_limit 1 * @gib
  @segment_total_memory_percent 1
  @segment_total_min_bytes 64 * @mib
  @segment_total_max_bytes 256 * @mib
  @segment_per_shard_min_bytes 8 * @mib
  @segment_per_shard_max_bytes 16 * @mib
  @segment_entry_budget_bytes 1024
  @segment_max_entries_cap 262_144

  # This is the Flow/WARaft handoff cache used while cold LMDB/history
  # projection catches up. It is not an LMDB queue, but it shows up as "LMDB
  # lag" operationally: if this cap is too low, terminal Flow bursts spill and
  # compact synchronously before the lagged projection can consume the rows.
  # The 1M DBOS-style benchmark regressed from ~58K/s to ~35K/s when this was
  # capped at 65K entries/shard, so keep the cap memory-derived and avoid
  # lowering `:waraft_apply_projection_cache_max_entries` without a burst test.
  @apply_projection_cache_memory_percent 2
  @apply_projection_cache_entry_bytes 512
  @apply_projection_cache_min_entries 1_024
  @apply_projection_cache_total_max_entries 4_194_304
  @apply_projection_cache_per_shard_max_entries 262_144

  @history_pending_memory_percent 2
  @history_pending_entry_bytes 2_048
  @history_pending_max_entries 200_000

  # LMDB writer limits protect the async cold-projection mailbox/enqueue path.
  # They should bound memory and projection lag, not throttle hot Flow command
  # throughput. If throughput drops while LMDB lag is visible, check the
  # apply-projection cache above before making these drains more aggressive.
  @lmdb_mailbox_memory_percent 1
  @lmdb_mailbox_entry_bytes 4_096
  @lmdb_mailbox_max_messages 100_000

  @lmdb_enqueue_memory_percent 2
  @lmdb_enqueue_op_bytes 2_048
  @lmdb_enqueue_max_ops 262_144
  @adaptive_cache_key {__MODULE__, :adaptive_limits}

  @known_keys MapSet.new([
                :flow_history_projector_max_pending_entries,
                :flow_lmdb_writer_max_mailbox_messages,
                :flow_lmdb_writer_max_enqueue_ops,
                :waraft_segment_log_max_ets_bytes,
                :waraft_segment_log_max_ets_entries,
                :waraft_segment_log_min_ets_entries,
                :waraft_apply_projection_cache_max_entries
              ])

  @type limit_key ::
          :flow_history_projector_max_pending_entries
          | :flow_lmdb_writer_max_mailbox_messages
          | :flow_lmdb_writer_max_enqueue_ops
          | :waraft_segment_log_max_ets_bytes
          | :waraft_segment_log_max_ets_entries
          | :waraft_segment_log_min_ets_entries
          | :waraft_apply_projection_cache_max_entries

  @doc """
  Returns a configured or adaptive limit for a guardrail key.

  `false`, `:infinity`, `"off"`, `"false"`, and `"infinity"` intentionally
  disable a cap and return `:infinity`.
  """
  @spec limit(atom(), non_neg_integer() | :infinity) :: non_neg_integer() | :infinity
  def limit(key, default \\ :infinity) when is_atom(key) do
    case explicit_limit(key) do
      {:ok, value} ->
        value

      :unset ->
        if MapSet.member?(@known_keys, key) do
          Map.fetch!(cached_adaptive_limits(), key)
        else
          default
        end
    end
  end

  @doc """
  Clears the cached adaptive default snapshot.

  Production code normally keeps one startup-sized snapshot so hot-path callers
  do not rescan RAM/disk/CPU state. Tests and config reload paths can reset it
  before constructing new workers.
  """
  @spec reset_cache() :: :ok
  def reset_cache do
    :persistent_term.erase(@adaptive_cache_key)
    :ok
  end

  @doc "Returns all adaptive limits for a hardware profile."
  @spec adaptive_limits(map() | keyword()) :: map()
  def adaptive_limits(profile \\ hardware_profile()) do
    profile = normalize_profile(profile)
    memory_budget = memory_budget_bytes(profile)
    shard_count = max(profile.shard_count, 1)
    cpu = max(profile.schedulers_online, 1)
    disk_factor = disk_pressure_factor(profile.disk_free_bytes)

    segment_total =
      memory_budget
      |> percent(@segment_total_memory_percent)
      |> clamp(@segment_total_min_bytes, @segment_total_max_bytes)

    segment_per_shard =
      segment_total
      |> div(shard_count)
      |> clamp(@segment_per_shard_min_bytes, @segment_per_shard_max_bytes)

    min_segment_entries = clamp(cpu * 32, 128, 1_024)

    max_segment_entries =
      segment_per_shard
      |> div(@segment_entry_budget_bytes)
      |> clamp(min_segment_entries, @segment_max_entries_cap)

    apply_projection_cache_entries =
      memory_budget
      |> adaptive_apply_projection_cache_entries(shard_count, cpu, disk_factor)

    %{
      waraft_segment_log_max_ets_bytes: segment_per_shard,
      waraft_segment_log_min_ets_entries: min(min_segment_entries, max_segment_entries),
      waraft_segment_log_max_ets_entries: max_segment_entries,
      waraft_apply_projection_cache_max_entries: apply_projection_cache_entries,
      flow_history_projector_max_pending_entries:
        adaptive_queue_limit(
          memory_budget,
          @history_pending_memory_percent,
          @history_pending_entry_bytes,
          cpu * 1024,
          @history_pending_max_entries,
          disk_factor
        ),
      flow_lmdb_writer_max_mailbox_messages:
        adaptive_queue_limit(
          memory_budget,
          @lmdb_mailbox_memory_percent,
          @lmdb_mailbox_entry_bytes,
          cpu * 1024,
          @lmdb_mailbox_max_messages,
          disk_factor
        ),
      flow_lmdb_writer_max_enqueue_ops:
        adaptive_queue_limit(
          memory_budget,
          @lmdb_enqueue_memory_percent,
          @lmdb_enqueue_op_bytes,
          cpu * 2048,
          @lmdb_enqueue_max_ops,
          disk_factor
        )
    }
  end

  defp adaptive_apply_projection_cache_entries(memory_budget, shard_count, cpu, disk_factor) do
    memory_budget
    |> adaptive_queue_limit(
      @apply_projection_cache_memory_percent,
      @apply_projection_cache_entry_bytes,
      cpu * 1024,
      @apply_projection_cache_total_max_entries,
      disk_factor
    )
    |> div(max(shard_count, 1))
    |> clamp(@apply_projection_cache_min_entries, @apply_projection_cache_per_shard_max_entries)
  end

  defp cached_adaptive_limits do
    case :persistent_term.get(@adaptive_cache_key, :unset) do
      :unset ->
        limits = adaptive_limits()
        :persistent_term.put(@adaptive_cache_key, limits)
        limits

      limits ->
        limits
    end
  end

  @doc "Returns the hardware profile used by adaptive limits."
  @spec hardware_profile(keyword()) :: map()
  def hardware_profile(opts \\ []) do
    data_dir = Keyword.get(opts, :data_dir, Application.get_env(:ferricstore, :data_dir, "data"))

    %{
      memory_limit_bytes: configured_memory_limit() || detected_memory_limit(),
      disk_free_bytes: Keyword.get(opts, :disk_free_bytes, disk_free_bytes(data_dir)),
      schedulers_online: Keyword.get(opts, :schedulers_online, System.schedulers_online()),
      shard_count:
        Keyword.get(opts, :shard_count, Application.get_env(:ferricstore, :shard_count, 4))
    }
  end

  defp explicit_limit(key) do
    case Application.fetch_env(:ferricstore, key) do
      {:ok, nil} ->
        :unset

      {:ok, value} ->
        case normalize_limit(value, :invalid) do
          :invalid -> :unset
          normalized -> {:ok, normalized}
        end

      :error ->
        :unset
    end
  end

  defp normalize_limit(:infinity, _default), do: :infinity
  defp normalize_limit(false, _default), do: :infinity
  defp normalize_limit(value, _default) when is_integer(value) and value >= 0, do: value

  defp normalize_limit(value, default) when is_binary(value) do
    case String.downcase(String.trim(value)) do
      value when value in ["", "false", "off", "infinity", "inf", "unlimited"] ->
        :infinity

      value ->
        case Integer.parse(value) do
          {parsed, ""} when parsed >= 0 -> parsed
          _ -> default
        end
    end
  end

  defp normalize_limit(_value, default), do: default

  defp normalize_profile(profile) when is_list(profile),
    do: profile |> Map.new() |> normalize_profile()

  defp normalize_profile(profile) when is_map(profile) do
    %{
      memory_limit_bytes:
        positive_int(Map.get(profile, :memory_limit_bytes), @default_memory_limit),
      disk_free_bytes: non_negative_int(Map.get(profile, :disk_free_bytes), 0),
      schedulers_online: positive_int(Map.get(profile, :schedulers_online), 1),
      shard_count: positive_int(Map.get(profile, :shard_count), 1)
    }
  end

  defp memory_budget_bytes(%{memory_limit_bytes: memory_limit_bytes}) do
    memory_limit_bytes
  end

  defp configured_memory_limit do
    case Application.get_env(:ferricstore, :max_memory_bytes) do
      value when is_integer(value) and value > 0 -> value
      value when is_binary(value) -> parse_positive_int(value)
      _ -> nil
    end
  end

  defp detected_memory_limit do
    try do
      Ferricstore.MemoryGuard.detect_memory_limit()
    rescue
      _ -> @default_memory_limit
    catch
      _, _ -> @default_memory_limit
    end
  end

  defp disk_free_bytes(data_dir) when is_binary(data_dir) do
    path = existing_parent(data_dir)

    case System.cmd("df", ["-Pk", path], stderr_to_stdout: true) do
      {output, 0} ->
        output
        |> String.split("\n", trim: true)
        |> Enum.drop(1)
        |> List.first()
        |> parse_df_available_bytes()

      _ ->
        0
    end
  rescue
    _ -> 0
  catch
    _, _ -> 0
  end

  defp disk_free_bytes(_data_dir), do: 0

  defp existing_parent(path) do
    expanded = Path.expand(path)

    cond do
      Ferricstore.FS.exists?(expanded) ->
        expanded

      parent = Path.dirname(expanded) ->
        if Ferricstore.FS.exists?(parent), do: parent, else: existing_parent(parent)
    end
  end

  defp parse_df_available_bytes(nil), do: 0

  defp parse_df_available_bytes(line) do
    case String.split(line, ~r/\s+/, trim: true) do
      [_filesystem, _blocks, _used, available_kb | _rest] ->
        case Integer.parse(available_kb) do
          {kb, ""} when kb >= 0 -> kb * 1024
          _ -> 0
        end

      _ ->
        0
    end
  end

  defp adaptive_queue_limit(
         memory_budget,
         percent,
         entry_bytes,
         min_value,
         max_value,
         disk_factor
       ) do
    raw =
      memory_budget
      |> percent(percent)
      |> div(max(entry_bytes, 1))
      |> clamp(min_value, max_value)

    raw
    |> div(max(disk_factor, 1))
    |> max(1_024)
  end

  defp disk_pressure_factor(free_bytes) when free_bytes > 20 * @gib, do: 1
  defp disk_pressure_factor(free_bytes) when free_bytes > 5 * @gib, do: 2
  defp disk_pressure_factor(_free_bytes), do: 4

  defp percent(value, percent), do: div(value * percent, 100)

  defp clamp(value, min_value, max_value) do
    value
    |> max(min_value)
    |> min(max_value)
  end

  defp positive_int(value, _default) when is_integer(value) and value > 0, do: value
  defp positive_int(_value, default), do: default

  defp non_negative_int(value, _default) when is_integer(value) and value >= 0, do: value
  defp non_negative_int(_value, default), do: default

  defp parse_positive_int(value) do
    case Integer.parse(String.trim(value)) do
      {parsed, ""} when parsed > 0 -> parsed
      _ -> nil
    end
  end
end
