defmodule Ferricstore.Flow.LMDB.SegmentPins do
  @moduledoc false

  @u64_decimal_zero_pad "00000000000000000000"

  def prefix, do: "flow-segment-value-pin:"

  def prefix(tag) when tag in [:waraft_segment, :waraft_apply_projection] do
    prefix() <> "batch:" <> Atom.to_string(tag) <> ":"
  end

  def key({tag, index}, value_key)
      when tag in [:waraft_segment, :waraft_apply_projection] and is_integer(index) and index > 0 and
             is_binary(value_key) do
    prefix(tag) <> pad_u64(index) <> <<0>> <> value_key
  end

  def key(_file_id, _value_key), do: nil

  def encode_batch(file_id, entries)
      when is_list(entries) and tuple_size(file_id) == 2 do
    :erlang.term_to_binary({:flow_segment_value_pin_batch, 1, file_id, entries})
  end

  def put_ops(value_key, expire_at_ms, file_id, offset, value_size) do
    batch_put_ops([{value_key, expire_at_ms, file_id, offset, value_size}])
  end

  def batch_put_ops(entries) when is_list(entries) do
    normalized = normalize_entries(entries)

    value_ops =
      Enum.map(normalized, fn %{
                                key: key,
                                expire_at_ms: expire_at_ms,
                                file_id: file_id,
                                offset: offset,
                                value_size: value_size
                              } ->
        {:put, key,
         Ferricstore.Flow.LMDB.encode_value_locator(expire_at_ms, file_id, offset, value_size)}
      end)

    pin_ops =
      normalized
      |> Enum.group_by(& &1.file_id)
      |> Enum.map(fn {file_id, group} ->
        batch_entries =
          Enum.map(group, fn %{
                               key: key,
                               expire_at_ms: expire_at_ms,
                               offset: offset,
                               value_size: value_size
                             } ->
            {key, expire_at_ms, offset, value_size}
          end)

        {:put, batch_key(file_id, batch_entries), encode_batch(file_id, batch_entries)}
      end)

    value_ops ++ pin_ops
  end

  def delete_ops(value_key, file_id) do
    case key(file_id, value_key) do
      pin_key when is_binary(pin_key) -> [{:delete, pin_key}]
      nil -> []
    end
  end

  def entries_before(path, trim_index, limit)
      when is_binary(path) and is_integer(trim_index) and trim_index > 0 and is_integer(limit) and
             limit > 0 do
    fetch_limit = limit + 1

    with {:ok, entries} <- entries_before_for_tags(path, trim_index, limit, fetch_limit) do
      decode_entries_before(entries, trim_index, limit)
    end
  end

  def entries_before(_path, _trim_index, _limit), do: {:ok, []}

  def entries_before_page(path, trim_index, after_key, limit)
      when is_binary(path) and is_integer(trim_index) and trim_index > 0 and
             is_binary(after_key) and is_integer(limit) and limit > 0 do
    with {:ok, entries} <- entries_before_page_for_tags(path, after_key, limit) do
      decode_entries_before_page(entries, trim_index, after_key, limit)
    end
  end

  def entries_before_page(_path, _trim_index, after_key, _limit) when is_binary(after_key),
    do: {:ok, [], after_key, true}

  defp normalize_entries(entries) do
    entries
    |> Enum.flat_map(fn
      %{
        key: key,
        expire_at_ms: expire_at_ms,
        source_file_id: file_id,
        source_offset: offset,
        source_value_size: value_size
      } ->
        normalize_entry(key, expire_at_ms, file_id, offset, value_size)

      %{
        key: key,
        expire_at_ms: expire_at_ms,
        file_id: file_id,
        offset: offset,
        value_size: value_size
      } ->
        normalize_entry(key, expire_at_ms, file_id, offset, value_size)

      {key, expire_at_ms, file_id, offset, value_size} ->
        normalize_entry(key, expire_at_ms, file_id, offset, value_size)

      _other ->
        []
    end)
  end

  defp normalize_entry(key, expire_at_ms, {tag, index} = file_id, offset, value_size)
       when tag in [:waraft_segment, :waraft_apply_projection] and is_binary(key) and
              is_integer(expire_at_ms) and is_integer(index) and index > 0 and
              is_integer(offset) and offset >= 0 and is_integer(value_size) and value_size >= 0 do
    [
      %{
        key: key,
        expire_at_ms: expire_at_ms,
        file_id: file_id,
        offset: offset,
        value_size: value_size
      }
    ]
  end

  defp normalize_entry(_key, _expire_at_ms, _file_id, _offset, _value_size), do: []

  defp batch_key({tag, index} = file_id, entries)
       when tag in [:waraft_segment, :waraft_apply_projection] and is_integer(index) and
              index > 0 and is_list(entries) do
    digest = :crypto.hash(:sha256, :erlang.term_to_binary({file_id, entries}))
    prefix(tag) <> pad_u64(index) <> <<0>> <> digest
  end

  defp batch_key(file_id, _entries), do: prefix() <> inspect(file_id)

  defp entries_before_for_tags(path, _trim_index, _limit, fetch_limit) do
    entries_for_tags(path, <<>>, fetch_limit)
  end

  defp entries_before_page_for_tags(path, after_key, limit) do
    entries_for_tags(path, after_key, limit)
  end

  defp entries_for_tags(path, after_key, limit) do
    [:waraft_apply_projection, :waraft_segment]
    |> Enum.reduce_while({:ok, []}, fn tag, {:ok, acc} ->
      result =
        if after_key == <<>> do
          Ferricstore.Flow.LMDB.prefix_entries(path, prefix(tag), limit)
        else
          Ferricstore.Flow.LMDB.prefix_entries_after(path, prefix(tag), after_key, limit)
        end

      case result do
        {:ok, entries} -> {:cont, {:ok, entries ++ acc}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, entries} ->
        entries =
          entries
          |> Enum.uniq_by(fn {key, _value} -> key end)
          |> Enum.sort_by(fn {key, _value} -> key end)
          |> Enum.take(limit)

        {:ok, entries}

      {:error, _reason} = error ->
        error
    end
  end

  defp decode_entries_before(entries, trim_index, limit) do
    entries
    |> Enum.reduce_while({:ok, [], false}, fn entry, {:ok, acc, _future?} ->
      case decode_entry(entry) do
        {:ok, %{file_id: {_tag, index}} = pin} when index < trim_index ->
          pins = List.wrap(pin.pins)

          if length(acc) + length(pins) > limit do
            {:halt, {:error, {:flow_segment_value_pin_scan_limit, limit}}}
          else
            {:cont, {:ok, :lists.reverse(pins, acc), false}}
          end

        {:ok, %{file_id: {_tag, index}}} when index >= trim_index ->
          {:halt, {:ok, acc, true}}

        :skip ->
          {:cont, {:ok, acc, false}}
      end
    end)
    |> case do
      {:ok, pins, saw_future?} ->
        if length(entries) > limit and not saw_future? do
          {:error, {:flow_segment_value_pin_scan_limit, limit}}
        else
          {:ok, Enum.reverse(pins)}
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp decode_entries_before_page(entries, trim_index, after_key, limit) do
    {pins, last_key, saw_future?} =
      Enum.reduce_while(entries, {[], after_key, false}, fn {key, _blob} = entry,
                                                            {acc, _last_key, _future?} ->
        case decode_entry(entry) do
          {:ok, %{file_id: {_tag, index}} = pin} when index < trim_index ->
            {:cont, {:lists.reverse(List.wrap(pin.pins), acc), key, false}}

          {:ok, %{file_id: {_tag, index}}} when index >= trim_index ->
            {:halt, {acc, key, true}}

          :skip ->
            {:cont, {acc, key, false}}
        end
      end)

    done? = saw_future? or length(entries) < limit
    {:ok, Enum.reverse(pins), last_key, done?}
  end

  defp decode_entry({pin_key, blob}) when is_binary(pin_key) and is_binary(blob) do
    with {:ok, {tag, index}} <- decode_key(pin_key),
         {:ok, {{^tag, ^index} = file_id, entries}} <- decode_batch_value(blob) do
      pins =
        entries
        |> Enum.flat_map(fn
          {value_key, expire_at_ms, offset, value_size}
          when is_binary(value_key) and is_integer(expire_at_ms) and is_integer(offset) and
                 offset >= 0 and is_integer(value_size) and value_size >= 0 ->
            [
              %{
                key: value_key,
                expire_at_ms: expire_at_ms,
                file_id: file_id,
                offset: offset,
                value_size: value_size,
                pin_key: pin_key
              }
            ]

          _bad ->
            []
        end)

      {:ok, %{file_id: file_id, pin_key: pin_key, pins: pins}}
    else
      _ -> :skip
    end
  end

  defp decode_entry(_entry), do: :skip

  defp decode_key(pin_key) do
    Enum.find_value([:waraft_apply_projection, :waraft_segment], :error, fn tag ->
      prefix = prefix(tag)
      prefix_size = byte_size(prefix)

      with true <- byte_size(pin_key) > prefix_size + 21,
           ^prefix <- binary_part(pin_key, 0, prefix_size),
           digits <- binary_part(pin_key, prefix_size, 20),
           <<0>> <- binary_part(pin_key, prefix_size + 20, 1),
           {index, ""} <- Integer.parse(digits) do
        {:ok, {tag, index}}
      else
        _ -> false
      end
    end)
  end

  defp decode_batch_value(blob) do
    case :erlang.binary_to_term(blob, [:safe]) do
      {:flow_segment_value_pin_batch, 1, {tag, index} = file_id, entries}
      when tag in [:waraft_segment, :waraft_apply_projection] and is_integer(index) and
             index > 0 and is_list(entries) ->
        {:ok, {file_id, entries}}

      _ ->
        :error
    end
  rescue
    _ -> :error
  end

  defp pad_u64(value) do
    encoded = Integer.to_string(value)

    case byte_size(encoded) do
      size when size < 20 -> binary_part(@u64_decimal_zero_pad, 0, 20 - size) <> encoded
      _size -> encoded
    end
  end
end
