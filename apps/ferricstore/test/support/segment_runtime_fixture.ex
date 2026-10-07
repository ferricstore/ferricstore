defmodule Ferricstore.Test.SegmentRuntimeFixture do
  @moduledoc false
  @provider :ferricstore_waraft_spike_segment_log

  # Only remove state beneath the unique fixture root after its test processes
  # have exited. Canonical and intentionally malformed test rows cannot be
  # reclaimed by production maintenance, and must not pollute later tests.
  def cleanup(root) do
    tables = [
      {:ferricstore_waraft_segment_offset_registry, fn {{dir, _}, _, _, _} -> dir end},
      {:ferricstore_waraft_segment_offset_index_generations, fn {dir, _} -> dir end},
      {:ferricstore_waraft_segment_writer_registry, fn row -> elem(row, 1) end}
    ]

    for {table, directory} <- tables, row <- rows(table) do
      if fixture_path?(directory.(row), root), do: :ets.delete_object(table, row)
    end

    for {{@provider, kind, dir} = key, _value} <- :persistent_term.get(),
        kind in [:records_per_segment, :trim_floor, :latest_config, :offset_index_untrusted],
        fixture_path?(dir, root) do
      :persistent_term.erase(key)
    end

    File.rm_rf!(root)
  end

  defp rows(table) do
    :ets.tab2list(table)
  catch
    :error, :badarg -> []
  end

  defp fixture_path?(dir, root) when is_list(dir) or is_binary(dir) do
    String.starts_with?(to_string(dir), root <> "/")
  end

  defp fixture_path?(_dir, _root), do: false
end
