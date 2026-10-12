defmodule Ferricstore.Flow.TransferFormatUpgradeTest do
  @moduledoc """
  Runs exported from a data format 1 node restore into a format 2 store: the
  same runs, history, values and policies, with every key and ref in format 2.
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Flow.{Digest, Transfer}
  alias Ferricstore.Test.{ClusterHelper, FlowTransferFixture, IsolatedInstance}

  @moduletag :cluster
  @moduletag timeout: 300_000

  setup do
    node = ClusterHelper.start_node(shards: 2, app_env: [data_format: 1])
    target = IsolatedInstance.checkout()

    path =
      Path.join(
        System.tmp_dir!(),
        "flow_format_upgrade_#{System.unique_integer([:positive])}.ffx"
      )

    on_exit(fn ->
      ClusterHelper.stop_node(node)
      IsolatedInstance.checkin(target)
      File.rm(path)
    end)

    {:ok, node: node, target: target, path: path}
  end

  test "a format 1 export restores into format 2 with every ref rewritten", ctx do
    assert :erpc.call(ctx.node, Ferricstore.DataFormat, :current, []) == 1
    assert Digest.format() == 2

    source = :erpc.call(ctx.node, FerricStore.Instance, :get, [:default])
    now_ms = System.system_time(:millisecond)
    manifest = :erpc.call(ctx.node, FlowTransferFixture, :build!, [source, "xf", now_ms])
    before = :erpc.call(ctx.node, FlowTransferFixture, :snapshot, [source, manifest])
    assert {:ok, counts} = :erpc.call(ctx.node, Transfer, :export, [source, ctx.path])
    assert counts.flows == length(manifest.flows)

    assert {:ok, ^counts} = Transfer.restore(ctx.target, ctx.path)

    expected = to_format_2(before)
    restored = FlowTransferFixture.snapshot(ctx.target, to_format_2(manifest))

    assert restored == expected

    refute inspect(restored, limit: :infinity, printable_limit: :infinity) =~
             ~r/\{f:[A-Za-z0-9_-]{43}\}/
  end

  # Every format 1 partition tag inside the term, rewritten as format 2 writes it.
  defp to_format_2(term) when is_binary(term) do
    Regex.replace(~r/\{f:([A-Za-z0-9_-]{43})\}/, term, fn whole, digest ->
      case Digest.convert_legacy(digest) do
        {:ok, short} -> "{f:" <> short <> "}"
        :error -> whole
      end
    end)
  end

  defp to_format_2(term) when is_list(term), do: Enum.map(term, &to_format_2/1)
  defp to_format_2(%MapSet{} = set), do: MapSet.new(set, &to_format_2/1)
  defp to_format_2(%_{} = struct), do: struct

  defp to_format_2(term) when is_map(term),
    do: Map.new(term, fn {k, v} -> {to_format_2(k), to_format_2(v)} end)

  defp to_format_2(term) when is_tuple(term),
    do: term |> Tuple.to_list() |> Enum.map(&to_format_2/1) |> List.to_tuple()

  defp to_format_2(term), do: term
end
