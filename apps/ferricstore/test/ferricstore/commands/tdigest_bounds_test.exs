defmodule Ferricstore.Commands.TDigestBoundsTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.TDigest
  alias Ferricstore.TermCodec
  alias Ferricstore.Test.MockStore

  @max_batch_items 10_000
  @max_exact_count 9_007_199_254_740_992

  test "batch commands reject excessive work before storage" do
    parent = self()
    store = %{get: fn key -> send(parent, {:store_get, key}) end}
    floats = List.duplicate(1.0, @max_batch_items + 1)
    ranks = List.duplicate(1, @max_batch_items + 1)

    for ast <- [
          {:tdigest_add, "digest", floats},
          {:tdigest_cdf, "digest", floats},
          {:tdigest_quantile, "digest", floats},
          {:tdigest_rank, "digest", floats},
          {:tdigest_revrank, "digest", floats},
          {:tdigest_byrank, "digest", ranks},
          {:tdigest_byrevrank, "digest", ranks}
        ] do
      assert {:error, "ERR TDIGEST: batch exceeds maximum of 10000 items"} ==
               TDigest.handle_ast(ast, store)
    end

    assert {:error, "ERR TDIGEST: batch exceeds maximum of 10000 items"} ==
             TDigest.handle(
               "TDIGEST.ADD",
               ["digest" | List.duplicate("1", @max_batch_items + 1)],
               store
             )

    refute_received {:store_get, _key}
  end

  # Replies were formatted with 17 fixed decimals: a value from about 1e252
  # up raised, one below 1e-17 read "0.0", and others carried noise digits.
  test "replies give every value as the shortest text that reads back to it" do
    store = MockStore.make()

    for value <- [1.0e300, -1.7976931348623157e308, 1.0e-300, 5.0e-324, 123.456, 1.0e16] do
      key = "digest:#{value}"
      assert :ok == TDigest.handle("TDIGEST.CREATE", [key], store)
      assert :ok == TDigest.handle("TDIGEST.ADD", [key, Float.to_string(value)], store)

      for command <- ["TDIGEST.MIN", "TDIGEST.MAX"] do
        reply = TDigest.handle(command, [key], store)
        assert {^value, ""} = Float.parse(reply), "#{command} #{value}: #{inspect(reply)}"
        assert reply == :erlang.float_to_binary(value, [:short])
      end

      assert [reply] = TDigest.handle("TDIGEST.QUANTILE", [key, "0.5"], store)
      assert {^value, ""} = Float.parse(reply)
    end

    key = "digest:plain"
    assert :ok == TDigest.handle("TDIGEST.CREATE", [key], store)
    assert :ok == TDigest.handle("TDIGEST.ADD", [key, "45", "2.5"], store)
    assert "45.0" == TDigest.handle("TDIGEST.MAX", [key], store)
    assert "2.5" == TDigest.handle("TDIGEST.MIN", [key], store)
  end

  test "add and merge reject counts that floats cannot represent exactly" do
    max_digest = encoded_digest(@max_exact_count, 1.0)
    one_digest = encoded_digest(1, 2.0)
    store = MockStore.make(%{"max" => {max_digest, 0}, "one" => {one_digest, 0}})

    assert {:error, "ERR TDIGEST: observation count exceeds supported maximum"} ==
             TDigest.handle_ast({:tdigest_add, "max", [2.0]}, store)

    assert max_digest == store.get.("max")

    assert {:error, "ERR TDIGEST: observation count exceeds supported maximum"} ==
             TDigest.handle_ast({:tdigest_merge, "merged", ["max", "one"], []}, store)

    assert nil == store.get.("merged")
  end

  defp encoded_digest(count, value) do
    TermCodec.encode(
      {:tdigest, [{value, count / 1}],
       %{
         compression: 100,
         count: count,
         min: value,
         max: value,
         buffer: [],
         buffer_size: 0,
         total_compressions: 1
       }}
    )
  end
end
