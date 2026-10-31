defmodule Ferricstore.Commands.LcsTest do
  @moduledoc "LCS, as Redis answers it (the examples are Redis's own)."
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.{Dispatcher, PreparedCommand}
  alias Ferricstore.Transaction.ReplicaCompat

  setup do
    ctx = FerricStore.Instance.get(:default)
    tag = "lcs:{#{System.unique_integer([:positive])}}"
    d = fn command, args -> Dispatcher.dispatch(command, args, ctx) end
    :ok = d.("SET", [tag <> "a", "ohmytext"])
    :ok = d.("SET", [tag <> "b", "mynewtext"])
    %{ctx: ctx, d: d, a: tag <> "a", b: tag <> "b", k: &(tag <> &1)}
  end

  test "the subsequence, its length and its matches", %{d: d, a: a, b: b} do
    assert "mytext" == d.("LCS", [a, b])
    assert 6 == d.("LCS", [a, b, "LEN"])

    assert %{"matches" => [[[4, 7], [5, 8]], [[2, 3], [0, 1]]], "len" => 6} ==
             d.("LCS", [a, b, "IDX"])

    assert %{"matches" => [[[4, 7], [5, 8]]], "len" => 6} ==
             d.("LCS", [a, b, "IDX", "MINMATCHLEN", "4"])

    assert %{"matches" => [[[4, 7], [5, 8], 4], [[2, 3], [0, 1], 2]], "len" => 6} ==
             d.("LCS", [a, b, "idx", "withmatchlen"])
  end

  test "a missing key is an empty string", %{d: d, a: a, k: k} do
    assert "" == d.("LCS", [a, k.("missing")])
    assert 0 == d.("LCS", [k.("missing"), a, "LEN"])
    assert %{"matches" => [], "len" => 0} == d.("LCS", [a, k.("missing"), "IDX"])
  end

  test "it matches the brute-force answer on random strings", %{d: d, k: k} do
    :rand.seed(:exsss, {7, 7, 7})

    for _ <- 1..40 do
      x = for _ <- 1..Enum.random(0..12)//1, into: "", do: <<Enum.random(?a..?c)>>
      y = for _ <- 1..Enum.random(0..12)//1, into: "", do: <<Enum.random(?a..?c)>>
      :ok = d.("SET", [k.("x"), x])
      :ok = d.("SET", [k.("y"), y])
      lcs = d.("LCS", [k.("x"), k.("y")])
      assert byte_size(lcs) == d.("LCS", [k.("x"), k.("y"), "LEN"])
      assert byte_size(lcs) == brute_lcs(x, y), "#{x} #{y}"
      assert subsequence?(lcs, x) and subsequence?(lcs, y)
    end
  end

  test "errors", %{d: d, a: a, b: b, k: k} do
    assert {:error, "ERR If you want both the length and indexes, please just use IDX."} ==
             d.("LCS", [a, b, "LEN", "IDX"])

    assert {:error, "ERR syntax error"} == d.("LCS", [a, b, "FOO"])

    assert {:error, "ERR value is not an integer or out of range"} ==
             d.("LCS", [a, b, "IDX", "MINMATCHLEN", "x"])

    1 = d.("RPUSH", [k.("l"), "x"])

    assert {:error, "ERR The specified keys must contain string values"} ==
             d.("LCS", [a, k.("l")])

    assert {:error, "ERR wrong number of arguments for 'lcs' command"} == d.("LCS", [a])
  end

  test "inside MULTI it needs feature level 12", %{ctx: ctx, a: a, b: b} do
    assert {:ok, prepared} = PreparedCommand.prepare("LCS", [a, b])
    assert [a, b] == prepared.command_keys
    assert 12 == ReplicaCompat.required_level(prepared.ast)
    assert ReplicaCompat.queue_error("LCS", prepared.ast, prepared.command_keys, ctx)
  end

  # The textbook recursion over suffixes, memoized.
  defp brute_lcs(x, y), do: x |> suffix_lcs(y, %{}) |> elem(0)

  defp suffix_lcs("", _y, memo), do: {0, memo}
  defp suffix_lcs(_x, "", memo), do: {0, memo}

  defp suffix_lcs(<<a, x::binary>> = ax, <<b, y::binary>> = by, memo) do
    case memo do
      %{{^ax, ^by} => known} ->
        {known, memo}

      _ ->
        {length, memo} =
          if a == b do
            {rest, memo} = suffix_lcs(x, y, memo)
            {rest + 1, memo}
          else
            {left, memo} = suffix_lcs(x, by, memo)
            {right, memo} = suffix_lcs(ax, y, memo)
            {max(left, right), memo}
          end

        {length, Map.put(memo, {ax, by}, length)}
    end
  end

  defp subsequence?("", _string), do: true
  defp subsequence?(_sub, ""), do: false
  defp subsequence?(<<c, sub::binary>>, <<c, rest::binary>>), do: subsequence?(sub, rest)
  defp subsequence?(sub, <<_c, rest::binary>>), do: subsequence?(sub, rest)
end
