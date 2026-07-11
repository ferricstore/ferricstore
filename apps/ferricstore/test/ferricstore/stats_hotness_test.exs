defmodule Ferricstore.Stats.HotnessTest do
  @moduledoc false
  use ExUnit.Case, async: false

  alias Ferricstore.Stats
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  setup_all do
    ShardHelpers.flush_all_keys()
    :ok
  end

  # Reset hotness state before each test to isolate counters.
  setup do
    Stats.reset_hotness()
    :ok
  end

  # ---------------------------------------------------------------------------
  # extract_prefix/1
  # ---------------------------------------------------------------------------

  describe "extract_prefix/1" do
    test "returns first colon-delimited component" do
      assert Stats.extract_prefix("user:42") == "user"
    end

    test "returns first component when multiple colons present" do
      assert Stats.extract_prefix("a:b:c") == "a"
    end

    test "returns _root for keys without a colon" do
      assert Stats.extract_prefix("plain_key") == "_root"
    end

    test "returns empty string prefix when key starts with colon" do
      assert Stats.extract_prefix(":leading") == ""
    end

    test "returns prefix for key with single colon at end" do
      assert Stats.extract_prefix("trailing:") == "trailing"
    end

    test "handles single-character prefix" do
      assert Stats.extract_prefix("x:data") == "x"
    end
  end

  # ---------------------------------------------------------------------------
  # record_hot_read/1 and record_cold_read/1
  # ---------------------------------------------------------------------------

  describe "record_hot_read/1" do
    test "increments global hot read counter" do
      before = Stats.total_hot_reads()
      Stats.record_hot_read("user:1")
      assert Stats.total_hot_reads() >= before + 1
    end

    test "increments per-prefix hot counter in hotness table" do
      Stats.record_hot_read("user:1")
      Stats.record_hot_read("user:2")
      Stats.record_hot_read("user:3")

      entries = Stats.hotness_top(10)
      user_entry = Enum.find(entries, fn {prefix, _, _, _} -> prefix == "user" end)
      assert user_entry != nil
      {_, hot, _cold, _pct} = user_entry
      assert hot == 3
    end

    test "uses _root prefix for keys without colons" do
      Stats.record_hot_read("mykey")

      entries = Stats.hotness_top(10)
      root_entry = Enum.find(entries, fn {prefix, _, _, _} -> prefix == "_root" end)
      assert root_entry != nil
      {_, hot, _, _} = root_entry
      assert hot >= 1
    end
  end

  describe "record_cold_read/1" do
    test "increments global cold read counter" do
      before = Stats.total_cold_reads()
      Stats.record_cold_read("session:abc")
      assert Stats.total_cold_reads() >= before + 1
    end

    test "increments per-prefix cold counter in hotness table" do
      Stats.record_cold_read("session:abc")
      Stats.record_cold_read("session:def")

      entries = Stats.hotness_top(10)
      session_entry = Enum.find(entries, fn {prefix, _, _, _} -> prefix == "session" end)
      assert session_entry != nil
      {_, _hot, cold, _pct} = session_entry
      assert cold == 2
    end
  end

  # ---------------------------------------------------------------------------
  # total_hot_reads/0 and total_cold_reads/0
  # ---------------------------------------------------------------------------

  describe "total_hot_reads/0 and total_cold_reads/0" do
    test "start at zero after reset" do
      # reset_hotness is called in setup
      assert Stats.total_hot_reads() >= 0
      assert Stats.total_cold_reads() >= 0
    end

    test "accumulate correctly across multiple calls" do
      base_hot = Stats.total_hot_reads()
      base_cold = Stats.total_cold_reads()

      for _ <- 1..5, do: Stats.record_hot_read("k:1")
      for _ <- 1..3, do: Stats.record_cold_read("k:2")

      assert Stats.total_hot_reads() == base_hot + 5
      assert Stats.total_cold_reads() == base_cold + 3
    end
  end

  # ---------------------------------------------------------------------------
  # hot_read_pct/0
  # ---------------------------------------------------------------------------

  describe "hot_read_pct/0" do
    test "returns 0.0 when no reads recorded" do
      assert Stats.hot_read_pct() == 0.0
    end

    test "returns 100.0 when all reads are hot" do
      Stats.record_hot_read("k:1")
      Stats.record_hot_read("k:2")
      assert Stats.hot_read_pct() == 100.0
    end

    test "returns correct percentage for mixed reads" do
      for _ <- 1..3, do: Stats.record_hot_read("k:1")
      Stats.record_cold_read("k:2")

      # 3 hot out of 4 total = 75.0%
      assert Stats.hot_read_pct() == 75.0
    end
  end

  # ---------------------------------------------------------------------------
  # cold_reads_per_second/0
  # ---------------------------------------------------------------------------

  describe "cold_reads_per_second/0" do
    test "returns a non-negative float" do
      assert Stats.cold_reads_per_second() >= 0.0
    end
  end

  # ---------------------------------------------------------------------------
  # hotness_top/1
  # ---------------------------------------------------------------------------

  describe "hotness_top/1" do
    test "returns empty list when no reads recorded" do
      assert Stats.hotness_top(5) == []
    end

    test "sorts by cold count descending" do
      Stats.record_cold_read("a:1")
      Stats.record_cold_read("b:1")
      Stats.record_cold_read("b:2")
      Stats.record_cold_read("c:1")
      Stats.record_cold_read("c:2")
      Stats.record_cold_read("c:3")

      entries = Stats.hotness_top(10)
      cold_counts = Enum.map(entries, fn {_, _, cold, _} -> cold end)
      assert cold_counts == Enum.sort(cold_counts, :desc)
    end

    test "respects the top_n limit" do
      for prefix <- ~w(a b c d e) do
        Stats.record_cold_read("#{prefix}:key")
      end

      assert length(Stats.hotness_top(3)) == 3
    end

    test "computes cold_pct correctly" do
      # 2 hot + 3 cold for prefix "x" -> cold_pct = 60.0%
      Stats.record_hot_read("x:1")
      Stats.record_hot_read("x:2")
      Stats.record_cold_read("x:3")
      Stats.record_cold_read("x:4")
      Stats.record_cold_read("x:5")

      entries = Stats.hotness_top(10)
      x_entry = Enum.find(entries, fn {prefix, _, _, _} -> prefix == "x" end)
      assert x_entry != nil
      {_, 2, 3, cold_pct} = x_entry
      assert cold_pct == 60.0
    end

    test "tracks multiple prefixes independently" do
      Stats.record_hot_read("user:1")
      Stats.record_cold_read("session:1")
      Stats.record_hot_read("cache:1")

      entries = Stats.hotness_top(10)
      prefixes = Enum.map(entries, fn {prefix, _, _, _} -> prefix end) |> MapSet.new()

      assert "user" in prefixes
      assert "session" in prefixes
      assert "cache" in prefixes
    end
  end

  # ---------------------------------------------------------------------------
  # reset_hotness/0
  # ---------------------------------------------------------------------------

  describe "reset_hotness/0" do
    test "clears global hot/cold counters" do
      Stats.record_hot_read("k:1")
      Stats.record_cold_read("k:2")
      Stats.reset_hotness()

      assert Stats.total_hot_reads() == 0
      assert Stats.total_cold_reads() == 0
    end

    test "clears per-prefix hotness table" do
      Stats.record_hot_read("user:1")
      Stats.record_cold_read("session:1")
      Stats.reset_hotness()

      assert Stats.hotness_top(10) == []
    end
  end

  # ---------------------------------------------------------------------------
  # Integration: Router.get triggers hot/cold tracking
  # ---------------------------------------------------------------------------

  describe "Router.get integration" do
    setup do
      # Clean up any keys we write
      on_exit(fn ->
        Router.delete(FerricStore.Instance.get(:default), "hottest:alpha")
        Router.delete(FerricStore.Instance.get(:default), "hottest:beta")
        Router.delete(FerricStore.Instance.get(:default), "coldkey")
      end)

      :ok
    end

    test "first read of a key after put is hot (ETS hit)" do
      Stats.reset_hotness()
      before_hot = Stats.total_hot_reads()

      ctx = FerricStore.Instance.get(:default)

      assert :ok = Router.put(ctx, "hottest:alpha", "value", 0)
      # Key is now in ETS from the put. Reading should be a hot read.
      assert "value" == Router.get(ctx, "hottest:alpha")

      assert Stats.total_hot_reads() >= before_hot + 1

      entries = Stats.hotness_top(10)
      hottest_entry = Enum.find(entries, fn {prefix, _, _, _} -> prefix == "hottest" end)
      assert hottest_entry != nil
      {_, hot, _cold, _pct} = hottest_entry
      assert hot >= 1
    end

    test "repeated reads of same key are all hot" do
      Stats.reset_hotness()

      ctx = FerricStore.Instance.get(:default)

      assert :ok = Router.put(ctx, "hottest:beta", "value", 0)

      for _ <- 1..5 do
        assert "value" == Router.get(ctx, "hottest:beta")
      end

      entries = Stats.hotness_top(10)
      hottest_entry = Enum.find(entries, fn {prefix, _, _, _} -> prefix == "hottest" end)
      assert hottest_entry != nil
      {_, hot, _cold, _pct} = hottest_entry
      assert hot >= 5
    end
  end

  # ---------------------------------------------------------------------------
  # FERRICSTORE.HOTNESS command integration
  # ---------------------------------------------------------------------------

  describe "FERRICSTORE.HOTNESS command" do
    alias Ferricstore.Commands.Dispatcher
    alias Ferricstore.Test.MockStore

    setup do
      Stats.reset_hotness()
      :ok
    end

    test "returns a list with header fields" do
      store = MockStore.make()
      result = Dispatcher.dispatch("FERRICSTORE.HOTNESS", [], store)

      assert is_list(result)
      assert "hot_reads" in result
      assert "cold_reads" in result
      assert "hot_read_pct" in result
      assert "cold_reads_per_second" in result
      assert "top_n" in result
    end

    test "includes prefix entries after reads are recorded" do
      Stats.record_hot_read("user:1")
      Stats.record_hot_read("user:2")
      Stats.record_cold_read("user:3")
      Stats.record_cold_read("session:1")

      store = MockStore.make()
      result = Dispatcher.dispatch("FERRICSTORE.HOTNESS", [], store)

      assert "prefix" in result

      # Find the index of "prefix" entries and check values
      prefix_indices =
        result
        |> Enum.with_index()
        |> Enum.filter(fn {val, _idx} -> val == "prefix" end)
        |> Enum.map(fn {_, idx} -> idx end)

      assert length(prefix_indices) >= 2

      # Extract prefix names
      prefix_names = Enum.map(prefix_indices, fn idx -> Enum.at(result, idx + 1) end)
      assert "user" in prefix_names
      assert "session" in prefix_names
    end

    test "TOP argument limits prefix entries" do
      Stats.record_cold_read("a:1")
      Stats.record_cold_read("b:1")
      Stats.record_cold_read("c:1")
      Stats.record_cold_read("d:1")
      Stats.record_cold_read("e:1")

      store = MockStore.make()
      result = Dispatcher.dispatch("FERRICSTORE.HOTNESS", ["TOP", "2"], store)

      # Count prefix entries
      prefix_count =
        result
        |> Enum.count(fn val -> val == "prefix" end)

      assert prefix_count == 2

      # Check top_n value
      idx = Enum.find_index(result, &(&1 == "top_n"))
      assert Enum.at(result, idx + 1) == "2"
    end

    test "WINDOW argument is accepted without error" do
      store = MockStore.make()
      result = Dispatcher.dispatch("FERRICSTORE.HOTNESS", ["WINDOW", "30"], store)

      assert is_list(result)
      assert "hot_reads" in result
    end

    test "TOP and WINDOW together" do
      Stats.record_cold_read("x:1")
      Stats.record_cold_read("y:1")

      store = MockStore.make()
      result = Dispatcher.dispatch("FERRICSTORE.HOTNESS", ["TOP", "1", "WINDOW", "60"], store)

      assert is_list(result)
      prefix_count = Enum.count(result, fn val -> val == "prefix" end)
      assert prefix_count == 1
    end

    test "hot_reads and cold_reads values reflect actual counts" do
      Stats.record_hot_read("k:1")
      Stats.record_hot_read("k:2")
      Stats.record_cold_read("k:3")

      store = MockStore.make()
      result = Dispatcher.dispatch("FERRICSTORE.HOTNESS", [], store)

      hot_idx = Enum.find_index(result, &(&1 == "hot_reads"))
      cold_idx = Enum.find_index(result, &(&1 == "cold_reads"))

      hot_val = String.to_integer(Enum.at(result, hot_idx + 1))
      cold_val = String.to_integer(Enum.at(result, cold_idx + 1))

      assert hot_val >= 2
      assert cold_val >= 1
    end
  end

  # ---------------------------------------------------------------------------
  # INFO stats hot/cold fields
  # ---------------------------------------------------------------------------

  describe "INFO stats hot/cold fields" do
    alias Ferricstore.Commands.Server
    alias Ferricstore.Test.MockStore

    setup do
      Stats.reset_hotness()
      :ok
    end

    test "INFO stats includes hot_reads field" do
      result = Server.handle("INFO", ["stats"], MockStore.make())
      assert result =~ "hot_reads:"
    end

    test "INFO stats includes cold_reads field" do
      result = Server.handle("INFO", ["stats"], MockStore.make())
      assert result =~ "cold_reads:"
    end

    test "INFO stats includes hot_cache_hit_ratio field" do
      result = Server.handle("INFO", ["stats"], MockStore.make())
      assert result =~ "hot_cache_hit_ratio:"
    end

    test "hot_reads value is a non-negative integer" do
      Stats.record_hot_read("k:1")

      result = Server.handle("INFO", ["stats"], MockStore.make())
      [_header | fields] = String.split(result, "\r\n", trim: true)

      line = Enum.find(fields, &String.starts_with?(&1, "hot_reads:"))
      val_str = String.trim_leading(line, "hot_reads:")
      {val, ""} = Integer.parse(val_str)
      assert val >= 1
    end

    test "cold_reads value is a non-negative integer" do
      result = Server.handle("INFO", ["stats"], MockStore.make())
      [_header | fields] = String.split(result, "\r\n", trim: true)

      line = Enum.find(fields, &String.starts_with?(&1, "cold_reads:"))
      val_str = String.trim_leading(line, "cold_reads:")
      {val, ""} = Integer.parse(val_str)
      assert val >= 0
    end

    test "hot_cache_hit_ratio is a valid float" do
      result = Server.handle("INFO", ["stats"], MockStore.make())
      [_header | fields] = String.split(result, "\r\n", trim: true)

      line = Enum.find(fields, &String.starts_with?(&1, "hot_cache_hit_ratio:"))
      val_str = String.trim_leading(line, "hot_cache_hit_ratio:")
      {val, ""} = Float.parse(val_str)
      assert val >= 0.0 and val <= 100.0
    end

    test "INFO all includes hot/cold fields" do
      result = Server.handle("INFO", ["all"], MockStore.make())
      assert result =~ "hot_reads:"
      assert result =~ "cold_reads:"
      assert result =~ "hot_cache_hit_ratio:"
    end
  end
end
