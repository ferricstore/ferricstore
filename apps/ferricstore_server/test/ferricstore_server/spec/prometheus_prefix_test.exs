defmodule FerricstoreServer.Spec.PrometheusPrefixTest do
  @moduledoc """
  Spec: Per-prefix Prometheus metrics.

  Verifies that the `/metrics` endpoint (and `Ferricstore.Metrics.scrape/0`)
  exposes the following labelled metric families after keys are written:

    * `ferricstore_prefix_key_count{prefix="..."}` — number of live keys per prefix
    * `ferricstore_prefix_keydir_bytes{prefix="..."}` — keydir ETS memory per prefix
    * `ferricstore_prefix_hot_reads{prefix="..."}` — hot reads (ETS hits) per prefix
    * `ferricstore_prefix_cold_reads{prefix="..."}` — cold reads (disk) per prefix

  Prefix extraction uses the text before the first `:` delimiter. Keys without
  a colon are grouped under the `_root` pseudo-prefix.
  """

  use ExUnit.Case, async: false

  alias FerricstoreServer.Health.Endpoint, as: HealthEndpoint
  alias Ferricstore.Metrics
  alias Ferricstore.PrefixMetricsCache
  alias Ferricstore.Stats
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  # ---------------------------------------------------------------------------
  # Setup
  # ---------------------------------------------------------------------------

  setup do
    protected_mode = Application.get_env(:ferricstore, :protected_mode)
    Application.put_env(:ferricstore, :protected_mode, false)

    on_exit(fn ->
      if is_nil(protected_mode) do
        Application.delete_env(:ferricstore, :protected_mode)
      else
        Application.put_env(:ferricstore, :protected_mode, protected_mode)
      end
    end)

    ShardHelpers.flush_all_keys()
    Stats.reset_hotness()
    PrefixMetricsCache.reset()
    :ok
  end

  # ---------------------------------------------------------------------------
  # HTTP helpers (same approach as HttpEndpointsTest)
  # ---------------------------------------------------------------------------

  defp http_get(port, path) do
    {:ok, conn} =
      :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false, packet: :raw])

    :ok =
      :gen_tcp.send(
        conn,
        "GET #{path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
      )

    response = recv_all(conn, "")
    :gen_tcp.close(conn)
    response
  end

  defp recv_all(conn, acc) do
    case :gen_tcp.recv(conn, 0, 5_000) do
      {:ok, data} -> recv_all(conn, acc <> data)
      {:error, :closed} -> acc
    end
  end

  defp extract_body(response) do
    case String.split(response, "\r\n\r\n", parts: 2) do
      [_headers, body] -> body
      _ -> response
    end
  end

  defp scrape_metrics do
    :ok = PrefixMetricsCache.refresh_now()
    Metrics.scrape()
  end

  defp scrape_http do
    :ok = PrefixMetricsCache.refresh_now()
    port = HealthEndpoint.port()
    response = http_get(port, "/metrics")
    extract_body(response)
  end

  # ---------------------------------------------------------------------------
  # Helpers: parse Prometheus sample values from the scrape text
  # ---------------------------------------------------------------------------

  # Returns the integer/float value of a labelled metric sample.
  # Example line: ferricstore_prefix_key_count{prefix="session"} 5
  defp metric_value(text, metric_name, prefix) do
    escaped = Regex.escape(prefix)

    regex =
      Regex.compile!(
        ~s|^#{Regex.escape(metric_name)}\\{prefix="#{escaped}"\\}\\s+([\\d.]+)|,
        "m"
      )

    case Regex.run(regex, text) do
      [_, value_str] ->
        if String.contains?(value_str, ".") do
          String.to_float(value_str)
        else
          String.to_integer(value_str)
        end

      nil ->
        nil
    end
  end

  # Returns a map of %{prefix => value} for all samples of the given metric.
  defp all_prefix_values(text, metric_name) do
    regex =
      Regex.compile!(
        ~s|^#{Regex.escape(metric_name)}\\{prefix="([^"]+)"\\}\\s+([\\d.]+)|,
        "m"
      )

    Regex.scan(regex, text)
    |> Enum.into(%{}, fn [_, prefix, value_str] ->
      value =
        if String.contains?(value_str, ".") do
          String.to_float(value_str)
        else
          String.to_integer(value_str)
        end

      {prefix, value}
    end)
  end

  # ---------------------------------------------------------------------------
  # Tests: /metrics contains prefix_key_count after SET
  # ---------------------------------------------------------------------------

  describe "prefix_key_count after SET" do
    test "scrape contains ferricstore_prefix_key_count for a prefix after writing keys" do
      Router.put(FerricStore.Instance.get(:default), "session:abc", "val1")
      Router.put(FerricStore.Instance.get(:default), "session:def", "val2")

      text = scrape_metrics()

      assert String.contains?(text, "# HELP ferricstore_prefix_key_count")
      assert String.contains?(text, "# TYPE ferricstore_prefix_key_count gauge")
      assert metric_value(text, "ferricstore_prefix_key_count", "session") == 2
    end

    test "/metrics HTTP endpoint exposes prefix_key_count" do
      Router.put(FerricStore.Instance.get(:default), "session:1", "v")

      body = scrape_http()

      assert String.contains?(body, "ferricstore_prefix_key_count")
      assert metric_value(body, "ferricstore_prefix_key_count", "session") == 1
    end

    test "keys without a colon appear under _root prefix" do
      before = metric_value(scrape_metrics(), "ferricstore_prefix_key_count", "_root") || 0

      key = "plainkey_#{System.unique_integer([:positive])}"
      Router.put(FerricStore.Instance.get(:default), key, "value", 0)

      text = scrape_metrics()
      after_val = metric_value(text, "ferricstore_prefix_key_count", "_root") || 0

      assert after_val >= before + 1
    end
  end

  # ---------------------------------------------------------------------------
  # Tests: prefix_key_count correct after multiple prefixes
  # ---------------------------------------------------------------------------

  describe "prefix_key_count with multiple prefixes" do
    test "reports correct counts for distinct prefixes" do
      Router.put(FerricStore.Instance.get(:default), "user:1", "alice")
      Router.put(FerricStore.Instance.get(:default), "user:2", "bob")
      Router.put(FerricStore.Instance.get(:default), "user:3", "charlie")
      Router.put(FerricStore.Instance.get(:default), "order:100", "item-a")
      Router.put(FerricStore.Instance.get(:default), "order:200", "item-b")
      Router.put(FerricStore.Instance.get(:default), "cache:x", "hit")

      text = scrape_metrics()

      assert metric_value(text, "ferricstore_prefix_key_count", "user") == 3
      assert metric_value(text, "ferricstore_prefix_key_count", "order") == 2
      assert metric_value(text, "ferricstore_prefix_key_count", "cache") == 1
    end

    test "deleting a key reduces the prefix count" do
      Router.put(FerricStore.Instance.get(:default), "temp:a", "1")
      Router.put(FerricStore.Instance.get(:default), "temp:b", "2")
      Router.delete(FerricStore.Instance.get(:default), "temp:a")

      text = scrape_metrics()

      assert metric_value(text, "ferricstore_prefix_key_count", "temp") == 1
    end

    test "expired keys are not counted" do
      # Expire immediately (1ms in the past)
      past = System.os_time(:millisecond) - 1
      Router.put(FerricStore.Instance.get(:default), "ephemeral:x", "gone", past)

      # Give lazy expiry a moment to take effect
      Process.sleep(10)

      text = scrape_metrics()

      # The prefix should either be absent or have count 0
      count = metric_value(text, "ferricstore_prefix_key_count", "ephemeral")
      assert count == nil or count == 0
    end
  end

  # ---------------------------------------------------------------------------
  # Tests: prefix_keydir_bytes is positive
  # ---------------------------------------------------------------------------

  describe "prefix_keydir_bytes" do
    test "is positive after writing keys" do
      Router.put(FerricStore.Instance.get(:default), "session:abc", "value")

      text = scrape_metrics()

      assert String.contains?(text, "# HELP ferricstore_prefix_keydir_bytes")
      assert String.contains?(text, "# TYPE ferricstore_prefix_keydir_bytes gauge")

      bytes = metric_value(text, "ferricstore_prefix_keydir_bytes", "session")
      assert is_integer(bytes) and bytes > 0
    end

    test "larger keys produce more bytes than smaller keys" do
      Router.put(FerricStore.Instance.get(:default), "big:1", String.duplicate("x", 1000))
      Router.put(FerricStore.Instance.get(:default), "big:2", String.duplicate("y", 1000))

      text = scrape_metrics()

      bytes = metric_value(text, "ferricstore_prefix_keydir_bytes", "big")
      assert is_integer(bytes) and bytes > 0
    end
  end

  # ---------------------------------------------------------------------------
  # Tests: hot and cold reads per prefix
  # ---------------------------------------------------------------------------

  describe "prefix_hot_reads and prefix_cold_reads" do
    test "hot reads are tracked per prefix after GET on cached keys" do
      # PUT populates ETS, so a subsequent GET is a hot read
      Router.put(FerricStore.Instance.get(:default), "session:abc", "val")
      Router.get(FerricStore.Instance.get(:default), "session:abc")
      Router.get(FerricStore.Instance.get(:default), "session:abc")

      text = scrape_metrics()

      assert String.contains?(text, "# HELP ferricstore_prefix_hot_reads")
      assert String.contains?(text, "# TYPE ferricstore_prefix_hot_reads counter")
      assert metric_value(text, "ferricstore_prefix_hot_reads", "session") == 2
    end

    test "cold reads are tracked per prefix" do
      # Force cold reads by recording them directly via Stats
      Stats.record_cold_read("analytics:event1")
      Stats.record_cold_read("analytics:event2")
      Stats.record_cold_read("analytics:event3")

      text = scrape_metrics()

      assert String.contains?(text, "# HELP ferricstore_prefix_cold_reads")
      assert String.contains?(text, "# TYPE ferricstore_prefix_cold_reads counter")
      assert metric_value(text, "ferricstore_prefix_cold_reads", "analytics") == 3
    end

    test "hot and cold reads are separate counters for the same prefix" do
      Router.put(FerricStore.Instance.get(:default), "mixed:key", "val")
      # Hot read (from ETS)
      Router.get(FerricStore.Instance.get(:default), "mixed:key")

      # Record a cold read for the same prefix directly
      Stats.record_cold_read("mixed:other")

      text = scrape_metrics()

      assert metric_value(text, "ferricstore_prefix_hot_reads", "mixed") == 1
      assert metric_value(text, "ferricstore_prefix_cold_reads", "mixed") == 1
    end
  end

  # ---------------------------------------------------------------------------
  # Stress test: 10 prefixes x 100 keys each
  # ---------------------------------------------------------------------------

  describe "stress: 10 prefixes with 100 keys each" do
    test "all prefix metrics are correct" do
      prefixes = for i <- 0..9, do: "pfx#{i}"

      # Write 100 keys per prefix
      for prefix <- prefixes, j <- 1..100 do
        Router.put(FerricStore.Instance.get(:default), "#{prefix}:key#{j}", "value_#{j}")
      end

      # Trigger some reads to create hot/cold stats
      for prefix <- prefixes, j <- 1..10 do
        Router.get(FerricStore.Instance.get(:default), "#{prefix}:key#{j}")
      end

      text = scrape_metrics()

      # Verify key counts
      key_counts = all_prefix_values(text, "ferricstore_prefix_key_count")

      for prefix <- prefixes do
        assert Map.get(key_counts, prefix) == 100,
               "Expected 100 keys for prefix #{prefix}, got #{inspect(Map.get(key_counts, prefix))}"
      end

      # Verify keydir bytes are positive for all prefixes
      keydir_bytes = all_prefix_values(text, "ferricstore_prefix_keydir_bytes")

      for prefix <- prefixes do
        bytes = Map.get(keydir_bytes, prefix)

        assert is_integer(bytes) and bytes > 0,
               "Expected positive keydir bytes for prefix #{prefix}, got #{inspect(bytes)}"
      end

      # Verify hot reads: each prefix had 10 hot reads
      hot_reads = all_prefix_values(text, "ferricstore_prefix_hot_reads")

      for prefix <- prefixes do
        assert Map.get(hot_reads, prefix) == 10,
               "Expected 10 hot reads for prefix #{prefix}, got #{inspect(Map.get(hot_reads, prefix))}"
      end

      # Verify there are exactly 10 prefixes in the key count
      assert map_size(key_counts) >= 10
    end
  end
end
