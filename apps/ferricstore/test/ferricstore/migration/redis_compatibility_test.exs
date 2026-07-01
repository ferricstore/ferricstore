defmodule Ferricstore.Migration.RedisCompatibilityTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.Catalog
  alias Ferricstore.Migration.RedisCompatibility

  @high_priority_commands ~w(
    get set del exists mget mset expire ttl pttl persist
    hget hset hdel lpush lrange sadd smembers zadd zrange
    xadd xread publish subscribe eval evalsha select migrate
  )

  describe "matrix/0" do
    test "covers high-priority Redis migration commands" do
      matrix = RedisCompatibility.matrix()
      by_name = Map.new(matrix, &{&1.command, &1})

      for command <- @high_priority_commands do
        assert Map.has_key?(by_name, command), "missing #{command} from compatibility matrix"
      end

      assert by_name["set"].status == :compatible
      assert by_name["set"].arity == -3
      assert "write" in by_name["set"].flags

      assert by_name["eval"].status == :unsupported
      assert by_name["eval"].alternative =~ "FerricFlow"

      assert by_name["select"].status == :different
      assert by_name["select"].notes =~ "named caches"

      assert by_name["ferricstore.key_info"].status == :ferricstore_extension
    end

    test "catalog-backed entries stay aligned with command metadata" do
      for entry <- RedisCompatibility.matrix(), entry.source == :catalog do
        assert {:ok, catalog_entry} = Catalog.lookup(entry.command)
        assert entry.arity == catalog_entry.arity
        assert entry.flags == catalog_entry.flags
        assert entry.first_key == catalog_entry.first_key
        assert entry.last_key == catalog_entry.last_key
        assert entry.step == catalog_entry.step
      end
    end
  end

  describe "assess_lines/1" do
    test "flags unsupported and behavior-different commands from common trace formats" do
      report =
        RedisCompatibility.assess_lines([
          "SET tenant:1 value",
          ~s(1700000000.000001 [0 127.0.0.1:6379] "EVAL" "return 1" "0"),
          "cmdstat_hget:calls=42,usec=100,usec_per_call=2.38",
          "SELECT 2",
          "MIGRATE 127.0.0.1 6379 key 0 1000"
        ])

      assert report.total_commands == 46
      assert report.summary.compatible == 43
      assert report.summary.unsupported == 2
      assert report.summary.different == 1

      assert report.commands["set"].calls == 1
      assert report.commands["hget"].calls == 42
      assert report.commands["eval"].status == :unsupported
      assert report.commands["migrate"].status == :unsupported
      assert report.commands["select"].status == :different
    end
  end

  describe "rendering" do
    test "renders markdown and json reports" do
      markdown = RedisCompatibility.render_matrix(:markdown)

      assert markdown =~ "| Command | Status |"
      assert markdown =~ "| `set` | compatible |"
      assert markdown =~ "`eval`"

      json =
        RedisCompatibility.assess_lines(["SET k v"])
        |> RedisCompatibility.render_assessment(:json)

      assert %{"summary" => %{"compatible" => 1}, "commands" => %{"set" => %{"calls" => 1}}} =
               Jason.decode!(json)
    end
  end

  test "migration guide documents differences and import strategy" do
    guide = File.read!(Path.expand("../../../../../guides/redis-migration.md", __DIR__))

    assert guide =~ "compatibility matrix"
    assert guide =~ "Lua"
    assert guide =~ "SELECT"
    assert guide =~ "RDB"
    assert guide =~ "AOF"
  end
end
