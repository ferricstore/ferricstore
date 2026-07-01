defmodule Ferricstore.Migration.RedisCompatibility do
  @moduledoc """
  Redis migration compatibility matrix and workload assessment helpers.

  The matrix is built from FerricStore command metadata first, then from native
  parser metadata for implemented commands that do not yet have full
  Redis-style `COMMAND` metadata. Curated entries are used for important Redis
  commands that FerricStore intentionally does not support.
  """

  alias Ferricstore.Commands.{Catalog, NativeAstParser}

  @type status ::
          :compatible
          | :different
          | :partial
          | :unsupported
          | :ferricstore_extension
          | :unknown

  @type source :: :catalog | :native_ast_parser | :migration_catalog

  @type matrix_entry :: %{
          command: binary(),
          status: status(),
          source: source(),
          arity: integer() | nil,
          flags: [binary()],
          first_key: integer() | nil,
          last_key: integer() | nil,
          step: integer() | nil,
          summary: binary(),
          notes: binary(),
          alternative: binary()
        }

  @unsupported %{
    "eval" => %{
      summary: "Lua scripting is not supported.",
      notes: "FerricStore does not execute Lua or server-side Redis scripts.",
      alternative:
        "Move server-side logic to FerricFlow workflows or client-side command batches."
    },
    "evalsha" => %{
      summary: "Lua script cache execution is not supported.",
      notes: "FerricStore does not maintain Redis Lua script caches.",
      alternative:
        "Move server-side logic to FerricFlow workflows or client-side command batches."
    },
    "script" => %{
      summary: "Lua script cache management is not supported.",
      notes: "FerricStore does not expose Redis Lua script cache commands.",
      alternative: "Use application deployment/versioning for workflow logic."
    },
    "function" => %{
      summary: "Redis Functions are not supported.",
      notes: "FerricStore does not execute Redis Functions.",
      alternative: "Use FerricFlow workflows or application-side functions."
    },
    "migrate" => %{
      summary: "Redis MIGRATE is not supported.",
      notes: "FerricStore does not import live Redis key migration streams.",
      alternative: "Use export/import tooling or replay a sanitized command/AOF workload."
    },
    "dump" => %{
      summary: "Redis serialized value export is not supported.",
      notes: "FerricStore does not expose Redis RDB value encoding.",
      alternative: "Use logical export formats or command replay."
    },
    "restore" => %{
      summary: "Redis serialized value import is not supported.",
      notes: "FerricStore does not ingest Redis RDB value blobs through RESTORE.",
      alternative: "Use logical export formats or command replay."
    },
    "sync" => %{
      summary: "Redis replication sync is not supported.",
      notes: "FerricStore uses its own clustering and durability model.",
      alternative: "Use FerricStore-native replication."
    },
    "psync" => %{
      summary: "Redis partial replication sync is not supported.",
      notes: "FerricStore uses its own clustering and durability model.",
      alternative: "Use FerricStore-native replication."
    },
    "replicaof" => %{
      summary: "Redis replica management is not supported.",
      notes: "FerricStore uses its own clustering and durability model.",
      alternative: "Use FerricStore-native cluster operations."
    },
    "slaveof" => %{
      summary: "Redis replica management is not supported.",
      notes: "FerricStore uses its own clustering and durability model.",
      alternative: "Use FerricStore-native cluster operations."
    },
    "sentinel" => %{
      summary: "Redis Sentinel commands are not supported.",
      notes: "FerricStore does not use Sentinel for availability.",
      alternative: "Use FerricStore-native health and cluster operations."
    },
    "module" => %{
      summary: "Redis module loading is not supported.",
      notes: "FerricStore does not load Redis modules at runtime.",
      alternative: "Use FerricStore built-in commands and FerricFlow."
    }
  }

  @different %{
    "select" => %{
      notes: "FerricStore does not expose Redis numbered databases; use named caches/namespaces.",
      alternative: "Map each Redis logical DB to a FerricStore cache or namespace."
    }
  }

  @partial %{
    "multi" => %{
      notes: "Transaction behavior is constrained by FerricStore command and shard semantics.",
      alternative:
        "Prefer single commands, same-shard batches, or FerricFlow for durable workflows."
    },
    "exec" => %{
      notes: "Transaction behavior is constrained by FerricStore command and shard semantics.",
      alternative:
        "Prefer single commands, same-shard batches, or FerricFlow for durable workflows."
    },
    "watch" => %{
      notes: "Optimistic transaction watches should be validated against the target workload.",
      alternative: "Use application compare-and-set or FerricFlow fencing."
    },
    "unwatch" => %{
      notes: "Optimistic transaction watches should be validated against the target workload.",
      alternative: "Use application compare-and-set or FerricFlow fencing."
    }
  }

  @curated_redis_commands Map.keys(@unsupported) ++ Map.keys(@different) ++ Map.keys(@partial)

  @doc "Returns the Redis migration compatibility matrix."
  @spec matrix() :: [matrix_entry()]
  def matrix do
    catalog_entries = Enum.map(Catalog.all(), &catalog_entry/1)
    catalog_names = catalog_entries |> Enum.map(& &1.command) |> MapSet.new()

    native_entries =
      NativeAstParser.supported_command_names()
      |> Enum.map(&String.downcase/1)
      |> Enum.uniq()
      |> Enum.reject(&MapSet.member?(catalog_names, &1))
      |> Enum.map(&native_entry/1)

    known_names =
      (catalog_entries ++ native_entries)
      |> Enum.map(& &1.command)
      |> MapSet.new()

    curated_entries =
      @curated_redis_commands
      |> Enum.reject(&MapSet.member?(known_names, &1))
      |> Enum.map(&curated_entry/1)

    (catalog_entries ++ native_entries ++ curated_entries)
    |> Enum.sort_by(& &1.command)
  end

  @doc """
  Assesses command usage lines from MONITOR logs, commandstats, or simple
  command-per-line traces.
  """
  @spec assess_lines(Enumerable.t()) :: map()
  def assess_lines(lines) do
    by_command = matrix() |> Map.new(&{&1.command, &1})

    commands =
      lines
      |> Enum.flat_map(&parse_usage_line/1)
      |> Enum.reduce(%{}, fn {command, calls}, acc ->
        entry = Map.get(by_command, command, unknown_entry(command))

        Map.update(acc, command, command_usage(entry, calls), fn usage ->
          %{usage | calls: usage.calls + calls}
        end)
      end)

    %{
      total_commands:
        Enum.reduce(commands, 0, fn {_command, usage}, total -> total + usage.calls end),
      summary: summarize(commands),
      commands: commands
    }
  end

  @doc "Renders the compatibility matrix as `:markdown` or `:json`."
  @spec render_matrix(:markdown | :json) :: binary()
  def render_matrix(:json), do: %{matrix: matrix()} |> json_ready() |> Jason.encode!()

  def render_matrix(:markdown) do
    rows =
      matrix()
      |> Enum.map(fn entry ->
        [
          "`#{entry.command}`",
          Atom.to_string(entry.status),
          source_label(entry.source),
          format_value(entry.arity),
          Enum.join(entry.flags, ", "),
          entry.notes,
          entry.alternative
        ]
      end)

    render_table(
      ["Command", "Status", "Source", "Arity", "Flags", "Notes", "Alternative"],
      rows
    )
  end

  @doc "Renders an assessment report as `:markdown` or `:json`."
  @spec render_assessment(map(), :markdown | :json) :: binary()
  def render_assessment(report, :json), do: report |> json_ready() |> Jason.encode!()

  def render_assessment(report, :markdown) do
    summary_rows =
      report.summary
      |> Map.to_list()
      |> Enum.sort_by(fn {status, _count} -> Atom.to_string(status) end)
      |> Enum.map(fn {status, count} -> [Atom.to_string(status), Integer.to_string(count)] end)

    command_rows =
      report.commands
      |> Map.to_list()
      |> Enum.sort_by(fn {command, _usage} -> command end)
      |> Enum.map(fn {command, usage} ->
        [
          "`#{command}`",
          Integer.to_string(usage.calls),
          Atom.to_string(usage.status),
          usage.notes,
          usage.alternative
        ]
      end)

    """
    ## Summary

    #{render_table(["Status", "Calls"], summary_rows)}

    ## Commands

    #{render_table(["Command", "Calls", "Status", "Notes", "Alternative"], command_rows)}
    """
    |> String.trim_trailing()
  end

  defp catalog_entry(cmd) do
    status = classify(cmd.name, cmd.summary)
    overrides = override_for(cmd.name)

    %{
      command: cmd.name,
      status: status,
      source: :catalog,
      arity: cmd.arity,
      flags: cmd.flags,
      first_key: cmd.first_key,
      last_key: cmd.last_key,
      step: cmd.step,
      summary: Map.get(overrides, :summary, cmd.summary),
      notes: Map.get(overrides, :notes, ""),
      alternative: Map.get(overrides, :alternative, "")
    }
  end

  defp native_entry(command) do
    overrides = override_for(command)

    %{
      command: command,
      status: classify(command, Map.get(overrides, :summary, "")),
      source: :native_ast_parser,
      arity: nil,
      flags: inferred_flags(command),
      first_key: nil,
      last_key: nil,
      step: nil,
      summary: Map.get(overrides, :summary, "Accepted by FerricStore native command parser."),
      notes: Map.get(overrides, :notes, native_notes(command)),
      alternative: Map.get(overrides, :alternative, "")
    }
  end

  defp curated_entry(command) do
    overrides = override_for(command)

    %{
      command: command,
      status: classify(command, Map.get(overrides, :summary, "")),
      source: :migration_catalog,
      arity: nil,
      flags: [],
      first_key: nil,
      last_key: nil,
      step: nil,
      summary: Map.get(overrides, :summary, ""),
      notes: Map.get(overrides, :notes, ""),
      alternative: Map.get(overrides, :alternative, "")
    }
  end

  defp classify(command, summary) do
    cond do
      Map.has_key?(@unsupported, command) -> :unsupported
      Map.has_key?(@different, command) -> :different
      Map.has_key?(@partial, command) -> :partial
      ferricstore_extension?(command) -> :ferricstore_extension
      String.contains?(String.downcase(summary || ""), "not supported") -> :unsupported
      true -> :compatible
    end
  end

  defp override_for(command) do
    Map.get(@unsupported, command) || Map.get(@different, command) || Map.get(@partial, command) ||
      %{}
  end

  defp ferricstore_extension?("ferricstore." <> _rest), do: true
  defp ferricstore_extension?("flow." <> _rest), do: true
  defp ferricstore_extension?(_command), do: false

  defp native_notes(command) do
    if String.contains?(command, ".") do
      "Accepted by native parser; full Redis COMMAND metadata is not available yet."
    else
      ""
    end
  end

  defp inferred_flags(command) do
    cond do
      command in ~w(
        get mget exists ttl pttl type object memory strlen getrange hget hgetall hkeys hvals
        hlen hexists hstrlen hmget lrange llen lindex scard smembers sismember smismember
        zrange zrevrange zrangebyscore zscore zrank zrevrank zcard xread xrange xrevrange
      ) ->
        ["readonly"]

      command in ~w(subscribe unsubscribe psubscribe punsubscribe publish pubsub) ->
        ["pubsub"]

      command in ~w(ping echo hello quit reset auth command info dbsize keys) ->
        ["fast"]

      command in ~w(acl client config debug save bgsave lastsave slowlog flushdb flushall) ->
        ["admin"]

      true ->
        ["write"]
    end
  end

  defp parse_usage_line(line) when is_binary(line) do
    line = String.trim(line)

    cond do
      line == "" ->
        []

      match = Regex.run(~r/^cmdstat_([A-Za-z0-9_.-]+):.*\bcalls=(\d+)/, line) ->
        [_, command, calls] = match
        [{normalize_command(command), String.to_integer(calls)}]

      quoted = Regex.run(~r/"((?:\\.|[^"\\])+)"/, line) ->
        [_, command] = quoted
        [{normalize_command(command), 1}]

      true ->
        case String.split(line, ~r/\s+/, parts: 2, trim: true) do
          [command | _] -> [{normalize_command(command), 1}]
          [] -> []
        end
    end
  end

  defp parse_usage_line(_line), do: []

  defp normalize_command(command) do
    command
    |> String.trim()
    |> String.trim_leading("\"")
    |> String.trim_trailing("\"")
    |> String.downcase()
  end

  defp unknown_entry(command) do
    %{
      command: command,
      status: :unknown,
      source: :migration_catalog,
      arity: nil,
      flags: [],
      first_key: nil,
      last_key: nil,
      step: nil,
      summary: "Command was observed in the workload but is not in FerricStore metadata.",
      notes: "Confirm manually before migration.",
      alternative: ""
    }
  end

  defp command_usage(entry, calls) do
    %{
      calls: calls,
      status: entry.status,
      source: entry.source,
      notes: entry.notes,
      alternative: entry.alternative
    }
  end

  defp summarize(commands) do
    base = %{
      compatible: 0,
      different: 0,
      partial: 0,
      unsupported: 0,
      ferricstore_extension: 0,
      unknown: 0
    }

    Enum.reduce(commands, base, fn {_command, usage}, acc ->
      Map.update!(acc, usage.status, &(&1 + usage.calls))
    end)
  end

  defp render_table(headers, rows) do
    header = "| " <> Enum.join(headers, " | ") <> " |"
    separator = "| " <> (headers |> Enum.map(fn _ -> "---" end) |> Enum.join(" | ")) <> " |"

    body =
      rows
      |> Enum.map(fn row ->
        "| " <> (row |> Enum.map(&escape_cell/1) |> Enum.join(" | ")) <> " |"
      end)
      |> Enum.join("\n")

    [header, separator, body]
    |> Enum.reject(&(&1 == ""))
    |> Enum.join("\n")
  end

  defp escape_cell(value) do
    value
    |> to_string()
    |> String.replace("|", "\\|")
    |> String.replace("\n", " ")
  end

  defp source_label(source), do: source |> Atom.to_string() |> String.replace("_", " ")

  defp format_value(nil), do: ""
  defp format_value(value), do: to_string(value)

  defp json_ready(%{} = map) do
    Map.new(map, fn {key, value} -> {json_key(key), json_ready(value)} end)
  end

  defp json_ready(values) when is_list(values), do: Enum.map(values, &json_ready/1)
  defp json_ready(value) when is_atom(value), do: Atom.to_string(value)
  defp json_ready(value), do: value

  defp json_key(key) when is_atom(key), do: Atom.to_string(key)
  defp json_key(key), do: key
end
