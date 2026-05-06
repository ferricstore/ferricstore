defmodule Ferricstore.Resp.Parser do
  @moduledoc """
  Rust NIF parser for the RESP3 protocol.

  Parses a binary buffer containing one or more RESP3-encoded values and returns
  all complete values along with any unparsed remainder. This module owns the
  public RESP parser API; parsing itself is performed by
  `Ferricstore.Resp.ParserNif`.

  ## Supported RESP3 types

  | Prefix | Type            | Elixir representation                  |
  |--------|-----------------|----------------------------------------|
  | `+`    | Simple string   | `{:simple, binary()}`                  |
  | `-`    | Simple error    | `{:error, binary()}`                   |
  | `:`    | Integer         | `integer()`                            |
  | `$`    | Bulk string     | `binary()` or `nil` for `$-1`          |
  | `*`    | Array           | `list()` or `nil` for `*-1`            |
  | `_`    | Null            | `nil`                                  |
  | `#`    | Boolean         | `true` or `false`                      |
  | `,`    | Double          | `float()` or `:infinity | :neg_infinity | :nan` |
  | `(`    | Big number      | `integer()`                            |
  | `!`    | Blob error      | `{:error, binary()}`                   |
  | `=`    | Verbatim string | `{:verbatim, encoding, data}`          |
  | `%`    | Map             | `map()`                                |
  | `~`    | Set             | `MapSet.t()`                           |
  | `>`    | Push            | `{:push, list()}`                      |
  | `|`    | Attribute       | `{:attribute, map()}`                  |

  Inline commands (plain text terminated by `\\r\\n`) are returned as
  `{:inline, [binary()]}`.

  Bulk-like payload length is checked at parse time against a configurable
  maximum (`:max_value_size` in Application env, default 1 MB). A hard cap of
  64 MB is enforced regardless of the configured value.
  """

  alias Ferricstore.Resp.ParserNif

  @hard_cap_bytes 67_108_864
  @default_max_value_size 1_048_576

  @doc """
  Returns the default maximum value size in bytes (1 MB).
  """
  @spec default_max_value_size() :: pos_integer()
  def default_max_value_size, do: @default_max_value_size

  @doc """
  Returns the hard cap on bulk-like payload length in bytes (64 MB).
  """
  @spec hard_cap_bytes() :: pos_integer()
  def hard_cap_bytes, do: @hard_cap_bytes

  @type parsed_value ::
          {:simple, binary()}
          | {:error, binary()}
          | integer()
          | binary()
          | nil
          | boolean()
          | float()
          | :infinity
          | :neg_infinity
          | :nan
          | {:verbatim, binary(), binary()}
          | map()
          | MapSet.t()
          | {:push, list()}
          | {:attribute, map()}
          | {:inline, [binary()]}
          | list()

  @type command_ast :: tuple()
  @type parsed_command :: {:command, binary(), [binary()], command_ast(), [binary()]}
  @type parse_result :: {:ok, [parsed_value()], binary()} | {:error, term()}
  @type parse_commands_result :: {:ok, [parsed_command()], binary()} | {:error, term()}

  @doc """
  Parses a binary buffer containing RESP3-encoded data.
  """
  @spec parse(binary()) :: parse_result()
  def parse(data) when is_binary(data) do
    max_value_size = Application.get_env(:ferricstore, :max_value_size, @default_max_value_size)
    parse(data, max_value_size)
  end

  @doc """
  Parses a binary buffer with an explicit maximum bulk-like payload size.
  """
  @spec parse(binary(), non_neg_integer()) :: parse_result()
  def parse(data, max_value_size)
      when is_binary(data) and is_integer(max_value_size) and max_value_size >= 0 do
    ParserNif.parse(data, min(max_value_size, @hard_cap_bytes))
  end

  @doc """
  Parses server command frames into normalized command tuples in Rust.

  Command names are uppercased in the NIF. Arguments remain binary sub-binaries
  of the input buffer. RESP arrays must contain bulk-string command arguments;
  inline commands are split on spaces and tabs.
  """
  @spec parse_commands(binary()) :: parse_commands_result()
  def parse_commands(data) when is_binary(data) do
    max_value_size = Application.get_env(:ferricstore, :max_value_size, @default_max_value_size)
    parse_commands(data, max_value_size)
  end

  @doc """
  Parses server command frames with an explicit maximum command argument size.
  """
  @spec parse_commands(binary(), non_neg_integer()) :: parse_commands_result()
  def parse_commands(data, max_value_size)
      when is_binary(data) and is_integer(max_value_size) and max_value_size >= 0 do
    ParserNif.parse_commands(data, min(max_value_size, @hard_cap_bytes))
  end
end
