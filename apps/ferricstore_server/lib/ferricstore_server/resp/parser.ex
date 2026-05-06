defmodule FerricstoreServer.Resp.Parser do
  @moduledoc """
  RESP parser API for server callers.

  The Rust NIF lives in the core `:ferricstore` app so embedded and server paths
  share one parser implementation.
  """

  @type parsed_value :: Ferricstore.Resp.Parser.parsed_value()
  @type command_ast :: Ferricstore.Resp.Parser.command_ast()
  @type parsed_command :: Ferricstore.Resp.Parser.parsed_command()
  @type parse_result :: Ferricstore.Resp.Parser.parse_result()
  @type parse_commands_result :: Ferricstore.Resp.Parser.parse_commands_result()

  defdelegate default_max_value_size(), to: Ferricstore.Resp.Parser
  defdelegate hard_cap_bytes(), to: Ferricstore.Resp.Parser
  defdelegate parse(data), to: Ferricstore.Resp.Parser
  defdelegate parse(data, max_value_size), to: Ferricstore.Resp.Parser
  defdelegate parse_commands(data), to: Ferricstore.Resp.Parser
  defdelegate parse_commands(data, max_value_size), to: Ferricstore.Resp.Parser
end
