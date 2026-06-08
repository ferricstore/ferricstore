Code.require_file("parser_test/sections/simple_string.exs", __DIR__)
Code.require_file("parser_test/sections/server_command_parser_part_1.exs", __DIR__)
Code.require_file("parser_test/sections/server_command_parser_part_2.exs", __DIR__)
Code.require_file("parser_test/sections/flow_command_ast.exs", __DIR__)

defmodule FerricstoreServer.Resp.ParserTest do
  use ExUnit.Case, async: true

  alias FerricstoreServer.Resp.Parser

  # ---------------------------------------------------------------------------
  # Simple string (+)
  # ---------------------------------------------------------------------------

  use FerricstoreServer.Resp.ParserTest.Sections.SimpleString
  use FerricstoreServer.Resp.ParserTest.Sections.ServerCommandParserPart1
  use FerricstoreServer.Resp.ParserTest.Sections.ServerCommandParserPart2
  use FerricstoreServer.Resp.ParserTest.Sections.FlowCommandAst
end
