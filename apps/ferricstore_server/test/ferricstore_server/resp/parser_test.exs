Code.require_file("parser_test/sections/part_01.exs", __DIR__)
Code.require_file("parser_test/sections/part_02.exs", __DIR__)
Code.require_file("parser_test/sections/part_03.exs", __DIR__)
Code.require_file("parser_test/sections/part_04.exs", __DIR__)

defmodule FerricstoreServer.Resp.ParserTest do
  use ExUnit.Case, async: true

  alias FerricstoreServer.Resp.Parser

  # ---------------------------------------------------------------------------
  # Simple string (+)
  # ---------------------------------------------------------------------------

  use FerricstoreServer.Resp.ParserTest.Sections.Part01
  use FerricstoreServer.Resp.ParserTest.Sections.Part02
  use FerricstoreServer.Resp.ParserTest.Sections.Part03
  use FerricstoreServer.Resp.ParserTest.Sections.Part04
end
