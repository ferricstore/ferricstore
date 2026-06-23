defmodule FerricstoreServer.Native.ProtocolAuditTest do
  use ExUnit.Case, async: true

  @deleted_modules [
    Module.concat([Ferricstore, String.to_atom("Re" <> "sp.Parser")]),
    Module.concat([Ferricstore, String.to_atom("Re" <> "sp.Encoder")]),
    Module.concat([FerricstoreServer, String.to_atom("Re" <> "sp.Parser")]),
    Module.concat([FerricstoreServer, String.to_atom("Re" <> "sp.Encoder")]),
    Module.concat([FerricstoreServer, :Listener]),
    Module.concat([FerricstoreServer, :TlsListener]),
    Module.concat([FerricstoreServer, String.to_atom("Client" <> "Tracking")])
  ]

  @forbidden_source_refs [
    "Ferricstore." <> "Resp",
    "FerricstoreServer." <> "Resp",
    "resp" <> "_parser_nif",
    "Parser" <> "Nif",
    "FerricstoreServer." <> "Listener",
    "FerricstoreServer." <> "TlsListener",
    "FerricstoreServer." <> "Client" <> "Tracking"
  ]

  test "deleted text protocol runtime modules are not loadable" do
    for module <- @deleted_modules do
      refute Code.ensure_loaded?(module), "#{inspect(module)} should not exist"
    end
  end

  test "application config enables the Ferric protocol listener by default" do
    assert is_integer(Application.get_env(:ferricstore, :native_port))
  end

  test "application and test source do not reference deleted text protocol modules" do
    files =
      Path.wildcard("apps/*/{lib,test}/**/*.{ex,exs}")
      |> Enum.reject(&(&1 == __ENV__.file))

    offenders =
      for file <- files,
          body = File.read!(file),
          ref <- @forbidden_source_refs,
          String.contains?(body, ref) do
        {file, ref}
      end

    assert offenders == []
  end
end
