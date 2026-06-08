defmodule FerricstoreServer.Spec.ConfigValuesTest.Sections.ConfigSetEmitsTelemetry do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Commands.Server
      alias Ferricstore.Config
      alias Ferricstore.Config.Local, as: ConfigLocal
      alias Ferricstore.Stats
      alias Ferricstore.Test.MockStore

      describe "CONFIG SET emits telemetry" do
        test "CONFIG SET emits [:ferricstore, :config, :changed] event", %{store: store} do
          ref = make_ref()
          test_pid = self()
          handler_id = "config-values-test-#{inspect(ref)}"

          :telemetry.attach(
            handler_id,
            [:ferricstore, :config, :changed],
            fn _event, _measurements, metadata, _config ->
              send(test_pid, {:config_changed, metadata})
            end,
            nil
          )

          on_exit(fn -> :telemetry.detach(handler_id) end)

          Server.handle("CONFIG", ["SET", "hz", "42"], store)

          assert_receive {:config_changed, metadata}, 1_000
          assert metadata.param == "hz"
          assert metadata.value == "42"
          assert is_binary(metadata.old_value)
        end

        test "CONFIG SET telemetry redacts sensitive values", %{store: store} do
          on_exit(fn -> Ferricstore.Config.set("requirepass", "") end)

          ref = make_ref()
          test_pid = self()
          handler_id = "config-values-sensitive-test-#{inspect(ref)}"

          :telemetry.attach(
            handler_id,
            [:ferricstore, :config, :changed],
            fn _event, _measurements, metadata, _config ->
              send(test_pid, {:config_changed, metadata})
            end,
            nil
          )

          on_exit(fn -> :telemetry.detach(handler_id) end)

          Server.handle("CONFIG", ["SET", "requirepass", "top-secret"], store)

          assert_receive {:config_changed, metadata}, 1_000
          assert metadata.param == "requirepass"
          assert metadata.value == "[redacted]"
          refute inspect(metadata) =~ "top-secret"
        end
      end

      describe "CONFIG GET pattern matching" do
        test "CONFIG GET max* matches maxmemory, maxmemory-policy, maxclients", %{store: store} do
          result = Server.handle("CONFIG", ["GET", "max*"], store)
          keys = every_other(result, 0)

          assert "maxmemory" in keys
          assert "maxmemory-policy" in keys
          assert "maxclients" in keys
          refute "hz" in keys
        end

        test "CONFIG GET slowlog-* matches both slowlog parameters", %{store: store} do
          result = Server.handle("CONFIG", ["GET", "slowlog-*"], store)
          keys = every_other(result, 0)

          assert "slowlog-log-slower-than" in keys
          assert "slowlog-max-len" in keys
        end

        test "CONFIG GET tls-* matches TLS parameters", %{store: store} do
          result = Server.handle("CONFIG", ["GET", "tls-*"], store)
          keys = every_other(result, 0)

          assert "tls-port" in keys
          assert "tls-cert-file" in keys
          assert "tls-key-file" in keys
        end

        test "CONFIG GET h? matches hz", %{store: store} do
          result = Server.handle("CONFIG", ["GET", "h?"], store)
          keys = every_other(result, 0)
          assert "hz" in keys
        end
      end

      describe "CONFIG unknown subcommand" do
        test "unknown CONFIG subcommand returns error", %{store: store} do
          result = Server.handle("CONFIG", ["BADSUBCMD"], store)
          assert {:error, msg} = result
          assert msg =~ "unknown subcommand"
        end

        test "CONFIG with no args returns error", %{store: store} do
          result = Server.handle("CONFIG", [], store)
          assert {:error, _} = result
        end
      end
    end
  end
end
