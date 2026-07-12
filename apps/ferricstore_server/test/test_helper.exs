# Server app test helper.
# Tags are inherited from the umbrella root run and from the core app.
{:ok, _apps} = Application.ensure_all_started(:ferricstore_server)

ExUnit.start(
  exclude: [:bench, :linux_io_uring, :large_alloc, :cluster, :jepsen, :shard_kill],
  formatters: [ExUnit.CLIFormatter, JUnitFormatter, Ferricstore.Test.AuditFormatter]
)

# Registration is idempotent when the core helper already ran in this VM.
:ok = Ferricstore.Test.DataDirLifecycle.register_generated_cleanup()
