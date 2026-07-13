# Release Process

## Prerequisites

- Push access to the GitHub repository
- `mix hex.user auth` configured for Hex.pm publishing

## Steps

1. Bump the version in `apps/ferricstore/mix.exs` and
   `apps/ferricstore_server/mix.exs`.
2. Stamp the release's BSL Change Date. The command uses today's UTC date and
   writes an explicit date four years later to `LICENSE`:
   ```bash
   elixir .github/scripts/stamp_bsl_license.exs X.Y.Z
   ```
3. Commit: `git commit -am "release: vX.Y.Z"`
4. Tag: `git tag vX.Y.Z`
5. Push: `git push origin main --tags`
6. Wait for the **Build precompiled NIFs** GitHub Actions workflow to complete
   - Verify all 6 platform binaries appear in the GitHub Release
7. Download checksums locally:
   ```bash
   mix rustler_precompiled.download Ferricstore.Bitcask.NIF --all --print
   ```
8. Commit the generated checksum file:
   ```bash
   git add apps/ferricstore/checksum-Elixir.Ferricstore.Bitcask.NIF.exs
   git commit -m "release: update NIF checksums for vX.Y.Z"
   ```
9. Hex.pm publish happens automatically via the **Publish to Hex.pm** workflow
   (requires `HEX_API_KEY` secret). Docker Hub push also triggers automatically
   (requires `DOCKERHUB_USERNAME` + `DOCKERHUB_TOKEN` secrets).

The tag workflows verify that the tag version matches the project version and
that `LICENSE` was stamped using the release commit date. They stop before
publishing artifacts if either check fails. If a release commit is prepared on
a different UTC date from the tag, rerun the stamp command and amend the
release commit before tagging.

## Development builds

Developers with Rust installed can compile the NIF from source:

```bash
mix compile
```

Pre-release versions (e.g., `0.2.0-dev`) automatically force source compilation.

## Platform targets

| Target | OS | Arch | Notes |
|--------|----|------|-------|
| `aarch64-apple-darwin` | macOS | ARM64 | Apple Silicon M1-M4 |
| `x86_64-apple-darwin` | macOS | x86_64 | Intel Macs |
| `aarch64-unknown-linux-gnu` | Linux | ARM64 | AWS Graviton, glibc |
| `aarch64-unknown-linux-musl` | Linux | ARM64 | Alpine ARM64 |
| `x86_64-unknown-linux-gnu` | Linux | x86_64 | Most production servers |
| `x86_64-unknown-linux-musl` | Linux | x86_64 | Alpine, distroless |
