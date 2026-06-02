# Release Process

## Prerequisites

- Push access to the GitHub repository
- `mix hex.user auth` configured for Hex.pm publishing

## Steps

1. Bump `@version` in `apps/ferricstore/mix.exs`
2. Commit: `git commit -am "release: vX.Y.Z"`
3. Tag: `git tag vX.Y.Z`
4. Push: `git push origin main --tags`
5. Wait for the **Build precompiled NIFs** GitHub Actions workflow to complete
   - Verify all 6 platform binaries appear in the GitHub Release
6. Download checksums locally:
   ```bash
   mix rustler_precompiled.download Ferricstore.Bitcask.NIF --all --print
   ```
7. Commit the generated checksum file:
   ```bash
   git add apps/ferricstore/checksum-Elixir.Ferricstore.Bitcask.NIF.exs
   git commit -m "release: update NIF checksums for vX.Y.Z"
   ```
8. Hex.pm publish happens automatically via the **Publish to Hex.pm** workflow
   (requires `HEX_API_KEY` secret). Docker Hub push also triggers automatically
   (requires `DOCKERHUB_USERNAME` + `DOCKERHUB_TOKEN` secrets).

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
