# Security Policy

## Reporting A Vulnerability

Please do not open a public GitHub issue for suspected vulnerabilities.

Report security issues by emailing the project maintainers or by using GitHub private vulnerability reporting if it is enabled for the repository. Include:

- affected version or commit
- reproduction steps
- impact
- any logs or traces that help validate the issue

We will acknowledge reports as quickly as possible and coordinate fixes before public disclosure.

## Supported Versions

FerricStore is early-stage open source. Security fixes are currently targeted at the latest public release and `main`.

## Scope

Security-sensitive areas include:

- ACL and authentication
- TLS configuration
- Native protocol frame decoding and command validation
- file path handling and snapshot/backup paths
- Flow value refs and payload retention
- Raft/Bitcask recovery paths
- dashboard/admin endpoints
