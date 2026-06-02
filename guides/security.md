# Security

FerricStore provides multiple layers of security: ACL user accounts with fine-grained permissions, TLS encryption, protected mode, password hashing, and audit logging.

## Access Control Lists (ACL)

FerricStore implements Redis-compatible ACL with enhancements for production use.

### Default User

On startup, a `default` user is created with:
- Enabled
- No password required
- Full access to all commands (`+@all`)
- Full access to all keys (`~*`)

The `default` user cannot be deleted.

### Creating Users

```bash
# Create a read-only user with password
redis-cli ACL SETUSER reader on >secretpassword +@read ~*

# Create an admin user with full access
redis-cli ACL SETUSER admin on >adminpassword +@all ~*

# Create a user restricted to specific key patterns
redis-cli ACL SETUSER appuser on >apppassword +@all ~app:*

# Create a user with specific commands only
redis-cli ACL SETUSER metrics on >metricspass +INFO +DBSIZE +KEYS ~*
```

### ACL Rules

| Rule | Description |
|------|-------------|
| `on` | Enable the user |
| `off` | Disable the user |
| `>password` | Set password (hashed with PBKDF2-SHA256) |
| `<password` | Remove password |
| `nopass` | Allow connection without password |
| `+command` | Allow a specific command |
| `-command` | Deny a specific command |
| `+@category` | Allow all commands in a category |
| `-@category` | Deny all commands in a category |
| `~pattern` | Allow read+write on keys matching the glob pattern |
| `%R~pattern` | Allow read-only on keys matching the glob pattern |
| `%W~pattern` | Allow write-only on keys matching the glob pattern |
| `allkeys` | Allow all keys (shorthand for `~*`) |
| `allcommands` | Allow all commands (shorthand for `+@all`) |

### Command Categories

| Category | Commands |
|----------|----------|
| `@read` | GET, MGET, HGET, HGETALL, LRANGE, SMEMBERS, ZSCORE, EXISTS, TTL, SCAN, TYPE, and other read-only commands |
| `@write` | SET, DEL, HSET, LPUSH, SADD, ZADD, INCR, EXPIRE, and other mutation commands |
| `@admin` | CONFIG, ACL, DEBUG, FLUSHDB, FLUSHALL, SHUTDOWN, FERRICSTORE.DOCTOR, and server administration |
| `@dangerous` | FLUSHDB, FLUSHALL, DEBUG, KEYS, SHUTDOWN, FERRICSTORE.DOCTOR repair jobs, and potentially destructive commands |

### Authenticating

```bash
# With default user (if password is set)
redis-cli AUTH mypassword

# With named user
redis-cli AUTH username password
```

### Managing Users

```bash
# List all users
redis-cli ACL LIST

# Get user details
redis-cli ACL GETUSER reader

# Check current user
redis-cli ACL WHOAMI

# Delete a user
redis-cli ACL DELUSER reader
```

### Persisting ACL

```bash
# Save current ACL to data_dir/acl.conf
redis-cli ACL SAVE

# Reload ACL from file
redis-cli ACL LOAD
```

ACL state is auto-loaded from `data_dir/acl.conf` on startup if the file exists. The file is written atomically (tmp + fsync + rename) with 0600 permissions.

## Password Hashing

Passwords are never stored in plaintext. FerricStore uses PBKDF2-SHA256 with:
- 100,000 iterations
- Random salt per password
- `:crypto` module for cryptographic operations

When you set a password with `>password`, the plaintext is immediately hashed and only the hash is stored in the ACL ETS table. `ACL LOAD` rejects files containing plaintext passwords.

## Protected Mode

When FerricStore starts with the `default` user having no password and no other ACL users configured, protected mode is active. In this state, non-localhost connections are rejected.

To disable protected mode, either:
1. Set a password on the default user: `ACL SETUSER default on >password`
2. Create at least one non-default user with a password

## TLS Configuration

FerricStore has **two separate network layers**, each with its own TLS:

| Layer | What it carries | How to encrypt |
|-------|----------------|---------------|
| **Client ↔ Server** | Redis commands (RESP3 over TCP) | FerricStore TLS config (`:tls_port`, `:tls_cert_file`, etc.) — configured below |
| **Node ↔ Node** | Raft consensus, cluster messages (Erlang distribution) | Erlang distribution TLS (`-proto_dist inet_tls`) — configured at VM level, see [Node-to-Node TLS](#node-to-node-tls-erlang-distribution) below |

Configuring client TLS does **not** encrypt Raft traffic between nodes. In
production multi-node deployments, you should configure both.

### Client-Server TLS

Encrypt all client-server communication with TLS.

### Server Configuration

```elixir
# config/config.exs
config :ferricstore, :tls_port, 6380
config :ferricstore, :tls_cert_file, "/etc/ssl/ferricstore/server-cert.pem"
config :ferricstore, :tls_key_file, "/etc/ssl/ferricstore/server-key.pem"
```

### Mutual TLS (Client Certificate Authentication)

```elixir
config :ferricstore, :tls_ca_cert_file, "/etc/ssl/ferricstore/ca-cert.pem"
```

When `tls_ca_cert_file` is set, the server requires clients to present a certificate signed by the specified CA.

### Enforcing TLS Only

```elixir
config :ferricstore, :require_tls, true
```

When `require_tls` is `true`, the plaintext TCP listener still starts (for health checks, local admin) but rejects non-TLS client connections.

### Connecting with TLS

```bash
redis-cli -p 6380 --tls \
  --cert /path/to/client-cert.pem \
  --key /path/to/client-key.pem \
  --cacert /path/to/ca-cert.pem
```

```elixir
{:ok, conn} = Redix.start_link(
  host: "localhost",
  port: 6380,
  ssl: true,
  socket_opts: [
    certfile: "/path/to/client-cert.pem",
    keyfile: "/path/to/client-key.pem",
    cacertfile: "/path/to/ca-cert.pem"
  ]
)
```

### TLS in Kubernetes

In Kubernetes, use cert-manager to issue and rotate certificates automatically.
Mount them as a Secret volume into the FerricStore pod.

**1. Create a Certificate resource:**

```yaml
# ferricstore-cert.yaml
apiVersion: cert-manager.io/v1
kind: Certificate
metadata:
  name: ferricstore-tls
  namespace: default
spec:
  secretName: ferricstore-tls-secret
  duration: 2160h    # 90 days
  renewBefore: 360h  # renew 15 days before expiry
  issuerRef:
    name: my-cluster-issuer   # your cert-manager ClusterIssuer
    kind: ClusterIssuer
  dnsNames:
    - ferricstore
    - ferricstore.default.svc.cluster.local
    - "*.ferricstore-headless.default.svc.cluster.local"  # for multi-node
```

**2. Mount the secret in the pod:**

```yaml
# ferricstore-deployment.yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: ferricstore
spec:
  template:
    spec:
      containers:
        - name: ferricstore
          env:
            - name: TLS_CERT_FILE
              value: /etc/tls/tls.crt
            - name: TLS_KEY_FILE
              value: /etc/tls/tls.key
            - name: TLS_CA_FILE
              value: /etc/tls/ca.crt
          volumeMounts:
            - name: tls-certs
              mountPath: /etc/tls
              readOnly: true
      volumes:
        - name: tls-certs
          secret:
            secretName: ferricstore-tls-secret
```

**3. Configure FerricStore to read from the mounted paths:**

```elixir
# config/runtime.exs
config :ferricstore, :tls_port, 6380
config :ferricstore, :tls_cert_file, System.get_env("TLS_CERT_FILE")
config :ferricstore, :tls_key_file, System.get_env("TLS_KEY_FILE")
config :ferricstore, :tls_ca_cert_file, System.get_env("TLS_CA_FILE")
config :ferricstore, :require_tls, true
```

cert-manager automatically renews the certificate and updates the Secret.
Kubernetes restarts the pod when the secret changes (or use a sidecar like
reloader to trigger a graceful restart).

### TLS in Nomad

In Nomad, use Vault's PKI secrets engine for certificate issuance.

**1. Vault template in the job spec:**

```hcl
job "ferricstore" {
  group "cache" {
    task "ferricstore" {
      template {
        data = <<EOF
{{ with secret "pki/issue/ferricstore" "common_name=ferricstore.service.consul" "ttl=720h" }}
{{ .Data.certificate }}
{{ end }}
EOF
        destination = "secrets/server-cert.pem"
        change_mode = "restart"
      }

      template {
        data = <<EOF
{{ with secret "pki/issue/ferricstore" "common_name=ferricstore.service.consul" "ttl=720h" }}
{{ .Data.private_key }}
{{ end }}
EOF
        destination = "secrets/server-key.pem"
        change_mode = "restart"
      }

      template {
        data = <<EOF
{{ with secret "pki/issue/ferricstore" "common_name=ferricstore.service.consul" "ttl=720h" }}
{{ .Data.issuing_ca }}
{{ end }}
EOF
        destination = "secrets/ca-cert.pem"
        change_mode = "restart"
      }

      env {
        TLS_CERT_FILE = "${NOMAD_SECRETS_DIR}/server-cert.pem"
        TLS_KEY_FILE  = "${NOMAD_SECRETS_DIR}/server-key.pem"
        TLS_CA_FILE   = "${NOMAD_SECRETS_DIR}/ca-cert.pem"
      }
    }
  }
}
```

**2. Same `runtime.exs` config** as the Kubernetes example — reads from
environment variables.

Vault automatically renews certificates before they expire. The
`change_mode = "restart"` triggers a graceful task restart when new certs are
issued.

### TLS Certificate Rotation

FerricStore reads certificate files at startup. When certificates are renewed:

- **Kubernetes**: cert-manager updates the Secret → pod restart picks up new certs
- **Nomad**: Vault template `change_mode = "restart"` triggers task restart
- **Manual**: Restart the FerricStore process after placing new certificates

> FerricStore does not currently support hot-reloading TLS certificates without
> restart. Existing connections continue with the old certificate until they
> disconnect. New connections after restart use the new certificate.

### Node-to-Node Security (Erlang Distribution)

#### What is Erlang distribution?

When FerricStore runs as a multi-node cluster, nodes talk to each other using
**Erlang distribution** — a protocol built into the BEAM VM. This is not
FerricStore-specific; it's how all Elixir/Erlang clusters communicate.

Everything between nodes goes over this channel:
- Raft consensus messages (AppendEntries, vote requests, commits)
- Cluster membership changes (node join/leave)
- `GenServer.call` across nodes
- Process monitoring (`Process.monitor` on remote processes)
- PubSub broadcasts

#### Why it's a security risk

Erlang distribution has **two serious security problems** out of the box:

**1. No encryption.** All traffic is plaintext. Anyone on the network can see
your data, Raft log entries, and internal messages. This is especially dangerous
across availability zones or any network you don't fully control.

**2. The Erlang cookie gives full access.** Nodes authenticate using a shared
secret called the "cookie" (stored in `~/.erlang.cookie` or set via
`--cookie`). Any process that knows the cookie can connect to any node and
execute **arbitrary code** — including `System.cmd("rm", ["-rf", "/"])`. The
cookie is NOT a password; it's a symmetric key that grants root-level access
to the BEAM VM.

```
  Without security:

  Attacker knows cookie → connects to node → :rpc.call(node, System, :cmd, ["rm", ["-rf", "/"]])
                                                 ↑
                                            full remote code execution
```

#### Step 1: Set a strong cookie

Never use the default cookie. Generate a random one:

```bash
# Generate a strong random cookie
openssl rand -base64 32 > /etc/ferricstore/cookie
chmod 400 /etc/ferricstore/cookie
```

In your release config (`rel/env.sh.eex`):

```bash
export RELEASE_COOKIE=$(cat /etc/ferricstore/cookie)
```

Or in Kubernetes, store it as a Secret:

```yaml
env:
  - name: RELEASE_COOKIE
    valueFrom:
      secretKeyRef:
        name: ferricstore-secrets
        key: erlang-cookie
```

> **All nodes in the cluster must share the same cookie.** They cannot
> communicate without it.

#### Step 2: Restrict distribution to a specific interface

By default, Erlang distribution listens on all interfaces (`0.0.0.0`). Bind it
to a private interface:

```bash
# Only listen on the private network interface
export ELIXIR_ERL_OPTIONS="-kernel inet_dist_use_interface {10,0,0,1}"
```

Or restrict the port range (useful for firewall rules):

```bash
export ELIXIR_ERL_OPTIONS="-kernel inet_dist_listen_min 9100 -kernel inet_dist_listen_max 9110"
```

Then firewall those ports to only allow traffic from other FerricStore nodes.

#### Step 3: Enable TLS for distribution

For full encryption and mutual authentication, enable Erlang distribution over
TLS. This encrypts all node-to-node traffic and requires each node to present
a valid certificate.

**1. Create an `inet_tls.conf` file:**

```erlang
%% inet_tls.conf
[{server, [
  {certfile, "/etc/ssl/ferricstore/node-cert.pem"},
  {keyfile, "/etc/ssl/ferricstore/node-key.pem"},
  {cacertfile, "/etc/ssl/ferricstore/ca-cert.pem"},
  {verify, verify_peer},
  {fail_if_no_peer_cert, true},
  {secure_renegotiate, true}
]},
{client, [
  {certfile, "/etc/ssl/ferricstore/node-cert.pem"},
  {keyfile, "/etc/ssl/ferricstore/node-key.pem"},
  {cacertfile, "/etc/ssl/ferricstore/ca-cert.pem"},
  {verify, verify_peer},
  {secure_renegotiate, true}
]}].
```

**2. Start the BEAM with TLS distribution:**

```bash
elixir --erl "-proto_dist inet_tls -ssl_dist_optfile /etc/ssl/ferricstore/inet_tls.conf" \
  -S mix run --no-halt
```

Or in a release (`rel/env.sh.eex`):

```bash
export ELIXIR_ERL_OPTIONS="-proto_dist inet_tls -ssl_dist_optfile /etc/ssl/ferricstore/inet_tls.conf"
```

**3. Kubernetes example** — same cert-manager certs can be used for both client
TLS and distribution TLS. Mount to the same path and reference from both
FerricStore config and `inet_tls.conf`.

**4. Nomad example** — Vault can issue separate node certificates:

```hcl
template {
  data = <<EOF
{{ with secret "pki/issue/ferricstore-node" "common_name={{ env "attr.unique.hostname" }}" "ttl=720h" }}
{{ .Data.certificate }}
{{ end }}
EOF
  destination = "secrets/node-cert.pem"
  change_mode = "restart"
}
```

> **Important:** All nodes in the cluster must use certificates signed by the
> same CA. A node presenting a certificate from a different CA will be rejected
> and unable to join the cluster.
>
> See the [Erlang/OTP documentation on SSL Distribution](https://www.erlang.org/doc/apps/ssl/ssl_distribution.html)
> for the full reference.

#### Security checklist for multi-node deployments

| Layer | Minimum | Recommended |
|-------|---------|-------------|
| Erlang cookie | Random, not default | Rotated periodically, stored in secret manager |
| Distribution interface | Bind to private network | Private network + firewall to cluster nodes only |
| Distribution encryption | None (default) | TLS with mutual certificate verification |
| Distribution ports | Random ephemeral | Fixed range, firewalled |
| Client TLS | Optional | Required (`require_tls: true`) with mutual TLS |
| ACL | Default user with password | Per-application users with key patterns |

#### Single-node deployments

If you're running FerricStore as a single node (the common case for embedded
mode), none of the distribution security applies — there is no inter-node
communication. Just configure client TLS and ACL.

## Audit Logging

FerricStore maintains an audit trail of security-relevant events in an ETS ring buffer. Logged events include:

- ACL command denials (user, command, client IP, client ID)
- Authentication attempts (success and failure)
- ACL mutations (SETUSER, DELUSER)
- Configuration changes (CONFIG SET)

### Viewing Audit Logs

Audit logs are accessible through the `Ferricstore.AuditLog` module in embedded mode:

```elixir
Ferricstore.AuditLog.recent(20)
```

In standalone mode, audit events are also emitted as telemetry events that can be forwarded to external logging systems.

## ACL Cache Invalidation

In standalone mode, each connection process caches the ACL user record for fast permission checks (~5ns vs ~250ns for ETS lookup on every command). When an ACL mutation occurs (SETUSER, DELUSER), a broadcast is sent via `:pg` to all connection processes, which invalidate their cached ACL records.

This means ACL changes take effect on the next command from each connection, with no need to disconnect and reconnect.

## Security Best Practices

1. **Set a password on the default user** in production:
   ```bash
   ACL SETUSER default on >strong_random_password
   ```

2. **Create dedicated users** for each application with minimum required permissions:
   ```bash
   ACL SETUSER myapp on >apppassword +@read +@write -@dangerous ~myapp:*
   ```

3. **Enable TLS** for all production deployments:
   ```elixir
   config :ferricstore, :require_tls, true
   ```

4. **Use key patterns** to restrict which keys each user can access:
   ```bash
   ACL SETUSER analytics on >pass +@read ~analytics:* ~metrics:*
   ```

5. **Deny dangerous commands** for non-admin users:
   ```bash
   ACL SETUSER appuser on >pass +@all -@dangerous -@admin ~app:*
   ```

6. **Persist ACL** so changes survive restarts:
   ```bash
   ACL SAVE
   ```

7. **Monitor audit logs** for unauthorized access attempts.

8. **Use mutual TLS** in zero-trust environments:
   ```elixir
   config :ferricstore, :tls_ca_cert_file, "/etc/ssl/ca.pem"
   ```

## Limitations

- **No encryption at rest** -- Bitcask data files are stored in plaintext on disk. Use filesystem-level encryption (LUKS, dm-crypt) for data-at-rest protection.
- **ACL key pattern enforcement** is still being finalized for all command types. String, hash, list, set, and sorted set commands enforce key patterns. Some administrative commands bypass key checks by design.
- **No client certificate to ACL user mapping** -- mutual TLS authenticates the client but does not automatically map to an ACL user. Use `AUTH` after connecting.
