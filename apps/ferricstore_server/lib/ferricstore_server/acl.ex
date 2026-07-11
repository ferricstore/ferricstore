defmodule FerricstoreServer.Acl do
  @moduledoc """
  GenServer managing the Access Control List (ACL) for FerricStore.

  Stores user accounts in a named ETS table (`:ferricstore_acl`). Each user
  record has a username, enabled/disabled flag, an optional password hash,
  allowed commands, denied commands, allowed key patterns, and allowed Pub/Sub
  channel patterns.

  The "default" user is always present and cannot be deleted. On startup it
  is initialised as enabled with no password and full access (`+@all`, `~*`).

  ## Spec reference

  Implements spec section 6.1: `ACL SETUSER`, `ACL DELUSER`, `ACL GETUSER`,
  `ACL LIST`, `ACL WHOAMI`.

  ## Security hardening (Phase 1)

  1. **Password hashing** -- passwords are hashed with PBKDF2-SHA256 (salt +
     :crypto) at ingestion time. Plaintext passwords are never stored in ETS.
  2. **denied_commands set** -- explicit denials via `-command` or `-@category`
     work even when `commands` is `:all`. Denials are tracked in a separate
     `denied_commands` MapSet and subtracted at check time.
  3. **Protected mode** -- non-localhost connections are rejected until at least
     one non-default ACL user with a password is configured.
  4. **max_acl_users** -- configurable safety limit (default 10,000) prevents
     unbounded ACL user creation.
  5. **ACL LOG denials** -- command denials are logged to the AuditLog with
     username, command, client IP, and client ID.

  ## File persistence (Phase 2)

  ACL state can be saved to and loaded from `data_dir/acl.conf`:

  - **ACL SAVE** -- atomic write (tmp + fsync + rename), 0600 permissions
  - **ACL LOAD** -- all-or-nothing validation, rejects plaintext passwords
  - **Auto-load on startup** -- loads from file if it exists
  - **Auto-save** -- configurable debounced save on ACL mutations

  ## Command categories

  Commands can be granted or revoked by category using `+@category` / `-@category`:

    - `@read`      -- read-only commands (GET, MGET, HGET, EXISTS, TTL, etc.)
    - `@write`     -- mutation commands (SET, DEL, HSET, LPUSH, INCR, etc.)
    - `@admin`     -- server administration (CONFIG, ACL, DEBUG, FLUSHDB, etc.)
    - `@dangerous` -- potentially destructive (FLUSHDB, FLUSHALL, DEBUG, KEYS, SHUTDOWN, etc.)
    - command-family categories such as `@string`, `@hash`, `@flow`, `@stream`,
      `@probabilistic`, `@pubsub`, `@connection`, and `@transaction`

  ## ETS schema

  Each row is a tuple:

      {username :: binary(), %{
        enabled: boolean(),
        password: binary() | nil,
        commands: :all | MapSet.t(binary()),
        denied_commands: MapSet.t(binary()),
        keys: :all | [key_pattern()],
        channels: :all | [channel_pattern()]
      }}

  ## Usage

      FerricstoreServer.Acl.set_user("alice", ["on", ">s3cret", "~cache:*", "+get", "+set"])
      FerricstoreServer.Acl.authenticate("alice", "s3cret")
      #=> {:ok, user}

      FerricstoreServer.Acl.check_command("alice", "GET")
      #=> :ok

      FerricstoreServer.Acl.check_command("alice", "FLUSHDB")
      #=> {:error, "NOPERM this user has no permissions to run the 'flushdb' command"}

      FerricstoreServer.Acl.del_user("alice")
      #=> :ok
  """

  use GenServer

  require Logger

  alias FerricstoreServer.Acl.{
    CommandCategories,
    Formatter,
    Password,
    Persistence,
    Rules,
    Tables,
    Patterns,
    Protection
  }

  # ---------------------------------------------------------------------------
  # Types
  # ---------------------------------------------------------------------------

  @typedoc """
  A compiled key pattern: {original_glob, access_mode, compiled_regex}.

  Access modes:
    - `:rw`    -- full read+write access (from `~pattern`)
    - `:read`  -- read-only access (from `%R~pattern`)
    - `:write` -- write-only access (from `%W~pattern`)
  """
  @type key_pattern :: {binary(), :rw | :read | :write, Regex.t()}

  @typedoc "A compiled Pub/Sub channel pattern: {original_glob, compiled_regex}."
  @type channel_pattern :: {binary(), Regex.t()}

  @typedoc "A user record stored in the ACL table."
  @type user :: %{
          enabled: boolean(),
          auth_epoch: non_neg_integer(),
          password: binary() | nil,
          commands: :all | MapSet.t(binary()),
          denied_commands: MapSet.t(binary()),
          keys: :all | [key_pattern()],
          channels: :all | [channel_pattern()]
        }

  # ---------------------------------------------------------------------------
  # Constants -- file persistence
  # ---------------------------------------------------------------------------

  @auto_save_debounce_ms 1_000

  # ---------------------------------------------------------------------------
  # Public API
  # ---------------------------------------------------------------------------

  @doc """
  Starts the ACL GenServer and creates the backing ETS table.

  Initialises the "default" user with full access. If an ACL file exists
  at `data_dir/acl.conf`, it is loaded on startup. If the file is invalid,
  a warning is logged and the server starts with the default user only.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Creates or updates a user with the given rules.

  Rules are a list of strings parsed in order:

    * `"on"`           -- enable the user
    * `"off"`          -- disable the user
    * `">password"`    -- set the user's password (hashed before storing)
    * `"nopass"`       -- clear the user's password (allow passwordless auth)
    * `"~pattern"`     -- add a key pattern (e.g. `"~*"` for all keys)
    * `"+command"`     -- allow a specific command
    * `"+@all"`        -- allow all commands
    * `"+@category"`   -- allow all commands in a category (read, write, admin, dangerous)
    * `"-command"`     -- deny a specific command (works even after +@all)
    * `"-@all"`        -- deny all commands
    * `"-@category"`   -- deny all commands in a category (works even after +@all)
    * `"allkeys"`      -- shorthand for `"~*"`
    * `"allcommands"`  -- shorthand for `"+@all"`
    * `"resetpass"`    -- clear the password

  When creating a new user with no rules, the user is created in a disabled
  state with no password and no permissions (safe default).

  Returns `:ok` on success, `{:error, reason}` on invalid rules.

  ## Parameters

    - `username` -- the username (case-sensitive binary)
    - `rules`    -- list of rule strings

  ## Examples

      FerricstoreServer.Acl.set_user("alice", ["on", ">s3cret", "~*", "+@all"])
      #=> :ok

      FerricstoreServer.Acl.set_user("reader", ["on", ">pass", "-@all", "+@read"])
      #=> :ok
  """
  @spec set_user(binary(), [binary()]) :: :ok | {:error, binary()}
  def set_user(username, rules) do
    GenServer.call(__MODULE__, {:set_user, username, rules})
  end

  @doc """
  Deletes a user from the ACL.

  The "default" user cannot be deleted.

  Returns `:ok` on success, `{:error, reason}` if the user is "default" or
  does not exist.

  ## Parameters

    - `username` -- the username to delete

  ## Examples

      FerricstoreServer.Acl.del_user("alice")
      #=> :ok
  """
  @spec del_user(binary()) :: :ok | {:error, binary()}
  def del_user(username) do
    GenServer.call(__MODULE__, {:del_user, username})
  end

  @doc """
  Deletes multiple users atomically.

  The full list is validated before any ETS row is removed, so a protected or
  missing user cannot leave the ACL partially mutated.
  """
  @spec del_users([binary()]) :: :ok | {:error, binary()}
  def del_users(usernames) when is_list(usernames) do
    GenServer.call(__MODULE__, {:del_users, usernames})
  end

  @doc """
  Returns the user record for the given username, or `nil` if not found.

  ## Parameters

    - `username` -- the username to look up

  ## Examples

      FerricstoreServer.Acl.get_user("default")
      #=> %{enabled: true, password: nil, commands: :all, denied_commands: MapSet.new(), keys: :all}
  """
  @spec get_user(binary()) :: user() | nil
  def get_user(username) do
    case :ets.lookup(Tables.active_table(), username) do
      [{^username, user}] -> user
      [] -> nil
    end
  end

  @doc """
  Returns a list of all users in Redis ACL LIST format.

  Each entry is a string like `"user default on ~* &* +@all"`.

  ## Examples

      FerricstoreServer.Acl.list_users()
      #=> ["user default on ~* &* +@all", "user alice on ~cache:* +get +set"]
  """
  @spec list_users() :: [binary()]
  def list_users do
    Tables.active_table()
    |> :ets.tab2list()
    |> Enum.sort_by(fn {name, _} -> name end)
    |> Enum.map(&Formatter.format_user_rule/1)
  end

  @doc """
  Returns the user info for `ACL GETUSER` in the flat command response format.

  Returns `nil` if the user does not exist.

  ## Parameters

    - `username` -- the username to look up

  ## Examples

      FerricstoreServer.Acl.get_user_info("default")
      #=> ["flags", ["on"], "passwords", [], "commands", "+@all", "keys", "~*", "channels", "&*"]
  """
  @spec get_user_info(binary()) :: [term()] | nil
  def get_user_info(username) do
    case get_user(username) do
      nil ->
        nil

      user ->
        flags = if user.enabled, do: ["on"], else: ["off"]

        passwords =
          if user.password, do: [Password.hash_for_display(user.password)], else: []

        commands = Formatter.format_user_commands(user)
        keys = Formatter.format_keys(user.keys)
        channels = Formatter.format_channels(Rules.user_channels(user))

        [
          "flags",
          flags,
          "passwords",
          passwords,
          "commands",
          commands,
          "keys",
          keys,
          "channels",
          channels
        ]
    end
  end

  @doc """
  Authenticates a user with the given password.

  Passwords are verified against the stored PBKDF2-SHA256 hash. Nopass users
  (password is `nil`) accept any password.

  Returns `{:ok, username}` on success, `{:error, reason}` on failure.

  ## Parameters

    - `username` -- the username to authenticate
    - `password` -- the plaintext password to check

  ## Examples

      FerricstoreServer.Acl.authenticate("default", "secret123")
      #=> {:ok, "default"}

      FerricstoreServer.Acl.authenticate("unknown", "pass")
      #=> {:error, "WRONGPASS invalid username-password pair or user is disabled."}
  """
  @spec authenticate(binary(), binary()) :: {:ok, binary()} | {:error, binary()}
  def authenticate(username, password) do
    case get_user(username) do
      nil ->
        verify_dummy_password(password, &Password.verify/2)
        authentication_error()

      %{enabled: false} ->
        verify_dummy_password(password, &Password.verify/2)
        authentication_error()

      %{password: nil} ->
        verify_dummy_password(password, &Password.verify/2)
        {:ok, username}

      %{password: stored_hash} ->
        if Password.verify(password, stored_hash) do
          maybe_upgrade_password_hash(username, password, stored_hash)
          {:ok, username}
        else
          authentication_error()
        end
    end
  end

  @doc false
  @spec authenticate(binary(), binary(), (binary(), binary() -> boolean())) ::
          {:ok, binary()} | {:error, binary()}
  def authenticate(username, password, verifier) when is_function(verifier, 2) do
    case get_user(username) do
      nil ->
        verify_dummy_password(password, verifier)
        authentication_error()

      %{enabled: false} ->
        verify_dummy_password(password, verifier)
        authentication_error()

      %{password: nil} ->
        verify_dummy_password(password, verifier)
        {:ok, username}

      %{password: stored_hash} ->
        if verifier.(password, stored_hash) do
          {:ok, username}
        else
          authentication_error()
        end
    end
  end

  defp verify_dummy_password(password, verifier) do
    _verified = verifier.(password, Password.dummy_hash())
    :ok
  end

  defp maybe_upgrade_password_hash(username, password, stored_hash) do
    if Password.needs_rehash?(stored_hash) do
      table = Tables.active_table()

      case :ets.lookup(table, username) do
        [{^username, %{password: ^stored_hash} = user}] ->
          updated = %{user | password: Password.hash(password)}
          :ets.insert(table, {username, updated})
          Tables.update_configured_user_witness(username, updated)

        _stale_or_missing ->
          :ok
      end
    end

    :ok
  end

  defp authentication_error do
    {:error, "WRONGPASS invalid username-password pair or user is disabled."}
  end

  @doc """
  Checks if the given user is allowed to run the given command (enabled check only).

  Legacy v1 check. Prefer `check_command/2` for full ACL enforcement.

  ## Parameters

    - `username` -- the username
    - `_command` -- the command name (currently unused)

  ## Returns

    - `:ok` if the user is allowed
    - `{:error, reason}` if denied
  """
  @spec check_permission(binary(), binary()) :: :ok | {:error, binary()}
  def check_permission(username, _command) do
    case get_user(username) do
      nil ->
        {:error, "NOPERM user '#{username}' does not exist"}

      %{enabled: false} ->
        {:error, "NOPERM user '#{username}' is disabled"}

      _ ->
        :ok
    end
  end

  @doc """
  Checks if the given user is allowed to run the given command.

  Performs a full ACL check:

    1. The user must exist.
    2. The user must be enabled.
    3. The command must not be in the user's `denied_commands` set.
    4. The command must be in the user's allowed command set (`:all` or a `MapSet`).

  When the user's commands field is `:all`, all commands are permitted unless
  they appear in `denied_commands`. When it is a `MapSet`, the command
  (uppercased) must be a member and not in `denied_commands`.

  ## Parameters

    - `username` -- the username
    - `command`  -- the command name (case-insensitive)

  ## Returns

    - `:ok` if the command is permitted
    - `{:error, reason}` with a `NOPERM` prefix if denied

  ## Examples

      FerricstoreServer.Acl.check_command("default", "GET")
      #=> :ok

      FerricstoreServer.Acl.check_command("readonly_user", "SET")
      #=> {:error, "NOPERM this user has no permissions to run the 'set' command"}
  """
  @spec check_command(binary(), binary()) :: :ok | {:error, binary()}
  def check_command(username, command) do
    cmd = Rules.normalize_acl_command_name(command)

    case get_user(username) do
      nil ->
        {:error,
         "NOPERM this user has no permissions to run the '#{String.downcase(cmd)}' command"}

      %{enabled: false} ->
        {:error,
         "NOPERM this user has no permissions to run the '#{String.downcase(cmd)}' command"}

      %{commands: :all, denied_commands: denied} ->
        if Rules.command_denied?(denied, cmd) do
          {:error,
           "NOPERM this user has no permissions to run the '#{String.downcase(cmd)}' command"}
        else
          :ok
        end

      %{commands: cmds, denied_commands: denied} ->
        cond do
          Rules.command_denied?(denied, cmd) ->
            {:error,
             "NOPERM this user has no permissions to run the '#{String.downcase(cmd)}' command"}

          Rules.command_allowed?(cmds, cmd) ->
            :ok

          true ->
            {:error,
             "NOPERM this user has no permissions to run the '#{String.downcase(cmd)}' command"}
        end
    end
  end

  @doc """
  Checks if the given user is allowed to access the given key with the given
  access type (`:read` or `:write`).

  Returns `:ok` if access is permitted, or `{:error, reason}` with a NOPERM
  prefix if denied.

  ## Parameters

    - `username`    -- the username
    - `key`         -- the key to check (binary)
    - `access_type` -- `:read` or `:write`

  ## Examples

      FerricstoreServer.Acl.check_key_access("default", "mykey", :read)
      #=> :ok

      FerricstoreServer.Acl.check_key_access("reader", "forbidden:key", :write)
      #=> {:error, "NOPERM this user has no permissions to access one of the keys mentioned in the command"}
  """
  @spec check_key_access(binary(), binary(), :read | :write) :: :ok | {:error, binary()}
  def check_key_access(username, key, access_type) do
    case get_user(username) do
      nil ->
        {:error,
         "NOPERM this user has no permissions to access one of the keys mentioned in the command"}

      %{enabled: false} ->
        {:error,
         "NOPERM this user has no permissions to access one of the keys mentioned in the command"}

      %{keys: :all} ->
        :ok

      %{keys: patterns} ->
        if key_matches_any?(key, access_type, patterns) do
          :ok
        else
          {:error,
           "NOPERM this user has no permissions to access one of the keys mentioned in the command"}
        end
    end
  end

  @doc """
  Checks if a key matches any of the given compiled key patterns for the
  given access type. Used by the cached ACL check in connection handlers.

  ## Parameters

    - `key`         -- the key to check
    - `access_type` -- `:read` or `:write`
    - `patterns`    -- list of `{glob, access_mode, regex}` tuples

  ## Returns

    - `true` if any pattern matches
    - `false` otherwise
  """
  @spec key_matches_any?(binary(), :read | :write, [key_pattern()]) :: boolean()
  def key_matches_any?(key, access_type, patterns),
    do: Patterns.key_matches_any?(key, access_type, patterns)

  @doc """
  Returns true if the Pub/Sub channel matches any ACL channel pattern.
  """
  @spec channel_matches_any?(binary(), [channel_pattern()]) :: boolean()
  def channel_matches_any?(channel, patterns),
    do: Patterns.channel_matches_any?(channel, patterns)

  @doc """
  Compiles a Redis ACL glob pattern into a regular expression.
  """
  @spec compile_glob(binary()) :: Regex.t()
  def compile_glob(pattern), do: Patterns.compile_glob(pattern)

  @doc """
  Returns the map of command categories.

  Each key is an uppercase category name (e.g. `"READ"`, `"WRITE"`, `"ADMIN"`,
  `"FLOW"`, `"PROBABILISTIC"`) and the value is a `MapSet` of uppercase command names.

  ## Examples

      FerricstoreServer.Acl.categories()
      #=> %{"READ" => MapSet.new(["GET", "MGET", ...]), ...}
  """
  @spec categories() :: %{binary() => MapSet.t(binary())}
  def categories, do: CommandCategories.categories()

  @doc """
  Resets the ACL to its initial state (only the default user).

  Used primarily in tests to avoid state leaking between test cases.
  """
  @spec reset!() :: :ok
  def reset! do
    GenServer.call(__MODULE__, :reset)
  end

  # ---------------------------------------------------------------------------
  # Raft replication hook
  # ---------------------------------------------------------------------------

  @doc """
  Handles ACL commands replicated through Raft.

  Called by the state machine's `:server_command` clause on all nodes.
  This ensures ACL mutations are applied consistently across the cluster.
  """
  @spec handle_raft_command(term()) :: term()
  def handle_raft_command({:acl_setuser, username, rules}), do: set_user(username, rules)

  def handle_raft_command({:acl_deluser, username}), do: del_user(username)

  def handle_raft_command({:acl_delusers, usernames}), do: del_users(usernames)

  def handle_raft_command({:acl_reset}), do: reset!()

  def handle_raft_command({:acl_load, contents}) when is_binary(contents),
    do: load_contents(contents)

  def handle_raft_command(_unknown), do: {:error, :unknown_acl_command}

  # ---------------------------------------------------------------------------
  # File persistence API (ACL SAVE / ACL LOAD)
  # ---------------------------------------------------------------------------

  @doc """
  Serializes all ACL users to the ACL file (`data_dir/acl.conf`).

  Uses atomic write (write to temp file, fsync, rename) to prevent
  corruption on crash. File permissions are set to 0600 (owner read/write
  only) to protect password hashes.

  The `data_dir` is read from application env `:ferricstore, :data_dir`.

  Returns `:ok` on success, `{:error, reason}` on failure.
  """
  @spec save() :: :ok | {:error, binary()}
  def save do
    GenServer.call(__MODULE__, :acl_save)
  end

  @doc """
  Saves ACL state to the given directory path.

  Same as `save/0` but with an explicit data directory.
  """
  @spec save(binary()) :: :ok | {:error, binary()}
  def save(data_dir) do
    GenServer.call(__MODULE__, {:acl_save, data_dir})
  end

  @doc """
  Reads the ACL file (`data_dir/acl.conf`) and replaces the current ACL state.

  Validates every line before applying. If any line is invalid, the entire
  file is rejected and the current ACL state is preserved (all-or-nothing).

  The `default` user must be defined in the file. If it is missing, the
  load is rejected.

  Returns `:ok` on success, `{:error, reason}` on failure (including the
  line number of the first error when applicable).
  """
  @spec load() :: :ok | {:error, binary()}
  def load do
    GenServer.call(__MODULE__, :acl_load)
  end

  @doc """
  Loads ACL state from the given directory path.

  Same as `load/0` but with an explicit data directory.
  """
  @spec load(binary()) :: :ok | {:error, binary()}
  def load(data_dir) do
    GenServer.call(__MODULE__, {:acl_load, data_dir})
  end

  @doc """
  Reads and validates the ACL file contents without applying them locally.

  `ACL LOAD` uses this before submitting the exact contents through the
  replicated server-command path so every node applies the same ACL snapshot.
  """
  @spec load_file_contents() :: {:ok, binary()} | {:error, binary()}
  def load_file_contents do
    data_dir = Application.get_env(:ferricstore, :data_dir, "data")
    load_file_contents(data_dir)
  end

  @doc """
  Reads and validates ACL file contents from an explicit data directory.
  """
  @spec load_file_contents(binary()) :: {:ok, binary()} | {:error, binary()}
  def load_file_contents(data_dir) do
    with {:ok, contents} <- Persistence.read_file_contents(data_dir),
         :ok <- Persistence.validate_contents(contents) do
      {:ok, contents}
    end
  end

  @doc """
  Applies already-validated ACL file contents to the in-memory ACL table.
  """
  @spec load_contents(binary()) :: :ok | {:error, binary()}
  def load_contents(contents) when is_binary(contents) do
    GenServer.call(__MODULE__, {:acl_load_contents, contents})
  end

  @doc """
  Returns the path to the ACL file for the given data directory.
  """
  @spec acl_file_path(binary()) :: binary()
  def acl_file_path(data_dir) do
    Persistence.acl_file_path(data_dir)
  end

  # ---------------------------------------------------------------------------
  # Protected mode API (Fix 3)
  # ---------------------------------------------------------------------------

  @doc """
  Returns whether protected mode is currently active.

  In standalone mode, protected mode defaults to `true`. In embedded mode,
  it defaults to `false`. The setting can be overridden via application env:

      config :ferricstore, :protected_mode, true

  ## Examples

      FerricstoreServer.Acl.protected_mode?()
      #=> true
  """
  @spec protected_mode?() :: boolean()
  def protected_mode?, do: Protection.protected_mode?()

  @spec has_configured_users?() :: boolean()
  def has_configured_users?, do: Protection.has_configured_users?()

  @spec localhost?({:inet.ip_address(), :inet.port_number()} | nil) :: boolean()
  def localhost?(peer), do: Protection.localhost?(peer)

  @spec check_protected_mode({:inet.ip_address(), :inet.port_number()} | nil) ::
          :ok | {:error, binary()}
  def check_protected_mode(peer), do: Protection.check_protected_mode(peer)

  @spec log_command_denied(binary(), binary(), binary(), term()) :: :ok
  def log_command_denied(username, command, client_ip, client_id),
    do: Protection.log_command_denied(username, command, client_ip, client_id)

  # ---------------------------------------------------------------------------
  # GenServer callbacks
  # ---------------------------------------------------------------------------

  @impl true
  def init(_opts) do
    Tables.cleanup_retired_tables()
    Tables.cleanup_new_swap_table()

    table = Tables.new_active_table()
    Tables.insert_default_user()

    # Auto-load from file on startup (design doc section 7 startup sequence)
    data_dir = Application.get_env(:ferricstore, :data_dir, "data")
    state = %{table: table, save_timer: nil, auth_epoch: 0}

    state =
      case Persistence.auto_load_from_file(data_dir, state.auth_epoch) do
        {:ok, auth_epoch} ->
          Logger.info("ACL loaded from #{acl_file_path(data_dir)}")
          %{state | auth_epoch: auth_epoch}

        {:error, :enoent} ->
          # No file -- start with default user only (normal for fresh installs)
          state

        {:error, reason} ->
          Logger.warning("ACL file load failed on startup, using defaults: #{reason}")
          state
      end

    {:ok, state}
  end

  @impl true
  def handle_call({:set_user, username, rules}, _from, state) do
    existing = get_user(username)
    table = Tables.active_table()

    # Fix 4: max_acl_users -- check limit before creating a new user.
    max = Application.get_env(:ferricstore, :max_acl_users, 10_000)

    if existing == nil and :ets.info(table, :size) >= max do
      {:reply, {:error, "ERR max ACL users reached (#{max})"}, state}
    else
      base =
        if existing do
          existing
        else
          %{
            enabled: false,
            password: nil,
            commands: MapSet.new(),
            denied_commands: MapSet.new(),
            keys: [],
            channels: []
          }
        end

      case Rules.apply_rules(base, rules) do
        {:ok, updated} ->
          auth_epoch = state.auth_epoch + 1
          updated = Map.put(updated, :auth_epoch, auth_epoch)
          :ets.insert(table, {username, updated})
          :ok = Tables.update_configured_user_witness(username, updated)
          :ok = broadcast_acl_invalidation(username)
          state = %{state | auth_epoch: auth_epoch}
          {:reply, :ok, maybe_schedule_auto_save(state)}

        {:error, _reason} = err ->
          {:reply, err, state}
      end
    end
  end

  def handle_call({:del_user, "default"}, _from, state) do
    {:reply, {:error, "ERR The 'default' user cannot be removed"}, state}
  end

  def handle_call({:del_user, username}, _from, state) do
    table = Tables.active_table()

    case :ets.lookup(table, username) do
      [] ->
        {:reply, {:error, "ERR User '#{username}' does not exist"}, state}

      _ ->
        :ets.delete(table, username)
        :ok = Tables.remove_configured_user_witnesses([username])
        :ok = broadcast_acl_invalidation(username)
        state = %{state | auth_epoch: state.auth_epoch + 1}
        {:reply, :ok, maybe_schedule_auto_save(state)}
    end
  end

  def handle_call({:del_users, usernames}, _from, state) do
    case validate_del_users(usernames) do
      :ok ->
        table = Tables.active_table()

        usernames = Enum.uniq(usernames)
        Enum.each(usernames, &:ets.delete(table, &1))
        :ok = Tables.remove_configured_user_witnesses(usernames)
        Enum.each(usernames, &broadcast_acl_invalidation/1)

        state = %{state | auth_epoch: state.auth_epoch + length(usernames)}
        {:reply, :ok, maybe_schedule_auto_save(state)}

      {:error, _reason} = error ->
        {:reply, error, state}
    end
  end

  def handle_call(:reset, _from, state) do
    auth_epoch = state.auth_epoch + 1
    :ets.delete_all_objects(ensure_active_table())
    :ok = Tables.clear_configured_user_witness()
    Tables.insert_default_user(auth_epoch)
    :ok = broadcast_acl_invalidation(:all)
    {:reply, :ok, %{state | auth_epoch: auth_epoch}}
  end

  # --- ACL SAVE ---

  def handle_call(:acl_save, _from, state) do
    data_dir = Application.get_env(:ferricstore, :data_dir, "data")
    {:reply, Persistence.save(data_dir, state.auth_epoch), state}
  end

  def handle_call({:acl_save, data_dir}, _from, state) do
    {:reply, Persistence.save(data_dir, state.auth_epoch), state}
  end

  # --- ACL LOAD ---

  def handle_call(:acl_load, _from, state) do
    data_dir = Application.get_env(:ferricstore, :data_dir, "data")
    load_acl_file(data_dir, state)
  end

  def handle_call({:acl_load, data_dir}, _from, state) do
    load_acl_file(data_dir, state)
  end

  def handle_call({:acl_load_contents, contents}, _from, state) do
    case Persistence.load_contents(contents, state.auth_epoch) do
      {:ok, auth_epoch} ->
        :ok = broadcast_acl_invalidation(:all)
        {:reply, :ok, %{state | auth_epoch: auth_epoch}}

      {:error, _reason} = error ->
        {:reply, error, state}
    end
  end

  @impl true
  def handle_info(:auto_save, state) do
    data_dir = Application.get_env(:ferricstore, :data_dir, "data")

    case Persistence.save(data_dir, state.auth_epoch) do
      :ok -> :ok
      {:error, reason} -> Logger.warning("ACL auto-save failed: #{reason}")
    end

    {:noreply, %{state | save_timer: nil}}
  end

  def handle_info({:cleanup_acl_retired_table, table_name}, state) do
    if Tables.retired_table?(table_name) do
      Tables.cleanup_named_table(table_name)
    end

    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  # ---------------------------------------------------------------------------
  # Private -- password hashing (Fix 1)
  # ---------------------------------------------------------------------------

  @spec maybe_schedule_auto_save(map()) :: map()
  defp maybe_schedule_auto_save(state) do
    if Application.get_env(:ferricstore, :acl_auto_save, false) do
      if state.save_timer, do: Process.cancel_timer(state.save_timer)
      timer = Process.send_after(self(), :auto_save, @auto_save_debounce_ms)
      %{state | save_timer: timer}
    else
      state
    end
  end

  defp load_acl_file(data_dir, state) do
    case Persistence.load(data_dir, state.auth_epoch) do
      {:ok, auth_epoch} ->
        :ok = broadcast_acl_invalidation(:all)
        {:reply, :ok, %{state | auth_epoch: auth_epoch}}

      {:error, _reason} = error ->
        {:reply, error, state}
    end
  end

  defp broadcast_acl_invalidation(username),
    do: FerricstoreServer.Connection.Auth.broadcast_acl_invalidation(username)

  defp ensure_active_table do
    table = Tables.active_table()

    case :ets.info(table) do
      :undefined -> Tables.new_active_table()
      _info -> table
    end
  rescue
    ArgumentError -> Tables.new_active_table()
  end

  @spec validate_del_users([binary()]) :: :ok | {:error, binary()}
  defp validate_del_users(usernames) do
    table = Tables.active_table()

    Enum.reduce_while(usernames, :ok, fn
      "default", :ok ->
        {:halt, {:error, "ERR The 'default' user cannot be removed"}}

      username, :ok ->
        case :ets.lookup(table, username) do
          [] -> {:halt, {:error, "ERR User '#{username}' does not exist"}}
          _ -> {:cont, :ok}
        end
    end)
  end
end
