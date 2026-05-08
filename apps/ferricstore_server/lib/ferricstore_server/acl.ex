defmodule FerricstoreServer.Acl do
  @moduledoc """
  GenServer managing the Access Control List (ACL) for FerricStore.

  Stores user accounts in a named ETS table (`:ferricstore_acl`). Each user
  record has a username, enabled/disabled flag, an optional password hash,
  allowed commands, denied commands, and allowed key patterns.

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
        keys: :all | [key_pattern()]
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

  alias Ferricstore.AuditLog
  alias FerricstoreServer.Acl.CommandCategories

  @table :ferricstore_acl

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

  @typedoc "A user record stored in the ACL table."
  @type user :: %{
          enabled: boolean(),
          password: binary() | nil,
          commands: :all | MapSet.t(binary()),
          denied_commands: MapSet.t(binary()),
          keys: :all | [key_pattern()]
        }

  # ---------------------------------------------------------------------------
  # Constants -- file persistence
  # ---------------------------------------------------------------------------

  @acl_filename "acl.conf"
  @max_line_length 1_048_576
  @max_file_size 50_000_000
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
  Returns the user record for the given username, or `nil` if not found.

  ## Parameters

    - `username` -- the username to look up

  ## Examples

      FerricstoreServer.Acl.get_user("default")
      #=> %{enabled: true, password: nil, commands: :all, denied_commands: MapSet.new(), keys: :all}
  """
  @spec get_user(binary()) :: user() | nil
  def get_user(username) do
    case :ets.lookup(@table, username) do
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
    @table
    |> :ets.tab2list()
    |> Enum.sort_by(fn {name, _} -> name end)
    |> Enum.map(&format_user_rule/1)
  end

  @doc """
  Returns the user info for `ACL GETUSER` in Redis-compatible flat list format.

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
          if user.password, do: [hash_for_display(user.password)], else: []

        commands = format_commands(user.commands)
        keys = format_keys(user.keys)

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
          "&*"
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
        {:error, "WRONGPASS invalid username-password pair or user is disabled."}

      %{enabled: false} ->
        {:error, "WRONGPASS invalid username-password pair or user is disabled."}

      %{password: nil} ->
        {:ok, username}

      %{password: stored_hash} ->
        if verify_password(password, stored_hash) do
          {:ok, username}
        else
          {:error, "WRONGPASS invalid username-password pair or user is disabled."}
        end
    end
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
    cmd = String.upcase(command)

    case get_user(username) do
      nil ->
        {:error,
         "NOPERM this user has no permissions to run the '#{String.downcase(cmd)}' command"}

      %{enabled: false} ->
        {:error,
         "NOPERM this user has no permissions to run the '#{String.downcase(cmd)}' command"}

      %{commands: :all, denied_commands: denied} ->
        if MapSet.member?(denied, cmd) do
          {:error,
           "NOPERM this user has no permissions to run the '#{String.downcase(cmd)}' command"}
        else
          :ok
        end

      %{commands: cmds} ->
        if MapSet.member?(cmds, cmd) do
          :ok
        else
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
  def key_matches_any?(_key, _access_type, []), do: false

  def key_matches_any?(key, access_type, [{_glob, mode, regex} | rest]) do
    if access_permitted?(mode, access_type) and Regex.match?(regex, key) do
      true
    else
      key_matches_any?(key, access_type, rest)
    end
  end

  # Check if the pattern's access mode permits the requested access type.
  @spec access_permitted?(:rw | :read | :write, :read | :write) :: boolean()
  defp access_permitted?(:rw, _access_type), do: true
  defp access_permitted?(:read, :read), do: true
  defp access_permitted?(:write, :write), do: true
  defp access_permitted?(_, _), do: false

  @doc """
  Compiles a glob pattern string into a `Regex`.

  Supports:
    - `*` -- matches any sequence of characters
    - `?` -- matches any single character
    - `[abc]` -- matches any character in the set

  All other characters are escaped for literal matching.

  ## Examples

      FerricstoreServer.Acl.compile_glob("cache:*")
      #=> ~r/\\Acache:.*\\z/s

      FerricstoreServer.Acl.compile_glob("user:?:profile")
      #=> ~r/\\Auser:..:profile\\z/s
  """
  @spec compile_glob(binary()) :: Regex.t()
  def compile_glob(pattern) do
    regex_str =
      pattern
      |> String.graphemes()
      |> compile_glob_chars([])
      |> IO.iodata_to_binary()

    # The \\A and \\z anchors ensure full-string match. The 's' flag makes
    # '.' match newlines (though keys shouldn't contain them).
    case Regex.compile("\\A" <> regex_str <> "\\z", "s") do
      {:ok, regex} ->
        regex

      {:error, _} ->
        Regex.compile!("\\A" <> Regex.escape(pattern) <> "\\z", "s")
    end
  end

  defp compile_glob_chars([], acc), do: Enum.reverse(acc)
  defp compile_glob_chars(["*" | rest], acc), do: compile_glob_chars(rest, [".*" | acc])
  defp compile_glob_chars(["?" | rest], acc), do: compile_glob_chars(rest, ["." | acc])

  defp compile_glob_chars(["[" | rest], acc) do
    # Pass through character class: collect until ']'
    {class_chars, remaining} = collect_char_class(rest, [])
    compile_glob_chars(remaining, [["[", class_chars, "]"] | acc])
  end

  defp compile_glob_chars([ch | rest], acc) do
    compile_glob_chars(rest, [Regex.escape(ch) | acc])
  end

  defp collect_char_class([], acc), do: {Enum.reverse(acc), []}
  defp collect_char_class(["]" | rest], acc), do: {Enum.reverse(acc), rest}
  defp collect_char_class([ch | rest], acc), do: collect_char_class(rest, [ch | acc])

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
  def handle_raft_command({:acl_setuser, username, rules}) do
    set_user(username, rules)
  end

  def handle_raft_command({:acl_deluser, username}) do
    del_user(username)
  end

  def handle_raft_command({:acl_reset}) do
    reset!()
  end

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
  Returns the path to the ACL file for the given data directory.
  """
  @spec acl_file_path(binary()) :: binary()
  def acl_file_path(data_dir) do
    Path.join(data_dir, @acl_filename)
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
  def protected_mode? do
    Application.get_env(:ferricstore, :protected_mode, true)
  end

  @doc """
  Returns whether at least one non-default ACL user with a password exists
  and is enabled.

  Used by the protected mode check to determine whether the server has been
  configured with real authentication.

  ## Examples

      FerricstoreServer.Acl.has_configured_users?()
      #=> false
  """
  @spec has_configured_users?() :: boolean()
  def has_configured_users? do
    @table
    |> :ets.tab2list()
    |> Enum.any?(fn {name, user} ->
      name != "default" and user.password != nil and user.enabled
    end)
  end

  @doc """
  Returns whether the given peer address is a localhost address.

  Recognizes IPv4 `127.0.0.1` and IPv6 `::1`.

  ## Parameters

    - `peer` -- a `{ip_tuple, port}` tuple or `nil`

  ## Examples

      FerricstoreServer.Acl.localhost?({{127, 0, 0, 1}, 12345})
      #=> true

      FerricstoreServer.Acl.localhost?({{192, 168, 1, 1}, 12345})
      #=> false
  """
  @spec localhost?({:inet.ip_address(), :inet.port_number()} | nil) :: boolean()
  def localhost?(nil), do: false
  def localhost?({{127, 0, 0, 1}, _port}), do: true
  def localhost?({{0, 0, 0, 0, 0, 0, 0, 1}, _port}), do: true
  def localhost?(_peer), do: false

  @doc """
  Checks whether a connection from the given peer should be allowed under
  protected mode rules.

  Returns `:ok` if the connection is allowed, or `{:error, message}` if the
  connection should be rejected.

  A connection is rejected when ALL of:
    1. Protected mode is active
    2. No non-default ACL user with a password exists
    3. The connection is not from localhost

  ## Parameters

    - `peer` -- a `{ip_tuple, port}` tuple or `nil`

  ## Examples

      FerricstoreServer.Acl.check_protected_mode({{127, 0, 0, 1}, 12345})
      #=> :ok

      FerricstoreServer.Acl.check_protected_mode({{192, 168, 1, 1}, 12345})
      #=> {:error, "DENIED FerricStore is in protected mode..."}
  """
  @spec check_protected_mode({:inet.ip_address(), :inet.port_number()} | nil) ::
          :ok | {:error, binary()}
  def check_protected_mode(peer) do
    if protected_mode?() and not has_configured_users?() and not localhost?(peer) do
      {:error,
       "DENIED FerricStore is in protected mode. " <>
         "Configure ACL users or set protected-mode false."}
    else
      :ok
    end
  end

  # ---------------------------------------------------------------------------
  # ACL LOG denial helper (Fix 5)
  # ---------------------------------------------------------------------------

  @doc """
  Logs a command denial to the audit log.

  Called by the connection handler when `check_command/2` returns an error.
  This is a convenience wrapper around `AuditLog.log/2` that uses the
  `:command_denied` event type.

  ## Parameters

    - `username`  -- the username that was denied
    - `command`   -- the command that was denied
    - `client_ip` -- formatted client IP string
    - `client_id` -- the connection's client ID

  ## Examples

      FerricstoreServer.Acl.log_command_denied("alice", "SET", "127.0.0.1:1234", 42)
  """
  @spec log_command_denied(binary(), binary(), binary(), term()) :: :ok
  def log_command_denied(username, command, client_ip, client_id) do
    AuditLog.log(:command_denied, %{
      username: username,
      command: command,
      client_ip: client_ip,
      client_id: client_id
    })
  end

  # ---------------------------------------------------------------------------
  # GenServer callbacks
  # ---------------------------------------------------------------------------

  @impl true
  def init(_opts) do
    table = :ets.new(@table, [:set, :public, :named_table])
    insert_default_user()

    # Auto-load from file on startup (design doc section 7 startup sequence)
    data_dir = Application.get_env(:ferricstore, :data_dir, "data")
    state = %{table: table, save_timer: nil}

    state =
      case auto_load_from_file(data_dir) do
        :ok ->
          Logger.info("ACL loaded from #{acl_file_path(data_dir)}")
          state

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

    # Fix 4: max_acl_users -- check limit before creating a new user.
    max = Application.get_env(:ferricstore, :max_acl_users, 10_000)

    if existing == nil and :ets.info(@table, :size) >= max do
      {:reply, {:error, "ERR max ACL users reached (#{max})"}, state}
    else
      base =
        if existing do
          existing
        else
          %{
            enabled: false,
            password: nil,
            commands: :all,
            denied_commands: MapSet.new(),
            keys: :all
          }
        end

      case apply_rules(base, rules) do
        {:ok, updated} ->
          :ets.insert(@table, {username, updated})
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
    case :ets.lookup(@table, username) do
      [] ->
        {:reply, {:error, "ERR User '#{username}' does not exist"}, state}

      _ ->
        :ets.delete(@table, username)
        {:reply, :ok, maybe_schedule_auto_save(state)}
    end
  end

  def handle_call(:reset, _from, state) do
    :ets.delete_all_objects(@table)
    insert_default_user()
    {:reply, :ok, state}
  end

  # --- ACL SAVE ---

  def handle_call(:acl_save, _from, state) do
    data_dir = Application.get_env(:ferricstore, :data_dir, "data")
    {:reply, do_save(data_dir), state}
  end

  def handle_call({:acl_save, data_dir}, _from, state) do
    {:reply, do_save(data_dir), state}
  end

  # --- ACL LOAD ---

  def handle_call(:acl_load, _from, state) do
    data_dir = Application.get_env(:ferricstore, :data_dir, "data")
    {:reply, do_load(data_dir), state}
  end

  def handle_call({:acl_load, data_dir}, _from, state) do
    {:reply, do_load(data_dir), state}
  end

  @impl true
  def handle_info(:auto_save, state) do
    data_dir = Application.get_env(:ferricstore, :data_dir, "data")

    case do_save(data_dir) do
      :ok -> :ok
      {:error, reason} -> Logger.warning("ACL auto-save failed: #{reason}")
    end

    {:noreply, %{state | save_timer: nil}}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  # ---------------------------------------------------------------------------
  # Private -- password hashing (Fix 1)
  # ---------------------------------------------------------------------------

  @spec hash_password(binary()) :: binary()
  # PBKDF2-SHA256 with 100K iterations. Makes brute-force of leaked acl.conf
  # impractical: ~100ms per guess vs nanoseconds with plain SHA-256.
  # Salt is 16 random bytes, unique per password.
  @pbkdf2_iterations 100_000
  @pbkdf2_key_length 32

  defp hash_password(password) do
    salt = :crypto.strong_rand_bytes(16)
    hash = :crypto.pbkdf2_hmac(:sha256, password, salt, @pbkdf2_iterations, @pbkdf2_key_length)
    Base.encode64(salt <> hash)
  end

  # Verifies a plaintext password against a stored PBKDF2 hash.
  # Extracts the 16-byte salt, recomputes PBKDF2, constant-time compares.
  # Also accepts legacy SHA-256 hashes (32 bytes after salt) for backward
  # compatibility with acl.conf files created before the PBKDF2 upgrade.
  @spec verify_password(binary(), binary()) :: boolean()
  defp verify_password(password, stored_hash) do
    case Base.decode64(stored_hash) do
      {:ok, <<salt::binary-16, hash::binary-32>>} ->
        # Try PBKDF2 first (new format)
        pbkdf2 =
          :crypto.pbkdf2_hmac(:sha256, password, salt, @pbkdf2_iterations, @pbkdf2_key_length)

        if :crypto.hash_equals(pbkdf2, hash) do
          true
        else
          # Fallback: legacy SHA-256 (for old acl.conf files)
          legacy = :crypto.hash(:sha256, salt <> password)
          :crypto.hash_equals(legacy, hash)
        end

      _ ->
        false
    end
  end

  # ---------------------------------------------------------------------------
  # Private -- rule parsing
  # ---------------------------------------------------------------------------

  @spec apply_rules(user(), [binary()]) :: {:ok, user()} | {:error, binary()}
  defp apply_rules(user, rules), do: apply_rules(user, rules, [])

  defp apply_rules(user, [], pending_key_patterns),
    do: {:ok, flush_pending_key_patterns(user, pending_key_patterns)}

  defp apply_rules(user, [rule | rest], pending_key_patterns) do
    case parse_key_rule(user, rule, pending_key_patterns) do
      {:ok, updated, next_pending} ->
        apply_rules(updated, rest, next_pending)

      :not_key_rule ->
        case parse_rule(user, rule) do
          {:ok, updated} -> apply_rules(updated, rest, pending_key_patterns)
          {:error, _} = err -> err
        end
    end
  end

  @spec parse_rule(user(), binary()) :: {:ok, user()} | {:error, binary()}
  defp parse_rule(user, "on"), do: {:ok, %{user | enabled: true}}
  defp parse_rule(user, "off"), do: {:ok, %{user | enabled: false}}

  # Fix 1: Hash the password before storing.
  defp parse_rule(user, ">" <> password) do
    {:ok, %{user | password: hash_password(password)}}
  end

  defp parse_rule(user, "nopass"), do: {:ok, %{user | password: nil}}
  defp parse_rule(user, "resetpass"), do: {:ok, %{user | password: nil}}

  defp parse_rule(user, "allcommands") do
    {:ok, %{user | commands: :all, denied_commands: MapSet.new()}}
  end

  defp parse_rule(user, "+@all") do
    {:ok, %{user | commands: :all, denied_commands: MapSet.new()}}
  end

  defp parse_rule(user, "-@all"),
    do: {:ok, %{user | commands: MapSet.new(), denied_commands: MapSet.new()}}

  # +@category -- expand the category to individual commands and add them all.
  # When commands is :all, also remove the category commands from denied_commands.
  defp parse_rule(user, "+@" <> category) do
    cat = String.upcase(category)

    case CommandCategories.category_commands(cat) do
      {:ok, cat_cmds} ->
        case user.commands do
          :all ->
            # Remove from denied_commands if they were explicitly denied
            new_denied = MapSet.difference(user.denied_commands, cat_cmds)
            {:ok, %{user | denied_commands: new_denied}}

          cmds ->
            {:ok, %{user | commands: MapSet.union(cmds, cat_cmds)}}
        end

      :error ->
        {:error,
         "ERR Error in ACL SETUSER modifier '+@#{category}': Unknown command category '#{category}'"}
    end
  end

  # -@category -- deny all commands in the category.
  # Fix 2: When commands is :all, add to denied_commands instead of ignoring.
  defp parse_rule(user, "-@" <> category) do
    cat = String.upcase(category)

    case CommandCategories.category_commands(cat) do
      {:ok, cat_cmds} ->
        case user.commands do
          :all ->
            new_denied = MapSet.union(user.denied_commands, cat_cmds)
            {:ok, %{user | denied_commands: new_denied}}

          cmds ->
            {:ok, %{user | commands: MapSet.difference(cmds, cat_cmds)}}
        end

      :error ->
        {:error,
         "ERR Error in ACL SETUSER modifier '-@#{category}': Unknown command category '#{category}'"}
    end
  end

  # +command -- allow a specific command.
  # When commands is :all, remove from denied_commands if present.
  defp parse_rule(user, "+" <> command) do
    cmd = String.upcase(command)

    case user.commands do
      :all ->
        new_denied = MapSet.delete(user.denied_commands, cmd)
        {:ok, %{user | denied_commands: new_denied}}

      cmds ->
        {:ok, %{user | commands: MapSet.put(cmds, cmd)}}
    end
  end

  # -command -- deny a specific command.
  # Fix 2: When commands is :all, add to denied_commands instead of ignoring.
  defp parse_rule(user, "-" <> command) do
    cmd = String.upcase(command)

    case user.commands do
      :all ->
        new_denied = MapSet.put(user.denied_commands, cmd)
        {:ok, %{user | denied_commands: new_denied}}

      cmds ->
        {:ok, %{user | commands: MapSet.delete(cmds, cmd)}}
    end
  end

  defp parse_rule(_user, rule) do
    {:error, "ERR Error in ACL SETUSER modifier '#{rule}': Syntax error"}
  end

  defp parse_key_rule(user, "%R~" <> pattern, pending_key_patterns) do
    queue_key_pattern(user, {pattern, :read, compile_glob(pattern)}, pending_key_patterns)
  end

  defp parse_key_rule(user, "%W~" <> pattern, pending_key_patterns) do
    queue_key_pattern(user, {pattern, :write, compile_glob(pattern)}, pending_key_patterns)
  end

  defp parse_key_rule(user, "~" <> pattern, pending_key_patterns) do
    queue_key_pattern(user, {pattern, :rw, compile_glob(pattern)}, pending_key_patterns)
  end

  defp parse_key_rule(user, "resetkeys", _pending_key_patterns),
    do: {:ok, %{user | keys: []}, []}

  defp parse_key_rule(user, "allkeys", _pending_key_patterns),
    do: {:ok, %{user | keys: :all}, []}

  defp parse_key_rule(_user, _rule, _pending_key_patterns), do: :not_key_rule

  defp queue_key_pattern(%{keys: :all} = user, compiled_pattern, pending_key_patterns) do
    {:ok, %{user | keys: []}, [compiled_pattern | pending_key_patterns]}
  end

  defp queue_key_pattern(user, compiled_pattern, pending_key_patterns) do
    {:ok, user, [compiled_pattern | pending_key_patterns]}
  end

  defp flush_pending_key_patterns(user, []), do: user

  defp flush_pending_key_patterns(%{keys: []} = user, pending_key_patterns) do
    %{user | keys: Enum.reverse(pending_key_patterns)}
  end

  defp flush_pending_key_patterns(user, pending_key_patterns) do
    %{user | keys: user.keys ++ Enum.reverse(pending_key_patterns)}
  end

  # ---------------------------------------------------------------------------
  # Private -- formatting (for ACL LIST display)
  # ---------------------------------------------------------------------------

  @spec format_user_rule({binary(), user()}) :: binary()
  defp format_user_rule({name, user}) do
    flag = if user.enabled, do: "on", else: "off"
    keys = format_keys(user.keys)
    cmds = format_commands(user.commands)
    "user #{name} #{flag} #{keys} &* #{cmds}"
  end

  @spec format_commands(:all | MapSet.t(binary())) :: binary()
  defp format_commands(:all), do: "+@all"

  defp format_commands(cmds) when is_struct(cmds, MapSet) do
    cmds
    |> MapSet.to_list()
    |> Enum.sort()
    |> Enum.map_join(" ", &"+#{String.downcase(&1)}")
  end

  @spec format_keys(:all | [key_pattern()]) :: binary()
  defp format_keys(:all), do: "~*"

  defp format_keys(patterns) when is_list(patterns) do
    Enum.map_join(patterns, " ", fn
      {glob, :rw, _regex} -> "~#{glob}"
      {glob, :read, _regex} -> "%R~#{glob}"
      {glob, :write, _regex} -> "%W~#{glob}"
    end)
  end

  # Displays the stored password hash as a SHA-256 hex digest (for ACL GETUSER).
  # The stored value is already a base64-encoded salt+hash, so we hash that
  # representation to produce a stable display value.
  @spec hash_for_display(binary()) :: binary()
  defp hash_for_display(stored_hash) do
    :crypto.hash(:sha256, stored_hash) |> Base.encode16(case: :lower)
  end

  # ---------------------------------------------------------------------------
  # Private -- default user
  # ---------------------------------------------------------------------------

  @spec insert_default_user() :: true
  defp insert_default_user do
    :ets.insert(
      @table,
      {"default",
       %{
         enabled: true,
         password: nil,
         commands: :all,
         denied_commands: MapSet.new(),
         keys: :all
       }}
    )
  end

  # ---------------------------------------------------------------------------
  # Private -- file persistence (ACL SAVE / LOAD)
  # ---------------------------------------------------------------------------

  # Performs the actual save to disk. Called inside GenServer to serialize
  # concurrent SAVE requests.
  #
  # Security: atomic write (tmp + fsync + rename), 0600 permissions,
  # path traversal validation, never writes plaintext passwords.
  @spec do_save(binary()) :: :ok | {:error, binary()}
  defp do_save(data_dir) do
    path = acl_file_path(data_dir)
    tmp = path <> ".tmp.#{System.unique_integer([:positive])}"

    case validate_acl_path(path, data_dir) do
      :ok -> :ok
      {:error, _} = err -> throw(err)
    end

    header =
      "# FerricStore ACL configuration\n" <>
        "# Generated by ACL SAVE at #{DateTime.utc_now() |> DateTime.to_iso8601()}\n\n"

    lines =
      @table
      |> :ets.tab2list()
      |> Enum.sort_by(fn {name, _} -> name end)
      |> Enum.map_join("\n", &format_user_for_file/1)

    contents = header <> lines <> "\n"

    with :ok <- Ferricstore.FS.mkdir_p(data_dir),
         :ok <- File.write(tmp, contents),
         :ok <- File.chmod(tmp, 0o600),
         :ok <- fsync_file(tmp),
         :ok <- Ferricstore.FS.rename(tmp, path),
         _ <- Ferricstore.Bitcask.NIF.v2_fsync_dir(data_dir) do
      :ok
    else
      {:error, reason} ->
        _ = Ferricstore.FS.rm(tmp)
        {:error, "ERR ACL save failed: #{inspect(reason)}"}
    end
  catch
    {:error, _} = err -> err
  end

  # Performs the actual load from disk. Called inside GenServer to serialize
  # concurrent LOAD requests and prevent race conditions with in-flight
  # commands. All-or-nothing: current state is preserved on any error.
  @spec do_load(binary()) :: :ok | {:error, binary()}
  defp do_load(data_dir) do
    path = acl_file_path(data_dir)

    case validate_acl_path(path, data_dir) do
      :ok -> :ok
      {:error, _} = err -> throw(err)
    end

    case File.read(path) do
      {:ok, contents} ->
        if byte_size(contents) > @max_file_size do
          {:error, "ERR ACL file too large (#{byte_size(contents)} bytes, max #{@max_file_size})"}
        else
          case parse_acl_file(contents) do
            {:ok, users} ->
              unless Enum.any?(users, fn {name, _} -> name == "default" end) do
                throw({:error, "ERR ACL file must contain a 'default' user definition"})
              end

              # All-or-nothing: only replace ETS if everything validated
              :ets.delete_all_objects(@table)

              Enum.each(users, fn {name, user_map} ->
                :ets.insert(@table, {name, user_map})
              end)

              :ok

            {:error, _} = err ->
              err
          end
        end

      {:error, :enoent} ->
        {:error, "ERR There is no ACL file to load"}

      {:error, :eacces} ->
        {:error, "ERR Permission denied reading ACL file"}

      {:error, reason} ->
        {:error, "ERR Cannot read ACL file: #{inspect(reason)}"}
    end
  catch
    {:error, _} = err -> err
  end

  # Auto-load from file on startup. Returns :ok, {:error, :enoent}, or
  # {:error, reason}. Called directly from init (not through GenServer call).
  @spec auto_load_from_file(binary()) :: :ok | {:error, :enoent} | {:error, binary()}
  defp auto_load_from_file(data_dir) do
    path = acl_file_path(data_dir)

    case File.read(path) do
      {:ok, contents} ->
        if byte_size(contents) > @max_file_size do
          {:error, "ACL file too large"}
        else
          case parse_acl_file(contents) do
            {:ok, users} ->
              if Enum.any?(users, fn {name, _} -> name == "default" end) do
                :ets.delete_all_objects(@table)

                Enum.each(users, fn {name, user_map} ->
                  :ets.insert(@table, {name, user_map})
                end)

                :ok
              else
                {:error, "ACL file missing 'default' user"}
              end

            {:error, reason} ->
              {:error, reason}
          end
        end

      {:error, :enoent} ->
        {:error, :enoent}

      {:error, reason} ->
        {:error, "Cannot read ACL file: #{inspect(reason)}"}
    end
  end

  # Schedules a debounced auto-save if :acl_auto_save is enabled.
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

  # Validates that the ACL file path stays within the data directory.
  # Prevents path traversal (CVE-2023-45145 pattern).
  @spec validate_acl_path(binary(), binary()) :: :ok | {:error, binary()}
  defp validate_acl_path(path, data_dir) do
    abs_path = Path.expand(path)
    abs_dir = Path.expand(data_dir)

    cond do
      not (String.starts_with?(abs_path, abs_dir <> "/") or abs_path == abs_dir) ->
        {:error, "ERR ACL file path escapes data directory"}

      symlink?(path) ->
        {:error, "ERR ACL file path is a symlink, refusing for security"}

      true ->
        :ok
    end
  end

  # Checks if a path is a symlink. Returns false if the path does not exist.
  @spec symlink?(binary()) :: boolean()
  defp symlink?(path) do
    case File.lstat(path) do
      {:ok, %{type: :symlink}} -> true
      _ -> false
    end
  end

  # Fsyncs a file to ensure data is flushed to disk before rename.
  @spec fsync_file(binary()) :: :ok | {:error, term()}
  defp fsync_file(path) do
    case :file.open(String.to_charlist(path), [:read, :write]) do
      {:ok, fd} ->
        result = :file.sync(fd)
        :file.close(fd)
        result

      {:error, reason} ->
        {:error, reason}
    end
  end

  # ---------------------------------------------------------------------------
  # Private -- ACL file serialization (SAVE)
  # ---------------------------------------------------------------------------

  # Formats a single user for file output. Password hashes use # prefix.
  # Denied commands are serialized as -command after +@all.
  @spec format_user_for_file({binary(), user()}) :: binary()
  defp format_user_for_file({name, user}) do
    parts = ["user", name]
    parts = parts ++ [if(user.enabled, do: "on", else: "off")]

    # Password: nopass or #<hash> (never plaintext)
    parts =
      case user.password do
        nil -> parts ++ ["nopass"]
        hash -> parts ++ ["#" <> hash]
      end

    # Key patterns with access mode prefixes
    parts =
      case user.keys do
        :all ->
          parts ++ ["~*"]

        patterns ->
          parts ++
            Enum.map(patterns, fn
              {glob, :rw, _regex} -> "~#{glob}"
              {glob, :read, _regex} -> "%R~#{glob}"
              {glob, :write, _regex} -> "%W~#{glob}"
            end)
      end

    # Channels (always &* for now)
    parts = parts ++ ["&*"]

    # Commands and denied commands
    parts =
      case user.commands do
        :all ->
          denied = user.denied_commands

          if MapSet.size(denied) == 0 do
            parts ++ ["+@all"]
          else
            parts ++
              ["+@all"] ++
              (denied
               |> MapSet.to_list()
               |> Enum.sort()
               |> Enum.map(&"-#{String.downcase(&1)}"))
          end

        cmds ->
          if MapSet.size(cmds) == 0 do
            parts
          else
            parts ++
              (cmds
               |> MapSet.to_list()
               |> Enum.sort()
               |> Enum.map(&"+#{String.downcase(&1)}"))
          end
      end

    Enum.join(parts, " ")
  end

  # ---------------------------------------------------------------------------
  # Private -- ACL file parsing (LOAD)
  # ---------------------------------------------------------------------------

  # Parses an ACL file's contents into a list of {username, user_map} tuples.
  # All-or-nothing: returns {:ok, users} or {:error, reason} with line number.
  # Duplicate usernames: last definition wins (matches Redis behavior).
  @spec parse_acl_file(binary()) :: {:ok, [{binary(), user()}]} | {:error, binary()}
  defp parse_acl_file(contents) do
    contents = strip_bom(contents)

    lines =
      contents
      |> String.split(~r/\r?\n/)
      |> Enum.with_index(1)

    result =
      Enum.reduce_while(lines, {:ok, %{}}, fn {line, line_num}, {:ok, acc} ->
        line = String.trim_trailing(line)

        cond do
          line == "" ->
            {:cont, {:ok, acc}}

          String.starts_with?(line, "#") ->
            {:cont, {:ok, acc}}

          byte_size(line) > @max_line_length ->
            {:halt, {:error, "ERR Invalid ACL line #{line_num}: line exceeds maximum length"}}

          true ->
            case parse_acl_line(line, line_num) do
              {:ok, {username, user_map}} ->
                {:cont, {:ok, Map.put(acc, username, user_map)}}

              {:error, _} = err ->
                {:halt, err}
            end
        end
      end)

    case result do
      {:ok, users_map} -> {:ok, Enum.to_list(users_map)}
      {:error, _} = err -> err
    end
  end

  # Parses a single "user <name> <rules...>" line.
  @spec parse_acl_line(binary(), pos_integer()) ::
          {:ok, {binary(), user()}} | {:error, binary()}
  defp parse_acl_line(line, line_num) do
    tokens = String.split(line, ~r/\s+/, trim: true)

    case tokens do
      ["user", username | rule_tokens] ->
        # Security: reject plaintext passwords in file (>password)
        if Enum.any?(rule_tokens, &String.starts_with?(&1, ">")) do
          {:error,
           "ERR Invalid ACL line #{line_num}: plaintext passwords (>) are not allowed in ACL files, use #<hash>"}
        else
          parse_file_rules(username, rule_tokens, line_num)
        end

      _ ->
        {:error, "ERR Invalid ACL line #{line_num}: expected 'user <username> <rules...>'"}
    end
  end

  # Parses file-format rules into a user map.
  @spec parse_file_rules(binary(), [binary()], pos_integer()) ::
          {:ok, {binary(), user()}} | {:error, binary()}
  defp parse_file_rules(username, tokens, line_num) do
    base = %{
      enabled: false,
      password: nil,
      commands: MapSet.new(),
      denied_commands: MapSet.new(),
      keys: []
    }

    result = parse_file_rules(tokens, line_num, base, [])

    case result do
      {:ok, user_map} -> {:ok, {username, user_map}}
      {:error, _} = err -> err
    end
  end

  defp parse_file_rules([], _line_num, user, pending_key_patterns) do
    {:ok, flush_pending_key_patterns(user, pending_key_patterns)}
  end

  defp parse_file_rules([token | rest], line_num, user, pending_key_patterns) do
    case parse_file_key_token(user, token, pending_key_patterns) do
      {:ok, updated, next_pending} ->
        parse_file_rules(rest, line_num, updated, next_pending)

      :not_key_token ->
        case parse_file_token(user, token) do
          {:ok, updated} ->
            parse_file_rules(rest, line_num, updated, pending_key_patterns)

          {:error, reason} ->
            {:error, "ERR Invalid ACL line #{line_num}: #{reason}"}
        end
    end
  end

  # Token parsers for ACL file format. Similar to parse_rule/2 but uses
  # #<hash> for passwords instead of ><plaintext>.
  @spec parse_file_token(user(), binary()) :: {:ok, user()} | {:error, binary()}
  defp parse_file_token(user, "on"), do: {:ok, %{user | enabled: true}}
  defp parse_file_token(user, "off"), do: {:ok, %{user | enabled: false}}
  defp parse_file_token(user, "nopass"), do: {:ok, %{user | password: nil}}
  defp parse_file_token(user, "resetpass"), do: {:ok, %{user | password: nil}}

  # Pre-hashed password from file (#<base64_hash>)
  defp parse_file_token(user, "#" <> hash) do
    case Base.decode64(hash) do
      {:ok, decoded} when byte_size(decoded) == 48 ->
        {:ok, %{user | password: hash}}

      {:ok, _} ->
        {:error, "invalid password hash length"}

      :error ->
        {:error, "invalid password hash encoding"}
    end
  end

  # Channel patterns (accepted but not enforced beyond &*)
  defp parse_file_token(user, "&" <> _pattern), do: {:ok, user}

  defp parse_file_token(user, "allcommands") do
    {:ok, %{user | commands: :all, denied_commands: MapSet.new()}}
  end

  defp parse_file_token(user, "nocommands") do
    {:ok, %{user | commands: MapSet.new(), denied_commands: MapSet.new()}}
  end

  defp parse_file_token(user, "+@all") do
    {:ok, %{user | commands: :all, denied_commands: MapSet.new()}}
  end

  defp parse_file_token(user, "-@all") do
    {:ok, %{user | commands: MapSet.new(), denied_commands: MapSet.new()}}
  end

  defp parse_file_token(user, "+@" <> category) do
    cat = String.upcase(category)

    case CommandCategories.category_commands(cat) do
      {:ok, cat_cmds} ->
        case user.commands do
          :all ->
            new_denied = MapSet.difference(user.denied_commands, cat_cmds)
            {:ok, %{user | denied_commands: new_denied}}

          cmds ->
            {:ok, %{user | commands: MapSet.union(cmds, cat_cmds)}}
        end

      :error ->
        {:error, "unknown command category '@#{category}'"}
    end
  end

  defp parse_file_token(user, "-@" <> category) do
    cat = String.upcase(category)

    case CommandCategories.category_commands(cat) do
      {:ok, cat_cmds} ->
        case user.commands do
          :all ->
            new_denied = MapSet.union(user.denied_commands, cat_cmds)
            {:ok, %{user | denied_commands: new_denied}}

          cmds ->
            {:ok, %{user | commands: MapSet.difference(cmds, cat_cmds)}}
        end

      :error ->
        {:error, "unknown command category '@#{category}'"}
    end
  end

  defp parse_file_token(user, "+" <> command) do
    cmd = String.upcase(command)

    case user.commands do
      :all ->
        new_denied = MapSet.delete(user.denied_commands, cmd)
        {:ok, %{user | denied_commands: new_denied}}

      cmds ->
        {:ok, %{user | commands: MapSet.put(cmds, cmd)}}
    end
  end

  defp parse_file_token(user, "-" <> command) do
    cmd = String.upcase(command)

    case user.commands do
      :all ->
        new_denied = MapSet.put(user.denied_commands, cmd)
        {:ok, %{user | denied_commands: new_denied}}

      cmds ->
        {:ok, %{user | commands: MapSet.delete(cmds, cmd)}}
    end
  end

  defp parse_file_token(_user, token) do
    {:error, "unknown token '#{token}'"}
  end

  defp parse_file_key_token(user, "~*", _pending_key_patterns),
    do: {:ok, %{user | keys: :all}, []}

  defp parse_file_key_token(user, "allkeys", _pending_key_patterns),
    do: {:ok, %{user | keys: :all}, []}

  defp parse_file_key_token(user, "resetkeys", _pending_key_patterns),
    do: {:ok, %{user | keys: []}, []}

  defp parse_file_key_token(user, "%R~" <> pattern, pending_key_patterns) do
    queue_file_key_pattern(user, {pattern, :read, compile_glob(pattern)}, pending_key_patterns)
  end

  defp parse_file_key_token(user, "%W~" <> pattern, pending_key_patterns) do
    queue_file_key_pattern(user, {pattern, :write, compile_glob(pattern)}, pending_key_patterns)
  end

  defp parse_file_key_token(user, "~" <> pattern, pending_key_patterns) do
    queue_file_key_pattern(user, {pattern, :rw, compile_glob(pattern)}, pending_key_patterns)
  end

  defp parse_file_key_token(_user, _token, _pending_key_patterns), do: :not_key_token

  defp queue_file_key_pattern(user, compiled_pattern, pending_key_patterns) do
    case user.keys do
      :all -> {:ok, user, pending_key_patterns}
      _patterns -> {:ok, user, [compiled_pattern | pending_key_patterns]}
    end
  end

  # Strips UTF-8 BOM from the beginning of a string.
  @spec strip_bom(binary()) :: binary()
  defp strip_bom(<<0xEF, 0xBB, 0xBF, rest::binary>>), do: rest
  defp strip_bom(contents), do: contents
end
