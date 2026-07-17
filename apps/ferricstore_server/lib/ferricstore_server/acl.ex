defmodule FerricstoreServer.Acl do
  @moduledoc """
  GenServer managing the Access Control List (ACL) for FerricStore.

  Stores user accounts in an atomically swappable ETS generation. Each user
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
  - **Explicit import** -- file state is loaded only when requested; the replicated catalog is
    authoritative at startup
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

  Initialises the "default" user with full access. The catalog projector
  replaces this bootstrap state from the replicated ACL catalog before
  network listeners start.
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
    with {:ok, store} <- default_store() do
      FerricstoreServer.Management.ACL.set_user(username, rules, store: store)
    end
  end

  @doc false
  @spec new_user() :: map()
  def new_user do
    %{
      enabled: false,
      password: nil,
      commands: MapSet.new(),
      denied_commands: MapSet.new(),
      keys: [],
      channels: []
    }
  end

  @doc false
  @spec default_user() :: map()
  def default_user do
    %{
      enabled: true,
      password: nil,
      commands: :all,
      denied_commands: MapSet.new(),
      keys: :all,
      channels: :all
    }
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
    with {:ok, store} <- default_store() do
      case FerricstoreServer.Management.ACL.del_user(username, store: store) do
        {:ok, 1} -> :ok
        result -> result
      end
    end
  end

  @doc """
  Deletes multiple users atomically.

  The full list is validated before any ETS row is removed, so a protected or
  missing user cannot leave the ACL partially mutated.
  """
  @spec del_users([binary()]) :: :ok | {:error, binary()}
  def del_users(usernames) when is_list(usernames) do
    with {:ok, store} <- default_store() do
      case FerricstoreServer.Management.ACL.del_users(usernames, store: store) do
        {:ok, _count} -> :ok
        result -> result
      end
    end
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
    case Tables.read(fn table -> :ets.lookup(table, username) end) do
      [{^username, user}] -> user
      [] -> nil
    end
  end

  @doc false
  @spec user_count() :: non_neg_integer()
  def user_count, do: Tables.read(fn table -> :ets.info(table, :size) end)

  @doc """
  Returns a list of all users in Redis ACL LIST format.

  Each entry is a string like `"user default on ~* &* +@all"`.

  ## Examples

      FerricstoreServer.Acl.list_users()
      #=> ["user default on ~* &* +@all", "user alice on ~cache:* +get +set"]
  """
  @spec list_users() :: [binary()]
  def list_users do
    Tables.read(&:ets.tab2list/1)
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
    if FerricstoreServer.Acl.CatalogProjector.ready?() do
      do_authenticate(username, password)
    else
      verify_dummy_password(password, &Password.verify/2)
      projection_authentication_error()
    end
  end

  defp do_authenticate(username, password) do
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
    if FerricstoreServer.Acl.CatalogProjector.ready?() do
      do_authenticate(username, password, verifier)
    else
      verify_dummy_password(password, verifier)
      projection_authentication_error()
    end
  end

  defp do_authenticate(username, password, verifier) do
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
      Tables.read(fn table ->
        case :ets.lookup(table, username) do
          [{^username, %{password: ^stored_hash} = user}] ->
            updated = %{user | password: Password.hash(password)}
            :ets.insert(table, {username, updated})
            Tables.update_configured_user_witness(username, updated)

          _stale_or_missing ->
            :ok
        end
      end)
    end

    :ok
  end

  defp authentication_error do
    {:error, "WRONGPASS invalid username-password pair or user is disabled."}
  end

  defp projection_authentication_error do
    {:error, "LOADING ACL catalog projection unavailable"}
  end

  defp ensure_projection_access_ready do
    if FerricstoreServer.Acl.CatalogProjector.ready?() do
      :ok
    else
      {:error, "NOPERM ACL catalog projection unavailable"}
    end
  end

  @doc """
  Checks whether the user exists and is enabled. Use `check_command/2` when a
  command-specific permission check is required.

  ## Parameters

    - `username` -- the username
    - `_command` -- the command name (currently unused)

  ## Returns

    - `:ok` if the user is allowed
    - `{:error, reason}` if denied
  """
  @spec check_permission(binary(), binary()) :: :ok | {:error, binary()}
  def check_permission(username, _command) do
    with :ok <- ensure_projection_access_ready() do
      case get_user(username) do
        nil ->
          {:error, "NOPERM user '#{username}' does not exist"}

        %{enabled: false} ->
          {:error, "NOPERM user '#{username}' is disabled"}

        _ ->
          :ok
      end
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

    with :ok <- ensure_projection_access_ready() do
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
    with :ok <- ensure_projection_access_ready() do
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
    with {:ok, store} <- default_store() do
      FerricstoreServer.Management.ACL.replace_users([{"default", default_user()}], store: store)
    end
  end

  @doc false
  @spec reset_projection!() :: :ok
  def reset_projection!, do: GenServer.call(__MODULE__, :reset)

  # ---------------------------------------------------------------------------
  # Raft replication hook
  # ---------------------------------------------------------------------------

  @doc false
  @spec handle_raft_command(term()) :: term()
  def handle_raft_command({"acl", username, encoded})
      when is_binary(username) and is_binary(encoded) do
    project_catalog_entry(username, encoded)
  end

  def handle_raft_command(_unknown), do: {:error, :unknown_acl_command}

  @doc false
  @spec project_catalog_entry(binary(), binary()) :: :ok | {:error, atom()}
  def project_catalog_entry(username, encoded) do
    do_project_catalog_entry(username, encoded, :unknown)
  end

  @doc false
  @spec project_catalog_entry(binary(), binary(), binary() | nil) :: :ok | {:error, atom()}
  def project_catalog_entry(username, encoded, expected_revision)
      when is_binary(username) and is_binary(encoded) and
             (is_binary(expected_revision) or is_nil(expected_revision)) do
    with {:ok, previous_revision} <- decode_projection_revision(expected_revision) do
      do_project_catalog_entry(username, encoded, previous_revision)
    end
  end

  defp do_project_catalog_entry(username, encoded, previous_revision)
       when is_binary(username) and is_binary(encoded) and
              (previous_revision == :unknown or
                 (is_integer(previous_revision) and previous_revision >= -1)) do
    with :ok <- validate_projected_username(username),
         {:ok, %{version: version, value: value}} <-
           Ferricstore.ServerCatalog.decode_entry(encoded) do
      case value do
        :deleted ->
          GenServer.call(
            __MODULE__,
            {:project_catalog_delete, username, version, previous_revision}
          )

        value when is_binary(value) ->
          with {:ok, user} <- FerricstoreServer.Management.ACL.decode_catalog_value(value) do
            GenServer.call(
              __MODULE__,
              {:project_catalog_user, username, user, version, previous_revision}
            )
          end

        _invalid ->
          {:error, :invalid_acl_catalog_value}
      end
    end
  end

  defp validate_projected_username(username) do
    case Rules.validate_username(username) do
      :ok -> :ok
      {:error, _reason} -> {:error, :invalid_acl_username}
    end
  end

  defp decode_projection_revision(nil), do: {:ok, -1}

  defp decode_projection_revision(encoded) when is_binary(encoded) do
    case Ferricstore.ServerCatalog.decode_revision(encoded) do
      {:ok, revision} -> {:ok, revision}
      {:error, _reason} -> {:error, :invalid_acl_catalog_revision}
    end
  end

  @doc false
  @spec replace_catalog_snapshot([{binary(), map(), non_neg_integer()}], non_neg_integer()) ::
          :ok | {:error, atom()}
  def replace_catalog_snapshot(users, revision)
      when is_list(users) and is_integer(revision) and revision >= 0 do
    GenServer.call(__MODULE__, {:replace_catalog_snapshot, users, revision})
  end

  def replace_catalog_snapshot(_users, _revision), do: {:error, :invalid_acl_catalog_snapshot}

  @doc false
  @spec catalog_projection_revision() :: integer()
  def catalog_projection_revision, do: GenServer.call(__MODULE__, :catalog_projection_revision)

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
    with {:ok, store} <- default_store() do
      FerricstoreServer.Management.ACL.load(store: store)
    end
  end

  @doc """
  Loads ACL state from the given directory path.

  Same as `load/0` but with an explicit data directory.
  """
  @spec load(binary()) :: :ok | {:error, binary()}
  def load(data_dir) do
    with {:ok, store} <- default_store() do
      FerricstoreServer.Management.ACL.load(store: store, data_dir: data_dir)
    end
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
    with {:ok, store} <- default_store() do
      FerricstoreServer.Management.ACL.import_contents(contents, store: store)
    end
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
    :ok = FerricstoreServer.Acl.CatalogProjector.mark_stale(:acl_projection_initializing)

    table = Tables.new_active_table()
    Tables.insert_default_user()

    state = %{
      table: table,
      save_timer: nil,
      auth_epoch: 0,
      catalog_versions: %{},
      catalog_revision: -1
    }

    {:ok, state}
  end

  @impl true
  def handle_call(
        {:project_catalog_user, username, user, version, expected_revision},
        _from,
        state
      ) do
    previous_version = max(Map.get(state.catalog_versions, username, -1), state.catalog_revision)

    cond do
      version <= previous_version ->
        {:reply, :ok, state}

      projection_gap?(state, expected_revision) ->
        :ok = FerricstoreServer.Acl.CatalogProjector.mark_stale(:acl_projection_revision_gap)
        {:reply, {:error, :acl_catalog_projection_gap}, state}

      true ->
        user = Map.put(user, :auth_epoch, version)
        :ets.insert(Tables.active_table(), {username, user})
        :ok = Tables.update_configured_user_witness(username, user)

        next_revision = next_catalog_projection_revision(state, expected_revision, version)

        state = %{
          state
          | auth_epoch: max(state.auth_epoch, version),
            catalog_versions: Map.put(state.catalog_versions, username, version),
            catalog_revision: next_revision
        }

        :ok =
          broadcast_local_acl_invalidation(
            username,
            projection_invalidation_revision(expected_revision, next_revision, version)
          )

        {:reply, :ok, maybe_schedule_auto_save(state)}
    end
  end

  def handle_call(
        {:project_catalog_delete, "default", _version, _expected_revision},
        _from,
        state
      ) do
    {:reply, {:error, :cannot_delete_default_acl_user}, state}
  end

  def handle_call(
        {:project_catalog_delete, username, version, expected_revision},
        _from,
        state
      ) do
    previous_version = max(Map.get(state.catalog_versions, username, -1), state.catalog_revision)

    cond do
      version <= previous_version ->
        {:reply, :ok, state}

      projection_gap?(state, expected_revision) ->
        :ok = FerricstoreServer.Acl.CatalogProjector.mark_stale(:acl_projection_revision_gap)
        {:reply, {:error, :acl_catalog_projection_gap}, state}

      true ->
        :ets.delete(Tables.active_table(), username)
        :ok = Tables.remove_configured_user_witnesses([username])

        next_revision = next_catalog_projection_revision(state, expected_revision, version)

        state = %{
          state
          | auth_epoch: max(state.auth_epoch, version),
            catalog_versions: Map.put(state.catalog_versions, username, version),
            catalog_revision: next_revision
        }

        :ok =
          broadcast_local_acl_invalidation(
            username,
            projection_invalidation_revision(expected_revision, next_revision, version)
          )

        {:reply, :ok, maybe_schedule_auto_save(state)}
    end
  end

  def handle_call(:catalog_projection_revision, _from, state) do
    {:reply, state.catalog_revision, state}
  end

  def handle_call({:replace_catalog_snapshot, users, revision}, _from, state) do
    case prepare_catalog_snapshot(users) do
      {:ok, rows, versions, max_version} when max_version <= revision ->
        if latest_catalog_projection_version(state) > revision do
          {:reply, {:error, :stale_acl_catalog_snapshot}, state}
        else
          :ok = Tables.replace_acl_snapshot(rows)

          state = %{
            state
            | auth_epoch: max(state.auth_epoch, revision),
              catalog_versions: versions,
              catalog_revision: revision
          }

          :ok = broadcast_local_acl_invalidation(:all, revision)
          {:reply, :ok, maybe_schedule_auto_save(state)}
        end

      {:ok, _rows, _versions, _max_version} ->
        {:reply, {:error, :invalid_acl_catalog_snapshot}, state}

      {:error, _reason} = error ->
        {:reply, error, state}
    end
  end

  def handle_call(:reset, _from, state) do
    auth_epoch = state.auth_epoch + 1
    :ets.delete_all_objects(ensure_active_table())
    :ok = Tables.clear_configured_user_witness()
    Tables.insert_default_user(auth_epoch)
    :ok = broadcast_local_acl_invalidation(:all, auth_epoch)
    :ok = FerricstoreServer.Acl.CatalogProjector.mark_stale(:acl_projection_reset)

    {:reply, :ok, %{state | auth_epoch: auth_epoch, catalog_versions: %{}, catalog_revision: -1}}
  end

  # --- ACL SAVE ---

  def handle_call(:acl_save, _from, state) do
    data_dir = Application.get_env(:ferricstore, :data_dir, "data")
    {:reply, Persistence.save(data_dir, state.auth_epoch), state}
  end

  def handle_call({:acl_save, data_dir}, _from, state) do
    {:reply, Persistence.save(data_dir, state.auth_epoch), state}
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

  def handle_info({:cleanup_acl_retired_table, table}, state) do
    Tables.cleanup_retired_table(table)
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

  defp broadcast_local_acl_invalidation(username, revision),
    do: FerricstoreServer.Connection.Auth.broadcast_local_acl_invalidation(username, revision)

  defp default_store do
    {:ok, FerricStore.Instance.get(:default)}
  rescue
    _error -> {:error, "ERR ACL catalog unavailable"}
  catch
    :exit, _reason -> {:error, "ERR ACL catalog unavailable"}
  end

  defp ensure_active_table do
    table = Tables.active_table()

    case :ets.info(table) do
      :undefined -> Tables.new_active_table()
      _info -> table
    end
  rescue
    ArgumentError -> Tables.new_active_table()
  end

  defp prepare_catalog_snapshot(users) do
    result =
      Enum.reduce_while(users, {:ok, [], %{}, MapSet.new(), 0}, fn
        {username, user, version}, {:ok, rows, versions, usernames, max_version}
        when is_binary(username) and is_map(user) and is_integer(version) and version >= 0 ->
          if MapSet.member?(usernames, username) do
            {:halt, {:error, :invalid_acl_catalog_snapshot}}
          else
            user = Map.put(user, :auth_epoch, version)

            {:cont,
             {:ok, [{username, user} | rows], Map.put(versions, username, version),
              MapSet.put(usernames, username), max(max_version, version)}}
          end

        _invalid, _acc ->
          {:halt, {:error, :invalid_acl_catalog_snapshot}}
      end)

    case result do
      {:ok, rows, versions, usernames, max_version} ->
        if MapSet.member?(usernames, "default") do
          {:ok, Enum.reverse(rows), versions, max_version}
        else
          {:error, :missing_default_acl_user}
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp latest_catalog_projection_version(state) do
    state.catalog_versions
    |> Map.values()
    |> Enum.max(fn -> state.catalog_revision end)
    |> max(state.catalog_revision)
  end

  defp projection_gap?(_state, :unknown), do: false
  defp projection_gap?(state, expected_revision), do: state.catalog_revision != expected_revision

  defp next_catalog_projection_revision(state, :unknown, _version), do: state.catalog_revision
  defp next_catalog_projection_revision(_state, _expected_revision, version), do: version

  defp projection_invalidation_revision(:unknown, _revision, version), do: version
  defp projection_invalidation_revision(_expected_revision, revision, _version), do: revision
end
