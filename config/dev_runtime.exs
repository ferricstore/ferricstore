defmodule Ferricstore.DevRuntime do
  @moduledoc """
  Pure development configuration derived from the checkout containing `config/`.

  This file is loaded by `config/dev.exs`, rather than by a production release.
  A linked worktree is identified by a file at `<checkout>/.git`; ordinary
  checkouts have a `.git` directory. Linked worktrees use OS-assigned listener
  ports so several checkouts can run at the same time without a port allocator.
  `FERRICSTORE_DATA_DIR`, `FERRICSTORE_NATIVE_PORT`,
  `FERRICSTORE_HEALTH_PORT`, `FERRICSTORE_HEALTH_PROBE_PORT`, and
  `FERRICSTORE_HTTP_PORT` remain explicit escape hatches; a fixed port or a
  path outside the checkout can reintroduce cross-worktree sharing.
  """

  @default_ports %{
    native_port: 6388,
    health_port: 4000,
    health_probe_port: 4001,
    http_port: 8080
  }

  @port_keys Map.keys(@default_ports)
  @override_keys [:data_dir | @port_keys]
  @env_overrides %{
    data_dir: "FERRICSTORE_DATA_DIR",
    native_port: "FERRICSTORE_NATIVE_PORT",
    health_port: "FERRICSTORE_HEALTH_PORT",
    health_probe_port: "FERRICSTORE_HEALTH_PROBE_PORT",
    http_port: "FERRICSTORE_HTTP_PORT"
  }

  @type settings :: %{
          checkout_root: Path.t(),
          config_dir: Path.t(),
          linked_worktree?: boolean(),
          isolated?: boolean(),
          data_dir: Path.t(),
          native_port: 0..65_535,
          health_port: 0..65_535,
          health_probe_port: 0..65_535,
          http_port: 0..65_535,
          node_identity: atom(),
          discovery: :disabled,
          explicit_overrides: [atom()],
          isolation_warning: binary() | nil
        }

  @doc """
  Derives standalone development settings for a config directory.

  The optional overrides are intended for local scripts that deliberately need
  a fixed port or a separate data root. Relative data paths are resolved from
  the checkout, never from the process working directory. Fixed ports and
  external data roots are reported in `:isolation_warning` because they can
  defeat linked-worktree isolation.
  """
  @spec settings(Path.t(), keyword()) :: settings()
  def settings(config_dir \\ __DIR__, opts \\ []) when is_binary(config_dir) and is_list(opts) do
    validate_options!(opts)

    config_dir = Path.expand(config_dir)
    checkout_root = Path.expand("..", config_dir)
    linked_worktree? = linked_worktree?(checkout_root)

    defaults =
      if linked_worktree? do
        Map.new(@default_ports, fn {key, _port} -> {key, 0} end)
      else
        @default_ports
      end

    overrides = Map.merge(environment_overrides(), Map.new(opts))

    data_dir =
      normalize_data_dir!(
        Map.get(overrides, :data_dir, Path.join(checkout_root, "data")),
        checkout_root
      )

    ports =
      Enum.into(@port_keys, %{}, fn key ->
        {key, normalize_port!(Map.get(overrides, key, Map.fetch!(defaults, key)), key)}
      end)

    explicit_overrides =
      overrides
      |> Map.keys()
      |> Enum.filter(&(&1 in @override_keys))
      |> Enum.sort()

    isolation_warning = isolation_warning(linked_worktree?, explicit_overrides)

    %{
      config_dir: config_dir,
      checkout_root: checkout_root,
      linked_worktree?: linked_worktree?,
      isolated?: linked_worktree? and is_nil(isolation_warning),
      data_dir: data_dir,
      native_port: ports.native_port,
      health_port: ports.health_port,
      health_probe_port: ports.health_probe_port,
      http_port: ports.http_port,
      node_identity: :nonode@nohost,
      discovery: :disabled,
      explicit_overrides: explicit_overrides,
      isolation_warning: isolation_warning
    }
  end

  @doc "Returns whether `checkout_root` is an attached Git worktree."
  @spec linked_worktree?(Path.t()) :: boolean()
  def linked_worktree?(checkout_root) when is_binary(checkout_root) do
    checkout_root
    |> Path.expand()
    |> Path.join(".git")
    |> File.read()
    |> case do
      {:ok, contents} -> String.starts_with?(String.trim_leading(contents), "gitdir:")
      {:error, _reason} -> false
    end
  end

  defp environment_overrides do
    Enum.reduce(@env_overrides, %{}, fn {key, variable}, overrides ->
      case System.get_env(variable) do
        nil -> overrides
        value -> Map.put(overrides, key, value)
      end
    end)
  end

  defp validate_options!(opts) do
    case Enum.find(opts, fn {key, _value} -> key not in @override_keys end) do
      nil ->
        :ok

      {key, _value} ->
        raise ArgumentError, "unsupported FerricStore dev runtime option: #{inspect(key)}"
    end
  end

  defp normalize_data_dir!(value, checkout_root) when is_binary(value) do
    if String.trim(value) == "" do
      raise ArgumentError, "data_dir must not be empty"
    end

    Path.expand(value, checkout_root)
  end

  defp normalize_data_dir!(value, _checkout_root) do
    raise ArgumentError, "data_dir must be a path, got: #{inspect(value)}"
  end

  defp normalize_port!(value, _key) when is_integer(value) and value in 0..65_535, do: value

  defp normalize_port!(value, key) when is_binary(value) do
    case Integer.parse(String.trim(value)) do
      {port, ""} when port in 0..65_535 -> port
      _other -> raise ArgumentError, "#{key} must be an integer between 0 and 65535"
    end
  end

  defp normalize_port!(value, key) do
    raise ArgumentError, "#{key} must be an integer between 0 and 65535, got: #{inspect(value)}"
  end

  defp isolation_warning(false, _explicit_overrides), do: nil

  defp isolation_warning(true, []) do
    nil
  end

  defp isolation_warning(true, explicit_overrides) do
    fixed_port_override? = Enum.any?(explicit_overrides, &(&1 in @port_keys))
    data_override? = :data_dir in explicit_overrides

    cond do
      fixed_port_override? and data_override? ->
        "explicit listener and data_dir overrides can defeat linked-worktree isolation"

      fixed_port_override? ->
        "explicit listener port overrides can collide with another checkout"

      data_override? ->
        "an explicit data_dir override can share storage with another checkout"

      true ->
        nil
    end
  end
end
