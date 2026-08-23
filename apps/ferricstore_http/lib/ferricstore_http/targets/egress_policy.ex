defmodule FerricstoreHttp.Targets.EgressPolicy do
  @moduledoc "Config-driven egress guard for outbound invocation targets."

  import Bitwise

  alias FerricstoreHttp.Config

  @type t :: %{
          allowed_hosts: [binary()],
          require_https?: boolean(),
          deny_private_networks?: boolean(),
          private_network_allowlist: [binary()]
        }

  @default_allowed_hosts ["localhost", "127.0.0.1", "::1"]

  @spec default() :: t()
  def default do
    %{
      allowed_hosts: @default_allowed_hosts,
      require_https?: false,
      deny_private_networks?: true,
      private_network_allowlist: @default_allowed_hosts
    }
  end

  @spec from_config(Config.t()) :: t()
  def from_config(%Config{} = config) do
    %{
      allowed_hosts: config.target_allowed_hosts,
      require_https?: config.target_require_https,
      deny_private_networks?: config.target_deny_private_networks,
      private_network_allowlist: config.target_private_network_allowlist
    }
  end

  @spec validate_uri(URI.t(), t()) :: :ok | {:error, term()} | {:retry, term()}
  def validate_uri(%URI{scheme: scheme, host: host}, policy) when is_map(policy) do
    policy = normalize(policy)

    with :ok <- validate_scheme(scheme, policy),
         :ok <- validate_allowed_host(host, policy) do
      validate_host_network(host, policy)
    end
  end

  @spec authorize_uri(URI.t(), t()) ::
          {:ok, :inet.ip_address()} | {:error, term()} | {:retry, term()}
  def authorize_uri(%URI{scheme: scheme, host: host}, policy) when is_map(policy) do
    policy = normalize(policy)

    with :ok <- validate_scheme(scheme, policy),
         :ok <- validate_allowed_host(host, policy),
         {:ok, addresses} <- resolve_for_request(host),
         :ok <- validate_private_network(host, addresses, policy) do
      {:ok, hd(addresses)}
    end
  end

  @spec normalize(map() | nil) :: t()
  def normalize(nil), do: default()

  def normalize(policy) when is_map(policy) do
    defaults = default()

    %{
      allowed_hosts: value(policy, :allowed_hosts, defaults.allowed_hosts),
      require_https?: value(policy, :require_https?, defaults.require_https?),
      deny_private_networks?:
        value(policy, :deny_private_networks?, defaults.deny_private_networks?),
      private_network_allowlist:
        value(policy, :private_network_allowlist, defaults.private_network_allowlist)
    }
  end

  defp validate_scheme("https", _policy), do: :ok
  defp validate_scheme("http", %{require_https?: false}), do: :ok
  defp validate_scheme("http", %{require_https?: true}), do: {:error, :target_https_required}
  defp validate_scheme(_scheme, _policy), do: {:error, :invalid_http_endpoint_url}

  defp validate_allowed_host(host, %{allowed_hosts: patterns}) when is_binary(host) do
    if host_allowed?(host, patterns), do: :ok, else: {:error, :target_host_not_allowed}
  end

  defp validate_allowed_host(_host, _policy), do: {:error, :invalid_http_endpoint_url}

  defp validate_private_network(_host, _addresses, %{deny_private_networks?: false}), do: :ok

  defp validate_private_network(host, addresses, %{private_network_allowlist: allowlist}) do
    if host_allowed?(host, allowlist) do
      :ok
    else
      validate_addresses(addresses)
    end
  end

  defp validate_addresses(addresses) do
    if Enum.any?(addresses, &private_or_local_address?/1),
      do: {:error, :target_private_network_denied},
      else: :ok
  end

  defp validate_host_network(_host, %{deny_private_networks?: false}), do: :ok

  defp validate_host_network(host, %{private_network_allowlist: allowlist}) do
    if host_allowed?(host, allowlist) do
      :ok
    else
      with {:ok, addresses} <- resolve_for_request(host), do: validate_addresses(addresses)
    end
  end

  defp resolve_for_request(host) do
    case resolve(host) do
      {:ok, addresses} ->
        {:ok, addresses}

      {:error, _reason} ->
        {:retry, %{"code" => "target_host_resolution_failed"}}
    end
  end

  defp resolve(host) do
    charlist = String.to_charlist(host)

    case :inet.parse_address(charlist) do
      {:ok, address} -> {:ok, [address]}
      {:error, :einval} -> resolve_dns(charlist)
    end
  end

  defp resolve_dns(charlist) do
    addresses =
      [:inet, :inet6]
      |> Enum.flat_map(fn family ->
        case :inet.getaddrs(charlist, family) do
          {:ok, values} -> values
          {:error, _reason} -> []
        end
      end)

    if addresses == [], do: {:error, :not_found}, else: {:ok, addresses}
  end

  defp private_or_local_address?({10, _, _, _}), do: true
  defp private_or_local_address?({127, _, _, _}), do: true
  defp private_or_local_address?({169, 254, _, _}), do: true
  defp private_or_local_address?({172, second, _, _}) when second in 16..31, do: true
  defp private_or_local_address?({192, 168, _, _}), do: true
  defp private_or_local_address?({100, second, _, _}) when second in 64..127, do: true
  defp private_or_local_address?({0, _, _, _}), do: true
  defp private_or_local_address?({192, 0, 0, _}), do: true
  defp private_or_local_address?({192, 0, 2, _}), do: true
  defp private_or_local_address?({198, second, _, _}) when second in 18..19, do: true
  defp private_or_local_address?({198, 51, 100, _}), do: true
  defp private_or_local_address?({203, 0, 113, _}), do: true
  defp private_or_local_address?({first, _, _, _}) when first >= 224, do: true
  defp private_or_local_address?({0, 0, 0, 0, 0, 0, 0, 0}), do: true
  defp private_or_local_address?({0, 0, 0, 0, 0, 0, 0, 1}), do: true

  defp private_or_local_address?({0, 0, 0, 0, 0, 0xFFFF, high, low}) do
    private_or_local_address?({high >>> 8, high &&& 0xFF, low >>> 8, low &&& 0xFF})
  end

  defp private_or_local_address?({0x64, 0xFF9B, 0, 0, 0, 0, high, low}) do
    private_or_local_address?({high >>> 8, high &&& 0xFF, low >>> 8, low &&& 0xFF})
  end

  defp private_or_local_address?({0x64, 0xFF9B, 1, _, _, _, _, _}), do: true

  defp private_or_local_address?({0, 0, 0, 0, 0, 0, high, low}) do
    private_or_local_address?({high >>> 8, high &&& 0xFF, low >>> 8, low &&& 0xFF})
  end

  defp private_or_local_address?({first, _, _, _, _, _, _, _})
       when (first &&& 0xFE00) == 0xFC00,
       do: true

  defp private_or_local_address?({first, _, _, _, _, _, _, _})
       when (first &&& 0xFFC0) == 0xFE80,
       do: true

  defp private_or_local_address?({first, _, _, _, _, _, _, _})
       when (first &&& 0xFFC0) == 0xFEC0,
       do: true

  defp private_or_local_address?({0x2002, high, low, _, _, _, _, _}) do
    private_or_local_address?({high >>> 8, high &&& 0xFF, low >>> 8, low &&& 0xFF})
  end

  defp private_or_local_address?({first, _, _, _, _, _, _, _})
       when (first &&& 0xFF00) == 0xFF00,
       do: true

  defp private_or_local_address?(_address), do: false

  defp host_allowed?(host, patterns) when is_binary(host) do
    host = normalize_host(host)
    Enum.any?(patterns, &host_matches?(host, normalize_host(&1)))
  end

  defp host_matches?(_host, "*"), do: true

  defp host_matches?(host, "*." <> suffix),
    do: String.ends_with?(host, "." <> suffix) and host != suffix

  defp host_matches?(host, host), do: true
  defp host_matches?(_host, _pattern), do: false

  defp normalize_host(host) do
    host
    |> to_string()
    |> String.trim()
    |> String.trim_leading("[")
    |> String.trim_trailing("]")
    |> String.downcase()
  end

  defp value(policy, key, default) do
    string_key = Atom.to_string(key)

    cond do
      Map.has_key?(policy, key) -> Map.fetch!(policy, key)
      Map.has_key?(policy, string_key) -> Map.fetch!(policy, string_key)
      true -> default
    end
  end
end
