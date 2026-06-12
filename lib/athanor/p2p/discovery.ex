defmodule Athanor.P2P.Discovery do
  @moduledoc """
  Turns DNS seeds and `addr` gossip into candidate `{ip4, port}` addresses for
  the peer pool (Phase 2, T2.2). The only IO (DNS resolution) is injected as a
  `resolver` function, so the discovery logic is deterministic and offline-testable.

  **Fallback seeds are deferred this phase** (plan §4: hardcoded `pnSeed6`-style
  IP tables go stale quickly), so the required bootstrap is DNS + `addr` gossip —
  both self-refreshing. `fallback_seeds/1` therefore returns the network's
  configured list, which is currently empty.
  """

  alias Athanor.P2P.Network
  alias Athanor.P2P.Messages.Addr

  @typedoc "An IPv4 address as a 4-tuple."
  @type ip4 :: {byte(), byte(), byte(), byte()}
  @type addr :: {ip4(), :inet.port_number()}
  @typedoc "Injected DNS resolver: host charlist/string → resolved IPv4 tuples."
  @type resolver :: (String.t() -> {:ok, [ip4()]} | {:error, term()})

  @doc """
  Resolves every DNS seed via `resolver`, unions the results with the network's
  fallback seeds, pairs each IP with the network's default port, and dedups. A
  resolver failure for one seed does not sink the others.
  """
  @spec seed_candidates(Network.t(), resolver()) :: [addr()]
  def seed_candidates(%Network{} = network, resolver) when is_function(resolver, 1) do
    dns =
      network.dns_seeds
      |> Enum.flat_map(fn host ->
        case resolver.(host) do
          {:ok, ips} -> ips
          _ -> []
        end
      end)
      |> Enum.map(&{&1, network.default_port})

    Enum.uniq(dns ++ fallback_seeds(network))
  end

  @doc """
  The network's hardcoded fallback seed addresses. **Deferred** in Phase 2 —
  currently empty; DNS + gossip is the bootstrap contract.
  """
  @spec fallback_seeds(Network.t()) :: [addr()]
  def fallback_seeds(%Network{fallback_seeds: seeds}), do: seeds

  @doc """
  Maps decoded `addr` gossip entries (`{time, services, ip16, port}`) to
  routable `{ip4, port}` candidates, dropping non-IPv4 and non-routable
  addresses, and dedups.
  """
  @spec absorb_addr([Addr.entry()]) :: [addr()]
  def absorb_addr(entries) when is_list(entries) do
    entries
    |> Enum.flat_map(fn {_time, _services, ip16, port} ->
      with {:ok, <<a, b, c, d>>} <- Addr.ipv4_from_16(ip16),
           ip <- {a, b, c, d},
           true <- routable?(ip) do
        [{ip, port}]
      else
        _ -> []
      end
    end)
    |> Enum.uniq()
  end

  @doc """
  Whether an IPv4 address is publicly routable. Rejects the usual special-use
  ranges (this/0, RFC1918 private, loopback, link-local, CGNAT, multicast,
  reserved). Shared by gossip ingestion and the address book.
  """
  @spec routable?(ip4()) :: boolean()
  def routable?({0, _, _, _}), do: false
  def routable?({10, _, _, _}), do: false
  def routable?({127, _, _, _}), do: false
  def routable?({169, 254, _, _}), do: false
  def routable?({172, b, _, _}) when b in 16..31, do: false
  def routable?({192, 168, _, _}), do: false
  def routable?({100, b, _, _}) when b in 64..127, do: false
  def routable?({a, _, _, _}) when a >= 224, do: false
  def routable?({_, _, _, _}), do: true
end
