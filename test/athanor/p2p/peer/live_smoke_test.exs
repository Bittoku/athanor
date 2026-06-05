defmodule Athanor.P2P.Peer.LiveSmokeTest do
  @moduledoc """
  Live wire-correctness smoke test against the real network (T1.8).

  Tagged `:external` and therefore **excluded from `mix test`** (see T1.S); run
  it deliberately with:

      mix test --only external

  It resolves the network's DNS seeds, opens real `Peer` connections to several
  candidate nodes, and asserts that at least one completes the handshake with a
  sane advertised `version` and that an `inv` arrives shortly after. This is the
  one true external-dependency check; a failure here where T1.7 passed points at
  magic/seed/user-agent issues, not framing.

  Network defaults to **testnet** (matching the repo's runtime default); set
  `P2P_SMOKE_NETWORK=mainnet` to run the mainnet variant instead.
  """
  use ExUnit.Case, async: false

  alias Athanor.P2P.{Frame, Network}
  alias Athanor.P2P.Messages.Version
  alias Athanor.P2P.{Peer, Transport}

  @candidates 10

  @tag :external
  @tag timeout: 60_000
  test "reaches :ready against a live node and receives an inv" do
    network = pick_network()
    ips = resolve_seed_ips(network)
    assert ips != [], "no seed IPs resolved for #{network.name}"

    our = our_version()

    # Fan out to several candidates; real nodes are often unreachable, so the
    # first to complete the handshake wins.
    for ip <- Enum.take(ips, @candidates) do
      config = %Peer.Config{
        host: ip,
        port: network.default_port,
        network: network,
        our_version: our,
        transport: Transport.Gen,
        transport_opts: [],
        owner: self(),
        timeouts: %{connect: 5_000, handshake: 8_000, inactivity: 30_000}
      }

      {:ok, _pid} = Peer.start_link(config)
    end

    assert_receive {:peer, _pid, :ready, %Version{} = v}, 20_000
    assert v.start_height > min_height(network)
    assert v.user_agent =~ "/"

    assert_receive {:peer, _other, :frame, %Frame{command: "inv"}}, 10_000
  end

  defp pick_network do
    case System.get_env("P2P_SMOKE_NETWORK") do
      "mainnet" -> Network.mainnet()
      _ -> Network.testnet()
    end
  end

  defp min_height(%Network{name: :mainnet}), do: 800_000
  defp min_height(%Network{name: :testnet}), do: 1_600_000

  defp our_version do
    na = Version.net_addr(0, <<0::128>>, 0)

    %Version{
      addr_recv: na,
      addr_from: na,
      nonce: 1,
      user_agent: "/Athanor:0.1.0/",
      start_height: 0
    }
  end

  # Resolve every DNS seed to its advertised node IPs (A records), as charlists.
  defp resolve_seed_ips(%Network{dns_seeds: seeds}) do
    seeds
    |> Enum.flat_map(fn seed ->
      case :inet.getaddrs(String.to_charlist(seed), :inet) do
        {:ok, addrs} -> addrs
        _ -> []
      end
    end)
    |> Enum.uniq()
    |> Enum.map(&:inet.ntoa/1)
  end
end
