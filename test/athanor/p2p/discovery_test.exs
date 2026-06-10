defmodule Athanor.P2P.DiscoveryTest do
  @moduledoc """
  Tests for `Athanor.P2P.Discovery` (T2.2): turning DNS seeds and `addr` gossip
  into candidate `{ip4, port}` addresses. The only IO (DNS) is injected as a
  resolver function, so this is fully deterministic. Fallback seeds are
  explicitly deferred this phase (DNS + gossip is the required bootstrap), so
  the tests prove `seed_candidates/2` works with DNS alone.
  """
  use ExUnit.Case, async: true

  alias Athanor.P2P.{Discovery, Network}
  alias Athanor.P2P.Messages.Addr

  test "seed_candidates resolves via the injected resolver, pairs the port, and dedups" do
    net = Network.testnet()
    [s1, _s2, s3 | _] = net.dns_seeds

    # s1 resolves to two IPs, s3 to one (with a duplicate of an s1 IP), the rest
    # fail — and a failing seed must not sink the others.
    resolver = fn
      ^s1 -> {:ok, [{1, 2, 3, 4}, {1, 2, 3, 5}]}
      ^s3 -> {:ok, [{5, 6, 7, 8}, {1, 2, 3, 4}]}
      _ -> {:error, :nxdomain}
    end

    cands = Discovery.seed_candidates(net, resolver)

    assert {{1, 2, 3, 4}, 18_333} in cands
    assert {{1, 2, 3, 5}, 18_333} in cands
    assert {{5, 6, 7, 8}, 18_333} in cands
    # Deduped (the {1,2,3,4} duplicate appears once) and no fallback dependency.
    assert length(cands) == 3
  end

  test "fallback_seeds is a list (deferred → empty) and differs by network shape" do
    assert is_list(Discovery.fallback_seeds(Network.mainnet()))
    assert is_list(Discovery.fallback_seeds(Network.testnet()))
    # Deferred this phase.
    assert Discovery.fallback_seeds(Network.testnet()) == []
  end

  test "absorb_addr maps gossip to {ip,port}, dropping non-IPv4 and unroutable" do
    routable = Addr.ipv4_to_16(<<8, 8, 8, 8>>)
    rfc1918 = Addr.ipv4_to_16(<<10, 0, 0, 1>>)
    loopback = Addr.ipv4_to_16(<<127, 0, 0, 1>>)
    not_v4 = <<0::128>>

    entries = [
      {0, 0, routable, 8333},
      {0, 0, rfc1918, 8333},
      {0, 0, loopback, 8333},
      {0, 0, not_v4, 8333},
      # duplicate routable entry → deduped
      {1, 1, routable, 8333}
    ]

    assert Discovery.absorb_addr(entries) == [{{8, 8, 8, 8}, 8333}]
  end
end
