defmodule Athanor.P2P.HeadersChain.LiveSmokeTest do
  @moduledoc """
  Live smoke test for the headers chain against the real network (T6.5).

  Tagged `:external` and excluded from `mix test` (see T1.S); run with:

      mix test --only external

  Starts a real `Athanor.P2P.Supervisor` (DNS resolution + real `Transport.Gen`
  peers) with the header tree seeded at the **genesis** block, and asserts the
  chain pulls real headers off live peers and reports an `{:extend, …}` tip —
  the end-to-end proof of `inv`/tick → `getheaders` → `headers` → cumulative-work
  tip selection with **real PoW validation** (the tree's default `pow_check`,
  no bypass). Testnet by default; `P2P_SMOKE_NETWORK=mainnet` runs the mainnet
  variant. A short tick drives the first opportunistic `getheaders` quickly.
  """
  use ExUnit.Case, async: false

  alias Athanor.P2P.Codec.Hash
  alias Athanor.P2P.{Network, PeerPool}
  alias Athanor.P2P.Messages.Version

  # Genesis block hashes (display order); reversed to wire order for the seed.
  @testnet_genesis "000000000933EA01AD0EE984209779BAAEC3CED90FA3F408719526F8D77F4943"
  @mainnet_genesis "000000000019D6689C085AE165831E934FF763AE46A2A6C172B3F1B60A8CE26F"

  defp pick_network do
    case System.get_env("P2P_SMOKE_NETWORK") do
      "mainnet" -> {Network.mainnet(), @mainnet_genesis}
      _ -> {Network.testnet(), @testnet_genesis}
    end
  end

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

  @tag :external
  @tag timeout: 120_000
  test "pulls real headers off live peers and reports an extend from genesis" do
    {network, genesis_display} = pick_network()
    {:ok, display} = Base.decode16(genesis_display, case: :mixed)
    genesis_wire = Hash.display_to_wire(display)
    test = self()

    config = %PeerPool.Config{
      network: network,
      target: 5,
      our_version: our_version()
    }

    headers_opts = [
      seed: fn -> {:ok, 0, genesis_wire} end,
      on_tip: fn ev -> send(test, {:tip, ev}) end,
      # This is a *wire* smoke: it proves real headers pull off live peers and
      # extend the tip with real PoW. The F7.1 cw-144 DAA gate is bypassed here
      # because a from-genesis 3-tuple seed has no 147-header bootstrap window, so
      # the armed gate would (correctly) refuse to seed and the chain would stay
      # inert. DAA correctness is covered by the mainnet golden-vector unit tests.
      daa_check: fn _parent, _header, _ancestor_fun -> :ok end,
      tick_interval_ms: 2_000
    ]

    start_supervised!({Athanor.P2P.Supervisor, pool_config: config, headers_opts: headers_opts})

    # The first headers run off genesis extends the tip; real PoW must pass.
    assert_receive {:tip, {:extend, hashes}}, 90_000
    assert is_list(hashes) and hashes != []
  end
end
