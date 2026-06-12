defmodule Athanor.Indexer.B2gResolverRoutingTest do
  @moduledoc """
  Tests for the Phase 5 T5.3 routing of `B2gResolver` remote parent-fetch through
  `SourceRouter` (`:raw_tx_fetch` = P2P primary, then RPC → JungleBus →
  WhatsOnChain). Providers are injected via `resolve/3` `opts[:providers]` (a
  `%{provider => (txid_hex -> {:ok, %BSV.Transaction{}} | :miss | {:error, _})}`
  map) and the cold-start gate via `opts[:p2p_available?]`, so no node/REST is hit.

  The fixture tx has a single non-STAS (p2pkh) output, so the chain walk stops at
  it as genesis after exactly **one** remote fetch — isolating the routing
  decision. `async: false` (DataCase + the local-DB lookup runs before the remote
  fetch).
  """
  use Athanor.DataCase, async: false

  alias Athanor.Indexer.B2gResolver

  defp fixture_tx do
    %BSV.Transaction{
      version: 1,
      inputs: [
        %BSV.Transaction.Input{
          source_txid: :crypto.strong_rand_bytes(32),
          source_tx_out_index: 0,
          unlocking_script: %BSV.Script{chunks: []},
          sequence_number: 0xFFFFFFFF
        }
      ],
      outputs: [
        %BSV.Transaction.Output{
          satoshis: 1000,
          locking_script: BSV.Script.p2pkh_lock(:binary.copy(<<0x51>>, 20))
        }
      ],
      lock_time: 0
    }
  end

  defp recorder do
    {:ok, agent} = Agent.start_link(fn -> [] end)
    {agent, fn p -> Agent.update(agent, &(&1 ++ [p])) end}
  end

  test "a P2P mempool hit resolves the parent and the REST/RPC providers are never called" do
    tx = fixture_tx()
    txid = BSV.Transaction.txid_binary(tx)

    providers = %{
      p2p: fn _hex -> {:ok, tx} end,
      rpc: fn _hex -> flunk("RPC must not be called on a P2P hit") end,
      junglebus: fn _hex -> flunk("JungleBus must not be called on a P2P hit") end,
      whatsonchain: fn _hex -> flunk("WhatsOnChain must not be called on a P2P hit") end
    }

    assert {:ok, [{_hex, 0}]} =
             B2gResolver.resolve(txid, 0, providers: providers, p2p_available?: true)
  end

  test "a P2P miss falls through the REST/RPC cascade in order (rpc → junglebus → whatsonchain)" do
    tx = fixture_tx()
    txid = BSV.Transaction.txid_binary(tx)
    {agent, rec} = recorder()

    providers = %{
      p2p: fn _hex -> rec.(:p2p) && :miss end,
      rpc: fn _hex -> rec.(:rpc) && {:error, :rpc_down} end,
      junglebus: fn _hex -> rec.(:junglebus) && {:error, :jb_down} end,
      whatsonchain: fn _hex -> rec.(:whatsonchain) && {:ok, tx} end
    }

    assert {:ok, [{_hex, 0}]} =
             B2gResolver.resolve(txid, 0, providers: providers, p2p_available?: true)

    assert Agent.get(agent, & &1) == [:p2p, :rpc, :junglebus, :whatsonchain]
  end

  test "cold start (no peers): P2P is skipped and the resolve uses RPC immediately" do
    tx = fixture_tx()
    txid = BSV.Transaction.txid_binary(tx)

    providers = %{
      p2p: fn _hex -> flunk("P2P must not be attempted when p2p_available? is false") end,
      rpc: fn _hex -> {:ok, tx} end,
      junglebus: fn _hex -> flunk("JungleBus should not be reached") end,
      whatsonchain: fn _hex -> flunk("WhatsOnChain should not be reached") end
    }

    assert {:ok, [{_hex, 0}]} =
             B2gResolver.resolve(txid, 0, providers: providers, p2p_available?: false)
  end
end
