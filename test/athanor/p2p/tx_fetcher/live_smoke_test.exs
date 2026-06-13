defmodule Athanor.P2P.TxFetcher.LiveSmokeTest do
  @moduledoc """
  Live smoke for the raw-tx pull-fetch against the real network (Phase 5 T5.6).

  Tagged `:external` and excluded from `mix test`; run with:

      mix test --only external

  Starts a real `Athanor.P2P.Supervisor` (DNS discovery + real `Transport.Gen`
  peers) on testnet and proves the `getdata` pull path against live nodes:

    * the pool bootstraps to at least one live peer, and
    * given a txid currently in the testnet mempool (operator-supplied via
      `P2P_SMOKE_FETCH_TXID`, **display-order hex** as shown by explorers),
      `TxFetcher.fetch/2` returns `{:ok, raw}` whose hash matches; with no txid
      supplied, a random (absent) txid returns `:miss` within the timeout —
      proving the `getdata → notfound`/timeout miss path.

  Testnet by default; `P2P_SMOKE_NETWORK=mainnet` opts in to mainnet.
  """
  use ExUnit.Case, async: false

  alias Athanor.P2P.{Network, PeerPool, PeerRegistry, TxFetcher}
  alias Athanor.P2P.Messages.Version

  defp pick_network do
    case System.get_env("P2P_SMOKE_NETWORK") do
      "mainnet" -> Network.mainnet()
      _ -> Network.testnet()
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

  defp eventually(fun, timeout) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_eventually(fun, deadline)
  end

  defp do_eventually(fun, deadline) do
    cond do
      fun.() ->
        true

      System.monotonic_time(:millisecond) >= deadline ->
        false

      true ->
        receive do
        after
          200 -> :ok
        end

        do_eventually(fun, deadline)
    end
  end

  # Display-order hex → wire/internal order (what inv/getdata use).
  defp display_to_wire(hex) do
    hex
    |> Base.decode16!(case: :mixed)
    |> :binary.bin_to_list()
    |> Enum.reverse()
    |> :binary.list_to_bin()
  end

  @tag :external
  @tag timeout: 180_000
  test "pull-fetches a live mempool tx (or misses an absent one)" do
    config = %PeerPool.Config{network: pick_network(), target: 5, our_version: our_version()}
    start_supervised!({Athanor.P2P.Supervisor, pool_config: config})

    assert eventually(fn -> length(PeerRegistry.pids()) >= 1 end, 90_000),
           "pool did not reach a live peer in time"

    case System.get_env("P2P_SMOKE_FETCH_TXID") do
      nil ->
        # No mempool txid supplied: an absent txid must miss within the timeout
        # (getdata → notfound/timeout), proving the miss path against live nodes.
        absent = :crypto.strong_rand_bytes(32)
        assert TxFetcher.fetch(TxFetcher, absent, call_timeout: 30_000) == :miss

      txid_hex ->
        wire = display_to_wire(txid_hex)
        assert {:ok, raw} = TxFetcher.fetch(TxFetcher, wire, call_timeout: 30_000)
        # The served bytes hash to exactly the requested txid (the forgery guard).
        {:ok, tx, _rest} = BSV.Transaction.from_binary(raw)
        assert BSV.Transaction.txid_binary(tx) == wire
    end
  end
end
