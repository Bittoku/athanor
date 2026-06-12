defmodule Athanor.P2P.MempoolObserver.IntegrationTest do
  @moduledoc """
  Real-socket end-to-end for the mempool observer (Phase 3 T3.4): a watched
  address payment is announced by a `FakePeerServer` via `inv`, the observer
  (wired as the pool's `frame_sink`, §C) requests it with `getdata` over a real
  `127.0.0.1` socket, the server answers with the `tx`, and the observer
  verifies the txid, runs the `Watchlist` prefilter → `TransactionFilter.matches?/1`
  → pipeline, persisting the tx with `metadata["sources"] == ["p2p"]`.

  `async: false` (real sockets + singleton registry + SQL sandbox). The
  prefilter-passing/`matches?`-failing drop is covered deterministically in the
  unit test (`mempool_observer_test.exs`); here we prove the live exchange and
  persistence. A bounded `eventually/1` poll is the one allowed real-process
  reality check (handshake + request/response complete asynchronously).
  """
  use Athanor.DataCase, async: false

  alias Athanor.P2P.{Network, PeerPool, Watchlist}
  alias Athanor.P2P.Messages.Version
  alias Athanor.P2P.Transport.LoopbackRewrite
  alias Athanor.P2P.{FakePeerServer, MempoolObserver}
  alias Athanor.Indexer.{TransactionFilter, TransactionProcessor}
  alias Athanor.Repo
  alias Athanor.Schema.MetaTransaction

  @addresses_table :watched_addresses
  @tokens_table :watched_tokens

  defp ver do
    na = Version.net_addr(0, <<0::128>>, 0)

    %Version{
      addr_recv: na,
      addr_from: na,
      nonce: 1,
      user_agent: "/Bitcoin SV:1.2.2/",
      start_height: 1
    }
  end

  defp p2pkh_tx(pkh) do
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
        %BSV.Transaction.Output{satoshis: 1000, locking_script: BSV.Script.p2pkh_lock(pkh)}
      ],
      lock_time: 0
    }
  end

  defp eventually(fun, timeout \\ 4_000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_eventually(fun, deadline)
  end

  defp do_eventually(fun, deadline) do
    cond do
      result = fun.() ->
        result

      System.monotonic_time(:millisecond) >= deadline ->
        flunk("condition not met within timeout")

      true ->
        receive do
        after
          25 -> :ok
        end

        do_eventually(fun, deadline)
    end
  end

  # The real `TransactionFilter` is replaced by a stub in the test env, so its
  # `:ets` tables aren't created. `matches?/1` is a plain module function reading
  # those tables; create them (idempotently) so it is the genuine inclusion
  # authority, and seed only `address`.
  defp seed_filter(address) do
    for t <- [@addresses_table, @tokens_table] do
      if :ets.whereis(t) == :undefined do
        :ets.new(t, [:set, :public, :named_table, read_concurrency: true])
      end
    end

    :ets.insert(@addresses_table, {address, true})
    :ok
  end

  setup do
    case Process.whereis(TransactionProcessor) do
      nil -> start_supervised!(TransactionProcessor)
      _ -> :ok
    end

    :ok
  end

  test "a watched-address payment is inv'd, requested, delivered, and persisted with source :p2p" do
    net = Network.testnet()

    pkh = :binary.copy(<<0x71>>, 20)
    address = BSV.Base58.check_encode(pkh, 0x00)
    tx = p2pkh_tx(pkh)
    payload = BSV.Transaction.to_binary(tx)
    txid = BSV.Transaction.txid_binary(tx)

    # The matcher is the genuine TransactionFilter.matches?/1 (single authority).
    seed_filter(address)
    watchlist = Watchlist.put_address(Watchlist.new(), address)

    observer =
      start_supervised!(
        {MempoolObserver,
         watchlist: watchlist,
         matcher: &TransactionFilter.matches?/1,
         tick_interval_ms: 60_000,
         request_timeout_ms: 30_000},
        id: {MempoolObserver, make_ref()}
      )

    {:ok, port, _server} =
      FakePeerServer.start(
        network: net,
        report_to: self(),
        peer_version: ver(),
        inv_hash: txid,
        tx_payload: payload,
        linger: true
      )

    syn = {{1, 0, 1, 1}, 18_333}
    rewrite = %{:inet.ntoa(elem(syn, 0)) => {~c"127.0.0.1", port}}

    config = %PeerPool.Config{
      network: net,
      target: 1,
      our_version: ver(),
      transport: LoopbackRewrite,
      transport_opts: [rewrite: rewrite],
      resolver: fn _ -> {:error, :nxdomain} end,
      seeds: [syn],
      frame_sink: observer
    }

    start_supervised!({Athanor.P2P.Supervisor, pool_config: config})

    # The pool dials → handshakes → server inv(txid) → observer getdata →
    # server tx → observer verifies/matches → pipeline persists.
    meta = eventually(fn -> Repo.get_by(MetaTransaction, txid: txid) end)

    assert meta.metadata["sources"] == ["p2p"]
    assert meta.txid == txid
  end
end
