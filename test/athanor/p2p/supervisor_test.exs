defmodule Athanor.P2P.SupervisorTest do
  @moduledoc """
  Tests for `Athanor.P2P.Supervisor` (T2.5 + Phase 3 §C): the config-gated tree
  that owns the `PeerRegistry`, `MempoolObserver`, and `PeerPool`. Verifies child
  membership, `:rest_for_one` restart semantics (registry restart takes the pool
  with it, not vice-versa), the `enabled?/0` config gate (default off), that the
  app tree excludes the P2P supervisor by default, and that the observer is
  wired as the pool's `frame_sink` so forwarded frames reach it (blocker 1).

  `async: false` (singleton `PeerRegistry` name + global app env).
  """
  use ExUnit.Case, async: false

  alias Athanor.P2P.{
    Frame,
    HeadersChain,
    MempoolObserver,
    Network,
    PeerPool,
    PeerRegistry,
    TxFetcher,
    TxRelay,
    Watchlist
  }

  alias Athanor.P2P.Messages.{Inv, Version}

  defp ver do
    na = Version.net_addr(0, <<0::128>>, 0)

    %Version{
      addr_recv: na,
      addr_from: na,
      nonce: 1,
      user_agent: "/Athanor:0.1.0/",
      start_height: 0
    }
  end

  defp fake_config do
    %PeerPool.Config{
      network: Network.testnet(),
      target: 2,
      our_version: ver(),
      peer_starter: fn _ -> {:ok, spawn(fn -> receive do: (:stop -> :ok) end)} end,
      resolver: fn _ -> {:error, :nxdomain} end,
      seeds: [],
      now_fun: fn -> 0 end
    }
  end

  defp child_pid(sup, id) do
    case Enum.find(Supervisor.which_children(sup), fn {cid, _, _, _} -> cid == id end) do
      {^id, pid, _, _} when is_pid(pid) -> pid
      _ -> nil
    end
  end

  test "supervises a PeerRegistry and a PeerPool" do
    sup = start_supervised!({Athanor.P2P.Supervisor, [pool_config: fake_config()]})

    ids = Supervisor.which_children(sup) |> Enum.map(&elem(&1, 0))
    assert PeerRegistry in ids
    assert PeerPool in ids
  end

  test "killing the pool restarts only the pool" do
    sup = start_supervised!({Athanor.P2P.Supervisor, [pool_config: fake_config()]})
    reg1 = child_pid(sup, PeerRegistry)
    pool1 = child_pid(sup, PeerPool)

    ref = Process.monitor(pool1)
    Process.exit(pool1, :kill)
    assert_receive {:DOWN, ^ref, :process, ^pool1, _}

    # The supervisor processed the child exit (enqueued before this call) and
    # restarted the pool; the registry is untouched.
    assert child_pid(sup, PeerRegistry) == reg1
    pool2 = child_pid(sup, PeerPool)
    assert is_pid(pool2) and pool2 != pool1
  end

  test "killing the registry restarts both (rest_for_one)" do
    sup = start_supervised!({Athanor.P2P.Supervisor, [pool_config: fake_config()]})
    reg1 = child_pid(sup, PeerRegistry)
    pool1 = child_pid(sup, PeerPool)

    ref = Process.monitor(reg1)
    Process.exit(reg1, :kill)
    assert_receive {:DOWN, ^ref, :process, ^reg1, _}

    # rest_for_one: the registry comes first, so its restart also restarts the
    # pool that was started after it.
    reg2 = child_pid(sup, PeerRegistry)
    pool2 = child_pid(sup, PeerPool)
    assert is_pid(reg2) and reg2 != reg1
    assert is_pid(pool2) and pool2 != pool1
  end

  test "enabled?/0 defaults to false and reflects config" do
    refute Athanor.P2P.Supervisor.enabled?()

    Application.put_env(:athanor, Athanor.P2P, enabled: true)
    on_exit(fn -> Application.delete_env(:athanor, Athanor.P2P) end)
    assert Athanor.P2P.Supervisor.enabled?()
  end

  test "the app supervision tree excludes the P2P supervisor by default" do
    ids = Supervisor.which_children(Athanor.Supervisor) |> Enum.map(&elem(&1, 0))
    refute Athanor.P2P.Supervisor in ids
  end

  test "runtime_pool_config/0 builds a valid pool config from env" do
    cfg = Athanor.P2P.Supervisor.runtime_pool_config()
    assert %PeerPool.Config{network: %Network{}, our_version: %Version{}} = cfg
    assert cfg.target > 0
  end

  describe "P2P network derives from the authoritative app network (blocker 2)" do
    setup do
      prev_p2p = Application.get_env(:athanor, Athanor.P2P, [])
      prev_net = Application.fetch_env(:athanor, :network)
      # No Athanor.P2P :network override: the P2P network must follow config :network.
      Application.put_env(:athanor, Athanor.P2P, Keyword.delete(prev_p2p, :network))

      on_exit(fn ->
        Application.put_env(:athanor, Athanor.P2P, prev_p2p)

        case prev_net do
          {:ok, v} -> Application.put_env(:athanor, :network, v)
          :error -> Application.delete_env(:athanor, :network)
        end
      end)

      :ok
    end

    # The mainnet checker yields :insufficient_window for an empty window; only the
    # testnet default yields :testnet_daa_unsupported — a crisp discriminator.
    defp daa_default_result(network_name, pow_limit) do
      check = HeadersChain.default_daa_check(network_name, pow_limit)
      header = %Athanor.P2P.Messages.BlockHeader{raw: <<0::640>>}
      check.(%{}, header, fn _node, _n -> nil end)
    end

    test "NETWORK=mainnet (no Athanor.P2P override) arms the mainnet cw-144 checker" do
      Application.put_env(:athanor, :network, "mainnet")
      cfg = Athanor.P2P.Supervisor.runtime_pool_config()

      assert cfg.network.name == :mainnet

      assert daa_default_result(cfg.network.name, cfg.network.pow_limit) ==
               {:error, :insufficient_window}
    end

    test "NETWORK=testnet keeps the intentional fail-closed stub" do
      Application.put_env(:athanor, :network, "testnet")
      cfg = Athanor.P2P.Supervisor.runtime_pool_config()

      assert cfg.network.name == :testnet

      assert daa_default_result(cfg.network.name, cfg.network.pow_limit) ==
               {:error, :testnet_daa_unsupported}
    end

    test "an explicit Athanor.P2P :network override still wins" do
      Application.put_env(:athanor, :network, "mainnet")
      Application.put_env(:athanor, Athanor.P2P, network: :testnet)

      assert Athanor.P2P.Supervisor.runtime_pool_config().network.name == :testnet
    end
  end

  ## ── Phase 3: observer wiring (the !12 review's blocker 1) ──

  test "starts the MempoolObserver under the tree, registered under its name" do
    sup =
      start_supervised!(
        {Athanor.P2P.Supervisor,
         pool_config: fake_config(), observer_opts: [watchlist: Watchlist.new()]}
      )

    ids = Supervisor.which_children(sup) |> Enum.map(&elem(&1, 0))
    assert MempoolObserver in ids
    assert is_pid(Process.whereis(MempoolObserver))
  end

  test "routes a pool frame to the observer, which requests the tx (frame_sink wiring)" do
    sup =
      start_supervised!(
        {Athanor.P2P.Supervisor,
         pool_config: fake_config(), observer_opts: [watchlist: Watchlist.new()]}
      )

    # Drive an inv into the pool as if a Peer forwarded it. The pool forwards it
    # to its frame_sink (the observer), which issues a getdata back to the
    # advertising peer — here self(), so the cast lands as a message to us.
    txid = :binary.copy(<<0xEE>>, 32)
    inv = %Frame{command: "inv", payload: Inv.serialize([{:tx, txid}])}
    send(child_pid(sup, PeerPool), {:peer, self(), :frame, inv})

    assert_receive {:"$gen_cast", {:send_frame, :getdata, payload}}, 1_000
    assert payload == Inv.serialize([{:tx, txid}])
  end

  test "runtime_pool_config wires the frame_sink as the [observer, relay, fetcher] fan-out (Phase 5 §A)" do
    assert Athanor.P2P.Supervisor.runtime_pool_config().frame_sink ==
             [MempoolObserver, TxRelay, TxFetcher, HeadersChain]
  end

  ## ── Phase 4: TxRelay wiring (§A fan-out) ──

  test "starts the TxRelay under the tree, registered under its name, after the observer" do
    sup = start_supervised!({Athanor.P2P.Supervisor, [pool_config: fake_config()]})

    ids = Supervisor.which_children(sup) |> Enum.map(&elem(&1, 0))
    assert TxRelay in ids
    assert is_pid(Process.whereis(TxRelay))

    # rest_for_one order: Registry → Observer → TxRelay → Pool, so both sink
    # names are registered before the pool starts forwarding frames to them.
    order = Supervisor.which_children(sup) |> Enum.map(&elem(&1, 0)) |> Enum.reverse()

    assert Enum.find_index(order, &(&1 == MempoolObserver)) <
             Enum.find_index(order, &(&1 == TxRelay))

    assert Enum.find_index(order, &(&1 == TxRelay)) < Enum.find_index(order, &(&1 == PeerPool))
  end

  test "the default frame_sink fans out to the observer, relay, fetcher, and headers chain" do
    sup = start_supervised!({Athanor.P2P.Supervisor, [pool_config: fake_config()]})

    assert :sys.get_state(child_pid(sup, PeerPool)).config.frame_sink == [
             MempoolObserver,
             TxRelay,
             TxFetcher,
             HeadersChain
           ]
  end

  ## ── Phase 6: HeadersChain wiring (§B fan-out) ──

  test "starts the HeadersChain under the tree, registered, after the fetcher and before the pool" do
    sup = start_supervised!({Athanor.P2P.Supervisor, [pool_config: fake_config()]})

    ids = Supervisor.which_children(sup) |> Enum.map(&elem(&1, 0))
    assert HeadersChain in ids
    assert is_pid(Process.whereis(HeadersChain))

    order = Supervisor.which_children(sup) |> Enum.map(&elem(&1, 0)) |> Enum.reverse()

    assert Enum.find_index(order, &(&1 == TxFetcher)) <
             Enum.find_index(order, &(&1 == HeadersChain))

    assert Enum.find_index(order, &(&1 == HeadersChain)) <
             Enum.find_index(order, &(&1 == PeerPool))
  end

  ## ── Phase 5: TxFetcher wiring (§A fan-out) ──

  test "starts the TxFetcher under the tree, registered, after the relay and before the pool" do
    sup = start_supervised!({Athanor.P2P.Supervisor, [pool_config: fake_config()]})

    ids = Supervisor.which_children(sup) |> Enum.map(&elem(&1, 0))
    assert TxFetcher in ids
    assert is_pid(Process.whereis(TxFetcher))

    # rest_for_one start order: Registry → Observer → TxRelay → TxFetcher → Pool.
    order = Supervisor.which_children(sup) |> Enum.map(&elem(&1, 0)) |> Enum.reverse()

    assert Enum.find_index(order, &(&1 == TxRelay)) <
             Enum.find_index(order, &(&1 == TxFetcher))

    assert Enum.find_index(order, &(&1 == TxFetcher)) < Enum.find_index(order, &(&1 == PeerPool))
  end

  test "an explicit frame_sink in the pool_config is preserved" do
    config = %{fake_config() | frame_sink: self()}

    sup =
      start_supervised!(
        {Athanor.P2P.Supervisor, pool_config: config, observer_opts: [watchlist: Watchlist.new()]}
      )

    assert :sys.get_state(child_pid(sup, PeerPool)).config.frame_sink == self()
  end
end
