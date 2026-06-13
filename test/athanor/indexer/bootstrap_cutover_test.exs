defmodule Athanor.Indexer.BootstrapCutoverTest do
  @moduledoc """
  Phase 7 F7.2 (T7.S) — the persisted bootstrap boundary and the producer cutover
  to a single index-tip mutation owner:

    * `Athanor.Indexer.Bootstrap` captures the boundary once (idempotent) and
      `BlockProcessor.current_bootstrap/0` reflects it (so the predecessor guard
      accepts exactly the bootstrap block);
    * the application supervision tree no longer supervises the legacy RPC poller
      `Athanor.Workers.ChainTipVerifier`, and supervises the new `TipController`
      (the single mutation owner);
    * the ZMQ `hashblock` event routes to `TipController.hint(:zmq, …)` instead of
      casting the index directly.
  """
  use Athanor.DataCase, async: false

  alias Athanor.Indexer.{BlockProcessor, Bootstrap}

  describe "Bootstrap boundary persistence" do
    test "ensure/2 captures the boundary once; fetch/0 returns it; it is idempotent" do
      assert Bootstrap.fetch() == nil
      assert %{height: 200, hash: "boot"} = Bootstrap.ensure(200, "boot")
      assert %{height: 200, hash: "boot"} = Bootstrap.fetch()

      # A second ensure with a different height must NOT overwrite the boundary.
      assert %{height: 200, hash: "boot"} = Bootstrap.ensure(999, "other")
      assert %{height: 200, hash: "boot"} = Bootstrap.fetch()
    end

    test "BlockProcessor's predecessor guard uses the persisted boundary" do
      Bootstrap.ensure(200, nil)

      # The bootstrap block (height 200, missing predecessor) is accepted via the
      # persisted boundary (no injected :bootstrap opt).
      assert :ok = BlockProcessor.predecessor_status("any", 200)
      # Any other missing-predecessor block is refused.
      assert {:error, :missing_predecessor} = BlockProcessor.predecessor_status("any", 205)
    end
  end

  describe "single index-tip mutation owner (supervision cutover)" do
    test "Workers.Supervisor no longer supervises ChainTipVerifier" do
      {:ok, {_flags, specs}} = Athanor.Workers.Supervisor.init(:ok)
      ids = Enum.map(specs, &child_id/1)
      refute Athanor.Workers.ChainTipVerifier in ids
    end

    test "Indexer.Supervisor supervises BlockProcessor and the TipController" do
      {:ok, {_flags, specs}} = Athanor.Indexer.Supervisor.init(:ok)
      ids = Enum.map(specs, &child_id/1)
      assert Athanor.Indexer.BlockProcessor in ids
      assert Athanor.Indexer.TipController in ids
    end
  end

  describe "ZMQ hashblock routes to a hint (not a direct index mutation)" do
    test "a hashblock event triggers a TipController reconcile, applying only the node branch" do
      test = self()
      node_hashes = %{5 => hex(5), 6 => hex(6)}

      # The named controller the listener will hint.
      start_supervised!(
        {Athanor.Indexer.TipController,
         name: Athanor.Indexer.TipController,
         rpc_height: fn -> {:ok, 6} end,
         rpc_hash_at: fn h -> node_hashes[h] end,
         local_height: fn -> 5 end,
         local_hash_at: fn h -> if(h == 5, do: hex(5), else: nil) end,
         apply_fun: fn _p, arg -> send(test, {:applied, arg}) && {:ok, length(arg.connect)} end,
         tick_interval_ms: 60_000}
      )

      # Drive the ZMQ listener's hashblock handler directly.
      msg = {:zmq, :sock, [<<"hashblock">>, <<0xAB>>], []}
      {:noreply, _} = Athanor.Blockchain.ZmqListener.handle_info(msg, %{socket: nil})

      _ = drain(Process.whereis(Athanor.Indexer.TipController))
      assert_received {:applied, %{connect: [_ | _]}}
    end
  end

  defp child_id(%{id: id}), do: id
  defp child_id(id) when is_atom(id), do: id

  defp hex(h), do: Base.encode16(<<h::16>>, case: :lower)

  defp drain(pid, n \\ 25)
  defp drain(pid, 0), do: :sys.get_state(pid)

  defp drain(pid, n) do
    s = :sys.get_state(pid)
    if s.machine.in_flight or s.machine.pending, do: drain(pid, n - 1), else: s
  end
end
