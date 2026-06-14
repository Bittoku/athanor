defmodule Athanor.Workers.StasObserverTest do
  @moduledoc """
  Covers `StasObserver.tally/0` and the `:stas_attribute_change` rebroadcast
  path. The periodic `:check_attributes` tick is not exercised — it only
  logs the same tally that `tally/0` returns.
  """

  use Athanor.DataCase, async: false

  alias Athanor.Workers.StasObserver
  alias Athanor.Repo
  alias Athanor.Schema.{Utxo, WatchingToken}

  ## ── Helpers ──

  defp seed_watched_token!(token_id) do
    {:ok, t} =
      %WatchingToken{}
      |> WatchingToken.changeset(%{token_id: token_id, symbol: "T"})
      |> Repo.insert()

    t
  end

  defp seed_utxo!(token_id, is_spent) do
    {:ok, _} =
      %Utxo{}
      |> Utxo.changeset(%{
        txid: :crypto.strong_rand_bytes(32),
        vout: 0,
        address: "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa",
        satoshis: 1000,
        script_hex: "76a91462e907b15cbf27d5425399ebf6f0fb50ebb88f1888ac",
        token_id: token_id,
        token_type: "stas3",
        is_spent: is_spent
      })
      |> Repo.insert()

    :ok
  end

  ## ── Tests ──

  describe "tally/0" do
    test "returns 0 for a watched token with no UTXOs" do
      seed_watched_token!("aa" |> String.duplicate(20))

      assert StasObserver.tally() == %{("aa" |> String.duplicate(20)) => 0}
    end

    test "counts only is_spent=false UTXOs per watched token" do
      tok = "bb" |> String.duplicate(20)
      seed_watched_token!(tok)
      seed_utxo!(tok, false)
      seed_utxo!(tok, false)
      seed_utxo!(tok, true)

      assert StasObserver.tally() == %{tok => 2}
    end

    test "returns separate counts per token and ignores untracked tokens" do
      tracked = "cc" |> String.duplicate(20)
      untracked = "dd" |> String.duplicate(20)
      seed_watched_token!(tracked)
      seed_utxo!(tracked, false)
      seed_utxo!(untracked, false)

      assert StasObserver.tally() == %{tracked => 1}
    end

    test "returns an empty map when no tokens are watched" do
      assert StasObserver.tally() == %{}
    end
  end

  describe ":stas_attribute_change rebroadcast" do
    test "re-publishes to the token:<id> topic on a custom pubsub" do
      token_id = "ee" |> String.duplicate(20)

      start_supervised!({Phoenix.PubSub, name: :observer_test_pubsub})

      pid =
        start_supervised!(
          {StasObserver, [pubsub: :observer_test_pubsub]}
          |> Supervisor.child_spec(id: :observer_under_test)
        )

      :ok = Phoenix.PubSub.subscribe(:observer_test_pubsub, "token:#{token_id}")

      send(pid, {:stas_attribute_change, %{token_id: token_id, change: :supply_increased}})

      assert_receive {:attribute_changed, %{token_id: ^token_id, change: :supply_increased}}, 200
    end
  end
end
