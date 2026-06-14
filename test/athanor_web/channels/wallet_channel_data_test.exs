defmodule AthanorWeb.WalletChannelDataTest do
  @moduledoc """
  Data-driven WalletChannel tests beyond the smoke-pass set in
  `wallet_channel_test.exs`. Seeds the DB so each handler exercises its
  full response shape, and pushes PubSub messages to verify the
  server→client forwarding handlers.
  """

  use AthanorWeb.ChannelCase, async: false

  alias Athanor.Repo
  alias Athanor.Schema.{Utxo, AddressHistory, MetaTransaction}

  @addr "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa"

  setup do
    {:ok, _, socket} =
      AthanorWeb.UserSocket
      |> socket("user_id", %{})
      |> subscribe_and_join(AthanorWeb.WalletChannel, "wallet:lobby")

    %{socket: socket}
  end

  defp insert_utxo!(attrs) do
    base = %{
      txid: :crypto.strong_rand_bytes(32),
      vout: 0,
      address: @addr,
      satoshis: 1000,
      script_hex: "76a90088ac",
      is_spent: false
    }

    {:ok, u} = %Utxo{} |> Utxo.changeset(Map.merge(base, attrs)) |> Repo.insert()
    u
  end

  describe "get_balance with real data" do
    test "returns full balance per requested address", %{socket: socket} do
      insert_utxo!(%{satoshis: 5000})
      insert_utxo!(%{satoshis: 250, token_type: "stas3", token_id: "tok-a"})

      ref = push(socket, "get_balance", %{"addresses" => [@addr]})

      assert_reply ref, :ok, %{balances: [entry]}
      assert entry.address == @addr
      assert entry.bsv === 5000
      assert [%{token_id: "tok-a", satoshis: 250, count: 1}] = entry.tokens
    end

    test "with token_ids: filters to just those tokens", %{socket: socket} do
      insert_utxo!(%{satoshis: 100, token_type: "stas3", token_id: "tok-a"})
      insert_utxo!(%{satoshis: 200, token_type: "stas3", token_id: "tok-b"})

      ref =
        push(socket, "get_balance", %{
          "addresses" => [@addr],
          "token_ids" => ["tok-a"]
        })

      assert_reply ref, :ok, %{balances: [entry]}
      assert [%{token_id: "tok-a"}] = entry.tokens
    end
  end

  describe "get_history with real data" do
    test "returns history entries with the documented shape", %{socket: socket} do
      {:ok, _} =
        %AddressHistory{}
        |> AddressHistory.changeset(%{
          address: @addr,
          txid: String.duplicate("ab", 32),
          direction: "in",
          satoshis: 4242,
          block_height: 800_000,
          timestamp: System.os_time(:second)
        })
        |> Repo.insert()

      ref = push(socket, "get_history", %{"address" => @addr})

      assert_reply ref, :ok, %{history: [entry]}
      assert entry.direction == "in"
      assert entry.satoshis == 4242
      assert entry.block_height == 800_000
    end
  end

  describe "get_utxo_set variants" do
    test "address only: returns all unspent UTXOs for the address", %{socket: socket} do
      insert_utxo!(%{satoshis: 100})
      insert_utxo!(%{satoshis: 200})

      ref = push(socket, "get_utxo_set", %{"address" => @addr})

      assert_reply ref, :ok, %{utxos: utxos}
      assert length(utxos) == 2
    end

    test "token only: returns all unspent UTXOs for the token across addresses", %{socket: socket} do
      insert_utxo!(%{satoshis: 100, token_type: "stas3", token_id: "tok-x", address: @addr})
      insert_utxo!(%{satoshis: 200, token_type: "stas3", token_id: "tok-x", address: "1Other"})

      ref = push(socket, "get_utxo_set", %{"token_id" => "tok-x"})

      assert_reply ref, :ok, %{utxos: utxos}
      assert length(utxos) == 2
    end

    test "token + address: returns only that address's UTXOs of that token", %{socket: socket} do
      insert_utxo!(%{satoshis: 100, token_type: "stas3", token_id: "tok-y", address: @addr})
      insert_utxo!(%{satoshis: 200, token_type: "stas3", token_id: "tok-y", address: "1Other"})

      ref = push(socket, "get_utxo_set", %{"token_id" => "tok-y", "address" => @addr})

      assert_reply ref, :ok, %{utxos: [u]}
      assert u.address == @addr
    end

    test "min satoshis filter: drops outputs below threshold", %{socket: socket} do
      insert_utxo!(%{satoshis: 100})
      insert_utxo!(%{satoshis: 500})
      insert_utxo!(%{satoshis: 1000})

      ref = push(socket, "get_utxo_set", %{"address" => @addr, "satoshis" => 400})

      assert_reply ref, :ok, %{utxos: utxos}
      assert length(utxos) == 2
      assert Enum.all?(utxos, &(&1.satoshis >= 400))
    end

    test "no filters: returns []", %{socket: socket} do
      insert_utxo!(%{satoshis: 100})

      ref = push(socket, "get_utxo_set", %{})

      assert_reply ref, :ok, %{utxos: []}
    end
  end

  describe "get_transactions" do
    test "returns the meta_transaction record when the txid is known", %{socket: socket} do
      txid_bin = :crypto.strong_rand_bytes(32)
      txid_hex = Base.encode16(txid_bin, case: :lower)

      {:ok, _} =
        %MetaTransaction{}
        |> MetaTransaction.changeset(%{
          txid: txid_bin,
          hex: "0100feed",
          timestamp: 1_700_000_000,
          is_confirmed: true,
          block_height: 800_000
        })
        |> Repo.insert()

      ref = push(socket, "get_transactions", %{"txids" => [txid_hex]})

      assert_reply ref, :ok, %{transactions: [t]}
      assert t.found == true
      assert t.is_confirmed == true
      assert t.hex == "0100feed"
      assert t.block_height == 800_000
    end

    test "marks unknown txid as found=false", %{socket: socket} do
      txid_hex = Base.encode16(:crypto.strong_rand_bytes(32), case: :lower)

      ref = push(socket, "get_transactions", %{"txids" => [txid_hex]})

      assert_reply ref, :ok, %{transactions: [%{found: false, txid: ^txid_hex}]}
    end

    test "marks unparseable txid hex as error=invalid_hex", %{socket: socket} do
      ref = push(socket, "get_transactions", %{"txids" => ["not-a-hex-string"]})

      assert_reply ref, :ok, %{transactions: [%{found: false, error: "invalid_hex"}]}
    end

    test "caps at 100 txids per call", %{socket: socket} do
      bigger_than_cap =
        Enum.map(1..150, fn _ ->
          Base.encode16(:crypto.strong_rand_bytes(32), case: :lower)
        end)

      ref = push(socket, "get_transactions", %{"txids" => bigger_than_cap})

      assert_reply ref, :ok, %{transactions: transactions}
      assert length(transactions) == 100
    end

    test "accepts a single txid string (not a list)", %{socket: socket} do
      txid_hex = Base.encode16(:crypto.strong_rand_bytes(32), case: :lower)

      ref = push(socket, "get_transactions", %{"txids" => txid_hex})

      assert_reply ref, :ok, %{transactions: [%{found: false}]}
    end
  end

  describe "broadcast error branches" do
    test "missing hex payload returns error=missing_hex", %{socket: socket} do
      ref = push(socket, "broadcast", %{})

      assert_reply ref, :error, %{reason: "missing_hex"}
    end
  end

  describe "PubSub push handlers (server → client)" do
    test "forwards :tx_found to client", %{socket: socket} do
      send(socket.channel_pid, {:tx_found, %{txid: "abc"}})
      assert_push "tx_found", %{txid: "abc"}
    end

    test "forwards :tx_deleted to client", %{socket: socket} do
      send(socket.channel_pid, {:tx_deleted, %{txid: "abc"}})
      assert_push "tx_deleted", %{txid: "abc"}
    end

    test "forwards :balance_changed to client", %{socket: socket} do
      send(socket.channel_pid, {:balance_changed, %{address: @addr, bsv: 100}})
      assert_push "balance_changed", %{address: @addr, bsv: 100}
    end

    test "ignores unknown messages without crashing", %{socket: socket} do
      send(socket.channel_pid, :random_message)
      # If the handler had crashed, the next push would fail. Smoke that.
      ref = push(socket, "subscribe", %{"address" => @addr})
      assert_reply ref, :ok, %{}
    end
  end

  describe "subscribe / unsubscribe" do
    test "subscribe stores the subscription in socket assigns", %{socket: socket} do
      ref = push(socket, "subscribe", %{"address" => @addr, "slim" => true})
      assert_reply ref, :ok, %{address: address}
      assert address == @addr
    end

    test "unsubscribe removes the subscription", %{socket: socket} do
      ref = push(socket, "subscribe", %{"address" => @addr})
      assert_reply ref, :ok, %{}

      ref = push(socket, "unsubscribe", %{"address" => @addr})
      assert_reply ref, :ok, %{address: address}
      assert address == @addr
    end
  end
end
