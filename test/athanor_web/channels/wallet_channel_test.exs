defmodule AthanorWeb.WalletChannelTest do
  use AthanorWeb.ChannelCase, async: true

  # `assert_reply`/`assert_receive` default to a 100ms timeout, which is too tight
  # for these DB-backed handlers under full-suite load (they flaked seed-dependently
  # with "no matching message after 100ms"). Replies are still effectively instant;
  # the generous timeout only removes the false negative under contention.
  @reply_timeout 1_000

  describe "join" do
    test "joins wallet:lobby successfully" do
      {:ok, _, socket} =
        AthanorWeb.UserSocket
        |> socket("user_id", %{})
        |> subscribe_and_join(AthanorWeb.WalletChannel, "wallet:lobby")

      assert socket.topic == "wallet:lobby"
    end

    test "joins wallet:{address} successfully" do
      {:ok, _, socket} =
        AthanorWeb.UserSocket
        |> socket("user_id", %{})
        |> subscribe_and_join(AthanorWeb.WalletChannel, "wallet:1ABC123")

      assert socket.topic == "wallet:1ABC123"
    end
  end

  describe "handle_in" do
    setup do
      {:ok, _, socket} =
        AthanorWeb.UserSocket
        |> socket("user_id", %{})
        |> subscribe_and_join(AthanorWeb.WalletChannel, "wallet:lobby")

      %{socket: socket}
    end

    test "subscribe replies ok", %{socket: socket} do
      ref = push(socket, "subscribe", %{"address" => "1TestAddr"})
      assert_reply ref, :ok, %{status: "subscribed"}, @reply_timeout
    end

    test "unsubscribe replies ok", %{socket: socket} do
      ref = push(socket, "unsubscribe", %{"address" => "1TestAddr"})
      assert_reply ref, :ok, %{status: "unsubscribed"}, @reply_timeout
    end

    test "get_balance replies ok", %{socket: socket} do
      ref = push(socket, "get_balance", %{})
      assert_reply ref, :ok, %{balances: []}, @reply_timeout
    end

    test "get_history replies ok", %{socket: socket} do
      ref = push(socket, "get_history", %{})
      assert_reply ref, :ok, %{history: []}, @reply_timeout
    end

    test "get_utxo_set replies ok", %{socket: socket} do
      ref = push(socket, "get_utxo_set", %{})
      assert_reply ref, :ok, %{utxos: []}, @reply_timeout
    end

    test "get_transactions replies ok", %{socket: socket} do
      ref = push(socket, "get_transactions", %{})
      assert_reply ref, :ok, %{transactions: []}, @reply_timeout
    end

    test "broadcast replies ok", %{socket: socket} do
      ref = push(socket, "broadcast", %{"hex" => "0100000001" <> String.duplicate("00", 50)})
      assert_reply ref, :ok, %{status: _, txid: _}, @reply_timeout
    end
  end
end
