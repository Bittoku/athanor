defmodule AthanorWeb.WalletChannel do
  @moduledoc """
  Phoenix Channel handling the "wallet:*" topic for real-time events.

  Supports per-address topics ("wallet:{address}") and a lobby ("wallet:lobby").

  ## Client → Server events:
    - subscribe, unsubscribe, get_balance, get_history,
      get_utxo_set, get_transactions, broadcast

  ## Server → Client events:
    - tx_found, tx_deleted, balance_changed
  """

  use AthanorWeb, :channel

  alias Athanor.Services.{Balance, AddressHistory, Broadcast}
  alias Athanor.Indexer.UtxoManager
  alias Athanor.Repo
  alias Athanor.Schema.MetaTransaction

  @impl true
  def join("wallet:" <> _subtopic, _payload, socket) do
    {:ok, socket}
  end

  # ── Client → Server handlers ──

  @impl true
  def handle_in("subscribe", %{"address" => address} = payload, socket) do
    slim = Map.get(payload, "slim", false)

    # Subscribe this socket to PubSub topics for this address
    Phoenix.PubSub.subscribe(Athanor.PubSub, "tx:#{address}")
    Phoenix.PubSub.subscribe(Athanor.PubSub, "balance:#{address}")

    # Track subscription in socket assigns
    subscriptions = Map.get(socket.assigns, :subscriptions, %{})
    subscriptions = Map.put(subscriptions, address, %{slim: slim})
    socket = assign(socket, :subscriptions, subscriptions)

    {:reply, {:ok, %{status: "subscribed", address: address}}, socket}
  end

  @impl true
  def handle_in("unsubscribe", %{"address" => address}, socket) do
    Phoenix.PubSub.unsubscribe(Athanor.PubSub, "tx:#{address}")
    Phoenix.PubSub.unsubscribe(Athanor.PubSub, "balance:#{address}")

    subscriptions = Map.get(socket.assigns, :subscriptions, %{})
    subscriptions = Map.delete(subscriptions, address)
    socket = assign(socket, :subscriptions, subscriptions)

    {:reply, {:ok, %{status: "unsubscribed", address: address}}, socket}
  end

  @impl true
  def handle_in("get_balance", payload, socket) do
    addresses = Map.get(payload, "addresses", [])
    token_ids = Map.get(payload, "token_ids", [])

    balances =
      Enum.map(addresses, fn address ->
        balance = Balance.get_full_balance(address)

        token_balances =
          if token_ids != [] do
            Balance.get_token_balances(address, token_ids)
          else
            balance.tokens
          end

        %{
          address: address,
          bsv: balance.bsv,
          tokens: token_balances
        }
      end)

    {:reply, {:ok, %{balances: balances}}, socket}
  end

  @impl true
  def handle_in("get_history", payload, socket) do
    address = Map.get(payload, "address", "")
    token_ids = Map.get(payload, "token_ids", [])
    desc = Map.get(payload, "desc", true)
    skip = Map.get(payload, "skip", 0)
    take = Map.get(payload, "take", 50)
    skip_zero = Map.get(payload, "skip_zero_balance", false)

    opts = [
      skip: skip,
      take: take,
      desc: desc,
      skip_zero_balance: skip_zero
    ]

    # If token_ids provided, query per token
    history =
      if token_ids != [] do
        Enum.flat_map(token_ids, fn token_id ->
          AddressHistory.list(address, Keyword.put(opts, :token_id, token_id))
        end)
      else
        AddressHistory.list(address, opts)
      end

    entries =
      Enum.map(history, fn h ->
        %{
          txid: h.txid,
          direction: h.direction,
          satoshis: h.satoshis,
          token_id: h.token_id,
          block_height: h.block_height,
          timestamp: h.timestamp
        }
      end)

    {:reply, {:ok, %{history: entries}}, socket}
  end

  @impl true
  def handle_in("get_utxo_set", payload, socket) do
    token_id = Map.get(payload, "token_id")
    address = Map.get(payload, "address")
    min_satoshis = Map.get(payload, "satoshis")

    utxos =
      cond do
        token_id && address ->
          UtxoManager.list_token_utxos(token_id, address)

        token_id ->
          UtxoManager.list_token_utxos(token_id)

        address ->
          UtxoManager.list_unspent(address)

        true ->
          []
      end

    utxos =
      if min_satoshis do
        Enum.filter(utxos, fn u -> u.satoshis >= min_satoshis end)
      else
        utxos
      end

    entries =
      Enum.map(utxos, fn u ->
        %{
          txid: Base.encode16(u.txid, case: :lower),
          vout: u.vout,
          address: u.address,
          satoshis: u.satoshis,
          token_id: u.token_id,
          token_type: u.token_type,
          script_hex: u.script_hex
        }
      end)

    {:reply, {:ok, %{utxos: entries}}, socket}
  end

  @impl true
  def handle_in("get_transactions", payload, socket) do
    txids =
      case Map.get(payload, "txids") do
        list when is_list(list) -> Enum.take(list, 100)
        str when is_binary(str) -> [str]
        _ -> []
      end

    transactions =
      Enum.map(txids, fn txid_hex ->
        case Base.decode16(txid_hex, case: :mixed) do
          {:ok, txid_binary} ->
            case Repo.get_by(MetaTransaction, txid: txid_binary) do
              nil ->
                %{txid: txid_hex, found: false}

              meta ->
                %{
                  txid: txid_hex,
                  found: true,
                  hex: meta.hex,
                  block_height: meta.block_height,
                  is_confirmed: meta.is_confirmed,
                  timestamp: meta.timestamp
                }
            end

          :error ->
            %{txid: txid_hex, found: false, error: "invalid_hex"}
        end
      end)

    {:reply, {:ok, %{transactions: transactions}}, socket}
  end

  @impl true
  def handle_in("broadcast", %{"hex" => raw_hex}, socket) do
    case Broadcast.broadcast_tx(raw_hex) do
      {:ok, broadcast} ->
        {:reply, {:ok, %{status: broadcast.status, txid: broadcast.txid}}, socket}

      {:error, changeset} ->
        {:reply, {:error, %{reason: "broadcast_failed", details: inspect(changeset)}}, socket}
    end
  end

  def handle_in("broadcast", _payload, socket) do
    {:reply, {:error, %{reason: "missing_hex"}}, socket}
  end

  # ── PubSub event handlers (Server → Client push) ──

  @impl true
  def handle_info({:tx_found, data}, socket) do
    push(socket, "tx_found", data)
    {:noreply, socket}
  end

  def handle_info({:tx_deleted, data}, socket) do
    push(socket, "tx_deleted", data)
    {:noreply, socket}
  end

  def handle_info({:balance_changed, data}, socket) do
    push(socket, "balance_changed", data)
    {:noreply, socket}
  end

  def handle_info(_msg, socket) do
    {:noreply, socket}
  end
end
