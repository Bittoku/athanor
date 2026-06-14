defmodule Athanor.Workers.StasObserver do
  @moduledoc """
  Watches for STAS token attribute changes (metadata updates,
  redemption events) by subscribing to PubSub token events
  and querying the chain for relevant changes.

  Two paths:

    * Periodic `:check_attributes` tick — calls `tally/0` to refresh
      per-token live-UTXO counts (currently logged; future: write to
      `WatchingToken.live_utxo_count`).
    * Inbound `{:stas_attribute_change, data}` messages on the
      `"stas:attributes"` topic — re-broadcast on `"token:<id>"` so
      individual wallet subscribers can react.
  """

  use GenServer
  require Logger

  alias Athanor.Repo
  alias Athanor.Schema.{Utxo, WatchingToken}
  import Ecto.Query

  @check_interval :timer.minutes(10)

  ## ── Client API ──

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Returns a map of `token_id => live_utxo_count` for every watched token.
  """
  @spec tally() :: %{String.t() => non_neg_integer()}
  def tally do
    WatchingToken
    |> Repo.all()
    |> Enum.into(%{}, fn token ->
      count =
        Utxo
        |> where([u], u.token_id == ^token.token_id and u.is_spent == false)
        |> select([u], count(u.id))
        |> Repo.one()

      {token.token_id, count}
    end)
  end

  ## ── Server Callbacks ──

  @impl true
  def init(opts) do
    pubsub = Keyword.get(opts, :pubsub, Athanor.PubSub)
    Phoenix.PubSub.subscribe(pubsub, "stas:attributes")
    schedule_check()
    {:ok, %{pubsub: pubsub}}
  end

  @impl true
  def handle_info(:check_attributes, state) do
    counts = tally()
    Logger.debug("StasObserver tally: #{inspect(counts)}")
    schedule_check()
    {:noreply, state}
  end

  def handle_info({:stas_attribute_change, %{token_id: token_id} = data}, state) do
    Logger.info("StasObserver: attribute change detected: #{inspect(data)}")

    Phoenix.PubSub.broadcast(
      state.pubsub,
      "token:#{token_id}",
      {:attribute_changed, data}
    )

    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  ## ── Private ──

  defp schedule_check do
    Process.send_after(self(), :check_attributes, @check_interval)
  end
end
