defmodule Athanor.P2P.PeerPool do
  @moduledoc """
  Maintains a pool of N healthy outbound peers (Phase 2, T2.3).

  The pool is the `owner` of every `Peer` it starts, so it receives each peer's
  `{:peer, pid, :ready|:frame|:down, _}` lifecycle message. All dial-selection,
  /24-diversity, and cooldown decisions live in the pure `PeerPool.AddrBook`;
  this GenServer only does process lifecycle, registry updates, and a periodic
  refresh. Peers are started through an injected `peer_starter` (default
  `Peer.start_link/1`) and time through an injected `now_fun`, so the pool is
  fully testable without real sockets or wall-clock waits.

  Lifecycle:

    * **bootstrap** — resolve DNS seeds + fold in explicit `seeds` into the book,
      then fill to `target`.
    * **`:ready`** — promote the dial to live and register it in `PeerRegistry`.
    * **`:down`** — release/fail the address (negative cooldown), unregister, and
      refill to `target` (self-heal).
    * **refresh tick** — periodically retry filling (e.g. once cooldowns expire).
  """

  use GenServer

  alias Athanor.P2P.{Discovery, Peer, PeerRegistry}
  alias Athanor.P2P.PeerPool.{AddrBook, Config}

  @refresh_interval 30_000

  @doc false
  def child_spec(%Config{} = config) do
    %{id: __MODULE__, start: {__MODULE__, :start_link, [config]}, restart: :permanent}
  end

  @doc "Starts the pool for `config`."
  @spec start_link(Config.t()) :: GenServer.on_start()
  def start_link(%Config{} = config), do: GenServer.start_link(__MODULE__, config)

  @impl true
  def init(%Config{} = config) do
    peer_starter = config.peer_starter || (&Peer.start_link/1)
    resolver = config.resolver || (&default_resolver/1)
    now_fun = config.now_fun || (&default_now/0)

    candidates = Discovery.seed_candidates(config.network, resolver) ++ config.seeds
    book = AddrBook.new(config.target) |> AddrBook.add_candidates(candidates)

    state = %{
      config: config,
      book: book,
      peer_starter: peer_starter,
      now_fun: now_fun,
      pid_to_addr: %{}
    }

    schedule_refresh()
    {:ok, state, {:continue, :fill}}
  end

  @impl true
  def handle_continue(:fill, state), do: {:noreply, fill(state)}

  @impl true
  def handle_info({:peer, pid, :ready, _version}, state) do
    case Map.fetch(state.pid_to_addr, pid) do
      {:ok, addr} ->
        PeerRegistry.register(addr, pid)
        {:noreply, %{state | book: AddrBook.promote(state.book, addr)}}

      :error ->
        {:noreply, state}
    end
  end

  def handle_info({:peer, pid, :down, _reason}, state) do
    case Map.pop(state.pid_to_addr, pid) do
      {nil, _} ->
        {:noreply, state}

      {addr, pid_to_addr} ->
        PeerRegistry.unregister(addr)
        book = cool_down(state.book, addr, state.now_fun.(), state.config.cooldown_ms)
        {:noreply, fill(%{state | book: book, pid_to_addr: pid_to_addr})}
    end
  end

  # Application frames are ignored in this phase (addr-gossip absorption is T2.4).
  def handle_info({:peer, _pid, :frame, _frame}, state), do: {:noreply, state}

  def handle_info(:refresh, state) do
    schedule_refresh()
    {:noreply, fill(state)}
  end

  ## Internals

  # Dial every selection the address book hands us this tick.
  defp fill(state) do
    state.book
    |> AddrBook.dial_targets(state.now_fun.())
    |> Enum.reduce(state, &dial(&2, &1))
  end

  defp dial(state, {ip, port} = addr) do
    config = state.config

    peer_config = %Peer.Config{
      host: :inet.ntoa(ip),
      port: port,
      network: config.network,
      our_version: config.our_version,
      transport: config.transport,
      transport_opts: config.transport_opts,
      owner: self()
    }

    case state.peer_starter.(peer_config) do
      {:ok, pid} ->
        %{
          state
          | book: AddrBook.mark_dialing(state.book, addr),
            pid_to_addr: Map.put(state.pid_to_addr, pid, addr)
        }

      {:error, _reason} ->
        %{
          state
          | book: AddrBook.fail_dial(state.book, addr, state.now_fun.(), config.cooldown_ms)
        }
    end
  end

  # A dropped peer that was dialing → fail_dial; one that was live → release.
  defp cool_down(book, addr, now, cooldown_ms) do
    cond do
      MapSet.member?(book.dialing, addr) -> AddrBook.fail_dial(book, addr, now, cooldown_ms)
      Map.has_key?(book.live, addr) -> AddrBook.release(book, addr, now, cooldown_ms)
      true -> book
    end
  end

  defp schedule_refresh, do: Process.send_after(self(), :refresh, @refresh_interval)

  defp default_now, do: System.monotonic_time(:millisecond)

  # Default DNS resolver: resolve a seed hostname to its A records as ip4 tuples.
  defp default_resolver(host) do
    case :inet.getaddrs(String.to_charlist(host), :inet) do
      {:ok, addrs} -> {:ok, addrs}
      error -> error
    end
  end
end
