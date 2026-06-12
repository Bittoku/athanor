defmodule Athanor.P2P.PeerRegistry do
  @moduledoc """
  A monitor-backed registry of live peers keyed by `{ip, port}` (Phase 2, T2.1).

  Unlike a bare `Registry` (whose entries are owned by the *calling* process and
  would survive a peer's death when the pool registers child pids), this is a
  small `GenServer` that `Process.monitor`s each registered peer and removes it
  on `:DOWN`. That keeps the `Peer` GenServer ignorant of the registry
  (preserving the Phase 1 owner-indirection seam) while guaranteeing
  `addresses/0` and `slash24s/0` never report a dead peer.

  Keys are unique: registering an already-taken `{ip, port}` returns
  `{:error, :already_registered}`.

  Most functions take the server as their first argument (defaulting to this
  module's name, the singleton used in production); tests start an unnamed
  instance and pass its pid.
  """

  use GenServer

  @type addr :: {{byte(), byte(), byte(), byte()}, :inet.port_number()}
  @type slash24 :: {byte(), byte(), byte()}

  @doc """
  Starts the registry. Pass `name:` to register a process name (production uses
  the module name); omit it for an anonymous instance (tests).
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    {name, opts} = Keyword.pop(opts, :name)
    GenServer.start_link(__MODULE__, opts, if(name, do: [name: name], else: []))
  end

  @doc "Registers `pid` under `addr`. `{:error, :already_registered}` if taken."
  @spec register(GenServer.server(), addr(), pid()) :: :ok | {:error, :already_registered}
  def register(server \\ __MODULE__, addr, pid),
    do: GenServer.call(server, {:register, addr, pid})

  @doc "Removes `addr` (no-op if absent)."
  @spec unregister(GenServer.server(), addr()) :: :ok
  def unregister(server \\ __MODULE__, addr), do: GenServer.call(server, {:unregister, addr})

  @doc "Looks up the pid registered for `addr`."
  @spec lookup(GenServer.server(), addr()) :: {:ok, pid()} | :error
  def lookup(server \\ __MODULE__, addr), do: GenServer.call(server, {:lookup, addr})

  @doc "All live peer addresses."
  @spec addresses(GenServer.server()) :: [addr()]
  def addresses(server \\ __MODULE__), do: GenServer.call(server, :addresses)

  @doc "The set of /24s occupied by live peers (for diversity checks)."
  @spec slash24s(GenServer.server()) :: MapSet.t(slash24())
  def slash24s(server \\ __MODULE__) do
    server |> addresses() |> Enum.map(fn {{a, b, c, _d}, _p} -> {a, b, c} end) |> MapSet.new()
  end

  ## Server

  @impl true
  def init(_opts), do: {:ok, %{by_addr: %{}, by_pid: %{}}}

  @impl true
  def handle_call({:register, addr, pid}, _from, state) do
    cond do
      Map.has_key?(state.by_addr, addr) ->
        {:reply, {:error, :already_registered}, state}

      # A pid maps to exactly one address. Allowing a pid under a second address
      # would let the first address survive the pid's death (`by_pid` is keyed by
      # pid, so the :DOWN handler only sees the latest entry).
      Map.has_key?(state.by_pid, pid) ->
        {:reply, {:error, :pid_already_registered}, state}

      true ->
        ref = Process.monitor(pid)

        state = %{
          state
          | by_addr: Map.put(state.by_addr, addr, pid),
            by_pid: Map.put(state.by_pid, pid, {addr, ref})
        }

        {:reply, :ok, state}
    end
  end

  def handle_call({:unregister, addr}, _from, state), do: {:reply, :ok, remove_addr(state, addr)}

  def handle_call({:lookup, addr}, _from, state) do
    {:reply, Map.fetch(state.by_addr, addr), state}
  end

  def handle_call(:addresses, _from, state), do: {:reply, Map.keys(state.by_addr), state}

  @impl true
  def handle_info({:DOWN, _ref, :process, pid, _reason}, state) do
    case Map.fetch(state.by_pid, pid) do
      {:ok, {addr, _ref}} -> {:noreply, remove_addr(state, addr)}
      :error -> {:noreply, state}
    end
  end

  defp remove_addr(state, addr) do
    case Map.fetch(state.by_addr, addr) do
      {:ok, pid} ->
        case Map.fetch(state.by_pid, pid) do
          {:ok, {_addr, ref}} -> Process.demonitor(ref, [:flush])
          :error -> :ok
        end

        %{state | by_addr: Map.delete(state.by_addr, addr), by_pid: Map.delete(state.by_pid, pid)}

      :error ->
        state
    end
  end
end
