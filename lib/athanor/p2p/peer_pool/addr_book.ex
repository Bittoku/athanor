defmodule Athanor.P2P.PeerPool.AddrBook do
  @moduledoc """
  Pure address-book reducer for the peer pool (Phase 2, T2.0) — the "brain" that
  decides which addresses to dial next. No process and no IO; time is injected as
  `now_ms`, so dialing/diversity/cooldown decisions are fully unit-testable.

  ## State

    * `candidates` — known, dialable addresses (a `MapSet`).
    * `dialing` — addresses with an **in-flight** dial (handshake not yet
      complete). Tracked explicitly so the pool never double-dials an address or
      another address in the same /24 while a dial is pending.
    * `live` — `%{addr => meta}` of peers that completed the handshake.
    * `cooldown` — `%{addr => until_ms}`; an address is skipped until `now_ms`
      reaches its deadline (negative cooldown after a failure/drop).
    * `target` — desired number of healthy peers.

  An address is `{ip4 :: {a,b,c,d}, port}`; its **/24** is `{a, b, c}`. A "slot"
  counts both `live` and `dialing` peers (`used`), and diversity is enforced
  across the union of both — see `dial_targets/2`.

  ## Dial lifecycle

      candidate --mark_dialing--> dialing --promote--> live
                                       \\--fail_dial--> cooldown (back to candidate)
                              live --release--> cooldown (back to candidate)

  `fail_dial/4` and `release/4` return the address to `candidates` (so it can be
  retried after its cooldown) while recording the cooldown deadline.
  """

  @enforce_keys [:target]
  defstruct candidates: MapSet.new(),
            dialing: MapSet.new(),
            live: %{},
            cooldown: %{},
            target: 8

  @type ip4 :: {byte(), byte(), byte(), byte()}
  @type addr :: {ip4(), :inet.port_number()}
  @type slash24 :: {byte(), byte(), byte()}
  @type t :: %__MODULE__{
          candidates: MapSet.t(addr()),
          dialing: MapSet.t(addr()),
          live: %{addr() => map()},
          cooldown: %{addr() => integer()},
          target: pos_integer()
        }

  @doc "Builds an empty book targeting `target` healthy peers."
  @spec new(pos_integer()) :: t()
  def new(target) when is_integer(target) and target > 0, do: %__MODULE__{target: target}

  @doc "Unions `addrs` into the candidate set (dedup)."
  @spec add_candidates(t(), [addr()]) :: t()
  def add_candidates(%__MODULE__{} = book, addrs) do
    %{book | candidates: Enum.into(addrs, book.candidates)}
  end

  @doc "Moves `addr` `candidate → dialing`, reserving its address and /24."
  @spec mark_dialing(t(), addr()) :: t()
  def mark_dialing(%__MODULE__{} = book, addr) do
    %{
      book
      | candidates: MapSet.delete(book.candidates, addr),
        dialing: MapSet.put(book.dialing, addr)
    }
  end

  @doc "Moves `addr` `dialing → live` (handshake completed)."
  @spec promote(t(), addr()) :: t()
  def promote(%__MODULE__{} = book, addr) do
    %{book | dialing: MapSet.delete(book.dialing, addr), live: Map.put(book.live, addr, %{})}
  end

  @doc "Moves `addr` `dialing → cooldown` (dial errored / never reached ready)."
  @spec fail_dial(t(), addr(), integer(), non_neg_integer()) :: t()
  def fail_dial(%__MODULE__{} = book, addr, now_ms, cooldown_ms) do
    %{
      book
      | dialing: MapSet.delete(book.dialing, addr),
        candidates: MapSet.put(book.candidates, addr),
        cooldown: Map.put(book.cooldown, addr, now_ms + cooldown_ms)
    }
  end

  @doc "Moves `addr` `live → cooldown` (a ready peer dropped)."
  @spec release(t(), addr(), integer(), non_neg_integer()) :: t()
  def release(%__MODULE__{} = book, addr, now_ms, cooldown_ms) do
    %{
      book
      | live: Map.delete(book.live, addr),
        candidates: MapSet.put(book.candidates, addr),
        cooldown: Map.put(book.cooldown, addr, now_ms + cooldown_ms)
    }
  end

  @doc "The set of /24s occupied by a live or dialing peer."
  @spec occupied_slash24s(t()) :: MapSet.t(slash24())
  def occupied_slash24s(%__MODULE__{} = book) do
    (Map.keys(book.live) ++ MapSet.to_list(book.dialing))
    |> Enum.map(&slash24/1)
    |> MapSet.new()
  end

  @doc """
  Selects up to `target - used` addresses to dial (`used = live + dialing`),
  excluding addresses that are live, dialing, or on active cooldown, and never
  returning two addresses in the same /24 or one whose /24 is already occupied.
  """
  @spec dial_targets(t(), integer()) :: [addr()]
  def dial_targets(%__MODULE__{} = book, now_ms) do
    slots = book.target - (map_size(book.live) + MapSet.size(book.dialing))

    if slots <= 0 do
      []
    else
      book.candidates
      |> MapSet.to_list()
      |> Enum.filter(&eligible?(&1, book, now_ms))
      |> pick_distinct_slash24(occupied_slash24s(book), slots)
    end
  end

  defp eligible?(addr, book, now_ms) do
    not Map.has_key?(book.live, addr) and
      not MapSet.member?(book.dialing, addr) and
      not on_cooldown?(addr, book, now_ms)
  end

  defp on_cooldown?(addr, book, now_ms) do
    case Map.get(book.cooldown, addr) do
      nil -> false
      until_ms -> until_ms > now_ms
    end
  end

  # Greedily take addresses with /24s not yet occupied/picked, up to `slots`.
  defp pick_distinct_slash24(addrs, occupied24, slots) do
    {picked, _seen} =
      Enum.reduce(addrs, {[], occupied24}, fn addr, {acc, seen} = state ->
        s24 = slash24(addr)

        cond do
          length(acc) >= slots -> state
          MapSet.member?(seen, s24) -> state
          true -> {[addr | acc], MapSet.put(seen, s24)}
        end
      end)

    Enum.reverse(picked)
  end

  defp slash24({{a, b, c, _d}, _port}), do: {a, b, c}
end
