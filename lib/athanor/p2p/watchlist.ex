defmodule Athanor.P2P.Watchlist do
  @moduledoc """
  Cheap `:ets` prefilter for P2P mempool ingest (Phase 3, T3.0).

  At P2P ingest rate we cannot run the full `TransactionFilter.matches?/1` on
  every announced tx, so this prefilter rejects the obviously-irrelevant first.
  It is **prefilter only** and a strict **superset** of `matches?/1`: it never
  drops a tx the indexer would include, and `matches?/1` remains the sole
  inclusion authority.

  `matches?/1` includes a tx on a watched **address** (P2PKH / P2MPKH hash160)
  **or** a watched **STAS/STAS3 token**. This prefilter therefore accepts a tx
  when **either**:

    1. an output's **hash160 8-byte prefix** is in the watched-address set, or
    2. an output is **STAS/STAS3-template-shaped** (a cheap script-shape check) —
       so every candidate token tx reaches `matches?/1`, which enforces the
       actual watched-token-ID membership.

  Over-accepting (prefix collisions, STAS outputs of unwatched tokens) is
  intentional: the prefilter's only job is to cheaply reject the irrelevant.
  """

  @prefix_bytes 8
  @stas_types [:stas, :stas_btg, :stas3]

  @typedoc "An `:ets` table id holding watched hash160 prefixes."
  @type t :: :ets.tid()

  @doc "Creates a new (empty) watchlist prefix table."
  @spec new() :: t()
  def new, do: :ets.new(:p2p_watchlist, [:set, :public])

  @doc """
  Adds a base58check address's hash160 prefix to the watchlist. Returns the
  table (for piping). Non-decodable addresses are ignored.
  """
  @spec put_address(t(), String.t()) :: t()
  def put_address(table, address) when is_binary(address) do
    case BSV.Base58.check_decode(address) do
      {:ok, {_version, <<h160::binary-20>>}} -> :ets.insert(table, {prefix(h160), true})
      _ -> :ok
    end

    table
  end

  @doc """
  Whether `tx` *might* be relevant — `true` if any output's hash160 prefix is
  watched or any output is STAS-shaped. A superset of `matches?/1`.
  """
  @spec maybe_relevant?(t(), BSV.Transaction.t()) :: boolean()
  def maybe_relevant?(table, %BSV.Transaction{outputs: outputs}) do
    Enum.any?(outputs, &output_relevant?(table, &1))
  end

  defp output_relevant?(table, %BSV.Transaction.Output{locking_script: script}) do
    bin = BSV.Script.to_binary(script)
    parsed = safe_read(bin)

    cond do
      parsed.script_type in @stas_types -> true
      true -> watched_hash160?(table, bin, parsed)
    end
  end

  # P2PKH: hash160 is the 20 bytes after OP_DUP OP_HASH160 PUSH20.
  defp watched_hash160?(table, <<0x76, 0xA9, 0x14, h160::binary-20, 0x88, 0xAC>>, _parsed),
    do: :ets.member(table, prefix(h160))

  # P2MPKH (STAS 3.0 §10.2): the MPKH sits at offset 3, like a P2PKH pubkey hash.
  defp watched_hash160?(table, <<_::binary-3, h160::binary-20, _::binary>>, %{
         script_type: :p2mpkh
       }),
       do: :ets.member(table, prefix(h160))

  defp watched_hash160?(_table, _bin, _parsed), do: false

  defp prefix(<<p::binary-size(@prefix_bytes), _::binary>>), do: p
  defp prefix(h) when is_binary(h), do: h

  # The token-script reader raises on malformed scripts; a prefilter must never
  # crash the ingest path, so treat unreadable scripts as not-STAS.
  defp safe_read(bin) do
    BSV.Tokens.Script.Reader.read_locking_script(bin)
  rescue
    _ -> %{script_type: :unknown}
  end
end
