defmodule Athanor.Indexer.B2gResolver do
  @moduledoc """
  Back-to-Genesis resolver — walks the input chain of a STAS token output
  back to the genesis issuance transaction to verify provenance.

  Strategy:
  1. Look up the UTXO locally
  2. Walk backwards through inputs, checking each for STAS token type
  3. For each step, try local DB first, then RPC, then WoC fallback
  4. Genesis is reached when a non-STAS input is found (the issuance tx)
  """

  require Logger

  alias Athanor.Repo
  alias Athanor.Schema.MetaTransaction
  alias Athanor.Blockchain.{RpcClient, JungleBusClient}
  alias Athanor.Infra.WhatsOnChain
  alias Athanor.P2P.{SourceRouter, TxFetcher}
  alias Athanor.Tokens.Classifier

  @max_depth 1000

  @doc """
  Resolves the provenance chain for a given STAS UTXO.

  ## Parameters
    - `txid` — transaction ID (binary or hex string)
    - `vout` — output index

  ## Returns
    `{:ok, chain}` where chain is a list of `{txid_hex, vout}` from tip to genesis,
    or `{:error, reason}`
  """
  def resolve(txid, vout, opts \\ []) do
    txid_hex = normalize_txid(txid)
    walk_chain(txid_hex, vout, [], 0, opts)
  end

  ## ── Private ──

  defp walk_chain(_txid_hex, _vout, _chain, depth, _opts) when depth >= @max_depth do
    {:error, :max_depth_exceeded}
  end

  defp walk_chain(txid_hex, vout, chain, depth, opts) do
    chain = [{txid_hex, vout} | chain]

    case fetch_tx(txid_hex, opts) do
      {:ok, tx} ->
        # Check if this output is a STAS token
        outputs = tx.outputs
        output = Enum.at(outputs, vout)

        if output do
          script_binary = BSV.Script.to_binary(output.locking_script)
          script_type = Classifier.classify(script_binary)

          if script_type in [:stas, :stas_btg, :stas3] do
            # Walk back through the corresponding input
            # STAS tokens typically spend input at same index or index 0
            input_idx = min(vout, length(tx.inputs) - 1)
            input = Enum.at(tx.inputs, input_idx)

            if input do
              prev_txid = Base.encode16(input.source_txid, case: :lower)
              prev_vout = input.source_tx_out_index
              walk_chain(prev_txid, prev_vout, chain, depth + 1, opts)
            else
              # No input — this IS genesis
              {:ok, Enum.reverse(chain)}
            end
          else
            # Non-STAS output — this is genesis (issuance tx)
            {:ok, Enum.reverse(chain)}
          end
        else
          {:error, :output_not_found}
        end

      {:error, reason} ->
        Logger.warning(
          "B2G resolve failed at #{txid_hex}:#{vout} depth=#{depth}: #{inspect(reason)}"
        )

        {:error, reason}
    end
  end

  defp fetch_tx(txid_hex, opts) do
    # Try local DB first
    case fetch_local(txid_hex) do
      {:ok, tx} -> {:ok, tx}
      {:error, _} -> fetch_remote(txid_hex, opts)
    end
  end

  defp fetch_local(txid_hex) do
    case Base.decode16(txid_hex, case: :mixed) do
      {:ok, txid_binary} ->
        case Repo.get_by(MetaTransaction, txid: txid_binary) do
          %MetaTransaction{hex: hex} when hex != nil ->
            case BSV.Transaction.from_hex(hex) do
              {:ok, tx} -> {:ok, tx}
              {:error, reason} -> {:error, reason}
            end

          _ ->
            {:error, :not_found}
        end

      :error ->
        {:error, :invalid_txid}
    end
  end

  # Route the parent-tx fetch through the capability router (Phase 5 §B):
  # `:raw_tx_fetch` = P2P primary (a mempool `getdata` fast path) then the
  # RPC → JungleBus → WhatsOnChain cascade, in that order. A P2P hit skips REST;
  # a P2P miss (or zero peers — the cold-start gate inside `SourceRouter.route`)
  # falls through to exactly the previous cascade. Providers are injectable for
  # tests; the production defaults preserve today's REST/RPC behavior byte-for-byte.
  defp fetch_remote(txid_hex, opts) do
    providers = Keyword.get(opts, :providers, default_providers())
    route_opts = Keyword.take(opts, [:p2p_available?])

    case SourceRouter.route(
           :raw_tx_fetch,
           fn p -> Map.fetch!(providers, p).(txid_hex) end,
           route_opts
         ) do
      {:ok, tx} -> {:ok, tx}
      {:error, _} = err -> err
      :miss -> {:error, :not_found}
    end
  end

  # Production provider seams, each `(txid_hex -> {:ok, %BSV.Transaction{}} | :miss
  # | {:error, reason})`. The REST/RPC seams are unchanged from the prior cascade.
  defp default_providers do
    %{
      p2p: &p2p_fetch/1,
      rpc: fn txid_hex -> from_hex_client(RpcClient.get_raw_transaction(txid_hex, false)) end,
      junglebus: fn txid_hex -> from_hex_client(JungleBusClient.get_raw_transaction(txid_hex)) end,
      whatsonchain: fn txid_hex -> from_hex_client(WhatsOnChain.get_raw_tx(txid_hex)) end
    }
  end

  # Pull the parent from a peer's mempool by txid. Within `walk_chain`, `txid_hex`
  # is **wire/internal order** (it comes from `input.source_txid`, which is wire
  # order — same convention as `Transaction.txid_binary/1`), so it decodes
  # directly to the wire txid the `inv`/`getdata` protocol uses; no reversal. The
  # fetcher's forgery guard makes this fail-safe: a wrong-order or absent txid can
  # only ever return `:miss` (never wrong bytes), so we harmlessly fall back to
  # REST. (NOTE for review: the existing REST/RPC seams are passed this same
  # wire-order `txid_hex` unchanged — a pre-existing display-vs-wire question in
  # the parent-fetch path that this MR deliberately does not alter.)
  defp p2p_fetch(txid_hex) do
    case Base.decode16(txid_hex, case: :mixed) do
      {:ok, wire_txid} -> p2p_fetch_wire(wire_txid)
      :error -> :miss
    end
  end

  # `TxFetcher.fetch/3` is a `GenServer.call`, which **exits** the caller if the
  # fetcher is absent (mid supervisor-restart, name not yet registered) or the
  # call times out. We must degrade to the REST/RPC cascade, not crash the b2g
  # walk — so normalize any exit/exception to `:miss` (no P2P data) here, before
  # the result reaches `SourceRouter.route/3`. This is what makes the P2P path
  # genuinely fail-closed.
  defp p2p_fetch_wire(wire_txid) do
    case TxFetcher.fetch(TxFetcher, wire_txid) do
      {:ok, raw_bin} -> parse_bin(raw_bin)
      :miss -> :miss
    end
  rescue
    _ -> :miss
  catch
    :exit, _ -> :miss
  end

  defp from_hex_client({:ok, raw_hex}), do: parse_hex(raw_hex)
  defp from_hex_client({:error, _} = err), do: err

  defp parse_hex(hex) do
    case BSV.Transaction.from_hex(hex) do
      {:ok, tx} -> {:ok, tx}
      {:error, reason} -> {:error, reason}
    end
  end

  defp parse_bin(bin) do
    case BSV.Transaction.from_binary(bin) do
      {:ok, tx, _rest} -> {:ok, tx}
      {:error, reason} -> {:error, reason}
    end
  end

  defp normalize_txid(txid) when is_binary(txid) and byte_size(txid) == 32 do
    Base.encode16(txid, case: :lower)
  end

  defp normalize_txid(txid) when is_binary(txid), do: String.downcase(txid)
end
