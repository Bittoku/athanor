defmodule Athanor.Indexer.TransactionProcessor do
  @moduledoc """
  Core indexing pipeline: receives filtered transactions, parses outputs,
  classifies them (P2PKH/STAS/STAS3), updates the UTXO set in Postgres,
  records address history, and publishes events via PubSub.

  Pipeline: filter → parse → classify → store → notify
  """

  use GenServer
  require Logger

  import Ecto.Query

  alias Athanor.Repo
  alias Athanor.Schema.{MetaTransaction, AddressHistory, WatchingToken, WatchingAddress, Utxo}
  alias Athanor.Indexer.UtxoManager
  alias Athanor.Tokens.{Classifier, SpendType, Stas3Meta}
  alias BSV.Tokens.Script.Reader, as: ScriptReader
  alias BSV.Transaction

  ## ── Client API ──

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Synchronously process a transaction (for block processing). `source` tags the
  observation's provider (`:p2p | :zmq | :junglebus | :whatsonchain | :bitails |
  :block | :unknown`); it is recorded in `MetaTransaction.metadata["sources"]`.
  """
  def process_tx(tx, matched_addresses, matched_tokens, source \\ :unknown) do
    GenServer.call(
      __MODULE__,
      {:process_tx, tx, matched_addresses, matched_tokens, source},
      30_000
    )
  end

  @doc """
  Index a matched transaction **in the calling process** (note-1065). Unlike
  `process_tx/4` (a cross-process `GenServer.call` whose Repo writes land on the
  TransactionProcessor's own connection), this runs the full indexing pipeline on the
  **caller's** connection — so block processing can wrap this and the
  `block_process_contexts` insert in ONE `Repo.transaction` and have a failed context
  insert roll back the MetaTransaction/UTXO/address-history side effects too. Returns
  `{:ok, txid_hex} | {:error, reason}`. The GenServer `process_tx/4` / `{:index_tx}`
  cast remain for realtime (mempool) indexing, where per-block atomicity is not needed.
  """
  def index_tx(tx, matched_addresses, matched_tokens, source \\ :unknown) do
    do_index_tx(tx, matched_addresses, matched_tokens, nil, source)
  end

  ## ── Server Callbacks ──

  @impl true
  def init(_opts) do
    {:ok, %{processed_count: 0}}
  end

  @impl true
  def handle_cast({:index_tx, tx, matched_addresses, matched_tokens, source}, state) do
    do_index_tx(tx, matched_addresses, matched_tokens, nil, source)
    {:noreply, %{state | processed_count: state.processed_count + 1}}
  end

  # Back-compat: a 4-tuple cast (no source) defaults to :unknown.
  def handle_cast({:index_tx, tx, matched_addresses, matched_tokens}, state),
    do: handle_cast({:index_tx, tx, matched_addresses, matched_tokens, :unknown}, state)

  @impl true
  def handle_call({:process_tx, tx, matched_addresses, matched_tokens, source}, _from, state) do
    result = do_index_tx(tx, matched_addresses, matched_tokens, nil, source)
    {:reply, result, %{state | processed_count: state.processed_count + 1}}
  end

  # Back-compat: a 4-tuple call (no source) defaults to :unknown.
  def handle_call({:process_tx, tx, matched_addresses, matched_tokens}, from, state),
    do: handle_call({:process_tx, tx, matched_addresses, matched_tokens, :unknown}, from, state)

  @impl true
  def handle_info(_msg, state) do
    {:noreply, state}
  end

  ## ── Private ──

  # Unions a new `source` into the existing `metadata["sources"]` set (or seeds
  # it on first sight). Order-independent; deduped.
  defp merge_sources(nil, source), do: [to_string(source)]

  defp merge_sources(%MetaTransaction{metadata: metadata}, source) do
    existing = (is_map(metadata) && Map.get(metadata, "sources")) || []
    Enum.uniq(existing ++ [to_string(source)])
  end

  defp do_index_tx(tx, matched_addresses, matched_tokens, block_height, source) do
    txid_binary = Transaction.txid_binary(tx)
    txid_hex = Transaction.tx_id_hex(tx)

    # Source tagging + dedupe (plan §2.5): first observation indexes fully;
    # a re-observation of the SAME txid (e.g. seen via REST then P2P) must only
    # union its `source` into `metadata["sources"]` and return — WITHOUT
    # replaying first-index side effects (input spends, UTXO creation,
    # address-history rows, PubSub, waiter reprocessing) and WITHOUT touching
    # the confirmation columns (`is_confirmed`/`block_height`), which are owned
    # by the block processor. First-seen wins for indexing; duplicates only add
    # their source. No schema migration.
    case Repo.get_by(MetaTransaction, txid: txid_binary) do
      %MetaTransaction{} = existing ->
        union_source_only(existing, source)
        {:ok, txid_hex}

      nil ->
        index_new_tx(
          tx,
          matched_addresses,
          matched_tokens,
          block_height,
          source,
          txid_binary,
          txid_hex
        )
    end
  end

  # Re-observation path. Union `source` into the existing row's
  # `metadata["sources"]` set and persist nothing else.
  defp union_source_only(%MetaTransaction{} = existing, source) do
    new_metadata = Map.put(existing.metadata || %{}, "sources", merge_sources(existing, source))

    existing
    |> MetaTransaction.changeset(%{metadata: new_metadata})
    |> Repo.update()
  end

  # First-index path: the full pipeline (meta insert → input spends → UTXO
  # creation → address history → PubSub → waiter reprocessing).
  defp index_new_tx(
         tx,
         matched_addresses,
         matched_tokens,
         block_height,
         source,
         txid_binary,
         txid_hex
       ) do
    tx_hex = Transaction.to_hex(tx)

    # Classify all outputs
    classified = Classifier.classify_outputs(tx)

    # Collect all involved addresses and token IDs
    all_addresses = extract_all_addresses(tx)
    all_token_ids = extract_all_token_ids(classified)

    # Compute STAS lineage attributes — the five-flag set + per-output
    # token-id decisions ported from dxs-consigliere's `UpdateStasAttributesQuery`
    # patch. MUST run before MetaTransaction insert (flags persisted on
    # `metadata`) and before output indexing (`token_id_per_vout` overrides
    # the naive script-derived tag).
    stas_attrs = compute_stas_attributes(tx, classified)

    # 1. Store MetaTransaction (first sight). Seed `metadata["sources"]` with the
    # observing source.
    meta_attrs = %{
      txid: txid_binary,
      hex: tx_hex,
      block_height: block_height,
      is_confirmed: block_height != nil,
      timestamp: System.os_time(:second),
      addresses: all_addresses,
      token_ids: all_token_ids,
      metadata: Map.put(stas_attrs.flags, "sources", merge_sources(nil, source))
    }

    # Check + propagate the first-sight insert (note-1065): a failed insert must
    # surface so a caller running this inside a transaction (block recording) fails
    # closed and rolls the whole per-block mutation back, rather than continuing the
    # side-effect pipeline against a poisoned transaction.
    case %MetaTransaction{}
         |> MetaTransaction.changeset(meta_attrs)
         |> Repo.insert() do
      {:error, changeset} ->
        {:error, {:meta_insert_failed, changeset}}

      {:ok, _meta} ->
        index_tx_effects(
          tx,
          classified,
          stas_attrs,
          matched_addresses,
          matched_tokens,
          block_height,
          txid_binary,
          txid_hex
        )
    end
  end

  # The first-sight side-effect pipeline (input spends → UTXO creation → address
  # history → PubSub → waiter reprocessing), run only AFTER a successful
  # MetaTransaction insert. Split out of `index_new_tx/7` (note-1065) so a failed
  # meta insert fails closed without running these effects.
  defp index_tx_effects(
         tx,
         classified,
         stas_attrs,
         matched_addresses,
         matched_tokens,
         block_height,
         txid_binary,
         txid_hex
       ) do
    # 2. Process inputs — mark UTXOs as spent. STAS 3.0 spec v0.1 §8.2 / §9.6:
    # the unlocking script encodes a `spendType` byte that classifies the
    # operation (transfer / freeze_unfreeze / confiscation / swap_cancel).
    # Record this on the spent UTXO so downstream queries can recover the
    # operation class. Non-STAS-3.0 inputs are skipped — the parser only
    # runs against confirmed STAS 3.0 unlocks.
    spend_op = stas3_spend_op(tx)

    Enum.each(tx.inputs, fn input ->
      unless Transaction.is_coinbase?(tx) do
        source_txid = input.source_txid
        UtxoManager.spend_utxo(source_txid, input.source_tx_out_index, txid_binary)

        if spend_op do
          UtxoManager.set_stas3_op(source_txid, input.source_tx_out_index, spend_op)
        end
      end
    end)

    # 3. Process outputs — create new UTXOs. For STAS 3.0 outputs whose
    # protoID matches a watched issuance, validate the post-OP_RETURN
    # payload (spec §4) byte-identity invariant and seed the canonical
    # bytes / service-field authorities (spec §5.2.3) on first sight.
    Enum.each(classified, fn {vout, script_type, token_id} ->
      output = Enum.at(tx.outputs, vout)
      script_binary = BSV.Script.to_binary(output.locking_script)

      address = script_address(output.locking_script)

      token_type =
        case script_type do
          :stas -> "stas"
          :stas_btg -> "stas"
          :stas3 -> "stas3"
          _ -> nil
        end

      tampered? =
        script_type == :stas3 and
          stas3_post_op_return_tampered?(token_id, script_binary)

      cond do
        tampered? ->
          Logger.warning(
            "STAS3 post-OP_RETURN mismatch for protoID=#{token_id} tx=#{txid_hex} vout=#{vout}; skipping output indexing per spec §4 byte-identity invariant"
          )

        true ->
          # `token_id` from `Classifier.classify_outputs/1` is the protoID
          # the SCRIPT self-asserts. The lineage-checked tag computed in
          # `compute_stas_attributes/2` (issuance gate + transfer
          # inheritance + illegal-root taint) overrides it. If the script
          # isn't STAS-templated, `token_id_per_vout[vout]` is `nil`.
          effective_token_id = Map.get(stas_attrs.token_id_per_vout, vout)

          utxo_attrs = %{
            txid: txid_binary,
            vout: vout,
            address: address,
            satoshis: output.satoshis,
            script_hex: Base.encode16(script_binary, case: :lower),
            token_id: effective_token_id,
            token_type: token_type,
            is_spent: false,
            block_height: block_height
          }

          case UtxoManager.create_utxo(utxo_attrs) do
            {:ok, _} ->
              :ok

            {:error, %Ecto.Changeset{} = cs} ->
              Logger.warning(
                "UTXO insert rejected for #{txid_hex} vout=#{vout}: " <>
                  inspect(cs.errors)
              )
          end

          if script_type == :stas3 do
            seed_watching_token_metadata(token_id, script_binary)
          end
      end
    end)

    # 4. Record address history. One directional row per watched address
    # involved in the tx — received (`in`, output side) or sent (`out`,
    # input side). The filter's `matched_addresses` only covers P2PKH
    # outputs, so we additionally resolve:
    #   * STAS 3.0 owner addresses on outputs (filter can't extract them)
    #   * input-side senders from the spent UTXOs (filter never scans inputs)
    # `matched_addresses` is kept as a trusted seed so the set is never
    # smaller than what the filter already matched.
    spent_utxos = stas_attrs.parent_outputs |> Map.values() |> Enum.reject(&is_nil/1)
    watched = watched_address_set()

    extra_addresses =
      tx.outputs
      |> Enum.map(fn o -> script_address(o.locking_script) end)
      |> Enum.concat(Enum.map(spent_utxos, & &1.address))
      |> Enum.reject(&is_nil/1)
      |> Enum.filter(&MapSet.member?(watched, &1))

    history_addresses = Enum.uniq(matched_addresses ++ extra_addresses)

    Enum.each(history_addresses, fn address ->
      direction = determine_direction(tx, address)
      {satoshis, token_id} = address_flow(tx, address, direction, spent_utxos, stas_attrs)

      history_attrs = %{
        address: address,
        txid: txid_hex,
        direction: direction,
        satoshis: satoshis,
        token_id: token_id,
        block_height: block_height,
        timestamp: System.os_time(:second)
      }

      %AddressHistory{}
      |> AddressHistory.changeset(history_attrs)
      |> Repo.insert()
    end)

    # 5. Publish events via PubSub
    Enum.each(matched_addresses, fn address ->
      Phoenix.PubSub.broadcast(
        Athanor.PubSub,
        "tx:#{address}",
        {:tx_found, %{txid: txid_hex, address: address}}
      )

      Phoenix.PubSub.broadcast(
        Athanor.PubSub,
        "balance:#{address}",
        {:balance_changed, %{address: address}}
      )
    end)

    Logger.info(
      "Indexed tx #{txid_hex} (#{length(matched_addresses)} addrs, #{length(matched_tokens)} tokens)"
    )

    # If any prior tx was deferred waiting on this txid as a parent,
    # re-run their lineage now. Cascades transitively for chains.
    reprocess_waiters(txid_hex)

    {:ok, txid_hex}
  end

  # Find every MetaTransaction whose `metadata["missing_transactions"]`
  # contains `parent_txid_hex`, re-run lineage, persist the updated
  # flags + per-output token_id decisions, and cascade for any waiter
  # whose missing-parent list has now been fully resolved.
  #
  # Port of dxs-consigliere's `StasAttributesMissingTransactions`
  # background task (see `BackgroundTasks/StasAttributesMissingTransactions.cs`).
  # The C# version polls every 30 s; we trigger inline because our
  # Postgres backend lets us read parent state synchronously without
  # waiting on Raven's eventually-consistent map-reduce index.
  defp reprocess_waiters(parent_txid_hex) do
    # Use jsonb_build_array on the server side. Passing the JSON-encoded
    # array via a parameter and casting `::jsonb` was hitting a Postgrex
    # parameter-type edge case where the operator would resolve against
    # a non-jsonb LHS — `jsonb_build_array(text)` keeps the cast inside
    # the database.
    waiters =
      Repo.all(
        from m in MetaTransaction,
          where:
            fragment(
              "? -> 'missing_transactions' @> jsonb_build_array(?::text)",
              m.metadata,
              ^parent_txid_hex
            )
      )

    Enum.each(waiters, fn waiter -> reindex_lineage(waiter) end)
  end

  defp reindex_lineage(%MetaTransaction{hex: nil}), do: :ok

  defp reindex_lineage(%MetaTransaction{txid: txid_bin, hex: tx_hex} = waiter) do
    with {:ok, tx} <- BSV.Transaction.from_hex(tx_hex) do
      classified = Classifier.classify_outputs(tx)
      stas_attrs = compute_stas_attributes(tx, classified)

      previously_deferred? =
        not Map.get(waiter.metadata || %{}, "all_stas_inputs_known", true)

      now_resolved? = stas_attrs.flags["all_stas_inputs_known"] == true

      # Recompute the lineage flags but PRESERVE unrelated metadata keys —
      # especially `"sources"` (Phase 3 §A). Fresh flags win on conflicting
      # keys (`is_stas`, `illegal_roots`, `missing_transactions`, …); the
      # source set and any other non-flag keys survive.
      merged_metadata = Map.merge(waiter.metadata || %{}, stas_attrs.flags)

      waiter
      |> MetaTransaction.changeset(%{metadata: merged_metadata})
      |> Repo.update()

      Enum.each(stas_attrs.token_id_per_vout, fn {vout, new_token_id} ->
        case Repo.get_by(Utxo, txid: txid_bin, vout: vout) do
          nil ->
            :ok

          %Utxo{token_id: ^new_token_id} ->
            :ok

          utxo ->
            utxo
            |> Utxo.changeset(%{token_id: new_token_id})
            |> Repo.update()
        end
      end)

      # Cascade: if this waiter was previously deferred and is now
      # resolved, anything waiting on it can also be reprocessed.
      if previously_deferred? and now_resolved? do
        reprocess_waiters(display_hex(txid_bin))
      end

      :ok
    else
      _ -> :ok
    end
  end

  # Port of dxs-consigliere's `UpdateStasAttributesQuery` (TransactionStore.cs
  # §81-188). Computes the five-flag lineage set + per-output token_id
  # decisions in a single pass over the tx, using ONLY direct-parent
  # lookups (no back-to-genesis walking).
  #
  #   * `is_stas` — any STAS input or any STAS output
  #   * `is_issue` — has STAS outputs AND zero STAS inputs (issuance candidate)
  #   * `is_valid_issue` — `is_issue` ∧ all inputs known ∧ single protoID
  #     across outputs ∧ that protoID == HASH160(Vin[0]'s spent output address)
  #   * `all_stas_inputs_known` — every STAS-typed parent UTXO is locally
  #     indexed (or there are no STAS inputs)
  #   * `illegal_roots` — set of ancestor txids that failed `is_valid_issue`,
  #     propagated forward by transfers
  #
  # `token_id_per_vout` is the authoritative tag the indexer writes onto
  # `utxos.token_id`:
  #
  #   * issuance txn (valid): script-derived protoID for each STAS output
  #   * issuance txn (invalid/forged): `nil` — script self-asserts a
  #     protoID that doesn't match `HASH160(Vin[0])`, so we refuse to
  #     admit the output to that issuance set
  #   * transfer txn: protoID inherited from the parent STAS UTXO Vin[0]
  #     spent — the script's self-asserted protoID is cross-checked and
  #     only honoured when it agrees with the parent's tag
  defp compute_stas_attributes(tx, classified) do
    parent_outputs = lookup_parent_outputs(tx)
    parent_metas = lookup_parent_metas(tx)

    stas_outputs = Enum.filter(classified, fn {_v, t, _} -> t in [:stas, :stas3, :stas_btg] end)
    has_stas_outputs = stas_outputs != []

    # Classify every non-coinbase input's parent. The class tells us
    # whether lineage is decidable now (settled clean / settled tainted /
    # plain non-STAS) or must wait (parent unindexed / parent itself not
    # yet settled).
    input_classes =
      if Transaction.is_coinbase?(tx) do
        []
      else
        Enum.map(tx.inputs, fn input ->
          utxo = Map.get(parent_outputs, {input.source_txid, input.source_tx_out_index})
          meta = Map.get(parent_metas, input.source_txid)
          {input, utxo, meta, classify_parent(utxo, meta)}
        end)
      end

    # Missing parents = inputs whose lineage isn't settled. A forged or
    # tainted parent is SETTLED (`:stas_tainted`) — it does NOT defer the
    # child; only genuinely-unknown parents (`:missing` / `:deferred`) do.
    missing_parents =
      input_classes
      |> Enum.filter(fn {_i, _u, _m, class} -> class in [:missing, :deferred] end)
      |> Enum.map(fn {input, _u, _m, _c} -> display_hex(input.source_txid) end)
      |> Enum.uniq()

    deferred? = has_stas_outputs and missing_parents != []

    # Settled STAS parents (clean or tainted). Their presence makes this
    # tx a transfer; their absence (with STAS outputs) makes it an issuance.
    stas_parents =
      input_classes
      |> Enum.filter(fn {_i, _u, _m, c} -> c in [:stas_clean, :stas_tainted] end)
      |> Enum.map(fn {_i, utxo, meta, class} -> {utxo, meta, class} end)

    is_stas = stas_parents != [] or has_stas_outputs
    all_stas_inputs_known = not deferred?

    {is_issue, is_valid_issue, illegal_roots, token_id_per_vout} =
      cond do
        deferred? ->
          # Defer all lineage decisions until missing parents arrive.
          # Outputs index for satoshi accounting but token_id stays nil.
          {false, false, [], untagged_outputs(stas_outputs)}

        is_stas and stas_parents == [] and has_stas_outputs ->
          {valid, roots, vout_map} = decide_issuance(tx, parent_outputs, stas_outputs)
          {true, valid, roots, vout_map}

        is_stas ->
          {valid, roots, vout_map} = decide_transfer(stas_parents, stas_outputs)
          {false, valid, roots, vout_map}

        true ->
          {false, false, [], %{}}
      end

    # Redemption: a transfer that burns a token back to plain BSV by
    # paying a P2PKH output to the issuance's redemption address (the
    # address whose HASH160 is the protoID). Only meaningful for a
    # settled, non-issuance STAS tx.
    {is_redeem, redeem_address} =
      if is_stas and not is_issue and not deferred? do
        detect_redemption(tx, classified, stas_parents)
      else
        {false, nil}
      end

    %{
      flags: %{
        "is_stas" => is_stas,
        "is_issue" => is_issue,
        "is_valid_issue" => is_valid_issue,
        "all_stas_inputs_known" => all_stas_inputs_known,
        "illegal_roots" => illegal_roots,
        "missing_transactions" => missing_parents,
        "is_redeem" => is_redeem,
        "redeem_address" => redeem_address
      },
      token_id_per_vout: token_id_per_vout,
      parent_outputs: parent_outputs
    }
  end

  # A transfer is a redemption if it pays a plain P2PKH output to the
  # redemption address of one of the STAS tokens it spends — i.e. an
  # output address whose HASH160 equals a spent parent's protoID
  # (`token_id`). Returns `{is_redeem?, redeem_address_or_nil}`.
  defp detect_redemption(tx, classified, stas_parents) do
    parent_protos =
      stas_parents
      |> Enum.map(fn {utxo, _meta, _class} -> utxo.token_id end)
      |> Enum.reject(&is_nil/1)
      |> MapSet.new()

    redeem_address =
      if MapSet.size(parent_protos) == 0 do
        nil
      else
        Enum.find_value(classified, fn
          {vout, type, _} when type in [:p2pkh, :p2mpkh] ->
            output = Enum.at(tx.outputs, vout)
            addr = script_address(output.locking_script)
            pkh = addr && address_to_pkh_hex(addr)
            if pkh && MapSet.member?(parent_protos, pkh), do: addr, else: nil

          _ ->
            nil
        end)
      end

    {redeem_address != nil, redeem_address}
  end

  # Classify a single input's parent for lineage purposes:
  #
  #   * `:missing`      — parent UTXO not indexed; lineage undecidable.
  #   * `:non_stas`     — plain (P2PKH etc.) parent; no lineage concern.
  #   * `:deferred`     — STAS parent whose own lineage isn't settled yet
  #                       (parent tx still has unknown ancestors).
  #   * `:stas_clean`   — STAS parent, settled, valid lineage.
  #   * `:stas_tainted` — STAS parent, settled, but poisoned by a forged
  #                       issuance somewhere upstream (`illegal_roots`
  #                       non-empty). Distinct from `:deferred`: the
  #                       parent IS known, it's just permanently invalid.
  defp classify_parent(nil, _meta), do: :missing

  defp classify_parent(%Utxo{token_type: t}, _meta) when t not in ["stas", "stas3"],
    do: :non_stas

  defp classify_parent(%Utxo{}, nil), do: :deferred

  defp classify_parent(%Utxo{}, %MetaTransaction{metadata: md}) do
    cond do
      Map.get(md, "all_stas_inputs_known", true) == false -> :deferred
      (Map.get(md, "illegal_roots") || []) != [] -> :stas_tainted
      true -> :stas_clean
    end
  end

  defp untagged_outputs(stas_outputs) do
    Enum.into(stas_outputs, %{}, fn {v, _t, _tid} -> {v, nil} end)
  end

  # Bitcoin txids are displayed in REVERSE byte order from their internal
  # SHA256d representation. `Transaction.tx_id_hex/1` and block-explorer
  # APIs use display order; `input.source_txid` and `Transaction.txid_binary/1`
  # return internal order. We store all txid-as-string fields
  # (`missing_transactions`, `illegal_roots`) in display order so a human
  # reading the JSONB blob sees the same hex they'd find in a block
  # explorer.
  defp display_hex(<<_::binary-size(32)>> = txid_binary) do
    txid_binary
    |> :binary.bin_to_list()
    |> Enum.reverse()
    |> :binary.list_to_bin()
    |> Base.encode16(case: :lower)
  end

  # Build a map of `{source_txid, vout} => parent_utxo_or_nil` for every
  # non-coinbase input. Caches `UtxoManager` lookups so downstream lineage
  # decisions don't re-query.
  defp lookup_parent_outputs(tx) do
    if Transaction.is_coinbase?(tx) do
      %{}
    else
      Enum.reduce(tx.inputs, %{}, fn input, acc ->
        key = {input.source_txid, input.source_tx_out_index}
        parent = Repo.get_by(Utxo, txid: input.source_txid, vout: input.source_tx_out_index)
        Map.put(acc, key, parent)
      end)
    end
  end

  # Build a map of `source_txid => parent_MetaTransaction_or_nil` for every
  # non-coinbase input. The parent's lineage flags (`all_stas_inputs_known`,
  # `illegal_roots`) drive the clean/tainted/deferred classification.
  defp lookup_parent_metas(tx) do
    if Transaction.is_coinbase?(tx) do
      %{}
    else
      Enum.reduce(tx.inputs, %{}, fn input, acc ->
        meta = Repo.get_by(MetaTransaction, txid: input.source_txid)
        Map.put(acc, input.source_txid, meta)
      end)
    end
  end

  # Issuance branch: protoID across STAS outputs must be unique AND equal
  # `HASH160(Vin[0]'s spent output)`. `parent_outputs[{Vin[0].txid, vout}]`
  # gives us that address; we decode the base58 to recover the PKH.
  defp decide_issuance(tx, parent_outputs, stas_outputs) do
    output_protos =
      stas_outputs
      |> Enum.map(fn {_v, _t, tid} -> tid end)
      |> Enum.reject(&is_nil/1)
      |> Enum.uniq()

    single_proto = match?([_], output_protos)
    [vin0 | _] = tx.inputs
    parent = Map.get(parent_outputs, {vin0.source_txid, vin0.source_tx_out_index})

    vin0_pkh_hex =
      case parent do
        %Utxo{address: addr} when is_binary(addr) -> address_to_pkh_hex(addr)
        _ -> nil
      end

    valid =
      single_proto and
        vin0_pkh_hex != nil and hd(output_protos) == vin0_pkh_hex

    token_id_per_vout =
      if valid do
        Enum.into(stas_outputs, %{}, fn {v, _t, tid} -> {v, tid} end)
      else
        # Forged/invalid issuance: don't tag any output with the claimed
        # protoID. The output is still indexed (for satoshi accounting)
        # but it's never admitted to the issuance set.
        Enum.into(stas_outputs, %{}, fn {v, _t, _tid} -> {v, nil} end)
      end

    illegal_roots =
      if valid, do: [], else: [display_hex(Transaction.txid_binary(tx))]

    {valid, illegal_roots, token_id_per_vout}
  end

  # Transfer branch. `stas_parents` is a list of `{utxo, meta, class}`
  # for every settled STAS input. Two outcomes:
  #
  #   * Tainted — any STAS parent is `:stas_tainted`. The poison
  #     (`illegal_roots`) is the union of every tainted parent's own
  #     `illegal_roots` set (a forged issuance's set is `[itself]`, a
  #     tainted transfer's set is whatever it inherited). All outputs
  #     are left untagged — a tainted UTXO is not a valid set member.
  #
  #   * Clean — every STAS parent is `:stas_clean`. Outputs inherit the
  #     parent `token_id`, but only where the child output's own
  #     script-asserted protoID agrees with it (a spliced-in alien
  #     protoID is rejected to nil).
  defp decide_transfer(stas_parents, stas_outputs) do
    illegal_roots =
      stas_parents
      |> Enum.flat_map(fn
        {_utxo, %MetaTransaction{metadata: md}, :stas_tainted} ->
          Map.get(md, "illegal_roots") || []

        {_utxo, _meta, :stas_clean} ->
          []
      end)
      |> Enum.uniq()

    parent_token_id =
      Enum.find_value(stas_parents, fn
        {%Utxo{token_id: tid}, _meta, :stas_clean} -> tid
        _ -> nil
      end)

    token_id_per_vout =
      if illegal_roots != [] or is_nil(parent_token_id) do
        untagged_outputs(stas_outputs)
      else
        Enum.into(stas_outputs, %{}, fn {v, _t, tid} ->
          {v, (tid == parent_token_id && parent_token_id) || nil}
        end)
      end

    {false, illegal_roots, token_id_per_vout}
  end

  # Decode a mainnet base58check P2PKH address to its 20-byte HASH160 in
  # lower-hex form, matching the protoID encoding used elsewhere in the
  # indexer (`Classifier.classify_outputs` returns hex protoIDs).
  defp address_to_pkh_hex(addr) do
    case BSV.Base58.check_decode(addr) do
      {:ok, {0x00, <<pkh::binary-size(20)>>}} -> Base.encode16(pkh, case: :lower)
      _ -> nil
    end
  end

  defp extract_all_addresses(tx) do
    tx.outputs
    |> Enum.map(fn output -> script_address(output.locking_script) end)
    |> Enum.reject(&is_nil/1)
    |> Enum.uniq()
  end

  defp extract_all_token_ids(classified) do
    classified
    |> Enum.map(fn {_vout, _type, token_id} -> token_id end)
    |> Enum.reject(&is_nil/1)
    |> Enum.uniq()
  end

  defp determine_direction(tx, address) do
    output_addrs =
      tx.outputs
      |> Enum.map(fn o -> script_address(o.locking_script) end)

    if address in output_addrs, do: "in", else: "out"
  end

  # The set of all watched address strings, as a MapSet for O(1) lookups.
  # Watched addresses are few (selective indexer) so loading them per tx
  # is cheap; doing so keeps the processor decoupled from the filter's
  # ETS tables, which are not present in some test setups.
  defp watched_address_set do
    Repo.all(from(w in WatchingAddress, select: w.address)) |> MapSet.new()
  end

  # Resolve the `{satoshis, token_id}` pair for a single address-history
  # row. For an `in` flow the amount + token come from the outputs paying
  # the address; for an `out` flow they come from the spent UTXOs the
  # address owned. `token_id` uses the lineage-checked tag (nil for
  # forged / deferred outputs), never the raw script-asserted protoID.
  defp address_flow(tx, address, "in", _spent_utxos, stas_attrs) do
    outs =
      tx.outputs
      |> Enum.with_index()
      |> Enum.filter(fn {o, _v} -> script_address(o.locking_script) == address end)

    satoshis = outs |> Enum.map(fn {o, _v} -> o.satoshis end) |> Enum.sum()

    token_id =
      outs
      |> Enum.map(fn {_o, v} -> Map.get(stas_attrs.token_id_per_vout, v) end)
      |> Enum.find(&(&1 != nil))

    {satoshis, token_id}
  end

  defp address_flow(_tx, address, "out", spent_utxos, _stas_attrs) do
    owned = Enum.filter(spent_utxos, fn u -> u.address == address end)
    satoshis = owned |> Enum.map(& &1.satoshis) |> Enum.sum()
    token_id = owned |> Enum.map(& &1.token_id) |> Enum.find(&(&1 != nil))
    {satoshis, token_id}
  end

  # Derive a base58 P2PKH address for an output's locking script. Handles
  # the standard P2PKH template via `BSV.Script.Address.from_script/2` and
  # the STAS 3.0 template by lifting the 20-byte owner PKH from the script
  # body (spec v0.1 §5.2.2) and base58-checking it as a mainnet address.
  # Returns `nil` for templates we don't know how to address (e.g. bare
  # OP_RETURN, custom scripts) — `:utxos.address` is nullable to allow
  # those rows to still record satoshis + script for accounting.
  defp script_address(%BSV.Script{} = script) do
    case BSV.Script.Address.from_script(script) do
      {:ok, addr} ->
        addr

      :error ->
        bin = BSV.Script.to_binary(script)

        case ScriptReader.read_locking_script(bin) do
          %{script_type: :stas3, stas3: %{owner: <<owner::binary-size(20)>>}} ->
            BSV.Base58.check_encode(owner, 0x00)

          %{script_type: :p2mpkh} ->
            # STAS 3.0 v0.1 §10.2 P2MPKH: the 20-byte MPKH sits at offset
            # 3 (after OP_DUP OP_HASH160 OP_DATA_20), exactly like a P2PKH
            # pubkey hash, so it base58-encodes into a mainnet address.
            <<_::binary-size(3), mpkh::binary-size(20), _::binary>> = bin
            BSV.Base58.check_encode(mpkh, 0x00)

          _ ->
            nil
        end
    end
  end

  # Walk the transaction's inputs and return the first observed STAS 3.0
  # spendType class (per spec v0.1 §8.2). Returns the string form expected
  # by `Athanor.Schema.Utxo.stas3_op` or `nil` when no STAS 3.0 unlocking
  # script is found / parses cleanly. Multiple STAS 3.0 inputs in a single
  # tx are required by the spec to share the same spendType, so the first
  # successful parse wins.
  defp stas3_spend_op(tx) do
    if Transaction.is_coinbase?(tx) do
      nil
    else
      Enum.find_value(tx.inputs, fn input ->
        case input.unlocking_script do
          nil ->
            nil

          script ->
            unlocking_bin = BSV.Script.to_binary(script)

            case SpendType.parse(unlocking_bin) do
              {:ok, op} -> SpendType.to_string(op)
              {:error, _} -> nil
            end
        end
      end)
    end
  end

  # Spec v0.1 §4: the post-OP_RETURN region is byte-identical across every
  # spend of a given STAS 3.0 issuance. If we already have a canonical copy
  # for this protoID and the new output's bytes diverge, return true so the
  # caller can warn + skip indexing the malformed output.
  defp stas3_post_op_return_tampered?(nil, _script_binary), do: false

  defp stas3_post_op_return_tampered?(proto_hex, script_binary) do
    with %WatchingToken{canonical_post_op_return: canonical}
         when not is_nil(canonical) <-
           Repo.get_by(WatchingToken, token_id: proto_hex),
         {:ok, observed} <- Stas3Meta.post_op_return(script_binary) do
      observed != canonical
    else
      _ -> false
    end
  end

  # On the first STAS 3.0 output we see for a watched protoID, persist the
  # canonical post-OP_RETURN bytes (spec §4) and any FREEZABLE / CONFISCATABLE
  # service-field authorities (spec §5.2.3). Subsequent outputs do nothing —
  # the byte-identity check above guards against drift.
  defp seed_watching_token_metadata(nil, _script_binary), do: :ok

  defp seed_watching_token_metadata(proto_hex, script_binary) do
    case Repo.get_by(WatchingToken, token_id: proto_hex) do
      nil ->
        :ok

      %WatchingToken{canonical_post_op_return: canonical} = wt
      when is_nil(canonical) ->
        with {:ok, post_op_return} <- Stas3Meta.post_op_return(script_binary),
             %{stas3: %_{} = fields} <- ScriptReader.read_locking_script(script_binary),
             {:ok, %{freeze_auth: f, confiscate_auth: c}} <-
               Stas3Meta.service_authorities(fields) do
          wt
          |> WatchingToken.changeset(%{
            canonical_post_op_return: post_op_return,
            freeze_auth: f,
            confiscate_auth: c
          })
          |> Repo.update()
          |> case do
            {:ok, _} -> :ok
            {:error, cs} -> Logger.warning("STAS3 metadata seed failed: #{inspect(cs.errors)}")
          end
        else
          _ -> :ok
        end

      _ ->
        :ok
    end
  end
end
