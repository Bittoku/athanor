defmodule Athanor.Indexer.TransactionFilter do
  @moduledoc """
  ETS-backed filter for checking whether a transaction involves watched
  addresses or STAS tokens. The hot path — every incoming tx hits this.

  ETS tables provide lock-free concurrent reads. Writes (admin adds/removes
  an address or token) are serialized through this GenServer.
  """

  use GenServer

  @addresses_table :watched_addresses
  @tokens_table :watched_tokens

  require Logger

  ## ── Client API ──

  @doc """
  Starts the TransactionFilter GenServer and creates ETS tables.
  """
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Adds an address to the ETS watched-addresses table.

  ## Parameters
    - `address` — BSV address string to watch

  ## Returns
    :ok
  """
  def add_address(address) do
    GenServer.call(__MODULE__, {:add_address, address})
  end

  @doc """
  Adds a token ID to the ETS watched-tokens table.

  ## Parameters
    - `token_id` — STAS token ID string to watch

  ## Returns
    :ok
  """
  def add_token(token_id) do
    GenServer.call(__MODULE__, {:add_token, token_id})
  end

  @doc """
  Checks whether a given address is in the watched set.

  ## Parameters
    - `address` — BSV address string

  ## Returns
    Boolean indicating whether the address is watched.
  """
  def watching_address?(address) do
    :ets.member(@addresses_table, address)
  end

  @doc """
  Checks whether a given token ID is in the watched set.

  ## Parameters
    - `token_id` — STAS token ID string

  ## Returns
    Boolean indicating whether the token is watched.
  """
  def watching_token?(token_id) do
    :ets.member(@tokens_table, token_id)
  end

  @doc """
  Submits a raw transaction binary for asynchronous filtering.
  The GenServer will parse the transaction, check outputs against watched
  addresses and tokens, and forward matches to TransactionProcessor.

  ## Parameters
    - `raw_tx_binary` — raw transaction bytes

  ## Returns
    :ok
  """
  def process_raw_tx(raw_tx_binary, source \\ :unknown) do
    GenServer.cast(__MODULE__, {:process_raw_tx, raw_tx_binary, source})
  end

  @doc """
  Synchronously checks a parsed `%BSV.Transaction{}` against all watched
  addresses and STAS token IDs. Useful for testing and one-off queries.

  ## Parameters
    - `tx` — a `%BSV.Transaction{}` struct

  ## Returns
    `{matched_addresses, matched_tokens}` where each is a list of strings.
  """
  @spec matches?(BSV.Transaction.t()) :: {[String.t()], [String.t()]}
  def matches?(tx) do
    scan_outputs(tx.outputs)
  end

  ## ── Server Callbacks ──

  @impl true
  def init(_opts) do
    :ets.new(@addresses_table, [:set, :public, :named_table, read_concurrency: true])
    :ets.new(@tokens_table, [:set, :public, :named_table, read_concurrency: true])

    # Load initial data from DB into ETS
    load_from_db()

    {:ok, %{}}
  end

  @impl true
  def handle_call({:add_address, address}, _from, state) do
    :ets.insert(@addresses_table, {address, true})
    {:reply, :ok, state}
  end

  def handle_call({:add_token, token_id}, _from, state) do
    :ets.insert(@tokens_table, {token_id, true})
    {:reply, :ok, state}
  end

  @impl true
  def handle_cast({:process_raw_tx, raw_tx_binary, source}, state) do
    case BSV.Transaction.from_binary(raw_tx_binary) do
      {:ok, tx, _rest} ->
        {matched_addresses, matched_tokens} = scan_outputs(tx.outputs)

        if matched_addresses != [] or matched_tokens != [] do
          Logger.info(
            "TransactionFilter matched addrs=#{inspect(matched_addresses)} tokens=#{inspect(matched_tokens)} source=#{source}"
          )

          GenServer.cast(
            Athanor.Indexer.TransactionProcessor,
            {:index_tx, tx, matched_addresses, matched_tokens, source}
          )
        end

      {:error, reason} ->
        Logger.warning("TransactionFilter failed to parse raw tx: #{inspect(reason)}")
    end

    {:noreply, state}
  end

  # Back-compat: a 2-tuple cast (no source) defaults to :unknown.
  def handle_cast({:process_raw_tx, raw_tx_binary}, state),
    do: handle_cast({:process_raw_tx, raw_tx_binary, :unknown}, state)

  @impl true
  def handle_info(_msg, state) do
    {:noreply, state}
  end

  ## ── Private Helpers ──

  # Iterates over all transaction outputs and collects watched addresses
  # and STAS/STAS3 token IDs that appear in the locking scripts.
  #
  # Returns {[matched_address_strings], [matched_token_id_strings]}.
  @spec scan_outputs([BSV.Transaction.Output.t()]) :: {[String.t()], [String.t()]}
  defp scan_outputs(outputs) do
    Enum.reduce(outputs, {[], []}, fn output, {addrs_acc, tokens_acc} ->
      script = output.locking_script
      script_binary = BSV.Script.to_binary(script)

      # Check for watched address (P2PKH)
      addrs_acc = check_address(script, addrs_acc)

      # Check for watched STAS / STAS3 token
      tokens_acc = check_token(script_binary, tokens_acc)

      {addrs_acc, tokens_acc}
    end)
    |> then(fn {addrs, tokens} -> {Enum.uniq(addrs), Enum.uniq(tokens)} end)
  end

  # Extracts a watchable address from a locking script and appends it to
  # the accumulator if it is in the :watched_addresses ETS table.
  #
  # Handles standard P2PKH and the STAS 3.0 v0.1 §10.2 P2MPKH locking
  # script (issuance/redemption boundary UTXOs). The P2MPKH MPKH sits at
  # offset 3 just like a P2PKH pubkey hash, so it base58-encodes into the
  # same address space.
  @spec check_address(BSV.Script.t(), [String.t()]) :: [String.t()]
  defp check_address(script, acc) do
    case BSV.Script.Address.from_script(script) do
      {:ok, address} ->
        if :ets.member(@addresses_table, address), do: [address | acc], else: acc

      :error ->
        bin = BSV.Script.to_binary(script)

        case BSV.Tokens.Script.Reader.read_locking_script(bin) do
          %{script_type: :p2mpkh} ->
            <<_::binary-size(3), mpkh::binary-size(20), _::binary>> = bin
            address = BSV.Base58.check_encode(mpkh, 0x00)
            if :ets.member(@addresses_table, address), do: [address | acc], else: acc

          _ ->
            acc
        end
    end
  rescue
    # Malformed scripts the reader can't handle
    _ -> acc
  end

  # Parses a locking script binary for STAS or STAS 3 token data and appends
  # the token ID string to the accumulator if it is in :watched_tokens.
  @spec check_token(binary(), [String.t()]) :: [String.t()]
  defp check_token(script_binary, acc) do
    parsed = BSV.Tokens.Script.Reader.read_locking_script(script_binary)

    case parsed.script_type do
      type when type in [:stas, :stas_btg] ->
        token_id_str = BSV.Tokens.TokenId.to_string(parsed.stas.token_id)
        if :ets.member(@tokens_table, token_id_str), do: [token_id_str | acc], else: acc

      :stas3 ->
        # STAS 3.0 token-id = protoID = HASH160 of the issuer/redemption address
        # (spec v0.1 §5.2.1, §14). This is the canonical, immutable identifier
        # for the issuance — owner PKH rotates per UTXO and must NOT be used.
        proto_hex = Base.encode16(parsed.stas3.redemption, case: :lower)
        if :ets.member(@tokens_table, proto_hex), do: [proto_hex | acc], else: acc

      _ ->
        acc
    end
  rescue
    # Malformed scripts that the reader can't handle
    e ->
      Logger.debug("TransactionFilter token parse error: #{inspect(e)}")
      acc
  end

  # Loads all watched addresses and tokens from the database into ETS tables
  # on startup, so the filter is immediately ready for matching.
  defp load_from_db do
    alias Athanor.Repo
    alias Athanor.Schema.{WatchingAddress, WatchingToken}

    try do
      Repo.all(WatchingAddress)
      |> Enum.each(fn wa -> :ets.insert(@addresses_table, {wa.address, true}) end)

      Repo.all(WatchingToken)
      |> Enum.each(fn wt -> :ets.insert(@tokens_table, {wt.token_id, true}) end)
    rescue
      # DB may not be available during tests or initial setup
      _ -> :ok
    end
  end
end
