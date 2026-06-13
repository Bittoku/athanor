defmodule Athanor.P2P.Network do
  @moduledoc """
  Per-network BSV P2P wire parameters and command-name encoding.

  Values are taken verbatim from bitcoin-sv `src/chainparams.cpp` (`netMagic`,
  `nDefaultPort`, `vSeeds`). The P2P client is network-parameterized: nothing in
  the codec or peer layer hardcodes mainnet. `for_network/1` maps the network
  atom resolved by `Athanor.Blockchain.Network` (`:mainnet` / `:testnet`) to the
  matching params.

  ## Struct fields
    * `:name`           — `:mainnet | :testnet`
    * `:magic`          — 4-byte wire magic (frame header prefix)
    * `:default_port`   — default TCP port
    * `:dns_seeds`      — DNS seeder hostnames (primary bootstrap)
    * `:fallback_seeds` — hardcoded `{ip, port}` seeds; empty for now (the
      `pnSeed6_main` / `pnSeed6_test` IP tables are loaded in Phase 2 discovery)
    * `:pow_limit` — the consensus maximum target as a compact `nBits` value
      (bitcoin-sv `consensus.powLimit`). Both mainnet and Testnet3 use
      `0x1d00ffff`. Header PoW validation rejects any `bits` whose target exceeds
      this (an easier-than-consensus difficulty a real node would reject).

  ## Known gap — STN
  bitcoin-sv also defines the Scaling Test Network (magic `fb ce c4 f9`, port
  9333). `Athanor.Blockchain.Network` currently folds an `"stn"` config into the
  `:testnet` atom, so STN is not reachable as a distinct P2P network yet; wiring
  it up requires a network-resolution change tracked for the integration phase.

  This module is pure (no IO, no process).
  """

  @enforce_keys [:name, :magic, :default_port, :dns_seeds, :fallback_seeds]
  defstruct [:name, :magic, :default_port, :dns_seeds, :fallback_seeds, pow_limit: 0x1D00FFFF]

  @type t :: %__MODULE__{
          name: :mainnet | :testnet,
          magic: <<_::32>>,
          default_port: :inet.port_number(),
          dns_seeds: [String.t()],
          fallback_seeds: [{:inet.ip_address(), :inet.port_number()}],
          pow_limit: 0..0xFFFFFFFF
        }

  # atom -> wire command string. is_map_key/2 guards make an unknown command
  # fail with FunctionClauseError rather than silently encoding garbage.
  @commands %{
    version: "version",
    verack: "verack",
    ping: "ping",
    pong: "pong",
    protoconf: "protoconf",
    inv: "inv",
    getdata: "getdata",
    notfound: "notfound",
    reject: "reject",
    addr: "addr",
    getaddr: "getaddr",
    headers: "headers",
    getheaders: "getheaders",
    getblocks: "getblocks",
    block: "block",
    tx: "tx",
    mempool: "mempool",
    sendheaders: "sendheaders",
    feefilter: "feefilter"
  }

  @doc "Returns the mainnet wire parameters."
  @spec mainnet() :: t()
  def mainnet do
    %__MODULE__{
      name: :mainnet,
      magic: <<0xE3, 0xE1, 0xF3, 0xE8>>,
      default_port: 8333,
      dns_seeds: [
        "seed.bitcoinsv.io",
        "seed.satoshisvision.network",
        "seed.bitcoinseed.directory"
      ],
      fallback_seeds: []
    }
  end

  @doc "Returns the testnet (Testnet3) wire parameters."
  @spec testnet() :: t()
  def testnet do
    %__MODULE__{
      name: :testnet,
      magic: <<0xF4, 0xE5, 0xF3, 0xF4>>,
      default_port: 18333,
      dns_seeds: [
        "testnet-seed.bitcoinsv.io",
        "testnet-seed.bitcoincloud.net",
        "testnet-seed.bitcoinseed.directory"
      ],
      fallback_seeds: []
    }
  end

  @doc """
  Resolves a network atom to its parameters.

  ## Parameters
    - `network` — `:mainnet` or `:testnet`.

  ## Returns
    The matching `t:t/0`. Raises `FunctionClauseError` on an unknown network.
  """
  @spec for_network(:mainnet | :testnet) :: t()
  def for_network(:mainnet), do: mainnet()
  def for_network(:testnet), do: testnet()

  @doc """
  Returns every known wire command atom.

  This is the single source of truth for the command set, so callers (e.g.
  property tests over "any known command") cannot drift from the registry.
  """
  @spec commands() :: [atom()]
  def commands, do: Map.keys(@commands)

  @doc """
  Returns the wire command string for a known command atom.
  Raises `FunctionClauseError` on an unknown command.
  """
  @spec command_name(atom()) :: String.t()
  def command_name(command) when is_map_key(@commands, command),
    do: Map.fetch!(@commands, command)

  @doc """
  Returns the 12-byte NUL-padded wire command field for a known command atom.
  Raises `FunctionClauseError` on an unknown command.
  """
  @spec padded_command(atom()) :: <<_::96>>
  def padded_command(command), do: String.pad_trailing(command_name(command), 12, <<0>>)
end
