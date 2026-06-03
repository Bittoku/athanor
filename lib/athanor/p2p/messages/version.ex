defmodule Athanor.P2P.Messages.Version do
  @moduledoc """
  BSV P2P `version` message (handshake) codec.

  Wire layout (Bitcoin protocol 70016; integers little-endian unless noted):

      | version       | int32  LE                                   |
      | services      | uint64 LE                                   |
      | timestamp     | int64  LE (signed)                          |
      | addr_recv     | 26-byte net-addr (no nTime)                 |
      | addr_from     | 26-byte net-addr (no nTime)                 |
      | nonce         | uint64 LE                                   |
      | user_agent    | var_str                                     |
      | start_height  | int32  LE                                   |
      | relay         | 1 byte (0/1)                                |

  A 26-byte net-addr (without the leading 4-byte nTime used elsewhere) is
  `services::uint64-LE` + `ip::16 bytes` + `port::uint16-BIG-endian`. The port
  is the one field that is big-endian on the wire.

  `parse/1` is deliberately lenient (matching bitcoin-sv `net_processing.cpp`):
  after the fixed 80-byte prefix, `user_agent`, `start_height`, and `relay` are
  each optional and default to `""`, `0`, and `true` respectively. This module
  is pure (no IO).
  """

  alias Athanor.P2P.Codec.VarBytes

  @protocol_version 70_016
  # version(4) + services(8) + timestamp(8) + addr_recv(26) + addr_from(26) + nonce(8)
  @fixed_prefix_size 80

  @enforce_keys [:addr_recv, :addr_from]
  defstruct version: @protocol_version,
            services: 0,
            timestamp: 0,
            addr_recv: nil,
            addr_from: nil,
            nonce: 0,
            user_agent: "/Athanor:0.1.0/",
            start_height: 0,
            relay: true

  @type t :: %__MODULE__{
          version: non_neg_integer(),
          services: non_neg_integer(),
          timestamp: integer(),
          addr_recv: <<_::208>>,
          addr_from: <<_::208>>,
          nonce: non_neg_integer(),
          user_agent: String.t(),
          start_height: integer(),
          relay: boolean()
        }

  @doc "The protocol version Athanor advertises (70016)."
  @spec protocol_version() :: pos_integer()
  def protocol_version, do: @protocol_version

  @doc """
  Builds a 26-byte net-addr: `services` (uint64 LE) + `ip` (16 bytes) + `port`
  (uint16 **big-endian**).
  """
  @spec net_addr(non_neg_integer(), <<_::128>>, non_neg_integer()) :: <<_::208>>
  def net_addr(services, <<ip::binary-16>>, port) do
    <<services::little-64, ip::binary-16, port::big-16>>
  end

  @doc "Serializes a `version` message to its wire payload."
  @spec serialize(t()) :: binary()
  def serialize(%__MODULE__{} = v) do
    <<v.version::little-32, v.services::little-64, v.timestamp::signed-little-64>> <>
      v.addr_recv <>
      v.addr_from <>
      <<v.nonce::little-64>> <>
      VarBytes.write_str(v.user_agent) <>
      <<v.start_height::little-32>> <>
      <<relay_byte(v.relay)>>
  end

  @doc """
  Parses a `version` payload. Returns `{:ok, %Version{}, rest}`, or `:need_more`
  if the fixed 80-byte prefix has not fully arrived.
  """
  @spec parse(binary()) :: {:ok, t(), binary()} | :need_more
  def parse(
        <<version::little-32, services::little-64, timestamp::signed-little-64,
          addr_recv::binary-26, addr_from::binary-26, nonce::little-64, rest::binary>>
      ) do
    {user_agent, after_ua} = take_user_agent(rest)
    {start_height, after_height} = take_start_height(after_ua)
    {relay, tail} = take_relay(after_height)

    {:ok,
     %__MODULE__{
       version: version,
       services: services,
       timestamp: timestamp,
       addr_recv: addr_recv,
       addr_from: addr_from,
       nonce: nonce,
       user_agent: user_agent,
       start_height: start_height,
       relay: relay
     }, tail}
  end

  def parse(buffer) when is_binary(buffer) and byte_size(buffer) < @fixed_prefix_size,
    do: :need_more

  defp relay_byte(true), do: 1
  defp relay_byte(false), do: 0

  defp take_user_agent(rest) do
    case VarBytes.read_str(rest) do
      {:ok, ua, tail} -> {ua, tail}
      _ -> {"", rest}
    end
  end

  defp take_start_height(<<height::little-32, tail::binary>>), do: {height, tail}
  defp take_start_height(rest), do: {0, rest}

  defp take_relay(<<0, tail::binary>>), do: {false, tail}
  defp take_relay(<<_byte, tail::binary>>), do: {true, tail}
  defp take_relay(<<>>), do: {true, <<>>}
end
