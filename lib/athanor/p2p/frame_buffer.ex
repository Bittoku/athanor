defmodule Athanor.P2P.FrameBuffer do
  @moduledoc """
  Pure reassembly accumulator that turns arbitrary TCP byte chunks into whole
  `Athanor.P2P.Frame`s (Phase 1, T1.1).

  TCP carries no message boundaries: a single delivery may hold a partial frame,
  several frames, or a frame followed by the start of the next. A `FrameBuffer`
  retains undecoded bytes between pushes and emits frames as soon as they are
  complete.

  ## API

    * `new/2` — create a buffer for a network (with an optional `:max_payload`).
    * `push/2` — fold a new chunk in. Returns `{frames, buffer}` with zero or
      more decoded frames (in wire order), or `{:error, reason, buffer}` if a
      frame failed to decode (bad magic, oversized, bad command/checksum). A
      decode error is fatal — the owning `Peer` should disconnect.

  The module holds no process state and performs no IO.
  """

  alias Athanor.P2P.{Frame, Network}

  @enforce_keys [:network, :decode_opts]
  defstruct network: nil, decode_opts: [], pending: <<>>

  @type t :: %__MODULE__{
          network: Network.t(),
          decode_opts: keyword(),
          pending: binary()
        }

  @doc """
  Builds an empty buffer for `network`.

  `opts` may carry `:max_payload` (passed through to `Frame.decode/3`); omitted,
  `Frame`'s own default applies.
  """
  @spec new(Network.t(), keyword()) :: t()
  def new(%Network{} = network, opts \\ []) do
    %__MODULE__{network: network, decode_opts: opts, pending: <<>>}
  end

  @doc """
  Folds `bytes` into the buffer and drains every whole frame now available.

  Returns `{frames, buffer}` (frames in wire order, possibly empty) or
  `{:error, reason, buffer}` on a fatal decode error.
  """
  @spec push(t(), binary()) :: {[Frame.t()], t()} | {:error, term(), t()}
  def push(%__MODULE__{pending: pending} = buf, bytes) when is_binary(bytes) do
    drain(buf, pending <> bytes, [])
  end

  # Tail-recursive: decode frames off the front of `data` until we hit a partial
  # frame (`:need_more`) or an error. Accumulated frames are reversed once.
  defp drain(buf, data, acc) do
    case Frame.decode(buf.network, data, buf.decode_opts) do
      {:ok, frame, rest} ->
        drain(buf, rest, [frame | acc])

      :need_more ->
        {Enum.reverse(acc), %{buf | pending: data}}

      {:error, reason} ->
        {:error, reason, %{buf | pending: data}}
    end
  end
end
