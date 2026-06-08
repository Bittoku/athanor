defmodule Athanor.P2P.Peer.Handshake do
  @moduledoc """
  Pure reducer for the BSV version/verack handshake (Phase 1, T1.2).

  All handshake protocol decisions live here, with no process and no IO, so they
  are exhaustively unit-testable. The owning `Peer` GenServer only feeds events
  in and performs the emitted actions.

  ## Model

  `step(state, event) -> {state, actions}` where:

    * events are `:start`, `{:frame, %Frame{}}`, or `:timeout`;
    * actions are a list of `{:send, frame_bytes}` | `{:done, peer_version}` |
      `{:error, reason}`.

  The handshake completes when three conditions hold: we have received the
  peer's `version` (`got_peer_version?`), we have sent our `verack` in reply
  (`sent_our_verack?`), and we have received the peer's `verack`
  (`got_peer_verack?`). On the transition to all-three-true the reducer emits
  `{:done, peer_version}` and moves to `:done`. Both arrival orderings
  (version-first, verack-first) converge to the same terminal state.

  Mid-handshake `ping`s are answered with `pong` (keepalive courtesy);
  unrelated frames are ignored. `reject`, `:timeout`, and a malformed peer
  `version` are fatal, surfaced as a single `{:error, reason}` action.
  """

  alias Athanor.P2P.{Frame, Network}
  alias Athanor.P2P.Messages.{Protoconf, Version}

  @enforce_keys [:network, :our_version]
  defstruct network: nil,
            our_version: nil,
            status: :init,
            got_peer_version?: false,
            got_peer_verack?: false,
            sent_our_verack?: false,
            peer_version: nil

  @type status :: :init | :awaiting | :done
  @type action ::
          {:send, binary()} | {:done, Version.t()} | {:error, atom()}
  @type t :: %__MODULE__{
          network: Network.t(),
          our_version: Version.t(),
          status: status(),
          got_peer_version?: boolean(),
          got_peer_verack?: boolean(),
          sent_our_verack?: boolean(),
          peer_version: Version.t() | nil
        }

  @doc "Builds the initial handshake state for `network` advertising `our_version`."
  @spec new(Network.t(), Version.t()) :: t()
  def new(%Network{} = network, %Version{} = our_version) do
    %__MODULE__{network: network, our_version: our_version}
  end

  @doc """
  Advances the handshake by one `event`. Returns `{new_state, actions}`.
  See the module doc for the event and action vocabulary.
  """
  @spec step(t(), :start | {:frame, Frame.t()} | :timeout) :: {t(), [action()]}
  def step(%__MODULE__{} = state, :start) do
    version_frame = encode(state, :version, Version.serialize(state.our_version))
    {%{state | status: :awaiting}, [{:send, version_frame}]}
  end

  def step(%__MODULE__{} = state, :timeout) do
    {state, [{:error, :handshake_timeout}]}
  end

  def step(%__MODULE__{} = state, {:frame, %Frame{command: "version", payload: payload}}) do
    case Version.parse(payload) do
      {:ok, peer_version, _rest} ->
        verack = encode(state, :verack, <<>>)
        protoconf = encode(state, :protoconf, Protoconf.serialize(%Protoconf{}))

        %{state | got_peer_version?: true, sent_our_verack?: true, peer_version: peer_version}
        |> maybe_complete([{:send, verack}, {:send, protoconf}])

      _ ->
        {state, [{:error, :bad_version}]}
    end
  end

  def step(%__MODULE__{} = state, {:frame, %Frame{command: "verack"}}) do
    maybe_complete(%{state | got_peer_verack?: true}, [])
  end

  def step(
        %__MODULE__{} = state,
        {:frame, %Frame{command: "ping", payload: <<nonce::little-64>>}}
      ) do
    {state, [{:send, encode(state, :pong, <<nonce::little-64>>)}]}
  end

  def step(%__MODULE__{} = state, {:frame, %Frame{command: "reject"}}) do
    {state, [{:error, :handshake_rejected}]}
  end

  # Any other frame mid-handshake (sendheaders, addr, feefilter, ...) is ignored.
  def step(%__MODULE__{} = state, {:frame, %Frame{}}), do: {state, []}

  # If the three flags are now all true, append `{:done, peer_version}` and
  # transition to `:done`; otherwise return the prior actions unchanged.
  defp maybe_complete(state, actions) do
    if done?(state) do
      {%{state | status: :done}, actions ++ [{:done, state.peer_version}]}
    else
      {state, actions}
    end
  end

  defp done?(%__MODULE__{} = s),
    do: s.got_peer_version? and s.got_peer_verack? and s.sent_our_verack?

  defp encode(%__MODULE__{network: network}, command, payload),
    do: Frame.encode(network, command, payload)
end
