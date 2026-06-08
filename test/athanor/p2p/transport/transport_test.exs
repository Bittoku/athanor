defmodule Athanor.P2P.TransportTest do
  @moduledoc """
  Contract tests for the `Athanor.P2P.Transport` behaviour and its two
  implementations (T1.0):

  - `Transport.Gen` — the real `:gen_tcp` wrapper, exercised against a throwaway
    loopback echo listener to prove connect/send/active-mode-receive round-trip.
  - `Transport.Fake` — the deterministic test double, proving outbound bytes are
    captured (`sent/1`) and inbound bytes can be injected (`deliver/2`) as the
    active-mode `{:tcp, socket, data}` message the Peer GenServer expects.
  """
  use ExUnit.Case, async: true

  alias Athanor.P2P.Transport

  describe "Transport.Gen over loopback" do
    test "connect, send, and receive an echoed chunk in active mode" do
      # A throwaway listener that echoes whatever it receives back to the client.
      {:ok, listener} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true])
      {:ok, port} = :inet.port(listener)

      server =
        spawn_link(fn ->
          {:ok, sock} = :gen_tcp.accept(listener)
          {:ok, data} = :gen_tcp.recv(sock, 0)
          :ok = :gen_tcp.send(sock, data)
          # Keep the socket open until this helper process is killed.
          receive do
            :stop -> :ok
          end
        end)

      {:ok, socket} = Transport.Gen.connect(~c"127.0.0.1", port, [], 1_000)
      assert :ok = Transport.Gen.send(socket, "ping-bytes")

      # Active mode: the echoed bytes arrive as a message to this process.
      assert_receive {:tcp, ^socket, "ping-bytes"}, 1_000

      :ok = Transport.Gen.close(socket)
      send(server, :stop)
      :gen_tcp.close(listener)
    end

    test "connect to a closed port returns an error tuple" do
      # Open then immediately close a listener to obtain a definitely-dead port.
      {:ok, listener} = :gen_tcp.listen(0, [:binary, active: false])
      {:ok, port} = :inet.port(listener)
      :ok = :gen_tcp.close(listener)

      assert {:error, _reason} = Transport.Gen.connect(~c"127.0.0.1", port, [], 200)
    end
  end

  describe "Transport.Fake" do
    test "captures outbound bytes in send order" do
      {:ok, socket} = Transport.Fake.connect(~c"peer", 8333, [fake: %{test: self()}], 1_000)

      assert :ok = Transport.Fake.send(socket, "first")
      assert :ok = Transport.Fake.send(socket, "second")

      assert Transport.Fake.sent(socket) == ["first", "second"]
    end

    test "deliver/2 injects an inbound active-mode {:tcp, socket, data} message" do
      {:ok, socket} = Transport.Fake.connect(~c"peer", 8333, [fake: %{test: self()}], 1_000)

      :ok = Transport.Fake.deliver(socket, "inbound")
      assert_receive {:tcp, ^socket, "inbound"}

      :ok = Transport.Fake.deliver_closed(socket)
      assert_receive {:tcp_closed, ^socket}
    end

    test "records setopts calls for active:once back-pressure assertions" do
      {:ok, socket} = Transport.Fake.connect(~c"peer", 8333, [fake: %{test: self()}], 1_000)

      :ok = Transport.Fake.setopts(socket, active: :once)
      :ok = Transport.Fake.setopts(socket, active: :once)

      assert Transport.Fake.setopts_log(socket) == [[active: :once], [active: :once]]
    end

    test "announces the handle to the configured test pid on connect" do
      {:ok, socket} = Transport.Fake.connect(~c"peer", 8333, [fake: %{test: self()}], 1_000)
      assert_receive {:fake_handle, ^socket}
    end

    test "connect refuses when configured to" do
      assert {:error, :econnrefused} =
               Transport.Fake.connect(
                 ~c"peer",
                 8333,
                 [fake: %{test: self(), refuse: true}],
                 1_000
               )
    end
  end
end
