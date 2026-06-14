defmodule Athanor.Blockchain.RpcClientTest do
  @moduledoc """
  Covers the JSON-RPC envelope, basic-auth header, error mapping, and
  request-id sequencing in `Athanor.Blockchain.RpcClient`. Requests are
  routed through `Req.Test` stubs so no live BSV node connection is
  needed.

  Tests target a per-test RpcClient started with `name: nil` (the global
  production name is held by `Athanor.Test.RpcClientStub` for the duration
  of the test suite — see `test/test_helper.exs:5`). Calls go through
  `GenServer.call(pid, ...)` directly, which exercises the same
  `handle_call` clauses, request construction, and response parsing that
  the public API uses.
  """

  use ExUnit.Case, async: false

  alias Athanor.Blockchain.RpcClient

  setup :set_req_test_to_shared

  defp set_req_test_to_shared(_ctx) do
    # async: false → shared mode lets the spawned RpcClient process use
    # the stub registered in this test process without an explicit allow/3.
    Req.Test.set_req_test_to_shared(%{})
    :ok
  end

  ## ── Helpers ──

  defp start_rpc!(opts \\ []) do
    opts = Keyword.merge([plug: {Req.Test, RpcClient}, rpc_url: "http://stub/", name: nil], opts)

    spec = Supervisor.child_spec({RpcClient, opts}, id: make_ref())
    start_supervised!(spec)
  end

  defp stub_json(body) do
    Req.Test.stub(RpcClient, fn conn -> Req.Test.json(conn, body) end)
  end

  defp stub_with_inspection(body, parent) do
    Req.Test.stub(RpcClient, fn conn ->
      {:ok, raw, conn} = Plug.Conn.read_body(conn)
      send(parent, {:request, conn.method, conn.req_headers, Jason.decode!(raw)})
      Req.Test.json(conn, body)
    end)
  end

  ## ── Tests ──

  describe "get_block_count" do
    test "returns {:ok, height} from the result field" do
      pid = start_rpc!()
      stub_json(%{"result" => 800_000, "error" => nil})

      assert {:ok, 800_000} = GenServer.call(pid, :get_block_count)
    end

    test "sends a JSON-RPC 2.0 envelope with method=getblockcount" do
      pid = start_rpc!()
      stub_with_inspection(%{"result" => 0, "error" => nil}, self())

      assert {:ok, 0} = GenServer.call(pid, :get_block_count)

      assert_receive {:request, "POST", _headers, body}
      assert body["jsonrpc"] == "2.0"
      assert body["method"] == "getblockcount"
      assert body["params"] == []
      assert is_integer(body["id"])
    end

    test "increments the request id across consecutive calls" do
      pid = start_rpc!()
      stub_with_inspection(%{"result" => 1, "error" => nil}, self())

      GenServer.call(pid, :get_block_count)
      GenServer.call(pid, :get_block_count)

      assert_receive {:request, _, _, %{"id" => first_id}}
      assert_receive {:request, _, _, %{"id" => second_id}}
      assert second_id == first_id + 1
    end
  end

  describe "get_block_hash" do
    test "passes the height through as the sole positional parameter" do
      pid = start_rpc!()
      stub_with_inspection(%{"result" => String.duplicate("ab", 32), "error" => nil}, self())

      assert {:ok, hex} = GenServer.call(pid, {:get_block_hash, 800_000})
      assert hex == String.duplicate("ab", 32)
      assert_receive {:request, _, _, %{"method" => "getblockhash", "params" => [800_000]}}
    end
  end

  describe "get_block" do
    test "passes hash + verbosity through as params" do
      pid = start_rpc!()
      stub_with_inspection(%{"result" => %{"height" => 800_000}, "error" => nil}, self())

      hash = String.duplicate("ab", 32)
      assert {:ok, %{"height" => 800_000}} = GenServer.call(pid, {:get_block, hash, 1})
      assert_receive {:request, _, _, %{"method" => "getblock", "params" => [^hash, 1]}}
    end

    test "supports verbosity=2" do
      pid = start_rpc!()
      stub_with_inspection(%{"result" => %{}, "error" => nil}, self())

      hash = String.duplicate("cd", 32)
      assert {:ok, _} = GenServer.call(pid, {:get_block, hash, 2})
      assert_receive {:request, _, _, %{"params" => [^hash, 2]}}
    end
  end

  describe "get_raw_transaction" do
    test "translates verbose=true to params=[txid, 1]" do
      pid = start_rpc!()
      stub_with_inspection(%{"result" => %{"txid" => "x"}, "error" => nil}, self())

      assert {:ok, _} = GenServer.call(pid, {:get_raw_transaction, "dead", true})
      assert_receive {:request, _, _, %{"method" => "getrawtransaction", "params" => ["dead", 1]}}
    end

    test "translates verbose=false to params=[txid, 0]" do
      pid = start_rpc!()
      stub_with_inspection(%{"result" => "0100...", "error" => nil}, self())

      assert {:ok, "0100..."} = GenServer.call(pid, {:get_raw_transaction, "beef", false})
      assert_receive {:request, _, _, %{"params" => ["beef", 0]}}
    end
  end

  describe "send_raw_transaction" do
    test "returns {:ok, txid} from the node" do
      pid = start_rpc!()
      returned_txid = String.duplicate("aa", 32)
      stub_json(%{"result" => returned_txid, "error" => nil})

      assert {:ok, ^returned_txid} = GenServer.call(pid, {:send_raw_transaction, "0100abc"})
    end

    test "surfaces bitcoind tx-rejection as {:error, {:rpc_error, code, msg}}" do
      pid = start_rpc!()

      Req.Test.stub(RpcClient, fn conn ->
        Req.Test.json(conn, %{
          "result" => nil,
          "error" => %{"code" => -26, "message" => "txn-already-known"}
        })
      end)

      assert {:error, {:rpc_error, -26, "txn-already-known"}} =
               GenServer.call(pid, {:send_raw_transaction, "0100abc"})
    end
  end

  describe "error mapping" do
    test "RPC error with code+message → {:error, {:rpc_error, code, msg}}" do
      pid = start_rpc!()

      Req.Test.stub(RpcClient, fn conn ->
        Req.Test.json(conn, %{"result" => nil, "error" => %{"code" => -5, "message" => "no tx"}})
      end)

      assert {:error, {:rpc_error, -5, "no tx"}} =
               GenServer.call(pid, {:get_raw_transaction, "ffff", true})
    end

    test "non-200 HTTP status → {:error, {:http_error, status}}" do
      pid = start_rpc!()

      Req.Test.stub(RpcClient, fn conn -> Plug.Conn.send_resp(conn, 502, "bad gateway") end)

      assert {:error, {:http_error, 502}} = GenServer.call(pid, :get_block_count)
    end

    test "transport-level failure → {:error, {:request_failed, _reason}}" do
      pid = start_rpc!()

      Req.Test.stub(RpcClient, fn conn ->
        Req.Test.transport_error(conn, :econnrefused)
      end)

      assert {:error, {:request_failed, %Req.TransportError{reason: :econnrefused}}} =
               GenServer.call(pid, :get_block_count)
    end
  end

  describe "basic auth header" do
    test "is sent when rpc_user and rpc_password are configured" do
      pid = start_rpc!(rpc_user: "alice", rpc_password: "s3cret")
      stub_with_inspection(%{"result" => 0, "error" => nil}, self())

      GenServer.call(pid, :get_block_count)

      assert_receive {:request, _, headers, _}
      expected = "Basic " <> Base.encode64("alice:s3cret")
      assert {"authorization", ^expected} = Enum.find(headers, &match?({"authorization", _}, &1))
    end

    test "is omitted when credentials are not configured" do
      pid = start_rpc!()
      stub_with_inspection(%{"result" => 0, "error" => nil}, self())

      GenServer.call(pid, :get_block_count)

      assert_receive {:request, _, headers, _}
      refute Enum.any?(headers, fn {k, _} -> String.downcase(k) == "authorization" end)
    end
  end
end
