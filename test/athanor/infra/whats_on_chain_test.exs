defmodule Athanor.Infra.WhatsOnChainTest do
  @moduledoc """
  Covers the WoC REST client. Requests are routed through a `Req.Test`
  stub via `Application.put_env(:athanor, :woc_http_opts, plug: ...)`
  so no network access is required.
  """

  use ExUnit.Case, async: false

  alias Athanor.Infra.WhatsOnChain

  setup do
    Req.Test.set_req_test_to_shared(%{})

    # Reset network up-front so order-shuffled tests can't inherit
    # the testnet override from an earlier sibling test.
    Application.delete_env(:athanor, :network)
    Application.put_env(:athanor, :woc_http_opts, plug: {Req.Test, WhatsOnChain}, retry: false)

    on_exit(fn ->
      Application.delete_env(:athanor, :woc_http_opts)
      Application.delete_env(:athanor, :network)
    end)

    :ok
  end

  defp stub(fun), do: Req.Test.stub(WhatsOnChain, fun)

  ## ── Tests ──

  describe "get_raw_tx/1" do
    test "returns the trimmed body on 200" do
      stub(fn conn ->
        assert conn.request_path == "/v1/bsv/main/tx/dead/hex"
        Plug.Conn.send_resp(conn, 200, "0100abcdef\n")
      end)

      assert {:ok, "0100abcdef"} = WhatsOnChain.get_raw_tx("dead")
    end

    test "returns {:error, {:http_error, status}} on non-200" do
      stub(fn conn -> Plug.Conn.send_resp(conn, 404, "not found") end)

      assert {:error, {:http_error, 404}} = WhatsOnChain.get_raw_tx("dead")
    end

    test "surfaces transport errors" do
      stub(fn conn -> Req.Test.transport_error(conn, :timeout) end)

      assert {:error, %Req.TransportError{reason: :timeout}} = WhatsOnChain.get_raw_tx("dead")
    end
  end

  describe "get_tx/1" do
    test "returns the parsed JSON map on 200" do
      stub(fn conn ->
        assert conn.request_path == "/v1/bsv/main/tx/hash/abcd"
        Req.Test.json(conn, %{"txid" => "abcd", "blockheight" => 800_000})
      end)

      assert {:ok, %{"txid" => "abcd", "blockheight" => 800_000}} = WhatsOnChain.get_tx("abcd")
    end

    test "returns {:error, {:http_error, status}} on non-200" do
      stub(fn conn -> Plug.Conn.send_resp(conn, 500, "{}") end)

      assert {:error, {:http_error, 500}} = WhatsOnChain.get_tx("abcd")
    end
  end

  describe "get_address_utxos/1" do
    test "returns the parsed JSON list on 200" do
      stub(fn conn ->
        assert conn.request_path == "/v1/bsv/main/address/1A1zP/unspent"

        Req.Test.json(conn, [
          %{"tx_hash" => "a", "value" => 100},
          %{"tx_hash" => "b", "value" => 200}
        ])
      end)

      assert {:ok, [%{"tx_hash" => "a"}, %{"tx_hash" => "b"}]} =
               WhatsOnChain.get_address_utxos("1A1zP")
    end

    test "returns {:error, {:http_error, status}} on non-200" do
      stub(fn conn -> Plug.Conn.send_resp(conn, 503, "") end)

      assert {:error, {:http_error, 503}} = WhatsOnChain.get_address_utxos("1A1zP")
    end
  end

  describe "get_address_history/1" do
    test "extracts tx_hash from each entry" do
      stub(fn conn ->
        Req.Test.json(conn, [
          %{"tx_hash" => "txA", "height" => 100},
          %{"tx_hash" => "txB", "height" => 101}
        ])
      end)

      assert {:ok, ["txA", "txB"]} = WhatsOnChain.get_address_history("1A1zP")
    end

    test "filters entries that have a nil tx_hash" do
      stub(fn conn ->
        Req.Test.json(conn, [
          %{"tx_hash" => "txA"},
          %{"height" => 99},
          %{"tx_hash" => "txB"}
        ])
      end)

      assert {:ok, ["txA", "txB"]} = WhatsOnChain.get_address_history("1A1zP")
    end

    test "returns {:error, {:http_error, status}} on non-200" do
      stub(fn conn -> Plug.Conn.send_resp(conn, 429, "rate limited") end)

      assert {:error, {:http_error, 429}} = WhatsOnChain.get_address_history("1A1zP")
    end
  end

  describe "network selection" do
    test "uses /main when :network is mainnet (default)" do
      stub(fn conn ->
        assert conn.request_path == "/v1/bsv/main/tx/x/hex"
        Plug.Conn.send_resp(conn, 200, "00")
      end)

      WhatsOnChain.get_raw_tx("x")
    end

    test "uses /test when :network is testnet" do
      Application.put_env(:athanor, :network, "testnet")

      stub(fn conn ->
        assert conn.request_path == "/v1/bsv/test/tx/x/hex"
        Plug.Conn.send_resp(conn, 200, "00")
      end)

      WhatsOnChain.get_raw_tx("x")
    end
  end
end
