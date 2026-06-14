defmodule Athanor.Infra.BitailsTest do
  @moduledoc """
  Covers the Bitails REST client. Requests are routed through a `Req.Test`
  stub via `Application.put_env(:athanor, :bitails_http_opts, plug: ...)`.
  """

  use ExUnit.Case, async: false

  alias Athanor.Infra.Bitails

  setup do
    Req.Test.set_req_test_to_shared(%{})
    Application.put_env(:athanor, :bitails_http_opts, plug: {Req.Test, Bitails}, retry: false)
    on_exit(fn -> Application.delete_env(:athanor, :bitails_http_opts) end)
    :ok
  end

  defp stub(fun), do: Req.Test.stub(Bitails, fun)

  ## ── Tests ──

  describe "get_tx/1" do
    test "returns the parsed JSON map on 200" do
      stub(fn conn ->
        assert conn.request_path == "/tx/abcd"
        Req.Test.json(conn, %{"txid" => "abcd"})
      end)

      assert {:ok, %{"txid" => "abcd"}} = Bitails.get_tx("abcd")
    end

    test "returns {:error, {:http_error, status}} on non-200" do
      stub(fn conn -> Plug.Conn.send_resp(conn, 404, "{}") end)
      assert {:error, {:http_error, 404}} = Bitails.get_tx("abcd")
    end

    test "surfaces transport errors" do
      stub(fn conn -> Req.Test.transport_error(conn, :nxdomain) end)
      assert {:error, %Req.TransportError{reason: :nxdomain}} = Bitails.get_tx("abcd")
    end
  end

  describe "get_raw_tx/1" do
    test "returns the trimmed body on 200" do
      stub(fn conn ->
        assert conn.request_path == "/tx/dead/raw"
        Plug.Conn.send_resp(conn, 200, " 0100abcdef ")
      end)

      assert {:ok, "0100abcdef"} = Bitails.get_raw_tx("dead")
    end

    test "returns {:error, {:http_error, status}} on non-200" do
      stub(fn conn -> Plug.Conn.send_resp(conn, 502, "") end)
      assert {:error, {:http_error, 502}} = Bitails.get_raw_tx("dead")
    end
  end

  describe "get_address_balance/1" do
    test "returns the parsed JSON map on 200" do
      stub(fn conn ->
        assert conn.request_path == "/address/1A1zP/balance"
        Req.Test.json(conn, %{"confirmed" => 100_000, "unconfirmed" => 0})
      end)

      assert {:ok, %{"confirmed" => 100_000}} = Bitails.get_address_balance("1A1zP")
    end

    test "returns {:error, {:http_error, status}} on non-200" do
      stub(fn conn -> Plug.Conn.send_resp(conn, 503, "") end)
      assert {:error, {:http_error, 503}} = Bitails.get_address_balance("1A1zP")
    end
  end
end
