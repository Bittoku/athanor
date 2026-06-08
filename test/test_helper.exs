ExUnit.start()

# Live-network tests (P2P smoke against real seed nodes) are tagged `:external`
# and excluded from the default `mix test` run. Run them explicitly with
# `mix test --only external`. See docs/thin-node-p2p-phase1-tasks.md (T1.S).
ExUnit.configure(exclude: [external: true])

Ecto.Adapters.SQL.Sandbox.mode(Athanor.Repo, :manual)

# Start stub GenServers for blockchain/indexer services not running in test
{:ok, _} = Athanor.Test.RpcClientStub.start_link()
{:ok, _} = Athanor.Test.TransactionFilterStub.start_link()
