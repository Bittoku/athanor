defmodule Athanor.Schema.BlockProcessContextTest do
  use Athanor.DataCase, async: true

  alias Athanor.Schema.BlockProcessContext
  import Athanor.Fixtures

  describe "changeset/2" do
    test "valid with required fields" do
      attrs = block_process_context_attrs()
      changeset = BlockProcessContext.changeset(%BlockProcessContext{}, attrs)
      assert changeset.valid?
    end

    test "invalid without id (block hash)" do
      attrs = block_process_context_attrs() |> Map.delete(:id)
      changeset = BlockProcessContext.changeset(%BlockProcessContext{}, attrs)
      refute changeset.valid?
      assert %{id: ["can't be blank"]} = errors_on(changeset)
    end

    test "invalid without height" do
      attrs = block_process_context_attrs() |> Map.delete(:height)
      changeset = BlockProcessContext.changeset(%BlockProcessContext{}, attrs)
      refute changeset.valid?
    end

    test "inserts successfully" do
      ctx = block_process_context_fixture()
      assert ctx.id != nil
      assert ctx.height == 800_000
    end

    test "enforces unique height" do
      block_process_context_fixture(%{id: "hash_a", height: 100})

      {:error, changeset} =
        %BlockProcessContext{}
        |> BlockProcessContext.changeset(%{
          id: "hash_b",
          height: 100,
          processed_at: DateTime.utc_now() |> DateTime.truncate(:second)
        })
        |> Repo.insert()

      assert %{height: ["has already been taken"]} = errors_on(changeset)
    end
  end
end
