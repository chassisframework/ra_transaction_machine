defmodule RaTransactionMachineTest do
  use ExUnit.Case
  alias RaTransactionMachine.Participant
  alias RaTransactionMachine.Test.AccountMachine
  alias RaTransactionMachine.Test.ClusterNodes

  setup_all do
    tmp_dir = Application.fetch_env!(:ra, :data_dir)

    Mix.Project.project_file()
    |> Path.dirname()
    |> Path.join(tmp_dir)
    |> File.rm_rf!()

    :ok
  end

  setup do
    start_nodes_and_raft_groups()
  end

  test "commits a simple homogeneous transaction", %{nodes: nodes, participants: [a, b]} do
    assert :ok =
      RaTransactionMachine.transaction(:coordinator, nodes, [a, b], fn txn ->
        AccountMachine.stage_change(txn, a, -50)
        AccountMachine.stage_change(txn, b, 50)
      end)

    assert {:ok, %AccountMachine.State{value: 0}} = state(a)
    assert {:ok, %AccountMachine.State{value: 100}} = state(b)
  end

  test "aborts and rolls back a simple homogeneous transaction", %{nodes: nodes, participants: [a, b]} do
    assert {:error, :aborted} =
      RaTransactionMachine.transaction(:coordinator, nodes, [a, b], fn txn ->
        AccountMachine.stage_change(txn, a, -500)
        AccountMachine.stage_change(txn, b, 500)
      end)

    assert {:ok, %AccountMachine.State{value: 50}} = state(a)
    assert {:ok, %AccountMachine.State{value: 50}} = state(b)
  end

  defp start_nodes_and_raft_groups do
    nodes = ClusterNodes.spawn_nodes(3)

    a = Participant.new(:a, AccountMachine, {:participant_a, nodes})
    b = Participant.new(:b, AccountMachine, {:participant_b, nodes})

    start(:coordinator, AccountMachine, %{value: 0}, nodes)
    start_participant(a, %{value: 50})
    start_participant(b, %{value: 50})

    %{nodes: nodes, participants: [a, b]}
  end

  defp start_participant(%Participant{module: module, private: {name, nodes}}, init_args) do
    start(name, module, init_args, nodes)
  end

  defdelegate start(raft_group, module, init_args, nodes), to: RaUtil, as: :start_group

  defp state(%Participant{private: {name, nodes}}) do
    state(name, nodes)
  end

  defp state(name, nodes) do
    RaUtil.command(name, List.first(nodes), :state)
  end
end
