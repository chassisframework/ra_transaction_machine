defmodule RaTransactionMachine.Coordinator do
  @moduledoc false

  alias RaTransactionMachine.Transaction
  alias RaTransactionMachine.CoordinatorMachine.State

  @type raft_group_state :: RaTransactionMachine.raft_group_state()
  @type participant_name :: RaTransactionMachine.participant_name()
  @type participant :: RaTransactionMachine.participant()
  @type participants :: RaTransactionMachine.participants()
  @type nodes :: RaTransactionMachine.nodes()

  @callback put_coordinator_state(raft_group_state(), State.t()) :: raft_group_state()
  @callback get_coordinator_state(raft_group_state()) :: State.t()

  def send(%Transaction{coordinator_group: raft_group_name, coordinator_nodes: [node | _]}, message) do
    RaUtil.command(raft_group_name, node, message)
  end

  def participant_prepared_message(participant, txn) do
    {:transaction_coordinator, {:participant_prepared, participant, txn}}
  end

  def participant_committed_message(participant, txn) do
    {:transaction_coordinator, {:participant_committed, participant, txn}}
  end

  def participant_aborted_message(participant, txn) do
    {:transaction_coordinator, {:participant_aborted, participant, txn}}
  end

  def participant_rolled_back_message(participant, txn) do
    {:transaction_coordinator, {:participant_rolled_back, participant, txn}}
  end
end
