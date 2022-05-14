defmodule RaTransactionMachine.Transaction do
  @type raft_group_name :: RaTransactionMachine.raft_group_name()
  @type txn_id :: integer()

  @type t :: %__MODULE__{
    id: txn_id(),
    coordinator_group: raft_group_name(),
    coordinator_nodes: RaTransactionMachine.nodes()
  }

  defstruct [:id, :coordinator_group, :coordinator_nodes]
end
