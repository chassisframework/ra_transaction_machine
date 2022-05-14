defmodule RaTransactionMachine.Test.AccountMachine do
  use RaTransactionMachine, role: :coordinator
  use RaTransactionMachine, role: :participant

  alias RaTransactionMachine.Participant
  alias RaTransactionMachine.Transaction

  defmodule State do
    defstruct [
      :coordinator_state,
      :participant_state,
      value: 0,
      staged_changes: %{}
    ]
  end

  def stage_change(%Transaction{id: txn_id}, %Participant{private: {raft_group, nodes}}, value) do
    RaUtil.command(raft_group, List.first(nodes), {:stage_change, txn_id, value})
  end



  @impl true
  def put_coordinator_state(%State{} = state, coordinator_state) do
    %State{state | coordinator_state: coordinator_state}
  end

  @impl true
  def get_coordinator_state(%State{coordinator_state: coordinator_state}) do
    coordinator_state
  end

  @impl true
  def put_participant_state(%State{} = state, participant_state) do
    %State{state | participant_state: participant_state}
  end

  @impl true
  def get_participant_state(%State{participant_state: participant_state}) do
    participant_state
  end

  @impl true
  def transaction_vote(id, %State{staged_changes: staged_changes, value: value}) do
    staged_change = Map.get(staged_changes, id, 0)

    if staged_change + value >= 0 do
      :commit
    else
      :abort
    end
  end

  @impl true
  def transaction_commit(id, %State{staged_changes: staged_changes, value: value} = state) do
    staged_change = Map.get(staged_changes, id, 0)

    %State{state | staged_changes: Map.delete(staged_changes, id), value: value + staged_change}
  end

  @impl true
  def transaction_roll_back(id, %State{staged_changes: staged_changes} = state) do
    %State{state | staged_changes: Map.delete(staged_changes, id)}
  end

  def init(%{app_args: %{value: value}}) do
    %State{value: value}
  end

  def state_enter(_role, _state) do
    []
  end

  def apply(_meta, {:stage_change, txn_id, value}, %State{staged_changes: staged_changes} = state) do
    staged_changes = Map.put(staged_changes, txn_id, value)

    {%State{state | staged_changes: staged_changes}, :ok}
  end

  def apply(_meta, :state, state) do
    {state, state}
  end

  def apply(_meta, _command, state) do
    {state, :ok, []}
  end
end
