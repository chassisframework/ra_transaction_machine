defmodule RaTransactionMachine.CoordinatorMachine do
  @moduledoc false

  require Logger

  alias RaTransactionMachine.Transaction

  @type raft_group_name :: any()
  @type raft_group_state :: RaTransactionMachine.raft_group_state()
  @type id :: RaTransactionMachine.txn_id()
  @opaque transactions :: %{id() => TwoPhaseCommit.t()}

  defmodule State do
    @opaque t :: %__MODULE__{
      name: RaTransactionMachine.raft_group_name(),
      transactions: RaTransactionMachine.CoordinatorMachine.transactions(),
      next_id: integer()
    }

    defstruct [
      :name,
      transactions: %{},
      next_id: 0
    ]
  end

  def handle_command({:create, client, participants}, %State{name: name, transactions: transactions, next_id: id} = state) do
    two_phase_commit = TwoPhaseCommit.new(participants, id: id, client: client)
    transactions = Map.put(transactions, id, two_phase_commit)
    txn = update_txn_nodes(%Transaction{id: id, coordinator_group: name})

    {txn, %State{state | transactions: transactions, next_id: id + 1}}
  end

  def handle_command({:add_participant, %Transaction{id: id} = txn, participant}, %State{transactions: transactions} = state) when is_map_key(transactions, id) do
    transactions = Map.update!(transactions, id, &TwoPhaseCommit.add_participant(&1, participant))
    txn = update_txn_nodes(txn)

    {txn, %State{state | transactions: transactions}}
  end
  def handle_command({:add_participant, _, _}, state), do: {{:error, :not_found}, state}

  def handle_command({:prepare, %Transaction{id: id} = transaction}, %State{transactions: transactions} = state) when is_map_key(transactions, id) do
    case TwoPhaseCommit.prepare(transactions[id]) do
      {:ok, two_phase_commit} ->
        {:vote, participants} = TwoPhaseCommit.next_action(two_phase_commit)

        state = %State{state | transactions: Map.put(transactions, id, two_phase_commit)}
        effects = Enum.map(participants, &send_participant_msg_effect(&1, {:transaction_participant, {:prepare, &1, transaction}}))

        {:ok, state, effects}

      error ->
        {error, state}
    end
  end
  def handle_command({:prepare, _id}, state), do: {{:error, :not_found}, state}

  def handle_command({:participant_prepared, participant, %Transaction{id: id} = transaction}, %State{transactions: transactions} = state) when is_map_key(transactions, id) do
    two_phase_commit = Map.get(transactions, id)

    with {:ok, two_phase_commit} <- TwoPhaseCommit.prepared(two_phase_commit, participant) do
      state = %State{state | transactions: Map.put(transactions, id, two_phase_commit)}

      case TwoPhaseCommit.next_action(two_phase_commit) do
        {:commit, participants} ->
          # all participants have voted to commit
          effects = Enum.map(participants, &send_participant_msg_effect(&1, {:transaction_participant, {:commit, &1, transaction}}))

          {:ok, state, effects}

        {:vote, _} ->
          # awaiting more votes
          {:ok, state}

        {:roll_back, _} ->
          # a participant voted to abort, they'll be sent a rollback command
          {:ok, state}
      end
    else
      error ->
        {{:error, error}, state}
    end
  end
  def handle_command({:participant_prepared, _participant_id, _transaction}, state), do: {{:error, :not_found}, state}

  def handle_command({:participant_committed, participant, %Transaction{id: id} = transaction}, %State{transactions: transactions} = state) when is_map_key(transactions, id) do
    two_phase_commit = Map.get(transactions, id)

    with {:ok, two_phase_commit} <- TwoPhaseCommit.committed(two_phase_commit, participant) do
      state = %State{state | transactions: Map.put(transactions, id, two_phase_commit)}

      case TwoPhaseCommit.next_action(two_phase_commit) do
        nil ->
          # all participants have committed
          effects =
            two_phase_commit
            |> TwoPhaseCommit.participants()
            |> Enum.map(&send_participant_msg_effect(&1, {:transaction_participant, {:clean_up, transaction}}))

          effects = [{:send_msg, two_phase_commit.client, {transaction, :committed}} | effects]

          {:ok, state, effects}

        {:commit, _} ->
          # awaiting more commit acks
          {:ok, state}
      end
    else
      error ->
        {{:error, error}, state}
    end
  end
  def handle_command({:participant_committed, _participant_id, _transaction}, state), do: {{:error, :not_found}, state}

  def handle_command({:participant_aborted, participant, %Transaction{id: id} = transaction}, %State{transactions: transactions} = state) when is_map_key(transactions, id) do
    two_phase_commit = Map.get(transactions, id)

    with {:ok, two_phase_commit} <- TwoPhaseCommit.aborted(two_phase_commit, participant),
         {:roll_back, participants} <- TwoPhaseCommit.next_action(two_phase_commit) do
      state = %State{state | transactions: Map.put(transactions, id, two_phase_commit)}
      effects = Enum.map(participants, &send_participant_msg_effect(&1, {:transaction_participant, {:roll_back, &1, transaction}}))

      {:ok, state, effects}
    else
      error ->
        {{:error, error}, state}
    end
  end
  def handle_command({:participant_aborted, _participant_id, _transaction}, state), do: {{:error, :not_found}, state}

  def handle_command({:participant_rolled_back, participant, %Transaction{id: id} = transaction}, %State{transactions: transactions} = state) when is_map_key(transactions, id) do
    two_phase_commit = Map.get(transactions, id)

    with {:ok, two_phase_commit} <- TwoPhaseCommit.rolled_back(two_phase_commit, participant) do
      state = %State{state | transactions: Map.put(transactions, id, two_phase_commit)}

      case TwoPhaseCommit.next_action(two_phase_commit) do
        nil ->
          # all participants have rolled back
          effects =
            two_phase_commit
            |> TwoPhaseCommit.participants()
            |> Enum.map(&send_participant_msg_effect(&1, {:transaction_participant, {:clean_up, transaction}}))

          state = %State{state | transactions: Map.delete(transactions, id)}
          effects = [{:send_msg, two_phase_commit.client, {transaction, :aborted}} | effects]

          {:ok, state, effects}

        {:roll_back, _} ->
          # awaiting more rollback acks
          {:ok, state}
      end
    else
      error ->
        {{:error, error}, state}
    end
  end
  def handle_command({:participant_rolled_back, _participant_id, _transaction}, state), do: {{:error, :not_found}, state}

  def handle_command(:state, state) do
    {state, state}
  end

  def state_enter(:leader, _state) do
    []
  end

  def state_enter(_role, _state), do: []

  defp send_participant_msg_effect(participant, msg) do
    {:mod_call, participant.module, :send, [participant.private, msg]}
  end

  # updates the location of the group's leader in the client's %Transaction{}
  def update_txn_nodes(%Transaction{coordinator_group: coordinator_group} = txn) do
    {_, leader} = :ra_leaderboard.lookup_leader(coordinator_group)

    nodes =
      coordinator_group
      |> :ra_leaderboard.lookup_members()
      |> Keyword.values()
      |> List.delete(leader)

    %Transaction{txn | coordinator_nodes: [leader | nodes]}
  end

  defmacro __using__(_opts) do
    quote do
      @before_compile unquote(__MODULE__)

      def apply(_meta, {:transaction_coordinator, command}, raft_group_state) do
        state = get_coordinator_state(raft_group_state)

        case unquote(__MODULE__).handle_command(command, state) do
          {response, state} ->
            {put_coordinator_state(raft_group_state, state), response}

          {response, state, effects} ->
            {put_coordinator_state(raft_group_state, state), response, effects}
        end
      end
    end
  end

  defmacro __before_compile__(_opts) do
    quote do
      defoverridable init: 1

      # def init(args) do
      #   unquote(__MODULE__).init(args)
      def init(%{name: name} = args) do
        state = %State{name: name}

        args
        |> super()
        |> put_coordinator_state(state)
      end

      if Module.defines?(__MODULE__, {:state_enter, 2}, :def) do
        defoverridable state_enter: 2

        def state_enter(role, state) do
          [
            super(role, state) |
            unquote(__MODULE__).state_enter(role, state)
          ]
        end
      else
        def state_enter(role, state) do
          unquote(__MODULE__).state_enter(role, state)
        end
      end
    end
  end
end
