defmodule RaTransactionMachine.ParticipantMachine do
  require Logger

  alias RaTransactionMachine.Coordinator
  alias RaTransactionMachine.Transaction

  @type raft_group_state :: RaTransactionMachine.raft_group_state()

  @type id :: RaTransactionMachine.txn_id()
  @type vote :: :commit | :abort
  @type txn_state :: {:voted, vote()} | :committed | :rolled_back
  @type transactions :: %{id() => txn_state}

  @callback put_participant_state(raft_group_state(), State.t()) :: raft_group_state()
  @callback get_participant_state(raft_group_state()) :: State.t()
  @callback transaction_vote(id, raft_group_state()) :: vote()
  @callback transaction_commit(id, raft_group_state()) :: raft_group_state()
  @callback transaction_roll_back(id, raft_group_state()) :: raft_group_state()

  defmacro __using__(_opts) do
    quote do
      @behaviour unquote(__MODULE__)
      @before_compile unquote(__MODULE__)

      def apply(meta, {:transaction_participant, command}, raft_group_state) do
        case unquote(__MODULE__).handle_command(command, __MODULE__, raft_group_state) do
          {response, state} ->
            {state, response}

          {response, state, effects} ->
            {state, response, effects}
        end
      end
    end
  end

  defmodule State do
    @opaque t :: %__MODULE__{
      name: RaTransactionMachine.raft_group_name(),
      transactions: RaTransactionMachine.ParticipantMachine.transactions()
    }

    defstruct [
      :name,
      transactions: %{},
    ]
  end

  def handle_command({:prepare, participant, %Transaction{id: id} = txn}, module, raft_group_state) do
    %State{transactions: transactions} = state = module.get_participant_state(raft_group_state)

    {raft_group_state, vote} =
      case Map.get(transactions, id) do
        # already voted, maybe coordinator missed the message
        {:voted, vote} ->
          {raft_group_state, vote}

        nil ->
          vote = module.transaction_vote(id, raft_group_state)
          state = %State{state | transactions: Map.put(transactions, id, {:voted, vote})}

          {module.put_participant_state(raft_group_state, state), vote}
      end

    message =
      case vote do
        :commit ->
          Coordinator.participant_prepared_message(participant, txn)

        :abort ->
          Coordinator.participant_aborted_message(participant, txn)
      end

    {:ok, raft_group_state, [send_coordinator_message_effect(txn, message)]}
  end

  def handle_command({:commit, participant, %Transaction{id: id} = txn}, module, raft_group_state) do
    %State{transactions: %{^id => {:voted, :commit}} = transactions} = state = module.get_participant_state(raft_group_state)

    state = %State{state | transactions: Map.put(transactions, id, :committed)}
    raft_group_state =
      id
      |> module.transaction_commit(raft_group_state)
      |> module.put_participant_state(state)

    message = Coordinator.participant_committed_message(participant, txn)

    {:ok, raft_group_state, [send_coordinator_message_effect(txn, message)]}
  end

  def handle_command({:roll_back, participant, %Transaction{id: id} = txn}, module, raft_group_state) do
    %State{transactions: %{^id => {:voted, _vote}} = transactions} = state = module.get_participant_state(raft_group_state)

    state = %State{state | transactions: Map.put(transactions, id, :rolled_back)}
    raft_group_state =
      id
      |> module.transaction_roll_back(raft_group_state)
      |> module.put_participant_state(state)

    message = Coordinator.participant_rolled_back_message(participant, txn)

    {:ok, raft_group_state, [send_coordinator_message_effect(txn, message)]}
  end

  def handle_command({:clean_up, %Transaction{id: id}}, module, raft_group_state) do
    %State{transactions: transactions} = state = module.get_participant_state(raft_group_state)
    state = %State{state | transactions: Map.delete(transactions, id)}

    {:ok, module.put_participant_state(raft_group_state, state)}
  end

  def send_coordinator_message_effect(txn, message) do
    {:mod_call, ARQ, :start, [{__MODULE__, :send_coordinator_msg, [txn, message]}]}
  end

  def send_coordinator_msg(txn, message) do
    case Coordinator.send(txn, message) do
      {:ok, _} ->
        ARQ.stop(self())

      error ->
        # TODO: after a number of failed sends, abort the transaction if it's abortable?
        IO.inspect(error)
        error
    end
  end

  defmacro __before_compile__(_opts) do
    quote do
      defoverridable init: 1

      def init(%{name: name} = args) do
        state = %State{name: name}

        args
        |> super()
        |> put_participant_state(state)
      end

      def send({raft_group_name, [node | _]}, message) do
        case RaUtil.command(raft_group_name, node, message) do
          {:ok, reply} ->
            reply

          error ->
            error
        end
      end

      # if Module.defines?(__MODULE__, {:state_enter, 2}, :def) do
      #   defoverridable state_enter: 2

      #   def state_enter(role, state) do
      #     [
      #       super(role, state) |
      #       unquote(__MODULE__).state_enter(role, state)
      #     ]
      #   end
      # else
      #   def state_enter(role, state) do
      #     unquote(__MODULE__).state_enter(role, state)
      #   end
      # end
    end
  end
end
