defmodule RaTransactionMachine do
  alias RaTransactionMachine.Coordinator
  alias RaTransactionMachine.CoordinatorMachine
  alias RaTransactionMachine.Participant
  alias RaTransactionMachine.ParticipantMachine

  @type raft_group_name :: any()
  @type participant_name :: any()
  @type participant :: Participant.t()
  @type participants :: [participant()]
  @type nodes :: [node()]

  # def go do
  #   participant_nodes = RaTransactionMachine.Test.ClusterNodes.spawn_nodes(3)

  #   # coordinator_raft_group = :abc
  #   participant_module = RaTransactionMachine.Test.AccountMachine

  #   # start(coordinator_raft_group, coordinator_nodes)
  #   start_participant(:a, participant_nodes)
  #   start_participant(:b, participant_nodes)

  #   {:ok, txn} = create(:a, participant_nodes)

  #   {:ok, txn} = add(txn, :a, participant_nodes, participant_module)
  #   {:ok, txn} = add(txn, :b, participant_nodes, participant_module)

  #   AccountMachine.stage_change(:a, participant_nodes, txn, -100)
  #   AccountMachine.stage_change(:b, participant_nodes, txn, 100)

  #   # state(coordinator_nodes) |> IO.inspect()

  #   :ok = prepare(txn)

  #   receive do
  #     {:committed, ^txn} ->
  #       IO.puts "committed!"

  #     msg ->
  #       IO.inspect msg
  #   end
  # end

  def transaction(coordinator_raft_group, coordinator_nodes, participants, func) do
    with {:ok, txn} <- create(coordinator_raft_group, coordinator_nodes, participants),
         func.(txn),
         :ok <- prepare(txn) do
      receive do
        {^txn, :committed} ->
          :ok

        {^txn, :aborted} ->
          {:error, :aborted}
      end
    end
  end

  #
  # TODO: allow client to provide list of participants up front
  #
  def create(coordinator_raft_group, coordinator_nodes, participants) do
    RaUtil.command(coordinator_raft_group, List.first(coordinator_nodes), {:transaction_coordinator, {:create, self(), participants}})
  end

  def add(txn, %Participant{} = participant) do
    msg = {:transaction_coordinator, {:add_participant, txn, participant}}

    RaUtil.command(txn.coordinator_group, List.first(txn.coordinator_nodes), msg)
  end

  def prepare(txn) do
    msg = {:transaction_coordinator, {:prepare, txn}}

    case RaUtil.command(txn.coordinator_group, List.first(txn.coordinator_nodes), msg) do
      {:ok, :ok} ->
        :ok

      error ->
        error
    end
  end

  defmacro __using__(opts) do
    {behaviour, module} =
      case Keyword.fetch!(opts, :role) do
        :coordinator ->
          {Coordinator, CoordinatorMachine}

        :participant ->
          {Participant, ParticipantMachine}
      end

    quote do
      @behaviour unquote(behaviour)

      use unquote(module), unquote(opts)
    end
  end
end
