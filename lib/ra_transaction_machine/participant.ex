defmodule RaTransactionMachine.Participant do
  @moduledoc false

  alias TwoPhaseCommit.Participant, as: StateMachineParticipant

  @type private :: any()

  @type t :: %__MODULE__{
    id: any(),
    module: module(),
    private: private()
  }

  @callback send(private(), message :: any()) :: :ok

  defstruct [:id, :module, :private]

  defimpl StateMachineParticipant do
    def id(%@for{id: id}), do: id
  end

  def new(id, module, private) do
    %__MODULE__{id: id, module: module, private: private}
  end
end
