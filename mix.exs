defmodule RaTransactionMachine.MixProject do
  use Mix.Project

  @version "0.1.0"

  def project do
    [
      app: :ra_transaction_machine,
      version: @version,
      elixir: "~> 1.11",
      start_permanent: Mix.env() == :prod,
      elixirc_paths: elixirc_paths(Mix.env),
      deps: deps()
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:ra_util, path: "../ra_util"},
      {:two_phase_commit, path: "../two_phase_commit"},
      {:arq, path: "../arq"}
    ]
  end

  defp elixirc_paths(:dev), do: ["lib", "test/support"]
  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]
end
