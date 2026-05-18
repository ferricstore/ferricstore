defmodule FerricStore.Flow.Worker do
  @moduledoc """
  Optional polling worker for `FerricStore.Flow.Workflow` modules.

  The worker is a convenience loop around `workflow.claim_due/2`. It does not
  add a scheduler or queue truth layer. Flow remains the durable source of
  leases, retries, fencing tokens, and state transitions.

  ## Example

      children = [
        {FerricStore.Flow.Worker,
         workflow: BillingFlow,
         state: :created,
         worker: "payment-\#{node()}",
         limit: 100,
         interval_ms: 250,
         handler: &MyApp.PaymentWorker.handle/1}
      ]

  Handler return values:

    * `{:ok, result}` - calls `workflow.ok(job, result)`
    * `{:error, reason}` - calls `workflow.error(job, reason)`
    * `:noreply` - leaves the job leased; handler owns the final Flow command

  Exceptions are caught and passed to `workflow.error/2`. This keeps worker
  crashes from losing the lease path; expired leases can still be reclaimed by
  later `claim_due` or `reclaim` calls.
  """

  use GenServer

  @type option ::
          {:workflow, module()}
          | {:state, atom() | binary() | [atom() | binary()]}
          | {:worker, binary()}
          | {:handler, (FerricStore.Flow.Job.t() -> term())}
          | {:limit, pos_integer()}
          | {:interval_ms, non_neg_integer()}
          | {:claim_opts, keyword()}
          | GenServer.option()

  @spec start_link([option()]) :: GenServer.on_start()
  def start_link(opts) when is_list(opts) do
    {server_opts, opts} =
      Keyword.split(opts, [:name, :timeout, :debug, :spawn_opt, :hibernate_after])

    GenServer.start_link(__MODULE__, opts, server_opts)
  end

  @impl true
  def init(opts) do
    with {:ok, state} <- build_state(opts) do
      send(self(), :poll)
      {:ok, state}
    end
  end

  @impl true
  def handle_info(:poll, state) do
    state = poll(state)
    schedule_next(state)
    {:noreply, state}
  end

  @impl true
  def handle_info(_message, state), do: {:noreply, state}

  defp build_state(opts) do
    with {:ok, workflow} <- required_module(opts, :workflow),
         {:ok, worker} <- required_binary(opts, :worker),
         {:ok, handler} <- required_handler(opts),
         {:ok, limit} <- positive_integer(opts, :limit, 100),
         {:ok, interval_ms} <- non_neg_integer(opts, :interval_ms, 250),
         claim_opts when is_list(claim_opts) <- Keyword.get(opts, :claim_opts, []) do
      {:ok,
       %{
         workflow: workflow,
         state: Keyword.get(opts, :state, :any),
         worker: worker,
         handler: handler,
         limit: limit,
         interval_ms: interval_ms,
         claim_opts: claim_opts
       }}
    else
      {:error, _reason} = error -> error
      _ -> {:error, {:bad_option, :claim_opts}}
    end
  end

  defp poll(%{workflow: workflow, state: flow_state} = state) do
    opts =
      state.claim_opts
      |> Keyword.put_new(:worker, state.worker)
      |> Keyword.put_new(:limit, state.limit)

    case workflow.claim_due(flow_state, opts) do
      {:ok, jobs} ->
        Enum.each(jobs, &handle_job(state, &1))
        state

      {:error, reason} ->
        :telemetry.execute(
          [:ferricstore, :flow, :worker, :claim_due, :error],
          %{count: 1},
          %{workflow: workflow, worker: state.worker, reason: reason}
        )

        state
    end
  end

  defp handle_job(%{workflow: workflow, handler: handler}, job) do
    case call_handler(handler, job) do
      {:ok, result} ->
        workflow.ok(job, result)

      {:error, reason} ->
        workflow.error(job, reason)

      :noreply ->
        :ok

      other ->
        workflow.error(job, {:unexpected_worker_result, other})
    end
  rescue
    exception ->
      workflow.error(job, {exception.__struct__, Exception.message(exception)})
  catch
    kind, reason ->
      workflow.error(job, {kind, reason})
  end

  defp call_handler(handler, job) when is_function(handler, 1), do: handler.(job)

  defp schedule_next(%{interval_ms: interval_ms}) do
    Process.send_after(self(), :poll, interval_ms)
  end

  defp required_module(opts, key) do
    case Keyword.get(opts, key) do
      module when is_atom(module) -> {:ok, module}
      _ -> {:error, {:missing_option, key}}
    end
  end

  defp required_binary(opts, key) do
    case Keyword.get(opts, key) do
      value when is_binary(value) and value != "" -> {:ok, value}
      _ -> {:error, {:missing_option, key}}
    end
  end

  defp required_handler(opts) do
    case Keyword.get(opts, :handler) do
      handler when is_function(handler, 1) -> {:ok, handler}
      _ -> {:error, {:missing_option, :handler}}
    end
  end

  defp positive_integer(opts, key, default) do
    case Keyword.get(opts, key, default) do
      value when is_integer(value) and value > 0 -> {:ok, value}
      _ -> {:error, {:bad_option, key}}
    end
  end

  defp non_neg_integer(opts, key, default) do
    case Keyword.get(opts, key, default) do
      value when is_integer(value) and value >= 0 -> {:ok, value}
      _ -> {:error, {:bad_option, key}}
    end
  end
end
