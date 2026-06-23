defmodule FerricStore.Flow.Workflow do
  @moduledoc """
  Elixir workflow SDK for FerricStore Flow.

  This module is an ergonomic layer over the embedded `flow_*` API. It does not
  change Flow correctness semantics: every mutation is still an explicit Flow
  command, guarded by the same lease/fencing tokens.

  ## Design

  `FerricStore.Flow.Workflow` gives Elixir users a Temporal-like call-site
  without adding Temporal-style replay. A workflow module declares Flow type,
  partitioning, states, retry policy, lease defaults, and success/error actions.
  The generated functions call the embedded FerricStore API directly.

      defmodule BillingFlow do
        use FerricStore.Flow.Workflow,
          type: "billing",
          partition_by: [:tenant_id, :invoice_id],
          initial_state: :created

        state :created do
          lease_ms 60_000
          claim_payload true, max_bytes: 64_000
          retry max_retries: 8,
                backoff: [kind: :exponential, base_ms: 1_000, max_ms: :timer.hours(1)]
          on_ok :charged
          on_error retry_or: :failed
        end

        state :charged do
          on_ok complete()
          on_error fail()
        end
      end

  This compiles to helpers such as `BillingFlow.create/2`,
  `BillingFlow.claim_due/2`, `BillingFlow.ok/3`, and `BillingFlow.error/3`.

  ## Core rule

  The SDK is not a hidden transaction engine. Reads and writes outside Flow are
  normal FerricStore calls. Flow state changes must still happen through explicit
  Flow commands.

  The generated path is:

      SDK call
      -> embedded `FerricStore.flow_*` call
      -> Ra/Bitcask Flow command
      -> hot Flow indexes
      -> async cold projections

  No external protocol client is used by this SDK.

  ## Options

    * `:type` - required Flow type. All generated commands use this type.
    * `:store` - module that exposes embedded `flow_*` functions. Defaults to
      `FerricStore`. Use this to point at a `use FerricStore` instance module in
      embedded mode or a fake module in tests.
    * `:partition_by` - list of attr keys used to build `partition_key`. Values
      are joined with `":"`. Same partition keeps ordering on the same shard.
    * `:initial_state` - state used by `create/2`. Defaults to first declared
      state.

  ## State DSL

  `state/2` declares defaults for claim and worker behavior:

    * `lease_ms n` - default lease used by `claim_due/2`.
    * `claim_payload boolean, max_bytes: n` - default payload hydration policy
      for claims.
    * `retry opts` - per-state retry override. Supports the same options as
      `flow_retry/3`: `:max_retries`, `:backoff`, and `:exhausted_to`.
    * `on_ok state` - `ok/3` transitions to another state.
    * `on_ok complete()` - `ok/3` completes the Flow.
    * `on_error retry_or: state` - `error/3` retries until policy is exhausted,
      then moves to `state`.
    * `on_error fail()` - `error/3` fails terminally.

  ## Generated API

  Each workflow module gets these functions:

    * `create(attrs, opts \\\\ [])`
    * `create_many(items, opts \\\\ [])`
    * `child(attrs, opts \\\\ [])`
    * `spawn_children(parent, children, opts \\\\ [])`
    * `fanout(parent, children, opts \\\\ [])`
    * `claim_due(state \\\\ :any, opts)`
    * `run_once(state \\\\ :any, opts)`
    * `ok(job, value \\\\ nil, opts \\\\ [])`
    * `error(job, reason, opts \\\\ [])`
    * `handle(job, fun, opts \\\\ [])`
    * `transition(job, to_state, value \\\\ nil, opts \\\\ [])`
    * `complete(job, result \\\\ nil, opts \\\\ [])`
    * `retry(job, reason, opts \\\\ [])`
    * `fail(job, reason, opts \\\\ [])`
    * `extend_lease(job, opts \\\\ [])`
    * `get(id, opts \\\\ [])`
    * `history(id, opts \\\\ [])`
    * `list(state \\\\ :any, opts \\\\ [])`
    * `children(parent, opts \\\\ [])`
    * `waiting_children(parent, opts \\\\ [])`
    * `by_parent(parent_flow_id, opts \\\\ [])`
    * `by_root(root_flow_id, opts \\\\ [])`
    * `by_correlation(correlation_id, opts \\\\ [])`
    * `info(opts \\\\ [])`
    * `stuck(opts \\\\ [])`
    * `reclaim(state \\\\ :running, opts)`
    * `reclaim_once(state \\\\ :running, opts)`
    * `cancel(id, opts \\\\ [])`
    * `rewind(id, opts)`
    * `install_policy(opts \\\\ [])`

  ## Payload rule

  Payload bytes are read only when the command asks for payload hydration.
  `claim_payload true` makes `claim_due/2` request payloads by default, capped by
  `:payload_max_bytes`. Large payloads stay omitted and are represented by
  payload refs/size metadata from core Flow.

  ## Worker

  `FerricStore.Flow.Worker` is optional. You can supervise it, use your own cron,
  or call `claim_due/2` manually. The worker only loops over `claim_due/2` and
  applies `ok/3` or `error/3` based on handler return values.

  ## Children and fanout

  `child/2` builds child specs with workflow defaults. `fanout/3` and
  `spawn_children/3` call `flow_spawn_children/3`, carrying parent partition,
  state, lease token, and fencing token when the parent is a `%Job{}`.

      children = [
        EmailFlow.child(%{id: "email-1", tenant_id: "t1", invoice_id: "i1"}),
        AuditFlow.child(%{id: "audit-1", tenant_id: "t1", invoice_id: "i1"})
      ]

      BillingFlow.fanout(job, children,
        group_id: "notify",
        wait: :all,
        on_all_ok: :notified,
        on_any_error: :notification_failed
      )

  `children/2` and `waiting_children/2` query child records through the parent
  lineage index.
  """

  alias FerricStore.Flow.Job

  defmacro __using__(opts) do
    quote bind_quoted: [opts: opts] do
      import FerricStore.Flow.Workflow,
        only: [
          state: 2,
          retry: 1,
          lease_ms: 1,
          claim_payload: 1,
          claim_payload: 2,
          on_ok: 1,
          on_error: 1,
          complete: 0,
          fail: 0
        ]

      Module.register_attribute(__MODULE__, :flow_sdk_states, accumulate: true)
      @flow_sdk_opts opts
      @before_compile FerricStore.Flow.Workflow
    end
  end

  defmacro state(name, do: block) do
    quote do
      @flow_sdk_current_state unquote(name)
      @flow_sdk_state_config %{name: FerricStore.Flow.Workflow.normalize_state(unquote(name))}
      unquote(block)
      @flow_sdk_states @flow_sdk_state_config
      Module.delete_attribute(__MODULE__, :flow_sdk_current_state)
      Module.delete_attribute(__MODULE__, :flow_sdk_state_config)
    end
  end

  defmacro retry(opts) do
    quote do
      @flow_sdk_state_config FerricStore.Flow.Workflow.put_state_config(
                               @flow_sdk_state_config,
                               :retry,
                               unquote(opts)
                             )
    end
  end

  defmacro lease_ms(value) do
    quote do
      @flow_sdk_state_config FerricStore.Flow.Workflow.put_state_config(
                               @flow_sdk_state_config,
                               :lease_ms,
                               unquote(value)
                             )
    end
  end

  defmacro claim_payload(value) do
    quote do
      @flow_sdk_state_config FerricStore.Flow.Workflow.put_state_config(
                               @flow_sdk_state_config,
                               :claim_payload,
                               unquote(value)
                             )
    end
  end

  defmacro claim_payload(value, opts) do
    quote do
      @flow_sdk_state_config FerricStore.Flow.Workflow.put_state_config(
                               @flow_sdk_state_config,
                               :claim_payload,
                               unquote(value)
                             )

      @flow_sdk_state_config FerricStore.Flow.Workflow.put_claim_payload_opts(
                               @flow_sdk_state_config,
                               unquote(opts)
                             )
    end
  end

  defmacro on_ok(action) do
    quote do
      @flow_sdk_state_config FerricStore.Flow.Workflow.put_state_config(
                               @flow_sdk_state_config,
                               :on_ok,
                               FerricStore.Flow.Workflow.normalize_action(unquote(action))
                             )
    end
  end

  defmacro on_error(action) do
    quote do
      @flow_sdk_state_config FerricStore.Flow.Workflow.put_state_config(
                               @flow_sdk_state_config,
                               :on_error,
                               FerricStore.Flow.Workflow.normalize_error_action(unquote(action))
                             )
    end
  end

  defmacro complete, do: quote(do: {:complete, []})
  defmacro fail, do: quote(do: {:fail, []})

  defmacro __before_compile__(env) do
    opts = Module.get_attribute(env.module, :flow_sdk_opts) || []
    states = Module.get_attribute(env.module, :flow_sdk_states) |> Enum.reverse()

    type =
      opts
      |> Keyword.fetch!(:type)
      |> normalize_type!()

    store = Keyword.get(opts, :store, FerricStore)
    partition_by = Keyword.get(opts, :partition_by, [])
    state_names = Enum.map(states, & &1.name)

    initial_state =
      Keyword.get(opts, :initial_state, List.first(state_names)) |> normalize_state()

    states_by_name = Map.new(states, &{&1.name, Map.delete(&1, :name)})

    escaped_states = Macro.escape(states_by_name)

    quote do
      @doc false
      def __flow_type__, do: unquote(type)

      @doc false
      def __flow_store__, do: unquote(store)

      @doc false
      def __flow_partition_by__, do: unquote(Macro.escape(partition_by))

      @doc false
      def __flow_initial_state__, do: unquote(initial_state)

      @doc false
      def __flow_states__, do: unquote(Macro.escape(state_names))

      @doc false
      def __flow_state_config__(state) do
        Map.get(unquote(escaped_states), FerricStore.Flow.Workflow.normalize_state(state), %{})
      end

      @doc """
      Installs the retry policy declared in this workflow module into the
      embedded Flow policy store.
      """
      def install_policy(opts \\ []) when is_list(opts) do
        policy = FerricStore.Flow.Workflow.policy_opts(unquote(escaped_states))
        unquote(store).flow_policy_set(unquote(type), Keyword.merge(policy, opts))
      end

      @doc "Creates one Flow record for this workflow type."
      def create(attrs, opts \\ []) when is_map(attrs) and is_list(opts) do
        FerricStore.Flow.Workflow.create(__MODULE__, attrs, opts)
      end

      @doc "Creates many Flow records for this workflow type."
      def create_many(items, opts \\ []) when is_list(items) and is_list(opts) do
        FerricStore.Flow.Workflow.create_many(__MODULE__, items, opts)
      end

      @doc "Builds a child Flow spec for `spawn_children/3` or `fanout/3`."
      def child(attrs, opts \\ []) when is_map(attrs) and is_list(opts) do
        FerricStore.Flow.Workflow.child(__MODULE__, attrs, opts)
      end

      @doc "Spawns child Flow records under a parent Flow or claimed job."
      def spawn_children(parent, children, opts \\ []) when is_list(children) and is_list(opts) do
        FerricStore.Flow.Workflow.spawn_children(__MODULE__, parent, children, opts)
      end

      @doc "Alias for `spawn_children/3` with fanout-oriented option names."
      def fanout(parent, children, opts \\ []) when is_list(children) and is_list(opts) do
        FerricStore.Flow.Workflow.spawn_children(__MODULE__, parent, children, opts)
      end

      @doc "Claims due jobs for this workflow type."
      def claim_due(state \\ :any, opts) when is_list(opts) do
        FerricStore.Flow.Workflow.claim_due(__MODULE__, state, opts)
      end

      @doc "Claims one batch and applies a handler to each job."
      def run_once(state \\ :any, opts) when is_list(opts) do
        FerricStore.Flow.Workflow.run_once(__MODULE__, state, opts)
      end

      @doc "Returns one Flow record."
      def get(id, opts \\ []) when is_binary(id) and is_list(opts) do
        unquote(store).flow_get(id, opts)
      end

      @doc "Returns Flow history."
      def history(id, opts \\ []) when is_binary(id) and is_list(opts) do
        unquote(store).flow_history(id, opts)
      end

      def list(state \\ :any, opts \\ []) when is_list(opts) do
        opts = FerricStore.Flow.Workflow.put_state_opt(opts, state)
        unquote(store).flow_list(unquote(type), opts)
      end

      def by_parent(parent_flow_id, opts \\ []) do
        unquote(store).flow_by_parent(parent_flow_id, opts)
      end

      def children(parent, opts \\ []) when is_list(opts) do
        FerricStore.Flow.Workflow.children(__MODULE__, parent, opts)
      end

      def waiting_children(parent, opts \\ []) when is_list(opts) do
        FerricStore.Flow.Workflow.waiting_children(__MODULE__, parent, opts)
      end

      def by_root(root_flow_id, opts \\ []) do
        unquote(store).flow_by_root(root_flow_id, opts)
      end

      def by_correlation(correlation_id, opts \\ []) do
        unquote(store).flow_by_correlation(correlation_id, opts)
      end

      def info(opts \\ []), do: unquote(store).flow_info(unquote(type), opts)

      def stuck(opts \\ []), do: unquote(store).flow_stuck(unquote(type), opts)

      def reclaim(state \\ :running, opts) when is_list(opts) do
        opts = FerricStore.Flow.Workflow.put_state_opt(opts, state)
        unquote(store).flow_reclaim(unquote(type), opts)
      end

      def reclaim_once(state \\ :running, opts) when is_list(opts) do
        FerricStore.Flow.Workflow.reclaim_once(__MODULE__, state, opts)
      end

      @doc "Applies the state's `on_ok` action."
      def ok(%Job{workflow: __MODULE__} = job, value \\ nil, opts \\ []) when is_list(opts) do
        FerricStore.Flow.Workflow.ok(job, value, opts)
      end

      @doc "Applies the state's `on_error` action."
      def error(%Job{workflow: __MODULE__} = job, reason, opts \\ []) when is_list(opts) do
        FerricStore.Flow.Workflow.error(job, reason, opts)
      end

      def handle(%Job{workflow: __MODULE__} = job, fun, opts \\ [])
          when is_function(fun, 1) and is_list(opts) do
        FerricStore.Flow.Workflow.handle(job, fun, opts)
      end

      def transition(%Job{workflow: __MODULE__} = job, to_state, value \\ nil, opts \\ [])
          when is_list(opts) do
        FerricStore.Flow.Workflow.transition(job, to_state, value, opts)
      end

      def complete(%Job{workflow: __MODULE__} = job, result \\ nil, opts \\ [])
          when is_list(opts) do
        FerricStore.Flow.Workflow.complete(job, result, opts)
      end

      def retry(%Job{workflow: __MODULE__} = job, reason, opts \\ []) when is_list(opts) do
        FerricStore.Flow.Workflow.retry(job, reason, opts)
      end

      def fail(%Job{workflow: __MODULE__} = job, reason, opts \\ []) when is_list(opts) do
        FerricStore.Flow.Workflow.fail(job, reason, opts)
      end

      def extend_lease(%Job{workflow: __MODULE__} = job, opts \\ []) when is_list(opts) do
        opts = Job.guard_opts(job, opts)
        unquote(store).flow_extend_lease(job.id, job.lease_token, opts)
      end

      def cancel(id, opts \\ []) when is_binary(id) and is_list(opts) do
        unquote(store).flow_cancel(id, opts)
      end

      def rewind(id, opts) when is_binary(id) and is_list(opts) do
        unquote(store).flow_rewind(id, opts)
      end
    end
  end

  @spec create(module(), map(), keyword()) :: {:ok, map()} | {:error, binary()}
  def create(workflow, attrs, opts) do
    with {:ok, id} <- required_attr(attrs, opts, :id),
         {:ok, partition_key} <- partition_key(workflow, attrs, opts) do
      flow_opts =
        attrs
        |> flow_opts_from_attrs()
        |> Keyword.merge(opts)
        |> Keyword.put_new(:type, workflow.__flow_type__())
        |> Keyword.put_new(:state, workflow.__flow_initial_state__())
        |> Keyword.put_new(:partition_key, partition_key)

      workflow.__flow_store__().flow_create(id, flow_opts)
    end
  end

  @spec create_many(module(), [map()], keyword()) :: {:ok, [map()]} | {:error, binary()}
  def create_many(workflow, items, opts) do
    with {:ok, mapped_items} <- create_many_items(workflow, items, opts) do
      create_opts =
        opts
        |> Keyword.put_new(:type, workflow.__flow_type__())
        |> Keyword.put_new(:state, workflow.__flow_initial_state__())

      workflow.__flow_store__().flow_create_many(nil, mapped_items, create_opts)
    end
  end

  @spec child(module(), map(), keyword()) :: map() | {:error, binary()}
  def child(workflow, attrs, opts) when is_map(attrs) and is_list(opts) do
    with {:ok, id} <- required_attr(attrs, opts, :id),
         {:ok, partition_key} <- optional_child_partition_key(workflow, attrs, opts) do
      attrs
      |> Map.take([
        :payload,
        :payload_ref,
        :run_at_ms,
        :priority,
        :parent_flow_id,
        :root_flow_id,
        :correlation_id,
        :idempotent,
        :retention_ttl_ms,
        :history_hot_max_events,
        :history_max_events
      ])
      |> Map.put(:id, id)
      |> Map.put_new(:type, Keyword.get(opts, :type, workflow.__flow_type__()))
      |> Map.put_new(:state, Keyword.get(opts, :state, workflow.__flow_initial_state__()))
      |> maybe_put_map(:partition_key, partition_key)
    end
  end

  @spec spawn_children(module(), Job.t() | map() | binary(), [map()], keyword()) ::
          {:ok, map()} | {:error, binary()}
  def spawn_children(workflow, parent, children, opts) do
    with {:ok, parent_id} <- parent_id(parent),
         {:ok, spawn_opts} <- spawn_children_opts(parent, opts) do
      workflow.__flow_store__().flow_spawn_children(parent_id, children, spawn_opts)
    end
  end

  @spec claim_due(module(), atom() | binary() | [atom() | binary()], keyword()) ::
          {:ok, [Job.t()]} | {:error, binary()}
  def claim_due(workflow, state, opts) do
    state_config = workflow.__flow_state_config__(state_for_config(state))

    claim_opts =
      opts
      |> Keyword.put_new(:lease_ms, state_config[:lease_ms])
      |> maybe_put_new(:payload, state_config[:claim_payload])
      |> maybe_put_new(:payload_max_bytes, state_config[:payload_max_bytes])
      |> put_state_opt(state)

    case workflow.__flow_store__().flow_claim_due(
           workflow.__flow_type__(),
           compact_opts(claim_opts)
         ) do
      {:ok, records} when is_list(records) ->
        {:ok, Enum.map(records, &Job.new(workflow, &1))}

      other ->
        other
    end
  end

  @spec run_once(module(), atom() | binary() | [atom() | binary()], keyword()) ::
          {:ok, [term()]} | {:error, binary()}
  def run_once(workflow, state, opts) do
    {handler_opts, claim_opts} = Keyword.split(opts, [:handler])

    with {:ok, handler} <- required_handler(handler_opts),
         {:ok, jobs} <- claim_due(workflow, state, claim_opts) do
      {:ok, Enum.map(jobs, &handle(&1, handler, []))}
    end
  end

  @spec reclaim_once(module(), atom() | binary() | [atom() | binary()], keyword()) ::
          {:ok, [Job.t()]} | {:error, binary()}
  def reclaim_once(workflow, state, opts) do
    opts =
      opts
      |> put_state_opt(state)
      |> Keyword.put_new(:reclaim_expired, false)

    case workflow.__flow_store__().flow_reclaim(workflow.__flow_type__(), opts) do
      {:ok, records} when is_list(records) ->
        {:ok, Enum.map(records, &Job.new(workflow, &1))}

      other ->
        other
    end
  end

  @spec ok(Job.t(), term(), keyword()) :: {:ok, map()} | {:error, binary()}
  def ok(%Job{} = job, value, opts) do
    action = job.workflow.__flow_state_config__(job.state)[:on_ok] || {:complete, []}
    apply_action(job, action, value, opts)
  end

  @spec error(Job.t(), term(), keyword()) :: {:ok, map()} | {:error, binary()}
  def error(%Job{} = job, reason, opts) do
    action = job.workflow.__flow_state_config__(job.state)[:on_error] || {:retry, []}
    apply_error_action(job, action, reason, opts)
  end

  @spec handle(Job.t(), (Job.t() -> term()), keyword()) :: term()
  def handle(%Job{} = job, fun, opts) when is_function(fun, 1) and is_list(opts) do
    case fun.(job) do
      {:ok, result} -> ok(job, result, opts)
      {:error, reason} -> error(job, reason, opts)
      :noreply -> :ok
      other -> error(job, {:unexpected_worker_result, other}, opts)
    end
  rescue
    exception ->
      error(job, {exception.__struct__, Exception.message(exception)}, opts)
  catch
    kind, reason ->
      error(job, {kind, reason}, opts)
  end

  @spec transition(Job.t(), atom() | binary(), term(), keyword()) ::
          {:ok, map()} | {:error, binary()}
  def transition(%Job{} = job, to_state, value, opts) do
    command_opts =
      job
      |> Job.lease_guard_opts(opts)
      |> maybe_put_value(:payload, value)

    job.workflow.__flow_store__().flow_transition(
      job.id,
      normalize_state(job.state),
      normalize_state(to_state),
      command_opts
    )
  end

  @spec complete(Job.t(), term(), keyword()) :: {:ok, map()} | {:error, binary()}
  def complete(%Job{} = job, result, opts) do
    command_opts =
      job
      |> Job.guard_opts(opts)
      |> maybe_put_value(:result, result)

    job.workflow.__flow_store__().flow_complete(job.id, job.lease_token, command_opts)
  end

  @spec retry(Job.t(), term(), keyword()) :: {:ok, map()} | {:error, binary()}
  def retry(%Job{} = job, reason, opts) do
    {retry_keys, command_opts} = Keyword.split(opts, [:max_retries, :backoff, :exhausted_to])
    retry_override = Keyword.get(command_opts, :retry, [])

    retry_policy =
      []
      |> Keyword.merge(job.workflow.__flow_state_config__(job.state)[:retry] || [])
      |> Keyword.merge(retry_override || [])
      |> Keyword.merge(retry_keys)

    command_opts =
      job
      |> Job.guard_opts(Keyword.delete(command_opts, :retry))
      |> maybe_put_value(:error, reason)
      |> maybe_put_new(:retry, retry_policy)

    job.workflow.__flow_store__().flow_retry(job.id, job.lease_token, compact_opts(command_opts))
  end

  @spec fail(Job.t(), term(), keyword()) :: {:ok, map()} | {:error, binary()}
  def fail(%Job{} = job, reason, opts) do
    command_opts =
      job
      |> Job.guard_opts(opts)
      |> maybe_put_value(:error, reason)

    job.workflow.__flow_store__().flow_fail(job.id, job.lease_token, command_opts)
  end

  @spec children(module(), Job.t() | map() | binary(), keyword()) ::
          {:ok, [map()]} | {:error, binary()}
  def children(workflow, parent, opts) do
    with {:ok, parent_id} <- parent_id(parent) do
      opts = maybe_parent_partition_opts(parent, opts)
      workflow.__flow_store__().flow_by_parent(parent_id, opts)
    end
  end

  @spec waiting_children(module(), Job.t() | map() | binary(), keyword()) ::
          {:ok, [map()]} | {:error, binary()}
  def waiting_children(workflow, parent, opts) do
    case children(workflow, parent, opts) do
      {:ok, records} ->
        {:ok,
         Enum.reject(records, &(Map.get(&1, :state) in ["completed", "failed", "cancelled"]))}

      other ->
        other
    end
  end

  @doc false
  def normalize_state(:any), do: :any
  def normalize_state(state) when is_atom(state), do: Atom.to_string(state)
  def normalize_state(state) when is_binary(state), do: state

  @doc false
  def normalize_action({:complete, opts}) when is_list(opts), do: {:complete, opts}
  def normalize_action(state), do: {:transition, normalize_state(state)}

  @doc false
  def normalize_error_action({:fail, opts}) when is_list(opts), do: {:fail, opts}
  def normalize_error_action(opts) when is_list(opts), do: normalize_error_keyword_action(opts)
  def normalize_error_action(state), do: {:transition, normalize_state(state)}

  @doc false
  def put_state_config(config, key, value), do: Map.put(config, key, value)

  @doc false
  def put_claim_payload_opts(config, opts) when is_list(opts) do
    config
    |> maybe_put_config(:payload_max_bytes, Keyword.get(opts, :max_bytes))
    |> maybe_put_config(:payload_max_bytes, Keyword.get(opts, :payload_max_bytes))
  end

  @doc false
  def put_state_opt(opts, :any), do: Keyword.put(opts, :state, :any)
  def put_state_opt(opts, nil), do: opts

  def put_state_opt(opts, states) when is_list(states),
    do: Keyword.put(opts, :state, Enum.map(states, &normalize_state/1))

  def put_state_opt(opts, state), do: Keyword.put(opts, :state, normalize_state(state))

  @doc false
  def policy_opts(states_by_name) do
    states =
      states_by_name
      |> Enum.reduce(%{}, fn {name, config}, acc ->
        retry = config[:retry]

        retry =
          case config[:on_error] do
            {:retry, extra} -> Keyword.merge(retry || [], extra)
            _ -> retry
          end

        if retry do
          Map.put(acc, name, retry: retry)
        else
          acc
        end
      end)

    if map_size(states) == 0, do: [], else: [states: states]
  end

  defp create_many_items(workflow, items, opts) do
    Enum.reduce_while(items, {:ok, []}, fn attrs, {:ok, acc} ->
      with true <- is_map(attrs) || {:error, "ERR flow item must be a map"},
           {:ok, id} <- required_attr(attrs, opts, :id),
           {:ok, partition_key} <- partition_key(workflow, attrs, opts) do
        item =
          attrs
          |> Map.take([
            :payload,
            :payload_ref,
            :run_at_ms,
            :priority,
            :parent_flow_id,
            :root_flow_id,
            :correlation_id,
            :idempotent,
            :retention_ttl_ms,
            :history_hot_max_events,
            :history_max_events
          ])
          |> Map.put(:id, id)
          |> Map.put(:partition_key, partition_key)

        {:cont, {:ok, [item | acc]}}
      else
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, mapped} -> {:ok, Enum.reverse(mapped)}
      {:error, _reason} = error -> error
    end
  end

  defp required_attr(attrs, opts, key) do
    value = Keyword.get(opts, key) || Map.get(attrs, key) || Map.get(attrs, Atom.to_string(key))

    if is_binary(value) and value != "" do
      {:ok, value}
    else
      {:error, "ERR flow #{key} must be a non-empty string"}
    end
  end

  defp partition_key(workflow, attrs, opts) do
    case Keyword.fetch(opts, :partition_key) do
      {:ok, value} ->
        {:ok, value}

      :error ->
        build_partition_key(workflow.__flow_partition_by__(), attrs)
    end
  end

  defp optional_child_partition_key(workflow, attrs, opts) do
    cond do
      Keyword.has_key?(opts, :partition_key) ->
        {:ok, Keyword.get(opts, :partition_key)}

      has_map_key?(attrs, :partition_key) ->
        {:ok, map_field(attrs, :partition_key)}

      workflow.__flow_partition_by__() == [] ->
        {:ok, nil}

      true ->
        build_partition_key(workflow.__flow_partition_by__(), attrs)
    end
  end

  defp parent_id(%Job{id: id}) when is_binary(id) and id != "", do: {:ok, id}
  defp parent_id(parent) when is_map(parent), do: parent_map_id(parent)
  defp parent_id(parent) when is_binary(parent) and parent != "", do: {:ok, parent}
  defp parent_id(_parent), do: {:error, "ERR flow parent id must be a non-empty string"}

  defp parent_map_id(parent) do
    case map_field(parent, :id) do
      id when is_binary(id) and id != "" -> {:ok, id}
      _ -> {:error, "ERR flow parent id must be a non-empty string"}
    end
  end

  defp spawn_children_opts(parent, opts) do
    wait = Keyword.get(opts, :wait, :all)

    spawn_opts =
      opts
      |> normalize_fanout_aliases()
      |> normalize_spawn_state_opts()
      |> Keyword.put_new(:group_id, "fanout")
      |> Keyword.put_new(:wait, wait)
      |> Keyword.put_new(:wait_state, default_wait_state(wait))
      |> Keyword.put_new(:on_child_failed, :fail_parent)
      |> Keyword.put_new(:on_parent_closed, :cancel_children)
      |> maybe_put_new(:partition_key, parent_partition_key(parent))
      |> maybe_put_new(:from_state, parent_state(parent))
      |> maybe_put_new(:lease_token, parent_lease_token(parent))
      |> maybe_put_new(:fencing_token, parent_fencing_token(parent))
      |> compact_opts()

    if Keyword.has_key?(spawn_opts, :fencing_token) do
      {:ok, spawn_opts}
    else
      {:error, "ERR flow fencing_token is required"}
    end
  end

  defp normalize_fanout_aliases(opts) do
    opts
    |> maybe_alias(:success, :on_success)
    |> maybe_alias(:failure, :on_failure)
    |> maybe_alias(:success, :on_all_ok)
    |> maybe_alias(:failure, :on_any_error)
    |> maybe_alias(:on_child_failed, :child_failure_policy)
  end

  defp normalize_spawn_state_opts(opts) do
    opts
    |> update_state_opt(:success)
    |> update_state_opt(:failure)
    |> update_state_opt(:wait_state)
    |> update_state_opt(:from_state)
  end

  defp update_state_opt(opts, key) do
    case Keyword.fetch(opts, key) do
      {:ok, nil} -> opts
      {:ok, value} -> Keyword.put(opts, key, normalize_state(value))
      :error -> opts
    end
  end

  defp maybe_alias(opts, target, source) do
    case Keyword.fetch(opts, source) do
      {:ok, value} -> Keyword.put_new(opts, target, value)
      :error -> opts
    end
  end

  defp default_wait_state(:none), do: nil
  defp default_wait_state("none"), do: nil
  defp default_wait_state(_wait), do: "waiting_children"

  defp maybe_parent_partition_opts(parent, opts) do
    maybe_put_new(opts, :partition_key, parent_partition_key(parent))
  end

  defp parent_partition_key(%Job{partition_key: partition_key}), do: partition_key
  defp parent_partition_key(parent) when is_map(parent), do: map_field(parent, :partition_key)
  defp parent_partition_key(_parent), do: nil

  defp parent_state(%Job{state: state}), do: state
  defp parent_state(parent) when is_map(parent), do: map_field(parent, :state)
  defp parent_state(_parent), do: nil

  defp parent_lease_token(%Job{lease_token: lease_token}), do: lease_token
  defp parent_lease_token(parent) when is_map(parent), do: map_field(parent, :lease_token)
  defp parent_lease_token(_parent), do: nil

  defp parent_fencing_token(%Job{fencing_token: fencing_token}), do: fencing_token
  defp parent_fencing_token(parent) when is_map(parent), do: map_field(parent, :fencing_token)
  defp parent_fencing_token(_parent), do: nil

  defp has_map_key?(map, key),
    do: Map.has_key?(map, key) or Map.has_key?(map, Atom.to_string(key))

  defp map_field(map, key) do
    case Map.fetch(map, key) do
      {:ok, value} -> value
      :error -> Map.get(map, Atom.to_string(key))
    end
  end

  defp build_partition_key([], _attrs), do: {:ok, nil}

  defp build_partition_key(fields, attrs) do
    parts =
      Enum.map(fields, fn field ->
        Map.get(attrs, field) || Map.get(attrs, Atom.to_string(field))
      end)

    if Enum.any?(parts, &is_nil/1) do
      {:error, "ERR flow partition key fields missing"}
    else
      {:ok, Enum.map_join(parts, ":", &to_string/1)}
    end
  end

  defp flow_opts_from_attrs(attrs) do
    attrs
    |> Map.take([
      :state,
      :payload,
      :payload_ref,
      :run_at_ms,
      :priority,
      :parent_flow_id,
      :root_flow_id,
      :correlation_id,
      :idempotent,
      :retention_ttl_ms,
      :history_hot_max_events,
      :history_max_events
    ])
    |> Enum.to_list()
  end

  defp apply_action(job, {:complete, action_opts}, value, opts),
    do: complete(job, value, Keyword.merge(action_opts, opts))

  defp apply_action(job, {:transition, to_state}, value, opts),
    do: transition(job, to_state, value, opts)

  defp apply_error_action(job, {:fail, action_opts}, reason, opts),
    do: fail(job, reason, Keyword.merge(action_opts, opts))

  defp apply_error_action(job, {:retry, action_opts}, reason, opts),
    do: retry(job, reason, Keyword.merge(action_opts, opts))

  defp apply_error_action(job, {:transition, to_state}, reason, opts),
    do: transition(job, to_state, reason, opts)

  defp normalize_error_keyword_action(opts) do
    cond do
      Keyword.has_key?(opts, :retry_or) ->
        {:retry, [exhausted_to: normalize_state(Keyword.fetch!(opts, :retry_or))]}

      Keyword.has_key?(opts, :fail) ->
        {:fail, []}

      true ->
        {:retry, opts}
    end
  end

  defp state_for_config([state | _]), do: state
  defp state_for_config(state), do: state

  defp normalize_type!(type) when is_binary(type) and type != "", do: type
  defp normalize_type!(type) when is_atom(type), do: Atom.to_string(type)

  defp maybe_put_new(opts, _key, nil), do: opts
  defp maybe_put_new(opts, key, value), do: Keyword.put_new(opts, key, value)

  defp maybe_put_value(opts, _key, nil), do: opts
  defp maybe_put_value(opts, key, value), do: Keyword.put(opts, key, value)

  defp maybe_put_config(config, _key, nil), do: config
  defp maybe_put_config(config, key, value), do: Map.put(config, key, value)

  defp maybe_put_map(map, _key, nil), do: map
  defp maybe_put_map(map, key, value), do: Map.put(map, key, value)

  defp required_handler(opts) do
    case Keyword.get(opts, :handler) do
      handler when is_function(handler, 1) -> {:ok, handler}
      _ -> {:error, "ERR flow handler must be a one-arity function"}
    end
  end

  defp compact_opts(opts), do: Enum.reject(opts, fn {_key, value} -> is_nil(value) end)
end
