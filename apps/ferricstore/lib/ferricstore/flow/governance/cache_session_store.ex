defmodule Ferricstore.Flow.Governance.CacheSessionStore do
  @moduledoc false

  alias Ferricstore.Flow.Governance.LimitStore
  alias Ferricstore.Flow.Governance.LimitRecord
  alias Ferricstore.Flow.Keys
  alias Ferricstore.Store.Router
  alias Ferricstore.TermCodec

  @head_tag :flow_governance_cache_session_head_v1
  @meta_tag :flow_governance_cache_session_meta_v1
  @page_tag :flow_governance_cache_session_page_v1
  @max_retries 16
  @max_page_size 256
  @max_prefetch_ids 1_000
  @floor_compaction_limit 256
  @max_exact_version 9_007_199_254_740_991
  @max_encoded_page_bytes 262_144
  @max_identity_bytes 65_535
  @set_nx %{expire_at_ms: 0, nx: true, xx: false, get: false, keepttl: false}

  @type session :: %{
          node_id: binary(),
          instance_name: binary(),
          session_id: binary(),
          generation: pos_integer(),
          previous_session_id: binary() | nil
        }

  @type page :: %{
          node_id: binary(),
          instance_name: binary(),
          session_id: binary(),
          generation: pos_integer(),
          sequence: pos_integer(),
          scope: binary(),
          shard_id: non_neg_integer(),
          expires_at_ms: non_neg_integer(),
          config_version: non_neg_integer(),
          effective_limit: non_neg_integer() | nil,
          reservation_ids: [binary()],
          state: :unused | :uncertain | :retained | :released
        }

  @spec open(FerricStore.Instance.t(), keyword()) :: {:ok, session()} | {:error, term()}
  def open(ctx, opts) when is_list(opts) do
    if Keyword.keyword?(opts) do
      before_head_replace_fun =
        Keyword.get(opts, :before_head_replace_fun, fn _session -> :ok end)

      with {:ok, node_id} <- required_identity(opts, :node_id),
           {:ok, instance_name} <- required_identity(opts, :instance_name),
           true <- is_function(before_head_replace_fun, 1) do
        do_open(ctx, node_id, instance_name, before_head_replace_fun, @max_retries)
      else
        false -> {:error, "ERR invalid flow governance cache session options"}
        {:error, _reason} = error -> error
      end
    else
      {:error, "ERR invalid flow governance cache session options"}
    end
  end

  def open(_ctx, _opts), do: {:error, "ERR invalid flow governance cache session options"}

  @spec persist_prefetch(
          FerricStore.Instance.t(),
          session(),
          binary(),
          non_neg_integer(),
          [binary()],
          keyword()
        ) :: {:ok, [page()]} | {:error, term()}
  def persist_prefetch(ctx, session, scope, shard_id, reservation_ids, opts)
      when is_map(session) and is_binary(scope) and scope != "" and is_integer(shard_id) and
             shard_id >= 0 and is_list(reservation_ids) and reservation_ids != [] and
             is_list(opts) do
    if Keyword.keyword?(opts) do
      page_size = Keyword.get(opts, :page_size, @max_page_size)
      expires_at_ms = Keyword.get(opts, :expires_at_ms, 0)
      config_version = Keyword.get(opts, :config_version, 0)
      effective_limit = Keyword.get(opts, :effective_limit)
      after_page_persist_fun = Keyword.get(opts, :after_page_persist_fun, fn _page -> :ok end)

      with :ok <- validate_session(session),
           :ok <- validate_page_size(page_size),
           :ok <- validate_expires_at(expires_at_ms),
           :ok <- validate_cache_configuration(config_version, effective_limit),
           true <- is_function(after_page_persist_fun, 1),
           :ok <- validate_prefetch_reservation_ids(reservation_ids),
           :ok <- assert_current(ctx, session),
           chunks = Enum.chunk_every(reservation_ids, page_size),
           {:ok, first_sequence} <- reserve_sequences(ctx, session, length(chunks)),
           :ok <- assert_current(ctx, session),
           pages =
             build_pages(
               session,
               first_sequence,
               scope,
               shard_id,
               expires_at_ms,
               config_version,
               effective_limit,
               chunks
             ) do
        persist_reserved_pages(ctx, session, pages, after_page_persist_fun)
      else
        false -> {:error, "ERR invalid flow governance cache prefetch manifest"}
        {:error, _reason} = error -> error
      end
    else
      {:error, "ERR invalid flow governance cache prefetch manifest"}
    end
  end

  def persist_prefetch(_ctx, _session, _scope, _shard_id, _reservation_ids, _opts),
    do: {:error, "ERR invalid flow governance cache prefetch manifest"}

  @spec activate_page(FerricStore.Instance.t(), session(), page()) ::
          {:ok, page()} | {:error, term()}
  def activate_page(ctx, session, page) when is_map(session) and is_map(page) do
    with :ok <- validate_session(session),
         :ok <- same_session(session, page),
         :ok <- assert_current(ctx, session),
         {:ok, activated} <- do_activate_page(ctx, page),
         :ok <- assert_current(ctx, session) do
      {:ok, activated}
    end
  end

  def activate_page(_ctx, _session, _page),
    do: {:error, "ERR invalid flow governance cache session page"}

  @spec recover(FerricStore.Instance.t(), session(), keyword()) :: {:ok, map()} | {:error, term()}
  def recover(ctx, session, opts) when is_map(session) and is_list(opts) do
    if Keyword.keyword?(opts) do
      cursor = Keyword.get(opts, :cursor)
      limit = Keyword.get(opts, :limit, @max_page_size)
      now_ms = Keyword.get(opts, :now_ms, Ferricstore.CommandTime.now_ms())
      release_fun = Keyword.get(opts, :release_fun, &LimitStore.release/3)
      page_delete_fun = Keyword.get(opts, :page_delete_fun, &Router.delete/2)
      page_read_fun = Keyword.get(opts, :page_read_fun, &read_durable_key/2)

      after_cleanup_mark_fun =
        Keyword.get(opts, :after_cleanup_mark_fun, fn _session_id -> :ok end)

      with :ok <- validate_session(session),
           :ok <-
             validate_recovery_opts(
               cursor,
               limit,
               now_ms,
               release_fun,
               page_delete_fun,
               page_read_fun,
               after_cleanup_mark_fun
             ),
           :ok <- assert_current(ctx, session),
           {:ok, start_cursor} <- recovery_start(ctx, session, cursor) do
        recover_pages(
          ctx,
          session,
          start_cursor,
          limit,
          %{
            now_ms: now_ms,
            release_fun: release_fun,
            page_delete_fun: page_delete_fun,
            page_read_fun: page_read_fun,
            after_cleanup_mark_fun: after_cleanup_mark_fun
          },
          %{released: 0, retained: 0, errors: 0, processed: 0}
        )
      end
    else
      {:error, "ERR invalid flow governance cache session recovery options"}
    end
  end

  def recover(_ctx, _session, _opts),
    do: {:error, "ERR invalid flow governance cache session recovery options"}

  @spec update_pages(FerricStore.Instance.t(), session(), [page()], keyword()) ::
          {:ok, [page()]} | {:error, term()}
  def update_pages(ctx, session, pages, opts)
      when is_map(session) and is_list(pages) and is_list(opts) do
    if Keyword.keyword?(opts) do
      with :ok <- validate_session(session),
           {:ok, expires_at_ms} <- required_update_option(opts, :expires_at_ms),
           {:ok, config_version} <- required_update_option(opts, :config_version),
           effective_limit = Keyword.get(opts, :effective_limit),
           :ok <- validate_expires_at(expires_at_ms),
           :ok <- validate_cache_configuration(config_version, effective_limit),
           :ok <- assert_current(ctx, session),
           :ok <- validate_session_pages(session, pages) do
        Enum.reduce_while(pages, {:ok, []}, fn page, {:ok, updated} ->
          case update_page_metadata(
                 ctx,
                 page,
                 expires_at_ms,
                 config_version,
                 effective_limit
               ) do
            {:ok, page} -> {:cont, {:ok, [page | updated]}}
            {:error, _reason} = error -> {:halt, error}
          end
        end)
        |> case do
          {:ok, updated} -> {:ok, Enum.reverse(updated)}
          {:error, _reason} = error -> error
        end
      end
    else
      {:error, "ERR invalid flow governance cache update options"}
    end
  end

  def update_pages(_ctx, _session, _pages, _opts),
    do: {:error, "ERR invalid flow governance cache update options"}

  @spec discard_pages(FerricStore.Instance.t(), session(), [page()]) :: :ok | {:error, term()}
  def discard_pages(ctx, session, pages), do: discard_pages(ctx, session, pages, [])

  @doc false
  @spec discard_pages(FerricStore.Instance.t(), session(), [page()], keyword()) ::
          :ok | {:error, term()}
  def discard_pages(ctx, session, pages, opts)
      when is_map(session) and is_list(pages) and is_list(opts) do
    if Keyword.keyword?(opts) do
      after_page_delete_fun = Keyword.get(opts, :after_page_delete_fun, fn -> :ok end)
      allowed_states = Keyword.get(opts, :allowed_states, [:unused])
      floor_read_fun = Keyword.get(opts, :floor_read_fun, &read_durable_keys/2)

      with :ok <- validate_session(session),
           true <- is_function(after_page_delete_fun, 0),
           true <- is_function(floor_read_fun, 2),
           :ok <- validate_discard_states(allowed_states),
           :ok <- assert_current(ctx, session),
           :ok <- validate_session_pages(session, pages),
           sequences = pages |> Enum.map(& &1.sequence) |> Enum.sort(),
           :ok <- claim_pages_for_discard(ctx, pages, allowed_states),
           :ok <- delete_pages(ctx, pages),
           :ok <- run_page_delete_checkpoint(after_page_delete_fun),
           {:ok, :ok} <-
             update_meta(ctx, session, fn meta ->
               recovery_floor = advance_discarded_floor(meta.recovery_floor, sequences)
               {:ok, %{meta | recovery_floor: recovery_floor}, :ok}
             end),
           :ok <- compact_recovery_floor(ctx, session, floor_read_fun) do
        :ok
      else
        false -> {:error, "ERR invalid flow governance cache discard options"}
        {:error, _reason} = error -> error
      end
    else
      {:error, "ERR invalid flow governance cache discard options"}
    end
  end

  def discard_pages(_ctx, _session, _pages, _opts),
    do: {:error, "ERR invalid flow governance cache discard options"}

  @spec acknowledge_page(FerricStore.Instance.t(), session(), page()) :: :ok | {:error, term()}
  def acknowledge_page(ctx, session, page), do: acknowledge_page(ctx, session, page, [])

  @doc false
  @spec acknowledge_page(FerricStore.Instance.t(), session(), page(), keyword()) ::
          :ok | {:error, term()}
  def acknowledge_page(ctx, session, %{state: :uncertain} = page, opts)
      when is_map(session) and is_list(opts) do
    if Keyword.keyword?(opts) do
      after_page_delete_fun = Keyword.get(opts, :after_page_delete_fun, fn -> :ok end)
      floor_read_fun = Keyword.get(opts, :floor_read_fun, &read_durable_keys/2)

      with :ok <- validate_session(session),
           true <- is_function(after_page_delete_fun, 0),
           true <- is_function(floor_read_fun, 2),
           :ok <- same_session(session, page),
           :ok <- assert_current(ctx, session),
           :ok <- Router.delete(ctx, page_key(page)),
           :ok <- run_page_delete_checkpoint(after_page_delete_fun),
           {:ok, :ok} <-
             update_meta(ctx, session, fn meta ->
               recovery_floor =
                 if page.sequence == meta.recovery_floor,
                   do: page.sequence + 1,
                   else: meta.recovery_floor

               {:ok, %{meta | recovery_floor: recovery_floor}, :ok}
             end),
           :ok <- compact_recovery_floor(ctx, session, floor_read_fun) do
        :ok
      else
        false -> {:error, "ERR invalid flow governance cache acknowledge options"}
        {:error, _reason} = error -> error
      end
    else
      {:error, "ERR invalid flow governance cache acknowledge options"}
    end
  end

  def acknowledge_page(_ctx, _session, _page, _opts),
    do: {:error, :cache_session_page_not_acknowledgeable}

  @spec page_present?(FerricStore.Instance.t(), page()) :: boolean()
  def page_present?(ctx, page) when is_map(page) do
    valid_page_identity?(page) and is_binary(Router.get(ctx, page_key(page)))
  end

  def page_present?(_ctx, _page), do: false

  @spec manifest_bounds(FerricStore.Instance.t(), session()) :: {:ok, map()} | {:error, term()}
  def manifest_bounds(ctx, session) when is_map(session) do
    with :ok <- validate_session(session),
         :ok <- assert_current(ctx, session),
         {:ok, meta} <- read_meta(ctx, session, session.session_id) do
      {:ok, %{page_count: meta.page_count, recovery_floor: meta.recovery_floor}}
    end
  end

  def manifest_bounds(_ctx, _session), do: {:error, "ERR invalid flow governance cache session"}

  @spec manifest_previous(FerricStore.Instance.t(), session()) ::
          {:ok, binary() | nil} | {:error, term()}
  def manifest_previous(ctx, session) when is_map(session) do
    with :ok <- validate_session(session),
         {:ok, meta} <- read_meta(ctx, session, session.session_id) do
      {:ok, meta.previous_session_id}
    end
  end

  def manifest_previous(_ctx, _session), do: {:error, "ERR invalid flow governance cache session"}

  @spec current?(FerricStore.Instance.t(), session()) :: boolean()
  def current?(ctx, session) when is_map(session),
    do: validate_session(session) == :ok and assert_current(ctx, session) == :ok

  def current?(_ctx, _session), do: false

  @spec head_present?(FerricStore.Instance.t(), keyword()) :: boolean()
  def head_present?(ctx, opts) when is_list(opts) do
    if Keyword.keyword?(opts) do
      with {:ok, node_id} <- required_identity(opts, :node_id),
           {:ok, instance_name} <- required_identity(opts, :instance_name) do
        key = Keys.governance_limit_cache_session_head_key(node_id, instance_name)
        is_binary(Router.get(ctx, key))
      else
        _invalid -> false
      end
    else
      false
    end
  end

  def head_present?(_ctx, _opts), do: false

  @spec node_id() :: binary()
  def node_id do
    :ferricstore
    |> Application.get_env(:node_name, node())
    |> to_string()
  end

  @spec instance_name(FerricStore.Instance.t()) :: binary()
  def instance_name(%{name: name}), do: to_string(name)

  defp do_open(_ctx, _node_id, _instance_name, _before_head_replace_fun, retries_left)
       when retries_left <= 0,
       do: {:error, :cache_session_conflict}

  defp do_open(ctx, node_id, instance_name, before_head_replace_fun, retries_left) do
    head_key = Keys.governance_limit_cache_session_head_key(node_id, instance_name)

    with {:ok, expected} <- safe_durable_read(&read_durable_key/2, ctx, head_key),
         {:ok, previous_head} <- decode_head(expected),
         {:ok, generation} <- next_generation(previous_head.generation),
         session = %{
           node_id: node_id,
           instance_name: instance_name,
           session_id:
             deterministic_session_id(
               node_id,
               instance_name,
               generation,
               previous_head.session_id
             ),
           generation: generation,
           previous_session_id: previous_head.session_id
         },
         {:ok, preparation} <- write_initial_meta(ctx, session) do
      case preparation do
        :linked ->
          {:ok, session}

        :prepared ->
          publish_prepared_session(
            ctx,
            head_key,
            expected,
            session,
            before_head_replace_fun,
            retries_left
          )
      end
    end
  end

  defp publish_prepared_session(
         ctx,
         head_key,
         expected,
         session,
         before_head_replace_fun,
         retries_left
       ) do
    with :ok <- run_before_head_replace(before_head_replace_fun, session) do
      case replace_head(ctx, head_key, expected, encode_head(session)) do
        :ok ->
          {:ok, session}

        :retry ->
          case head_session_status(ctx, head_key, session) do
            :current ->
              {:ok, session}

            :different ->
              do_open(
                ctx,
                session.node_id,
                session.instance_name,
                before_head_replace_fun,
                retries_left - 1
              )

            {:error, _reason} = error ->
              error
          end

        {:error, _reason} = error ->
          case head_session_status(ctx, head_key, session) do
            :current -> {:ok, session}
            :different -> error
            {:error, _reason} = read_error -> read_error
          end
      end
    end
  end

  defp head_session_status(ctx, head_key, session) do
    with {:ok, encoded} <- safe_durable_read(&read_durable_key/2, ctx, head_key),
         {:ok, head} <- decode_head(encoded) do
      if head.session_id == session.session_id and head.generation == session.generation,
        do: :current,
        else: :different
    end
  end

  defp run_before_head_replace(before_head_replace_fun, session) do
    case before_head_replace_fun.(session) do
      :ok -> :ok
      {:error, _reason} = error -> error
      other -> {:error, {:invalid_head_replace_checkpoint, other}}
    end
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  defp replace_head(ctx, key, nil, new_head) do
    case Router.set(ctx, key, new_head, @set_nx) do
      :ok -> :ok
      nil -> :retry
      {:error, _reason} = error -> error
      _other -> {:error, :cache_session_head_write_failed}
    end
  end

  defp replace_head(ctx, key, expected, new_head) when is_binary(expected) do
    case Router.cas(ctx, key, expected, new_head, nil) do
      1 -> :ok
      result when result in [0, nil] -> :retry
      {:error, _reason} = error -> error
      _other -> {:error, :cache_session_head_write_failed}
    end
  end

  defp next_generation(generation)
       when is_integer(generation) and generation >= 0 and generation < @max_exact_version,
       do: {:ok, generation + 1}

  defp next_generation(@max_exact_version), do: {:error, :cache_session_generation_exhausted}

  defp write_initial_meta(ctx, session) do
    meta = %{
      session_id: session.session_id,
      generation: session.generation,
      previous_session_id: session.previous_session_id,
      cleanup_session_id: nil,
      page_count: 0,
      recovery_floor: 1,
      state: :active
    }

    key = meta_key(session)
    encoded = encode_meta(meta)

    case Router.set(ctx, key, encoded, @set_nx) do
      :ok ->
        {:ok, :prepared}

      nil ->
        classify_existing_initial_meta(ctx, session, encoded)

      {:error, _reason} = error ->
        case classify_existing_initial_meta(ctx, session, encoded) do
          {:ok, _status} = ok -> ok
          {:error, _reason} -> error
        end

      _other ->
        {:error, :cache_session_meta_write_failed}
    end
  end

  defp classify_existing_initial_meta(ctx, session, expected) do
    case safe_durable_read(&read_durable_key/2, ctx, meta_key(session)) do
      {:ok, ^expected} ->
        {:ok, :prepared}

      {:ok, existing} when is_binary(existing) ->
        with {:ok, meta} <- decode_meta(existing),
             true <- meta.session_id == session.session_id,
             true <- meta.generation == session.generation,
             :current <-
               head_session_status(
                 ctx,
                 Keys.governance_limit_cache_session_head_key(
                   session.node_id,
                   session.instance_name
                 ),
                 session
               ) do
          {:ok, :linked}
        else
          false -> {:error, :cache_session_meta_conflict}
          :different -> {:error, :cache_session_meta_conflict}
          {:error, _reason} = error -> error
        end

      {:ok, nil} ->
        {:error, :cache_session_manifest_missing}

      {:error, _reason} = error ->
        error
    end
  end

  defp reserve_sequences(ctx, session, count) when count > 0 do
    update_meta(ctx, session, fn meta ->
      if meta.page_count <= @max_exact_version - count do
        first_sequence = meta.page_count + 1
        updated = %{meta | page_count: meta.page_count + count}
        {:ok, updated, first_sequence}
      else
        {:error, :cache_session_manifest_full}
      end
    end)
  end

  defp update_meta(ctx, session, update_fun, retries_left \\ @max_retries)

  defp update_meta(_ctx, _session, _update_fun, retries_left) when retries_left <= 0,
    do: {:error, :cache_session_conflict}

  defp update_meta(ctx, session, update_fun, retries_left) do
    key = meta_key(session)

    case safe_durable_read(&read_durable_key/2, ctx, key) do
      {:ok, expected} when is_binary(expected) ->
        with {:ok, meta} <- decode_meta(expected),
             :ok <- validate_meta_session(meta, session),
             {:ok, updated, reply} <- update_fun.(meta) do
          case Router.cas(ctx, key, expected, encode_meta(updated), nil) do
            1 ->
              {:ok, reply}

            result when result in [0, nil] ->
              update_meta(ctx, session, update_fun, retries_left - 1)

            {:error, _reason} = error ->
              error

            _other ->
              {:error, :cache_session_meta_write_failed}
          end
        end

      {:ok, nil} ->
        {:error, :cache_session_manifest_missing}

      {:error, _reason} = error ->
        error
    end
  end

  defp build_pages(
         session,
         first_sequence,
         scope,
         shard_id,
         expires_at_ms,
         config_version,
         effective_limit,
         chunks
       ) do
    chunks
    |> Enum.with_index(first_sequence)
    |> Enum.map(fn {reservation_ids, sequence} ->
      %{
        node_id: session.node_id,
        instance_name: session.instance_name,
        session_id: session.session_id,
        generation: session.generation,
        sequence: sequence,
        scope: scope,
        shard_id: shard_id,
        expires_at_ms: expires_at_ms,
        config_version: config_version,
        effective_limit: effective_limit,
        reservation_ids: reservation_ids,
        state: :unused
      }
    end)
  end

  defp persist_reserved_pages(ctx, session, pages, after_page_persist_fun) do
    case persist_pages(ctx, pages, after_page_persist_fun) do
      :ok ->
        case assert_current(ctx, session) do
          :ok ->
            {:ok, pages}

          {:error, _reason} = error ->
            _ = cleanup_unspent_pages(ctx, session, pages)
            error
        end

      {:error, _reason} = error ->
        _ = cleanup_unspent_pages(ctx, session, pages)
        error
    end
  end

  defp persist_pages(ctx, pages, after_page_persist_fun) do
    Enum.reduce_while(pages, :ok, fn page, :ok ->
      case persist_page(ctx, page) do
        :ok ->
          case run_page_persist_checkpoint(after_page_persist_fun, page) do
            :ok -> {:cont, :ok}
            {:error, _reason} = error -> {:halt, error}
          end

        {:error, _reason} = error ->
          {:halt, error}
      end
    end)
  end

  defp cleanup_unspent_pages(ctx, session, pages) do
    sequences = pages |> Enum.map(& &1.sequence) |> Enum.sort()

    with :ok <- delete_pages(ctx, pages),
         {:ok, :ok} <-
           update_meta(ctx, session, fn meta ->
             recovery_floor = advance_discarded_floor(meta.recovery_floor, sequences)
             {:ok, %{meta | recovery_floor: recovery_floor}, :ok}
           end),
         :ok <- compact_recovery_floor(ctx, session, &read_durable_keys/2) do
      :ok
    end
  end

  defp run_page_persist_checkpoint(checkpoint_fun, page) do
    case checkpoint_fun.(page) do
      :ok -> :ok
      {:error, _reason} = error -> error
      other -> {:error, {:invalid_page_persist_checkpoint, other}}
    end
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  defp persist_page(ctx, page) do
    key = page_key(page)
    encoded = encode_page(page)

    if byte_size(encoded) > @max_encoded_page_bytes do
      {:error, :cache_session_page_too_large}
    else
      persist_encoded_page(ctx, key, encoded)
    end
  end

  defp persist_encoded_page(ctx, key, encoded) do
    case Router.set(ctx, key, encoded, @set_nx) do
      :ok ->
        :ok

      nil ->
        verify_persisted_page(ctx, key, encoded)

      {:error, _reason} = error ->
        case verify_persisted_page(ctx, key, encoded) do
          :ok -> :ok
          {:error, _reason} -> error
        end

      _other ->
        {:error, :cache_session_page_write_failed}
    end
  end

  defp verify_persisted_page(ctx, key, encoded) do
    if Router.get(ctx, key) == encoded,
      do: :ok,
      else: {:error, :cache_session_page_conflict}
  end

  defp do_activate_page(ctx, page), do: do_activate_page(ctx, page, @max_retries)

  defp do_activate_page(_ctx, _page, retries_left) when retries_left <= 0,
    do: {:error, :cache_session_conflict}

  defp do_activate_page(ctx, page, retries_left) do
    key = page_key(page)

    case Router.get(ctx, key) do
      expected when is_binary(expected) ->
        with {:ok, stored} <- decode_page(expected),
             :ok <- same_page(page, stored) do
          case stored.state do
            :unused ->
              activated = %{stored | state: :uncertain}

              case Router.cas(ctx, key, expected, encode_page(activated), nil) do
                1 -> {:ok, activated}
                result when result in [0, nil] -> do_activate_page(ctx, page, retries_left - 1)
                {:error, _reason} = error -> error
                _other -> {:error, :cache_session_page_write_failed}
              end

            :uncertain ->
              {:ok, stored}

            _retained_or_released ->
              {:error, :cache_session_page_not_activatable}
          end
        end

      nil ->
        {:error, :cache_session_page_missing}

      {:error, _reason} = error ->
        error

      _other ->
        {:error, :cache_session_page_corrupt}
    end
  end

  defp update_page_metadata(
         ctx,
         page,
         expires_at_ms,
         config_version,
         effective_limit
       ),
       do:
         update_page_metadata(
           ctx,
           page,
           expires_at_ms,
           config_version,
           effective_limit,
           @max_retries
         )

  defp update_page_metadata(
         _ctx,
         _page,
         _expires_at_ms,
         _config_version,
         _effective_limit,
         retries_left
       )
       when retries_left <= 0,
       do: {:error, :cache_session_conflict}

  defp update_page_metadata(
         ctx,
         page,
         expires_at_ms,
         config_version,
         effective_limit,
         retries_left
       ) do
    key = page_key(page)

    case Router.get(ctx, key) do
      expected when is_binary(expected) ->
        with {:ok, stored} <- decode_page(expected),
             :ok <- same_page(page, stored),
             true <- stored.state == :unused do
          updated = %{
            stored
            | expires_at_ms: expires_at_ms,
              config_version: config_version,
              effective_limit: effective_limit
          }

          case Router.cas(ctx, key, expected, encode_page(updated), nil) do
            1 ->
              {:ok, updated}

            result when result in [0, nil] ->
              update_page_metadata(
                ctx,
                page,
                expires_at_ms,
                config_version,
                effective_limit,
                retries_left - 1
              )

            {:error, _reason} = error ->
              error

            _other ->
              {:error, :cache_session_page_write_failed}
          end
        else
          false -> {:error, :cache_session_page_not_updatable}
          {:error, _reason} = error -> error
        end

      nil ->
        {:error, :cache_session_page_missing}

      {:error, _reason} = error ->
        error

      _other ->
        {:error, :cache_session_page_corrupt}
    end
  end

  defp recover_pages(ctx, session, nil, _remaining, _recovery, counts) do
    with :ok <- assert_current(ctx, session) do
      {:ok, Map.put(counts, :next_cursor, nil)}
    end
  end

  defp recover_pages(ctx, session, cursor, 0, _recovery, counts) do
    with :ok <- assert_current(ctx, session) do
      {:ok, Map.put(counts, :next_cursor, cursor)}
    end
  end

  defp recover_pages(
         ctx,
         session,
         %{session_id: target_session_id, sequence: sequence},
         remaining,
         recovery,
         counts
       ) do
    with :ok <- assert_current(ctx, session),
         {:ok, target_meta} <- read_recovery_meta(ctx, session, target_session_id) do
      cond do
        sequence != target_meta.recovery_floor ->
          next_cursor = %{session_id: target_session_id, sequence: target_meta.recovery_floor}
          recover_pages(ctx, session, next_cursor, remaining, recovery, counts)

        sequence > target_meta.page_count ->
          case finalize_recovered_session(
                 ctx,
                 session,
                 target_meta,
                 recovery.after_cleanup_mark_fun
               ) do
            :ok ->
              next_cursor = previous_cursor(target_meta.previous_session_id)
              recover_pages(ctx, session, next_cursor, remaining, recovery, counts)

            {:error, _reason} ->
              {:ok,
               counts
               |> Map.update!(:errors, &(&1 + 1))
               |> Map.put(:next_cursor, %{
                 session_id: target_session_id,
                 sequence: sequence
               })}
          end

        true ->
          {page_counts, next_sequence, advance?} =
            recover_one_page(
              ctx,
              session,
              target_session_id,
              target_meta.generation,
              sequence,
              recovery
            )

          counts = merge_counts(counts, page_counts)

          if advance? do
            case advance_recovery_floor(ctx, session, target_meta, sequence) do
              :ok ->
                next_cursor = %{session_id: target_session_id, sequence: next_sequence}

                recover_pages(ctx, session, next_cursor, remaining - 1, recovery, counts)

              {:error, _reason} ->
                {:ok,
                 counts
                 |> Map.update!(:errors, &(&1 + 1))
                 |> Map.put(:next_cursor, %{
                   session_id: target_session_id,
                   sequence: sequence
                 })}
            end
          else
            {:ok,
             Map.put(counts, :next_cursor, %{
               session_id: target_session_id,
               sequence: sequence
             })}
          end
      end
    end
  end

  defp recover_one_page(
         ctx,
         current_session,
         target_session_id,
         target_generation,
         sequence,
         recovery
       ) do
    target = %{
      node_id: current_session.node_id,
      instance_name: current_session.instance_name,
      session_id: target_session_id,
      generation: target_generation,
      sequence: sequence
    }

    {counts, advance?} =
      case safe_durable_read(recovery.page_read_fun, ctx, page_key(target)) do
        {:ok, value} when is_binary(value) ->
          recover_stored_page(ctx, value, recovery)

        {:ok, nil} ->
          {%{released: 0, retained: 0, errors: 0, processed: 1}, true}

        {:error, _reason} ->
          {%{released: 0, retained: 0, errors: 1, processed: 1}, false}
      end

    {counts, sequence + 1, advance?}
  end

  defp advance_recovery_floor(ctx, current_session, target_meta, sequence) do
    target_session = session_for_meta(current_session, target_meta)

    case update_meta(ctx, target_session, fn latest ->
           cond do
             latest.recovery_floor == sequence ->
               {:ok, %{latest | recovery_floor: sequence + 1}, :ok}

             latest.recovery_floor > sequence ->
               {:ok, latest, :ok}

             true ->
               {:error, :cache_session_recovery_order_conflict}
           end
         end) do
      {:ok, :ok} -> :ok
      {:error, _reason} = error -> error
    end
  end

  defp recover_stored_page(ctx, encoded, recovery) do
    case decode_page(encoded) do
      {:ok, %{state: :unused} = page} ->
        opts = [
          shard_id: page.shard_id,
          amount: length(page.reservation_ids),
          reservation_ids: page.reservation_ids,
          now_ms: recovery.now_ms
        ]

        case safe_release(recovery.release_fun, ctx, page.scope, opts) do
          {:ok, _owner} ->
            case safe_page_delete(recovery.page_delete_fun, ctx, page) do
              :ok ->
                {%{
                   released: length(page.reservation_ids),
                   retained: 0,
                   errors: 0,
                   processed: 1
                 }, true}

              {:error, _reason} ->
                {%{released: 0, retained: 0, errors: 1, processed: 1}, false}
            end

          {:error, _reason} ->
            {%{released: 0, retained: 0, errors: 1, processed: 1}, false}
        end

      {:ok, %{state: :uncertain} = page} ->
        case safe_page_delete(recovery.page_delete_fun, ctx, page) do
          :ok ->
            {%{
               released: 0,
               retained: length(page.reservation_ids),
               errors: 0,
               processed: 1
             }, true}

          {:error, _reason} ->
            {%{released: 0, retained: 0, errors: 1, processed: 1}, false}
        end

      {:ok, %{state: state} = page} when state in [:retained, :released] ->
        case safe_page_delete(recovery.page_delete_fun, ctx, page) do
          :ok -> {%{released: 0, retained: 0, errors: 0, processed: 1}, true}
          {:error, _reason} -> {%{released: 0, retained: 0, errors: 1, processed: 1}, false}
        end

      {:error, _reason} ->
        {%{released: 0, retained: 0, errors: 1, processed: 1}, false}
    end
  end

  defp finalize_recovered_session(ctx, current_session, recovered_meta, checkpoint_fun) do
    recovered_session_id = recovered_meta.session_id

    with {:ok, checkpoint} <-
           update_meta(ctx, current_session, fn meta ->
             cond do
               meta.cleanup_session_id == recovered_session_id ->
                 {:ok, meta, :pending}

               is_nil(meta.cleanup_session_id) and
                   meta.previous_session_id == recovered_session_id ->
                 updated = %{
                   meta
                   | previous_session_id: recovered_meta.previous_session_id,
                     cleanup_session_id: recovered_session_id
                 }

                 {:ok, updated, :marked}

               true ->
                 {:error, :cache_session_cleanup_conflict}
             end
           end),
         :ok <- run_cleanup_checkpoint(checkpoint, checkpoint_fun, recovered_session_id),
         {:ok, current_meta} <- read_meta(ctx, current_session, current_session.session_id),
         {:ok, _cleaned_meta} <- finish_pending_cleanup(ctx, current_session, current_meta) do
      :ok
    end
  end

  defp read_meta(ctx, current_session, target_session_id) do
    target = %{current_session | session_id: target_session_id}

    case safe_durable_read(&read_durable_key/2, ctx, meta_key(target)) do
      {:ok, value} when is_binary(value) -> decode_meta(value)
      {:ok, nil} -> {:error, :cache_session_manifest_missing}
      {:error, _reason} = error -> error
    end
  end

  defp read_recovery_meta(ctx, current_session, target_session_id) do
    with {:ok, meta} <- read_meta(ctx, current_session, target_session_id),
         {:ok, meta} <- finish_pending_cleanup(ctx, current_session, meta) do
      {:ok, meta}
    end
  end

  defp finish_pending_cleanup(_ctx, _current_session, %{cleanup_session_id: nil} = meta),
    do: {:ok, meta}

  defp finish_pending_cleanup(ctx, current_session, meta) do
    cleanup_session_id = meta.cleanup_session_id
    holder = session_for_meta(current_session, meta)
    cleanup = %{holder | session_id: cleanup_session_id}

    with :ok <- validate_cleanup_target(ctx, cleanup, meta),
         :ok <- Router.delete(ctx, meta_key(cleanup)),
         {:ok, :ok} <-
           update_meta(ctx, holder, fn latest ->
             case latest.cleanup_session_id do
               ^cleanup_session_id ->
                 {:ok, %{latest | cleanup_session_id: nil}, :ok}

               nil ->
                 {:ok, latest, :ok}

               _other ->
                 {:error, :cache_session_cleanup_conflict}
             end
           end),
         {:ok, cleaned} <- read_meta(ctx, current_session, meta.session_id) do
      {:ok, cleaned}
    end
  end

  defp validate_cleanup_target(ctx, cleanup, holder_meta) do
    case safe_durable_read(&read_durable_key/2, ctx, meta_key(cleanup)) do
      {:ok, nil} ->
        :ok

      {:ok, value} when is_binary(value) ->
        with {:ok, cleanup_meta} <- decode_meta(value),
             true <- cleanup_meta.session_id == cleanup.session_id,
             true <- cleanup_meta.generation < holder_meta.generation do
          :ok
        else
          false -> {:error, :cache_session_cleanup_conflict}
          {:error, _reason} = error -> error
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp session_for_meta(current_session, meta) do
    %{current_session | session_id: meta.session_id, generation: meta.generation}
  end

  defp run_cleanup_checkpoint(:pending, _checkpoint_fun, _session_id), do: :ok

  defp run_cleanup_checkpoint(:marked, checkpoint_fun, session_id) do
    _ = checkpoint_fun.(session_id)
    :ok
  end

  defp recovery_start(ctx, session, cursor) when is_nil(cursor) or is_map(cursor) do
    _ = cursor

    with {:ok, current_meta} <- read_recovery_meta(ctx, session, session.session_id) do
      expected_session_id = current_meta.previous_session_id

      case expected_session_id do
        nil ->
          {:ok, nil}

        target_session_id ->
          with {:ok, target_meta} <- read_recovery_meta(ctx, session, target_session_id) do
            {:ok,
             %{
               session_id: target_session_id,
               sequence: target_meta.recovery_floor
             }}
          end
      end
    end
  end

  defp recovery_start(_ctx, _session, _cursor),
    do: {:error, "ERR invalid flow governance cache recovery cursor"}

  defp previous_cursor(nil), do: nil
  defp previous_cursor(session_id), do: %{session_id: session_id, sequence: 1}

  defp merge_counts(left, right) do
    Map.new([:released, :retained, :errors, :processed], fn key ->
      {key, Map.fetch!(left, key) + Map.fetch!(right, key)}
    end)
  end

  defp claim_pages_for_discard(ctx, pages, allowed_states) do
    Enum.reduce_while(pages, :ok, fn page, :ok ->
      case claim_page_for_discard(ctx, page, allowed_states) do
        :ok -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp claim_page_for_discard(ctx, page, allowed_states),
    do: claim_page_for_discard(ctx, page, allowed_states, @max_retries)

  defp claim_page_for_discard(_ctx, _page, _allowed_states, retries_left)
       when retries_left <= 0,
       do: {:error, :cache_session_conflict}

  defp claim_page_for_discard(ctx, page, allowed_states, retries_left) do
    key = page_key(page)

    case safe_durable_read(&read_durable_key/2, ctx, key) do
      {:ok, nil} ->
        :ok

      {:ok, expected} when is_binary(expected) ->
        with {:ok, stored} <- decode_page(expected),
             :ok <- same_page(page, stored) do
          cond do
            stored.state == :released ->
              :ok

            stored.state in allowed_states ->
              released = %{stored | state: :released}

              case Router.cas(ctx, key, expected, encode_page(released), nil) do
                1 ->
                  :ok

                result when result in [0, nil] ->
                  claim_page_for_discard(ctx, page, allowed_states, retries_left - 1)

                {:error, _reason} = error ->
                  error

                _other ->
                  {:error, :cache_session_page_write_failed}
              end

            true ->
              {:error, :cache_session_page_not_discardable}
          end
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp delete_pages(ctx, pages) do
    Enum.reduce_while(pages, :ok, fn page, :ok ->
      case Router.delete(ctx, page_key(page)) do
        :ok -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
        _other -> {:halt, {:error, :cache_session_page_delete_failed}}
      end
    end)
  end

  defp compact_recovery_floor(ctx, session, floor_read_fun) do
    with {:ok, meta} <- read_meta(ctx, session, session.session_id) do
      first_sequence = meta.recovery_floor
      last_sequence = min(meta.page_count, first_sequence + @floor_compaction_limit - 1)

      if first_sequence > last_sequence do
        :ok
      else
        keys =
          Enum.map(first_sequence..last_sequence, fn sequence ->
            page_key(Map.put(session, :sequence, sequence))
          end)

        case safe_durable_batch_read(floor_read_fun, ctx, keys) do
          {:ok, values} ->
            target_floor =
              values
              |> Enum.take_while(&is_nil/1)
              |> length()
              |> Kernel.+(first_sequence)

            advance_compacted_floor(ctx, session, first_sequence, target_floor)

          {:error, _reason} ->
            :ok
        end
      end
    end
  end

  defp advance_compacted_floor(_ctx, _session, floor, floor), do: :ok

  defp advance_compacted_floor(ctx, session, expected_floor, target_floor) do
    case update_meta(ctx, session, fn meta ->
           if meta.recovery_floor == expected_floor do
             floor = min(target_floor, meta.page_count + 1)
             {:ok, %{meta | recovery_floor: floor}, :ok}
           else
             {:ok, meta, :ok}
           end
         end) do
      {:ok, :ok} -> :ok
      {:error, _reason} = error -> error
    end
  end

  defp run_page_delete_checkpoint(checkpoint_fun) do
    _ = checkpoint_fun.()
    :ok
  end

  defp advance_discarded_floor(recovery_floor, []), do: recovery_floor

  defp advance_discarded_floor(recovery_floor, sequences) do
    if hd(sequences) == recovery_floor and contiguous_sequences?(sequences) do
      List.last(sequences) + 1
    else
      recovery_floor
    end
  end

  defp contiguous_sequences?([_sequence]), do: true

  defp contiguous_sequences?([left, right | rest]) do
    right == left + 1 and contiguous_sequences?([right | rest])
  end

  defp validate_discard_states(states) when is_list(states) and states != [] do
    if length(states) == length(Enum.uniq(states)) and
         Enum.all?(states, &(&1 in [:unused, :uncertain])) do
      :ok
    else
      {:error, "ERR invalid flow governance cache discard options"}
    end
  end

  defp validate_discard_states(_states),
    do: {:error, "ERR invalid flow governance cache discard options"}

  defp assert_current(ctx, session) do
    key = Keys.governance_limit_cache_session_head_key(session.node_id, session.instance_name)

    case safe_durable_read(&read_durable_key/2, ctx, key) do
      {:ok, value} when is_binary(value) ->
        case decode_head(value) do
          {:ok, %{session_id: session_id, generation: generation}}
          when session_id == session.session_id and generation == session.generation ->
            :ok

          {:ok, _other} ->
            {:error, :stale_cache_session}

          {:error, _reason} = error ->
            error
        end

      {:ok, nil} ->
        {:error, :stale_cache_session}

      {:error, _reason} = error ->
        error
    end
  end

  defp validate_meta_session(meta, session) do
    if meta.session_id == session.session_id and meta.generation == session.generation and
         meta.state == :active do
      :ok
    else
      {:error, :stale_cache_session}
    end
  end

  defp same_session(session, page) do
    if Map.get(page, :session_id) == Map.get(session, :session_id) and
         Map.get(page, :generation) == Map.get(session, :generation) and
         Map.get(page, :node_id) == Map.get(session, :node_id) and
         Map.get(page, :instance_name) == Map.get(session, :instance_name) and
         is_integer(Map.get(page, :sequence)) and Map.get(page, :sequence) > 0 and
         Map.get(page, :sequence) <= @max_exact_version do
      :ok
    else
      {:error, :cache_session_page_mismatch}
    end
  end

  defp validate_session_pages(session, pages) do
    Enum.reduce_while(pages, :ok, fn page, :ok ->
      case same_session(session, page) do
        :ok -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp same_page(expected, stored) do
    fields = [:node_id, :instance_name, :session_id, :generation, :sequence]

    if Enum.all?(fields, &(Map.get(expected, &1) == Map.get(stored, &1))) do
      :ok
    else
      {:error, :cache_session_page_mismatch}
    end
  end

  defp meta_key(session) do
    Keys.governance_limit_cache_session_meta_key(
      session.node_id,
      session.instance_name,
      session.session_id
    )
  end

  defp page_key(page) do
    Keys.governance_limit_cache_session_page_key(
      page.node_id,
      page.instance_name,
      page.session_id,
      page.sequence
    )
  end

  defp encode_head(session) do
    TermCodec.encode({@head_tag, session.generation, session.session_id})
  end

  defp decode_head(nil), do: {:ok, %{generation: 0, session_id: nil}}

  defp decode_head(value) when is_binary(value) do
    case TermCodec.decode(value) do
      {:ok, {@head_tag, generation, session_id}}
      when is_integer(generation) and generation > 0 and is_binary(session_id) and
             generation <= @max_exact_version and session_id != "" and
             byte_size(session_id) <= @max_identity_bytes ->
        {:ok, %{generation: generation, session_id: session_id}}

      _other ->
        {:error, :cache_session_head_corrupt}
    end
  end

  defp decode_head(_value), do: {:error, :cache_session_head_corrupt}

  defp encode_meta(meta) do
    TermCodec.encode(
      {@meta_tag, meta.session_id, meta.generation, meta.previous_session_id,
       meta.cleanup_session_id, meta.page_count, meta.recovery_floor, meta.state}
    )
  end

  defp decode_meta(value) when is_binary(value) do
    case TermCodec.decode(value) do
      {:ok,
       {@meta_tag, session_id, generation, previous_session_id, cleanup_session_id, page_count,
        recovery_floor, state}}
      when is_binary(session_id) and session_id != "" and is_integer(generation) and
             generation > 0 and generation <= @max_exact_version and
             byte_size(session_id) <= @max_identity_bytes and
             (is_nil(previous_session_id) or
                (is_binary(previous_session_id) and previous_session_id != "" and
                   byte_size(previous_session_id) <= @max_identity_bytes)) and
             (is_nil(cleanup_session_id) or
                (is_binary(cleanup_session_id) and cleanup_session_id != "" and
                   byte_size(cleanup_session_id) <= @max_identity_bytes)) and
             cleanup_session_id != session_id and is_integer(page_count) and page_count >= 0 and
             page_count <= @max_exact_version and is_integer(recovery_floor) and
             recovery_floor > 0 and recovery_floor <= page_count + 1 and
             state in [:active, :recovered] ->
        {:ok,
         %{
           session_id: session_id,
           generation: generation,
           previous_session_id: previous_session_id,
           cleanup_session_id: cleanup_session_id,
           page_count: page_count,
           recovery_floor: recovery_floor,
           state: state
         }}

      _other ->
        {:error, :cache_session_manifest_corrupt}
    end
  end

  defp decode_meta(_value), do: {:error, :cache_session_manifest_corrupt}

  defp encode_page(page) do
    TermCodec.encode(
      {@page_tag, page.node_id, page.instance_name, page.session_id, page.generation,
       page.sequence, page.scope, page.shard_id, page.expires_at_ms, page.config_version,
       page.effective_limit, page.reservation_ids, page.state}
    )
  end

  defp decode_page(value) when is_binary(value) do
    if byte_size(value) > @max_encoded_page_bytes do
      {:error, :cache_session_page_corrupt}
    else
      case TermCodec.decode(value) do
        {:ok,
         {@page_tag, node_id, instance_name, session_id, generation, sequence, scope, shard_id,
          expires_at_ms, config_version, effective_limit, reservation_ids, state}}
        when is_binary(node_id) and node_id != "" and is_binary(instance_name) and
               instance_name != "" and is_binary(session_id) and session_id != "" and
               byte_size(node_id) <= @max_identity_bytes and
               byte_size(instance_name) <= @max_identity_bytes and
               byte_size(session_id) <= @max_identity_bytes and
               is_integer(generation) and generation > 0 and generation <= @max_exact_version and
               is_integer(sequence) and sequence > 0 and sequence <= @max_exact_version and
               is_binary(scope) and scope != "" and byte_size(scope) <= @max_identity_bytes and
               is_integer(shard_id) and shard_id >= 0 and
               is_integer(expires_at_ms) and expires_at_ms >= 0 and
               expires_at_ms <= @max_exact_version and is_list(reservation_ids) and
               reservation_ids != [] and is_integer(config_version) and config_version >= 0 and
               config_version <= @max_exact_version and
               (is_nil(effective_limit) or
                  (is_integer(effective_limit) and effective_limit >= 0 and
                     effective_limit <= @max_exact_version)) and
               state in [:unused, :uncertain, :retained, :released] ->
          page = %{
            node_id: node_id,
            instance_name: instance_name,
            session_id: session_id,
            generation: generation,
            sequence: sequence,
            scope: scope,
            shard_id: shard_id,
            expires_at_ms: expires_at_ms,
            config_version: config_version,
            effective_limit: effective_limit,
            reservation_ids: reservation_ids,
            state: state
          }

          with :ok <- validate_page_reservation_ids(reservation_ids), do: {:ok, page}

        _other ->
          {:error, :cache_session_page_corrupt}
      end
    end
  end

  defp decode_page(_value), do: {:error, :cache_session_page_corrupt}

  defp safe_release(release_fun, ctx, scope, opts) do
    case release_fun.(ctx, scope, opts) do
      {:ok, _owner} = ok -> ok
      {:error, _reason} = error -> error
      other -> {:error, {:invalid_release_result, other}}
    end
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  defp safe_page_delete(delete_fun, ctx, page) do
    case delete_fun.(ctx, page_key(page)) do
      :ok -> :ok
      {:error, _reason} = error -> error
      other -> {:error, {:invalid_page_delete_result, other}}
    end
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  defp safe_durable_read(read_fun, ctx, key) do
    case read_fun.(ctx, key) do
      {:ok, value} when is_binary(value) or is_nil(value) -> {:ok, value}
      :unavailable -> {:error, :cache_session_storage_unavailable}
      {:error, _reason} = error -> error
      other -> {:error, {:invalid_cache_session_read_result, other}}
    end
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  defp safe_durable_batch_read(read_fun, ctx, keys) do
    case read_fun.(ctx, keys) do
      {:ok, values} when is_list(values) and length(values) == length(keys) ->
        if Enum.all?(values, &(is_binary(&1) or is_nil(&1))) do
          {:ok, values}
        else
          {:error, :cache_session_batch_read_corrupt}
        end

      :unavailable ->
        {:error, :cache_session_storage_unavailable}

      {:error, _reason} = error ->
        error

      other ->
        {:error, {:invalid_cache_session_batch_read_result, other}}
    end
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  defp read_durable_key(ctx, key) do
    Router.read_shard_value(ctx, Router.shard_for(ctx, key), key)
  end

  defp read_durable_keys(_ctx, []), do: {:ok, []}

  defp read_durable_keys(ctx, [first_key | _rest] = keys) do
    shard_index = Router.shard_for(ctx, first_key)

    if Enum.all?(keys, &(Router.shard_for(ctx, &1) == shard_index)) do
      Router.read_shard_values(ctx, shard_index, keys)
    else
      {:error, :cache_session_batch_cross_shard}
    end
  end

  defp validate_recovery_opts(
         cursor,
         limit,
         now_ms,
         release_fun,
         page_delete_fun,
         page_read_fun,
         checkpoint_fun
       ) do
    if valid_recovery_cursor?(cursor) and is_integer(limit) and limit > 0 and
         limit <= @max_page_size and is_integer(now_ms) and now_ms >= 0 and
         now_ms <= @max_exact_version and
         is_function(release_fun, 3) and is_function(page_delete_fun, 2) and
         is_function(page_read_fun, 2) and
         is_function(checkpoint_fun, 1) do
      :ok
    else
      {:error, "ERR invalid flow governance cache session recovery options"}
    end
  end

  defp valid_recovery_cursor?(nil), do: true

  defp valid_recovery_cursor?(%{session_id: session_id, sequence: sequence}) do
    is_binary(session_id) and session_id != "" and is_integer(sequence) and sequence > 0 and
      sequence <= @max_exact_version
  end

  defp valid_recovery_cursor?(_cursor), do: false

  defp validate_session(session) when is_map(session) do
    if valid_identity?(Map.get(session, :node_id)) and
         valid_identity?(Map.get(session, :instance_name)) and
         valid_identity?(Map.get(session, :session_id)) and
         is_integer(Map.get(session, :generation)) and Map.get(session, :generation) > 0 and
         Map.get(session, :generation) <= @max_exact_version and
         (is_nil(Map.get(session, :previous_session_id)) or
            valid_identity?(Map.get(session, :previous_session_id))) do
      :ok
    else
      {:error, "ERR invalid flow governance cache session"}
    end
  end

  defp validate_session(_session), do: {:error, "ERR invalid flow governance cache session"}

  defp valid_page_identity?(page) do
    valid_identity?(Map.get(page, :node_id)) and
      valid_identity?(Map.get(page, :instance_name)) and
      valid_identity?(Map.get(page, :session_id)) and
      is_integer(Map.get(page, :sequence)) and Map.get(page, :sequence) > 0 and
      Map.get(page, :sequence) <= @max_exact_version
  end

  defp valid_identity?(value) do
    is_binary(value) and value != "" and byte_size(value) <= Router.max_key_size()
  end

  defp required_update_option(opts, key) do
    case Keyword.fetch(opts, key) do
      {:ok, value} -> {:ok, value}
      :error -> {:error, "ERR invalid flow governance cache update options"}
    end
  end

  defp validate_page_size(page_size)
       when is_integer(page_size) and page_size > 0 and page_size <= @max_page_size,
       do: :ok

  defp validate_page_size(_page_size),
    do: {:error, "ERR flow governance cache session page_size must be between 1 and 256"}

  defp validate_expires_at(expires_at_ms)
       when is_integer(expires_at_ms) and expires_at_ms >= 0 and
              expires_at_ms <= @max_exact_version,
       do: :ok

  defp validate_expires_at(_expires_at_ms),
    do: {:error, "ERR invalid flow governance cache session expiry"}

  defp validate_cache_configuration(config_version, effective_limit)
       when is_integer(config_version) and config_version >= 0 and
              config_version <= @max_exact_version and
              (is_nil(effective_limit) or
                 (is_integer(effective_limit) and effective_limit >= 0 and
                    effective_limit <= @max_exact_version)),
       do: :ok

  defp validate_cache_configuration(_config_version, _effective_limit),
    do: {:error, "ERR invalid flow governance cache session configuration"}

  defp validate_prefetch_reservation_ids(reservation_ids),
    do: validate_reservation_ids(reservation_ids, @max_prefetch_ids)

  defp validate_page_reservation_ids(reservation_ids),
    do: validate_reservation_ids(reservation_ids, LimitRecord.page_size())

  defp validate_reservation_ids(reservation_ids, max_count) do
    if length(reservation_ids) <= max_count and
         Enum.all?(reservation_ids, &LimitRecord.valid_reservation_id?/1) and
         length(reservation_ids) == length(Enum.uniq(reservation_ids)) do
      :ok
    else
      {:error, "ERR invalid flow governance cache reservation ids"}
    end
  end

  defp required_identity(opts, key) do
    case Keyword.fetch(opts, key) do
      {:ok, value} when is_binary(value) ->
        if valid_identity?(value),
          do: {:ok, value},
          else: {:error, "ERR invalid flow governance cache session identity"}

      {:ok, value} when is_atom(value) ->
        value = Atom.to_string(value)

        if valid_identity?(value),
          do: {:ok, value},
          else: {:error, "ERR invalid flow governance cache session identity"}

      _missing_or_invalid ->
        {:error, "ERR invalid flow governance cache session identity"}
    end
  end

  defp deterministic_session_id(node_id, instance_name, generation, previous_session_id) do
    {:flow_governance_cache_session, node_id, instance_name, generation, previous_session_id}
    |> TermCodec.encode()
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.url_encode64(padding: false)
  end
end
