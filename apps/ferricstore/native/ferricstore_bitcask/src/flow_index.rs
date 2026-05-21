use rustler::{Binary, Encoder, Env, NifResult, OwnedBinary, ResourceArc, Term};
use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap};
use std::sync::Mutex;

#[derive(Clone, Copy, Debug)]
struct Score(f64);

impl PartialEq for Score {
    fn eq(&self, other: &Self) -> bool {
        self.0.total_cmp(&other.0) == Ordering::Equal
    }
}

impl Eq for Score {}

impl PartialOrd for Score {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Score {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.total_cmp(&other.0)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct OrderedEntry {
    key: Vec<u8>,
    score: Score,
    member: Vec<u8>,
}

#[derive(Default)]
struct FlowOrderedIndex {
    ordered: BTreeSet<OrderedEntry>,
    lookup: HashMap<(Vec<u8>, Vec<u8>), f64>,
    counts: HashMap<Vec<u8>, i64>,
}

pub struct FlowOrderedIndexResource {
    inner: Mutex<FlowOrderedIndex>,
}

#[derive(rustler::NifTuple)]
struct ClaimEntry<'a>(
    Binary<'a>,
    Binary<'a>,
    f64,
    Binary<'a>,
    f64,
    Binary<'a>,
    f64,
    Binary<'a>,
    f64,
    Binary<'a>,
    Binary<'a>,
    f64,
);

const FLOW_RECORD_MAGIC: &[u8; 4] = b"FSF4";
const FLOW_HISTORY_MAGIC: &[u8; 4] = b"FSH1";
const RUNNING_STATE: &[u8] = b"running";
const DEFAULT_RUNNING_RUN_STATE: &[u8] = b"queued";

struct FlowRecordParts<'a> {
    id: &'a [u8],
    flow_type: &'a [u8],
    state: &'a [u8],
    version: Option<u64>,
    attempts: Option<u64>,
    fencing_token: Option<u64>,
    created_at_ms: Option<u64>,
    updated_at_ms: Option<u64>,
    next_run_at_ms: Option<u64>,
    priority: Option<u64>,
    ttl_ms: Option<u64>,
    history_hot_max_events: Option<u64>,
    history_max_events: Option<u64>,
    retention_ttl_ms: Option<u64>,
    terminal_retention_until_ms: Option<u64>,
    partition_key: Option<&'a [u8]>,
    payload_ref: Option<&'a [u8]>,
    parent_flow_id: Option<&'a [u8]>,
    parent_partition_key: Option<&'a [u8]>,
    root_flow_id: Option<&'a [u8]>,
    correlation_id: Option<&'a [u8]>,
    result_ref: Option<&'a [u8]>,
    error_ref: Option<&'a [u8]>,
    lease_owner: Option<&'a [u8]>,
    lease_token: Option<&'a [u8]>,
    lease_deadline_ms: Option<u64>,
    run_state: Option<&'a [u8]>,
    rewound_to_event_id: Option<&'a [u8]>,
    child_groups_encoded: &'a [u8],
}

#[allow(non_local_definitions)]
pub fn register_resource(env: Env) {
    let _ = rustler::resource!(FlowOrderedIndexResource, env);
}

#[rustler::nif(schedule = "Normal")]
pub fn flow_index_new() -> ResourceArc<FlowOrderedIndexResource> {
    ResourceArc::new(FlowOrderedIndexResource {
        inner: Mutex::new(FlowOrderedIndex::default()),
    })
}

#[rustler::nif(schedule = "Normal")]
pub fn flow_index_put_entries<'a>(
    env: Env<'a>,
    resource: ResourceArc<FlowOrderedIndexResource>,
    entries: Vec<(Binary<'a>, Binary<'a>, f64)>,
) -> NifResult<Term<'a>> {
    let mut index = resource.inner.lock().expect("flow index mutex poisoned");

    for (key_bin, member_bin, score) in entries {
        index.put(key_bin.as_slice(), member_bin.as_slice(), score, false);
    }

    Ok(crate::atoms::ok().encode(env))
}

#[rustler::nif(schedule = "Normal")]
pub fn flow_index_put_new_entries<'a>(
    env: Env<'a>,
    resource: ResourceArc<FlowOrderedIndexResource>,
    entries: Vec<(Binary<'a>, Binary<'a>, f64)>,
) -> NifResult<Term<'a>> {
    let mut index = resource.inner.lock().expect("flow index mutex poisoned");

    for (key_bin, member_bin, score) in entries {
        index.put(key_bin.as_slice(), member_bin.as_slice(), score, true);
    }

    Ok(crate::atoms::ok().encode(env))
}

#[rustler::nif(schedule = "Normal")]
pub fn flow_index_move_entries<'a>(
    env: Env<'a>,
    resource: ResourceArc<FlowOrderedIndexResource>,
    entries: Vec<(Binary<'a>, Binary<'a>, Binary<'a>, f64)>,
) -> NifResult<Term<'a>> {
    let mut index = resource.inner.lock().expect("flow index mutex poisoned");

    for (from_key_bin, to_key_bin, member_bin, score) in entries {
        index.move_member(
            from_key_bin.as_slice(),
            to_key_bin.as_slice(),
            member_bin.as_slice(),
            score,
        );
    }

    Ok(crate::atoms::ok().encode(env))
}

#[rustler::nif(schedule = "Normal")]
pub fn flow_index_delete_members<'a>(
    env: Env<'a>,
    resource: ResourceArc<FlowOrderedIndexResource>,
    key: Binary<'a>,
    members: Vec<Binary<'a>>,
) -> NifResult<Term<'a>> {
    let mut index = resource.inner.lock().expect("flow index mutex poisoned");

    for member in members {
        index.delete(key.as_slice(), member.as_slice());
    }

    Ok(crate::atoms::ok().encode(env))
}

#[rustler::nif(schedule = "Normal")]
pub fn flow_index_apply_batch<'a>(
    env: Env<'a>,
    resource: ResourceArc<FlowOrderedIndexResource>,
    put_entries: Vec<(Binary<'a>, Binary<'a>, f64)>,
    put_new_entries: Vec<(Binary<'a>, Binary<'a>, f64)>,
    move_entries: Vec<(Binary<'a>, Binary<'a>, Binary<'a>, f64)>,
    delete_entries: Vec<(Binary<'a>, Vec<Binary<'a>>)>,
    claim_entries: Vec<ClaimEntry<'a>>,
) -> NifResult<Term<'a>> {
    let mut index = resource.inner.lock().expect("flow index mutex poisoned");

    for (key_bin, member_bin, score) in put_entries {
        index.put(key_bin.as_slice(), member_bin.as_slice(), score, false);
    }

    for (key_bin, member_bin, score) in put_new_entries {
        index.put(key_bin.as_slice(), member_bin.as_slice(), score, true);
    }

    for (from_key_bin, to_key_bin, member_bin, score) in move_entries {
        index.move_member(
            from_key_bin.as_slice(),
            to_key_bin.as_slice(),
            member_bin.as_slice(),
            score,
        );
    }

    for (key_bin, members) in delete_entries {
        for member in members {
            index.delete(key_bin.as_slice(), member.as_slice());
        }
    }

    apply_claim_entries_locked(&mut index, claim_entries);

    Ok(crate::atoms::ok().encode(env))
}

#[rustler::nif(schedule = "Normal")]
pub fn flow_index_score_of<'a>(
    env: Env<'a>,
    resource: ResourceArc<FlowOrderedIndexResource>,
    key: Binary<'a>,
    member: Binary<'a>,
) -> NifResult<Term<'a>> {
    let index = resource.inner.lock().expect("flow index mutex poisoned");

    match index.lookup.get(&(key.as_slice().to_vec(), member.as_slice().to_vec())) {
        Some(score) => Ok((crate::atoms::ok(), *score).encode(env)),
        None => Ok(crate::atoms::miss().encode(env)),
    }
}

#[rustler::nif(schedule = "Normal")]
pub fn flow_index_range_slice<'a>(
    env: Env<'a>,
    resource: ResourceArc<FlowOrderedIndexResource>,
    key: Binary<'a>,
    min_kind: u8,
    min_score: f64,
    max_kind: u8,
    max_score: f64,
    reverse: bool,
    offset: usize,
    count: isize,
) -> NifResult<Term<'a>> {
    let index = resource.inner.lock().expect("flow index mutex poisoned");
    let rows = index.range_slice(
        key.as_slice(),
        Bound::from_min(min_kind, min_score),
        Bound::from_max(max_kind, max_score),
        reverse,
        offset,
        count,
    );

    encode_member_scores(env, rows)
}

#[rustler::nif(schedule = "Normal")]
pub fn flow_index_take_due<'a>(
    env: Env<'a>,
    resource: ResourceArc<FlowOrderedIndexResource>,
    key: Binary<'a>,
    max_score: f64,
    count: usize,
) -> NifResult<Term<'a>> {
    let mut index = resource.inner.lock().expect("flow index mutex poisoned");
    let rows = index.take_due(key.as_slice(), max_score, count);

    encode_owned_member_scores(env, rows)
}

#[rustler::nif(schedule = "Normal")]
pub fn flow_index_claim_due_candidates<'a>(
    env: Env<'a>,
    resource: ResourceArc<FlowOrderedIndexResource>,
    keys: Vec<Binary<'a>>,
    max_score: f64,
    limit: usize,
    max_scan: usize,
) -> NifResult<Term<'a>> {
    let index = resource.inner.lock().expect("flow index mutex poisoned");
    let key_refs = keys
        .iter()
        .map(|key| key.as_slice())
        .collect::<Vec<_>>();
    let rows = index.claim_due_candidates(&key_refs, max_score, limit, max_scan);

    encode_owned_key_member_scores(env, rows)
}

#[rustler::nif(schedule = "Normal")]
pub fn flow_index_count_all<'a>(
    resource: ResourceArc<FlowOrderedIndexResource>,
    key: Binary<'a>,
) -> i64 {
    let index = resource.inner.lock().expect("flow index mutex poisoned");
    index.counts.get(key.as_slice()).copied().unwrap_or(0).max(0)
}

#[rustler::nif(schedule = "Normal")]
pub fn flow_index_count_many<'a>(
    resource: ResourceArc<FlowOrderedIndexResource>,
    keys: Vec<Binary<'a>>,
) -> Vec<i64> {
    let index = resource.inner.lock().expect("flow index mutex poisoned");

    keys.into_iter()
        .map(|key| index.counts.get(key.as_slice()).copied().unwrap_or(0).max(0))
        .collect()
}

#[rustler::nif(schedule = "Normal")]
pub fn flow_index_count_keys<'a>(
    env: Env<'a>,
    resource: ResourceArc<FlowOrderedIndexResource>,
) -> NifResult<Term<'a>> {
    let index = resource.inner.lock().expect("flow index mutex poisoned");
    let keys = index
        .counts
        .iter()
        .filter_map(|(key, count)| if *count > 0 { Some(key.as_slice()) } else { None })
        .collect::<Vec<_>>();

    encode_binaries(env, keys)
}

#[rustler::nif(schedule = "Normal")]
pub fn flow_index_due_count_keys<'a>(
    env: Env<'a>,
    resource: ResourceArc<FlowOrderedIndexResource>,
) -> NifResult<Term<'a>> {
    let index = resource.inner.lock().expect("flow index mutex poisoned");
    let keys = index
        .counts
        .iter()
        .filter_map(|(key, count)| {
            if *count > 0 && due_key(key) {
                Some(key.as_slice())
            } else {
                None
            }
        })
        .collect::<Vec<_>>();

    encode_binaries(env, keys)
}

#[rustler::nif(schedule = "Normal")]
pub fn flow_index_restore_count<'a>(
    env: Env<'a>,
    resource: ResourceArc<FlowOrderedIndexResource>,
    key: Binary<'a>,
    count: i64,
) -> NifResult<Term<'a>> {
    let mut index = resource.inner.lock().expect("flow index mutex poisoned");
    index.counts.insert(key.as_slice().to_vec(), count);
    Ok(crate::atoms::ok().encode(env))
}

#[rustler::nif(schedule = "Normal")]
pub fn flow_index_delete_count<'a>(
    env: Env<'a>,
    resource: ResourceArc<FlowOrderedIndexResource>,
    key: Binary<'a>,
) -> NifResult<Term<'a>> {
    let mut index = resource.inner.lock().expect("flow index mutex poisoned");
    index.counts.remove(key.as_slice());
    Ok(crate::atoms::ok().encode(env))
}

#[rustler::nif(schedule = "Normal")]
pub fn flow_index_apply_claim_entries<'a>(
    env: Env<'a>,
    resource: ResourceArc<FlowOrderedIndexResource>,
    entries: Vec<ClaimEntry<'a>>,
) -> NifResult<Term<'a>> {
    let mut index = resource.inner.lock().expect("flow index mutex poisoned");
    apply_claim_entries_locked(&mut index, entries);

    Ok(crate::atoms::ok().encode(env))
}

fn apply_claim_entries_locked<'a>(index: &mut FlowOrderedIndex, entries: Vec<ClaimEntry<'a>>) {
    let mut count_deltas: HashMap<Vec<u8>, i64> = HashMap::new();

    for ClaimEntry(
        id,
        from_due_key,
        _from_due_score,
        to_due_key,
        to_due_score,
        from_state_key,
        _from_state_score,
        to_state_key,
        to_state_score,
        inflight_key,
        worker_key,
        lease_score,
    ) in entries
    {
        let id = id.as_slice();
        if index.delete_without_count(from_due_key.as_slice(), id) {
            add_count_delta(&mut count_deltas, from_due_key.as_slice(), -1);
        }
        if index.put_new_without_count(to_due_key.as_slice(), id, to_due_score) {
            add_count_delta(&mut count_deltas, to_due_key.as_slice(), 1);
        }
        if index.delete_without_count(from_state_key.as_slice(), id) {
            add_count_delta(&mut count_deltas, from_state_key.as_slice(), -1);
        }
        if index.put_new_without_count(to_state_key.as_slice(), id, to_state_score) {
            add_count_delta(&mut count_deltas, to_state_key.as_slice(), 1);
        }
        if index.put_new_without_count(inflight_key.as_slice(), id, lease_score) {
            add_count_delta(&mut count_deltas, inflight_key.as_slice(), 1);
        }
        if index.put_new_without_count(worker_key.as_slice(), id, lease_score) {
            add_count_delta(&mut count_deltas, worker_key.as_slice(), 1);
        }
    }

    index.apply_count_deltas(count_deltas);
}

#[rustler::nif(schedule = "Normal")]
pub fn flow_index_rollback_claim_entries<'a>(
    env: Env<'a>,
    resource: ResourceArc<FlowOrderedIndexResource>,
    entries: Vec<ClaimEntry<'a>>,
) -> NifResult<Term<'a>> {
    let mut index = resource.inner.lock().expect("flow index mutex poisoned");
    let mut count_deltas: HashMap<Vec<u8>, i64> = HashMap::new();

    for ClaimEntry(
        id,
        from_due_key,
        from_due_score,
        to_due_key,
        _to_due_score,
        from_state_key,
        from_state_score,
        to_state_key,
        _to_state_score,
        inflight_key,
        worker_key,
        _lease_score,
    ) in entries
    {
        let id = id.as_slice();
        if index.delete_without_count(to_due_key.as_slice(), id) {
            add_count_delta(&mut count_deltas, to_due_key.as_slice(), -1);
        }
        if index.put_new_without_count(from_due_key.as_slice(), id, from_due_score) {
            add_count_delta(&mut count_deltas, from_due_key.as_slice(), 1);
        }
        if index.delete_without_count(to_state_key.as_slice(), id) {
            add_count_delta(&mut count_deltas, to_state_key.as_slice(), -1);
        }
        if index.delete_without_count(inflight_key.as_slice(), id) {
            add_count_delta(&mut count_deltas, inflight_key.as_slice(), -1);
        }
        if index.delete_without_count(worker_key.as_slice(), id) {
            add_count_delta(&mut count_deltas, worker_key.as_slice(), -1);
        }
        if index.put_new_without_count(from_state_key.as_slice(), id, from_state_score) {
            add_count_delta(&mut count_deltas, from_state_key.as_slice(), 1);
        }
    }

    index.apply_count_deltas(count_deltas);

    Ok(crate::atoms::ok().encode(env))
}

#[rustler::nif(schedule = "Normal")]
pub fn flow_record_plan_claims<'a>(
    env: Env<'a>,
    candidates: Vec<(Binary<'a>, f64)>,
    values: Vec<Option<Binary<'a>>>,
    flow_type: Binary<'a>,
    expected_state: Binary<'a>,
    worker: Binary<'a>,
    lease_ms: u64,
    now_ms: u64,
    remaining: usize,
    from_due_key: Binary<'a>,
    to_due_key: Binary<'a>,
    from_state_key: Binary<'a>,
    to_state_key: Binary<'a>,
    inflight_key: Binary<'a>,
    worker_key: Binary<'a>,
    state_key_prefix: Binary<'a>,
) -> NifResult<Term<'a>> {
    if values.len() != candidates.len() || expected_state.as_slice() == RUNNING_STATE {
        return Ok(crate::atoms::fallback().encode(env));
    }

    if remaining == 0 {
        let empty: Vec<Term<'a>> = Vec::new();
        return Ok((crate::atoms::ok(), empty.clone(), empty, 0usize).encode(env));
    }

    let flow_type = flow_type.as_slice();
    let expected_state = expected_state.as_slice();
    let worker = worker.as_slice();
    let deadline_ms = now_ms.saturating_add(lease_ms);
    let plan_capacity = remaining.min(candidates.len());
    let mut plan_terms = Vec::with_capacity(plan_capacity);
    let mut stale_terms = Vec::new();
    let mut accepted = 0usize;

    for ((id_bin, due_score), value) in candidates.iter().zip(values.iter()) {
        if accepted >= remaining {
            break;
        }

        let id = id_bin.as_slice();
        let Some(value) = value else {
            stale_terms.push(id_bin.encode(env));
            continue;
        };

        let Some(record) = decode_flow_record(value.as_slice()) else {
            stale_terms.push(id_bin.encode(env));
            continue;
        };

        if record.id != id {
            return Ok(crate::atoms::fallback().encode(env));
        }

        if record.flow_type != flow_type {
            continue;
        }

        if record.state != expected_state {
            stale_terms.push(id_bin.encode(env));
            continue;
        }

        if !flow_record_fast_claim_shape(&record) {
            return Ok(crate::atoms::fallback().encode(env));
        }

        let Some(next_run_at_ms) = record.next_run_at_ms else {
            continue;
        };

        if next_run_at_ms > now_ms {
            continue;
        }

        let Some(version) = record.version else {
            return Ok(crate::atoms::fallback().encode(env));
        };
        let Some(fencing_token) = record.fencing_token else {
            return Ok(crate::atoms::fallback().encode(env));
        };
        let Some(updated_at_ms) = record.updated_at_ms else {
            return Ok(crate::atoms::fallback().encode(env));
        };
        if record.priority.is_none() {
            return Ok(crate::atoms::fallback().encode(env));
        }

        let next_version = version.saturating_add(1);
        let next_fencing_token = fencing_token.saturating_add(1);
        let lease_token = claim_lease_token(worker, now_ms, next_fencing_token);
        let next_value = encode_claimed_record(
            &record,
            worker,
            &lease_token,
            deadline_ms,
            now_ms,
            next_version,
            next_fencing_token,
        );

        let mut state_key = Vec::with_capacity(state_key_prefix.as_slice().len() + id.len());
        state_key.extend_from_slice(state_key_prefix.as_slice());
        state_key.extend_from_slice(id);

        let next_value_term = binary_term(env, &next_value)?;
        let state_key_term = binary_term(env, &state_key)?;
        let deadline_score = deadline_ms as f64;
        let now_score = now_ms as f64;
        let updated_score = updated_at_ms as f64;
        let previous_history_ms = record.updated_at_ms.or(record.created_at_ms);

        let entry_term = ClaimEntry(
            id_bin.clone(),
            from_due_key.clone(),
            *due_score,
            to_due_key.clone(),
            deadline_score,
            from_state_key.clone(),
            updated_score,
            to_state_key.clone(),
            now_score,
            inflight_key.clone(),
            worker_key.clone(),
            deadline_score,
        )
            .encode(env);

        plan_terms.push((
            next_value_term,
            entry_term,
            state_key_term,
            previous_history_ms,
        )
            .encode(env));
        accepted += 1;
    }

    Ok((crate::atoms::ok(), plan_terms, stale_terms, accepted).encode(env))
}

#[rustler::nif(schedule = "Normal")]
pub fn flow_record_encode<'a>(
    env: Env<'a>,
    id: Option<Binary<'a>>,
    flow_type: Option<Binary<'a>>,
    state: Option<Binary<'a>>,
    version: Option<u64>,
    attempts: Option<u64>,
    fencing_token: Option<u64>,
    created_at_ms: Option<u64>,
    updated_at_ms: Option<u64>,
    next_run_at_ms: Option<u64>,
    priority: Option<u64>,
    ttl_ms: Option<u64>,
    history_hot_max_events: Option<u64>,
    history_max_events: Option<u64>,
    retention_ttl_ms: Option<u64>,
    terminal_retention_until_ms: Option<u64>,
    partition_key: Option<Binary<'a>>,
    payload_ref: Option<Binary<'a>>,
    parent_flow_id: Option<Binary<'a>>,
    parent_partition_key: Option<Binary<'a>>,
    root_flow_id: Option<Binary<'a>>,
    correlation_id: Option<Binary<'a>>,
    result_ref: Option<Binary<'a>>,
    error_ref: Option<Binary<'a>>,
    lease_owner: Option<Binary<'a>>,
    lease_token: Option<Binary<'a>>,
    lease_deadline_ms: Option<u64>,
    run_state: Option<Binary<'a>>,
    rewound_to_event_id: Option<Binary<'a>>,
    child_groups_encoded: Binary<'a>,
) -> NifResult<Term<'a>> {
    let mut out = Vec::with_capacity(
        FLOW_RECORD_MAGIC.len()
            + optional_bin_len(id.as_ref())
            + optional_bin_len(flow_type.as_ref())
            + optional_bin_len(state.as_ref())
            + optional_bin_len(partition_key.as_ref())
            + optional_bin_len(payload_ref.as_ref())
            + optional_bin_len(parent_flow_id.as_ref())
            + optional_bin_len(parent_partition_key.as_ref())
            + optional_bin_len(root_flow_id.as_ref())
            + optional_bin_len(correlation_id.as_ref())
            + optional_bin_len(result_ref.as_ref())
            + optional_bin_len(error_ref.as_ref())
            + optional_bin_len(lease_owner.as_ref())
            + optional_bin_len(lease_token.as_ref())
            + optional_bin_len(run_state.as_ref())
            + optional_bin_len(rewound_to_event_id.as_ref())
            + child_groups_encoded.as_slice().len()
            + 96,
    );

    out.extend_from_slice(FLOW_RECORD_MAGIC);
    encode_bin(&mut out, optional_bin_slice(id.as_ref()));
    encode_bin(&mut out, optional_bin_slice(flow_type.as_ref()));
    encode_bin(&mut out, optional_bin_slice(state.as_ref()));
    encode_int(&mut out, version);
    encode_int(&mut out, attempts);
    encode_int(&mut out, fencing_token);
    encode_int(&mut out, created_at_ms);
    encode_int(&mut out, updated_at_ms);
    encode_int(&mut out, next_run_at_ms);
    encode_int(&mut out, priority);
    encode_int(&mut out, ttl_ms);
    encode_int(&mut out, history_hot_max_events);
    encode_int(&mut out, history_max_events);
    encode_int(&mut out, retention_ttl_ms);
    encode_int(&mut out, terminal_retention_until_ms);
    encode_bin(&mut out, optional_bin_slice(partition_key.as_ref()));
    encode_bin(&mut out, optional_bin_slice(payload_ref.as_ref()));
    encode_bin(&mut out, optional_bin_slice(parent_flow_id.as_ref()));
    encode_bin(&mut out, optional_bin_slice(parent_partition_key.as_ref()));
    encode_bin(&mut out, optional_bin_slice(root_flow_id.as_ref()));
    encode_bin(&mut out, optional_bin_slice(correlation_id.as_ref()));
    encode_bin(&mut out, optional_bin_slice(result_ref.as_ref()));
    encode_bin(&mut out, optional_bin_slice(error_ref.as_ref()));
    encode_bin(&mut out, optional_bin_slice(lease_owner.as_ref()));
    encode_bin(&mut out, optional_bin_slice(lease_token.as_ref()));
    encode_int(&mut out, lease_deadline_ms);
    encode_bin(&mut out, optional_bin_slice(run_state.as_ref()));
    encode_bin(&mut out, optional_bin_slice(rewound_to_event_id.as_ref()));
    out.extend_from_slice(child_groups_encoded.as_slice());

    binary_term(env, &out)
}

#[rustler::nif(schedule = "Normal")]
pub fn flow_record_decode<'a>(env: Env<'a>, value: Binary<'a>) -> NifResult<Term<'a>> {
    let Some(record) = decode_flow_record(value.as_slice()) else {
        return Ok(crate::atoms::error().encode(env));
    };

    let fields = vec![
        binary_term(env, record.id)?,
        binary_term(env, record.flow_type)?,
        binary_term(env, record.state)?,
        option_u64_term(env, record.version),
        option_u64_term(env, record.attempts),
        option_u64_term(env, record.fencing_token),
        option_u64_term(env, record.created_at_ms),
        option_u64_term(env, record.updated_at_ms),
        option_u64_term(env, record.next_run_at_ms),
        option_u64_term(env, record.priority),
        option_u64_term(env, record.ttl_ms),
        option_u64_term(env, record.history_hot_max_events),
        option_u64_term(env, record.history_max_events),
        option_u64_term(env, record.retention_ttl_ms),
        option_u64_term(env, record.terminal_retention_until_ms),
        option_binary_term(env, record.partition_key)?,
        option_binary_term(env, record.payload_ref)?,
        option_binary_term(env, record.parent_flow_id)?,
        option_binary_term(env, record.parent_partition_key)?,
        option_binary_term(env, record.root_flow_id)?,
        option_binary_term(env, record.correlation_id)?,
        option_binary_term(env, record.result_ref)?,
        option_binary_term(env, record.error_ref)?,
        option_binary_term(env, record.lease_owner)?,
        option_binary_term(env, record.lease_token)?,
        option_u64_term(env, record.lease_deadline_ms),
        option_binary_term(env, record.run_state)?,
        option_binary_term(env, record.rewound_to_event_id)?,
        binary_term(env, record.child_groups_encoded)?,
    ];

    Ok((crate::atoms::ok(), fields).encode(env))
}

#[rustler::nif(schedule = "Normal")]
pub fn flow_records_terminal_after_noop(values: Vec<Binary<'_>>) -> Vec<bool> {
    values
        .iter()
        .map(|value| {
            decode_flow_record(value.as_slice())
                .map(|record| {
                    blank(record.parent_flow_id)
                        && encoded_child_groups_empty(record.child_groups_encoded)
                })
                .unwrap_or(false)
        })
        .collect()
}

#[rustler::nif(schedule = "Normal")]
pub fn flow_history_encode<'a>(
    env: Env<'a>,
    event: Binary<'a>,
    version: Option<u64>,
    now_ms: Option<u64>,
    id: Option<Binary<'a>>,
    flow_type: Option<Binary<'a>>,
    state: Option<Binary<'a>>,
    priority: Option<u64>,
    attempts: Option<u64>,
    fencing_token: Option<u64>,
    created_at_ms: Option<u64>,
    updated_at_ms: Option<u64>,
    next_run_at_ms: Option<u64>,
    lease_deadline_ms: Option<u64>,
    lease_owner: Option<Binary<'a>>,
    payload_ref: Option<Binary<'a>>,
    parent_flow_id: Option<Binary<'a>>,
    root_flow_id: Option<Binary<'a>>,
    correlation_id: Option<Binary<'a>>,
    result_ref: Option<Binary<'a>>,
    error_ref: Option<Binary<'a>>,
    rewound_to_event_id: Option<Binary<'a>>,
    meta_encoded: Binary<'a>,
) -> NifResult<Term<'a>> {
    let mut out = Vec::with_capacity(
        FLOW_HISTORY_MAGIC.len()
            + event.as_slice().len()
            + optional_bin_len(id.as_ref())
            + optional_bin_len(flow_type.as_ref())
            + optional_bin_len(state.as_ref())
            + optional_bin_len(lease_owner.as_ref())
            + optional_bin_len(payload_ref.as_ref())
            + optional_bin_len(parent_flow_id.as_ref())
            + optional_bin_len(root_flow_id.as_ref())
            + optional_bin_len(correlation_id.as_ref())
            + optional_bin_len(result_ref.as_ref())
            + optional_bin_len(error_ref.as_ref())
            + optional_bin_len(rewound_to_event_id.as_ref())
            + meta_encoded.as_slice().len()
            + 72,
    );

    out.extend_from_slice(FLOW_HISTORY_MAGIC);
    encode_bin(&mut out, Some(event.as_slice()));
    encode_int(&mut out, version);
    encode_int(&mut out, now_ms);
    encode_bin(&mut out, optional_bin_slice(id.as_ref()));
    encode_bin(&mut out, optional_bin_slice(flow_type.as_ref()));
    encode_bin(&mut out, optional_bin_slice(state.as_ref()));
    encode_int(&mut out, priority);
    encode_int(&mut out, attempts);
    encode_int(&mut out, fencing_token);
    encode_int(&mut out, created_at_ms);
    encode_int(&mut out, updated_at_ms);
    encode_int(&mut out, next_run_at_ms);
    encode_int(&mut out, lease_deadline_ms);
    encode_bin(&mut out, optional_bin_slice(lease_owner.as_ref()));
    encode_bin(&mut out, optional_bin_slice(payload_ref.as_ref()));
    encode_bin(&mut out, optional_bin_slice(parent_flow_id.as_ref()));
    encode_bin(&mut out, optional_bin_slice(root_flow_id.as_ref()));
    encode_bin(&mut out, optional_bin_slice(correlation_id.as_ref()));
    encode_bin(&mut out, optional_bin_slice(result_ref.as_ref()));
    encode_bin(&mut out, optional_bin_slice(error_ref.as_ref()));
    encode_bin(&mut out, optional_bin_slice(rewound_to_event_id.as_ref()));
    out.extend_from_slice(meta_encoded.as_slice());

    binary_term(env, &out)
}

#[rustler::nif(schedule = "Normal")]
pub fn flow_history_decode<'a>(env: Env<'a>, value: Binary<'a>) -> NifResult<Term<'a>> {
    let input = value.as_slice();
    if !input.starts_with(FLOW_HISTORY_MAGIC) {
        return Ok(crate::atoms::error().encode(env));
    }

    let rest = &input[FLOW_HISTORY_MAGIC.len()..];
    let Some((event, rest)) = decode_required_bin(rest) else {
        return Ok(crate::atoms::error().encode(env));
    };
    let Some((version, rest)) = decode_int(rest) else {
        return Ok(crate::atoms::error().encode(env));
    };
    let Some((at, rest)) = decode_int(rest) else {
        return Ok(crate::atoms::error().encode(env));
    };
    let Some((id, rest)) = decode_bin(rest) else {
        return Ok(crate::atoms::error().encode(env));
    };
    let Some((flow_type, rest)) = decode_bin(rest) else {
        return Ok(crate::atoms::error().encode(env));
    };
    let Some((state, rest)) = decode_bin(rest) else {
        return Ok(crate::atoms::error().encode(env));
    };
    let Some((priority, rest)) = decode_int(rest) else {
        return Ok(crate::atoms::error().encode(env));
    };
    let Some((attempts, rest)) = decode_int(rest) else {
        return Ok(crate::atoms::error().encode(env));
    };
    let Some((fencing_token, rest)) = decode_int(rest) else {
        return Ok(crate::atoms::error().encode(env));
    };
    let Some((created_at_ms, rest)) = decode_int(rest) else {
        return Ok(crate::atoms::error().encode(env));
    };
    let Some((updated_at_ms, rest)) = decode_int(rest) else {
        return Ok(crate::atoms::error().encode(env));
    };
    let Some((next_run_at_ms, rest)) = decode_int(rest) else {
        return Ok(crate::atoms::error().encode(env));
    };
    let Some((lease_deadline_ms, rest)) = decode_int(rest) else {
        return Ok(crate::atoms::error().encode(env));
    };
    let Some((lease_owner, rest)) = decode_bin(rest) else {
        return Ok(crate::atoms::error().encode(env));
    };
    let Some((payload_ref, rest)) = decode_bin(rest) else {
        return Ok(crate::atoms::error().encode(env));
    };
    let Some((parent_flow_id, rest)) = decode_bin(rest) else {
        return Ok(crate::atoms::error().encode(env));
    };
    let Some((root_flow_id, rest)) = decode_bin(rest) else {
        return Ok(crate::atoms::error().encode(env));
    };
    let Some((correlation_id, rest)) = decode_bin(rest) else {
        return Ok(crate::atoms::error().encode(env));
    };
    let Some((result_ref, rest)) = decode_bin(rest) else {
        return Ok(crate::atoms::error().encode(env));
    };
    let Some((error_ref, rest)) = decode_bin(rest) else {
        return Ok(crate::atoms::error().encode(env));
    };
    let Some((rewound_to_event_id, rest)) = decode_bin(rest) else {
        return Ok(crate::atoms::error().encode(env));
    };
    let Some((meta_count, mut rest)) = decode_int(rest) else {
        return Ok(crate::atoms::error().encode(env));
    };
    let Some(meta_count) = meta_count else {
        return Ok(crate::atoms::error().encode(env));
    };

    let mut fields =
        Vec::with_capacity(42usize.saturating_add(usize::try_from(meta_count).unwrap_or(0) * 2));
    push_history_binary_field(env, &mut fields, b"event", event)?;
    push_history_int_field(env, &mut fields, b"version", version, false)?;
    push_history_int_field(env, &mut fields, b"at", at, false)?;
    push_history_binary_field(env, &mut fields, b"id", id.unwrap_or(&[]))?;
    push_history_binary_field(env, &mut fields, b"type", flow_type.unwrap_or(&[]))?;
    push_history_binary_field(env, &mut fields, b"state", state.unwrap_or(&[]))?;
    push_history_int_field(env, &mut fields, b"priority", priority, false)?;
    push_history_int_field(env, &mut fields, b"attempts", attempts, false)?;
    push_history_int_field(env, &mut fields, b"fencing_token", fencing_token, false)?;
    push_history_int_field(env, &mut fields, b"created_at_ms", created_at_ms, false)?;
    push_history_int_field(env, &mut fields, b"updated_at_ms", updated_at_ms, false)?;
    push_history_int_field(env, &mut fields, b"next_run_at_ms", next_run_at_ms, true)?;
    push_history_int_field(
        env,
        &mut fields,
        b"lease_deadline_ms",
        lease_deadline_ms,
        true,
    )?;
    push_history_binary_field(env, &mut fields, b"lease_owner", lease_owner.unwrap_or(&[]))?;
    push_history_binary_field(env, &mut fields, b"payload_ref", payload_ref.unwrap_or(&[]))?;
    push_history_binary_field(
        env,
        &mut fields,
        b"parent_flow_id",
        parent_flow_id.unwrap_or(&[]),
    )?;
    push_history_binary_field(env, &mut fields, b"root_flow_id", root_flow_id.unwrap_or(&[]))?;
    push_history_binary_field(
        env,
        &mut fields,
        b"correlation_id",
        correlation_id.unwrap_or(&[]),
    )?;
    push_history_binary_field(env, &mut fields, b"result_ref", result_ref.unwrap_or(&[]))?;
    push_history_binary_field(env, &mut fields, b"error_ref", error_ref.unwrap_or(&[]))?;
    push_history_binary_field(
        env,
        &mut fields,
        b"rewound_to_event_id",
        rewound_to_event_id.unwrap_or(&[]),
    )?;

    for _ in 0..meta_count {
        let Some((key, next)) = decode_required_bin(rest) else {
            return Ok(crate::atoms::error().encode(env));
        };
        let Some((value, next)) = decode_required_bin(next) else {
            return Ok(crate::atoms::error().encode(env));
        };
        push_history_binary_pair(env, &mut fields, key, value)?;
        rest = next;
    }

    if !rest.is_empty() {
        return Ok(crate::atoms::error().encode(env));
    }

    Ok((crate::atoms::ok(), fields).encode(env))
}

impl FlowOrderedIndex {
    fn put(&mut self, key: &[u8], member: &[u8], score: f64, new_only: bool) {
        let lookup_key = (key.to_vec(), member.to_vec());

        if let Some(old_score) = self.lookup.get(&lookup_key).copied() {
            if new_only {
                return;
            }

            self.ordered.remove(&OrderedEntry {
                key: key.to_vec(),
                score: Score(old_score),
                member: member.to_vec(),
            });
        } else {
            self.increment_count(key, 1);
        }

        self.ordered.insert(OrderedEntry {
            key: key.to_vec(),
            score: Score(score),
            member: member.to_vec(),
        });
        self.lookup.insert(lookup_key, score);
    }

    fn delete(&mut self, key: &[u8], member: &[u8]) -> Option<f64> {
        let lookup_key = (key.to_vec(), member.to_vec());
        let old_score = self.lookup.remove(&lookup_key)?;

        self.ordered.remove(&OrderedEntry {
            key: key.to_vec(),
            score: Score(old_score),
            member: member.to_vec(),
        });
        self.increment_count(key, -1);
        Some(old_score)
    }

    fn put_new_without_count(&mut self, key: &[u8], member: &[u8], score: f64) -> bool {
        let lookup_key = (key.to_vec(), member.to_vec());

        if self.lookup.contains_key(&lookup_key) {
            return false;
        }

        self.ordered.insert(OrderedEntry {
            key: key.to_vec(),
            score: Score(score),
            member: member.to_vec(),
        });
        self.lookup.insert(lookup_key, score);
        true
    }

    fn delete_without_count(&mut self, key: &[u8], member: &[u8]) -> bool {
        let lookup_key = (key.to_vec(), member.to_vec());
        let Some(old_score) = self.lookup.remove(&lookup_key) else {
            return false;
        };

        self.ordered.remove(&OrderedEntry {
            key: key.to_vec(),
            score: Score(old_score),
            member: member.to_vec(),
        });
        true
    }

    fn move_member(&mut self, from_key: &[u8], to_key: &[u8], member: &[u8], score: f64) {
        if from_key == to_key {
            if let Some(old_score) = self.lookup.get(&(from_key.to_vec(), member.to_vec())).copied()
            {
                self.ordered.remove(&OrderedEntry {
                    key: from_key.to_vec(),
                    score: Score(old_score),
                    member: member.to_vec(),
                });
            }

            self.put(to_key, member, score, false);
            return;
        }

        let _source_exists = self.delete(from_key, member).is_some();
        let _destination_exists = self.delete(to_key, member).is_some();

        self.ordered.insert(OrderedEntry {
            key: to_key.to_vec(),
            score: Score(score),
            member: member.to_vec(),
        });
        self.lookup.insert((to_key.to_vec(), member.to_vec()), score);

        self.increment_count(to_key, 1);
    }

    fn increment_count(&mut self, key: &[u8], delta: i64) {
        if delta == 0 {
            return;
        }

        let next = self.counts.get(key).copied().unwrap_or(0) + delta;
        self.counts.insert(key.to_vec(), next);
    }

    fn apply_count_deltas(&mut self, count_deltas: HashMap<Vec<u8>, i64>) {
        for (key, delta) in count_deltas {
            if delta == 0 {
                continue;
            }

            let next = self.counts.get(key.as_slice()).copied().unwrap_or(0) + delta;
            self.counts.insert(key, next);
        }
    }

    fn range_slice(
        &self,
        key: &[u8],
        min: Bound,
        max: Bound,
        reverse: bool,
        offset: usize,
        count: isize,
    ) -> Vec<(&[u8], f64)> {
        if !min.can_match_min() || !max.can_match_max() || count == 0 {
            return Vec::new();
        }

        let mut rows = Vec::new();
        let mut skipped = 0usize;
        let unlimited = count < 0;
        let limit = if unlimited { usize::MAX } else { count as usize };

        if reverse {
            for entry in self.ordered.iter().rev() {
                if entry.key.as_slice() != key {
                    continue;
                }

                if !max.matches_upper(entry.score.0) || !min.matches_lower(entry.score.0) {
                    continue;
                }

                if skipped < offset {
                    skipped += 1;
                    continue;
                }

                if rows.len() >= limit {
                    break;
                }

                rows.push((entry.member.as_slice(), entry.score.0));
            }
        } else {
            for entry in &self.ordered {
                if entry.key.as_slice() != key {
                    continue;
                }

                if !min.matches_lower(entry.score.0) || !max.matches_upper(entry.score.0) {
                    continue;
                }

                if skipped < offset {
                    skipped += 1;
                    continue;
                }

                if rows.len() >= limit {
                    break;
                }

                rows.push((entry.member.as_slice(), entry.score.0));
            }
        }

        rows
    }

    fn take_due(&mut self, key: &[u8], max_score: f64, count: usize) -> Vec<(Vec<u8>, f64)> {
        if count == 0 {
            return Vec::new();
        }

        let mut rows = Vec::with_capacity(count);
        let lower = OrderedEntry {
            key: key.to_vec(),
            score: Score(f64::NEG_INFINITY),
            member: Vec::new(),
        };

        for entry in self.ordered.range(lower..) {
            if entry.key.as_slice() != key {
                break;
            }

            if entry.score.0 > max_score {
                break;
            }

            rows.push((entry.member.clone(), entry.score.0));

            if rows.len() >= count {
                break;
            }
        }

        for (member, _score) in &rows {
            self.delete(key, member);
        }

        rows
    }

    fn claim_due_candidates(
        &self,
        keys: &[&[u8]],
        max_score: f64,
        limit: usize,
        max_scan: usize,
    ) -> Vec<(Vec<u8>, Vec<u8>, f64)> {
        if keys.is_empty() || limit == 0 || max_scan == 0 {
            return Vec::new();
        }

        let mut rows = Vec::with_capacity(limit.min(max_scan));
        let mut scanned = 0usize;

        for key in keys {
            let lower = OrderedEntry {
                key: key.to_vec(),
                score: Score(f64::NEG_INFINITY),
                member: Vec::new(),
            };

            for entry in self.ordered.range(lower..) {
                if entry.key.as_slice() != *key {
                    break;
                }

                if entry.score.0 > max_score {
                    break;
                }

                scanned += 1;
                rows.push((entry.key.clone(), entry.member.clone(), entry.score.0));

                if rows.len() >= limit || scanned >= max_scan {
                    return rows;
                }
            }
        }

        rows
    }
}

#[derive(Clone, Copy)]
enum Bound {
    NegInf,
    PosInf,
    Inclusive(f64),
    Exclusive(f64),
    Impossible,
}

impl Bound {
    fn from_min(kind: u8, score: f64) -> Self {
        match kind {
            0 => Self::NegInf,
            1 => Self::Inclusive(score),
            2 => Self::Exclusive(score),
            _ => Self::Impossible,
        }
    }

    fn from_max(kind: u8, score: f64) -> Self {
        match kind {
            0 => Self::PosInf,
            1 => Self::Inclusive(score),
            2 => Self::Exclusive(score),
            _ => Self::Impossible,
        }
    }

    fn can_match_min(self) -> bool {
        !matches!(self, Self::PosInf | Self::Impossible)
    }

    fn can_match_max(self) -> bool {
        !matches!(self, Self::NegInf | Self::Impossible)
    }

    fn matches_lower(self, score: f64) -> bool {
        match self {
            Self::NegInf => true,
            Self::Inclusive(bound) => score >= bound,
            Self::Exclusive(bound) => score > bound,
            Self::PosInf | Self::Impossible => false,
        }
    }

    fn matches_upper(self, score: f64) -> bool {
        match self {
            Self::PosInf => true,
            Self::Inclusive(bound) => score <= bound,
            Self::Exclusive(bound) => score < bound,
            Self::NegInf | Self::Impossible => false,
        }
    }
}

fn due_key(key: &[u8]) -> bool {
    key.starts_with(b"f:{f")
        && (key.windows(4).any(|window| window == b"}:d:")
            || key.windows(5).any(|window| window == b"}:da:"))
}

fn add_count_delta(count_deltas: &mut HashMap<Vec<u8>, i64>, key: &[u8], delta: i64) {
    if delta == 0 {
        return;
    }

    if let Some(current) = count_deltas.get_mut(key) {
        *current += delta;
    } else {
        count_deltas.insert(key.to_vec(), delta);
    }
}

fn decode_flow_record(value: &[u8]) -> Option<FlowRecordParts<'_>> {
    let mut input = value.strip_prefix(FLOW_RECORD_MAGIC)?;

    let (id, rest) = decode_required_bin(input)?;
    input = rest;
    let (flow_type, rest) = decode_required_bin(input)?;
    input = rest;
    let (state, rest) = decode_required_bin(input)?;
    input = rest;
    let (version, rest) = decode_int(input)?;
    input = rest;
    let (attempts, rest) = decode_int(input)?;
    input = rest;
    let (fencing_token, rest) = decode_int(input)?;
    input = rest;
    let (created_at_ms, rest) = decode_int(input)?;
    input = rest;
    let (updated_at_ms, rest) = decode_int(input)?;
    input = rest;
    let (next_run_at_ms, rest) = decode_int(input)?;
    input = rest;
    let (priority, rest) = decode_int(input)?;
    input = rest;
    let (ttl_ms, rest) = decode_int(input)?;
    input = rest;
    let (history_hot_max_events, rest) = decode_int(input)?;
    input = rest;
    let (history_max_events, rest) = decode_int(input)?;
    input = rest;
    let (retention_ttl_ms, rest) = decode_int(input)?;
    input = rest;
    let (terminal_retention_until_ms, rest) = decode_int(input)?;
    input = rest;
    let (partition_key, rest) = decode_bin(input)?;
    input = rest;
    let (payload_ref, rest) = decode_bin(input)?;
    input = rest;
    let (parent_flow_id, rest) = decode_bin(input)?;
    input = rest;
    let (parent_partition_key, rest) = decode_bin(input)?;
    input = rest;
    let (root_flow_id, rest) = decode_bin(input)?;
    input = rest;
    let (correlation_id, rest) = decode_bin(input)?;
    input = rest;
    let (result_ref, rest) = decode_bin(input)?;
    input = rest;
    let (error_ref, rest) = decode_bin(input)?;
    input = rest;
    let (lease_owner, rest) = decode_bin(input)?;
    input = rest;
    let (lease_token, rest) = decode_bin(input)?;
    input = rest;
    let (lease_deadline_ms, rest) = decode_int(input)?;
    input = rest;
    let (run_state, rest) = decode_bin(input)?;
    input = rest;
    let (rewound_to_event_id, rest) = decode_bin(input)?;
    input = rest;
    let (child_groups_encoded, rest) = decode_encoded_bin_field(input)?;

    if !rest.is_empty() {
        return None;
    }

    Some(FlowRecordParts {
        id,
        flow_type,
        state,
        version,
        attempts,
        fencing_token,
        created_at_ms,
        updated_at_ms,
        next_run_at_ms,
        priority,
        ttl_ms,
        history_hot_max_events,
        history_max_events,
        retention_ttl_ms,
        terminal_retention_until_ms,
        partition_key,
        payload_ref,
        parent_flow_id,
        parent_partition_key,
        root_flow_id,
        correlation_id,
        result_ref,
        error_ref,
        lease_owner,
        lease_token,
        lease_deadline_ms,
        run_state,
        rewound_to_event_id,
        child_groups_encoded,
    })
}

fn decode_varint(input: &[u8]) -> Option<(u64, &[u8])> {
    let mut result = 0u64;
    let mut shift = 0u32;
    let mut rest = input;

    for _ in 0..10 {
        let (&byte, next) = rest.split_first()?;
        rest = next;
        result |= u64::from(byte & 0x7f) << shift;

        if byte & 0x80 == 0 {
            return Some((result, rest));
        }

        shift += 7;
    }

    None
}

fn decode_int(input: &[u8]) -> Option<(Option<u64>, &[u8])> {
    let (encoded, rest) = decode_varint(input)?;
    if encoded == 0 {
        Some((None, rest))
    } else {
        Some((Some(encoded - 1), rest))
    }
}

fn decode_required_bin(input: &[u8]) -> Option<(&[u8], &[u8])> {
    let (value, rest) = decode_bin(input)?;
    Some((value?, rest))
}

fn decode_bin(input: &[u8]) -> Option<(Option<&[u8]>, &[u8])> {
    let (encoded_len, rest) = decode_varint(input)?;

    if encoded_len == 0 {
        return Some((None, rest));
    }

    let len = usize::try_from(encoded_len - 1).ok()?;
    if rest.len() < len {
        return None;
    }

    let (value, remaining) = rest.split_at(len);
    Some((Some(value), remaining))
}

fn decode_encoded_bin_field(input: &[u8]) -> Option<(&[u8], &[u8])> {
    let before_len = input.len();
    let (encoded_len, rest) = decode_varint(input)?;

    if encoded_len == 0 {
        let consumed = before_len - rest.len();
        return Some((&input[..consumed], rest));
    }

    let len = usize::try_from(encoded_len - 1).ok()?;
    if rest.len() < len {
        return None;
    }

    let remaining = &rest[len..];
    let consumed = before_len - remaining.len();
    Some((&input[..consumed], remaining))
}

fn encoded_child_groups_empty(encoded: &[u8]) -> bool {
    match decode_bin(encoded) {
        Some((None, rest)) => rest.is_empty(),
        Some((Some(value), rest)) => rest.is_empty() && value == b"J{}",
        None => false,
    }
}

fn encode_claimed_record(
    record: &FlowRecordParts<'_>,
    worker: &[u8],
    lease_token: &[u8],
    deadline_ms: u64,
    now_ms: u64,
    next_version: u64,
    next_fencing_token: u64,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(estimate_claimed_record_size(record, worker, lease_token));
    out.extend_from_slice(FLOW_RECORD_MAGIC);
    encode_bin(&mut out, Some(record.id));
    encode_bin(&mut out, Some(record.flow_type));
    encode_bin(&mut out, Some(RUNNING_STATE));
    encode_int(&mut out, Some(next_version));
    encode_int(&mut out, record.attempts);
    encode_int(&mut out, Some(next_fencing_token));
    encode_int(&mut out, record.created_at_ms);
    encode_int(&mut out, Some(now_ms));
    encode_int(&mut out, Some(deadline_ms));
    encode_int(&mut out, record.priority);
    encode_int(&mut out, None);
    encode_int(&mut out, record.history_hot_max_events);
    encode_int(&mut out, record.history_max_events);
    encode_int(&mut out, record.retention_ttl_ms);
    encode_int(&mut out, None);
    encode_bin(&mut out, record.partition_key);
    encode_bin(&mut out, record.payload_ref);
    encode_bin(&mut out, record.parent_flow_id);
    encode_bin(&mut out, record.parent_partition_key);
    encode_bin(&mut out, record.root_flow_id);
    encode_bin(&mut out, record.correlation_id);
    encode_bin(&mut out, record.result_ref);
    encode_bin(&mut out, record.error_ref);
    encode_bin(&mut out, Some(worker));
    encode_bin(&mut out, Some(lease_token));
    encode_int(&mut out, Some(deadline_ms));
    encode_bin(&mut out, Some(flow_claim_run_state(record)));
    encode_bin(&mut out, record.rewound_to_event_id);
    out.extend_from_slice(record.child_groups_encoded);
    out
}

fn encode_varint(out: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        out.push((value as u8) | 0x80);
        value >>= 7;
    }

    out.push(value as u8);
}

fn encode_int(out: &mut Vec<u8>, value: Option<u64>) {
    match value {
        Some(value) => encode_varint(out, value.saturating_add(1)),
        None => out.push(0),
    }
}

fn encode_bin(out: &mut Vec<u8>, value: Option<&[u8]>) {
    match value {
        Some(value) => {
            encode_varint(out, value.len().saturating_add(1) as u64);
            out.extend_from_slice(value);
        }
        None => out.push(0),
    }
}

fn optional_bin_slice<'a>(value: Option<&Binary<'a>>) -> Option<&'a [u8]> {
    value.map(Binary::as_slice)
}

fn optional_bin_len(value: Option<&Binary<'_>>) -> usize {
    value.map_or(0, |bin| bin.as_slice().len())
}

fn option_binary_term<'a>(env: Env<'a>, value: Option<&[u8]>) -> NifResult<Term<'a>> {
    match value {
        Some(value) => binary_term(env, value),
        None => Ok(crate::atoms::nil().encode(env)),
    }
}

fn option_u64_term<'a>(env: Env<'a>, value: Option<u64>) -> Term<'a> {
    match value {
        Some(value) => value.encode(env),
        None => crate::atoms::nil().encode(env),
    }
}

fn estimate_claimed_record_size(
    record: &FlowRecordParts<'_>,
    worker: &[u8],
    lease_token: &[u8],
) -> usize {
    FLOW_RECORD_MAGIC.len()
        + record.id.len()
        + record.flow_type.len()
        + RUNNING_STATE.len()
        + record.partition_key.map_or(0, <[u8]>::len)
        + record.payload_ref.map_or(0, <[u8]>::len)
        + record.parent_flow_id.map_or(0, <[u8]>::len)
        + record.parent_partition_key.map_or(0, <[u8]>::len)
        + record.root_flow_id.map_or(0, <[u8]>::len)
        + record.correlation_id.map_or(0, <[u8]>::len)
        + record.result_ref.map_or(0, <[u8]>::len)
        + record.error_ref.map_or(0, <[u8]>::len)
        + worker.len()
        + lease_token.len()
        + flow_claim_run_state(record).len()
        + record.rewound_to_event_id.map_or(0, <[u8]>::len)
        + record.child_groups_encoded.len()
        + 96
}

fn claim_lease_token(worker: &[u8], now_ms: u64, fencing_token: u64) -> Vec<u8> {
    let mut token = Vec::with_capacity(worker.len() + 2 + 20 + 20);
    token.extend_from_slice(worker);
    token.push(b':');
    token.extend_from_slice(now_ms.to_string().as_bytes());
    token.push(b':');
    token.extend_from_slice(fencing_token.to_string().as_bytes());
    token
}

fn flow_claim_run_state<'a>(record: &'a FlowRecordParts<'a>) -> &'a [u8] {
    if record.state == RUNNING_STATE {
        DEFAULT_RUNNING_RUN_STATE
    } else {
        record.state
    }
}

fn flow_record_fast_claim_shape(record: &FlowRecordParts<'_>) -> bool {
    blank(record.parent_flow_id)
        && blank(record.correlation_id)
        && match record.root_flow_id {
            None => true,
            Some(root) => root.is_empty() || root == record.id,
        }
}

fn blank(value: Option<&[u8]>) -> bool {
    value.map_or(true, <[u8]>::is_empty)
}

fn encode_member_scores<'a>(env: Env<'a>, rows: Vec<(&[u8], f64)>) -> NifResult<Term<'a>> {
    let mut terms = Vec::with_capacity(rows.len());

    for (member, score) in rows {
        terms.push((binary_term(env, member)?, score).encode(env));
    }

    Ok(terms.encode(env))
}

fn encode_owned_member_scores<'a>(env: Env<'a>, rows: Vec<(Vec<u8>, f64)>) -> NifResult<Term<'a>> {
    let mut terms = Vec::with_capacity(rows.len());

    for (member, score) in rows {
        terms.push((binary_term(env, &member)?, score).encode(env));
    }

    Ok(terms.encode(env))
}

fn encode_owned_key_member_scores<'a>(
    env: Env<'a>,
    rows: Vec<(Vec<u8>, Vec<u8>, f64)>,
) -> NifResult<Term<'a>> {
    let mut terms = Vec::with_capacity(rows.len());

    for (key, member, score) in rows {
        terms.push((binary_term(env, &key)?, binary_term(env, &member)?, score).encode(env));
    }

    Ok(terms.encode(env))
}

fn encode_binaries<'a>(env: Env<'a>, values: Vec<&[u8]>) -> NifResult<Term<'a>> {
    let mut terms = Vec::with_capacity(values.len());

    for value in values {
        terms.push(binary_term(env, value)?);
    }

    Ok(terms.encode(env))
}

fn push_history_binary_field<'a>(
    env: Env<'a>,
    fields: &mut Vec<Term<'a>>,
    key: &[u8],
    value: &[u8],
) -> NifResult<()> {
    fields.push(binary_term(env, key)?);
    fields.push(binary_term(env, value)?);
    Ok(())
}

fn push_history_binary_pair<'a>(
    env: Env<'a>,
    fields: &mut Vec<Term<'a>>,
    key: &[u8],
    value: &[u8],
) -> NifResult<()> {
    fields.push(binary_term(env, key)?);
    fields.push(binary_term(env, value)?);
    Ok(())
}

fn push_history_int_field<'a>(
    env: Env<'a>,
    fields: &mut Vec<Term<'a>>,
    key: &[u8],
    value: Option<u64>,
    optional: bool,
) -> NifResult<()> {
    fields.push(binary_term(env, key)?);

    match value {
        Some(value) => {
            let rendered = value.to_string();
            fields.push(binary_term(env, rendered.as_bytes())?);
        }
        None if optional => fields.push(binary_term(env, b"")?),
        None => fields.push(binary_term(env, b"0")?),
    }

    Ok(())
}

fn binary_term<'a>(env: Env<'a>, value: &[u8]) -> NifResult<Term<'a>> {
    Ok(owned_binary(env, value)?.encode(env))
}

fn owned_binary<'a>(env: Env<'a>, value: &[u8]) -> NifResult<Binary<'a>> {
    let mut binary = OwnedBinary::new(value.len())
        .ok_or_else(|| rustler::Error::Term(Box::new("flow index binary allocation failed")))?;
    binary.as_mut_slice().copy_from_slice(value);
    Ok(Binary::from_owned(binary, env))
}
