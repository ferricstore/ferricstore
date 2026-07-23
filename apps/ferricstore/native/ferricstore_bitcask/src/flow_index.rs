use rustler::{Binary, Decoder, Encoder, Env, NifResult, OwnedBinary, ResourceArc, Term};
use std::cmp::Ordering;
use std::collections::{BTreeSet, BinaryHeap, HashMap};
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard, TryLockError};

const MAX_FLOW_INDEX_REQUEST_ITEMS: usize = 100_000;
const MAX_FLOW_INDEX_REQUEST_BYTES: usize = 64 * 1024 * 1024;
const MAX_FLOW_INDEX_PAGE_ITEMS: usize = 4_096;
const FLOW_INDEX_REQUEST_TOO_LARGE: &str = "flow index native request exceeds safety budget";

struct FlowIndexRequestBudget {
    items: usize,
    bytes: usize,
    valid: bool,
}

impl FlowIndexRequestBudget {
    fn new() -> Self {
        Self {
            items: 0,
            bytes: 0,
            valid: true,
        }
    }

    fn add_items(&mut self, count: usize) {
        let Some(items) = self.items.checked_add(count) else {
            self.valid = false;
            return;
        };
        self.items = items;
        if self.items > MAX_FLOW_INDEX_REQUEST_ITEMS {
            self.valid = false;
        }
    }

    fn add_bytes(&mut self, bytes: usize) {
        let Some(total) = self.bytes.checked_add(bytes) else {
            self.valid = false;
            return;
        };
        self.bytes = total;
        if self.bytes > MAX_FLOW_INDEX_REQUEST_BYTES {
            self.valid = false;
        }
    }

    fn add_item<'a>(&mut self, binaries: impl IntoIterator<Item = &'a [u8]>) {
        self.add_items(1);
        for binary in binaries {
            self.add_bytes(binary.len());
        }
    }

    fn within_limits(&self) -> bool {
        self.valid
    }
}

#[cfg(test)]
fn flow_index_request_within_budget(parts: impl IntoIterator<Item = (usize, usize)>) -> bool {
    let mut budget = FlowIndexRequestBudget::new();
    for (items, bytes) in parts {
        budget.add_items(items);
        budget.add_bytes(bytes);
    }
    budget.within_limits()
}

fn flow_index_page_limit_within_bounds(limit: usize) -> bool {
    limit <= MAX_FLOW_INDEX_PAGE_ITEMS
}

fn preflight_list_lengths(
    weighted_terms: &[(Term<'_>, usize)],
    max_items: usize,
) -> NifResult<Option<Vec<usize>>> {
    let mut total = 0usize;
    let mut lengths = Vec::with_capacity(weighted_terms.len());

    for (term, weight) in weighted_terms {
        let length = term.list_length()?;
        let Some(weighted_length) = length.checked_mul(*weight) else {
            return Ok(None);
        };
        let Some(next_total) = total.checked_add(weighted_length) else {
            return Ok(None);
        };
        if next_total > max_items {
            return Ok(None);
        }

        total = next_total;
        lengths.push(length);
    }

    Ok(Some(lengths))
}

fn preflight_each_list_length(
    terms: &[Term<'_>],
    max_items: usize,
) -> NifResult<Option<Vec<usize>>> {
    let mut lengths = Vec::with_capacity(terms.len());

    for term in terms {
        let length = term.list_length()?;
        if length > max_items {
            return Ok(None);
        }
        lengths.push(length);
    }

    Ok(Some(lengths))
}

fn decode_list_with_length<'a, T: Decoder<'a>>(term: Term<'a>, length: usize) -> NifResult<Vec<T>> {
    let mut values = Vec::with_capacity(length);
    for item in term.into_list_iterator()? {
        values.push(item.decode::<T>()?);
    }
    Ok(values)
}

fn decode_bounded_list<'a, T: Decoder<'a>>(
    term: Term<'a>,
    max_items: usize,
) -> NifResult<Option<Vec<T>>> {
    let length = term.list_length()?;
    if length > max_items {
        return Ok(None);
    }

    Ok(Some(decode_list_with_length(term, length)?))
}

macro_rules! enforce_flow_index_request_budget {
    ($env:expr, $budget:expr) => {
        if !$budget.within_limits() {
            return Ok((crate::atoms::error(), FLOW_INDEX_REQUEST_TOO_LARGE).encode($env));
        }
    };
}

#[derive(Clone, Copy, Debug)]
struct Score(f64);

impl Score {
    fn canonical(self) -> f64 {
        if self.0 == 0.0 {
            0.0
        } else {
            self.0
        }
    }
}

impl PartialEq for Score {
    fn eq(&self, other: &Self) -> bool {
        self.canonical().total_cmp(&other.canonical()) == Ordering::Equal
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
        self.canonical().total_cmp(&other.canonical())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct OrderedEntry {
    key: Vec<u8>,
    score: Score,
    member: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct DueCandidate {
    key_index: usize,
    entry: OrderedEntry,
}

impl Ord for DueCandidate {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .entry
            .score
            .cmp(&self.entry.score)
            .then_with(|| other.entry.member.cmp(&self.entry.member))
            .then_with(|| other.entry.key.cmp(&self.entry.key))
            .then_with(|| other.key_index.cmp(&self.key_index))
    }
}

impl PartialOrd for DueCandidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Default)]
struct FlowOrderedIndex {
    ordered: BTreeSet<OrderedEntry>,
    lookup: HashMap<Vec<u8>, HashMap<Vec<u8>, f64>>,
    counts: HashMap<Arc<[u8]>, i64>,
    positive_count_keys: BTreeSet<Arc<[u8]>>,
    positive_due_count_keys: BTreeSet<Arc<[u8]>>,
}

pub struct FlowOrderedIndexResource {
    inner: RwLock<FlowOrderedIndex>,
}

enum IndexLockError {
    Busy,
    Poisoned,
}

fn try_read_index(
    resource: &FlowOrderedIndexResource,
) -> Result<RwLockReadGuard<'_, FlowOrderedIndex>, IndexLockError> {
    resource.inner.try_read().map_err(|error| match error {
        TryLockError::WouldBlock => IndexLockError::Busy,
        TryLockError::Poisoned(_) => IndexLockError::Poisoned,
    })
}

fn try_write_index(
    resource: &FlowOrderedIndexResource,
) -> Result<RwLockWriteGuard<'_, FlowOrderedIndex>, IndexLockError> {
    resource.inner.try_write().map_err(|error| match error {
        TryLockError::WouldBlock => IndexLockError::Busy,
        TryLockError::Poisoned(_) => IndexLockError::Poisoned,
    })
}

fn read_index(
    resource: &FlowOrderedIndexResource,
) -> Result<RwLockReadGuard<'_, FlowOrderedIndex>, IndexLockError> {
    resource.inner.read().map_err(|_| IndexLockError::Poisoned)
}

fn write_index(
    resource: &FlowOrderedIndexResource,
) -> Result<RwLockWriteGuard<'_, FlowOrderedIndex>, IndexLockError> {
    resource.inner.write().map_err(|_| IndexLockError::Poisoned)
}

macro_rules! try_index_guard {
    ($env:expr, $guard:expr) => {
        match $guard {
            Ok(index) => index,
            Err(IndexLockError::Busy) => return Ok(crate::atoms::busy().encode($env)),
            Err(IndexLockError::Poisoned) => {
                return Ok((crate::atoms::error(), "flow index lock poisoned").encode($env));
            }
        }
    };
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

#[derive(rustler::NifTuple)]
struct ClaimHistoryEntry<'a>(
    Term<'a>,
    Term<'a>,
    u64,
    u64,
    Term<'a>,
    Term<'a>,
    Term<'a>,
    Term<'a>,
    bool,
);

fn add_claim_entry_budget(budget: &mut FlowIndexRequestBudget, entry: &ClaimEntry<'_>) {
    let ClaimEntry(
        id,
        from_due_key,
        _from_due_score,
        to_due_key,
        _to_due_score,
        from_state_key,
        _from_state_score,
        to_state_key,
        _to_state_score,
        inflight_key,
        worker_key,
        _lease_score,
    ) = entry;

    budget.add_items(6);
    for _operation in 0..6 {
        budget.add_bytes(id.as_slice().len());
    }
    for key in [
        from_due_key.as_slice(),
        to_due_key.as_slice(),
        from_state_key.as_slice(),
        to_state_key.as_slice(),
        inflight_key.as_slice(),
        worker_key.as_slice(),
    ] {
        budget.add_bytes(key.len());
    }
}

fn flow_record_plan_request_budget(
    candidates: &[(Binary<'_>, f64)],
    values: &[Option<Binary<'_>>],
    fixed_binaries: &[&[u8]],
) -> FlowIndexRequestBudget {
    let mut budget = FlowIndexRequestBudget::new();
    budget.add_items(candidates.len().max(values.len()));

    for (id, _score) in candidates {
        budget.add_bytes(id.as_slice().len());
    }
    for value in values.iter().flatten() {
        budget.add_bytes(value.as_slice().len());
    }
    for value in fixed_binaries {
        budget.add_bytes(value.len());
    }

    budget
}

const FLOW_RECORD_MAGIC: &[u8; 4] = b"FSF5";
const FLOW_HISTORY_MAGIC: &[u8; 4] = b"FSH2";
const RUNNING_STATE: &[u8] = b"running";
const DEFAULT_RUNNING_RUN_STATE: &[u8] = b"queued";
const EMPTY_CHILD_GROUPS_ENCODED: &[u8; 4] = b"\x04J{}";
const CLAIMED_EVENT: &[u8] = b"claimed";
const EMPTY_HISTORY_META_ENCODED: &[u8; 1] = b"\x01";

// FSF5/FSH2 are durable Flow metadata formats. Keep flag numbers and wire order
// in lockstep with Ferricstore.Flow; changing field order/type after release
// requires a new magic and migration decoder.
const RECORD_FLAG_ATTEMPTS: u64 = 1 << 0;
const RECORD_FLAG_FENCING_TOKEN: u64 = 1 << 1;
const RECORD_FLAG_NEXT_RUN_AT_MS: u64 = 1 << 2;
const RECORD_FLAG_PRIORITY: u64 = 1 << 3;
const RECORD_FLAG_TTL_MS: u64 = 1 << 4;
const RECORD_FLAG_HISTORY_HOT_MAX_EVENTS: u64 = 1 << 5;
const RECORD_FLAG_HISTORY_MAX_EVENTS: u64 = 1 << 6;
const RECORD_FLAG_RETENTION_TTL_MS: u64 = 1 << 7;
const RECORD_FLAG_TERMINAL_RETENTION_UNTIL_MS: u64 = 1 << 8;
const RECORD_FLAG_PARTITION_KEY: u64 = 1 << 9;
const RECORD_FLAG_PAYLOAD_REF: u64 = 1 << 10;
const RECORD_FLAG_PARENT_FLOW_ID: u64 = 1 << 11;
const RECORD_FLAG_PARENT_PARTITION_KEY: u64 = 1 << 12;
const RECORD_FLAG_ROOT_FLOW_ID: u64 = 1 << 13;
const RECORD_FLAG_ROOT_FLOW_ID_SELF: u64 = 1 << 14;
const RECORD_FLAG_CORRELATION_ID: u64 = 1 << 15;
const RECORD_FLAG_RESULT_REF: u64 = 1 << 16;
const RECORD_FLAG_ERROR_REF: u64 = 1 << 17;
const RECORD_FLAG_LEASE_OWNER: u64 = 1 << 18;
const RECORD_FLAG_LEASE_TOKEN: u64 = 1 << 19;
const RECORD_FLAG_LEASE_DEADLINE_MS: u64 = 1 << 20;
const RECORD_FLAG_RUN_STATE: u64 = 1 << 21;
const RECORD_FLAG_REWOUND_TO_EVENT_ID: u64 = 1 << 22;
const RECORD_FLAG_SIDECAR: u64 = 1 << 23;
const RECORD_FLAG_MAX_ACTIVE_MS: u64 = 1 << 24;
const RECORD_KNOWN_FLAGS: u64 = (1 << 25) - 1;
const MAX_EXACT_INTEGER: u64 = (1 << 53) - 1;

const HISTORY_FLAG_PRIORITY: u64 = 1 << 0;
const HISTORY_FLAG_ATTEMPTS: u64 = 1 << 1;
const HISTORY_FLAG_FENCING_TOKEN: u64 = 1 << 2;
const HISTORY_FLAG_CREATED_AT_MS: u64 = 1 << 3;
const HISTORY_FLAG_UPDATED_AT_MS: u64 = 1 << 4;
const HISTORY_FLAG_NEXT_RUN_AT_MS: u64 = 1 << 5;
const HISTORY_FLAG_LEASE_DEADLINE_MS: u64 = 1 << 6;
const HISTORY_FLAG_LEASE_OWNER: u64 = 1 << 7;
const HISTORY_FLAG_PAYLOAD_REF: u64 = 1 << 8;
const HISTORY_FLAG_RESULT_REF: u64 = 1 << 9;
const HISTORY_FLAG_ERROR_REF: u64 = 1 << 10;
const HISTORY_FLAG_REWOUND_TO_EVENT_ID: u64 = 1 << 11;
const HISTORY_FLAG_META: u64 = 1 << 12;
const HISTORY_KNOWN_FLAGS: u64 = (1 << 13) - 1;

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
    max_active_ms: Option<u64>,
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
        inner: RwLock::new(FlowOrderedIndex::default()),
    })
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn flow_index_put_entries<'a>(
    env: Env<'a>,
    resource: ResourceArc<FlowOrderedIndexResource>,
    entries_term: Term<'a>,
) -> NifResult<Term<'a>> {
    let Some(entries) = decode_bounded_list::<(Binary<'a>, Binary<'a>, f64)>(
        entries_term,
        MAX_FLOW_INDEX_REQUEST_ITEMS,
    )?
    else {
        return Ok((crate::atoms::error(), FLOW_INDEX_REQUEST_TOO_LARGE).encode(env));
    };

    let mut budget = FlowIndexRequestBudget::new();
    for (key, member, _score) in &entries {
        budget.add_item([key.as_slice(), member.as_slice()]);
    }
    enforce_flow_index_request_budget!(env, budget);

    let mut index = try_index_guard!(env, try_write_index(&resource));

    for (key_bin, member_bin, score) in entries {
        index.put(key_bin.as_slice(), member_bin.as_slice(), score, false);
    }

    Ok(crate::atoms::ok().encode(env))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn flow_index_put_new_entries<'a>(
    env: Env<'a>,
    resource: ResourceArc<FlowOrderedIndexResource>,
    entries_term: Term<'a>,
) -> NifResult<Term<'a>> {
    let Some(entries) = decode_bounded_list::<(Binary<'a>, Binary<'a>, f64)>(
        entries_term,
        MAX_FLOW_INDEX_REQUEST_ITEMS,
    )?
    else {
        return Ok((crate::atoms::error(), FLOW_INDEX_REQUEST_TOO_LARGE).encode(env));
    };

    let mut budget = FlowIndexRequestBudget::new();
    for (key, member, _score) in &entries {
        budget.add_item([key.as_slice(), member.as_slice()]);
    }
    enforce_flow_index_request_budget!(env, budget);

    let mut index = try_index_guard!(env, try_write_index(&resource));

    for (key_bin, member_bin, score) in entries {
        index.put(key_bin.as_slice(), member_bin.as_slice(), score, true);
    }

    Ok(crate::atoms::ok().encode(env))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn flow_index_move_entries<'a>(
    env: Env<'a>,
    resource: ResourceArc<FlowOrderedIndexResource>,
    entries_term: Term<'a>,
) -> NifResult<Term<'a>> {
    let Some(entries) = decode_bounded_list::<(Binary<'a>, Binary<'a>, Binary<'a>, f64)>(
        entries_term,
        MAX_FLOW_INDEX_REQUEST_ITEMS,
    )?
    else {
        return Ok((crate::atoms::error(), FLOW_INDEX_REQUEST_TOO_LARGE).encode(env));
    };

    let mut budget = FlowIndexRequestBudget::new();
    for (from_key, to_key, member, _score) in &entries {
        budget.add_item([from_key.as_slice(), to_key.as_slice(), member.as_slice()]);
    }
    enforce_flow_index_request_budget!(env, budget);

    let mut index = try_index_guard!(env, try_write_index(&resource));

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

#[rustler::nif(schedule = "DirtyCpu")]
pub fn flow_index_delete_members<'a>(
    env: Env<'a>,
    resource: ResourceArc<FlowOrderedIndexResource>,
    key: Binary<'a>,
    members_term: Term<'a>,
) -> NifResult<Term<'a>> {
    let Some(members) =
        decode_bounded_list::<Binary<'a>>(members_term, MAX_FLOW_INDEX_REQUEST_ITEMS)?
    else {
        return Ok((crate::atoms::error(), FLOW_INDEX_REQUEST_TOO_LARGE).encode(env));
    };

    let mut budget = FlowIndexRequestBudget::new();
    budget.add_bytes(key.as_slice().len());
    for member in &members {
        budget.add_item([member.as_slice()]);
    }
    enforce_flow_index_request_budget!(env, budget);

    let mut index = try_index_guard!(env, try_write_index(&resource));

    for member in members {
        index.delete(key.as_slice(), member.as_slice());
    }

    Ok(crate::atoms::ok().encode(env))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn flow_index_delete_entries<'a>(
    env: Env<'a>,
    resource: ResourceArc<FlowOrderedIndexResource>,
    entries_term: Term<'a>,
) -> NifResult<Term<'a>> {
    let Some(entries) = decode_bounded_list::<(Binary<'a>, Binary<'a>)>(
        entries_term,
        MAX_FLOW_INDEX_REQUEST_ITEMS,
    )?
    else {
        return Ok((crate::atoms::error(), FLOW_INDEX_REQUEST_TOO_LARGE).encode(env));
    };

    let mut budget = FlowIndexRequestBudget::new();
    for (key, member) in &entries {
        budget.add_item([key.as_slice(), member.as_slice()]);
    }
    enforce_flow_index_request_budget!(env, budget);

    let mut index = try_index_guard!(env, try_write_index(&resource));

    for (key, member) in entries {
        index.delete(key.as_slice(), member.as_slice());
    }

    Ok(crate::atoms::ok().encode(env))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn flow_index_apply_batch<'a>(
    env: Env<'a>,
    resource: ResourceArc<FlowOrderedIndexResource>,
    put_entries_term: Term<'a>,
    put_new_entries_term: Term<'a>,
    move_entries_term: Term<'a>,
    delete_entries_term: Term<'a>,
    claim_entries_term: Term<'a>,
) -> NifResult<Term<'a>> {
    let Some(lengths) = preflight_list_lengths(
        &[
            (put_entries_term, 1),
            (put_new_entries_term, 1),
            (move_entries_term, 1),
            (delete_entries_term, 1),
            (claim_entries_term, 6),
        ],
        MAX_FLOW_INDEX_REQUEST_ITEMS,
    )?
    else {
        return Ok((crate::atoms::error(), FLOW_INDEX_REQUEST_TOO_LARGE).encode(env));
    };

    let put_entries =
        decode_list_with_length::<(Binary<'a>, Binary<'a>, f64)>(put_entries_term, lengths[0])?;
    let put_new_entries =
        decode_list_with_length::<(Binary<'a>, Binary<'a>, f64)>(put_new_entries_term, lengths[1])?;
    let move_entries = decode_list_with_length::<(Binary<'a>, Binary<'a>, Binary<'a>, f64)>(
        move_entries_term,
        lengths[2],
    )?;
    let delete_entries =
        decode_list_with_length::<(Binary<'a>, Binary<'a>)>(delete_entries_term, lengths[3])?;
    let claim_entries = decode_list_with_length::<ClaimEntry<'a>>(claim_entries_term, lengths[4])?;

    let mut budget = FlowIndexRequestBudget::new();
    for (key, member, _score) in &put_entries {
        budget.add_item([key.as_slice(), member.as_slice()]);
    }
    for (key, member, _score) in &put_new_entries {
        budget.add_item([key.as_slice(), member.as_slice()]);
    }
    for (from_key, to_key, member, _score) in &move_entries {
        budget.add_item([from_key.as_slice(), to_key.as_slice(), member.as_slice()]);
    }
    for (key, member) in &delete_entries {
        budget.add_item([key.as_slice(), member.as_slice()]);
    }
    for entry in &claim_entries {
        add_claim_entry_budget(&mut budget, entry);
    }
    enforce_flow_index_request_budget!(env, budget);

    let mut index = try_index_guard!(env, write_index(&resource));

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

    for (key_bin, member) in delete_entries {
        index.delete(key_bin.as_slice(), member.as_slice());
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
    let index = try_index_guard!(env, try_read_index(&resource));

    match index.lookup_score(key.as_slice(), member.as_slice()) {
        Some(score) => Ok((crate::atoms::ok(), *score).encode(env)),
        None => Ok(crate::atoms::miss().encode(env)),
    }
}

#[rustler::nif(schedule = "DirtyCpu")]
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
    count: usize,
) -> NifResult<Term<'a>> {
    if !flow_index_page_limit_within_bounds(count) {
        return Ok((crate::atoms::error(), FLOW_INDEX_REQUEST_TOO_LARGE).encode(env));
    }

    let rows = {
        let index = try_index_guard!(env, read_index(&resource));
        index.range_slice(
            key.as_slice(),
            Bound::from_min(min_kind, min_score),
            Bound::from_max(max_kind, max_score),
            reverse,
            offset,
            count,
        )
    };

    encode_owned_member_scores(env, rows)
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn flow_index_range_cursor_slice<'a>(
    env: Env<'a>,
    resource: ResourceArc<FlowOrderedIndexResource>,
    key: Binary<'a>,
    min_kind: u8,
    min_score: f64,
    max_kind: u8,
    max_score: f64,
    cursor_score: f64,
    cursor_member: Binary<'a>,
    offset: usize,
    count: usize,
) -> NifResult<Term<'a>> {
    if !flow_index_page_limit_within_bounds(count) {
        return Ok((crate::atoms::error(), FLOW_INDEX_REQUEST_TOO_LARGE).encode(env));
    }

    let rows = {
        let index = try_index_guard!(env, read_index(&resource));
        index.range_reverse_before(
            key.as_slice(),
            Bound::from_min(min_kind, min_score),
            Bound::from_max(max_kind, max_score),
            cursor_score,
            cursor_member.as_slice(),
            offset,
            count,
        )
    };

    encode_owned_member_scores(env, rows)
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn flow_index_range_after_slice<'a>(
    env: Env<'a>,
    resource: ResourceArc<FlowOrderedIndexResource>,
    key: Binary<'a>,
    min_kind: u8,
    min_score: f64,
    max_kind: u8,
    max_score: f64,
    cursor_score: f64,
    cursor_member: Binary<'a>,
    offset: usize,
    count: usize,
) -> NifResult<Term<'a>> {
    if !flow_index_page_limit_within_bounds(count) {
        return Ok((crate::atoms::error(), FLOW_INDEX_REQUEST_TOO_LARGE).encode(env));
    }

    let rows = {
        let index = try_index_guard!(env, read_index(&resource));
        index.range_forward_after(
            key.as_slice(),
            Bound::from_min(min_kind, min_score),
            Bound::from_max(max_kind, max_score),
            cursor_score,
            cursor_member.as_slice(),
            offset,
            count,
        )
    };

    encode_owned_member_scores(env, rows)
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn flow_index_take_due<'a>(
    env: Env<'a>,
    resource: ResourceArc<FlowOrderedIndexResource>,
    key: Binary<'a>,
    max_score: f64,
    count: usize,
) -> NifResult<Term<'a>> {
    if count > MAX_FLOW_INDEX_REQUEST_ITEMS {
        return Ok((crate::atoms::error(), FLOW_INDEX_REQUEST_TOO_LARGE).encode(env));
    }

    let mut index = try_index_guard!(env, write_index(&resource));
    let rows = index.take_due(key.as_slice(), max_score, count);

    encode_owned_member_scores(env, rows)
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn flow_index_claim_due_candidates<'a>(
    env: Env<'a>,
    resource: ResourceArc<FlowOrderedIndexResource>,
    keys_term: Term<'a>,
    max_score: f64,
    limit: usize,
    max_scan: usize,
) -> NifResult<Term<'a>> {
    let Some(keys) = decode_bounded_list::<Binary<'a>>(keys_term, MAX_FLOW_INDEX_REQUEST_ITEMS)?
    else {
        return Ok((crate::atoms::error(), FLOW_INDEX_REQUEST_TOO_LARGE).encode(env));
    };

    let mut budget = FlowIndexRequestBudget::new();
    for key in &keys {
        budget.add_item([key.as_slice()]);
    }
    if limit > MAX_FLOW_INDEX_REQUEST_ITEMS || max_scan > MAX_FLOW_INDEX_REQUEST_ITEMS {
        budget.valid = false;
    }
    enforce_flow_index_request_budget!(env, budget);

    let rows = {
        let index = try_index_guard!(env, read_index(&resource));
        let key_refs = keys.iter().map(|key| key.as_slice()).collect::<Vec<_>>();
        index.claim_due_candidates(&key_refs, max_score, limit, max_scan)
    };

    encode_owned_key_member_score_runs(env, ordered_due_candidate_runs(rows))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn flow_index_fifo_lane_heads<'a>(
    env: Env<'a>,
    resource: ResourceArc<FlowOrderedIndexResource>,
    due_key: Binary<'a>,
    lane_keys_term: Term<'a>,
) -> NifResult<Term<'a>> {
    let Some(lane_keys) =
        decode_bounded_list::<Binary<'a>>(lane_keys_term, MAX_FLOW_INDEX_REQUEST_ITEMS)?
    else {
        return Ok((crate::atoms::error(), FLOW_INDEX_REQUEST_TOO_LARGE).encode(env));
    };

    let mut budget = FlowIndexRequestBudget::new();
    budget.add_item([due_key.as_slice()]);
    for lane_key in &lane_keys {
        budget.add_item([lane_key.as_slice()]);
    }
    enforce_flow_index_request_budget!(env, budget);

    let rows = {
        let index = try_index_guard!(env, read_index(&resource));
        let lane_key_refs = lane_keys
            .iter()
            .map(|lane_key| lane_key.as_slice())
            .collect::<Vec<_>>();
        index.fifo_lane_heads(due_key.as_slice(), &lane_key_refs)
    };

    encode_owned_fifo_lane_heads(env, rows)
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn flow_index_fifo_lane_heads_many<'a>(
    env: Env<'a>,
    resource: ResourceArc<FlowOrderedIndexResource>,
    due_lane_keys_term: Term<'a>,
) -> NifResult<Term<'a>> {
    let Some(due_lane_keys) = decode_bounded_list::<(Binary<'a>, Binary<'a>)>(
        due_lane_keys_term,
        MAX_FLOW_INDEX_REQUEST_ITEMS,
    )?
    else {
        return Ok((crate::atoms::error(), FLOW_INDEX_REQUEST_TOO_LARGE).encode(env));
    };

    let mut budget = FlowIndexRequestBudget::new();
    for (due_key, lane_key) in &due_lane_keys {
        budget.add_item([due_key.as_slice(), lane_key.as_slice()]);
    }
    enforce_flow_index_request_budget!(env, budget);

    let rows = {
        let index = try_index_guard!(env, read_index(&resource));
        let key_refs = due_lane_keys
            .iter()
            .map(|(due_key, lane_key)| (due_key.as_slice(), lane_key.as_slice()))
            .collect::<Vec<_>>();
        index.fifo_lane_heads_many(&key_refs)
    };

    encode_owned_fifo_lane_heads_many(env, rows)
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn flow_index_due_keys_present<'a>(
    env: Env<'a>,
    resource: ResourceArc<FlowOrderedIndexResource>,
    keys_term: Term<'a>,
    max_score: f64,
) -> NifResult<Term<'a>> {
    let Some(keys) = decode_bounded_list::<Binary<'a>>(keys_term, MAX_FLOW_INDEX_REQUEST_ITEMS)?
    else {
        return Ok((crate::atoms::error(), FLOW_INDEX_REQUEST_TOO_LARGE).encode(env));
    };

    let mut budget = FlowIndexRequestBudget::new();
    for key in &keys {
        budget.add_item([key.as_slice()]);
    }
    enforce_flow_index_request_budget!(env, budget);

    let rows = {
        let index = try_index_guard!(env, try_read_index(&resource));
        let key_refs = keys.iter().map(|key| key.as_slice()).collect::<Vec<_>>();
        index.due_keys_present(&key_refs, max_score)
    };

    encode_owned_binaries(env, rows)
}

#[rustler::nif(schedule = "Normal")]
pub fn flow_index_count_all<'a>(
    env: Env<'a>,
    resource: ResourceArc<FlowOrderedIndexResource>,
    key: Binary<'a>,
) -> NifResult<Term<'a>> {
    let index = try_index_guard!(env, try_read_index(&resource));
    let count = index
        .counts
        .get(key.as_slice())
        .copied()
        .unwrap_or(0)
        .max(0);

    Ok(count.encode(env))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn flow_index_count_many<'a>(
    env: Env<'a>,
    resource: ResourceArc<FlowOrderedIndexResource>,
    keys_term: Term<'a>,
) -> NifResult<Term<'a>> {
    let Some(keys) = decode_bounded_list::<Binary<'a>>(keys_term, MAX_FLOW_INDEX_REQUEST_ITEMS)?
    else {
        return Ok((crate::atoms::error(), FLOW_INDEX_REQUEST_TOO_LARGE).encode(env));
    };

    let mut budget = FlowIndexRequestBudget::new();
    for key in &keys {
        budget.add_item([key.as_slice()]);
    }
    enforce_flow_index_request_budget!(env, budget);

    let index = try_index_guard!(env, try_read_index(&resource));

    let counts = keys
        .into_iter()
        .map(|key| {
            index
                .counts
                .get(key.as_slice())
                .copied()
                .unwrap_or(0)
                .max(0)
        })
        .collect::<Vec<_>>();

    Ok(counts.encode(env))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn flow_index_count_keys_page<'a>(
    env: Env<'a>,
    resource: ResourceArc<FlowOrderedIndexResource>,
    cursor: Option<Binary<'a>>,
    limit: usize,
) -> NifResult<Term<'a>> {
    if !flow_index_page_limit_within_bounds(limit) {
        return Ok((crate::atoms::error(), FLOW_INDEX_REQUEST_TOO_LARGE).encode(env));
    }

    let keys = {
        let index = try_index_guard!(env, read_index(&resource));
        index.count_keys_page(false, cursor.as_ref().map(Binary::as_slice), limit)
    };

    encode_owned_binaries(env, keys)
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn flow_index_due_count_keys_page<'a>(
    env: Env<'a>,
    resource: ResourceArc<FlowOrderedIndexResource>,
    cursor: Option<Binary<'a>>,
    limit: usize,
) -> NifResult<Term<'a>> {
    if !flow_index_page_limit_within_bounds(limit) {
        return Ok((crate::atoms::error(), FLOW_INDEX_REQUEST_TOO_LARGE).encode(env));
    }

    let keys = {
        let index = try_index_guard!(env, read_index(&resource));
        index.count_keys_page(true, cursor.as_ref().map(Binary::as_slice), limit)
    };

    encode_owned_binaries(env, keys)
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn flow_index_earliest_due_score<'a>(
    env: Env<'a>,
    resource: ResourceArc<FlowOrderedIndexResource>,
    prefixes_term: Term<'a>,
    needles_term: Term<'a>,
    suffixes_term: Term<'a>,
) -> NifResult<Term<'a>> {
    let Some(lengths) = preflight_list_lengths(
        &[(prefixes_term, 1), (needles_term, 1), (suffixes_term, 1)],
        MAX_FLOW_INDEX_REQUEST_ITEMS,
    )?
    else {
        return Ok((crate::atoms::error(), FLOW_INDEX_REQUEST_TOO_LARGE).encode(env));
    };
    let prefixes = decode_list_with_length::<Binary<'a>>(prefixes_term, lengths[0])?;
    let needles = decode_list_with_length::<Binary<'a>>(needles_term, lengths[1])?;
    let suffixes = decode_list_with_length::<Binary<'a>>(suffixes_term, lengths[2])?;

    let mut budget = FlowIndexRequestBudget::new();
    for value in prefixes.iter().chain(&needles).chain(&suffixes) {
        budget.add_item([value.as_slice()]);
    }
    enforce_flow_index_request_budget!(env, budget);

    let index = try_index_guard!(env, read_index(&resource));
    let prefix_refs = prefixes.iter().map(Binary::as_slice).collect::<Vec<_>>();
    let needle_refs = needles.iter().map(Binary::as_slice).collect::<Vec<_>>();
    let suffix_refs = suffixes.iter().map(Binary::as_slice).collect::<Vec<_>>();

    match index.earliest_due_score_matching(&prefix_refs, &needle_refs, &suffix_refs) {
        Some(score) => Ok(score.encode(env)),
        None => Ok(crate::atoms::nil().encode(env)),
    }
}

#[rustler::nif(schedule = "Normal")]
pub fn flow_index_restore_count<'a>(
    env: Env<'a>,
    resource: ResourceArc<FlowOrderedIndexResource>,
    key: Binary<'a>,
    count: i64,
) -> NifResult<Term<'a>> {
    let mut index = try_index_guard!(env, try_write_index(&resource));
    index.set_count(key.as_slice(), count);
    Ok(crate::atoms::ok().encode(env))
}

#[rustler::nif(schedule = "Normal")]
pub fn flow_index_delete_count<'a>(
    env: Env<'a>,
    resource: ResourceArc<FlowOrderedIndexResource>,
    key: Binary<'a>,
) -> NifResult<Term<'a>> {
    let mut index = try_index_guard!(env, try_write_index(&resource));
    index.remove_count(key.as_slice());
    Ok(crate::atoms::ok().encode(env))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn flow_index_apply_claim_entries<'a>(
    env: Env<'a>,
    resource: ResourceArc<FlowOrderedIndexResource>,
    entries_term: Term<'a>,
) -> NifResult<Term<'a>> {
    let Some(entries) =
        decode_bounded_list::<ClaimEntry<'a>>(entries_term, MAX_FLOW_INDEX_REQUEST_ITEMS / 6)?
    else {
        return Ok((crate::atoms::error(), FLOW_INDEX_REQUEST_TOO_LARGE).encode(env));
    };

    let mut budget = FlowIndexRequestBudget::new();
    for entry in &entries {
        add_claim_entry_budget(&mut budget, entry);
    }
    enforce_flow_index_request_budget!(env, budget);

    let mut index = try_index_guard!(env, write_index(&resource));
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

#[rustler::nif(schedule = "DirtyCpu")]
pub fn flow_index_rollback_claim_entries<'a>(
    env: Env<'a>,
    resource: ResourceArc<FlowOrderedIndexResource>,
    entries_term: Term<'a>,
) -> NifResult<Term<'a>> {
    let Some(entries) =
        decode_bounded_list::<ClaimEntry<'a>>(entries_term, MAX_FLOW_INDEX_REQUEST_ITEMS / 6)?
    else {
        return Ok((crate::atoms::error(), FLOW_INDEX_REQUEST_TOO_LARGE).encode(env));
    };

    let mut budget = FlowIndexRequestBudget::new();
    for entry in &entries {
        add_claim_entry_budget(&mut budget, entry);
    }
    enforce_flow_index_request_budget!(env, budget);

    let mut index = try_index_guard!(env, write_index(&resource));
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

#[rustler::nif(schedule = "DirtyCpu")]
pub fn flow_record_plan_claims<'a>(
    env: Env<'a>,
    candidates_term: Term<'a>,
    values_term: Term<'a>,
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
    let Some(lengths) = preflight_each_list_length(
        &[candidates_term, values_term],
        MAX_FLOW_INDEX_REQUEST_ITEMS,
    )?
    else {
        return Ok((crate::atoms::error(), FLOW_INDEX_REQUEST_TOO_LARGE).encode(env));
    };
    let candidates = decode_list_with_length::<(Binary<'a>, f64)>(candidates_term, lengths[0])?;
    let values = decode_list_with_length::<Option<Binary<'a>>>(values_term, lengths[1])?;

    let budget = flow_record_plan_request_budget(
        &candidates,
        &values,
        &[
            flow_type.as_slice(),
            expected_state.as_slice(),
            worker.as_slice(),
            from_due_key.as_slice(),
            to_due_key.as_slice(),
            from_state_key.as_slice(),
            to_state_key.as_slice(),
            inflight_key.as_slice(),
            worker_key.as_slice(),
            state_key_prefix.as_slice(),
        ],
    );
    enforce_flow_index_request_budget!(env, budget);

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

        let Some((next_version, next_fencing_token)) =
            checked_claim_counters(version, fencing_token)
        else {
            return Ok(crate::atoms::fallback().encode(env));
        };
        let lease_token = claim_lease_token(worker, now_ms, next_fencing_token);
        let Some(next_value) = encode_claimed_record(
            &record,
            worker,
            &lease_token,
            deadline_ms,
            now_ms,
            next_version,
            next_fencing_token,
        ) else {
            return Ok(crate::atoms::fallback().encode(env));
        };

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

        plan_terms.push(
            (
                next_value_term,
                entry_term,
                state_key_term,
                previous_history_ms,
            )
                .encode(env),
        );
        accepted += 1;
    }

    Ok((crate::atoms::ok(), plan_terms, stale_terms, accepted).encode(env))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn flow_record_plan_claims_with_history<'a>(
    env: Env<'a>,
    candidates_term: Term<'a>,
    values_term: Term<'a>,
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
    history_key_prefix: Binary<'a>,
) -> NifResult<Term<'a>> {
    let Some(lengths) = preflight_each_list_length(
        &[candidates_term, values_term],
        MAX_FLOW_INDEX_REQUEST_ITEMS,
    )?
    else {
        return Ok((crate::atoms::error(), FLOW_INDEX_REQUEST_TOO_LARGE).encode(env));
    };
    let candidates = decode_list_with_length::<(Binary<'a>, f64)>(candidates_term, lengths[0])?;
    let values = decode_list_with_length::<Option<Binary<'a>>>(values_term, lengths[1])?;

    let budget = flow_record_plan_request_budget(
        &candidates,
        &values,
        &[
            flow_type.as_slice(),
            expected_state.as_slice(),
            worker.as_slice(),
            from_due_key.as_slice(),
            to_due_key.as_slice(),
            from_state_key.as_slice(),
            to_state_key.as_slice(),
            inflight_key.as_slice(),
            worker_key.as_slice(),
            state_key_prefix.as_slice(),
            history_key_prefix.as_slice(),
        ],
    );
    enforce_flow_index_request_budget!(env, budget);

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

        let Some((next_version, next_fencing_token)) =
            checked_claim_counters(version, fencing_token)
        else {
            return Ok(crate::atoms::fallback().encode(env));
        };
        let lease_token = claim_lease_token(worker, now_ms, next_fencing_token);
        let Some(next_value) = encode_claimed_record(
            &record,
            worker,
            &lease_token,
            deadline_ms,
            now_ms,
            next_version,
            next_fencing_token,
        ) else {
            return Ok(crate::atoms::fallback().encode(env));
        };

        let mut state_key = Vec::with_capacity(state_key_prefix.as_slice().len() + id.len());
        state_key.extend_from_slice(state_key_prefix.as_slice());
        state_key.extend_from_slice(id);

        let previous_history_ms = record.updated_at_ms.or(record.created_at_ms);
        let history_event_ms = previous_history_ms.map_or(now_ms, |previous| previous.max(now_ms));
        let event_id = flow_history_event_id(history_event_ms, next_version);

        let mut history_key = Vec::with_capacity(history_key_prefix.as_slice().len() + id.len());
        history_key.extend_from_slice(history_key_prefix.as_slice());
        history_key.extend_from_slice(id);

        let mut history_entry_key = Vec::with_capacity(2 + history_key.len() + 1 + event_id.len());
        history_entry_key.extend_from_slice(b"X:");
        history_entry_key.extend_from_slice(&history_key);
        history_entry_key.push(0);
        history_entry_key.extend_from_slice(&event_id);

        let Some(history_value) = encode_flow_history_compact(
            CLAIMED_EVENT,
            Some(next_version),
            Some(now_ms),
            Some(RUNNING_STATE),
            record.priority,
            record.attempts,
            Some(next_fencing_token),
            record.created_at_ms,
            Some(now_ms),
            Some(deadline_ms),
            Some(deadline_ms),
            Some(worker),
            record.payload_ref,
            record.result_ref,
            record.error_ref,
            record.rewound_to_event_id,
            EMPTY_HISTORY_META_ENCODED,
        ) else {
            return Ok(crate::atoms::fallback().encode(env));
        };

        let next_value_term = binary_term(env, &next_value)?;
        let state_key_term = binary_term(env, &state_key)?;
        let history_key_term = binary_term(env, &history_key)?;
        let event_id_term = binary_term(env, &event_id)?;
        let history_entry_key_term = binary_term(env, &history_entry_key)?;
        let history_value_term = binary_term(env, &history_value)?;
        let deadline_score = deadline_ms as f64;
        let now_score = now_ms as f64;
        let updated_score = updated_at_ms as f64;

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

        let history_entry_term = ClaimHistoryEntry(
            history_key_term,
            event_id_term,
            history_event_ms,
            next_version,
            history_entry_key_term,
            history_value_term,
            option_u64_term(env, record.history_hot_max_events),
            option_u64_term(env, record.history_max_events),
            false,
        )
        .encode(env);

        plan_terms.push(
            (
                next_value_term,
                entry_term,
                state_key_term,
                previous_history_ms,
                history_entry_term,
            )
                .encode(env),
        );
        accepted += 1;
    }

    Ok((crate::atoms::ok(), plan_terms, stale_terms, accepted).encode(env))
}

#[rustler::nif(schedule = "DirtyCpu")]
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
    max_active_ms: Option<u64>,
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
    let Some(out) = encode_flow_record_compact(
        optional_bin_slice(id.as_ref()),
        optional_bin_slice(flow_type.as_ref()),
        optional_bin_slice(state.as_ref()),
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
        max_active_ms,
        optional_bin_slice(partition_key.as_ref()),
        optional_bin_slice(payload_ref.as_ref()),
        optional_bin_slice(parent_flow_id.as_ref()),
        optional_bin_slice(parent_partition_key.as_ref()),
        optional_bin_slice(root_flow_id.as_ref()),
        optional_bin_slice(correlation_id.as_ref()),
        optional_bin_slice(result_ref.as_ref()),
        optional_bin_slice(error_ref.as_ref()),
        optional_bin_slice(lease_owner.as_ref()),
        optional_bin_slice(lease_token.as_ref()),
        lease_deadline_ms,
        optional_bin_slice(run_state.as_ref()),
        optional_bin_slice(rewound_to_event_id.as_ref()),
        child_groups_encoded.as_slice(),
    ) else {
        return Err(rustler::Error::BadArg);
    };

    binary_term(env, &out)
}

#[allow(clippy::too_many_arguments)]
fn encode_flow_record_compact(
    id: Option<&[u8]>,
    flow_type: Option<&[u8]>,
    state: Option<&[u8]>,
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
    max_active_ms: Option<u64>,
    partition_key: Option<&[u8]>,
    payload_ref: Option<&[u8]>,
    parent_flow_id: Option<&[u8]>,
    parent_partition_key: Option<&[u8]>,
    root_flow_id: Option<&[u8]>,
    correlation_id: Option<&[u8]>,
    result_ref: Option<&[u8]>,
    error_ref: Option<&[u8]>,
    lease_owner: Option<&[u8]>,
    lease_token: Option<&[u8]>,
    lease_deadline_ms: Option<u64>,
    run_state: Option<&[u8]>,
    rewound_to_event_id: Option<&[u8]>,
    child_groups_encoded: &[u8],
) -> Option<Vec<u8>> {
    if !matches!(id, Some(value) if !value.is_empty())
        || !matches!(flow_type, Some(value) if !value.is_empty())
        || !matches!(state, Some(value) if !value.is_empty())
        || !matches!(version, Some(value) if value <= MAX_EXACT_INTEGER)
        || !matches!(created_at_ms, Some(value) if value <= MAX_EXACT_INTEGER)
        || !matches!(updated_at_ms, Some(value) if value <= MAX_EXACT_INTEGER)
    {
        return None;
    }

    // Required fields stay inline. Nil/default optional fields are skipped by
    // flags so common state records avoid repeated policy/lease metadata.
    let flags = encode_record_flags(
        id,
        attempts,
        fencing_token,
        next_run_at_ms,
        priority,
        ttl_ms,
        history_hot_max_events,
        history_max_events,
        retention_ttl_ms,
        terminal_retention_until_ms,
        max_active_ms,
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
    );

    let mut out = Vec::with_capacity(
        FLOW_RECORD_MAGIC.len()
            + optional_slice_len(id)
            + optional_slice_len(flow_type)
            + optional_slice_len(state)
            + optional_slice_len(partition_key)
            + optional_slice_len(payload_ref)
            + optional_slice_len(parent_flow_id)
            + optional_slice_len(parent_partition_key)
            + optional_slice_len(root_flow_id)
            + optional_slice_len(correlation_id)
            + optional_slice_len(result_ref)
            + optional_slice_len(error_ref)
            + optional_slice_len(lease_owner)
            + optional_slice_len(lease_token)
            + optional_slice_len(run_state)
            + optional_slice_len(rewound_to_event_id)
            + child_groups_encoded.len()
            + 48,
    );

    // Wire order is schema-owned; update Elixir codec/tests with every change.
    out.extend_from_slice(FLOW_RECORD_MAGIC);
    encode_int(&mut out, Some(flags))?;
    encode_bin(&mut out, id);
    encode_bin(&mut out, flow_type);
    encode_bin(&mut out, state);
    encode_int(&mut out, version)?;
    encode_int(&mut out, created_at_ms)?;
    encode_int(&mut out, updated_at_ms)?;
    encode_flagged_int(&mut out, flags, RECORD_FLAG_ATTEMPTS, attempts)?;
    encode_flagged_int(&mut out, flags, RECORD_FLAG_FENCING_TOKEN, fencing_token)?;
    encode_flagged_int(&mut out, flags, RECORD_FLAG_NEXT_RUN_AT_MS, next_run_at_ms)?;
    encode_flagged_int(&mut out, flags, RECORD_FLAG_PRIORITY, priority)?;
    encode_flagged_int(&mut out, flags, RECORD_FLAG_TTL_MS, ttl_ms)?;
    encode_flagged_int(
        &mut out,
        flags,
        RECORD_FLAG_HISTORY_HOT_MAX_EVENTS,
        history_hot_max_events,
    )?;
    encode_flagged_int(
        &mut out,
        flags,
        RECORD_FLAG_HISTORY_MAX_EVENTS,
        history_max_events,
    )?;
    encode_flagged_int(
        &mut out,
        flags,
        RECORD_FLAG_RETENTION_TTL_MS,
        retention_ttl_ms,
    )?;
    encode_flagged_int(
        &mut out,
        flags,
        RECORD_FLAG_TERMINAL_RETENTION_UNTIL_MS,
        terminal_retention_until_ms,
    )?;
    encode_flagged_int(&mut out, flags, RECORD_FLAG_MAX_ACTIVE_MS, max_active_ms)?;
    encode_flagged_bin(&mut out, flags, RECORD_FLAG_PARTITION_KEY, partition_key);
    encode_flagged_bin(&mut out, flags, RECORD_FLAG_PAYLOAD_REF, payload_ref);
    encode_flagged_bin(&mut out, flags, RECORD_FLAG_PARENT_FLOW_ID, parent_flow_id);
    encode_flagged_bin(
        &mut out,
        flags,
        RECORD_FLAG_PARENT_PARTITION_KEY,
        parent_partition_key,
    );
    encode_flagged_bin(&mut out, flags, RECORD_FLAG_ROOT_FLOW_ID, root_flow_id);
    encode_flagged_bin(&mut out, flags, RECORD_FLAG_CORRELATION_ID, correlation_id);
    encode_flagged_bin(&mut out, flags, RECORD_FLAG_RESULT_REF, result_ref);
    encode_flagged_bin(&mut out, flags, RECORD_FLAG_ERROR_REF, error_ref);
    encode_flagged_bin(&mut out, flags, RECORD_FLAG_LEASE_OWNER, lease_owner);
    encode_flagged_bin(&mut out, flags, RECORD_FLAG_LEASE_TOKEN, lease_token);
    encode_flagged_int(
        &mut out,
        flags,
        RECORD_FLAG_LEASE_DEADLINE_MS,
        lease_deadline_ms,
    )?;
    encode_flagged_bin(&mut out, flags, RECORD_FLAG_RUN_STATE, run_state);
    encode_flagged_bin(
        &mut out,
        flags,
        RECORD_FLAG_REWOUND_TO_EVENT_ID,
        rewound_to_event_id,
    );

    if flags & RECORD_FLAG_SIDECAR != 0 {
        out.extend_from_slice(child_groups_encoded);
    }

    Some(out)
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn flow_record_decode<'a>(env: Env<'a>, value: Binary<'a>) -> NifResult<Term<'a>> {
    let Some(record) = decode_flow_record(value.as_slice()) else {
        return Ok(crate::atoms::error().encode(env));
    };

    Ok((crate::atoms::ok(), flow_record_fields(env, &record)?).encode(env))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn flow_records_decode<'a>(env: Env<'a>, values_term: Term<'a>) -> NifResult<Term<'a>> {
    let Some(values) = decode_bounded_list::<Binary<'a>>(values_term, MAX_FLOW_INDEX_PAGE_ITEMS)?
    else {
        return Ok(crate::atoms::error().encode(env));
    };

    let mut total_bytes = 0usize;
    let mut decoded = Vec::with_capacity(values.len());

    for value in &values {
        let Some(next_total) = total_bytes.checked_add(value.len()) else {
            return Ok(crate::atoms::error().encode(env));
        };
        if next_total > MAX_FLOW_INDEX_REQUEST_BYTES {
            return Ok(crate::atoms::error().encode(env));
        }
        total_bytes = next_total;

        let Some(record) = decode_flow_record(value.as_slice()) else {
            return Ok(crate::atoms::error().encode(env));
        };
        decoded.push(flow_record_fields(env, &record)?);
    }

    Ok((crate::atoms::ok(), decoded).encode(env))
}

fn flow_record_fields<'a>(env: Env<'a>, record: &FlowRecordParts<'_>) -> NifResult<Vec<Term<'a>>> {
    Ok(vec![
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
        option_u64_term(env, record.max_active_ms),
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
    ])
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn flow_record_decode_meta<'a>(env: Env<'a>, value: Binary<'a>) -> NifResult<Term<'a>> {
    let Some(record) = decode_flow_record(value.as_slice()) else {
        return Ok(crate::atoms::error().encode(env));
    };

    let fields = vec![
        binary_term(env, record.id)?,
        binary_term(env, record.flow_type)?,
        binary_term(env, record.state)?,
        option_u64_term(env, record.version),
        option_u64_term(env, record.priority),
        option_binary_term(env, record.partition_key)?,
        option_binary_term(env, record.payload_ref)?,
        option_binary_term(env, record.result_ref)?,
        option_binary_term(env, record.error_ref)?,
        option_u64_term(env, record.created_at_ms),
        option_u64_term(env, record.updated_at_ms),
        option_u64_term(env, record.next_run_at_ms),
        option_u64_term(env, record.lease_deadline_ms),
        option_binary_term(env, record.lease_owner)?,
        option_binary_term(env, record.lease_token)?,
        option_u64_term(env, record.fencing_token),
        option_u64_term(env, record.attempts),
        option_binary_term(env, record.run_state)?,
        option_u64_term(env, record.max_active_ms),
        binary_term(env, record.child_groups_encoded)?,
    ];

    Ok((crate::atoms::ok(), fields).encode(env))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn flow_records_terminal_after_noop<'a>(
    env: Env<'a>,
    values_term: Term<'a>,
) -> NifResult<Term<'a>> {
    let Some(values) =
        decode_bounded_list::<Binary<'a>>(values_term, MAX_FLOW_INDEX_REQUEST_ITEMS)?
    else {
        return Ok((crate::atoms::error(), FLOW_INDEX_REQUEST_TOO_LARGE).encode(env));
    };

    let flags = values
        .iter()
        .map(|value| {
            decode_flow_record(value.as_slice())
                .map(|record| {
                    blank(record.parent_flow_id)
                        && encoded_child_groups_empty(record.child_groups_encoded)
                })
                .unwrap_or(false)
        })
        .collect::<Vec<_>>();

    Ok(flags.encode(env))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn flow_history_encode<'a>(
    env: Env<'a>,
    event: Binary<'a>,
    version: Option<u64>,
    now_ms: Option<u64>,
    _id: Option<Binary<'a>>,
    _flow_type: Option<Binary<'a>>,
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
    _parent_flow_id: Option<Binary<'a>>,
    _root_flow_id: Option<Binary<'a>>,
    _correlation_id: Option<Binary<'a>>,
    result_ref: Option<Binary<'a>>,
    error_ref: Option<Binary<'a>>,
    rewound_to_event_id: Option<Binary<'a>>,
    meta_encoded: Binary<'a>,
) -> NifResult<Term<'a>> {
    // History is compact by design: workflow identity fields passed above are
    // omitted and reconstructed from record context on user-facing decode.
    let Some(out) = encode_flow_history_compact(
        event.as_slice(),
        version,
        now_ms,
        optional_bin_slice(state.as_ref()),
        priority,
        attempts,
        fencing_token,
        created_at_ms,
        updated_at_ms,
        next_run_at_ms,
        lease_deadline_ms,
        optional_bin_slice(lease_owner.as_ref()),
        optional_bin_slice(payload_ref.as_ref()),
        optional_bin_slice(result_ref.as_ref()),
        optional_bin_slice(error_ref.as_ref()),
        optional_bin_slice(rewound_to_event_id.as_ref()),
        meta_encoded.as_slice(),
    ) else {
        return Err(rustler::Error::BadArg);
    };

    binary_term(env, &out)
}

#[allow(clippy::too_many_arguments)]
fn encode_flow_history_compact(
    event: &[u8],
    version: Option<u64>,
    now_ms: Option<u64>,
    state: Option<&[u8]>,
    priority: Option<u64>,
    attempts: Option<u64>,
    fencing_token: Option<u64>,
    created_at_ms: Option<u64>,
    updated_at_ms: Option<u64>,
    next_run_at_ms: Option<u64>,
    lease_deadline_ms: Option<u64>,
    lease_owner: Option<&[u8]>,
    payload_ref: Option<&[u8]>,
    result_ref: Option<&[u8]>,
    error_ref: Option<&[u8]>,
    rewound_to_event_id: Option<&[u8]>,
    meta_encoded: &[u8],
) -> Option<Vec<u8>> {
    let flags = encode_history_flags(
        priority,
        attempts,
        fencing_token,
        created_at_ms,
        updated_at_ms,
        now_ms,
        next_run_at_ms,
        lease_deadline_ms,
        lease_owner,
        payload_ref,
        result_ref,
        error_ref,
        rewound_to_event_id,
        meta_encoded,
    );

    let mut out = Vec::with_capacity(
        FLOW_HISTORY_MAGIC.len()
            + event.len()
            + optional_slice_len(state)
            + optional_slice_len(lease_owner)
            + optional_slice_len(payload_ref)
            + optional_slice_len(result_ref)
            + optional_slice_len(error_ref)
            + optional_slice_len(rewound_to_event_id)
            + meta_encoded.len()
            + 40,
    );

    // Keep this field order identical to Flow.encode_history_parts_elixir/23.
    out.extend_from_slice(FLOW_HISTORY_MAGIC);
    encode_int(&mut out, Some(flags))?;
    encode_bin(&mut out, Some(event));
    encode_int(&mut out, version)?;
    encode_int(&mut out, now_ms)?;
    encode_bin(&mut out, state);
    encode_flagged_int(&mut out, flags, HISTORY_FLAG_PRIORITY, priority)?;
    encode_flagged_int(&mut out, flags, HISTORY_FLAG_ATTEMPTS, attempts)?;
    encode_flagged_int(&mut out, flags, HISTORY_FLAG_FENCING_TOKEN, fencing_token)?;
    encode_flagged_int(&mut out, flags, HISTORY_FLAG_CREATED_AT_MS, created_at_ms)?;
    encode_flagged_int(&mut out, flags, HISTORY_FLAG_UPDATED_AT_MS, updated_at_ms)?;
    encode_flagged_int(&mut out, flags, HISTORY_FLAG_NEXT_RUN_AT_MS, next_run_at_ms)?;
    encode_flagged_int(
        &mut out,
        flags,
        HISTORY_FLAG_LEASE_DEADLINE_MS,
        lease_deadline_ms,
    )?;
    encode_flagged_bin(&mut out, flags, HISTORY_FLAG_LEASE_OWNER, lease_owner);
    encode_flagged_bin(&mut out, flags, HISTORY_FLAG_PAYLOAD_REF, payload_ref);
    encode_flagged_bin(&mut out, flags, HISTORY_FLAG_RESULT_REF, result_ref);
    encode_flagged_bin(&mut out, flags, HISTORY_FLAG_ERROR_REF, error_ref);
    encode_flagged_bin(
        &mut out,
        flags,
        HISTORY_FLAG_REWOUND_TO_EVENT_ID,
        rewound_to_event_id,
    );

    if flags & HISTORY_FLAG_META != 0 {
        out.extend_from_slice(meta_encoded);
    }

    Some(out)
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn flow_history_decode<'a>(env: Env<'a>, value: Binary<'a>) -> NifResult<Term<'a>> {
    let input = value.as_slice();
    if !input.starts_with(FLOW_HISTORY_MAGIC) {
        return Ok(crate::atoms::error().encode(env));
    }

    let rest = &input[FLOW_HISTORY_MAGIC.len()..];
    let Some((flags, rest)) = decode_int(rest) else {
        return Ok(crate::atoms::error().encode(env));
    };
    let Some(flags) = flags else {
        return Ok(crate::atoms::error().encode(env));
    };
    if flags & !HISTORY_KNOWN_FLAGS != 0 {
        return Ok(crate::atoms::error().encode(env));
    }
    let Some((event, rest)) = decode_required_bin(rest) else {
        return Ok(crate::atoms::error().encode(env));
    };
    let Some((version, rest)) = decode_int(rest) else {
        return Ok(crate::atoms::error().encode(env));
    };
    let Some((at, rest)) = decode_int(rest) else {
        return Ok(crate::atoms::error().encode(env));
    };
    let Some((state, rest)) = decode_bin(rest) else {
        return Ok(crate::atoms::error().encode(env));
    };
    let Some((priority, rest)) = decode_flagged_int(rest, flags, HISTORY_FLAG_PRIORITY, Some(0))
    else {
        return Ok(crate::atoms::error().encode(env));
    };
    let Some((attempts, rest)) = decode_flagged_int(rest, flags, HISTORY_FLAG_ATTEMPTS, Some(0))
    else {
        return Ok(crate::atoms::error().encode(env));
    };
    let Some((fencing_token, rest)) =
        decode_flagged_int(rest, flags, HISTORY_FLAG_FENCING_TOKEN, Some(0))
    else {
        return Ok(crate::atoms::error().encode(env));
    };
    let Some((created_at_ms, rest)) =
        decode_flagged_int(rest, flags, HISTORY_FLAG_CREATED_AT_MS, at)
    else {
        return Ok(crate::atoms::error().encode(env));
    };
    let Some((updated_at_ms, rest)) =
        decode_flagged_int(rest, flags, HISTORY_FLAG_UPDATED_AT_MS, at)
    else {
        return Ok(crate::atoms::error().encode(env));
    };
    let Some((next_run_at_ms, rest)) =
        decode_flagged_int(rest, flags, HISTORY_FLAG_NEXT_RUN_AT_MS, None)
    else {
        return Ok(crate::atoms::error().encode(env));
    };
    let Some((lease_deadline_ms, rest)) =
        decode_flagged_int(rest, flags, HISTORY_FLAG_LEASE_DEADLINE_MS, None)
    else {
        return Ok(crate::atoms::error().encode(env));
    };
    let Some((lease_owner, rest)) = decode_flagged_bin(rest, flags, HISTORY_FLAG_LEASE_OWNER)
    else {
        return Ok(crate::atoms::error().encode(env));
    };
    let Some((payload_ref, rest)) = decode_flagged_bin(rest, flags, HISTORY_FLAG_PAYLOAD_REF)
    else {
        return Ok(crate::atoms::error().encode(env));
    };
    let parent_flow_id: Option<&[u8]> = None;
    let root_flow_id: Option<&[u8]> = None;
    let correlation_id: Option<&[u8]> = None;
    let Some((result_ref, rest)) = decode_flagged_bin(rest, flags, HISTORY_FLAG_RESULT_REF) else {
        return Ok(crate::atoms::error().encode(env));
    };
    let Some((error_ref, rest)) = decode_flagged_bin(rest, flags, HISTORY_FLAG_ERROR_REF) else {
        return Ok(crate::atoms::error().encode(env));
    };
    let Some((rewound_to_event_id, rest)) =
        decode_flagged_bin(rest, flags, HISTORY_FLAG_REWOUND_TO_EVENT_ID)
    else {
        return Ok(crate::atoms::error().encode(env));
    };
    let (meta_count, mut rest) = if flags & HISTORY_FLAG_META != 0 {
        let Some((meta_count, rest)) = decode_int(rest) else {
            return Ok(crate::atoms::error().encode(env));
        };
        let Some(meta_count) = meta_count else {
            return Ok(crate::atoms::error().encode(env));
        };
        (meta_count, rest)
    } else {
        (0, rest)
    };

    let Some(field_capacity) = checked_history_field_capacity(meta_count, rest.len()) else {
        return Ok(crate::atoms::error().encode(env));
    };
    let mut fields = Vec::with_capacity(field_capacity);
    push_history_binary_field(env, &mut fields, b"event", event)?;
    push_history_int_field(env, &mut fields, b"version", version, false)?;
    push_history_int_field(env, &mut fields, b"at", at, false)?;
    push_history_binary_field(env, &mut fields, b"id", b"")?;
    push_history_binary_field(env, &mut fields, b"type", b"")?;
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
    push_history_binary_field(
        env,
        &mut fields,
        b"root_flow_id",
        root_flow_id.unwrap_or(&[]),
    )?;
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

fn checked_history_field_capacity(meta_count: u64, remaining_bytes: usize) -> Option<usize> {
    let meta_count = usize::try_from(meta_count).ok()?;
    // Each required key/value pair consumes at least two one-byte length
    // prefixes, even when both binaries are empty.
    if meta_count > remaining_bytes / 2 {
        return None;
    }
    42usize.checked_add(meta_count.checked_mul(2)?)
}

impl FlowOrderedIndex {
    fn lookup_score(&self, key: &[u8], member: &[u8]) -> Option<&f64> {
        self.lookup.get(key)?.get(member)
    }

    fn insert_lookup(&mut self, key: &[u8], member: &[u8], score: f64) -> Option<f64> {
        if let Some(members) = self.lookup.get_mut(key) {
            return members.insert(member.to_vec(), score);
        }

        self.lookup
            .insert(key.to_vec(), HashMap::from([(member.to_vec(), score)]));
        None
    }

    fn remove_lookup(&mut self, key: &[u8], member: &[u8]) -> Option<f64> {
        let (old_score, remove_key) = {
            let members = self.lookup.get_mut(key)?;
            let old_score = members.remove(member)?;
            (old_score, members.is_empty())
        };

        if remove_key {
            self.lookup.remove(key);
        }

        Some(old_score)
    }

    fn put(&mut self, key: &[u8], member: &[u8], score: f64, new_only: bool) {
        if let Some(old_score) = self.lookup_score(key, member).copied() {
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
        self.insert_lookup(key, member, score);
    }

    fn delete(&mut self, key: &[u8], member: &[u8]) -> Option<f64> {
        let old_score = self.remove_lookup(key, member)?;

        self.ordered.remove(&OrderedEntry {
            key: key.to_vec(),
            score: Score(old_score),
            member: member.to_vec(),
        });
        self.increment_count(key, -1);
        Some(old_score)
    }

    fn put_new_without_count(&mut self, key: &[u8], member: &[u8], score: f64) -> bool {
        if self.lookup_score(key, member).is_some() {
            return false;
        }

        self.ordered.insert(OrderedEntry {
            key: key.to_vec(),
            score: Score(score),
            member: member.to_vec(),
        });
        self.insert_lookup(key, member, score);
        true
    }

    fn delete_without_count(&mut self, key: &[u8], member: &[u8]) -> bool {
        let Some(old_score) = self.remove_lookup(key, member) else {
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
            if let Some(old_score) = self.lookup_score(from_key, member).copied() {
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
        self.insert_lookup(to_key, member, score);

        self.increment_count(to_key, 1);
    }

    fn increment_count(&mut self, key: &[u8], delta: i64) {
        if delta == 0 {
            return;
        }

        let next = self
            .counts
            .get(key)
            .copied()
            .unwrap_or(0)
            .saturating_add(delta);
        self.set_count(key, next);
    }

    fn apply_count_deltas(&mut self, count_deltas: HashMap<Vec<u8>, i64>) {
        for (key, delta) in count_deltas {
            if delta == 0 {
                continue;
            }

            let next = self
                .counts
                .get(key.as_slice())
                .copied()
                .unwrap_or(0)
                .saturating_add(delta);
            self.set_owned_count(key, next);
        }
    }

    fn set_count(&mut self, key: &[u8], count: i64) {
        let shared_key = self
            .counts
            .get_key_value(key)
            .map_or_else(|| Arc::from(key), |(stored, _count)| Arc::clone(stored));
        self.set_shared_count(shared_key, count);
    }

    fn set_owned_count(&mut self, key: Vec<u8>, count: i64) {
        let shared_key = self
            .counts
            .get_key_value(key.as_slice())
            .map_or_else(|| Arc::from(key), |(stored, _count)| Arc::clone(stored));
        self.set_shared_count(shared_key, count);
    }

    fn set_shared_count(&mut self, key: Arc<[u8]>, count: i64) {
        if count > 0 {
            self.positive_count_keys.insert(Arc::clone(&key));
            if due_key(&key) {
                self.positive_due_count_keys.insert(Arc::clone(&key));
            } else {
                self.positive_due_count_keys.remove(key.as_ref());
            }
        } else {
            self.positive_count_keys.remove(key.as_ref());
            self.positive_due_count_keys.remove(key.as_ref());
        }

        self.counts.insert(key, count);
    }

    fn remove_count(&mut self, key: &[u8]) {
        self.counts.remove(key);
        self.positive_count_keys.remove(key);
        self.positive_due_count_keys.remove(key);
    }

    fn count_keys_page(&self, due_only: bool, cursor: Option<&[u8]>, limit: usize) -> Vec<Vec<u8>> {
        if limit == 0 {
            return Vec::new();
        }

        let catalog = if due_only {
            &self.positive_due_count_keys
        } else {
            &self.positive_count_keys
        };
        let mut keys = Vec::with_capacity(limit.min(catalog.len()));

        if let Some(cursor) = cursor {
            for key in catalog
                .range::<[u8], _>((Excluded(cursor), Unbounded))
                .take(limit)
            {
                keys.push(key.to_vec());
            }
        } else {
            for key in catalog.iter().take(limit) {
                keys.push(key.to_vec());
            }
        }

        keys
    }

    fn range_slice(
        &self,
        key: &[u8],
        min: Bound,
        max: Bound,
        reverse: bool,
        offset: usize,
        count: usize,
    ) -> Vec<(Vec<u8>, f64)> {
        if !min.can_match_min() || !max.can_match_max() || count == 0 {
            return Vec::new();
        }

        let mut rows = Vec::with_capacity(count);
        let mut skipped = 0usize;

        let Some((lower, upper)) = exact_key_range_bounds(key, min, max) else {
            return Vec::new();
        };

        if reverse {
            for entry in self.ordered.range((Included(lower), Excluded(upper))).rev() {
                if skipped < offset {
                    skipped += 1;
                    continue;
                }

                if rows.len() >= count {
                    break;
                }

                rows.push((entry.member.clone(), entry.score.0));
            }
        } else {
            for entry in self.ordered.range((Included(lower), Excluded(upper))) {
                if skipped < offset {
                    skipped += 1;
                    continue;
                }

                if rows.len() >= count {
                    break;
                }

                rows.push((entry.member.clone(), entry.score.0));
            }
        }

        rows
    }

    fn range_reverse_before(
        &self,
        key: &[u8],
        min: Bound,
        max: Bound,
        cursor_score: f64,
        cursor_member: &[u8],
        offset: usize,
        count: usize,
    ) -> Vec<(Vec<u8>, f64)> {
        if !min.can_match_min() || !max.can_match_max() || count == 0 {
            return Vec::new();
        }

        let cursor_upper = OrderedEntry {
            key: key.to_vec(),
            score: Score(cursor_score),
            member: cursor_member.to_vec(),
        };
        let Some((lower, score_upper)) = exact_key_range_bounds(key, min, max) else {
            return Vec::new();
        };
        let upper = std::cmp::min(cursor_upper, score_upper);

        if lower >= upper {
            return Vec::new();
        }

        let mut rows = Vec::with_capacity(count);
        let mut skipped = 0usize;

        for entry in self.ordered.range((Included(lower), Excluded(upper))).rev() {
            if skipped < offset {
                skipped += 1;
                continue;
            }

            if rows.len() >= count {
                break;
            }

            rows.push((entry.member.clone(), entry.score.0));
        }

        rows
    }

    fn range_forward_after(
        &self,
        key: &[u8],
        min: Bound,
        max: Bound,
        cursor_score: f64,
        cursor_member: &[u8],
        offset: usize,
        count: usize,
    ) -> Vec<(Vec<u8>, f64)> {
        if !min.can_match_min() || !max.can_match_max() || count == 0 {
            return Vec::new();
        }

        let cursor_lower = OrderedEntry {
            key: key.to_vec(),
            score: Score(cursor_score),
            member: cursor_member.to_vec(),
        };
        let Some((score_lower, upper)) = exact_key_range_bounds(key, min, max) else {
            return Vec::new();
        };
        if cursor_lower >= upper || score_lower >= upper {
            return Vec::new();
        }

        let lower_bound = if cursor_lower < score_lower {
            Included(score_lower)
        } else {
            Excluded(cursor_lower)
        };
        let mut rows = Vec::with_capacity(count);
        let mut skipped = 0usize;

        for entry in self.ordered.range((lower_bound, Excluded(upper))) {
            if skipped < offset {
                skipped += 1;
                continue;
            }

            if rows.len() >= count {
                break;
            }

            rows.push((entry.member.clone(), entry.score.0));
        }

        rows
    }

    fn take_due(&mut self, key: &[u8], max_score: f64, count: usize) -> Vec<(Vec<u8>, f64)> {
        if count == 0 {
            return Vec::new();
        }

        let mut rows = Vec::with_capacity(count.min(self.ordered.len()));
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

        if keys.len() == 1 {
            return self.claim_due_candidates_single_key(keys[0], max_score, limit, max_scan);
        }

        let result_capacity = limit.min(max_scan).min(self.ordered.len());
        let mut rows = Vec::with_capacity(result_capacity);
        let mut scanned = 0usize;
        let mut heap = BinaryHeap::with_capacity(keys.len().min(self.ordered.len()));

        for (key_index, key) in keys.iter().enumerate() {
            if let Some(entry) = self.first_due_entry_for_key(key, max_score) {
                heap.push(DueCandidate { key_index, entry });
            }
        }

        while rows.len() < limit && scanned < max_scan {
            let Some(candidate) = heap.pop() else {
                break;
            };

            scanned += 1;
            rows.push((
                candidate.entry.key.clone(),
                candidate.entry.member.clone(),
                candidate.entry.score.0,
            ));

            if let Some(next) =
                self.next_due_entry_for_key(keys[candidate.key_index], max_score, &candidate.entry)
            {
                heap.push(DueCandidate {
                    key_index: candidate.key_index,
                    entry: next,
                });
            }
        }

        rows
    }

    fn fifo_lane_heads(
        &self,
        due_key: &[u8],
        lane_keys: &[&[u8]],
    ) -> Vec<(Vec<u8>, Vec<u8>, Option<f64>)> {
        let mut rows = Vec::with_capacity(lane_keys.len());

        for lane_key in lane_keys {
            let lower = OrderedEntry {
                key: (*lane_key).to_vec(),
                score: Score(f64::NEG_INFINITY),
                member: Vec::new(),
            };

            let Some(entry) = self.ordered.range(lower..).next() else {
                continue;
            };

            if entry.key.as_slice() != *lane_key || entry.member.len() <= 16 {
                continue;
            }

            let due_score = self.lookup_score(due_key, &entry.member[16..]).copied();
            rows.push(((*lane_key).to_vec(), entry.member.clone(), due_score));
        }

        rows
    }

    fn fifo_lane_heads_many(
        &self,
        due_lane_keys: &[(&[u8], &[u8])],
    ) -> Vec<(Vec<u8>, Vec<u8>, Vec<u8>, Option<f64>)> {
        let mut rows = Vec::with_capacity(due_lane_keys.len());

        for (due_key, lane_key) in due_lane_keys {
            let lower = OrderedEntry {
                key: (*lane_key).to_vec(),
                score: Score(f64::NEG_INFINITY),
                member: Vec::new(),
            };

            let Some(entry) = self.ordered.range(lower..).next() else {
                continue;
            };

            if entry.key.as_slice() != *lane_key || entry.member.len() <= 16 {
                continue;
            }

            let due_score = self.lookup_score(due_key, &entry.member[16..]).copied();
            rows.push((
                (*due_key).to_vec(),
                (*lane_key).to_vec(),
                entry.member.clone(),
                due_score,
            ));
        }

        rows
    }

    fn claim_due_candidates_single_key(
        &self,
        key: &[u8],
        max_score: f64,
        limit: usize,
        max_scan: usize,
    ) -> Vec<(Vec<u8>, Vec<u8>, f64)> {
        let mut rows = Vec::with_capacity(limit.min(max_scan).min(self.ordered.len()));
        let mut scanned = 0usize;
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

            scanned += 1;
            rows.push((entry.key.clone(), entry.member.clone(), entry.score.0));

            if rows.len() >= limit || scanned >= max_scan {
                return rows;
            }
        }

        rows
    }

    fn first_due_entry_for_key(&self, key: &[u8], max_score: f64) -> Option<OrderedEntry> {
        let lower = OrderedEntry {
            key: key.to_vec(),
            score: Score(f64::NEG_INFINITY),
            member: Vec::new(),
        };

        self.next_due_entry_from_range(key, max_score, lower, false)
    }

    fn next_due_entry_for_key(
        &self,
        key: &[u8],
        max_score: f64,
        previous: &OrderedEntry,
    ) -> Option<OrderedEntry> {
        self.next_due_entry_from_range(key, max_score, previous.clone(), true)
    }

    fn next_due_entry_from_range(
        &self,
        key: &[u8],
        max_score: f64,
        lower: OrderedEntry,
        exclude_lower: bool,
    ) -> Option<OrderedEntry> {
        if exclude_lower {
            for entry in self.ordered.range((Excluded(lower), Unbounded)) {
                match due_entry_match(entry, key, max_score) {
                    DueEntryMatch::Match => return Some(entry.clone()),
                    DueEntryMatch::Stop => return None,
                    DueEntryMatch::Continue => continue,
                }
            }
        } else {
            for entry in self.ordered.range(lower..) {
                match due_entry_match(entry, key, max_score) {
                    DueEntryMatch::Match => return Some(entry.clone()),
                    DueEntryMatch::Stop => return None,
                    DueEntryMatch::Continue => continue,
                }
            }
        }

        None
    }

    fn due_keys_present(&self, keys: &[&[u8]], max_score: f64) -> Vec<Vec<u8>> {
        if keys.is_empty() {
            return Vec::new();
        }

        let mut rows = Vec::with_capacity(keys.len());

        for key in keys {
            let lower = OrderedEntry {
                key: key.to_vec(),
                score: Score(f64::NEG_INFINITY),
                member: Vec::new(),
            };

            if let Some(entry) = self.ordered.range(lower..).next() {
                if entry.key.as_slice() == *key && entry.score.0 <= max_score {
                    rows.push((*key).to_vec());
                }
            }
        }

        rows
    }

    fn earliest_due_score_matching(
        &self,
        prefixes: &[&[u8]],
        needles: &[&[u8]],
        suffixes: &[&[u8]],
    ) -> Option<f64> {
        let mut earliest = None;

        for key in &self.positive_due_count_keys {
            if !matches_any(prefixes, |prefix| key.starts_with(prefix))
                || !matches_any(needles, |needle| contains_bytes(key, needle))
                || !matches_any(suffixes, |suffix| key.ends_with(suffix))
            {
                continue;
            }

            let Some(score) = self.first_score_for_key(key) else {
                continue;
            };

            if earliest.map_or(true, |current| Score(score) < Score(current)) {
                earliest = Some(score);
            }
        }

        earliest
    }

    fn first_score_for_key(&self, key: &[u8]) -> Option<f64> {
        let lower = OrderedEntry {
            key: key.to_vec(),
            score: Score(f64::NEG_INFINITY),
            member: Vec::new(),
        };

        self.ordered
            .range(lower..)
            .next()
            .filter(|entry| entry.key.as_slice() == key)
            .map(|entry| entry.score.0)
    }
}

fn matches_any(patterns: &[&[u8]], mut predicate: impl FnMut(&[u8]) -> bool) -> bool {
    patterns.is_empty() || patterns.iter().any(|pattern| predicate(pattern))
}

fn contains_bytes(value: &[u8], needle: &[u8]) -> bool {
    needle.is_empty() || value.windows(needle.len()).any(|window| window == needle)
}

enum DueEntryMatch {
    Match,
    Continue,
    Stop,
}

fn due_entry_match(entry: &OrderedEntry, key: &[u8], max_score: f64) -> DueEntryMatch {
    match entry.key.as_slice().cmp(key) {
        Ordering::Less => DueEntryMatch::Continue,
        Ordering::Greater => DueEntryMatch::Stop,
        Ordering::Equal if entry.score.0 <= max_score => DueEntryMatch::Match,
        Ordering::Equal => DueEntryMatch::Stop,
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
        *current = current.saturating_add(delta);
    } else {
        count_deltas.insert(key.to_vec(), delta);
    }
}

fn decode_flow_record(value: &[u8]) -> Option<FlowRecordParts<'_>> {
    let mut input = value.strip_prefix(FLOW_RECORD_MAGIC)?;

    let (flags, rest) = decode_int(input)?;
    let flags = flags?;
    if flags & !RECORD_KNOWN_FLAGS != 0 {
        return None;
    }
    input = rest;
    let (id, rest) = decode_required_record_bin(input)?;
    input = rest;
    let (flow_type, rest) = decode_required_record_bin(input)?;
    input = rest;
    let (state, rest) = decode_required_record_bin(input)?;
    input = rest;
    let (version, rest) = decode_required_record_int(input)?;
    input = rest;
    let (created_at_ms, rest) = decode_required_record_int(input)?;
    input = rest;
    let (updated_at_ms, rest) = decode_required_record_int(input)?;
    input = rest;
    let (attempts, rest) = decode_flagged_int(input, flags, RECORD_FLAG_ATTEMPTS, Some(0))?;
    input = rest;
    let (fencing_token, rest) =
        decode_flagged_int(input, flags, RECORD_FLAG_FENCING_TOKEN, Some(0))?;
    input = rest;
    let (next_run_at_ms, rest) =
        decode_flagged_int(input, flags, RECORD_FLAG_NEXT_RUN_AT_MS, None)?;
    input = rest;
    let (priority, rest) = decode_flagged_int(input, flags, RECORD_FLAG_PRIORITY, Some(0))?;
    input = rest;
    let (ttl_ms, rest) = decode_flagged_int(input, flags, RECORD_FLAG_TTL_MS, None)?;
    input = rest;
    let (history_hot_max_events, rest) =
        decode_flagged_int(input, flags, RECORD_FLAG_HISTORY_HOT_MAX_EVENTS, None)?;
    input = rest;
    let (history_max_events, rest) =
        decode_flagged_int(input, flags, RECORD_FLAG_HISTORY_MAX_EVENTS, None)?;
    input = rest;
    let (retention_ttl_ms, rest) =
        decode_flagged_int(input, flags, RECORD_FLAG_RETENTION_TTL_MS, None)?;
    input = rest;
    let (terminal_retention_until_ms, rest) =
        decode_flagged_int(input, flags, RECORD_FLAG_TERMINAL_RETENTION_UNTIL_MS, None)?;
    input = rest;
    let (max_active_ms, rest) = decode_flagged_int(input, flags, RECORD_FLAG_MAX_ACTIVE_MS, None)?;
    input = rest;
    let (partition_key, rest) = decode_flagged_bin(input, flags, RECORD_FLAG_PARTITION_KEY)?;
    input = rest;
    let (payload_ref, rest) = decode_flagged_bin(input, flags, RECORD_FLAG_PAYLOAD_REF)?;
    input = rest;
    let (parent_flow_id, rest) = decode_flagged_bin(input, flags, RECORD_FLAG_PARENT_FLOW_ID)?;
    input = rest;
    let (parent_partition_key, rest) =
        decode_flagged_bin(input, flags, RECORD_FLAG_PARENT_PARTITION_KEY)?;
    input = rest;
    let (root_flow_id, rest) = decode_record_root(input, flags, id)?;
    input = rest;
    let (correlation_id, rest) = decode_flagged_bin(input, flags, RECORD_FLAG_CORRELATION_ID)?;
    input = rest;
    let (result_ref, rest) = decode_flagged_bin(input, flags, RECORD_FLAG_RESULT_REF)?;
    input = rest;
    let (error_ref, rest) = decode_flagged_bin(input, flags, RECORD_FLAG_ERROR_REF)?;
    input = rest;
    let (lease_owner, rest) = decode_flagged_bin(input, flags, RECORD_FLAG_LEASE_OWNER)?;
    input = rest;
    let (lease_token, rest) = decode_flagged_bin(input, flags, RECORD_FLAG_LEASE_TOKEN)?;
    input = rest;
    let (lease_deadline_ms, rest) =
        decode_flagged_int(input, flags, RECORD_FLAG_LEASE_DEADLINE_MS, Some(0))?;
    input = rest;
    let (run_state, rest) = decode_flagged_bin(input, flags, RECORD_FLAG_RUN_STATE)?;
    input = rest;
    let (rewound_to_event_id, rest) =
        decode_flagged_bin(input, flags, RECORD_FLAG_REWOUND_TO_EVENT_ID)?;
    input = rest;
    let (child_groups_encoded, rest) = decode_record_sidecar(input, flags)?;

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
        max_active_ms,
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

fn upper_key_for_exact_key(key: &[u8]) -> Vec<u8> {
    let mut upper = Vec::with_capacity(key.len() + 1);
    upper.extend_from_slice(key);
    upper.push(0);
    upper
}

fn lower_entry_for_exact_key(key: &[u8], min: Bound) -> Option<OrderedEntry> {
    let score = match min {
        Bound::NegInf => f64::NEG_INFINITY,
        Bound::Inclusive(score) => score,
        Bound::Exclusive(score) => next_score(score)?,
        Bound::PosInf | Bound::Impossible => return None,
    };

    Some(OrderedEntry {
        key: key.to_vec(),
        score: Score(score),
        member: Vec::new(),
    })
}

fn upper_entry_for_exact_key(key: &[u8], max: Bound) -> Option<OrderedEntry> {
    let score = match max {
        Bound::PosInf => {
            return Some(OrderedEntry {
                key: upper_key_for_exact_key(key),
                score: Score(f64::NEG_INFINITY),
                member: Vec::new(),
            });
        }
        Bound::Exclusive(score) => score,
        Bound::Inclusive(score) => match next_score(score) {
            Some(next) => next,
            None if score == f64::INFINITY => {
                return Some(OrderedEntry {
                    key: upper_key_for_exact_key(key),
                    score: Score(f64::NEG_INFINITY),
                    member: Vec::new(),
                });
            }
            None => return None,
        },
        Bound::NegInf | Bound::Impossible => return None,
    };

    Some(OrderedEntry {
        key: key.to_vec(),
        score: Score(score),
        member: Vec::new(),
    })
}

fn exact_key_range_bounds(
    key: &[u8],
    min: Bound,
    max: Bound,
) -> Option<(OrderedEntry, OrderedEntry)> {
    let lower = lower_entry_for_exact_key(key, min)?;
    let upper = upper_entry_for_exact_key(key, max)?;
    (lower < upper).then_some((lower, upper))
}

fn next_score(score: f64) -> Option<f64> {
    if score.is_nan() || score == f64::INFINITY {
        return None;
    }

    if score == 0.0 {
        return Some(f64::from_bits(1));
    }

    let bits = score.to_bits();
    Some(f64::from_bits(if score > 0.0 {
        bits + 1
    } else {
        bits - 1
    }))
}

fn decode_varint(input: &[u8]) -> Option<(u64, &[u8])> {
    let mut result = 0u64;
    let mut shift = 0u32;
    let mut rest = input;

    for index in 0..10 {
        let (&byte, next) = rest.split_first()?;
        rest = next;

        // A u64 uses at most one payload bit in byte ten. The final zero byte
        // of a multi-byte value is also noncanonical (for example 80 00).
        if index == 9 && byte & 0xfe != 0 {
            return None;
        }
        result |= u64::from(byte & 0x7f) << shift;

        if byte & 0x80 == 0 {
            if index > 0 && byte == 0 {
                return None;
            }
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

fn decode_required_record_bin(input: &[u8]) -> Option<(&[u8], &[u8])> {
    let (value, rest) = decode_required_bin(input)?;
    (!value.is_empty()).then_some((value, rest))
}

fn decode_required_record_int(input: &[u8]) -> Option<(Option<u64>, &[u8])> {
    let (value, rest) = decode_int(input)?;
    let value = value?;
    (value <= MAX_EXACT_INTEGER).then_some((Some(value), rest))
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

fn decode_flagged_int<'a>(
    input: &'a [u8],
    flags: u64,
    flag: u64,
    default: Option<u64>,
) -> Option<(Option<u64>, &'a [u8])> {
    if flags & flag != 0 {
        decode_int(input)
    } else {
        Some((default, input))
    }
}

fn decode_flagged_bin<'a>(
    input: &'a [u8],
    flags: u64,
    flag: u64,
) -> Option<(Option<&'a [u8]>, &'a [u8])> {
    if flags & flag != 0 {
        decode_bin(input)
    } else {
        Some((None, input))
    }
}

fn decode_record_root<'a>(
    input: &'a [u8],
    flags: u64,
    id: &'a [u8],
) -> Option<(Option<&'a [u8]>, &'a [u8])> {
    if flags & RECORD_FLAG_ROOT_FLOW_ID_SELF != 0 {
        Some((Some(id), input))
    } else if flags & RECORD_FLAG_ROOT_FLOW_ID != 0 {
        decode_bin(input)
    } else {
        Some((None, input))
    }
}

fn decode_record_sidecar(input: &[u8], flags: u64) -> Option<(&[u8], &[u8])> {
    if flags & RECORD_FLAG_SIDECAR != 0 {
        decode_encoded_bin_field(input)
    } else {
        Some((EMPTY_CHILD_GROUPS_ENCODED, input))
    }
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
) -> Option<Vec<u8>> {
    encode_flow_record_compact(
        Some(record.id),
        Some(record.flow_type),
        Some(RUNNING_STATE),
        Some(next_version),
        record.attempts,
        Some(next_fencing_token),
        record.created_at_ms,
        Some(now_ms),
        Some(deadline_ms),
        record.priority,
        None,
        record.history_hot_max_events,
        record.history_max_events,
        record.retention_ttl_ms,
        None,
        record.max_active_ms,
        record.partition_key,
        record.payload_ref,
        record.parent_flow_id,
        record.parent_partition_key,
        record.root_flow_id,
        record.correlation_id,
        record.result_ref,
        record.error_ref,
        Some(worker),
        Some(lease_token),
        Some(deadline_ms),
        Some(flow_claim_run_state(record)),
        record.rewound_to_event_id,
        record.child_groups_encoded,
    )
}

fn encode_varint(out: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        out.push((value as u8) | 0x80);
        value >>= 7;
    }

    out.push(value as u8);
}

fn encode_int(out: &mut Vec<u8>, value: Option<u64>) -> Option<()> {
    match value {
        Some(value) => encode_varint(out, value.checked_add(1)?),
        None => out.push(0),
    }
    Some(())
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

fn encode_flagged_int(out: &mut Vec<u8>, flags: u64, flag: u64, value: Option<u64>) -> Option<()> {
    if flags & flag != 0 {
        encode_int(out, value)?;
    }
    Some(())
}

fn encode_flagged_bin(out: &mut Vec<u8>, flags: u64, flag: u64, value: Option<&[u8]>) {
    if flags & flag != 0 {
        encode_bin(out, value);
    }
}

fn optional_slice_len(value: Option<&[u8]>) -> usize {
    value.map_or(0, <[u8]>::len)
}

fn flag_int(flags: &mut u64, flag: u64, value: Option<u64>, omitted_default: u64) {
    if matches!(value, Some(v) if v != omitted_default) {
        *flags |= flag;
    }
}

fn flag_int_present(flags: &mut u64, flag: u64, value: Option<u64>) {
    if value.is_some() {
        *flags |= flag;
    }
}

fn flag_bin(flags: &mut u64, flag: u64, value: Option<&[u8]>) {
    if value.is_some() {
        *flags |= flag;
    }
}

fn flag_nonempty_bin(flags: &mut u64, flag: u64, value: Option<&[u8]>) {
    if matches!(value, Some(v) if !v.is_empty()) {
        *flags |= flag;
    }
}

#[allow(clippy::too_many_arguments)]
fn encode_record_flags(
    id: Option<&[u8]>,
    attempts: Option<u64>,
    fencing_token: Option<u64>,
    next_run_at_ms: Option<u64>,
    priority: Option<u64>,
    ttl_ms: Option<u64>,
    history_hot_max_events: Option<u64>,
    history_max_events: Option<u64>,
    retention_ttl_ms: Option<u64>,
    terminal_retention_until_ms: Option<u64>,
    max_active_ms: Option<u64>,
    partition_key: Option<&[u8]>,
    payload_ref: Option<&[u8]>,
    parent_flow_id: Option<&[u8]>,
    parent_partition_key: Option<&[u8]>,
    root_flow_id: Option<&[u8]>,
    correlation_id: Option<&[u8]>,
    result_ref: Option<&[u8]>,
    error_ref: Option<&[u8]>,
    lease_owner: Option<&[u8]>,
    lease_token: Option<&[u8]>,
    lease_deadline_ms: Option<u64>,
    run_state: Option<&[u8]>,
    rewound_to_event_id: Option<&[u8]>,
    child_groups_encoded: &[u8],
) -> u64 {
    let mut flags = 0u64;
    flag_int(&mut flags, RECORD_FLAG_ATTEMPTS, attempts, 0);
    flag_int(&mut flags, RECORD_FLAG_FENCING_TOKEN, fencing_token, 0);
    flag_int_present(&mut flags, RECORD_FLAG_NEXT_RUN_AT_MS, next_run_at_ms);
    flag_int(&mut flags, RECORD_FLAG_PRIORITY, priority, 0);
    flag_int_present(&mut flags, RECORD_FLAG_TTL_MS, ttl_ms);
    flag_int_present(
        &mut flags,
        RECORD_FLAG_HISTORY_HOT_MAX_EVENTS,
        history_hot_max_events,
    );
    flag_int_present(
        &mut flags,
        RECORD_FLAG_HISTORY_MAX_EVENTS,
        history_max_events,
    );
    flag_int_present(&mut flags, RECORD_FLAG_RETENTION_TTL_MS, retention_ttl_ms);
    flag_int_present(
        &mut flags,
        RECORD_FLAG_TERMINAL_RETENTION_UNTIL_MS,
        terminal_retention_until_ms,
    );
    flag_int_present(&mut flags, RECORD_FLAG_MAX_ACTIVE_MS, max_active_ms);
    flag_bin(&mut flags, RECORD_FLAG_PARTITION_KEY, partition_key);
    flag_bin(&mut flags, RECORD_FLAG_PAYLOAD_REF, payload_ref);
    flag_bin(&mut flags, RECORD_FLAG_PARENT_FLOW_ID, parent_flow_id);
    flag_bin(
        &mut flags,
        RECORD_FLAG_PARENT_PARTITION_KEY,
        parent_partition_key,
    );

    match (id, root_flow_id) {
        (Some(id), Some(root)) if id == root => flags |= RECORD_FLAG_ROOT_FLOW_ID_SELF,
        (_, Some(_)) => flags |= RECORD_FLAG_ROOT_FLOW_ID,
        _ => {}
    }

    flag_bin(&mut flags, RECORD_FLAG_CORRELATION_ID, correlation_id);
    flag_bin(&mut flags, RECORD_FLAG_RESULT_REF, result_ref);
    flag_bin(&mut flags, RECORD_FLAG_ERROR_REF, error_ref);
    flag_bin(&mut flags, RECORD_FLAG_LEASE_OWNER, lease_owner);
    flag_bin(&mut flags, RECORD_FLAG_LEASE_TOKEN, lease_token);
    flag_int(
        &mut flags,
        RECORD_FLAG_LEASE_DEADLINE_MS,
        lease_deadline_ms,
        0,
    );
    flag_bin(&mut flags, RECORD_FLAG_RUN_STATE, run_state);
    flag_bin(
        &mut flags,
        RECORD_FLAG_REWOUND_TO_EVENT_ID,
        rewound_to_event_id,
    );

    if !encoded_child_groups_empty(child_groups_encoded) {
        flags |= RECORD_FLAG_SIDECAR;
    }

    flags
}

#[allow(clippy::too_many_arguments)]
fn encode_history_flags(
    priority: Option<u64>,
    attempts: Option<u64>,
    fencing_token: Option<u64>,
    created_at_ms: Option<u64>,
    updated_at_ms: Option<u64>,
    now_ms: Option<u64>,
    next_run_at_ms: Option<u64>,
    lease_deadline_ms: Option<u64>,
    lease_owner: Option<&[u8]>,
    payload_ref: Option<&[u8]>,
    result_ref: Option<&[u8]>,
    error_ref: Option<&[u8]>,
    rewound_to_event_id: Option<&[u8]>,
    meta_encoded: &[u8],
) -> u64 {
    let mut flags = 0u64;
    flag_int(&mut flags, HISTORY_FLAG_PRIORITY, priority, 0);
    flag_int(&mut flags, HISTORY_FLAG_ATTEMPTS, attempts, 0);
    flag_int(&mut flags, HISTORY_FLAG_FENCING_TOKEN, fencing_token, 0);

    if matches!((created_at_ms, now_ms), (Some(created), Some(now)) if created != now) {
        flags |= HISTORY_FLAG_CREATED_AT_MS;
    }

    if matches!((updated_at_ms, now_ms), (Some(updated), Some(now)) if updated != now) {
        flags |= HISTORY_FLAG_UPDATED_AT_MS;
    }

    flag_int_present(&mut flags, HISTORY_FLAG_NEXT_RUN_AT_MS, next_run_at_ms);
    flag_int(
        &mut flags,
        HISTORY_FLAG_LEASE_DEADLINE_MS,
        lease_deadline_ms,
        0,
    );
    flag_nonempty_bin(&mut flags, HISTORY_FLAG_LEASE_OWNER, lease_owner);
    flag_nonempty_bin(&mut flags, HISTORY_FLAG_PAYLOAD_REF, payload_ref);
    flag_nonempty_bin(&mut flags, HISTORY_FLAG_RESULT_REF, result_ref);
    flag_nonempty_bin(&mut flags, HISTORY_FLAG_ERROR_REF, error_ref);
    flag_nonempty_bin(
        &mut flags,
        HISTORY_FLAG_REWOUND_TO_EVENT_ID,
        rewound_to_event_id,
    );

    if !meta_encoded.is_empty() && meta_encoded != [1] {
        flags |= HISTORY_FLAG_META;
    }

    flags
}

fn optional_bin_slice<'a>(value: Option<&Binary<'a>>) -> Option<&'a [u8]> {
    value.map(Binary::as_slice)
}

fn flow_history_event_id(event_ms: u64, version: u64) -> Vec<u8> {
    format!("{event_ms}-{version}").into_bytes()
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

fn claim_lease_token(worker: &[u8], now_ms: u64, fencing_token: u64) -> Vec<u8> {
    let mut token = Vec::with_capacity(worker.len() + 2 + 20 + 20);
    token.extend_from_slice(worker);
    token.push(b':');
    token.extend_from_slice(now_ms.to_string().as_bytes());
    token.push(b':');
    token.extend_from_slice(fencing_token.to_string().as_bytes());
    token
}

fn checked_claim_counters(version: u64, fencing_token: u64) -> Option<(u64, u64)> {
    let next_version = version.checked_add(1)?;
    let next_fencing_token = fencing_token.checked_add(1)?;
    (next_version < u64::MAX && next_fencing_token < u64::MAX)
        .then_some((next_version, next_fencing_token))
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

fn encode_owned_member_scores<'a>(env: Env<'a>, rows: Vec<(Vec<u8>, f64)>) -> NifResult<Term<'a>> {
    let mut terms = Vec::with_capacity(rows.len());

    for (member, score) in rows {
        terms.push((binary_term(env, &member)?, score).encode(env));
    }

    Ok(terms.encode(env))
}

fn ordered_due_candidate_runs(
    rows: Vec<(Vec<u8>, Vec<u8>, f64)>,
) -> Vec<(Vec<u8>, Vec<(Vec<u8>, f64)>)> {
    let mut runs: Vec<(Vec<u8>, Vec<(Vec<u8>, f64)>)> = Vec::new();

    for (key, member, score) in rows {
        match runs.last_mut() {
            Some((last_key, members)) if last_key.as_slice() == key.as_slice() => {
                members.push((member, score));
            }
            _other => runs.push((key, vec![(member, score)])),
        }
    }

    runs
}

fn encode_owned_key_member_score_runs<'a>(
    env: Env<'a>,
    runs: Vec<(Vec<u8>, Vec<(Vec<u8>, f64)>)>,
) -> NifResult<Term<'a>> {
    let mut terms = Vec::with_capacity(runs.len());

    for (key, members) in runs {
        let mut member_terms = Vec::with_capacity(members.len());

        for (member, score) in members {
            member_terms.push((binary_term(env, &member)?, score).encode(env));
        }

        terms.push((binary_term(env, &key)?, member_terms).encode(env));
    }

    Ok(terms.encode(env))
}

fn encode_owned_fifo_lane_heads<'a>(
    env: Env<'a>,
    rows: Vec<(Vec<u8>, Vec<u8>, Option<f64>)>,
) -> NifResult<Term<'a>> {
    let mut terms = Vec::with_capacity(rows.len());

    for (lane_key, member, due_score) in rows {
        let score_term = match due_score {
            Some(score) => score.encode(env),
            None => crate::atoms::nil().encode(env),
        };

        terms.push(
            (
                binary_term(env, &lane_key)?,
                binary_term(env, &member)?,
                score_term,
            )
                .encode(env),
        );
    }

    Ok(terms.encode(env))
}

fn encode_owned_fifo_lane_heads_many<'a>(
    env: Env<'a>,
    rows: Vec<(Vec<u8>, Vec<u8>, Vec<u8>, Option<f64>)>,
) -> NifResult<Term<'a>> {
    let mut terms = Vec::with_capacity(rows.len());

    for (due_key, lane_key, member, due_score) in rows {
        let score_term = match due_score {
            Some(score) => score.encode(env),
            None => crate::atoms::nil().encode(env),
        };

        terms.push(
            (
                binary_term(env, &due_key)?,
                binary_term(env, &lane_key)?,
                binary_term(env, &member)?,
                score_term,
            )
                .encode(env),
        );
    }

    Ok(terms.encode(env))
}

fn encode_owned_binaries<'a>(env: Env<'a>, values: Vec<Vec<u8>>) -> NifResult<Term<'a>> {
    let mut terms = Vec::with_capacity(values.len());

    for value in values {
        terms.push(binary_term(env, &value)?);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn score_ranges_treat_signed_zero_as_one_numeric_value() {
        let mut index = FlowOrderedIndex::default();
        index.put(b"scores", b"negative-zero", -0.0, false);
        index.put(b"scores", b"positive-zero", 0.0, false);

        let inclusive = index.range_slice(
            b"scores",
            Bound::Inclusive(0.0),
            Bound::Inclusive(0.0),
            false,
            0,
            8,
        );
        assert_eq!(inclusive.len(), 2);

        let exclusive = index.range_slice(
            b"scores",
            Bound::Exclusive(0.0),
            Bound::Inclusive(f64::INFINITY),
            false,
            0,
            8,
        );
        assert!(exclusive.is_empty());
    }

    #[test]
    fn lookup_owns_each_index_key_once_across_members() {
        let mut index = FlowOrderedIndex::default();
        index.put(b"shared-index", b"member-a", 1.0, false);
        index.put(b"shared-index", b"member-b", 2.0, false);

        assert_eq!(index.lookup.len(), 1);
    }

    #[test]
    fn claim_due_candidates_merge_by_due_time_across_keys() {
        let mut index = FlowOrderedIndex::default();
        index.put(b"due:queued", b"queued-job", 100.0, false);
        index.put(b"due:retry", b"retry-job", 10.0, false);

        let rows = index.claim_due_candidates(
            &[b"due:queued".as_slice(), b"due:retry".as_slice()],
            200.0,
            1,
            16,
        );

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, b"due:retry");
        assert_eq!(rows[0].1, b"retry-job");
        assert_eq!(rows[0].2, 10.0);
    }

    #[test]
    fn take_due_does_not_preallocate_the_untrusted_result_limit() {
        let mut index = FlowOrderedIndex::default();
        index.put(b"due:queued", b"only-job", 10.0, false);

        let rows = index.take_due(b"due:queued", 10.0, usize::MAX);

        assert_eq!(rows, vec![(b"only-job".to_vec(), 10.0)]);
    }

    #[test]
    fn claim_due_does_not_preallocate_untrusted_scan_limits() {
        let mut index = FlowOrderedIndex::default();
        index.put(b"due:queued", b"only-job", 10.0, false);

        let rows =
            index.claim_due_candidates(&[b"due:queued".as_slice()], 10.0, usize::MAX, usize::MAX);

        assert_eq!(
            rows,
            vec![(b"due:queued".to_vec(), b"only-job".to_vec(), 10.0)]
        );
    }

    #[test]
    fn positive_count_key_pages_are_ordered_bounded_and_catalog_consistent() {
        let mut index = FlowOrderedIndex::default();
        let due_a = b"f:{flow:a}:d:email:queued:p0";
        let due_b = b"f:{flow:b}:d:email:queued:p0";

        index.set_count(b"plain:b", 1);
        index.set_count(due_b, 2);
        index.set_count(b"plain:a", 3);
        index.set_count(due_a, 4);
        index.set_count(b"zero", 0);

        let count_key = index.counts.get_key_value(due_a.as_slice()).unwrap().0;
        let all_catalog_key = index.positive_count_keys.get(due_a.as_slice()).unwrap();
        let due_catalog_key = index.positive_due_count_keys.get(due_a.as_slice()).unwrap();
        assert_eq!(count_key.as_ptr(), all_catalog_key.as_ptr());
        assert_eq!(count_key.as_ptr(), due_catalog_key.as_ptr());

        let first = index.count_keys_page(false, None, 2);
        assert_eq!(first, vec![due_a.to_vec(), due_b.to_vec()]);
        let second = index.count_keys_page(false, first.last().map(Vec::as_slice), 2);
        assert_eq!(second, vec![b"plain:a".to_vec(), b"plain:b".to_vec()]);
        assert!(index
            .count_keys_page(false, second.last().map(Vec::as_slice), 2)
            .is_empty());

        assert_eq!(index.count_keys_page(true, None, 1), vec![due_a.to_vec()]);
        assert_eq!(
            index.count_keys_page(true, Some(due_a), 1),
            vec![due_b.to_vec()]
        );

        index.set_count(due_a, 0);
        index.remove_count(due_b);
        assert!(index.count_keys_page(true, None, 2).is_empty());
        assert_eq!(
            index.count_keys_page(false, None, 8),
            vec![b"plain:a".to_vec(), b"plain:b".to_vec()]
        );
    }

    #[test]
    fn positive_count_catalogs_follow_every_count_mutation_path() {
        let mut index = FlowOrderedIndex::default();
        let due_a = b"f:{flow:a}:d:email:queued:p0";
        let due_b = b"f:{flow:b}:d:email:queued:p0";
        let ordinary = b"state:queued";

        index.put(due_a, b"flow-1", 1.0, false);
        assert_positive_count_catalogs_exact(&index);

        index.put(ordinary, b"flow-2", 2.0, false);
        assert_positive_count_catalogs_exact(&index);

        index.put(due_a, b"flow-1", 3.0, false);
        assert_positive_count_catalogs_exact(&index);

        index.move_member(due_a, due_b, b"flow-1", 4.0);
        assert_positive_count_catalogs_exact(&index);

        index.take_due(due_b, 4.0, 1);
        assert_positive_count_catalogs_exact(&index);

        index.set_count(due_a, 2);
        assert_positive_count_catalogs_exact(&index);

        index.set_count(due_a, 0);
        assert_positive_count_catalogs_exact(&index);

        index.set_count(due_b, -1);
        assert_positive_count_catalogs_exact(&index);

        index.remove_count(due_b);
        assert_positive_count_catalogs_exact(&index);

        assert!(index.put_new_without_count(due_a, b"flow-3", 5.0));
        assert_positive_count_catalogs_exact(&index);
        index.apply_count_deltas(HashMap::from([(due_a.to_vec(), 1)]));
        assert_positive_count_catalogs_exact(&index);

        assert!(index.delete_without_count(due_a, b"flow-3"));
        assert_positive_count_catalogs_exact(&index);
        index.apply_count_deltas(HashMap::from([(due_a.to_vec(), -1)]));
        assert_positive_count_catalogs_exact(&index);

        index.delete(ordinary, b"flow-2");
        assert_positive_count_catalogs_exact(&index);
    }

    #[test]
    fn range_cursor_pages_resume_exactly_across_tied_scores() {
        let mut index = FlowOrderedIndex::default();
        for (member, score) in [
            (b"a".as_slice(), 10.0),
            (b"b".as_slice(), 10.0),
            (b"c".as_slice(), 10.0),
            (b"d".as_slice(), 11.0),
        ] {
            index.put(b"state:page", member, score, false);
        }

        let forward_first =
            index.range_slice(b"state:page", Bound::NegInf, Bound::PosInf, false, 0, 2);
        assert_eq!(
            forward_first,
            vec![(b"a".to_vec(), 10.0), (b"b".to_vec(), 10.0)]
        );
        assert_eq!(
            index.range_forward_after(
                b"state:page",
                Bound::NegInf,
                Bound::PosInf,
                10.0,
                b"b",
                0,
                2,
            ),
            vec![(b"c".to_vec(), 10.0), (b"d".to_vec(), 11.0)]
        );

        let reverse_first =
            index.range_slice(b"state:page", Bound::NegInf, Bound::PosInf, true, 0, 2);
        assert_eq!(
            reverse_first,
            vec![(b"d".to_vec(), 11.0), (b"c".to_vec(), 10.0)]
        );
        assert_eq!(
            index.range_reverse_before(
                b"state:page",
                Bound::NegInf,
                Bound::PosInf,
                10.0,
                b"c",
                0,
                2,
            ),
            vec![(b"b".to_vec(), 10.0), (b"a".to_vec(), 10.0)]
        );
    }

    #[test]
    fn native_request_budget_rejects_oversized_cardinality_bytes_and_overflow() {
        assert!(flow_index_request_within_budget([
            (MAX_FLOW_INDEX_REQUEST_ITEMS, 0),
            (0, MAX_FLOW_INDEX_REQUEST_BYTES),
        ]));

        assert!(!flow_index_request_within_budget([(
            MAX_FLOW_INDEX_REQUEST_ITEMS + 1,
            0,
        )]));
        assert!(!flow_index_request_within_budget([(
            0,
            MAX_FLOW_INDEX_REQUEST_BYTES + 1,
        )]));
        assert!(!flow_index_request_within_budget(
            [(usize::MAX, 0), (1, 0),]
        ));
        assert!(!flow_index_request_within_budget(
            [(0, usize::MAX), (0, 1),]
        ));
    }

    #[test]
    fn native_page_limits_accept_zero_and_exact_maximum_but_reject_larger() {
        assert!(flow_index_page_limit_within_bounds(0));
        assert!(flow_index_page_limit_within_bounds(
            MAX_FLOW_INDEX_PAGE_ITEMS
        ));
        assert!(!flow_index_page_limit_within_bounds(
            MAX_FLOW_INDEX_PAGE_ITEMS + 1
        ));
    }

    #[test]
    fn list_nifs_preflight_cardinality_before_allocating_native_vectors() {
        let source = include_str!("flow_index.rs");

        for function in [
            "flow_index_put_entries",
            "flow_index_put_new_entries",
            "flow_index_move_entries",
            "flow_index_delete_members",
            "flow_index_delete_entries",
            "flow_index_apply_batch",
            "flow_index_claim_due_candidates",
            "flow_index_fifo_lane_heads",
            "flow_index_fifo_lane_heads_many",
            "flow_index_due_keys_present",
            "flow_index_count_many",
            "flow_index_earliest_due_score",
            "flow_index_apply_claim_entries",
            "flow_index_rollback_claim_entries",
            "flow_record_plan_claims",
            "flow_record_plan_claims_with_history",
            "flow_records_terminal_after_noop",
        ] {
            let start = source
                .find(&format!("pub fn {function}"))
                .unwrap_or_else(|| panic!("missing {function}"));
            let signature = &source[start..source[start..].find(") ->").unwrap() + start];

            assert!(
                !signature.contains("Vec<"),
                "{function} lets Rustler allocate an unbounded Vec before native budget checks"
            );
            assert!(
                signature.contains("Term<"),
                "{function} must receive list terms for cardinality preflight"
            );
        }

        let decoder_start = source
            .find("fn decode_bounded_list")
            .expect("missing bounded list decoder");
        let decoder = &source[decoder_start..];
        let length_check = decoder
            .find(".list_length()")
            .expect("bounded decoder must inspect BEAM list cardinality");
        let allocation = decoder
            .find("Vec::with_capacity")
            .expect("bounded decoder should reserve exact bounded capacity");

        assert!(
            length_check < allocation,
            "BEAM list cardinality must be checked before native Vec allocation"
        );
    }

    fn assert_positive_count_catalogs_exact(index: &FlowOrderedIndex) {
        let expected_all = index
            .counts
            .iter()
            .filter(|(_key, count)| **count > 0)
            .map(|(key, _count)| key.to_vec())
            .collect::<BTreeSet<_>>();
        let expected_due = expected_all
            .iter()
            .filter(|key| due_key(key))
            .cloned()
            .collect::<BTreeSet<_>>();
        let actual_all = index
            .positive_count_keys
            .iter()
            .map(|key| key.to_vec())
            .collect::<BTreeSet<_>>();
        let actual_due = index
            .positive_due_count_keys
            .iter()
            .map(|key| key.to_vec())
            .collect::<BTreeSet<_>>();

        assert_eq!(actual_all, expected_all);
        assert_eq!(actual_due, expected_due);
    }

    #[test]
    fn ordered_due_candidate_runs_preserve_global_order() {
        let rows = vec![
            (b"due:retry".to_vec(), b"retry-1".to_vec(), 10.0),
            (b"due:queued".to_vec(), b"queued-1".to_vec(), 100.0),
            (b"due:queued".to_vec(), b"queued-2".to_vec(), 101.0),
            (b"due:retry".to_vec(), b"retry-2".to_vec(), 102.0),
        ];

        assert_eq!(
            ordered_due_candidate_runs(rows),
            vec![
                (b"due:retry".to_vec(), vec![(b"retry-1".to_vec(), 10.0)]),
                (
                    b"due:queued".to_vec(),
                    vec![(b"queued-1".to_vec(), 100.0), (b"queued-2".to_vec(), 101.0)]
                ),
                (b"due:retry".to_vec(), vec![(b"retry-2".to_vec(), 102.0)])
            ]
        );
    }

    #[test]
    fn claim_due_candidates_keep_single_key_order() {
        let mut index = FlowOrderedIndex::default();
        index.put(b"due:queued", b"second", 20.0, false);
        index.put(b"due:queued", b"first", 10.0, false);

        let rows = index.claim_due_candidates(&[b"due:queued".as_slice()], 100.0, 2, 16);

        assert_eq!(
            rows,
            vec![
                (b"due:queued".to_vec(), b"first".to_vec(), 10.0),
                (b"due:queued".to_vec(), b"second".to_vec(), 20.0)
            ]
        );
    }

    #[test]
    fn earliest_due_score_matching_filters_native_positive_counts_without_copying_keys() {
        let mut index = FlowOrderedIndex::default();

        index.put(b"f:{flow}:d:email:queued:p1", b"queued-later", 40.0, false);
        index.put(b"f:{flow}:d:email:queued:p1", b"queued-first", 30.0, false);
        index.put(b"f:{flow}:d:email:retry:p1", b"retry-first", 20.0, false);
        index.put(b"f:{flow}:da:email:p1", b"any-state-first", 15.0, false);
        index.put(
            b"f:{flow}:d:email:queued:p2",
            b"wrong-priority",
            10.0,
            false,
        );
        index.put(b"f:{flow}:d:sms:queued:p1", b"wrong-type", 5.0, false);
        index.put(b"not-a-due-key", b"not-due", 1.0, false);

        let zero_count_key = b"f:{flow}:d:email:zero:p1";
        index.put(zero_count_key, b"ignored-zero-count", 2.0, false);
        index.set_count(zero_count_key, 0);

        index.set_count(b"f:{flow}:d:email:stale:p1", 1);

        assert_eq!(
            index.earliest_due_score_matching(
                &[b"f:{flow}:".as_slice()],
                &[b"}:d:email:".as_slice(), b"}:da:email:p".as_slice()],
                &[b":p1".as_slice()],
            ),
            Some(15.0)
        );
    }

    #[test]
    fn earliest_due_score_matching_treats_empty_matcher_groups_as_wildcards() {
        let mut index = FlowOrderedIndex::default();
        index.put(b"f:{flow}:d:email:queued:p1", b"later", 30.0, false);
        index.put(b"f:{fa:auto}:d:sms:ready:p2", b"earlier", 12.0, false);
        index.put(b"ordinary:index", b"not-due", 1.0, false);

        assert_eq!(index.earliest_due_score_matching(&[], &[], &[]), Some(12.0));
        assert_eq!(
            index.earliest_due_score_matching(&[b"f:{missing}".as_slice()], &[], &[]),
            None
        );
    }

    #[test]
    fn reverse_cursor_uses_the_minimum_score_as_its_btree_lower_bound() {
        let lower = lower_entry_for_exact_key(b"state:tied", Bound::Inclusive(42.0)).unwrap();

        assert_eq!(lower.key, b"state:tied");
        assert_eq!(lower.score.0, 42.0);
        assert!(lower.member.is_empty());
        assert!(lower_entry_for_exact_key(b"state:tied", Bound::PosInf).is_none());
    }

    #[test]
    fn score_range_builds_a_two_sided_btree_window() {
        let mut index = FlowOrderedIndex::default();

        for score in 0..10_000 {
            index.put(
                b"state:bounded",
                format!("flow-{score:05}").as_bytes(),
                score as f64,
                false,
            );
        }

        let (lower, upper) = exact_key_range_bounds(
            b"state:bounded",
            Bound::Inclusive(10.0),
            Bound::Inclusive(20.0),
        )
        .unwrap();

        assert_eq!(
            index
                .ordered
                .range((Included(lower), Excluded(upper)))
                .count(),
            11
        );
    }

    #[test]
    fn normal_scheduler_lock_attempts_report_busy_instead_of_waiting() {
        let resource = FlowOrderedIndexResource {
            inner: RwLock::new(FlowOrderedIndex::default()),
        };
        let _writer = resource.inner.write().unwrap();

        assert!(matches!(
            try_read_index(&resource),
            Err(IndexLockError::Busy)
        ));
        assert!(matches!(
            try_write_index(&resource),
            Err(IndexLockError::Busy)
        ));
    }

    #[test]
    fn blocking_guards_report_poison_without_panicking() {
        let resource = FlowOrderedIndexResource {
            inner: RwLock::new(FlowOrderedIndex::default()),
        };

        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _writer = resource.inner.write().unwrap();
            panic!("poison flow index");
        }));

        assert!(matches!(
            read_index(&resource),
            Err(IndexLockError::Poisoned)
        ));
        assert!(matches!(
            write_index(&resource),
            Err(IndexLockError::Poisoned)
        ));
    }

    #[test]
    fn unbounded_flow_nifs_run_on_dirty_cpu_schedulers() {
        let source = include_str!("flow_index.rs");
        for function in [
            "flow_index_put_entries",
            "flow_index_put_new_entries",
            "flow_index_move_entries",
            "flow_index_delete_members",
            "flow_index_delete_entries",
            "flow_index_due_keys_present",
            "flow_index_count_many",
            "flow_index_earliest_due_score",
            "flow_record_plan_claims",
            "flow_record_plan_claims_with_history",
            "flow_record_encode",
            "flow_record_decode",
            "flow_records_decode",
            "flow_record_decode_meta",
            "flow_records_terminal_after_noop",
            "flow_history_encode",
            "flow_history_decode",
        ] {
            let declaration = format!("pub fn {function}");
            let declaration_offset = source.find(&declaration).unwrap();
            let attribute_start = source[..declaration_offset]
                .rfind("#[rustler::nif")
                .unwrap();
            let attribute = &source[attribute_start..declaration_offset];
            assert!(
                attribute.contains("schedule = \"DirtyCpu\""),
                "{function} accepts unbounded work and must not monopolize a normal scheduler"
            );
        }
    }

    #[test]
    fn mutation_request_budgets_are_checked_before_index_locks() {
        let source = include_str!("flow_index.rs");

        for function in [
            "flow_index_put_entries",
            "flow_index_put_new_entries",
            "flow_index_move_entries",
            "flow_index_delete_members",
            "flow_index_delete_entries",
            "flow_index_apply_batch",
            "flow_index_take_due",
            "flow_index_apply_claim_entries",
            "flow_index_rollback_claim_entries",
        ] {
            let start = source
                .find(&format!("pub fn {function}"))
                .unwrap_or_else(|| panic!("missing {function}"));
            let rest = &source[start..];
            let end = rest.find("\n#[rustler::nif").unwrap_or(rest.len());
            let body = &rest[..end];
            let budget = body
                .find("FLOW_INDEX_REQUEST_TOO_LARGE")
                .or_else(|| body.find("enforce_flow_index_request_budget!"))
                .unwrap_or_else(|| panic!("{function} has no native request budget"));
            let lock = body
                .find("index_guard!")
                .unwrap_or_else(|| panic!("{function} has no index lock"));

            assert!(
                budget < lock,
                "{function} must reject oversized input before taking the index lock"
            );
        }
    }

    #[test]
    fn durable_varints_reject_overlong_and_overflowing_encodings() {
        assert!(decode_varint(&[0x80, 0x00]).is_none());
        assert!(decode_varint(&[0xff; 10]).is_none());

        let max = [0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01];
        assert_eq!(decode_varint(&max), Some((u64::MAX, &[][..])));
    }

    #[test]
    fn durable_optional_int_encoder_rejects_unrepresentable_max() {
        let mut bytes = vec![0xaa];
        assert_eq!(encode_int(&mut bytes, Some(u64::MAX)), None);
        assert_eq!(bytes, vec![0xaa]);

        assert_eq!(encode_int(&mut bytes, Some(u64::MAX - 1)), Some(()));
        assert_eq!(decode_int(&bytes[1..]), Some((Some(u64::MAX - 1), &[][..])));
    }

    #[test]
    fn claim_counter_step_rejects_version_or_fencing_overflow() {
        assert_eq!(checked_claim_counters(7, 11), Some((8, 12)));
        assert_eq!(checked_claim_counters(u64::MAX - 1, 11), None);
        assert_eq!(checked_claim_counters(7, u64::MAX - 1), None);
        assert_eq!(checked_claim_counters(u64::MAX, 11), None);
        assert_eq!(checked_claim_counters(7, u64::MAX), None);
    }

    #[test]
    fn restored_count_boundaries_do_not_panic_during_index_updates() {
        let mut index = FlowOrderedIndex::default();
        index.set_count(b"max", i64::MAX);
        index.increment_count(b"max", 1);
        assert_eq!(index.counts.get(b"max".as_slice()), Some(&i64::MAX));

        index.set_count(b"min", i64::MIN);
        index.apply_count_deltas(HashMap::from([(b"min".to_vec(), -1)]));
        assert_eq!(index.counts.get(b"min".as_slice()), Some(&i64::MIN));
    }

    #[test]
    fn flow_record_decoder_rejects_unknown_persisted_flags() {
        let mut bytes = FLOW_RECORD_MAGIC.to_vec();
        encode_int(&mut bytes, Some(1 << 63)).unwrap();
        encode_bin(&mut bytes, Some(b"id"));
        encode_bin(&mut bytes, Some(b"type"));
        encode_bin(&mut bytes, Some(b"queued"));
        encode_int(&mut bytes, Some(1)).unwrap();
        encode_int(&mut bytes, Some(2)).unwrap();
        encode_int(&mut bytes, Some(3)).unwrap();

        assert!(decode_flow_record(&bytes).is_none());
    }

    #[test]
    fn flow_record_decoder_rejects_invalid_required_fields() {
        fn record(
            id: Option<&[u8]>,
            flow_type: Option<&[u8]>,
            state: Option<&[u8]>,
            version: Option<u64>,
            created_at_ms: Option<u64>,
            updated_at_ms: Option<u64>,
        ) -> Vec<u8> {
            let mut bytes = FLOW_RECORD_MAGIC.to_vec();
            encode_int(&mut bytes, Some(0)).unwrap();
            encode_bin(&mut bytes, id);
            encode_bin(&mut bytes, flow_type);
            encode_bin(&mut bytes, state);
            encode_int(&mut bytes, version).unwrap();
            encode_int(&mut bytes, created_at_ms).unwrap();
            encode_int(&mut bytes, updated_at_ms).unwrap();
            bytes
        }

        let valid = || {
            record(
                Some(b"id"),
                Some(b"type"),
                Some(b"queued"),
                Some(1),
                Some(2),
                Some(3),
            )
        };

        assert!(decode_flow_record(&valid()).is_some());

        for invalid in [
            record(
                None,
                Some(b"type"),
                Some(b"queued"),
                Some(1),
                Some(2),
                Some(3),
            ),
            record(
                Some(b""),
                Some(b"type"),
                Some(b"queued"),
                Some(1),
                Some(2),
                Some(3),
            ),
            record(
                Some(b"id"),
                None,
                Some(b"queued"),
                Some(1),
                Some(2),
                Some(3),
            ),
            record(
                Some(b"id"),
                Some(b""),
                Some(b"queued"),
                Some(1),
                Some(2),
                Some(3),
            ),
            record(Some(b"id"), Some(b"type"), None, Some(1), Some(2), Some(3)),
            record(
                Some(b"id"),
                Some(b"type"),
                Some(b""),
                Some(1),
                Some(2),
                Some(3),
            ),
            record(
                Some(b"id"),
                Some(b"type"),
                Some(b"queued"),
                None,
                Some(2),
                Some(3),
            ),
            record(
                Some(b"id"),
                Some(b"type"),
                Some(b"queued"),
                Some(1),
                None,
                Some(3),
            ),
            record(
                Some(b"id"),
                Some(b"type"),
                Some(b"queued"),
                Some(1),
                Some(2),
                None,
            ),
            record(
                Some(b"id"),
                Some(b"type"),
                Some(b"queued"),
                Some(9_007_199_254_740_992),
                Some(2),
                Some(3),
            ),
            record(
                Some(b"id"),
                Some(b"type"),
                Some(b"queued"),
                Some(1),
                Some(9_007_199_254_740_992),
                Some(3),
            ),
            record(
                Some(b"id"),
                Some(b"type"),
                Some(b"queued"),
                Some(1),
                Some(2),
                Some(9_007_199_254_740_992),
            ),
        ] {
            assert!(decode_flow_record(&invalid).is_none());
        }
    }

    #[test]
    fn flow_record_encoder_rejects_values_its_decoder_cannot_read() {
        fn encode_minimal(
            id: Option<&[u8]>,
            flow_type: Option<&[u8]>,
            state: Option<&[u8]>,
            version: Option<u64>,
            created_at_ms: Option<u64>,
            updated_at_ms: Option<u64>,
        ) -> Option<Vec<u8>> {
            encode_flow_record_compact(
                id,
                flow_type,
                state,
                version,
                None,
                None,
                created_at_ms,
                updated_at_ms,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                EMPTY_CHILD_GROUPS_ENCODED,
            )
        }

        let valid = encode_minimal(
            Some(b"id"),
            Some(b"type"),
            Some(b"queued"),
            Some(1),
            Some(2),
            Some(3),
        )
        .expect("valid record must encode");
        assert!(decode_flow_record(&valid).is_some());

        for invalid in [
            encode_minimal(
                None,
                Some(b"type"),
                Some(b"queued"),
                Some(1),
                Some(2),
                Some(3),
            ),
            encode_minimal(
                Some(b""),
                Some(b"type"),
                Some(b"queued"),
                Some(1),
                Some(2),
                Some(3),
            ),
            encode_minimal(
                Some(b"id"),
                Some(b""),
                Some(b"queued"),
                Some(1),
                Some(2),
                Some(3),
            ),
            encode_minimal(
                Some(b"id"),
                Some(b"type"),
                Some(b""),
                Some(1),
                Some(2),
                Some(3),
            ),
            encode_minimal(
                Some(b"id"),
                Some(b"type"),
                Some(b"queued"),
                None,
                Some(2),
                Some(3),
            ),
            encode_minimal(
                Some(b"id"),
                Some(b"type"),
                Some(b"queued"),
                Some(MAX_EXACT_INTEGER + 1),
                Some(2),
                Some(3),
            ),
            encode_minimal(
                Some(b"id"),
                Some(b"type"),
                Some(b"queued"),
                Some(1),
                Some(MAX_EXACT_INTEGER + 1),
                Some(3),
            ),
            encode_minimal(
                Some(b"id"),
                Some(b"type"),
                Some(b"queued"),
                Some(1),
                Some(2),
                Some(MAX_EXACT_INTEGER + 1),
            ),
        ] {
            assert!(invalid.is_none());
        }
    }

    #[test]
    fn history_meta_count_is_bounded_by_the_remaining_payload() {
        assert_eq!(checked_history_field_capacity(0, 0), Some(42));
        assert_eq!(checked_history_field_capacity(2, 4), Some(46));
        assert_eq!(checked_history_field_capacity(3, 4), None);
        assert_eq!(checked_history_field_capacity(u64::MAX, 0), None);
    }
}
// This file is intentionally kept as a single fused Rust source unit even
// though it is larger than the normal module-size target.
//
// Flow index/codec/claim planning sits directly on the FerricFlow hot path:
// FLOW.CREATE, FLOW.CLAIM_DUE, terminal commands, and DBOS-style queue
// throughput all pass through this code. A mechanical split was tested and
// rejected because DBOS 1M benchmark samples fell outside the 5% performance
// gate, while restoring the monolithic file recovered the baseline range.
//
// Do not split this file into normal Rust modules only for readability. If it
// must be reorganized, preserve the fused compilation shape first and prove it
// with the DBOS + native-protocol KV benchmark gate.
