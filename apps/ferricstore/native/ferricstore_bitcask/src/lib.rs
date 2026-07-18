#![deny(clippy::all, clippy::pedantic)]
// These pedantic lints are noisy without adding safety value for this codebase:
// - possible_truncation: we target 64-bit Linux only; u64→usize is always safe.
// - cast_sign_loss / cast_lossless: io_uring result codes require these casts.
// - items_after_statements: common in test helpers and is clear.
// - doc_markdown: minor style preference, not a correctness issue.
// - missing_errors_doc / missing_panics_doc: NIF wrapper docs don't benefit from # Errors sections.
// - must_use_candidate: most public methods are called via NIF wrappers where must_use is irrelevant.
// - cast_possible_wrap: u64→i64 casts are intentional in store code.
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_sign_loss)]
#![allow(clippy::cast_lossless)]
#![allow(clippy::cast_precision_loss)]
#![allow(clippy::items_after_statements)]
#![allow(clippy::doc_markdown)]
#![allow(clippy::missing_errors_doc)]
#![allow(clippy::missing_panics_doc)]
#![allow(clippy::must_use_candidate)]
#![allow(clippy::cast_possible_wrap)]
// NIF functions must return NifResult<Term> per the Rustler API, even when they never fail:
#![allow(clippy::unnecessary_wraps)]
// io_other_error: io_uring code uses Error::new(ErrorKind::Other, ..) for clarity; lint is Rust 1.83+.
#![allow(clippy::io_other_error)]
// Mmap modules (bloom, cms, cuckoo, topk) use raw pointer casts:
#![allow(clippy::ptr_as_ptr)]
#![allow(clippy::similar_names)]
#![allow(clippy::ref_option)]
#![allow(clippy::manual_let_else)]
#![allow(clippy::doc_link_with_quotes)]
// Rustler NIF functions require owned String/Vec params (not &str/&[u8]):
#![allow(clippy::needless_pass_by_value)]
// Wildcard imports in match arms for io::ErrorKind are clearer:
#![allow(clippy::enum_glob_use)]
// map().flatten() is clearer than and_then() in some contexts:
#![allow(clippy::map_flatten)]
// format!("{}", x) vs format!("{x}") — both fine, don't force inline:
#![allow(clippy::uninlined_format_args)]
// Rustler NIF exported functions often have BEAM-shaped signatures and large
// encode/decode bodies. These lints do not signal correctness issues here.
#![allow(clippy::too_many_arguments)]
#![allow(clippy::too_many_lines)]
#![allow(clippy::elidable_lifetime_names)]
#![allow(clippy::type_complexity)]
#![allow(clippy::unnecessary_debug_formatting)]
#![allow(clippy::redundant_closure_for_method_calls)]
#![allow(clippy::map_unwrap_or)]
#![allow(clippy::implicit_clone)]
#![allow(clippy::float_cmp)]
#![allow(clippy::clone_on_copy)]
#![allow(clippy::used_underscore_binding)]
#![allow(clippy::unnecessary_literal_unwrap)]
#![allow(clippy::needless_continue)]
#![allow(clippy::match_same_arms)]
#![allow(clippy::needless_lifetimes)]
#![allow(clippy::large_stack_arrays)]
#![allow(clippy::match_bool)]
#![allow(clippy::single_match)]
#![allow(clippy::option_option)]

pub mod async_io;
pub mod bloom;
pub mod cms;
pub mod cuckoo;
pub mod flow_index;
pub mod fs_nif;
pub mod hint;
pub mod io_backend;
pub mod log;
mod path_open;
pub mod prob_txn;
pub mod tdigest;
pub mod topk;
pub mod tracking_alloc;

include!("sections/part_01.rs");
include!("sections/part_02.rs");
include!("sections/part_03.rs");
include!("sections/part_04.rs");
include!("sections/part_05.rs");
include!("sections/part_06.rs");
include!("sections/part_07.rs");
