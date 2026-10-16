#![allow(clippy::type_complexity, dead_code, unused_imports)]

mod prefix_merge {
    include!("../src/prefix_merge.rs");
}

use crc32fast::Hasher;
use prefix_merge::Candidate;
use std::collections::BinaryHeap;
use std::fs::{self, File, OpenOptions};
use std::hint::black_box;
use std::io::{self, Write};
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::time::Instant;

const HEADER_SIZE: usize = 26;
const MERGE_CALLS_PER_SAMPLE: usize = 2_000;
const MERGE_WARMUP_CALLS: usize = 2_000;
const SAMPLES: usize = 31;
const TOMBSTONE: u32 = u32::MAX;
const VALUE_CALLS_PER_SAMPLE: usize = 64;
const VALUE_WARMUP_CALLS: usize = 64;

type OwnedCandidate = (Vec<u8>, usize, Vec<u8>);
type Row = (Vec<u8>, Vec<u8>);
type ValueReader = fn(&File, u64) -> io::Result<Option<Option<Vec<u8>>>>;

fn main() -> io::Result<()> {
    println!(
        "prefix_merge_cpu samples={SAMPLES} merge_warmup_calls={MERGE_WARMUP_CALLS} merge_calls_per_sample={MERGE_CALLS_PER_SAMPLE} value_warmup_calls={VALUE_WARMUP_CALLS} value_calls_per_sample={VALUE_CALLS_PER_SAMPLE}"
    );

    let clustered = build_rows(true);
    let interleaved = build_rows(false);
    let order: Vec<usize> = (0..8).collect();
    for (name, rows, limit) in [
        ("clustered_limit100", &clustered, 100usize),
        ("interleaved_limit100", &interleaved, 100usize),
        ("clustered_limit4", &clustered, 4usize),
        ("interleaved_limit4", &interleaved, 4usize),
        ("clustered_limit1", &clustered, 1usize),
        ("interleaved_limit1", &interleaved, 1usize),
    ] {
        bench_merge(name, rows, &order, limit);
    }

    bench_value_only_pread()
}

fn build_rows(clustered: bool) -> Vec<Vec<Row>> {
    (0..8)
        .map(|source| {
            (0..500)
                .map(|local| {
                    let row = if clustered {
                        source * 500 + local
                    } else {
                        local * 8 + source
                    };
                    (
                        format!("prefix-merge:{row:08}").into_bytes(),
                        vec![source as u8, (local >> 8) as u8, local as u8],
                    )
                })
                .collect()
        })
        .collect()
}

fn baseline_merge(rows: &[Vec<Row>], order: &[usize], cap: usize) -> (Vec<OwnedCandidate>, usize) {
    let mut selected: BinaryHeap<Candidate<'_>> = BinaryHeap::with_capacity(cap);
    let mut scanned = 0usize;
    for &source in order {
        for (key, value) in rows[source].iter().take(cap) {
            scanned += 1;
            let candidate = (key.as_slice(), source, value.as_slice());
            if selected.len() < cap {
                selected.push(candidate);
            } else {
                let largest = *selected.peek().expect("full heap");
                if candidate < largest {
                    selected.pop();
                    selected.push(candidate);
                }
            }
        }
    }
    let selected = selected.into_sorted_vec();
    (
        selected
            .into_iter()
            .map(|(key, source, value)| (key.to_vec(), source, value.to_vec()))
            .collect(),
        scanned,
    )
}

fn candidate_merge(rows: &[Vec<Row>], order: &[usize], cap: usize) -> (Vec<OwnedCandidate>, usize) {
    let mut selected: BinaryHeap<Candidate<'_>> = BinaryHeap::with_capacity(cap);
    let mut scanned = 0usize;
    for &source in order {
        let source_scanned = prefix_merge::scan_source(
            &mut selected,
            source,
            cap,
            rows[source]
                .iter()
                .map(|(key, value)| Ok::<_, ()>((key.as_slice(), value.as_slice()))),
        )
        .expect("synthetic rows are infallible");
        scanned += source_scanned;
    }
    let selected = selected.into_sorted_vec();
    (
        selected
            .into_iter()
            .map(|(key, source, value)| (key.to_vec(), source, value.to_vec()))
            .collect(),
        scanned,
    )
}

fn threshold_merge(rows: &[Vec<Row>], order: &[usize], cap: usize) -> (Vec<OwnedCandidate>, usize) {
    let mut selected: BinaryHeap<Candidate<'_>> = BinaryHeap::with_capacity(cap);
    let mut scanned = 0usize;
    for &source in order {
        for (key, value) in rows[source].iter().take(cap) {
            scanned += 1;
            let candidate = (key.as_slice(), source, value.as_slice());
            if selected.len() < cap {
                selected.push(candidate);
                continue;
            }

            let largest = *selected.peek().expect("full heap");
            if key.as_slice() > largest.0 {
                break;
            }
            if candidate < largest {
                selected.pop();
                selected.push(candidate);
            }
        }
    }
    let selected = selected.into_sorted_vec();
    (
        selected
            .into_iter()
            .map(|(key, source, value)| (key.to_vec(), source, value.to_vec()))
            .collect(),
        scanned,
    )
}

fn bench_merge(name: &str, rows: &[Vec<Row>], order: &[usize], cap: usize) {
    let (baseline_expected, baseline_scanned) = baseline_merge(rows, order, cap);
    let (candidate_expected, candidate_scanned) = candidate_merge(rows, order, cap);
    let (threshold_expected, threshold_scanned) = threshold_merge(rows, order, cap);
    assert_eq!(
        candidate_expected, baseline_expected,
        "result mismatch: {name}"
    );
    assert_eq!(
        threshold_expected, baseline_expected,
        "threshold mismatch: {name}"
    );

    warm(MERGE_WARMUP_CALLS, || baseline_merge(rows, order, cap));
    warm(MERGE_WARMUP_CALLS, || candidate_merge(rows, order, cap));
    warm(MERGE_WARMUP_CALLS, || threshold_merge(rows, order, cap));

    let mut baseline_ns = Vec::with_capacity(SAMPLES);
    let mut candidate_ns = Vec::with_capacity(SAMPLES);
    let mut threshold_ns = Vec::with_capacity(SAMPLES);
    for sample in 0..SAMPLES {
        if sample % 2 == 0 {
            baseline_ns.push(measure(MERGE_CALLS_PER_SAMPLE, || {
                baseline_merge(rows, order, cap)
            }));
            candidate_ns.push(measure(MERGE_CALLS_PER_SAMPLE, || {
                candidate_merge(rows, order, cap)
            }));
            threshold_ns.push(measure(MERGE_CALLS_PER_SAMPLE, || {
                threshold_merge(rows, order, cap)
            }));
        } else {
            candidate_ns.push(measure(MERGE_CALLS_PER_SAMPLE, || {
                candidate_merge(rows, order, cap)
            }));
            threshold_ns.push(measure(MERGE_CALLS_PER_SAMPLE, || {
                threshold_merge(rows, order, cap)
            }));
            baseline_ns.push(measure(MERGE_CALLS_PER_SAMPLE, || {
                baseline_merge(rows, order, cap)
            }));
        }
    }

    println!(
        "merge {name} baseline_scanned={baseline_scanned} candidate_scanned={candidate_scanned} threshold_scanned={threshold_scanned} baseline_median_ns_per_call={:.2} candidate_median_ns_per_call={:.2} threshold_median_ns_per_call={:.2} baseline_p95_ns_per_call={:.2} candidate_p95_ns_per_call={:.2} threshold_p95_ns_per_call={:.2}",
        percentile(&mut baseline_ns, 50),
        percentile(&mut candidate_ns, 50),
        percentile(&mut threshold_ns, 50),
        percentile(&mut baseline_ns, 95),
        percentile(&mut candidate_ns, 95),
        percentile(&mut threshold_ns, 95),
    );
}

fn warm<T>(calls: usize, mut call: impl FnMut() -> T) {
    for _ in 0..calls {
        black_box(call());
    }
}

fn measure<T>(calls: usize, mut call: impl FnMut() -> T) -> f64 {
    let started = Instant::now();
    for _ in 0..calls {
        black_box(call());
    }
    started.elapsed().as_nanos() as f64 / calls as f64
}

fn percentile(values: &mut [f64], percentile: usize) -> f64 {
    values.sort_by(f64::total_cmp);
    let index = (values.len() - 1) * percentile / 100;
    values[index]
}

fn bench_value_only_pread() -> io::Result<()> {
    let root = std::env::temp_dir().join(format!(
        "ferricstore-prefix-merge-cpu-{}",
        std::process::id()
    ));
    fs::create_dir_all(&root)?;

    for (name, value_size, count) in [
        ("small", 128usize, 1_000usize),
        ("large", 64 * 1024, 100usize),
    ] {
        let path = root.join(format!("{name}.log"));
        let offsets = write_records(&path, value_size, count)?;
        let file = File::open(&path)?;
        let baseline_expected = read_all(&file, &offsets, read_value_baseline)?;
        let candidate_expected = read_all(&file, &offsets, read_value_only_candidate)?;
        assert_eq!(
            candidate_expected, baseline_expected,
            "value result mismatch: {name}"
        );

        warm(VALUE_WARMUP_CALLS, || {
            read_all(&file, &offsets, read_value_baseline).expect("baseline read")
        });
        warm(VALUE_WARMUP_CALLS, || {
            read_all(&file, &offsets, read_value_only_candidate).expect("candidate read")
        });

        let mut baseline_ns = Vec::with_capacity(SAMPLES);
        let mut candidate_ns = Vec::with_capacity(SAMPLES);
        for sample in 0..SAMPLES {
            if sample % 2 == 0 {
                baseline_ns.push(measure(VALUE_CALLS_PER_SAMPLE, || {
                    read_all(&file, &offsets, read_value_baseline).expect("baseline read")
                }));
                candidate_ns.push(measure(VALUE_CALLS_PER_SAMPLE, || {
                    read_all(&file, &offsets, read_value_only_candidate).expect("candidate read")
                }));
            } else {
                candidate_ns.push(measure(VALUE_CALLS_PER_SAMPLE, || {
                    read_all(&file, &offsets, read_value_only_candidate).expect("candidate read")
                }));
                baseline_ns.push(measure(VALUE_CALLS_PER_SAMPLE, || {
                    read_all(&file, &offsets, read_value_baseline).expect("baseline read")
                }));
            }
        }

        println!(
            "pread {name} records={count} value_bytes={value_size} baseline_median_ns_per_call={:.2} candidate_median_ns_per_call={:.2} baseline_p95_ns_per_call={:.2} candidate_p95_ns_per_call={:.2}",
            percentile(&mut baseline_ns, 50),
            percentile(&mut candidate_ns, 50),
            percentile(&mut baseline_ns, 95),
            percentile(&mut candidate_ns, 95),
        );
    }

    fs::remove_dir_all(root)
}

fn write_records(path: &Path, value_size: usize, count: usize) -> io::Result<Vec<u64>> {
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)?;
    let mut offsets = Vec::with_capacity(count + 1);
    let mut offset = 0u64;
    for index in 0..count {
        offsets.push(offset);
        let key = format!("key-{index:08}").into_bytes();
        let value = vec![(index as u8).wrapping_add(3); value_size];
        let record = encode_record(&key, &value, false);
        file.write_all(&record)?;
        offset += record.len() as u64;
    }
    offsets.push(offset);
    file.write_all(&encode_record(b"tombstone", &[], true))?;
    file.sync_all()?;
    Ok(offsets)
}

fn encode_record(key: &[u8], value: &[u8], tombstone: bool) -> Vec<u8> {
    let mut header = [0u8; HEADER_SIZE];
    header[4..12].copy_from_slice(&1u64.to_le_bytes());
    header[12..20].copy_from_slice(&0u64.to_le_bytes());
    header[20..22].copy_from_slice(&(key.len() as u16).to_le_bytes());
    let value_size = if tombstone {
        TOMBSTONE
    } else {
        value.len() as u32
    };
    header[22..26].copy_from_slice(&value_size.to_le_bytes());
    let mut hasher = Hasher::new();
    hasher.update(&header[4..]);
    hasher.update(key);
    if !tombstone {
        hasher.update(value);
    }
    header[..4].copy_from_slice(&hasher.finalize().to_le_bytes());

    let mut encoded = header.to_vec();
    encoded.extend_from_slice(key);
    if !tombstone {
        encoded.extend_from_slice(value);
    }
    encoded
}

fn read_all(file: &File, offsets: &[u64], reader: ValueReader) -> io::Result<Vec<Option<Vec<u8>>>> {
    offsets
        .iter()
        .map(|offset| reader(file, *offset).map(|record| record.flatten()))
        .collect()
}

fn read_value_baseline(file: &File, offset: u64) -> io::Result<Option<Option<Vec<u8>>>> {
    let header = read_header(file, offset)?;
    let key_size = u16::from_le_bytes(header[20..22].try_into().unwrap()) as usize;
    let value_size_raw = u32::from_le_bytes(header[22..26].try_into().unwrap());
    let tombstone = value_size_raw == TOMBSTONE;
    let value_size = if tombstone {
        0
    } else {
        value_size_raw as usize
    };
    let mut body = vec![0u8; key_size + value_size];
    read_exact_at(file, &mut body, offset + HEADER_SIZE as u64)?;

    let key = body[..key_size].to_vec();
    let value = body[key_size..].to_vec();
    validate_crc(&header, &key, &value, tombstone)?;
    Ok(Some(if tombstone { None } else { Some(value) }))
}

fn read_value_only_candidate(file: &File, offset: u64) -> io::Result<Option<Option<Vec<u8>>>> {
    let header = read_header(file, offset)?;
    let key_size = u16::from_le_bytes(header[20..22].try_into().unwrap()) as usize;
    let value_size_raw = u32::from_le_bytes(header[22..26].try_into().unwrap());
    let tombstone = value_size_raw == TOMBSTONE;
    let value_size = if tombstone {
        0
    } else {
        value_size_raw as usize
    };
    let mut body = vec![0u8; key_size + value_size];
    read_exact_at(file, &mut body, offset + HEADER_SIZE as u64)?;

    let value = if tombstone {
        Vec::new()
    } else {
        body.split_off(key_size)
    };
    validate_crc(&header, &body, &value, tombstone)?;
    Ok(Some(if tombstone { None } else { Some(value) }))
}

fn read_header(file: &File, offset: u64) -> io::Result<[u8; HEADER_SIZE]> {
    let mut header = [0u8; HEADER_SIZE];
    read_exact_at(file, &mut header, offset)?;
    Ok(header)
}

fn read_exact_at(file: &File, buf: &mut [u8], offset: u64) -> io::Result<()> {
    let mut total = 0usize;
    while total < buf.len() {
        let read = file.read_at(&mut buf[total..], offset + total as u64)?;
        if read == 0 {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "short read"));
        }
        total += read;
    }
    Ok(())
}

fn validate_crc(
    header: &[u8; HEADER_SIZE],
    key: &[u8],
    value: &[u8],
    tombstone: bool,
) -> io::Result<()> {
    let stored = u32::from_le_bytes(header[0..4].try_into().unwrap());
    let mut hasher = Hasher::new();
    hasher.update(&header[4..]);
    hasher.update(key);
    if !tombstone {
        hasher.update(value);
    }
    if hasher.finalize() != stored {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "CRC mismatch"));
    }
    Ok(())
}
