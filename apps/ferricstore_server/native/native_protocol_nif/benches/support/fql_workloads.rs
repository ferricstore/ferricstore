pub const MAX_QUERY_BYTES: usize = 16 * 1024;
pub const MAX_TOKENS: usize = 256;
pub const MAX_PREDICATES: usize = 12;
pub const MAX_IN_VALUES: usize = 20;

pub fn parser_shapes() -> Vec<(&'static str, Vec<u8>)> {
    vec![
        (
            "point",
            b"FROM runs WHERE partition_key = @partition AND run_id = @run_id RETURN RECORD"
                .to_vec(),
        ),
        (
            "collection",
            b"FROM runs WHERE partition_key = @partition AND state IN ('failed', 'completed') AND updated_at_ms FROM @from TO @until ORDER BY updated_at_ms DESC LIMIT 25 RETURN RECORDS".to_vec(),
        ),
        (
            "count",
            b"FROM runs WHERE partition_key = @partition AND type = 'payment' AND state = 'failed' RETURN COUNT".to_vec(),
        ),
        (
            "history",
            b"FROM events WHERE partition_key = @partition AND run_id = @run_id ORDER BY event_id DESC LIMIT 100 RETURN RECORDS".to_vec(),
        ),
        (
            "explain",
            b"EXPLAIN FROM runs WHERE partition_key = @partition AND state = 'failed' ORDER BY updated_at_ms DESC LIMIT 25 RETURN RECORDS".to_vec(),
        ),
        (
            "explain_analyze",
            b"EXPLAIN ANALYZE FROM runs WHERE partition_key = @partition AND state = 'failed' ORDER BY updated_at_ms DESC LIMIT 25 RETURN RECORDS".to_vec(),
        ),
    ]
}

pub fn max_valid_query() -> Vec<u8> {
    let prefix = b"FROM runs WHERE partition_key = '";
    let suffix = b"' ORDER BY updated_at_ms ASC LIMIT 25 RETURN RECORDS";
    let fill = MAX_QUERY_BYTES - prefix.len() - suffix.len();
    let mut query = Vec::with_capacity(MAX_QUERY_BYTES);
    query.extend_from_slice(prefix);
    query.resize(query.len() + fill, b'x');
    query.extend_from_slice(suffix);
    debug_assert_eq!(query.len(), MAX_QUERY_BYTES);
    query
}

pub fn max_malformed_query() -> Vec<u8> {
    let mut query = Vec::with_capacity(MAX_QUERY_BYTES);
    query.push(b'\'');
    query.resize(MAX_QUERY_BYTES, b'x');
    query
}

pub fn token_scaling_query(tokens: usize) -> Vec<u8> {
    assert!(tokens <= MAX_TOKENS);
    "x ".repeat(tokens).into_bytes()
}

pub fn predicate_scaling_query(predicates: usize) -> Vec<u8> {
    assert!(predicates > 0);
    let mut query = String::from("FROM runs WHERE partition_key = @partition");
    for index in 1..predicates {
        query.push_str(" AND attribute.field");
        query.push_str(&index.to_string());
        query.push_str(" = @value");
        query.push_str(&index.to_string());
    }
    query.push_str(" ORDER BY updated_at_ms ASC LIMIT 25 RETURN RECORDS");
    query.into_bytes()
}

pub fn in_cardinality_query(cardinality: usize) -> Vec<u8> {
    assert!(cardinality > 0);
    let values = (0..cardinality)
        .map(|index| format!("'state{index}'"))
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "FROM runs WHERE partition_key = @partition AND state IN ({values}) ORDER BY updated_at_ms DESC LIMIT 25 RETURN RECORDS"
    )
    .into_bytes()
}

pub fn metadata_field_query(bracketed: bool) -> Vec<u8> {
    let field = if bracketed {
        "state_meta['checkout-state']['customer.region']"
    } else {
        "state_meta.checkout_state.customer_region"
    };
    format!(
        "FROM runs WHERE partition_key = @partition AND {field} = @value ORDER BY updated_at_ms DESC LIMIT 25 RETURN RECORDS"
    )
    .into_bytes()
}

pub fn escaped_string_query(escaped_quotes: usize) -> Vec<u8> {
    let literal = "a''".repeat(escaped_quotes.max(1));
    format!(
        "FROM runs WHERE partition_key = '{literal}' ORDER BY updated_at_ms ASC LIMIT 25 RETURN RECORDS"
    )
    .into_bytes()
}

pub fn adversarial_queries() -> Vec<(&'static str, Vec<u8>)> {
    let quote_storm = "''".repeat(MAX_QUERY_BYTES / 2).into_bytes();
    let long_identifier = vec![b'a'; MAX_QUERY_BYTES];
    let huge_integer = format!(
        "FROM runs WHERE partition_key = @partition AND priority BETWEEN -{} AND 1 ORDER BY updated_at_ms ASC LIMIT 1 RETURN RECORDS",
        "9".repeat(MAX_QUERY_BYTES / 2)
    )
    .into_bytes();

    vec![
        ("max_malformed", max_malformed_query()),
        ("quote_storm", quote_storm),
        ("long_identifier", long_identifier),
        ("huge_integer", huge_integer),
        ("max_tokens", token_scaling_query(MAX_TOKENS)),
    ]
}
