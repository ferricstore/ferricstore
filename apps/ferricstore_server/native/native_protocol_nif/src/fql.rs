pub const MAX_QUERY_BYTES: usize = 16 * 1024;
const MAX_TOKENS: usize = 256;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    Execute,
    Explain,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Source {
    Runs,
    Events,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Shape {
    Point,
    Collection,
    History,
    Count,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ValueType {
    Keyword,
    Integer,
    Dynamic,
}

#[derive(Debug, PartialEq, Eq)]
pub enum QueryValue {
    LiteralKeyword(Vec<u8>),
    LiteralInteger(i64),
    Parameter(ValueType, Vec<u8>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Field {
    pub external_name: Vec<u8>,
    kind: FieldKind,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Predicate {
    Eq(Field, QueryValue),
    In(Field, Vec<QueryValue>),
    Range(Field, QueryValue, QueryValue),
    TimeWindow(Field, QueryValue, QueryValue),
    IsNull(Field),
    IsMissing(Field),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Direction {
    Asc,
    Desc,
}

#[derive(Debug, PartialEq, Eq)]
pub struct Order {
    pub field: Field,
    pub direction: Direction,
}

#[derive(Debug, PartialEq, Eq)]
pub struct ParsedQuery {
    pub mode: Mode,
    pub source: Source,
    pub shape: Shape,
    pub predicates: Vec<Predicate>,
    pub order_by: Vec<Order>,
    pub limit: Option<usize>,
    pub cursor: Option<QueryValue>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ParseError {
    InvalidParameterType,
    InvalidSyntax,
    QueryTooLarge,
    UnsupportedField,
    UnsupportedQueryShape,
    UnsupportedSource,
}

#[derive(Debug, PartialEq, Eq)]
enum Token<'a> {
    Word(&'a [u8]),
    String(Vec<u8>),
    Parameter(&'a [u8]),
    Integer(&'a [u8]),
    Equals,
    LeftParen,
    RightParen,
    Comma,
    Semicolon,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FieldKind {
    PartitionKey,
    RunId,
    EventId,
    Type,
    State,
    Version,
    Priority,
    CreatedAtMs,
    UpdatedAtMs,
    NextRunAtMs,
    LeaseDeadlineMs,
    Attempts,
    RunState,
    MaxActiveMs,
    ParentFlowId,
    RootFlowId,
    CorrelationId,
    Attribute,
    StateMeta,
}

impl Field {
    fn value_type(&self) -> ValueType {
        match self.kind {
            FieldKind::Version
            | FieldKind::Priority
            | FieldKind::CreatedAtMs
            | FieldKind::UpdatedAtMs
            | FieldKind::NextRunAtMs
            | FieldKind::LeaseDeadlineMs
            | FieldKind::Attempts
            | FieldKind::MaxActiveMs => ValueType::Integer,
            FieldKind::Attribute | FieldKind::StateMeta => ValueType::Dynamic,
            _ => ValueType::Keyword,
        }
    }
}

pub fn parse(query: &[u8]) -> Result<ParsedQuery, ParseError> {
    if query.len() > MAX_QUERY_BYTES {
        return Err(ParseError::QueryTooLarge);
    }

    let tokens = tokenize(query)?;
    let (mode, tokens) = parse_mode(&tokens);
    parse_query(mode, tokens)
}

fn parse_mode<'tokens, 'query>(
    tokens: &'tokens [Token<'query>],
) -> (Mode, &'tokens [Token<'query>]) {
    match tokens.first() {
        Some(Token::Word(word)) if keyword(word, b"EXPLAIN") => (Mode::Explain, &tokens[1..]),
        _ => (Mode::Execute, tokens),
    }
}

fn parse_query(mode: Mode, tokens: &[Token<'_>]) -> Result<ParsedQuery, ParseError> {
    match tokens {
        [Token::Word(from), Token::Word(source), Token::Word(where_keyword), rest @ ..] => {
            if !keyword(from, b"FROM") {
                Err(ParseError::InvalidSyntax)
            } else if !keyword(where_keyword, b"WHERE") {
                Err(ParseError::UnsupportedQueryShape)
            } else {
                parse_predicates(mode, parse_source(source)?, rest)
            }
        }
        [Token::Word(from), Token::Word(source), ..] => {
            if !keyword(from, b"FROM") {
                Err(ParseError::InvalidSyntax)
            } else {
                parse_source(source)?;
                Err(ParseError::UnsupportedQueryShape)
            }
        }
        _ => Err(ParseError::InvalidSyntax),
    }
}

fn parse_source(source: &[u8]) -> Result<Source, ParseError> {
    if keyword(source, b"RUNS") {
        Ok(Source::Runs)
    } else if keyword(source, b"EVENTS") {
        Ok(Source::Events)
    } else {
        Err(ParseError::UnsupportedSource)
    }
}

fn parse_predicates(
    mode: Mode,
    source: Source,
    tokens: &[Token<'_>],
) -> Result<ParsedQuery, ParseError> {
    let mut predicates = Vec::with_capacity(4);
    let mut offset = 0;

    loop {
        let (predicate, consumed) = parse_predicate(&tokens[offset..])?;
        predicates.push(predicate);
        offset += consumed;

        match tokens.get(offset) {
            Some(Token::Word(word)) if keyword(word, b"AND") => offset += 1,
            _ => break,
        }
    }

    parse_tail(mode, source, predicates, &tokens[offset..])
}

fn parse_predicate(tokens: &[Token<'_>]) -> Result<(Predicate, usize), ParseError> {
    let field_name = match tokens.first() {
        Some(Token::Word(field_name)) => *field_name,
        _ => return Err(ParseError::UnsupportedQueryShape),
    };
    let field = parse_field(field_name)?;

    match tokens.get(1) {
        Some(Token::Equals) => {
            let value = parse_value(
                tokens.get(2).ok_or(ParseError::UnsupportedQueryShape)?,
                &field,
            )?;
            Ok((Predicate::Eq(field, value), 3))
        }
        Some(Token::Word(operator)) if keyword(operator, b"IN") => parse_in(field, tokens),
        Some(Token::Word(operator)) if keyword(operator, b"IS") => parse_is(field, tokens),
        Some(Token::Word(operator))
            if keyword(operator, b"BETWEEN") || keyword(operator, b"FROM") =>
        {
            parse_range(field, operator, tokens)
        }
        Some(Token::Word(_)) | None => Err(ParseError::UnsupportedQueryShape),
        _ => Err(ParseError::UnsupportedQueryShape),
    }
}

fn parse_in(field: Field, tokens: &[Token<'_>]) -> Result<(Predicate, usize), ParseError> {
    if !matches!(tokens.get(2), Some(Token::LeftParen)) {
        return Err(ParseError::UnsupportedQueryShape);
    }

    let mut values = Vec::with_capacity(4);
    let mut offset = 3;

    if matches!(tokens.get(offset), Some(Token::RightParen)) {
        return Err(ParseError::UnsupportedQueryShape);
    }

    loop {
        let value = parse_value(
            tokens
                .get(offset)
                .ok_or(ParseError::UnsupportedQueryShape)?,
            &field,
        )?;
        values.push(value);
        offset += 1;

        match (tokens.get(offset), tokens.get(offset + 1)) {
            (Some(Token::Comma), Some(Token::RightParen)) => {
                return Err(ParseError::UnsupportedQueryShape);
            }
            (Some(Token::Comma), _) => offset += 1,
            (Some(Token::RightParen), _) => {
                offset += 1;
                break;
            }
            _ => return Err(ParseError::UnsupportedQueryShape),
        }
    }

    Ok((Predicate::In(field, values), offset))
}

fn parse_is(field: Field, tokens: &[Token<'_>]) -> Result<(Predicate, usize), ParseError> {
    match tokens.get(2) {
        Some(Token::Word(kind)) if keyword(kind, b"NULL") => Ok((Predicate::IsNull(field), 3)),
        Some(Token::Word(kind)) if keyword(kind, b"MISSING") => {
            Ok((Predicate::IsMissing(field), 3))
        }
        _ => Err(ParseError::UnsupportedQueryShape),
    }
}

fn parse_range(
    field: Field,
    operator: &[u8],
    tokens: &[Token<'_>],
) -> Result<(Predicate, usize), ParseError> {
    let separator = match tokens.get(3) {
        Some(Token::Word(separator)) => *separator,
        _ => return Err(ParseError::UnsupportedQueryShape),
    };

    let range = keyword(operator, b"BETWEEN") && keyword(separator, b"AND");
    let time_window = keyword(operator, b"FROM") && keyword(separator, b"TO");

    if !range && !time_window {
        return Err(ParseError::UnsupportedQueryShape);
    }

    let lower = parse_value(
        tokens.get(2).ok_or(ParseError::UnsupportedQueryShape)?,
        &field,
    )?;
    let upper = parse_value(
        tokens.get(4).ok_or(ParseError::UnsupportedQueryShape)?,
        &field,
    )?;

    let predicate = if range {
        Predicate::Range(field, lower, upper)
    } else {
        Predicate::TimeWindow(field, lower, upper)
    };

    Ok((predicate, 5))
}

fn parse_value(token: &Token<'_>, field: &Field) -> Result<QueryValue, ParseError> {
    match token {
        Token::String(value) => Ok(QueryValue::LiteralKeyword(value.clone())),
        Token::Integer(raw) => parse_i64(raw)
            .map(QueryValue::LiteralInteger)
            .ok_or(ParseError::InvalidParameterType),
        Token::Parameter(name) => Ok(QueryValue::Parameter(field.value_type(), name.to_vec())),
        _ => Err(ParseError::UnsupportedQueryShape),
    }
}

fn parse_tail(
    mode: Mode,
    source: Source,
    predicates: Vec<Predicate>,
    tokens: &[Token<'_>],
) -> Result<ParsedQuery, ParseError> {
    if let [Token::Word(return_keyword), Token::Word(return_shape), tail @ ..] = tokens {
        if keyword(return_keyword, b"RETURN") {
            if source == Source::Runs && keyword(return_shape, b"RECORD") && valid_terminator(tail)
            {
                return canonical_point(mode, predicates);
            }
            if source == Source::Runs && keyword(return_shape, b"COUNT") && valid_terminator(tail) {
                return Ok(ParsedQuery {
                    mode,
                    source,
                    shape: Shape::Count,
                    predicates,
                    order_by: Vec::new(),
                    limit: None,
                    cursor: None,
                });
            }
            return Err(ParseError::UnsupportedQueryShape);
        }
    }

    parse_collection_tail(mode, source, predicates, tokens)
}

fn canonical_point(mode: Mode, predicates: Vec<Predicate>) -> Result<ParsedQuery, ParseError> {
    if predicates.len() == 1 {
        let predicate = predicates
            .into_iter()
            .next()
            .ok_or(ParseError::UnsupportedQueryShape)?;

        return match predicate {
            Predicate::Eq(field, value) if field.kind == FieldKind::RunId => Ok(ParsedQuery {
                mode,
                source: Source::Runs,
                shape: Shape::Point,
                predicates: vec![Predicate::Eq(field, value)],
                order_by: Vec::new(),
                limit: Some(1),
                cursor: None,
            }),
            _ => Err(ParseError::UnsupportedQueryShape),
        };
    }

    if predicates.len() != 2 {
        return Err(ParseError::UnsupportedQueryShape);
    }

    let mut partition = None;
    let mut run_id = None;

    for predicate in predicates {
        match predicate {
            Predicate::Eq(field, value) if field.kind == FieldKind::PartitionKey => {
                partition = Some((field, value));
            }
            Predicate::Eq(field, value) if field.kind == FieldKind::RunId => {
                run_id = Some((field, value));
            }
            _ => return Err(ParseError::UnsupportedQueryShape),
        }
    }

    match (partition, run_id) {
        (Some((partition_field, partition_value)), Some((run_field, run_value))) => {
            Ok(ParsedQuery {
                mode,
                source: Source::Runs,
                shape: Shape::Point,
                predicates: vec![
                    Predicate::Eq(partition_field, partition_value),
                    Predicate::Eq(run_field, run_value),
                ],
                order_by: Vec::new(),
                limit: Some(1),
                cursor: None,
            })
        }
        _ => Err(ParseError::UnsupportedQueryShape),
    }
}

fn parse_collection_tail(
    mode: Mode,
    source: Source,
    predicates: Vec<Predicate>,
    tokens: &[Token<'_>],
) -> Result<ParsedQuery, ParseError> {
    if !matches!(tokens.first(), Some(Token::Word(word)) if keyword(word, b"ORDER"))
        || !matches!(tokens.get(1), Some(Token::Word(word)) if keyword(word, b"BY"))
    {
        return Err(ParseError::UnsupportedQueryShape);
    }

    let mut offset = 2;
    let mut order_by = Vec::with_capacity(2);

    loop {
        if order_by.len() >= 2 {
            return Err(ParseError::UnsupportedQueryShape);
        }

        let field = match tokens.get(offset) {
            Some(Token::Word(field)) => parse_field(field)?,
            _ => return Err(ParseError::UnsupportedQueryShape),
        };
        let direction = match tokens.get(offset + 1) {
            Some(Token::Word(direction)) if keyword(direction, b"ASC") => Direction::Asc,
            Some(Token::Word(direction)) if keyword(direction, b"DESC") => Direction::Desc,
            _ => return Err(ParseError::UnsupportedQueryShape),
        };
        order_by.push(Order { field, direction });
        offset += 2;

        if matches!(tokens.get(offset), Some(Token::Comma)) {
            offset += 1;
        } else {
            break;
        }
    }

    if !matches!(tokens.get(offset), Some(Token::Word(word)) if keyword(word, b"LIMIT")) {
        return Err(ParseError::UnsupportedQueryShape);
    }
    let limit = match tokens.get(offset + 1) {
        Some(Token::Integer(raw)) => parse_limit(raw)?,
        _ => return Err(ParseError::UnsupportedQueryShape),
    };
    offset += 2;

    let cursor = if matches!(tokens.get(offset), Some(Token::Word(word)) if keyword(word, b"CURSOR"))
    {
        let name = match tokens.get(offset + 1) {
            Some(Token::Parameter(name)) => *name,
            _ => return Err(ParseError::UnsupportedQueryShape),
        };
        offset += 2;
        Some(QueryValue::Parameter(ValueType::Keyword, name.to_vec()))
    } else {
        None
    };

    if !matches!(tokens.get(offset), Some(Token::Word(word)) if keyword(word, b"RETURN"))
        || !matches!(tokens.get(offset + 1), Some(Token::Word(word)) if keyword(word, b"RECORDS"))
        || !valid_terminator(&tokens[offset + 2..])
    {
        return Err(ParseError::UnsupportedQueryShape);
    }

    match source {
        Source::Runs => Ok(ParsedQuery {
            mode,
            source,
            shape: Shape::Collection,
            predicates,
            order_by,
            limit: Some(limit),
            cursor,
        }),
        Source::Events => canonical_history(mode, predicates, order_by, limit, cursor),
    }
}

fn canonical_history(
    mode: Mode,
    predicates: Vec<Predicate>,
    order_by: Vec<Order>,
    limit: usize,
    cursor: Option<QueryValue>,
) -> Result<ParsedQuery, ParseError> {
    if order_by.len() != 1 || order_by[0].field.kind != FieldKind::EventId {
        return Err(ParseError::UnsupportedQueryShape);
    }

    let mut partition_count = 0usize;
    let mut run_count = 0usize;

    for predicate in &predicates {
        match predicate {
            Predicate::Eq(field, _) if field.kind == FieldKind::PartitionKey => {
                partition_count += 1;
            }
            Predicate::Eq(field, _) if field.kind == FieldKind::RunId => {
                run_count += 1;
            }
            _ => return Err(ParseError::UnsupportedQueryShape),
        }
    }

    if run_count != 1 || partition_count > 1 || predicates.len() != run_count + partition_count {
        return Err(ParseError::UnsupportedQueryShape);
    }

    Ok(ParsedQuery {
        mode,
        source: Source::Events,
        shape: Shape::History,
        predicates,
        order_by,
        limit: Some(limit),
        cursor,
    })
}

fn parse_limit(raw: &[u8]) -> Result<usize, ParseError> {
    let value = parse_i64(raw).ok_or(ParseError::UnsupportedQueryShape)?;

    if (1..=100).contains(&value) {
        Ok(value as usize)
    } else {
        Err(ParseError::UnsupportedQueryShape)
    }
}

fn valid_terminator(tokens: &[Token<'_>]) -> bool {
    tokens.is_empty() || matches!(tokens, [Token::Semicolon])
}

fn parse_field(input: &[u8]) -> Result<Field, ParseError> {
    let kind = if keyword(input, b"PARTITION_KEY") {
        FieldKind::PartitionKey
    } else if keyword(input, b"RUN_ID") {
        FieldKind::RunId
    } else if keyword(input, b"EVENT_ID") {
        FieldKind::EventId
    } else if keyword(input, b"TYPE") {
        FieldKind::Type
    } else if keyword(input, b"STATE") {
        FieldKind::State
    } else if keyword(input, b"VERSION") {
        FieldKind::Version
    } else if keyword(input, b"PRIORITY") {
        FieldKind::Priority
    } else if keyword(input, b"CREATED_AT_MS") {
        FieldKind::CreatedAtMs
    } else if keyword(input, b"UPDATED_AT_MS") {
        FieldKind::UpdatedAtMs
    } else if keyword(input, b"NEXT_RUN_AT_MS") {
        FieldKind::NextRunAtMs
    } else if keyword(input, b"LEASE_DEADLINE_MS") {
        FieldKind::LeaseDeadlineMs
    } else if keyword(input, b"ATTEMPTS") {
        FieldKind::Attempts
    } else if keyword(input, b"RUN_STATE") {
        FieldKind::RunState
    } else if keyword(input, b"MAX_ACTIVE_MS") {
        FieldKind::MaxActiveMs
    } else if keyword(input, b"PARENT_FLOW_ID") {
        FieldKind::ParentFlowId
    } else if keyword(input, b"ROOT_FLOW_ID") {
        FieldKind::RootFlowId
    } else if keyword(input, b"CORRELATION_ID") {
        FieldKind::CorrelationId
    } else {
        parse_metadata_field(input)?
    };

    Ok(Field {
        external_name: input.to_vec(),
        kind,
    })
}

fn parse_metadata_field(input: &[u8]) -> Result<FieldKind, ParseError> {
    let mut parts = input.split(|byte| *byte == b'.');
    let prefix = parts.next().unwrap_or_default();
    let first = parts.next().ok_or(ParseError::UnsupportedField)?;
    let second = parts.next();

    if parts.next().is_some() {
        return Err(ParseError::UnsupportedField);
    }

    match second {
        None if keyword(prefix, b"ATTRIBUTE") && valid_metadata_name(first) => {
            Ok(FieldKind::Attribute)
        }
        Some(name)
            if keyword(prefix, b"STATE_META")
                && valid_metadata_name(first)
                && valid_metadata_name(name) =>
        {
            Ok(FieldKind::StateMeta)
        }
        _ => Err(ParseError::UnsupportedField),
    }
}

fn valid_metadata_name(name: &[u8]) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && !name.starts_with(b"__")
        && name
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
}

fn parse_i64(raw: &[u8]) -> Option<i64> {
    let (negative, digits) = match raw {
        [b'-', digits @ ..] if !digits.is_empty() => (true, digits),
        [] => return None,
        digits => (false, digits),
    };
    let limit = if negative {
        (i64::MAX as u64) + 1
    } else {
        i64::MAX as u64
    };
    let mut value = 0u64;

    for byte in digits {
        if !byte.is_ascii_digit() {
            return None;
        }
        value = value.checked_mul(10)?.checked_add((byte - b'0') as u64)?;
        if value > limit {
            return None;
        }
    }

    if negative {
        if value == (i64::MAX as u64) + 1 {
            Some(i64::MIN)
        } else {
            Some(-(value as i64))
        }
    } else {
        Some(value as i64)
    }
}

fn tokenize(query: &[u8]) -> Result<Vec<Token<'_>>, ParseError> {
    let mut tokens = Vec::with_capacity(48);
    let mut offset = 0;

    while offset < query.len() {
        while offset < query.len() && is_whitespace(query[offset]) {
            offset += 1;
        }
        if offset == query.len() {
            break;
        }
        if tokens.len() >= MAX_TOKENS {
            return Err(ParseError::UnsupportedQueryShape);
        }

        match query[offset] {
            b'=' => {
                tokens.push(Token::Equals);
                offset += 1;
            }
            b'(' => {
                tokens.push(Token::LeftParen);
                offset += 1;
            }
            b')' => {
                tokens.push(Token::RightParen);
                offset += 1;
            }
            b',' => {
                tokens.push(Token::Comma);
                offset += 1;
            }
            b';' => {
                tokens.push(Token::Semicolon);
                offset += 1;
            }
            b'\'' => {
                let (value, next_offset) = take_string(query, offset + 1)?;
                tokens.push(Token::String(value));
                offset = next_offset;
            }
            b'@' => {
                let start = offset + 1;
                let end = take_identifier_end(query, start);
                if end == start {
                    return Err(ParseError::InvalidSyntax);
                }
                tokens.push(Token::Parameter(&query[start..end]));
                offset = end;
            }
            byte if byte.is_ascii_digit() || (byte == b'-' && offset + 1 < query.len()) => {
                let end = take_integer_end(query, offset);
                if end == offset || (end == offset + 1 && byte == b'-') {
                    return Err(ParseError::InvalidSyntax);
                }
                tokens.push(Token::Integer(&query[offset..end]));
                offset = end;
            }
            byte if is_word_start(byte) => {
                let end = take_identifier_end(query, offset);
                tokens.push(Token::Word(&query[offset..end]));
                offset = end;
            }
            _ => return Err(ParseError::InvalidSyntax),
        }
    }

    Ok(tokens)
}

fn take_string(query: &[u8], mut offset: usize) -> Result<(Vec<u8>, usize), ParseError> {
    let mut value = Vec::with_capacity(query.len().saturating_sub(offset).min(32));

    while offset < query.len() {
        if query[offset] == b'\'' {
            if query.get(offset + 1) == Some(&b'\'') {
                value.push(b'\'');
                offset += 2;
            } else {
                return Ok((value, offset + 1));
            }
        } else {
            value.push(query[offset]);
            offset += 1;
        }
    }

    Err(ParseError::InvalidSyntax)
}

fn take_integer_end(query: &[u8], mut offset: usize) -> usize {
    if query.get(offset) == Some(&b'-') {
        offset += 1;
    }
    while offset < query.len() && query[offset].is_ascii_digit() {
        offset += 1;
    }
    offset
}

fn take_identifier_end(query: &[u8], mut offset: usize) -> usize {
    while offset < query.len() && is_word_continue(query[offset]) {
        offset += 1;
    }
    offset
}

fn keyword(actual: &[u8], expected: &[u8]) -> bool {
    actual.len() == expected.len()
        && actual
            .iter()
            .zip(expected)
            .all(|(actual, expected)| actual.to_ascii_uppercase() == *expected)
}

fn is_whitespace(byte: u8) -> bool {
    matches!(byte, b' ' | b'\t' | b'\n' | b'\r')
}

fn is_word_start(byte: u8) -> bool {
    byte.is_ascii_alphabetic() || byte == b'_'
}

fn is_word_continue(byte: u8) -> bool {
    is_word_start(byte) || byte.is_ascii_digit() || matches!(byte, b'.' | b'-')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_point_reads_and_normalizes_predicate_order() {
        let parsed = parse(
            b"from RUNS where RUN_ID = 'Run''''42' and PARTITION_KEY = @partition return record;",
        )
        .expect("point query parses");

        assert_eq!(parsed.mode, Mode::Execute);
        assert_eq!(parsed.shape, Shape::Point);
        assert_eq!(parsed.limit, Some(1));
        assert_eq!(parsed.predicates.len(), 2);
        assert!(matches!(
            &parsed.predicates[0],
            Predicate::Eq(
                Field {
                    kind: FieldKind::PartitionKey,
                    ..
                },
                QueryValue::Parameter(ValueType::Keyword, name)
            ) if name == b"partition"
        ));
        assert!(matches!(
            &parsed.predicates[1],
            Predicate::Eq(
                Field {
                    kind: FieldKind::RunId,
                    ..
                },
                QueryValue::LiteralKeyword(value)
            ) if value == b"Run''42"
        ));
    }

    #[test]
    fn parses_run_id_only_point_reads() {
        let parsed = parse(b"FROM runs WHERE run_id = @run_id RETURN RECORD")
            .expect("auto-partition point query parses");

        assert_eq!(parsed.shape, Shape::Point);
        assert_eq!(parsed.predicates.len(), 1);
        assert!(matches!(
            &parsed.predicates[0],
            Predicate::Eq(
                Field {
                    kind: FieldKind::RunId,
                    ..
                },
                QueryValue::Parameter(ValueType::Keyword, name)
            ) if name == b"run_id"
        ));
    }

    #[test]
    fn parses_composite_collection_operators() {
        let parsed = parse(
            b"EXPLAIN FROM runs WHERE partition_key = @tenant AND state IN ('failed', 'completed') AND updated_at_ms FROM @from TO @until ORDER BY updated_at_ms DESC, run_id DESC LIMIT 25 RETURN RECORDS",
        )
        .expect("collection query parses");

        assert_eq!(parsed.mode, Mode::Explain);
        assert_eq!(parsed.shape, Shape::Collection);
        assert_eq!(parsed.predicates.len(), 3);
        assert!(matches!(&parsed.predicates[1], Predicate::In(_, values) if values.len() == 2));
        assert!(matches!(
            &parsed.predicates[2],
            Predicate::TimeWindow(
                _,
                QueryValue::Parameter(ValueType::Integer, _),
                QueryValue::Parameter(ValueType::Integer, _)
            )
        ));
        assert_eq!(parsed.order_by.len(), 2);
        assert_eq!(parsed.limit, Some(25));
        assert_eq!(parsed.cursor, None);
    }

    #[test]
    fn parses_direct_event_history() {
        let parsed = parse(
            b"FROM events WHERE run_id = @run_id ORDER BY event_id DESC LIMIT 25 RETURN RECORDS",
        )
        .expect("history query parses");

        assert_eq!(parsed.source, Source::Events);
        assert_eq!(parsed.shape, Shape::History);
        assert_eq!(parsed.predicates.len(), 1);
        assert_eq!(parsed.order_by.len(), 1);
        assert_eq!(parsed.limit, Some(25));
    }

    #[test]
    fn parses_scalar_counts_without_row_pagination() {
        let parsed = parse(
            b"EXPLAIN FROM runs WHERE partition_key = @partition AND type = 'payment' AND state = 'failed' RETURN COUNT;",
        )
        .expect("count query parses");

        assert_eq!(parsed.mode, Mode::Explain);
        assert_eq!(parsed.source, Source::Runs);
        assert_eq!(parsed.shape, Shape::Count);
        assert_eq!(parsed.predicates.len(), 3);
        assert!(parsed.order_by.is_empty());
        assert_eq!(parsed.limit, None);
        assert_eq!(parsed.cursor, None);

        for query in [
            b"FROM events WHERE partition_key = 'p' RETURN COUNT".as_slice(),
            b"FROM runs WHERE partition_key = 'p' LIMIT 1 RETURN COUNT".as_slice(),
            b"FROM runs WHERE partition_key = 'p' CURSOR @page RETURN COUNT".as_slice(),
        ] {
            assert_eq!(parse(query), Err(ParseError::UnsupportedQueryShape));
        }
    }

    #[test]
    fn parses_only_parameterized_collection_cursors() {
        let parsed = parse(
            b"FROM runs WHERE partition_key = @tenant ORDER BY run_id ASC LIMIT 25 CURSOR @page RETURN RECORDS",
        )
        .expect("cursor query parses");

        assert!(matches!(
            parsed.cursor,
            Some(QueryValue::Parameter(ValueType::Keyword, name)) if name == b"page"
        ));

        assert_eq!(
            parse(b"FROM runs WHERE partition_key = @tenant ORDER BY run_id ASC LIMIT 25 CURSOR 'plaintext-token' RETURN RECORDS"),
            Err(ParseError::UnsupportedQueryShape)
        );
    }

    #[test]
    fn parses_ranges_and_null_missing_checks() {
        for predicate in [
            b"priority BETWEEN 1 AND 5".as_slice(),
            b"attribute.region IS NULL".as_slice(),
            b"attribute.region IS MISSING".as_slice(),
        ] {
            let mut query = b"FROM runs WHERE partition_key = 'p' AND ".to_vec();
            query.extend_from_slice(predicate);
            query.extend_from_slice(b" ORDER BY run_id ASC LIMIT 1 RETURN RECORDS");
            assert!(parse(&query).is_ok());
        }
    }

    #[test]
    fn rejects_unsupported_shapes_and_unbounded_integers() {
        assert_eq!(
            parse(b"FROM runs RETURN RECORDS"),
            Err(ParseError::UnsupportedQueryShape)
        );
        assert_eq!(
            parse(b"FROM events WHERE partition_key = 'p' AND run_id = 'r' RETURN RECORD"),
            Err(ParseError::UnsupportedQueryShape)
        );
        assert_eq!(
            parse(b"FROM runs WHERE partition_key = 'p' AND priority BETWEEN 999999999999999999999999 AND 9 ORDER BY run_id ASC LIMIT 1 RETURN RECORDS"),
            Err(ParseError::InvalidParameterType)
        );
    }

    #[test]
    fn enforces_byte_and_token_budgets() {
        let query = vec![b'x'; MAX_QUERY_BYTES + 1];
        assert_eq!(parse(&query), Err(ParseError::QueryTooLarge));

        let exact_tokens = "x ".repeat(MAX_TOKENS).into_bytes();
        assert_eq!(parse(&exact_tokens), Err(ParseError::InvalidSyntax));

        let over_tokens = "x ".repeat(MAX_TOKENS + 1).into_bytes();
        assert_eq!(parse(&over_tokens), Err(ParseError::UnsupportedQueryShape));
    }
}
