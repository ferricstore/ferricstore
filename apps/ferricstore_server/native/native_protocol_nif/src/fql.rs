use std::borrow::Cow;

pub const MAX_QUERY_BYTES: usize = 16 * 1024;
pub const MAX_TOKENS: usize = 256;
pub const MAX_PREDICATES: usize = 12;
pub const MAX_IN_VALUES: usize = 20;
pub const MAX_RETURN_FIELDS: usize = 32;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    Execute,
    Explain,
    Analyze,
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
pub enum QueryValue<'query> {
    LiteralKeyword(Cow<'query, [u8]>),
    LiteralInteger(i64),
    Parameter(ValueType, Cow<'query, [u8]>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Field<'query> {
    pub external_name: Cow<'query, [u8]>,
    kind: FieldKind,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Predicate<'query> {
    Eq(Field<'query>, QueryValue<'query>),
    In(Field<'query>, Vec<QueryValue<'query>>),
    Range(Field<'query>, QueryValue<'query>, QueryValue<'query>),
    TimeWindow(Field<'query>, QueryValue<'query>, QueryValue<'query>),
    IsNull(Field<'query>),
    IsMissing(Field<'query>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Direction {
    Asc,
    Desc,
}

#[derive(Debug, PartialEq, Eq)]
pub struct Order<'query> {
    pub field: Field<'query>,
    pub direction: Direction,
}

#[derive(Debug, PartialEq, Eq)]
pub struct ProjectionField<'query> {
    pub external_name: Cow<'query, [u8]>,
}

#[derive(Clone, Copy)]
enum ProjectionSegment<'query> {
    Plain(&'query [u8]),
    Quoted(&'query [u8]),
}

#[derive(Debug, PartialEq, Eq)]
pub struct ParsedQuery<'query> {
    pub mode: Mode,
    pub source: Source,
    pub shape: Shape,
    pub predicates: Vec<Predicate<'query>>,
    pub order_by: Vec<Order<'query>>,
    pub limit: Option<usize>,
    pub cursor: Option<QueryValue<'query>>,
    pub projection: Option<Vec<ProjectionField<'query>>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ParseError {
    DuplicateProjectionField,
    InvalidParameterType,
    InvalidSyntax,
    QueryCursorInvalid,
    QueryProjectionLimitExceeded,
    QueryTooLarge,
    UnsupportedField,
    UnsupportedQueryShape,
    UnsupportedSource,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ParseFailure {
    pub reason: ParseError,
    pub byte: usize,
}

#[derive(Debug, PartialEq, Eq)]
struct Token<'a> {
    kind: TokenKind<'a>,
    start: usize,
}

#[derive(Debug, PartialEq, Eq)]
enum TokenKind<'a> {
    Word(&'a [u8]),
    String(Vec<u8>),
    Parameter(&'a [u8]),
    Integer(&'a [u8]),
    Equals,
    LeftParen,
    RightParen,
    LeftBracket,
    RightBracket,
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

impl Field<'_> {
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

#[cfg(test)]
pub fn parse(query: &[u8]) -> Result<ParsedQuery<'_>, ParseError> {
    parse_with_diagnostic(query).map_err(|failure| failure.reason)
}

pub fn parse_with_diagnostic(query: &[u8]) -> Result<ParsedQuery<'_>, ParseFailure> {
    if query.len() > MAX_QUERY_BYTES {
        return Err(ParseFailure {
            reason: ParseError::QueryTooLarge,
            byte: MAX_QUERY_BYTES + 1,
        });
    }

    let tokens = tokenize(query)?;
    let (mode, tokens) = parse_mode(&tokens);
    parse_query(mode, tokens, query.len())
}

fn word<'query>(token: &Token<'query>) -> Option<&'query [u8]> {
    match &token.kind {
        TokenKind::Word(value) => Some(*value),
        _ => None,
    }
}

fn word_at<'query>(tokens: &[Token<'query>], index: usize) -> Option<&'query [u8]> {
    tokens.get(index).and_then(word)
}

fn string_at<'tokens, 'query>(
    tokens: &'tokens [Token<'query>],
    index: usize,
) -> Option<&'tokens Vec<u8>> {
    match tokens.get(index).map(|token| &token.kind) {
        Some(TokenKind::String(value)) => Some(value),
        _ => None,
    }
}

fn byte_at(tokens: &[Token<'_>], index: usize, query_len: usize) -> usize {
    tokens
        .get(index)
        .map_or(query_len.saturating_add(1), |token| token.start + 1)
}

fn failure(
    reason: ParseError,
    tokens: &[Token<'_>],
    index: usize,
    query_len: usize,
) -> ParseFailure {
    ParseFailure {
        reason,
        byte: byte_at(tokens, index, query_len),
    }
}

fn expect_kind(
    tokens: &[Token<'_>],
    index: usize,
    expected: &TokenKind<'_>,
    query_len: usize,
) -> Result<(), ParseFailure> {
    if tokens
        .get(index)
        .is_some_and(|token| &token.kind == expected)
    {
        Ok(())
    } else {
        Err(failure(
            ParseError::UnsupportedQueryShape,
            tokens,
            index,
            query_len,
        ))
    }
}

fn parse_mode<'tokens, 'query>(
    tokens: &'tokens [Token<'query>],
) -> (Mode, &'tokens [Token<'query>]) {
    if matches!(word_at(tokens, 0), Some(word) if keyword(word, b"EXPLAIN")) {
        if matches!(word_at(tokens, 1), Some(word) if keyword(word, b"ANALYZE")) {
            (Mode::Analyze, &tokens[2..])
        } else {
            (Mode::Explain, &tokens[1..])
        }
    } else {
        (Mode::Execute, tokens)
    }
}

fn parse_query<'query>(
    mode: Mode,
    tokens: &[Token<'query>],
    query_len: usize,
) -> Result<ParsedQuery<'query>, ParseFailure> {
    let from = word_at(tokens, 0)
        .ok_or_else(|| failure(ParseError::InvalidSyntax, tokens, 0, query_len))?;
    if !keyword(from, b"FROM") {
        return Err(failure(ParseError::InvalidSyntax, tokens, 0, query_len));
    }

    let source_token = word_at(tokens, 1)
        .ok_or_else(|| failure(ParseError::InvalidSyntax, tokens, 1, query_len))?;
    match word_at(tokens, 2) {
        Some(where_keyword) if keyword(where_keyword, b"WHERE") => {}
        Some(_) => {
            return Err(failure(
                ParseError::UnsupportedQueryShape,
                tokens,
                2,
                query_len,
            ));
        }
        None => {
            parse_source(source_token).map_err(|reason| failure(reason, tokens, 1, query_len))?;
            return Err(failure(
                ParseError::UnsupportedQueryShape,
                tokens,
                2,
                query_len,
            ));
        }
    }

    let source =
        parse_source(source_token).map_err(|reason| failure(reason, tokens, 1, query_len))?;

    parse_predicates(mode, source, &tokens[3..], query_len)
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

fn parse_predicates<'query>(
    mode: Mode,
    source: Source,
    tokens: &[Token<'query>],
    query_len: usize,
) -> Result<ParsedQuery<'query>, ParseFailure> {
    let mut predicates = Vec::with_capacity(4);
    let mut offset = 0;

    loop {
        let (predicate, consumed) = parse_predicate(&tokens[offset..], query_len)?;
        predicates.push(predicate);
        offset += consumed;

        match tokens.get(offset) {
            Some(token) if matches!(word(token), Some(word) if keyword(word, b"AND")) => {
                offset += 1;
                if predicates.len() == MAX_PREDICATES {
                    return Err(failure(
                        ParseError::UnsupportedQueryShape,
                        tokens,
                        offset,
                        query_len,
                    ));
                }
            }
            _ => break,
        }
    }

    parse_tail(mode, source, predicates, &tokens[offset..], query_len)
}

fn parse_predicate<'query>(
    tokens: &[Token<'query>],
    query_len: usize,
) -> Result<(Predicate<'query>, usize), ParseFailure> {
    let (field, field_tokens) = parse_field_tokens(tokens, query_len)?;
    let tail = &tokens[field_tokens..];

    match tail.first().map(|token| &token.kind) {
        Some(TokenKind::Equals) => {
            let value_token = tail
                .get(1)
                .ok_or_else(|| failure(ParseError::UnsupportedQueryShape, tail, 1, query_len))?;
            let value = parse_value(value_token, &field)
                .map_err(|reason| failure(reason, tail, 1, query_len))?;
            Ok((Predicate::Eq(field, value), field_tokens + 2))
        }
        Some(TokenKind::Word(operator)) if keyword(operator, b"IN") => {
            let (predicate, consumed) = parse_in(field, tail, query_len)?;
            Ok((predicate, field_tokens + consumed))
        }
        Some(TokenKind::Word(operator)) if keyword(operator, b"IS") => {
            let (predicate, consumed) = parse_is(field, tail, query_len)?;
            Ok((predicate, field_tokens + consumed))
        }
        Some(TokenKind::Word(operator))
            if keyword(operator, b"BETWEEN") || keyword(operator, b"FROM") =>
        {
            let (predicate, consumed) = parse_range(field, operator, tail, query_len)?;
            Ok((predicate, field_tokens + consumed))
        }
        _ => Err(failure(
            ParseError::UnsupportedQueryShape,
            tail,
            0,
            query_len,
        )),
    }
}

fn parse_in<'query>(
    field: Field<'query>,
    tokens: &[Token<'query>],
    query_len: usize,
) -> Result<(Predicate<'query>, usize), ParseFailure> {
    if !matches!(
        tokens.get(1).map(|token| &token.kind),
        Some(TokenKind::LeftParen)
    ) {
        return Err(failure(
            ParseError::UnsupportedQueryShape,
            tokens,
            1,
            query_len,
        ));
    }

    let mut values = Vec::with_capacity(4);
    let mut offset = 2;

    if matches!(
        tokens.get(offset).map(|token| &token.kind),
        Some(TokenKind::RightParen)
    ) {
        return Err(failure(
            ParseError::UnsupportedQueryShape,
            tokens,
            offset,
            query_len,
        ));
    }

    loop {
        let value_token = tokens
            .get(offset)
            .ok_or_else(|| failure(ParseError::UnsupportedQueryShape, tokens, offset, query_len))?;
        let value = parse_value(value_token, &field)
            .map_err(|reason| failure(reason, tokens, offset, query_len))?;
        values.push(value);
        offset += 1;

        match (
            tokens.get(offset).map(|token| &token.kind),
            tokens.get(offset + 1).map(|token| &token.kind),
        ) {
            (Some(TokenKind::Comma), Some(TokenKind::RightParen)) => {
                return Err(failure(
                    ParseError::UnsupportedQueryShape,
                    tokens,
                    offset + 1,
                    query_len,
                ));
            }
            (Some(TokenKind::Comma), _) => {
                offset += 1;
                if values.len() == MAX_IN_VALUES {
                    return Err(failure(
                        ParseError::UnsupportedQueryShape,
                        tokens,
                        offset,
                        query_len,
                    ));
                }
            }
            (Some(TokenKind::RightParen), _) => {
                offset += 1;
                break;
            }
            _ => {
                return Err(failure(
                    ParseError::UnsupportedQueryShape,
                    tokens,
                    offset,
                    query_len,
                ));
            }
        }
    }

    Ok((Predicate::In(field, values), offset))
}

fn parse_is<'query>(
    field: Field<'query>,
    tokens: &[Token<'query>],
    query_len: usize,
) -> Result<(Predicate<'query>, usize), ParseFailure> {
    match tokens.get(1).map(|token| &token.kind) {
        Some(TokenKind::Word(kind)) if keyword(kind, b"NULL") => Ok((Predicate::IsNull(field), 2)),
        Some(TokenKind::Word(kind)) if keyword(kind, b"MISSING") => {
            Ok((Predicate::IsMissing(field), 2))
        }
        _ => Err(failure(
            ParseError::UnsupportedQueryShape,
            tokens,
            1,
            query_len,
        )),
    }
}

fn parse_range<'query>(
    field: Field<'query>,
    operator: &[u8],
    tokens: &[Token<'query>],
    query_len: usize,
) -> Result<(Predicate<'query>, usize), ParseFailure> {
    let separator = match word_at(tokens, 2) {
        Some(separator) => separator,
        None => {
            return Err(failure(
                ParseError::UnsupportedQueryShape,
                tokens,
                2,
                query_len,
            ));
        }
    };

    let range = keyword(operator, b"BETWEEN") && keyword(separator, b"AND");
    let time_window = keyword(operator, b"FROM") && keyword(separator, b"TO");

    if !range && !time_window {
        return Err(failure(
            ParseError::UnsupportedQueryShape,
            tokens,
            2,
            query_len,
        ));
    }

    let lower_token = tokens
        .get(1)
        .ok_or_else(|| failure(ParseError::UnsupportedQueryShape, tokens, 1, query_len))?;
    let lower =
        parse_value(lower_token, &field).map_err(|reason| failure(reason, tokens, 1, query_len))?;
    let upper_token = tokens
        .get(3)
        .ok_or_else(|| failure(ParseError::UnsupportedQueryShape, tokens, 3, query_len))?;
    let upper =
        parse_value(upper_token, &field).map_err(|reason| failure(reason, tokens, 3, query_len))?;

    let predicate = if range {
        Predicate::Range(field, lower, upper)
    } else {
        Predicate::TimeWindow(field, lower, upper)
    };

    Ok((predicate, 4))
}

fn parse_value<'query>(
    token: &Token<'query>,
    field: &Field<'query>,
) -> Result<QueryValue<'query>, ParseError> {
    match &token.kind {
        TokenKind::String(value) => Ok(QueryValue::LiteralKeyword(Cow::Owned(value.clone()))),
        TokenKind::Integer(raw) => parse_i64(raw)
            .map(QueryValue::LiteralInteger)
            .ok_or(ParseError::InvalidParameterType),
        TokenKind::Parameter(name) => Ok(QueryValue::Parameter(
            field.value_type(),
            Cow::Borrowed(*name),
        )),
        _ => Err(ParseError::UnsupportedQueryShape),
    }
}

fn parse_tail<'query>(
    mode: Mode,
    source: Source,
    predicates: Vec<Predicate<'query>>,
    tokens: &[Token<'query>],
    query_len: usize,
) -> Result<ParsedQuery<'query>, ParseFailure> {
    if matches!(word_at(tokens, 0), Some(word) if keyword(word, b"RETURN")) {
        if let Some(return_shape) = word_at(tokens, 1) {
            if source == Source::Runs && keyword(return_shape, b"RECORD") {
                let projection = parse_return_projection(source, &tokens[2..], query_len)?;
                return canonical_point(
                    mode,
                    predicates,
                    projection,
                    byte_at(tokens, 0, query_len),
                );
            }
            if source == Source::Runs && keyword(return_shape, b"COUNT") {
                validate_terminator(&tokens[2..], query_len)?;
                return Ok(ParsedQuery {
                    mode,
                    source,
                    shape: Shape::Count,
                    predicates,
                    order_by: Vec::new(),
                    limit: None,
                    cursor: None,
                    projection: None,
                });
            }
        }

        return Err(failure(
            ParseError::UnsupportedQueryShape,
            tokens,
            1,
            query_len,
        ));
    }

    parse_collection_tail(mode, source, predicates, tokens, query_len)
}

fn canonical_point<'query>(
    mode: Mode,
    predicates: Vec<Predicate<'query>>,
    projection: Option<Vec<ProjectionField<'query>>>,
    error_byte: usize,
) -> Result<ParsedQuery<'query>, ParseFailure> {
    if predicates.len() == 1 {
        let predicate = predicates.into_iter().next().ok_or(ParseFailure {
            reason: ParseError::UnsupportedQueryShape,
            byte: error_byte,
        })?;

        return match predicate {
            Predicate::Eq(field, value) if field.kind == FieldKind::RunId => Ok(ParsedQuery {
                mode,
                source: Source::Runs,
                shape: Shape::Point,
                predicates: vec![Predicate::Eq(field, value)],
                order_by: Vec::new(),
                limit: Some(1),
                cursor: None,
                projection,
            }),
            _ => Err(ParseFailure {
                reason: ParseError::UnsupportedQueryShape,
                byte: error_byte,
            }),
        };
    }

    if predicates.len() != 2 {
        return Err(ParseFailure {
            reason: ParseError::UnsupportedQueryShape,
            byte: error_byte,
        });
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
            _ => {
                return Err(ParseFailure {
                    reason: ParseError::UnsupportedQueryShape,
                    byte: error_byte,
                });
            }
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
                projection,
            })
        }
        _ => Err(ParseFailure {
            reason: ParseError::UnsupportedQueryShape,
            byte: error_byte,
        }),
    }
}

fn parse_collection_tail<'query>(
    mode: Mode,
    source: Source,
    predicates: Vec<Predicate<'query>>,
    tokens: &[Token<'query>],
    query_len: usize,
) -> Result<ParsedQuery<'query>, ParseFailure> {
    if !matches!(word_at(tokens, 0), Some(word) if keyword(word, b"ORDER")) {
        return Err(failure(
            ParseError::UnsupportedQueryShape,
            tokens,
            0,
            query_len,
        ));
    }
    if !matches!(word_at(tokens, 1), Some(word) if keyword(word, b"BY")) {
        return Err(failure(
            ParseError::UnsupportedQueryShape,
            tokens,
            1,
            query_len,
        ));
    }

    let mut offset = 2;
    let mut order_by = Vec::with_capacity(2);

    loop {
        if order_by.len() >= 2 {
            return Err(failure(
                ParseError::UnsupportedQueryShape,
                tokens,
                offset,
                query_len,
            ));
        }

        let field_token = word_at(tokens, offset)
            .ok_or_else(|| failure(ParseError::UnsupportedQueryShape, tokens, offset, query_len))?;
        let field = parse_field(field_token)
            .map_err(|reason| failure(reason, tokens, offset, query_len))?;
        let direction = match word_at(tokens, offset + 1) {
            Some(direction) if keyword(direction, b"ASC") => Direction::Asc,
            Some(direction) if keyword(direction, b"DESC") => Direction::Desc,
            _ => {
                return Err(failure(
                    ParseError::UnsupportedQueryShape,
                    tokens,
                    offset + 1,
                    query_len,
                ));
            }
        };
        order_by.push(Order { field, direction });
        offset += 2;

        if matches!(
            tokens.get(offset).map(|token| &token.kind),
            Some(TokenKind::Comma)
        ) {
            offset += 1;
        } else {
            break;
        }
    }

    if !matches!(word_at(tokens, offset), Some(word) if keyword(word, b"LIMIT")) {
        return Err(failure(
            ParseError::UnsupportedQueryShape,
            tokens,
            offset,
            query_len,
        ));
    }
    let limit = match tokens.get(offset + 1).map(|token| &token.kind) {
        Some(TokenKind::Integer(raw)) => {
            parse_limit(raw).map_err(|reason| failure(reason, tokens, offset + 1, query_len))?
        }
        _ => {
            return Err(failure(
                ParseError::UnsupportedQueryShape,
                tokens,
                offset + 1,
                query_len,
            ));
        }
    };
    offset += 2;

    let cursor = if matches!(word_at(tokens, offset), Some(word) if keyword(word, b"CURSOR")) {
        if mode != Mode::Execute {
            return Err(failure(
                ParseError::QueryCursorInvalid,
                tokens,
                offset,
                query_len,
            ));
        }

        let name = match tokens.get(offset + 1).map(|token| &token.kind) {
            Some(TokenKind::Parameter(name)) => *name,
            _ => {
                return Err(failure(
                    ParseError::UnsupportedQueryShape,
                    tokens,
                    offset + 1,
                    query_len,
                ));
            }
        };
        offset += 2;
        Some(QueryValue::Parameter(
            ValueType::Keyword,
            Cow::Borrowed(name),
        ))
    } else {
        None
    };

    if !matches!(word_at(tokens, offset), Some(word) if keyword(word, b"RETURN")) {
        return Err(failure(
            ParseError::UnsupportedQueryShape,
            tokens,
            offset,
            query_len,
        ));
    }
    if !matches!(word_at(tokens, offset + 1), Some(word) if keyword(word, b"RECORDS")) {
        return Err(failure(
            ParseError::UnsupportedQueryShape,
            tokens,
            offset + 1,
            query_len,
        ));
    }
    let projection = parse_return_projection(source, &tokens[offset + 2..], query_len)?;

    match source {
        Source::Runs => Ok(ParsedQuery {
            mode,
            source,
            shape: Shape::Collection,
            predicates,
            order_by,
            limit: Some(limit),
            cursor,
            projection,
        }),
        Source::Events => canonical_history(
            mode,
            predicates,
            order_by,
            limit,
            cursor,
            projection,
            byte_at(tokens, offset, query_len),
        ),
    }
}

fn canonical_history<'query>(
    mode: Mode,
    predicates: Vec<Predicate<'query>>,
    order_by: Vec<Order<'query>>,
    limit: usize,
    cursor: Option<QueryValue<'query>>,
    projection: Option<Vec<ProjectionField<'query>>>,
    error_byte: usize,
) -> Result<ParsedQuery<'query>, ParseFailure> {
    if order_by.len() != 1 || order_by[0].field.kind != FieldKind::EventId {
        return Err(ParseFailure {
            reason: ParseError::UnsupportedQueryShape,
            byte: error_byte,
        });
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
            _ => {
                return Err(ParseFailure {
                    reason: ParseError::UnsupportedQueryShape,
                    byte: error_byte,
                });
            }
        }
    }

    if run_count != 1 || partition_count > 1 || predicates.len() != run_count + partition_count {
        return Err(ParseFailure {
            reason: ParseError::UnsupportedQueryShape,
            byte: error_byte,
        });
    }

    Ok(ParsedQuery {
        mode,
        source: Source::Events,
        shape: Shape::History,
        predicates,
        order_by,
        limit: Some(limit),
        cursor,
        projection,
    })
}

fn parse_return_projection<'query>(
    source: Source,
    tokens: &[Token<'query>],
    query_len: usize,
) -> Result<Option<Vec<ProjectionField<'query>>>, ParseFailure> {
    if !matches!(
        tokens.first().map(|token| &token.kind),
        Some(TokenKind::LeftParen)
    ) {
        validate_terminator(tokens, query_len)?;
        return Ok(None);
    }

    let mut projection = Vec::with_capacity(4);
    let mut offset = 1usize;

    if matches!(
        tokens.get(offset).map(|token| &token.kind),
        Some(TokenKind::RightParen)
    ) {
        return Err(failure(
            ParseError::UnsupportedQueryShape,
            tokens,
            offset,
            query_len,
        ));
    }

    loop {
        if projection.len() >= MAX_RETURN_FIELDS {
            return Err(failure(
                ParseError::QueryProjectionLimitExceeded,
                tokens,
                offset,
                query_len,
            ));
        }

        let (field, consumed) = parse_projection_field(source, &tokens[offset..], query_len)?;

        if projection
            .iter()
            .any(|existing| same_projection_field(existing, &field))
        {
            return Err(failure(
                ParseError::DuplicateProjectionField,
                tokens,
                offset,
                query_len,
            ));
        }

        projection.push(field);
        offset += consumed;

        match tokens.get(offset).map(|token| &token.kind) {
            Some(TokenKind::Comma) => {
                offset += 1;
                if matches!(
                    tokens.get(offset).map(|token| &token.kind),
                    Some(TokenKind::RightParen)
                ) {
                    return Err(failure(
                        ParseError::UnsupportedQueryShape,
                        tokens,
                        offset,
                        query_len,
                    ));
                }
            }
            Some(TokenKind::RightParen) => {
                offset += 1;
                validate_terminator(&tokens[offset..], query_len)?;
                return Ok(Some(projection));
            }
            _ => {
                return Err(failure(
                    ParseError::UnsupportedQueryShape,
                    tokens,
                    offset,
                    query_len,
                ));
            }
        }
    }
}

fn parse_projection_field<'query>(
    source: Source,
    tokens: &[Token<'query>],
    query_len: usize,
) -> Result<(ProjectionField<'query>, usize), ParseFailure> {
    if source == Source::Runs {
        if let Some(field_name) = word_at(tokens, 0) {
            if keyword(field_name, b"ATTRIBUTES") {
                return Ok((
                    ProjectionField {
                        external_name: Cow::Borrowed(&b"attributes"[..]),
                    },
                    1,
                ));
            }

            if keyword(field_name, b"STATE_META")
                && !matches!(
                    tokens.get(1).map(|token| &token.kind),
                    Some(TokenKind::LeftBracket)
                )
            {
                return Ok((
                    ProjectionField {
                        external_name: Cow::Borrowed(&b"state_meta"[..]),
                    },
                    1,
                ));
            }
        }

        let (field, consumed) = parse_field_tokens(tokens, query_len)?;
        if field.kind == FieldKind::EventId {
            return Err(failure(ParseError::UnsupportedField, tokens, 0, query_len));
        }

        return Ok((
            ProjectionField {
                external_name: field.external_name,
            },
            consumed,
        ));
    }

    let field_name = word_at(tokens, 0)
        .ok_or_else(|| failure(ParseError::UnsupportedQueryShape, tokens, 0, query_len))?;

    if keyword(field_name, b"FIELDS")
        && matches!(
            tokens.get(1).map(|token| &token.kind),
            Some(TokenKind::LeftBracket)
        )
    {
        let name = string_at(tokens, 2)
            .ok_or_else(|| failure(ParseError::UnsupportedQueryShape, tokens, 2, query_len))?;
        expect_kind(tokens, 3, &TokenKind::RightBracket, query_len)?;
        if !valid_metadata_key(name) {
            return Err(failure(ParseError::UnsupportedField, tokens, 2, query_len));
        }

        return Ok((
            ProjectionField {
                external_name: Cow::Owned(bracket_field(b"fields", &[name])),
            },
            4,
        ));
    }

    let external_name = if keyword(field_name, b"EVENT_ID") {
        Cow::Borrowed(&b"event_id"[..])
    } else if keyword(field_name, b"FIELDS") {
        Cow::Borrowed(&b"fields"[..])
    } else {
        return Err(failure(ParseError::UnsupportedField, tokens, 0, query_len));
    };

    Ok((ProjectionField { external_name }, 1))
}

fn same_projection_field(left: &ProjectionField<'_>, right: &ProjectionField<'_>) -> bool {
    let left = left.external_name.as_ref();
    let right = right.external_name.as_ref();
    let left_separator = left.iter().position(|byte| matches!(byte, b'.' | b'['));
    let right_separator = right.iter().position(|byte| matches!(byte, b'.' | b'['));

    match (left_separator, right_separator) {
        (None, None) => left.eq_ignore_ascii_case(right),
        (Some(left_at), Some(right_at)) => {
            if !left[..left_at].eq_ignore_ascii_case(&right[..right_at]) {
                return false;
            }

            match (
                projection_segments(left, left_at),
                projection_segments(right, right_at),
            ) {
                (Some((left_first, left_second)), Some((right_first, right_second))) => {
                    same_projection_segment(left_first, right_first)
                        && match (left_second, right_second) {
                            (None, None) => true,
                            (Some(left), Some(right)) => same_projection_segment(left, right),
                            _ => false,
                        }
                }
                _ => false,
            }
        }
        _ => false,
    }
}

fn projection_segments(
    field: &[u8],
    separator: usize,
) -> Option<(ProjectionSegment<'_>, Option<ProjectionSegment<'_>>)> {
    match field.get(separator) {
        Some(b'.') => {
            let mut parts = field.get(separator + 1..)?.split(|byte| *byte == b'.');
            let first = parts.next()?;
            let second = parts.next();

            if first.is_empty()
                || second.is_some_and(|segment| segment.is_empty())
                || parts.next().is_some()
            {
                return None;
            }

            Some((
                ProjectionSegment::Plain(first),
                second.map(ProjectionSegment::Plain),
            ))
        }
        Some(b'[') => {
            let (first, offset) = quoted_projection_segment(field, separator)?;

            if offset == field.len() {
                Some((first, None))
            } else {
                let (second, offset) = quoted_projection_segment(field, offset)?;
                (offset == field.len()).then_some((first, Some(second)))
            }
        }
        _ => None,
    }
}

fn quoted_projection_segment(
    field: &[u8],
    offset: usize,
) -> Option<(ProjectionSegment<'_>, usize)> {
    if field.get(offset..offset + 2)? != b"['" {
        return None;
    }

    let start = offset + 2;
    let mut cursor = start;

    while cursor < field.len() {
        if field[cursor] != b'\'' {
            cursor += 1;
        } else if field.get(cursor + 1) == Some(&b'\'') {
            cursor += 2;
        } else if field.get(cursor + 1) == Some(&b']') {
            return Some((ProjectionSegment::Quoted(&field[start..cursor]), cursor + 2));
        } else {
            return None;
        }
    }

    None
}

fn same_projection_segment(left: ProjectionSegment<'_>, right: ProjectionSegment<'_>) -> bool {
    match (left, right) {
        (ProjectionSegment::Plain(left), ProjectionSegment::Plain(right))
        | (ProjectionSegment::Quoted(left), ProjectionSegment::Quoted(right)) => left == right,
        (ProjectionSegment::Plain(plain), ProjectionSegment::Quoted(quoted))
        | (ProjectionSegment::Quoted(quoted), ProjectionSegment::Plain(plain)) => {
            quoted_projection_segment_equals_plain(quoted, plain)
        }
    }
}

fn quoted_projection_segment_equals_plain(quoted: &[u8], plain: &[u8]) -> bool {
    let mut quoted_at = 0usize;
    let mut plain_at = 0usize;

    while quoted_at < quoted.len() && plain_at < plain.len() {
        if quoted[quoted_at] == b'\'' && quoted.get(quoted_at + 1) == Some(&b'\'') {
            quoted_at += 1;
        }

        if quoted[quoted_at] != plain[plain_at] {
            return false;
        }

        quoted_at += 1;
        plain_at += 1;
    }

    quoted_at == quoted.len() && plain_at == plain.len()
}

fn parse_limit(raw: &[u8]) -> Result<usize, ParseError> {
    let value = parse_i64(raw).ok_or(ParseError::UnsupportedQueryShape)?;

    if (1..=100).contains(&value) {
        Ok(value as usize)
    } else {
        Err(ParseError::UnsupportedQueryShape)
    }
}

fn validate_terminator(tokens: &[Token<'_>], query_len: usize) -> Result<(), ParseFailure> {
    match tokens.first().map(|token| &token.kind) {
        None => Ok(()),
        Some(TokenKind::Semicolon) if tokens.len() == 1 => Ok(()),
        Some(TokenKind::Semicolon) => Err(failure(
            ParseError::UnsupportedQueryShape,
            tokens,
            1,
            query_len,
        )),
        _ => Err(failure(
            ParseError::UnsupportedQueryShape,
            tokens,
            0,
            query_len,
        )),
    }
}

fn parse_field_tokens<'query>(
    tokens: &[Token<'query>],
    query_len: usize,
) -> Result<(Field<'query>, usize), ParseFailure> {
    let field_name = word_at(tokens, 0)
        .ok_or_else(|| failure(ParseError::UnsupportedQueryShape, tokens, 0, query_len))?;

    if keyword(field_name, b"STATE_META")
        && matches!(
            tokens.get(1).map(|token| &token.kind),
            Some(TokenKind::LeftBracket)
        )
    {
        let state = string_at(tokens, 2)
            .ok_or_else(|| failure(ParseError::UnsupportedQueryShape, tokens, 2, query_len))?;
        expect_kind(tokens, 3, &TokenKind::RightBracket, query_len)?;
        expect_kind(tokens, 4, &TokenKind::LeftBracket, query_len)?;
        let name = string_at(tokens, 5)
            .ok_or_else(|| failure(ParseError::UnsupportedQueryShape, tokens, 5, query_len))?;
        expect_kind(tokens, 6, &TokenKind::RightBracket, query_len)?;

        if !valid_state_name(state) {
            return Err(failure(ParseError::UnsupportedField, tokens, 2, query_len));
        }
        if !valid_metadata_key(name) {
            return Err(failure(ParseError::UnsupportedField, tokens, 5, query_len));
        }

        return Ok((
            Field {
                external_name: Cow::Owned(bracket_field(b"state_meta", &[state, name])),
                kind: FieldKind::StateMeta,
            },
            7,
        ));
    }

    if keyword(field_name, b"ATTRIBUTE")
        && matches!(
            tokens.get(1).map(|token| &token.kind),
            Some(TokenKind::LeftBracket)
        )
    {
        let name = string_at(tokens, 2)
            .ok_or_else(|| failure(ParseError::UnsupportedQueryShape, tokens, 2, query_len))?;
        expect_kind(tokens, 3, &TokenKind::RightBracket, query_len)?;

        if !valid_metadata_key(name) {
            return Err(failure(ParseError::UnsupportedField, tokens, 2, query_len));
        }

        return Ok((
            Field {
                external_name: Cow::Owned(bracket_field(b"attribute", &[name])),
                kind: FieldKind::Attribute,
            },
            4,
        ));
    }

    parse_field(field_name)
        .map(|field| (field, 1))
        .map_err(|reason| failure(reason, tokens, 0, query_len))
}

fn bracket_field(prefix: &[u8], segments: &[&Vec<u8>]) -> Vec<u8> {
    let extra = segments
        .iter()
        .map(|segment| segment.len() + 4)
        .sum::<usize>();
    let mut external = Vec::with_capacity(prefix.len() + extra);
    external.extend_from_slice(prefix);

    for segment in segments {
        external.extend_from_slice(b"['");
        for byte in segment.iter().copied() {
            external.push(byte);
            if byte == b'\'' {
                external.push(byte);
            }
        }
        external.extend_from_slice(b"']");
    }

    external
}

fn parse_field(input: &[u8]) -> Result<Field<'_>, ParseError> {
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
        external_name: Cow::Borrowed(input),
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

fn valid_metadata_key(name: &[u8]) -> bool {
    valid_quoted_name(name) && !name.starts_with(b"__")
}

fn valid_state_name(name: &[u8]) -> bool {
    valid_quoted_name(name)
}

fn valid_quoted_name(name: &[u8]) -> bool {
    !name.is_empty() && name.len() <= 64 && std::str::from_utf8(name).is_ok()
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

fn tokenize(query: &[u8]) -> Result<Vec<Token<'_>>, ParseFailure> {
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
            return Err(ParseFailure {
                reason: ParseError::UnsupportedQueryShape,
                byte: offset + 1,
            });
        }

        let start_offset = offset;
        match query[offset] {
            b'=' => {
                tokens.push(Token {
                    kind: TokenKind::Equals,
                    start: start_offset,
                });
                offset += 1;
            }
            b'(' => {
                tokens.push(Token {
                    kind: TokenKind::LeftParen,
                    start: start_offset,
                });
                offset += 1;
            }
            b')' => {
                tokens.push(Token {
                    kind: TokenKind::RightParen,
                    start: start_offset,
                });
                offset += 1;
            }
            b'[' => {
                tokens.push(Token {
                    kind: TokenKind::LeftBracket,
                    start: start_offset,
                });
                offset += 1;
            }
            b']' => {
                tokens.push(Token {
                    kind: TokenKind::RightBracket,
                    start: start_offset,
                });
                offset += 1;
            }
            b',' => {
                tokens.push(Token {
                    kind: TokenKind::Comma,
                    start: start_offset,
                });
                offset += 1;
            }
            b';' => {
                tokens.push(Token {
                    kind: TokenKind::Semicolon,
                    start: start_offset,
                });
                offset += 1;
            }
            b'\'' => {
                let (value, next_offset) =
                    take_string(query, offset + 1).map_err(|reason| ParseFailure {
                        reason,
                        byte: start_offset + 1,
                    })?;
                tokens.push(Token {
                    kind: TokenKind::String(value),
                    start: start_offset,
                });
                offset = next_offset;
            }
            b'@' => {
                let start = offset + 1;
                let end = take_identifier_end(query, start);
                if end == start {
                    return Err(ParseFailure {
                        reason: ParseError::InvalidSyntax,
                        byte: start_offset + 1,
                    });
                }
                tokens.push(Token {
                    kind: TokenKind::Parameter(&query[start..end]),
                    start: start_offset,
                });
                offset = end;
            }
            byte if byte.is_ascii_digit() || (byte == b'-' && offset + 1 < query.len()) => {
                let end = take_integer_end(query, offset);
                if end == offset || (end == offset + 1 && byte == b'-') {
                    return Err(ParseFailure {
                        reason: ParseError::InvalidSyntax,
                        byte: start_offset + 1,
                    });
                }
                tokens.push(Token {
                    kind: TokenKind::Integer(&query[offset..end]),
                    start: start_offset,
                });
                offset = end;
            }
            byte if is_word_start(byte) => {
                let end = take_identifier_end(query, offset);
                tokens.push(Token {
                    kind: TokenKind::Word(&query[offset..end]),
                    start: start_offset,
                });
                offset = end;
            }
            _ => {
                return Err(ParseFailure {
                    reason: ParseError::InvalidSyntax,
                    byte: start_offset + 1,
                });
            }
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
            ) if name.as_ref() == b"partition"
        ));
        assert!(matches!(
            &parsed.predicates[1],
            Predicate::Eq(
                Field {
                    kind: FieldKind::RunId,
                    ..
                },
                QueryValue::LiteralKeyword(value)
            ) if value.as_ref() == b"Run''42"
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
            ) if name.as_ref() == b"run_id"
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
    fn parses_explain_analyze_as_a_distinct_mode() {
        let parsed = parse(
            b"EXPLAIN ANALYZE FROM runs WHERE partition_key = @tenant ORDER BY updated_at_ms DESC LIMIT 10 RETURN RECORDS",
        )
        .expect("analyzed collection query parses");

        assert_eq!(parsed.mode, Mode::Analyze);
        assert_eq!(parsed.shape, Shape::Collection);
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
    fn parses_bounded_source_specific_return_projections() {
        let runs = parse(
            b"FROM runs WHERE run_id = @run_id RETURN RECORD (run_id, state, attributes, state_meta, attribute['customer'])",
        )
        .expect("run projection parses");

        let run_fields = runs
            .projection
            .expect("projection exists")
            .into_iter()
            .map(|field| field.external_name.into_owned())
            .collect::<Vec<_>>();

        assert_eq!(
            run_fields,
            vec![
                b"run_id".to_vec(),
                b"state".to_vec(),
                b"attributes".to_vec(),
                b"state_meta".to_vec(),
                b"attribute['customer']".to_vec()
            ]
        );

        let events = parse(
            b"FROM events WHERE run_id = @run_id ORDER BY event_id ASC LIMIT 10 RETURN RECORDS (event_id, fields['event'])",
        )
        .expect("event projection parses");

        let event_fields = events
            .projection
            .expect("projection exists")
            .into_iter()
            .map(|field| field.external_name.into_owned())
            .collect::<Vec<_>>();

        assert_eq!(
            event_fields,
            vec![b"event_id".to_vec(), b"fields['event']".to_vec()]
        );

        assert_eq!(
            parse(b"FROM runs WHERE run_id = 'run-1' RETURN RECORD (event_id)"),
            Err(ParseError::UnsupportedField)
        );
        assert_eq!(
            parse(b"FROM events WHERE run_id = 'run-1' ORDER BY event_id ASC LIMIT 1 RETURN RECORDS (state)"),
            Err(ParseError::UnsupportedField)
        );
        assert_eq!(
            parse(b"FROM runs WHERE run_id = 'run-1' RETURN RECORD (state, STATE)"),
            Err(ParseError::DuplicateProjectionField)
        );
        assert_eq!(
            parse(
                b"FROM runs WHERE run_id = 'run-1' RETURN RECORD (attribute.customer, attribute['customer'])"
            ),
            Err(ParseError::DuplicateProjectionField)
        );
        assert_eq!(
            parse(
                b"FROM runs WHERE run_id = 'run-1' RETURN RECORD (state_meta.review.owner, state_meta['review']['owner'])"
            ),
            Err(ParseError::DuplicateProjectionField)
        );
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
            Some(QueryValue::Parameter(ValueType::Keyword, name)) if name.as_ref() == b"page"
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

    #[test]
    fn enforces_predicate_budget_at_the_first_excess_predicate() {
        let exact = predicate_query(12);
        assert!(parse(&exact).is_ok());

        let over = predicate_query(13);
        let excess_byte = find_byte(&over, b"attribute.field12");
        assert_eq!(
            parse_with_diagnostic(&over),
            Err(ParseFailure {
                reason: ParseError::UnsupportedQueryShape,
                byte: excess_byte,
            })
        );
    }

    #[test]
    fn enforces_in_value_budget_at_the_first_excess_value() {
        let exact = in_query(20);
        assert!(parse(&exact).is_ok());

        let over = in_query(21);
        let excess_byte = find_byte(&over, b"'state20'");
        assert_eq!(
            parse_with_diagnostic(&over),
            Err(ParseFailure {
                reason: ParseError::UnsupportedQueryShape,
                byte: excess_byte,
            })
        );
    }

    #[test]
    fn enforces_return_projection_budget_at_the_first_excess_field() {
        let exact = projection_query(MAX_RETURN_FIELDS);
        assert!(parse(&exact).is_ok());

        let over = projection_query(MAX_RETURN_FIELDS + 1);
        let excess = format!("attribute['field-{}']", MAX_RETURN_FIELDS);
        let excess_byte = find_byte(&over, excess.as_bytes());

        assert_eq!(
            parse_with_diagnostic(&over),
            Err(ParseFailure {
                reason: ParseError::QueryProjectionLimitExceeded,
                byte: excess_byte,
            })
        );
    }

    #[test]
    fn diagnostics_report_the_actual_failure_token() {
        let missing_tail =
            b"FROM runs WHERE partition_key = 'p' AND attribute['customer.region'] = 'eu'";

        assert_eq!(
            parse_with_diagnostic(missing_tail),
            Err(ParseFailure {
                reason: ParseError::UnsupportedQueryShape,
                byte: missing_tail.len() + 1,
            })
        );

        let explain_cursor = b"EXPLAIN FROM runs WHERE partition_key = @tenant ORDER BY updated_at_ms DESC LIMIT 10 CURSOR @page RETURN RECORDS";
        let cursor_byte = explain_cursor
            .windows(b"CURSOR".len())
            .position(|window| window == b"CURSOR")
            .expect("cursor token exists")
            + 1;

        assert_eq!(
            parse_with_diagnostic(explain_cursor),
            Err(ParseFailure {
                reason: ParseError::QueryCursorInvalid,
                byte: cursor_byte,
            })
        );
    }

    fn predicate_query(predicates: usize) -> Vec<u8> {
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

    fn in_query(cardinality: usize) -> Vec<u8> {
        let values = (0..cardinality)
            .map(|index| format!("'state{index}'"))
            .collect::<Vec<_>>()
            .join(",");
        format!(
            "FROM runs WHERE partition_key = @partition AND state IN ({values}) ORDER BY updated_at_ms DESC LIMIT 25 RETURN RECORDS"
        )
        .into_bytes()
    }

    fn projection_query(cardinality: usize) -> Vec<u8> {
        let fields = (0..cardinality)
            .map(|index| format!("attribute['field-{index}']"))
            .collect::<Vec<_>>()
            .join(",");

        format!(
            "FROM runs WHERE partition_key = @partition ORDER BY updated_at_ms DESC LIMIT 25 RETURN RECORDS ({fields})"
        )
        .into_bytes()
    }

    fn find_byte(query: &[u8], needle: &[u8]) -> usize {
        query
            .windows(needle.len())
            .position(|window| window == needle)
            .expect("needle exists")
            + 1
    }
}
