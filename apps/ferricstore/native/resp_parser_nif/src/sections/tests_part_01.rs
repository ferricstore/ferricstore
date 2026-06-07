    use super::resp::*;
    use super::*;

    const MAX_SIZE: usize = 512 * 1024 * 1024; // 512 MB — generous limit for tests

    // Helper: parse single value, assert Ok, return (value, consumed_bytes)
    fn parse_ok(input: &[u8]) -> (RespValue<'_>, usize) {
        match parse_one_resp(input, 0, MAX_SIZE) {
            RespParseResult::Ok(v, pos) => (v, pos),
            other => panic!("expected Ok, got {:?}", other),
        }
    }

    // =========================================================================
    // parse_int_bytes
    // =========================================================================

    #[test]
    fn parse_int_positive() {
        assert_eq!(parse_int_bytes(b"123"), Some(123));
        assert_eq!(parse_int_bytes(b"0"), Some(0));
        assert_eq!(parse_int_bytes(b"1"), Some(1));
        assert_eq!(parse_int_bytes(b"9999999999"), Some(9_999_999_999));
    }

    #[test]
    fn parse_int_negative() {
        assert_eq!(parse_int_bytes(b"-1"), Some(-1));
        assert_eq!(parse_int_bytes(b"-123"), Some(-123));
        assert_eq!(parse_int_bytes(b"-0"), Some(0));
    }

    #[test]
    fn parse_int_explicit_positive_sign() {
        assert_eq!(parse_int_bytes(b"+42"), Some(42));
        assert_eq!(parse_int_bytes(b"+0"), Some(0));
    }

    #[test]
    fn parse_int_empty() {
        assert_eq!(parse_int_bytes(b""), None);
    }

    #[test]
    fn parse_int_sign_only() {
        assert_eq!(parse_int_bytes(b"-"), None);
        assert_eq!(parse_int_bytes(b"+"), None);
    }

    #[test]
    fn parse_int_non_digit() {
        assert_eq!(parse_int_bytes(b"12a3"), None);
        assert_eq!(parse_int_bytes(b"abc"), None);
        assert_eq!(parse_int_bytes(b"1.5"), None);
        assert_eq!(parse_int_bytes(b" 1"), None);
        assert_eq!(parse_int_bytes(b"1 "), None);
    }

    #[test]
    fn parse_int_i64_max() {
        // i64::MAX = 9223372036854775807
        assert_eq!(parse_int_bytes(b"9223372036854775807"), Some(i64::MAX));
    }

    #[test]
    fn parse_int_overflow() {
        // i64::MAX + 1 overflows
        assert_eq!(parse_int_bytes(b"9223372036854775808"), None);
        // Way beyond i64
        assert_eq!(parse_int_bytes(b"99999999999999999999"), None);
    }

    #[test]
    fn parse_int_negative_overflow() {
        assert_eq!(parse_int_bytes(b"-9223372036854775808"), Some(i64::MIN));
        assert_eq!(parse_int_bytes(b"-9223372036854775809"), None);
    }

    #[test]
    fn parse_int_leading_zeros() {
        assert_eq!(parse_int_bytes(b"007"), Some(7));
        assert_eq!(parse_int_bytes(b"00"), Some(0));
    }

    // =========================================================================
    // find_crlf
    // =========================================================================

    #[test]
    fn find_crlf_basic() {
        assert_eq!(find_crlf(b"hello\r\n", 0), Some((5, 7)));
        assert_eq!(find_crlf(b"OK\r\n", 0), Some((2, 4)));
    }

    #[test]
    fn find_crlf_at_start() {
        assert_eq!(find_crlf(b"\r\n", 0), Some((0, 2)));
        assert_eq!(find_crlf(b"\r\nrest", 0), Some((0, 2)));
    }

    #[test]
    fn find_crlf_with_offset() {
        assert_eq!(find_crlf(b"skip\r\n", 2), Some((4, 6)));
        assert_eq!(find_crlf(b"AB\r\nCD\r\n", 4), Some((6, 8)));
    }

    #[test]
    fn find_crlf_not_found() {
        assert_eq!(find_crlf(b"no crlf here", 0), None);
        assert_eq!(find_crlf(b"only\n", 0), None);
        assert_eq!(find_crlf(b"only\r", 0), None);
    }

    #[test]
    fn find_crlf_buffer_too_short() {
        assert_eq!(find_crlf(b"", 0), None);
        assert_eq!(find_crlf(b"x", 0), None);
        assert_eq!(find_crlf(b"ab", 1), None);
    }

    #[test]
    fn find_crlf_cr_without_lf() {
        assert_eq!(find_crlf(b"a\rb", 0), None);
        assert_eq!(find_crlf(b"\r\r\r", 0), None);
    }

    #[test]
    fn find_crlf_lf_without_cr() {
        assert_eq!(find_crlf(b"\n\n", 0), None);
        assert_eq!(find_crlf(b"a\nb\n", 0), None);
    }

    #[test]
    fn find_crlf_multiple_takes_first() {
        assert_eq!(find_crlf(b"a\r\nb\r\n", 0), Some((1, 3)));
    }

    #[test]
    fn find_crlf_start_beyond_buffer() {
        assert_eq!(find_crlf(b"ab\r\n", 10), None);
    }

    // =========================================================================
    // lossy_str
    // =========================================================================

    #[test]
    fn lossy_str_valid_utf8() {
        assert_eq!(lossy_str(b"hello"), "hello");
        assert_eq!(lossy_str(b""), "");
        assert_eq!(lossy_str(b"123"), "123");
    }

    #[test]
    fn lossy_str_invalid_utf8() {
        let result = lossy_str(&[0xFF, 0xFE, b'a']);
        assert!(result.contains('\u{FFFD}'));
        assert!(result.contains('a'));
    }

    #[test]
    fn lossy_str_mixed_valid_invalid() {
        let input = b"hello\xFFworld";
        let result = lossy_str(input);
        assert!(result.starts_with("hello"));
        assert!(result.ends_with("world"));
        assert!(result.contains('\u{FFFD}'));
    }

    // =========================================================================
    // MAX_ARRAY_COUNT constant
    // =========================================================================

    #[test]
    fn max_array_count_value() {
        assert_eq!(MAX_ARRAY_COUNT, 1_048_576);
    }

    // =========================================================================
    // Basic RESP types — parse_one_resp
    // =========================================================================

    #[test]
    fn bulk_string_basic() {
        let (val, pos) = parse_ok(b"$3\r\nfoo\r\n");
        assert_eq!(val, RespValue::BulkString(b"foo"));
        assert_eq!(pos, 9);
    }

    #[test]
    fn bulk_string_empty() {
        let (val, pos) = parse_ok(b"$0\r\n\r\n");
        assert_eq!(val, RespValue::BulkString(b""));
        assert_eq!(pos, 6);
    }

    #[test]
    fn bulk_string_nil() {
        let (val, _) = parse_ok(b"$-1\r\n");
        assert_eq!(val, RespValue::Nil);
    }

    #[test]
    fn bulk_string_binary_data() {
        // Bulk string containing bytes that are NOT valid utf-8
        let mut input = b"$4\r\n".to_vec();
        input.extend_from_slice(&[0x00, 0xFF, 0xFE, 0x01]);
        input.extend_from_slice(b"\r\n");
        let (val, pos) = parse_ok(&input);
        assert_eq!(val, RespValue::BulkString(&[0x00, 0xFF, 0xFE, 0x01]));
        assert_eq!(pos, input.len());
    }

    #[test]
    fn bulk_string_with_crlf_inside() {
        // Bulk string containing \r\n within its payload: "he\r\nlo" = 6 bytes
        let (val, pos) = parse_ok(b"$6\r\nhe\r\nlo\r\n");
        assert_eq!(val, RespValue::BulkString(b"he\r\nlo"));
        assert_eq!(pos, 12); // $6\r\n(4) + he\r\nlo(6) + \r\n(2) = 12
    }

    #[test]
    fn simple_string() {
        let (val, pos) = parse_ok(b"+OK\r\n");
        assert_eq!(val, RespValue::SimpleString(b"OK"));
        assert_eq!(pos, 5);
    }

    #[test]
    fn simple_string_empty() {
        let (val, _) = parse_ok(b"+\r\n");
        assert_eq!(val, RespValue::SimpleString(b""));
    }

    #[test]
    fn simple_string_with_spaces() {
        let (val, _) = parse_ok(b"+hello world\r\n");
        assert_eq!(val, RespValue::SimpleString(b"hello world"));
    }

    #[test]
    fn simple_error() {
        let (val, pos) = parse_ok(b"-ERR unknown command\r\n");
        assert_eq!(val, RespValue::SimpleError(b"ERR unknown command"));
        assert_eq!(pos, 22);
    }

    #[test]
    fn simple_error_empty() {
        let (val, _) = parse_ok(b"-\r\n");
        assert_eq!(val, RespValue::SimpleError(b""));
    }

    #[test]
    fn integer_positive() {
        let (val, pos) = parse_ok(b":42\r\n");
        assert_eq!(val, RespValue::Integer(42));
        assert_eq!(pos, 5);
    }

    #[test]
    fn integer_zero() {
        let (val, _) = parse_ok(b":0\r\n");
        assert_eq!(val, RespValue::Integer(0));
    }

    #[test]
    fn integer_negative() {
        let (val, _) = parse_ok(b":-1\r\n");
        assert_eq!(val, RespValue::Integer(-1));
    }

    #[test]
    fn integer_large() {
        let (val, _) = parse_ok(b":9223372036854775807\r\n");
        assert_eq!(val, RespValue::Integer(i64::MAX));
    }

    #[test]
    fn null_resp3() {
        let (val, pos) = parse_ok(b"_\r\n");
        assert_eq!(val, RespValue::Nil);
        assert_eq!(pos, 3);
    }

    #[test]
    fn array_empty() {
        let (val, pos) = parse_ok(b"*0\r\n");
        assert_eq!(val, RespValue::Array(vec![]));
        assert_eq!(pos, 4);
    }

    #[test]
    fn array_nil() {
        let (val, _) = parse_ok(b"*-1\r\n");
        assert_eq!(val, RespValue::Nil);
    }

    #[test]
    fn array_single_bulk_string() {
        let (val, _) = parse_ok(b"*1\r\n$4\r\nPING\r\n");
        assert_eq!(val, RespValue::Array(vec![RespValue::BulkString(b"PING")]));
    }

    #[test]
    fn array_mixed_types() {
        // Array with bulk string, integer, simple string
        let input = b"*3\r\n$3\r\nfoo\r\n:42\r\n+OK\r\n";
        let (val, _) = parse_ok(input);
        assert_eq!(
            val,
            RespValue::Array(vec![
                RespValue::BulkString(b"foo"),
                RespValue::Integer(42),
                RespValue::SimpleString(b"OK"),
            ])
        );
    }

    #[test]
    fn array_nested() {
        // *2\r\n *1\r\n$1\r\na\r\n *1\r\n$1\r\nb\r\n
        let input = b"*2\r\n*1\r\n$1\r\na\r\n*1\r\n$1\r\nb\r\n";
        let (val, _) = parse_ok(input);
        assert_eq!(
            val,
            RespValue::Array(vec![
                RespValue::Array(vec![RespValue::BulkString(b"a")]),
                RespValue::Array(vec![RespValue::BulkString(b"b")]),
            ])
        );
    }

    // =========================================================================
    // Full commands via parse_resp
    // =========================================================================

    #[test]
    fn full_command_get() {
        let input = b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n";
        let (cmds, consumed) = parse_resp(input, MAX_SIZE).unwrap();
        assert_eq!(consumed, input.len());
        assert_eq!(cmds.len(), 1);
        assert_eq!(
            cmds[0],
            RespValue::Array(vec![
                RespValue::BulkString(b"GET"),
                RespValue::BulkString(b"foo"),
            ])
        );
    }

    #[test]
    fn full_command_set() {
        let input = b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
        let (cmds, consumed) = parse_resp(input, MAX_SIZE).unwrap();
        assert_eq!(consumed, input.len());
        assert_eq!(cmds.len(), 1);
        assert_eq!(
            cmds[0],
            RespValue::Array(vec![
                RespValue::BulkString(b"SET"),
                RespValue::BulkString(b"foo"),
                RespValue::BulkString(b"bar"),
            ])
        );
    }

    #[test]
    fn multiple_commands_in_buffer() {
        // Two pipelined commands
        let input = b"*1\r\n$4\r\nPING\r\n*2\r\n$3\r\nGET\r\n$1\r\nk\r\n";
        let (cmds, consumed) = parse_resp(input, MAX_SIZE).unwrap();
        assert_eq!(consumed, input.len());
        assert_eq!(cmds.len(), 2);
        assert_eq!(
            cmds[0],
            RespValue::Array(vec![RespValue::BulkString(b"PING")])
        );
        assert_eq!(
            cmds[1],
            RespValue::Array(vec![
                RespValue::BulkString(b"GET"),
                RespValue::BulkString(b"k"),
            ])
        );
    }

    #[test]
    fn command_with_trailing_partial() {
        // One complete command + partial second command
        // *1\r\n$4\r\nPING\r\n = 4+4+4+2 = 14 bytes
        let input = b"*1\r\n$4\r\nPING\r\n*2\r\n$3\r\nGET\r\n";
        let (cmds, consumed) = parse_resp(input, MAX_SIZE).unwrap();
        assert_eq!(cmds.len(), 1); // only the PING command
        assert_eq!(consumed, 14); // consumed the first command
        assert_eq!(
            cmds[0],
            RespValue::Array(vec![RespValue::BulkString(b"PING")])
        );
    }

    // =========================================================================
    // Inline commands
    // =========================================================================

    #[test]
    fn inline_ping() {
        let (val, pos) = parse_ok(b"PING\r\n");
        assert_eq!(val, RespValue::Inline(vec![b"PING".as_slice()]));
        assert_eq!(pos, 6);
    }

    #[test]
    fn inline_set_with_args() {
        let (val, _) = parse_ok(b"SET foo bar\r\n");
        assert_eq!(
            val,
            RespValue::Inline(vec![
                b"SET".as_slice(),
                b"foo".as_slice(),
                b"bar".as_slice(),
            ])
        );
    }

    #[test]
    fn inline_extra_whitespace() {
        let (val, _) = parse_ok(b"SET  foo \t bar\r\n");
        assert_eq!(
            val,
            RespValue::Inline(vec![
                b"SET".as_slice(),
                b"foo".as_slice(),
                b"bar".as_slice(),
            ])
        );
    }

    #[test]
    fn inline_via_parse_resp() {
        let input = b"PING\r\n";
        let (cmds, consumed) = parse_resp(input, MAX_SIZE).unwrap();
        assert_eq!(consumed, input.len());
        assert_eq!(cmds.len(), 1);
        assert_eq!(cmds[0], RespValue::Inline(vec![b"PING".as_slice()]));
    }

    #[test]
    fn inline_multiple_commands() {
        let input = b"PING\r\nINFO\r\n";
        let (cmds, consumed) = parse_resp(input, MAX_SIZE).unwrap();
        assert_eq!(consumed, input.len());
        assert_eq!(cmds.len(), 2);
        assert_eq!(cmds[0], RespValue::Inline(vec![b"PING".as_slice()]));
        assert_eq!(cmds[1], RespValue::Inline(vec![b"INFO".as_slice()]));
    }

    // =========================================================================
    // Incomplete messages
    // =========================================================================

    #[test]
    fn incomplete_empty_input() {
        let (cmds, consumed) = parse_resp(b"", MAX_SIZE).unwrap();
        assert_eq!(cmds.len(), 0);
        assert_eq!(consumed, 0);
    }

    #[test]
    fn incomplete_partial_bulk_header() {
        // "$3\r\n" without the data
        let result = parse_one_resp(b"$3\r\n", 0, MAX_SIZE);
        assert_eq!(result, RespParseResult::Incomplete);
    }

    #[test]
    fn incomplete_partial_bulk_data() {
        // "$3\r\nfo" — data truncated
        let result = parse_one_resp(b"$3\r\nfo", 0, MAX_SIZE);
        assert_eq!(result, RespParseResult::Incomplete);
    }

    #[test]
    fn incomplete_partial_bulk_no_trailing_crlf() {
        // "$3\r\nfoo" — data present but missing trailing \r\n
        let result = parse_one_resp(b"$3\r\nfoo", 0, MAX_SIZE);
        assert_eq!(result, RespParseResult::Incomplete);
    }

    #[test]
    fn incomplete_partial_array_header() {
        // "*2\r\n" without any elements
        let result = parse_one_resp(b"*2\r\n", 0, MAX_SIZE);
        assert_eq!(result, RespParseResult::Incomplete);
    }

    #[test]
    fn incomplete_partial_array_one_of_two() {
        // "*2\r\n$3\r\nfoo\r\n" — first element present, second missing
        let result = parse_one_resp(b"*2\r\n$3\r\nfoo\r\n", 0, MAX_SIZE);
        assert_eq!(result, RespParseResult::Incomplete);
    }

    #[test]
    fn incomplete_truncated_header_no_crlf() {
        let result = parse_one_resp(b"$3", 0, MAX_SIZE);
        assert_eq!(result, RespParseResult::Incomplete);
    }

    #[test]
    fn incomplete_just_type_byte() {
        assert_eq!(
            parse_one_resp(b"*", 0, MAX_SIZE),
            RespParseResult::Incomplete
        );
        assert_eq!(
            parse_one_resp(b"$", 0, MAX_SIZE),
            RespParseResult::Incomplete
        );
        assert_eq!(
            parse_one_resp(b"+", 0, MAX_SIZE),
            RespParseResult::Incomplete
        );
        assert_eq!(
            parse_one_resp(b"-", 0, MAX_SIZE),
            RespParseResult::Incomplete
        );
        assert_eq!(
            parse_one_resp(b":", 0, MAX_SIZE),
            RespParseResult::Incomplete
        );
        assert_eq!(
            parse_one_resp(b"_", 0, MAX_SIZE),
            RespParseResult::Incomplete
        );
    }

    #[test]
    fn incomplete_simple_string_no_crlf() {
        let result = parse_one_resp(b"+OK", 0, MAX_SIZE);
        assert_eq!(result, RespParseResult::Incomplete);
    }

    #[test]
    fn incomplete_integer_no_crlf() {
        let result = parse_one_resp(b":42", 0, MAX_SIZE);
        assert_eq!(result, RespParseResult::Incomplete);
    }

    #[test]
    fn incomplete_inline_no_crlf() {
        let result = parse_one_resp(b"PING", 0, MAX_SIZE);
        assert_eq!(result, RespParseResult::Incomplete);
    }

    #[test]
    fn incomplete_preserves_rest_in_parse_resp() {
        // One complete command + incomplete fragment
        let input = b"+OK\r\n$3\r\nfo";
        let (cmds, consumed) = parse_resp(input, MAX_SIZE).unwrap();
        assert_eq!(cmds.len(), 1);
        assert_eq!(consumed, 5); // only "+OK\r\n"
        assert_eq!(cmds[0], RespValue::SimpleString(b"OK"));
    }

    // =========================================================================
    // Malformed input
    // =========================================================================

    #[test]
    fn malformed_invalid_array_count() {
        let result = parse_one_resp(b"*abc\r\n", 0, MAX_SIZE);
        assert_eq!(
            result,
            RespParseResult::Error(RespError::InvalidArrayCount("abc".into()))
        );
    }

    #[test]
    fn malformed_negative_array_count() {
        // *-2 is invalid (only *-1 for nil array)
        let result = parse_one_resp(b"*-2\r\n", 0, MAX_SIZE);
        assert_eq!(
            result,
            RespParseResult::Error(RespError::InvalidArrayCount("-2".into()))
        );
    }

    #[test]
    fn malformed_array_too_large() {
        // MAX_ARRAY_COUNT + 1
        let input = format!("*{}\r\n", MAX_ARRAY_COUNT + 1);
        let result = parse_one_resp(input.as_bytes(), 0, MAX_SIZE);
        assert_eq!(result, RespParseResult::Error(RespError::ArrayTooLarge));
    }

    #[test]
    fn malformed_bulk_string_exceeds_max_value_size() {
        // max_value_size = 10, but bulk string claims length 100
        let result = parse_one_resp(b"$100\r\n", 0, 10);
        assert_eq!(
            result,
            RespParseResult::Error(RespError::ValueTooLarge { len: 100, max: 10 })
        );
    }

    #[test]
    fn malformed_invalid_bulk_length() {
        let result = parse_one_resp(b"$xyz\r\n", 0, MAX_SIZE);
        assert_eq!(
            result,
            RespParseResult::Error(RespError::InvalidBulkLength("xyz".into()))
        );
    }

    #[test]
    fn malformed_negative_bulk_length() {
        // $-2 is invalid (only $-1 for nil)
        let result = parse_one_resp(b"$-2\r\n", 0, MAX_SIZE);
        assert_eq!(
            result,
            RespParseResult::Error(RespError::InvalidBulkLength("-2".into()))
        );
    }

    #[test]
    fn malformed_bulk_string_missing_crlf_terminator() {
        // Data present but \r\n replaced with something else
        let result = parse_one_resp(b"$3\r\nfooXY", 0, MAX_SIZE);
        assert_eq!(result, RespParseResult::Error(RespError::BulkCrlfMissing));
    }

    #[test]
    fn malformed_invalid_integer() {
        let result = parse_one_resp(b":abc\r\n", 0, MAX_SIZE);
        assert_eq!(
            result,
            RespParseResult::Error(RespError::InvalidInteger("abc".into()))
        );
    }

    #[test]
    fn malformed_invalid_null() {
        // "_" should be followed immediately by \r\n, not extra data
        let result = parse_one_resp(b"_extra\r\n", 0, MAX_SIZE);
        assert_eq!(
            result,
            RespParseResult::Error(RespError::InvalidNull("extra".into()))
        );
    }

    #[test]
    fn malformed_error_propagates_through_parse_resp() {
        let result = parse_resp(b"$xyz\r\n", MAX_SIZE);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            RespError::InvalidBulkLength("xyz".into())
        );
    }

    // =========================================================================
    // Edge cases
    // =========================================================================

    #[test]
    fn edge_bulk_string_large_valid() {
        let data = vec![b'x'; 1000];
        let mut input = format!("${}\r\n", data.len()).into_bytes();
        input.extend_from_slice(&data);
        input.extend_from_slice(b"\r\n");
        let (val, pos) = parse_ok(&input);
        assert_eq!(val, RespValue::BulkString(&data));
        assert_eq!(pos, input.len());
    }

    #[test]
    fn edge_array_with_nil_elements() {
        // Array containing nil bulk strings
        let input = b"*2\r\n$-1\r\n$-1\r\n";
        let (val, _) = parse_ok(input);
        assert_eq!(val, RespValue::Array(vec![RespValue::Nil, RespValue::Nil]));
    }

    #[test]
    fn edge_position_tracking_across_multiple() {
        // Verify parse_resp consumes exactly the right number of bytes
        let input = b":1\r\n:2\r\n:3\r\n";
        let (cmds, consumed) = parse_resp(input, MAX_SIZE).unwrap();
        assert_eq!(consumed, input.len());
        assert_eq!(cmds.len(), 3);
        assert_eq!(cmds[0], RespValue::Integer(1));
        assert_eq!(cmds[1], RespValue::Integer(2));
        assert_eq!(cmds[2], RespValue::Integer(3));
    }

    #[test]
    fn edge_bulk_string_with_zero_bytes() {
        // Bulk string containing null bytes
        let mut input = b"$3\r\n".to_vec();
        input.extend_from_slice(&[0x00, 0x00, 0x00]);
        input.extend_from_slice(b"\r\n");
        let (val, _) = parse_ok(&input);
        assert_eq!(val, RespValue::BulkString(&[0x00, 0x00, 0x00]));
    }

    #[test]
    fn edge_max_value_size_boundary() {
        // Exactly at the limit should succeed
        let data = vec![b'a'; 100];
        let mut input = format!("${}\r\n", data.len()).into_bytes();
        input.extend_from_slice(&data);
        input.extend_from_slice(b"\r\n");
        let (val, _) = match parse_one_resp(&input, 0, 100) {
            RespParseResult::Ok(v, p) => (v, p),
            other => panic!("expected Ok, got {:?}", other),
        };
        assert_eq!(val, RespValue::BulkString(data.as_slice()));

        // One over the limit should fail
        let data2 = vec![b'a'; 101];
        let mut input2 = format!("${}\r\n", data2.len()).into_bytes();
        input2.extend_from_slice(&data2);
        input2.extend_from_slice(b"\r\n");
        let result = parse_one_resp(&input2, 0, 100);
        assert_eq!(
            result,
            RespParseResult::Error(RespError::ValueTooLarge { len: 101, max: 100 })
        );
    }

    #[test]
    fn edge_array_at_max_count_boundary() {
        // Array count exactly at MAX_ARRAY_COUNT should be accepted (structurally)
        // but will be Incomplete since we don't provide the elements
        let input = format!("*{}\r\n", MAX_ARRAY_COUNT);
        let result = parse_one_resp(input.as_bytes(), 0, MAX_SIZE);
        assert_eq!(result, RespParseResult::Incomplete);

        // One over should be rejected immediately
        let input2 = format!("*{}\r\n", MAX_ARRAY_COUNT + 1);
        let result2 = parse_one_resp(input2.as_bytes(), 0, MAX_SIZE);
        assert_eq!(result2, RespParseResult::Error(RespError::ArrayTooLarge));
    }

    #[test]
    fn edge_deeply_nested_array() {
        // *1\r\n *1\r\n *1\r\n $1\r\na\r\n
        let input = b"*1\r\n*1\r\n*1\r\n$1\r\na\r\n";
        let (val, _) = parse_ok(input);
        assert_eq!(
            val,
            RespValue::Array(vec![RespValue::Array(vec![RespValue::Array(vec![
                RespValue::BulkString(b"a")
            ])])])
        );
    }

    #[test]
    fn edge_nested_array_at_depth_limit() {
        let input = nested_array(MAX_RESP_NESTING_DEPTH);
        let result = parse_one_resp(&input, 0, MAX_SIZE);
        assert!(matches!(result, RespParseResult::Ok(_, _)));
    }

    #[test]
    fn malformed_nested_array_exceeds_depth_limit() {
        let input = nested_array(MAX_RESP_NESTING_DEPTH + 1);
        let result = parse_one_resp(&input, 0, MAX_SIZE);
        assert_eq!(result, RespParseResult::Error(RespError::NestingTooDeep));
    }

    #[test]
    fn edge_array_containing_error_element() {
        let input = b"*2\r\n$3\r\nfoo\r\n-ERR bad\r\n";
        let (val, _) = parse_ok(input);
        assert_eq!(
            val,
            RespValue::Array(vec![
                RespValue::BulkString(b"foo"),
                RespValue::SimpleError(b"ERR bad"),
            ])
        );
    }

    #[test]
    fn edge_inline_single_char() {
        let (val, _) = parse_ok(b"Q\r\n");
        assert_eq!(val, RespValue::Inline(vec![b"Q".as_slice()]));
    }

    #[test]
    fn edge_parse_resp_empty_yields_no_commands() {
        let (cmds, consumed) = parse_resp(b"", MAX_SIZE).unwrap();
        assert_eq!(cmds.len(), 0);
        assert_eq!(consumed, 0);
    }

