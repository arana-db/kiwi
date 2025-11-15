# RESP Inline Commands Support

## Overview
The RESP parser now supports inline commands for easier debugging with telnet and TCL tests.

## Supported Formats

### Standard RESP Array Format
```
*2\r\n$4\r\nPING\r\n$4\r\ntest\r\n
```

### Inline Format (NEW)
```
PING\r\n
GET key\r\n
SET key value\r\n
HMGET fruit apple banana watermelon\r\n
```

## Usage with Telnet
```bash
telnet localhost 6379
PING
GET mykey
SET mykey myvalue
```

## Implementation
The inline command parser is implemented in `src/resp/src/parse.rs` in the `parse_inline()` function. It automatically detects inline format when the first byte is not a RESP type indicator (`+`, `-`, `:`, `$`, `*`, etc.).

## Testing
See tests in `src/resp/src/parse.rs`:
- `test_parse_inline()`
- `test_parse_inline_params()`
- `test_parse_multiple_inline()`
