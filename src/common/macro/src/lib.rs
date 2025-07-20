/*
 * Copyright (c) 2024-present, arana-db Community.  All rights reserved.
 * 
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


mod error_variant;
mod macro_stack_trace_debug;

use proc_macro::TokenStream;

/// Attribute macro to derive [std::fmt::Debug] for the annotated `Error` type.
///
/// The generated `Debug` implementation will print the error in a stack trace style. E.g.:
/// ```plaintext
/// 0: Foo error, at src/storage/src/error.rs:108:65
/// 1: Root cause, Os { code: 2, kind: NotFound, message: "No such file or directory" }
/// ```
///
/// Notes on using this macro:
/// - `#[snafu(display)]` must present on each enum variants,and should not include `location` and `source`.
/// - Only our internal error can be named `source`.All external error should be `error` with an `#[snafu(source)]` annotation.
#[proc_macro_attribute]
pub fn stack_trace_debug(args: TokenStream, input: TokenStream) -> TokenStream {
    macro_stack_trace_debug::stack_trace_style_impl(args.into(), input.into()).into()
}