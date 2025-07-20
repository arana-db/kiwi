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


use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::quote_spanned;
use syn::spanned::Spanned;
use syn::{parenthesized, Ident, Variant};

#[derive(Clone, Debug)]
pub struct ErrorVariant {
    name: Ident,
    fields: Vec<Ident>,
    has_location: bool,
    has_source: bool,
    has_external_cause: bool,
    // Contents of the snafu display attribute
    display: TokenStream2,
    // The mechanism that the Rust compiler uses to track code location information
    span: Span,
}

impl ErrorVariant {
    pub fn from_enum_variant(variant: Variant) -> Self {
        let span = variant.span();
        let mut has_location = false;
        let mut has_source = false;
        let mut has_external_cause = false;

        for filed in &variant.fields {
            if let Some(ident) = &filed.ident {
                if ident == "location" {
                    has_location = true;
                } else if ident == "source" {
                    has_source = true;
                } else if ident == "error" {
                    has_external_cause = true;
                }
            }
        }

        let mut display = None;
        for attr in variant.attrs {
            if attr.path().is_ident("snafu") {
                attr.parse_nested_meta(|meta| {
                    // snafu(display = "{}"),Currently only handles display situations
                    if meta.path.is_ident("display") {
                        let content;
                        parenthesized!(content in meta.input);
                        let display_ts: TokenStream2 = content.parse()?;
                        display = Some(display_ts);
                        Ok(())
                    } else {
                        Err(meta.error("unrecognized repr"))
                    }
                })
                .unwrap_or_else(|e| panic!("{e}"));
            }
        }

        let display = display.unwrap_or_else(|| {
            panic!(
                r#"Error "{}" must be annotated with attribute "display"."#,
                variant.ident,
            )
        });

        let field_ident = variant
            .fields
            .iter()
            .map(|f| f.ident.clone().unwrap_or_else(|| Ident::new("_", f.span())))
            .collect();

        Self {
            name: variant.ident,
            fields: field_ident,
            has_location,
            has_source,
            has_external_cause,
            display,
            span,
        }
    }

    pub fn to_debug_match_arm(&self) -> TokenStream2 {
        let name = &self.name;
        let fields = &self.fields;
        let display = &self.display;

        match (self.has_location, self.has_source, self.has_external_cause) {
            (true, true, _) => quote_spanned! {
                self.span => #name { #(#fields),*, } => {
                    buf.push(format!("{layer}: {}, at {}", format!(#display), location));
                    source.debug_fmt(layer + 1, buf);
                },
            },
            (true, false, true) => quote_spanned! {
                self.span => #name { #(#fields),*, } => {
                    buf.push(format!("{layer}: {}, at {}", format!(#display), location));
                    buf.push(format!("{}: {:?}", layer + 1, error));
                },
            },
            (true, false, false) => quote_spanned! {
                self.span => #name { #(#fields),*, } => {
                    buf.push(format!("{layer}: {}, at {}", format!(#display), location));
                },
            },
            (false, true, _) => quote_spanned! {
                self.span => #name { #(#fields),*, } => {
                    buf.push(format!("{layer}: {}", format!(#display)));
                    source.debug_fmt(layer + 1, buf);
                },
            },
            (false, false, true) => quote_spanned! {
                self.span => #name { #(#fields),*, } => {
                    buf.push(format!("{layer}: {}", format!(#display)));
                    buf.push(format!("{}: {:?}", layer + 1, error));
                },
            },
            (false, false, false) => quote_spanned! {
                self.span => #name { #(#fields),*, } => {
                    buf.push(format!("{layer}: {}", format!(#display)));
                },
            },
        }
    }
}