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


use crate::error_variant::ErrorVariant;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{Ident, ItemEnum};

pub fn stack_trace_style_impl(args: TokenStream2, input: TokenStream2) -> TokenStream2 {
    let input_cloned: TokenStream2 = input.clone();

    let error_enum_definition: ItemEnum = syn::parse2(input_cloned).unwrap();
    let enum_name = error_enum_definition.ident;

    let mut variants = vec![];

    for error_variant in error_enum_definition.variants {
        let variant = ErrorVariant::from_enum_variant(error_variant);
        variants.push(variant);
    }

    let debug_fmt_fn = build_debug_fmt_impl(enum_name.clone(), variants.clone());
    let debug_impl = build_debug_impl(enum_name.clone());

    quote! {
        #args
        #input

        impl #enum_name {
            #debug_fmt_fn
        }

        #debug_impl
    }
}

fn build_debug_fmt_impl(enum_name: Ident, variants: Vec<ErrorVariant>) -> TokenStream2 {
    let match_arms = variants
        .iter()
        .map(|v| v.to_debug_match_arm())
        .collect::<Vec<_>>();

    quote! {
        fn debug_fmt(&self, layer: usize, buf: &mut Vec<String>) {
            use #enum_name::*;
            match self {
                #(#match_arms)*
            }
        }
    }
}

fn build_debug_impl(enum_name: Ident) -> TokenStream2 {
    quote! {
        impl std::fmt::Debug for #enum_name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                let mut buf = vec![];
                self.debug_fmt(0, &mut buf);
                write!(f, "{}", buf.join("\n"))
            }
        }
    }
}