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
