extern crate proc_macro;

use proc_macro::TokenStream;

use quote::quote;
use syn;
use syn::{parse_macro_input, DeriveInput, Expr, Lit, Meta};

#[proc_macro_derive(LogicalTask)]
pub fn logical_task_derive(input: TokenStream) -> TokenStream {
    let ast = syn::parse(input).unwrap();
    impl_logical_task_trait(&ast, 0b11)
}

#[proc_macro_derive(SrcLogicTask)]
pub fn source_logical_task_derive(input: TokenStream) -> TokenStream {
    let ast = syn::parse(input).unwrap();
    impl_logical_task_trait(&ast, 0b01)
}

#[proc_macro_derive(MapLogicTask)]
pub fn map_logical_task_trait(input: TokenStream) -> TokenStream {
    let ast = syn::parse(input).unwrap();
    impl_logical_task_trait(&ast, 0b10)
}

#[proc_macro_derive(SinkLogicTask)]
pub fn sink_logical_task_derive(input: TokenStream) -> TokenStream {
    let ast = syn::parse(input).unwrap();
    impl_logical_task_trait(&ast, 0b10)
}

fn impl_logical_task_trait(ast: &syn::DeriveInput, tp: i32) -> TokenStream {
    let struct_name = &ast.ident;

    let mut launch_impl_block;
    let mut compute_impl_block;
    let mut eof_callback;
    if tp & 0b01 == 0b01 {
        launch_impl_block = quote! {
            fn internal_launch<'life0, 'async_trait>(
                &'life0 self,
                context: *const fusion_unit_sdk::graph::types::TaskContext,
            ) -> anyhow::Result<::core::pin::Pin<Box<dyn ::core::future::Future<Output = fusion_unit_sdk::runtime::UnitResult<()>> + ::core::marker::Send + 'async_trait>>>
            where
                'life0: 'async_trait,
                Self: 'async_trait,
            {
                let ctx = unsafe {Box::from_raw(context as *mut fusion_unit_sdk::graph::types::TaskContext)};
                let arc_ctx = Arc::new(*ctx);
                let box_pin = Box::pin(async move {
                    let __self = self;
                    let unit_id = arc_ctx.unit.get_id().clone();
                    let result = __self.launch(arc_ctx.clone())?.await;
                    // Always send EOF so downstream tasks can terminate,
                    // even if launch returned an error.
                    arc_ctx.send(fusion_unit_sdk::proto::transfer::Row::eof(unit_id)).await;
                    result
               });
               Ok(box_pin)
            }
        }
    } else {
        launch_impl_block = quote! {
            fn internal_launch<'life0, 'async_trait>(
                &'life0 self,
                context: *const fusion_unit_sdk::graph::types::TaskContext,
            ) -> anyhow::Result<::core::pin::Pin<Box<dyn ::core::future::Future<Output = fusion_unit_sdk::runtime::UnitResult<()>> + ::core::marker::Send + 'async_trait>>>
            where
                'life0: 'async_trait,
                Self: 'async_trait,
            {
                Ok(Box::pin(async move {
                    Ok(())
                }))
            }
        }
    }

    if tp & 0b10 == 0b10 {
        compute_impl_block = quote! {
            fn internal_compute<'life0, 'async_trait>(
                &'life0 self,
                row: *const fusion_unit_sdk::proto::transfer::Row,
                context: *const fusion_unit_sdk::graph::types::TaskContext,
            ) -> anyhow::Result<::core::pin::Pin<Box<dyn ::core::future::Future<Output = fusion_unit_sdk::runtime::UnitResult<()>> + ::core::marker::Send + 'async_trait>>>
            where
                'life0: 'async_trait,
                Self: 'async_trait,
            {
                let cloned_row = unsafe {(*row).clone()};
                let cloned_ctx = unsafe {(*context).clone()};
                Ok(Box::pin(async move {
                    match self.compute(cloned_row, &cloned_ctx) {
                        Ok(f) => f.await,
                        Err(err) => {
                            Err(fusion_unit_sdk::runtime::UnitError::Unknown(String::from("compute error")))
                        }
                    }
                }))
            }
        };

        eof_callback = quote! {
            self.on_eof(row, ctx)?.await?
        };
    } else {
        compute_impl_block = quote! {
            fn internal_compute<'life0, 'async_trait>(
                &'life0 self,
                row: *const fusion_unit_sdk::proto::transfer::Row,
                context: *const fusion_unit_sdk::graph::types::TaskContext,
            ) -> anyhow::Result<::core::pin::Pin<Box<dyn ::core::future::Future<Output = fusion_unit_sdk::runtime::UnitResult<()>> + ::core::marker::Send + 'async_trait>>>
            where
                'life0: 'async_trait,
                Self: 'async_trait,
            {
                unimplemented!();
            }
        };

        eof_callback = quote! {
            unimplemented!();
        };
    }

    let start_callback = quote! {
        self.on_start().await;
    };

    let token_stream = quote! {
        impl fusion_unit_sdk::runtime::logical::LogicalTaskMeta for #struct_name {
            fn get_id(&self) -> String {
                self.meta.get_id()
            }

            fn set_id(&mut self, id: fusion_unit_sdk::graph::types::UnitIdx) {
                self.meta.set_id(id);
            }
        }

        impl fusion_unit_sdk::runtime::logical::LogicalTask for #struct_name {

            fn create(unit: fusion_unit_sdk::graph::types::ComputingUnit) -> fusion_unit_sdk::runtime::UnitResult<Box<dyn fusion_unit_sdk::runtime::logical::LogicalTask + ::core::marker::Send>>
            where
                Self: Sized
            {
                let mut instance: #struct_name = #struct_name::default();
                // must impl `InitUnit` trait
                instance.init(unit)?;
                Ok(Box::new(instance))
            }

            #launch_impl_block

            #compute_impl_block

            fn event<'life0, 'async_trait>(
                &'life0 self,
                event_type: i32,
                ctx: &'life0 fusion_unit_sdk::graph::types::TaskContext,
                row: fusion_unit_sdk::proto::transfer::Row,
                args: Vec<&dyn std::any::Any>)
             -> ::core::pin::Pin<Box<dyn ::core::future::Future<Output = fusion_unit_sdk::runtime::UnitResult<()>> + ::core::marker::Send + 'async_trait>>
            where
                'life0: 'async_trait,
                Self: 'async_trait,
            {
                Box::pin(async move {
                    match event_type {
                        1 => {
                            // EOF event
                            #eof_callback
                        },
                        2 => {
                            // Task start event
                            #start_callback
                        },
                        _ => unreachable!()
                    }
                    Ok(())
                })
            }
        }

        impl #struct_name {
            pub fn register_unit(manifest: &mut fusion_unit_sdk::UnitManifest, version: &str) {
                let unit_name = stringify!(#struct_name);
                fusion_unit_sdk::runtime::GLOBAL_REGISTRY.register::<#struct_name>(unit_name);
                let key = format!("{}#{}", unit_name, version);
                manifest.add(key);
            }
        }
    };
    token_stream.into()
}

#[proc_macro_derive(ScriptEngine, attributes(script_type))]
pub fn derive_factory(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    let struct_name = &input.ident;

    // 获取 script_type 属性值
    let script_type =
        get_script_type_name(&input).unwrap_or_else(|| "struct_name.to_string()".to_string());

    let expanded = quote! {
        // 自动实现 Factory trait
        impl fusion_unit_sdk::runtime::script_engine_factory::ScriptEngineFactory for #struct_name {
            fn name() -> &'static str {
                #script_type
            }

            fn create_scripter(origin_script: String, states: fusion_unit_sdk::runtime::state::GraphStates) -> anyhow::Result<Box<dyn fusion_unit_sdk::runtime::script::Scripter + Send>>
            where
                Self: Sized + 'static,
            {
                Self::create(origin_script, states)
            }
        }
    };

    TokenStream::from(expanded)
}

fn get_script_type_name(input: &DeriveInput) -> Option<String> {
    for attr in &input.attrs {
        if !attr.path().is_ident("script_type") {
            continue;
        }

        match &attr.meta {
            // #[script_type = "value"]
            Meta::NameValue(name_value) => {
                if let Expr::Lit(expr_lit) = &name_value.value {
                    if let Lit::Str(lit_str) = &expr_lit.lit {
                        return Some(lit_str.value());
                    }
                }
            }
            // #[script_type("value")]
            Meta::List(meta_list) => {
                // 直接解析 token 流
                let tokens = meta_list.tokens.clone();
                let mut iter = tokens.into_iter();

                // 跳过可能的 punct 和 group
                while let Some(token) = iter.next() {
                    match token {
                        proc_macro2::TokenTree::Literal(lit) => {
                            let s = lit.to_string();
                            if s.starts_with('"') && s.ends_with('"') {
                                return Some(s[1..s.len() - 1].to_string());
                            }
                        }
                        proc_macro2::TokenTree::Punct(p) if p.as_char() == '=' => {
                            // 跳过等号，继续查找字面量
                            continue;
                        }
                        _ => {}
                    }
                }
            }
            // #[script_type]
            Meta::Path(_) => {
                // 没有值，返回 None 让调用者使用默认值
                return None;
            }
        }
    }
    None
}
