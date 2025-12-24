extern crate proc_macro;

use proc_macro::TokenStream;

use quote::quote;
use syn;

#[proc_macro_derive(UnitTask)]
pub fn unittask_trait_derive(input: TokenStream) -> TokenStream {
    // 解析输入的Rust代码
    let ast = syn::parse(input).unwrap();

    // 构建trait实现
    impl_my_trait(&ast)
}

fn impl_my_trait(ast: &syn::DeriveInput) -> TokenStream {
    let name = &ast.ident;
    let r#gen = quote! {
        // impl crate::task::UnitTask for #name {
        //     fn new(unit: ComputingUnit) -> Self {
        //         let mut task = #name::default();
        //         let mut core = TaskCore::new(unit.get_id());
        //         core.set_unit(unit.clone());
        //         task.core = core;
        //         task.init(&unit);
        //         task
        //     }
        //
        //     fn get_core(&self) -> &TaskCore {
        //         &self.core
        //     }
        // }

    };
    r#gen.into()
}

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
                context: *const fusion_unit_sdk::graph::types::Context,
            ) -> ::core::pin::Pin<Box<dyn ::core::future::Future<Output = fusion_unit_sdk::runtime::UnitResult<()>> + ::core::marker::Send + 'async_trait>>
            where
                'life0: 'async_trait,
                Self: 'async_trait,
            {
                let ctx = unsafe {Box::from_raw(context as *mut fusion_unit_sdk::graph::types::Context)};
                let arc_ctx = Arc::new(*ctx);
                let box_pin = Box::pin(async move {
                    let __self = self;
                    let () = {
                        let unit_id = arc_ctx.unit.get_id().clone();
                        __self.launch(arc_ctx.clone()).await;
                        arc_ctx.send(fusion_unit_sdk::proto::transfer::Row::eof(unit_id)).await;
                    };
                    Ok(())
               });
               box_pin
            }
        }
    } else {
        launch_impl_block = quote! {
            fn internal_launch<'life0, 'async_trait>(
                &'life0 self,
                context: *const fusion_unit_sdk::graph::types::Context,
            ) -> ::core::pin::Pin<Box<dyn ::core::future::Future<Output = fusion_unit_sdk::runtime::UnitResult<()>> + ::core::marker::Send + 'async_trait>>
            where
                'life0: 'async_trait,
                Self: 'async_trait,
            {
                Box::pin(async move {
                    Ok(())
                })
            }
        }
    }

    if tp & 0b10 == 0b10 {
        compute_impl_block = quote! {
            fn internal_compute<'life0, 'async_trait>(
                &'life0 self,
                row: *const fusion_unit_sdk::proto::transfer::Row,
                context: *const fusion_unit_sdk::graph::types::Context,
            ) -> ::core::pin::Pin<Box<dyn ::core::future::Future<Output = ()> + ::core::marker::Send + 'async_trait>>
            where
                'life0: 'async_trait,
                Self: 'async_trait,
            {
                let cloned_row = unsafe {(*row).clone()};
                let cloned_ctx = unsafe {(*context).clone()};
                Box::pin(async move {
                    self.compute(cloned_row, &cloned_ctx).await;
                })
            }
        };

        eof_callback = quote! {
            self.on_eof(row, ctx).await;
        };
    } else {
        compute_impl_block = quote! {
            fn internal_compute<'life0, 'async_trait>(
                &'life0 self,
                row: *const fusion_unit_sdk::proto::transfer::Row,
                context: *const fusion_unit_sdk::graph::types::Context,
            ) -> ::core::pin::Pin<Box<dyn ::core::future::Future<Output = ()> + ::core::marker::Send + 'async_trait>>
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
        impl fusion_unit_sdk::runtime::logical::LogicalTask for #struct_name {

            fn create(unit: fusion_unit_sdk::graph::types::ComputingUnit) -> Box<dyn fusion_unit_sdk::runtime::logical::LogicalTask + ::core::marker::Send>
            where
                Self: Sized
            {
                let mut instance: #struct_name = #struct_name::default();
                // must impl `InitUnit` trait
                instance.init(unit);
                Box::new(instance)
            }

            #launch_impl_block

            #compute_impl_block

            fn event<'life0, 'async_trait>(
                &'life0 self,
                event_type: i32,
                ctx: &'life0 fusion_unit_sdk::graph::types::Context,
                row: fusion_unit_sdk::proto::transfer::Row,
                args: Vec<&dyn std::any::Any>)
             -> ::core::pin::Pin<Box<dyn ::core::future::Future<Output = ()> + ::core::marker::Send + 'async_trait>>
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
