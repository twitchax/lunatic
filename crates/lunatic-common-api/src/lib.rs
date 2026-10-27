use anyhow::{anyhow, Context, Result};
use std::{fmt::Display, future::Future, io::Write, pin::Pin};
use wasmtime::{Caller, Linker, Memory, Val, WasmRet, WasmTy, WasmTyList};

const ALLOCATOR_FUNCTION_NAME: &str = "lunatic_alloc";
const FREEING_FUNCTION_NAME: &str = "lunatic_free";

// Get exported memory
pub fn get_memory<T>(caller: &mut Caller<T>) -> Result<Memory> {
    caller
        .get_export("memory")
        .or_trap("No export `memory` found")?
        .into_memory()
        .or_trap("Export `memory` is not a memory")
}

// Call guest to allocate a Vec of size `size`
pub fn allocate_guest_memory<'a, T: Send>(
    caller: &'a mut Caller<T>,
    size: u32,
) -> Pin<Box<dyn Future<Output = Result<u32>> + Send + 'a>> {
    Box::pin(async move {
        let mut results = [Val::I32(0)];
        caller
            .get_export(ALLOCATOR_FUNCTION_NAME)
            .or_trap(format!("no export named {ALLOCATOR_FUNCTION_NAME} found"))?
            .into_func()
            .or_trap("cannot turn export into func")?
            .call_async(caller, &[Val::I32(size as i32)], &mut results)
            .await
            .or_trap(format!("failed to call {ALLOCATOR_FUNCTION_NAME}"))?;

        Ok(results[0]
            .i32()
            .or_trap(format!("result of {ALLOCATOR_FUNCTION_NAME} is not i32"))? as u32)
    })
}

// Call guest to free a slice of memory at location ptr
pub fn free_guest_memory<'a, T: Send>(
    caller: &'a mut Caller<T>,
    ptr: u32,
) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
    Box::pin(async move {
        let mut results = [];
        let result = caller
            .get_export(FREEING_FUNCTION_NAME)
            .or_trap(format!("no export named {FREEING_FUNCTION_NAME} found"))?
            .into_func()
            .or_trap("cannot turn export into func")?
            .call_async(caller, &[Val::I32(ptr as i32)], &mut results)
            .await;

        result.or_trap(format!("failed to call {FREEING_FUNCTION_NAME}"))?;
        Ok(())
    })
}

// Allocates and writes data to guest memory, updating the len_ptr and returning the allocated ptr.
pub async fn write_to_guest_vec<T: Send>(
    caller: &mut Caller<'_, T>,
    memory: &Memory,
    data: &[u8],
    len_ptr: u32,
) -> Result<u32> {
    let alloc_len = data.len();
    let alloc_ptr = allocate_guest_memory(caller, alloc_len as u32).await?;

    let (memory_slice, _) = memory.data_and_store_mut(&mut (*caller));
    let mut alloc_vec = memory_slice
        .get_mut(alloc_ptr as usize..(alloc_ptr as usize + alloc_len))
        .context("allocated memory does not exist")?;

    alloc_vec.write_all(data)?;

    memory.write(caller, len_ptr as usize, &alloc_len.to_le_bytes())?;

    Ok(alloc_ptr)
}

pub trait IntoTrap<T> {
    fn or_trap<S: Display>(self, info: S) -> Result<T>;
}

impl<T, E: Display> IntoTrap<T> for Result<T, E> {
    fn or_trap<S: Display>(self, info: S) -> Result<T> {
        match self {
            Ok(result) => Ok(result),
            Err(error) => Err(anyhow!(
                "Trap raised during host call: {} ({}).",
                error,
                info
            )),
        }
    }
}

impl<T> IntoTrap<T> for Option<T> {
    fn or_trap<S: Display>(self, info: S) -> Result<T> {
        match self {
            Some(result) => Ok(result),
            None => Err(anyhow!(
                "Trap raised during host call: Expected `Some({})` got `None` ({}).",
                std::any::type_name::<T>(),
                info
            )),
        }
    }
}

/// Wasmtime requires host functions to return a `wasmtime::Result`, whose error type is not
/// `anyhow::Error`. Lunatic's host functions are written against `anyhow`, so these adapters
/// convert the error once at the linker boundary instead of in every host function.
pub trait LinkerExt<T> {
    /// Like [`Linker::func_wrap`], but for a host function returning an `anyhow::Result`.
    fn func_wrap_anyhow<Params, R>(
        &mut self,
        module: &str,
        name: &str,
        func: impl AnyhowFunc<T, Params, R>,
    ) -> Result<&mut Self>;

    /// Like [`Linker::func_wrap_async`], but for an async host function returning an
    /// `anyhow::Result`. The function takes its arguments individually, not as a tuple.
    fn func_wrap_async_anyhow<Params, R>(
        &mut self,
        module: &str,
        name: &str,
        func: impl AnyhowAsyncFunc<T, Params, R>,
    ) -> Result<&mut Self>;
}

impl<T> LinkerExt<T> for Linker<T> {
    fn func_wrap_anyhow<Params, R>(
        &mut self,
        module: &str,
        name: &str,
        func: impl AnyhowFunc<T, Params, R>,
    ) -> Result<&mut Self> {
        func.wrap_into(self, module, name)?;
        Ok(self)
    }

    fn func_wrap_async_anyhow<Params, R>(
        &mut self,
        module: &str,
        name: &str,
        func: impl AnyhowAsyncFunc<T, Params, R>,
    ) -> Result<&mut Self> {
        func.wrap_into(self, module, name)?;
        Ok(self)
    }
}

/// A host function taking a [`Caller`] and returning an `anyhow::Result`.
pub trait AnyhowFunc<T, Params, R>: Send + Sync + 'static {
    fn wrap_into(self, linker: &mut Linker<T>, module: &str, name: &str) -> Result<()>;
}

/// An async host function taking a [`Caller`] and returning an `anyhow::Result`.
pub trait AnyhowAsyncFunc<T, Params, R>: Send + Sync + 'static {
    fn wrap_into(self, linker: &mut Linker<T>, module: &str, name: &str) -> Result<()>;
}

macro_rules! impl_anyhow_funcs {
    ($($args:ident)*) => {
        impl<T, F, $($args,)* R> AnyhowFunc<T, ($($args,)*), R> for F
        where
            T: 'static,
            F: Fn(Caller<'_, T>, $($args),*) -> Result<R> + Send + Sync + 'static,
            $($args: WasmTy,)*
            R: WasmRet,
        {
            #[allow(non_snake_case)]
            fn wrap_into(self, linker: &mut Linker<T>, module: &str, name: &str) -> Result<()> {
                linker.func_wrap(
                    module,
                    name,
                    move |caller: Caller<'_, T>, $($args: $args),*| -> wasmtime::Result<R> {
                        self(caller, $($args),*).map_err(wasmtime::Error::from_anyhow)
                    },
                )?;
                Ok(())
            }
        }

        impl<T, F, $($args,)* R> AnyhowAsyncFunc<T, ($($args,)*), R> for F
        where
            T: Send + 'static,
            F: for<'a> Fn(Caller<'a, T>, $($args),*) -> Box<dyn Future<Output = Result<R>> + Send + 'a>
                + Send
                + Sync
                + 'static,
            ($($args,)*): WasmTyList,
            R: WasmRet + 'static,
        {
            #[allow(non_snake_case)]
            fn wrap_into(self, linker: &mut Linker<T>, module: &str, name: &str) -> Result<()> {
                linker.func_wrap_async(module, name, move |caller, ($($args,)*)| {
                    let future = Box::into_pin(self(caller, $($args),*));
                    Box::new(async move { future.await.map_err(wasmtime::Error::from_anyhow) })
                })?;
                Ok(())
            }
        }
    };
}

impl_anyhow_funcs!();
impl_anyhow_funcs!(A1);
impl_anyhow_funcs!(A1 A2);
impl_anyhow_funcs!(A1 A2 A3);
impl_anyhow_funcs!(A1 A2 A3 A4);
impl_anyhow_funcs!(A1 A2 A3 A4 A5);
impl_anyhow_funcs!(A1 A2 A3 A4 A5 A6);
impl_anyhow_funcs!(A1 A2 A3 A4 A5 A6 A7);
impl_anyhow_funcs!(A1 A2 A3 A4 A5 A6 A7 A8);
impl_anyhow_funcs!(A1 A2 A3 A4 A5 A6 A7 A8 A9);
impl_anyhow_funcs!(A1 A2 A3 A4 A5 A6 A7 A8 A9 A10);
impl_anyhow_funcs!(A1 A2 A3 A4 A5 A6 A7 A8 A9 A10 A11);
impl_anyhow_funcs!(A1 A2 A3 A4 A5 A6 A7 A8 A9 A10 A11 A12);
