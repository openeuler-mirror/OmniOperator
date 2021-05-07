/*
 * Copyright (C) 2018-2020. Huawei Technologies Co., Ltd. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#[macro_use]
extern crate cached;

use core::mem;
use std::alloc::GlobalAlloc;
use std::any::Any;
use std::collections::HashMap;
use std::convert::TryInto;
use std::ffi::c_void;
use std::fmt::Debug;
use std::fs::File;
use std::mem::ManuallyDrop;
use std::ops::Deref;
use std::os::raw::{c_int, c_long};
use std::ptr;
use std::ptr::null_mut;
use std::time::Instant;

use inkwell::context::Context;
use inkwell::execution_engine::JitFunction;
use jni::JNIEnv;
use jni::objects::{JByteBuffer, JClass, JObject, JString, JValue};
use jni::sys::{_jobject, jint, jintArray, jlong, jlongArray, jobject, jobjectArray, jshort, jstring};
use log::{debug, info, trace};

use omnicache::runtime::codegen::OmniCodeGen;

use crate::omnicache::runtime::filter::filter::FilterFuncType;
use crate::omnicache::runtime::table::{ColumnBuilder, Layout, Table};

mod omnicache;

//Use OmniMalloc for Rust Default Memory Manager
// #[global_allocator]
// static ALLOC: OmniMalloc = OmniMalloc;

#[link(name = "joy")]
extern "C" {
    //todo:new api need change call to omni_allocate
    pub fn call_allocate(size: c_long) -> *const c_void;
    //todo:new api need change call to omni_release
    pub fn call_release(address: c_long);
}

struct OmniMalloc;

unsafe impl GlobalAlloc for OmniMalloc {
    #[inline]
    unsafe fn alloc(&self, layout: std::alloc::Layout) -> *mut u8 {
        let ptr = call_allocate(layout.size() as i64);
        ptr as *mut u8
    }
    #[inline]
    unsafe fn dealloc(&self, ptr: *mut u8, layout: std::alloc::Layout) {
        call_release(ptr as _);
    }
}


#[no_mangle]
pub extern "system" fn Java_nova_hetu_omnicache_runtime_JniWrapper_filterCompile(
    env: JNIEnv,
    this_obj: jobject,
    j_filter_expression: JString,
    j_input_types: jlong,
    j_input_vec_count: jint,
) -> jlong {
    let filter_expression = get_str(env, j_filter_expression);
    // println!("expression: {}", filter_expression);
    unsafe {
        // TODO: the +3 is a hack here, find a better way to get the correct address
        let input_types = Vec::from_raw_parts((j_input_types) as *mut i32, j_input_vec_count as usize, j_input_vec_count as usize);
        // println!("input types: {:?}", input_types);
        let context = Context::create();
        let compile_result = OmniCodeGen::filter_compile(&context, filter_expression.as_str(), &input_types);
        mem::forget(input_types);

        return match compile_result {
            Ok(filter_function) => {
                let filter_ptr = Box::into_raw(Box::new(filter_function)) as _;
                filter_ptr
            }
            Err(err) => {
                println!("{}", err);
                0
            }
        };
    }
}

#[no_mangle]
pub extern "system" fn Java_nova_hetu_omnicache_runtime_JniWrapper_filterExecute(
    env: JNIEnv,
    this_obj: jobject,
    j_filter_ptr: jlong,
    j_input_data: jlongArray,
    j_input_types: jlong,
    j_input_vec_count: jint,
    j_selected_rows_address: jlong,
    j_input_row_number: jint,
) -> i32 {
    unsafe {
        let filter_module = j_filter_ptr as *mut JitFunction<'static, FilterFuncType>;
        let filter_module = &mut *filter_module;

        let row_number = j_input_row_number as usize;
        let column_number = j_input_vec_count as usize;

        let input_types = Vec::from_raw_parts((j_input_types) as *mut i32, column_number, column_number);
        let mut selected_rows = Vec::from_raw_parts(j_selected_rows_address as *mut i32, row_number, row_number);

        let mut vec_addrs = vec![0i64; column_number];
        env.get_long_array_region(j_input_data, 0, vec_addrs.as_mut());

        let mut columns: Vec<Box<dyn Any>> = Vec::new();
        for (i, mut addr) in vec_addrs.iter().enumerate() {
            let vec_type = input_types[i];
            // println!("input type: {}", vec_type);
            match vec_type {
                1 => {
                    // i32
                    let vec = Vec::from_raw_parts(*addr as *mut i32, row_number, row_number);
                    // println!("c1: {}", &vec[0]);
                    let column = ColumnBuilder::ColumnI32("c1", ManuallyDrop::new(vec));
                    columns.push(Box::new(column));
                }
                2 => {
                    // i64
                    let vec = Vec::from_raw_parts(*addr as *mut i64, row_number, row_number);
                    // println!("c2: {}", &vec[0]);
                    let column = ColumnBuilder::ColumnI64("c1", ManuallyDrop::new(vec));
                    columns.push(Box::new(column));
                }
                3 => {
                    // f64
                    let vec = Vec::from_raw_parts(*addr as *mut f64, row_number, row_number);
                    // println!("c3: {}", &vec[0]);
                    let column = ColumnBuilder::ColumnF64("c1", ManuallyDrop::new(vec));
                    columns.push(Box::new(column));
                }
                _ => {
                    panic!("Unsupported input type");
                }
            }
        }
        let table = Table::new("test_table", columns);
        let table_ptr = table.into_ffi_args();

        let mut index = 0;
        for row_index in 0..row_number {
            if filter_module.call(table_ptr.as_ptr() as *const c_void, row_index) {
                selected_rows[index] = row_index as i32;
                index += 1;
            }
        }

        //forget omni vector,not release by rust lifetime manager
        mem::forget(selected_rows);
        mem::forget(input_types);
        mem::forget(vec_addrs);

        index as i32
    }
}

#[no_mangle]
pub extern "system" fn Java_nova_hetu_omnicache_runtime_JniWrapper_filterExecuteV1(
    env: JNIEnv,
    this_obj: jobject,
    j_filter_ptr: jlong,
    j_input_data: jlongArray,
    j_input_types: jlong,
    j_input_vec_count: jint,
    j_input_row_number: jint,
    j_project_vec_address: jlongArray,
    j_project_idx: jintArray,
    j_project_vec_count: jint
) -> i32 {
    unsafe {
        let filter_module = j_filter_ptr as *mut JitFunction<'static, FilterFuncType>;
        let filter_module = &mut *filter_module;

        let row_number = j_input_row_number as usize;
        let column_number = j_input_vec_count as usize;

        let input_types = Vec::from_raw_parts((j_input_types) as *mut i32, column_number, column_number);
        let mut selected_rows = vec![-1i32; row_number];

        let mut vec_addrs = vec![0i64; column_number];
        env.get_long_array_region(j_input_data, 0, vec_addrs.as_mut());

        let mut columns: Vec<Box<dyn Any>> = Vec::new();
        for (i, mut addr) in vec_addrs.iter().enumerate() {
            let vec_type = input_types[i];
            // println!("input type: {}", vec_type);
            match vec_type {
                1 => {
                    // i32
                    let vec = Vec::from_raw_parts(*addr as *mut i32, row_number, row_number);
                    // println!("c1: {}", &vec[0]);
                    let column = ColumnBuilder::ColumnI32("c1", ManuallyDrop::new(vec));
                    columns.push(Box::new(column));
                }
                2 => {
                    // i64
                    let vec = Vec::from_raw_parts(*addr as *mut i64, row_number, row_number);
                    // println!("c2: {}", &vec[0]);
                    let column = ColumnBuilder::ColumnI64("c1", ManuallyDrop::new(vec));
                    columns.push(Box::new(column));
                }
                3 => {
                    // f64
                    let vec = Vec::from_raw_parts(*addr as *mut f64, row_number, row_number);
                    // println!("c3: {}", &vec[0]);
                    let column = ColumnBuilder::ColumnF64("c1", ManuallyDrop::new(vec));
                    columns.push(Box::new(column));
                }
                _ => {
                    panic!("Unsupported input type");
                }
            }
        }
        let table = Table::new("test_table", columns);
        let table_ptr = table.into_ffi_args();

        let mut index = 0;
        for row_index in 0..row_number {
            if filter_module.call(table_ptr.as_ptr() as *const c_void, row_index) {
                selected_rows[index] = row_index as i32;
                index += 1;
            }
        }

        // handle project
        let mut output_position = 0;
        let mut project_vec_count = j_project_vec_count as usize;
        let mut project_vec_addrs = vec![0i64; project_vec_count];
        env.get_long_array_region(j_project_vec_address, 0, project_vec_addrs.as_mut());
        let mut project_idx = vec![-1i32;project_vec_count];
        env.get_int_array_region(j_project_idx, 0, project_idx.as_mut());
        if index < row_number {
            for project_index in 0..project_idx.len() {
                // todo:handle different data type
                let mut input_vector = Vec::from_raw_parts(vec_addrs[project_idx[project_index as usize] as usize] as *mut i64, row_number, row_number);
                let mut copy_vector = Vec::from_raw_parts(project_vec_addrs[project_index] as *mut i64, index, index);
                for selected_index in 0..index {
                        let element = selected_rows[selected_index] as usize;
                    copy_vector[selected_index] = input_vector[element];
                }
                mem::forget(input_vector);
                mem::forget(copy_vector);
            }
        }

        //forget omni vector,not release by rust lifetime manager
        //mem::forget(selected_rows);
        mem::forget(project_vec_addrs);
        mem::forget(input_types);
        mem::forget(vec_addrs);

        index as i32
    }
}

#[no_mangle]
pub extern "system" fn Java_nova_hetu_omnicache_runtime_JniWrapper_filterFinished(env: JNIEnv,
                                                                                  this_obj: jobject,
                                                                                  j_filter_ptr: jlong) {
    unsafe {
        let filter_module = j_filter_ptr as *mut JitFunction<'static, FilterFuncType>;
        let filter_module = &mut *filter_module;
        //TODO:is need release some LLVM Jit Codegen Module Release?
    }
}

#[no_mangle]
pub extern "system" fn Java_nova_hetu_omnicache_vector_OMVectorBase_copy(
    env: JNIEnv,
    this_obj: jobject,
    j_type: jint,
    j_this_address: jlong,
    j_this_size: jint,
    j_other_address: jlong,
    j_elements_to_copy: jintArray,
    j_offset: jint,
    j_length: jint,
    j_this_offset: jint,
) {
    unsafe {
        let length = j_length as usize;
        let this_offset = j_this_offset as usize;

        let mut elements_to_copy = vec![0i32; length];
        env.get_int_array_region(j_elements_to_copy, j_offset, elements_to_copy.as_mut());

        // println!("lenght: {}, offset: {}, this_offset: {}, elements_to_copy: {}, this_size: {}", length, j_offset, this_offset, elements_to_copy.len(), j_this_size);

        match j_type {
            1 => {
                let mut this_vector = Vec::from_raw_parts((j_this_address) as *mut i32, j_this_size as usize, j_this_size as usize);
                let mut other_vector = Vec::from_raw_parts((j_other_address) as *mut i32, length, length);
                for i in 0..length {
                    let element = elements_to_copy[i] as usize;
                    other_vector[i] = this_vector[element + this_offset];
                }
                mem::forget(this_vector);
                mem::forget(other_vector);
            }
            2 => {
                let mut this_vector = Vec::from_raw_parts((j_this_address) as *mut i64, j_this_size as usize, j_this_size as usize);
                let mut other_vector = Vec::from_raw_parts((j_other_address) as *mut i64, length, length);
                for i in 0..length {
                    let element = elements_to_copy[i] as usize;
                    other_vector[i] = this_vector[element + this_offset];
                }
                mem::forget(this_vector);
                mem::forget(other_vector);
            }
            3 => {
                let mut this_vector = Vec::from_raw_parts((j_this_address) as *mut f64, j_this_size as usize, j_this_size as usize);
                let mut other_vector = Vec::from_raw_parts((j_other_address) as *mut f64, length, length);
                for i in 0..length {
                    let element = elements_to_copy[i] as usize;
                    other_vector[i] = this_vector[element + this_offset];
                }
                mem::forget(this_vector);
                mem::forget(other_vector);
            }
            _ => {
                panic!("Unsupported input type");
            }
        }
    }
}

fn build_om_result(env: JNIEnv, buf_array: *mut _jobject, output_len: i32, key: String) -> JObject {
    // todo need cache the jni info
    let om_result_cls = env
        .find_class("nova/hetu/omnicache/runtime/OMResult")
        .expect("find the class failed.");
    let j_om_result_obj = env
        .new_object(om_result_cls, "()V", &[])
        .expect("create failed.");
    env.call_method(
        j_om_result_obj,
        "setBuffers",
        "([Ljava/nio/ByteBuffer;)V",
        &[JValue::from(buf_array)],
    );
    env.call_method(
        j_om_result_obj,
        "setLength",
        "(I)V",
        &[JValue::from(output_len)],
    );
    let j_key = env.new_string(key).expect("get the key failed.");
    env.call_method(
        j_om_result_obj,
        "setKey",
        "(Ljava/lang/String;)V",
        &[JValue::from(j_key.into_inner())],
    );
    j_om_result_obj
}

fn get_int_array_elements(env: JNIEnv, array: jobjectArray) -> Vec<i32> {
    //dbg!(array);
    if array.is_null() {
        panic!("input data cannot be null or empty");
    }
    let len = env
        .get_array_length(array)
        .expect("get the type number failed");
    //dbg!(len);
    let mut buf = vec![-1; len as usize];
    let _ = env.get_int_array_region(array, 0, buf.as_mut());
    buf
}


unsafe fn add_buf_to_output<T: Clone>(
    env: JNIEnv,
    original: &mut Vec<T>,
    output: *mut _jobject,
    index: i32,
) {
    let mut res = into_u8(original.as_mut());
    let buf = env
        .new_direct_byte_buffer(res.as_mut())
        .expect("create direct buffer failed.");
    mem::forget(res);
    env.set_object_array_element(output, index, buf);
    mem::forget(buf);
}

fn get_str(env: JNIEnv, j_str: JString) -> String {
    env.get_string(j_str)
        .expect("couldn't get code from java.")
        .into()
}

unsafe fn transform_weld_to_vec<T>(result: *mut c_void, len: i64) -> Vec<T> {
    let result_ptr = result as *mut T;
    mem::forget(result);
    Vec::from_raw_parts(result_ptr, len as usize, len as usize)
}


unsafe fn transform_buf_to_vec<T: Clone>(rows: usize, buf: &[u8]) -> Vec<T> {
    if buf.len() % mem::size_of::<T>() != 0 {
        panic!("Misaligned vector size, cannot convert vector of size {} to vector with element size {}", buf.len(), mem::size_of::<T>());
    }

    let result = buf.as_ptr() as *mut T;
    mem::forget(buf);
    Vec::from_raw_parts(result, rows, rows)
}

/// Allocates an vector holds 1024 items of T
unsafe fn allocate_vec<T: Clone + Debug>(init: T) -> Vec<u8> {
    let mut vec = vec![init; 1024];
    let length = vec.len() * mem::size_of::<T>();
    let capacity = vec.capacity() * mem::size_of::<T>();
    let vec_ptr = vec.as_mut_ptr() as *mut u8;
    mem::forget(vec);
    Vec::from_raw_parts(vec_ptr, length, capacity)
}

/// Converts the [u8] array into a Vec[T]
/// This can be used to convert any JByteBuffer into target data types
unsafe fn into<T: Clone>(original: &mut [u8]) -> Vec<T> {
    if original.len() % mem::size_of::<T>() != 0 {
        panic!("Misaligned vector size, cannot convert vector of size {} to vector with element size {}", original.len(), mem::size_of::<T>());
    }

    let length = original.len() / mem::size_of::<T>();
    let capacity = length / mem::size_of::<T>();
    let result = original.as_mut_ptr() as *mut T;
    mem::forget(original); // don't run the destructor for vec32
    Vec::from_raw_parts(result, length, capacity)
}

unsafe fn into_u8<U: Clone>(original: &mut Vec<U>) -> Vec<u8> {
    let length = original.len() * mem::size_of::<U>();
    let capacity = original.capacity() * mem::size_of::<U>();
    let result = original.as_mut_ptr() as *mut u8;
    mem::forget(original);
    Vec::from_raw_parts(result, length, capacity)
}

