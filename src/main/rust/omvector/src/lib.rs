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
use std::convert::TryInto;
use std::ffi::c_void;
use std::fmt::Debug;
use std::ops::Deref;
use std::os::raw::{c_int, c_long};
use std::ptr::null_mut;

use jni::JNIEnv;
use jni::objects::{JByteBuffer, JClass, JObject, JString, JValue};
use jni::sys::{_jobject, jint, jintArray, jlong, jobject, jobjectArray, jstring};
use log::{debug, info, trace};
use weld::{Data, WeldValue};

use omnicache::runtime::cache::INTERMEDIATE_CACHE;
use omnicache::runtime::cache::IntermediateState;
use omnicache::runtime::codegen::OmniCodeGen;
use omnicache::utils::wrapper::{free_weld_vec_mem, get_output_data, weld_vec_mem_alloc};

use crate::omnicache::utils::wrapper::{transform_vec_in_vec_data, VecType};
use crate::omnicache::utils::wrapper::VecType::{DOUBLE, INT32, INT64};

mod omnicache;

#[no_mangle]
pub unsafe extern "C" fn toRust(addr: *const c_long, len: *const c_int) -> *const c_void {
    let longV = Vec::from_raw_parts(addr as *mut i64, len as usize, len as usize);
    println!("{:?}", longV);
    return longV.as_ptr() as _;
}

#[no_mangle]
pub extern "system" fn Java_nova_hetu_omnicache_runtime_demo_UnsafeVec_toRust(
    env: JNIEnv,
    _jobject: jobject,
    address: jlong,
    length: jint,
) -> jlong {
    unsafe {
        let ref longv = Vec::from_raw_parts(address as *mut i64, length as usize, length as usize);
        println!("{:?}", longv);
        mem::forget(longv);
        return 100;
    }
}

/*
 * Class:     nova_hetu_omnicache_OMVectorBase
 * Method:    mul
 * Signature: (ILjava/nio/ByteBuffer;I)V
 */

#[no_mangle]
#[allow(non_snake_case)]
pub extern "system" fn Java_nova_hetu_omnicache_vector_OMVectorBase_mul(
    env: JNIEnv,
    calling_object: jobject,
    d_type: jint,
    data: JByteBuffer,
    multiplier: jint,
) {
    //println!("multiple {} {}", d_type, multiplier);

    let buffer_ptr = env.get_direct_buffer_address(data);
    let buffer = buffer_ptr.expect("error getting buffer pointer");
    unsafe {
        if d_type == 1 {
            //int
            let mut vec = into::<i32>(buffer);
            for i in 0..vec.len() {
                vec[i] *= multiplier;
            }
            //ensure the memeory referenced by vec is not released
            mem::forget(vec);
        } else if d_type == 2 {
            //long
            let mut vec = into::<i64>(buffer);
            for i in 0..vec.len() {
                vec[i] *= multiplier as i64;
            }
            //ensure the memeory referenced by vec is not released
            mem::forget(vec);
        } else if d_type == 3 {
            //double
            let mut vec = into::<f64>(buffer);
            for i in 0..vec.len() {
                vec[i] *= multiplier as f64;
            }
            // //println!("double multiply result: {:?}", vec);
            //ensure the memeory referenced by vec is not released
            mem::forget(vec);
        }
    }
}

/*
 * Class:     nova_hetu_omnicache_OMVectorBase
 * Method:    allocate
 * Signature: (I)Ljava/nio/ByteBuffer;
 */
#[no_mangle]
#[allow(non_snake_case)]
pub extern "system" fn Java_nova_hetu_omnicache_vector_OMVectorBase_allocate(
    env: JNIEnv,
    _clazz: JClass,
    size: jint,
) -> jobject {
    unsafe {
        let mut vec8 = vec![0u8; size as usize];
        let buffer = env.new_direct_byte_buffer(vec8.as_mut());
        let result = buffer
            .expect("Error allocating direct byte buffer")
            .into_inner();
        mem::forget(vec8);
        result
    }
}

/*
 * Class:     nova_hetu_omnicache_OMVectorBase
 * Method:    concat
 * Signature: (I)Ljava/nio/ByteBuffer;
 */
#[allow(non_snake_case)]
#[no_mangle]
pub extern "system" fn Java_nova_hetu_omnicache_vector_OMVectorBase_concat(
    env: JNIEnv,
    this_class: JClass,
    buffer1: JByteBuffer,
    buffer2: JByteBuffer,
    size1: jint,
    size2: jint,
) -> jobject {
    // 1. allocate new memory
    let mut vec8 = vec![0u8; (size1 + size2) as usize];
    let result = env
        .new_direct_byte_buffer(vec8.as_mut())
        .expect("Error allocating direct byte buffer")
        .into_inner();

    // 2. copy two buffers as one
    let buf1_address = env.get_direct_buffer_address(buffer1).expect("");
    let buf2_address = env.get_direct_buffer_address(buffer2).expect("");
    unsafe {
        let new_buff_address = vec8.as_mut_ptr() as *mut c_void;
        libc::memmove(
            new_buff_address,
            buf1_address as *const _ as *const c_void,
            size1 as usize,
        );
        libc::memmove(
            new_buff_address.offset(size1 as isize),
            buf2_address as *const _ as *const c_void,
            size2 as usize,
        );
        mem::forget(vec8);
        mem::forget(new_buff_address);
        result
    }
}

#[allow(non_snake_case)]
#[no_mangle]
pub extern "system" fn Java_nova_hetu_omnicache_vector_OMVectorBase_free(
    env: JNIEnv,
    this_class: JClass,
    buffer: JByteBuffer,
) {
    let buf_addr_result = env.get_direct_buffer_address(buffer);

    let buf_addr = match buf_addr_result {
        Ok(buf_addr) => buf_addr,
        Err(error) => panic!("Can't free buffer: {:?}", error),
    };
    // let buf_addr = buf_addr_result.expect("");
    unsafe {
        //taking the ownership of the buffer which will be released once out of scope
        Box::from_raw(buf_addr);
    }
}

/*
 * Class:     nova_hetu_omnicache_runtime_JniWrapper
 * Method:    compile
 * Signature: (Ljava/lang/String;)Ljava/lang/String;
 */
#[no_mangle]
pub extern "system" fn Java_nova_hetu_omnicache_runtime_JniWrapper_compile(
    env: JNIEnv,
    this_obj: jobject,
    j_code: JString,
) -> jstring {
    let code: String = env
        .get_string(j_code)
        .expect("couldn't get code from java.")
        .into();
    let neid = OmniCodeGen::compile(&code);
    let j_neid = env.new_string(neid).expect("");
    j_neid.into_inner()
}

#[no_mangle]
pub extern "system" fn Java_nova_hetu_omnicache_runtime_JniWrapper_getFinalResult(
    env: JNIEnv,
    this_obj: jobject,
    j_key: JString,
    j_output_types: jintArray,
) -> jobject {
    let output_types = get_int_array_elements(env, j_output_types);
    let mut output_row_count = 0;
    let mut j_result = env
        .new_object_array(
            output_types.len() as i32,
            "java/nio/ByteBuffer",
            std::ptr::null_mut(),
        )
        .expect("create output buffer failed.");
    let omni_key = get_str(env, j_key);
    unsafe {
        let tmp_res_key = get_str(env, j_key);
        let weld_result;
        let tmp_res = INTERMEDIATE_CACHE.clone().lock().unwrap()
            .get(&tmp_res_key)
            .expect("invalid value")
            .deref()
            .clone();

        weld_result = WeldValue::new_from_data(tmp_res as Data);
        mem::forget(tmp_res);
        mem::forget(j_result);
        output_row_count = build_output_data(env, &output_types, &weld_result, j_result);
        mem::forget(weld_result);
        let result = build_om_result(env, j_result, output_row_count, omni_key).into_inner();
        let remove = INTERMEDIATE_CACHE.clone().lock().unwrap()
            .remove(&tmp_res_key)
            .expect("error removing intermediate result");
        result
    }
}

/*
 * Class:     nova_hetu_omnicache_runtime_JniWrapper
 * Method:    execute
 * Signature: (Ljava/lang/String;[Ljava/nio/ByteBuffer;[IJ[I)Lnova/hetu/omnicache/runtime/OMResult;
 */
#[no_mangle]
pub extern "system" fn Java_nova_hetu_omnicache_runtime_JniWrapper_executeV1(
    env: JNIEnv,
    this_obj: jobject,
    j_func: JString,
    j_key: JString,
    j_input_data: jobjectArray,
    j_input_row_num: jlong,
    j_state_data: jobjectArray,
    j_stat_row_num: jlong,
    j_input_types: jintArray,
    j_output_types: jintArray,
) -> jobject {
    //dbg!(j_input_types);
    let input_types = get_int_array_elements(env, j_input_types);
    let output_types = get_int_array_elements(env, j_output_types);
    let mut output_row_count = 0;
    //dbg!(output_types.len());

    // new java.nio.ByteBuffer[output_types.len()]
    let mut j_result = env
        .new_object_array(
            output_types.len() as i32,
            env.find_class("java/nio/ByteBuffer")
                .expect("error getting class"),
            std::ptr::null_mut(),
        )
        .expect("create output buffer failed.");
    //dbg!(j_result);
    let omni_key = get_str(env, j_key);
    let tmp_res_key = get_str(env, j_key);
    let weld_result;
    // transform input data to weld input data
    let function = get_str(env, j_func);

    unsafe {
        let input_data = into_weld_vec(
            env,
            j_input_data,
            j_input_row_num as usize,
            j_state_data,
            j_stat_row_num as usize,
            &input_types,
            &tmp_res_key as &str,
        );
        // execute weld ir
        weld_result = OmniCodeGen::execute(function, &(*input_data))
            .expect("OmniCache Native execute failed!");
        // release the mem for build input data
        //dbg!(&weld_result);
        free_weld_vec_mem(input_data);

        INTERMEDIATE_CACHE.clone().lock().unwrap()
            .insert(tmp_res_key, weld_result.data() as *const u8);
        // if old_inter_result.is_some(){
        //     Box::from_raw(old_inter_result.unwrap() as *mut _)
        // }
        mem::forget(j_result);
        mem::forget(input_data);
        output_row_count = build_output_data(env, &output_types, &weld_result, j_result);
        mem::forget(weld_result);
    }
    // handle the weld result
    build_om_result(env, j_result, output_row_count, omni_key).into_inner()
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

unsafe fn build_output_data(
    env: JNIEnv,
    data_type: &[i32],
    w_result: &WeldValue,
    output: *mut _jobject,
) -> i32 {
    let mut output_len = 0;
    let col_count = data_type.len() as i32;

    for col_idx in 0..col_count {
        let current_len;
        let d_type = data_type[col_idx as usize];
        match d_type.try_into() {
            Ok(INT32) => {
                let result_i32 = get_output_data(w_result, col_idx as isize, INT32);
                //dbg!(result_i32);
                let mut vec_i32 = transform_weld_to_vec::<i32>(result_i32.0, result_i32.1);
                // //println!("{:?}", vec_i32);
                current_len = vec_i32.len();
                add_buf_to_output(env, vec_i32.as_mut(), output, col_idx);
                mem::forget(vec_i32);
            }
            Ok(INT64) => {
                let result_i64 = get_output_data(w_result, col_idx as isize, INT64);
                let mut vec_i64 = transform_weld_to_vec::<i64>(result_i64.0, result_i64.1);
                // //println!("{:?}", vec_i64);
                current_len = vec_i64.len();
                add_buf_to_output(env, vec_i64.as_mut(), output, col_idx);
                mem::forget(vec_i64);
            }
            Ok(DOUBLE) => {
                let result_f64 = get_output_data(w_result, col_idx as isize, DOUBLE);
                let mut vec_f64 = transform_weld_to_vec::<f64>(result_f64.0, result_f64.1);
                // //println!("{:?}", vec_f64);
                current_len = vec_f64.len();
                add_buf_to_output(env, vec_f64.as_mut(), output, col_idx);
                mem::forget(vec_f64);
            }
            _ => panic!("Unsupported date type:{}", d_type),
        }
        // check output rows
        if output_len != 0 && output_len != current_len {
            panic!(
                "Columns unaligned with different number of rows:{},{}",
                output_len, current_len
            );
        }
        output_len = current_len;
        //dbg!(output_len);
    }
    output_len as i32
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

unsafe fn get_intermediate_vec<T>(tmp_res_key: &str, c_index: i32, vec_type: VecType) -> Vec<T> {
    let tmp_res = INTERMEDIATE_CACHE.clone().lock().unwrap()
        .get(tmp_res_key)
        .expect("Invalid tmp result!")
        .deref()
        .clone();
    // //println!("tmp_res is :{:?}", tmp_res);
    let weld_value = WeldValue::new_from_data(tmp_res as Data);
    let result_ = get_output_data(&weld_value, c_index as isize, vec_type);
    let vec_tmp = transform_weld_to_vec::<T>(result_.0, result_.1);
    vec_tmp
}

// fn get_bytebuffer_address(env: JNIEnv, bufs: jobjectArray, idx: i32) -> &mut [u8] {
//     let buf = env
//         .get_object_array_element(bufs, idx)
//         .expect("couldn't get buffer");
//     let addr = env
//         .get_direct_buffer_address(JByteBuffer::from(buf))
//         .expect("couldn't get the address of buffer");
//     return addr;
// }

unsafe fn into_weld_vec(
    env: JNIEnv,
    bufs: jobjectArray,
    rows: usize,
    state_bufs: jobjectArray,
    statRowSize: usize,
    data_type: &[i32],
    tmp_res_key: &str,
) -> *mut c_void {
    let mut col_count = env
        .get_array_length(bufs)
        .expect("error getting column count");
    if col_count != data_type.len() as i32 {
        panic!(
            "Column count:{}  and input type size:{} mismatch.",
            col_count,
            data_type.len()
        );
    }
    let has_tmp = !state_bufs.is_null();
    // let has_tmp = !INTERMEDIATE_CACHE.clone().lock().unwrap()
    //     .get(tmp_res_key).is_none();
    let mut address = weld_vec_mem_alloc(col_count as usize);

    for c_index in 0..col_count {
        let buf = env
            .get_object_array_element(bufs, c_index)
            .expect("couldn't get input buffer");
        let buf_addr = env
            .get_direct_buffer_address(JByteBuffer::from(buf))
            .expect("couldn't get the address of input data buffer");



        // let buf_addr = get_bytebuffer_address(env,bufs, c_index);
        // let stat_addr = get_bytebuffer_address(env,state_bufs, c_index);

        let d_type = data_type[c_index as usize];
        match d_type.try_into() {
            Ok(INT32) => {
                let mut input_vectors = vec![];
                if has_tmp {
                    let stat = env
                        .get_object_array_element(state_bufs, c_index)
                        .expect("couldn't get stats buffer");
                    let stat_addr = env
                        .get_direct_buffer_address(JByteBuffer::from(stat))
                        .expect("couldn't get the address of stats data buffer");
                    let vec_i32_tmp = transform_buf_to_vec::<i32>(statRowSize, stat_addr);//get_intermediate_vec::<i32>(tmp_res_key, c_index, INT32);
                    input_vectors.push(vec_i32_tmp);
                }
                let vec_i32 = transform_buf_to_vec::<i32>(rows, buf_addr);
                input_vectors.push(vec_i32);
                transform_vec_in_vec_data(&input_vectors, address, c_index as isize);
                mem::forget(input_vectors);
            }
            Ok(INT64) => {
                let mut input_vectors = vec![];
                if has_tmp {
                    let stat = env
                        .get_object_array_element(state_bufs, c_index)
                        .expect("couldn't get stats buffer");
                    let stat_addr = env
                        .get_direct_buffer_address(JByteBuffer::from(stat))
                        .expect("couldn't get the address of stats data buffer");
                    let vec_i64_tmp = transform_buf_to_vec::<i64>(statRowSize, stat_addr);
                    input_vectors.push(vec_i64_tmp);
                }
                let vec_i64 = transform_buf_to_vec::<i64>(rows, buf_addr);
                input_vectors.push(vec_i64);
                // println!("{:?}",input_vectors);
                transform_vec_in_vec_data(&input_vectors, address, c_index as isize);
                mem::forget(input_vectors);
            }
            Ok(DOUBLE) => {
                let mut input_vectors = vec![];
                if has_tmp {
                    let stat = env
                        .get_object_array_element(state_bufs, c_index)
                        .expect("couldn't get stats buffer");
                    let stat_addr = env
                        .get_direct_buffer_address(JByteBuffer::from(stat))
                        .expect("couldn't get the address of stats data buffer");
                    let vec_f64_tmp = transform_buf_to_vec::<f64>(statRowSize, stat_addr);
                    input_vectors.push(vec_f64_tmp);
                }
                let vec_f64 = transform_buf_to_vec::<f64>(rows, buf_addr);
                input_vectors.push(vec_f64);
                transform_vec_in_vec_data(&input_vectors, address, c_index as isize);
                mem::forget(input_vectors);
            }
            Err(_) => panic!("Unsupported date type:{}", d_type),
        }
    }
    address
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

#[cfg(test)]
mod tests {
    use std::thread;

    use super::*;

    #[test]
    fn test_allocate() {
        unsafe {
            let vec_i8 = allocate_vec::<i8>(0);
            assert_eq!(1024, vec_i8.len());
            let vec_i16 = allocate_vec::<i16>(0);
            assert_eq!(2048, vec_i16.len());
            let vec_i32 = allocate_vec::<i32>(0);
            assert_eq!(4096, vec_i32.len());
            let vec_i64 = allocate_vec::<i64>(0);
            assert_eq!(8192, vec_i64.len());
            let vec_f32 = allocate_vec::<f32>(0.);
            assert_eq!(4096, vec_f32.len());
            let vec_f64 = allocate_vec::<f64>(0.);
            assert_eq!(8192, vec_f64.len());
        }
    }

    #[test]
    fn test_multiple_allocate() {
        unsafe {
            verify_2_vec_alloc();
        }
    }

    #[test]
    fn test_multi_thread_allocate() {
        unsafe {
            let mut t_vec = vec![];
            for i in 0..24 {
                t_vec.push(thread::spawn(move || {
                    for i in 0..1000000 {
                        verify_2_vec_alloc();
                    }
                    //println!("finished: {}", i);
                }));
            }

            for x in t_vec {
                x.join();
            }
        }
    }

    unsafe fn verify_2_vec_alloc() {
        let mut vec_1 = allocate_vec::<i8>(0);
        let mut vec_2 = allocate_vec::<i8>(0);

        for i in 0..vec_1.len() {
            vec_1[i] = i as u8;
        }
        for i in 0..vec_1.len() {
            vec_2[i] = (i * 2) as u8;
        }

        for i in 0..vec_1.len() {
            assert_eq!(i as u8, vec_1[i] as u8);
        }
        for i in 0..vec_1.len() {
            assert_eq!((i * 2) as u8, vec_2[i]);
        }
    }

    #[test]
    fn test_long_multiple_allocate() {
        unsafe {
            let mut raw1 = allocate_vec::<i64>(10i64);
            let mut raw2 = allocate_vec::<i64>(0i64);
            let mut vec_1 = into::<i64>(raw1.as_mut());
            let mut vec_2 = into::<i64>(raw2.as_mut());

            mem::forget(raw1);
            mem::forget(raw2);

            for i in 0..vec_1.len() {
                //println!("initial value: {:?}", vec_1);
            }
            for i in 0..vec_1.len() {
                vec_1[i] = i as i64;
            }
            for i in 0..vec_1.len() {
                vec_2[i] = 1024 + (i * 2) as i64;
            }

            for i in 0..vec_1.len() {
                assert_eq!(i as i64, vec_1[i]);
            }
            for i in 0..vec_1.len() {
                assert_eq!(1024 + (i * 2) as i64, vec_2[i]);
            }
        }
    }

    #[test]
    #[should_panic]
    fn test_convert_misaligned1() {
        let mut src = vec![0u8; 1023];
        unsafe {
            into::<i16>(src.as_mut_slice());
        }
    }

    #[test]
    #[should_panic]
    fn test_convert_misaligned2() {
        let mut src = vec![0u8; 1022];
        unsafe {
            into::<i32>(src.as_mut_slice());
        }
    }

    #[test]
    #[should_panic]
    fn test_convert_misaligned3() {
        let mut src = vec![0u8; 1020];
        unsafe {
            into::<i64>(src.as_mut_slice());
        }
    }
}
