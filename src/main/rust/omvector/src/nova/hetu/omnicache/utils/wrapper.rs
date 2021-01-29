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
use weld::data::WeldVec;
use std::os::raw::c_void;
use std::mem::size_of_val;
use libc::free;
use std::mem;
use lazy_static::*;
use weld::WeldValue;

lazy_static! {
     static ref WELD_VEC_WIDTH: usize = 16;
}

#[derive(Debug)]
pub enum VecType {
    INT32 = 1,
    INT64 = 2,
    DOUBLE = 3
}

pub unsafe fn weld_vec_mem_alloc(size: usize) -> *mut c_void {
    libc::malloc(size * *WELD_VEC_WIDTH)
}

pub unsafe fn transform_input_data<T>(v_ref:&Vec<T>, addr:*mut c_void, offset:isize) {
    let weld_vec = WeldVec::from(v_ref);
    libc::memcpy(addr.offset(offset * *WELD_VEC_WIDTH as isize), &weld_vec as *const _ as *const c_void, *WELD_VEC_WIDTH);
    mem::forget(weld_vec);
}

pub unsafe fn get_output_data(result: &WeldValue, offset: isize, vec_type: VecType) -> (*mut c_void, i64){
    match vec_type {
        VecType::INT64=> {
            let data = result.data().offset(offset * *WELD_VEC_WIDTH as isize) as *const WeldVec<i64>;
            let out = (*data).clone();
            let v = into_vec(out.data, out.len);
            let result = (v.as_ptr() as *mut c_void, out.len);
            mem::forget(v);
            result
        },
        VecType::INT32=> {
            let data= result.data().offset(offset * *WELD_VEC_WIDTH as isize) as *const WeldVec<i32>;
            let out = (*data).clone();
            let v = into_vec(out.data, out.len);
            let result = (v.as_ptr() as *mut c_void, out.len);
            mem::forget(v);
            result
        },
        VecType::DOUBLE=> {
            let data= result.data().offset(offset * *WELD_VEC_WIDTH as isize) as *const WeldVec<f64>;
            let out = (*data).clone();
            let v = into_vec(out.data, out.len);
            let result = (v.as_ptr() as *mut c_void, out.len);
            mem::forget(v);
            result
        },
        _ => panic!()
    }
}

pub unsafe fn free_weld_vec_mem(addr:*mut c_void) {
    free(addr);
}

pub fn into_vec<T>(data :*const T, len :i64) -> Vec<T> {
    unsafe {
        Vec::from_raw_parts(data as *mut T, len as usize, len as usize)
    }
}