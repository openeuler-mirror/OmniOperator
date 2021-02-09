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
use libc::free;
use std::mem;
use lazy_static::*;
use weld::WeldValue;
use std::convert::TryFrom;

lazy_static! {
     static ref WELD_VEC_WIDTH: usize = 16;
}

#[derive(Debug)]
pub enum VecType {
    //TODO: use the column types from Joy
    INT32 = 1,
    INT64 = 2,
    DOUBLE = 3
}

impl TryFrom<i32> for VecType {
    type Error = ();

    fn try_from(v: i32) -> Result<Self, Self::Error> {
        match v {
            x if x == VecType::INT32 as i32 => Ok(VecType::INT32),
            x if x == VecType::INT64 as i32 => Ok(VecType::INT64),
            x if x == VecType::DOUBLE as i32 => Ok(VecType::DOUBLE),
            _ => Err(()),
        }
    }
}

pub enum OmniOpStep {
    INTERMEDIATE = 0,
    FINAL = 1
}

pub unsafe fn weld_vec_mem_alloc(size: usize) -> *mut c_void {
    libc::malloc(size * *WELD_VEC_WIDTH)
}

pub unsafe fn transform_input_data<T>(v_ref:&Vec<T>, addr:*mut c_void, offset:isize) {
    let weld_vec = WeldVec::from(v_ref);
    libc::memcpy(addr.offset(offset * *WELD_VEC_WIDTH as isize), &weld_vec as *const _ as *const c_void, *WELD_VEC_WIDTH);
    mem::forget(weld_vec);
}

pub unsafe fn transform_vec_in_vec_data<T>(v_ref: &Vec<Vec<T>>, addr:*mut c_void, offset:isize) {
    let mut vec_of_vec = vec![];
    let v_count = v_ref.len();

    for i in 0..v_count {
        vec_of_vec.push(WeldVec::from(&v_ref[i]));
    }
    transform_input_data(&vec_of_vec, addr, offset);
    mem::forget(vec_of_vec);
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