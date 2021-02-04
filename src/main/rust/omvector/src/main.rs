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
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use chashmap::CHashMap;
use lazy_static::lazy_static;
use weld::WeldModule;

use nova::hetu::omnicache::runtime::codegen::OmniCodeGen;
use std::mem;
use std::ptr;
use crate::nova::hetu::omnicache::utils::wrapper::{weld_vec_mem_alloc, transform_input_data, free_weld_vec_mem, get_output_data};
use crate::nova::hetu::omnicache::utils::wrapper::VecType::{DOUBLE,INT64};
use std::collections::HashMap;

mod nova;
lazy_static! {
    static ref CACHE:CHashMap<i32,WeldModule> = Default::default();
}

unsafe fn append_weldvec<T>(src_ptr: *const T,
                            dst_ptr: *mut T, elts: usize) {

    // SAFETY: Our precondition ensures the source is aligned and valid,
    // and `Vec::with_capacity` ensures that we have usable space to write them.
    ptr::copy(src_ptr, dst_ptr, elts);
}

fn main() {
        let code = "|v0 :vec[i64], v1: vec[i64], v2: vec[f64], v3: vec[f64]|\
                          # generate combined key first
                          # 1. calculate distinct value in group by columns
                          #let distinct_v0_bd = for(v0, dictmerger[i64, i64, +], |b,i,n| merge(b, {n, 1}));
                          #let distinct_v1_bd = for(v1, dictmerger[i64, i64, +], |b,i,n| merge(b, {n, 1}));
                          #let distinct_v0 = result(for(tovec(result(distinct_v0_bd)), appender[i64], |b, i, n| merge(b, n.$0)));
                          #let distinct_v1 = result(for(tovec(result(distinct_v1_bd)), appender[i64], |b, i, n| merge(b, n.$0)));
                          # 2. combine values to key tuples
                          #let keys = result(for(distinct_v0, appender[{i64, i64}], |b, i, n|
                          #                     for(distinct_v1, b, |b_, i_, m|
                          #                         merge(b, {n, m}))
                          #                      )
                          #                  );
                          # keys : vec[{i64, i64}]

                          let sum_dict_2 = for(zip(v0, v1, v2), dictmerger[{i64,i64}, f64,+], |b,i,n| merge(b, {{n.$0,n.$1}, n.$2}));\
                          let dict_0_1 = tovec(result(sum_dict_2));
                          let k0 = result(for(dict_0_1, appender[i64], |b, i, n| merge(b, n.$0.$0)));
                          let k1 = result(for(dict_0_1, appender[i64], |b, i, n| merge(b, n.$0.$1)));
                          let sum_2 = result(for(dict_0_1, appender[f64], |b, i, n| merge(b, n.$1)));
                          let avg_sum_3 = for(zip(v0, v1, v3), dictmerger[{i64,i64}, {f64, f64}, +], |b,i,n| merge(b, {{n.$0, n.$1}, {n.$2, 1.0}}));\
                          let avg_3 = result(for(tovec(result(avg_sum_3)), appender[f64], |b, i, n| merge(b, n.$1.$0 / n.$1.$1)));

                          {k0, k1, sum_2, avg_3}";
        let mut configurations = HashMap::new();
        configurations.insert("weld.compile.dumpCode", "true");
        configurations.insert("weld.compile.dumpCodeDir", "/usr/code/olk_dev/rust-omni-cache/omni-cache/");
        let confs = OmniCodeGen::set_configurations(&configurations);

        let mod_id = OmniCodeGen::compile_with_confs(code, &confs);
        let v0: Vec<i64> = vec![1,2,3,4,1,2,3,4,1,2,3,4];
        let v1: Vec<i64> = vec![50, 51, 52, 53, 50, 55, 56, 57, 58, 59, 60, 61];
        let v2: Vec<f64> = vec![1.2f64; 12];
        let v3: Vec<f64> = vec![1.2, 1.2,1.2, 1.2,2.4,2.4,2.4,2.4,2.4,2.4,2.4,2.4];

        let a;
        unsafe {
            a = weld_vec_mem_alloc(4);
            transform_input_data(&v0, a, 0);
            transform_input_data(&v1, a, 1);
            transform_input_data(&v2, a, 2);
            transform_input_data(&v3, a, 3);
        }

        let result;
        unsafe {
            result = OmniCodeGen::execute_with_confs(mod_id, &*a, &confs).expect("OmniCache Native execute failed!");
            // result = OmniCodeGen::execute(mod_id,&*a).expect("OmniCache Native execute failed!");
            free_weld_vec_mem(a);
        }
        let result_v0;
        let result_v1;
        let result_v2;
        let result_v3;
        let res0;
        let res1;
        let res2;
        let res3;
        unsafe {
            result_v0 = get_output_data(&result, 0, INT64);
            res0 = Vec::from_raw_parts( result_v0.0 as *mut i64, result_v0.1 as usize,
                                        result_v0.1 as usize);
            result_v1 = get_output_data(&result, 1, INT64);
            res1 = Vec::from_raw_parts( result_v1.0 as *mut i64, result_v1.1 as usize,
                                        result_v1.1 as usize);
            result_v2 = get_output_data(&result, 2, DOUBLE);
            res2 = Vec::from_raw_parts( result_v2.0 as *mut f64, result_v2.1 as usize,
                                        result_v2.1 as usize);
            result_v3 = get_output_data(&result, 3, DOUBLE);
            res3 = Vec::from_raw_parts( result_v3.0 as *mut f64, result_v1.1 as usize,
                                        result_v1.1 as usize);
        }
        println!("key0-----{:?}", res0);
        println!("key1-----{:?}", res1);
        println!("val0-----{:?}", res2);
        println!("val1-----{:?}", res3);
        mem::forget(res0);
        mem::forget(res1);
        mem::forget(res2);
        mem::forget(res3);
}