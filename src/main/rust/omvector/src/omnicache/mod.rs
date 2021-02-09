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
use rand::Rng;
use runtime::codegen::OmniCodeGen;
use std::mem;
use time::now;
use utils::wrapper::VecType::{DOUBLE, INT32, INT64};
use utils::wrapper::{
    free_weld_vec_mem, get_output_data, transform_input_data, transform_vec_in_vec_data,
    weld_vec_mem_alloc,
};
use weld::data::WeldVec;

pub mod data;
pub mod runtime;
pub mod utils;

static column_base_group_by_sum_code: &str = "|k:vec[i32],v:vec[i32]| \
        let rs = tovec(result(for(zip(k,v),dictmerger[i32,i32,+],|b,i,n| merge(b,{n.$0,n.$1}))));\
        let k = result(for(rs,appender[i32],|b,i,n| merge(b,n.$0)));\
        let v = result(for(rs,appender[i32],|b,i,n| merge(b,n.$1)));\
        {k,v}";

static row_base_group_by_sum_code: &str =
    "|v:vec[{i32,i32}]| tovec(result(for(v,dictmerger[i32,i32,+],|b,i,n| merge(b,{n.$0,n.$1}))))";
static code: &str = "|v0 :vec[i32], v1: vec[i64]|\
                          let pairs = tovec(result(for(zip(v0, v1), dictmerger[i32,i64,+], |b,i,n| merge(b, {n.$0, n.$1}))));\
                          let k = result(for(pairs, appender[i32], |b,i,n| merge(b, n.$0)));
                          let v = result(for(pairs, appender[i64], |b,i,n| merge(b, n.$1)));
                          {k,v}";

#[derive(Clone, Debug)]
pub struct TupleData2<V1, V2> {
    pub v1: WeldVec<V1>,
    pub v2: WeldVec<V2>,
}

#[derive(Clone, Debug)]
pub struct TupleData1<V> {
    pub v: WeldVec<V>,
}

#[test]
fn column_base_group_by_sum_multi_type_input() {
    let mod_id = OmniCodeGen::compile(code);
    let v0 = vec![1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4];
    let v1 = vec![100i64; 12];

    let a;
    unsafe {
        a = weld_vec_mem_alloc(2);
        transform_input_data(&v0, a, 0);
        transform_input_data(&v1, a, 1);
    }

    let result;
    unsafe {
        result = OmniCodeGen::execute(mod_id, &*a).expect("OmniCache Native execute failed!");
        free_weld_vec_mem(a);
    }
    let result_v0;
    let result_v1;
    let result_keys;
    let result_values;
    unsafe {
        result_v0 = get_output_data(&result, 0, INT32);
        result_keys = Vec::from_raw_parts(
            result_v0.0 as *mut i32,
            result_v0.1 as usize,
            result_v0.1 as usize,
        );
        result_v1 = get_output_data(&result, 1, INT64);
        result_values = Vec::from_raw_parts(
            result_v1.0 as *mut i64,
            result_v1.1 as usize,
            result_v1.1 as usize,
        );

        let expect_keys = vec![1, 2, 3, 4];
        let expect_vals = vec![300, 300, 300, 300];
        assert_eq!(expect_keys.len(), result_keys.len());
        for i in 0..expect_keys.len() {
            let mut success = false;
            for j in 0..expect_keys.len() {
                if expect_keys.get(i) == result_keys.get(j) {
                    if expect_vals.get(i) == result_values.get(j) {
                        success = true;
                        break;
                    }
                }
            }
            assert_eq!(success, true);
        }
        mem::forget(result_keys);
        mem::forget(result_values);
    }
}

#[test]
fn column_base_group_by_sum() {
    let mut key: Vec<i32> = Vec::new();
    let mut value: Vec<i32> = Vec::new();
    for idx in 0..10 {
        key.push(idx % 3);
        value.push(idx);
    }
    let neid = OmniCodeGen::compile(column_base_group_by_sum_code);
    let ref data = TupleData2 {
        v1: WeldVec::from(&key),
        v2: WeldVec::from(&value),
    };
    unsafe {
        let result = OmniCodeGen::execute(neid, data).expect("group by sum execution error!");
        let data = result.data() as *const TupleData2<i32, i32>;
        let result = (*data).clone();
        let expect_keys = vec![1, 0, 2];
        let expect_vals = vec![12, 18, 15];

        assert_eq!(result.v1.len, expect_keys.len() as i64);
        for i in 0..(expect_keys.len() as isize) {
            let mut success = false;
            let rKey = *result.v1.data.offset(i);
            let rValue = *result.v2.data.offset(i);
            for j in 0..(expect_keys.len()) {
                if expect_keys[j] == rKey {
                    if expect_vals[j] == rValue {
                        success = true;
                        break;
                    }
                }
            }
            assert_eq!(success, true);
        }
    }
}

#[test]
fn row_base_group_by_sum() {
    let mut data = Vec::new();
    for idx in 0..10 {
        data.push((idx % 3, idx));
    }
    let neid = OmniCodeGen::compile(row_base_group_by_sum_code);
    let ref data = TupleData1 {
        v: WeldVec::from(&data),
    };
    unsafe {
        let result = OmniCodeGen::execute(neid, data).expect("group by sum execution error!");
        let data = result.data() as *const TupleData1<(i32, i32)>;
        let result = (*data).clone();
        let output_keys = vec![1, 0, 2];
        let output_vals = vec![12, 18, 15];
        assert_eq!(result.v.len, output_keys.len() as i64);
        for i in 0..(output_keys.len() as isize) {
            let mut success = false;
            let kv = *result.v.data.offset(i);
            for j in 0..(output_keys.len()) {
                if output_keys[j] == kv.0 {
                    if output_vals[j] == kv.1 {
                        success = true;
                        break;
                    }
                }
            }
            assert_eq!(success, true);
        }
    }
}

#[test]
fn benchmark_column_base_group_by_sum() {
    //benchmark testing
    let mut key: Vec<i32> = Vec::new();
    let mut value: Vec<i32> = Vec::new();
    let mut rng = rand::thread_rng();
    for idx in 0..10_000_000 {
        key.push(rng.gen_range(0..1_000_000));
        value.push(idx);
    }
    unsafe {
        //println!("start execution ....");
        for i in 0..5 {
            let start = now();
            let neid = OmniCodeGen::compile(column_base_group_by_sum_code);
            let end = now();
            //println!("Compiling time {:?}", (end - start).num_milliseconds());
            let ref data = TupleData2 {
                v1: WeldVec::from(&key),
                v2: WeldVec::from(&value),
            };
            let result = OmniCodeGen::execute(neid, data).expect("group by sum execution error!");
            let end = now();
            let data = result.data() as *const TupleData2<i32, i32>;
            let result = unsafe { (*data).clone() };
            let elapsed_millis = end - start;
            //println!("column base group_by_sum exec data length:{},used time:{:?} ms", key.len(), elapsed_millis.num_milliseconds());
        }
    }
}

#[test]
fn benchmark_row_base_group_by_sum() {
    let mut kv = Vec::new();
    for idx in 0..100000000 {
        kv.push((idx % 1000000, idx));
    }
    unsafe {
        //println!("start execution ....");
        let start = now();
        let neid = OmniCodeGen::compile(row_base_group_by_sum_code);
        let ref data = TupleData1 {
            v: WeldVec::from(&kv),
        };
        let result = OmniCodeGen::execute(neid, data).expect("group by sum execution error!");
        let end = now();
        let data = result.data() as *const TupleData1<(i32, i32)>;
        let result = unsafe { (*data).clone() };
        //println!("row base group_by_sum exec data length:{},used time:{:?}", kv.len(), end - start);
    }
}

#[test]
fn sum_avg_group_by_two_columns_test() {
    let sum_avg_group_by_two_columns = "|v0 :vec[i64], v1: vec[i64], v2: vec[f64], v3: vec[f64]|\
                          let sum_dict_2 = for(zip(v0, v1, v2), dictmerger[{i64,i64}, f64,+], |b,i,n| merge(b, {{n.$0,n.$1}, n.$2}));\
                          let dict_0_1 = tovec(result(sum_dict_2));\
                          let k0 = result(for(dict_0_1, appender[i64], |b, i, n| merge(b, n.$0.$0)));\
                          let k1 = result(for(dict_0_1, appender[i64], |b, i, n| merge(b, n.$0.$1)));\
                          let sum_2 = result(for(dict_0_1, appender[f64], |b, i, n| merge(b, n.$1)));\
                          let avg_sum_3 = for(zip(v0, v1, v3), dictmerger[{i64,i64}, {f64, f64}, +], |b,i,n| merge(b, {{n.$0, n.$1}, {n.$2, 1.0}}));\
                          let avg_3 = result(for(tovec(result(avg_sum_3)), appender[f64], |b, i, n| merge(b, n.$1.$0 / n.$1.$1)));\
                          {k0, k1, sum_2, avg_3}";
    let mod_id = OmniCodeGen::compile(sum_avg_group_by_two_columns);
    let v0: Vec<i64> = vec![1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4];
    let v1: Vec<i64> = vec![50, 51, 52, 53, 50, 55, 56, 57, 58, 59, 60, 61];
    let v2: Vec<f64> = vec![1.2f64; 12];
    let v3: Vec<f64> = vec![1.2, 1.2, 1.2, 1.2, 2.4, 2.4, 2.4, 2.4, 2.4, 2.4, 2.4, 2.4];

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
        result = OmniCodeGen::execute(mod_id, &*a).expect("OmniCache Native execute failed!");
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
        res0 = Vec::from_raw_parts(
            result_v0.0 as *mut i64,
            result_v0.1 as usize,
            result_v0.1 as usize,
        );
        result_v1 = get_output_data(&result, 1, INT64);
        res1 = Vec::from_raw_parts(
            result_v1.0 as *mut i64,
            result_v1.1 as usize,
            result_v1.1 as usize,
        );
        result_v2 = get_output_data(&result, 2, DOUBLE);
        res2 = Vec::from_raw_parts(
            result_v2.0 as *mut f64,
            result_v2.1 as usize,
            result_v2.1 as usize,
        );
        result_v3 = get_output_data(&result, 3, DOUBLE);
        res3 = Vec::from_raw_parts(
            result_v3.0 as *mut f64,
            result_v1.1 as usize,
            result_v1.1 as usize,
        );
        assert_eq!(11, res0.len());
        mem::forget(res0);
        mem::forget(res1);
        mem::forget(res2);
        mem::forget(res3);
    }
}

#[test]
fn sum_avg_group_by_vec_in_vec_test() {
    let sum_avg_group_by_two_columns = "|v0 :vec[vec[i64]], v1: vec[vec[i64]], v2: vec[vec[f64]], v3: vec[vec[f64]]|\
                          let sum_dict_2 = for(zip(v0, v1, v2), dictmerger[{i64,i64}, f64,+], |b,i,n| \
                                                for(zip(n.$0, n.$1, n.$2), b, |b_, i_, m|\
                                                    merge(b, {{m.$0, m.$1}, m.$2})));\
                          let dict_0_1 = tovec(result(sum_dict_2));\
                          let k0 = result(for(dict_0_1, appender[i64], |b, i, n| merge(b, n.$0.$0)));\
                          let k1 = result(for(dict_0_1, appender[i64], |b, i, n| merge(b, n.$0.$1)));\
                          let sum_2 = result(for(dict_0_1, appender[f64], |b, i, n| merge(b, n.$1)));\
                          let avg_sum_3 = for(zip(v0, v1, v3), dictmerger[{i64,i64}, {f64, f64}, +], |b,i,n| \
                                                for(zip(n.$0, n.$1, n.$2), b, |b_, i_, m|\
                                                    merge(b, {{m.$0, m.$1}, {m.$2, 1.0}})));\
                          let avg_3 = result(for(tovec(result(avg_sum_3)), appender[f64], |b, i, n| merge(b, n.$1.$0 / n.$1.$1)));\
                          {k0, k1, sum_2, avg_3}";
    let mod_id = OmniCodeGen::compile(sum_avg_group_by_two_columns);
    // let v0: Vec<Vec<i64>> = vec![(0..1000000).collect(); 100];
    let v0: Vec<Vec<i64>> = vec![vec![1, 2, 3, 4]; 3];
    let v1: Vec<Vec<i64>> = vec![
        vec![50, 51, 52, 53],
        vec![50, 55, 56, 57],
        vec![58, 59, 60, 61],
    ];
    let v2: Vec<Vec<f64>> = vec![
        vec![1.2, 1.2, 1.2, 1.2],
        vec![2.4, 2.4, 2.4, 2.4],
        vec![2.4, 2.4, 2.4, 2.4],
    ];
    let v3: Vec<Vec<f64>> = vec![
        vec![1.2, 1.2, 1.2, 1.2],
        vec![2.4, 2.4, 2.4, 2.4],
        vec![2.4, 2.4, 2.4, 2.4],
    ];

    let input_addr;
    unsafe {
        input_addr = weld_vec_mem_alloc(4);
        transform_vec_in_vec_data(&v0, input_addr, 0);
        transform_vec_in_vec_data(&v1, input_addr, 1);
        transform_vec_in_vec_data(&v2, input_addr, 2);
        transform_vec_in_vec_data(&v3, input_addr, 3);
    }

    let result;
    unsafe {
        result =
            OmniCodeGen::execute(mod_id, &*input_addr).expect("OmniCache Native execute failed!");
        free_weld_vec_mem(input_addr);
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
        res0 = Vec::from_raw_parts(
            result_v0.0 as *mut i64,
            result_v0.1 as usize,
            result_v0.1 as usize,
        );
        result_v1 = get_output_data(&result, 1, INT64);
        res1 = Vec::from_raw_parts(
            result_v1.0 as *mut i64,
            result_v1.1 as usize,
            result_v1.1 as usize,
        );
        result_v2 = get_output_data(&result, 2, DOUBLE);
        res2 = Vec::from_raw_parts(
            result_v2.0 as *mut f64,
            result_v2.1 as usize,
            result_v2.1 as usize,
        );
        result_v3 = get_output_data(&result, 3, DOUBLE);
        res3 = Vec::from_raw_parts(
            result_v3.0 as *mut f64,
            result_v1.1 as usize,
            result_v1.1 as usize,
        );
        assert_eq!(11, res0.len());
        mem::forget(res0);
        mem::forget(res1);
        mem::forget(res2);
        mem::forget(res3);
    }
}

#[test]
fn sum_group_by_vec_in_vec_perf_test() {
    let sum_avg_group_by_two_columns = "|v0 :vec[vec[i64]], v1: vec[vec[f64]]|\
                          let sum_ = for(zip(v0, v1), dictmerger[i64, f64,+], |b,i,n| \
                                                for(zip(n.$0, n.$1), b, |b_, i_, m|\
                                                    merge(b, {m.$0, m.$1})));\
                          let dict_ = tovec(result(sum_));\
                          let k = result(for(dict_, appender[i64], |b, i, n| merge(b, n.$0)));\
                          let val = result(for(dict_, appender[f64], |b, i, n| merge(b, n.$1)));\
                          {k, val}";

    let v0: Vec<Vec<i64>> = vec![(0..1_000_000).collect(); 100];
    let v1: Vec<Vec<f64>> = vec![vec![1.2; 1_000_000]; 100];
    let start = now();
    let mod_id = OmniCodeGen::compile(sum_avg_group_by_two_columns);

    let input_addr;
    unsafe {
        input_addr = weld_vec_mem_alloc(2);
        transform_vec_in_vec_data(&v0, input_addr, 0);
        transform_vec_in_vec_data(&v1, input_addr, 1);
    }
    let result;
    unsafe {
        result =
            OmniCodeGen::execute(mod_id, &*input_addr).expect("OmniCache Native execute failed!");
        free_weld_vec_mem(input_addr);
    }
    //println!("Vec in Vec case executed with 100 million rows consumed : {}ms", (now() - start).num_milliseconds());

    let sum_avg_group_by_two_columns = "|v0: vec[i64], v1: vec[f64]|\
                          let sum_ = for(zip(v0, v1), dictmerger[i64, f64, +], |b, i, n| merge(b, {n.$0, n.$1}));\
                          let dict_ = tovec(result(sum_));\
                          let k = result(for(dict_, appender[i64], |b, i, n| merge(b, n.$0)));\
                          let val = result(for(dict_, appender[f64], |b, i, n| merge(b, n.$1)));\
                          {k, val}";

    let mut v0: Vec<i64> = vec![];
    for i in 0..100_000_000 {
        v0.push(i % 1_000_000 as i64);
    }
    let v1: Vec<f64> = vec![1.2; 100_000_000];
    let start = now();
    let mod_id = OmniCodeGen::compile(sum_avg_group_by_two_columns);

    let input_addr_;
    unsafe {
        input_addr_ = weld_vec_mem_alloc(2);
        transform_input_data(&v0, input_addr_, 0);
        transform_input_data(&v1, input_addr_, 1);
    }
    let result;
    unsafe {
        result =
            OmniCodeGen::execute(mod_id, &*input_addr_).expect("OmniCache Native execute failed!");
        free_weld_vec_mem(input_addr_);
    }
    //println!("Flatten Vec executed with 100 million rows consumed : {}ms", (now() - start).num_milliseconds());

    let result_v0;
    let result_v1;
    let res0;
    let res1;
    unsafe {
        result_v0 = get_output_data(&result, 0, INT64);
        res0 = Vec::from_raw_parts(
            result_v0.0 as *mut i64,
            result_v0.1 as usize,
            result_v0.1 as usize,
        );
        result_v1 = get_output_data(&result, 1, DOUBLE);
        res1 = Vec::from_raw_parts(
            result_v1.0 as *mut i64,
            result_v1.1 as usize,
            result_v1.1 as usize,
        );
        assert_eq!(1_000_000, res0.len());
        mem::forget(res0);
        mem::forget(res1);
    }
}

#[test]
fn weldvec_to_rustvec() {
    let mut rawData = Vec::new();
    rawData.push(1);
    rawData.push(2);
    rawData.push(3);

    let weldVec = WeldVec::from(&rawData);

    let transferedVec = unsafe {
        let v = Vec::from_raw_parts(
            weldVec.data as *mut i32,
            weldVec.len as usize,
            weldVec.len as usize,
        );
        v
    };
    for idx in 0..rawData.len() {
        assert_eq!(rawData.get(idx), transferedVec.get(idx));
    }

    mem::forget(rawData);
}
