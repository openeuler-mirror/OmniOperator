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
use lazy_static::lazy_static;
use chashmap::CHashMap;
use weld::{WeldModule, WeldConf, WeldContext, WeldValue, Data, WeldResult};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use weld::data::WeldVec;
use std::os::raw::c_void;
use std::borrow::Borrow;
use std::collections::HashMap;

lazy_static!{
    static ref CACHE:CHashMap<String,WeldModule> = Default::default();
}
pub struct OmniCodeGen;

impl OmniCodeGen {
    pub fn compile(code: &str) -> String {
        let mut s = DefaultHasher::new();
        code.hash(&mut s);
        let key:String = s.finish().to_string();
        if CACHE.contains_key(&key) {
            return key;
        }
        let conf = WeldConf::new();
        let module =  WeldModule::compile(code,&conf).expect("OmniCache code gen failed!");
        CACHE.insert(key.clone(),module);
        key
    }
    pub fn compile_with_confs(code: &str, confs: &WeldConf) -> String {
        let mut s = DefaultHasher::new();
        code.hash(&mut s);
        let key:String = s.finish().to_string();
        if CACHE.contains_key(&key) {
            return key;
        }
        let module =  WeldModule::compile(code,confs).expect("OmniCache code gen failed!");
        CACHE.insert(key.clone(),module);
        key
    }
    pub unsafe fn execute<IN>(native_exec_id: String, ptr: &IN) -> WeldResult<WeldValue> {
        let module = CACHE.get(&native_exec_id).expect("Not find execution native code,please code compile first!");
        let ref input_value= WeldValue::new_from_data(ptr as *const _ as Data);
        let ref conf = WeldConf::new();
        let ref mut context = WeldContext::new(&conf).unwrap();
        module.run(context,input_value)
    }
    pub unsafe fn execute_with_confs<IN>(native_exec_id: String, ptr: &IN, confs: &WeldConf) -> WeldResult<WeldValue> {
        let module = CACHE.get(&native_exec_id).expect("Not find execution native code,please code compile first!");
        let ref input_value= WeldValue::new_from_data(ptr as *const _ as Data);
        let ref mut context = WeldContext::new(confs).unwrap();
        module.run(context,input_value)
    }
    pub unsafe fn execute_(native_exec_id: String, ptr: *const c_void) -> WeldResult<WeldValue> {
        let module = CACHE.get(&native_exec_id).expect("Not find execution native code,please code compile first!");
        let ref input_value= WeldValue::new_from_data(ptr);
        let ref conf = WeldConf::new();
        let ref mut context = WeldContext::new(&conf).unwrap();
        module.run(context,input_value)
    }
    pub fn set_configurations(confs: &HashMap<&str, &str>) -> WeldConf {
        let mut conf = WeldConf::new();
        for (k, v) in confs.iter() {
            conf.set(k.to_string(), v.to_string());
        }
        conf
    }
}