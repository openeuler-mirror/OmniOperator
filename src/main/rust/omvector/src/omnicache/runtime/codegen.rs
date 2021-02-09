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
use crate::omnicache::runtime::cache::module_cache;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::os::raw::c_void;
use weld::{Data, WeldConf, WeldContext, WeldModule, WeldResult, WeldValue};

pub struct OmniCodeGen;

impl OmniCodeGen {
    pub fn compile(code: &str) -> String {
        let mut s = DefaultHasher::new();
        code.hash(&mut s);
        let key = s.finish().to_string();
        let ref conf = WeldConf::new();
        module_cache(key.as_str(), code, conf);
        //println!("compile hit:{},miss:{}", get_module_cache_hits(),get_module_cache_misses());
        key
    }
    pub fn compile_with_confs(code: &str, confs: &WeldConf) -> String {
        let mut s = DefaultHasher::new();
        code.hash(&mut s);
        let key: String = s.finish().to_string();
        module_cache(key.as_str(), code, confs);
        key
    }
    pub unsafe fn execute<IN>(native_exec_id: String, ptr: &IN) -> WeldResult<WeldValue> {
        let ref conf = WeldConf::new();
        // todo:maybe we only need code, no need to maintain native_exec_id
        let module = module_cache(native_exec_id.as_str(), "execute", conf);
        //println!("execute hit:{},miss:{}", get_module_cache_hits(),get_module_cache_misses());
        let ref input_value = WeldValue::new_from_data(ptr as *const _ as Data);

        let ref mut context = WeldContext::new(&conf).unwrap();
        module.run(context, input_value)
    }
    pub unsafe fn execute_with_confs<IN>(
        native_exec_id: String,
        ptr: &IN,
        confs: &WeldConf,
    ) -> WeldResult<WeldValue> {
        let module = module_cache(native_exec_id.as_str(), "execute", confs);
        let ref input_value = WeldValue::new_from_data(ptr as *const _ as Data);
        let ref mut context = WeldContext::new(confs).unwrap();
        module.run(context, input_value)
    }
    pub unsafe fn execute_(native_exec_id: String, ptr: *const c_void) -> WeldResult<WeldValue> {
        let ref conf = WeldConf::new();
        let module = module_cache(native_exec_id.as_str(), "execute", conf);
        let ref input_value = WeldValue::new_from_data(ptr as *const _ as Data);
        let ref mut context = WeldContext::new(&conf).unwrap();
        module.run(context, input_value)
    }
    pub fn set_configurations(confs: &HashMap<&str, &str>) -> WeldConf {
        let mut conf = WeldConf::new();
        for (k, v) in confs.iter() {
            conf.set(k.to_string(), v.to_string());
        }
        conf
    }
}
