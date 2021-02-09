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
use chashmap::CHashMap;
use once_cell::sync::Lazy;
use weld::{WeldModule, WeldConf};
use std::collections::HashMap;
use std::sync::Arc;
use cached::{TimedSizedCache, Cached};

// static INTERMEDIATE_CACHE: CHashMap<String, IntermediateState<'static>> = Default::default();


#[allow(dead_code)]
#[derive(Clone, Debug)]
pub struct IntermediateState {
    pub addr: *const u8,
    pub len: usize,
}

pub static mut INTERMEDIATE_CACHE: Lazy<HashMap<String, *const u8>> = Lazy::new(|| Default::default());

// module cache
cached_key! {
    MODULE_CACHE:TimedSizedCache<String, Arc<WeldModule>> = TimedSizedCache::with_size_and_lifespan(100,300);
    Key = {format!("{}", neid)};
    fn module_cache(neid: &str, code: &str, conf: &WeldConf) -> Arc<WeldModule> = {
    //println!("create new weld module:{},{}", neid, code);
    let mut module = WeldModule::compile(code, &conf).expect("OmniCache code gen failed!");
    Arc::new(module)
    }
}

pub fn get_module_cache_misses () -> u64 {
    let cache = MODULE_CACHE.lock().unwrap();
    cache.cache_misses().unwrap()
}

pub fn get_module_cache_hits () -> u64 {
    let cache = MODULE_CACHE.lock().unwrap();
    cache.cache_hits().unwrap()
}
