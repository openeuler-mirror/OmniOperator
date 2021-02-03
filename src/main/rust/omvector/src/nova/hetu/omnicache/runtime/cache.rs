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
use weld::WeldModule;
use std::collections::HashMap;

// static INTERMEDIATE_CACHE: CHashMap<String, IntermediateState<'static>> = Default::default();


#[allow(dead_code)]
#[derive(Clone, Debug)]
pub struct IntermediateState {
    pub addr: *const u8,
    pub len: usize,
}

pub static mut INTERMEDIATE_CACHE: Lazy<HashMap<String, IntermediateState>> = Lazy::new(|| Default::default());
pub static CACHE: Lazy<CHashMap<String, WeldModule>> = Lazy::new(|| Default::default());
