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
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::os::raw::c_void;
use std::time::Instant;

use inkwell::context::Context;
use inkwell::execution_engine::JitFunction;
use regex::Regex;

use crate::omnicache::runtime::filter::compiler::Data as FilterData;
use crate::omnicache::runtime::filter::compiler::Expr;
use crate::omnicache::runtime::filter::filter::{Filter, FilterFuncType};
use crate::omnicache::runtime::table::Table;

pub struct OmniCodeGen;

impl OmniCodeGen {
    pub fn filter_compile<'ctx>(context: &'ctx Context, expression: &'ctx str, input_type: &Vec<i32>) -> Result<JitFunction<'ctx, FilterFuncType>, &'ctx str> {
        let q1_re = Regex::new(r"\$operator\$LESS_THAN_OR_EQUAL\(#(?P<COL>\d+), (?P<VAL>\d+)\)").unwrap();
        let q6_re = Regex::new(r"AND\(AND\(\$operator\$GREATER_THAN_OR_EQUAL\(#(?P<COL1>\d+), (?P<VAL1>\d+)\), \$operator\$LESS_THAN\(#(?P<COL2>\d+), (?P<VAL2>\d+)\)\), AND\(\$operator\$BETWEEN\(#(?P<COL3>\d+), (?P<VAL3>([0-9]*[.])?[0-9]+), (?P<VAL4>([0-9]*[.])?[0-9]+)\), \$operator\$LESS_THAN\(#(?P<COL4>\d+), (?P<VAL5>([0-9]*[.])?[0-9]+)\)\)\)").unwrap();

        // println!("expression: {}", expression);

        let mut caps_result = q1_re.captures(expression);
        if caps_result.is_some() {
            let caps = caps_result.unwrap();

            let column = Expr::InputReference(caps["COL"].parse::<u32>().unwrap());
            let constant = Expr::Constant(FilterData::Int(caps["VAL"].parse::<i32>().unwrap()));
            let mut expr = Expr::Binary { op: String::from("$operator$LESS_THAN_OR_EQUAL"), left: Box::new(column), right: Box::new(constant) };

            let filter_module = Filter::compile(&expr, &context, &input_type);

            return Result::Ok(filter_module);
        }

        caps_result = q6_re.captures(expression);
        if caps_result.is_some() {
            let caps = caps_result.unwrap();

            let constant = Expr::Constant(FilterData::Long(caps["VAL1"].parse::<i64>().unwrap()));
            let column = Expr::InputReference(caps["COL1"].parse::<u32>().unwrap());
            let constant2 = Expr::Constant(FilterData::Long(caps["VAL2"].parse::<i64>().unwrap()));
            let column2 = Expr::InputReference(caps["COL2"].parse::<u32>().unwrap());
            let constant3 = Expr::Constant(FilterData::Float(caps["VAL3"].parse::<f64>().unwrap()));
            let column3 = Expr::InputReference(caps["COL3"].parse::<u32>().unwrap());
            let constant4 = Expr::Constant(FilterData::Float(caps["VAL4"].parse::<f64>().unwrap()));
            let column4 = Expr::InputReference(caps["COL3"].parse::<u32>().unwrap());
            let constant5 = Expr::Constant(FilterData::Float(caps["VAL5"].parse::<f64>().unwrap()));
            let column5 = Expr::InputReference(caps["COL4"].parse::<u32>().unwrap());
            let gt = Expr::Binary { op: String::from("$operator$GREATER_THAN_OR_EQUAL"), left: Box::new(column), right: Box::new(constant) };
            let lt = Expr::Binary { op: String::from("$operator$LESS_THAN"), left: Box::new(column2), right: Box::new(constant2) };
            let gt2 = Expr::Binary { op: String::from("$operator$GREATER_THAN_OR_EQUAL"), left: Box::new(column3), right: Box::new(constant3) };
            let lt2 = Expr::Binary { op: String::from("$operator$LESS_THAN_OR_EQUAL"), left: Box::new(column4), right: Box::new(constant4) };
            let lt3 = Expr::Binary { op: String::from("$operator$LESS_THAN"), left: Box::new(column5), right: Box::new(constant5) };

            let and = Expr::Binary { op: String::from("AND"), left: Box::new(lt), right: Box::new(gt) };
            let and2 = Expr::Binary { op: String::from("AND"), left: Box::new(lt2), right: Box::new(gt2) };
            let and3 = Expr::Binary { op: String::from("AND"), left: Box::new(and2), right: Box::new(lt3) };

            let mut expr = Expr::Binary { op: String::from("AND"), left: Box::new(and), right: Box::new(and3) };

            let filter_module = Filter::compile(&expr, &context, &input_type);
            return Result::Ok(filter_module);
        }

        return Err("Unsupported row expression");
    }
    pub fn filter_execute(filter_module: JitFunction<'static, FilterFuncType>, input_data: Table, row_number: usize, selected_rows: &mut Vec<bool>) {
        for row_index in 0..row_number - 1 {
            unsafe { selected_rows[row_index] = filter_module.call(input_data.into_ffi_args().as_ptr() as *const c_void, row_index); }
        }
    }
}
