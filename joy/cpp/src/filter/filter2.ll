define i1 @filter(%table* %table, i32 %row_index) {
entry:
  %malloccall = tail call i8* @malloc(i32 ptrtoint (<1024 x i1>* getelementptr (<1024 x i1>, <1024 x i1>* null, i32 1) to i32))
  %vec_alloca = bitcast i8* %malloccall to <1024 x i1>*
  %index_var = alloca i32, align 4
  store i32 0, i32* %index_var, align 4
  br label %condition

condition:                                        ; preds = %increment, %entry
  %index = load i32, i32* %index_var, align 4
  %loop_exit_condition = icmp slt i32 %index, 1024
  br i1 %loop_exit_condition, label %body, label %end

body:                                             ; preds = %condition
  %index1 = load i32, i32* %index_var, align 4
  %col_0 = getelementptr inbounds %table, %table* %table, i32 0, i32 0
  %col_0_vec_ptr = load <1024 x i32>*, <1024 x i32>** %col_0, align 8
  %col_0_vec = load <1024 x i32>, <1024 x i32>* %col_0_vec_ptr, align 4096
  %col_0_vec2 = extractelement <1024 x i32> %col_0_vec, i32 %index1
  %tmplt = icmp ult i32 %col_0_vec2, 50
  %col_1 = getelementptr inbounds %table, %table* %table, i32 0, i32 1
  %col_1_vec_ptr = load <1024 x i64>*, <1024 x i64>** %col_1, align 8
  %col_1_vec = load <1024 x i64>, <1024 x i64>* %col_1_vec_ptr, align 8192
  %col_1_vec3 = extractelement <1024 x i64> %col_1_vec, i32 %index1
  %tmpgt = icmp ugt i64 %col_1_vec3, 1
  %tmpand = and i1 %tmplt, %tmpgt
  %col_2 = getelementptr inbounds %table, %table* %table, i32 0, i32 2
  %col_2_vec_ptr = load <1024 x double>*, <1024 x double>** %col_2, align 8
  %col_2_vec = load <1024 x double>, <1024 x double>* %col_2_vec_ptr, align 8192
  %col_2_vec4 = extractelement <1024 x double> %col_2_vec, i32 %index1
  %tmpgt5 = fcmp ugt double %col_2_vec4, 5.800000e+00
  %tmpand6 = and i1 %tmpand, %tmpgt5
  %vec_ptr = load <1024 x i1>, <1024 x i1>* %vec_alloca, align 1024
  %insert = insertelement <1024 x i1> %vec_ptr, i1 %tmpand6, i32 %index1
  store <1024 x i1> %insert, <1024 x i1>* %vec_alloca, align 1024
  br label %increment

increment:                                        ; preds = %body
  %"increment idx" = add i32 %index, 1
  store i32 %"increment idx", i32* %index_var, align 4
  br label %condition

end:                                              ; preds = %condition
  %result_load = load <1024 x i1>, <1024 x i1>* %vec_alloca, align 1024
  %extract = extractelement <1024 x i1> %result_load, i32 50
  ret i1 %extract
}