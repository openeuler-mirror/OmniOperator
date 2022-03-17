/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
#include "./omni_jit.h"
#include "gtest/gtest.h"
#include "llvm/Support/SourceMgr.h"
#include "llvm/IRReader/IRReader.h"

// using namespace omniruntime::codegen;
// //TEST(hammer, multi_module) {
// //
// //        outs() << "about to run main\n";
// //    }
// //}
// //
// //TEST(hammer, test_3_columns) {
// //
// //    int v2[] = {2, 1, 2}; //type of each column, should be 1, or 2 for testing now
// //    int v3 = 3;           //number of columns, should be the same as p1 size
// //
// //
// //
// //        outs() << "about to run main\n";
// //    }
// //
// //}
// //
// //
// //TEST(hammer, test_5_columns_with_call) {
// //
// //    int v1[] = {10, 20, 30, 40, 50}; //value of each column, represents a row
// //    int v2[] = {2, 1, 2, 1, 1};      //type of each column, should be 1, or 2 for testing now
// //    int v3 = 5;                      //number of columns, should be the same as p1 size
// //
// //
// //
// //
// //        outs() << "about to run main\n";
// //    }
// //}
// //
// //TEST(hammer, partitial_hardening_5_columns_with_call) {
// //
// //    int v1[] = {11, 22, 33, 44, 66}; //value of each column, represents a row
// //    int v2[] = {2, 1, 2, 1, 1};      //type of each column, should be 1, or 2 for testing now
// //    int v3 = 5;                      //number of columns, should be the same as p1 size
// //
// //
// //
// //
// //
// //        outs() << "about to run main\n";
// //    }
// //}
// //
// //TEST(hammer, partitial_hardening_repeat_5_columns_with_call) {
// //
// //    int v1[] = {11, 22, 33, 44, 66}; //value of each column, represents a row
// //    int v2[] = {2, 1, 2, 1, 1};      //type of each column, should be 1, or 2 for testing now
// //    int v3 = 5;                      //number of columns, should be the same as p1 size
// //
// //
// //
// //
// //        outs() << "about to run main\n";
// //
// //        int v1_1[] = {1, 22, 33, 44, 66}; //value of each column, represents a row
// //
// //        int v1_2[] = {2, 23, 33, 44, 66}; //value of each column, represents a row
// //    }
// //}
// //
// //
// //TEST(hammer, test_5_columns_single_func) {
// //
// //    int v1[] = {11, 22, 33, 44, 66}; //value of each column, represents a row
// //    int v2[] = {2, 1, 2, 1, 1};      //type of each column, should be 1, or 2 for testing now
// //    int v3 = 5;                      //number of columns, should be the same as p1 size
// //
// //
// //
// //
// //        outs() << "about to run main\n";
// //    }
// //}
// //
// //TEST(hammer, test_5_columns_float) {
// //
// //    double v1[] = {11.0, 22.0, 33.0, 44.0, 66.0}; //value of each column, represents a row
// //    int v2[] = {2, 1, 2, 1, 1};      //type of each column, should be 1, or 2 for testing now
// //    int v3 = 5;                      //number of columns, should be the same as p1 size
// //
// //
// //
// //
// //        outs() << "about to run main\n";
// //    }
// //}
// //
// //TEST(hammer, test_2darray_hardening) {
// //
// //    //is there easier way to test which would make it feels almost like
// //    //calling a native function?
// //
// //    //FIXME: need a better way to prepare the paramters
// //    int v1[] = {110, 220, 330, 440, 660}; //value of a column
// //    int v2[] = {1, 2, 3, 4, 5}; //value of a column
// //    int v3[] = {11, 12, 13, 14, 15}; //value of a column
// //
// //
// //    int column_type[] = {1, 1, 1};      //type of each column, should be 1, or 2 for testing now
// //
// //
// //
// //
// //
// //        outs() << "about to run main\n";
// //
// //    }
// //}
// //
// //TEST(hammer, test_2darray_partial_hardening) {
// //
// //    //is there easier way to test which would make it feels almost like
// //    //calling a native function?
// //
// //    //FIXME: need a better way to prepare the paramters
// //    int v1[] = {110, 220, 330, 440, 660}; //value of a column
// //    int v2[] = {1, 2, 3, 4, 5}; //value of a column
// //    int v3[] = {11, 12, 13, 14, 15}; //value of a column
// //
// //
// //    int column_type[] = {1, 1, 1};      //type of each column, should be 1, or 2 for testing now
// //
// //
// //
// //
// //
// //        outs() << "about to run main\n";
// //
// //    }
// //}
// //
// //TEST(hammer, test_2darray_partial_hardening_repeat) {
// //
// //    //is there easier way to test which would make it feels almost like
// //    //calling a native function?
// //
// //    //FIXME: need a better way to prepare the paramters
// //    int v1[] = {110, 220, 330, 440, 660}; //value of a column
// //    int v2[] = {1, 2, 3, 4, 5}; //value of a column
// //    int v3[] = {11, 12, 13, 14, 15}; //value of a column
// //    int v3_2[] = {21, 22, 23, 24, 25}; //value of a column
// //
// //
// //    int column_type[] = {1, 1, 1};      //type of each column, should be 1, or 2 for testing now
// //
// //
// //
// //
// //
// //
// //        outs() << "about to run main\n";
// //
// //    }
// //}
// //
// //TEST(hammer, test_2darray_param) {
// //    int v1[] = {11, 22, 33, 44, 66}; //value of each column, represents a row
// //    double v2[] = {2.0, 1.0, 2.0, 1.0, 1.0};      //type of each column, should be 1, or 2 for testing now
// //
// //
// //            }
// //        }
// //            }
// //        }
// //    }
// //}
// //
// //TEST(hammer, test_vector) {
// //
// //    //is there easier way to test which would make it feels almost like
// //    //calling a native function?
// //
// //
// //
// //
// //
// //        outs() << "about to run main\n";
// //
// //    }
// //}
// //
// //TEST(hammer, test_remove_attribute) {
// //
// //    int v1[] = {11, 22, 33, 44, 66}; //value of each column, represents a row
// //    int v2[] = {2, 1, 2, 1, 1};      //type of each column, should be 1, or 2 for testing now
// //    int v3 = 5;                      //number of columns, should be the same as p1 size
// //
// //
// //
// //
// //        outs() << "about to run main\n";
// //    }
// //}

// int main(int argc, char **argv) {
// }
