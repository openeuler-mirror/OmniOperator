#include "./hammer.h"
#include "gtest/gtest.h"
#include "llvm/Support/SourceMgr.h"
#include "llvm/IRReader/IRReader.h"

using namespace codegen;
//TEST(hammer, multi_module) {
//    std::map<std::string, ParamValue *> testParam;
//    Hammer hammer("test_data/multi-module/a.ll", testParam);
//    HammerConfig * conf = HammerConfig::getConf();
//    std::list<Hammer*> deps;
//    auto JITTER = hammer.create_jitter(deps, *conf);
//printf("1\n");
//    SMDiagnostic error;
//    auto ctx1 = std::make_unique<LLVMContext>();
//    auto lib1 = parseIRFile("test_data/multi-module/b.ll", error, *ctx1);
//    auto err = JITTER->addIRModule(ThreadSafeModule(std::move(lib1), std::move(ctx1)));
//    printf("2\n");
//
//    auto func = JITTER->lookup("_Z6callerv");
//    if (func) {
//        printf("3\n");
//        outs() << "looking up main\n";
//        auto main = (int (*)(void)) func->getAddress();
//        outs() << "about to run main\n";
//        auto result = main();
//        EXPECT_EQ(1234, result);
//    }
//}
//
//TEST(hammer, test_3_columns) {
//    std::map<std::string, ParamValue *> testParam;
//
//    int v1[] = {10, 20, 30};
//    int v2[] = {2, 1, 2}; //type of each column, should be 1, or 2 for testing now
//    int v3 = 3;           //number of columns, should be the same as p1 size
//
//    auto p1 = ParamValue(v1 /* value */, 3 /*size*/);
//    auto p2 = ParamValue(v2, 3);
//    auto p3 = ParamValue(&v3);
//    testParam["_Z7processPiS_i@0"] = &p1;
//    testParam["_Z7processPiS_i@1"] = &p2;
//    testParam["_Z7processPiS_i@2"] = &p3;
//
//    Hammer hammer1("test_data/func_w_param.ll", testParam);
//    hammer1.harden();
//
//    HammerConfig * conf = HammerConfig::getConf();
//    std::list<Hammer*> deps;
//    auto JITTER = hammer1.create_jitter(deps, *conf);
//    auto func = JITTER->lookup("_Z7processPiS_i");
//    if (func) {
//        outs() << "looking up main\n";
//        auto main = (int (*)(int *, int *, int)) func->getAddress();
//        outs() << "about to run main\n";
//        auto result = main(v1, v2, v3);
//        EXPECT_EQ(60, result);
//    }
//
//}
//
//
//TEST(hammer, test_5_columns_with_call) {
//    std::map<std::string, ParamValue *> testParam;
//
//    int v1[] = {10, 20, 30, 40, 50}; //value of each column, represents a row
//    int v2[] = {2, 1, 2, 1, 1};      //type of each column, should be 1, or 2 for testing now
//    int v3 = 5;                      //number of columns, should be the same as p1 size
//
//    auto p1 = ParamValue(v1, 5);
//    auto p2 = ParamValue(v2, 5);
//    auto p3 = ParamValue(&v3);
//
//    testParam["_Z7processPiS_i@0"] = &p1;
//    testParam["_Z7processPiS_i@1"] = &p2;
//    testParam["_Z7processPiS_i@2"] = &p3;
//
//    Hammer hammer1("test_data/func_w_call.ll", testParam);
//    hammer1.harden();
//
//    HammerConfig * conf = HammerConfig::getConf();
//    std::list<Hammer*> deps;
//    auto JITTER = hammer1.create_jitter(deps, *conf);
//    auto func = JITTER->lookup("_Z7processPiS_i");
//    if (func) {
//        outs() << "looking up main\n";
//        auto main = (int (*)(int *, int *, int)) func->getAddress();
//        outs() << "about to run main\n";
//        auto result = main(v1, v2, v3);
//        EXPECT_EQ(155, result);
//    }
//}
//
//TEST(hammer, partitial_hardening_5_columns_with_call) {
//    std::map<std::string, ParamValue *> testParam;
//
//    int v1[] = {11, 22, 33, 44, 66}; //value of each column, represents a row
//    int v2[] = {2, 1, 2, 1, 1};      //type of each column, should be 1, or 2 for testing now
//    int v3 = 5;                      //number of columns, should be the same as p1 size
//
//    auto p1 = ParamValue(v1, 5);
//    auto p2 = ParamValue(v2, 5);
//    auto p3 = ParamValue(&v3);
//
//    //testParam["_Z7processPiS_i@0"] = &p1;
//    testParam["_Z7processPiS_i@1"] = &p2;
//    testParam["_Z7processPiS_i@2"] = &p3;
//
//
//    Hammer hammer1("test_data/func_w_call.ll", testParam);
//    hammer1.harden();
//
//    HammerConfig * conf = HammerConfig::getConf();
//    std::list<Hammer*> deps;
//    auto JITTER = hammer1.create_jitter(deps,*conf);
//    auto func = JITTER->lookup("_Z7processPiS_i");
//    if (func) {
//        outs() << "looking up main\n";
//        auto main = (int (*)(int *, int *, int)) func->getAddress();
//        outs() << "about to run main\n";
//        auto result = main(v1, v2, v3);
//        EXPECT_EQ(181, result);
//    }
//}
//
//TEST(hammer, partitial_hardening_repeat_5_columns_with_call) {
//    std::map<std::string, ParamValue *> testParam;
//
//    int v1[] = {11, 22, 33, 44, 66}; //value of each column, represents a row
//    int v2[] = {2, 1, 2, 1, 1};      //type of each column, should be 1, or 2 for testing now
//    int v3 = 5;                      //number of columns, should be the same as p1 size
//
//    auto p1 = ParamValue(v1, 5);
//    auto p2 = ParamValue(v2, 5);
//    auto p3 = ParamValue(&v3);
//
//    //testParam["_Z7processPiS_i@0"] = &p1;
//    testParam["_Z7processPiS_i@1"] = &p2;
//    testParam["_Z7processPiS_i@2"] = &p3;
//
//
//    Hammer hammer1("test_data/func_w_call.ll", testParam);
//    hammer1.harden();
//    HammerConfig * conf = HammerConfig::getConf();
//    std::list<Hammer*> deps;
//    auto JITTER = hammer1.create_jitter(deps,*conf);
//    auto func = JITTER->lookup("_Z7processPiS_i");
//    if (func) {
//        outs() << "looking up main\n";
//        auto main = (int (*)(int *, int *, int)) func->getAddress();
//        outs() << "about to run main\n";
//        auto result = main(v1, v2, v3);
//        EXPECT_EQ(181, result);
//
//        int v1_1[] = {1, 22, 33, 44, 66}; //value of each column, represents a row
//        result = main(v1_1, v2, v3);
//        EXPECT_EQ(171, result);
//
//        int v1_2[] = {2, 23, 33, 44, 66}; //value of each column, represents a row
//        result = main(v1_2, v2, v3);
//        EXPECT_EQ(173, result);
//    }
//}
//
//
//TEST(hammer, test_5_columns_single_func) {
//    std::map<std::string, ParamValue *> testParam;
//
//    int v1[] = {11, 22, 33, 44, 66}; //value of each column, represents a row
//    int v2[] = {2, 1, 2, 1, 1};      //type of each column, should be 1, or 2 for testing now
//    int v3 = 5;                      //number of columns, should be the same as p1 size
//
//    auto p1 = ParamValue(v1, 5);
//    auto p2 = ParamValue(v2, 5);
//    auto p3 = ParamValue(&v3);
//
//    testParam["_Z7processPiS_i@0"] = &p1;
//    testParam["_Z7processPiS_i@1"] = &p2;
//    testParam["_Z7processPiS_i@2"] = &p3;
//
//    Hammer hammer1("test_data/func_w_call.ll", testParam);
//    hammer1.harden();
//
//    HammerConfig * conf = HammerConfig::getConf();
//    std::list<Hammer*> deps;
//    auto JITTER = hammer1.create_jitter(deps,*conf);
//    auto func = JITTER->lookup("_Z7processPiS_i");
//    if (func) {
//        outs() << "looking up main\n";
//        auto main = (int (*)(int *, int *, int)) func->getAddress();
//        outs() << "about to run main\n";
//        auto result = main(v1, v2, v3);
//        EXPECT_EQ(181, result);
//    }
//}
//
//TEST(hammer, test_5_columns_float) {
//    std::map<std::string, ParamValue *> testParam;
//
//    double v1[] = {11.0, 22.0, 33.0, 44.0, 66.0}; //value of each column, represents a row
//    int v2[] = {2, 1, 2, 1, 1};      //type of each column, should be 1, or 2 for testing now
//    int v3 = 5;                      //number of columns, should be the same as p1 size
//
//    auto p1 = ParamValue(v1, 5);
//    auto p2 = ParamValue(v2, 5);
//    auto p3 = ParamValue(&v3);
//
//    testParam["_Z7processPiS_i@0"] = &p1;
//    testParam["_Z7processPiS_i@1"] = &p2;
//    testParam["_Z7processPiS_i@2"] = &p3;
//
//    Hammer hammer1("test_data/func_w_double_param.ll", testParam);
//    hammer1.harden();
//
//    HammerConfig * conf = HammerConfig::getConf();
//    std::list<Hammer*> deps;
//    auto JITTER = hammer1.create_jitter(deps,*conf);
//    auto func = JITTER->lookup("_Z7processPdPii");
//    if (func) {
//        outs() << "looking up main\n";
//        auto main = (double (*)(double *, int *, int)) func->getAddress();
//        outs() << "about to run main\n";
//        auto result = main(v1, v2, v3);
//        EXPECT_EQ(176.0, result);
//    }
//}
//
//TEST(hammer, test_2darray_hardening) {
//    std::map<std::string, ParamValue *> testParam;
//
//    //is there easier way to test which would make it feels almost like
//    //calling a native function?
//
//    //FIXME: need a better way to prepare the paramters
//    int v1[] = {110, 220, 330, 440, 660}; //value of a column
//    int v2[] = {1, 2, 3, 4, 5}; //value of a column
//    int v3[] = {11, 12, 13, 14, 15}; //value of a column
//
//    void *columns[] = {v1, v2, v3};
//
//    int column_type[] = {1, 1, 1};      //type of each column, should be 1, or 2 for testing now
//    int column_count = 3;
//    int row_count = 5;
//
//    auto p1 = ParamValue(v1, 5);
//    auto p2 = ParamValue(v2, 5);
//    auto p3 = ParamValue(v3, 5);
//    auto p3_list = std::list<ParamValue>();
//    p3_list.push_back(p1);
//    p3_list.push_back(p2);
//    p3_list.push_back(p3);
//    auto p4 = ParamValue(&p3_list, 3);
//
//    auto p_col_type = ParamValue(column_type, 3);
//    auto p_col_count = ParamValue(&column_count);
//    auto p_row_count = ParamValue(&row_count);
//
//    testParam["_Z7processPPvPiii@0"] = &p4;
//    testParam["_Z7processPPvPiii@1"] = &p_col_type;
//    testParam["_Z7processPPvPiii@2"] = &p_col_count;
//    testParam["_Z7processPPvPiii@3"] = &p_row_count;
//
//    Hammer hammer("test_data/func_w_2darray_param.ll", testParam);
//    hammer.harden();
//
//    HammerConfig * conf = HammerConfig::getConf();
//    std::list<Hammer*> deps;
//    auto JITTER = hammer.create_jitter(deps,*conf);
//    auto func = JITTER->lookup("_Z7processPPvPiii");
//    if (func) {
//        outs() << "looking up main\n";
//        auto main = (double (*)(void **, int *, int, int)) func->getAddress();
//        outs() << "about to run main\n";
//
//        auto result = main((void **) columns, column_type, column_count, row_count);
//        EXPECT_EQ(1840.0, result);
//    }
//}
//
//TEST(hammer, test_2darray_partial_hardening) {
//    std::map<std::string, ParamValue *> testParam;
//
//    //is there easier way to test which would make it feels almost like
//    //calling a native function?
//
//    //FIXME: need a better way to prepare the paramters
//    int v1[] = {110, 220, 330, 440, 660}; //value of a column
//    int v2[] = {1, 2, 3, 4, 5}; //value of a column
//    int v3[] = {11, 12, 13, 14, 15}; //value of a column
//
//    void *columns[] = {v1, v2, v3};
//
//    int column_type[] = {1, 1, 1};      //type of each column, should be 1, or 2 for testing now
//    int column_count = 3;
//    int row_count = 5;
//
//    auto p1 = ParamValue(v1, 5);
//    auto p2 = ParamValue(v2, 5);
//    auto p3 = ParamValue(v3, 5);
//    auto p3_list = std::list<ParamValue>();
//    p3_list.push_back(p1);
//    p3_list.push_back(p2);
//    p3_list.push_back(p3);
//    auto p4 = ParamValue(&p3_list, 3);
//
//    auto p_col_type = ParamValue(column_type, 3);
//    auto p_col_count = ParamValue(&column_count);
//    auto p_row_count = ParamValue(&row_count);
//
//    //testParam["_Z7processPPvPiii@0"] = &p4;
//    testParam["_Z7processPPvPiii@1"] = &p_col_type;
//    testParam["_Z7processPPvPiii@2"] = &p_col_count;
//    testParam["_Z7processPPvPiii@3"] = &p_row_count;
//
//    Hammer hammer("test_data/func_w_2darray_param.ll", testParam);
//    hammer.harden();
//
//    HammerConfig * conf = HammerConfig::getConf();
//    std::list<Hammer*> deps;
//    auto JITTER = hammer.create_jitter(deps,*conf);
//    auto func = JITTER->lookup("_Z7processPPvPiii");
//    if (func) {
//        outs() << "looking up main\n";
//        auto main = (double (*)(void **, int *, int, int)) func->getAddress();
//        outs() << "about to run main\n";
//
//        auto result = main((void **) columns, column_type, column_count, row_count);
//        EXPECT_EQ(1840.0, result);
//    }
//}
//
//TEST(hammer, test_2darray_partial_hardening_repeat) {
//    std::map<std::string, ParamValue *> testParam;
//
//    //is there easier way to test which would make it feels almost like
//    //calling a native function?
//
//    //FIXME: need a better way to prepare the paramters
//    int v1[] = {110, 220, 330, 440, 660}; //value of a column
//    int v2[] = {1, 2, 3, 4, 5}; //value of a column
//    int v3[] = {11, 12, 13, 14, 15}; //value of a column
//    int v3_2[] = {21, 22, 23, 24, 25}; //value of a column
//
//    void *columns[] = {v1, v2, v3};
//    void *columns2[] = {v1, v2, v3_2};
//
//    int column_type[] = {1, 1, 1};      //type of each column, should be 1, or 2 for testing now
//    int column_count = 3;
//    int row_count = 5;
//
//    auto p1 = ParamValue(v1, 5);
//    auto p2 = ParamValue(v2, 5);
//    auto p3 = ParamValue(v3, 5);
//    auto p3_list = std::list<ParamValue>();
//    p3_list.push_back(p1);
//    p3_list.push_back(p2);
//    p3_list.push_back(p3);
//    auto p4 = ParamValue(&p3_list, 3);
//
//    auto p_col_type = ParamValue(column_type, 3);
//    auto p_col_count = ParamValue(&column_count);
//    auto p_row_count = ParamValue(&row_count);
//
//    //testParam["_Z7processPPvPiii@0"] = &p4;
//    testParam["_Z7processPPvPiii@1"] = &p_col_type;
//    testParam["_Z7processPPvPiii@2"] = &p_col_count;
//    testParam["_Z7processPPvPiii@3"] = &p_row_count;
//
//    Hammer hammer("test_data/func_w_2darray_param.ll", testParam);
//    hammer.harden();
//
//    HammerConfig * conf = HammerConfig::getConf();
//    std::list<Hammer*> deps;
//    auto JITTER = hammer.create_jitter(deps,*conf);
//
//    auto func = JITTER->lookup("_Z7processPPvPiii");
//    if (func) {
//        outs() << "looking up main\n";
//        auto main = (double (*)(void **, int *, int, int)) func->getAddress();
//        outs() << "about to run main\n";
//
//        auto result = main((void **) columns, column_type, column_count, row_count);
//        EXPECT_EQ(1840.0, result);
//        result = main((void **) columns2, column_type, column_count, row_count);
//        EXPECT_EQ(1890.0, result);
//    }
//}
//
//TEST(hammer, test_2darray_param) {
//    int v1[] = {11, 22, 33, 44, 66}; //value of each column, represents a row
//    double v2[] = {2.0, 1.0, 2.0, 1.0, 1.0};      //type of each column, should be 1, or 2 for testing now
//
//    auto p1 = ParamValue(v1, 5);
//    auto p2 = ParamValue(v2, 5);
//    auto p3_list = std::list<ParamValue>();
//    p3_list.push_back(p1);
//    p3_list.push_back(p2);
//    auto p3 = ParamValue(&p3_list, 2);
//
//    auto p4 = p3.to_param_list();
//    int count = 0;
//    for (ParamValue param : *p4) {
//        if (count == 0) {
//            auto value = param.to_int32_array();
//            for (int j = 0; j < param.size; j++) {
//                EXPECT_EQ(v1[j], value[j]);
//            }
//        }
//        if (count == 1) {
//            auto value = param.to_fp64_array();
//            for (int j = 0; j < param.size; j++) {
//                EXPECT_EQ(v2[j], value[j]);
//            }
//        }
//        count++;
//    }
//}
//
//TEST(hammer, test_vector) {
//    std::map<std::string, ParamValue *> testParam;
//
//    //is there easier way to test which would make it feels almost like
//    //calling a native function?
//
//    std::vector<int> vec;
//    vec.push_back(1);
//    vec.push_back(2);
//    auto p1 = ParamValue(&vec);
//
//    testParam["_Z7processSt6vectorIiSaIiEE@0"] = &p1;
//
//    Hammer hammer("test_data/func_w_vector_param.ll", testParam);
//    //hammer.harden();
//
//    HammerConfig * conf = HammerConfig::getConf();
//    std::list<Hammer*> deps;
//    auto JITTER = hammer.create_jitter(deps,*conf);
//
//    auto func = JITTER->lookup("_Z7processSt6vectorIiSaIiEE");
//    if (func) {
//        outs() << "looking up main\n";
//        auto main = (void (*)(std::vector<int> vector)) func->getAddress();
//        outs() << "about to run main\n";
//
//        main(vec);
//    }
//}
//
//TEST(hammer, test_remove_attribute) {
//    std::map<std::string, ParamValue *> testParam;
//
//    int v1[] = {11, 22, 33, 44, 66}; //value of each column, represents a row
//    int v2[] = {2, 1, 2, 1, 1};      //type of each column, should be 1, or 2 for testing now
//    int v3 = 5;                      //number of columns, should be the same as p1 size
//
//    auto p1 = ParamValue(v1, 5);
//    auto p2 = ParamValue(v2, 5);
//    auto p3 = ParamValue(&v3);
//
//    testParam["_Z7processPiS_i@0"] = &p1;
//    testParam["_Z7processPiS_i@1"] = &p2;
//    testParam["_Z7processPiS_i@2"] = &p3;
//
//    Hammer hammer1("test_data/func_w_call.ll", testParam);
//    hammer1.harden();
//
//    HammerConfig * conf = HammerConfig::getConf();
//    std::list<Hammer*> deps;
//    auto JITTER = hammer1.create_jitter(deps,*conf);
//    auto func = JITTER->lookup("_Z7processPiS_i");
//    if (func) {
//        outs() << "looking up main\n";
//        auto main = (int (*)(int *, int *, int)) func->getAddress();
//        outs() << "about to run main\n";
//        auto result = main(v1, v2, v3);
//        EXPECT_EQ(181, result);
//    }
//}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
