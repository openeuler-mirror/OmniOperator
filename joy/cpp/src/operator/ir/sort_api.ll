; ModuleID = '/usr/code/olk_dev/omni_runtime_joy/omni-cache/joy/cpp/src/operator/ir/../../jni/sort_api.cpp'
source_filename = "/usr/code/olk_dev/omni_runtime_joy/omni-cache/joy/cpp/src/operator/ir/../../jni/sort_api.cpp"
target datalayout = "e-m:e-p270:32:32-p271:32:32-p272:64:64-i64:64-f80:128-n8:16:32:64-S128"
target triple = "x86_64-pc-linux-gnu"

%"class.std::ios_base::Init" = type { i8 }
%class.OpTemplateCache = type { %"class.std::mutex", %"class.std::unordered_map" }
%"class.std::mutex" = type { %"class.std::__mutex_base" }
%"class.std::__mutex_base" = type { %union.pthread_mutex_t }
%union.pthread_mutex_t = type { %struct.__pthread_mutex_s }
%struct.__pthread_mutex_s = type { i32, i32, i32, i32, i32, i16, i16, %struct.__pthread_internal_list }
%struct.__pthread_internal_list = type { %struct.__pthread_internal_list*, %struct.__pthread_internal_list* }
%"class.std::unordered_map" = type { %"class.std::_Hashtable" }
%"class.std::_Hashtable" = type { %"struct.std::__detail::_Hash_node_base"**, i64, %"struct.std::__detail::_Hash_node_base", i64, %"struct.std::__detail::_Prime_rehash_policy", %"struct.std::__detail::_Hash_node_base"* }
%"struct.std::__detail::_Hash_node_base" = type { %"struct.std::__detail::_Hash_node_base"* }
%"struct.std::__detail::_Prime_rehash_policy" = type { float, i64 }
%"struct.std::piecewise_construct_t" = type { i8 }
%"struct.std::__detail::_Hash_node" = type { %"struct.std::__detail::_Hash_node_value_base" }
%"struct.std::__detail::_Hash_node_value_base" = type { %"struct.std::__detail::_Hash_node_base", %"struct.__gnu_cxx::__aligned_buffer" }
%"struct.__gnu_cxx::__aligned_buffer" = type { %"union.std::aligned_storage<16, 8>::type" }
%"union.std::aligned_storage<16, 8>::type" = type { [16 x i8] }
%"struct.std::_Rb_tree<std::__cxx11::basic_string<char>, std::pair<const std::__cxx11::basic_string<char>, codegen::ParamValue *>, std::_Select1st<std::pair<const std::__cxx11::basic_string<char>, codegen::ParamValue *>>, std::less<std::__cxx11::basic_string<char>>>::_Alloc_node" = type { %"class.std::_Rb_tree"* }
%"class.std::_Rb_tree" = type { %"struct.std::_Rb_tree<std::__cxx11::basic_string<char>, std::pair<const std::__cxx11::basic_string<char>, codegen::ParamValue *>, std::_Select1st<std::pair<const std::__cxx11::basic_string<char>, codegen::ParamValue *>>, std::less<std::__cxx11::basic_string<char>>>::_Rb_tree_impl" }
%"struct.std::_Rb_tree<std::__cxx11::basic_string<char>, std::pair<const std::__cxx11::basic_string<char>, codegen::ParamValue *>, std::_Select1st<std::pair<const std::__cxx11::basic_string<char>, codegen::ParamValue *>>, std::less<std::__cxx11::basic_string<char>>>::_Rb_tree_impl" = type { %"struct.std::_Rb_tree_key_compare", %"struct.std::_Rb_tree_header" }
%"struct.std::_Rb_tree_key_compare" = type { %"struct.std::less" }
%"struct.std::less" = type { i8 }
%"struct.std::_Rb_tree_header" = type { %"struct.std::_Rb_tree_node_base", i64 }
%"struct.std::_Rb_tree_node_base" = type { i32, %"struct.std::_Rb_tree_node_base"*, %"struct.std::_Rb_tree_node_base"*, %"struct.std::_Rb_tree_node_base"* }
%"class.std::map" = type { %"class.std::_Rb_tree" }
%"class.std::__cxx11::list" = type { %"class.std::__cxx11::_List_base" }
%"class.std::__cxx11::_List_base" = type { %"struct.std::__cxx11::_List_base<codegen::Hammer *, std::allocator<codegen::Hammer *>>::_List_impl" }
%"struct.std::__cxx11::_List_base<codegen::Hammer *, std::allocator<codegen::Hammer *>>::_List_impl" = type { %"struct.std::_List_node" }
%"struct.std::_List_node" = type { %"struct.std::__detail::_List_node_base", %"struct.__gnu_cxx::__aligned_membuf" }
%"struct.std::__detail::_List_node_base" = type { %"struct.std::__detail::_List_node_base"*, %"struct.std::__detail::_List_node_base"* }
%"struct.__gnu_cxx::__aligned_membuf" = type { [8 x i8] }
%"class.codegen::ParamValue" = type <{ i8*, i32, i32, i8, [7 x i8] }>
%"class.std::__cxx11::basic_string" = type { %"struct.std::__cxx11::basic_string<char>::_Alloc_hider", i64, %union.anon }
%"struct.std::__cxx11::basic_string<char>::_Alloc_hider" = type { i8* }
%union.anon = type { i64, [8 x i8] }
%"class.codegen::Hammer" = type { %"class.llvm::ExitOnError", %"class.std::unique_ptr", %"class.std::unique_ptr.16", %"class.std::unique_ptr.25", %"class.std::unique_ptr.34", %"class.std::unique_ptr.119", %"class.std::map" }
%"class.llvm::ExitOnError" = type { %"class.std::__cxx11::basic_string", %"class.std::function" }
%"class.std::function" = type { %"class.std::_Function_base", i32 (%"union.std::_Any_data"*, %"class.llvm::Error"*)* }
%"class.std::_Function_base" = type { %"union.std::_Any_data", i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* }
%"union.std::_Any_data" = type { %"union.std::_Nocopy_types" }
%"union.std::_Nocopy_types" = type { { i64, i64 } }
%"class.llvm::Error" = type { %"class.llvm::ErrorInfoBase"* }
%"class.llvm::ErrorInfoBase" = type { i32 (...)** }
%"class.std::unique_ptr" = type { %"class.std::__uniq_ptr_impl" }
%"class.std::__uniq_ptr_impl" = type { %"class.std::tuple" }
%"class.std::tuple" = type { %"struct.std::_Tuple_impl" }
%"struct.std::_Tuple_impl" = type { %"struct.std::_Head_base.15" }
%"struct.std::_Head_base.15" = type { %"class.llvm::LLVMContext"* }
%"class.llvm::LLVMContext" = type { %"class.llvm::LLVMContextImpl"* }
%"class.llvm::LLVMContextImpl" = type opaque
%"class.std::unique_ptr.16" = type { %"class.std::__uniq_ptr_impl.17" }
%"class.std::__uniq_ptr_impl.17" = type { %"class.std::tuple.18" }
%"class.std::tuple.18" = type { %"struct.std::_Tuple_impl.19" }
%"struct.std::_Tuple_impl.19" = type { %"struct.std::_Head_base.24" }
%"struct.std::_Head_base.24" = type { %"class.llvm::StringRef"* }
%"class.llvm::StringRef" = type { i8*, i64 }
%"class.std::unique_ptr.25" = type { %"class.std::__uniq_ptr_impl.26" }
%"class.std::__uniq_ptr_impl.26" = type { %"class.std::tuple.27" }
%"class.std::tuple.27" = type { %"struct.std::_Tuple_impl.28" }
%"struct.std::_Tuple_impl.28" = type { %"struct.std::_Head_base.33" }
%"struct.std::_Head_base.33" = type { %"class.llvm::IRBuilder"* }
%"class.llvm::IRBuilder" = type { %"class.llvm::IRBuilderBase", %"class.llvm::ConstantFolder", %"class.llvm::IRBuilderDefaultInserter" }
%"class.llvm::IRBuilderBase" = type { %"class.llvm::SmallVector.396", %"class.llvm::BasicBlock"*, %"class.llvm::ilist_iterator", %"class.llvm::LLVMContext"*, %"class.llvm::IRBuilderFolder"*, %"class.llvm::IRBuilderDefaultInserter"*, %"class.llvm::MDNode"*, %"class.llvm::FastMathFlags", i8, i8, i8, %"class.llvm::ArrayRef" }
%"class.llvm::SmallVector.396" = type { %"class.llvm::SmallVectorImpl.397", %"struct.llvm::SmallVectorStorage.400" }
%"class.llvm::SmallVectorImpl.397" = type { %"class.llvm::SmallVectorTemplateBase.398" }
%"class.llvm::SmallVectorTemplateBase.398" = type { %"class.llvm::SmallVectorTemplateCommon.399" }
%"class.llvm::SmallVectorTemplateCommon.399" = type { %"class.llvm::SmallVectorBase.107" }
%"class.llvm::SmallVectorBase.107" = type { i8*, i32, i32 }
%"struct.llvm::SmallVectorStorage.400" = type { [32 x i8] }
%"class.llvm::BasicBlock" = type { %"class.llvm::Value", %"class.llvm::ilist_node_with_parent", %"class.llvm::SymbolTableList.402", %"class.llvm::Function"* }
%"class.llvm::Value" = type { %"class.llvm::Type"*, %"class.llvm::Use"*, i8, i8, i16, i32 }
%"class.llvm::Type" = type { %"class.llvm::LLVMContext"*, i32, i32, %"class.llvm::Type"** }
%"class.llvm::Use" = type { %"class.llvm::Value"*, %"class.llvm::Use"*, %"class.llvm::Use"**, %"class.llvm::User"* }
%"class.llvm::User" = type { %"class.llvm::Value" }
%"class.llvm::ilist_node_with_parent" = type { %"class.llvm::ilist_node" }
%"class.llvm::ilist_node" = type { %"class.llvm::ilist_node_impl.401" }
%"class.llvm::ilist_node_impl.401" = type { %"class.llvm::ilist_node_base" }
%"class.llvm::ilist_node_base" = type { %"class.llvm::ilist_node_base"*, %"class.llvm::ilist_node_base"* }
%"class.llvm::SymbolTableList.402" = type { %"class.llvm::iplist_impl.403" }
%"class.llvm::iplist_impl.403" = type { %"class.llvm::simple_ilist.406" }
%"class.llvm::simple_ilist.406" = type { %"class.llvm::ilist_sentinel.408" }
%"class.llvm::ilist_sentinel.408" = type { %"class.llvm::ilist_node_impl.409" }
%"class.llvm::ilist_node_impl.409" = type { %"class.llvm::ilist_node_base" }
%"class.llvm::Function" = type { %"class.llvm::GlobalObject", %"class.llvm::ilist_node.411", %"class.llvm::SymbolTableList.412", %"class.llvm::Argument"*, i64, %"class.std::unique_ptr.73", %"class.llvm::AttributeList" }
%"class.llvm::GlobalObject" = type { %"class.llvm::GlobalValue", %"class.llvm::Comdat"* }
%"class.llvm::GlobalValue" = type { %"class.llvm::Constant", %"class.llvm::Type"*, i32, i32, %"class.llvm::Module"* }
%"class.llvm::Constant" = type { %"class.llvm::User" }
%"class.llvm::Module" = type { %"class.llvm::LLVMContext"*, %"class.llvm::SymbolTableList", %"class.llvm::SymbolTableList.43", %"class.llvm::SymbolTableList.51", %"class.llvm::SymbolTableList.59", %"class.llvm::iplist", %"class.std::__cxx11::basic_string", %"class.std::unique_ptr.73", %"class.llvm::StringMap", %"class.std::unique_ptr.82", %"class.std::unique_ptr.91", %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string", %"class.llvm::StringMap.100", %"class.llvm::DataLayout" }
%"class.llvm::SymbolTableList" = type { %"class.llvm::iplist_impl" }
%"class.llvm::iplist_impl" = type { %"class.llvm::simple_ilist" }
%"class.llvm::simple_ilist" = type { %"class.llvm::ilist_sentinel" }
%"class.llvm::ilist_sentinel" = type { %"class.llvm::ilist_node_impl" }
%"class.llvm::ilist_node_impl" = type { %"class.llvm::ilist_node_base" }
%"class.llvm::SymbolTableList.43" = type { %"class.llvm::iplist_impl.44" }
%"class.llvm::iplist_impl.44" = type { %"class.llvm::simple_ilist.47" }
%"class.llvm::simple_ilist.47" = type { %"class.llvm::ilist_sentinel.49" }
%"class.llvm::ilist_sentinel.49" = type { %"class.llvm::ilist_node_impl.50" }
%"class.llvm::ilist_node_impl.50" = type { %"class.llvm::ilist_node_base" }
%"class.llvm::SymbolTableList.51" = type { %"class.llvm::iplist_impl.52" }
%"class.llvm::iplist_impl.52" = type { %"class.llvm::simple_ilist.55" }
%"class.llvm::simple_ilist.55" = type { %"class.llvm::ilist_sentinel.57" }
%"class.llvm::ilist_sentinel.57" = type { %"class.llvm::ilist_node_impl.58" }
%"class.llvm::ilist_node_impl.58" = type { %"class.llvm::ilist_node_base" }
%"class.llvm::SymbolTableList.59" = type { %"class.llvm::iplist_impl.60" }
%"class.llvm::iplist_impl.60" = type { %"class.llvm::simple_ilist.63" }
%"class.llvm::simple_ilist.63" = type { %"class.llvm::ilist_sentinel.65" }
%"class.llvm::ilist_sentinel.65" = type { %"class.llvm::ilist_node_impl.66" }
%"class.llvm::ilist_node_impl.66" = type { %"class.llvm::ilist_node_base" }
%"class.llvm::iplist" = type { %"class.llvm::iplist_impl.67" }
%"class.llvm::iplist_impl.67" = type { %"class.llvm::simple_ilist.69" }
%"class.llvm::simple_ilist.69" = type { %"class.llvm::ilist_sentinel.71" }
%"class.llvm::ilist_sentinel.71" = type { %"class.llvm::ilist_node_impl.72" }
%"class.llvm::ilist_node_impl.72" = type { %"class.llvm::ilist_node_base" }
%"class.llvm::StringMap" = type <{ %"class.llvm::StringMapImpl", %"class.llvm::MallocAllocator", [7 x i8] }>
%"class.llvm::StringMapImpl" = type { %"class.llvm::StringMapEntryBase"**, i32, i32, i32, i32 }
%"class.llvm::StringMapEntryBase" = type { i64 }
%"class.llvm::MallocAllocator" = type { i8 }
%"class.std::unique_ptr.82" = type { %"class.std::__uniq_ptr_impl.83" }
%"class.std::__uniq_ptr_impl.83" = type { %"class.std::tuple.84" }
%"class.std::tuple.84" = type { %"struct.std::_Tuple_impl.85" }
%"struct.std::_Tuple_impl.85" = type { %"struct.std::_Head_base.90" }
%"struct.std::_Head_base.90" = type { %"class.llvm::MemoryBuffer"* }
%"class.llvm::MemoryBuffer" = type { i32 (...)**, i8*, i8* }
%"class.std::unique_ptr.91" = type { %"class.std::__uniq_ptr_impl.92" }
%"class.std::__uniq_ptr_impl.92" = type { %"class.std::tuple.93" }
%"class.std::tuple.93" = type { %"struct.std::_Tuple_impl.94" }
%"struct.std::_Tuple_impl.94" = type { %"struct.std::_Head_base.99" }
%"struct.std::_Head_base.99" = type { %"class.llvm::GVMaterializer"* }
%"class.llvm::GVMaterializer" = type opaque
%"class.llvm::StringMap.100" = type <{ %"class.llvm::StringMapImpl", %"class.llvm::MallocAllocator", [7 x i8] }>
%"class.llvm::DataLayout" = type { i8, i32, %"struct.llvm::MaybeAlign", i32, i32, %"struct.llvm::MaybeAlign", i32, i32, %"class.llvm::SmallVector", %"class.llvm::SmallVector.103", %"class.std::__cxx11::basic_string", %"class.llvm::SmallVector.109", i8*, %"class.llvm::SmallVector.114" }
%"struct.llvm::MaybeAlign" = type { %"class.llvm::Optional" }
%"class.llvm::Optional" = type { %"class.llvm::optional_detail::OptionalStorage" }
%"class.llvm::optional_detail::OptionalStorage" = type { %union.anon.102, i8 }
%union.anon.102 = type { i8 }
%"class.llvm::SmallVector" = type { %"class.llvm::SmallVectorImpl", %"struct.llvm::SmallVectorStorage" }
%"class.llvm::SmallVectorImpl" = type { %"class.llvm::SmallVectorTemplateBase" }
%"class.llvm::SmallVectorTemplateBase" = type { %"class.llvm::SmallVectorTemplateCommon" }
%"class.llvm::SmallVectorTemplateCommon" = type { %"class.llvm::SmallVectorBase" }
%"class.llvm::SmallVectorBase" = type { i8*, i64, i64 }
%"struct.llvm::SmallVectorStorage" = type { [8 x i8] }
%"class.llvm::SmallVector.103" = type { %"class.llvm::SmallVectorImpl.104", %"struct.llvm::SmallVectorStorage.108" }
%"class.llvm::SmallVectorImpl.104" = type { %"class.llvm::SmallVectorTemplateBase.105" }
%"class.llvm::SmallVectorTemplateBase.105" = type { %"class.llvm::SmallVectorTemplateCommon.106" }
%"class.llvm::SmallVectorTemplateCommon.106" = type { %"class.llvm::SmallVectorBase.107" }
%"struct.llvm::SmallVectorStorage.108" = type { [128 x i8] }
%"class.llvm::SmallVector.109" = type { %"class.llvm::SmallVectorImpl.110", %"struct.llvm::SmallVectorStorage.113" }
%"class.llvm::SmallVectorImpl.110" = type { %"class.llvm::SmallVectorTemplateBase.111" }
%"class.llvm::SmallVectorTemplateBase.111" = type { %"class.llvm::SmallVectorTemplateCommon.112" }
%"class.llvm::SmallVectorTemplateCommon.112" = type { %"class.llvm::SmallVectorBase.107" }
%"struct.llvm::SmallVectorStorage.113" = type { [128 x i8] }
%"class.llvm::SmallVector.114" = type { %"class.llvm::SmallVectorImpl.115", %"struct.llvm::SmallVectorStorage.118" }
%"class.llvm::SmallVectorImpl.115" = type { %"class.llvm::SmallVectorTemplateBase.116" }
%"class.llvm::SmallVectorTemplateBase.116" = type { %"class.llvm::SmallVectorTemplateCommon.117" }
%"class.llvm::SmallVectorTemplateCommon.117" = type { %"class.llvm::SmallVectorBase.107" }
%"struct.llvm::SmallVectorStorage.118" = type { [32 x i8] }
%"class.llvm::Comdat" = type <{ %"class.llvm::StringMapEntry.410"*, i32, [4 x i8] }>
%"class.llvm::StringMapEntry.410" = type opaque
%"class.llvm::ilist_node.411" = type { %"class.llvm::ilist_node_impl.50" }
%"class.llvm::SymbolTableList.412" = type { %"class.llvm::iplist_impl.413" }
%"class.llvm::iplist_impl.413" = type { %"class.llvm::simple_ilist.416" }
%"class.llvm::simple_ilist.416" = type { %"class.llvm::ilist_sentinel.418" }
%"class.llvm::ilist_sentinel.418" = type { %"class.llvm::ilist_node_impl.401" }
%"class.llvm::Argument" = type <{ %"class.llvm::Value", %"class.llvm::Function"*, i32, [4 x i8] }>
%"class.std::unique_ptr.73" = type { %"class.std::__uniq_ptr_impl.74" }
%"class.std::__uniq_ptr_impl.74" = type { %"class.std::tuple.75" }
%"class.std::tuple.75" = type { %"struct.std::_Tuple_impl.76" }
%"struct.std::_Tuple_impl.76" = type { %"struct.std::_Head_base.81" }
%"struct.std::_Head_base.81" = type { %"class.llvm::ValueSymbolTable"* }
%"class.llvm::ValueSymbolTable" = type opaque
%"class.llvm::AttributeList" = type { %"class.llvm::AttributeListImpl"* }
%"class.llvm::AttributeListImpl" = type opaque
%"class.llvm::ilist_iterator" = type { %"class.llvm::ilist_node_impl.409"* }
%"class.llvm::IRBuilderFolder" = type { i32 (...)** }
%"class.llvm::MDNode" = type { %"class.llvm::Metadata", i32, i32, %"class.llvm::ContextAndReplaceableUses" }
%"class.llvm::Metadata" = type { i8, i8, i16, i32 }
%"class.llvm::ContextAndReplaceableUses" = type { %"class.llvm::PointerUnion" }
%"class.llvm::PointerUnion" = type { %"class.llvm::pointer_union_detail::PointerUnionMembers" }
%"class.llvm::pointer_union_detail::PointerUnionMembers" = type { %"class.llvm::pointer_union_detail::PointerUnionMembers.419" }
%"class.llvm::pointer_union_detail::PointerUnionMembers.419" = type { %"class.llvm::pointer_union_detail::PointerUnionMembers.420" }
%"class.llvm::pointer_union_detail::PointerUnionMembers.420" = type { %"class.llvm::PointerIntPair.421" }
%"class.llvm::PointerIntPair.421" = type { i64 }
%"class.llvm::FastMathFlags" = type { i32 }
%"class.llvm::ArrayRef" = type { %"class.llvm::OperandBundleDefT"*, i64 }
%"class.llvm::OperandBundleDefT" = type { %"class.std::__cxx11::basic_string", %"class.std::vector.422" }
%"class.std::vector.422" = type { %"struct.std::_Vector_base.423" }
%"struct.std::_Vector_base.423" = type { %"struct.std::_Vector_base<llvm::Value *, std::allocator<llvm::Value *>>::_Vector_impl" }
%"struct.std::_Vector_base<llvm::Value *, std::allocator<llvm::Value *>>::_Vector_impl" = type { %"class.llvm::Value"**, %"class.llvm::Value"**, %"class.llvm::Value"** }
%"class.llvm::ConstantFolder" = type { %"class.llvm::IRBuilderFolder" }
%"class.llvm::IRBuilderDefaultInserter" = type { i32 (...)** }
%"class.std::unique_ptr.34" = type { %"class.std::__uniq_ptr_impl.35" }
%"class.std::__uniq_ptr_impl.35" = type { %"class.std::tuple.36" }
%"class.std::tuple.36" = type { %"struct.std::_Tuple_impl.37" }
%"struct.std::_Tuple_impl.37" = type { %"struct.std::_Head_base.42" }
%"struct.std::_Head_base.42" = type { %"class.llvm::legacy::FunctionPassManager"* }
%"class.llvm::legacy::FunctionPassManager" = type { %"class.llvm::legacy::PassManagerBase", %"class.llvm::legacy::FunctionPassManagerImpl"*, %"class.llvm::Module"* }
%"class.llvm::legacy::PassManagerBase" = type { i32 (...)** }
%"class.llvm::legacy::FunctionPassManagerImpl" = type opaque
%"class.std::unique_ptr.119" = type { %"class.std::__uniq_ptr_impl.120" }
%"class.std::__uniq_ptr_impl.120" = type { %"class.std::tuple.121" }
%"class.std::tuple.121" = type { %"struct.std::_Tuple_impl.122" }
%"struct.std::_Tuple_impl.122" = type { %"struct.std::_Head_base.127" }
%"struct.std::_Head_base.127" = type { %"class.llvm::Module"* }
%"class.codegen::HammerConfig" = type { i8*, i8*, [100 x %"class.llvm::Pass"* ()*], [100 x %"class.llvm::Pass"* ()*] }
%"class.llvm::Pass" = type <{ i32 (...)**, %"class.llvm::AnalysisResolver"*, i8*, i32, [4 x i8] }>
%"class.llvm::AnalysisResolver" = type { %"class.std::vector", %"class.llvm::PMDataManager"* }
%"class.std::vector" = type { %"struct.std::_Vector_base" }
%"struct.std::_Vector_base" = type { %"struct.std::_Vector_base<std::pair<const void *, llvm::Pass *>, std::allocator<std::pair<const void *, llvm::Pass *>>>::_Vector_impl" }
%"struct.std::_Vector_base<std::pair<const void *, llvm::Pass *>, std::allocator<std::pair<const void *, llvm::Pass *>>>::_Vector_impl" = type { %"struct.std::pair"*, %"struct.std::pair"*, %"struct.std::pair"* }
%"struct.std::pair" = type { i8*, %"class.llvm::Pass"* }
%"class.llvm::PMDataManager" = type opaque
%"class.std::unique_ptr.131" = type { %"class.std::__uniq_ptr_impl.132" }
%"class.std::__uniq_ptr_impl.132" = type { %"class.std::tuple.133" }
%"class.std::tuple.133" = type { %"struct.std::_Tuple_impl.134" }
%"struct.std::_Tuple_impl.134" = type { %"struct.std::_Head_base.139" }
%"struct.std::_Head_base.139" = type { %"class.llvm::orc::LLJIT"* }
%"class.llvm::orc::LLJIT" = type { %"class.std::unique_ptr.140", %"class.std::unique_ptr.245", %"class.llvm::orc::JITDylib"*, %"class.llvm::DataLayout", %"class.llvm::Triple", %"class.std::unique_ptr.254", %"class.std::unique_ptr.274", %"class.std::unique_ptr.283", %"class.std::unique_ptr.292", %"class.std::unique_ptr.316", %"class.std::unique_ptr.316" }
%"class.std::unique_ptr.140" = type { %"class.std::__uniq_ptr_impl.141" }
%"class.std::__uniq_ptr_impl.141" = type { %"class.std::tuple.142" }
%"class.std::tuple.142" = type { %"struct.std::_Tuple_impl.143" }
%"struct.std::_Tuple_impl.143" = type { %"struct.std::_Head_base.148" }
%"struct.std::_Head_base.148" = type { %"class.llvm::orc::ExecutionSession"* }
%"class.llvm::orc::ExecutionSession" = type { %"class.std::recursive_mutex", i8, %"class.std::shared_ptr", %"class.std::unique_ptr.151", %"class.std::function.160", %"class.std::function.163", %"class.std::vector.229", %"class.std::vector.234", %"class.std::recursive_mutex", %"class.std::vector.239" }
%"class.std::shared_ptr" = type { %"class.std::__shared_ptr" }
%"class.std::__shared_ptr" = type { %"class.llvm::orc::SymbolStringPool"*, %"class.std::__shared_count" }
%"class.llvm::orc::SymbolStringPool" = type { %"class.std::mutex", %"class.llvm::StringMap.149" }
%"class.llvm::StringMap.149" = type <{ %"class.llvm::StringMapImpl", %"class.llvm::MallocAllocator", [7 x i8] }>
%"class.std::__shared_count" = type { %"class.std::_Sp_counted_base"* }
%"class.std::_Sp_counted_base" = type { i32 (...)**, i32, i32 }
%"class.std::unique_ptr.151" = type { %"class.std::__uniq_ptr_impl.152" }
%"class.std::__uniq_ptr_impl.152" = type { %"class.std::tuple.153" }
%"class.std::tuple.153" = type { %"struct.std::_Tuple_impl.154" }
%"struct.std::_Tuple_impl.154" = type { %"struct.std::_Head_base.159" }
%"struct.std::_Head_base.159" = type { %"class.llvm::orc::Platform"* }
%"class.llvm::orc::Platform" = type { i32 (...)** }
%"class.std::function.160" = type { %"class.std::_Function_base", void (%"union.std::_Any_data"*, %"class.llvm::Error"*)* }
%"class.std::function.163" = type { %"class.std::_Function_base", void (%"union.std::_Any_data"*, %"class.std::unique_ptr.166"*, %"class.std::unique_ptr.178"*)* }
%"class.std::unique_ptr.166" = type { %"class.std::__uniq_ptr_impl.167" }
%"class.std::__uniq_ptr_impl.167" = type { %"class.std::tuple.168" }
%"class.std::tuple.168" = type { %"struct.std::_Tuple_impl.169" }
%"struct.std::_Tuple_impl.169" = type { %"struct.std::_Head_base.174" }
%"struct.std::_Head_base.174" = type { %"class.llvm::orc::MaterializationUnit"* }
%"class.llvm::orc::MaterializationUnit" = type { i32 (...)**, %"class.llvm::DenseMap", %"class.llvm::orc::SymbolStringPtr" }
%"class.llvm::DenseMap" = type <{ %"struct.llvm::detail::DenseMapPair"*, i32, i32, i32, [4 x i8] }>
%"struct.llvm::detail::DenseMapPair" = type { %"struct.std::pair.base", [6 x i8] }
%"struct.std::pair.base" = type <{ %"class.llvm::orc::SymbolStringPtr", %"class.llvm::JITSymbolFlags" }>
%"class.llvm::JITSymbolFlags" = type { i8, i8 }
%"class.llvm::orc::SymbolStringPtr" = type { %"class.llvm::StringMapEntry"* }
%"class.llvm::StringMapEntry" = type { %"class.llvm::StringMapEntryStorage" }
%"class.llvm::StringMapEntryStorage" = type { %"class.llvm::StringMapEntryBase", %"struct.std::atomic" }
%"struct.std::atomic" = type { %"struct.std::__atomic_base" }
%"struct.std::__atomic_base" = type { i64 }
%"class.std::unique_ptr.178" = type { %"class.std::__uniq_ptr_impl.179" }
%"class.std::__uniq_ptr_impl.179" = type { %"class.std::tuple.180" }
%"class.std::tuple.180" = type { %"struct.std::_Tuple_impl.181" }
%"struct.std::_Tuple_impl.181" = type { %"struct.std::_Head_base.186" }
%"struct.std::_Head_base.186" = type { %"class.llvm::orc::MaterializationResponsibility"* }
%"class.llvm::orc::MaterializationResponsibility" = type { %"class.llvm::IntrusiveRefCntPtr", %"class.llvm::DenseMap", %"class.llvm::orc::SymbolStringPtr" }
%"class.llvm::IntrusiveRefCntPtr" = type { %"class.llvm::orc::JITDylib"* }
%"class.std::vector.229" = type { %"struct.std::_Vector_base.230" }
%"struct.std::_Vector_base.230" = type { %"struct.std::_Vector_base<llvm::orc::ResourceManager *, std::allocator<llvm::orc::ResourceManager *>>::_Vector_impl" }
%"struct.std::_Vector_base<llvm::orc::ResourceManager *, std::allocator<llvm::orc::ResourceManager *>>::_Vector_impl" = type { %"class.llvm::orc::ResourceManager"**, %"class.llvm::orc::ResourceManager"**, %"class.llvm::orc::ResourceManager"** }
%"class.llvm::orc::ResourceManager" = type { i32 (...)** }
%"class.std::vector.234" = type { %"struct.std::_Vector_base.235" }
%"struct.std::_Vector_base.235" = type { %"struct.std::_Vector_base<llvm::IntrusiveRefCntPtr<llvm::orc::JITDylib>, std::allocator<llvm::IntrusiveRefCntPtr<llvm::orc::JITDylib>>>::_Vector_impl" }
%"struct.std::_Vector_base<llvm::IntrusiveRefCntPtr<llvm::orc::JITDylib>, std::allocator<llvm::IntrusiveRefCntPtr<llvm::orc::JITDylib>>>::_Vector_impl" = type { %"class.llvm::IntrusiveRefCntPtr"*, %"class.llvm::IntrusiveRefCntPtr"*, %"class.llvm::IntrusiveRefCntPtr"* }
%"class.std::recursive_mutex" = type { %"class.std::__recursive_mutex_base" }
%"class.std::__recursive_mutex_base" = type { %union.pthread_mutex_t }
%"class.std::vector.239" = type { %"struct.std::_Vector_base.240" }
%"struct.std::_Vector_base.240" = type { %"struct.std::_Vector_base<std::pair<std::unique_ptr<llvm::orc::MaterializationUnit>, std::unique_ptr<llvm::orc::MaterializationResponsibility>>, std::allocator<std::pair<std::unique_ptr<llvm::orc::MaterializationUnit>, std::unique_ptr<llvm::orc::MaterializationResponsibility>>>>::_Vector_impl" }
%"struct.std::_Vector_base<std::pair<std::unique_ptr<llvm::orc::MaterializationUnit>, std::unique_ptr<llvm::orc::MaterializationResponsibility>>, std::allocator<std::pair<std::unique_ptr<llvm::orc::MaterializationUnit>, std::unique_ptr<llvm::orc::MaterializationResponsibility>>>>::_Vector_impl" = type { %"struct.std::pair.244"*, %"struct.std::pair.244"*, %"struct.std::pair.244"* }
%"struct.std::pair.244" = type opaque
%"class.std::unique_ptr.245" = type { %"class.std::__uniq_ptr_impl.246" }
%"class.std::__uniq_ptr_impl.246" = type { %"class.std::tuple.247" }
%"class.std::tuple.247" = type { %"struct.std::_Tuple_impl.248" }
%"struct.std::_Tuple_impl.248" = type { %"struct.std::_Head_base.253" }
%"struct.std::_Head_base.253" = type { %"class.llvm::orc::LLJIT::PlatformSupport"* }
%"class.llvm::orc::LLJIT::PlatformSupport" = type { i32 (...)** }
%"class.llvm::orc::JITDylib" = type { %"class.llvm::ThreadSafeRefCountedBase", %"class.llvm::orc::ExecutionSession"*, %"class.std::__cxx11::basic_string", %"class.std::mutex", i8, [7 x i8], %"class.llvm::DenseMap.189", %"class.llvm::DenseMap.193", %"class.llvm::DenseMap.197", %"class.std::vector.201", %"class.std::vector.207", %"class.llvm::IntrusiveRefCntPtr.216", %"class.llvm::DenseMap.218", %"class.llvm::DenseMap.222" }
%"class.llvm::ThreadSafeRefCountedBase" = type { %"struct.std::atomic.187" }
%"struct.std::atomic.187" = type { %"struct.std::__atomic_base.188" }
%"struct.std::__atomic_base.188" = type { i32 }
%"class.llvm::DenseMap.189" = type <{ %"struct.llvm::detail::DenseMapPair.191"*, i32, i32, i32, [4 x i8] }>
%"struct.llvm::detail::DenseMapPair.191" = type { %"struct.std::pair.343" }
%"struct.std::pair.343" = type { %"class.llvm::orc::SymbolStringPtr", %"class.llvm::orc::JITDylib::SymbolTableEntry" }
%"class.llvm::orc::JITDylib::SymbolTableEntry" = type <{ i64, %"class.llvm::JITSymbolFlags", i8, [5 x i8] }>
%"class.llvm::DenseMap.193" = type <{ %"struct.llvm::detail::DenseMapPair.195"*, i32, i32, i32, [4 x i8] }>
%"struct.llvm::detail::DenseMapPair.195" = type { %"struct.std::pair.346" }
%"struct.std::pair.346" = type { %"class.llvm::orc::SymbolStringPtr", %"class.std::shared_ptr.349" }
%"class.std::shared_ptr.349" = type { %"class.std::__shared_ptr.350" }
%"class.std::__shared_ptr.350" = type { %"struct.llvm::orc::JITDylib::UnmaterializedInfo"*, %"class.std::__shared_count" }
%"struct.llvm::orc::JITDylib::UnmaterializedInfo" = type { %"class.std::unique_ptr.166", %"class.llvm::orc::ResourceTracker"* }
%"class.llvm::orc::ResourceTracker" = type { %"class.llvm::ThreadSafeRefCountedBase.217", %"struct.std::atomic" }
%"class.llvm::ThreadSafeRefCountedBase.217" = type { %"struct.std::atomic.187" }
%"class.llvm::DenseMap.197" = type <{ %"struct.llvm::detail::DenseMapPair.199"*, i32, i32, i32, [4 x i8] }>
%"struct.llvm::detail::DenseMapPair.199" = type { %"struct.std::pair.352" }
%"struct.std::pair.352" = type { %"class.llvm::orc::SymbolStringPtr", %"struct.llvm::orc::JITDylib::MaterializingInfo" }
%"struct.llvm::orc::JITDylib::MaterializingInfo" = type { %"class.llvm::DenseMap.355", %"class.llvm::DenseMap.355", %"class.std::vector.365" }
%"class.llvm::DenseMap.355" = type <{ %"struct.llvm::detail::DenseMapPair.357"*, i32, i32, i32, [4 x i8] }>
%"struct.llvm::detail::DenseMapPair.357" = type { %"struct.std::pair.358" }
%"struct.std::pair.358" = type { %"class.llvm::orc::JITDylib"*, %"class.llvm::DenseSet" }
%"class.llvm::DenseSet" = type { %"class.llvm::detail::DenseSetImpl" }
%"class.llvm::detail::DenseSetImpl" = type { %"class.llvm::DenseMap.361" }
%"class.llvm::DenseMap.361" = type <{ %"class.llvm::detail::DenseSetPair"*, i32, i32, i32, [4 x i8] }>
%"class.llvm::detail::DenseSetPair" = type { %"class.llvm::orc::SymbolStringPtr" }
%"class.std::vector.365" = type { %"struct.std::_Vector_base.366" }
%"struct.std::_Vector_base.366" = type { %"struct.std::_Vector_base<std::shared_ptr<llvm::orc::AsynchronousSymbolQuery>, std::allocator<std::shared_ptr<llvm::orc::AsynchronousSymbolQuery>>>::_Vector_impl" }
%"struct.std::_Vector_base<std::shared_ptr<llvm::orc::AsynchronousSymbolQuery>, std::allocator<std::shared_ptr<llvm::orc::AsynchronousSymbolQuery>>>::_Vector_impl" = type { %"class.std::shared_ptr.370"*, %"class.std::shared_ptr.370"*, %"class.std::shared_ptr.370"* }
%"class.std::shared_ptr.370" = type { %"class.std::__shared_ptr.371" }
%"class.std::__shared_ptr.371" = type { %"class.llvm::orc::AsynchronousSymbolQuery"*, %"class.std::__shared_count" }
%"class.llvm::orc::AsynchronousSymbolQuery" = type <{ %"class.llvm::unique_function.373", %"class.llvm::DenseMap.355", %"class.llvm::DenseMap.376", i64, i8, [7 x i8] }>
%"class.llvm::unique_function.373" = type { %"class.llvm::detail::UniqueFunctionBase.374" }
%"class.llvm::detail::UniqueFunctionBase.374" = type { %"union.llvm::detail::UniqueFunctionBase<void, llvm::Expected<llvm::DenseMap<llvm::orc::SymbolStringPtr, llvm::JITEvaluatedSymbol>>>::StorageUnionT", %"class.llvm::PointerIntPair.375" }
%"union.llvm::detail::UniqueFunctionBase<void, llvm::Expected<llvm::DenseMap<llvm::orc::SymbolStringPtr, llvm::JITEvaluatedSymbol>>>::StorageUnionT" = type { %"struct.llvm::detail::UniqueFunctionBase<void, llvm::Expected<llvm::DenseMap<llvm::orc::SymbolStringPtr, llvm::JITEvaluatedSymbol>>>::StorageUnionT::OutOfLineStorageT" }
%"struct.llvm::detail::UniqueFunctionBase<void, llvm::Expected<llvm::DenseMap<llvm::orc::SymbolStringPtr, llvm::JITEvaluatedSymbol>>>::StorageUnionT::OutOfLineStorageT" = type { i8*, i64, i64 }
%"class.llvm::PointerIntPair.375" = type { i64 }
%"class.llvm::DenseMap.376" = type <{ %"struct.llvm::detail::DenseMapPair.378"*, i32, i32, i32, [4 x i8] }>
%"struct.llvm::detail::DenseMapPair.378" = type { %"struct.std::pair.379" }
%"struct.std::pair.379" = type { %"class.llvm::orc::SymbolStringPtr", %"class.llvm::JITEvaluatedSymbol" }
%"class.llvm::JITEvaluatedSymbol" = type <{ i64, %"class.llvm::JITSymbolFlags", [6 x i8] }>
%"class.std::vector.201" = type { %"struct.std::_Vector_base.202" }
%"struct.std::_Vector_base.202" = type { %"struct.std::_Vector_base<std::shared_ptr<llvm::orc::DefinitionGenerator>, std::allocator<std::shared_ptr<llvm::orc::DefinitionGenerator>>>::_Vector_impl" }
%"struct.std::_Vector_base<std::shared_ptr<llvm::orc::DefinitionGenerator>, std::allocator<std::shared_ptr<llvm::orc::DefinitionGenerator>>>::_Vector_impl" = type { %"class.std::shared_ptr.206"*, %"class.std::shared_ptr.206"*, %"class.std::shared_ptr.206"* }
%"class.std::shared_ptr.206" = type { %"class.std::__shared_ptr.383" }
%"class.std::__shared_ptr.383" = type { %"class.llvm::orc::DefinitionGenerator"*, %"class.std::__shared_count" }
%"class.llvm::orc::DefinitionGenerator" = type { i32 (...)** }
%"class.std::vector.207" = type { %"struct.std::_Vector_base.208" }
%"struct.std::_Vector_base.208" = type { %"struct.std::_Vector_base<std::pair<llvm::orc::JITDylib *, llvm::orc::JITDylibLookupFlags>, std::allocator<std::pair<llvm::orc::JITDylib *, llvm::orc::JITDylibLookupFlags>>>::_Vector_impl" }
%"struct.std::_Vector_base<std::pair<llvm::orc::JITDylib *, llvm::orc::JITDylibLookupFlags>, std::allocator<std::pair<llvm::orc::JITDylib *, llvm::orc::JITDylibLookupFlags>>>::_Vector_impl" = type { %"struct.std::pair.212"*, %"struct.std::pair.212"*, %"struct.std::pair.212"* }
%"struct.std::pair.212" = type <{ %"class.llvm::orc::JITDylib"*, i32, [4 x i8] }>
%"class.llvm::IntrusiveRefCntPtr.216" = type { %"class.llvm::orc::ResourceTracker"* }
%"class.llvm::DenseMap.218" = type <{ %"struct.llvm::detail::DenseMapPair.220"*, i32, i32, i32, [4 x i8] }>
%"struct.llvm::detail::DenseMapPair.220" = type { %"struct.std::pair.385" }
%"struct.std::pair.385" = type { %"class.llvm::orc::ResourceTracker"*, %"class.std::vector.388" }
%"class.std::vector.388" = type { %"struct.std::_Vector_base.389" }
%"struct.std::_Vector_base.389" = type { %"struct.std::_Vector_base<llvm::orc::SymbolStringPtr, std::allocator<llvm::orc::SymbolStringPtr>>::_Vector_impl" }
%"struct.std::_Vector_base<llvm::orc::SymbolStringPtr, std::allocator<llvm::orc::SymbolStringPtr>>::_Vector_impl" = type { %"class.llvm::orc::SymbolStringPtr"*, %"class.llvm::orc::SymbolStringPtr"*, %"class.llvm::orc::SymbolStringPtr"* }
%"class.llvm::DenseMap.222" = type <{ %"struct.llvm::detail::DenseMapPair.224"*, i32, i32, i32, [4 x i8] }>
%"struct.llvm::detail::DenseMapPair.224" = type { %"struct.std::pair.225" }
%"struct.std::pair.225" = type { %"class.llvm::orc::MaterializationResponsibility"*, %"class.llvm::orc::ResourceTracker"* }
%"class.llvm::Triple" = type { %"class.std::__cxx11::basic_string", i32, i32, i32, i32, i32, i32 }
%"class.std::unique_ptr.254" = type { %"class.std::__uniq_ptr_impl.255" }
%"class.std::__uniq_ptr_impl.255" = type { %"class.std::tuple.256" }
%"class.std::tuple.256" = type { %"struct.std::_Tuple_impl.257" }
%"struct.std::_Tuple_impl.257" = type { %"struct.std::_Head_base.262" }
%"struct.std::_Head_base.262" = type { %"class.llvm::ThreadPool"* }
%"class.llvm::ThreadPool" = type <{ %"class.std::vector.263", %"class.std::queue", %"class.std::mutex", %"class.std::condition_variable", %"class.std::condition_variable", i32, i8, [3 x i8], i32, [4 x i8] }>
%"class.std::vector.263" = type { %"struct.std::_Vector_base.264" }
%"struct.std::_Vector_base.264" = type { %"struct.std::_Vector_base<std::thread, std::allocator<std::thread>>::_Vector_impl" }
%"struct.std::_Vector_base<std::thread, std::allocator<std::thread>>::_Vector_impl" = type { %"class.std::thread"*, %"class.std::thread"*, %"class.std::thread"* }
%"class.std::thread" = type { %"class.std::thread::id" }
%"class.std::thread::id" = type { i64 }
%"class.std::queue" = type { %"class.std::deque" }
%"class.std::deque" = type { %"class.std::_Deque_base" }
%"class.std::_Deque_base" = type { %"struct.std::_Deque_base<std::packaged_task<void ()>, std::allocator<std::packaged_task<void ()>>>::_Deque_impl" }
%"struct.std::_Deque_base<std::packaged_task<void ()>, std::allocator<std::packaged_task<void ()>>>::_Deque_impl" = type { %"class.std::packaged_task"**, i64, %"struct.std::_Deque_iterator", %"struct.std::_Deque_iterator" }
%"class.std::packaged_task" = type { %"class.std::shared_ptr.393" }
%"class.std::shared_ptr.393" = type { %"class.std::__shared_ptr.394" }
%"class.std::__shared_ptr.394" = type { %"class.std::__future_base::_Task_state_base"*, %"class.std::__shared_count" }
%"class.std::__future_base::_Task_state_base" = type opaque
%"struct.std::_Deque_iterator" = type { %"class.std::packaged_task"*, %"class.std::packaged_task"*, %"class.std::packaged_task"*, %"class.std::packaged_task"** }
%"class.std::condition_variable" = type { %union.pthread_cond_t }
%union.pthread_cond_t = type { %struct.__pthread_cond_s }
%struct.__pthread_cond_s = type { %union.anon.271, %union.anon.272, [2 x i32], [2 x i32], i32, i32, [2 x i32] }
%union.anon.271 = type { i64 }
%union.anon.272 = type { i64 }
%"class.std::unique_ptr.274" = type { %"class.std::__uniq_ptr_impl.275" }
%"class.std::__uniq_ptr_impl.275" = type { %"class.std::tuple.276" }
%"class.std::tuple.276" = type { %"struct.std::_Tuple_impl.277" }
%"struct.std::_Tuple_impl.277" = type { %"struct.std::_Head_base.282" }
%"struct.std::_Head_base.282" = type { %"class.llvm::orc::ObjectLayer"* }
%"class.llvm::orc::ObjectLayer" = type { i32 (...)**, %"class.llvm::orc::ExecutionSession"* }
%"class.std::unique_ptr.283" = type { %"class.std::__uniq_ptr_impl.284" }
%"class.std::__uniq_ptr_impl.284" = type { %"class.std::tuple.285" }
%"class.std::tuple.285" = type { %"struct.std::_Tuple_impl.286" }
%"struct.std::_Tuple_impl.286" = type { %"struct.std::_Head_base.291" }
%"struct.std::_Head_base.291" = type { %"class.llvm::orc::ObjectTransformLayer"* }
%"class.llvm::orc::ObjectTransformLayer" = type opaque
%"class.std::unique_ptr.292" = type { %"class.std::__uniq_ptr_impl.293" }
%"class.std::__uniq_ptr_impl.293" = type { %"class.std::tuple.294" }
%"class.std::tuple.294" = type { %"struct.std::_Tuple_impl.295" }
%"struct.std::_Tuple_impl.295" = type { %"struct.std::_Head_base.300" }
%"struct.std::_Head_base.300" = type { %"class.llvm::orc::IRCompileLayer"* }
%"class.llvm::orc::IRCompileLayer" = type { %"class.llvm::orc::IRLayer", %"class.std::mutex", %"class.llvm::orc::ObjectLayer"*, %"class.std::unique_ptr.301", %"struct.llvm::orc::IRSymbolMapper::ManglingOptions"*, %"class.std::function.310" }
%"class.llvm::orc::IRLayer" = type { i32 (...)**, i8, %"class.llvm::orc::ExecutionSession"*, %"struct.llvm::orc::IRSymbolMapper::ManglingOptions"** }
%"class.std::unique_ptr.301" = type { %"class.std::__uniq_ptr_impl.302" }
%"class.std::__uniq_ptr_impl.302" = type { %"class.std::tuple.303" }
%"class.std::tuple.303" = type { %"struct.std::_Tuple_impl.304" }
%"struct.std::_Tuple_impl.304" = type { %"struct.std::_Head_base.309" }
%"struct.std::_Head_base.309" = type { %"class.llvm::orc::IRCompileLayer::IRCompiler"* }
%"class.llvm::orc::IRCompileLayer::IRCompiler" = type <{ i32 (...)**, %"struct.llvm::orc::IRSymbolMapper::ManglingOptions", [7 x i8] }>
%"struct.llvm::orc::IRSymbolMapper::ManglingOptions" = type { i8 }
%"class.std::function.310" = type { %"class.std::_Function_base", void (%"union.std::_Any_data"*, %"class.llvm::orc::MaterializationResponsibility"*, %"class.llvm::orc::ThreadSafeModule"*)* }
%"class.llvm::orc::ThreadSafeModule" = type { %"class.std::unique_ptr.119", %"class.llvm::orc::ThreadSafeContext" }
%"class.llvm::orc::ThreadSafeContext" = type { %"class.std::shared_ptr.313" }
%"class.std::shared_ptr.313" = type { %"class.std::__shared_ptr.314" }
%"class.std::__shared_ptr.314" = type { %"struct.llvm::orc::ThreadSafeContext::State"*, %"class.std::__shared_count" }
%"struct.llvm::orc::ThreadSafeContext::State" = type { %"class.std::unique_ptr", %"class.std::recursive_mutex" }
%"class.std::unique_ptr.316" = type { %"class.std::__uniq_ptr_impl.317" }
%"class.std::__uniq_ptr_impl.317" = type { %"class.std::tuple.318" }
%"class.std::tuple.318" = type { %"struct.std::_Tuple_impl.319" }
%"struct.std::_Tuple_impl.319" = type { %"struct.std::_Head_base.324" }
%"struct.std::_Head_base.324" = type { %"class.llvm::orc::IRTransformLayer"* }
%"class.llvm::orc::IRTransformLayer" = type { %"class.llvm::orc::IRLayer", %"class.llvm::orc::IRLayer"*, %"class.llvm::unique_function" }
%"class.llvm::unique_function" = type { %"class.llvm::detail::UniqueFunctionBase" }
%"class.llvm::detail::UniqueFunctionBase" = type { %"union.llvm::detail::UniqueFunctionBase<llvm::Expected<llvm::orc::ThreadSafeModule>, llvm::orc::ThreadSafeModule, llvm::orc::MaterializationResponsibility &>::StorageUnionT", %"class.llvm::PointerIntPair" }
%"union.llvm::detail::UniqueFunctionBase<llvm::Expected<llvm::orc::ThreadSafeModule>, llvm::orc::ThreadSafeModule, llvm::orc::MaterializationResponsibility &>::StorageUnionT" = type { %"struct.llvm::detail::UniqueFunctionBase<llvm::Expected<llvm::orc::ThreadSafeModule>, llvm::orc::ThreadSafeModule, llvm::orc::MaterializationResponsibility &>::StorageUnionT::OutOfLineStorageT" }
%"struct.llvm::detail::UniqueFunctionBase<llvm::Expected<llvm::orc::ThreadSafeModule>, llvm::orc::ThreadSafeModule, llvm::orc::MaterializationResponsibility &>::StorageUnionT::OutOfLineStorageT" = type { i8*, i64, i64 }
%"class.llvm::PointerIntPair" = type { i64 }
%"class.llvm::Expected" = type { %union.anon.326, i8, [7 x i8] }
%union.anon.326 = type { %"struct.llvm::AlignedCharArrayUnion" }
%"struct.llvm::AlignedCharArrayUnion" = type { [16 x i8] }
%"struct.std::_Rb_tree_node" = type { %"struct.std::_Rb_tree_node_base", %"struct.__gnu_cxx::__aligned_membuf.437" }
%"struct.__gnu_cxx::__aligned_membuf.437" = type { [40 x i8] }
%"class.std::tuple.464" = type { %"struct.std::_Tuple_impl.465" }
%"struct.std::_Tuple_impl.465" = type { %"struct.std::_Head_base.466" }
%"struct.std::_Head_base.466" = type { %"class.std::__cxx11::basic_string"* }
%"class.std::tuple.467" = type { i8 }
%class.Sort = type { %class.OpTemplate, i32*, i32, i32*, i32, i32*, i32*, i32*, i32, %class.PagesIndex* }
%class.OpTemplate = type { i32 (...)** }
%class.PagesIndex = type <{ i32*, i32, [4 x i8], %"class.std::vector.328", %"class.std::vector.333", i32, [4 x i8] }>
%"class.std::vector.328" = type { %"struct.std::_Vector_base.329" }
%"struct.std::_Vector_base.329" = type { %"struct.std::_Vector_base<long, std::allocator<long>>::_Vector_impl" }
%"struct.std::_Vector_base<long, std::allocator<long>>::_Vector_impl" = type { i64*, i64*, i64* }
%"class.std::vector.333" = type { %"struct.std::_Vector_base.334" }
%"struct.std::_Vector_base.334" = type { %"struct.std::_Vector_base<std::vector<Column *>, std::allocator<std::vector<Column *>>>::_Vector_impl" }
%"struct.std::_Vector_base<std::vector<Column *>, std::allocator<std::vector<Column *>>>::_Vector_impl" = type { %"class.std::vector.338"*, %"class.std::vector.338"*, %"class.std::vector.338"* }
%"class.std::vector.338" = type { %"struct.std::_Vector_base.339" }
%"struct.std::_Vector_base.339" = type { %"struct.std::_Vector_base<Column *, std::allocator<Column *>>::_Vector_impl" }
%"struct.std::_Vector_base<Column *, std::allocator<Column *>>::_Vector_impl" = type { %class.Column**, %class.Column**, %class.Column** }
%class.Column = type { i32 (...)**, i8*, i32*, i32, i64 }
%class.Table = type <{ i32 (...)**, %class.Layout, [7 x i8], %"class.std::vector.338", i32*, i32, i32, i32, [4 x i8] }>
%class.Layout = type { i8 }
%"struct.std::pair.438" = type { %"class.std::__cxx11::basic_string", %"class.codegen::ParamValue"* }

$_ZN15OpTemplateCacheIP14JitSortContextED2Ev = comdat any

$_ZNSt3mapINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEPN7codegen10ParamValueESt4lessIS5_ESaISt4pairIKS5_S8_EEEixEOS5_ = comdat any

$_ZN7codegen6HammerD2Ev = comdat any

$__clang_call_terminate = comdat any

$_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE7_M_copyINSH_11_Alloc_nodeEEEPSt13_Rb_tree_nodeISB_EPKSL_PSt18_Rb_tree_node_baseRT_ = comdat any

$_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE8_M_eraseEPSt13_Rb_tree_nodeISB_E = comdat any

$_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE17_M_construct_nodeIJRKSB_EEEvPSt13_Rb_tree_nodeISB_EDpOT_ = comdat any

$_ZN4llvm3orc5LLJIT6lookupERNS0_8JITDylibENS_9StringRefE = comdat any

$_ZN4llvm9StringMapISt6atomicImENS_15MallocAllocatorEE11try_emplaceIJiEEESt4pairINS_17StringMapIteratorIS2_EEbENS_9StringRefEDpOT_ = comdat any

$_ZN5TableD2Ev = comdat any

$_ZN5TableD0Ev = comdat any

$_ZN6ColumnD2Ev = comdat any

$_ZN6ColumnD0Ev = comdat any

$_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE22_M_emplace_hint_uniqueIJRKSt21piecewise_construct_tSt5tupleIJOS5_EESM_IJEEEEESt17_Rb_tree_iteratorISB_ESt23_Rb_tree_const_iteratorISB_EDpOT_ = comdat any

$_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE29_M_get_insert_hint_unique_posESt23_Rb_tree_const_iteratorISB_ERS7_ = comdat any

$_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE24_M_get_insert_unique_posERS7_ = comdat any

$_ZNSt10_HashtableImSt4pairIKmP14JitSortContextESaIS4_ENSt8__detail10_Select1stESt8equal_toImESt4hashImENS6_18_Mod_range_hashingENS6_20_Default_ranged_hashENS6_20_Prime_rehash_policyENS6_17_Hashtable_traitsILb0ELb0ELb1EEEE21_M_insert_unique_nodeEmmPNS6_10_Hash_nodeIS4_Lb0EEE = comdat any

$_ZTV5Table = comdat any

$_ZTS5Table = comdat any

$_ZTI5Table = comdat any

$_ZTV6Column = comdat any

$_ZTS6Column = comdat any

$_ZTI6Column = comdat any

@_ZN4llvm24DisableABIBreakingChecksE = external dso_local global i32, align 4
@_ZN4llvm30VerifyDisableABIBreakingChecksE = weak hidden local_unnamed_addr global i32* @_ZN4llvm24DisableABIBreakingChecksE, align 8
@_ZStL8__ioinit = internal global %"class.std::ios_base::Init" zeroinitializer, align 1
@__dso_handle = external hidden global i8
@g_jitSortContexts = dso_local global %class.OpTemplateCache zeroinitializer, align 8
@.str = private unnamed_addr constant [27 x i8] c"_Z9compareTolPiS_S_S_ijj@1\00", align 1
@.str.2 = private unnamed_addr constant [27 x i8] c"_Z9compareTolPiS_S_S_ijj@2\00", align 1
@.str.3 = private unnamed_addr constant [27 x i8] c"_Z9compareTolPiS_S_S_ijj@3\00", align 1
@.str.4 = private unnamed_addr constant [27 x i8] c"_Z9compareTolPiS_S_S_ijj@4\00", align 1
@.str.5 = private unnamed_addr constant [27 x i8] c"_Z9compareTolPiS_S_S_ijj@5\00", align 1
@.str.6 = private unnamed_addr constant [26 x i8] c"_Z12allocColumnslPiS_ij@1\00", align 1
@.str.7 = private unnamed_addr constant [26 x i8] c"_Z12allocColumnslPiS_ij@2\00", align 1
@.str.8 = private unnamed_addr constant [26 x i8] c"_Z12allocColumnslPiS_ij@3\00", align 1
@.str.9 = private unnamed_addr constant [23 x i8] c"_Z9getResultlPiilS_j@1\00", align 1
@.str.10 = private unnamed_addr constant [23 x i8] c"_Z9getResultlPiilS_j@2\00", align 1
@.str.11 = private unnamed_addr constant [23 x i8] c"_Z9getResultlPiilS_j@4\00", align 1
@.str.12 = private unnamed_addr constant [45 x i8] c"/usr/lib/gcc/x86_64-linux-gnu/7/libstdc++.so\00", align 1
@.str.13 = private unnamed_addr constant [38 x i8] c"/home/joy/cpp/src/operator/ir/sort.ll\00", align 1
@.str.14 = private unnamed_addr constant [45 x i8] c"/home/joy/cpp/src/operator/ir/memory_pool.ll\00", align 1
@.str.15 = private unnamed_addr constant [25 x i8] c"_Z9quickSortlPiS_S_S_ijj\00", align 1
@.str.16 = private unnamed_addr constant [24 x i8] c"_Z12allocColumnslPiS_ij\00", align 1
@.str.17 = private unnamed_addr constant [21 x i8] c"_Z9getResultlPiilS_j\00", align 1
@__const.HammerConfig.ALL = private unnamed_addr constant <{ i8, [31 x i8] }> <{ i8 1, [31 x i8] zeroinitializer }>, align 16
@.str.19 = private unnamed_addr constant [39 x i8] c"NumItems + NumTombstones <= NumBuckets\00", align 1
@.str.20 = private unnamed_addr constant [42 x i8] c"/usr/include/llvm-12/llvm/ADT/StringMap.h\00", align 1
@__PRETTY_FUNCTION__._ZN4llvm9StringMapISt6atomicImENS_15MallocAllocatorEE11try_emplaceIJiEEESt4pairINS_17StringMapIteratorIS2_EEbENS_9StringRefEDpOT_ = private unnamed_addr constant [206 x i8] c"std::pair<iterator, bool> llvm::StringMap<std::atomic<unsigned long>>::try_emplace(llvm::StringRef, ArgsTy &&...) [ValueTy = std::atomic<unsigned long>, AllocatorTy = llvm::MallocAllocator, ArgsTy = <int>]\00", align 1
@_ZTV5Table = linkonce_odr dso_local unnamed_addr constant { [4 x i8*] } { [4 x i8*] [i8* null, i8* bitcast ({ i8*, i8* }* @_ZTI5Table to i8*), i8* bitcast (void (%class.Table*)* @_ZN5TableD2Ev to i8*), i8* bitcast (void (%class.Table*)* @_ZN5TableD0Ev to i8*)] }, comdat, align 8
@_ZTVN10__cxxabiv117__class_type_infoE = external dso_local global i8*
@_ZTS5Table = linkonce_odr dso_local constant [7 x i8] c"5Table\00", comdat, align 1
@_ZTI5Table = linkonce_odr dso_local constant { i8*, i8* } { i8* bitcast (i8** getelementptr inbounds (i8*, i8** @_ZTVN10__cxxabiv117__class_type_infoE, i64 2) to i8*), i8* getelementptr inbounds ([7 x i8], [7 x i8]* @_ZTS5Table, i32 0, i32 0) }, comdat, align 8
@_ZTV6Column = linkonce_odr dso_local unnamed_addr constant { [4 x i8*] } { [4 x i8*] [i8* null, i8* bitcast ({ i8*, i8* }* @_ZTI6Column to i8*), i8* bitcast (void (%class.Column*)* @_ZN6ColumnD2Ev to i8*), i8* bitcast (void (%class.Column*)* @_ZN6ColumnD0Ev to i8*)] }, comdat, align 8
@_ZTS6Column = linkonce_odr dso_local constant [8 x i8] c"6Column\00", comdat, align 1
@_ZTI6Column = linkonce_odr dso_local constant { i8*, i8* } { i8* bitcast (i8** getelementptr inbounds (i8*, i8** @_ZTVN10__cxxabiv117__class_type_infoE, i64 2) to i8*), i8* getelementptr inbounds ([8 x i8], [8 x i8]* @_ZTS6Column, i32 0, i32 0) }, comdat, align 8
@.str.26 = private unnamed_addr constant [54 x i8] c"!HasError && \22Cannot get value when an error exists!\22\00", align 1
@.str.27 = private unnamed_addr constant [42 x i8] c"/usr/include/llvm-12/llvm/Support/Error.h\00", align 1
@__PRETTY_FUNCTION__._ZN4llvm8ExpectedINS_18JITEvaluatedSymbolEE10getStorageEv = private unnamed_addr constant [116 x i8] c"llvm::Expected::storage_type *llvm::Expected<llvm::JITEvaluatedSymbol>::getStorage() [T = llvm::JITEvaluatedSymbol]\00", align 1
@_ZStL19piecewise_construct = internal constant %"struct.std::piecewise_construct_t" undef, align 1
@llvm.global_ctors = appending global [1 x { i32, void ()*, i8* }] [{ i32, void ()*, i8* } { i32 65535, void ()* @_GLOBAL__sub_I_sort_api.cpp, i8* null }]

declare dso_local void @_ZNSt8ios_base4InitC1Ev(%"class.std::ios_base::Init"* nonnull dereferenceable(1)) unnamed_addr #0

; Function Attrs: nounwind
declare dso_local void @_ZNSt8ios_base4InitD1Ev(%"class.std::ios_base::Init"* nonnull dereferenceable(1)) unnamed_addr #1

; Function Attrs: nofree nounwind
declare dso_local i32 @__cxa_atexit(void (i8*)*, i8*, i8*) local_unnamed_addr #2

; Function Attrs: inlinehint nounwind uwtable
define linkonce_odr dso_local void @_ZN15OpTemplateCacheIP14JitSortContextED2Ev(%class.OpTemplateCache* nonnull dereferenceable(96) %this) unnamed_addr #3 comdat align 2 personality i32 (...)* @__gxx_personality_v0 {
entry:
  %opTemplateMap = getelementptr inbounds %class.OpTemplateCache, %class.OpTemplateCache* %this, i64 0, i32 1
  %_M_nxt.i.i.i.i = getelementptr inbounds %class.OpTemplateCache, %class.OpTemplateCache* %this, i64 0, i32 1, i32 0, i32 2, i32 0
  %0 = bitcast %"struct.std::__detail::_Hash_node_base"** %_M_nxt.i.i.i.i to %"struct.std::__detail::_Hash_node"**
  %1 = load %"struct.std::__detail::_Hash_node"*, %"struct.std::__detail::_Hash_node"** %0, align 8, !tbaa !2
  %tobool.not5.i.i.i.i = icmp eq %"struct.std::__detail::_Hash_node"* %1, null
  br i1 %tobool.not5.i.i.i.i, label %_ZNSt10_HashtableImSt4pairIKmP14JitSortContextESaIS4_ENSt8__detail10_Select1stESt8equal_toImESt4hashImENS6_18_Mod_range_hashingENS6_20_Default_ranged_hashENS6_20_Prime_rehash_policyENS6_17_Hashtable_traitsILb0ELb0ELb1EEEE5clearEv.exit.i.i, label %while.body.i.i.i.i

while.body.i.i.i.i:                               ; preds = %entry, %while.body.i.i.i.i
  %__n.addr.06.i.i.i.i = phi %"struct.std::__detail::_Hash_node"* [ %3, %while.body.i.i.i.i ], [ %1, %entry ]
  %2 = bitcast %"struct.std::__detail::_Hash_node"* %__n.addr.06.i.i.i.i to %"struct.std::__detail::_Hash_node"**
  %3 = load %"struct.std::__detail::_Hash_node"*, %"struct.std::__detail::_Hash_node"** %2, align 8, !tbaa !11
  %4 = bitcast %"struct.std::__detail::_Hash_node"* %__n.addr.06.i.i.i.i to i8*
  tail call void @_ZdlPv(i8* nonnull %4) #20
  %tobool.not.i.i.i.i = icmp eq %"struct.std::__detail::_Hash_node"* %3, null
  br i1 %tobool.not.i.i.i.i, label %_ZNSt10_HashtableImSt4pairIKmP14JitSortContextESaIS4_ENSt8__detail10_Select1stESt8equal_toImESt4hashImENS6_18_Mod_range_hashingENS6_20_Default_ranged_hashENS6_20_Prime_rehash_policyENS6_17_Hashtable_traitsILb0ELb0ELb1EEEE5clearEv.exit.i.i, label %while.body.i.i.i.i, !llvm.loop !12

_ZNSt10_HashtableImSt4pairIKmP14JitSortContextESaIS4_ENSt8__detail10_Select1stESt8equal_toImESt4hashImENS6_18_Mod_range_hashingENS6_20_Default_ranged_hashENS6_20_Prime_rehash_policyENS6_17_Hashtable_traitsILb0ELb0ELb1EEEE5clearEv.exit.i.i: ; preds = %while.body.i.i.i.i, %entry
  %5 = bitcast %"class.std::unordered_map"* %opTemplateMap to i8**
  %6 = load i8*, i8** %5, align 8, !tbaa !14
  %_M_bucket_count.i.i.i = getelementptr inbounds %class.OpTemplateCache, %class.OpTemplateCache* %this, i64 0, i32 1, i32 0, i32 1
  %7 = load i64, i64* %_M_bucket_count.i.i.i, align 8, !tbaa !15
  %mul.i.i.i = shl i64 %7, 3
  tail call void @llvm.memset.p0i8.i64(i8* align 8 %6, i8 0, i64 %mul.i.i.i, i1 false) #20
  %8 = bitcast %"struct.std::__detail::_Hash_node_base"** %_M_nxt.i.i.i.i to i8*
  tail call void @llvm.memset.p0i8.i64(i8* nonnull align 8 dereferenceable(16) %8, i8 0, i64 16, i1 false) #20
  %_M_buckets.i.i.i = getelementptr inbounds %"class.std::unordered_map", %"class.std::unordered_map"* %opTemplateMap, i64 0, i32 0, i32 0
  %9 = load %"struct.std::__detail::_Hash_node_base"**, %"struct.std::__detail::_Hash_node_base"*** %_M_buckets.i.i.i, align 8, !tbaa !14
  %_M_single_bucket.i.i.i.i.i = getelementptr inbounds %class.OpTemplateCache, %class.OpTemplateCache* %this, i64 0, i32 1, i32 0, i32 5
  %cmp.i.i.i.i.i = icmp eq %"struct.std::__detail::_Hash_node_base"** %_M_single_bucket.i.i.i.i.i, %9
  br i1 %cmp.i.i.i.i.i, label %_ZNSt13unordered_mapImP14JitSortContextSt4hashImESt8equal_toImESaISt4pairIKmS1_EEED2Ev.exit, label %if.end.i.i.i.i

if.end.i.i.i.i:                                   ; preds = %_ZNSt10_HashtableImSt4pairIKmP14JitSortContextESaIS4_ENSt8__detail10_Select1stESt8equal_toImESt4hashImENS6_18_Mod_range_hashingENS6_20_Default_ranged_hashENS6_20_Prime_rehash_policyENS6_17_Hashtable_traitsILb0ELb0ELb1EEEE5clearEv.exit.i.i
  %10 = bitcast %"struct.std::__detail::_Hash_node_base"** %9 to i8*
  tail call void @_ZdlPv(i8* %10) #20
  br label %_ZNSt13unordered_mapImP14JitSortContextSt4hashImESt8equal_toImESaISt4pairIKmS1_EEED2Ev.exit

_ZNSt13unordered_mapImP14JitSortContextSt4hashImESt8equal_toImESaISt4pairIKmS1_EEED2Ev.exit: ; preds = %_ZNSt10_HashtableImSt4pairIKmP14JitSortContextESaIS4_ENSt8__detail10_Select1stESt8equal_toImESt4hashImENS6_18_Mod_range_hashingENS6_20_Default_ranged_hashENS6_20_Prime_rehash_policyENS6_17_Hashtable_traitsILb0ELb0ELb1EEEE5clearEv.exit.i.i, %if.end.i.i.i.i
  ret void
}

; Function Attrs: uwtable
define dso_local void @_Z20createJitSortContextlPiiS_iS_S_S_i(i64 %stageId, i32* %sourceTypes, i32 %typeCount, i32* %outputCols, i32 %outputColCount, i32* %sortCols, i32* %sortAscendings, i32* %sortNullFirsts, i32 %sortColCount) local_unnamed_addr #4 personality i8* bitcast (i32 (...)* @__gxx_personality_v0 to i8*) {
entry:
  %ALL.i = alloca [32 x i8], align 16
  %__an.i.i.i606 = alloca %"struct.std::_Rb_tree<std::__cxx11::basic_string<char>, std::pair<const std::__cxx11::basic_string<char>, codegen::ParamValue *>, std::_Select1st<std::pair<const std::__cxx11::basic_string<char>, codegen::ParamValue *>>, std::less<std::__cxx11::basic_string<char>>>::_Alloc_node", align 8
  %__an.i.i.i = alloca %"struct.std::_Rb_tree<std::__cxx11::basic_string<char>, std::pair<const std::__cxx11::basic_string<char>, codegen::ParamValue *>, std::_Select1st<std::pair<const std::__cxx11::basic_string<char>, codegen::ParamValue *>>, std::less<std::__cxx11::basic_string<char>>>::_Alloc_node", align 8
  %__dnew.i.i.i.i568 = alloca i64, align 8
  %__dnew.i.i.i.i543 = alloca i64, align 8
  %__dnew.i.i.i.i518 = alloca i64, align 8
  %__dnew.i.i.i.i493 = alloca i64, align 8
  %__dnew.i.i.i.i468 = alloca i64, align 8
  %__dnew.i.i.i.i443 = alloca i64, align 8
  %__dnew.i.i.i.i405 = alloca i64, align 8
  %__dnew.i.i.i.i381 = alloca i64, align 8
  %__dnew.i.i.i.i354 = alloca i64, align 8
  %__dnew.i.i.i.i323 = alloca i64, align 8
  %__dnew.i.i.i.i = alloca i64, align 8
  %outputColCount.addr = alloca i32, align 4
  %sortColCount.addr = alloca i32, align 4
  %testParam = alloca %"class.std::map", align 8
  %deps = alloca %"class.std::__cxx11::list", align 8
  %p_sortCols = alloca %"class.codegen::ParamValue", align 8
  %p_sortColTypes = alloca %"class.codegen::ParamValue", align 8
  %p_sortAscendings = alloca %"class.codegen::ParamValue", align 8
  %p_sortNullFirsts = alloca %"class.codegen::ParamValue", align 8
  %p_sortColCount = alloca %"class.codegen::ParamValue", align 8
  %ref.tmp = alloca %"class.std::__cxx11::basic_string", align 8
  %ref.tmp20 = alloca %"class.std::__cxx11::basic_string", align 8
  %ref.tmp31 = alloca %"class.std::__cxx11::basic_string", align 8
  %ref.tmp42 = alloca %"class.std::__cxx11::basic_string", align 8
  %ref.tmp53 = alloca %"class.std::__cxx11::basic_string", align 8
  %p_sourceTypes = alloca %"class.codegen::ParamValue", align 8
  %p_outputCols = alloca %"class.codegen::ParamValue", align 8
  %p_outputColCount = alloca %"class.codegen::ParamValue", align 8
  %ref.tmp70 = alloca %"class.std::__cxx11::basic_string", align 8
  %ref.tmp81 = alloca %"class.std::__cxx11::basic_string", align 8
  %ref.tmp92 = alloca %"class.std::__cxx11::basic_string", align 8
  %ref.tmp103 = alloca %"class.std::__cxx11::basic_string", align 8
  %ref.tmp114 = alloca %"class.std::__cxx11::basic_string", align 8
  %ref.tmp125 = alloca %"class.std::__cxx11::basic_string", align 8
  %hammer1 = alloca %"class.codegen::Hammer", align 8
  %agg.tmp140 = alloca %"class.std::map", align 8
  %hammer2 = alloca %"class.codegen::Hammer", align 8
  %agg.tmp148 = alloca %"class.std::map", align 8
  %hammerConfig = alloca %"class.codegen::HammerConfig", align 8
  %jitter = alloca %"class.std::unique_ptr.131", align 8
  %agg.tmp162 = alloca %"class.std::__cxx11::list", align 8
  %ref.tmp168 = alloca %"class.llvm::Expected", align 8
  %ref.tmp181 = alloca %"class.llvm::Expected", align 8
  %ref.tmp194 = alloca %"class.llvm::Expected", align 8
  store i32 %outputColCount, i32* %outputColCount.addr, align 4, !tbaa !16
  store i32 %sortColCount, i32* %sortColCount.addr, align 4, !tbaa !16
  %0 = getelementptr inbounds %"class.std::map", %"class.std::map"* %testParam, i64 0, i32 0, i32 0, i32 0, i32 0, i32 0
  call void @llvm.lifetime.start.p0i8(i64 48, i8* nonnull %0) #20
  %1 = getelementptr inbounds i8, i8* %0, i64 8
  %_M_color.i.i.i.i = bitcast i8* %1 to i32*
  store i32 0, i32* %_M_color.i.i.i.i, align 8, !tbaa !18
  %_M_parent.i.i.i.i.i = getelementptr inbounds i8, i8* %0, i64 16
  %2 = bitcast i8* %_M_parent.i.i.i.i.i to %"struct.std::_Rb_tree_node_base"**
  store %"struct.std::_Rb_tree_node_base"* null, %"struct.std::_Rb_tree_node_base"** %2, align 8, !tbaa !22
  %_M_left.i.i.i.i.i = getelementptr inbounds i8, i8* %0, i64 24
  %3 = bitcast i8* %_M_left.i.i.i.i.i to i8**
  store i8* %1, i8** %3, align 8, !tbaa !23
  %_M_right.i.i.i.i.i = getelementptr inbounds i8, i8* %0, i64 32
  %4 = bitcast i8* %_M_right.i.i.i.i.i to i8**
  store i8* %1, i8** %4, align 8, !tbaa !24
  %_M_node_count.i.i.i.i.i = getelementptr inbounds i8, i8* %0, i64 40
  %5 = bitcast i8* %_M_node_count.i.i.i.i.i to i64*
  store i64 0, i64* %5, align 8, !tbaa !25
  %6 = bitcast %"class.std::__cxx11::list"* %deps to i8*
  call void @llvm.lifetime.start.p0i8(i64 24, i8* nonnull %6) #20
  %7 = getelementptr inbounds %"class.std::__cxx11::list", %"class.std::__cxx11::list"* %deps, i64 0, i32 0, i32 0, i32 0, i32 0
  %_M_next.i.i.i = getelementptr inbounds %"class.std::__cxx11::list", %"class.std::__cxx11::list"* %deps, i64 0, i32 0, i32 0, i32 0, i32 0, i32 0
  store %"struct.std::__detail::_List_node_base"* %7, %"struct.std::__detail::_List_node_base"** %_M_next.i.i.i, align 8, !tbaa !26
  %_M_prev.i.i.i = getelementptr inbounds %"class.std::__cxx11::list", %"class.std::__cxx11::list"* %deps, i64 0, i32 0, i32 0, i32 0, i32 0, i32 1
  store %"struct.std::__detail::_List_node_base"* %7, %"struct.std::__detail::_List_node_base"** %_M_prev.i.i.i, align 8, !tbaa !28
  %_M_storage.i.i.i.i.i = getelementptr inbounds %"class.std::__cxx11::list", %"class.std::__cxx11::list"* %deps, i64 0, i32 0, i32 0, i32 0, i32 1
  %8 = bitcast %"struct.__gnu_cxx::__aligned_membuf"* %_M_storage.i.i.i.i.i to i64*
  store i64 0, i64* %8, align 8, !tbaa !29
  %9 = zext i32 %sortColCount to i64
  %10 = call i8* @llvm.stacksave()
  %vla = alloca i32, i64 %9, align 16
  %cmp810 = icmp sgt i32 %sortColCount, 0
  br i1 %cmp810, label %for.body.lr.ph, label %for.cond.cleanup

for.body.lr.ph:                                   ; preds = %entry
  %wide.trip.count = zext i32 %sortColCount to i64
  %11 = add nsw i64 %wide.trip.count, -1
  %xtraiter = and i64 %wide.trip.count, 3
  %12 = icmp ult i64 %11, 3
  br i1 %12, label %for.cond.cleanup.loopexit.unr-lcssa, label %for.body.lr.ph.new

for.body.lr.ph.new:                               ; preds = %for.body.lr.ph
  %unroll_iter = and i64 %wide.trip.count, 4294967292
  br label %for.body

for.cond.cleanup.loopexit.unr-lcssa:              ; preds = %for.body, %for.body.lr.ph
  %indvars.iv.unr = phi i64 [ 0, %for.body.lr.ph ], [ %indvars.iv.next.3, %for.body ]
  %lcmp.mod.not = icmp eq i64 %xtraiter, 0
  br i1 %lcmp.mod.not, label %for.cond.cleanup, label %for.body.epil

for.body.epil:                                    ; preds = %for.cond.cleanup.loopexit.unr-lcssa, %for.body.epil
  %indvars.iv.epil = phi i64 [ %indvars.iv.next.epil, %for.body.epil ], [ %indvars.iv.unr, %for.cond.cleanup.loopexit.unr-lcssa ]
  %epil.iter = phi i64 [ %epil.iter.sub, %for.body.epil ], [ %xtraiter, %for.cond.cleanup.loopexit.unr-lcssa ]
  %arrayidx.epil = getelementptr inbounds i32, i32* %sortCols, i64 %indvars.iv.epil
  %13 = load i32, i32* %arrayidx.epil, align 4, !tbaa !16
  %idxprom1.epil = sext i32 %13 to i64
  %arrayidx2.epil = getelementptr inbounds i32, i32* %sourceTypes, i64 %idxprom1.epil
  %14 = load i32, i32* %arrayidx2.epil, align 4, !tbaa !16
  %arrayidx4.epil = getelementptr inbounds i32, i32* %vla, i64 %indvars.iv.epil
  store i32 %14, i32* %arrayidx4.epil, align 4, !tbaa !16
  %indvars.iv.next.epil = add nuw nsw i64 %indvars.iv.epil, 1
  %epil.iter.sub = add i64 %epil.iter, -1
  %epil.iter.cmp.not = icmp eq i64 %epil.iter.sub, 0
  br i1 %epil.iter.cmp.not, label %for.cond.cleanup, label %for.body.epil, !llvm.loop !30

for.cond.cleanup:                                 ; preds = %for.cond.cleanup.loopexit.unr-lcssa, %for.body.epil, %entry
  %15 = bitcast %"class.codegen::ParamValue"* %p_sortCols to i8*
  call void @llvm.lifetime.start.p0i8(i64 24, i8* nonnull %15) #20
  %16 = bitcast %"class.codegen::ParamValue"* %p_sortCols to i32**
  store i32* %sortCols, i32** %16, align 8, !tbaa !32
  %size2.i = getelementptr inbounds %"class.codegen::ParamValue", %"class.codegen::ParamValue"* %p_sortCols, i64 0, i32 1
  store i32 %sortColCount, i32* %size2.i, align 8, !tbaa !36
  %type.i = getelementptr inbounds %"class.codegen::ParamValue", %"class.codegen::ParamValue"* %p_sortCols, i64 0, i32 2
  store i32 0, i32* %type.i, align 4, !tbaa !37
  %vector.i = getelementptr inbounds %"class.codegen::ParamValue", %"class.codegen::ParamValue"* %p_sortCols, i64 0, i32 3
  store i8 0, i8* %vector.i, align 8, !tbaa !38
  %17 = bitcast %"class.codegen::ParamValue"* %p_sortColTypes to i8*
  call void @llvm.lifetime.start.p0i8(i64 24, i8* nonnull %17) #20
  %18 = bitcast %"class.codegen::ParamValue"* %p_sortColTypes to i32**
  store i32* %vla, i32** %18, align 8, !tbaa !32
  %size2.i299 = getelementptr inbounds %"class.codegen::ParamValue", %"class.codegen::ParamValue"* %p_sortColTypes, i64 0, i32 1
  store i32 %sortColCount, i32* %size2.i299, align 8, !tbaa !36
  %type.i300 = getelementptr inbounds %"class.codegen::ParamValue", %"class.codegen::ParamValue"* %p_sortColTypes, i64 0, i32 2
  store i32 0, i32* %type.i300, align 4, !tbaa !37
  %vector.i301 = getelementptr inbounds %"class.codegen::ParamValue", %"class.codegen::ParamValue"* %p_sortColTypes, i64 0, i32 3
  store i8 0, i8* %vector.i301, align 8, !tbaa !38
  %19 = bitcast %"class.codegen::ParamValue"* %p_sortAscendings to i8*
  call void @llvm.lifetime.start.p0i8(i64 24, i8* nonnull %19) #20
  %20 = bitcast %"class.codegen::ParamValue"* %p_sortAscendings to i32**
  store i32* %sortAscendings, i32** %20, align 8, !tbaa !32
  %size2.i302 = getelementptr inbounds %"class.codegen::ParamValue", %"class.codegen::ParamValue"* %p_sortAscendings, i64 0, i32 1
  store i32 %sortColCount, i32* %size2.i302, align 8, !tbaa !36
  %type.i303 = getelementptr inbounds %"class.codegen::ParamValue", %"class.codegen::ParamValue"* %p_sortAscendings, i64 0, i32 2
  store i32 0, i32* %type.i303, align 4, !tbaa !37
  %vector.i304 = getelementptr inbounds %"class.codegen::ParamValue", %"class.codegen::ParamValue"* %p_sortAscendings, i64 0, i32 3
  store i8 0, i8* %vector.i304, align 8, !tbaa !38
  %21 = bitcast %"class.codegen::ParamValue"* %p_sortNullFirsts to i8*
  call void @llvm.lifetime.start.p0i8(i64 24, i8* nonnull %21) #20
  %22 = bitcast %"class.codegen::ParamValue"* %p_sortNullFirsts to i32**
  store i32* %sortNullFirsts, i32** %22, align 8, !tbaa !32
  %size2.i305 = getelementptr inbounds %"class.codegen::ParamValue", %"class.codegen::ParamValue"* %p_sortNullFirsts, i64 0, i32 1
  store i32 %sortColCount, i32* %size2.i305, align 8, !tbaa !36
  %type.i306 = getelementptr inbounds %"class.codegen::ParamValue", %"class.codegen::ParamValue"* %p_sortNullFirsts, i64 0, i32 2
  store i32 0, i32* %type.i306, align 4, !tbaa !37
  %vector.i307 = getelementptr inbounds %"class.codegen::ParamValue", %"class.codegen::ParamValue"* %p_sortNullFirsts, i64 0, i32 3
  store i8 0, i8* %vector.i307, align 8, !tbaa !38
  %23 = bitcast %"class.codegen::ParamValue"* %p_sortColCount to i8*
  call void @llvm.lifetime.start.p0i8(i64 24, i8* nonnull %23) #20
  %24 = bitcast %"class.codegen::ParamValue"* %p_sortColCount to i32**
  store i32* %sortColCount.addr, i32** %24, align 8, !tbaa !32
  %size.i = getelementptr inbounds %"class.codegen::ParamValue", %"class.codegen::ParamValue"* %p_sortColCount, i64 0, i32 1
  store i32 1, i32* %size.i, align 8, !tbaa !36
  %type.i308 = getelementptr inbounds %"class.codegen::ParamValue", %"class.codegen::ParamValue"* %p_sortColCount, i64 0, i32 2
  store i32 0, i32* %type.i308, align 4, !tbaa !37
  %vector.i309 = getelementptr inbounds %"class.codegen::ParamValue", %"class.codegen::ParamValue"* %p_sortColCount, i64 0, i32 3
  store i8 0, i8* %vector.i309, align 8, !tbaa !38
  %25 = bitcast %"class.std::__cxx11::basic_string"* %ref.tmp to i8*
  call void @llvm.lifetime.start.p0i8(i64 32, i8* nonnull %25) #20
  %26 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp, i64 0, i32 2
  %27 = bitcast %"class.std::__cxx11::basic_string"* %ref.tmp to %union.anon**
  store %union.anon* %26, %union.anon** %27, align 8, !tbaa !39
  %28 = bitcast %union.anon* %26 to i8*
  %29 = bitcast i64* %__dnew.i.i.i.i to i8*
  call void @llvm.lifetime.start.p0i8(i64 8, i8* nonnull %29) #20
  store i64 26, i64* %__dnew.i.i.i.i, align 8, !tbaa !29
  %call5.i.i.i10.i311 = invoke i8* @_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE9_M_createERmm(%"class.std::__cxx11::basic_string"* nonnull dereferenceable(32) %ref.tmp, i64* nonnull align 8 dereferenceable(8) %__dnew.i.i.i.i, i64 0)
          to label %call5.i.i.i10.i.noexc unwind label %lpad14

call5.i.i.i10.i.noexc:                            ; preds = %for.cond.cleanup
  %_M_p.i13.i.i.i.i = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp, i64 0, i32 0, i32 0
  store i8* %call5.i.i.i10.i311, i8** %_M_p.i13.i.i.i.i, align 8, !tbaa !41
  %30 = load i64, i64* %__dnew.i.i.i.i, align 8, !tbaa !29
  %_M_allocated_capacity.i.i.i.i.i = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp, i64 0, i32 2, i32 0
  store i64 %30, i64* %_M_allocated_capacity.i.i.i.i.i, align 8, !tbaa !43
  call void @llvm.memcpy.p0i8.p0i8.i64(i8* nonnull align 1 dereferenceable(26) %call5.i.i.i10.i311, i8* nonnull align 1 dereferenceable(26) getelementptr inbounds ([27 x i8], [27 x i8]* @.str, i64 0, i64 0), i64 26, i1 false) #20
  %_M_string_length.i.i.i.i.i.i = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp, i64 0, i32 1
  store i64 %30, i64* %_M_string_length.i.i.i.i.i.i, align 8, !tbaa !44
  %31 = load i8*, i8** %_M_p.i13.i.i.i.i, align 8, !tbaa !41
  %arrayidx.i.i.i.i.i = getelementptr inbounds i8, i8* %31, i64 %30
  store i8 0, i8* %arrayidx.i.i.i.i.i, align 1, !tbaa !43
  call void @llvm.lifetime.end.p0i8(i64 8, i8* nonnull %29) #20
  %call = invoke nonnull align 8 dereferenceable(8) %"class.codegen::ParamValue"** @_ZNSt3mapINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEPN7codegen10ParamValueESt4lessIS5_ESaISt4pairIKS5_S8_EEEixEOS5_(%"class.std::map"* nonnull dereferenceable(48) %testParam, %"class.std::__cxx11::basic_string"* nonnull align 8 dereferenceable(32) %ref.tmp)
          to label %invoke.cont17 unwind label %lpad16

for.body:                                         ; preds = %for.body, %for.body.lr.ph.new
  %indvars.iv = phi i64 [ 0, %for.body.lr.ph.new ], [ %indvars.iv.next.3, %for.body ]
  %niter = phi i64 [ %unroll_iter, %for.body.lr.ph.new ], [ %niter.nsub.3, %for.body ]
  %arrayidx = getelementptr inbounds i32, i32* %sortCols, i64 %indvars.iv
  %32 = load i32, i32* %arrayidx, align 4, !tbaa !16
  %idxprom1 = sext i32 %32 to i64
  %arrayidx2 = getelementptr inbounds i32, i32* %sourceTypes, i64 %idxprom1
  %33 = load i32, i32* %arrayidx2, align 4, !tbaa !16
  %arrayidx4 = getelementptr inbounds i32, i32* %vla, i64 %indvars.iv
  store i32 %33, i32* %arrayidx4, align 16, !tbaa !16
  %indvars.iv.next = or i64 %indvars.iv, 1
  %arrayidx.1 = getelementptr inbounds i32, i32* %sortCols, i64 %indvars.iv.next
  %34 = load i32, i32* %arrayidx.1, align 4, !tbaa !16
  %idxprom1.1 = sext i32 %34 to i64
  %arrayidx2.1 = getelementptr inbounds i32, i32* %sourceTypes, i64 %idxprom1.1
  %35 = load i32, i32* %arrayidx2.1, align 4, !tbaa !16
  %arrayidx4.1 = getelementptr inbounds i32, i32* %vla, i64 %indvars.iv.next
  store i32 %35, i32* %arrayidx4.1, align 4, !tbaa !16
  %indvars.iv.next.1 = or i64 %indvars.iv, 2
  %arrayidx.2 = getelementptr inbounds i32, i32* %sortCols, i64 %indvars.iv.next.1
  %36 = load i32, i32* %arrayidx.2, align 4, !tbaa !16
  %idxprom1.2 = sext i32 %36 to i64
  %arrayidx2.2 = getelementptr inbounds i32, i32* %sourceTypes, i64 %idxprom1.2
  %37 = load i32, i32* %arrayidx2.2, align 4, !tbaa !16
  %arrayidx4.2 = getelementptr inbounds i32, i32* %vla, i64 %indvars.iv.next.1
  store i32 %37, i32* %arrayidx4.2, align 8, !tbaa !16
  %indvars.iv.next.2 = or i64 %indvars.iv, 3
  %arrayidx.3 = getelementptr inbounds i32, i32* %sortCols, i64 %indvars.iv.next.2
  %38 = load i32, i32* %arrayidx.3, align 4, !tbaa !16
  %idxprom1.3 = sext i32 %38 to i64
  %arrayidx2.3 = getelementptr inbounds i32, i32* %sourceTypes, i64 %idxprom1.3
  %39 = load i32, i32* %arrayidx2.3, align 4, !tbaa !16
  %arrayidx4.3 = getelementptr inbounds i32, i32* %vla, i64 %indvars.iv.next.2
  store i32 %39, i32* %arrayidx4.3, align 4, !tbaa !16
  %indvars.iv.next.3 = add nuw nsw i64 %indvars.iv, 4
  %niter.nsub.3 = add i64 %niter, -4
  %niter.ncmp.3 = icmp eq i64 %niter.nsub.3, 0
  br i1 %niter.ncmp.3, label %for.cond.cleanup.loopexit.unr-lcssa, label %for.body, !llvm.loop !45

invoke.cont17:                                    ; preds = %call5.i.i.i10.i.noexc
  store %"class.codegen::ParamValue"* %p_sortCols, %"class.codegen::ParamValue"** %call, align 8, !tbaa !46
  %40 = load i8*, i8** %_M_p.i13.i.i.i.i, align 8, !tbaa !41
  %cmp.i.i.i = icmp eq i8* %40, %28
  br i1 %cmp.i.i.i, label %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit, label %if.then.i.i

if.then.i.i:                                      ; preds = %invoke.cont17
  call void @_ZdlPv(i8* %40) #20
  br label %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit

_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit: ; preds = %invoke.cont17, %if.then.i.i
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %25) #20
  %41 = bitcast %"class.std::__cxx11::basic_string"* %ref.tmp20 to i8*
  call void @llvm.lifetime.start.p0i8(i64 32, i8* nonnull %41) #20
  %42 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp20, i64 0, i32 2
  %43 = bitcast %"class.std::__cxx11::basic_string"* %ref.tmp20 to %union.anon**
  store %union.anon* %42, %union.anon** %43, align 8, !tbaa !39
  %44 = bitcast %union.anon* %42 to i8*
  %45 = bitcast i64* %__dnew.i.i.i.i323 to i8*
  call void @llvm.lifetime.start.p0i8(i64 8, i8* nonnull %45) #20
  store i64 26, i64* %__dnew.i.i.i.i323, align 8, !tbaa !29
  %call5.i.i.i10.i336 = invoke i8* @_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE9_M_createERmm(%"class.std::__cxx11::basic_string"* nonnull dereferenceable(32) %ref.tmp20, i64* nonnull align 8 dereferenceable(8) %__dnew.i.i.i.i323, i64 0)
          to label %call5.i.i.i10.i.noexc335 unwind label %lpad22

call5.i.i.i10.i.noexc335:                         ; preds = %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit
  %_M_p.i13.i.i.i.i326 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp20, i64 0, i32 0, i32 0
  store i8* %call5.i.i.i10.i336, i8** %_M_p.i13.i.i.i.i326, align 8, !tbaa !41
  %46 = load i64, i64* %__dnew.i.i.i.i323, align 8, !tbaa !29
  %_M_allocated_capacity.i.i.i.i.i327 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp20, i64 0, i32 2, i32 0
  store i64 %46, i64* %_M_allocated_capacity.i.i.i.i.i327, align 8, !tbaa !43
  call void @llvm.memcpy.p0i8.p0i8.i64(i8* nonnull align 1 dereferenceable(26) %call5.i.i.i10.i336, i8* nonnull align 1 dereferenceable(26) getelementptr inbounds ([27 x i8], [27 x i8]* @.str.2, i64 0, i64 0), i64 26, i1 false) #20
  %_M_string_length.i.i.i.i.i.i333 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp20, i64 0, i32 1
  store i64 %46, i64* %_M_string_length.i.i.i.i.i.i333, align 8, !tbaa !44
  %47 = load i8*, i8** %_M_p.i13.i.i.i.i326, align 8, !tbaa !41
  %arrayidx.i.i.i.i.i334 = getelementptr inbounds i8, i8* %47, i64 %46
  store i8 0, i8* %arrayidx.i.i.i.i.i334, align 1, !tbaa !43
  call void @llvm.lifetime.end.p0i8(i64 8, i8* nonnull %45) #20
  %call26 = invoke nonnull align 8 dereferenceable(8) %"class.codegen::ParamValue"** @_ZNSt3mapINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEPN7codegen10ParamValueESt4lessIS5_ESaISt4pairIKS5_S8_EEEixEOS5_(%"class.std::map"* nonnull dereferenceable(48) %testParam, %"class.std::__cxx11::basic_string"* nonnull align 8 dereferenceable(32) %ref.tmp20)
          to label %invoke.cont25 unwind label %lpad24

invoke.cont25:                                    ; preds = %call5.i.i.i10.i.noexc335
  store %"class.codegen::ParamValue"* %p_sortColTypes, %"class.codegen::ParamValue"** %call26, align 8, !tbaa !46
  %48 = load i8*, i8** %_M_p.i13.i.i.i.i326, align 8, !tbaa !41
  %cmp.i.i.i340 = icmp eq i8* %48, %44
  br i1 %cmp.i.i.i340, label %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit342, label %if.then.i.i341

if.then.i.i341:                                   ; preds = %invoke.cont25
  call void @_ZdlPv(i8* %48) #20
  br label %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit342

_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit342: ; preds = %invoke.cont25, %if.then.i.i341
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %41) #20
  %49 = bitcast %"class.std::__cxx11::basic_string"* %ref.tmp31 to i8*
  call void @llvm.lifetime.start.p0i8(i64 32, i8* nonnull %49) #20
  %50 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp31, i64 0, i32 2
  %51 = bitcast %"class.std::__cxx11::basic_string"* %ref.tmp31 to %union.anon**
  store %union.anon* %50, %union.anon** %51, align 8, !tbaa !39
  %52 = bitcast %union.anon* %50 to i8*
  %53 = bitcast i64* %__dnew.i.i.i.i354 to i8*
  call void @llvm.lifetime.start.p0i8(i64 8, i8* nonnull %53) #20
  store i64 26, i64* %__dnew.i.i.i.i354, align 8, !tbaa !29
  %call5.i.i.i10.i367 = invoke i8* @_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE9_M_createERmm(%"class.std::__cxx11::basic_string"* nonnull dereferenceable(32) %ref.tmp31, i64* nonnull align 8 dereferenceable(8) %__dnew.i.i.i.i354, i64 0)
          to label %call5.i.i.i10.i.noexc366 unwind label %lpad33

call5.i.i.i10.i.noexc366:                         ; preds = %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit342
  %_M_p.i13.i.i.i.i357 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp31, i64 0, i32 0, i32 0
  store i8* %call5.i.i.i10.i367, i8** %_M_p.i13.i.i.i.i357, align 8, !tbaa !41
  %54 = load i64, i64* %__dnew.i.i.i.i354, align 8, !tbaa !29
  %_M_allocated_capacity.i.i.i.i.i358 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp31, i64 0, i32 2, i32 0
  store i64 %54, i64* %_M_allocated_capacity.i.i.i.i.i358, align 8, !tbaa !43
  call void @llvm.memcpy.p0i8.p0i8.i64(i8* nonnull align 1 dereferenceable(26) %call5.i.i.i10.i367, i8* nonnull align 1 dereferenceable(26) getelementptr inbounds ([27 x i8], [27 x i8]* @.str.3, i64 0, i64 0), i64 26, i1 false) #20
  %_M_string_length.i.i.i.i.i.i364 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp31, i64 0, i32 1
  store i64 %54, i64* %_M_string_length.i.i.i.i.i.i364, align 8, !tbaa !44
  %55 = load i8*, i8** %_M_p.i13.i.i.i.i357, align 8, !tbaa !41
  %arrayidx.i.i.i.i.i365 = getelementptr inbounds i8, i8* %55, i64 %54
  store i8 0, i8* %arrayidx.i.i.i.i.i365, align 1, !tbaa !43
  call void @llvm.lifetime.end.p0i8(i64 8, i8* nonnull %53) #20
  %call37 = invoke nonnull align 8 dereferenceable(8) %"class.codegen::ParamValue"** @_ZNSt3mapINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEPN7codegen10ParamValueESt4lessIS5_ESaISt4pairIKS5_S8_EEEixEOS5_(%"class.std::map"* nonnull dereferenceable(48) %testParam, %"class.std::__cxx11::basic_string"* nonnull align 8 dereferenceable(32) %ref.tmp31)
          to label %invoke.cont36 unwind label %lpad35

invoke.cont36:                                    ; preds = %call5.i.i.i10.i.noexc366
  store %"class.codegen::ParamValue"* %p_sortAscendings, %"class.codegen::ParamValue"** %call37, align 8, !tbaa !46
  %56 = load i8*, i8** %_M_p.i13.i.i.i.i357, align 8, !tbaa !41
  %cmp.i.i.i371 = icmp eq i8* %56, %52
  br i1 %cmp.i.i.i371, label %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit373, label %if.then.i.i372

if.then.i.i372:                                   ; preds = %invoke.cont36
  call void @_ZdlPv(i8* %56) #20
  br label %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit373

_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit373: ; preds = %invoke.cont36, %if.then.i.i372
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %49) #20
  %57 = bitcast %"class.std::__cxx11::basic_string"* %ref.tmp42 to i8*
  call void @llvm.lifetime.start.p0i8(i64 32, i8* nonnull %57) #20
  %58 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp42, i64 0, i32 2
  %59 = bitcast %"class.std::__cxx11::basic_string"* %ref.tmp42 to %union.anon**
  store %union.anon* %58, %union.anon** %59, align 8, !tbaa !39
  %60 = bitcast %union.anon* %58 to i8*
  %61 = bitcast i64* %__dnew.i.i.i.i381 to i8*
  call void @llvm.lifetime.start.p0i8(i64 8, i8* nonnull %61) #20
  store i64 26, i64* %__dnew.i.i.i.i381, align 8, !tbaa !29
  %call5.i.i.i10.i394 = invoke i8* @_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE9_M_createERmm(%"class.std::__cxx11::basic_string"* nonnull dereferenceable(32) %ref.tmp42, i64* nonnull align 8 dereferenceable(8) %__dnew.i.i.i.i381, i64 0)
          to label %call5.i.i.i10.i.noexc393 unwind label %lpad44

call5.i.i.i10.i.noexc393:                         ; preds = %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit373
  %_M_p.i13.i.i.i.i384 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp42, i64 0, i32 0, i32 0
  store i8* %call5.i.i.i10.i394, i8** %_M_p.i13.i.i.i.i384, align 8, !tbaa !41
  %62 = load i64, i64* %__dnew.i.i.i.i381, align 8, !tbaa !29
  %_M_allocated_capacity.i.i.i.i.i385 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp42, i64 0, i32 2, i32 0
  store i64 %62, i64* %_M_allocated_capacity.i.i.i.i.i385, align 8, !tbaa !43
  call void @llvm.memcpy.p0i8.p0i8.i64(i8* nonnull align 1 dereferenceable(26) %call5.i.i.i10.i394, i8* nonnull align 1 dereferenceable(26) getelementptr inbounds ([27 x i8], [27 x i8]* @.str.4, i64 0, i64 0), i64 26, i1 false) #20
  %_M_string_length.i.i.i.i.i.i391 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp42, i64 0, i32 1
  store i64 %62, i64* %_M_string_length.i.i.i.i.i.i391, align 8, !tbaa !44
  %63 = load i8*, i8** %_M_p.i13.i.i.i.i384, align 8, !tbaa !41
  %arrayidx.i.i.i.i.i392 = getelementptr inbounds i8, i8* %63, i64 %62
  store i8 0, i8* %arrayidx.i.i.i.i.i392, align 1, !tbaa !43
  call void @llvm.lifetime.end.p0i8(i64 8, i8* nonnull %61) #20
  %call48 = invoke nonnull align 8 dereferenceable(8) %"class.codegen::ParamValue"** @_ZNSt3mapINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEPN7codegen10ParamValueESt4lessIS5_ESaISt4pairIKS5_S8_EEEixEOS5_(%"class.std::map"* nonnull dereferenceable(48) %testParam, %"class.std::__cxx11::basic_string"* nonnull align 8 dereferenceable(32) %ref.tmp42)
          to label %invoke.cont47 unwind label %lpad46

invoke.cont47:                                    ; preds = %call5.i.i.i10.i.noexc393
  store %"class.codegen::ParamValue"* %p_sortNullFirsts, %"class.codegen::ParamValue"** %call48, align 8, !tbaa !46
  %64 = load i8*, i8** %_M_p.i13.i.i.i.i384, align 8, !tbaa !41
  %cmp.i.i.i398 = icmp eq i8* %64, %60
  br i1 %cmp.i.i.i398, label %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit400, label %if.then.i.i399

if.then.i.i399:                                   ; preds = %invoke.cont47
  call void @_ZdlPv(i8* %64) #20
  br label %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit400

_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit400: ; preds = %invoke.cont47, %if.then.i.i399
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %57) #20
  %65 = bitcast %"class.std::__cxx11::basic_string"* %ref.tmp53 to i8*
  call void @llvm.lifetime.start.p0i8(i64 32, i8* nonnull %65) #20
  %66 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp53, i64 0, i32 2
  %67 = bitcast %"class.std::__cxx11::basic_string"* %ref.tmp53 to %union.anon**
  store %union.anon* %66, %union.anon** %67, align 8, !tbaa !39
  %68 = bitcast %union.anon* %66 to i8*
  %69 = bitcast i64* %__dnew.i.i.i.i405 to i8*
  call void @llvm.lifetime.start.p0i8(i64 8, i8* nonnull %69) #20
  store i64 26, i64* %__dnew.i.i.i.i405, align 8, !tbaa !29
  %call5.i.i.i10.i418 = invoke i8* @_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE9_M_createERmm(%"class.std::__cxx11::basic_string"* nonnull dereferenceable(32) %ref.tmp53, i64* nonnull align 8 dereferenceable(8) %__dnew.i.i.i.i405, i64 0)
          to label %call5.i.i.i10.i.noexc417 unwind label %lpad55

call5.i.i.i10.i.noexc417:                         ; preds = %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit400
  %_M_p.i13.i.i.i.i408 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp53, i64 0, i32 0, i32 0
  store i8* %call5.i.i.i10.i418, i8** %_M_p.i13.i.i.i.i408, align 8, !tbaa !41
  %70 = load i64, i64* %__dnew.i.i.i.i405, align 8, !tbaa !29
  %_M_allocated_capacity.i.i.i.i.i409 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp53, i64 0, i32 2, i32 0
  store i64 %70, i64* %_M_allocated_capacity.i.i.i.i.i409, align 8, !tbaa !43
  call void @llvm.memcpy.p0i8.p0i8.i64(i8* nonnull align 1 dereferenceable(26) %call5.i.i.i10.i418, i8* nonnull align 1 dereferenceable(26) getelementptr inbounds ([27 x i8], [27 x i8]* @.str.5, i64 0, i64 0), i64 26, i1 false) #20
  %_M_string_length.i.i.i.i.i.i415 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp53, i64 0, i32 1
  store i64 %70, i64* %_M_string_length.i.i.i.i.i.i415, align 8, !tbaa !44
  %71 = load i8*, i8** %_M_p.i13.i.i.i.i408, align 8, !tbaa !41
  %arrayidx.i.i.i.i.i416 = getelementptr inbounds i8, i8* %71, i64 %70
  store i8 0, i8* %arrayidx.i.i.i.i.i416, align 1, !tbaa !43
  call void @llvm.lifetime.end.p0i8(i64 8, i8* nonnull %69) #20
  %call59 = invoke nonnull align 8 dereferenceable(8) %"class.codegen::ParamValue"** @_ZNSt3mapINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEPN7codegen10ParamValueESt4lessIS5_ESaISt4pairIKS5_S8_EEEixEOS5_(%"class.std::map"* nonnull dereferenceable(48) %testParam, %"class.std::__cxx11::basic_string"* nonnull align 8 dereferenceable(32) %ref.tmp53)
          to label %invoke.cont58 unwind label %lpad57

invoke.cont58:                                    ; preds = %call5.i.i.i10.i.noexc417
  store %"class.codegen::ParamValue"* %p_sortColCount, %"class.codegen::ParamValue"** %call59, align 8, !tbaa !46
  %72 = load i8*, i8** %_M_p.i13.i.i.i.i408, align 8, !tbaa !41
  %cmp.i.i.i422 = icmp eq i8* %72, %68
  br i1 %cmp.i.i.i422, label %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit424, label %if.then.i.i423

if.then.i.i423:                                   ; preds = %invoke.cont58
  call void @_ZdlPv(i8* %72) #20
  br label %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit424

_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit424: ; preds = %invoke.cont58, %if.then.i.i423
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %65) #20
  %73 = bitcast %"class.codegen::ParamValue"* %p_sourceTypes to i8*
  call void @llvm.lifetime.start.p0i8(i64 24, i8* nonnull %73) #20
  %74 = bitcast %"class.codegen::ParamValue"* %p_sourceTypes to i32**
  store i32* %sourceTypes, i32** %74, align 8, !tbaa !32
  %size2.i425 = getelementptr inbounds %"class.codegen::ParamValue", %"class.codegen::ParamValue"* %p_sourceTypes, i64 0, i32 1
  store i32 %typeCount, i32* %size2.i425, align 8, !tbaa !36
  %type.i426 = getelementptr inbounds %"class.codegen::ParamValue", %"class.codegen::ParamValue"* %p_sourceTypes, i64 0, i32 2
  store i32 0, i32* %type.i426, align 4, !tbaa !37
  %vector.i427 = getelementptr inbounds %"class.codegen::ParamValue", %"class.codegen::ParamValue"* %p_sourceTypes, i64 0, i32 3
  store i8 0, i8* %vector.i427, align 8, !tbaa !38
  %75 = bitcast %"class.codegen::ParamValue"* %p_outputCols to i8*
  call void @llvm.lifetime.start.p0i8(i64 24, i8* nonnull %75) #20
  %76 = load i32, i32* %outputColCount.addr, align 4, !tbaa !16
  %77 = bitcast %"class.codegen::ParamValue"* %p_outputCols to i32**
  store i32* %outputCols, i32** %77, align 8, !tbaa !32
  %size2.i428 = getelementptr inbounds %"class.codegen::ParamValue", %"class.codegen::ParamValue"* %p_outputCols, i64 0, i32 1
  store i32 %76, i32* %size2.i428, align 8, !tbaa !36
  %type.i429 = getelementptr inbounds %"class.codegen::ParamValue", %"class.codegen::ParamValue"* %p_outputCols, i64 0, i32 2
  store i32 0, i32* %type.i429, align 4, !tbaa !37
  %vector.i430 = getelementptr inbounds %"class.codegen::ParamValue", %"class.codegen::ParamValue"* %p_outputCols, i64 0, i32 3
  store i8 0, i8* %vector.i430, align 8, !tbaa !38
  %78 = bitcast %"class.codegen::ParamValue"* %p_outputColCount to i8*
  call void @llvm.lifetime.start.p0i8(i64 24, i8* nonnull %78) #20
  %79 = bitcast %"class.codegen::ParamValue"* %p_outputColCount to i32**
  store i32* %outputColCount.addr, i32** %79, align 8, !tbaa !32
  %size.i435 = getelementptr inbounds %"class.codegen::ParamValue", %"class.codegen::ParamValue"* %p_outputColCount, i64 0, i32 1
  store i32 1, i32* %size.i435, align 8, !tbaa !36
  %type.i436 = getelementptr inbounds %"class.codegen::ParamValue", %"class.codegen::ParamValue"* %p_outputColCount, i64 0, i32 2
  store i32 0, i32* %type.i436, align 4, !tbaa !37
  %vector.i437 = getelementptr inbounds %"class.codegen::ParamValue", %"class.codegen::ParamValue"* %p_outputColCount, i64 0, i32 3
  store i8 0, i8* %vector.i437, align 8, !tbaa !38
  %80 = bitcast %"class.std::__cxx11::basic_string"* %ref.tmp70 to i8*
  call void @llvm.lifetime.start.p0i8(i64 32, i8* nonnull %80) #20
  %81 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp70, i64 0, i32 2
  %82 = bitcast %"class.std::__cxx11::basic_string"* %ref.tmp70 to %union.anon**
  store %union.anon* %81, %union.anon** %82, align 8, !tbaa !39
  %83 = bitcast %union.anon* %81 to i8*
  %84 = bitcast i64* %__dnew.i.i.i.i443 to i8*
  call void @llvm.lifetime.start.p0i8(i64 8, i8* nonnull %84) #20
  store i64 25, i64* %__dnew.i.i.i.i443, align 8, !tbaa !29
  %call5.i.i.i10.i456 = invoke i8* @_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE9_M_createERmm(%"class.std::__cxx11::basic_string"* nonnull dereferenceable(32) %ref.tmp70, i64* nonnull align 8 dereferenceable(8) %__dnew.i.i.i.i443, i64 0)
          to label %call5.i.i.i10.i.noexc455 unwind label %lpad72

call5.i.i.i10.i.noexc455:                         ; preds = %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit424
  %_M_p.i13.i.i.i.i446 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp70, i64 0, i32 0, i32 0
  store i8* %call5.i.i.i10.i456, i8** %_M_p.i13.i.i.i.i446, align 8, !tbaa !41
  %85 = load i64, i64* %__dnew.i.i.i.i443, align 8, !tbaa !29
  %_M_allocated_capacity.i.i.i.i.i447 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp70, i64 0, i32 2, i32 0
  store i64 %85, i64* %_M_allocated_capacity.i.i.i.i.i447, align 8, !tbaa !43
  call void @llvm.memcpy.p0i8.p0i8.i64(i8* nonnull align 1 dereferenceable(25) %call5.i.i.i10.i456, i8* nonnull align 1 dereferenceable(25) getelementptr inbounds ([26 x i8], [26 x i8]* @.str.6, i64 0, i64 0), i64 25, i1 false) #20
  %_M_string_length.i.i.i.i.i.i453 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp70, i64 0, i32 1
  store i64 %85, i64* %_M_string_length.i.i.i.i.i.i453, align 8, !tbaa !44
  %86 = load i8*, i8** %_M_p.i13.i.i.i.i446, align 8, !tbaa !41
  %arrayidx.i.i.i.i.i454 = getelementptr inbounds i8, i8* %86, i64 %85
  store i8 0, i8* %arrayidx.i.i.i.i.i454, align 1, !tbaa !43
  call void @llvm.lifetime.end.p0i8(i64 8, i8* nonnull %84) #20
  %call76 = invoke nonnull align 8 dereferenceable(8) %"class.codegen::ParamValue"** @_ZNSt3mapINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEPN7codegen10ParamValueESt4lessIS5_ESaISt4pairIKS5_S8_EEEixEOS5_(%"class.std::map"* nonnull dereferenceable(48) %testParam, %"class.std::__cxx11::basic_string"* nonnull align 8 dereferenceable(32) %ref.tmp70)
          to label %invoke.cont75 unwind label %lpad74

invoke.cont75:                                    ; preds = %call5.i.i.i10.i.noexc455
  store %"class.codegen::ParamValue"* %p_sourceTypes, %"class.codegen::ParamValue"** %call76, align 8, !tbaa !46
  %87 = load i8*, i8** %_M_p.i13.i.i.i.i446, align 8, !tbaa !41
  %cmp.i.i.i460 = icmp eq i8* %87, %83
  br i1 %cmp.i.i.i460, label %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit462, label %if.then.i.i461

if.then.i.i461:                                   ; preds = %invoke.cont75
  call void @_ZdlPv(i8* %87) #20
  br label %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit462

_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit462: ; preds = %invoke.cont75, %if.then.i.i461
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %80) #20
  %88 = bitcast %"class.std::__cxx11::basic_string"* %ref.tmp81 to i8*
  call void @llvm.lifetime.start.p0i8(i64 32, i8* nonnull %88) #20
  %89 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp81, i64 0, i32 2
  %90 = bitcast %"class.std::__cxx11::basic_string"* %ref.tmp81 to %union.anon**
  store %union.anon* %89, %union.anon** %90, align 8, !tbaa !39
  %91 = bitcast %union.anon* %89 to i8*
  %92 = bitcast i64* %__dnew.i.i.i.i468 to i8*
  call void @llvm.lifetime.start.p0i8(i64 8, i8* nonnull %92) #20
  store i64 25, i64* %__dnew.i.i.i.i468, align 8, !tbaa !29
  %call5.i.i.i10.i481 = invoke i8* @_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE9_M_createERmm(%"class.std::__cxx11::basic_string"* nonnull dereferenceable(32) %ref.tmp81, i64* nonnull align 8 dereferenceable(8) %__dnew.i.i.i.i468, i64 0)
          to label %call5.i.i.i10.i.noexc480 unwind label %lpad83

call5.i.i.i10.i.noexc480:                         ; preds = %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit462
  %_M_p.i13.i.i.i.i471 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp81, i64 0, i32 0, i32 0
  store i8* %call5.i.i.i10.i481, i8** %_M_p.i13.i.i.i.i471, align 8, !tbaa !41
  %93 = load i64, i64* %__dnew.i.i.i.i468, align 8, !tbaa !29
  %_M_allocated_capacity.i.i.i.i.i472 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp81, i64 0, i32 2, i32 0
  store i64 %93, i64* %_M_allocated_capacity.i.i.i.i.i472, align 8, !tbaa !43
  call void @llvm.memcpy.p0i8.p0i8.i64(i8* nonnull align 1 dereferenceable(25) %call5.i.i.i10.i481, i8* nonnull align 1 dereferenceable(25) getelementptr inbounds ([26 x i8], [26 x i8]* @.str.7, i64 0, i64 0), i64 25, i1 false) #20
  %_M_string_length.i.i.i.i.i.i478 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp81, i64 0, i32 1
  store i64 %93, i64* %_M_string_length.i.i.i.i.i.i478, align 8, !tbaa !44
  %94 = load i8*, i8** %_M_p.i13.i.i.i.i471, align 8, !tbaa !41
  %arrayidx.i.i.i.i.i479 = getelementptr inbounds i8, i8* %94, i64 %93
  store i8 0, i8* %arrayidx.i.i.i.i.i479, align 1, !tbaa !43
  call void @llvm.lifetime.end.p0i8(i64 8, i8* nonnull %92) #20
  %call87 = invoke nonnull align 8 dereferenceable(8) %"class.codegen::ParamValue"** @_ZNSt3mapINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEPN7codegen10ParamValueESt4lessIS5_ESaISt4pairIKS5_S8_EEEixEOS5_(%"class.std::map"* nonnull dereferenceable(48) %testParam, %"class.std::__cxx11::basic_string"* nonnull align 8 dereferenceable(32) %ref.tmp81)
          to label %invoke.cont86 unwind label %lpad85

invoke.cont86:                                    ; preds = %call5.i.i.i10.i.noexc480
  store %"class.codegen::ParamValue"* %p_outputCols, %"class.codegen::ParamValue"** %call87, align 8, !tbaa !46
  %95 = load i8*, i8** %_M_p.i13.i.i.i.i471, align 8, !tbaa !41
  %cmp.i.i.i485 = icmp eq i8* %95, %91
  br i1 %cmp.i.i.i485, label %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit487, label %if.then.i.i486

if.then.i.i486:                                   ; preds = %invoke.cont86
  call void @_ZdlPv(i8* %95) #20
  br label %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit487

_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit487: ; preds = %invoke.cont86, %if.then.i.i486
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %88) #20
  %96 = bitcast %"class.std::__cxx11::basic_string"* %ref.tmp92 to i8*
  call void @llvm.lifetime.start.p0i8(i64 32, i8* nonnull %96) #20
  %97 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp92, i64 0, i32 2
  %98 = bitcast %"class.std::__cxx11::basic_string"* %ref.tmp92 to %union.anon**
  store %union.anon* %97, %union.anon** %98, align 8, !tbaa !39
  %99 = bitcast %union.anon* %97 to i8*
  %100 = bitcast i64* %__dnew.i.i.i.i493 to i8*
  call void @llvm.lifetime.start.p0i8(i64 8, i8* nonnull %100) #20
  store i64 25, i64* %__dnew.i.i.i.i493, align 8, !tbaa !29
  %call5.i.i.i10.i506 = invoke i8* @_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE9_M_createERmm(%"class.std::__cxx11::basic_string"* nonnull dereferenceable(32) %ref.tmp92, i64* nonnull align 8 dereferenceable(8) %__dnew.i.i.i.i493, i64 0)
          to label %call5.i.i.i10.i.noexc505 unwind label %lpad94

call5.i.i.i10.i.noexc505:                         ; preds = %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit487
  %_M_p.i13.i.i.i.i496 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp92, i64 0, i32 0, i32 0
  store i8* %call5.i.i.i10.i506, i8** %_M_p.i13.i.i.i.i496, align 8, !tbaa !41
  %101 = load i64, i64* %__dnew.i.i.i.i493, align 8, !tbaa !29
  %_M_allocated_capacity.i.i.i.i.i497 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp92, i64 0, i32 2, i32 0
  store i64 %101, i64* %_M_allocated_capacity.i.i.i.i.i497, align 8, !tbaa !43
  call void @llvm.memcpy.p0i8.p0i8.i64(i8* nonnull align 1 dereferenceable(25) %call5.i.i.i10.i506, i8* nonnull align 1 dereferenceable(25) getelementptr inbounds ([26 x i8], [26 x i8]* @.str.8, i64 0, i64 0), i64 25, i1 false) #20
  %_M_string_length.i.i.i.i.i.i503 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp92, i64 0, i32 1
  store i64 %101, i64* %_M_string_length.i.i.i.i.i.i503, align 8, !tbaa !44
  %102 = load i8*, i8** %_M_p.i13.i.i.i.i496, align 8, !tbaa !41
  %arrayidx.i.i.i.i.i504 = getelementptr inbounds i8, i8* %102, i64 %101
  store i8 0, i8* %arrayidx.i.i.i.i.i504, align 1, !tbaa !43
  call void @llvm.lifetime.end.p0i8(i64 8, i8* nonnull %100) #20
  %call98 = invoke nonnull align 8 dereferenceable(8) %"class.codegen::ParamValue"** @_ZNSt3mapINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEPN7codegen10ParamValueESt4lessIS5_ESaISt4pairIKS5_S8_EEEixEOS5_(%"class.std::map"* nonnull dereferenceable(48) %testParam, %"class.std::__cxx11::basic_string"* nonnull align 8 dereferenceable(32) %ref.tmp92)
          to label %invoke.cont97 unwind label %lpad96

invoke.cont97:                                    ; preds = %call5.i.i.i10.i.noexc505
  store %"class.codegen::ParamValue"* %p_outputColCount, %"class.codegen::ParamValue"** %call98, align 8, !tbaa !46
  %103 = load i8*, i8** %_M_p.i13.i.i.i.i496, align 8, !tbaa !41
  %cmp.i.i.i510 = icmp eq i8* %103, %99
  br i1 %cmp.i.i.i510, label %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit512, label %if.then.i.i511

if.then.i.i511:                                   ; preds = %invoke.cont97
  call void @_ZdlPv(i8* %103) #20
  br label %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit512

_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit512: ; preds = %invoke.cont97, %if.then.i.i511
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %96) #20
  %104 = bitcast %"class.std::__cxx11::basic_string"* %ref.tmp103 to i8*
  call void @llvm.lifetime.start.p0i8(i64 32, i8* nonnull %104) #20
  %105 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp103, i64 0, i32 2
  %106 = bitcast %"class.std::__cxx11::basic_string"* %ref.tmp103 to %union.anon**
  store %union.anon* %105, %union.anon** %106, align 8, !tbaa !39
  %107 = bitcast %union.anon* %105 to i8*
  %108 = bitcast i64* %__dnew.i.i.i.i518 to i8*
  call void @llvm.lifetime.start.p0i8(i64 8, i8* nonnull %108) #20
  store i64 22, i64* %__dnew.i.i.i.i518, align 8, !tbaa !29
  %call5.i.i.i10.i531 = invoke i8* @_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE9_M_createERmm(%"class.std::__cxx11::basic_string"* nonnull dereferenceable(32) %ref.tmp103, i64* nonnull align 8 dereferenceable(8) %__dnew.i.i.i.i518, i64 0)
          to label %call5.i.i.i10.i.noexc530 unwind label %lpad105

call5.i.i.i10.i.noexc530:                         ; preds = %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit512
  %_M_p.i13.i.i.i.i521 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp103, i64 0, i32 0, i32 0
  store i8* %call5.i.i.i10.i531, i8** %_M_p.i13.i.i.i.i521, align 8, !tbaa !41
  %109 = load i64, i64* %__dnew.i.i.i.i518, align 8, !tbaa !29
  %_M_allocated_capacity.i.i.i.i.i522 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp103, i64 0, i32 2, i32 0
  store i64 %109, i64* %_M_allocated_capacity.i.i.i.i.i522, align 8, !tbaa !43
  call void @llvm.memcpy.p0i8.p0i8.i64(i8* nonnull align 1 dereferenceable(22) %call5.i.i.i10.i531, i8* nonnull align 1 dereferenceable(22) getelementptr inbounds ([23 x i8], [23 x i8]* @.str.9, i64 0, i64 0), i64 22, i1 false) #20
  %_M_string_length.i.i.i.i.i.i528 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp103, i64 0, i32 1
  store i64 %109, i64* %_M_string_length.i.i.i.i.i.i528, align 8, !tbaa !44
  %110 = load i8*, i8** %_M_p.i13.i.i.i.i521, align 8, !tbaa !41
  %arrayidx.i.i.i.i.i529 = getelementptr inbounds i8, i8* %110, i64 %109
  store i8 0, i8* %arrayidx.i.i.i.i.i529, align 1, !tbaa !43
  call void @llvm.lifetime.end.p0i8(i64 8, i8* nonnull %108) #20
  %call109 = invoke nonnull align 8 dereferenceable(8) %"class.codegen::ParamValue"** @_ZNSt3mapINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEPN7codegen10ParamValueESt4lessIS5_ESaISt4pairIKS5_S8_EEEixEOS5_(%"class.std::map"* nonnull dereferenceable(48) %testParam, %"class.std::__cxx11::basic_string"* nonnull align 8 dereferenceable(32) %ref.tmp103)
          to label %invoke.cont108 unwind label %lpad107

invoke.cont108:                                   ; preds = %call5.i.i.i10.i.noexc530
  store %"class.codegen::ParamValue"* %p_outputCols, %"class.codegen::ParamValue"** %call109, align 8, !tbaa !46
  %111 = load i8*, i8** %_M_p.i13.i.i.i.i521, align 8, !tbaa !41
  %cmp.i.i.i535 = icmp eq i8* %111, %107
  br i1 %cmp.i.i.i535, label %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit537, label %if.then.i.i536

if.then.i.i536:                                   ; preds = %invoke.cont108
  call void @_ZdlPv(i8* %111) #20
  br label %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit537

_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit537: ; preds = %invoke.cont108, %if.then.i.i536
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %104) #20
  %112 = bitcast %"class.std::__cxx11::basic_string"* %ref.tmp114 to i8*
  call void @llvm.lifetime.start.p0i8(i64 32, i8* nonnull %112) #20
  %113 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp114, i64 0, i32 2
  %114 = bitcast %"class.std::__cxx11::basic_string"* %ref.tmp114 to %union.anon**
  store %union.anon* %113, %union.anon** %114, align 8, !tbaa !39
  %115 = bitcast %union.anon* %113 to i8*
  %116 = bitcast i64* %__dnew.i.i.i.i543 to i8*
  call void @llvm.lifetime.start.p0i8(i64 8, i8* nonnull %116) #20
  store i64 22, i64* %__dnew.i.i.i.i543, align 8, !tbaa !29
  %call5.i.i.i10.i556 = invoke i8* @_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE9_M_createERmm(%"class.std::__cxx11::basic_string"* nonnull dereferenceable(32) %ref.tmp114, i64* nonnull align 8 dereferenceable(8) %__dnew.i.i.i.i543, i64 0)
          to label %call5.i.i.i10.i.noexc555 unwind label %lpad116

call5.i.i.i10.i.noexc555:                         ; preds = %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit537
  %_M_p.i13.i.i.i.i546 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp114, i64 0, i32 0, i32 0
  store i8* %call5.i.i.i10.i556, i8** %_M_p.i13.i.i.i.i546, align 8, !tbaa !41
  %117 = load i64, i64* %__dnew.i.i.i.i543, align 8, !tbaa !29
  %_M_allocated_capacity.i.i.i.i.i547 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp114, i64 0, i32 2, i32 0
  store i64 %117, i64* %_M_allocated_capacity.i.i.i.i.i547, align 8, !tbaa !43
  call void @llvm.memcpy.p0i8.p0i8.i64(i8* nonnull align 1 dereferenceable(22) %call5.i.i.i10.i556, i8* nonnull align 1 dereferenceable(22) getelementptr inbounds ([23 x i8], [23 x i8]* @.str.10, i64 0, i64 0), i64 22, i1 false) #20
  %_M_string_length.i.i.i.i.i.i553 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp114, i64 0, i32 1
  store i64 %117, i64* %_M_string_length.i.i.i.i.i.i553, align 8, !tbaa !44
  %118 = load i8*, i8** %_M_p.i13.i.i.i.i546, align 8, !tbaa !41
  %arrayidx.i.i.i.i.i554 = getelementptr inbounds i8, i8* %118, i64 %117
  store i8 0, i8* %arrayidx.i.i.i.i.i554, align 1, !tbaa !43
  call void @llvm.lifetime.end.p0i8(i64 8, i8* nonnull %116) #20
  %call120 = invoke nonnull align 8 dereferenceable(8) %"class.codegen::ParamValue"** @_ZNSt3mapINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEPN7codegen10ParamValueESt4lessIS5_ESaISt4pairIKS5_S8_EEEixEOS5_(%"class.std::map"* nonnull dereferenceable(48) %testParam, %"class.std::__cxx11::basic_string"* nonnull align 8 dereferenceable(32) %ref.tmp114)
          to label %invoke.cont119 unwind label %lpad118

invoke.cont119:                                   ; preds = %call5.i.i.i10.i.noexc555
  store %"class.codegen::ParamValue"* %p_outputColCount, %"class.codegen::ParamValue"** %call120, align 8, !tbaa !46
  %119 = load i8*, i8** %_M_p.i13.i.i.i.i546, align 8, !tbaa !41
  %cmp.i.i.i560 = icmp eq i8* %119, %115
  br i1 %cmp.i.i.i560, label %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit562, label %if.then.i.i561

if.then.i.i561:                                   ; preds = %invoke.cont119
  call void @_ZdlPv(i8* %119) #20
  br label %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit562

_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit562: ; preds = %invoke.cont119, %if.then.i.i561
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %112) #20
  %120 = bitcast %"class.std::__cxx11::basic_string"* %ref.tmp125 to i8*
  call void @llvm.lifetime.start.p0i8(i64 32, i8* nonnull %120) #20
  %121 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp125, i64 0, i32 2
  %122 = bitcast %"class.std::__cxx11::basic_string"* %ref.tmp125 to %union.anon**
  store %union.anon* %121, %union.anon** %122, align 8, !tbaa !39
  %123 = bitcast %union.anon* %121 to i8*
  %124 = bitcast i64* %__dnew.i.i.i.i568 to i8*
  call void @llvm.lifetime.start.p0i8(i64 8, i8* nonnull %124) #20
  store i64 22, i64* %__dnew.i.i.i.i568, align 8, !tbaa !29
  %call5.i.i.i10.i581 = invoke i8* @_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE9_M_createERmm(%"class.std::__cxx11::basic_string"* nonnull dereferenceable(32) %ref.tmp125, i64* nonnull align 8 dereferenceable(8) %__dnew.i.i.i.i568, i64 0)
          to label %call5.i.i.i10.i.noexc580 unwind label %lpad127

call5.i.i.i10.i.noexc580:                         ; preds = %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit562
  %_M_p.i13.i.i.i.i571 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp125, i64 0, i32 0, i32 0
  store i8* %call5.i.i.i10.i581, i8** %_M_p.i13.i.i.i.i571, align 8, !tbaa !41
  %125 = load i64, i64* %__dnew.i.i.i.i568, align 8, !tbaa !29
  %_M_allocated_capacity.i.i.i.i.i572 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp125, i64 0, i32 2, i32 0
  store i64 %125, i64* %_M_allocated_capacity.i.i.i.i.i572, align 8, !tbaa !43
  call void @llvm.memcpy.p0i8.p0i8.i64(i8* nonnull align 1 dereferenceable(22) %call5.i.i.i10.i581, i8* nonnull align 1 dereferenceable(22) getelementptr inbounds ([23 x i8], [23 x i8]* @.str.11, i64 0, i64 0), i64 22, i1 false) #20
  %_M_string_length.i.i.i.i.i.i578 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp125, i64 0, i32 1
  store i64 %125, i64* %_M_string_length.i.i.i.i.i.i578, align 8, !tbaa !44
  %126 = load i8*, i8** %_M_p.i13.i.i.i.i571, align 8, !tbaa !41
  %arrayidx.i.i.i.i.i579 = getelementptr inbounds i8, i8* %126, i64 %125
  store i8 0, i8* %arrayidx.i.i.i.i.i579, align 1, !tbaa !43
  call void @llvm.lifetime.end.p0i8(i64 8, i8* nonnull %124) #20
  %call131 = invoke nonnull align 8 dereferenceable(8) %"class.codegen::ParamValue"** @_ZNSt3mapINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEPN7codegen10ParamValueESt4lessIS5_ESaISt4pairIKS5_S8_EEEixEOS5_(%"class.std::map"* nonnull dereferenceable(48) %testParam, %"class.std::__cxx11::basic_string"* nonnull align 8 dereferenceable(32) %ref.tmp125)
          to label %invoke.cont130 unwind label %lpad129

invoke.cont130:                                   ; preds = %call5.i.i.i10.i.noexc580
  store %"class.codegen::ParamValue"* %p_sourceTypes, %"class.codegen::ParamValue"** %call131, align 8, !tbaa !46
  %127 = load i8*, i8** %_M_p.i13.i.i.i.i571, align 8, !tbaa !41
  %cmp.i.i.i585 = icmp eq i8* %127, %123
  br i1 %cmp.i.i.i585, label %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit587, label %if.then.i.i586

if.then.i.i586:                                   ; preds = %invoke.cont130
  call void @_ZdlPv(i8* %127) #20
  br label %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit587

_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit587: ; preds = %invoke.cont130, %if.then.i.i586
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %120) #20
  %call.i588 = invoke i8* @_ZN4llvm3sys14DynamicLibrary19getPermanentLibraryEPKcPNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEE(i8* getelementptr inbounds ([45 x i8], [45 x i8]* @.str.12, i64 0, i64 0), %"class.std::__cxx11::basic_string"* null)
          to label %invoke.cont139 unwind label %lpad68

invoke.cont139:                                   ; preds = %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit587
  %128 = bitcast %"class.codegen::Hammer"* %hammer1 to i8*
  call void @llvm.lifetime.start.p0i8(i64 152, i8* nonnull %128) #20
  %_M_t.i590 = getelementptr inbounds %"class.std::map", %"class.std::map"* %agg.tmp140, i64 0, i32 0
  %129 = getelementptr inbounds %"class.std::map", %"class.std::map"* %agg.tmp140, i64 0, i32 0, i32 0, i32 0, i32 0, i32 0
  %130 = getelementptr inbounds i8, i8* %129, i64 8
  %_M_color.i.i.i.i591 = bitcast i8* %130 to i32*
  store i32 0, i32* %_M_color.i.i.i.i591, align 8, !tbaa !18
  %_M_parent.i.i.i.i.i592 = getelementptr inbounds i8, i8* %129, i64 16
  %131 = bitcast i8* %_M_parent.i.i.i.i.i592 to %"struct.std::_Rb_tree_node_base"**
  store %"struct.std::_Rb_tree_node_base"* null, %"struct.std::_Rb_tree_node_base"** %131, align 8, !tbaa !22
  %_M_left.i.i.i.i.i593 = getelementptr inbounds i8, i8* %129, i64 24
  %132 = bitcast i8* %_M_left.i.i.i.i.i593 to i8**
  store i8* %130, i8** %132, align 8, !tbaa !23
  %_M_right.i.i.i.i.i594 = getelementptr inbounds i8, i8* %129, i64 32
  %133 = bitcast i8* %_M_right.i.i.i.i.i594 to i8**
  store i8* %130, i8** %133, align 8, !tbaa !24
  %_M_node_count.i.i.i.i.i595 = getelementptr inbounds i8, i8* %129, i64 40
  %134 = bitcast i8* %_M_node_count.i.i.i.i.i595 to i64*
  store i64 0, i64* %134, align 8, !tbaa !25
  %135 = load %"struct.std::_Rb_tree_node_base"*, %"struct.std::_Rb_tree_node_base"** %2, align 8, !tbaa !22
  %cmp.not.i.i596 = icmp eq %"struct.std::_Rb_tree_node_base"* %135, null
  br i1 %cmp.not.i.i596, label %invoke.cont141, label %if.then.i.i597

if.then.i.i597:                                   ; preds = %invoke.cont139
  %136 = bitcast %"struct.std::_Rb_tree_node_base"* %135 to %"struct.std::_Rb_tree_node"*
  %137 = bitcast %"struct.std::_Rb_tree<std::__cxx11::basic_string<char>, std::pair<const std::__cxx11::basic_string<char>, codegen::ParamValue *>, std::_Select1st<std::pair<const std::__cxx11::basic_string<char>, codegen::ParamValue *>>, std::less<std::__cxx11::basic_string<char>>>::_Alloc_node"* %__an.i.i.i to i8*
  call void @llvm.lifetime.start.p0i8(i64 8, i8* nonnull %137) #20
  %_M_t.i.i.i.i = getelementptr inbounds %"struct.std::_Rb_tree<std::__cxx11::basic_string<char>, std::pair<const std::__cxx11::basic_string<char>, codegen::ParamValue *>, std::_Select1st<std::pair<const std::__cxx11::basic_string<char>, codegen::ParamValue *>>, std::less<std::__cxx11::basic_string<char>>>::_Alloc_node", %"struct.std::_Rb_tree<std::__cxx11::basic_string<char>, std::pair<const std::__cxx11::basic_string<char>, codegen::ParamValue *>, std::_Select1st<std::pair<const std::__cxx11::basic_string<char>, codegen::ParamValue *>>, std::less<std::__cxx11::basic_string<char>>>::_Alloc_node"* %__an.i.i.i, i64 0, i32 0
  store %"class.std::_Rb_tree"* %_M_t.i590, %"class.std::_Rb_tree"** %_M_t.i.i.i.i, align 8, !tbaa !46
  %_M_header.i.i.i.i.i = bitcast i8* %130 to %"struct.std::_Rb_tree_node_base"*
  %call3.i.i11.i.i598 = invoke %"struct.std::_Rb_tree_node"* @_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE7_M_copyINSH_11_Alloc_nodeEEEPSt13_Rb_tree_nodeISB_EPKSL_PSt18_Rb_tree_node_baseRT_(%"class.std::_Rb_tree"* nonnull dereferenceable(48) %_M_t.i590, %"struct.std::_Rb_tree_node"* nonnull %136, %"struct.std::_Rb_tree_node_base"* nonnull %_M_header.i.i.i.i.i, %"struct.std::_Rb_tree<std::__cxx11::basic_string<char>, std::pair<const std::__cxx11::basic_string<char>, codegen::ParamValue *>, std::_Select1st<std::pair<const std::__cxx11::basic_string<char>, codegen::ParamValue *>>, std::less<std::__cxx11::basic_string<char>>>::_Alloc_node"* nonnull align 8 dereferenceable(8) %__an.i.i.i)
          to label %call3.i.i11.i.i.noexc unwind label %lpad138

call3.i.i11.i.i.noexc:                            ; preds = %if.then.i.i597
  %138 = getelementptr %"struct.std::_Rb_tree_node", %"struct.std::_Rb_tree_node"* %call3.i.i11.i.i598, i64 0, i32 0
  br label %while.cond.i.i17.i.i.i.i

while.cond.i.i17.i.i.i.i:                         ; preds = %while.cond.i.i17.i.i.i.i, %call3.i.i11.i.i.noexc
  %__x.addr.0.i.i15.i.i.i.i = phi %"struct.std::_Rb_tree_node_base"* [ %138, %call3.i.i11.i.i.noexc ], [ %139, %while.cond.i.i17.i.i.i.i ]
  %_M_left.i.i.i.i.i.i = getelementptr inbounds %"struct.std::_Rb_tree_node_base", %"struct.std::_Rb_tree_node_base"* %__x.addr.0.i.i15.i.i.i.i, i64 0, i32 2
  %139 = load %"struct.std::_Rb_tree_node_base"*, %"struct.std::_Rb_tree_node_base"** %_M_left.i.i.i.i.i.i, align 8, !tbaa !47
  %cmp.not.i.i16.i.i.i.i = icmp eq %"struct.std::_Rb_tree_node_base"* %139, null
  br i1 %cmp.not.i.i16.i.i.i.i, label %_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE10_S_minimumEPSt18_Rb_tree_node_base.exit.i.i.i.i, label %while.cond.i.i17.i.i.i.i, !llvm.loop !48

_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE10_S_minimumEPSt18_Rb_tree_node_base.exit.i.i.i.i: ; preds = %while.cond.i.i17.i.i.i.i
  %140 = bitcast i8* %_M_left.i.i.i.i.i593 to %"struct.std::_Rb_tree_node_base"**
  store %"struct.std::_Rb_tree_node_base"* %__x.addr.0.i.i15.i.i.i.i, %"struct.std::_Rb_tree_node_base"** %140, align 8, !tbaa !46
  br label %while.cond.i.i.i.i.i.i

while.cond.i.i.i.i.i.i:                           ; preds = %while.cond.i.i.i.i.i.i, %_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE10_S_minimumEPSt18_Rb_tree_node_base.exit.i.i.i.i
  %__x.addr.0.i.i.i.i.i.i = phi %"struct.std::_Rb_tree_node_base"* [ %138, %_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE10_S_minimumEPSt18_Rb_tree_node_base.exit.i.i.i.i ], [ %141, %while.cond.i.i.i.i.i.i ]
  %_M_right.i.i.i.i.i.i = getelementptr inbounds %"struct.std::_Rb_tree_node_base", %"struct.std::_Rb_tree_node_base"* %__x.addr.0.i.i.i.i.i.i, i64 0, i32 3
  %141 = load %"struct.std::_Rb_tree_node_base"*, %"struct.std::_Rb_tree_node_base"** %_M_right.i.i.i.i.i.i, align 8, !tbaa !49
  %cmp.not.i.i.i.i.i.i = icmp eq %"struct.std::_Rb_tree_node_base"* %141, null
  br i1 %cmp.not.i.i.i.i.i.i, label %invoke.cont.i.i, label %while.cond.i.i.i.i.i.i, !llvm.loop !50

invoke.cont.i.i:                                  ; preds = %while.cond.i.i.i.i.i.i
  %142 = bitcast i8* %_M_right.i.i.i.i.i594 to %"struct.std::_Rb_tree_node_base"**
  store %"struct.std::_Rb_tree_node_base"* %__x.addr.0.i.i.i.i.i.i, %"struct.std::_Rb_tree_node_base"** %142, align 8, !tbaa !46
  %143 = load i64, i64* %5, align 8, !tbaa !25
  store i64 %143, i64* %134, align 8, !tbaa !25
  call void @llvm.lifetime.end.p0i8(i64 8, i8* nonnull %137) #20
  store %"struct.std::_Rb_tree_node_base"* %138, %"struct.std::_Rb_tree_node_base"** %131, align 8, !tbaa !46
  br label %invoke.cont141

invoke.cont141:                                   ; preds = %invoke.cont.i.i, %invoke.cont139
  invoke void @_ZN7codegen6HammerC1EN4llvm9StringRefESt3mapINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEPNS_10ParamValueESt4lessIS9_ESaISt4pairIKS9_SB_EEE(%"class.codegen::Hammer"* nonnull dereferenceable(152) %hammer1, i8* getelementptr inbounds ([38 x i8], [38 x i8]* @.str.13, i64 0, i64 0), i64 37, %"class.std::map"* nonnull %agg.tmp140)
          to label %invoke.cont143 unwind label %lpad142

invoke.cont143:                                   ; preds = %invoke.cont141
  %144 = bitcast i8* %_M_parent.i.i.i.i.i592 to %"struct.std::_Rb_tree_node"**
  %145 = load %"struct.std::_Rb_tree_node"*, %"struct.std::_Rb_tree_node"** %144, align 8, !tbaa !22
  invoke void @_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE8_M_eraseEPSt13_Rb_tree_nodeISB_E(%"class.std::_Rb_tree"* nonnull dereferenceable(48) %_M_t.i590, %"struct.std::_Rb_tree_node"* %145)
          to label %invoke.cont147 unwind label %lpad.i.i601

lpad.i.i601:                                      ; preds = %invoke.cont143
  %146 = landingpad { i8*, i32 }
          catch i8* null
  %147 = extractvalue { i8*, i32 } %146, 0
  call void @__clang_call_terminate(i8* %147) #21
  unreachable

invoke.cont147:                                   ; preds = %invoke.cont143
  %148 = bitcast %"class.codegen::Hammer"* %hammer2 to i8*
  call void @llvm.lifetime.start.p0i8(i64 152, i8* nonnull %148) #20
  %_M_t.i607 = getelementptr inbounds %"class.std::map", %"class.std::map"* %agg.tmp148, i64 0, i32 0
  %149 = getelementptr inbounds %"class.std::map", %"class.std::map"* %agg.tmp148, i64 0, i32 0, i32 0, i32 0, i32 0, i32 0
  %150 = getelementptr inbounds i8, i8* %149, i64 8
  %_M_color.i.i.i.i608 = bitcast i8* %150 to i32*
  store i32 0, i32* %_M_color.i.i.i.i608, align 8, !tbaa !18
  %_M_parent.i.i.i.i.i609 = getelementptr inbounds i8, i8* %149, i64 16
  %151 = bitcast i8* %_M_parent.i.i.i.i.i609 to %"struct.std::_Rb_tree_node_base"**
  store %"struct.std::_Rb_tree_node_base"* null, %"struct.std::_Rb_tree_node_base"** %151, align 8, !tbaa !22
  %_M_left.i.i.i.i.i610 = getelementptr inbounds i8, i8* %149, i64 24
  %152 = bitcast i8* %_M_left.i.i.i.i.i610 to i8**
  store i8* %150, i8** %152, align 8, !tbaa !23
  %_M_right.i.i.i.i.i611 = getelementptr inbounds i8, i8* %149, i64 32
  %153 = bitcast i8* %_M_right.i.i.i.i.i611 to i8**
  store i8* %150, i8** %153, align 8, !tbaa !24
  %_M_node_count.i.i.i.i.i612 = getelementptr inbounds i8, i8* %149, i64 40
  %154 = bitcast i8* %_M_node_count.i.i.i.i.i612 to i64*
  store i64 0, i64* %154, align 8, !tbaa !25
  %155 = load %"struct.std::_Rb_tree_node_base"*, %"struct.std::_Rb_tree_node_base"** %2, align 8, !tbaa !22
  %cmp.not.i.i614 = icmp eq %"struct.std::_Rb_tree_node_base"* %155, null
  br i1 %cmp.not.i.i614, label %invoke.cont149, label %if.then.i.i617

if.then.i.i617:                                   ; preds = %invoke.cont147
  %156 = bitcast %"struct.std::_Rb_tree_node_base"* %155 to %"struct.std::_Rb_tree_node"*
  %157 = bitcast %"struct.std::_Rb_tree<std::__cxx11::basic_string<char>, std::pair<const std::__cxx11::basic_string<char>, codegen::ParamValue *>, std::_Select1st<std::pair<const std::__cxx11::basic_string<char>, codegen::ParamValue *>>, std::less<std::__cxx11::basic_string<char>>>::_Alloc_node"* %__an.i.i.i606 to i8*
  call void @llvm.lifetime.start.p0i8(i64 8, i8* nonnull %157) #20
  %_M_t.i.i.i.i615 = getelementptr inbounds %"struct.std::_Rb_tree<std::__cxx11::basic_string<char>, std::pair<const std::__cxx11::basic_string<char>, codegen::ParamValue *>, std::_Select1st<std::pair<const std::__cxx11::basic_string<char>, codegen::ParamValue *>>, std::less<std::__cxx11::basic_string<char>>>::_Alloc_node", %"struct.std::_Rb_tree<std::__cxx11::basic_string<char>, std::pair<const std::__cxx11::basic_string<char>, codegen::ParamValue *>, std::_Select1st<std::pair<const std::__cxx11::basic_string<char>, codegen::ParamValue *>>, std::less<std::__cxx11::basic_string<char>>>::_Alloc_node"* %__an.i.i.i606, i64 0, i32 0
  store %"class.std::_Rb_tree"* %_M_t.i607, %"class.std::_Rb_tree"** %_M_t.i.i.i.i615, align 8, !tbaa !46
  %_M_header.i.i.i.i.i616 = bitcast i8* %150 to %"struct.std::_Rb_tree_node_base"*
  %call3.i.i11.i.i630 = invoke %"struct.std::_Rb_tree_node"* @_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE7_M_copyINSH_11_Alloc_nodeEEEPSt13_Rb_tree_nodeISB_EPKSL_PSt18_Rb_tree_node_baseRT_(%"class.std::_Rb_tree"* nonnull dereferenceable(48) %_M_t.i607, %"struct.std::_Rb_tree_node"* nonnull %156, %"struct.std::_Rb_tree_node_base"* nonnull %_M_header.i.i.i.i.i616, %"struct.std::_Rb_tree<std::__cxx11::basic_string<char>, std::pair<const std::__cxx11::basic_string<char>, codegen::ParamValue *>, std::_Select1st<std::pair<const std::__cxx11::basic_string<char>, codegen::ParamValue *>>, std::less<std::__cxx11::basic_string<char>>>::_Alloc_node"* nonnull align 8 dereferenceable(8) %__an.i.i.i606)
          to label %call3.i.i11.i.i.noexc629 unwind label %lpad146

call3.i.i11.i.i.noexc629:                         ; preds = %if.then.i.i617
  %158 = getelementptr %"struct.std::_Rb_tree_node", %"struct.std::_Rb_tree_node"* %call3.i.i11.i.i630, i64 0, i32 0
  br label %while.cond.i.i17.i.i.i.i621

while.cond.i.i17.i.i.i.i621:                      ; preds = %while.cond.i.i17.i.i.i.i621, %call3.i.i11.i.i.noexc629
  %__x.addr.0.i.i15.i.i.i.i618 = phi %"struct.std::_Rb_tree_node_base"* [ %158, %call3.i.i11.i.i.noexc629 ], [ %159, %while.cond.i.i17.i.i.i.i621 ]
  %_M_left.i.i.i.i.i.i619 = getelementptr inbounds %"struct.std::_Rb_tree_node_base", %"struct.std::_Rb_tree_node_base"* %__x.addr.0.i.i15.i.i.i.i618, i64 0, i32 2
  %159 = load %"struct.std::_Rb_tree_node_base"*, %"struct.std::_Rb_tree_node_base"** %_M_left.i.i.i.i.i.i619, align 8, !tbaa !47
  %cmp.not.i.i16.i.i.i.i620 = icmp eq %"struct.std::_Rb_tree_node_base"* %159, null
  br i1 %cmp.not.i.i16.i.i.i.i620, label %_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE10_S_minimumEPSt18_Rb_tree_node_base.exit.i.i.i.i622, label %while.cond.i.i17.i.i.i.i621, !llvm.loop !48

_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE10_S_minimumEPSt18_Rb_tree_node_base.exit.i.i.i.i622: ; preds = %while.cond.i.i17.i.i.i.i621
  %160 = bitcast i8* %_M_left.i.i.i.i.i610 to %"struct.std::_Rb_tree_node_base"**
  store %"struct.std::_Rb_tree_node_base"* %__x.addr.0.i.i15.i.i.i.i618, %"struct.std::_Rb_tree_node_base"** %160, align 8, !tbaa !46
  br label %while.cond.i.i.i.i.i.i626

while.cond.i.i.i.i.i.i626:                        ; preds = %while.cond.i.i.i.i.i.i626, %_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE10_S_minimumEPSt18_Rb_tree_node_base.exit.i.i.i.i622
  %__x.addr.0.i.i.i.i.i.i623 = phi %"struct.std::_Rb_tree_node_base"* [ %158, %_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE10_S_minimumEPSt18_Rb_tree_node_base.exit.i.i.i.i622 ], [ %161, %while.cond.i.i.i.i.i.i626 ]
  %_M_right.i.i.i.i.i.i624 = getelementptr inbounds %"struct.std::_Rb_tree_node_base", %"struct.std::_Rb_tree_node_base"* %__x.addr.0.i.i.i.i.i.i623, i64 0, i32 3
  %161 = load %"struct.std::_Rb_tree_node_base"*, %"struct.std::_Rb_tree_node_base"** %_M_right.i.i.i.i.i.i624, align 8, !tbaa !49
  %cmp.not.i.i.i.i.i.i625 = icmp eq %"struct.std::_Rb_tree_node_base"* %161, null
  br i1 %cmp.not.i.i.i.i.i.i625, label %invoke.cont.i.i628, label %while.cond.i.i.i.i.i.i626, !llvm.loop !50

invoke.cont.i.i628:                               ; preds = %while.cond.i.i.i.i.i.i626
  %162 = bitcast i8* %_M_right.i.i.i.i.i611 to %"struct.std::_Rb_tree_node_base"**
  store %"struct.std::_Rb_tree_node_base"* %__x.addr.0.i.i.i.i.i.i623, %"struct.std::_Rb_tree_node_base"** %162, align 8, !tbaa !46
  %163 = load i64, i64* %5, align 8, !tbaa !25
  store i64 %163, i64* %154, align 8, !tbaa !25
  call void @llvm.lifetime.end.p0i8(i64 8, i8* nonnull %157) #20
  store %"struct.std::_Rb_tree_node_base"* %158, %"struct.std::_Rb_tree_node_base"** %151, align 8, !tbaa !46
  br label %invoke.cont149

invoke.cont149:                                   ; preds = %invoke.cont.i.i628, %invoke.cont147
  invoke void @_ZN7codegen6HammerC1EN4llvm9StringRefESt3mapINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEPNS_10ParamValueESt4lessIS9_ESaISt4pairIKS9_SB_EEE(%"class.codegen::Hammer"* nonnull dereferenceable(152) %hammer2, i8* getelementptr inbounds ([45 x i8], [45 x i8]* @.str.14, i64 0, i64 0), i64 44, %"class.std::map"* nonnull %agg.tmp148)
          to label %invoke.cont151 unwind label %lpad150

invoke.cont151:                                   ; preds = %invoke.cont149
  %164 = bitcast i8* %_M_parent.i.i.i.i.i609 to %"struct.std::_Rb_tree_node"**
  %165 = load %"struct.std::_Rb_tree_node"*, %"struct.std::_Rb_tree_node"** %164, align 8, !tbaa !22
  invoke void @_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE8_M_eraseEPSt13_Rb_tree_nodeISB_E(%"class.std::_Rb_tree"* nonnull dereferenceable(48) %_M_t.i607, %"struct.std::_Rb_tree_node"* %165)
          to label %_ZNSt3mapINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEPN7codegen10ParamValueESt4lessIS5_ESaISt4pairIKS5_S8_EEED2Ev.exit635 unwind label %lpad.i.i634

lpad.i.i634:                                      ; preds = %invoke.cont151
  %166 = landingpad { i8*, i32 }
          catch i8* null
  %167 = extractvalue { i8*, i32 } %166, 0
  call void @__clang_call_terminate(i8* %167) #21
  unreachable

_ZNSt3mapINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEPN7codegen10ParamValueESt4lessIS5_ESaISt4pairIKS5_S8_EEED2Ev.exit635: ; preds = %invoke.cont151
  %call155 = invoke zeroext i1 @_ZN7codegen6Hammer6hardenEv(%"class.codegen::Hammer"* nonnull dereferenceable(152) %hammer1)
          to label %invoke.cont154 unwind label %lpad153

invoke.cont154:                                   ; preds = %_ZNSt3mapINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEPN7codegen10ParamValueESt4lessIS5_ESaISt4pairIKS5_S8_EEED2Ev.exit635
  %call2.i.i.i.i.i.i636 = invoke noalias nonnull i8* @_Znwm(i64 24) #22
          to label %invoke.cont158 unwind label %lpad157

invoke.cont158:                                   ; preds = %invoke.cont154
  %_M_storage.i.i4.i.i = getelementptr inbounds i8, i8* %call2.i.i.i.i.i.i636, i64 16
  %168 = bitcast i8* %_M_storage.i.i4.i.i to %"class.codegen::Hammer"**
  store %"class.codegen::Hammer"* %hammer2, %"class.codegen::Hammer"** %168, align 8, !tbaa !46
  %169 = bitcast i8* %call2.i.i.i.i.i.i636 to %"struct.std::__detail::_List_node_base"*
  call void @_ZNSt8__detail15_List_node_base7_M_hookEPS0_(%"struct.std::__detail::_List_node_base"* nonnull dereferenceable(16) %169, %"struct.std::__detail::_List_node_base"* nonnull %7) #20
  %170 = load i64, i64* %8, align 8, !tbaa !29
  %add.i.i.i = add i64 %170, 1
  store i64 %add.i.i.i, i64* %8, align 8, !tbaa !29
  %171 = bitcast %"class.codegen::HammerConfig"* %hammerConfig to i8*
  call void @llvm.lifetime.start.p0i8(i64 1616, i8* nonnull %171) #20
  %172 = getelementptr inbounds [32 x i8], [32 x i8]* %ALL.i, i64 0, i64 0
  call void @llvm.lifetime.start.p0i8(i64 32, i8* nonnull %172) #20
  call void @llvm.memcpy.p0i8.p0i8.i64(i8* nonnull align 16 dereferenceable(32) %172, i8* nonnull align 16 dereferenceable(32) getelementptr inbounds (<{ i8, [31 x i8] }>, <{ i8, [31 x i8] }>* @__const.HammerConfig.ALL, i64 0, i32 0), i64 32, i1 false)
  %func_conf.i = getelementptr inbounds %"class.codegen::HammerConfig", %"class.codegen::HammerConfig"* %hammerConfig, i64 0, i32 0
  store i8* %172, i8** %func_conf.i, align 8, !tbaa !51
  %module_conf.i = getelementptr inbounds %"class.codegen::HammerConfig", %"class.codegen::HammerConfig"* %hammerConfig, i64 0, i32 1
  store i8* %172, i8** %module_conf.i, align 8, !tbaa !53
  invoke void @_ZN7codegen12HammerConfig14init_func_passEv(%"class.codegen::HammerConfig"* nonnull dereferenceable(1616) %hammerConfig)
          to label %.noexc unwind label %lpad160

.noexc:                                           ; preds = %invoke.cont158
  invoke void @_ZN7codegen12HammerConfig16init_module_passEv(%"class.codegen::HammerConfig"* nonnull dereferenceable(1616) %hammerConfig)
          to label %invoke.cont161 unwind label %lpad160

invoke.cont161:                                   ; preds = %.noexc
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %172) #20
  %173 = bitcast %"class.std::unique_ptr.131"* %jitter to i8*
  call void @llvm.lifetime.start.p0i8(i64 8, i8* nonnull %173) #20
  %174 = getelementptr inbounds %"class.std::__cxx11::list", %"class.std::__cxx11::list"* %agg.tmp162, i64 0, i32 0, i32 0, i32 0, i32 0
  %_M_next.i.i14.i = getelementptr inbounds %"class.std::__cxx11::list", %"class.std::__cxx11::list"* %agg.tmp162, i64 0, i32 0, i32 0, i32 0, i32 0, i32 0
  store %"struct.std::__detail::_List_node_base"* %174, %"struct.std::__detail::_List_node_base"** %_M_next.i.i14.i, align 8, !tbaa !26
  %_M_prev.i.i.i638 = getelementptr inbounds %"class.std::__cxx11::list", %"class.std::__cxx11::list"* %agg.tmp162, i64 0, i32 0, i32 0, i32 0, i32 0, i32 1
  store %"struct.std::__detail::_List_node_base"* %174, %"struct.std::__detail::_List_node_base"** %_M_prev.i.i.i638, align 8, !tbaa !28
  %_M_storage.i.i.i.i.i639 = getelementptr inbounds %"class.std::__cxx11::list", %"class.std::__cxx11::list"* %agg.tmp162, i64 0, i32 0, i32 0, i32 0, i32 1
  %175 = bitcast %"struct.__gnu_cxx::__aligned_membuf"* %_M_storage.i.i.i.i.i639 to i64*
  store i64 0, i64* %175, align 8, !tbaa !29
  %176 = load %"struct.std::__detail::_List_node_base"*, %"struct.std::__detail::_List_node_base"** %_M_next.i.i.i, align 8, !tbaa !26
  %cmp.i.not8.i.i = icmp eq %"struct.std::__detail::_List_node_base"* %176, %7
  br i1 %cmp.i.not8.i.i, label %invoke.cont164, label %for.body.i.i

for.body.i.i:                                     ; preds = %invoke.cont161, %call2.i.i.i.i.i.i.i.noexc.i
  %__first.sroa.0.09.i.i = phi %"struct.std::__detail::_List_node_base"* [ %182, %call2.i.i.i.i.i.i.i.noexc.i ], [ %176, %invoke.cont161 ]
  %call2.i.i.i.i.i.i.i13.i = invoke noalias nonnull i8* @_Znwm(i64 24) #22
          to label %call2.i.i.i.i.i.i.i.noexc.i unwind label %lpad.i

call2.i.i.i.i.i.i.i.noexc.i:                      ; preds = %for.body.i.i
  %_M_storage.i.i.i.i641 = getelementptr inbounds %"struct.std::__detail::_List_node_base", %"struct.std::__detail::_List_node_base"* %__first.sroa.0.09.i.i, i64 1
  %177 = bitcast %"struct.std::__detail::_List_node_base"* %_M_storage.i.i.i.i641 to %"class.codegen::Hammer"**
  %_M_storage.i.i4.i.i.i.i = getelementptr inbounds i8, i8* %call2.i.i.i.i.i.i.i13.i, i64 16
  %178 = bitcast i8* %_M_storage.i.i4.i.i.i.i to %"class.codegen::Hammer"**
  %179 = load %"class.codegen::Hammer"*, %"class.codegen::Hammer"** %177, align 8, !tbaa !46
  store %"class.codegen::Hammer"* %179, %"class.codegen::Hammer"** %178, align 8, !tbaa !46
  %180 = bitcast i8* %call2.i.i.i.i.i.i.i13.i to %"struct.std::__detail::_List_node_base"*
  call void @_ZNSt8__detail15_List_node_base7_M_hookEPS0_(%"struct.std::__detail::_List_node_base"* nonnull dereferenceable(16) %180, %"struct.std::__detail::_List_node_base"* nonnull %174) #20
  %181 = load i64, i64* %175, align 8, !tbaa !29
  %add.i.i.i.i.i = add i64 %181, 1
  store i64 %add.i.i.i.i.i, i64* %175, align 8, !tbaa !29
  %_M_next.i.i12.i = getelementptr inbounds %"struct.std::__detail::_List_node_base", %"struct.std::__detail::_List_node_base"* %__first.sroa.0.09.i.i, i64 0, i32 0
  %182 = load %"struct.std::__detail::_List_node_base"*, %"struct.std::__detail::_List_node_base"** %_M_next.i.i12.i, align 8, !tbaa !26
  %cmp.i.not.i.i = icmp eq %"struct.std::__detail::_List_node_base"* %182, %7
  br i1 %cmp.i.not.i.i, label %invoke.cont164, label %for.body.i.i, !llvm.loop !54

lpad.i:                                           ; preds = %for.body.i.i
  %183 = landingpad { i8*, i32 }
          cleanup
  %184 = load %"struct.std::__detail::_List_node_base"*, %"struct.std::__detail::_List_node_base"** %_M_next.i.i14.i, align 8, !tbaa !26
  %cmp.not16.i.i.i = icmp eq %"struct.std::__detail::_List_node_base"* %184, %174
  br i1 %cmp.not16.i.i.i, label %ehcleanup221, label %invoke.cont6.i.i.i

invoke.cont6.i.i.i:                               ; preds = %lpad.i, %invoke.cont6.i.i.i
  %__cur.017.i.i.i = phi %"struct.std::__detail::_List_node_base"* [ %185, %invoke.cont6.i.i.i ], [ %184, %lpad.i ]
  %_M_next4.i.i.i = getelementptr inbounds %"struct.std::__detail::_List_node_base", %"struct.std::__detail::_List_node_base"* %__cur.017.i.i.i, i64 0, i32 0
  %185 = load %"struct.std::__detail::_List_node_base"*, %"struct.std::__detail::_List_node_base"** %_M_next4.i.i.i, align 8, !tbaa !26
  %186 = bitcast %"struct.std::__detail::_List_node_base"* %__cur.017.i.i.i to i8*
  call void @_ZdlPv(i8* %186) #20
  %cmp.not.i.i.i = icmp eq %"struct.std::__detail::_List_node_base"* %185, %174
  br i1 %cmp.not.i.i.i, label %ehcleanup221, label %invoke.cont6.i.i.i, !llvm.loop !55

invoke.cont164:                                   ; preds = %call2.i.i.i.i.i.i.i.noexc.i, %invoke.cont161
  invoke void @_ZN7codegen6Hammer13create_jitterENSt7__cxx114listIPS0_SaIS3_EEERNS_12HammerConfigE(%"class.std::unique_ptr.131"* nonnull sret(%"class.std::unique_ptr.131") align 8 %jitter, %"class.codegen::Hammer"* nonnull dereferenceable(152) %hammer1, %"class.std::__cxx11::list"* nonnull %agg.tmp162, %"class.codegen::HammerConfig"* nonnull align 8 dereferenceable(1616) %hammerConfig)
          to label %invoke.cont166 unwind label %lpad165

invoke.cont166:                                   ; preds = %invoke.cont164
  %187 = load %"struct.std::__detail::_List_node_base"*, %"struct.std::__detail::_List_node_base"** %_M_next.i.i14.i, align 8, !tbaa !26
  %cmp.not16.i.i643 = icmp eq %"struct.std::__detail::_List_node_base"* %187, %174
  br i1 %cmp.not16.i.i643, label %invoke.cont172, label %invoke.cont6.i.i647

invoke.cont6.i.i647:                              ; preds = %invoke.cont166, %invoke.cont6.i.i647
  %__cur.017.i.i644 = phi %"struct.std::__detail::_List_node_base"* [ %188, %invoke.cont6.i.i647 ], [ %187, %invoke.cont166 ]
  %_M_next4.i.i645 = getelementptr inbounds %"struct.std::__detail::_List_node_base", %"struct.std::__detail::_List_node_base"* %__cur.017.i.i644, i64 0, i32 0
  %188 = load %"struct.std::__detail::_List_node_base"*, %"struct.std::__detail::_List_node_base"** %_M_next4.i.i645, align 8, !tbaa !26
  %189 = bitcast %"struct.std::__detail::_List_node_base"* %__cur.017.i.i644 to i8*
  call void @_ZdlPv(i8* %189) #20
  %cmp.not.i.i646 = icmp eq %"struct.std::__detail::_List_node_base"* %188, %174
  br i1 %cmp.not.i.i646, label %invoke.cont172, label %invoke.cont6.i.i647, !llvm.loop !55

invoke.cont172:                                   ; preds = %invoke.cont6.i.i647, %invoke.cont166
  %190 = getelementptr inbounds %"class.llvm::Expected", %"class.llvm::Expected"* %ref.tmp168, i64 0, i32 0, i32 0, i32 0, i64 0
  call void @llvm.lifetime.start.p0i8(i64 24, i8* nonnull %190) #20
  %_M_head_impl.i.i.i.i.i.i.i649 = getelementptr inbounds %"class.std::unique_ptr.131", %"class.std::unique_ptr.131"* %jitter, i64 0, i32 0, i32 0, i32 0, i32 0, i32 0
  %191 = load %"class.llvm::orc::LLJIT"*, %"class.llvm::orc::LLJIT"** %_M_head_impl.i.i.i.i.i.i.i649, align 8, !tbaa !46
  %Main.i = getelementptr inbounds %"class.llvm::orc::LLJIT", %"class.llvm::orc::LLJIT"* %191, i64 0, i32 2
  %192 = load %"class.llvm::orc::JITDylib"*, %"class.llvm::orc::JITDylib"** %Main.i, align 8, !tbaa !56, !noalias !94
  invoke void @_ZN4llvm3orc5LLJIT6lookupERNS0_8JITDylibENS_9StringRefE(%"class.llvm::Expected"* nonnull sret(%"class.llvm::Expected") align 8 %ref.tmp168, %"class.llvm::orc::LLJIT"* nonnull dereferenceable(568) %191, %"class.llvm::orc::JITDylib"* nonnull align 8 dereferenceable(272) %192, i8* getelementptr inbounds ([25 x i8], [25 x i8]* @.str.15, i64 0, i64 0), i64 24)
          to label %invoke.cont173 unwind label %lpad171

invoke.cont173:                                   ; preds = %invoke.cont172
  %HasError.i.i = getelementptr inbounds %"class.llvm::Expected", %"class.llvm::Expected"* %ref.tmp168, i64 0, i32 1
  %bf.load.i.i = load i8, i8* %HasError.i.i, align 8
  %193 = and i8 %bf.load.i.i, 1
  %bf.cast.not.i.i = icmp eq i8 %193, 0
  br i1 %bf.cast.not.i.i, label %invoke.cont185, label %cond.false.i.i

cond.false.i.i:                                   ; preds = %invoke.cont173
  call void @__assert_fail(i8* getelementptr inbounds ([54 x i8], [54 x i8]* @.str.26, i64 0, i64 0), i8* getelementptr inbounds ([42 x i8], [42 x i8]* @.str.27, i64 0, i64 0), i32 631, i8* getelementptr inbounds ([116 x i8], [116 x i8]* @__PRETTY_FUNCTION__._ZN4llvm8ExpectedINS_18JITEvaluatedSymbolEE10getStorageEv, i64 0, i64 0)) #21
  unreachable

invoke.cont185:                                   ; preds = %invoke.cont173
  %Address.i = bitcast %"class.llvm::Expected"* %ref.tmp168 to i64*
  %194 = load i64, i64* %Address.i, align 8, !tbaa !97
  %195 = inttoptr i64 %194 to void (i64, i32*, i32*, i32*, i32*, i32, i32, i32)*
  call void @llvm.lifetime.end.p0i8(i64 24, i8* nonnull %190) #20
  %196 = getelementptr inbounds %"class.llvm::Expected", %"class.llvm::Expected"* %ref.tmp181, i64 0, i32 0, i32 0, i32 0, i64 0
  call void @llvm.lifetime.start.p0i8(i64 24, i8* nonnull %196) #20
  %197 = load %"class.llvm::orc::LLJIT"*, %"class.llvm::orc::LLJIT"** %_M_head_impl.i.i.i.i.i.i.i649, align 8, !tbaa !46
  %Main.i669 = getelementptr inbounds %"class.llvm::orc::LLJIT", %"class.llvm::orc::LLJIT"* %197, i64 0, i32 2
  %198 = load %"class.llvm::orc::JITDylib"*, %"class.llvm::orc::JITDylib"** %Main.i669, align 8, !tbaa !56, !noalias !101
  invoke void @_ZN4llvm3orc5LLJIT6lookupERNS0_8JITDylibENS_9StringRefE(%"class.llvm::Expected"* nonnull sret(%"class.llvm::Expected") align 8 %ref.tmp181, %"class.llvm::orc::LLJIT"* nonnull dereferenceable(568) %197, %"class.llvm::orc::JITDylib"* nonnull align 8 dereferenceable(272) %198, i8* getelementptr inbounds ([24 x i8], [24 x i8]* @.str.16, i64 0, i64 0), i64 23)
          to label %invoke.cont186 unwind label %lpad184

invoke.cont186:                                   ; preds = %invoke.cont185
  %HasError.i.i672 = getelementptr inbounds %"class.llvm::Expected", %"class.llvm::Expected"* %ref.tmp181, i64 0, i32 1
  %bf.load.i.i673 = load i8, i8* %HasError.i.i672, align 8
  %199 = and i8 %bf.load.i.i673, 1
  %bf.cast.not.i.i674 = icmp eq i8 %199, 0
  br i1 %bf.cast.not.i.i674, label %invoke.cont198, label %cond.false.i.i675

cond.false.i.i675:                                ; preds = %invoke.cont186
  call void @__assert_fail(i8* getelementptr inbounds ([54 x i8], [54 x i8]* @.str.26, i64 0, i64 0), i8* getelementptr inbounds ([42 x i8], [42 x i8]* @.str.27, i64 0, i64 0), i32 631, i8* getelementptr inbounds ([116 x i8], [116 x i8]* @__PRETTY_FUNCTION__._ZN4llvm8ExpectedINS_18JITEvaluatedSymbolEE10getStorageEv, i64 0, i64 0)) #21
  unreachable

invoke.cont198:                                   ; preds = %invoke.cont186
  %Address.i677 = bitcast %"class.llvm::Expected"* %ref.tmp181 to i64*
  %200 = load i64, i64* %Address.i677, align 8, !tbaa !97
  %201 = inttoptr i64 %200 to void (i64, i32*, i32*, i32, i32)*
  call void @llvm.lifetime.end.p0i8(i64 24, i8* nonnull %196) #20
  %202 = getelementptr inbounds %"class.llvm::Expected", %"class.llvm::Expected"* %ref.tmp194, i64 0, i32 0, i32 0, i32 0, i64 0
  call void @llvm.lifetime.start.p0i8(i64 24, i8* nonnull %202) #20
  %203 = load %"class.llvm::orc::LLJIT"*, %"class.llvm::orc::LLJIT"** %_M_head_impl.i.i.i.i.i.i.i649, align 8, !tbaa !46
  %Main.i693 = getelementptr inbounds %"class.llvm::orc::LLJIT", %"class.llvm::orc::LLJIT"* %203, i64 0, i32 2
  %204 = load %"class.llvm::orc::JITDylib"*, %"class.llvm::orc::JITDylib"** %Main.i693, align 8, !tbaa !56, !noalias !104
  invoke void @_ZN4llvm3orc5LLJIT6lookupERNS0_8JITDylibENS_9StringRefE(%"class.llvm::Expected"* nonnull sret(%"class.llvm::Expected") align 8 %ref.tmp194, %"class.llvm::orc::LLJIT"* nonnull dereferenceable(568) %203, %"class.llvm::orc::JITDylib"* nonnull align 8 dereferenceable(272) %204, i8* getelementptr inbounds ([21 x i8], [21 x i8]* @.str.17, i64 0, i64 0), i64 20)
          to label %invoke.cont199 unwind label %lpad197

invoke.cont199:                                   ; preds = %invoke.cont198
  %HasError.i.i696 = getelementptr inbounds %"class.llvm::Expected", %"class.llvm::Expected"* %ref.tmp194, i64 0, i32 1
  %bf.load.i.i697 = load i8, i8* %HasError.i.i696, align 8
  %205 = and i8 %bf.load.i.i697, 1
  %bf.cast.not.i.i698 = icmp eq i8 %205, 0
  br i1 %bf.cast.not.i.i698, label %_ZN4llvm8ExpectedINS_18JITEvaluatedSymbolEED2Ev.exit712, label %cond.false.i.i699

cond.false.i.i699:                                ; preds = %invoke.cont199
  call void @__assert_fail(i8* getelementptr inbounds ([54 x i8], [54 x i8]* @.str.26, i64 0, i64 0), i8* getelementptr inbounds ([42 x i8], [42 x i8]* @.str.27, i64 0, i64 0), i32 631, i8* getelementptr inbounds ([116 x i8], [116 x i8]* @__PRETTY_FUNCTION__._ZN4llvm8ExpectedINS_18JITEvaluatedSymbolEE10getStorageEv, i64 0, i64 0)) #21
  unreachable

_ZN4llvm8ExpectedINS_18JITEvaluatedSymbolEED2Ev.exit712: ; preds = %invoke.cont199
  %Address.i701 = bitcast %"class.llvm::Expected"* %ref.tmp194 to i64*
  %206 = load i64, i64* %Address.i701, align 8, !tbaa !97
  call void @llvm.lifetime.end.p0i8(i64 24, i8* nonnull %202) #20
  %call209 = invoke noalias nonnull dereferenceable(32) i8* @_Znwm(i64 32) #23
          to label %invoke.cont208 unwind label %lpad207

invoke.cont208:                                   ; preds = %_ZN4llvm8ExpectedINS_18JITEvaluatedSymbolEED2Ev.exit712
  %207 = inttoptr i64 %206 to void (i64, i32*, i32, i64, i32*, i32)*
  %sortFunc210 = getelementptr inbounds i8, i8* %call209, i64 8
  %208 = bitcast i8* %sortFunc210 to void (i64, i32*, i32*, i32*, i32*, i32, i32, i32)**
  store void (i64, i32*, i32*, i32*, i32*, i32, i32, i32)* %195, void (i64, i32*, i32*, i32*, i32*, i32, i32, i32)** %208, align 8, !tbaa !107
  %allocColumnsFunc211 = getelementptr inbounds i8, i8* %call209, i64 16
  %209 = bitcast i8* %allocColumnsFunc211 to void (i64, i32*, i32*, i32, i32)**
  store void (i64, i32*, i32*, i32, i32)* %201, void (i64, i32*, i32*, i32, i32)** %209, align 8, !tbaa !109
  %getResultFunc212 = getelementptr inbounds i8, i8* %call209, i64 24
  %210 = bitcast i8* %getResultFunc212 to void (i64, i32*, i32, i64, i32*, i32)**
  store void (i64, i32*, i32, i64, i32*, i32)* %207, void (i64, i32*, i32, i64, i32*, i32)** %210, align 8, !tbaa !110
  %211 = load %"class.llvm::orc::LLJIT"*, %"class.llvm::orc::LLJIT"** %_M_head_impl.i.i.i.i.i.i.i649, align 8, !tbaa !46
  store %"class.llvm::orc::LLJIT"* null, %"class.llvm::orc::LLJIT"** %_M_head_impl.i.i.i.i.i.i.i649, align 8, !tbaa !46
  %jitter214 = bitcast i8* %call209 to %"class.llvm::orc::LLJIT"**
  store %"class.llvm::orc::LLJIT"* %211, %"class.llvm::orc::LLJIT"** %jitter214, align 8, !tbaa !111
  br i1 icmp ne (i8* bitcast (i32 (i32*, void (i8*)*)* @__pthread_key_create to i8*), i8* null), label %_ZL20__gthread_mutex_lockP15pthread_mutex_t.exit.i.i, label %_ZNSt5mutex6unlockEv.exit.critedge.i

_ZL20__gthread_mutex_lockP15pthread_mutex_t.exit.i.i: ; preds = %invoke.cont208
  %call1.i.i.i = call i32 @pthread_mutex_lock(%union.pthread_mutex_t* nonnull getelementptr inbounds (%class.OpTemplateCache, %class.OpTemplateCache* @g_jitSortContexts, i64 0, i32 0, i32 0, i32 0)) #20
  %tobool.not.i.i = icmp eq i32 %call1.i.i.i, 0
  br i1 %tobool.not.i.i, label %_ZNSt5mutex4lockEv.exit.i, label %if.then.i.i719

if.then.i.i719:                                   ; preds = %_ZL20__gthread_mutex_lockP15pthread_mutex_t.exit.i.i
  invoke void @_ZSt20__throw_system_errori(i32 %call1.i.i.i) #24
          to label %.noexc720 unwind label %lpad207

.noexc720:                                        ; preds = %if.then.i.i719
  unreachable

_ZNSt5mutex4lockEv.exit.i:                        ; preds = %_ZL20__gthread_mutex_lockP15pthread_mutex_t.exit.i.i
  %call2.i.i.i.i.i.i795 = invoke noalias nonnull i8* @_Znwm(i64 24) #22
          to label %call2.i.i.i.i.i.i.noexc794 unwind label %lpad207

call2.i.i.i.i.i.i.noexc794:                       ; preds = %_ZNSt5mutex4lockEv.exit.i
  %212 = bitcast i8* %call2.i.i.i.i.i.i795 to %"struct.std::__detail::_Hash_node"*
  %_M_nxt.i.i.i.i.i.i.i762 = bitcast i8* %call2.i.i.i.i.i.i795 to %"struct.std::__detail::_Hash_node_base"**
  store %"struct.std::__detail::_Hash_node_base"* null, %"struct.std::__detail::_Hash_node_base"** %_M_nxt.i.i.i.i.i.i.i762, align 8, !tbaa !11
  %_M_storage.i.i.i.i.i763 = getelementptr inbounds i8, i8* %call2.i.i.i.i.i.i795, i64 8
  %first.i.i.i.i.i.i.i764 = bitcast i8* %_M_storage.i.i.i.i.i763 to i64*
  store i64 %stageId, i64* %first.i.i.i.i.i.i.i764, align 8, !tbaa !112
  %second.i.i.i.i.i.i.i766 = getelementptr inbounds i8, i8* %call2.i.i.i.i.i.i795, i64 16
  %213 = bitcast i8* %second.i.i.i.i.i.i.i766 to i8**
  store i8* %call209, i8** %213, align 8, !tbaa !114
  %214 = load i64, i64* getelementptr inbounds (%class.OpTemplateCache, %class.OpTemplateCache* @g_jitSortContexts, i64 0, i32 1, i32 0, i32 1), align 8, !tbaa !15
  %rem.i.i.i.i.i.i768 = urem i64 %stageId, %214
  %215 = load %"struct.std::__detail::_Hash_node_base"**, %"struct.std::__detail::_Hash_node_base"*** getelementptr inbounds (%class.OpTemplateCache, %class.OpTemplateCache* @g_jitSortContexts, i64 0, i32 1, i32 0, i32 0), align 8, !tbaa !14
  %arrayidx.i.i.i.i.i769 = getelementptr inbounds %"struct.std::__detail::_Hash_node_base"*, %"struct.std::__detail::_Hash_node_base"** %215, i64 %rem.i.i.i.i.i.i768
  %216 = load %"struct.std::__detail::_Hash_node_base"*, %"struct.std::__detail::_Hash_node_base"** %arrayidx.i.i.i.i.i769, align 8, !tbaa !46
  %tobool.not.i.i.i.i.i770 = icmp eq %"struct.std::__detail::_Hash_node_base"* %216, null
  br i1 %tobool.not.i.i.i.i.i770, label %cleanup.cont.i.i.i789, label %if.end.i.i.i.i.i774

if.end.i.i.i.i.i774:                              ; preds = %call2.i.i.i.i.i.i.noexc794
  %217 = bitcast %"struct.std::__detail::_Hash_node_base"* %216 to %"struct.std::__detail::_Hash_node"**
  %218 = load %"struct.std::__detail::_Hash_node"*, %"struct.std::__detail::_Hash_node"** %217, align 8, !tbaa !11
  %_M_storage.i.i.i.i23.i.i.i.i.i771 = getelementptr inbounds %"struct.std::__detail::_Hash_node", %"struct.std::__detail::_Hash_node"* %218, i64 0, i32 0, i32 1
  %first.i.i.i.i.i24.i.i.i.i.i772 = bitcast %"struct.__gnu_cxx::__aligned_buffer"* %_M_storage.i.i.i.i23.i.i.i.i.i771 to i64*
  %219 = load i64, i64* %first.i.i.i.i.i24.i.i.i.i.i772, align 8, !tbaa !29
  %cmp.i.i.i25.i.i.i.i.i773 = icmp eq i64 %219, %stageId
  br i1 %cmp.i.i.i25.i.i.i.i.i773, label %cleanup.i.i.i788, label %if.end3.i.i.i.i.i780

for.cond.i.i.i.i.i776:                            ; preds = %lor.lhs.false.i.i.i.i.i785
  %cmp.i.i.i.i.i.i.i.i775 = icmp eq i64 %222, %stageId
  br i1 %cmp.i.i.i.i.i.i.i.i775, label %cleanup.i.i.i788, label %if.end3.i.i.i.i.i780

if.end3.i.i.i.i.i780:                             ; preds = %if.end.i.i.i.i.i774, %for.cond.i.i.i.i.i776
  %__p.026.i.i.i.i.i777 = phi %"struct.std::__detail::_Hash_node"* [ %221, %for.cond.i.i.i.i.i776 ], [ %218, %if.end.i.i.i.i.i774 ]
  %_M_nxt4.i.i.i.i.i778 = getelementptr inbounds %"struct.std::__detail::_Hash_node", %"struct.std::__detail::_Hash_node"* %__p.026.i.i.i.i.i777, i64 0, i32 0, i32 0, i32 0
  %220 = load %"struct.std::__detail::_Hash_node_base"*, %"struct.std::__detail::_Hash_node_base"** %_M_nxt4.i.i.i.i.i778, align 8, !tbaa !11
  %tobool5.not.i.i.i.i.i779 = icmp eq %"struct.std::__detail::_Hash_node_base"* %220, null
  %221 = bitcast %"struct.std::__detail::_Hash_node_base"* %220 to %"struct.std::__detail::_Hash_node"*
  br i1 %tobool5.not.i.i.i.i.i779, label %cleanup.cont.i.i.i789, label %lor.lhs.false.i.i.i.i.i785

lor.lhs.false.i.i.i.i.i785:                       ; preds = %if.end3.i.i.i.i.i780
  %_M_storage.i.i.i.i21.i.i.i.i.i781 = getelementptr inbounds %"struct.std::__detail::_Hash_node_base", %"struct.std::__detail::_Hash_node_base"* %220, i64 1
  %first.i.i.i.i.i22.i.i.i.i.i782 = bitcast %"struct.std::__detail::_Hash_node_base"* %_M_storage.i.i.i.i21.i.i.i.i.i781 to i64*
  %222 = load i64, i64* %first.i.i.i.i.i22.i.i.i.i.i782, align 8, !tbaa !29
  %rem.i.i.i.i.i.i.i.i783 = urem i64 %222, %214
  %cmp.not.i.i.i.i.i784 = icmp eq i64 %rem.i.i.i.i.i.i.i.i783, %rem.i.i.i.i.i.i768
  br i1 %cmp.not.i.i.i.i.i784, label %for.cond.i.i.i.i.i776, label %cleanup.cont.i.i.i789

cleanup.i.i.i788:                                 ; preds = %for.cond.i.i.i.i.i776, %if.end.i.i.i.i.i774
  call void @_ZdlPv(i8* nonnull %call2.i.i.i.i.i.i795) #20
  br label %call.i.noexc

cleanup.cont.i.i.i789:                            ; preds = %lor.lhs.false.i.i.i.i.i785, %if.end3.i.i.i.i.i780, %call2.i.i.i.i.i.i.noexc794
  %call15.i.i.i797 = invoke %"struct.std::__detail::_Hash_node"* @_ZNSt10_HashtableImSt4pairIKmP14JitSortContextESaIS4_ENSt8__detail10_Select1stESt8equal_toImESt4hashImENS6_18_Mod_range_hashingENS6_20_Default_ranged_hashENS6_20_Prime_rehash_policyENS6_17_Hashtable_traitsILb0ELb0ELb1EEEE21_M_insert_unique_nodeEmmPNS6_10_Hash_nodeIS4_Lb0EEE(%"class.std::_Hashtable"* nonnull dereferenceable(56) getelementptr inbounds (%class.OpTemplateCache, %class.OpTemplateCache* @g_jitSortContexts, i64 0, i32 1, i32 0), i64 %rem.i.i.i.i.i.i768, i64 %stageId, %"struct.std::__detail::_Hash_node"* nonnull %212)
          to label %call.i.noexc unwind label %lpad207

call.i.noexc:                                     ; preds = %cleanup.i.i.i788, %cleanup.cont.i.i.i789
  br i1 icmp ne (i8* bitcast (i32 (i32*, void (i8*)*)* @__pthread_key_create to i8*), i8* null), label %if.then.i.i.i, label %invoke.cont215

if.then.i.i.i:                                    ; preds = %call.i.noexc
  %call1.i.i4.i = call i32 @pthread_mutex_unlock(%union.pthread_mutex_t* nonnull getelementptr inbounds (%class.OpTemplateCache, %class.OpTemplateCache* @g_jitSortContexts, i64 0, i32 0, i32 0, i32 0)) #20
  br label %invoke.cont215

_ZNSt5mutex6unlockEv.exit.critedge.i:             ; preds = %invoke.cont208
  %call2.i.i.i.i.i.i725 = invoke noalias nonnull i8* @_Znwm(i64 24) #22
          to label %call2.i.i.i.i.i.i.noexc unwind label %lpad207

call2.i.i.i.i.i.i.noexc:                          ; preds = %_ZNSt5mutex6unlockEv.exit.critedge.i
  %223 = bitcast i8* %call2.i.i.i.i.i.i725 to %"struct.std::__detail::_Hash_node"*
  %_M_nxt.i.i.i.i.i.i.i = bitcast i8* %call2.i.i.i.i.i.i725 to %"struct.std::__detail::_Hash_node_base"**
  store %"struct.std::__detail::_Hash_node_base"* null, %"struct.std::__detail::_Hash_node_base"** %_M_nxt.i.i.i.i.i.i.i, align 8, !tbaa !11
  %_M_storage.i.i.i.i.i723 = getelementptr inbounds i8, i8* %call2.i.i.i.i.i.i725, i64 8
  %first.i.i.i.i.i.i.i = bitcast i8* %_M_storage.i.i.i.i.i723 to i64*
  store i64 %stageId, i64* %first.i.i.i.i.i.i.i, align 8, !tbaa !112
  %second.i.i.i.i.i.i.i = getelementptr inbounds i8, i8* %call2.i.i.i.i.i.i725, i64 16
  %224 = bitcast i8* %second.i.i.i.i.i.i.i to i8**
  store i8* %call209, i8** %224, align 8, !tbaa !114
  %225 = load i64, i64* getelementptr inbounds (%class.OpTemplateCache, %class.OpTemplateCache* @g_jitSortContexts, i64 0, i32 1, i32 0, i32 1), align 8, !tbaa !15
  %rem.i.i.i.i.i.i = urem i64 %stageId, %225
  %226 = load %"struct.std::__detail::_Hash_node_base"**, %"struct.std::__detail::_Hash_node_base"*** getelementptr inbounds (%class.OpTemplateCache, %class.OpTemplateCache* @g_jitSortContexts, i64 0, i32 1, i32 0, i32 0), align 8, !tbaa !14
  %arrayidx.i.i.i.i.i724 = getelementptr inbounds %"struct.std::__detail::_Hash_node_base"*, %"struct.std::__detail::_Hash_node_base"** %226, i64 %rem.i.i.i.i.i.i
  %227 = load %"struct.std::__detail::_Hash_node_base"*, %"struct.std::__detail::_Hash_node_base"** %arrayidx.i.i.i.i.i724, align 8, !tbaa !46
  %tobool.not.i.i.i.i.i = icmp eq %"struct.std::__detail::_Hash_node_base"* %227, null
  br i1 %tobool.not.i.i.i.i.i, label %cleanup.cont.i.i.i, label %if.end.i.i.i.i.i

if.end.i.i.i.i.i:                                 ; preds = %call2.i.i.i.i.i.i.noexc
  %228 = bitcast %"struct.std::__detail::_Hash_node_base"* %227 to %"struct.std::__detail::_Hash_node"**
  %229 = load %"struct.std::__detail::_Hash_node"*, %"struct.std::__detail::_Hash_node"** %228, align 8, !tbaa !11
  %_M_storage.i.i.i.i23.i.i.i.i.i = getelementptr inbounds %"struct.std::__detail::_Hash_node", %"struct.std::__detail::_Hash_node"* %229, i64 0, i32 0, i32 1
  %first.i.i.i.i.i24.i.i.i.i.i = bitcast %"struct.__gnu_cxx::__aligned_buffer"* %_M_storage.i.i.i.i23.i.i.i.i.i to i64*
  %230 = load i64, i64* %first.i.i.i.i.i24.i.i.i.i.i, align 8, !tbaa !29
  %cmp.i.i.i25.i.i.i.i.i = icmp eq i64 %230, %stageId
  br i1 %cmp.i.i.i25.i.i.i.i.i, label %cleanup.i.i.i, label %if.end3.i.i.i.i.i

for.cond.i.i.i.i.i:                               ; preds = %lor.lhs.false.i.i.i.i.i
  %cmp.i.i.i.i.i.i.i.i = icmp eq i64 %233, %stageId
  br i1 %cmp.i.i.i.i.i.i.i.i, label %cleanup.i.i.i, label %if.end3.i.i.i.i.i

if.end3.i.i.i.i.i:                                ; preds = %if.end.i.i.i.i.i, %for.cond.i.i.i.i.i
  %__p.026.i.i.i.i.i = phi %"struct.std::__detail::_Hash_node"* [ %232, %for.cond.i.i.i.i.i ], [ %229, %if.end.i.i.i.i.i ]
  %_M_nxt4.i.i.i.i.i = getelementptr inbounds %"struct.std::__detail::_Hash_node", %"struct.std::__detail::_Hash_node"* %__p.026.i.i.i.i.i, i64 0, i32 0, i32 0, i32 0
  %231 = load %"struct.std::__detail::_Hash_node_base"*, %"struct.std::__detail::_Hash_node_base"** %_M_nxt4.i.i.i.i.i, align 8, !tbaa !11
  %tobool5.not.i.i.i.i.i = icmp eq %"struct.std::__detail::_Hash_node_base"* %231, null
  %232 = bitcast %"struct.std::__detail::_Hash_node_base"* %231 to %"struct.std::__detail::_Hash_node"*
  br i1 %tobool5.not.i.i.i.i.i, label %cleanup.cont.i.i.i, label %lor.lhs.false.i.i.i.i.i

lor.lhs.false.i.i.i.i.i:                          ; preds = %if.end3.i.i.i.i.i
  %_M_storage.i.i.i.i21.i.i.i.i.i = getelementptr inbounds %"struct.std::__detail::_Hash_node_base", %"struct.std::__detail::_Hash_node_base"* %231, i64 1
  %first.i.i.i.i.i22.i.i.i.i.i = bitcast %"struct.std::__detail::_Hash_node_base"* %_M_storage.i.i.i.i21.i.i.i.i.i to i64*
  %233 = load i64, i64* %first.i.i.i.i.i22.i.i.i.i.i, align 8, !tbaa !29
  %rem.i.i.i.i.i.i.i.i = urem i64 %233, %225
  %cmp.not.i.i.i.i.i = icmp eq i64 %rem.i.i.i.i.i.i.i.i, %rem.i.i.i.i.i.i
  br i1 %cmp.not.i.i.i.i.i, label %for.cond.i.i.i.i.i, label %cleanup.cont.i.i.i

cleanup.i.i.i:                                    ; preds = %for.cond.i.i.i.i.i, %if.end.i.i.i.i.i
  call void @_ZdlPv(i8* nonnull %call2.i.i.i.i.i.i725) #20
  br label %invoke.cont215

cleanup.cont.i.i.i:                               ; preds = %lor.lhs.false.i.i.i.i.i, %if.end3.i.i.i.i.i, %call2.i.i.i.i.i.i.noexc
  %call15.i.i.i726 = invoke %"struct.std::__detail::_Hash_node"* @_ZNSt10_HashtableImSt4pairIKmP14JitSortContextESaIS4_ENSt8__detail10_Select1stESt8equal_toImESt4hashImENS6_18_Mod_range_hashingENS6_20_Default_ranged_hashENS6_20_Prime_rehash_policyENS6_17_Hashtable_traitsILb0ELb0ELb1EEEE21_M_insert_unique_nodeEmmPNS6_10_Hash_nodeIS4_Lb0EEE(%"class.std::_Hashtable"* nonnull dereferenceable(56) getelementptr inbounds (%class.OpTemplateCache, %class.OpTemplateCache* @g_jitSortContexts, i64 0, i32 1, i32 0), i64 %rem.i.i.i.i.i.i, i64 %stageId, %"struct.std::__detail::_Hash_node"* nonnull %223)
          to label %invoke.cont215 unwind label %lpad207

invoke.cont215:                                   ; preds = %cleanup.cont.i.i.i, %cleanup.i.i.i, %if.then.i.i.i, %call.i.noexc
  %234 = load %"class.llvm::orc::LLJIT"*, %"class.llvm::orc::LLJIT"** %_M_head_impl.i.i.i.i.i.i.i649, align 8, !tbaa !46
  %cmp.not.i728 = icmp eq %"class.llvm::orc::LLJIT"* %234, null
  br i1 %cmp.not.i728, label %_ZNSt10unique_ptrIN4llvm3orc5LLJITESt14default_deleteIS2_EED2Ev.exit730, label %_ZNKSt14default_deleteIN4llvm3orc5LLJITEEclEPS2_.exit.i729

_ZNKSt14default_deleteIN4llvm3orc5LLJITEEclEPS2_.exit.i729: ; preds = %invoke.cont215
  call void @_ZN4llvm3orc5LLJITD1Ev(%"class.llvm::orc::LLJIT"* nonnull dereferenceable(568) %234) #20
  %235 = bitcast %"class.llvm::orc::LLJIT"* %234 to i8*
  call void @_ZdlPv(i8* %235) #25
  br label %_ZNSt10unique_ptrIN4llvm3orc5LLJITESt14default_deleteIS2_EED2Ev.exit730

_ZNSt10unique_ptrIN4llvm3orc5LLJITESt14default_deleteIS2_EED2Ev.exit730: ; preds = %invoke.cont215, %_ZNKSt14default_deleteIN4llvm3orc5LLJITEEclEPS2_.exit.i729
  call void @llvm.lifetime.end.p0i8(i64 8, i8* nonnull %173) #20
  call void @llvm.lifetime.end.p0i8(i64 1616, i8* nonnull %171) #20
  call void @_ZN7codegen6HammerD2Ev(%"class.codegen::Hammer"* nonnull dereferenceable(152) %hammer2) #20
  call void @llvm.lifetime.end.p0i8(i64 152, i8* nonnull %148) #20
  call void @_ZN7codegen6HammerD2Ev(%"class.codegen::Hammer"* nonnull dereferenceable(152) %hammer1) #20
  call void @llvm.lifetime.end.p0i8(i64 152, i8* nonnull %128) #20
  call void @llvm.lifetime.end.p0i8(i64 24, i8* nonnull %78) #20
  call void @llvm.lifetime.end.p0i8(i64 24, i8* nonnull %75) #20
  call void @llvm.lifetime.end.p0i8(i64 24, i8* nonnull %73) #20
  call void @llvm.lifetime.end.p0i8(i64 24, i8* nonnull %23) #20
  call void @llvm.lifetime.end.p0i8(i64 24, i8* nonnull %21) #20
  call void @llvm.lifetime.end.p0i8(i64 24, i8* nonnull %19) #20
  call void @llvm.lifetime.end.p0i8(i64 24, i8* nonnull %17) #20
  call void @llvm.lifetime.end.p0i8(i64 24, i8* nonnull %15) #20
  call void @llvm.stackrestore(i8* %10)
  %236 = load %"struct.std::__detail::_List_node_base"*, %"struct.std::__detail::_List_node_base"** %_M_next.i.i.i, align 8, !tbaa !26
  %cmp.not16.i.i732 = icmp eq %"struct.std::__detail::_List_node_base"* %236, %7
  br i1 %cmp.not16.i.i732, label %_ZNSt7__cxx1110_List_baseIPN7codegen6HammerESaIS3_EED2Ev.exit737, label %invoke.cont6.i.i736

invoke.cont6.i.i736:                              ; preds = %_ZNSt10unique_ptrIN4llvm3orc5LLJITESt14default_deleteIS2_EED2Ev.exit730, %invoke.cont6.i.i736
  %__cur.017.i.i733 = phi %"struct.std::__detail::_List_node_base"* [ %237, %invoke.cont6.i.i736 ], [ %236, %_ZNSt10unique_ptrIN4llvm3orc5LLJITESt14default_deleteIS2_EED2Ev.exit730 ]
  %_M_next4.i.i734 = getelementptr inbounds %"struct.std::__detail::_List_node_base", %"struct.std::__detail::_List_node_base"* %__cur.017.i.i733, i64 0, i32 0
  %237 = load %"struct.std::__detail::_List_node_base"*, %"struct.std::__detail::_List_node_base"** %_M_next4.i.i734, align 8, !tbaa !26
  %238 = bitcast %"struct.std::__detail::_List_node_base"* %__cur.017.i.i733 to i8*
  call void @_ZdlPv(i8* %238) #20
  %cmp.not.i.i735 = icmp eq %"struct.std::__detail::_List_node_base"* %237, %7
  br i1 %cmp.not.i.i735, label %_ZNSt7__cxx1110_List_baseIPN7codegen6HammerESaIS3_EED2Ev.exit737, label %invoke.cont6.i.i736, !llvm.loop !55

_ZNSt7__cxx1110_List_baseIPN7codegen6HammerESaIS3_EED2Ev.exit737: ; preds = %invoke.cont6.i.i736, %_ZNSt10unique_ptrIN4llvm3orc5LLJITESt14default_deleteIS2_EED2Ev.exit730
  call void @llvm.lifetime.end.p0i8(i64 24, i8* nonnull %6) #20
  %_M_t.i738 = getelementptr inbounds %"class.std::map", %"class.std::map"* %testParam, i64 0, i32 0
  %239 = bitcast i8* %_M_parent.i.i.i.i.i to %"struct.std::_Rb_tree_node"**
  %240 = load %"struct.std::_Rb_tree_node"*, %"struct.std::_Rb_tree_node"** %239, align 8, !tbaa !22
  invoke void @_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE8_M_eraseEPSt13_Rb_tree_nodeISB_E(%"class.std::_Rb_tree"* nonnull dereferenceable(48) %_M_t.i738, %"struct.std::_Rb_tree_node"* %240)
          to label %_ZNSt3mapINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEPN7codegen10ParamValueESt4lessIS5_ESaISt4pairIKS5_S8_EEED2Ev.exit741 unwind label %lpad.i.i740

lpad.i.i740:                                      ; preds = %_ZNSt7__cxx1110_List_baseIPN7codegen6HammerESaIS3_EED2Ev.exit737
  %241 = landingpad { i8*, i32 }
          catch i8* null
  %242 = extractvalue { i8*, i32 } %241, 0
  call void @__clang_call_terminate(i8* %242) #21
  unreachable

_ZNSt3mapINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEPN7codegen10ParamValueESt4lessIS5_ESaISt4pairIKS5_S8_EEED2Ev.exit741: ; preds = %_ZNSt7__cxx1110_List_baseIPN7codegen6HammerESaIS3_EED2Ev.exit737
  call void @llvm.lifetime.end.p0i8(i64 48, i8* nonnull %0) #20
  ret void

lpad14:                                           ; preds = %for.cond.cleanup
  %243 = landingpad { i8*, i32 }
          cleanup
  br label %ehcleanup

lpad16:                                           ; preds = %call5.i.i.i10.i.noexc
  %244 = landingpad { i8*, i32 }
          cleanup
  %245 = load i8*, i8** %_M_p.i13.i.i.i.i, align 8, !tbaa !41
  %cmp.i.i.i744 = icmp eq i8* %245, %28
  br i1 %cmp.i.i.i744, label %ehcleanup, label %if.then.i.i745

if.then.i.i745:                                   ; preds = %lpad16
  call void @_ZdlPv(i8* %245) #20
  br label %ehcleanup

ehcleanup:                                        ; preds = %if.then.i.i745, %lpad16, %lpad14
  %.pn = phi { i8*, i32 } [ %243, %lpad14 ], [ %244, %lpad16 ], [ %244, %if.then.i.i745 ]
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %25) #20
  br label %ehcleanup230

lpad22:                                           ; preds = %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit
  %246 = landingpad { i8*, i32 }
          cleanup
  br label %ehcleanup28

lpad24:                                           ; preds = %call5.i.i.i10.i.noexc335
  %247 = landingpad { i8*, i32 }
          cleanup
  %248 = load i8*, i8** %_M_p.i13.i.i.i.i326, align 8, !tbaa !41
  %cmp.i.i.i749 = icmp eq i8* %248, %44
  br i1 %cmp.i.i.i749, label %ehcleanup28, label %if.then.i.i750

if.then.i.i750:                                   ; preds = %lpad24
  call void @_ZdlPv(i8* %248) #20
  br label %ehcleanup28

ehcleanup28:                                      ; preds = %if.then.i.i750, %lpad24, %lpad22
  %.pn262 = phi { i8*, i32 } [ %246, %lpad22 ], [ %247, %lpad24 ], [ %247, %if.then.i.i750 ]
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %41) #20
  br label %ehcleanup230

lpad33:                                           ; preds = %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit342
  %249 = landingpad { i8*, i32 }
          cleanup
  br label %ehcleanup39

lpad35:                                           ; preds = %call5.i.i.i10.i.noexc366
  %250 = landingpad { i8*, i32 }
          cleanup
  %251 = load i8*, i8** %_M_p.i13.i.i.i.i357, align 8, !tbaa !41
  %cmp.i.i.i754 = icmp eq i8* %251, %52
  br i1 %cmp.i.i.i754, label %ehcleanup39, label %if.then.i.i755

if.then.i.i755:                                   ; preds = %lpad35
  call void @_ZdlPv(i8* %251) #20
  br label %ehcleanup39

ehcleanup39:                                      ; preds = %if.then.i.i755, %lpad35, %lpad33
  %.pn264 = phi { i8*, i32 } [ %249, %lpad33 ], [ %250, %lpad35 ], [ %250, %if.then.i.i755 ]
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %49) #20
  br label %ehcleanup230

lpad44:                                           ; preds = %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit373
  %252 = landingpad { i8*, i32 }
          cleanup
  br label %ehcleanup50

lpad46:                                           ; preds = %call5.i.i.i10.i.noexc393
  %253 = landingpad { i8*, i32 }
          cleanup
  %254 = load i8*, i8** %_M_p.i13.i.i.i.i384, align 8, !tbaa !41
  %cmp.i.i.i759 = icmp eq i8* %254, %60
  br i1 %cmp.i.i.i759, label %ehcleanup50, label %if.then.i.i760

if.then.i.i760:                                   ; preds = %lpad46
  call void @_ZdlPv(i8* %254) #20
  br label %ehcleanup50

ehcleanup50:                                      ; preds = %if.then.i.i760, %lpad46, %lpad44
  %.pn266 = phi { i8*, i32 } [ %252, %lpad44 ], [ %253, %lpad46 ], [ %253, %if.then.i.i760 ]
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %57) #20
  br label %ehcleanup230

lpad55:                                           ; preds = %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit400
  %255 = landingpad { i8*, i32 }
          cleanup
  br label %ehcleanup61

lpad57:                                           ; preds = %call5.i.i.i10.i.noexc417
  %256 = landingpad { i8*, i32 }
          cleanup
  %257 = load i8*, i8** %_M_p.i13.i.i.i.i408, align 8, !tbaa !41
  %cmp.i.i.i716 = icmp eq i8* %257, %68
  br i1 %cmp.i.i.i716, label %ehcleanup61, label %if.then.i.i717

if.then.i.i717:                                   ; preds = %lpad57
  call void @_ZdlPv(i8* %257) #20
  br label %ehcleanup61

ehcleanup61:                                      ; preds = %if.then.i.i717, %lpad57, %lpad55
  %.pn268 = phi { i8*, i32 } [ %255, %lpad55 ], [ %256, %lpad57 ], [ %256, %if.then.i.i717 ]
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %65) #20
  br label %ehcleanup230

lpad68:                                           ; preds = %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit587
  %258 = landingpad { i8*, i32 }
          cleanup
  br label %ehcleanup227

lpad72:                                           ; preds = %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit424
  %259 = landingpad { i8*, i32 }
          cleanup
  br label %ehcleanup78

lpad74:                                           ; preds = %call5.i.i.i10.i.noexc455
  %260 = landingpad { i8*, i32 }
          cleanup
  %261 = load i8*, i8** %_M_p.i13.i.i.i.i446, align 8, !tbaa !41
  %cmp.i.i.i565 = icmp eq i8* %261, %83
  br i1 %cmp.i.i.i565, label %ehcleanup78, label %if.then.i.i566

if.then.i.i566:                                   ; preds = %lpad74
  call void @_ZdlPv(i8* %261) #20
  br label %ehcleanup78

ehcleanup78:                                      ; preds = %if.then.i.i566, %lpad74, %lpad72
  %.pn270 = phi { i8*, i32 } [ %259, %lpad72 ], [ %260, %lpad74 ], [ %260, %if.then.i.i566 ]
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %80) #20
  br label %ehcleanup227

lpad83:                                           ; preds = %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit462
  %262 = landingpad { i8*, i32 }
          cleanup
  br label %ehcleanup89

lpad85:                                           ; preds = %call5.i.i.i10.i.noexc480
  %263 = landingpad { i8*, i32 }
          cleanup
  %264 = load i8*, i8** %_M_p.i13.i.i.i.i471, align 8, !tbaa !41
  %cmp.i.i.i540 = icmp eq i8* %264, %91
  br i1 %cmp.i.i.i540, label %ehcleanup89, label %if.then.i.i541

if.then.i.i541:                                   ; preds = %lpad85
  call void @_ZdlPv(i8* %264) #20
  br label %ehcleanup89

ehcleanup89:                                      ; preds = %if.then.i.i541, %lpad85, %lpad83
  %.pn272 = phi { i8*, i32 } [ %262, %lpad83 ], [ %263, %lpad85 ], [ %263, %if.then.i.i541 ]
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %88) #20
  br label %ehcleanup227

lpad94:                                           ; preds = %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit487
  %265 = landingpad { i8*, i32 }
          cleanup
  br label %ehcleanup100

lpad96:                                           ; preds = %call5.i.i.i10.i.noexc505
  %266 = landingpad { i8*, i32 }
          cleanup
  %267 = load i8*, i8** %_M_p.i13.i.i.i.i496, align 8, !tbaa !41
  %cmp.i.i.i515 = icmp eq i8* %267, %99
  br i1 %cmp.i.i.i515, label %ehcleanup100, label %if.then.i.i516

if.then.i.i516:                                   ; preds = %lpad96
  call void @_ZdlPv(i8* %267) #20
  br label %ehcleanup100

ehcleanup100:                                     ; preds = %if.then.i.i516, %lpad96, %lpad94
  %.pn274 = phi { i8*, i32 } [ %265, %lpad94 ], [ %266, %lpad96 ], [ %266, %if.then.i.i516 ]
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %96) #20
  br label %ehcleanup227

lpad105:                                          ; preds = %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit512
  %268 = landingpad { i8*, i32 }
          cleanup
  br label %ehcleanup111

lpad107:                                          ; preds = %call5.i.i.i10.i.noexc530
  %269 = landingpad { i8*, i32 }
          cleanup
  %270 = load i8*, i8** %_M_p.i13.i.i.i.i521, align 8, !tbaa !41
  %cmp.i.i.i490 = icmp eq i8* %270, %107
  br i1 %cmp.i.i.i490, label %ehcleanup111, label %if.then.i.i491

if.then.i.i491:                                   ; preds = %lpad107
  call void @_ZdlPv(i8* %270) #20
  br label %ehcleanup111

ehcleanup111:                                     ; preds = %if.then.i.i491, %lpad107, %lpad105
  %.pn276 = phi { i8*, i32 } [ %268, %lpad105 ], [ %269, %lpad107 ], [ %269, %if.then.i.i491 ]
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %104) #20
  br label %ehcleanup227

lpad116:                                          ; preds = %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit537
  %271 = landingpad { i8*, i32 }
          cleanup
  br label %ehcleanup122

lpad118:                                          ; preds = %call5.i.i.i10.i.noexc555
  %272 = landingpad { i8*, i32 }
          cleanup
  %273 = load i8*, i8** %_M_p.i13.i.i.i.i546, align 8, !tbaa !41
  %cmp.i.i.i465 = icmp eq i8* %273, %115
  br i1 %cmp.i.i.i465, label %ehcleanup122, label %if.then.i.i466

if.then.i.i466:                                   ; preds = %lpad118
  call void @_ZdlPv(i8* %273) #20
  br label %ehcleanup122

ehcleanup122:                                     ; preds = %if.then.i.i466, %lpad118, %lpad116
  %.pn278 = phi { i8*, i32 } [ %271, %lpad116 ], [ %272, %lpad118 ], [ %272, %if.then.i.i466 ]
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %112) #20
  br label %ehcleanup227

lpad127:                                          ; preds = %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit562
  %274 = landingpad { i8*, i32 }
          cleanup
  br label %ehcleanup133

lpad129:                                          ; preds = %call5.i.i.i10.i.noexc580
  %275 = landingpad { i8*, i32 }
          cleanup
  %276 = load i8*, i8** %_M_p.i13.i.i.i.i571, align 8, !tbaa !41
  %cmp.i.i.i440 = icmp eq i8* %276, %123
  br i1 %cmp.i.i.i440, label %ehcleanup133, label %if.then.i.i441

if.then.i.i441:                                   ; preds = %lpad129
  call void @_ZdlPv(i8* %276) #20
  br label %ehcleanup133

ehcleanup133:                                     ; preds = %if.then.i.i441, %lpad129, %lpad127
  %.pn280 = phi { i8*, i32 } [ %274, %lpad127 ], [ %275, %lpad129 ], [ %275, %if.then.i.i441 ]
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %120) #20
  br label %ehcleanup227

lpad138:                                          ; preds = %if.then.i.i597
  %277 = landingpad { i8*, i32 }
          cleanup
  br label %ehcleanup226

lpad142:                                          ; preds = %invoke.cont141
  %278 = landingpad { i8*, i32 }
          cleanup
  %279 = bitcast i8* %_M_parent.i.i.i.i.i592 to %"struct.std::_Rb_tree_node"**
  %280 = load %"struct.std::_Rb_tree_node"*, %"struct.std::_Rb_tree_node"** %279, align 8, !tbaa !22
  invoke void @_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE8_M_eraseEPSt13_Rb_tree_nodeISB_E(%"class.std::_Rb_tree"* nonnull dereferenceable(48) %_M_t.i590, %"struct.std::_Rb_tree_node"* %280)
          to label %ehcleanup226 unwind label %lpad.i.i433

lpad.i.i433:                                      ; preds = %lpad142
  %281 = landingpad { i8*, i32 }
          catch i8* null
  %282 = extractvalue { i8*, i32 } %281, 0
  call void @__clang_call_terminate(i8* %282) #21
  unreachable

lpad146:                                          ; preds = %if.then.i.i617
  %283 = landingpad { i8*, i32 }
          cleanup
  br label %ehcleanup224

lpad150:                                          ; preds = %invoke.cont149
  %284 = landingpad { i8*, i32 }
          cleanup
  %285 = bitcast i8* %_M_parent.i.i.i.i.i609 to %"struct.std::_Rb_tree_node"**
  %286 = load %"struct.std::_Rb_tree_node"*, %"struct.std::_Rb_tree_node"** %285, align 8, !tbaa !22
  invoke void @_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE8_M_eraseEPSt13_Rb_tree_nodeISB_E(%"class.std::_Rb_tree"* nonnull dereferenceable(48) %_M_t.i607, %"struct.std::_Rb_tree_node"* %286)
          to label %ehcleanup224 unwind label %lpad.i.i403

lpad.i.i403:                                      ; preds = %lpad150
  %287 = landingpad { i8*, i32 }
          catch i8* null
  %288 = extractvalue { i8*, i32 } %287, 0
  call void @__clang_call_terminate(i8* %288) #21
  unreachable

lpad153:                                          ; preds = %_ZNSt3mapINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEPN7codegen10ParamValueESt4lessIS5_ESaISt4pairIKS5_S8_EEED2Ev.exit635
  %289 = landingpad { i8*, i32 }
          cleanup
  br label %ehcleanup223

lpad157:                                          ; preds = %invoke.cont154
  %290 = landingpad { i8*, i32 }
          cleanup
  br label %ehcleanup223

lpad160:                                          ; preds = %.noexc, %invoke.cont158
  %291 = landingpad { i8*, i32 }
          cleanup
  br label %ehcleanup222

lpad165:                                          ; preds = %invoke.cont164
  %292 = landingpad { i8*, i32 }
          cleanup
  %293 = load %"struct.std::__detail::_List_node_base"*, %"struct.std::__detail::_List_node_base"** %_M_next.i.i14.i, align 8, !tbaa !26
  %cmp.not16.i.i375 = icmp eq %"struct.std::__detail::_List_node_base"* %293, %174
  br i1 %cmp.not16.i.i375, label %ehcleanup221, label %invoke.cont6.i.i379

invoke.cont6.i.i379:                              ; preds = %lpad165, %invoke.cont6.i.i379
  %__cur.017.i.i376 = phi %"struct.std::__detail::_List_node_base"* [ %294, %invoke.cont6.i.i379 ], [ %293, %lpad165 ]
  %_M_next4.i.i377 = getelementptr inbounds %"struct.std::__detail::_List_node_base", %"struct.std::__detail::_List_node_base"* %__cur.017.i.i376, i64 0, i32 0
  %294 = load %"struct.std::__detail::_List_node_base"*, %"struct.std::__detail::_List_node_base"** %_M_next4.i.i377, align 8, !tbaa !26
  %295 = bitcast %"struct.std::__detail::_List_node_base"* %__cur.017.i.i376 to i8*
  call void @_ZdlPv(i8* %295) #20
  %cmp.not.i.i378 = icmp eq %"struct.std::__detail::_List_node_base"* %294, %174
  br i1 %cmp.not.i.i378, label %ehcleanup221, label %invoke.cont6.i.i379, !llvm.loop !55

lpad171:                                          ; preds = %invoke.cont172
  %296 = landingpad { i8*, i32 }
          cleanup
  call void @llvm.lifetime.end.p0i8(i64 24, i8* nonnull %190) #20
  br label %ehcleanup219

lpad184:                                          ; preds = %invoke.cont185
  %297 = landingpad { i8*, i32 }
          cleanup
  call void @llvm.lifetime.end.p0i8(i64 24, i8* nonnull %196) #20
  br label %ehcleanup219

lpad197:                                          ; preds = %invoke.cont198
  %298 = landingpad { i8*, i32 }
          cleanup
  call void @llvm.lifetime.end.p0i8(i64 24, i8* nonnull %202) #20
  br label %ehcleanup219

lpad207:                                          ; preds = %cleanup.cont.i.i.i789, %_ZNSt5mutex4lockEv.exit.i, %cleanup.cont.i.i.i, %_ZNSt5mutex6unlockEv.exit.critedge.i, %if.then.i.i719, %_ZN4llvm8ExpectedINS_18JITEvaluatedSymbolEED2Ev.exit712
  %299 = landingpad { i8*, i32 }
          cleanup
  br label %ehcleanup219

ehcleanup219:                                     ; preds = %lpad184, %lpad207, %lpad197, %lpad171
  %.pn288.pn.pn = phi { i8*, i32 } [ %296, %lpad171 ], [ %297, %lpad184 ], [ %299, %lpad207 ], [ %298, %lpad197 ]
  %300 = load %"class.llvm::orc::LLJIT"*, %"class.llvm::orc::LLJIT"** %_M_head_impl.i.i.i.i.i.i.i649, align 8, !tbaa !46
  %cmp.not.i = icmp eq %"class.llvm::orc::LLJIT"* %300, null
  br i1 %cmp.not.i, label %ehcleanup221, label %_ZNKSt14default_deleteIN4llvm3orc5LLJITEEclEPS2_.exit.i

_ZNKSt14default_deleteIN4llvm3orc5LLJITEEclEPS2_.exit.i: ; preds = %ehcleanup219
  call void @_ZN4llvm3orc5LLJITD1Ev(%"class.llvm::orc::LLJIT"* nonnull dereferenceable(568) %300) #20
  %301 = bitcast %"class.llvm::orc::LLJIT"* %300 to i8*
  call void @_ZdlPv(i8* %301) #25
  br label %ehcleanup221

ehcleanup221:                                     ; preds = %invoke.cont6.i.i.i, %invoke.cont6.i.i379, %_ZNKSt14default_deleteIN4llvm3orc5LLJITEEclEPS2_.exit.i, %ehcleanup219, %lpad165, %lpad.i
  %.pn288.pn.pn.pn = phi { i8*, i32 } [ %183, %lpad.i ], [ %292, %lpad165 ], [ %.pn288.pn.pn, %ehcleanup219 ], [ %.pn288.pn.pn, %_ZNKSt14default_deleteIN4llvm3orc5LLJITEEclEPS2_.exit.i ], [ %292, %invoke.cont6.i.i379 ], [ %183, %invoke.cont6.i.i.i ]
  call void @llvm.lifetime.end.p0i8(i64 8, i8* nonnull %173) #20
  br label %ehcleanup222

ehcleanup222:                                     ; preds = %ehcleanup221, %lpad160
  %.pn288.pn.pn.pn.pn = phi { i8*, i32 } [ %.pn288.pn.pn.pn, %ehcleanup221 ], [ %291, %lpad160 ]
  call void @llvm.lifetime.end.p0i8(i64 1616, i8* nonnull %171) #20
  br label %ehcleanup223

ehcleanup223:                                     ; preds = %ehcleanup222, %lpad157, %lpad153
  %.pn288.pn.pn.pn.pn.pn = phi { i8*, i32 } [ %.pn288.pn.pn.pn.pn, %ehcleanup222 ], [ %290, %lpad157 ], [ %289, %lpad153 ]
  call void @_ZN7codegen6HammerD2Ev(%"class.codegen::Hammer"* nonnull dereferenceable(152) %hammer2) #20
  br label %ehcleanup224

ehcleanup224:                                     ; preds = %lpad150, %ehcleanup223, %lpad146
  %.pn288.pn.pn.pn.pn.pn.pn = phi { i8*, i32 } [ %.pn288.pn.pn.pn.pn.pn, %ehcleanup223 ], [ %283, %lpad146 ], [ %284, %lpad150 ]
  call void @llvm.lifetime.end.p0i8(i64 152, i8* nonnull %148) #20
  call void @_ZN7codegen6HammerD2Ev(%"class.codegen::Hammer"* nonnull dereferenceable(152) %hammer1) #20
  br label %ehcleanup226

ehcleanup226:                                     ; preds = %lpad142, %ehcleanup224, %lpad138
  %.pn288.pn.pn.pn.pn.pn.pn.pn = phi { i8*, i32 } [ %.pn288.pn.pn.pn.pn.pn.pn, %ehcleanup224 ], [ %277, %lpad138 ], [ %278, %lpad142 ]
  call void @llvm.lifetime.end.p0i8(i64 152, i8* nonnull %128) #20
  br label %ehcleanup227

ehcleanup227:                                     ; preds = %ehcleanup226, %ehcleanup133, %ehcleanup122, %ehcleanup111, %ehcleanup100, %ehcleanup89, %ehcleanup78, %lpad68
  %.pn288.pn.pn.pn.pn.pn.pn.pn.pn = phi { i8*, i32 } [ %.pn288.pn.pn.pn.pn.pn.pn.pn, %ehcleanup226 ], [ %258, %lpad68 ], [ %.pn280, %ehcleanup133 ], [ %.pn278, %ehcleanup122 ], [ %.pn276, %ehcleanup111 ], [ %.pn274, %ehcleanup100 ], [ %.pn272, %ehcleanup89 ], [ %.pn270, %ehcleanup78 ]
  call void @llvm.lifetime.end.p0i8(i64 24, i8* nonnull %78) #20
  call void @llvm.lifetime.end.p0i8(i64 24, i8* nonnull %75) #20
  call void @llvm.lifetime.end.p0i8(i64 24, i8* nonnull %73) #20
  br label %ehcleanup230

ehcleanup230:                                     ; preds = %ehcleanup227, %ehcleanup61, %ehcleanup50, %ehcleanup39, %ehcleanup28, %ehcleanup
  %.pn288.pn.pn.pn.pn.pn.pn.pn.pn.pn = phi { i8*, i32 } [ %.pn288.pn.pn.pn.pn.pn.pn.pn.pn, %ehcleanup227 ], [ %.pn268, %ehcleanup61 ], [ %.pn266, %ehcleanup50 ], [ %.pn264, %ehcleanup39 ], [ %.pn262, %ehcleanup28 ], [ %.pn, %ehcleanup ]
  call void @llvm.lifetime.end.p0i8(i64 24, i8* nonnull %23) #20
  call void @llvm.lifetime.end.p0i8(i64 24, i8* nonnull %21) #20
  call void @llvm.lifetime.end.p0i8(i64 24, i8* nonnull %19) #20
  call void @llvm.lifetime.end.p0i8(i64 24, i8* nonnull %17) #20
  call void @llvm.lifetime.end.p0i8(i64 24, i8* nonnull %15) #20
  %302 = load %"struct.std::__detail::_List_node_base"*, %"struct.std::__detail::_List_node_base"** %_M_next.i.i.i, align 8, !tbaa !26
  %cmp.not16.i.i = icmp eq %"struct.std::__detail::_List_node_base"* %302, %7
  br i1 %cmp.not16.i.i, label %_ZNSt7__cxx1110_List_baseIPN7codegen6HammerESaIS3_EED2Ev.exit, label %invoke.cont6.i.i

invoke.cont6.i.i:                                 ; preds = %ehcleanup230, %invoke.cont6.i.i
  %__cur.017.i.i = phi %"struct.std::__detail::_List_node_base"* [ %303, %invoke.cont6.i.i ], [ %302, %ehcleanup230 ]
  %_M_next4.i.i = getelementptr inbounds %"struct.std::__detail::_List_node_base", %"struct.std::__detail::_List_node_base"* %__cur.017.i.i, i64 0, i32 0
  %303 = load %"struct.std::__detail::_List_node_base"*, %"struct.std::__detail::_List_node_base"** %_M_next4.i.i, align 8, !tbaa !26
  %304 = bitcast %"struct.std::__detail::_List_node_base"* %__cur.017.i.i to i8*
  call void @_ZdlPv(i8* %304) #20
  %cmp.not.i.i = icmp eq %"struct.std::__detail::_List_node_base"* %303, %7
  br i1 %cmp.not.i.i, label %_ZNSt7__cxx1110_List_baseIPN7codegen6HammerESaIS3_EED2Ev.exit, label %invoke.cont6.i.i, !llvm.loop !55

_ZNSt7__cxx1110_List_baseIPN7codegen6HammerESaIS3_EED2Ev.exit: ; preds = %invoke.cont6.i.i, %ehcleanup230
  call void @llvm.lifetime.end.p0i8(i64 24, i8* nonnull %6) #20
  %_M_t.i = getelementptr inbounds %"class.std::map", %"class.std::map"* %testParam, i64 0, i32 0
  %305 = bitcast i8* %_M_parent.i.i.i.i.i to %"struct.std::_Rb_tree_node"**
  %306 = load %"struct.std::_Rb_tree_node"*, %"struct.std::_Rb_tree_node"** %305, align 8, !tbaa !22
  invoke void @_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE8_M_eraseEPSt13_Rb_tree_nodeISB_E(%"class.std::_Rb_tree"* nonnull dereferenceable(48) %_M_t.i, %"struct.std::_Rb_tree_node"* %306)
          to label %_ZNSt3mapINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEPN7codegen10ParamValueESt4lessIS5_ESaISt4pairIKS5_S8_EEED2Ev.exit unwind label %lpad.i.i

lpad.i.i:                                         ; preds = %_ZNSt7__cxx1110_List_baseIPN7codegen6HammerESaIS3_EED2Ev.exit
  %307 = landingpad { i8*, i32 }
          catch i8* null
  %308 = extractvalue { i8*, i32 } %307, 0
  call void @__clang_call_terminate(i8* %308) #21
  unreachable

_ZNSt3mapINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEPN7codegen10ParamValueESt4lessIS5_ESaISt4pairIKS5_S8_EEED2Ev.exit: ; preds = %_ZNSt7__cxx1110_List_baseIPN7codegen6HammerESaIS3_EED2Ev.exit
  call void @llvm.lifetime.end.p0i8(i64 48, i8* nonnull %0) #20
  resume { i8*, i32 } %.pn288.pn.pn.pn.pn.pn.pn.pn.pn.pn
}

; Function Attrs: argmemonly nofree nosync nounwind willreturn
declare void @llvm.lifetime.start.p0i8(i64 immarg, i8* nocapture) #5

; Function Attrs: nofree nosync nounwind willreturn
declare i8* @llvm.stacksave() #6

; Function Attrs: argmemonly nofree nosync nounwind willreturn
declare void @llvm.lifetime.end.p0i8(i64 immarg, i8* nocapture) #5

declare dso_local i32 @__gxx_personality_v0(...)

; Function Attrs: uwtable
define linkonce_odr dso_local nonnull align 8 dereferenceable(8) %"class.codegen::ParamValue"** @_ZNSt3mapINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEPN7codegen10ParamValueESt4lessIS5_ESaISt4pairIKS5_S8_EEEixEOS5_(%"class.std::map"* nonnull dereferenceable(48) %this, %"class.std::__cxx11::basic_string"* nonnull align 8 dereferenceable(32) %__k) local_unnamed_addr #4 comdat align 2 personality i32 (...)* @__gxx_personality_v0 {
entry:
  %ref.tmp9 = alloca %"class.std::tuple.464", align 8
  %ref.tmp11 = alloca %"class.std::tuple.467", align 1
  %0 = getelementptr inbounds %"class.std::map", %"class.std::map"* %this, i64 0, i32 0, i32 0, i32 0, i32 0, i32 0
  %_M_parent.i.i.i = getelementptr inbounds i8, i8* %0, i64 16
  %1 = bitcast i8* %_M_parent.i.i.i to %"struct.std::_Rb_tree_node"**
  %2 = load %"struct.std::_Rb_tree_node"*, %"struct.std::_Rb_tree_node"** %1, align 8, !tbaa !22
  %add.ptr.i.i.i = getelementptr inbounds i8, i8* %0, i64 8
  %_M_header.i.i.i = bitcast i8* %add.ptr.i.i.i to %"struct.std::_Rb_tree_node_base"*
  %cmp.not11.i.i.i = icmp eq %"struct.std::_Rb_tree_node"* %2, null
  br i1 %cmp.not11.i.i.i, label %if.then, label %while.body.lr.ph.i.i.i

while.body.lr.ph.i.i.i:                           ; preds = %entry
  %_M_string_length.i14.i.i.i.i.i.i = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %__k, i64 0, i32 1
  %3 = load i64, i64* %_M_string_length.i14.i.i.i.i.i.i, align 8, !tbaa !44
  %_M_p.i.i.i.i.i.i.i.i = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %__k, i64 0, i32 0, i32 0
  %4 = load i8*, i8** %_M_p.i.i.i.i.i.i.i.i, align 8
  br label %while.body.i.i.i

while.body.i.i.i:                                 ; preds = %if.end.i.i.i, %while.body.lr.ph.i.i.i
  %__x.addr.013.i.i.i = phi %"struct.std::_Rb_tree_node"* [ %2, %while.body.lr.ph.i.i.i ], [ %__x.addr.1.i.i.i, %if.end.i.i.i ]
  %__y.addr.012.i.i.i = phi %"struct.std::_Rb_tree_node_base"* [ %_M_header.i.i.i, %while.body.lr.ph.i.i.i ], [ %__y.addr.1.i.i.i, %if.end.i.i.i ]
  %_M_string_length.i.i.i.i.i.i.i = getelementptr inbounds %"struct.std::_Rb_tree_node", %"struct.std::_Rb_tree_node"* %__x.addr.013.i.i.i, i64 0, i32 1, i32 0, i64 8
  %5 = bitcast i8* %_M_string_length.i.i.i.i.i.i.i to i64*
  %6 = load i64, i64* %5, align 8, !tbaa !44
  %cmp.i15.i.i.i.i.i.i = icmp ugt i64 %6, %3
  %.sroa.speculated.i.i.i.i.i.i = select i1 %cmp.i15.i.i.i.i.i.i, i64 %3, i64 %6
  %cmp.i13.i.i.i.i.i.i = icmp eq i64 %.sroa.speculated.i.i.i.i.i.i, 0
  br i1 %cmp.i13.i.i.i.i.i.i, label %if.then.i.i.i.i.i.i, label %_ZNSt11char_traitsIcE7compareEPKcS2_m.exit.i.i.i.i.i.i

_ZNSt11char_traitsIcE7compareEPKcS2_m.exit.i.i.i.i.i.i: ; preds = %while.body.i.i.i
  %_M_storage.i.i.i.i.i.i = getelementptr inbounds %"struct.std::_Rb_tree_node", %"struct.std::_Rb_tree_node"* %__x.addr.013.i.i.i, i64 0, i32 1
  %_M_p.i.i.i.i.i.i.i = bitcast %"struct.__gnu_cxx::__aligned_membuf.437"* %_M_storage.i.i.i.i.i.i to i8**
  %7 = load i8*, i8** %_M_p.i.i.i.i.i.i.i, align 8, !tbaa !41
  %call.i.i.i.i.i.i.i = tail call i32 @memcmp(i8* %7, i8* %4, i64 %.sroa.speculated.i.i.i.i.i.i) #20
  %tobool.not.i.i.i.i.i.i = icmp eq i32 %call.i.i.i.i.i.i.i, 0
  br i1 %tobool.not.i.i.i.i.i.i, label %if.then.i.i.i.i.i.i, label %_ZNKSt4lessINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEEclERKS5_S8_.exit.i.i.i

if.then.i.i.i.i.i.i:                              ; preds = %_ZNSt11char_traitsIcE7compareEPKcS2_m.exit.i.i.i.i.i.i, %while.body.i.i.i
  %sub.i.i.i.i.i.i.i = sub i64 %6, %3
  %cmp.i.i.i.i.i.i.i = icmp sgt i64 %sub.i.i.i.i.i.i.i, 2147483647
  br i1 %cmp.i.i.i.i.i.i.i, label %if.then.i.i.i, label %if.else.i.i.i.i.i.i.i

if.else.i.i.i.i.i.i.i:                            ; preds = %if.then.i.i.i.i.i.i
  %8 = icmp sgt i64 %sub.i.i.i.i.i.i.i, -2147483648
  %spec.select7.i.i.i.i.i.i.i = select i1 %8, i64 %sub.i.i.i.i.i.i.i, i64 -2147483648
  %9 = trunc i64 %spec.select7.i.i.i.i.i.i.i to i32
  br label %_ZNKSt4lessINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEEclERKS5_S8_.exit.i.i.i

_ZNKSt4lessINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEEclERKS5_S8_.exit.i.i.i: ; preds = %if.else.i.i.i.i.i.i.i, %_ZNSt11char_traitsIcE7compareEPKcS2_m.exit.i.i.i.i.i.i
  %__r.0.i.i.i.i.i.i = phi i32 [ %call.i.i.i.i.i.i.i, %_ZNSt11char_traitsIcE7compareEPKcS2_m.exit.i.i.i.i.i.i ], [ %9, %if.else.i.i.i.i.i.i.i ]
  %cmp.i.i.i.i.i = icmp slt i32 %__r.0.i.i.i.i.i.i, 0
  br i1 %cmp.i.i.i.i.i, label %if.else.i.i.i, label %if.then.i.i.i

if.then.i.i.i:                                    ; preds = %_ZNKSt4lessINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEEclERKS5_S8_.exit.i.i.i, %if.then.i.i.i.i.i.i
  %10 = getelementptr %"struct.std::_Rb_tree_node", %"struct.std::_Rb_tree_node"* %__x.addr.013.i.i.i, i64 0, i32 0
  %_M_left.i.i.i.i = getelementptr inbounds %"struct.std::_Rb_tree_node", %"struct.std::_Rb_tree_node"* %__x.addr.013.i.i.i, i64 0, i32 0, i32 2
  br label %if.end.i.i.i

if.else.i.i.i:                                    ; preds = %_ZNKSt4lessINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEEclERKS5_S8_.exit.i.i.i
  %_M_right.i.i.i.i = getelementptr inbounds %"struct.std::_Rb_tree_node", %"struct.std::_Rb_tree_node"* %__x.addr.013.i.i.i, i64 0, i32 0, i32 3
  br label %if.end.i.i.i

if.end.i.i.i:                                     ; preds = %if.else.i.i.i, %if.then.i.i.i
  %__y.addr.1.i.i.i = phi %"struct.std::_Rb_tree_node_base"* [ %__y.addr.012.i.i.i, %if.else.i.i.i ], [ %10, %if.then.i.i.i ]
  %__x.addr.1.in.in.i.i.i = phi %"struct.std::_Rb_tree_node_base"** [ %_M_right.i.i.i.i, %if.else.i.i.i ], [ %_M_left.i.i.i.i, %if.then.i.i.i ]
  %__x.addr.1.in.i.i.i = bitcast %"struct.std::_Rb_tree_node_base"** %__x.addr.1.in.in.i.i.i to %"struct.std::_Rb_tree_node"**
  %__x.addr.1.i.i.i = load %"struct.std::_Rb_tree_node"*, %"struct.std::_Rb_tree_node"** %__x.addr.1.in.i.i.i, align 8, !tbaa !46
  %cmp.not.i.i.i = icmp eq %"struct.std::_Rb_tree_node"* %__x.addr.1.i.i.i, null
  br i1 %cmp.not.i.i.i, label %_ZNSt3mapINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEPN7codegen10ParamValueESt4lessIS5_ESaISt4pairIKS5_S8_EEE11lower_boundERSC_.exit, label %while.body.i.i.i, !llvm.loop !115

_ZNSt3mapINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEPN7codegen10ParamValueESt4lessIS5_ESaISt4pairIKS5_S8_EEE11lower_boundERSC_.exit: ; preds = %if.end.i.i.i
  %cmp.i = icmp eq %"struct.std::_Rb_tree_node_base"* %__y.addr.1.i.i.i, %_M_header.i.i.i
  br i1 %cmp.i, label %if.then, label %lor.rhs

lor.rhs:                                          ; preds = %_ZNSt3mapINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEPN7codegen10ParamValueESt4lessIS5_ESaISt4pairIKS5_S8_EEE11lower_boundERSC_.exit
  %_M_string_length.i14.i.i.i = getelementptr inbounds %"struct.std::_Rb_tree_node_base", %"struct.std::_Rb_tree_node_base"* %__y.addr.1.i.i.i, i64 1, i32 1
  %11 = bitcast %"struct.std::_Rb_tree_node_base"** %_M_string_length.i14.i.i.i to i64*
  %12 = load i64, i64* %11, align 8, !tbaa !44
  %cmp.i15.i.i.i = icmp ugt i64 %3, %12
  %.sroa.speculated.i.i.i = select i1 %cmp.i15.i.i.i, i64 %12, i64 %3
  %cmp.i13.i.i.i = icmp eq i64 %.sroa.speculated.i.i.i, 0
  br i1 %cmp.i13.i.i.i, label %if.then.i.i.i21, label %_ZNSt11char_traitsIcE7compareEPKcS2_m.exit.i.i.i

_ZNSt11char_traitsIcE7compareEPKcS2_m.exit.i.i.i: ; preds = %lor.rhs
  %_M_storage.i.i22 = getelementptr inbounds %"struct.std::_Rb_tree_node_base", %"struct.std::_Rb_tree_node_base"* %__y.addr.1.i.i.i, i64 1
  %_M_p.i.i.i.i.i = bitcast %"struct.std::_Rb_tree_node_base"* %_M_storage.i.i22 to i8**
  %13 = load i8*, i8** %_M_p.i.i.i.i.i, align 8, !tbaa !41
  %call.i.i.i.i = tail call i32 @memcmp(i8* %4, i8* %13, i64 %.sroa.speculated.i.i.i) #20
  %tobool.not.i.i.i = icmp eq i32 %call.i.i.i.i, 0
  br i1 %tobool.not.i.i.i, label %if.then.i.i.i21, label %_ZNKSt4lessINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEEclERKS5_S8_.exit

if.then.i.i.i21:                                  ; preds = %_ZNSt11char_traitsIcE7compareEPKcS2_m.exit.i.i.i, %lor.rhs
  %sub.i.i.i.i = sub i64 %3, %12
  %cmp.i.i.i.i = icmp sgt i64 %sub.i.i.i.i, 2147483647
  br i1 %cmp.i.i.i.i, label %if.end, label %if.else.i.i.i.i

if.else.i.i.i.i:                                  ; preds = %if.then.i.i.i21
  %14 = icmp sgt i64 %sub.i.i.i.i, -2147483648
  %spec.select7.i.i.i.i = select i1 %14, i64 %sub.i.i.i.i, i64 -2147483648
  %15 = trunc i64 %spec.select7.i.i.i.i to i32
  br label %_ZNKSt4lessINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEEclERKS5_S8_.exit

_ZNKSt4lessINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEEclERKS5_S8_.exit: ; preds = %_ZNSt11char_traitsIcE7compareEPKcS2_m.exit.i.i.i, %if.else.i.i.i.i
  %__r.0.i.i.i = phi i32 [ %call.i.i.i.i, %_ZNSt11char_traitsIcE7compareEPKcS2_m.exit.i.i.i ], [ %15, %if.else.i.i.i.i ]
  %cmp.i.i = icmp slt i32 %__r.0.i.i.i, 0
  br i1 %cmp.i.i, label %if.then, label %if.end

if.then:                                          ; preds = %entry, %_ZNSt3mapINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEPN7codegen10ParamValueESt4lessIS5_ESaISt4pairIKS5_S8_EEE11lower_boundERSC_.exit, %_ZNKSt4lessINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEEclERKS5_S8_.exit
  %__y.addr.0.lcssa.i.i.i31 = phi %"struct.std::_Rb_tree_node_base"* [ %__y.addr.1.i.i.i, %_ZNKSt4lessINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEEclERKS5_S8_.exit ], [ %_M_header.i.i.i, %_ZNSt3mapINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEPN7codegen10ParamValueESt4lessIS5_ESaISt4pairIKS5_S8_EEE11lower_boundERSC_.exit ], [ %_M_header.i.i.i, %entry ]
  %_M_t = getelementptr inbounds %"class.std::map", %"class.std::map"* %this, i64 0, i32 0
  %16 = bitcast %"class.std::tuple.464"* %ref.tmp9 to i8*
  call void @llvm.lifetime.start.p0i8(i64 8, i8* nonnull %16) #20
  %_M_head_impl.i.i.i.i = getelementptr inbounds %"class.std::tuple.464", %"class.std::tuple.464"* %ref.tmp9, i64 0, i32 0, i32 0, i32 0
  store %"class.std::__cxx11::basic_string"* %__k, %"class.std::__cxx11::basic_string"** %_M_head_impl.i.i.i.i, align 8, !tbaa !46, !alias.scope !116
  %17 = getelementptr inbounds %"class.std::tuple.467", %"class.std::tuple.467"* %ref.tmp11, i64 0, i32 0
  call void @llvm.lifetime.start.p0i8(i64 1, i8* nonnull %17) #20
  %call13 = call %"struct.std::_Rb_tree_node_base"* @_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE22_M_emplace_hint_uniqueIJRKSt21piecewise_construct_tSt5tupleIJOS5_EESM_IJEEEEESt17_Rb_tree_iteratorISB_ESt23_Rb_tree_const_iteratorISB_EDpOT_(%"class.std::_Rb_tree"* nonnull dereferenceable(48) %_M_t, %"struct.std::_Rb_tree_node_base"* %__y.addr.0.lcssa.i.i.i31, %"struct.std::piecewise_construct_t"* nonnull align 1 dereferenceable(1) @_ZStL19piecewise_construct, %"class.std::tuple.464"* nonnull align 8 dereferenceable(8) %ref.tmp9, %"class.std::tuple.467"* nonnull align 1 dereferenceable(1) %ref.tmp11)
  call void @llvm.lifetime.end.p0i8(i64 1, i8* nonnull %17) #20
  call void @llvm.lifetime.end.p0i8(i64 8, i8* nonnull %16) #20
  br label %if.end

if.end:                                           ; preds = %if.then.i.i.i21, %if.then, %_ZNKSt4lessINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEEclERKS5_S8_.exit
  %__i.sroa.0.0 = phi %"struct.std::_Rb_tree_node_base"* [ %call13, %if.then ], [ %__y.addr.1.i.i.i, %_ZNKSt4lessINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEEclERKS5_S8_.exit ], [ %__y.addr.1.i.i.i, %if.then.i.i.i21 ]
  %second = getelementptr inbounds %"struct.std::_Rb_tree_node_base", %"struct.std::_Rb_tree_node_base"* %__i.sroa.0.0, i64 2
  %18 = bitcast %"struct.std::_Rb_tree_node_base"* %second to %"class.codegen::ParamValue"**
  ret %"class.codegen::ParamValue"** %18
}

declare dso_local void @_ZN7codegen6HammerC1EN4llvm9StringRefESt3mapINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEPNS_10ParamValueESt4lessIS9_ESaISt4pairIKS9_SB_EEE(%"class.codegen::Hammer"* nonnull dereferenceable(152), i8*, i64, %"class.std::map"*) unnamed_addr #0

declare dso_local zeroext i1 @_ZN7codegen6Hammer6hardenEv(%"class.codegen::Hammer"* nonnull dereferenceable(152)) local_unnamed_addr #0

declare dso_local void @_ZN7codegen6Hammer13create_jitterENSt7__cxx114listIPS0_SaIS3_EEERNS_12HammerConfigE(%"class.std::unique_ptr.131"* sret(%"class.std::unique_ptr.131") align 8, %"class.codegen::Hammer"* nonnull dereferenceable(152), %"class.std::__cxx11::list"*, %"class.codegen::HammerConfig"* nonnull align 8 dereferenceable(1616)) local_unnamed_addr #0

; Function Attrs: nobuiltin nofree allocsize(0)
declare dso_local nonnull i8* @_Znwm(i64) local_unnamed_addr #7

; Function Attrs: inlinehint nounwind uwtable
define linkonce_odr dso_local void @_ZN7codegen6HammerD2Ev(%"class.codegen::Hammer"* nonnull dereferenceable(152) %this) unnamed_addr #3 comdat align 2 personality i32 (...)* @__gxx_personality_v0 {
entry:
  %params = getelementptr inbounds %"class.codegen::Hammer", %"class.codegen::Hammer"* %this, i64 0, i32 6
  %_M_t.i = getelementptr inbounds %"class.std::map", %"class.std::map"* %params, i64 0, i32 0
  %0 = getelementptr inbounds %"class.std::map", %"class.std::map"* %params, i64 0, i32 0, i32 0, i32 0, i32 0, i32 0
  %_M_parent.i.i.i = getelementptr inbounds i8, i8* %0, i64 16
  %1 = bitcast i8* %_M_parent.i.i.i to %"struct.std::_Rb_tree_node"**
  %2 = load %"struct.std::_Rb_tree_node"*, %"struct.std::_Rb_tree_node"** %1, align 8, !tbaa !22
  invoke void @_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE8_M_eraseEPSt13_Rb_tree_nodeISB_E(%"class.std::_Rb_tree"* nonnull dereferenceable(48) %_M_t.i, %"struct.std::_Rb_tree_node"* %2)
          to label %_ZNSt3mapINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEPN7codegen10ParamValueESt4lessIS5_ESaISt4pairIKS5_S8_EEED2Ev.exit unwind label %lpad.i.i

lpad.i.i:                                         ; preds = %entry
  %3 = landingpad { i8*, i32 }
          catch i8* null
  %4 = extractvalue { i8*, i32 } %3, 0
  tail call void @__clang_call_terminate(i8* %4) #21
  unreachable

_ZNSt3mapINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEPN7codegen10ParamValueESt4lessIS5_ESaISt4pairIKS5_S8_EEED2Ev.exit: ; preds = %entry
  %_M_head_impl.i.i.i.i.i.i = getelementptr inbounds %"class.codegen::Hammer", %"class.codegen::Hammer"* %this, i64 0, i32 5, i32 0, i32 0, i32 0, i32 0, i32 0
  %5 = load %"class.llvm::Module"*, %"class.llvm::Module"** %_M_head_impl.i.i.i.i.i.i, align 8, !tbaa !46
  %cmp.not.i = icmp eq %"class.llvm::Module"* %5, null
  br i1 %cmp.not.i, label %_ZNSt10unique_ptrIN4llvm6ModuleESt14default_deleteIS1_EED2Ev.exit, label %_ZNKSt14default_deleteIN4llvm6ModuleEEclEPS1_.exit.i

_ZNKSt14default_deleteIN4llvm6ModuleEEclEPS1_.exit.i: ; preds = %_ZNSt3mapINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEPN7codegen10ParamValueESt4lessIS5_ESaISt4pairIKS5_S8_EEED2Ev.exit
  tail call void @_ZN4llvm6ModuleD1Ev(%"class.llvm::Module"* nonnull dereferenceable(744) %5) #20
  %6 = bitcast %"class.llvm::Module"* %5 to i8*
  tail call void @_ZdlPv(i8* %6) #25
  br label %_ZNSt10unique_ptrIN4llvm6ModuleESt14default_deleteIS1_EED2Ev.exit

_ZNSt10unique_ptrIN4llvm6ModuleESt14default_deleteIS1_EED2Ev.exit: ; preds = %_ZNSt3mapINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEPN7codegen10ParamValueESt4lessIS5_ESaISt4pairIKS5_S8_EEED2Ev.exit, %_ZNKSt14default_deleteIN4llvm6ModuleEEclEPS1_.exit.i
  store %"class.llvm::Module"* null, %"class.llvm::Module"** %_M_head_impl.i.i.i.i.i.i, align 8, !tbaa !46
  %_M_head_impl.i.i.i.i.i.i2 = getelementptr inbounds %"class.codegen::Hammer", %"class.codegen::Hammer"* %this, i64 0, i32 4, i32 0, i32 0, i32 0, i32 0, i32 0
  %7 = load %"class.llvm::legacy::FunctionPassManager"*, %"class.llvm::legacy::FunctionPassManager"** %_M_head_impl.i.i.i.i.i.i2, align 8, !tbaa !46
  %cmp.not.i3 = icmp eq %"class.llvm::legacy::FunctionPassManager"* %7, null
  br i1 %cmp.not.i3, label %_ZNSt10unique_ptrIN4llvm6legacy19FunctionPassManagerESt14default_deleteIS2_EED2Ev.exit, label %_ZNKSt14default_deleteIN4llvm6legacy19FunctionPassManagerEEclEPS2_.exit.i

_ZNKSt14default_deleteIN4llvm6legacy19FunctionPassManagerEEclEPS2_.exit.i: ; preds = %_ZNSt10unique_ptrIN4llvm6ModuleESt14default_deleteIS1_EED2Ev.exit
  %8 = bitcast %"class.llvm::legacy::FunctionPassManager"* %7 to void (%"class.llvm::legacy::FunctionPassManager"*)***
  %vtable.i.i = load void (%"class.llvm::legacy::FunctionPassManager"*)**, void (%"class.llvm::legacy::FunctionPassManager"*)*** %8, align 8, !tbaa !119
  %vfn.i.i = getelementptr inbounds void (%"class.llvm::legacy::FunctionPassManager"*)*, void (%"class.llvm::legacy::FunctionPassManager"*)** %vtable.i.i, i64 1
  %9 = load void (%"class.llvm::legacy::FunctionPassManager"*)*, void (%"class.llvm::legacy::FunctionPassManager"*)** %vfn.i.i, align 8
  tail call void %9(%"class.llvm::legacy::FunctionPassManager"* nonnull dereferenceable(24) %7) #20
  br label %_ZNSt10unique_ptrIN4llvm6legacy19FunctionPassManagerESt14default_deleteIS2_EED2Ev.exit

_ZNSt10unique_ptrIN4llvm6legacy19FunctionPassManagerESt14default_deleteIS2_EED2Ev.exit: ; preds = %_ZNSt10unique_ptrIN4llvm6ModuleESt14default_deleteIS1_EED2Ev.exit, %_ZNKSt14default_deleteIN4llvm6legacy19FunctionPassManagerEEclEPS2_.exit.i
  store %"class.llvm::legacy::FunctionPassManager"* null, %"class.llvm::legacy::FunctionPassManager"** %_M_head_impl.i.i.i.i.i.i2, align 8, !tbaa !46
  %_M_head_impl.i.i.i.i.i.i4 = getelementptr inbounds %"class.codegen::Hammer", %"class.codegen::Hammer"* %this, i64 0, i32 3, i32 0, i32 0, i32 0, i32 0, i32 0
  %10 = load %"class.llvm::IRBuilder"*, %"class.llvm::IRBuilder"** %_M_head_impl.i.i.i.i.i.i4, align 8, !tbaa !46
  %cmp.not.i5 = icmp eq %"class.llvm::IRBuilder"* %10, null
  br i1 %cmp.not.i5, label %_ZNSt10unique_ptrIN4llvm9IRBuilderINS0_14ConstantFolderENS0_24IRBuilderDefaultInserterEEESt14default_deleteIS4_EED2Ev.exit, label %delete.notnull.i.i

delete.notnull.i.i:                               ; preds = %_ZNSt10unique_ptrIN4llvm6legacy19FunctionPassManagerESt14default_deleteIS2_EED2Ev.exit
  %Inserter.i.i.i = getelementptr inbounds %"class.llvm::IRBuilder", %"class.llvm::IRBuilder"* %10, i64 0, i32 2
  tail call void @_ZN4llvm24IRBuilderDefaultInserterD1Ev(%"class.llvm::IRBuilderDefaultInserter"* nonnull dereferenceable(8) %Inserter.i.i.i) #20
  %11 = getelementptr inbounds %"class.llvm::IRBuilder", %"class.llvm::IRBuilder"* %10, i64 0, i32 1, i32 0
  tail call void @_ZN4llvm15IRBuilderFolderD2Ev(%"class.llvm::IRBuilderFolder"* nonnull dereferenceable(8) %11) #20
  %12 = getelementptr inbounds %"class.llvm::IRBuilder", %"class.llvm::IRBuilder"* %10, i64 0, i32 0, i32 0, i32 0
  %BeginX.i.i.i.i.i.i.i = getelementptr inbounds %"class.llvm::IRBuilder", %"class.llvm::IRBuilder"* %10, i64 0, i32 0, i32 0, i32 0, i32 0, i32 0, i32 0, i32 0
  %13 = load i8*, i8** %BeginX.i.i.i.i.i.i.i, align 8, !tbaa !121
  %add.ptr2.i.i.i.i.i.i.i.i = getelementptr inbounds %"class.llvm::SmallVectorImpl.397", %"class.llvm::SmallVectorImpl.397"* %12, i64 1, i32 0, i32 0
  %add.ptr.i.i.i.i.i.i.i.i = bitcast %"class.llvm::SmallVectorTemplateCommon.399"* %add.ptr2.i.i.i.i.i.i.i.i to i8*
  %cmp.i.i.i.i.i.i.i = icmp eq i8* %13, %add.ptr.i.i.i.i.i.i.i.i
  br i1 %cmp.i.i.i.i.i.i.i, label %_ZNKSt14default_deleteIN4llvm9IRBuilderINS0_14ConstantFolderENS0_24IRBuilderDefaultInserterEEEEclEPS4_.exit.i, label %if.then.i.i.i.i.i.i

if.then.i.i.i.i.i.i:                              ; preds = %delete.notnull.i.i
  tail call void @free(i8* %13) #20
  br label %_ZNKSt14default_deleteIN4llvm9IRBuilderINS0_14ConstantFolderENS0_24IRBuilderDefaultInserterEEEEclEPS4_.exit.i

_ZNKSt14default_deleteIN4llvm9IRBuilderINS0_14ConstantFolderENS0_24IRBuilderDefaultInserterEEEEclEPS4_.exit.i: ; preds = %if.then.i.i.i.i.i.i, %delete.notnull.i.i
  %14 = bitcast %"class.llvm::IRBuilder"* %10 to i8*
  tail call void @_ZdlPv(i8* %14) #25
  br label %_ZNSt10unique_ptrIN4llvm9IRBuilderINS0_14ConstantFolderENS0_24IRBuilderDefaultInserterEEESt14default_deleteIS4_EED2Ev.exit

_ZNSt10unique_ptrIN4llvm9IRBuilderINS0_14ConstantFolderENS0_24IRBuilderDefaultInserterEEESt14default_deleteIS4_EED2Ev.exit: ; preds = %_ZNSt10unique_ptrIN4llvm6legacy19FunctionPassManagerESt14default_deleteIS2_EED2Ev.exit, %_ZNKSt14default_deleteIN4llvm9IRBuilderINS0_14ConstantFolderENS0_24IRBuilderDefaultInserterEEEEclEPS4_.exit.i
  store %"class.llvm::IRBuilder"* null, %"class.llvm::IRBuilder"** %_M_head_impl.i.i.i.i.i.i4, align 8, !tbaa !46
  %_M_head_impl.i.i.i.i.i.i6 = getelementptr inbounds %"class.codegen::Hammer", %"class.codegen::Hammer"* %this, i64 0, i32 2, i32 0, i32 0, i32 0, i32 0, i32 0
  %15 = load %"class.llvm::StringRef"*, %"class.llvm::StringRef"** %_M_head_impl.i.i.i.i.i.i6, align 8, !tbaa !46
  %cmp.not.i7 = icmp eq %"class.llvm::StringRef"* %15, null
  br i1 %cmp.not.i7, label %_ZNSt10unique_ptrIN4llvm9StringRefESt14default_deleteIS1_EED2Ev.exit, label %_ZNKSt14default_deleteIN4llvm9StringRefEEclEPS1_.exit.i

_ZNKSt14default_deleteIN4llvm9StringRefEEclEPS1_.exit.i: ; preds = %_ZNSt10unique_ptrIN4llvm9IRBuilderINS0_14ConstantFolderENS0_24IRBuilderDefaultInserterEEESt14default_deleteIS4_EED2Ev.exit
  %16 = bitcast %"class.llvm::StringRef"* %15 to i8*
  tail call void @_ZdlPv(i8* %16) #25
  br label %_ZNSt10unique_ptrIN4llvm9StringRefESt14default_deleteIS1_EED2Ev.exit

_ZNSt10unique_ptrIN4llvm9StringRefESt14default_deleteIS1_EED2Ev.exit: ; preds = %_ZNSt10unique_ptrIN4llvm9IRBuilderINS0_14ConstantFolderENS0_24IRBuilderDefaultInserterEEESt14default_deleteIS4_EED2Ev.exit, %_ZNKSt14default_deleteIN4llvm9StringRefEEclEPS1_.exit.i
  store %"class.llvm::StringRef"* null, %"class.llvm::StringRef"** %_M_head_impl.i.i.i.i.i.i6, align 8, !tbaa !46
  %_M_head_impl.i.i.i.i.i.i8 = getelementptr inbounds %"class.codegen::Hammer", %"class.codegen::Hammer"* %this, i64 0, i32 1, i32 0, i32 0, i32 0, i32 0, i32 0
  %17 = load %"class.llvm::LLVMContext"*, %"class.llvm::LLVMContext"** %_M_head_impl.i.i.i.i.i.i8, align 8, !tbaa !46
  %cmp.not.i9 = icmp eq %"class.llvm::LLVMContext"* %17, null
  br i1 %cmp.not.i9, label %_ZNSt10unique_ptrIN4llvm11LLVMContextESt14default_deleteIS1_EED2Ev.exit, label %_ZNKSt14default_deleteIN4llvm11LLVMContextEEclEPS1_.exit.i

_ZNKSt14default_deleteIN4llvm11LLVMContextEEclEPS1_.exit.i: ; preds = %_ZNSt10unique_ptrIN4llvm9StringRefESt14default_deleteIS1_EED2Ev.exit
  tail call void @_ZN4llvm11LLVMContextD1Ev(%"class.llvm::LLVMContext"* nonnull dereferenceable(8) %17) #20
  %18 = bitcast %"class.llvm::LLVMContext"* %17 to i8*
  tail call void @_ZdlPv(i8* %18) #25
  br label %_ZNSt10unique_ptrIN4llvm11LLVMContextESt14default_deleteIS1_EED2Ev.exit

_ZNSt10unique_ptrIN4llvm11LLVMContextESt14default_deleteIS1_EED2Ev.exit: ; preds = %_ZNSt10unique_ptrIN4llvm9StringRefESt14default_deleteIS1_EED2Ev.exit, %_ZNKSt14default_deleteIN4llvm11LLVMContextEEclEPS1_.exit.i
  store %"class.llvm::LLVMContext"* null, %"class.llvm::LLVMContext"** %_M_head_impl.i.i.i.i.i.i8, align 8, !tbaa !46
  %_M_manager.i.i = getelementptr inbounds %"class.codegen::Hammer", %"class.codegen::Hammer"* %this, i64 0, i32 0, i32 1, i32 0, i32 1
  %19 = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %_M_manager.i.i, align 8, !tbaa !123
  %tobool.not.i.i = icmp eq i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %19, null
  br i1 %tobool.not.i.i, label %_ZNSt14_Function_baseD2Ev.exit.i, label %if.then.i.i

if.then.i.i:                                      ; preds = %_ZNSt10unique_ptrIN4llvm11LLVMContextESt14default_deleteIS1_EED2Ev.exit
  %_M_functor.i.i = getelementptr inbounds %"class.codegen::Hammer", %"class.codegen::Hammer"* %this, i64 0, i32 0, i32 1, i32 0, i32 0
  %call.i.i = invoke zeroext i1 %19(%"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %_M_functor.i.i, %"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %_M_functor.i.i, i32 3)
          to label %_ZNSt14_Function_baseD2Ev.exit.i unwind label %terminate.lpad.i.i

terminate.lpad.i.i:                               ; preds = %if.then.i.i
  %20 = landingpad { i8*, i32 }
          catch i8* null
  %21 = extractvalue { i8*, i32 } %20, 0
  tail call void @__clang_call_terminate(i8* %21) #21
  unreachable

_ZNSt14_Function_baseD2Ev.exit.i:                 ; preds = %if.then.i.i, %_ZNSt10unique_ptrIN4llvm11LLVMContextESt14default_deleteIS1_EED2Ev.exit
  %_M_p.i.i.i.i.i = getelementptr inbounds %"class.codegen::Hammer", %"class.codegen::Hammer"* %this, i64 0, i32 0, i32 0, i32 0, i32 0
  %22 = load i8*, i8** %_M_p.i.i.i.i.i, align 8, !tbaa !41
  %23 = getelementptr inbounds %"class.codegen::Hammer", %"class.codegen::Hammer"* %this, i64 0, i32 0, i32 0, i32 2
  %arraydecay.i.i.i.i.i = bitcast %union.anon* %23 to i8*
  %cmp.i.i.i.i = icmp eq i8* %22, %arraydecay.i.i.i.i.i
  br i1 %cmp.i.i.i.i, label %_ZN4llvm11ExitOnErrorD2Ev.exit, label %if.then.i.i.i

if.then.i.i.i:                                    ; preds = %_ZNSt14_Function_baseD2Ev.exit.i
  tail call void @_ZdlPv(i8* %22) #20
  br label %_ZN4llvm11ExitOnErrorD2Ev.exit

_ZN4llvm11ExitOnErrorD2Ev.exit:                   ; preds = %_ZNSt14_Function_baseD2Ev.exit.i, %if.then.i.i.i
  ret void
}

; Function Attrs: nofree nosync nounwind willreturn
declare void @llvm.stackrestore(i8*) #6

; Function Attrs: uwtable mustprogress
define dso_local i64 @_Z16allocAndInitSortlPiiS_iS_S_S_i(i64 %stageId, i32* %sourceTypes, i32 %typeCount, i32* %outputCols, i32 %outputColCount, i32* %sortCols, i32* %ascendings, i32* %nullFirsts, i32 %sortColCount) local_unnamed_addr #8 personality i8* bitcast (i32 (...)* @__gxx_personality_v0 to i8*) {
entry:
  %call = tail call noalias nonnull dereferenceable(80) i8* @_Znwm(i64 80) #23
  %0 = bitcast i8* %call to %class.Sort*
  invoke void @_ZN4SortC1EPiiS0_iS0_S0_S0_i(%class.Sort* nonnull dereferenceable(80) %0, i32* %sourceTypes, i32 %typeCount, i32* %outputCols, i32 %outputColCount, i32* %sortCols, i32* %ascendings, i32* %nullFirsts, i32 %sortColCount)
          to label %invoke.cont unwind label %lpad

invoke.cont:                                      ; preds = %entry
  %1 = ptrtoint i8* %call to i64
  ret i64 %1

lpad:                                             ; preds = %entry
  %2 = landingpad { i8*, i32 }
          cleanup
  tail call void @_ZdlPv(i8* nonnull %call) #25
  resume { i8*, i32 } %2
}

declare dso_local void @_ZN4SortC1EPiiS0_iS0_S0_S0_i(%class.Sort* nonnull dereferenceable(80), i32*, i32, i32*, i32, i32*, i32*, i32*, i32) unnamed_addr #0

; Function Attrs: nobuiltin nounwind
declare dso_local void @_ZdlPv(i8*) local_unnamed_addr #9

; Function Attrs: uwtable
define dso_local void @_Z8addTablelPlS_j(i64 %sortAddress, i64* nocapture readonly %datas, i64* nocapture readonly %nulls, i32 %rowNum) local_unnamed_addr #4 personality i8* bitcast (i32 (...)* @__gxx_personality_v0 to i8*) {
entry:
  %0 = inttoptr i64 %sortAddress to %class.Sort*
  %pagesIndex.i = getelementptr inbounds %class.Sort, %class.Sort* %0, i64 0, i32 9
  %1 = load %class.PagesIndex*, %class.PagesIndex** %pagesIndex.i, align 8, !tbaa !125
  %typesCount.i = getelementptr inbounds %class.Sort, %class.Sort* %0, i64 0, i32 2
  %2 = load i32, i32* %typesCount.i, align 8, !tbaa !127
  %sourceTypes.i = getelementptr inbounds %class.Sort, %class.Sort* %0, i64 0, i32 1
  %3 = load i32*, i32** %sourceTypes.i, align 8, !tbaa !128
  %call3 = tail call noalias nonnull dereferenceable(64) i8* @_Znwm(i64 64) #23
  %4 = bitcast i8* %call3 to i32 (...)***
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [4 x i8*] }, { [4 x i8*] }* @_ZTV5Table, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %4, align 8, !tbaa !119
  %data.i45 = getelementptr inbounds i8, i8* %call3, i64 16
  tail call void @llvm.memset.p0i8.i64(i8* nonnull align 8 dereferenceable(24) %data.i45, i8 0, i64 24, i1 false) #20
  %positionCount2.i = getelementptr inbounds i8, i8* %call3, i64 48
  %5 = bitcast i8* %positionCount2.i to i32*
  store i32 %rowNum, i32* %5, align 8, !tbaa !129
  %columnCount3.i = getelementptr inbounds i8, i8* %call3, i64 52
  %6 = bitcast i8* %columnCount3.i to i32*
  store i32 %2, i32* %6, align 4, !tbaa !133
  %conv.i = zext i32 %2 to i64
  %7 = shl nuw nsw i64 %conv.i, 2
  %call.i48 = invoke noalias nonnull i8* @_Znam(i64 %7) #23
          to label %_ZN5TableC2Ejj.exit unwind label %lpad

_ZN5TableC2Ejj.exit:                              ; preds = %entry
  %types.i46 = getelementptr inbounds i8, i8* %call3, i64 40
  %8 = bitcast i8* %types.i46 to i32**
  %9 = bitcast i8* %types.i46 to i8**
  store i8* %call.i48, i8** %9, align 8, !tbaa !134
  %columnSize.i47 = getelementptr inbounds i8, i8* %call3, i64 56
  %10 = bitcast i8* %columnSize.i47 to i32*
  store i32 0, i32* %10, align 8, !tbaa !135
  %cmp49 = icmp sgt i32 %2, 0
  br i1 %cmp49, label %for.body.lr.ph, label %for.cond.cleanup

for.body.lr.ph:                                   ; preds = %_ZN5TableC2Ejj.exit
  %conv = zext i32 %rowNum to i64
  %_M_finish.i.i = getelementptr inbounds i8, i8* %call3, i64 24
  %11 = bitcast i8* %_M_finish.i.i to %class.Column***
  %_M_end_of_storage.i.i = getelementptr inbounds i8, i8* %call3, i64 32
  %12 = bitcast i8* %_M_end_of_storage.i.i to %class.Column***
  %13 = bitcast i8* %data.i45 to %class.Column***
  br label %for.body

for.cond.cleanup:                                 ; preds = %_ZN5Table9setColumnEP6ColumnN3opt10ColumnTypeE.exit, %_ZN5TableC2Ejj.exit
  %14 = bitcast i8* %call3 to %class.Table*
  tail call void @_ZN10PagesIndex8addTableEP5Tableij(%class.PagesIndex* nonnull dereferenceable(68) %1, %class.Table* nonnull %14, i32 %2, i32 %rowNum)
  ret void

lpad:                                             ; preds = %entry
  %15 = landingpad { i8*, i32 }
          cleanup
  tail call void @_ZdlPv(i8* nonnull %call3) #25
  resume { i8*, i32 } %15

for.body:                                         ; preds = %for.body.lr.ph, %_ZN5Table9setColumnEP6ColumnN3opt10ColumnTypeE.exit
  %indvars.iv = phi i64 [ 0, %for.body.lr.ph ], [ %indvars.iv.next, %_ZN5Table9setColumnEP6ColumnN3opt10ColumnTypeE.exit ]
  %arrayidx = getelementptr inbounds i32, i32* %3, i64 %indvars.iv
  %16 = load i32, i32* %arrayidx, align 4, !tbaa !16
  %call4 = tail call i32 @_Z13getColumnTypei(i32 %16)
  %call5 = tail call noalias nonnull dereferenceable(40) i8* @_Znwm(i64 40) #23
  %arrayidx7 = getelementptr inbounds i64, i64* %datas, i64 %indvars.iv
  %17 = load i64, i64* %arrayidx7, align 8, !tbaa !29
  %18 = inttoptr i64 %17 to i8*
  %arrayidx9 = getelementptr inbounds i64, i64* %nulls, i64 %indvars.iv
  %19 = load i64, i64* %arrayidx9, align 8, !tbaa !29
  %20 = inttoptr i64 %19 to i32*
  %21 = bitcast i8* %call5 to i32 (...)***
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [4 x i8*] }, { [4 x i8*] }* @_ZTV6Column, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %21, align 8, !tbaa !119
  %data.i = getelementptr inbounds i8, i8* %call5, i64 8
  %22 = bitcast i8* %data.i to i8**
  store i8* %18, i8** %22, align 8, !tbaa !136
  %nulls.i = getelementptr inbounds i8, i8* %call5, i64 16
  %23 = bitcast i8* %nulls.i to i32**
  store i32* %20, i32** %23, align 8, !tbaa !139
  %type.i = getelementptr inbounds i8, i8* %call5, i64 24
  %24 = bitcast i8* %type.i to i32*
  store i32 %call4, i32* %24, align 8, !tbaa !140
  %size.i = getelementptr inbounds i8, i8* %call5, i64 32
  %25 = bitcast i8* %size.i to i64*
  store i64 %conv, i64* %25, align 8, !tbaa !141
  %26 = load i32*, i32** %8, align 8, !tbaa !134
  %27 = load i32, i32* %10, align 8, !tbaa !135
  %idxprom.i = zext i32 %27 to i64
  %arrayidx.i = getelementptr inbounds i32, i32* %26, i64 %idxprom.i
  store i32 %call4, i32* %arrayidx.i, align 4, !tbaa !142
  %inc.i = add i32 %27, 1
  store i32 %inc.i, i32* %10, align 8, !tbaa !135
  %28 = load %class.Column**, %class.Column*** %11, align 8, !tbaa !143
  %29 = load %class.Column**, %class.Column*** %12, align 8, !tbaa !146
  %cmp.not.i.i = icmp eq %class.Column** %28, %29
  br i1 %cmp.not.i.i, label %if.else.i.i, label %if.then.i.i

if.then.i.i:                                      ; preds = %for.body
  %30 = bitcast %class.Column** %28 to i8**
  store i8* %call5, i8** %30, align 8, !tbaa !46
  %31 = load %class.Column**, %class.Column*** %11, align 8, !tbaa !143
  %incdec.ptr.i.i = getelementptr inbounds %class.Column*, %class.Column** %31, i64 1
  br label %_ZN5Table9setColumnEP6ColumnN3opt10ColumnTypeE.exit

if.else.i.i:                                      ; preds = %for.body
  %32 = load %class.Column**, %class.Column*** %13, align 8, !tbaa !147
  %sub.ptr.lhs.cast.i28.i.i.i.i = ptrtoint %class.Column** %28 to i64
  %sub.ptr.rhs.cast.i29.i.i.i.i = ptrtoint %class.Column** %32 to i64
  %sub.ptr.sub.i30.i.i.i.i = sub i64 %sub.ptr.lhs.cast.i28.i.i.i.i, %sub.ptr.rhs.cast.i29.i.i.i.i
  %sub.ptr.div.i31.i.i.i.i = ashr exact i64 %sub.ptr.sub.i30.i.i.i.i, 3
  %cmp.i.i.i.i.i = icmp eq i64 %sub.ptr.sub.i30.i.i.i.i, 0
  %.sroa.speculated.i.i.i.i = select i1 %cmp.i.i.i.i.i, i64 1, i64 %sub.ptr.div.i31.i.i.i.i
  %add.i.i.i.i = add nsw i64 %.sroa.speculated.i.i.i.i, %sub.ptr.div.i31.i.i.i.i
  %cmp7.i.i.i.i = icmp ult i64 %add.i.i.i.i, %sub.ptr.div.i31.i.i.i.i
  %cmp9.i.i.i.i = icmp ugt i64 %add.i.i.i.i, 2305843009213693951
  %or.cond.i.i.i.i = or i1 %cmp7.i.i.i.i, %cmp9.i.i.i.i
  %cond.i.i.i.i = select i1 %or.cond.i.i.i.i, i64 2305843009213693951, i64 %add.i.i.i.i
  %cmp.not.i.i.i.i = icmp eq i64 %cond.i.i.i.i, 0
  br i1 %cmp.not.i.i.i.i, label %invoke.cont.i.i.i, label %_ZNSt16allocator_traitsISaIP6ColumnEE8allocateERS2_m.exit.i.i.i.i

_ZNSt16allocator_traitsISaIP6ColumnEE8allocateERS2_m.exit.i.i.i.i: ; preds = %if.else.i.i
  %mul.i.i.i.i.i.i = shl nuw i64 %cond.i.i.i.i, 3
  %call2.i.i.i.i.i.i = tail call noalias nonnull i8* @_Znwm(i64 %mul.i.i.i.i.i.i) #22
  %33 = bitcast i8* %call2.i.i.i.i.i.i to %class.Column**
  %.pre.i.i.i = load %class.Column**, %class.Column*** %13, align 8, !tbaa !147
  %.pre83.i.i.i = ptrtoint %class.Column** %.pre.i.i.i to i64
  %.pre84.i.i.i = sub i64 %sub.ptr.lhs.cast.i28.i.i.i.i, %.pre83.i.i.i
  br label %invoke.cont.i.i.i

invoke.cont.i.i.i:                                ; preds = %_ZNSt16allocator_traitsISaIP6ColumnEE8allocateERS2_m.exit.i.i.i.i, %if.else.i.i
  %sub.ptr.sub.i.i.i.i.i.i.i.i74.pre-phi.i.i.i = phi i64 [ %.pre84.i.i.i, %_ZNSt16allocator_traitsISaIP6ColumnEE8allocateERS2_m.exit.i.i.i.i ], [ %sub.ptr.sub.i30.i.i.i.i, %if.else.i.i ]
  %34 = phi %class.Column** [ %.pre.i.i.i, %_ZNSt16allocator_traitsISaIP6ColumnEE8allocateERS2_m.exit.i.i.i.i ], [ %32, %if.else.i.i ]
  %cond.i67.i.i.i = phi %class.Column** [ %33, %_ZNSt16allocator_traitsISaIP6ColumnEE8allocateERS2_m.exit.i.i.i.i ], [ null, %if.else.i.i ]
  %add.ptr.i.i.i = getelementptr inbounds %class.Column*, %class.Column** %cond.i67.i.i.i, i64 %sub.ptr.div.i31.i.i.i.i
  %35 = bitcast %class.Column** %add.ptr.i.i.i to i8**
  store i8* %call5, i8** %35, align 8, !tbaa !46
  %tobool.not.i.i.i.i.i.i.i.i75.i.i.i = icmp eq i64 %sub.ptr.sub.i.i.i.i.i.i.i.i74.pre-phi.i.i.i, 0
  br i1 %tobool.not.i.i.i.i.i.i.i.i75.i.i.i, label %invoke.cont10.i.i.i, label %if.then.i.i.i.i.i.i.i.i76.i.i.i

if.then.i.i.i.i.i.i.i.i76.i.i.i:                  ; preds = %invoke.cont.i.i.i
  %36 = bitcast %class.Column** %cond.i67.i.i.i to i8*
  %37 = bitcast %class.Column** %34 to i8*
  tail call void @llvm.memmove.p0i8.p0i8.i64(i8* align 8 %36, i8* align 8 %37, i64 %sub.ptr.sub.i.i.i.i.i.i.i.i74.pre-phi.i.i.i, i1 false) #20
  br label %invoke.cont10.i.i.i

invoke.cont10.i.i.i:                              ; preds = %if.then.i.i.i.i.i.i.i.i76.i.i.i, %invoke.cont.i.i.i
  %sub.ptr.div.i.i.i.i.i.i.i.i77.i.i.i = ashr exact i64 %sub.ptr.sub.i.i.i.i.i.i.i.i74.pre-phi.i.i.i, 3
  %add.ptr.i.i.i.i.i.i.i.i78.i.i.i = getelementptr inbounds %class.Column*, %class.Column** %cond.i67.i.i.i, i64 1
  %incdec.ptr.i.i.i = getelementptr inbounds %class.Column*, %class.Column** %add.ptr.i.i.i.i.i.i.i.i78.i.i.i, i64 %sub.ptr.div.i.i.i.i.i.i.i.i77.i.i.i
  %38 = load %class.Column**, %class.Column*** %11, align 8, !tbaa !143
  %sub.ptr.lhs.cast.i.i.i.i.i.i.i.i.i.i.i = ptrtoint %class.Column** %38 to i64
  %sub.ptr.sub.i.i.i.i.i.i.i.i.i.i.i = sub i64 %sub.ptr.lhs.cast.i.i.i.i.i.i.i.i.i.i.i, %sub.ptr.lhs.cast.i28.i.i.i.i
  %tobool.not.i.i.i.i.i.i.i.i.i.i.i = icmp eq i64 %sub.ptr.sub.i.i.i.i.i.i.i.i.i.i.i, 0
  br i1 %tobool.not.i.i.i.i.i.i.i.i.i.i.i, label %invoke.cont15.i.i.i, label %if.then.i.i.i.i.i.i.i.i.i.i.i

if.then.i.i.i.i.i.i.i.i.i.i.i:                    ; preds = %invoke.cont10.i.i.i
  %39 = bitcast %class.Column** %incdec.ptr.i.i.i to i8*
  %40 = bitcast %class.Column** %28 to i8*
  tail call void @llvm.memmove.p0i8.p0i8.i64(i8* nonnull align 8 %39, i8* align 8 %40, i64 %sub.ptr.sub.i.i.i.i.i.i.i.i.i.i.i, i1 false) #20
  br label %invoke.cont15.i.i.i

invoke.cont15.i.i.i:                              ; preds = %if.then.i.i.i.i.i.i.i.i.i.i.i, %invoke.cont10.i.i.i
  %tobool.not.i68.i.i.i = icmp eq %class.Column** %34, null
  br i1 %tobool.not.i68.i.i.i, label %_ZNSt6vectorIP6ColumnSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i.i, label %if.then.i69.i.i.i

if.then.i69.i.i.i:                                ; preds = %invoke.cont15.i.i.i
  %41 = bitcast %class.Column** %34 to i8*
  tail call void @_ZdlPv(i8* nonnull %41) #20
  br label %_ZNSt6vectorIP6ColumnSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i.i

_ZNSt6vectorIP6ColumnSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i.i: ; preds = %if.then.i69.i.i.i, %invoke.cont15.i.i.i
  %sub.ptr.div.i.i.i.i.i.i.i.i.i.i.i = ashr exact i64 %sub.ptr.sub.i.i.i.i.i.i.i.i.i.i.i, 3
  %add.ptr.i.i.i.i.i.i.i.i.i.i.i = getelementptr inbounds %class.Column*, %class.Column** %incdec.ptr.i.i.i, i64 %sub.ptr.div.i.i.i.i.i.i.i.i.i.i.i
  store %class.Column** %cond.i67.i.i.i, %class.Column*** %13, align 8, !tbaa !147
  store %class.Column** %add.ptr.i.i.i.i.i.i.i.i.i.i.i, %class.Column*** %11, align 8, !tbaa !143
  %add.ptr39.i.i.i = getelementptr inbounds %class.Column*, %class.Column** %cond.i67.i.i.i, i64 %cond.i.i.i.i
  br label %_ZN5Table9setColumnEP6ColumnN3opt10ColumnTypeE.exit

_ZN5Table9setColumnEP6ColumnN3opt10ColumnTypeE.exit: ; preds = %if.then.i.i, %_ZNSt6vectorIP6ColumnSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i.i
  %.sink = phi %class.Column*** [ %11, %if.then.i.i ], [ %12, %_ZNSt6vectorIP6ColumnSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i.i ]
  %incdec.ptr.i.i.sink = phi %class.Column** [ %incdec.ptr.i.i, %if.then.i.i ], [ %add.ptr39.i.i.i, %_ZNSt6vectorIP6ColumnSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i.i ]
  store %class.Column** %incdec.ptr.i.i.sink, %class.Column*** %.sink, align 8, !tbaa !46
  %indvars.iv.next = add nuw nsw i64 %indvars.iv, 1
  %exitcond.not = icmp eq i64 %indvars.iv.next, %conv.i
  br i1 %exitcond.not, label %for.cond.cleanup, label %for.body, !llvm.loop !148
}

declare dso_local i32 @_Z13getColumnTypei(i32) local_unnamed_addr #0

declare dso_local void @_ZN10PagesIndex8addTableEP5Tableij(%class.PagesIndex* nonnull dereferenceable(68), %class.Table*, i32, i32) local_unnamed_addr #0

; Function Attrs: uwtable mustprogress
define dso_local void @_Z4sortll(i64 %sortAddress, i64 %stageId) local_unnamed_addr #8 {
entry:
  %0 = inttoptr i64 %sortAddress to %class.Sort*
  %sourceTypes.i = getelementptr inbounds %class.Sort, %class.Sort* %0, i64 0, i32 1
  %1 = load i32*, i32** %sourceTypes.i, align 8, !tbaa !128
  %sortCols.i = getelementptr inbounds %class.Sort, %class.Sort* %0, i64 0, i32 5
  %2 = load i32*, i32** %sortCols.i, align 8, !tbaa !149
  %sortColCount.i = getelementptr inbounds %class.Sort, %class.Sort* %0, i64 0, i32 8
  %3 = load i32, i32* %sortColCount.i, align 8, !tbaa !150
  %pagesIndex.i = getelementptr inbounds %class.Sort, %class.Sort* %0, i64 0, i32 9
  %4 = load %class.PagesIndex*, %class.PagesIndex** %pagesIndex.i, align 8, !tbaa !125
  %positionCount.i = getelementptr inbounds %class.PagesIndex, %class.PagesIndex* %4, i64 0, i32 5
  %5 = load i32, i32* %positionCount.i, align 8, !tbaa !151
  %6 = zext i32 %3 to i64
  %vla = alloca i32, i64 %6, align 16
  %cmp44 = icmp sgt i32 %3, 0
  br i1 %cmp44, label %for.body.preheader, label %if.else

for.body.preheader:                               ; preds = %entry
  %7 = add nsw i64 %6, -1
  %xtraiter = and i64 %6, 3
  %8 = icmp ult i64 %7, 3
  br i1 %8, label %if.else.loopexit.unr-lcssa, label %for.body.preheader.new

for.body.preheader.new:                           ; preds = %for.body.preheader
  %unroll_iter = and i64 %6, 4294967292
  br label %for.body

for.body:                                         ; preds = %for.body, %for.body.preheader.new
  %indvars.iv = phi i64 [ 0, %for.body.preheader.new ], [ %indvars.iv.next.3, %for.body ]
  %niter = phi i64 [ %unroll_iter, %for.body.preheader.new ], [ %niter.nsub.3, %for.body ]
  %arrayidx = getelementptr inbounds i32, i32* %2, i64 %indvars.iv
  %9 = load i32, i32* %arrayidx, align 4, !tbaa !16
  %idxprom5 = sext i32 %9 to i64
  %arrayidx6 = getelementptr inbounds i32, i32* %1, i64 %idxprom5
  %10 = load i32, i32* %arrayidx6, align 4, !tbaa !16
  %arrayidx8 = getelementptr inbounds i32, i32* %vla, i64 %indvars.iv
  store i32 %10, i32* %arrayidx8, align 16, !tbaa !16
  %indvars.iv.next = or i64 %indvars.iv, 1
  %arrayidx.1 = getelementptr inbounds i32, i32* %2, i64 %indvars.iv.next
  %11 = load i32, i32* %arrayidx.1, align 4, !tbaa !16
  %idxprom5.1 = sext i32 %11 to i64
  %arrayidx6.1 = getelementptr inbounds i32, i32* %1, i64 %idxprom5.1
  %12 = load i32, i32* %arrayidx6.1, align 4, !tbaa !16
  %arrayidx8.1 = getelementptr inbounds i32, i32* %vla, i64 %indvars.iv.next
  store i32 %12, i32* %arrayidx8.1, align 4, !tbaa !16
  %indvars.iv.next.1 = or i64 %indvars.iv, 2
  %arrayidx.2 = getelementptr inbounds i32, i32* %2, i64 %indvars.iv.next.1
  %13 = load i32, i32* %arrayidx.2, align 4, !tbaa !16
  %idxprom5.2 = sext i32 %13 to i64
  %arrayidx6.2 = getelementptr inbounds i32, i32* %1, i64 %idxprom5.2
  %14 = load i32, i32* %arrayidx6.2, align 4, !tbaa !16
  %arrayidx8.2 = getelementptr inbounds i32, i32* %vla, i64 %indvars.iv.next.1
  store i32 %14, i32* %arrayidx8.2, align 8, !tbaa !16
  %indvars.iv.next.2 = or i64 %indvars.iv, 3
  %arrayidx.3 = getelementptr inbounds i32, i32* %2, i64 %indvars.iv.next.2
  %15 = load i32, i32* %arrayidx.3, align 4, !tbaa !16
  %idxprom5.3 = sext i32 %15 to i64
  %arrayidx6.3 = getelementptr inbounds i32, i32* %1, i64 %idxprom5.3
  %16 = load i32, i32* %arrayidx6.3, align 4, !tbaa !16
  %arrayidx8.3 = getelementptr inbounds i32, i32* %vla, i64 %indvars.iv.next.2
  store i32 %16, i32* %arrayidx8.3, align 4, !tbaa !16
  %indvars.iv.next.3 = add nuw nsw i64 %indvars.iv, 4
  %niter.nsub.3 = add i64 %niter, -4
  %niter.ncmp.3 = icmp eq i64 %niter.nsub.3, 0
  br i1 %niter.ncmp.3, label %if.else.loopexit.unr-lcssa, label %for.body, !llvm.loop !155

if.else.loopexit.unr-lcssa:                       ; preds = %for.body, %for.body.preheader
  %indvars.iv.unr = phi i64 [ 0, %for.body.preheader ], [ %indvars.iv.next.3, %for.body ]
  %lcmp.mod.not = icmp eq i64 %xtraiter, 0
  br i1 %lcmp.mod.not, label %if.else, label %for.body.epil

for.body.epil:                                    ; preds = %if.else.loopexit.unr-lcssa, %for.body.epil
  %indvars.iv.epil = phi i64 [ %indvars.iv.next.epil, %for.body.epil ], [ %indvars.iv.unr, %if.else.loopexit.unr-lcssa ]
  %epil.iter = phi i64 [ %epil.iter.sub, %for.body.epil ], [ %xtraiter, %if.else.loopexit.unr-lcssa ]
  %arrayidx.epil = getelementptr inbounds i32, i32* %2, i64 %indvars.iv.epil
  %17 = load i32, i32* %arrayidx.epil, align 4, !tbaa !16
  %idxprom5.epil = sext i32 %17 to i64
  %arrayidx6.epil = getelementptr inbounds i32, i32* %1, i64 %idxprom5.epil
  %18 = load i32, i32* %arrayidx6.epil, align 4, !tbaa !16
  %arrayidx8.epil = getelementptr inbounds i32, i32* %vla, i64 %indvars.iv.epil
  store i32 %18, i32* %arrayidx8.epil, align 4, !tbaa !16
  %indvars.iv.next.epil = add nuw nsw i64 %indvars.iv.epil, 1
  %epil.iter.sub = add i64 %epil.iter, -1
  %epil.iter.cmp.not = icmp eq i64 %epil.iter.sub, 0
  br i1 %epil.iter.cmp.not, label %if.else, label %for.body.epil, !llvm.loop !156

if.else:                                          ; preds = %if.else.loopexit.unr-lcssa, %for.body.epil, %entry
  %19 = ptrtoint %class.PagesIndex* %4 to i64
  %sortAscendings.i = getelementptr inbounds %class.Sort, %class.Sort* %0, i64 0, i32 6
  %20 = load i32*, i32** %sortAscendings.i, align 8, !tbaa !157
  %sortNullFirsts.i = getelementptr inbounds %class.Sort, %class.Sort* %0, i64 0, i32 7
  %21 = load i32*, i32** %sortNullFirsts.i, align 8, !tbaa !158
  call void @_Z9quickSortlPiS_S_S_ijj(i64 %19, i32* %2, i32* nonnull %vla, i32* %20, i32* %21, i32 %3, i32 0, i32 %5)
  ret void
}

declare dso_local void @_Z9quickSortlPiS_S_S_ijj(i64, i32*, i32*, i32*, i32*, i32, i32, i32) local_unnamed_addr #0

; Function Attrs: uwtable
define dso_local %class.Table* @_Z9getResultll(i64 %sortAddress, i64 %stageId) local_unnamed_addr #4 personality i8* bitcast (i32 (...)* @__gxx_personality_v0 to i8*) {
entry:
  %0 = inttoptr i64 %sortAddress to %class.Sort*
  %pagesIndex.i = getelementptr inbounds %class.Sort, %class.Sort* %0, i64 0, i32 9
  %1 = load %class.PagesIndex*, %class.PagesIndex** %pagesIndex.i, align 8, !tbaa !125
  %positionCount.i = getelementptr inbounds %class.PagesIndex, %class.PagesIndex* %1, i64 0, i32 5
  %2 = load i32, i32* %positionCount.i, align 8, !tbaa !151
  %outputColsCount.i = getelementptr inbounds %class.Sort, %class.Sort* %0, i64 0, i32 4
  %3 = load i32, i32* %outputColsCount.i, align 8, !tbaa !159
  %outputCols.i = getelementptr inbounds %class.Sort, %class.Sort* %0, i64 0, i32 3
  %4 = load i32*, i32** %outputCols.i, align 8, !tbaa !160
  %sourceTypes.i = getelementptr inbounds %class.Sort, %class.Sort* %0, i64 0, i32 1
  %5 = load i32*, i32** %sourceTypes.i, align 8, !tbaa !128
  %call5 = tail call noalias nonnull dereferenceable(64) i8* @_Znwm(i64 64) #23
  %6 = bitcast i8* %call5 to i32 (...)***
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [4 x i8*] }, { [4 x i8*] }* @_ZTV5Table, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %6, align 8, !tbaa !119
  %data.i = getelementptr inbounds i8, i8* %call5, i64 16
  tail call void @llvm.memset.p0i8.i64(i8* nonnull align 8 dereferenceable(24) %data.i, i8 0, i64 24, i1 false) #20
  %positionCount2.i = getelementptr inbounds i8, i8* %call5, i64 48
  %7 = bitcast i8* %positionCount2.i to i32*
  store i32 %2, i32* %7, align 8, !tbaa !129
  %columnCount3.i = getelementptr inbounds i8, i8* %call5, i64 52
  %8 = bitcast i8* %columnCount3.i to i32*
  store i32 %3, i32* %8, align 4, !tbaa !133
  %conv.i = zext i32 %3 to i64
  %9 = shl nuw nsw i64 %conv.i, 2
  %call.i55 = invoke noalias nonnull i8* @_Znam(i64 %9) #23
          to label %if.else unwind label %lpad

lpad:                                             ; preds = %entry
  %10 = landingpad { i8*, i32 }
          cleanup
  tail call void @_ZdlPv(i8* nonnull %call5) #25
  resume { i8*, i32 } %10

if.else:                                          ; preds = %entry
  %11 = bitcast i8* %call5 to %class.Table*
  %types.i = getelementptr inbounds i8, i8* %call5, i64 40
  %12 = bitcast i8* %types.i to i8**
  store i8* %call.i55, i8** %12, align 8, !tbaa !134
  %columnSize.i = getelementptr inbounds i8, i8* %call5, i64 56
  %13 = bitcast i8* %columnSize.i to i32*
  store i32 0, i32* %13, align 8, !tbaa !135
  %14 = ptrtoint i8* %call5 to i64
  tail call void @_Z12allocColumnslPiS_ij(i64 %14, i32* %5, i32* %4, i32 %3, i32 %2)
  %15 = ptrtoint %class.PagesIndex* %1 to i64
  tail call void @_Z9getResultlPiilS_j(i64 %15, i32* %4, i32 %3, i64 %14, i32* %5, i32 %2)
  %16 = inttoptr i64 %sortAddress to void (%class.Sort*)***
  %vtable = load void (%class.Sort*)**, void (%class.Sort*)*** %16, align 8, !tbaa !119
  %vfn = getelementptr inbounds void (%class.Sort*)*, void (%class.Sort*)** %vtable, i64 6
  %17 = load void (%class.Sort*)*, void (%class.Sort*)** %vfn, align 8
  tail call void %17(%class.Sort* nonnull dereferenceable(80) %0) #20
  ret %class.Table* %11
}

declare dso_local void @_Z12allocColumnslPiS_ij(i64, i32*, i32*, i32, i32) local_unnamed_addr #0

declare dso_local void @_Z9getResultlPiilS_j(i64, i32*, i32, i64, i32*, i32) local_unnamed_addr #0

; Function Attrs: argmemonly nofree nosync nounwind willreturn writeonly
declare void @llvm.memset.p0i8.i64(i8* nocapture writeonly, i8, i64, i1 immarg) #10

; Function Attrs: noinline noreturn nounwind
define linkonce_odr hidden void @__clang_call_terminate(i8* %0) local_unnamed_addr #11 comdat {
  %2 = tail call i8* @__cxa_begin_catch(i8* %0) #20
  tail call void @_ZSt9terminatev() #21
  unreachable
}

declare dso_local i8* @__cxa_begin_catch(i8*) local_unnamed_addr

declare dso_local void @_ZSt9terminatev() local_unnamed_addr

declare dso_local i8* @_ZN4llvm3sys14DynamicLibrary19getPermanentLibraryEPKcPNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEE(i8*, %"class.std::__cxx11::basic_string"*) local_unnamed_addr #0

; Function Attrs: uwtable mustprogress
define linkonce_odr dso_local %"struct.std::_Rb_tree_node"* @_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE7_M_copyINSH_11_Alloc_nodeEEEPSt13_Rb_tree_nodeISB_EPKSL_PSt18_Rb_tree_node_baseRT_(%"class.std::_Rb_tree"* nonnull dereferenceable(48) %this, %"struct.std::_Rb_tree_node"* %__x, %"struct.std::_Rb_tree_node_base"* %__p, %"struct.std::_Rb_tree<std::__cxx11::basic_string<char>, std::pair<const std::__cxx11::basic_string<char>, codegen::ParamValue *>, std::_Select1st<std::pair<const std::__cxx11::basic_string<char>, codegen::ParamValue *>>, std::less<std::__cxx11::basic_string<char>>>::_Alloc_node"* nonnull align 8 dereferenceable(8) %__node_gen) local_unnamed_addr #8 comdat align 2 personality i8* bitcast (i32 (...)* @__gxx_personality_v0 to i8*) {
entry:
  %_M_storage.i.i = getelementptr inbounds %"struct.std::_Rb_tree_node", %"struct.std::_Rb_tree_node"* %__x, i64 0, i32 1
  %0 = bitcast %"struct.__gnu_cxx::__aligned_membuf.437"* %_M_storage.i.i to %"struct.std::pair.438"*
  %_M_t.i.i = getelementptr inbounds %"struct.std::_Rb_tree<std::__cxx11::basic_string<char>, std::pair<const std::__cxx11::basic_string<char>, codegen::ParamValue *>, std::_Select1st<std::pair<const std::__cxx11::basic_string<char>, codegen::ParamValue *>>, std::less<std::__cxx11::basic_string<char>>>::_Alloc_node", %"struct.std::_Rb_tree<std::__cxx11::basic_string<char>, std::pair<const std::__cxx11::basic_string<char>, codegen::ParamValue *>, std::_Select1st<std::pair<const std::__cxx11::basic_string<char>, codegen::ParamValue *>>, std::less<std::__cxx11::basic_string<char>>>::_Alloc_node"* %__node_gen, i64 0, i32 0
  %1 = load %"class.std::_Rb_tree"*, %"class.std::_Rb_tree"** %_M_t.i.i, align 8, !tbaa !161
  %call2.i.i.i.i.i.i = tail call noalias nonnull i8* @_Znwm(i64 72) #22
  %2 = bitcast i8* %call2.i.i.i.i.i.i to %"struct.std::_Rb_tree_node"*
  tail call void @_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE17_M_construct_nodeIJRKSB_EEEvPSt13_Rb_tree_nodeISB_EDpOT_(%"class.std::_Rb_tree"* nonnull dereferenceable(48) %1, %"struct.std::_Rb_tree_node"* nonnull %2, %"struct.std::pair.438"* nonnull align 8 dereferenceable(40) %0)
  %_M_color.i = getelementptr inbounds %"struct.std::_Rb_tree_node", %"struct.std::_Rb_tree_node"* %__x, i64 0, i32 0, i32 0
  %3 = load i32, i32* %_M_color.i, align 8, !tbaa !163
  %_M_color3.i = bitcast i8* %call2.i.i.i.i.i.i to i32*
  store i32 %3, i32* %_M_color3.i, align 8, !tbaa !163
  %_M_left.i = getelementptr inbounds i8, i8* %call2.i.i.i.i.i.i, i64 16
  tail call void @llvm.memset.p0i8.i64(i8* nonnull align 8 dereferenceable(16) %_M_left.i, i8 0, i64 16, i1 false)
  %4 = bitcast i8* %call2.i.i.i.i.i.i to %"struct.std::_Rb_tree_node_base"*
  %_M_parent = getelementptr inbounds i8, i8* %call2.i.i.i.i.i.i, i64 8
  %5 = bitcast i8* %_M_parent to %"struct.std::_Rb_tree_node_base"**
  store %"struct.std::_Rb_tree_node_base"* %__p, %"struct.std::_Rb_tree_node_base"** %5, align 8, !tbaa !164
  %_M_right = getelementptr inbounds %"struct.std::_Rb_tree_node", %"struct.std::_Rb_tree_node"* %__x, i64 0, i32 0, i32 3
  %6 = load %"struct.std::_Rb_tree_node_base"*, %"struct.std::_Rb_tree_node_base"** %_M_right, align 8, !tbaa !49
  %tobool.not = icmp eq %"struct.std::_Rb_tree_node_base"* %6, null
  br i1 %tobool.not, label %if.end, label %if.then

if.then:                                          ; preds = %entry
  %7 = bitcast %"struct.std::_Rb_tree_node_base"* %6 to %"struct.std::_Rb_tree_node"*
  %call3 = invoke %"struct.std::_Rb_tree_node"* @_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE7_M_copyINSH_11_Alloc_nodeEEEPSt13_Rb_tree_nodeISB_EPKSL_PSt18_Rb_tree_node_baseRT_(%"class.std::_Rb_tree"* nonnull dereferenceable(48) %this, %"struct.std::_Rb_tree_node"* nonnull %7, %"struct.std::_Rb_tree_node_base"* nonnull %4, %"struct.std::_Rb_tree<std::__cxx11::basic_string<char>, std::pair<const std::__cxx11::basic_string<char>, codegen::ParamValue *>, std::_Select1st<std::pair<const std::__cxx11::basic_string<char>, codegen::ParamValue *>>, std::less<std::__cxx11::basic_string<char>>>::_Alloc_node"* nonnull align 8 dereferenceable(8) %__node_gen)
          to label %invoke.cont unwind label %lpad

invoke.cont:                                      ; preds = %if.then
  %8 = getelementptr %"struct.std::_Rb_tree_node", %"struct.std::_Rb_tree_node"* %call3, i64 0, i32 0
  %_M_right4 = getelementptr inbounds i8, i8* %call2.i.i.i.i.i.i, i64 24
  %9 = bitcast i8* %_M_right4 to %"struct.std::_Rb_tree_node_base"**
  store %"struct.std::_Rb_tree_node_base"* %8, %"struct.std::_Rb_tree_node_base"** %9, align 8, !tbaa !49
  br label %if.end

lpad:                                             ; preds = %if.then
  %10 = landingpad { i8*, i32 }
          catch i8* null
  br label %catch

if.end:                                           ; preds = %invoke.cont, %entry
  %__x.addr.0.in.in60 = getelementptr inbounds %"struct.std::_Rb_tree_node", %"struct.std::_Rb_tree_node"* %__x, i64 0, i32 0, i32 2
  %__x.addr.0.in61 = bitcast %"struct.std::_Rb_tree_node_base"** %__x.addr.0.in.in60 to %"struct.std::_Rb_tree_node"**
  %__x.addr.062 = load %"struct.std::_Rb_tree_node"*, %"struct.std::_Rb_tree_node"** %__x.addr.0.in61, align 8, !tbaa !47
  %cmp.not63 = icmp eq %"struct.std::_Rb_tree_node"* %__x.addr.062, null
  br i1 %cmp.not63, label %try.cont, label %while.body

while.body:                                       ; preds = %if.end, %if.end17
  %__x.addr.065 = phi %"struct.std::_Rb_tree_node"* [ %__x.addr.0, %if.end17 ], [ %__x.addr.062, %if.end ]
  %__p.addr.064 = phi %"struct.std::_Rb_tree_node_base"* [ %15, %if.end17 ], [ %4, %if.end ]
  %11 = load %"class.std::_Rb_tree"*, %"class.std::_Rb_tree"** %_M_t.i.i, align 8, !tbaa !161
  %call2.i.i.i.i.i.i5357 = invoke noalias nonnull i8* @_Znwm(i64 72) #22
          to label %call2.i.i.i.i.i.i53.noexc unwind label %lpad6

call2.i.i.i.i.i.i53.noexc:                        ; preds = %while.body
  %_M_storage.i.i51 = getelementptr inbounds %"struct.std::_Rb_tree_node", %"struct.std::_Rb_tree_node"* %__x.addr.065, i64 0, i32 1
  %12 = bitcast %"struct.__gnu_cxx::__aligned_membuf.437"* %_M_storage.i.i51 to %"struct.std::pair.438"*
  %13 = bitcast i8* %call2.i.i.i.i.i.i5357 to %"struct.std::_Rb_tree_node"*
  invoke void @_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE17_M_construct_nodeIJRKSB_EEEvPSt13_Rb_tree_nodeISB_EDpOT_(%"class.std::_Rb_tree"* nonnull dereferenceable(48) %11, %"struct.std::_Rb_tree_node"* nonnull %13, %"struct.std::pair.438"* nonnull align 8 dereferenceable(40) %12)
          to label %invoke.cont7 unwind label %lpad6

invoke.cont7:                                     ; preds = %call2.i.i.i.i.i.i53.noexc
  %_M_color.i54 = getelementptr inbounds %"struct.std::_Rb_tree_node", %"struct.std::_Rb_tree_node"* %__x.addr.065, i64 0, i32 0, i32 0
  %14 = load i32, i32* %_M_color.i54, align 8, !tbaa !163
  %_M_color3.i55 = bitcast i8* %call2.i.i.i.i.i.i5357 to i32*
  store i32 %14, i32* %_M_color3.i55, align 8, !tbaa !163
  %_M_left.i56 = getelementptr inbounds i8, i8* %call2.i.i.i.i.i.i5357, i64 16
  tail call void @llvm.memset.p0i8.i64(i8* nonnull align 8 dereferenceable(16) %_M_left.i56, i8 0, i64 16, i1 false)
  %15 = bitcast i8* %call2.i.i.i.i.i.i5357 to %"struct.std::_Rb_tree_node_base"*
  %_M_left = getelementptr inbounds %"struct.std::_Rb_tree_node_base", %"struct.std::_Rb_tree_node_base"* %__p.addr.064, i64 0, i32 2
  %16 = bitcast %"struct.std::_Rb_tree_node_base"** %_M_left to i8**
  store i8* %call2.i.i.i.i.i.i5357, i8** %16, align 8, !tbaa !47
  %_M_parent9 = getelementptr inbounds i8, i8* %call2.i.i.i.i.i.i5357, i64 8
  %17 = bitcast i8* %_M_parent9 to %"struct.std::_Rb_tree_node_base"**
  store %"struct.std::_Rb_tree_node_base"* %__p.addr.064, %"struct.std::_Rb_tree_node_base"** %17, align 8, !tbaa !164
  %_M_right10 = getelementptr inbounds %"struct.std::_Rb_tree_node", %"struct.std::_Rb_tree_node"* %__x.addr.065, i64 0, i32 0, i32 3
  %18 = load %"struct.std::_Rb_tree_node_base"*, %"struct.std::_Rb_tree_node_base"** %_M_right10, align 8, !tbaa !49
  %tobool11.not = icmp eq %"struct.std::_Rb_tree_node_base"* %18, null
  br i1 %tobool11.not, label %if.end17, label %if.then12

if.then12:                                        ; preds = %invoke.cont7
  %19 = bitcast %"struct.std::_Rb_tree_node_base"* %18 to %"struct.std::_Rb_tree_node"*
  %call15 = invoke %"struct.std::_Rb_tree_node"* @_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE7_M_copyINSH_11_Alloc_nodeEEEPSt13_Rb_tree_nodeISB_EPKSL_PSt18_Rb_tree_node_baseRT_(%"class.std::_Rb_tree"* nonnull dereferenceable(48) %this, %"struct.std::_Rb_tree_node"* nonnull %19, %"struct.std::_Rb_tree_node_base"* nonnull %15, %"struct.std::_Rb_tree<std::__cxx11::basic_string<char>, std::pair<const std::__cxx11::basic_string<char>, codegen::ParamValue *>, std::_Select1st<std::pair<const std::__cxx11::basic_string<char>, codegen::ParamValue *>>, std::less<std::__cxx11::basic_string<char>>>::_Alloc_node"* nonnull align 8 dereferenceable(8) %__node_gen)
          to label %invoke.cont14 unwind label %lpad6

invoke.cont14:                                    ; preds = %if.then12
  %20 = getelementptr %"struct.std::_Rb_tree_node", %"struct.std::_Rb_tree_node"* %call15, i64 0, i32 0
  %_M_right16 = getelementptr inbounds i8, i8* %call2.i.i.i.i.i.i5357, i64 24
  %21 = bitcast i8* %_M_right16 to %"struct.std::_Rb_tree_node_base"**
  store %"struct.std::_Rb_tree_node_base"* %20, %"struct.std::_Rb_tree_node_base"** %21, align 8, !tbaa !49
  br label %if.end17

lpad6:                                            ; preds = %call2.i.i.i.i.i.i53.noexc, %while.body, %if.then12
  %22 = landingpad { i8*, i32 }
          catch i8* null
  br label %catch

catch:                                            ; preds = %lpad6, %lpad
  %.pn = phi { i8*, i32 } [ %22, %lpad6 ], [ %10, %lpad ]
  %exn.slot.0 = extractvalue { i8*, i32 } %.pn, 0
  %23 = tail call i8* @__cxa_begin_catch(i8* %exn.slot.0) #20
  invoke void @_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE8_M_eraseEPSt13_Rb_tree_nodeISB_E(%"class.std::_Rb_tree"* nonnull dereferenceable(48) %this, %"struct.std::_Rb_tree_node"* nonnull %2)
          to label %invoke.cont20 unwind label %lpad19

invoke.cont20:                                    ; preds = %catch
  invoke void @__cxa_rethrow() #24
          to label %unreachable unwind label %lpad19

if.end17:                                         ; preds = %invoke.cont14, %invoke.cont7
  %__x.addr.0.in.in = getelementptr inbounds %"struct.std::_Rb_tree_node", %"struct.std::_Rb_tree_node"* %__x.addr.065, i64 0, i32 0, i32 2
  %__x.addr.0.in = bitcast %"struct.std::_Rb_tree_node_base"** %__x.addr.0.in.in to %"struct.std::_Rb_tree_node"**
  %__x.addr.0 = load %"struct.std::_Rb_tree_node"*, %"struct.std::_Rb_tree_node"** %__x.addr.0.in, align 8, !tbaa !47
  %cmp.not = icmp eq %"struct.std::_Rb_tree_node"* %__x.addr.0, null
  br i1 %cmp.not, label %try.cont, label %while.body, !llvm.loop !165

lpad19:                                           ; preds = %invoke.cont20, %catch
  %24 = landingpad { i8*, i32 }
          cleanup
  invoke void @__cxa_end_catch()
          to label %invoke.cont21 unwind label %terminate.lpad

invoke.cont21:                                    ; preds = %lpad19
  resume { i8*, i32 } %24

try.cont:                                         ; preds = %if.end17, %if.end
  ret %"struct.std::_Rb_tree_node"* %2

terminate.lpad:                                   ; preds = %lpad19
  %25 = landingpad { i8*, i32 }
          catch i8* null
  %26 = extractvalue { i8*, i32 } %25, 0
  tail call void @__clang_call_terminate(i8* %26) #21
  unreachable

unreachable:                                      ; preds = %invoke.cont20
  unreachable
}

; Function Attrs: uwtable
define linkonce_odr dso_local void @_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE8_M_eraseEPSt13_Rb_tree_nodeISB_E(%"class.std::_Rb_tree"* nonnull dereferenceable(48) %this, %"struct.std::_Rb_tree_node"* %__x) local_unnamed_addr #4 comdat align 2 personality i32 (...)* @__gxx_personality_v0 {
entry:
  %cmp.not7 = icmp eq %"struct.std::_Rb_tree_node"* %__x, null
  br i1 %cmp.not7, label %while.end, label %while.body

while.body:                                       ; preds = %entry, %_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE12_M_drop_nodeEPSt13_Rb_tree_nodeISB_E.exit
  %__x.addr.08 = phi %"struct.std::_Rb_tree_node"* [ %3, %_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE12_M_drop_nodeEPSt13_Rb_tree_nodeISB_E.exit ], [ %__x, %entry ]
  %_M_right.i = getelementptr inbounds %"struct.std::_Rb_tree_node", %"struct.std::_Rb_tree_node"* %__x.addr.08, i64 0, i32 0, i32 3
  %0 = bitcast %"struct.std::_Rb_tree_node_base"** %_M_right.i to %"struct.std::_Rb_tree_node"**
  %1 = load %"struct.std::_Rb_tree_node"*, %"struct.std::_Rb_tree_node"** %0, align 8, !tbaa !49
  tail call void @_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE8_M_eraseEPSt13_Rb_tree_nodeISB_E(%"class.std::_Rb_tree"* nonnull dereferenceable(48) %this, %"struct.std::_Rb_tree_node"* %1)
  %_M_left.i = getelementptr inbounds %"struct.std::_Rb_tree_node", %"struct.std::_Rb_tree_node"* %__x.addr.08, i64 0, i32 0, i32 2
  %2 = bitcast %"struct.std::_Rb_tree_node_base"** %_M_left.i to %"struct.std::_Rb_tree_node"**
  %3 = load %"struct.std::_Rb_tree_node"*, %"struct.std::_Rb_tree_node"** %2, align 8, !tbaa !47
  %_M_storage.i.i.i = getelementptr inbounds %"struct.std::_Rb_tree_node", %"struct.std::_Rb_tree_node"* %__x.addr.08, i64 0, i32 1
  %_M_p.i.i.i.i.i.i.i.i.i = bitcast %"struct.__gnu_cxx::__aligned_membuf.437"* %_M_storage.i.i.i to i8**
  %4 = load i8*, i8** %_M_p.i.i.i.i.i.i.i.i.i, align 8, !tbaa !41
  %5 = getelementptr inbounds %"struct.std::_Rb_tree_node", %"struct.std::_Rb_tree_node"* %__x.addr.08, i64 0, i32 1, i32 0, i64 16
  %cmp.i.i.i.i.i.i.i.i = icmp eq i8* %4, %5
  br i1 %cmp.i.i.i.i.i.i.i.i, label %_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE12_M_drop_nodeEPSt13_Rb_tree_nodeISB_E.exit, label %if.then.i.i.i.i.i.i.i

if.then.i.i.i.i.i.i.i:                            ; preds = %while.body
  tail call void @_ZdlPv(i8* %4) #20
  br label %_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE12_M_drop_nodeEPSt13_Rb_tree_nodeISB_E.exit

_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE12_M_drop_nodeEPSt13_Rb_tree_nodeISB_E.exit: ; preds = %while.body, %if.then.i.i.i.i.i.i.i
  %6 = bitcast %"struct.std::_Rb_tree_node"* %__x.addr.08 to i8*
  tail call void @_ZdlPv(i8* nonnull %6) #20
  %cmp.not = icmp eq %"struct.std::_Rb_tree_node"* %3, null
  br i1 %cmp.not, label %while.end, label %while.body, !llvm.loop !166

while.end:                                        ; preds = %_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE12_M_drop_nodeEPSt13_Rb_tree_nodeISB_E.exit, %entry
  ret void
}

declare dso_local void @__cxa_rethrow() local_unnamed_addr

declare dso_local void @__cxa_end_catch() local_unnamed_addr

; Function Attrs: uwtable
define linkonce_odr dso_local void @_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE17_M_construct_nodeIJRKSB_EEEvPSt13_Rb_tree_nodeISB_EDpOT_(%"class.std::_Rb_tree"* nonnull dereferenceable(48) %this, %"struct.std::_Rb_tree_node"* %__node, %"struct.std::pair.438"* nonnull align 8 dereferenceable(40) %__args) local_unnamed_addr #4 comdat align 2 personality i8* bitcast (i32 (...)* @__gxx_personality_v0 to i8*) {
entry:
  %__dnew.i.i.i.i.i.i.i = alloca i64, align 8
  %_M_storage.i = getelementptr inbounds %"struct.std::_Rb_tree_node", %"struct.std::_Rb_tree_node"* %__node, i64 0, i32 1
  %0 = getelementptr inbounds %"struct.std::_Rb_tree_node", %"struct.std::_Rb_tree_node"* %__node, i64 0, i32 1, i32 0, i64 16
  %1 = bitcast %"struct.__gnu_cxx::__aligned_membuf.437"* %_M_storage.i to i8**
  store i8* %0, i8** %1, align 8, !tbaa !39
  %_M_p.i15.i.i.i.i = getelementptr inbounds %"struct.std::pair.438", %"struct.std::pair.438"* %__args, i64 0, i32 0, i32 0, i32 0
  %2 = load i8*, i8** %_M_p.i15.i.i.i.i, align 8, !tbaa !41
  %_M_string_length.i.i.i.i.i = getelementptr inbounds %"struct.std::pair.438", %"struct.std::pair.438"* %__args, i64 0, i32 0, i32 1
  %3 = load i64, i64* %_M_string_length.i.i.i.i.i, align 8, !tbaa !44
  %4 = bitcast i64* %__dnew.i.i.i.i.i.i.i to i8*
  call void @llvm.lifetime.start.p0i8(i64 8, i8* nonnull %4) #20
  store i64 %3, i64* %__dnew.i.i.i.i.i.i.i, align 8, !tbaa !29
  %cmp3.i.i.i.i.i.i.i = icmp ugt i64 %3, 15
  br i1 %cmp3.i.i.i.i.i.i.i, label %if.then4.i.i.i.i.i.i.i, label %if.end6.i.i.i.i.i.i.i

if.then4.i.i.i.i.i.i.i:                           ; preds = %entry
  %first.i.i.i = bitcast %"struct.__gnu_cxx::__aligned_membuf.437"* %_M_storage.i to %"class.std::__cxx11::basic_string"*
  %call5.i.i.i14.i.i.i.i12 = invoke i8* @_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE9_M_createERmm(%"class.std::__cxx11::basic_string"* nonnull dereferenceable(32) %first.i.i.i, i64* nonnull align 8 dereferenceable(8) %__dnew.i.i.i.i.i.i.i, i64 0)
          to label %call5.i.i.i14.i.i.i.i.noexc unwind label %lpad

call5.i.i.i14.i.i.i.i.noexc:                      ; preds = %if.then4.i.i.i.i.i.i.i
  store i8* %call5.i.i.i14.i.i.i.i12, i8** %1, align 8, !tbaa !41
  %5 = load i64, i64* %__dnew.i.i.i.i.i.i.i, align 8, !tbaa !29
  %6 = bitcast i8* %0 to i64*
  store i64 %5, i64* %6, align 8, !tbaa !43
  br label %if.end6.i.i.i.i.i.i.i

if.end6.i.i.i.i.i.i.i:                            ; preds = %entry, %call5.i.i.i14.i.i.i.i.noexc
  %7 = phi i8* [ %call5.i.i.i14.i.i.i.i12, %call5.i.i.i14.i.i.i.i.noexc ], [ %0, %entry ]
  switch i64 %3, label %if.end.i.i.i.i.i.i.i.i.i.i [
    i64 1, label %if.then.i.i.i.i.i.i.i.i.i
    i64 0, label %try.cont
  ]

if.then.i.i.i.i.i.i.i.i.i:                        ; preds = %if.end6.i.i.i.i.i.i.i
  %8 = load i8, i8* %2, align 1, !tbaa !43
  store i8 %8, i8* %7, align 1, !tbaa !43
  br label %try.cont

if.end.i.i.i.i.i.i.i.i.i.i:                       ; preds = %if.end6.i.i.i.i.i.i.i
  call void @llvm.memcpy.p0i8.p0i8.i64(i8* align 1 %7, i8* align 1 %2, i64 %3, i1 false) #20
  br label %try.cont

lpad:                                             ; preds = %if.then4.i.i.i.i.i.i.i
  %9 = landingpad { i8*, i32 }
          catch i8* null
  %10 = extractvalue { i8*, i32 } %9, 0
  %11 = call i8* @__cxa_begin_catch(i8* %10) #20
  %12 = bitcast %"struct.std::_Rb_tree_node"* %__node to i8*
  call void @_ZdlPv(i8* %12) #20
  invoke void @__cxa_rethrow() #24
          to label %unreachable unwind label %lpad5

lpad5:                                            ; preds = %lpad
  %13 = landingpad { i8*, i32 }
          cleanup
  invoke void @__cxa_end_catch()
          to label %eh.resume unwind label %terminate.lpad

try.cont:                                         ; preds = %if.end.i.i.i.i.i.i.i.i.i.i, %if.then.i.i.i.i.i.i.i.i.i, %if.end6.i.i.i.i.i.i.i
  %14 = load i64, i64* %__dnew.i.i.i.i.i.i.i, align 8, !tbaa !29
  %_M_string_length.i.i.i.i.i.i.i.i.i = getelementptr inbounds %"struct.std::_Rb_tree_node", %"struct.std::_Rb_tree_node"* %__node, i64 0, i32 1, i32 0, i64 8
  %15 = bitcast i8* %_M_string_length.i.i.i.i.i.i.i.i.i to i64*
  store i64 %14, i64* %15, align 8, !tbaa !44
  %16 = load i8*, i8** %1, align 8, !tbaa !41
  %arrayidx.i.i.i.i.i.i.i.i = getelementptr inbounds i8, i8* %16, i64 %14
  store i8 0, i8* %arrayidx.i.i.i.i.i.i.i.i, align 1, !tbaa !43
  call void @llvm.lifetime.end.p0i8(i64 8, i8* nonnull %4) #20
  %second.i.i.i = getelementptr inbounds %"struct.std::_Rb_tree_node", %"struct.std::_Rb_tree_node"* %__node, i64 0, i32 1, i32 0, i64 32
  %17 = bitcast i8* %second.i.i.i to %"class.codegen::ParamValue"**
  %second3.i.i.i = getelementptr inbounds %"struct.std::pair.438", %"struct.std::pair.438"* %__args, i64 0, i32 1
  %18 = load %"class.codegen::ParamValue"*, %"class.codegen::ParamValue"** %second3.i.i.i, align 8, !tbaa !167
  store %"class.codegen::ParamValue"* %18, %"class.codegen::ParamValue"** %17, align 8, !tbaa !167
  ret void

eh.resume:                                        ; preds = %lpad5
  resume { i8*, i32 } %13

terminate.lpad:                                   ; preds = %lpad5
  %19 = landingpad { i8*, i32 }
          catch i8* null
  %20 = extractvalue { i8*, i32 } %19, 0
  call void @__clang_call_terminate(i8* %20) #21
  unreachable

unreachable:                                      ; preds = %lpad
  unreachable
}

; Function Attrs: noreturn
declare dso_local void @_ZSt17__throw_bad_allocv() local_unnamed_addr #12

declare dso_local i8* @_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE9_M_createERmm(%"class.std::__cxx11::basic_string"* nonnull dereferenceable(32), i64* nonnull align 8 dereferenceable(8), i64) local_unnamed_addr #0

; Function Attrs: argmemonly nofree nosync nounwind willreturn
declare void @llvm.memcpy.p0i8.p0i8.i64(i8* noalias nocapture writeonly, i8* noalias nocapture readonly, i64, i1 immarg) #5

declare dso_local void @_ZN7codegen12HammerConfig14init_func_passEv(%"class.codegen::HammerConfig"* nonnull dereferenceable(1616)) local_unnamed_addr #0

declare dso_local void @_ZN7codegen12HammerConfig16init_module_passEv(%"class.codegen::HammerConfig"* nonnull dereferenceable(1616)) local_unnamed_addr #0

; Function Attrs: uwtable
define linkonce_odr dso_local void @_ZN4llvm3orc5LLJIT6lookupERNS0_8JITDylibENS_9StringRefE(%"class.llvm::Expected"* noalias sret(%"class.llvm::Expected") align 8 %agg.result, %"class.llvm::orc::LLJIT"* nonnull dereferenceable(568) %this, %"class.llvm::orc::JITDylib"* nonnull align 8 dereferenceable(272) %JD, i8* %UnmangledName.coerce0, i64 %UnmangledName.coerce1) local_unnamed_addr #4 comdat align 2 personality i8* bitcast (i32 (...)* @__gxx_personality_v0 to i8*) {
entry:
  %ref.tmp2.i.i = alloca i32, align 4
  %agg.tmp.i = alloca %"class.llvm::orc::SymbolStringPtr", align 8
  %ref.tmp = alloca %"class.std::__cxx11::basic_string", align 8
  %0 = bitcast %"class.std::__cxx11::basic_string"* %ref.tmp to i8*
  call void @llvm.lifetime.start.p0i8(i64 32, i8* nonnull %0) #20
  call void @_ZNK4llvm3orc5LLJIT6mangleB5cxx11ENS_9StringRefE(%"class.std::__cxx11::basic_string"* nonnull sret(%"class.std::__cxx11::basic_string") align 8 %ref.tmp, %"class.llvm::orc::LLJIT"* nonnull dereferenceable(568) %this, i8* %UnmangledName.coerce0, i64 %UnmangledName.coerce1)
  %_M_p.i.i.i = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp, i64 0, i32 0, i32 0
  %1 = load i8*, i8** %_M_p.i.i.i, align 8, !tbaa !41
  %_M_string_length.i.i = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp, i64 0, i32 1
  %2 = load i64, i64* %_M_string_length.i.i, align 8, !tbaa !44
  %3 = bitcast %"class.llvm::orc::SymbolStringPtr"* %agg.tmp.i to i8*
  call void @llvm.lifetime.start.p0i8(i64 8, i8* nonnull %3)
  %_M_head_impl.i.i.i.i.i.i.i.i = getelementptr inbounds %"class.llvm::orc::LLJIT", %"class.llvm::orc::LLJIT"* %this, i64 0, i32 0, i32 0, i32 0, i32 0, i32 0, i32 0
  %4 = load %"class.llvm::orc::ExecutionSession"*, %"class.llvm::orc::ExecutionSession"** %_M_head_impl.i.i.i.i.i.i.i.i, align 8, !tbaa !46, !noalias !169
  call void @llvm.experimental.noalias.scope.decl(metadata !172)
  %_M_ptr.i.i.i.i = getelementptr inbounds %"class.llvm::orc::ExecutionSession", %"class.llvm::orc::ExecutionSession"* %4, i64 0, i32 2, i32 0, i32 0
  %5 = load %"class.llvm::orc::SymbolStringPool"*, %"class.llvm::orc::SymbolStringPool"** %_M_ptr.i.i.i.i, align 8, !tbaa !175, !noalias !172
  call void @llvm.experimental.noalias.scope.decl(metadata !178), !noalias !172
  br i1 icmp ne (i8* bitcast (i32 (i32*, void (i8*)*)* @__pthread_key_create to i8*), i8* null), label %_ZL20__gthread_mutex_lockP15pthread_mutex_t.exit.i.i.i.i, label %_ZNSt10lock_guardISt5mutexEC2ERS0_.exit.i.i

_ZL20__gthread_mutex_lockP15pthread_mutex_t.exit.i.i.i.i: ; preds = %entry
  %_M_mutex.i.i.i.i = getelementptr inbounds %"class.llvm::orc::SymbolStringPool", %"class.llvm::orc::SymbolStringPool"* %5, i64 0, i32 0, i32 0, i32 0
  %call1.i.i.i.i.i = call i32 @pthread_mutex_lock(%union.pthread_mutex_t* nonnull %_M_mutex.i.i.i.i) #20, !noalias !181
  %tobool.not.i.i.i.i = icmp eq i32 %call1.i.i.i.i.i, 0
  br i1 %tobool.not.i.i.i.i, label %_ZNSt10lock_guardISt5mutexEC2ERS0_.exit.i.i, label %if.then.i.i.i.i

if.then.i.i.i.i:                                  ; preds = %_ZL20__gthread_mutex_lockP15pthread_mutex_t.exit.i.i.i.i
  invoke void @_ZSt20__throw_system_errori(i32 %call1.i.i.i.i.i) #24
          to label %.noexc8 unwind label %lpad

.noexc8:                                          ; preds = %if.then.i.i.i.i
  unreachable

_ZNSt10lock_guardISt5mutexEC2ERS0_.exit.i.i:      ; preds = %_ZL20__gthread_mutex_lockP15pthread_mutex_t.exit.i.i.i.i, %entry
  %Pool.i.i = getelementptr inbounds %"class.llvm::orc::SymbolStringPool", %"class.llvm::orc::SymbolStringPool"* %5, i64 0, i32 1
  %6 = bitcast i32* %ref.tmp2.i.i to i8*
  call void @llvm.lifetime.start.p0i8(i64 4, i8* nonnull %6) #20, !noalias !181
  store i32 0, i32* %ref.tmp2.i.i, align 4, !tbaa !16, !noalias !181
  %call.i.i = invoke { %"class.llvm::StringMapEntryBase"**, i8 } @_ZN4llvm9StringMapISt6atomicImENS_15MallocAllocatorEE11try_emplaceIJiEEESt4pairINS_17StringMapIteratorIS2_EEbENS_9StringRefEDpOT_(%"class.llvm::StringMap.149"* nonnull dereferenceable(25) %Pool.i.i, i8* %1, i64 %2, i32* nonnull align 4 dereferenceable(4) %ref.tmp2.i.i)
          to label %invoke.cont.i.i unwind label %lpad.i.i, !noalias !181

invoke.cont.i.i:                                  ; preds = %_ZNSt10lock_guardISt5mutexEC2ERS0_.exit.i.i
  %7 = extractvalue { %"class.llvm::StringMapEntryBase"**, i8 } %call.i.i, 0
  call void @llvm.lifetime.end.p0i8(i64 4, i8* nonnull %6) #20, !noalias !181
  %8 = bitcast %"class.llvm::StringMapEntryBase"** %7 to %"class.llvm::StringMapEntry"**
  %9 = load %"class.llvm::StringMapEntry"*, %"class.llvm::StringMapEntry"** %8, align 8, !tbaa !46, !noalias !181
  %S2.i.i.i = getelementptr inbounds %"class.llvm::orc::SymbolStringPtr", %"class.llvm::orc::SymbolStringPtr"* %agg.tmp.i, i64 0, i32 0
  store %"class.llvm::StringMapEntry"* %9, %"class.llvm::StringMapEntry"** %S2.i.i.i, align 8, !tbaa !182, !alias.scope !181
  %10 = ptrtoint %"class.llvm::StringMapEntry"* %9 to i64
  %sub.i.i.i.i = add i64 %10, -1
  %cmp.i.i.i.i = icmp ult i64 %sub.i.i.i.i, -32
  br i1 %cmp.i.i.i.i, label %if.then.i.i.i, label %invoke.cont11.i.i

if.then.i.i.i:                                    ; preds = %invoke.cont.i.i
  %_M_i.i.i.i.i = getelementptr inbounds %"class.llvm::StringMapEntry", %"class.llvm::StringMapEntry"* %9, i64 0, i32 0, i32 1, i32 0, i32 0
  %11 = atomicrmw add i64* %_M_i.i.i.i.i, i64 1 seq_cst, !noalias !181
  br label %invoke.cont11.i.i

invoke.cont11.i.i:                                ; preds = %if.then.i.i.i, %invoke.cont.i.i
  br i1 icmp ne (i8* bitcast (i32 (i32*, void (i8*)*)* @__pthread_key_create to i8*), i8* null), label %if.then.i.i.i25.i.i, label %.noexc

if.then.i.i.i25.i.i:                              ; preds = %invoke.cont11.i.i
  %_M_mutex.i.i23.i.i = getelementptr inbounds %"class.llvm::orc::SymbolStringPool", %"class.llvm::orc::SymbolStringPool"* %5, i64 0, i32 0, i32 0, i32 0
  %call1.i.i.i24.i.i = call i32 @pthread_mutex_unlock(%union.pthread_mutex_t* nonnull %_M_mutex.i.i23.i.i) #20, !noalias !181
  br label %.noexc

lpad.i.i:                                         ; preds = %_ZNSt10lock_guardISt5mutexEC2ERS0_.exit.i.i
  %12 = landingpad { i8*, i32 }
          cleanup
  call void @llvm.lifetime.end.p0i8(i64 4, i8* nonnull %6) #20, !noalias !181
  br i1 icmp ne (i8* bitcast (i32 (i32*, void (i8*)*)* @__pthread_key_create to i8*), i8* null), label %if.then.i.i.i.i.i, label %lpad.body

if.then.i.i.i.i.i:                                ; preds = %lpad.i.i
  %_M_mutex.i.i20.i.i = getelementptr inbounds %"class.llvm::orc::SymbolStringPool", %"class.llvm::orc::SymbolStringPool"* %5, i64 0, i32 0, i32 0, i32 0
  %call1.i.i.i21.i.i = call i32 @pthread_mutex_unlock(%union.pthread_mutex_t* nonnull %_M_mutex.i.i20.i.i) #20, !noalias !181
  br label %lpad.body

.noexc:                                           ; preds = %if.then.i.i.i25.i.i, %invoke.cont11.i.i
  invoke void @_ZN4llvm3orc5LLJIT19lookupLinkerMangledERNS0_8JITDylibENS0_15SymbolStringPtrE(%"class.llvm::Expected"* sret(%"class.llvm::Expected") align 8 %agg.result, %"class.llvm::orc::LLJIT"* nonnull dereferenceable(568) %this, %"class.llvm::orc::JITDylib"* nonnull align 8 dereferenceable(272) %JD, %"class.llvm::orc::SymbolStringPtr"* nonnull %agg.tmp.i)
          to label %invoke.cont.i unwind label %lpad.i

invoke.cont.i:                                    ; preds = %.noexc
  %13 = load %"class.llvm::StringMapEntry"*, %"class.llvm::StringMapEntry"** %S2.i.i.i, align 8, !tbaa !182, !noalias !169
  %14 = ptrtoint %"class.llvm::StringMapEntry"* %13 to i64
  %sub.i.i6.i = add i64 %14, -1
  %cmp.i.i7.i = icmp ult i64 %sub.i.i6.i, -32
  br i1 %cmp.i.i7.i, label %if.then.i9.i, label %invoke.cont3

if.then.i9.i:                                     ; preds = %invoke.cont.i
  %_M_i.i.i8.i = getelementptr inbounds %"class.llvm::StringMapEntry", %"class.llvm::StringMapEntry"* %13, i64 0, i32 0, i32 1, i32 0, i32 0
  %15 = atomicrmw sub i64* %_M_i.i.i8.i, i64 1 seq_cst
  br label %invoke.cont3

lpad.i:                                           ; preds = %.noexc
  %16 = landingpad { i8*, i32 }
          cleanup
  %17 = load %"class.llvm::StringMapEntry"*, %"class.llvm::StringMapEntry"** %S2.i.i.i, align 8, !tbaa !182, !noalias !169
  %18 = ptrtoint %"class.llvm::StringMapEntry"* %17 to i64
  %sub.i.i.i = add i64 %18, -1
  %cmp.i.i.i6 = icmp ult i64 %sub.i.i.i, -32
  br i1 %cmp.i.i.i6, label %if.then.i.i7, label %lpad.body

if.then.i.i7:                                     ; preds = %lpad.i
  %_M_i.i.i.i = getelementptr inbounds %"class.llvm::StringMapEntry", %"class.llvm::StringMapEntry"* %17, i64 0, i32 0, i32 1, i32 0, i32 0
  %19 = atomicrmw sub i64* %_M_i.i.i.i, i64 1 seq_cst
  br label %lpad.body

invoke.cont3:                                     ; preds = %if.then.i9.i, %invoke.cont.i
  call void @llvm.lifetime.end.p0i8(i64 8, i8* nonnull %3)
  %20 = load i8*, i8** %_M_p.i.i.i, align 8, !tbaa !41
  %21 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp, i64 0, i32 2
  %arraydecay.i.i.i.i12 = bitcast %union.anon* %21 to i8*
  %cmp.i.i.i13 = icmp eq i8* %20, %arraydecay.i.i.i.i12
  br i1 %cmp.i.i.i13, label %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit16, label %if.then.i.i14

if.then.i.i14:                                    ; preds = %invoke.cont3
  call void @_ZdlPv(i8* %20) #20
  br label %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit16

_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit16: ; preds = %invoke.cont3, %if.then.i.i14
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %0) #20
  ret void

lpad:                                             ; preds = %if.then.i.i.i.i
  %22 = landingpad { i8*, i32 }
          cleanup
  br label %lpad.body

lpad.body:                                        ; preds = %lpad, %if.then.i.i.i.i.i, %lpad.i.i, %lpad.i, %if.then.i.i7
  %eh.lpad-body = phi { i8*, i32 } [ %16, %if.then.i.i7 ], [ %16, %lpad.i ], [ %22, %lpad ], [ %12, %if.then.i.i.i.i.i ], [ %12, %lpad.i.i ]
  %23 = load i8*, i8** %_M_p.i.i.i, align 8, !tbaa !41
  %24 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp, i64 0, i32 2
  %arraydecay.i.i.i.i = bitcast %union.anon* %24 to i8*
  %cmp.i.i.i = icmp eq i8* %23, %arraydecay.i.i.i.i
  br i1 %cmp.i.i.i, label %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit, label %if.then.i.i

if.then.i.i:                                      ; preds = %lpad.body
  call void @_ZdlPv(i8* %23) #20
  br label %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit

_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit: ; preds = %lpad.body, %if.then.i.i
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %0) #20
  resume { i8*, i32 } %eh.lpad-body
}

declare dso_local void @_ZNK4llvm3orc5LLJIT6mangleB5cxx11ENS_9StringRefE(%"class.std::__cxx11::basic_string"* sret(%"class.std::__cxx11::basic_string") align 8, %"class.llvm::orc::LLJIT"* nonnull dereferenceable(568), i8*, i64) local_unnamed_addr #0

declare dso_local void @_ZN4llvm3orc5LLJIT19lookupLinkerMangledERNS0_8JITDylibENS0_15SymbolStringPtrE(%"class.llvm::Expected"* sret(%"class.llvm::Expected") align 8, %"class.llvm::orc::LLJIT"* nonnull dereferenceable(568), %"class.llvm::orc::JITDylib"* nonnull align 8 dereferenceable(272), %"class.llvm::orc::SymbolStringPtr"*) local_unnamed_addr #0

; Function Attrs: uwtable
define linkonce_odr dso_local { %"class.llvm::StringMapEntryBase"**, i8 } @_ZN4llvm9StringMapISt6atomicImENS_15MallocAllocatorEE11try_emplaceIJiEEESt4pairINS_17StringMapIteratorIS2_EEbENS_9StringRefEDpOT_(%"class.llvm::StringMap.149"* nonnull dereferenceable(25) %this, i8* %Key.coerce0, i64 %Key.coerce1, i32* nonnull align 4 dereferenceable(4) %Args) local_unnamed_addr #4 comdat align 2 {
entry:
  %0 = getelementptr inbounds %"class.llvm::StringMap.149", %"class.llvm::StringMap.149"* %this, i64 0, i32 0
  %call = tail call i32 @_ZN4llvm13StringMapImpl15LookupBucketForENS_9StringRefE(%"class.llvm::StringMapImpl"* nonnull dereferenceable(24) %0, i8* %Key.coerce0, i64 %Key.coerce1)
  %TheTable = getelementptr inbounds %"class.llvm::StringMap.149", %"class.llvm::StringMap.149"* %this, i64 0, i32 0, i32 0
  %1 = load %"class.llvm::StringMapEntryBase"**, %"class.llvm::StringMapEntryBase"*** %TheTable, align 8, !tbaa !184
  %idxprom = zext i32 %call to i64
  %arrayidx = getelementptr inbounds %"class.llvm::StringMapEntryBase"*, %"class.llvm::StringMapEntryBase"** %1, i64 %idxprom
  %2 = load %"class.llvm::StringMapEntryBase"*, %"class.llvm::StringMapEntryBase"** %arrayidx, align 8, !tbaa !46
  %magicptr = ptrtoint %"class.llvm::StringMapEntryBase"* %2 to i64
  switch i64 %magicptr, label %while.cond.i.i.i38 [
    i64 0, label %if.end9
    i64 -8, label %if.then8
  ]

while.cond.i.i.i38:                               ; preds = %entry, %while.body.i.i.i40
  %3 = phi %"class.llvm::StringMapEntryBase"* [ %.pre, %while.body.i.i.i40 ], [ %2, %entry ]
  %ref.tmp.sroa.0.0 = phi %"class.llvm::StringMapEntryBase"** [ %incdec.ptr.i.i.i39, %while.body.i.i.i40 ], [ %arrayidx, %entry ]
  %magicptr.i.i.i37 = ptrtoint %"class.llvm::StringMapEntryBase"* %3 to i64
  switch i64 %magicptr.i.i.i37, label %cleanup [
    i64 0, label %while.body.i.i.i40
    i64 -8, label %while.body.i.i.i40
  ]

while.body.i.i.i40:                               ; preds = %while.cond.i.i.i38, %while.cond.i.i.i38
  %incdec.ptr.i.i.i39 = getelementptr inbounds %"class.llvm::StringMapEntryBase"*, %"class.llvm::StringMapEntryBase"** %ref.tmp.sroa.0.0, i64 1
  %.pre = load %"class.llvm::StringMapEntryBase"*, %"class.llvm::StringMapEntryBase"** %incdec.ptr.i.i.i39, align 8, !tbaa !46
  br label %while.cond.i.i.i38, !llvm.loop !186

if.then8:                                         ; preds = %entry
  %NumTombstones = getelementptr inbounds %"class.llvm::StringMap.149", %"class.llvm::StringMap.149"* %this, i64 0, i32 0, i32 3
  %4 = load i32, i32* %NumTombstones, align 8, !tbaa !187
  %dec = add i32 %4, -1
  store i32 %dec, i32* %NumTombstones, align 8, !tbaa !187
  br label %if.end9

if.end9:                                          ; preds = %entry, %if.then8
  %add1.i = add i64 %Key.coerce1, 17
  %call.i.i = tail call noalias nonnull i8* @_ZN4llvm15allocate_bufferEmm(i64 %add1.i, i64 8)
  %keyLength2.i.i.i.i = bitcast i8* %call.i.i to i64*
  store i64 %Key.coerce1, i64* %keyLength2.i.i.i.i, align 8, !tbaa !188
  %5 = load i32, i32* %Args, align 4, !tbaa !16
  %conv.i.i.i = sext i32 %5 to i64
  %_M_i.i.i.i.i.i = getelementptr inbounds i8, i8* %call.i.i, i64 8
  %6 = bitcast i8* %_M_i.i.i.i.i.i to i64*
  store i64 %conv.i.i.i, i64* %6, align 8, !tbaa !190
  %add.ptr.i.i = getelementptr inbounds i8, i8* %call.i.i, i64 16
  %cmp.not.i = icmp eq i64 %Key.coerce1, 0
  br i1 %cmp.not.i, label %_ZN4llvm14StringMapEntryISt6atomicImEE6CreateINS_15MallocAllocatorEJiEEEPS3_NS_9StringRefERT_DpOT0_.exit, label %if.then.i

if.then.i:                                        ; preds = %if.end9
  tail call void @llvm.memcpy.p0i8.p0i8.i64(i8* nonnull align 1 %add.ptr.i.i, i8* align 1 %Key.coerce0, i64 %Key.coerce1, i1 false)
  br label %_ZN4llvm14StringMapEntryISt6atomicImEE6CreateINS_15MallocAllocatorEJiEEEPS3_NS_9StringRefERT_DpOT0_.exit

_ZN4llvm14StringMapEntryISt6atomicImEE6CreateINS_15MallocAllocatorEJiEEEPS3_NS_9StringRefERT_DpOT0_.exit: ; preds = %if.end9, %if.then.i
  %arrayidx.i = getelementptr inbounds i8, i8* %add.ptr.i.i, i64 %Key.coerce1
  store i8 0, i8* %arrayidx.i, align 1, !tbaa !43
  %7 = bitcast %"class.llvm::StringMapEntryBase"** %arrayidx to i8**
  store i8* %call.i.i, i8** %7, align 8, !tbaa !46
  %NumItems = getelementptr inbounds %"class.llvm::StringMap.149", %"class.llvm::StringMap.149"* %this, i64 0, i32 0, i32 2
  %8 = load i32, i32* %NumItems, align 4, !tbaa !192
  %inc = add i32 %8, 1
  store i32 %inc, i32* %NumItems, align 4, !tbaa !192
  %NumTombstones14 = getelementptr inbounds %"class.llvm::StringMap.149", %"class.llvm::StringMap.149"* %this, i64 0, i32 0, i32 3
  %9 = load i32, i32* %NumTombstones14, align 8, !tbaa !187
  %add = add i32 %9, %inc
  %NumBuckets = getelementptr inbounds %"class.llvm::StringMap.149", %"class.llvm::StringMap.149"* %this, i64 0, i32 0, i32 1
  %10 = load i32, i32* %NumBuckets, align 8, !tbaa !193
  %cmp15.not = icmp ugt i32 %add, %10
  br i1 %cmp15.not, label %cond.false, label %cond.end

cond.false:                                       ; preds = %_ZN4llvm14StringMapEntryISt6atomicImEE6CreateINS_15MallocAllocatorEJiEEEPS3_NS_9StringRefERT_DpOT0_.exit
  tail call void @__assert_fail(i8* getelementptr inbounds ([39 x i8], [39 x i8]* @.str.19, i64 0, i64 0), i8* getelementptr inbounds ([42 x i8], [42 x i8]* @.str.20, i64 0, i64 0), i32 326, i8* getelementptr inbounds ([206 x i8], [206 x i8]* @__PRETTY_FUNCTION__._ZN4llvm9StringMapISt6atomicImENS_15MallocAllocatorEE11try_emplaceIJiEEESt4pairINS_17StringMapIteratorIS2_EEbENS_9StringRefEDpOT_, i64 0, i64 0)) #21
  unreachable

cond.end:                                         ; preds = %_ZN4llvm14StringMapEntryISt6atomicImEE6CreateINS_15MallocAllocatorEJiEEEPS3_NS_9StringRefERT_DpOT0_.exit
  %call16 = tail call i32 @_ZN4llvm13StringMapImpl11RehashTableEj(%"class.llvm::StringMapImpl"* nonnull dereferenceable(24) %0, i32 %call)
  %11 = load %"class.llvm::StringMapEntryBase"**, %"class.llvm::StringMapEntryBase"*** %TheTable, align 8, !tbaa !184
  %idx.ext19 = zext i32 %call16 to i64
  %add.ptr20 = getelementptr inbounds %"class.llvm::StringMapEntryBase"*, %"class.llvm::StringMapEntryBase"** %11, i64 %idx.ext19
  br label %while.cond.i.i.i

while.cond.i.i.i:                                 ; preds = %while.body.i.i.i, %cond.end
  %ref.tmp17.sroa.0.0 = phi %"class.llvm::StringMapEntryBase"** [ %add.ptr20, %cond.end ], [ %incdec.ptr.i.i.i, %while.body.i.i.i ]
  %12 = load %"class.llvm::StringMapEntryBase"*, %"class.llvm::StringMapEntryBase"** %ref.tmp17.sroa.0.0, align 8, !tbaa !46
  %magicptr.i.i.i = ptrtoint %"class.llvm::StringMapEntryBase"* %12 to i64
  switch i64 %magicptr.i.i.i, label %cleanup [
    i64 0, label %while.body.i.i.i
    i64 -8, label %while.body.i.i.i
  ]

while.body.i.i.i:                                 ; preds = %while.cond.i.i.i, %while.cond.i.i.i
  %incdec.ptr.i.i.i = getelementptr inbounds %"class.llvm::StringMapEntryBase"*, %"class.llvm::StringMapEntryBase"** %ref.tmp17.sroa.0.0, i64 1
  br label %while.cond.i.i.i, !llvm.loop !186

cleanup:                                          ; preds = %while.cond.i.i.i, %while.cond.i.i.i38
  %ref.tmp.sroa.0.0.pn = phi %"class.llvm::StringMapEntryBase"** [ %ref.tmp.sroa.0.0, %while.cond.i.i.i38 ], [ %ref.tmp17.sroa.0.0, %while.cond.i.i.i ]
  %.pn = phi i8 [ 0, %while.cond.i.i.i38 ], [ 1, %while.cond.i.i.i ]
  %.fca.0.insert.i42.pn = insertvalue { %"class.llvm::StringMapEntryBase"**, i8 } undef, %"class.llvm::StringMapEntryBase"** %ref.tmp.sroa.0.0.pn, 0
  %call5.pn = insertvalue { %"class.llvm::StringMapEntryBase"**, i8 } %.fca.0.insert.i42.pn, i8 %.pn, 1
  ret { %"class.llvm::StringMapEntryBase"**, i8 } %call5.pn
}

; Function Attrs: noreturn
declare dso_local void @_ZSt20__throw_system_errori(i32) local_unnamed_addr #12

; Function Attrs: nounwind
declare extern_weak dso_local i32 @pthread_mutex_lock(%union.pthread_mutex_t*) local_unnamed_addr #1

; Function Attrs: nounwind
declare extern_weak dso_local i32 @__pthread_key_create(i32*, void (i8*)*) #1

declare dso_local i32 @_ZN4llvm13StringMapImpl15LookupBucketForENS_9StringRefE(%"class.llvm::StringMapImpl"* nonnull dereferenceable(24), i8*, i64) local_unnamed_addr #0

; Function Attrs: noreturn nounwind
declare dso_local void @__assert_fail(i8*, i8*, i32, i8*) local_unnamed_addr #13

declare dso_local i32 @_ZN4llvm13StringMapImpl11RehashTableEj(%"class.llvm::StringMapImpl"* nonnull dereferenceable(24), i32) local_unnamed_addr #0

declare dso_local noalias nonnull i8* @_ZN4llvm15allocate_bufferEmm(i64, i64) local_unnamed_addr #0

; Function Attrs: nounwind
declare extern_weak dso_local i32 @pthread_mutex_unlock(%union.pthread_mutex_t*) local_unnamed_addr #1

; Function Attrs: nounwind
declare dso_local void @_ZN4llvm6ModuleD1Ev(%"class.llvm::Module"* nonnull dereferenceable(744)) unnamed_addr #1

; Function Attrs: nounwind
declare dso_local void @_ZN4llvm24IRBuilderDefaultInserterD1Ev(%"class.llvm::IRBuilderDefaultInserter"* nonnull dereferenceable(8)) unnamed_addr #1

; Function Attrs: nounwind
declare dso_local void @_ZN4llvm15IRBuilderFolderD2Ev(%"class.llvm::IRBuilderFolder"* nonnull dereferenceable(8)) unnamed_addr #1

; Function Attrs: inaccessiblemem_or_argmemonly nounwind willreturn
declare dso_local void @free(i8* nocapture noundef) local_unnamed_addr #14

; Function Attrs: nounwind
declare dso_local void @_ZN4llvm11LLVMContextD1Ev(%"class.llvm::LLVMContext"* nonnull dereferenceable(8)) unnamed_addr #1

; Function Attrs: nobuiltin nofree allocsize(0)
declare dso_local nonnull i8* @_Znam(i64) local_unnamed_addr #7

; Function Attrs: nounwind uwtable
define linkonce_odr dso_local void @_ZN5TableD2Ev(%class.Table* nonnull dereferenceable(60) %this) unnamed_addr #15 comdat align 2 personality i32 (...)* @__gxx_personality_v0 {
entry:
  %0 = getelementptr inbounds %class.Table, %class.Table* %this, i64 0, i32 0
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [4 x i8*] }, { [4 x i8*] }* @_ZTV5Table, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %0, align 8, !tbaa !119
  %types = getelementptr inbounds %class.Table, %class.Table* %this, i64 0, i32 4
  %1 = load i32*, i32** %types, align 8, !tbaa !134
  %isnull = icmp eq i32* %1, null
  br i1 %isnull, label %delete.end, label %delete.notnull

delete.notnull:                                   ; preds = %entry
  %2 = bitcast i32* %1 to i8*
  tail call void @_ZdaPv(i8* %2) #25
  br label %delete.end

delete.end:                                       ; preds = %delete.notnull, %entry
  %_M_start.i.i = getelementptr inbounds %class.Table, %class.Table* %this, i64 0, i32 3, i32 0, i32 0, i32 0
  %3 = load %class.Column**, %class.Column*** %_M_start.i.i, align 8, !tbaa !147
  %tobool.not.i.i.i = icmp eq %class.Column** %3, null
  br i1 %tobool.not.i.i.i, label %_ZNSt6vectorIP6ColumnSaIS1_EED2Ev.exit, label %if.then.i.i.i

if.then.i.i.i:                                    ; preds = %delete.end
  %4 = bitcast %class.Column** %3 to i8*
  tail call void @_ZdlPv(i8* nonnull %4) #20
  br label %_ZNSt6vectorIP6ColumnSaIS1_EED2Ev.exit

_ZNSt6vectorIP6ColumnSaIS1_EED2Ev.exit:           ; preds = %delete.end, %if.then.i.i.i
  ret void
}

; Function Attrs: nounwind uwtable
define linkonce_odr dso_local void @_ZN5TableD0Ev(%class.Table* nonnull dereferenceable(60) %this) unnamed_addr #15 comdat align 2 personality i32 (...)* @__gxx_personality_v0 {
entry:
  %0 = getelementptr inbounds %class.Table, %class.Table* %this, i64 0, i32 0
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [4 x i8*] }, { [4 x i8*] }* @_ZTV5Table, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %0, align 8, !tbaa !119
  %types.i = getelementptr inbounds %class.Table, %class.Table* %this, i64 0, i32 4
  %1 = load i32*, i32** %types.i, align 8, !tbaa !134
  %isnull.i = icmp eq i32* %1, null
  br i1 %isnull.i, label %delete.end.i, label %delete.notnull.i

delete.notnull.i:                                 ; preds = %entry
  %2 = bitcast i32* %1 to i8*
  tail call void @_ZdaPv(i8* %2) #25
  br label %delete.end.i

delete.end.i:                                     ; preds = %delete.notnull.i, %entry
  %_M_start.i.i.i = getelementptr inbounds %class.Table, %class.Table* %this, i64 0, i32 3, i32 0, i32 0, i32 0
  %3 = load %class.Column**, %class.Column*** %_M_start.i.i.i, align 8, !tbaa !147
  %tobool.not.i.i.i.i = icmp eq %class.Column** %3, null
  br i1 %tobool.not.i.i.i.i, label %_ZN5TableD2Ev.exit, label %if.then.i.i.i.i

if.then.i.i.i.i:                                  ; preds = %delete.end.i
  %4 = bitcast %class.Column** %3 to i8*
  tail call void @_ZdlPv(i8* nonnull %4) #20
  br label %_ZN5TableD2Ev.exit

_ZN5TableD2Ev.exit:                               ; preds = %delete.end.i, %if.then.i.i.i.i
  %5 = bitcast %class.Table* %this to i8*
  tail call void @_ZdlPv(i8* nonnull %5) #25
  ret void
}

; Function Attrs: nobuiltin nounwind
declare dso_local void @_ZdaPv(i8*) local_unnamed_addr #9

; Function Attrs: nounwind uwtable willreturn
define linkonce_odr dso_local void @_ZN6ColumnD2Ev(%class.Column* nonnull dereferenceable(40) %this) unnamed_addr #16 comdat align 2 {
entry:
  ret void
}

; Function Attrs: nounwind uwtable
define linkonce_odr dso_local void @_ZN6ColumnD0Ev(%class.Column* nonnull dereferenceable(40) %this) unnamed_addr #15 comdat align 2 {
entry:
  %0 = bitcast %class.Column* %this to i8*
  tail call void @_ZdlPv(i8* nonnull %0) #25
  ret void
}

; Function Attrs: argmemonly nofree nosync nounwind willreturn
declare void @llvm.memmove.p0i8.p0i8.i64(i8* nocapture writeonly, i8* nocapture readonly, i64, i1 immarg) #5

; Function Attrs: uwtable
define linkonce_odr dso_local %"struct.std::_Rb_tree_node_base"* @_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE22_M_emplace_hint_uniqueIJRKSt21piecewise_construct_tSt5tupleIJOS5_EESM_IJEEEEESt17_Rb_tree_iteratorISB_ESt23_Rb_tree_const_iteratorISB_EDpOT_(%"class.std::_Rb_tree"* nonnull dereferenceable(48) %this, %"struct.std::_Rb_tree_node_base"* %__pos.coerce, %"struct.std::piecewise_construct_t"* nonnull align 1 dereferenceable(1) %__args, %"class.std::tuple.464"* nonnull align 8 dereferenceable(8) %__args1, %"class.std::tuple.467"* nonnull align 1 dereferenceable(1) %__args3) local_unnamed_addr #4 comdat align 2 personality i8* bitcast (i32 (...)* @__gxx_personality_v0 to i8*) {
entry:
  %call2.i.i.i.i = tail call noalias nonnull i8* @_Znwm(i64 72) #22
  %_M_storage.i.i.i35 = getelementptr inbounds i8, i8* %call2.i.i.i.i, i64 32
  %_M_head_impl.i.i.i.i.i.i.i.i = getelementptr inbounds %"class.std::tuple.464", %"class.std::tuple.464"* %__args1, i64 0, i32 0, i32 0, i32 0
  %0 = load %"class.std::__cxx11::basic_string"*, %"class.std::__cxx11::basic_string"** %_M_head_impl.i.i.i.i.i.i.i.i, align 8, !tbaa !194
  %1 = getelementptr inbounds i8, i8* %call2.i.i.i.i, i64 48
  %2 = bitcast i8* %_M_storage.i.i.i35 to i8**
  store i8* %1, i8** %2, align 8, !tbaa !39
  %_M_p.i.i25.i.i.i.i.i.i.i = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %0, i64 0, i32 0, i32 0
  %3 = load i8*, i8** %_M_p.i.i25.i.i.i.i.i.i.i, align 8, !tbaa !41
  %4 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %0, i64 0, i32 2
  %arraydecay.i.i.i.i.i.i.i.i.i = bitcast %union.anon* %4 to i8*
  %cmp.i.i.i.i.i.i.i.i36 = icmp eq i8* %3, %arraydecay.i.i.i.i.i.i.i.i.i
  br i1 %cmp.i.i.i.i.i.i.i.i36, label %if.then.i.i.i.i.i.i.i37, label %if.else.i.i.i.i.i.i.i

if.then.i.i.i.i.i.i.i37:                          ; preds = %entry
  tail call void @llvm.memcpy.p0i8.p0i8.i64(i8* nonnull align 1 dereferenceable(16) %1, i8* nonnull align 8 dereferenceable(16) %3, i64 16, i1 false) #20
  br label %invoke.cont

if.else.i.i.i.i.i.i.i:                            ; preds = %entry
  store i8* %3, i8** %2, align 8, !tbaa !41
  %_M_allocated_capacity.i.i.i.i.i.i.i = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %0, i64 0, i32 2, i32 0
  %5 = load i64, i64* %_M_allocated_capacity.i.i.i.i.i.i.i, align 8, !tbaa !43
  %6 = bitcast i8* %1 to i64*
  store i64 %5, i64* %6, align 8, !tbaa !43
  br label %invoke.cont

invoke.cont:                                      ; preds = %if.else.i.i.i.i.i.i.i, %if.then.i.i.i.i.i.i.i37
  %_M_string_length.i22.i.i.i.i.i.i.i = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %0, i64 0, i32 1
  %7 = load i64, i64* %_M_string_length.i22.i.i.i.i.i.i.i, align 8, !tbaa !44
  %8 = getelementptr inbounds i8, i8* %call2.i.i.i.i, i64 40
  %9 = bitcast i8* %8 to i64*
  store i64 %7, i64* %9, align 8, !tbaa !44
  %10 = bitcast %"class.std::__cxx11::basic_string"* %0 to %union.anon**
  store %union.anon* %4, %union.anon** %10, align 8, !tbaa !41
  store i64 0, i64* %_M_string_length.i22.i.i.i.i.i.i.i, align 8, !tbaa !44
  store i8 0, i8* %arraydecay.i.i.i.i.i.i.i.i.i, align 8, !tbaa !43
  %11 = getelementptr inbounds i8, i8* %call2.i.i.i.i, i64 64
  %12 = bitcast i8* %11 to %"class.codegen::ParamValue"**
  store %"class.codegen::ParamValue"* null, %"class.codegen::ParamValue"** %12, align 8, !tbaa !167
  %first.i.i = bitcast i8* %_M_storage.i.i.i35 to %"class.std::__cxx11::basic_string"*
  %call12 = invoke { %"struct.std::_Rb_tree_node_base"*, %"struct.std::_Rb_tree_node_base"* } @_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE29_M_get_insert_hint_unique_posESt23_Rb_tree_const_iteratorISB_ERS7_(%"class.std::_Rb_tree"* nonnull dereferenceable(48) %this, %"struct.std::_Rb_tree_node_base"* %__pos.coerce, %"class.std::__cxx11::basic_string"* nonnull align 8 dereferenceable(32) %first.i.i)
          to label %invoke.cont11 unwind label %lpad

invoke.cont11:                                    ; preds = %invoke.cont
  %13 = extractvalue { %"struct.std::_Rb_tree_node_base"*, %"struct.std::_Rb_tree_node_base"* } %call12, 0
  %14 = extractvalue { %"struct.std::_Rb_tree_node_base"*, %"struct.std::_Rb_tree_node_base"* } %call12, 1
  %tobool.not = icmp eq %"struct.std::_Rb_tree_node_base"* %14, null
  br i1 %tobool.not, label %if.end, label %if.then

if.then:                                          ; preds = %invoke.cont11
  %cmp.not.i = icmp eq %"struct.std::_Rb_tree_node_base"* %13, null
  br i1 %cmp.not.i, label %lor.lhs.false.i, label %invoke.cont14

lor.lhs.false.i:                                  ; preds = %if.then
  %15 = getelementptr inbounds %"class.std::_Rb_tree", %"class.std::_Rb_tree"* %this, i64 0, i32 0, i32 0, i32 0, i32 0
  %add.ptr.i.i = getelementptr inbounds i8, i8* %15, i64 8
  %_M_header.i.i = bitcast i8* %add.ptr.i.i to %"struct.std::_Rb_tree_node_base"*
  %cmp2.i = icmp eq %"struct.std::_Rb_tree_node_base"* %14, %_M_header.i.i
  br i1 %cmp2.i, label %invoke.cont14, label %lor.rhs.i

lor.rhs.i:                                        ; preds = %lor.lhs.false.i
  %16 = load i64, i64* %9, align 8, !tbaa !44
  %_M_string_length.i14.i.i.i.i = getelementptr inbounds %"struct.std::_Rb_tree_node_base", %"struct.std::_Rb_tree_node_base"* %14, i64 1, i32 1
  %17 = bitcast %"struct.std::_Rb_tree_node_base"** %_M_string_length.i14.i.i.i.i to i64*
  %18 = load i64, i64* %17, align 8, !tbaa !44
  %cmp.i15.i.i.i.i = icmp ugt i64 %16, %18
  %.sroa.speculated.i.i.i.i = select i1 %cmp.i15.i.i.i.i, i64 %18, i64 %16
  %cmp.i13.i.i.i.i = icmp eq i64 %.sroa.speculated.i.i.i.i, 0
  br i1 %cmp.i13.i.i.i.i, label %if.then.i.i.i.i, label %_ZNSt11char_traitsIcE7compareEPKcS2_m.exit.i.i.i.i

_ZNSt11char_traitsIcE7compareEPKcS2_m.exit.i.i.i.i: ; preds = %lor.rhs.i
  %_M_storage.i.i.i13.i = getelementptr inbounds %"struct.std::_Rb_tree_node_base", %"struct.std::_Rb_tree_node_base"* %14, i64 1
  %_M_p.i.i.i.i.i.i = bitcast %"struct.std::_Rb_tree_node_base"* %_M_storage.i.i.i13.i to i8**
  %19 = load i8*, i8** %_M_p.i.i.i.i.i.i, align 8, !tbaa !41
  %20 = load i8*, i8** %2, align 8, !tbaa !41
  %call.i.i.i.i.i = tail call i32 @memcmp(i8* %20, i8* %19, i64 %.sroa.speculated.i.i.i.i) #20
  %tobool.not.i.i.i.i = icmp eq i32 %call.i.i.i.i.i, 0
  br i1 %tobool.not.i.i.i.i, label %if.then.i.i.i.i, label %_ZNKSt4lessINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEEclERKS5_S8_.exit.i

if.then.i.i.i.i:                                  ; preds = %_ZNSt11char_traitsIcE7compareEPKcS2_m.exit.i.i.i.i, %lor.rhs.i
  %sub.i.i.i.i.i = sub i64 %16, %18
  %cmp.i.i.i.i.i = icmp sgt i64 %sub.i.i.i.i.i, 2147483647
  br i1 %cmp.i.i.i.i.i, label %_ZNKSt4lessINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEEclERKS5_S8_.exit.i, label %if.else.i.i.i.i.i

if.else.i.i.i.i.i:                                ; preds = %if.then.i.i.i.i
  %21 = icmp sgt i64 %sub.i.i.i.i.i, -2147483648
  %spec.select7.i.i.i.i.i = select i1 %21, i64 %sub.i.i.i.i.i, i64 -2147483648
  %22 = trunc i64 %spec.select7.i.i.i.i.i to i32
  br label %_ZNKSt4lessINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEEclERKS5_S8_.exit.i

_ZNKSt4lessINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEEclERKS5_S8_.exit.i: ; preds = %if.else.i.i.i.i.i, %if.then.i.i.i.i, %_ZNSt11char_traitsIcE7compareEPKcS2_m.exit.i.i.i.i
  %__r.0.i.i.i.i = phi i32 [ %call.i.i.i.i.i, %_ZNSt11char_traitsIcE7compareEPKcS2_m.exit.i.i.i.i ], [ %22, %if.else.i.i.i.i.i ], [ 2147483647, %if.then.i.i.i.i ]
  %cmp.i.i.i = icmp slt i32 %__r.0.i.i.i.i, 0
  br label %invoke.cont14

invoke.cont14:                                    ; preds = %if.then, %_ZNKSt4lessINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEEclERKS5_S8_.exit.i, %lor.lhs.false.i
  %23 = phi i1 [ true, %lor.lhs.false.i ], [ %cmp.i.i.i, %_ZNKSt4lessINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEEclERKS5_S8_.exit.i ], [ true, %if.then ]
  %24 = bitcast i8* %call2.i.i.i.i to %"struct.std::_Rb_tree_node_base"*
  %25 = getelementptr inbounds %"class.std::_Rb_tree", %"class.std::_Rb_tree"* %this, i64 0, i32 0, i32 0, i32 0, i32 0
  %add.ptr.i = getelementptr inbounds i8, i8* %25, i64 8
  %_M_header.i = bitcast i8* %add.ptr.i to %"struct.std::_Rb_tree_node_base"*
  tail call void @_ZSt29_Rb_tree_insert_and_rebalancebPSt18_Rb_tree_node_baseS0_RS_(i1 zeroext %23, %"struct.std::_Rb_tree_node_base"* nonnull %24, %"struct.std::_Rb_tree_node_base"* nonnull %14, %"struct.std::_Rb_tree_node_base"* nonnull align 8 dereferenceable(32) %_M_header.i) #20
  %_M_node_count.i = getelementptr inbounds i8, i8* %25, i64 40
  %26 = bitcast i8* %_M_node_count.i to i64*
  %27 = load i64, i64* %26, align 8, !tbaa !25
  %inc.i = add i64 %27, 1
  store i64 %inc.i, i64* %26, align 8, !tbaa !25
  br label %cleanup

lpad:                                             ; preds = %invoke.cont
  %28 = landingpad { i8*, i32 }
          catch i8* null
  %29 = extractvalue { i8*, i32 } %28, 0
  %30 = tail call i8* @__cxa_begin_catch(i8* %29) #20
  %31 = load i8*, i8** %2, align 8, !tbaa !41
  %cmp.i.i.i.i.i.i.i.i41 = icmp eq i8* %31, %1
  br i1 %cmp.i.i.i.i.i.i.i.i41, label %_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE12_M_drop_nodeEPSt13_Rb_tree_nodeISB_E.exit43, label %if.then.i.i.i.i.i.i.i42

if.then.i.i.i.i.i.i.i42:                          ; preds = %lpad
  tail call void @_ZdlPv(i8* %31) #20
  br label %_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE12_M_drop_nodeEPSt13_Rb_tree_nodeISB_E.exit43

_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE12_M_drop_nodeEPSt13_Rb_tree_nodeISB_E.exit43: ; preds = %lpad, %if.then.i.i.i.i.i.i.i42
  tail call void @_ZdlPv(i8* nonnull %call2.i.i.i.i) #20
  invoke void @__cxa_rethrow() #24
          to label %unreachable unwind label %lpad18

if.end:                                           ; preds = %invoke.cont11
  %32 = load i8*, i8** %2, align 8, !tbaa !41
  %cmp.i.i.i.i.i.i.i.i = icmp eq i8* %32, %1
  br i1 %cmp.i.i.i.i.i.i.i.i, label %_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE12_M_drop_nodeEPSt13_Rb_tree_nodeISB_E.exit, label %if.then.i.i.i.i.i.i.i

if.then.i.i.i.i.i.i.i:                            ; preds = %if.end
  tail call void @_ZdlPv(i8* %32) #20
  br label %_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE12_M_drop_nodeEPSt13_Rb_tree_nodeISB_E.exit

_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE12_M_drop_nodeEPSt13_Rb_tree_nodeISB_E.exit: ; preds = %if.end, %if.then.i.i.i.i.i.i.i
  tail call void @_ZdlPv(i8* nonnull %call2.i.i.i.i) #20
  br label %cleanup

cleanup:                                          ; preds = %_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE12_M_drop_nodeEPSt13_Rb_tree_nodeISB_E.exit, %invoke.cont14
  %retval.sroa.0.0 = phi %"struct.std::_Rb_tree_node_base"* [ %13, %_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE12_M_drop_nodeEPSt13_Rb_tree_nodeISB_E.exit ], [ %24, %invoke.cont14 ]
  ret %"struct.std::_Rb_tree_node_base"* %retval.sroa.0.0

lpad18:                                           ; preds = %_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE12_M_drop_nodeEPSt13_Rb_tree_nodeISB_E.exit43
  %33 = landingpad { i8*, i32 }
          cleanup
  invoke void @__cxa_end_catch()
          to label %invoke.cont19 unwind label %terminate.lpad

invoke.cont19:                                    ; preds = %lpad18
  resume { i8*, i32 } %33

terminate.lpad:                                   ; preds = %lpad18
  %34 = landingpad { i8*, i32 }
          catch i8* null
  %35 = extractvalue { i8*, i32 } %34, 0
  tail call void @__clang_call_terminate(i8* %35) #21
  unreachable

unreachable:                                      ; preds = %_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE12_M_drop_nodeEPSt13_Rb_tree_nodeISB_E.exit43
  unreachable
}

; Function Attrs: argmemonly nofree nounwind readonly willreturn
declare dso_local i32 @memcmp(i8* nocapture, i8* nocapture, i64) local_unnamed_addr #17

; Function Attrs: uwtable
define linkonce_odr dso_local { %"struct.std::_Rb_tree_node_base"*, %"struct.std::_Rb_tree_node_base"* } @_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE29_M_get_insert_hint_unique_posESt23_Rb_tree_const_iteratorISB_ERS7_(%"class.std::_Rb_tree"* nonnull dereferenceable(48) %this, %"struct.std::_Rb_tree_node_base"* %__position.coerce, %"class.std::__cxx11::basic_string"* nonnull align 8 dereferenceable(32) %__k) local_unnamed_addr #4 comdat align 2 personality i32 (...)* @__gxx_personality_v0 {
entry:
  %0 = getelementptr inbounds %"class.std::_Rb_tree", %"class.std::_Rb_tree"* %this, i64 0, i32 0, i32 0, i32 0, i32 0
  %add.ptr.i = getelementptr inbounds i8, i8* %0, i64 8
  %_M_header.i = bitcast i8* %add.ptr.i to %"struct.std::_Rb_tree_node_base"*
  %cmp = icmp eq %"struct.std::_Rb_tree_node_base"* %_M_header.i, %__position.coerce
  br i1 %cmp, label %if.then, label %if.else12

if.then:                                          ; preds = %entry
  %_M_node_count.i = getelementptr inbounds i8, i8* %0, i64 40
  %1 = bitcast i8* %_M_node_count.i to i64*
  %2 = load i64, i64* %1, align 8, !tbaa !25
  %cmp5.not = icmp eq i64 %2, 0
  br i1 %cmp5.not, label %if.else, label %land.lhs.true

land.lhs.true:                                    ; preds = %if.then
  %_M_right.i94 = getelementptr inbounds i8, i8* %0, i64 32
  %3 = bitcast i8* %_M_right.i94 to %"struct.std::_Rb_tree_node_base"**
  %4 = load %"struct.std::_Rb_tree_node_base"*, %"struct.std::_Rb_tree_node_base"** %3, align 8, !tbaa !46
  %_M_string_length.i.i.i.i121 = getelementptr inbounds %"struct.std::_Rb_tree_node_base", %"struct.std::_Rb_tree_node_base"* %4, i64 1, i32 1
  %5 = bitcast %"struct.std::_Rb_tree_node_base"** %_M_string_length.i.i.i.i121 to i64*
  %6 = load i64, i64* %5, align 8, !tbaa !44
  %_M_string_length.i14.i.i.i122 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %__k, i64 0, i32 1
  %7 = load i64, i64* %_M_string_length.i14.i.i.i122, align 8, !tbaa !44
  %cmp.i15.i.i.i123 = icmp ugt i64 %6, %7
  %.sroa.speculated.i.i.i124 = select i1 %cmp.i15.i.i.i123, i64 %7, i64 %6
  %cmp.i13.i.i.i125 = icmp eq i64 %.sroa.speculated.i.i.i124, 0
  br i1 %cmp.i13.i.i.i125, label %if.then.i.i.i133, label %_ZNSt11char_traitsIcE7compareEPKcS2_m.exit.i.i.i130

_ZNSt11char_traitsIcE7compareEPKcS2_m.exit.i.i.i130: ; preds = %land.lhs.true
  %_M_storage.i.i.i117 = getelementptr inbounds %"struct.std::_Rb_tree_node_base", %"struct.std::_Rb_tree_node_base"* %4, i64 1
  %_M_p.i.i.i.i.i126 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %__k, i64 0, i32 0, i32 0
  %8 = load i8*, i8** %_M_p.i.i.i.i.i126, align 8, !tbaa !41
  %_M_p.i.i.i.i127 = bitcast %"struct.std::_Rb_tree_node_base"* %_M_storage.i.i.i117 to i8**
  %9 = load i8*, i8** %_M_p.i.i.i.i127, align 8, !tbaa !41
  %call.i.i.i.i128 = tail call i32 @memcmp(i8* %9, i8* %8, i64 %.sroa.speculated.i.i.i124) #20
  %tobool.not.i.i.i129 = icmp eq i32 %call.i.i.i.i128, 0
  br i1 %tobool.not.i.i.i129, label %if.then.i.i.i133, label %_ZNKSt4lessINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEEclERKS5_S8_.exit138

if.then.i.i.i133:                                 ; preds = %_ZNSt11char_traitsIcE7compareEPKcS2_m.exit.i.i.i130, %land.lhs.true
  %sub.i.i.i.i131 = sub i64 %6, %7
  %cmp.i.i.i.i132 = icmp sgt i64 %sub.i.i.i.i131, 2147483647
  br i1 %cmp.i.i.i.i132, label %if.else, label %if.else.i.i.i.i135

if.else.i.i.i.i135:                               ; preds = %if.then.i.i.i133
  %10 = icmp sgt i64 %sub.i.i.i.i131, -2147483648
  %spec.select7.i.i.i.i134 = select i1 %10, i64 %sub.i.i.i.i131, i64 -2147483648
  %11 = trunc i64 %spec.select7.i.i.i.i134 to i32
  br label %_ZNKSt4lessINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEEclERKS5_S8_.exit138

_ZNKSt4lessINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEEclERKS5_S8_.exit138: ; preds = %_ZNSt11char_traitsIcE7compareEPKcS2_m.exit.i.i.i130, %if.else.i.i.i.i135
  %__r.0.i.i.i136 = phi i32 [ %call.i.i.i.i128, %_ZNSt11char_traitsIcE7compareEPKcS2_m.exit.i.i.i130 ], [ %11, %if.else.i.i.i.i135 ]
  %cmp.i.i137 = icmp slt i32 %__r.0.i.i.i136, 0
  br i1 %cmp.i.i137, label %cleanup80, label %if.else

if.else:                                          ; preds = %if.then.i.i.i133, %_ZNKSt4lessINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEEclERKS5_S8_.exit138, %if.then
  %call11 = tail call { %"struct.std::_Rb_tree_node_base"*, %"struct.std::_Rb_tree_node_base"* } @_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE24_M_get_insert_unique_posERS7_(%"class.std::_Rb_tree"* nonnull dereferenceable(48) %this, %"class.std::__cxx11::basic_string"* nonnull align 8 dereferenceable(32) %__k)
  %12 = extractvalue { %"struct.std::_Rb_tree_node_base"*, %"struct.std::_Rb_tree_node_base"* } %call11, 0
  %13 = extractvalue { %"struct.std::_Rb_tree_node_base"*, %"struct.std::_Rb_tree_node_base"* } %call11, 1
  br label %cleanup80

if.else12:                                        ; preds = %entry
  %_M_storage.i.i.i169 = getelementptr inbounds %"struct.std::_Rb_tree_node_base", %"struct.std::_Rb_tree_node_base"* %__position.coerce, i64 1
  %_M_string_length.i.i.i.i175 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %__k, i64 0, i32 1
  %14 = load i64, i64* %_M_string_length.i.i.i.i175, align 8, !tbaa !44
  %_M_string_length.i14.i.i.i176 = getelementptr inbounds %"struct.std::_Rb_tree_node_base", %"struct.std::_Rb_tree_node_base"* %__position.coerce, i64 1, i32 1
  %15 = bitcast %"struct.std::_Rb_tree_node_base"** %_M_string_length.i14.i.i.i176 to i64*
  %16 = load i64, i64* %15, align 8, !tbaa !44
  %cmp.i15.i.i.i177 = icmp ugt i64 %14, %16
  %.sroa.speculated.i.i.i178 = select i1 %cmp.i15.i.i.i177, i64 %16, i64 %14
  %cmp.i13.i.i.i179 = icmp eq i64 %.sroa.speculated.i.i.i178, 0
  br i1 %cmp.i13.i.i.i179, label %if.then.i.i.i187, label %_ZNSt11char_traitsIcE7compareEPKcS2_m.exit.i.i.i184

_ZNSt11char_traitsIcE7compareEPKcS2_m.exit.i.i.i184: ; preds = %if.else12
  %_M_p.i.i.i.i.i180 = bitcast %"struct.std::_Rb_tree_node_base"* %_M_storage.i.i.i169 to i8**
  %17 = load i8*, i8** %_M_p.i.i.i.i.i180, align 8, !tbaa !41
  %_M_p.i.i.i.i181 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %__k, i64 0, i32 0, i32 0
  %18 = load i8*, i8** %_M_p.i.i.i.i181, align 8, !tbaa !41
  %call.i.i.i.i182 = tail call i32 @memcmp(i8* %18, i8* %17, i64 %.sroa.speculated.i.i.i178) #20
  %tobool.not.i.i.i183 = icmp eq i32 %call.i.i.i.i182, 0
  br i1 %tobool.not.i.i.i183, label %if.then.i.i.i187, label %_ZNKSt4lessINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEEclERKS5_S8_.exit192

if.then.i.i.i187:                                 ; preds = %_ZNSt11char_traitsIcE7compareEPKcS2_m.exit.i.i.i184, %if.else12
  %sub.i.i.i.i185 = sub i64 %14, %16
  %cmp.i.i.i.i186 = icmp sgt i64 %sub.i.i.i.i185, 2147483647
  br i1 %cmp.i.i.i.i186, label %if.else44, label %if.else.i.i.i.i189

if.else.i.i.i.i189:                               ; preds = %if.then.i.i.i187
  %19 = icmp sgt i64 %sub.i.i.i.i185, -2147483648
  %spec.select7.i.i.i.i188 = select i1 %19, i64 %sub.i.i.i.i185, i64 -2147483648
  %20 = trunc i64 %spec.select7.i.i.i.i188 to i32
  br label %_ZNKSt4lessINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEEclERKS5_S8_.exit192

_ZNKSt4lessINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEEclERKS5_S8_.exit192: ; preds = %_ZNSt11char_traitsIcE7compareEPKcS2_m.exit.i.i.i184, %if.else.i.i.i.i189
  %__r.0.i.i.i190 = phi i32 [ %call.i.i.i.i182, %_ZNSt11char_traitsIcE7compareEPKcS2_m.exit.i.i.i184 ], [ %20, %if.else.i.i.i.i189 ]
  %cmp.i.i191 = icmp slt i32 %__r.0.i.i.i190, 0
  br i1 %cmp.i.i191, label %if.then18, label %if.else44

if.then18:                                        ; preds = %_ZNKSt4lessINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEEclERKS5_S8_.exit192
  %_M_left.i174 = getelementptr inbounds i8, i8* %0, i64 24
  %21 = bitcast i8* %_M_left.i174 to %"struct.std::_Rb_tree_node_base"**
  %22 = load %"struct.std::_Rb_tree_node_base"*, %"struct.std::_Rb_tree_node_base"** %21, align 8, !tbaa !46
  %cmp21 = icmp eq %"struct.std::_Rb_tree_node_base"* %22, %__position.coerce
  br i1 %cmp21, label %cleanup80, label %if.else25

if.else25:                                        ; preds = %if.then18
  %call.i168 = tail call %"struct.std::_Rb_tree_node_base"* @_ZSt18_Rb_tree_decrementPSt18_Rb_tree_node_base(%"struct.std::_Rb_tree_node_base"* nonnull %__position.coerce) #26
  %_M_string_length.i.i.i.i145 = getelementptr inbounds %"struct.std::_Rb_tree_node_base", %"struct.std::_Rb_tree_node_base"* %call.i168, i64 1, i32 1
  %23 = bitcast %"struct.std::_Rb_tree_node_base"** %_M_string_length.i.i.i.i145 to i64*
  %24 = load i64, i64* %23, align 8, !tbaa !44
  %cmp.i15.i.i.i147 = icmp ugt i64 %24, %14
  %.sroa.speculated.i.i.i148 = select i1 %cmp.i15.i.i.i147, i64 %14, i64 %24
  %cmp.i13.i.i.i149 = icmp eq i64 %.sroa.speculated.i.i.i148, 0
  br i1 %cmp.i13.i.i.i149, label %if.then.i.i.i157, label %_ZNSt11char_traitsIcE7compareEPKcS2_m.exit.i.i.i154

_ZNSt11char_traitsIcE7compareEPKcS2_m.exit.i.i.i154: ; preds = %if.else25
  %_M_storage.i.i.i165 = getelementptr inbounds %"struct.std::_Rb_tree_node_base", %"struct.std::_Rb_tree_node_base"* %call.i168, i64 1
  %_M_p.i.i.i.i.i150 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %__k, i64 0, i32 0, i32 0
  %25 = load i8*, i8** %_M_p.i.i.i.i.i150, align 8, !tbaa !41
  %_M_p.i.i.i.i151 = bitcast %"struct.std::_Rb_tree_node_base"* %_M_storage.i.i.i165 to i8**
  %26 = load i8*, i8** %_M_p.i.i.i.i151, align 8, !tbaa !41
  %call.i.i.i.i152 = tail call i32 @memcmp(i8* %26, i8* %25, i64 %.sroa.speculated.i.i.i148) #20
  %tobool.not.i.i.i153 = icmp eq i32 %call.i.i.i.i152, 0
  br i1 %tobool.not.i.i.i153, label %if.then.i.i.i157, label %_ZNKSt4lessINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEEclERKS5_S8_.exit162

if.then.i.i.i157:                                 ; preds = %_ZNSt11char_traitsIcE7compareEPKcS2_m.exit.i.i.i154, %if.else25
  %sub.i.i.i.i155 = sub i64 %24, %14
  %cmp.i.i.i.i156 = icmp sgt i64 %sub.i.i.i.i155, 2147483647
  br i1 %cmp.i.i.i.i156, label %if.else42, label %if.else.i.i.i.i159

if.else.i.i.i.i159:                               ; preds = %if.then.i.i.i157
  %27 = icmp sgt i64 %sub.i.i.i.i155, -2147483648
  %spec.select7.i.i.i.i158 = select i1 %27, i64 %sub.i.i.i.i155, i64 -2147483648
  %28 = trunc i64 %spec.select7.i.i.i.i158 to i32
  br label %_ZNKSt4lessINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEEclERKS5_S8_.exit162

_ZNKSt4lessINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEEclERKS5_S8_.exit162: ; preds = %_ZNSt11char_traitsIcE7compareEPKcS2_m.exit.i.i.i154, %if.else.i.i.i.i159
  %__r.0.i.i.i160 = phi i32 [ %call.i.i.i.i152, %_ZNSt11char_traitsIcE7compareEPKcS2_m.exit.i.i.i154 ], [ %28, %if.else.i.i.i.i159 ]
  %cmp.i.i161 = icmp slt i32 %__r.0.i.i.i160, 0
  br i1 %cmp.i.i161, label %if.then32, label %if.else42

if.then32:                                        ; preds = %_ZNKSt4lessINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEEclERKS5_S8_.exit162
  %_M_right.i144 = getelementptr inbounds %"struct.std::_Rb_tree_node_base", %"struct.std::_Rb_tree_node_base"* %call.i168, i64 0, i32 3
  %29 = bitcast %"struct.std::_Rb_tree_node_base"** %_M_right.i144 to %"struct.std::_Rb_tree_node"**
  %30 = load %"struct.std::_Rb_tree_node"*, %"struct.std::_Rb_tree_node"** %29, align 8, !tbaa !49
  %cmp35 = icmp eq %"struct.std::_Rb_tree_node"* %30, null
  %spec.select = select i1 %cmp35, %"struct.std::_Rb_tree_node_base"* null, %"struct.std::_Rb_tree_node_base"* %__position.coerce
  %spec.select224 = select i1 %cmp35, %"struct.std::_Rb_tree_node_base"* %call.i168, %"struct.std::_Rb_tree_node_base"* %__position.coerce
  br label %cleanup80

if.else42:                                        ; preds = %if.then.i.i.i157, %_ZNKSt4lessINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEEclERKS5_S8_.exit162
  %call43 = tail call { %"struct.std::_Rb_tree_node_base"*, %"struct.std::_Rb_tree_node_base"* } @_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE24_M_get_insert_unique_posERS7_(%"class.std::_Rb_tree"* nonnull dereferenceable(48) %this, %"class.std::__cxx11::basic_string"* nonnull align 8 dereferenceable(32) %__k)
  %31 = extractvalue { %"struct.std::_Rb_tree_node_base"*, %"struct.std::_Rb_tree_node_base"* } %call43, 0
  %32 = extractvalue { %"struct.std::_Rb_tree_node_base"*, %"struct.std::_Rb_tree_node_base"* } %call43, 1
  br label %cleanup80

if.else44:                                        ; preds = %if.then.i.i.i187, %_ZNKSt4lessINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEEclERKS5_S8_.exit192
  br i1 %cmp.i13.i.i.i179, label %if.then.i.i.i111, label %_ZNSt11char_traitsIcE7compareEPKcS2_m.exit.i.i.i108

_ZNSt11char_traitsIcE7compareEPKcS2_m.exit.i.i.i108: ; preds = %if.else44
  %_M_p.i.i.i.i.i104 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %__k, i64 0, i32 0, i32 0
  %33 = load i8*, i8** %_M_p.i.i.i.i.i104, align 8, !tbaa !41
  %_M_p.i.i.i.i105 = bitcast %"struct.std::_Rb_tree_node_base"* %_M_storage.i.i.i169 to i8**
  %34 = load i8*, i8** %_M_p.i.i.i.i105, align 8, !tbaa !41
  %call.i.i.i.i106 = tail call i32 @memcmp(i8* %34, i8* %33, i64 %.sroa.speculated.i.i.i178) #20
  %tobool.not.i.i.i107 = icmp eq i32 %call.i.i.i.i106, 0
  br i1 %tobool.not.i.i.i107, label %if.then.i.i.i111, label %_ZNKSt4lessINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEEclERKS5_S8_.exit116

if.then.i.i.i111:                                 ; preds = %_ZNSt11char_traitsIcE7compareEPKcS2_m.exit.i.i.i108, %if.else44
  %sub.i.i.i.i109 = sub i64 %16, %14
  %cmp.i.i.i.i110 = icmp sgt i64 %sub.i.i.i.i109, 2147483647
  br i1 %cmp.i.i.i.i110, label %cleanup80, label %if.else.i.i.i.i113

if.else.i.i.i.i113:                               ; preds = %if.then.i.i.i111
  %35 = icmp sgt i64 %sub.i.i.i.i109, -2147483648
  %spec.select7.i.i.i.i112 = select i1 %35, i64 %sub.i.i.i.i109, i64 -2147483648
  %36 = trunc i64 %spec.select7.i.i.i.i112 to i32
  br label %_ZNKSt4lessINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEEclERKS5_S8_.exit116

_ZNKSt4lessINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEEclERKS5_S8_.exit116: ; preds = %_ZNSt11char_traitsIcE7compareEPKcS2_m.exit.i.i.i108, %if.else.i.i.i.i113
  %__r.0.i.i.i114 = phi i32 [ %call.i.i.i.i106, %_ZNSt11char_traitsIcE7compareEPKcS2_m.exit.i.i.i108 ], [ %36, %if.else.i.i.i.i113 ]
  %cmp.i.i115 = icmp slt i32 %__r.0.i.i.i114, 0
  br i1 %cmp.i.i115, label %if.then50, label %cleanup80

if.then50:                                        ; preds = %_ZNKSt4lessINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEEclERKS5_S8_.exit116
  %_M_right.i98 = getelementptr inbounds i8, i8* %0, i64 32
  %37 = bitcast i8* %_M_right.i98 to %"struct.std::_Rb_tree_node_base"**
  %38 = load %"struct.std::_Rb_tree_node_base"*, %"struct.std::_Rb_tree_node_base"** %37, align 8, !tbaa !46
  %cmp53 = icmp eq %"struct.std::_Rb_tree_node_base"* %38, %__position.coerce
  br i1 %cmp53, label %cleanup80, label %if.else57

if.else57:                                        ; preds = %if.then50
  %call.i = tail call %"struct.std::_Rb_tree_node_base"* @_ZSt18_Rb_tree_incrementPSt18_Rb_tree_node_base(%"struct.std::_Rb_tree_node_base"* nonnull %__position.coerce) #26
  %_M_string_length.i14.i.i.i = getelementptr inbounds %"struct.std::_Rb_tree_node_base", %"struct.std::_Rb_tree_node_base"* %call.i, i64 1, i32 1
  %39 = bitcast %"struct.std::_Rb_tree_node_base"** %_M_string_length.i14.i.i.i to i64*
  %40 = load i64, i64* %39, align 8, !tbaa !44
  %cmp.i15.i.i.i = icmp ugt i64 %14, %40
  %.sroa.speculated.i.i.i = select i1 %cmp.i15.i.i.i, i64 %40, i64 %14
  %cmp.i13.i.i.i = icmp eq i64 %.sroa.speculated.i.i.i, 0
  br i1 %cmp.i13.i.i.i, label %if.then.i.i.i, label %_ZNSt11char_traitsIcE7compareEPKcS2_m.exit.i.i.i

_ZNSt11char_traitsIcE7compareEPKcS2_m.exit.i.i.i: ; preds = %if.else57
  %_M_storage.i.i.i = getelementptr inbounds %"struct.std::_Rb_tree_node_base", %"struct.std::_Rb_tree_node_base"* %call.i, i64 1
  %_M_p.i.i.i.i.i = bitcast %"struct.std::_Rb_tree_node_base"* %_M_storage.i.i.i to i8**
  %41 = load i8*, i8** %_M_p.i.i.i.i.i, align 8, !tbaa !41
  %_M_p.i.i.i.i = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %__k, i64 0, i32 0, i32 0
  %42 = load i8*, i8** %_M_p.i.i.i.i, align 8, !tbaa !41
  %call.i.i.i.i = tail call i32 @memcmp(i8* %42, i8* %41, i64 %.sroa.speculated.i.i.i) #20
  %tobool.not.i.i.i = icmp eq i32 %call.i.i.i.i, 0
  br i1 %tobool.not.i.i.i, label %if.then.i.i.i, label %_ZNKSt4lessINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEEclERKS5_S8_.exit

if.then.i.i.i:                                    ; preds = %_ZNSt11char_traitsIcE7compareEPKcS2_m.exit.i.i.i, %if.else57
  %sub.i.i.i.i = sub i64 %14, %40
  %cmp.i.i.i.i = icmp sgt i64 %sub.i.i.i.i, 2147483647
  br i1 %cmp.i.i.i.i, label %if.else74, label %if.else.i.i.i.i

if.else.i.i.i.i:                                  ; preds = %if.then.i.i.i
  %43 = icmp sgt i64 %sub.i.i.i.i, -2147483648
  %spec.select7.i.i.i.i = select i1 %43, i64 %sub.i.i.i.i, i64 -2147483648
  %44 = trunc i64 %spec.select7.i.i.i.i to i32
  br label %_ZNKSt4lessINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEEclERKS5_S8_.exit

_ZNKSt4lessINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEEclERKS5_S8_.exit: ; preds = %_ZNSt11char_traitsIcE7compareEPKcS2_m.exit.i.i.i, %if.else.i.i.i.i
  %__r.0.i.i.i = phi i32 [ %call.i.i.i.i, %_ZNSt11char_traitsIcE7compareEPKcS2_m.exit.i.i.i ], [ %44, %if.else.i.i.i.i ]
  %cmp.i.i = icmp slt i32 %__r.0.i.i.i, 0
  br i1 %cmp.i.i, label %if.then64, label %if.else74

if.then64:                                        ; preds = %_ZNKSt4lessINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEEclERKS5_S8_.exit
  %_M_right.i = getelementptr inbounds %"struct.std::_Rb_tree_node_base", %"struct.std::_Rb_tree_node_base"* %__position.coerce, i64 0, i32 3
  %45 = bitcast %"struct.std::_Rb_tree_node_base"** %_M_right.i to %"struct.std::_Rb_tree_node"**
  %46 = load %"struct.std::_Rb_tree_node"*, %"struct.std::_Rb_tree_node"** %45, align 8, !tbaa !49
  %cmp67 = icmp eq %"struct.std::_Rb_tree_node"* %46, null
  %spec.select225 = select i1 %cmp67, %"struct.std::_Rb_tree_node_base"* null, %"struct.std::_Rb_tree_node_base"* %call.i
  %spec.select226 = select i1 %cmp67, %"struct.std::_Rb_tree_node_base"* %__position.coerce, %"struct.std::_Rb_tree_node_base"* %call.i
  br label %cleanup80

if.else74:                                        ; preds = %if.then.i.i.i, %_ZNKSt4lessINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEEclERKS5_S8_.exit
  %call75 = tail call { %"struct.std::_Rb_tree_node_base"*, %"struct.std::_Rb_tree_node_base"* } @_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE24_M_get_insert_unique_posERS7_(%"class.std::_Rb_tree"* nonnull dereferenceable(48) %this, %"class.std::__cxx11::basic_string"* nonnull align 8 dereferenceable(32) %__k)
  %47 = extractvalue { %"struct.std::_Rb_tree_node_base"*, %"struct.std::_Rb_tree_node_base"* } %call75, 0
  %48 = extractvalue { %"struct.std::_Rb_tree_node_base"*, %"struct.std::_Rb_tree_node_base"* } %call75, 1
  br label %cleanup80

cleanup80:                                        ; preds = %if.then64, %if.then32, %if.then.i.i.i111, %_ZNKSt4lessINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEEclERKS5_S8_.exit116, %if.else74, %if.then50, %if.else42, %if.then18, %_ZNKSt4lessINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEEclERKS5_S8_.exit138, %if.else
  %retval.sroa.0.2 = phi %"struct.std::_Rb_tree_node_base"* [ %12, %if.else ], [ null, %_ZNKSt4lessINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEEclERKS5_S8_.exit138 ], [ %31, %if.else42 ], [ %__position.coerce, %if.then18 ], [ %47, %if.else74 ], [ null, %if.then50 ], [ %__position.coerce, %_ZNKSt4lessINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEEclERKS5_S8_.exit116 ], [ %__position.coerce, %if.then.i.i.i111 ], [ %spec.select, %if.then32 ], [ %spec.select225, %if.then64 ]
  %retval.sroa.12.2 = phi %"struct.std::_Rb_tree_node_base"* [ %13, %if.else ], [ %4, %_ZNKSt4lessINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEEclERKS5_S8_.exit138 ], [ %32, %if.else42 ], [ %__position.coerce, %if.then18 ], [ %48, %if.else74 ], [ %__position.coerce, %if.then50 ], [ null, %_ZNKSt4lessINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEEclERKS5_S8_.exit116 ], [ null, %if.then.i.i.i111 ], [ %spec.select224, %if.then32 ], [ %spec.select226, %if.then64 ]
  %.fca.0.insert = insertvalue { %"struct.std::_Rb_tree_node_base"*, %"struct.std::_Rb_tree_node_base"* } undef, %"struct.std::_Rb_tree_node_base"* %retval.sroa.0.2, 0
  %.fca.1.insert = insertvalue { %"struct.std::_Rb_tree_node_base"*, %"struct.std::_Rb_tree_node_base"* } %.fca.0.insert, %"struct.std::_Rb_tree_node_base"* %retval.sroa.12.2, 1
  ret { %"struct.std::_Rb_tree_node_base"*, %"struct.std::_Rb_tree_node_base"* } %.fca.1.insert
}

; Function Attrs: uwtable
define linkonce_odr dso_local { %"struct.std::_Rb_tree_node_base"*, %"struct.std::_Rb_tree_node_base"* } @_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE24_M_get_insert_unique_posERS7_(%"class.std::_Rb_tree"* nonnull dereferenceable(48) %this, %"class.std::__cxx11::basic_string"* nonnull align 8 dereferenceable(32) %__k) local_unnamed_addr #4 comdat align 2 personality i32 (...)* @__gxx_personality_v0 {
entry:
  %0 = getelementptr inbounds %"class.std::_Rb_tree", %"class.std::_Rb_tree"* %this, i64 0, i32 0, i32 0, i32 0, i32 0
  %_M_parent.i = getelementptr inbounds i8, i8* %0, i64 16
  %1 = bitcast i8* %_M_parent.i to %"struct.std::_Rb_tree_node"**
  %add.ptr.i = getelementptr inbounds i8, i8* %0, i64 8
  %_M_header.i = bitcast i8* %add.ptr.i to %"struct.std::_Rb_tree_node_base"*
  %__x.075 = load %"struct.std::_Rb_tree_node"*, %"struct.std::_Rb_tree_node"** %1, align 8, !tbaa !46
  %cmp.not76 = icmp eq %"struct.std::_Rb_tree_node"* %__x.075, null
  br i1 %cmp.not76, label %if.then, label %while.body.lr.ph

while.body.lr.ph:                                 ; preds = %entry
  %_M_string_length.i.i.i.i35 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %__k, i64 0, i32 1
  %2 = load i64, i64* %_M_string_length.i.i.i.i35, align 8, !tbaa !44
  %_M_p.i.i.i.i41 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %__k, i64 0, i32 0, i32 0
  %3 = load i8*, i8** %_M_p.i.i.i.i41, align 8
  br label %while.body

while.body:                                       ; preds = %while.body.backedge, %while.body.lr.ph
  %__x.077 = phi %"struct.std::_Rb_tree_node"* [ %__x.075, %while.body.lr.ph ], [ %__x.077.be, %while.body.backedge ]
  %_M_string_length.i14.i.i.i36 = getelementptr inbounds %"struct.std::_Rb_tree_node", %"struct.std::_Rb_tree_node"* %__x.077, i64 0, i32 1, i32 0, i64 8
  %4 = bitcast i8* %_M_string_length.i14.i.i.i36 to i64*
  %5 = load i64, i64* %4, align 8, !tbaa !44
  %cmp.i15.i.i.i37 = icmp ugt i64 %2, %5
  %.sroa.speculated.i.i.i38 = select i1 %cmp.i15.i.i.i37, i64 %5, i64 %2
  %cmp.i13.i.i.i39 = icmp eq i64 %.sroa.speculated.i.i.i38, 0
  br i1 %cmp.i13.i.i.i39, label %if.then.i.i.i47, label %_ZNSt11char_traitsIcE7compareEPKcS2_m.exit.i.i.i44

_ZNSt11char_traitsIcE7compareEPKcS2_m.exit.i.i.i44: ; preds = %while.body
  %_M_storage.i.i.i28 = getelementptr inbounds %"struct.std::_Rb_tree_node", %"struct.std::_Rb_tree_node"* %__x.077, i64 0, i32 1
  %_M_p.i.i.i.i.i40 = bitcast %"struct.__gnu_cxx::__aligned_membuf.437"* %_M_storage.i.i.i28 to i8**
  %6 = load i8*, i8** %_M_p.i.i.i.i.i40, align 8, !tbaa !41
  %call.i.i.i.i42 = tail call i32 @memcmp(i8* %3, i8* %6, i64 %.sroa.speculated.i.i.i38) #20
  %tobool.not.i.i.i43 = icmp eq i32 %call.i.i.i.i42, 0
  br i1 %tobool.not.i.i.i43, label %if.then.i.i.i47, label %_ZNKSt4lessINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEEclERKS5_S8_.exit52

if.then.i.i.i47:                                  ; preds = %_ZNSt11char_traitsIcE7compareEPKcS2_m.exit.i.i.i44, %while.body
  %sub.i.i.i.i45 = sub i64 %2, %5
  %cmp.i.i.i.i46 = icmp sgt i64 %sub.i.i.i.i45, 2147483647
  br i1 %cmp.i.i.i.i46, label %cond.end.thread, label %if.else.i.i.i.i49

if.else.i.i.i.i49:                                ; preds = %if.then.i.i.i47
  %7 = icmp sgt i64 %sub.i.i.i.i45, -2147483648
  %spec.select7.i.i.i.i48 = select i1 %7, i64 %sub.i.i.i.i45, i64 -2147483648
  %8 = trunc i64 %spec.select7.i.i.i.i48 to i32
  br label %_ZNKSt4lessINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEEclERKS5_S8_.exit52

_ZNKSt4lessINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEEclERKS5_S8_.exit52: ; preds = %_ZNSt11char_traitsIcE7compareEPKcS2_m.exit.i.i.i44, %if.else.i.i.i.i49
  %__r.0.i.i.i50 = phi i32 [ %call.i.i.i.i42, %_ZNSt11char_traitsIcE7compareEPKcS2_m.exit.i.i.i44 ], [ %8, %if.else.i.i.i.i49 ]
  %cmp.i.i51 = icmp slt i32 %__r.0.i.i.i50, 0
  br i1 %cmp.i.i51, label %cond.end, label %cond.end.thread

cond.end:                                         ; preds = %_ZNKSt4lessINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEEclERKS5_S8_.exit52
  %_M_left.i34 = getelementptr inbounds %"struct.std::_Rb_tree_node", %"struct.std::_Rb_tree_node"* %__x.077, i64 0, i32 0, i32 2
  %cond.in = bitcast %"struct.std::_Rb_tree_node_base"** %_M_left.i34 to %"struct.std::_Rb_tree_node"**
  %__x.0 = load %"struct.std::_Rb_tree_node"*, %"struct.std::_Rb_tree_node"** %cond.in, align 8, !tbaa !46
  %cmp.not = icmp eq %"struct.std::_Rb_tree_node"* %__x.0, null
  br i1 %cmp.not, label %while.end, label %while.body.backedge

while.body.backedge:                              ; preds = %cond.end, %cond.end.thread
  %__x.077.be = phi %"struct.std::_Rb_tree_node"* [ %__x.0, %cond.end ], [ %__x.089, %cond.end.thread ]
  br label %while.body, !llvm.loop !196

cond.end.thread:                                  ; preds = %_ZNKSt4lessINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEEclERKS5_S8_.exit52, %if.then.i.i.i47
  %_M_right.i = getelementptr inbounds %"struct.std::_Rb_tree_node", %"struct.std::_Rb_tree_node"* %__x.077, i64 0, i32 0, i32 3
  %cond.in88 = bitcast %"struct.std::_Rb_tree_node_base"** %_M_right.i to %"struct.std::_Rb_tree_node"**
  %__x.089 = load %"struct.std::_Rb_tree_node"*, %"struct.std::_Rb_tree_node"** %cond.in88, align 8, !tbaa !46
  %cmp.not90 = icmp eq %"struct.std::_Rb_tree_node"* %__x.089, null
  br i1 %cmp.not90, label %while.end.thread92, label %while.body.backedge

while.end.thread92:                               ; preds = %cond.end.thread
  %9 = getelementptr %"struct.std::_Rb_tree_node", %"struct.std::_Rb_tree_node"* %__x.077, i64 0, i32 0
  br label %if.end12

while.end:                                        ; preds = %cond.end
  %10 = getelementptr %"struct.std::_Rb_tree_node", %"struct.std::_Rb_tree_node"* %__x.077, i64 0, i32 0
  br label %if.then

if.then:                                          ; preds = %entry, %while.end
  %__y.0.lcssa85 = phi %"struct.std::_Rb_tree_node_base"* [ %10, %while.end ], [ %_M_header.i, %entry ]
  %_M_left.i = getelementptr inbounds i8, i8* %0, i64 24
  %11 = bitcast i8* %_M_left.i to %"struct.std::_Rb_tree_node_base"**
  %12 = load %"struct.std::_Rb_tree_node_base"*, %"struct.std::_Rb_tree_node_base"** %11, align 8, !tbaa !23
  %cmp.i = icmp eq %"struct.std::_Rb_tree_node_base"* %__y.0.lcssa85, %12
  br i1 %cmp.i, label %cleanup, label %if.else

if.else:                                          ; preds = %if.then
  %call.i = tail call %"struct.std::_Rb_tree_node_base"* @_ZSt18_Rb_tree_decrementPSt18_Rb_tree_node_base(%"struct.std::_Rb_tree_node_base"* nonnull %__y.0.lcssa85) #26
  br label %if.end12

if.end12:                                         ; preds = %while.end.thread92, %if.else
  %__y.0.lcssa84 = phi %"struct.std::_Rb_tree_node_base"* [ %__y.0.lcssa85, %if.else ], [ %9, %while.end.thread92 ]
  %__j.sroa.0.0 = phi %"struct.std::_Rb_tree_node_base"* [ %call.i, %if.else ], [ %9, %while.end.thread92 ]
  %_M_string_length.i.i.i.i = getelementptr inbounds %"struct.std::_Rb_tree_node_base", %"struct.std::_Rb_tree_node_base"* %__j.sroa.0.0, i64 1, i32 1
  %13 = bitcast %"struct.std::_Rb_tree_node_base"** %_M_string_length.i.i.i.i to i64*
  %14 = load i64, i64* %13, align 8, !tbaa !44
  %_M_string_length.i14.i.i.i = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %__k, i64 0, i32 1
  %15 = load i64, i64* %_M_string_length.i14.i.i.i, align 8, !tbaa !44
  %cmp.i15.i.i.i = icmp ugt i64 %14, %15
  %.sroa.speculated.i.i.i = select i1 %cmp.i15.i.i.i, i64 %15, i64 %14
  %cmp.i13.i.i.i = icmp eq i64 %.sroa.speculated.i.i.i, 0
  br i1 %cmp.i13.i.i.i, label %if.then.i.i.i, label %_ZNSt11char_traitsIcE7compareEPKcS2_m.exit.i.i.i

_ZNSt11char_traitsIcE7compareEPKcS2_m.exit.i.i.i: ; preds = %if.end12
  %_M_storage.i.i.i = getelementptr inbounds %"struct.std::_Rb_tree_node_base", %"struct.std::_Rb_tree_node_base"* %__j.sroa.0.0, i64 1
  %_M_p.i.i.i.i.i = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %__k, i64 0, i32 0, i32 0
  %16 = load i8*, i8** %_M_p.i.i.i.i.i, align 8, !tbaa !41
  %_M_p.i.i.i.i = bitcast %"struct.std::_Rb_tree_node_base"* %_M_storage.i.i.i to i8**
  %17 = load i8*, i8** %_M_p.i.i.i.i, align 8, !tbaa !41
  %call.i.i.i.i = tail call i32 @memcmp(i8* %17, i8* %16, i64 %.sroa.speculated.i.i.i) #20
  %tobool.not.i.i.i = icmp eq i32 %call.i.i.i.i, 0
  br i1 %tobool.not.i.i.i, label %if.then.i.i.i, label %_ZNKSt4lessINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEEclERKS5_S8_.exit

if.then.i.i.i:                                    ; preds = %_ZNSt11char_traitsIcE7compareEPKcS2_m.exit.i.i.i, %if.end12
  %sub.i.i.i.i = sub i64 %14, %15
  %cmp.i.i.i.i = icmp sgt i64 %sub.i.i.i.i, 2147483647
  br i1 %cmp.i.i.i.i, label %cleanup, label %if.else.i.i.i.i

if.else.i.i.i.i:                                  ; preds = %if.then.i.i.i
  %18 = icmp sgt i64 %sub.i.i.i.i, -2147483648
  %spec.select7.i.i.i.i = select i1 %18, i64 %sub.i.i.i.i, i64 -2147483648
  %19 = trunc i64 %spec.select7.i.i.i.i to i32
  br label %_ZNKSt4lessINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEEclERKS5_S8_.exit

_ZNKSt4lessINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEEclERKS5_S8_.exit: ; preds = %_ZNSt11char_traitsIcE7compareEPKcS2_m.exit.i.i.i, %if.else.i.i.i.i
  %__r.0.i.i.i = phi i32 [ %call.i.i.i.i, %_ZNSt11char_traitsIcE7compareEPKcS2_m.exit.i.i.i ], [ %19, %if.else.i.i.i.i ]
  %cmp.i.i = icmp slt i32 %__r.0.i.i.i, 0
  %spec.select = select i1 %cmp.i.i, %"struct.std::_Rb_tree_node_base"* null, %"struct.std::_Rb_tree_node_base"* %__j.sroa.0.0
  %spec.select74 = select i1 %cmp.i.i, %"struct.std::_Rb_tree_node_base"* %__y.0.lcssa84, %"struct.std::_Rb_tree_node_base"* null
  br label %cleanup

cleanup:                                          ; preds = %if.then, %_ZNKSt4lessINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEEclERKS5_S8_.exit, %if.then.i.i.i
  %retval.sroa.0.0 = phi %"struct.std::_Rb_tree_node_base"* [ %__j.sroa.0.0, %if.then.i.i.i ], [ %spec.select, %_ZNKSt4lessINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEEclERKS5_S8_.exit ], [ null, %if.then ]
  %retval.sroa.4.0 = phi %"struct.std::_Rb_tree_node_base"* [ null, %if.then.i.i.i ], [ %spec.select74, %_ZNKSt4lessINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEEclERKS5_S8_.exit ], [ %__y.0.lcssa85, %if.then ]
  %.fca.0.insert = insertvalue { %"struct.std::_Rb_tree_node_base"*, %"struct.std::_Rb_tree_node_base"* } undef, %"struct.std::_Rb_tree_node_base"* %retval.sroa.0.0, 0
  %.fca.1.insert = insertvalue { %"struct.std::_Rb_tree_node_base"*, %"struct.std::_Rb_tree_node_base"* } %.fca.0.insert, %"struct.std::_Rb_tree_node_base"* %retval.sroa.4.0, 1
  ret { %"struct.std::_Rb_tree_node_base"*, %"struct.std::_Rb_tree_node_base"* } %.fca.1.insert
}

; Function Attrs: nounwind readonly willreturn
declare dso_local %"struct.std::_Rb_tree_node_base"* @_ZSt18_Rb_tree_decrementPSt18_Rb_tree_node_base(%"struct.std::_Rb_tree_node_base"*) local_unnamed_addr #18

; Function Attrs: nounwind readonly willreturn
declare dso_local %"struct.std::_Rb_tree_node_base"* @_ZSt18_Rb_tree_incrementPSt18_Rb_tree_node_base(%"struct.std::_Rb_tree_node_base"*) local_unnamed_addr #18

; Function Attrs: nounwind
declare dso_local void @_ZSt29_Rb_tree_insert_and_rebalancebPSt18_Rb_tree_node_baseS0_RS_(i1 zeroext, %"struct.std::_Rb_tree_node_base"*, %"struct.std::_Rb_tree_node_base"*, %"struct.std::_Rb_tree_node_base"* nonnull align 8 dereferenceable(32)) local_unnamed_addr #1

; Function Attrs: nounwind
declare dso_local void @_ZNSt8__detail15_List_node_base7_M_hookEPS0_(%"struct.std::__detail::_List_node_base"* nonnull dereferenceable(16), %"struct.std::__detail::_List_node_base"*) local_unnamed_addr #1

; Function Attrs: nounwind
declare dso_local void @_ZN4llvm3orc5LLJITD1Ev(%"class.llvm::orc::LLJIT"* nonnull dereferenceable(568)) unnamed_addr #1

; Function Attrs: uwtable
define linkonce_odr dso_local %"struct.std::__detail::_Hash_node"* @_ZNSt10_HashtableImSt4pairIKmP14JitSortContextESaIS4_ENSt8__detail10_Select1stESt8equal_toImESt4hashImENS6_18_Mod_range_hashingENS6_20_Default_ranged_hashENS6_20_Prime_rehash_policyENS6_17_Hashtable_traitsILb0ELb0ELb1EEEE21_M_insert_unique_nodeEmmPNS6_10_Hash_nodeIS4_Lb0EEE(%"class.std::_Hashtable"* nonnull dereferenceable(56) %this, i64 %__bkt, i64 %__code, %"struct.std::__detail::_Hash_node"* %__node) local_unnamed_addr #4 comdat align 2 personality i8* bitcast (i32 (...)* @__gxx_personality_v0 to i8*) {
entry:
  %_M_rehash_policy = getelementptr inbounds %"class.std::_Hashtable", %"class.std::_Hashtable"* %this, i64 0, i32 4
  %_M_next_resize.i = getelementptr inbounds %"class.std::_Hashtable", %"class.std::_Hashtable"* %this, i64 0, i32 4, i32 1
  %0 = load i64, i64* %_M_next_resize.i, align 8, !tbaa !197
  %_M_bucket_count = getelementptr inbounds %"class.std::_Hashtable", %"class.std::_Hashtable"* %this, i64 0, i32 1
  %1 = load i64, i64* %_M_bucket_count, align 8, !tbaa !15
  %_M_element_count = getelementptr inbounds %"class.std::_Hashtable", %"class.std::_Hashtable"* %this, i64 0, i32 3
  %2 = load i64, i64* %_M_element_count, align 8, !tbaa !198
  %call3 = tail call { i8, i64 } @_ZNKSt8__detail20_Prime_rehash_policy14_M_need_rehashEmmm(%"struct.std::__detail::_Prime_rehash_policy"* nonnull dereferenceable(16) %_M_rehash_policy, i64 %1, i64 %2, i64 1)
  %3 = extractvalue { i8, i64 } %call3, 0
  %4 = and i8 %3, 1
  %tobool.not = icmp eq i8 %4, 0
  br i1 %tobool.not, label %entry.if.end_crit_edge, label %if.then

entry.if.end_crit_edge:                           ; preds = %entry
  %_M_buckets.i.phi.trans.insert = getelementptr inbounds %"class.std::_Hashtable", %"class.std::_Hashtable"* %this, i64 0, i32 0
  %.pre = load %"struct.std::__detail::_Hash_node_base"**, %"struct.std::__detail::_Hash_node_base"*** %_M_buckets.i.phi.trans.insert, align 8, !tbaa !14
  br label %if.end

if.then:                                          ; preds = %entry
  %5 = extractvalue { i8, i64 } %call3, 1
  %cmp.i.i = icmp eq i64 %5, 1
  br i1 %cmp.i.i, label %if.then.i.i, label %if.end.i.i, !prof !199

if.then.i.i:                                      ; preds = %if.then
  %_M_single_bucket.i.i = getelementptr inbounds %"class.std::_Hashtable", %"class.std::_Hashtable"* %this, i64 0, i32 5
  store %"struct.std::__detail::_Hash_node_base"* null, %"struct.std::__detail::_Hash_node_base"** %_M_single_bucket.i.i, align 8, !tbaa !200
  br label %_ZNSt10_HashtableImSt4pairIKmP14JitSortContextESaIS4_ENSt8__detail10_Select1stESt8equal_toImESt4hashImENS6_18_Mod_range_hashingENS6_20_Default_ranged_hashENS6_20_Prime_rehash_policyENS6_17_Hashtable_traitsILb0ELb0ELb1EEEE19_M_allocate_bucketsEm.exit.i

if.end.i.i:                                       ; preds = %if.then
  %cmp.i.i.i.i.i = icmp ugt i64 %5, 2305843009213693951
  br i1 %cmp.i.i.i.i.i, label %if.then.i.i.i.i.i, label %_ZNSt8__detail16_Hashtable_allocISaINS_10_Hash_nodeISt4pairIKmP14JitSortContextELb0EEEEE19_M_allocate_bucketsEm.exit.i.i

if.then.i.i.i.i.i:                                ; preds = %if.end.i.i
  invoke void @_ZSt17__throw_bad_allocv() #24
          to label %.noexc unwind label %lpad.i

.noexc:                                           ; preds = %if.then.i.i.i.i.i
  unreachable

_ZNSt8__detail16_Hashtable_allocISaINS_10_Hash_nodeISt4pairIKmP14JitSortContextELb0EEEEE19_M_allocate_bucketsEm.exit.i.i: ; preds = %if.end.i.i
  %mul.i.i.i.i.i = shl nuw i64 %5, 3
  %call2.i.i10.i.i.i33 = invoke noalias nonnull i8* @_Znwm(i64 %mul.i.i.i.i.i) #22
          to label %call2.i.i10.i.i.i.noexc unwind label %lpad.i

call2.i.i10.i.i.i.noexc:                          ; preds = %_ZNSt8__detail16_Hashtable_allocISaINS_10_Hash_nodeISt4pairIKmP14JitSortContextELb0EEEEE19_M_allocate_bucketsEm.exit.i.i
  %6 = bitcast i8* %call2.i.i10.i.i.i33 to %"struct.std::__detail::_Hash_node_base"**
  tail call void @llvm.memset.p0i8.i64(i8* nonnull align 8 %call2.i.i10.i.i.i33, i8 0, i64 %mul.i.i.i.i.i, i1 false)
  br label %_ZNSt10_HashtableImSt4pairIKmP14JitSortContextESaIS4_ENSt8__detail10_Select1stESt8equal_toImESt4hashImENS6_18_Mod_range_hashingENS6_20_Default_ranged_hashENS6_20_Prime_rehash_policyENS6_17_Hashtable_traitsILb0ELb0ELb1EEEE19_M_allocate_bucketsEm.exit.i

_ZNSt10_HashtableImSt4pairIKmP14JitSortContextESaIS4_ENSt8__detail10_Select1stESt8equal_toImESt4hashImENS6_18_Mod_range_hashingENS6_20_Default_ranged_hashENS6_20_Prime_rehash_policyENS6_17_Hashtable_traitsILb0ELb0ELb1EEEE19_M_allocate_bucketsEm.exit.i: ; preds = %call2.i.i10.i.i.i.noexc, %if.then.i.i
  %retval.0.i.i = phi %"struct.std::__detail::_Hash_node_base"** [ %_M_single_bucket.i.i, %if.then.i.i ], [ %6, %call2.i.i10.i.i.i.noexc ]
  %_M_nxt.i.i = getelementptr inbounds %"class.std::_Hashtable", %"class.std::_Hashtable"* %this, i64 0, i32 2, i32 0
  %7 = bitcast %"struct.std::__detail::_Hash_node_base"** %_M_nxt.i.i to %"struct.std::__detail::_Hash_node"**
  %8 = load %"struct.std::__detail::_Hash_node"*, %"struct.std::__detail::_Hash_node"** %7, align 8, !tbaa !2
  %_M_before_begin.i = getelementptr inbounds %"class.std::_Hashtable", %"class.std::_Hashtable"* %this, i64 0, i32 2
  %_M_nxt.i = getelementptr inbounds %"struct.std::__detail::_Hash_node_base", %"struct.std::__detail::_Hash_node_base"* %_M_before_begin.i, i64 0, i32 0
  store %"struct.std::__detail::_Hash_node_base"* null, %"struct.std::__detail::_Hash_node_base"** %_M_nxt.i, align 8, !tbaa !2
  %tobool.not47.i = icmp eq %"struct.std::__detail::_Hash_node"* %8, null
  br i1 %tobool.not47.i, label %while.end.i, label %while.body.i

while.body.i:                                     ; preds = %_ZNSt10_HashtableImSt4pairIKmP14JitSortContextESaIS4_ENSt8__detail10_Select1stESt8equal_toImESt4hashImENS6_18_Mod_range_hashingENS6_20_Default_ranged_hashENS6_20_Prime_rehash_policyENS6_17_Hashtable_traitsILb0ELb0ELb1EEEE19_M_allocate_bucketsEm.exit.i, %if.end22.i
  %__p.049.i = phi %"struct.std::__detail::_Hash_node"* [ %10, %if.end22.i ], [ %8, %_ZNSt10_HashtableImSt4pairIKmP14JitSortContextESaIS4_ENSt8__detail10_Select1stESt8equal_toImESt4hashImENS6_18_Mod_range_hashingENS6_20_Default_ranged_hashENS6_20_Prime_rehash_policyENS6_17_Hashtable_traitsILb0ELb0ELb1EEEE19_M_allocate_bucketsEm.exit.i ]
  %__bbegin_bkt.048.i = phi i64 [ %__bbegin_bkt.1.i, %if.end22.i ], [ 0, %_ZNSt10_HashtableImSt4pairIKmP14JitSortContextESaIS4_ENSt8__detail10_Select1stESt8equal_toImESt4hashImENS6_18_Mod_range_hashingENS6_20_Default_ranged_hashENS6_20_Prime_rehash_policyENS6_17_Hashtable_traitsILb0ELb0ELb1EEEE19_M_allocate_bucketsEm.exit.i ]
  %9 = bitcast %"struct.std::__detail::_Hash_node"* %__p.049.i to %"struct.std::__detail::_Hash_node"**
  %10 = load %"struct.std::__detail::_Hash_node"*, %"struct.std::__detail::_Hash_node"** %9, align 8, !tbaa !11
  %_M_storage.i.i.i.i = getelementptr inbounds %"struct.std::__detail::_Hash_node", %"struct.std::__detail::_Hash_node"* %__p.049.i, i64 0, i32 0, i32 1
  %first.i.i.i.i.i = bitcast %"struct.__gnu_cxx::__aligned_buffer"* %_M_storage.i.i.i.i to i64*
  %11 = load i64, i64* %first.i.i.i.i.i, align 8, !tbaa !29
  %rem.i.i.i31 = urem i64 %11, %5
  %arrayidx.i = getelementptr inbounds %"struct.std::__detail::_Hash_node_base"*, %"struct.std::__detail::_Hash_node_base"** %retval.0.i.i, i64 %rem.i.i.i31
  %12 = load %"struct.std::__detail::_Hash_node_base"*, %"struct.std::__detail::_Hash_node_base"** %arrayidx.i, align 8, !tbaa !46
  %tobool5.not.i = icmp eq %"struct.std::__detail::_Hash_node_base"* %12, null
  br i1 %tobool5.not.i, label %if.then.i, label %if.else.i

if.then.i:                                        ; preds = %while.body.i
  %13 = load %"struct.std::__detail::_Hash_node_base"*, %"struct.std::__detail::_Hash_node_base"** %_M_nxt.i, align 8, !tbaa !2
  %14 = getelementptr %"struct.std::__detail::_Hash_node", %"struct.std::__detail::_Hash_node"* %__p.049.i, i64 0, i32 0, i32 0
  %_M_nxt8.i = getelementptr inbounds %"struct.std::__detail::_Hash_node", %"struct.std::__detail::_Hash_node"* %__p.049.i, i64 0, i32 0, i32 0, i32 0
  store %"struct.std::__detail::_Hash_node_base"* %13, %"struct.std::__detail::_Hash_node_base"** %_M_nxt8.i, align 8, !tbaa !11
  store %"struct.std::__detail::_Hash_node_base"* %14, %"struct.std::__detail::_Hash_node_base"** %_M_nxt.i, align 8, !tbaa !2
  store %"struct.std::__detail::_Hash_node_base"* %_M_before_begin.i, %"struct.std::__detail::_Hash_node_base"** %arrayidx.i, align 8, !tbaa !46
  %15 = load %"struct.std::__detail::_Hash_node_base"*, %"struct.std::__detail::_Hash_node_base"** %_M_nxt8.i, align 8, !tbaa !11
  %tobool14.not.i = icmp eq %"struct.std::__detail::_Hash_node_base"* %15, null
  br i1 %tobool14.not.i, label %if.end22.i, label %if.then15.i

if.then15.i:                                      ; preds = %if.then.i
  %arrayidx16.i = getelementptr inbounds %"struct.std::__detail::_Hash_node_base"*, %"struct.std::__detail::_Hash_node_base"** %retval.0.i.i, i64 %__bbegin_bkt.048.i
  store %"struct.std::__detail::_Hash_node_base"* %14, %"struct.std::__detail::_Hash_node_base"** %arrayidx16.i, align 8, !tbaa !46
  br label %if.end22.i

if.else.i:                                        ; preds = %while.body.i
  %_M_nxt18.i = getelementptr inbounds %"struct.std::__detail::_Hash_node_base", %"struct.std::__detail::_Hash_node_base"* %12, i64 0, i32 0
  %16 = load %"struct.std::__detail::_Hash_node_base"*, %"struct.std::__detail::_Hash_node_base"** %_M_nxt18.i, align 8, !tbaa !11
  %17 = getelementptr %"struct.std::__detail::_Hash_node", %"struct.std::__detail::_Hash_node"* %__p.049.i, i64 0, i32 0, i32 0
  %_M_nxt19.i = getelementptr inbounds %"struct.std::__detail::_Hash_node", %"struct.std::__detail::_Hash_node"* %__p.049.i, i64 0, i32 0, i32 0, i32 0
  store %"struct.std::__detail::_Hash_node_base"* %16, %"struct.std::__detail::_Hash_node_base"** %_M_nxt19.i, align 8, !tbaa !11
  %18 = load %"struct.std::__detail::_Hash_node_base"*, %"struct.std::__detail::_Hash_node_base"** %arrayidx.i, align 8, !tbaa !46
  %_M_nxt21.i = getelementptr inbounds %"struct.std::__detail::_Hash_node_base", %"struct.std::__detail::_Hash_node_base"* %18, i64 0, i32 0
  store %"struct.std::__detail::_Hash_node_base"* %17, %"struct.std::__detail::_Hash_node_base"** %_M_nxt21.i, align 8, !tbaa !11
  br label %if.end22.i

if.end22.i:                                       ; preds = %if.else.i, %if.then15.i, %if.then.i
  %__bbegin_bkt.1.i = phi i64 [ %__bbegin_bkt.048.i, %if.else.i ], [ %rem.i.i.i31, %if.then15.i ], [ %rem.i.i.i31, %if.then.i ]
  %tobool.not.i = icmp eq %"struct.std::__detail::_Hash_node"* %10, null
  br i1 %tobool.not.i, label %while.end.i, label %while.body.i, !llvm.loop !201

while.end.i:                                      ; preds = %if.end22.i, %_ZNSt10_HashtableImSt4pairIKmP14JitSortContextESaIS4_ENSt8__detail10_Select1stESt8equal_toImESt4hashImENS6_18_Mod_range_hashingENS6_20_Default_ranged_hashENS6_20_Prime_rehash_policyENS6_17_Hashtable_traitsILb0ELb0ELb1EEEE19_M_allocate_bucketsEm.exit.i
  %_M_buckets.i.i = getelementptr inbounds %"class.std::_Hashtable", %"class.std::_Hashtable"* %this, i64 0, i32 0
  %19 = load %"struct.std::__detail::_Hash_node_base"**, %"struct.std::__detail::_Hash_node_base"*** %_M_buckets.i.i, align 8, !tbaa !14
  %_M_single_bucket.i.i.i.i = getelementptr inbounds %"class.std::_Hashtable", %"class.std::_Hashtable"* %this, i64 0, i32 5
  %cmp.i.i.i.i = icmp eq %"struct.std::__detail::_Hash_node_base"** %_M_single_bucket.i.i.i.i, %19
  br i1 %cmp.i.i.i.i, label %invoke.cont4, label %if.end.i.i.i

if.end.i.i.i:                                     ; preds = %while.end.i
  %20 = bitcast %"struct.std::__detail::_Hash_node_base"** %19 to i8*
  tail call void @_ZdlPv(i8* %20) #20
  br label %invoke.cont4

lpad.i:                                           ; preds = %_ZNSt8__detail16_Hashtable_allocISaINS_10_Hash_nodeISt4pairIKmP14JitSortContextELb0EEEEE19_M_allocate_bucketsEm.exit.i.i, %if.then.i.i.i.i.i
  %21 = landingpad { i8*, i32 }
          catch i8* null
  %22 = extractvalue { i8*, i32 } %21, 0
  %23 = tail call i8* @__cxa_begin_catch(i8* %22) #20
  store i64 %0, i64* %_M_next_resize.i, align 8, !tbaa !197
  invoke void @__cxa_rethrow() #24
          to label %unreachable.i unwind label %lpad2.i

lpad2.i:                                          ; preds = %lpad.i
  %24 = landingpad { i8*, i32 }
          catch i8* null
  invoke void @__cxa_end_catch()
          to label %invoke.cont14 unwind label %terminate.lpad.i

terminate.lpad.i:                                 ; preds = %lpad2.i
  %25 = landingpad { i8*, i32 }
          catch i8* null
  %26 = extractvalue { i8*, i32 } %25, 0
  tail call void @__clang_call_terminate(i8* %26) #21
  unreachable

unreachable.i:                                    ; preds = %lpad.i
  unreachable

invoke.cont4:                                     ; preds = %if.end.i.i.i, %while.end.i
  store i64 %5, i64* %_M_bucket_count, align 8, !tbaa !15
  store %"struct.std::__detail::_Hash_node_base"** %retval.0.i.i, %"struct.std::__detail::_Hash_node_base"*** %_M_buckets.i.i, align 8, !tbaa !14
  %rem.i.i.i = urem i64 %__code, %5
  br label %if.end

invoke.cont14:                                    ; preds = %lpad2.i
  %27 = extractvalue { i8*, i32 } %24, 0
  %28 = tail call i8* @__cxa_begin_catch(i8* %27) #20
  %29 = bitcast %"struct.std::__detail::_Hash_node"* %__node to i8*
  tail call void @_ZdlPv(i8* %29) #20
  invoke void @__cxa_rethrow() #24
          to label %unreachable unwind label %lpad13

if.end:                                           ; preds = %entry.if.end_crit_edge, %invoke.cont4
  %30 = phi %"struct.std::__detail::_Hash_node_base"** [ %.pre, %entry.if.end_crit_edge ], [ %retval.0.i.i, %invoke.cont4 ]
  %__bkt.addr.0 = phi i64 [ %__bkt, %entry.if.end_crit_edge ], [ %rem.i.i.i, %invoke.cont4 ]
  %_M_buckets.i = getelementptr inbounds %"class.std::_Hashtable", %"class.std::_Hashtable"* %this, i64 0, i32 0
  %arrayidx.i34 = getelementptr inbounds %"struct.std::__detail::_Hash_node_base"*, %"struct.std::__detail::_Hash_node_base"** %30, i64 %__bkt.addr.0
  %31 = load %"struct.std::__detail::_Hash_node_base"*, %"struct.std::__detail::_Hash_node_base"** %arrayidx.i34, align 8, !tbaa !46
  %tobool.not.i35 = icmp eq %"struct.std::__detail::_Hash_node_base"* %31, null
  br i1 %tobool.not.i35, label %if.else.i40, label %if.then.i37

if.then.i37:                                      ; preds = %if.end
  %_M_nxt.i36 = getelementptr inbounds %"struct.std::__detail::_Hash_node_base", %"struct.std::__detail::_Hash_node_base"* %31, i64 0, i32 0
  %32 = load %"struct.std::__detail::_Hash_node_base"*, %"struct.std::__detail::_Hash_node_base"** %_M_nxt.i36, align 8, !tbaa !11
  %33 = getelementptr %"struct.std::__detail::_Hash_node", %"struct.std::__detail::_Hash_node"* %__node, i64 0, i32 0, i32 0
  %_M_nxt4.i = getelementptr inbounds %"struct.std::__detail::_Hash_node", %"struct.std::__detail::_Hash_node"* %__node, i64 0, i32 0, i32 0, i32 0
  store %"struct.std::__detail::_Hash_node_base"* %32, %"struct.std::__detail::_Hash_node_base"** %_M_nxt4.i, align 8, !tbaa !11
  %34 = load %"struct.std::__detail::_Hash_node_base"*, %"struct.std::__detail::_Hash_node_base"** %arrayidx.i34, align 8, !tbaa !46
  %_M_nxt7.i = getelementptr inbounds %"struct.std::__detail::_Hash_node_base", %"struct.std::__detail::_Hash_node_base"* %34, i64 0, i32 0
  store %"struct.std::__detail::_Hash_node_base"* %33, %"struct.std::__detail::_Hash_node_base"** %_M_nxt7.i, align 8, !tbaa !11
  br label %_ZNSt10_HashtableImSt4pairIKmP14JitSortContextESaIS4_ENSt8__detail10_Select1stESt8equal_toImESt4hashImENS6_18_Mod_range_hashingENS6_20_Default_ranged_hashENS6_20_Prime_rehash_policyENS6_17_Hashtable_traitsILb0ELb0ELb1EEEE22_M_insert_bucket_beginEmPNS6_10_Hash_nodeIS4_Lb0EEE.exit

if.else.i40:                                      ; preds = %if.end
  %_M_before_begin.i38 = getelementptr inbounds %"class.std::_Hashtable", %"class.std::_Hashtable"* %this, i64 0, i32 2
  %_M_nxt8.i39 = getelementptr inbounds %"struct.std::__detail::_Hash_node_base", %"struct.std::__detail::_Hash_node_base"* %_M_before_begin.i38, i64 0, i32 0
  %35 = load %"struct.std::__detail::_Hash_node_base"*, %"struct.std::__detail::_Hash_node_base"** %_M_nxt8.i39, align 8, !tbaa !2
  %36 = getelementptr %"struct.std::__detail::_Hash_node", %"struct.std::__detail::_Hash_node"* %__node, i64 0, i32 0, i32 0
  %_M_nxt9.i = getelementptr inbounds %"struct.std::__detail::_Hash_node", %"struct.std::__detail::_Hash_node"* %__node, i64 0, i32 0, i32 0, i32 0
  store %"struct.std::__detail::_Hash_node_base"* %35, %"struct.std::__detail::_Hash_node_base"** %_M_nxt9.i, align 8, !tbaa !11
  store %"struct.std::__detail::_Hash_node_base"* %36, %"struct.std::__detail::_Hash_node_base"** %_M_nxt8.i39, align 8, !tbaa !2
  %37 = load %"struct.std::__detail::_Hash_node_base"*, %"struct.std::__detail::_Hash_node_base"** %_M_nxt9.i, align 8, !tbaa !11
  %tobool13.not.i = icmp eq %"struct.std::__detail::_Hash_node_base"* %37, null
  br i1 %tobool13.not.i, label %if.end.i, label %if.then14.i

if.then14.i:                                      ; preds = %if.else.i40
  %38 = load i64, i64* %_M_bucket_count, align 8, !tbaa !15
  %_M_storage.i.i.i.i.i = getelementptr inbounds %"struct.std::__detail::_Hash_node_base", %"struct.std::__detail::_Hash_node_base"* %37, i64 1
  %first.i.i.i.i.i.i = bitcast %"struct.std::__detail::_Hash_node_base"* %_M_storage.i.i.i.i.i to i64*
  %39 = load i64, i64* %first.i.i.i.i.i.i, align 8, !tbaa !29
  %rem.i.i.i.i = urem i64 %39, %38
  %arrayidx17.i = getelementptr inbounds %"struct.std::__detail::_Hash_node_base"*, %"struct.std::__detail::_Hash_node_base"** %30, i64 %rem.i.i.i.i
  store %"struct.std::__detail::_Hash_node_base"* %36, %"struct.std::__detail::_Hash_node_base"** %arrayidx17.i, align 8, !tbaa !46
  %.pre.i = load %"struct.std::__detail::_Hash_node_base"**, %"struct.std::__detail::_Hash_node_base"*** %_M_buckets.i, align 8, !tbaa !14
  br label %if.end.i

if.end.i:                                         ; preds = %if.then14.i, %if.else.i40
  %40 = phi %"struct.std::__detail::_Hash_node_base"** [ %.pre.i, %if.then14.i ], [ %30, %if.else.i40 ]
  %arrayidx20.i = getelementptr inbounds %"struct.std::__detail::_Hash_node_base"*, %"struct.std::__detail::_Hash_node_base"** %40, i64 %__bkt.addr.0
  store %"struct.std::__detail::_Hash_node_base"* %_M_before_begin.i38, %"struct.std::__detail::_Hash_node_base"** %arrayidx20.i, align 8, !tbaa !46
  br label %_ZNSt10_HashtableImSt4pairIKmP14JitSortContextESaIS4_ENSt8__detail10_Select1stESt8equal_toImESt4hashImENS6_18_Mod_range_hashingENS6_20_Default_ranged_hashENS6_20_Prime_rehash_policyENS6_17_Hashtable_traitsILb0ELb0ELb1EEEE22_M_insert_bucket_beginEmPNS6_10_Hash_nodeIS4_Lb0EEE.exit

_ZNSt10_HashtableImSt4pairIKmP14JitSortContextESaIS4_ENSt8__detail10_Select1stESt8equal_toImESt4hashImENS6_18_Mod_range_hashingENS6_20_Default_ranged_hashENS6_20_Prime_rehash_policyENS6_17_Hashtable_traitsILb0ELb0ELb1EEEE22_M_insert_bucket_beginEmPNS6_10_Hash_nodeIS4_Lb0EEE.exit: ; preds = %if.then.i37, %if.end.i
  %41 = load i64, i64* %_M_element_count, align 8, !tbaa !198
  %inc = add i64 %41, 1
  store i64 %inc, i64* %_M_element_count, align 8, !tbaa !198
  ret %"struct.std::__detail::_Hash_node"* %__node

lpad13:                                           ; preds = %invoke.cont14
  %42 = landingpad { i8*, i32 }
          cleanup
  invoke void @__cxa_end_catch()
          to label %invoke.cont15 unwind label %terminate.lpad

invoke.cont15:                                    ; preds = %lpad13
  resume { i8*, i32 } %42

terminate.lpad:                                   ; preds = %lpad13
  %43 = landingpad { i8*, i32 }
          catch i8* null
  %44 = extractvalue { i8*, i32 } %43, 0
  tail call void @__clang_call_terminate(i8* %44) #21
  unreachable

unreachable:                                      ; preds = %invoke.cont14
  unreachable
}

declare dso_local { i8, i64 } @_ZNKSt8__detail20_Prime_rehash_policy14_M_need_rehashEmmm(%"struct.std::__detail::_Prime_rehash_policy"* nonnull dereferenceable(16), i64, i64, i64) local_unnamed_addr #0

; Function Attrs: uwtable
define internal void @_GLOBAL__sub_I_sort_api.cpp() #4 section ".text.startup" {
entry:
  tail call void @_ZNSt8ios_base4InitC1Ev(%"class.std::ios_base::Init"* nonnull dereferenceable(1) @_ZStL8__ioinit)
  %0 = tail call i32 @__cxa_atexit(void (i8*)* bitcast (void (%"class.std::ios_base::Init"*)* @_ZNSt8ios_base4InitD1Ev to void (i8*)*), i8* getelementptr inbounds (%"class.std::ios_base::Init", %"class.std::ios_base::Init"* @_ZStL8__ioinit, i64 0, i32 0), i8* nonnull @__dso_handle) #20
  tail call void @llvm.memset.p0i8.i64(i8* nonnull align 8 dereferenceable(40) bitcast (%class.OpTemplateCache* @g_jitSortContexts to i8*), i8 0, i64 40, i1 false) #20
  store %"struct.std::__detail::_Hash_node_base"** getelementptr inbounds (%class.OpTemplateCache, %class.OpTemplateCache* @g_jitSortContexts, i64 0, i32 1, i32 0, i32 5), %"struct.std::__detail::_Hash_node_base"*** getelementptr inbounds (%class.OpTemplateCache, %class.OpTemplateCache* @g_jitSortContexts, i64 0, i32 1, i32 0, i32 0), align 8, !tbaa !14
  store i64 1, i64* getelementptr inbounds (%class.OpTemplateCache, %class.OpTemplateCache* @g_jitSortContexts, i64 0, i32 1, i32 0, i32 1), align 8, !tbaa !15
  tail call void @llvm.memset.p0i8.i64(i8* nonnull align 8 dereferenceable(16) bitcast (%"struct.std::__detail::_Hash_node_base"** getelementptr inbounds (%class.OpTemplateCache, %class.OpTemplateCache* @g_jitSortContexts, i64 0, i32 1, i32 0, i32 2, i32 0) to i8*), i8 0, i64 16, i1 false) #20
  store float 1.000000e+00, float* getelementptr inbounds (%class.OpTemplateCache, %class.OpTemplateCache* @g_jitSortContexts, i64 0, i32 1, i32 0, i32 4, i32 0), align 8, !tbaa !202
  tail call void @llvm.memset.p0i8.i64(i8* nonnull align 8 dereferenceable(16) bitcast (i64* getelementptr inbounds (%class.OpTemplateCache, %class.OpTemplateCache* @g_jitSortContexts, i64 0, i32 1, i32 0, i32 4, i32 1) to i8*), i8 0, i64 16, i1 false) #20
  %1 = tail call i32 @__cxa_atexit(void (i8*)* bitcast (void (%class.OpTemplateCache*)* @_ZN15OpTemplateCacheIP14JitSortContextED2Ev to void (i8*)*), i8* bitcast (%class.OpTemplateCache* @g_jitSortContexts to i8*), i8* nonnull @__dso_handle) #20
  ret void
}

; Function Attrs: inaccessiblememonly nofree nosync nounwind willreturn
declare void @llvm.experimental.noalias.scope.decl(metadata) #19

attributes #0 = { "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "no-infs-fp-math"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #1 = { nounwind "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "no-infs-fp-math"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #2 = { nofree nounwind }
attributes #3 = { inlinehint nounwind uwtable "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "min-legal-vector-width"="0" "no-infs-fp-math"="false" "no-jump-tables"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #4 = { uwtable "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "min-legal-vector-width"="0" "no-infs-fp-math"="false" "no-jump-tables"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #5 = { argmemonly nofree nosync nounwind willreturn }
attributes #6 = { nofree nosync nounwind willreturn }
attributes #7 = { nobuiltin nofree allocsize(0) "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "no-infs-fp-math"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #8 = { uwtable mustprogress "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "min-legal-vector-width"="0" "no-infs-fp-math"="false" "no-jump-tables"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #9 = { nobuiltin nounwind "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "no-infs-fp-math"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #10 = { argmemonly nofree nosync nounwind willreturn writeonly }
attributes #11 = { noinline noreturn nounwind }
attributes #12 = { noreturn "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "no-infs-fp-math"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #13 = { noreturn nounwind "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "no-infs-fp-math"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #14 = { inaccessiblemem_or_argmemonly nounwind willreturn "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "no-infs-fp-math"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #15 = { nounwind uwtable "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "min-legal-vector-width"="0" "no-infs-fp-math"="false" "no-jump-tables"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #16 = { nounwind uwtable willreturn "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "min-legal-vector-width"="0" "no-infs-fp-math"="false" "no-jump-tables"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #17 = { argmemonly nofree nounwind readonly willreturn "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "no-infs-fp-math"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #18 = { nounwind readonly willreturn "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "no-infs-fp-math"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #19 = { inaccessiblememonly nofree nosync nounwind willreturn }
attributes #20 = { nounwind }
attributes #21 = { noreturn nounwind }
attributes #22 = { allocsize(0) }
attributes #23 = { builtin allocsize(0) }
attributes #24 = { noreturn }
attributes #25 = { builtin nounwind }
attributes #26 = { nounwind readonly willreturn }

!llvm.module.flags = !{!0}
!llvm.ident = !{!1}

!0 = !{i32 1, !"wchar_size", i32 4}
!1 = !{!"Ubuntu clang version 12.0.0-++20210225092633+e0e6b1e39e7e-1~exp1~20210225083352.50"}
!2 = !{!3, !4, i64 16}
!3 = !{!"_ZTSSt10_HashtableImSt4pairIKmP14JitSortContextESaIS4_ENSt8__detail10_Select1stESt8equal_toImESt4hashImENS6_18_Mod_range_hashingENS6_20_Default_ranged_hashENS6_20_Prime_rehash_policyENS6_17_Hashtable_traitsILb0ELb0ELb1EEEE", !4, i64 0, !7, i64 8, !8, i64 16, !7, i64 24, !9, i64 32, !4, i64 48}
!4 = !{!"any pointer", !5, i64 0}
!5 = !{!"omnipotent char", !6, i64 0}
!6 = !{!"Simple C++ TBAA"}
!7 = !{!"long", !5, i64 0}
!8 = !{!"_ZTSNSt8__detail15_Hash_node_baseE", !4, i64 0}
!9 = !{!"_ZTSNSt8__detail20_Prime_rehash_policyE", !10, i64 0, !7, i64 8}
!10 = !{!"float", !5, i64 0}
!11 = !{!8, !4, i64 0}
!12 = distinct !{!12, !13}
!13 = !{!"llvm.loop.mustprogress"}
!14 = !{!3, !4, i64 0}
!15 = !{!3, !7, i64 8}
!16 = !{!17, !17, i64 0}
!17 = !{!"int", !5, i64 0}
!18 = !{!19, !21, i64 0}
!19 = !{!"_ZTSSt15_Rb_tree_header", !20, i64 0, !7, i64 32}
!20 = !{!"_ZTSSt18_Rb_tree_node_base", !21, i64 0, !4, i64 8, !4, i64 16, !4, i64 24}
!21 = !{!"_ZTSSt14_Rb_tree_color", !5, i64 0}
!22 = !{!19, !4, i64 8}
!23 = !{!19, !4, i64 16}
!24 = !{!19, !4, i64 24}
!25 = !{!19, !7, i64 32}
!26 = !{!27, !4, i64 0}
!27 = !{!"_ZTSNSt8__detail15_List_node_baseE", !4, i64 0, !4, i64 8}
!28 = !{!27, !4, i64 8}
!29 = !{!7, !7, i64 0}
!30 = distinct !{!30, !31}
!31 = !{!"llvm.loop.unroll.disable"}
!32 = !{!33, !4, i64 0}
!33 = !{!"_ZTSN7codegen10ParamValueE", !4, i64 0, !17, i64 8, !34, i64 12, !35, i64 16}
!34 = !{!"_ZTSN7codegen9ParamTypeE", !5, i64 0}
!35 = !{!"bool", !5, i64 0}
!36 = !{!33, !17, i64 8}
!37 = !{!33, !34, i64 12}
!38 = !{!33, !35, i64 16}
!39 = !{!40, !4, i64 0}
!40 = !{!"_ZTSNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE12_Alloc_hiderE", !4, i64 0}
!41 = !{!42, !4, i64 0}
!42 = !{!"_ZTSNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEE", !40, i64 0, !7, i64 8, !5, i64 16}
!43 = !{!5, !5, i64 0}
!44 = !{!42, !7, i64 8}
!45 = distinct !{!45, !13}
!46 = !{!4, !4, i64 0}
!47 = !{!20, !4, i64 16}
!48 = distinct !{!48, !13}
!49 = !{!20, !4, i64 24}
!50 = distinct !{!50, !13}
!51 = !{!52, !4, i64 0}
!52 = !{!"_ZTSN7codegen12HammerConfigE", !4, i64 0, !4, i64 8, !5, i64 16, !5, i64 816}
!53 = !{!52, !4, i64 8}
!54 = distinct !{!54, !13}
!55 = distinct !{!55, !13}
!56 = !{!57, !4, i64 16}
!57 = !{!"_ZTSN4llvm3orc5LLJITE", !58, i64 0, !61, i64 8, !4, i64 16, !64, i64 24, !72, i64 464, !79, i64 520, !82, i64 528, !85, i64 536, !88, i64 544, !91, i64 552, !91, i64 560}
!58 = !{!"_ZTSSt10unique_ptrIN4llvm3orc16ExecutionSessionESt14default_deleteIS2_EE", !59, i64 0}
!59 = !{!"_ZTSSt15__uniq_ptr_implIN4llvm3orc16ExecutionSessionESt14default_deleteIS2_EE", !60, i64 0}
!60 = !{!"_ZTSSt5tupleIJPN4llvm3orc16ExecutionSessionESt14default_deleteIS2_EEE"}
!61 = !{!"_ZTSSt10unique_ptrIN4llvm3orc5LLJIT15PlatformSupportESt14default_deleteIS3_EE", !62, i64 0}
!62 = !{!"_ZTSSt15__uniq_ptr_implIN4llvm3orc5LLJIT15PlatformSupportESt14default_deleteIS3_EE", !63, i64 0}
!63 = !{!"_ZTSSt5tupleIJPN4llvm3orc5LLJIT15PlatformSupportESt14default_deleteIS3_EEE"}
!64 = !{!"_ZTSN4llvm10DataLayoutE", !35, i64 0, !17, i64 4, !65, i64 8, !17, i64 12, !17, i64 16, !65, i64 20, !66, i64 24, !67, i64 28, !68, i64 32, !69, i64 64, !42, i64 208, !70, i64 240, !4, i64 384, !71, i64 392}
!65 = !{!"_ZTSN4llvm10MaybeAlignE"}
!66 = !{!"_ZTSN4llvm10DataLayout20FunctionPtrAlignTypeE", !5, i64 0}
!67 = !{!"_ZTSN4llvm10DataLayout13ManglingModeTE", !5, i64 0}
!68 = !{!"_ZTSN4llvm11SmallVectorIhLj8EEE"}
!69 = !{!"_ZTSN4llvm11SmallVectorINS_15LayoutAlignElemELj16EEE"}
!70 = !{!"_ZTSN4llvm11SmallVectorINS_16PointerAlignElemELj8EEE"}
!71 = !{!"_ZTSN4llvm11SmallVectorIjLj8EEE"}
!72 = !{!"_ZTSN4llvm6TripleE", !42, i64 0, !73, i64 32, !74, i64 36, !75, i64 40, !76, i64 44, !77, i64 48, !78, i64 52}
!73 = !{!"_ZTSN4llvm6Triple8ArchTypeE", !5, i64 0}
!74 = !{!"_ZTSN4llvm6Triple11SubArchTypeE", !5, i64 0}
!75 = !{!"_ZTSN4llvm6Triple10VendorTypeE", !5, i64 0}
!76 = !{!"_ZTSN4llvm6Triple6OSTypeE", !5, i64 0}
!77 = !{!"_ZTSN4llvm6Triple15EnvironmentTypeE", !5, i64 0}
!78 = !{!"_ZTSN4llvm6Triple16ObjectFormatTypeE", !5, i64 0}
!79 = !{!"_ZTSSt10unique_ptrIN4llvm10ThreadPoolESt14default_deleteIS1_EE", !80, i64 0}
!80 = !{!"_ZTSSt15__uniq_ptr_implIN4llvm10ThreadPoolESt14default_deleteIS1_EE", !81, i64 0}
!81 = !{!"_ZTSSt5tupleIJPN4llvm10ThreadPoolESt14default_deleteIS1_EEE"}
!82 = !{!"_ZTSSt10unique_ptrIN4llvm3orc11ObjectLayerESt14default_deleteIS2_EE", !83, i64 0}
!83 = !{!"_ZTSSt15__uniq_ptr_implIN4llvm3orc11ObjectLayerESt14default_deleteIS2_EE", !84, i64 0}
!84 = !{!"_ZTSSt5tupleIJPN4llvm3orc11ObjectLayerESt14default_deleteIS2_EEE"}
!85 = !{!"_ZTSSt10unique_ptrIN4llvm3orc20ObjectTransformLayerESt14default_deleteIS2_EE", !86, i64 0}
!86 = !{!"_ZTSSt15__uniq_ptr_implIN4llvm3orc20ObjectTransformLayerESt14default_deleteIS2_EE", !87, i64 0}
!87 = !{!"_ZTSSt5tupleIJPN4llvm3orc20ObjectTransformLayerESt14default_deleteIS2_EEE"}
!88 = !{!"_ZTSSt10unique_ptrIN4llvm3orc14IRCompileLayerESt14default_deleteIS2_EE", !89, i64 0}
!89 = !{!"_ZTSSt15__uniq_ptr_implIN4llvm3orc14IRCompileLayerESt14default_deleteIS2_EE", !90, i64 0}
!90 = !{!"_ZTSSt5tupleIJPN4llvm3orc14IRCompileLayerESt14default_deleteIS2_EEE"}
!91 = !{!"_ZTSSt10unique_ptrIN4llvm3orc16IRTransformLayerESt14default_deleteIS2_EE", !92, i64 0}
!92 = !{!"_ZTSSt15__uniq_ptr_implIN4llvm3orc16IRTransformLayerESt14default_deleteIS2_EE", !93, i64 0}
!93 = !{!"_ZTSSt5tupleIJPN4llvm3orc16IRTransformLayerESt14default_deleteIS2_EEE"}
!94 = !{!95}
!95 = distinct !{!95, !96, !"_ZN4llvm3orc5LLJIT6lookupENS_9StringRefE: %agg.result"}
!96 = distinct !{!96, !"_ZN4llvm3orc5LLJIT6lookupENS_9StringRefE"}
!97 = !{!98, !7, i64 0}
!98 = !{!"_ZTSN4llvm18JITEvaluatedSymbolE", !7, i64 0, !99, i64 8}
!99 = !{!"_ZTSN4llvm14JITSymbolFlagsE", !5, i64 0, !100, i64 1}
!100 = !{!"_ZTSN4llvm14JITSymbolFlags9FlagNamesE", !5, i64 0}
!101 = !{!102}
!102 = distinct !{!102, !103, !"_ZN4llvm3orc5LLJIT6lookupENS_9StringRefE: %agg.result"}
!103 = distinct !{!103, !"_ZN4llvm3orc5LLJIT6lookupENS_9StringRefE"}
!104 = !{!105}
!105 = distinct !{!105, !106, !"_ZN4llvm3orc5LLJIT6lookupENS_9StringRefE: %agg.result"}
!106 = distinct !{!106, !"_ZN4llvm3orc5LLJIT6lookupENS_9StringRefE"}
!107 = !{!108, !4, i64 8}
!108 = !{!"_ZTS14JitSortContext", !4, i64 0, !4, i64 8, !4, i64 16, !4, i64 24}
!109 = !{!108, !4, i64 16}
!110 = !{!108, !4, i64 24}
!111 = !{!108, !4, i64 0}
!112 = !{!113, !7, i64 0}
!113 = !{!"_ZTSSt4pairIKmP14JitSortContextE", !7, i64 0, !4, i64 8}
!114 = !{!113, !4, i64 8}
!115 = distinct !{!115, !13}
!116 = !{!117}
!117 = distinct !{!117, !118, !"_ZSt16forward_as_tupleIJNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEEESt5tupleIJDpOT_EES9_: %agg.result"}
!118 = distinct !{!118, !"_ZSt16forward_as_tupleIJNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEEESt5tupleIJDpOT_EES9_"}
!119 = !{!120, !120, i64 0}
!120 = !{!"vtable pointer", !6, i64 0}
!121 = !{!122, !4, i64 0}
!122 = !{!"_ZTSN4llvm15SmallVectorBaseIjEE", !4, i64 0, !17, i64 8, !17, i64 12}
!123 = !{!124, !4, i64 16}
!124 = !{!"_ZTSSt14_Function_base", !5, i64 0, !4, i64 16}
!125 = !{!126, !4, i64 72}
!126 = !{!"_ZTS4Sort", !4, i64 8, !17, i64 16, !4, i64 24, !17, i64 32, !4, i64 40, !4, i64 48, !4, i64 56, !17, i64 64, !4, i64 72}
!127 = !{!126, !17, i64 16}
!128 = !{!126, !4, i64 8}
!129 = !{!130, !17, i64 48}
!130 = !{!"_ZTS5Table", !131, i64 8, !132, i64 16, !4, i64 40, !17, i64 48, !17, i64 52, !17, i64 56}
!131 = !{!"_ZTS6Layout"}
!132 = !{!"_ZTSSt6vectorIP6ColumnSaIS1_EE"}
!133 = !{!130, !17, i64 52}
!134 = !{!130, !4, i64 40}
!135 = !{!130, !17, i64 56}
!136 = !{!137, !4, i64 8}
!137 = !{!"_ZTS6Column", !4, i64 8, !4, i64 16, !138, i64 24, !7, i64 32}
!138 = !{!"_ZTSN3opt10ColumnTypeE", !5, i64 0}
!139 = !{!137, !4, i64 16}
!140 = !{!137, !138, i64 24}
!141 = !{!137, !7, i64 32}
!142 = !{!138, !138, i64 0}
!143 = !{!144, !4, i64 8}
!144 = !{!"_ZTSSt12_Vector_baseIP6ColumnSaIS1_EE", !145, i64 0}
!145 = !{!"_ZTSNSt12_Vector_baseIP6ColumnSaIS1_EE12_Vector_implE", !4, i64 0, !4, i64 8, !4, i64 16}
!146 = !{!144, !4, i64 16}
!147 = !{!144, !4, i64 0}
!148 = distinct !{!148, !13}
!149 = !{!126, !4, i64 40}
!150 = !{!126, !17, i64 64}
!151 = !{!152, !17, i64 64}
!152 = !{!"_ZTS10PagesIndex", !4, i64 0, !17, i64 8, !153, i64 16, !154, i64 40, !17, i64 64}
!153 = !{!"_ZTSSt6vectorIlSaIlEE"}
!154 = !{!"_ZTSSt6vectorIS_IP6ColumnSaIS1_EESaIS3_EE"}
!155 = distinct !{!155, !13}
!156 = distinct !{!156, !31}
!157 = !{!126, !4, i64 48}
!158 = !{!126, !4, i64 56}
!159 = !{!126, !17, i64 32}
!160 = !{!126, !4, i64 24}
!161 = !{!162, !4, i64 0}
!162 = !{!"_ZTSNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE11_Alloc_nodeE", !4, i64 0}
!163 = !{!20, !21, i64 0}
!164 = !{!20, !4, i64 8}
!165 = distinct !{!165, !13}
!166 = distinct !{!166, !13}
!167 = !{!168, !4, i64 32}
!168 = !{!"_ZTSSt4pairIKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEPN7codegen10ParamValueEE", !42, i64 0, !4, i64 32}
!169 = !{!170}
!170 = distinct !{!170, !171, !"_ZN4llvm3orc5LLJIT19lookupLinkerMangledERNS0_8JITDylibENS_9StringRefE: %agg.result"}
!171 = distinct !{!171, !"_ZN4llvm3orc5LLJIT19lookupLinkerMangledERNS0_8JITDylibENS_9StringRefE"}
!172 = !{!173}
!173 = distinct !{!173, !174, !"_ZN4llvm3orc16ExecutionSession6internENS_9StringRefE: %agg.result"}
!174 = distinct !{!174, !"_ZN4llvm3orc16ExecutionSession6internENS_9StringRefE"}
!175 = !{!176, !4, i64 0}
!176 = !{!"_ZTSSt12__shared_ptrIN4llvm3orc16SymbolStringPoolELN9__gnu_cxx12_Lock_policyE2EE", !4, i64 0, !177, i64 8}
!177 = !{!"_ZTSSt14__shared_countILN9__gnu_cxx12_Lock_policyE2EE", !4, i64 0}
!178 = !{!179}
!179 = distinct !{!179, !180, !"_ZN4llvm3orc16SymbolStringPool6internENS_9StringRefE: %agg.result"}
!180 = distinct !{!180, !"_ZN4llvm3orc16SymbolStringPool6internENS_9StringRefE"}
!181 = !{!179, !173}
!182 = !{!183, !4, i64 0}
!183 = !{!"_ZTSN4llvm3orc15SymbolStringPtrE", !4, i64 0}
!184 = !{!185, !4, i64 0}
!185 = !{!"_ZTSN4llvm13StringMapImplE", !4, i64 0, !17, i64 8, !17, i64 12, !17, i64 16, !17, i64 20}
!186 = distinct !{!186, !13}
!187 = !{!185, !17, i64 16}
!188 = !{!189, !7, i64 0}
!189 = !{!"_ZTSN4llvm18StringMapEntryBaseE", !7, i64 0}
!190 = !{!191, !7, i64 0}
!191 = !{!"_ZTSSt13__atomic_baseImE", !7, i64 0}
!192 = !{!185, !17, i64 12}
!193 = !{!185, !17, i64 8}
!194 = !{!195, !4, i64 0}
!195 = !{!"_ZTSSt10_Head_baseILm0EONSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEELb0EE", !4, i64 0}
!196 = distinct !{!196, !13}
!197 = !{!9, !7, i64 8}
!198 = !{!3, !7, i64 24}
!199 = !{!"branch_weights", i32 1, i32 2000}
!200 = !{!3, !4, i64 48}
!201 = distinct !{!201, !13}
!202 = !{!9, !10, i64 0}
