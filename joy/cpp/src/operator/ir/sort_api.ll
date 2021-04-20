; ModuleID = '/usr/code/olk_dev/omni_runtime_joy/omni-cache/joy/cpp/src/operator/ir/../../jni/sort_api.cpp'
source_filename = "/usr/code/olk_dev/omni_runtime_joy/omni-cache/joy/cpp/src/operator/ir/../../jni/sort_api.cpp"
target datalayout = "e-m:e-p270:32:32-p271:32:32-p272:64:64-i64:64-f80:128-n8:16:32:64-S128"
target triple = "x86_64-pc-linux-gnu"

%"class.std::ios_base::Init" = type { i8 }
%"struct.std::piecewise_construct_t" = type { i8 }
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
%"class.codegen::Hammer" = type { %"class.llvm::ExitOnError", %"class.std::unique_ptr", %"class.std::unique_ptr.8", %"class.std::unique_ptr.17", %"class.std::unique_ptr.26", %"class.std::unique_ptr.111", %"class.std::map" }
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
%"struct.std::_Tuple_impl" = type { %"struct.std::_Head_base.7" }
%"struct.std::_Head_base.7" = type { %"class.llvm::LLVMContext"* }
%"class.llvm::LLVMContext" = type { %"class.llvm::LLVMContextImpl"* }
%"class.llvm::LLVMContextImpl" = type opaque
%"class.std::unique_ptr.8" = type { %"class.std::__uniq_ptr_impl.9" }
%"class.std::__uniq_ptr_impl.9" = type { %"class.std::tuple.10" }
%"class.std::tuple.10" = type { %"struct.std::_Tuple_impl.11" }
%"struct.std::_Tuple_impl.11" = type { %"struct.std::_Head_base.16" }
%"struct.std::_Head_base.16" = type { %"class.llvm::StringRef"* }
%"class.llvm::StringRef" = type { i8*, i64 }
%"class.std::unique_ptr.17" = type { %"class.std::__uniq_ptr_impl.18" }
%"class.std::__uniq_ptr_impl.18" = type { %"class.std::tuple.19" }
%"class.std::tuple.19" = type { %"struct.std::_Tuple_impl.20" }
%"struct.std::_Tuple_impl.20" = type { %"struct.std::_Head_base.25" }
%"struct.std::_Head_base.25" = type { %"class.llvm::IRBuilder"* }
%"class.llvm::IRBuilder" = type { %"class.llvm::IRBuilderBase", %"class.llvm::ConstantFolder", %"class.llvm::IRBuilderDefaultInserter" }
%"class.llvm::IRBuilderBase" = type { %"class.llvm::SmallVector.378", %"class.llvm::BasicBlock"*, %"class.llvm::ilist_iterator", %"class.llvm::LLVMContext"*, %"class.llvm::IRBuilderFolder"*, %"class.llvm::IRBuilderDefaultInserter"*, %"class.llvm::MDNode"*, %"class.llvm::FastMathFlags", i8, i8, i8, %"class.llvm::ArrayRef" }
%"class.llvm::SmallVector.378" = type { %"class.llvm::SmallVectorImpl.379", %"struct.llvm::SmallVectorStorage.382" }
%"class.llvm::SmallVectorImpl.379" = type { %"class.llvm::SmallVectorTemplateBase.380" }
%"class.llvm::SmallVectorTemplateBase.380" = type { %"class.llvm::SmallVectorTemplateCommon.381" }
%"class.llvm::SmallVectorTemplateCommon.381" = type { %"class.llvm::SmallVectorBase.99" }
%"class.llvm::SmallVectorBase.99" = type { i8*, i32, i32 }
%"struct.llvm::SmallVectorStorage.382" = type { [32 x i8] }
%"class.llvm::BasicBlock" = type { %"class.llvm::Value", %"class.llvm::ilist_node_with_parent", %"class.llvm::SymbolTableList.384", %"class.llvm::Function"* }
%"class.llvm::Value" = type { %"class.llvm::Type"*, %"class.llvm::Use"*, i8, i8, i16, i32 }
%"class.llvm::Type" = type { %"class.llvm::LLVMContext"*, i32, i32, %"class.llvm::Type"** }
%"class.llvm::Use" = type { %"class.llvm::Value"*, %"class.llvm::Use"*, %"class.llvm::Use"**, %"class.llvm::User"* }
%"class.llvm::User" = type { %"class.llvm::Value" }
%"class.llvm::ilist_node_with_parent" = type { %"class.llvm::ilist_node" }
%"class.llvm::ilist_node" = type { %"class.llvm::ilist_node_impl.383" }
%"class.llvm::ilist_node_impl.383" = type { %"class.llvm::ilist_node_base" }
%"class.llvm::ilist_node_base" = type { %"class.llvm::ilist_node_base"*, %"class.llvm::ilist_node_base"* }
%"class.llvm::SymbolTableList.384" = type { %"class.llvm::iplist_impl.385" }
%"class.llvm::iplist_impl.385" = type { %"class.llvm::simple_ilist.388" }
%"class.llvm::simple_ilist.388" = type { %"class.llvm::ilist_sentinel.390" }
%"class.llvm::ilist_sentinel.390" = type { %"class.llvm::ilist_node_impl.391" }
%"class.llvm::ilist_node_impl.391" = type { %"class.llvm::ilist_node_base" }
%"class.llvm::Function" = type { %"class.llvm::GlobalObject", %"class.llvm::ilist_node.393", %"class.llvm::SymbolTableList.394", %"class.llvm::Argument"*, i64, %"class.std::unique_ptr.65", %"class.llvm::AttributeList" }
%"class.llvm::GlobalObject" = type { %"class.llvm::GlobalValue", %"class.llvm::Comdat"* }
%"class.llvm::GlobalValue" = type { %"class.llvm::Constant", %"class.llvm::Type"*, i32, i32, %"class.llvm::Module"* }
%"class.llvm::Constant" = type { %"class.llvm::User" }
%"class.llvm::Module" = type { %"class.llvm::LLVMContext"*, %"class.llvm::SymbolTableList", %"class.llvm::SymbolTableList.35", %"class.llvm::SymbolTableList.43", %"class.llvm::SymbolTableList.51", %"class.llvm::iplist", %"class.std::__cxx11::basic_string", %"class.std::unique_ptr.65", %"class.llvm::StringMap", %"class.std::unique_ptr.74", %"class.std::unique_ptr.83", %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string", %"class.llvm::StringMap.92", %"class.llvm::DataLayout" }
%"class.llvm::SymbolTableList" = type { %"class.llvm::iplist_impl" }
%"class.llvm::iplist_impl" = type { %"class.llvm::simple_ilist" }
%"class.llvm::simple_ilist" = type { %"class.llvm::ilist_sentinel" }
%"class.llvm::ilist_sentinel" = type { %"class.llvm::ilist_node_impl" }
%"class.llvm::ilist_node_impl" = type { %"class.llvm::ilist_node_base" }
%"class.llvm::SymbolTableList.35" = type { %"class.llvm::iplist_impl.36" }
%"class.llvm::iplist_impl.36" = type { %"class.llvm::simple_ilist.39" }
%"class.llvm::simple_ilist.39" = type { %"class.llvm::ilist_sentinel.41" }
%"class.llvm::ilist_sentinel.41" = type { %"class.llvm::ilist_node_impl.42" }
%"class.llvm::ilist_node_impl.42" = type { %"class.llvm::ilist_node_base" }
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
%"class.llvm::iplist" = type { %"class.llvm::iplist_impl.59" }
%"class.llvm::iplist_impl.59" = type { %"class.llvm::simple_ilist.61" }
%"class.llvm::simple_ilist.61" = type { %"class.llvm::ilist_sentinel.63" }
%"class.llvm::ilist_sentinel.63" = type { %"class.llvm::ilist_node_impl.64" }
%"class.llvm::ilist_node_impl.64" = type { %"class.llvm::ilist_node_base" }
%"class.llvm::StringMap" = type <{ %"class.llvm::StringMapImpl", %"class.llvm::MallocAllocator", [7 x i8] }>
%"class.llvm::StringMapImpl" = type { %"class.llvm::StringMapEntryBase"**, i32, i32, i32, i32 }
%"class.llvm::StringMapEntryBase" = type { i64 }
%"class.llvm::MallocAllocator" = type { i8 }
%"class.std::unique_ptr.74" = type { %"class.std::__uniq_ptr_impl.75" }
%"class.std::__uniq_ptr_impl.75" = type { %"class.std::tuple.76" }
%"class.std::tuple.76" = type { %"struct.std::_Tuple_impl.77" }
%"struct.std::_Tuple_impl.77" = type { %"struct.std::_Head_base.82" }
%"struct.std::_Head_base.82" = type { %"class.llvm::MemoryBuffer"* }
%"class.llvm::MemoryBuffer" = type { i32 (...)**, i8*, i8* }
%"class.std::unique_ptr.83" = type { %"class.std::__uniq_ptr_impl.84" }
%"class.std::__uniq_ptr_impl.84" = type { %"class.std::tuple.85" }
%"class.std::tuple.85" = type { %"struct.std::_Tuple_impl.86" }
%"struct.std::_Tuple_impl.86" = type { %"struct.std::_Head_base.91" }
%"struct.std::_Head_base.91" = type { %"class.llvm::GVMaterializer"* }
%"class.llvm::GVMaterializer" = type opaque
%"class.llvm::StringMap.92" = type <{ %"class.llvm::StringMapImpl", %"class.llvm::MallocAllocator", [7 x i8] }>
%"class.llvm::DataLayout" = type { i8, i32, %"struct.llvm::MaybeAlign", i32, i32, %"struct.llvm::MaybeAlign", i32, i32, %"class.llvm::SmallVector", %"class.llvm::SmallVector.95", %"class.std::__cxx11::basic_string", %"class.llvm::SmallVector.101", i8*, %"class.llvm::SmallVector.106" }
%"struct.llvm::MaybeAlign" = type { %"class.llvm::Optional" }
%"class.llvm::Optional" = type { %"class.llvm::optional_detail::OptionalStorage" }
%"class.llvm::optional_detail::OptionalStorage" = type { %union.anon.94, i8 }
%union.anon.94 = type { i8 }
%"class.llvm::SmallVector" = type { %"class.llvm::SmallVectorImpl", %"struct.llvm::SmallVectorStorage" }
%"class.llvm::SmallVectorImpl" = type { %"class.llvm::SmallVectorTemplateBase" }
%"class.llvm::SmallVectorTemplateBase" = type { %"class.llvm::SmallVectorTemplateCommon" }
%"class.llvm::SmallVectorTemplateCommon" = type { %"class.llvm::SmallVectorBase" }
%"class.llvm::SmallVectorBase" = type { i8*, i64, i64 }
%"struct.llvm::SmallVectorStorage" = type { [8 x i8] }
%"class.llvm::SmallVector.95" = type { %"class.llvm::SmallVectorImpl.96", %"struct.llvm::SmallVectorStorage.100" }
%"class.llvm::SmallVectorImpl.96" = type { %"class.llvm::SmallVectorTemplateBase.97" }
%"class.llvm::SmallVectorTemplateBase.97" = type { %"class.llvm::SmallVectorTemplateCommon.98" }
%"class.llvm::SmallVectorTemplateCommon.98" = type { %"class.llvm::SmallVectorBase.99" }
%"struct.llvm::SmallVectorStorage.100" = type { [128 x i8] }
%"class.llvm::SmallVector.101" = type { %"class.llvm::SmallVectorImpl.102", %"struct.llvm::SmallVectorStorage.105" }
%"class.llvm::SmallVectorImpl.102" = type { %"class.llvm::SmallVectorTemplateBase.103" }
%"class.llvm::SmallVectorTemplateBase.103" = type { %"class.llvm::SmallVectorTemplateCommon.104" }
%"class.llvm::SmallVectorTemplateCommon.104" = type { %"class.llvm::SmallVectorBase.99" }
%"struct.llvm::SmallVectorStorage.105" = type { [128 x i8] }
%"class.llvm::SmallVector.106" = type { %"class.llvm::SmallVectorImpl.107", %"struct.llvm::SmallVectorStorage.110" }
%"class.llvm::SmallVectorImpl.107" = type { %"class.llvm::SmallVectorTemplateBase.108" }
%"class.llvm::SmallVectorTemplateBase.108" = type { %"class.llvm::SmallVectorTemplateCommon.109" }
%"class.llvm::SmallVectorTemplateCommon.109" = type { %"class.llvm::SmallVectorBase.99" }
%"struct.llvm::SmallVectorStorage.110" = type { [32 x i8] }
%"class.llvm::Comdat" = type <{ %"class.llvm::StringMapEntry.392"*, i32, [4 x i8] }>
%"class.llvm::StringMapEntry.392" = type opaque
%"class.llvm::ilist_node.393" = type { %"class.llvm::ilist_node_impl.42" }
%"class.llvm::SymbolTableList.394" = type { %"class.llvm::iplist_impl.395" }
%"class.llvm::iplist_impl.395" = type { %"class.llvm::simple_ilist.398" }
%"class.llvm::simple_ilist.398" = type { %"class.llvm::ilist_sentinel.400" }
%"class.llvm::ilist_sentinel.400" = type { %"class.llvm::ilist_node_impl.383" }
%"class.llvm::Argument" = type <{ %"class.llvm::Value", %"class.llvm::Function"*, i32, [4 x i8] }>
%"class.std::unique_ptr.65" = type { %"class.std::__uniq_ptr_impl.66" }
%"class.std::__uniq_ptr_impl.66" = type { %"class.std::tuple.67" }
%"class.std::tuple.67" = type { %"struct.std::_Tuple_impl.68" }
%"struct.std::_Tuple_impl.68" = type { %"struct.std::_Head_base.73" }
%"struct.std::_Head_base.73" = type { %"class.llvm::ValueSymbolTable"* }
%"class.llvm::ValueSymbolTable" = type opaque
%"class.llvm::AttributeList" = type { %"class.llvm::AttributeListImpl"* }
%"class.llvm::AttributeListImpl" = type opaque
%"class.llvm::ilist_iterator" = type { %"class.llvm::ilist_node_impl.391"* }
%"class.llvm::IRBuilderFolder" = type { i32 (...)** }
%"class.llvm::MDNode" = type { %"class.llvm::Metadata", i32, i32, %"class.llvm::ContextAndReplaceableUses" }
%"class.llvm::Metadata" = type { i8, i8, i16, i32 }
%"class.llvm::ContextAndReplaceableUses" = type { %"class.llvm::PointerUnion" }
%"class.llvm::PointerUnion" = type { %"class.llvm::pointer_union_detail::PointerUnionMembers" }
%"class.llvm::pointer_union_detail::PointerUnionMembers" = type { %"class.llvm::pointer_union_detail::PointerUnionMembers.401" }
%"class.llvm::pointer_union_detail::PointerUnionMembers.401" = type { %"class.llvm::pointer_union_detail::PointerUnionMembers.402" }
%"class.llvm::pointer_union_detail::PointerUnionMembers.402" = type { %"class.llvm::PointerIntPair.403" }
%"class.llvm::PointerIntPair.403" = type { i64 }
%"class.llvm::FastMathFlags" = type { i32 }
%"class.llvm::ArrayRef" = type { %"class.llvm::OperandBundleDefT"*, i64 }
%"class.llvm::OperandBundleDefT" = type { %"class.std::__cxx11::basic_string", %"class.std::vector.404" }
%"class.std::vector.404" = type { %"struct.std::_Vector_base.405" }
%"struct.std::_Vector_base.405" = type { %"struct.std::_Vector_base<llvm::Value *, std::allocator<llvm::Value *>>::_Vector_impl" }
%"struct.std::_Vector_base<llvm::Value *, std::allocator<llvm::Value *>>::_Vector_impl" = type { %"class.llvm::Value"**, %"class.llvm::Value"**, %"class.llvm::Value"** }
%"class.llvm::ConstantFolder" = type { %"class.llvm::IRBuilderFolder" }
%"class.llvm::IRBuilderDefaultInserter" = type { i32 (...)** }
%"class.std::unique_ptr.26" = type { %"class.std::__uniq_ptr_impl.27" }
%"class.std::__uniq_ptr_impl.27" = type { %"class.std::tuple.28" }
%"class.std::tuple.28" = type { %"struct.std::_Tuple_impl.29" }
%"struct.std::_Tuple_impl.29" = type { %"struct.std::_Head_base.34" }
%"struct.std::_Head_base.34" = type { %"class.llvm::legacy::FunctionPassManager"* }
%"class.llvm::legacy::FunctionPassManager" = type { %"class.llvm::legacy::PassManagerBase", %"class.llvm::legacy::FunctionPassManagerImpl"*, %"class.llvm::Module"* }
%"class.llvm::legacy::PassManagerBase" = type { i32 (...)** }
%"class.llvm::legacy::FunctionPassManagerImpl" = type opaque
%"class.std::unique_ptr.111" = type { %"class.std::__uniq_ptr_impl.112" }
%"class.std::__uniq_ptr_impl.112" = type { %"class.std::tuple.113" }
%"class.std::tuple.113" = type { %"struct.std::_Tuple_impl.114" }
%"struct.std::_Tuple_impl.114" = type { %"struct.std::_Head_base.119" }
%"struct.std::_Head_base.119" = type { %"class.llvm::Module"* }
%"class.codegen::HammerConfig" = type { i8*, i8*, [100 x %"class.llvm::Pass"* ()*], [100 x %"class.llvm::Pass"* ()*] }
%"class.llvm::Pass" = type <{ i32 (...)**, %"class.llvm::AnalysisResolver"*, i8*, i32, [4 x i8] }>
%"class.llvm::AnalysisResolver" = type { %"class.std::vector", %"class.llvm::PMDataManager"* }
%"class.std::vector" = type { %"struct.std::_Vector_base" }
%"struct.std::_Vector_base" = type { %"struct.std::_Vector_base<std::pair<const void *, llvm::Pass *>, std::allocator<std::pair<const void *, llvm::Pass *>>>::_Vector_impl" }
%"struct.std::_Vector_base<std::pair<const void *, llvm::Pass *>, std::allocator<std::pair<const void *, llvm::Pass *>>>::_Vector_impl" = type { %"struct.std::pair"*, %"struct.std::pair"*, %"struct.std::pair"* }
%"struct.std::pair" = type { i8*, %"class.llvm::Pass"* }
%"class.llvm::PMDataManager" = type opaque
%"class.std::unique_ptr.123" = type { %"class.std::__uniq_ptr_impl.124" }
%"class.std::__uniq_ptr_impl.124" = type { %"class.std::tuple.125" }
%"class.std::tuple.125" = type { %"struct.std::_Tuple_impl.126" }
%"struct.std::_Tuple_impl.126" = type { %"struct.std::_Head_base.131" }
%"struct.std::_Head_base.131" = type { %"class.llvm::orc::LLJIT"* }
%"class.llvm::orc::LLJIT" = type { %"class.std::unique_ptr.132", %"class.std::unique_ptr.237", %"class.llvm::orc::JITDylib"*, %"class.llvm::DataLayout", %"class.llvm::Triple", %"class.std::unique_ptr.246", %"class.std::unique_ptr.266", %"class.std::unique_ptr.275", %"class.std::unique_ptr.284", %"class.std::unique_ptr.308", %"class.std::unique_ptr.308" }
%"class.std::unique_ptr.132" = type { %"class.std::__uniq_ptr_impl.133" }
%"class.std::__uniq_ptr_impl.133" = type { %"class.std::tuple.134" }
%"class.std::tuple.134" = type { %"struct.std::_Tuple_impl.135" }
%"struct.std::_Tuple_impl.135" = type { %"struct.std::_Head_base.140" }
%"struct.std::_Head_base.140" = type { %"class.llvm::orc::ExecutionSession"* }
%"class.llvm::orc::ExecutionSession" = type { %"class.std::recursive_mutex", i8, %"class.std::shared_ptr", %"class.std::unique_ptr.143", %"class.std::function.152", %"class.std::function.155", %"class.std::vector.221", %"class.std::vector.226", %"class.std::recursive_mutex", %"class.std::vector.231" }
%"class.std::shared_ptr" = type { %"class.std::__shared_ptr" }
%"class.std::__shared_ptr" = type { %"class.llvm::orc::SymbolStringPool"*, %"class.std::__shared_count" }
%"class.llvm::orc::SymbolStringPool" = type { %"class.std::mutex", %"class.llvm::StringMap.141" }
%"class.std::mutex" = type { %"class.std::__mutex_base" }
%"class.std::__mutex_base" = type { %union.pthread_mutex_t }
%union.pthread_mutex_t = type { %struct.__pthread_mutex_s }
%struct.__pthread_mutex_s = type { i32, i32, i32, i32, i32, i16, i16, %struct.__pthread_internal_list }
%struct.__pthread_internal_list = type { %struct.__pthread_internal_list*, %struct.__pthread_internal_list* }
%"class.llvm::StringMap.141" = type <{ %"class.llvm::StringMapImpl", %"class.llvm::MallocAllocator", [7 x i8] }>
%"class.std::__shared_count" = type { %"class.std::_Sp_counted_base"* }
%"class.std::_Sp_counted_base" = type { i32 (...)**, i32, i32 }
%"class.std::unique_ptr.143" = type { %"class.std::__uniq_ptr_impl.144" }
%"class.std::__uniq_ptr_impl.144" = type { %"class.std::tuple.145" }
%"class.std::tuple.145" = type { %"struct.std::_Tuple_impl.146" }
%"struct.std::_Tuple_impl.146" = type { %"struct.std::_Head_base.151" }
%"struct.std::_Head_base.151" = type { %"class.llvm::orc::Platform"* }
%"class.llvm::orc::Platform" = type { i32 (...)** }
%"class.std::function.152" = type { %"class.std::_Function_base", void (%"union.std::_Any_data"*, %"class.llvm::Error"*)* }
%"class.std::function.155" = type { %"class.std::_Function_base", void (%"union.std::_Any_data"*, %"class.std::unique_ptr.158"*, %"class.std::unique_ptr.170"*)* }
%"class.std::unique_ptr.158" = type { %"class.std::__uniq_ptr_impl.159" }
%"class.std::__uniq_ptr_impl.159" = type { %"class.std::tuple.160" }
%"class.std::tuple.160" = type { %"struct.std::_Tuple_impl.161" }
%"struct.std::_Tuple_impl.161" = type { %"struct.std::_Head_base.166" }
%"struct.std::_Head_base.166" = type { %"class.llvm::orc::MaterializationUnit"* }
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
%"class.std::unique_ptr.170" = type { %"class.std::__uniq_ptr_impl.171" }
%"class.std::__uniq_ptr_impl.171" = type { %"class.std::tuple.172" }
%"class.std::tuple.172" = type { %"struct.std::_Tuple_impl.173" }
%"struct.std::_Tuple_impl.173" = type { %"struct.std::_Head_base.178" }
%"struct.std::_Head_base.178" = type { %"class.llvm::orc::MaterializationResponsibility"* }
%"class.llvm::orc::MaterializationResponsibility" = type { %"class.llvm::IntrusiveRefCntPtr", %"class.llvm::DenseMap", %"class.llvm::orc::SymbolStringPtr" }
%"class.llvm::IntrusiveRefCntPtr" = type { %"class.llvm::orc::JITDylib"* }
%"class.std::vector.221" = type { %"struct.std::_Vector_base.222" }
%"struct.std::_Vector_base.222" = type { %"struct.std::_Vector_base<llvm::orc::ResourceManager *, std::allocator<llvm::orc::ResourceManager *>>::_Vector_impl" }
%"struct.std::_Vector_base<llvm::orc::ResourceManager *, std::allocator<llvm::orc::ResourceManager *>>::_Vector_impl" = type { %"class.llvm::orc::ResourceManager"**, %"class.llvm::orc::ResourceManager"**, %"class.llvm::orc::ResourceManager"** }
%"class.llvm::orc::ResourceManager" = type { i32 (...)** }
%"class.std::vector.226" = type { %"struct.std::_Vector_base.227" }
%"struct.std::_Vector_base.227" = type { %"struct.std::_Vector_base<llvm::IntrusiveRefCntPtr<llvm::orc::JITDylib>, std::allocator<llvm::IntrusiveRefCntPtr<llvm::orc::JITDylib>>>::_Vector_impl" }
%"struct.std::_Vector_base<llvm::IntrusiveRefCntPtr<llvm::orc::JITDylib>, std::allocator<llvm::IntrusiveRefCntPtr<llvm::orc::JITDylib>>>::_Vector_impl" = type { %"class.llvm::IntrusiveRefCntPtr"*, %"class.llvm::IntrusiveRefCntPtr"*, %"class.llvm::IntrusiveRefCntPtr"* }
%"class.std::recursive_mutex" = type { %"class.std::__recursive_mutex_base" }
%"class.std::__recursive_mutex_base" = type { %union.pthread_mutex_t }
%"class.std::vector.231" = type { %"struct.std::_Vector_base.232" }
%"struct.std::_Vector_base.232" = type { %"struct.std::_Vector_base<std::pair<std::unique_ptr<llvm::orc::MaterializationUnit>, std::unique_ptr<llvm::orc::MaterializationResponsibility>>, std::allocator<std::pair<std::unique_ptr<llvm::orc::MaterializationUnit>, std::unique_ptr<llvm::orc::MaterializationResponsibility>>>>::_Vector_impl" }
%"struct.std::_Vector_base<std::pair<std::unique_ptr<llvm::orc::MaterializationUnit>, std::unique_ptr<llvm::orc::MaterializationResponsibility>>, std::allocator<std::pair<std::unique_ptr<llvm::orc::MaterializationUnit>, std::unique_ptr<llvm::orc::MaterializationResponsibility>>>>::_Vector_impl" = type { %"struct.std::pair.236"*, %"struct.std::pair.236"*, %"struct.std::pair.236"* }
%"struct.std::pair.236" = type opaque
%"class.std::unique_ptr.237" = type { %"class.std::__uniq_ptr_impl.238" }
%"class.std::__uniq_ptr_impl.238" = type { %"class.std::tuple.239" }
%"class.std::tuple.239" = type { %"struct.std::_Tuple_impl.240" }
%"struct.std::_Tuple_impl.240" = type { %"struct.std::_Head_base.245" }
%"struct.std::_Head_base.245" = type { %"class.llvm::orc::LLJIT::PlatformSupport"* }
%"class.llvm::orc::LLJIT::PlatformSupport" = type { i32 (...)** }
%"class.llvm::orc::JITDylib" = type { %"class.llvm::ThreadSafeRefCountedBase", %"class.llvm::orc::ExecutionSession"*, %"class.std::__cxx11::basic_string", %"class.std::mutex", i8, [7 x i8], %"class.llvm::DenseMap.181", %"class.llvm::DenseMap.185", %"class.llvm::DenseMap.189", %"class.std::vector.193", %"class.std::vector.199", %"class.llvm::IntrusiveRefCntPtr.208", %"class.llvm::DenseMap.210", %"class.llvm::DenseMap.214" }
%"class.llvm::ThreadSafeRefCountedBase" = type { %"struct.std::atomic.179" }
%"struct.std::atomic.179" = type { %"struct.std::__atomic_base.180" }
%"struct.std::__atomic_base.180" = type { i32 }
%"class.llvm::DenseMap.181" = type <{ %"struct.llvm::detail::DenseMapPair.183"*, i32, i32, i32, [4 x i8] }>
%"struct.llvm::detail::DenseMapPair.183" = type { %"struct.std::pair.325" }
%"struct.std::pair.325" = type { %"class.llvm::orc::SymbolStringPtr", %"class.llvm::orc::JITDylib::SymbolTableEntry" }
%"class.llvm::orc::JITDylib::SymbolTableEntry" = type <{ i64, %"class.llvm::JITSymbolFlags", i8, [5 x i8] }>
%"class.llvm::DenseMap.185" = type <{ %"struct.llvm::detail::DenseMapPair.187"*, i32, i32, i32, [4 x i8] }>
%"struct.llvm::detail::DenseMapPair.187" = type { %"struct.std::pair.328" }
%"struct.std::pair.328" = type { %"class.llvm::orc::SymbolStringPtr", %"class.std::shared_ptr.331" }
%"class.std::shared_ptr.331" = type { %"class.std::__shared_ptr.332" }
%"class.std::__shared_ptr.332" = type { %"struct.llvm::orc::JITDylib::UnmaterializedInfo"*, %"class.std::__shared_count" }
%"struct.llvm::orc::JITDylib::UnmaterializedInfo" = type { %"class.std::unique_ptr.158", %"class.llvm::orc::ResourceTracker"* }
%"class.llvm::orc::ResourceTracker" = type { %"class.llvm::ThreadSafeRefCountedBase.209", %"struct.std::atomic" }
%"class.llvm::ThreadSafeRefCountedBase.209" = type { %"struct.std::atomic.179" }
%"class.llvm::DenseMap.189" = type <{ %"struct.llvm::detail::DenseMapPair.191"*, i32, i32, i32, [4 x i8] }>
%"struct.llvm::detail::DenseMapPair.191" = type { %"struct.std::pair.334" }
%"struct.std::pair.334" = type { %"class.llvm::orc::SymbolStringPtr", %"struct.llvm::orc::JITDylib::MaterializingInfo" }
%"struct.llvm::orc::JITDylib::MaterializingInfo" = type { %"class.llvm::DenseMap.337", %"class.llvm::DenseMap.337", %"class.std::vector.347" }
%"class.llvm::DenseMap.337" = type <{ %"struct.llvm::detail::DenseMapPair.339"*, i32, i32, i32, [4 x i8] }>
%"struct.llvm::detail::DenseMapPair.339" = type { %"struct.std::pair.340" }
%"struct.std::pair.340" = type { %"class.llvm::orc::JITDylib"*, %"class.llvm::DenseSet" }
%"class.llvm::DenseSet" = type { %"class.llvm::detail::DenseSetImpl" }
%"class.llvm::detail::DenseSetImpl" = type { %"class.llvm::DenseMap.343" }
%"class.llvm::DenseMap.343" = type <{ %"class.llvm::detail::DenseSetPair"*, i32, i32, i32, [4 x i8] }>
%"class.llvm::detail::DenseSetPair" = type { %"class.llvm::orc::SymbolStringPtr" }
%"class.std::vector.347" = type { %"struct.std::_Vector_base.348" }
%"struct.std::_Vector_base.348" = type { %"struct.std::_Vector_base<std::shared_ptr<llvm::orc::AsynchronousSymbolQuery>, std::allocator<std::shared_ptr<llvm::orc::AsynchronousSymbolQuery>>>::_Vector_impl" }
%"struct.std::_Vector_base<std::shared_ptr<llvm::orc::AsynchronousSymbolQuery>, std::allocator<std::shared_ptr<llvm::orc::AsynchronousSymbolQuery>>>::_Vector_impl" = type { %"class.std::shared_ptr.352"*, %"class.std::shared_ptr.352"*, %"class.std::shared_ptr.352"* }
%"class.std::shared_ptr.352" = type { %"class.std::__shared_ptr.353" }
%"class.std::__shared_ptr.353" = type { %"class.llvm::orc::AsynchronousSymbolQuery"*, %"class.std::__shared_count" }
%"class.llvm::orc::AsynchronousSymbolQuery" = type <{ %"class.llvm::unique_function.355", %"class.llvm::DenseMap.337", %"class.llvm::DenseMap.358", i64, i8, [7 x i8] }>
%"class.llvm::unique_function.355" = type { %"class.llvm::detail::UniqueFunctionBase.356" }
%"class.llvm::detail::UniqueFunctionBase.356" = type { %"union.llvm::detail::UniqueFunctionBase<void, llvm::Expected<llvm::DenseMap<llvm::orc::SymbolStringPtr, llvm::JITEvaluatedSymbol>>>::StorageUnionT", %"class.llvm::PointerIntPair.357" }
%"union.llvm::detail::UniqueFunctionBase<void, llvm::Expected<llvm::DenseMap<llvm::orc::SymbolStringPtr, llvm::JITEvaluatedSymbol>>>::StorageUnionT" = type { %"struct.llvm::detail::UniqueFunctionBase<void, llvm::Expected<llvm::DenseMap<llvm::orc::SymbolStringPtr, llvm::JITEvaluatedSymbol>>>::StorageUnionT::OutOfLineStorageT" }
%"struct.llvm::detail::UniqueFunctionBase<void, llvm::Expected<llvm::DenseMap<llvm::orc::SymbolStringPtr, llvm::JITEvaluatedSymbol>>>::StorageUnionT::OutOfLineStorageT" = type { i8*, i64, i64 }
%"class.llvm::PointerIntPair.357" = type { i64 }
%"class.llvm::DenseMap.358" = type <{ %"struct.llvm::detail::DenseMapPair.360"*, i32, i32, i32, [4 x i8] }>
%"struct.llvm::detail::DenseMapPair.360" = type { %"struct.std::pair.361" }
%"struct.std::pair.361" = type { %"class.llvm::orc::SymbolStringPtr", %"class.llvm::JITEvaluatedSymbol" }
%"class.llvm::JITEvaluatedSymbol" = type <{ i64, %"class.llvm::JITSymbolFlags", [6 x i8] }>
%"class.std::vector.193" = type { %"struct.std::_Vector_base.194" }
%"struct.std::_Vector_base.194" = type { %"struct.std::_Vector_base<std::shared_ptr<llvm::orc::DefinitionGenerator>, std::allocator<std::shared_ptr<llvm::orc::DefinitionGenerator>>>::_Vector_impl" }
%"struct.std::_Vector_base<std::shared_ptr<llvm::orc::DefinitionGenerator>, std::allocator<std::shared_ptr<llvm::orc::DefinitionGenerator>>>::_Vector_impl" = type { %"class.std::shared_ptr.198"*, %"class.std::shared_ptr.198"*, %"class.std::shared_ptr.198"* }
%"class.std::shared_ptr.198" = type { %"class.std::__shared_ptr.365" }
%"class.std::__shared_ptr.365" = type { %"class.llvm::orc::DefinitionGenerator"*, %"class.std::__shared_count" }
%"class.llvm::orc::DefinitionGenerator" = type { i32 (...)** }
%"class.std::vector.199" = type { %"struct.std::_Vector_base.200" }
%"struct.std::_Vector_base.200" = type { %"struct.std::_Vector_base<std::pair<llvm::orc::JITDylib *, llvm::orc::JITDylibLookupFlags>, std::allocator<std::pair<llvm::orc::JITDylib *, llvm::orc::JITDylibLookupFlags>>>::_Vector_impl" }
%"struct.std::_Vector_base<std::pair<llvm::orc::JITDylib *, llvm::orc::JITDylibLookupFlags>, std::allocator<std::pair<llvm::orc::JITDylib *, llvm::orc::JITDylibLookupFlags>>>::_Vector_impl" = type { %"struct.std::pair.204"*, %"struct.std::pair.204"*, %"struct.std::pair.204"* }
%"struct.std::pair.204" = type <{ %"class.llvm::orc::JITDylib"*, i32, [4 x i8] }>
%"class.llvm::IntrusiveRefCntPtr.208" = type { %"class.llvm::orc::ResourceTracker"* }
%"class.llvm::DenseMap.210" = type <{ %"struct.llvm::detail::DenseMapPair.212"*, i32, i32, i32, [4 x i8] }>
%"struct.llvm::detail::DenseMapPair.212" = type { %"struct.std::pair.367" }
%"struct.std::pair.367" = type { %"class.llvm::orc::ResourceTracker"*, %"class.std::vector.370" }
%"class.std::vector.370" = type { %"struct.std::_Vector_base.371" }
%"struct.std::_Vector_base.371" = type { %"struct.std::_Vector_base<llvm::orc::SymbolStringPtr, std::allocator<llvm::orc::SymbolStringPtr>>::_Vector_impl" }
%"struct.std::_Vector_base<llvm::orc::SymbolStringPtr, std::allocator<llvm::orc::SymbolStringPtr>>::_Vector_impl" = type { %"class.llvm::orc::SymbolStringPtr"*, %"class.llvm::orc::SymbolStringPtr"*, %"class.llvm::orc::SymbolStringPtr"* }
%"class.llvm::DenseMap.214" = type <{ %"struct.llvm::detail::DenseMapPair.216"*, i32, i32, i32, [4 x i8] }>
%"struct.llvm::detail::DenseMapPair.216" = type { %"struct.std::pair.217" }
%"struct.std::pair.217" = type { %"class.llvm::orc::MaterializationResponsibility"*, %"class.llvm::orc::ResourceTracker"* }
%"class.llvm::Triple" = type { %"class.std::__cxx11::basic_string", i32, i32, i32, i32, i32, i32 }
%"class.std::unique_ptr.246" = type { %"class.std::__uniq_ptr_impl.247" }
%"class.std::__uniq_ptr_impl.247" = type { %"class.std::tuple.248" }
%"class.std::tuple.248" = type { %"struct.std::_Tuple_impl.249" }
%"struct.std::_Tuple_impl.249" = type { %"struct.std::_Head_base.254" }
%"struct.std::_Head_base.254" = type { %"class.llvm::ThreadPool"* }
%"class.llvm::ThreadPool" = type <{ %"class.std::vector.255", %"class.std::queue", %"class.std::mutex", %"class.std::condition_variable", %"class.std::condition_variable", i32, i8, [3 x i8], i32, [4 x i8] }>
%"class.std::vector.255" = type { %"struct.std::_Vector_base.256" }
%"struct.std::_Vector_base.256" = type { %"struct.std::_Vector_base<std::thread, std::allocator<std::thread>>::_Vector_impl" }
%"struct.std::_Vector_base<std::thread, std::allocator<std::thread>>::_Vector_impl" = type { %"class.std::thread"*, %"class.std::thread"*, %"class.std::thread"* }
%"class.std::thread" = type { %"class.std::thread::id" }
%"class.std::thread::id" = type { i64 }
%"class.std::queue" = type { %"class.std::deque" }
%"class.std::deque" = type { %"class.std::_Deque_base" }
%"class.std::_Deque_base" = type { %"struct.std::_Deque_base<std::packaged_task<void ()>, std::allocator<std::packaged_task<void ()>>>::_Deque_impl" }
%"struct.std::_Deque_base<std::packaged_task<void ()>, std::allocator<std::packaged_task<void ()>>>::_Deque_impl" = type { %"class.std::packaged_task"**, i64, %"struct.std::_Deque_iterator", %"struct.std::_Deque_iterator" }
%"class.std::packaged_task" = type { %"class.std::shared_ptr.375" }
%"class.std::shared_ptr.375" = type { %"class.std::__shared_ptr.376" }
%"class.std::__shared_ptr.376" = type { %"class.std::__future_base::_Task_state_base"*, %"class.std::__shared_count" }
%"class.std::__future_base::_Task_state_base" = type opaque
%"struct.std::_Deque_iterator" = type { %"class.std::packaged_task"*, %"class.std::packaged_task"*, %"class.std::packaged_task"*, %"class.std::packaged_task"** }
%"class.std::condition_variable" = type { %union.pthread_cond_t }
%union.pthread_cond_t = type { %struct.__pthread_cond_s }
%struct.__pthread_cond_s = type { %union.anon.263, %union.anon.264, [2 x i32], [2 x i32], i32, i32, [2 x i32] }
%union.anon.263 = type { i64 }
%union.anon.264 = type { i64 }
%"class.std::unique_ptr.266" = type { %"class.std::__uniq_ptr_impl.267" }
%"class.std::__uniq_ptr_impl.267" = type { %"class.std::tuple.268" }
%"class.std::tuple.268" = type { %"struct.std::_Tuple_impl.269" }
%"struct.std::_Tuple_impl.269" = type { %"struct.std::_Head_base.274" }
%"struct.std::_Head_base.274" = type { %"class.llvm::orc::ObjectLayer"* }
%"class.llvm::orc::ObjectLayer" = type { i32 (...)**, %"class.llvm::orc::ExecutionSession"* }
%"class.std::unique_ptr.275" = type { %"class.std::__uniq_ptr_impl.276" }
%"class.std::__uniq_ptr_impl.276" = type { %"class.std::tuple.277" }
%"class.std::tuple.277" = type { %"struct.std::_Tuple_impl.278" }
%"struct.std::_Tuple_impl.278" = type { %"struct.std::_Head_base.283" }
%"struct.std::_Head_base.283" = type { %"class.llvm::orc::ObjectTransformLayer"* }
%"class.llvm::orc::ObjectTransformLayer" = type opaque
%"class.std::unique_ptr.284" = type { %"class.std::__uniq_ptr_impl.285" }
%"class.std::__uniq_ptr_impl.285" = type { %"class.std::tuple.286" }
%"class.std::tuple.286" = type { %"struct.std::_Tuple_impl.287" }
%"struct.std::_Tuple_impl.287" = type { %"struct.std::_Head_base.292" }
%"struct.std::_Head_base.292" = type { %"class.llvm::orc::IRCompileLayer"* }
%"class.llvm::orc::IRCompileLayer" = type { %"class.llvm::orc::IRLayer", %"class.std::mutex", %"class.llvm::orc::ObjectLayer"*, %"class.std::unique_ptr.293", %"struct.llvm::orc::IRSymbolMapper::ManglingOptions"*, %"class.std::function.302" }
%"class.llvm::orc::IRLayer" = type { i32 (...)**, i8, %"class.llvm::orc::ExecutionSession"*, %"struct.llvm::orc::IRSymbolMapper::ManglingOptions"** }
%"class.std::unique_ptr.293" = type { %"class.std::__uniq_ptr_impl.294" }
%"class.std::__uniq_ptr_impl.294" = type { %"class.std::tuple.295" }
%"class.std::tuple.295" = type { %"struct.std::_Tuple_impl.296" }
%"struct.std::_Tuple_impl.296" = type { %"struct.std::_Head_base.301" }
%"struct.std::_Head_base.301" = type { %"class.llvm::orc::IRCompileLayer::IRCompiler"* }
%"class.llvm::orc::IRCompileLayer::IRCompiler" = type <{ i32 (...)**, %"struct.llvm::orc::IRSymbolMapper::ManglingOptions", [7 x i8] }>
%"struct.llvm::orc::IRSymbolMapper::ManglingOptions" = type { i8 }
%"class.std::function.302" = type { %"class.std::_Function_base", void (%"union.std::_Any_data"*, %"class.llvm::orc::MaterializationResponsibility"*, %"class.llvm::orc::ThreadSafeModule"*)* }
%"class.llvm::orc::ThreadSafeModule" = type { %"class.std::unique_ptr.111", %"class.llvm::orc::ThreadSafeContext" }
%"class.llvm::orc::ThreadSafeContext" = type { %"class.std::shared_ptr.305" }
%"class.std::shared_ptr.305" = type { %"class.std::__shared_ptr.306" }
%"class.std::__shared_ptr.306" = type { %"struct.llvm::orc::ThreadSafeContext::State"*, %"class.std::__shared_count" }
%"struct.llvm::orc::ThreadSafeContext::State" = type { %"class.std::unique_ptr", %"class.std::recursive_mutex" }
%"class.std::unique_ptr.308" = type { %"class.std::__uniq_ptr_impl.309" }
%"class.std::__uniq_ptr_impl.309" = type { %"class.std::tuple.310" }
%"class.std::tuple.310" = type { %"struct.std::_Tuple_impl.311" }
%"struct.std::_Tuple_impl.311" = type { %"struct.std::_Head_base.316" }
%"struct.std::_Head_base.316" = type { %"class.llvm::orc::IRTransformLayer"* }
%"class.llvm::orc::IRTransformLayer" = type { %"class.llvm::orc::IRLayer", %"class.llvm::orc::IRLayer"*, %"class.llvm::unique_function" }
%"class.llvm::unique_function" = type { %"class.llvm::detail::UniqueFunctionBase" }
%"class.llvm::detail::UniqueFunctionBase" = type { %"union.llvm::detail::UniqueFunctionBase<llvm::Expected<llvm::orc::ThreadSafeModule>, llvm::orc::ThreadSafeModule, llvm::orc::MaterializationResponsibility &>::StorageUnionT", %"class.llvm::PointerIntPair" }
%"union.llvm::detail::UniqueFunctionBase<llvm::Expected<llvm::orc::ThreadSafeModule>, llvm::orc::ThreadSafeModule, llvm::orc::MaterializationResponsibility &>::StorageUnionT" = type { %"struct.llvm::detail::UniqueFunctionBase<llvm::Expected<llvm::orc::ThreadSafeModule>, llvm::orc::ThreadSafeModule, llvm::orc::MaterializationResponsibility &>::StorageUnionT::OutOfLineStorageT" }
%"struct.llvm::detail::UniqueFunctionBase<llvm::Expected<llvm::orc::ThreadSafeModule>, llvm::orc::ThreadSafeModule, llvm::orc::MaterializationResponsibility &>::StorageUnionT::OutOfLineStorageT" = type { i8*, i64, i64 }
%"class.llvm::PointerIntPair" = type { i64 }
%"class.llvm::Expected" = type { %union.anon.318, i8, [7 x i8] }
%union.anon.318 = type { %"struct.llvm::AlignedCharArrayUnion" }
%"struct.llvm::AlignedCharArrayUnion" = type { [16 x i8] }
%"struct.std::_Rb_tree_node" = type { %"struct.std::_Rb_tree_node_base", %"struct.__gnu_cxx::__aligned_membuf.409" }
%"struct.__gnu_cxx::__aligned_membuf.409" = type { [40 x i8] }
%"class.std::tuple.436" = type { %"struct.std::_Tuple_impl.437" }
%"struct.std::_Tuple_impl.437" = type { %"struct.std::_Head_base.438" }
%"struct.std::_Head_base.438" = type { %"class.std::__cxx11::basic_string"* }
%"class.std::tuple.439" = type { i8 }
%struct.JitSortContext = type { %"class.llvm::orc::LLJIT"*, i64 (i32*, i32, i32*, i32, i32*, i32*, i32*, i32)*, void (i64, i32*, i32*, i32*, i32*, i32, i32, i32)*, void (i64, i32*, i32*, i32, i32)*, void (i64, i32*, i32, i64, i32*, i32, i32)* }
%class.Sort = type { %class.OpTemplate, i32*, i32, i32*, i32, i32*, i32*, i32*, i32, %class.PagesIndex* }
%class.OpTemplate = type { i32 (...)** }
%class.PagesIndex = type <{ i32*, i32, [4 x i8], i64*, i32, [4 x i8], %class.Column***, i32, [4 x i8] }>
%class.Column = type { i32 (...)**, i8*, i32*, i32, i64 }
%class.Table = type <{ i32 (...)**, %class.Layout, [7 x i8], %"class.std::vector.320", i32*, i32, i32, i32, [4 x i8] }>
%class.Layout = type { i8 }
%"class.std::vector.320" = type { %"struct.std::_Vector_base.321" }
%"struct.std::_Vector_base.321" = type { %"struct.std::_Vector_base<Column *, std::allocator<Column *>>::_Vector_impl" }
%"struct.std::_Vector_base<Column *, std::allocator<Column *>>::_Vector_impl" = type { %class.Column**, %class.Column**, %class.Column** }
%"struct.std::pair.410" = type { %"class.std::__cxx11::basic_string", %"class.codegen::ParamValue"* }

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

$_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE22_M_emplace_hint_uniqueIJRKSt21piecewise_construct_tSt5tupleIJOS5_EESM_IJEEEEESt17_Rb_tree_iteratorISB_ESt23_Rb_tree_const_iteratorISB_EDpOT_ = comdat any

$_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE29_M_get_insert_hint_unique_posESt23_Rb_tree_const_iteratorISB_ERS7_ = comdat any

$_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE24_M_get_insert_unique_posERS7_ = comdat any

$_ZTV5Table = comdat any

$_ZTS5Table = comdat any

$_ZTI5Table = comdat any

@_ZStL8__ioinit = internal global %"class.std::ios_base::Init" zeroinitializer, align 1
@__dso_handle = external hidden global i8
@_ZN4llvm24DisableABIBreakingChecksE = external dso_local global i32, align 4
@_ZN4llvm30VerifyDisableABIBreakingChecksE = weak hidden local_unnamed_addr global i32* @_ZN4llvm24DisableABIBreakingChecksE, align 8
@.str = private unnamed_addr constant [30 x i8] c"_Z10createSortPiiS_iS_S_S_i@0\00", align 1
@.str.1 = private unnamed_addr constant [30 x i8] c"_Z10createSortPiiS_iS_S_S_i@1\00", align 1
@.str.2 = private unnamed_addr constant [30 x i8] c"_Z10createSortPiiS_iS_S_S_i@2\00", align 1
@.str.3 = private unnamed_addr constant [30 x i8] c"_Z10createSortPiiS_iS_S_S_i@3\00", align 1
@.str.4 = private unnamed_addr constant [30 x i8] c"_Z10createSortPiiS_iS_S_S_i@4\00", align 1
@.str.5 = private unnamed_addr constant [30 x i8] c"_Z10createSortPiiS_iS_S_S_i@5\00", align 1
@.str.6 = private unnamed_addr constant [30 x i8] c"_Z10createSortPiiS_iS_S_S_i@6\00", align 1
@.str.7 = private unnamed_addr constant [30 x i8] c"_Z10createSortPiiS_iS_S_S_i@7\00", align 1
@.str.8 = private unnamed_addr constant [27 x i8] c"_Z9compareTolPiS_S_S_iii@1\00", align 1
@.str.9 = private unnamed_addr constant [27 x i8] c"_Z9compareTolPiS_S_S_iii@2\00", align 1
@.str.10 = private unnamed_addr constant [27 x i8] c"_Z9compareTolPiS_S_S_iii@3\00", align 1
@.str.11 = private unnamed_addr constant [27 x i8] c"_Z9compareTolPiS_S_S_iii@4\00", align 1
@.str.12 = private unnamed_addr constant [27 x i8] c"_Z9compareTolPiS_S_S_iii@5\00", align 1
@.str.13 = private unnamed_addr constant [26 x i8] c"_Z12allocColumnslPiS_ii@1\00", align 1
@.str.14 = private unnamed_addr constant [26 x i8] c"_Z12allocColumnslPiS_ii@2\00", align 1
@.str.15 = private unnamed_addr constant [26 x i8] c"_Z12allocColumnslPiS_ii@3\00", align 1
@.str.16 = private unnamed_addr constant [24 x i8] c"_Z9getResultlPiilS_ii@1\00", align 1
@.str.17 = private unnamed_addr constant [24 x i8] c"_Z9getResultlPiilS_ii@2\00", align 1
@.str.18 = private unnamed_addr constant [24 x i8] c"_Z9getResultlPiilS_ii@4\00", align 1
@.str.19 = private unnamed_addr constant [45 x i8] c"/usr/lib/gcc/x86_64-linux-gnu/7/libstdc++.so\00", align 1
@.str.20 = private unnamed_addr constant [20 x i8] c"/opt/lib/ir/sort.ll\00", align 1
@.str.21 = private unnamed_addr constant [27 x i8] c"/opt/lib/ir/memory_pool.ll\00", align 1
@.str.22 = private unnamed_addr constant [28 x i8] c"_Z10createSortPiiS_iS_S_S_i\00", align 1
@.str.23 = private unnamed_addr constant [25 x i8] c"_Z9quickSortlPiS_S_S_iii\00", align 1
@.str.24 = private unnamed_addr constant [24 x i8] c"_Z12allocColumnslPiS_ii\00", align 1
@.str.25 = private unnamed_addr constant [22 x i8] c"_Z9getResultlPiilS_ii\00", align 1
@DEFAULT_MAX_PAGE_SIZE_IN_BYTES = dso_local local_unnamed_addr global i32 1048576, align 4
@__const.HammerConfig.ALL = private unnamed_addr constant <{ i8, [31 x i8] }> <{ i8 1, [31 x i8] zeroinitializer }>, align 16
@.str.27 = private unnamed_addr constant [39 x i8] c"NumItems + NumTombstones <= NumBuckets\00", align 1
@.str.28 = private unnamed_addr constant [42 x i8] c"/usr/include/llvm-12/llvm/ADT/StringMap.h\00", align 1
@__PRETTY_FUNCTION__._ZN4llvm9StringMapISt6atomicImENS_15MallocAllocatorEE11try_emplaceIJiEEESt4pairINS_17StringMapIteratorIS2_EEbENS_9StringRefEDpOT_ = private unnamed_addr constant [206 x i8] c"std::pair<iterator, bool> llvm::StringMap<std::atomic<unsigned long>>::try_emplace(llvm::StringRef, ArgsTy &&...) [ValueTy = std::atomic<unsigned long>, AllocatorTy = llvm::MallocAllocator, ArgsTy = <int>]\00", align 1
@_ZTV5Table = linkonce_odr dso_local unnamed_addr constant { [4 x i8*] } { [4 x i8*] [i8* null, i8* bitcast ({ i8*, i8* }* @_ZTI5Table to i8*), i8* bitcast (void (%class.Table*)* @_ZN5TableD2Ev to i8*), i8* bitcast (void (%class.Table*)* @_ZN5TableD0Ev to i8*)] }, comdat, align 8
@_ZTVN10__cxxabiv117__class_type_infoE = external dso_local global i8*
@_ZTS5Table = linkonce_odr dso_local constant [7 x i8] c"5Table\00", comdat, align 1
@_ZTI5Table = linkonce_odr dso_local constant { i8*, i8* } { i8* bitcast (i8** getelementptr inbounds (i8*, i8** @_ZTVN10__cxxabiv117__class_type_infoE, i64 2) to i8*), i8* getelementptr inbounds ([7 x i8], [7 x i8]* @_ZTS5Table, i32 0, i32 0) }, comdat, align 8
@.str.33 = private unnamed_addr constant [54 x i8] c"!HasError && \22Cannot get value when an error exists!\22\00", align 1
@.str.34 = private unnamed_addr constant [42 x i8] c"/usr/include/llvm-12/llvm/Support/Error.h\00", align 1
@__PRETTY_FUNCTION__._ZN4llvm8ExpectedINS_18JITEvaluatedSymbolEE10getStorageEv = private unnamed_addr constant [116 x i8] c"llvm::Expected::storage_type *llvm::Expected<llvm::JITEvaluatedSymbol>::getStorage() [T = llvm::JITEvaluatedSymbol]\00", align 1
@_ZStL19piecewise_construct = internal constant %"struct.std::piecewise_construct_t" undef, align 1
@llvm.global_ctors = appending global [1 x { i32, void ()*, i8* }] [{ i32, void ()*, i8* } { i32 65535, void ()* @_GLOBAL__sub_I_sort_api.cpp, i8* null }]

declare dso_local void @_ZNSt8ios_base4InitC1Ev(%"class.std::ios_base::Init"* nonnull dereferenceable(1)) unnamed_addr #0

; Function Attrs: nounwind
declare dso_local void @_ZNSt8ios_base4InitD1Ev(%"class.std::ios_base::Init"* nonnull dereferenceable(1)) unnamed_addr #1

; Function Attrs: nofree nounwind
declare dso_local i32 @__cxa_atexit(void (i8*)*, i8*, i8*) local_unnamed_addr #2

; Function Attrs: uwtable
define dso_local i64 @_Z11sortPreparePiiS_iS_S_S_i(i32* %sourceTypes, i32 %typeCount, i32* %outputCols, i32 %outputColCount, i32* %sortCols, i32* %sortAscendings, i32* %sortNullFirsts, i32 %sortColCount) local_unnamed_addr #3 personality i8* bitcast (i32 (...)* @__gxx_personality_v0 to i8*) {
entry:
  %ALL.i = alloca [32 x i8], align 16
  %__an.i.i.i948 = alloca %"struct.std::_Rb_tree<std::__cxx11::basic_string<char>, std::pair<const std::__cxx11::basic_string<char>, codegen::ParamValue *>, std::_Select1st<std::pair<const std::__cxx11::basic_string<char>, codegen::ParamValue *>>, std::less<std::__cxx11::basic_string<char>>>::_Alloc_node", align 8
  %__an.i.i.i = alloca %"struct.std::_Rb_tree<std::__cxx11::basic_string<char>, std::pair<const std::__cxx11::basic_string<char>, codegen::ParamValue *>, std::_Select1st<std::pair<const std::__cxx11::basic_string<char>, codegen::ParamValue *>>, std::less<std::__cxx11::basic_string<char>>>::_Alloc_node", align 8
  %__dnew.i.i.i.i910 = alloca i64, align 8
  %__dnew.i.i.i.i885 = alloca i64, align 8
  %__dnew.i.i.i.i860 = alloca i64, align 8
  %__dnew.i.i.i.i835 = alloca i64, align 8
  %__dnew.i.i.i.i810 = alloca i64, align 8
  %__dnew.i.i.i.i785 = alloca i64, align 8
  %__dnew.i.i.i.i760 = alloca i64, align 8
  %__dnew.i.i.i.i735 = alloca i64, align 8
  %__dnew.i.i.i.i710 = alloca i64, align 8
  %__dnew.i.i.i.i685 = alloca i64, align 8
  %__dnew.i.i.i.i660 = alloca i64, align 8
  %__dnew.i.i.i.i635 = alloca i64, align 8
  %__dnew.i.i.i.i610 = alloca i64, align 8
  %__dnew.i.i.i.i585 = alloca i64, align 8
  %__dnew.i.i.i.i560 = alloca i64, align 8
  %__dnew.i.i.i.i540 = alloca i64, align 8
  %__dnew.i.i.i.i516 = alloca i64, align 8
  %__dnew.i.i.i.i496 = alloca i64, align 8
  %__dnew.i.i.i.i = alloca i64, align 8
  %typeCount.addr = alloca i32, align 4
  %outputColCount.addr = alloca i32, align 4
  %sortColCount.addr = alloca i32, align 4
  %testParam = alloca %"class.std::map", align 8
  %deps = alloca %"class.std::__cxx11::list", align 8
  %p_sourceTypes = alloca %"class.codegen::ParamValue", align 8
  %p_typeCount = alloca %"class.codegen::ParamValue", align 8
  %p_outputCols = alloca %"class.codegen::ParamValue", align 8
  %p_outputColCount = alloca %"class.codegen::ParamValue", align 8
  %p_sortCols = alloca %"class.codegen::ParamValue", align 8
  %p_sortColTypes = alloca %"class.codegen::ParamValue", align 8
  %p_sortAscendings = alloca %"class.codegen::ParamValue", align 8
  %p_sortNullFirsts = alloca %"class.codegen::ParamValue", align 8
  %p_sortColCount = alloca %"class.codegen::ParamValue", align 8
  %ref.tmp = alloca %"class.std::__cxx11::basic_string", align 8
  %ref.tmp28 = alloca %"class.std::__cxx11::basic_string", align 8
  %ref.tmp39 = alloca %"class.std::__cxx11::basic_string", align 8
  %ref.tmp50 = alloca %"class.std::__cxx11::basic_string", align 8
  %ref.tmp61 = alloca %"class.std::__cxx11::basic_string", align 8
  %ref.tmp72 = alloca %"class.std::__cxx11::basic_string", align 8
  %ref.tmp83 = alloca %"class.std::__cxx11::basic_string", align 8
  %ref.tmp94 = alloca %"class.std::__cxx11::basic_string", align 8
  %ref.tmp105 = alloca %"class.std::__cxx11::basic_string", align 8
  %ref.tmp116 = alloca %"class.std::__cxx11::basic_string", align 8
  %ref.tmp127 = alloca %"class.std::__cxx11::basic_string", align 8
  %ref.tmp138 = alloca %"class.std::__cxx11::basic_string", align 8
  %ref.tmp149 = alloca %"class.std::__cxx11::basic_string", align 8
  %ref.tmp160 = alloca %"class.std::__cxx11::basic_string", align 8
  %ref.tmp171 = alloca %"class.std::__cxx11::basic_string", align 8
  %ref.tmp182 = alloca %"class.std::__cxx11::basic_string", align 8
  %ref.tmp193 = alloca %"class.std::__cxx11::basic_string", align 8
  %ref.tmp204 = alloca %"class.std::__cxx11::basic_string", align 8
  %ref.tmp215 = alloca %"class.std::__cxx11::basic_string", align 8
  %hammer1 = alloca %"class.codegen::Hammer", align 8
  %agg.tmp230 = alloca %"class.std::map", align 8
  %hammer2 = alloca %"class.codegen::Hammer", align 8
  %agg.tmp238 = alloca %"class.std::map", align 8
  %hammerConfig = alloca %"class.codegen::HammerConfig", align 8
  %jitter = alloca %"class.std::unique_ptr.123", align 8
  %agg.tmp254 = alloca %"class.std::__cxx11::list", align 8
  %ref.tmp260 = alloca %"class.llvm::Expected", align 8
  %ref.tmp273 = alloca %"class.llvm::Expected", align 8
  %ref.tmp286 = alloca %"class.llvm::Expected", align 8
  %ref.tmp299 = alloca %"class.llvm::Expected", align 8
  store i32 %typeCount, i32* %typeCount.addr, align 4, !tbaa !2
  store i32 %outputColCount, i32* %outputColCount.addr, align 4, !tbaa !2
  store i32 %sortColCount, i32* %sortColCount.addr, align 4, !tbaa !2
  %0 = getelementptr inbounds %"class.std::map", %"class.std::map"* %testParam, i64 0, i32 0, i32 0, i32 0, i32 0, i32 0
  call void @llvm.lifetime.start.p0i8(i64 48, i8* nonnull %0) #22
  %1 = getelementptr inbounds i8, i8* %0, i64 8
  %_M_color.i.i.i.i = bitcast i8* %1 to i32*
  store i32 0, i32* %_M_color.i.i.i.i, align 8, !tbaa !6
  %_M_parent.i.i.i.i.i = getelementptr inbounds i8, i8* %0, i64 16
  %2 = bitcast i8* %_M_parent.i.i.i.i.i to %"struct.std::_Rb_tree_node_base"**
  store %"struct.std::_Rb_tree_node_base"* null, %"struct.std::_Rb_tree_node_base"** %2, align 8, !tbaa !12
  %_M_left.i.i.i.i.i = getelementptr inbounds i8, i8* %0, i64 24
  %3 = bitcast i8* %_M_left.i.i.i.i.i to i8**
  store i8* %1, i8** %3, align 8, !tbaa !13
  %_M_right.i.i.i.i.i = getelementptr inbounds i8, i8* %0, i64 32
  %4 = bitcast i8* %_M_right.i.i.i.i.i to i8**
  store i8* %1, i8** %4, align 8, !tbaa !14
  %_M_node_count.i.i.i.i.i = getelementptr inbounds i8, i8* %0, i64 40
  %5 = bitcast i8* %_M_node_count.i.i.i.i.i to i64*
  store i64 0, i64* %5, align 8, !tbaa !15
  %6 = bitcast %"class.std::__cxx11::list"* %deps to i8*
  call void @llvm.lifetime.start.p0i8(i64 24, i8* nonnull %6) #22
  %7 = getelementptr inbounds %"class.std::__cxx11::list", %"class.std::__cxx11::list"* %deps, i64 0, i32 0, i32 0, i32 0, i32 0
  %_M_next.i.i.i = getelementptr inbounds %"class.std::__cxx11::list", %"class.std::__cxx11::list"* %deps, i64 0, i32 0, i32 0, i32 0, i32 0, i32 0
  store %"struct.std::__detail::_List_node_base"* %7, %"struct.std::__detail::_List_node_base"** %_M_next.i.i.i, align 8, !tbaa !16
  %_M_prev.i.i.i = getelementptr inbounds %"class.std::__cxx11::list", %"class.std::__cxx11::list"* %deps, i64 0, i32 0, i32 0, i32 0, i32 0, i32 1
  store %"struct.std::__detail::_List_node_base"* %7, %"struct.std::__detail::_List_node_base"** %_M_prev.i.i.i, align 8, !tbaa !18
  %_M_storage.i.i.i.i.i = getelementptr inbounds %"class.std::__cxx11::list", %"class.std::__cxx11::list"* %deps, i64 0, i32 0, i32 0, i32 0, i32 1
  %8 = bitcast %"struct.__gnu_cxx::__aligned_membuf"* %_M_storage.i.i.i.i.i to i64*
  store i64 0, i64* %8, align 8, !tbaa !19
  %9 = zext i32 %sortColCount to i64
  %10 = call i8* @llvm.stacksave()
  %vla = alloca i32, i64 %9, align 16
  %cmp1123 = icmp sgt i32 %sortColCount, 0
  br i1 %cmp1123, label %for.body.lr.ph, label %for.cond.cleanup

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
  %13 = load i32, i32* %arrayidx.epil, align 4, !tbaa !2
  %idxprom1.epil = sext i32 %13 to i64
  %arrayidx2.epil = getelementptr inbounds i32, i32* %sourceTypes, i64 %idxprom1.epil
  %14 = load i32, i32* %arrayidx2.epil, align 4, !tbaa !2
  %arrayidx4.epil = getelementptr inbounds i32, i32* %vla, i64 %indvars.iv.epil
  store i32 %14, i32* %arrayidx4.epil, align 4, !tbaa !2
  %indvars.iv.next.epil = add nuw nsw i64 %indvars.iv.epil, 1
  %epil.iter.sub = add i64 %epil.iter, -1
  %epil.iter.cmp.not = icmp eq i64 %epil.iter.sub, 0
  br i1 %epil.iter.cmp.not, label %for.cond.cleanup, label %for.body.epil, !llvm.loop !20

for.cond.cleanup:                                 ; preds = %for.cond.cleanup.loopexit.unr-lcssa, %for.body.epil, %entry
  %15 = bitcast %"class.codegen::ParamValue"* %p_sourceTypes to i8*
  call void @llvm.lifetime.start.p0i8(i64 24, i8* nonnull %15) #22
  %16 = bitcast %"class.codegen::ParamValue"* %p_sourceTypes to i32**
  store i32* %sourceTypes, i32** %16, align 8, !tbaa !22
  %size2.i = getelementptr inbounds %"class.codegen::ParamValue", %"class.codegen::ParamValue"* %p_sourceTypes, i64 0, i32 1
  store i32 %typeCount, i32* %size2.i, align 8, !tbaa !26
  %type.i = getelementptr inbounds %"class.codegen::ParamValue", %"class.codegen::ParamValue"* %p_sourceTypes, i64 0, i32 2
  store i32 0, i32* %type.i, align 4, !tbaa !27
  %vector.i = getelementptr inbounds %"class.codegen::ParamValue", %"class.codegen::ParamValue"* %p_sourceTypes, i64 0, i32 3
  store i8 0, i8* %vector.i, align 8, !tbaa !28
  %17 = bitcast %"class.codegen::ParamValue"* %p_typeCount to i8*
  call void @llvm.lifetime.start.p0i8(i64 24, i8* nonnull %17) #22
  %18 = bitcast %"class.codegen::ParamValue"* %p_typeCount to i32**
  store i32* %typeCount.addr, i32** %18, align 8, !tbaa !22
  %size.i = getelementptr inbounds %"class.codegen::ParamValue", %"class.codegen::ParamValue"* %p_typeCount, i64 0, i32 1
  store i32 1, i32* %size.i, align 8, !tbaa !26
  %type.i427 = getelementptr inbounds %"class.codegen::ParamValue", %"class.codegen::ParamValue"* %p_typeCount, i64 0, i32 2
  store i32 0, i32* %type.i427, align 4, !tbaa !27
  %vector.i428 = getelementptr inbounds %"class.codegen::ParamValue", %"class.codegen::ParamValue"* %p_typeCount, i64 0, i32 3
  store i8 0, i8* %vector.i428, align 8, !tbaa !28
  %19 = bitcast %"class.codegen::ParamValue"* %p_outputCols to i8*
  call void @llvm.lifetime.start.p0i8(i64 24, i8* nonnull %19) #22
  %20 = bitcast %"class.codegen::ParamValue"* %p_outputCols to i32**
  store i32* %outputCols, i32** %20, align 8, !tbaa !22
  %size2.i429 = getelementptr inbounds %"class.codegen::ParamValue", %"class.codegen::ParamValue"* %p_outputCols, i64 0, i32 1
  store i32 %outputColCount, i32* %size2.i429, align 8, !tbaa !26
  %type.i430 = getelementptr inbounds %"class.codegen::ParamValue", %"class.codegen::ParamValue"* %p_outputCols, i64 0, i32 2
  store i32 0, i32* %type.i430, align 4, !tbaa !27
  %vector.i431 = getelementptr inbounds %"class.codegen::ParamValue", %"class.codegen::ParamValue"* %p_outputCols, i64 0, i32 3
  store i8 0, i8* %vector.i431, align 8, !tbaa !28
  %21 = bitcast %"class.codegen::ParamValue"* %p_outputColCount to i8*
  call void @llvm.lifetime.start.p0i8(i64 24, i8* nonnull %21) #22
  %22 = bitcast %"class.codegen::ParamValue"* %p_outputColCount to i32**
  store i32* %outputColCount.addr, i32** %22, align 8, !tbaa !22
  %size.i432 = getelementptr inbounds %"class.codegen::ParamValue", %"class.codegen::ParamValue"* %p_outputColCount, i64 0, i32 1
  store i32 1, i32* %size.i432, align 8, !tbaa !26
  %type.i433 = getelementptr inbounds %"class.codegen::ParamValue", %"class.codegen::ParamValue"* %p_outputColCount, i64 0, i32 2
  store i32 0, i32* %type.i433, align 4, !tbaa !27
  %vector.i434 = getelementptr inbounds %"class.codegen::ParamValue", %"class.codegen::ParamValue"* %p_outputColCount, i64 0, i32 3
  store i8 0, i8* %vector.i434, align 8, !tbaa !28
  %23 = bitcast %"class.codegen::ParamValue"* %p_sortCols to i8*
  call void @llvm.lifetime.start.p0i8(i64 24, i8* nonnull %23) #22
  %24 = bitcast %"class.codegen::ParamValue"* %p_sortCols to i32**
  store i32* %sortCols, i32** %24, align 8, !tbaa !22
  %size2.i435 = getelementptr inbounds %"class.codegen::ParamValue", %"class.codegen::ParamValue"* %p_sortCols, i64 0, i32 1
  store i32 %sortColCount, i32* %size2.i435, align 8, !tbaa !26
  %type.i436 = getelementptr inbounds %"class.codegen::ParamValue", %"class.codegen::ParamValue"* %p_sortCols, i64 0, i32 2
  store i32 0, i32* %type.i436, align 4, !tbaa !27
  %vector.i437 = getelementptr inbounds %"class.codegen::ParamValue", %"class.codegen::ParamValue"* %p_sortCols, i64 0, i32 3
  store i8 0, i8* %vector.i437, align 8, !tbaa !28
  %25 = bitcast %"class.codegen::ParamValue"* %p_sortColTypes to i8*
  call void @llvm.lifetime.start.p0i8(i64 24, i8* nonnull %25) #22
  %26 = bitcast %"class.codegen::ParamValue"* %p_sortColTypes to i32**
  store i32* %vla, i32** %26, align 8, !tbaa !22
  %size2.i438 = getelementptr inbounds %"class.codegen::ParamValue", %"class.codegen::ParamValue"* %p_sortColTypes, i64 0, i32 1
  store i32 %sortColCount, i32* %size2.i438, align 8, !tbaa !26
  %type.i439 = getelementptr inbounds %"class.codegen::ParamValue", %"class.codegen::ParamValue"* %p_sortColTypes, i64 0, i32 2
  store i32 0, i32* %type.i439, align 4, !tbaa !27
  %vector.i440 = getelementptr inbounds %"class.codegen::ParamValue", %"class.codegen::ParamValue"* %p_sortColTypes, i64 0, i32 3
  store i8 0, i8* %vector.i440, align 8, !tbaa !28
  %27 = bitcast %"class.codegen::ParamValue"* %p_sortAscendings to i8*
  call void @llvm.lifetime.start.p0i8(i64 24, i8* nonnull %27) #22
  %28 = bitcast %"class.codegen::ParamValue"* %p_sortAscendings to i32**
  store i32* %sortAscendings, i32** %28, align 8, !tbaa !22
  %size2.i442 = getelementptr inbounds %"class.codegen::ParamValue", %"class.codegen::ParamValue"* %p_sortAscendings, i64 0, i32 1
  store i32 %sortColCount, i32* %size2.i442, align 8, !tbaa !26
  %type.i443 = getelementptr inbounds %"class.codegen::ParamValue", %"class.codegen::ParamValue"* %p_sortAscendings, i64 0, i32 2
  store i32 0, i32* %type.i443, align 4, !tbaa !27
  %vector.i444 = getelementptr inbounds %"class.codegen::ParamValue", %"class.codegen::ParamValue"* %p_sortAscendings, i64 0, i32 3
  store i8 0, i8* %vector.i444, align 8, !tbaa !28
  %29 = bitcast %"class.codegen::ParamValue"* %p_sortNullFirsts to i8*
  call void @llvm.lifetime.start.p0i8(i64 24, i8* nonnull %29) #22
  %30 = bitcast %"class.codegen::ParamValue"* %p_sortNullFirsts to i32**
  store i32* %sortNullFirsts, i32** %30, align 8, !tbaa !22
  %size2.i456 = getelementptr inbounds %"class.codegen::ParamValue", %"class.codegen::ParamValue"* %p_sortNullFirsts, i64 0, i32 1
  store i32 %sortColCount, i32* %size2.i456, align 8, !tbaa !26
  %type.i457 = getelementptr inbounds %"class.codegen::ParamValue", %"class.codegen::ParamValue"* %p_sortNullFirsts, i64 0, i32 2
  store i32 0, i32* %type.i457, align 4, !tbaa !27
  %vector.i458 = getelementptr inbounds %"class.codegen::ParamValue", %"class.codegen::ParamValue"* %p_sortNullFirsts, i64 0, i32 3
  store i8 0, i8* %vector.i458, align 8, !tbaa !28
  %31 = bitcast %"class.codegen::ParamValue"* %p_sortColCount to i8*
  call void @llvm.lifetime.start.p0i8(i64 24, i8* nonnull %31) #22
  %32 = bitcast %"class.codegen::ParamValue"* %p_sortColCount to i32**
  store i32* %sortColCount.addr, i32** %32, align 8, !tbaa !22
  %size.i470 = getelementptr inbounds %"class.codegen::ParamValue", %"class.codegen::ParamValue"* %p_sortColCount, i64 0, i32 1
  store i32 1, i32* %size.i470, align 8, !tbaa !26
  %type.i471 = getelementptr inbounds %"class.codegen::ParamValue", %"class.codegen::ParamValue"* %p_sortColCount, i64 0, i32 2
  store i32 0, i32* %type.i471, align 4, !tbaa !27
  %vector.i472 = getelementptr inbounds %"class.codegen::ParamValue", %"class.codegen::ParamValue"* %p_sortColCount, i64 0, i32 3
  store i8 0, i8* %vector.i472, align 8, !tbaa !28
  %33 = bitcast %"class.std::__cxx11::basic_string"* %ref.tmp to i8*
  call void @llvm.lifetime.start.p0i8(i64 32, i8* nonnull %33) #22
  %34 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp, i64 0, i32 2
  %35 = bitcast %"class.std::__cxx11::basic_string"* %ref.tmp to %union.anon**
  store %union.anon* %34, %union.anon** %35, align 8, !tbaa !29
  %36 = bitcast %union.anon* %34 to i8*
  %37 = bitcast i64* %__dnew.i.i.i.i to i8*
  call void @llvm.lifetime.start.p0i8(i64 8, i8* nonnull %37) #22
  store i64 29, i64* %__dnew.i.i.i.i, align 8, !tbaa !19
  %call5.i.i.i10.i491 = invoke i8* @_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE9_M_createERmm(%"class.std::__cxx11::basic_string"* nonnull dereferenceable(32) %ref.tmp, i64* nonnull align 8 dereferenceable(8) %__dnew.i.i.i.i, i64 0)
          to label %call5.i.i.i10.i.noexc unwind label %lpad22

call5.i.i.i10.i.noexc:                            ; preds = %for.cond.cleanup
  %_M_p.i13.i.i.i.i = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp, i64 0, i32 0, i32 0
  store i8* %call5.i.i.i10.i491, i8** %_M_p.i13.i.i.i.i, align 8, !tbaa !31
  %38 = load i64, i64* %__dnew.i.i.i.i, align 8, !tbaa !19
  %_M_allocated_capacity.i.i.i.i.i = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp, i64 0, i32 2, i32 0
  store i64 %38, i64* %_M_allocated_capacity.i.i.i.i.i, align 8, !tbaa !33
  call void @llvm.memcpy.p0i8.p0i8.i64(i8* nonnull align 1 dereferenceable(29) %call5.i.i.i10.i491, i8* nonnull align 1 dereferenceable(29) getelementptr inbounds ([30 x i8], [30 x i8]* @.str, i64 0, i64 0), i64 29, i1 false) #22
  %_M_string_length.i.i.i.i.i.i = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp, i64 0, i32 1
  store i64 %38, i64* %_M_string_length.i.i.i.i.i.i, align 8, !tbaa !34
  %39 = load i8*, i8** %_M_p.i13.i.i.i.i, align 8, !tbaa !31
  %arrayidx.i.i.i.i.i = getelementptr inbounds i8, i8* %39, i64 %38
  store i8 0, i8* %arrayidx.i.i.i.i.i, align 1, !tbaa !33
  call void @llvm.lifetime.end.p0i8(i64 8, i8* nonnull %37) #22
  %call = invoke nonnull align 8 dereferenceable(8) %"class.codegen::ParamValue"** @_ZNSt3mapINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEPN7codegen10ParamValueESt4lessIS5_ESaISt4pairIKS5_S8_EEEixEOS5_(%"class.std::map"* nonnull dereferenceable(48) %testParam, %"class.std::__cxx11::basic_string"* nonnull align 8 dereferenceable(32) %ref.tmp)
          to label %invoke.cont25 unwind label %lpad24

for.body:                                         ; preds = %for.body, %for.body.lr.ph.new
  %indvars.iv = phi i64 [ 0, %for.body.lr.ph.new ], [ %indvars.iv.next.3, %for.body ]
  %niter = phi i64 [ %unroll_iter, %for.body.lr.ph.new ], [ %niter.nsub.3, %for.body ]
  %arrayidx = getelementptr inbounds i32, i32* %sortCols, i64 %indvars.iv
  %40 = load i32, i32* %arrayidx, align 4, !tbaa !2
  %idxprom1 = sext i32 %40 to i64
  %arrayidx2 = getelementptr inbounds i32, i32* %sourceTypes, i64 %idxprom1
  %41 = load i32, i32* %arrayidx2, align 4, !tbaa !2
  %arrayidx4 = getelementptr inbounds i32, i32* %vla, i64 %indvars.iv
  store i32 %41, i32* %arrayidx4, align 16, !tbaa !2
  %indvars.iv.next = or i64 %indvars.iv, 1
  %arrayidx.1 = getelementptr inbounds i32, i32* %sortCols, i64 %indvars.iv.next
  %42 = load i32, i32* %arrayidx.1, align 4, !tbaa !2
  %idxprom1.1 = sext i32 %42 to i64
  %arrayidx2.1 = getelementptr inbounds i32, i32* %sourceTypes, i64 %idxprom1.1
  %43 = load i32, i32* %arrayidx2.1, align 4, !tbaa !2
  %arrayidx4.1 = getelementptr inbounds i32, i32* %vla, i64 %indvars.iv.next
  store i32 %43, i32* %arrayidx4.1, align 4, !tbaa !2
  %indvars.iv.next.1 = or i64 %indvars.iv, 2
  %arrayidx.2 = getelementptr inbounds i32, i32* %sortCols, i64 %indvars.iv.next.1
  %44 = load i32, i32* %arrayidx.2, align 4, !tbaa !2
  %idxprom1.2 = sext i32 %44 to i64
  %arrayidx2.2 = getelementptr inbounds i32, i32* %sourceTypes, i64 %idxprom1.2
  %45 = load i32, i32* %arrayidx2.2, align 4, !tbaa !2
  %arrayidx4.2 = getelementptr inbounds i32, i32* %vla, i64 %indvars.iv.next.1
  store i32 %45, i32* %arrayidx4.2, align 8, !tbaa !2
  %indvars.iv.next.2 = or i64 %indvars.iv, 3
  %arrayidx.3 = getelementptr inbounds i32, i32* %sortCols, i64 %indvars.iv.next.2
  %46 = load i32, i32* %arrayidx.3, align 4, !tbaa !2
  %idxprom1.3 = sext i32 %46 to i64
  %arrayidx2.3 = getelementptr inbounds i32, i32* %sourceTypes, i64 %idxprom1.3
  %47 = load i32, i32* %arrayidx2.3, align 4, !tbaa !2
  %arrayidx4.3 = getelementptr inbounds i32, i32* %vla, i64 %indvars.iv.next.2
  store i32 %47, i32* %arrayidx4.3, align 4, !tbaa !2
  %indvars.iv.next.3 = add nuw nsw i64 %indvars.iv, 4
  %niter.nsub.3 = add i64 %niter, -4
  %niter.ncmp.3 = icmp eq i64 %niter.nsub.3, 0
  br i1 %niter.ncmp.3, label %for.cond.cleanup.loopexit.unr-lcssa, label %for.body, !llvm.loop !35

invoke.cont25:                                    ; preds = %call5.i.i.i10.i.noexc
  store %"class.codegen::ParamValue"* %p_sourceTypes, %"class.codegen::ParamValue"** %call, align 8, !tbaa !37
  %48 = load i8*, i8** %_M_p.i13.i.i.i.i, align 8, !tbaa !31
  %cmp.i.i.i = icmp eq i8* %48, %36
  br i1 %cmp.i.i.i, label %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit, label %if.then.i.i

if.then.i.i:                                      ; preds = %invoke.cont25
  call void @_ZdlPv(i8* %48) #22
  br label %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit

_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit: ; preds = %invoke.cont25, %if.then.i.i
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %33) #22
  %49 = bitcast %"class.std::__cxx11::basic_string"* %ref.tmp28 to i8*
  call void @llvm.lifetime.start.p0i8(i64 32, i8* nonnull %49) #22
  %50 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp28, i64 0, i32 2
  %51 = bitcast %"class.std::__cxx11::basic_string"* %ref.tmp28 to %union.anon**
  store %union.anon* %50, %union.anon** %51, align 8, !tbaa !29
  %52 = bitcast %union.anon* %50 to i8*
  %53 = bitcast i64* %__dnew.i.i.i.i496 to i8*
  call void @llvm.lifetime.start.p0i8(i64 8, i8* nonnull %53) #22
  store i64 29, i64* %__dnew.i.i.i.i496, align 8, !tbaa !19
  %call5.i.i.i10.i509 = invoke i8* @_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE9_M_createERmm(%"class.std::__cxx11::basic_string"* nonnull dereferenceable(32) %ref.tmp28, i64* nonnull align 8 dereferenceable(8) %__dnew.i.i.i.i496, i64 0)
          to label %call5.i.i.i10.i.noexc508 unwind label %lpad30

call5.i.i.i10.i.noexc508:                         ; preds = %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit
  %_M_p.i13.i.i.i.i499 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp28, i64 0, i32 0, i32 0
  store i8* %call5.i.i.i10.i509, i8** %_M_p.i13.i.i.i.i499, align 8, !tbaa !31
  %54 = load i64, i64* %__dnew.i.i.i.i496, align 8, !tbaa !19
  %_M_allocated_capacity.i.i.i.i.i500 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp28, i64 0, i32 2, i32 0
  store i64 %54, i64* %_M_allocated_capacity.i.i.i.i.i500, align 8, !tbaa !33
  call void @llvm.memcpy.p0i8.p0i8.i64(i8* nonnull align 1 dereferenceable(29) %call5.i.i.i10.i509, i8* nonnull align 1 dereferenceable(29) getelementptr inbounds ([30 x i8], [30 x i8]* @.str.1, i64 0, i64 0), i64 29, i1 false) #22
  %_M_string_length.i.i.i.i.i.i506 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp28, i64 0, i32 1
  store i64 %54, i64* %_M_string_length.i.i.i.i.i.i506, align 8, !tbaa !34
  %55 = load i8*, i8** %_M_p.i13.i.i.i.i499, align 8, !tbaa !31
  %arrayidx.i.i.i.i.i507 = getelementptr inbounds i8, i8* %55, i64 %54
  store i8 0, i8* %arrayidx.i.i.i.i.i507, align 1, !tbaa !33
  call void @llvm.lifetime.end.p0i8(i64 8, i8* nonnull %53) #22
  %call34 = invoke nonnull align 8 dereferenceable(8) %"class.codegen::ParamValue"** @_ZNSt3mapINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEPN7codegen10ParamValueESt4lessIS5_ESaISt4pairIKS5_S8_EEEixEOS5_(%"class.std::map"* nonnull dereferenceable(48) %testParam, %"class.std::__cxx11::basic_string"* nonnull align 8 dereferenceable(32) %ref.tmp28)
          to label %invoke.cont33 unwind label %lpad32

invoke.cont33:                                    ; preds = %call5.i.i.i10.i.noexc508
  store %"class.codegen::ParamValue"* %p_typeCount, %"class.codegen::ParamValue"** %call34, align 8, !tbaa !37
  %56 = load i8*, i8** %_M_p.i13.i.i.i.i499, align 8, !tbaa !31
  %cmp.i.i.i513 = icmp eq i8* %56, %52
  br i1 %cmp.i.i.i513, label %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit515, label %if.then.i.i514

if.then.i.i514:                                   ; preds = %invoke.cont33
  call void @_ZdlPv(i8* %56) #22
  br label %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit515

_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit515: ; preds = %invoke.cont33, %if.then.i.i514
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %49) #22
  %57 = bitcast %"class.std::__cxx11::basic_string"* %ref.tmp39 to i8*
  call void @llvm.lifetime.start.p0i8(i64 32, i8* nonnull %57) #22
  %58 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp39, i64 0, i32 2
  %59 = bitcast %"class.std::__cxx11::basic_string"* %ref.tmp39 to %union.anon**
  store %union.anon* %58, %union.anon** %59, align 8, !tbaa !29
  %60 = bitcast %union.anon* %58 to i8*
  %61 = bitcast i64* %__dnew.i.i.i.i516 to i8*
  call void @llvm.lifetime.start.p0i8(i64 8, i8* nonnull %61) #22
  store i64 29, i64* %__dnew.i.i.i.i516, align 8, !tbaa !19
  %call5.i.i.i10.i529 = invoke i8* @_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE9_M_createERmm(%"class.std::__cxx11::basic_string"* nonnull dereferenceable(32) %ref.tmp39, i64* nonnull align 8 dereferenceable(8) %__dnew.i.i.i.i516, i64 0)
          to label %call5.i.i.i10.i.noexc528 unwind label %lpad41

call5.i.i.i10.i.noexc528:                         ; preds = %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit515
  %_M_p.i13.i.i.i.i519 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp39, i64 0, i32 0, i32 0
  store i8* %call5.i.i.i10.i529, i8** %_M_p.i13.i.i.i.i519, align 8, !tbaa !31
  %62 = load i64, i64* %__dnew.i.i.i.i516, align 8, !tbaa !19
  %_M_allocated_capacity.i.i.i.i.i520 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp39, i64 0, i32 2, i32 0
  store i64 %62, i64* %_M_allocated_capacity.i.i.i.i.i520, align 8, !tbaa !33
  call void @llvm.memcpy.p0i8.p0i8.i64(i8* nonnull align 1 dereferenceable(29) %call5.i.i.i10.i529, i8* nonnull align 1 dereferenceable(29) getelementptr inbounds ([30 x i8], [30 x i8]* @.str.2, i64 0, i64 0), i64 29, i1 false) #22
  %_M_string_length.i.i.i.i.i.i526 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp39, i64 0, i32 1
  store i64 %62, i64* %_M_string_length.i.i.i.i.i.i526, align 8, !tbaa !34
  %63 = load i8*, i8** %_M_p.i13.i.i.i.i519, align 8, !tbaa !31
  %arrayidx.i.i.i.i.i527 = getelementptr inbounds i8, i8* %63, i64 %62
  store i8 0, i8* %arrayidx.i.i.i.i.i527, align 1, !tbaa !33
  call void @llvm.lifetime.end.p0i8(i64 8, i8* nonnull %61) #22
  %call45 = invoke nonnull align 8 dereferenceable(8) %"class.codegen::ParamValue"** @_ZNSt3mapINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEPN7codegen10ParamValueESt4lessIS5_ESaISt4pairIKS5_S8_EEEixEOS5_(%"class.std::map"* nonnull dereferenceable(48) %testParam, %"class.std::__cxx11::basic_string"* nonnull align 8 dereferenceable(32) %ref.tmp39)
          to label %invoke.cont44 unwind label %lpad43

invoke.cont44:                                    ; preds = %call5.i.i.i10.i.noexc528
  store %"class.codegen::ParamValue"* %p_outputCols, %"class.codegen::ParamValue"** %call45, align 8, !tbaa !37
  %64 = load i8*, i8** %_M_p.i13.i.i.i.i519, align 8, !tbaa !31
  %cmp.i.i.i533 = icmp eq i8* %64, %60
  br i1 %cmp.i.i.i533, label %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit535, label %if.then.i.i534

if.then.i.i534:                                   ; preds = %invoke.cont44
  call void @_ZdlPv(i8* %64) #22
  br label %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit535

_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit535: ; preds = %invoke.cont44, %if.then.i.i534
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %57) #22
  %65 = bitcast %"class.std::__cxx11::basic_string"* %ref.tmp50 to i8*
  call void @llvm.lifetime.start.p0i8(i64 32, i8* nonnull %65) #22
  %66 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp50, i64 0, i32 2
  %67 = bitcast %"class.std::__cxx11::basic_string"* %ref.tmp50 to %union.anon**
  store %union.anon* %66, %union.anon** %67, align 8, !tbaa !29
  %68 = bitcast %union.anon* %66 to i8*
  %69 = bitcast i64* %__dnew.i.i.i.i540 to i8*
  call void @llvm.lifetime.start.p0i8(i64 8, i8* nonnull %69) #22
  store i64 29, i64* %__dnew.i.i.i.i540, align 8, !tbaa !19
  %call5.i.i.i10.i553 = invoke i8* @_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE9_M_createERmm(%"class.std::__cxx11::basic_string"* nonnull dereferenceable(32) %ref.tmp50, i64* nonnull align 8 dereferenceable(8) %__dnew.i.i.i.i540, i64 0)
          to label %call5.i.i.i10.i.noexc552 unwind label %lpad52

call5.i.i.i10.i.noexc552:                         ; preds = %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit535
  %_M_p.i13.i.i.i.i543 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp50, i64 0, i32 0, i32 0
  store i8* %call5.i.i.i10.i553, i8** %_M_p.i13.i.i.i.i543, align 8, !tbaa !31
  %70 = load i64, i64* %__dnew.i.i.i.i540, align 8, !tbaa !19
  %_M_allocated_capacity.i.i.i.i.i544 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp50, i64 0, i32 2, i32 0
  store i64 %70, i64* %_M_allocated_capacity.i.i.i.i.i544, align 8, !tbaa !33
  call void @llvm.memcpy.p0i8.p0i8.i64(i8* nonnull align 1 dereferenceable(29) %call5.i.i.i10.i553, i8* nonnull align 1 dereferenceable(29) getelementptr inbounds ([30 x i8], [30 x i8]* @.str.3, i64 0, i64 0), i64 29, i1 false) #22
  %_M_string_length.i.i.i.i.i.i550 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp50, i64 0, i32 1
  store i64 %70, i64* %_M_string_length.i.i.i.i.i.i550, align 8, !tbaa !34
  %71 = load i8*, i8** %_M_p.i13.i.i.i.i543, align 8, !tbaa !31
  %arrayidx.i.i.i.i.i551 = getelementptr inbounds i8, i8* %71, i64 %70
  store i8 0, i8* %arrayidx.i.i.i.i.i551, align 1, !tbaa !33
  call void @llvm.lifetime.end.p0i8(i64 8, i8* nonnull %69) #22
  %call56 = invoke nonnull align 8 dereferenceable(8) %"class.codegen::ParamValue"** @_ZNSt3mapINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEPN7codegen10ParamValueESt4lessIS5_ESaISt4pairIKS5_S8_EEEixEOS5_(%"class.std::map"* nonnull dereferenceable(48) %testParam, %"class.std::__cxx11::basic_string"* nonnull align 8 dereferenceable(32) %ref.tmp50)
          to label %invoke.cont55 unwind label %lpad54

invoke.cont55:                                    ; preds = %call5.i.i.i10.i.noexc552
  store %"class.codegen::ParamValue"* %p_outputColCount, %"class.codegen::ParamValue"** %call56, align 8, !tbaa !37
  %72 = load i8*, i8** %_M_p.i13.i.i.i.i543, align 8, !tbaa !31
  %cmp.i.i.i557 = icmp eq i8* %72, %68
  br i1 %cmp.i.i.i557, label %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit559, label %if.then.i.i558

if.then.i.i558:                                   ; preds = %invoke.cont55
  call void @_ZdlPv(i8* %72) #22
  br label %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit559

_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit559: ; preds = %invoke.cont55, %if.then.i.i558
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %65) #22
  %73 = bitcast %"class.std::__cxx11::basic_string"* %ref.tmp61 to i8*
  call void @llvm.lifetime.start.p0i8(i64 32, i8* nonnull %73) #22
  %74 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp61, i64 0, i32 2
  %75 = bitcast %"class.std::__cxx11::basic_string"* %ref.tmp61 to %union.anon**
  store %union.anon* %74, %union.anon** %75, align 8, !tbaa !29
  %76 = bitcast %union.anon* %74 to i8*
  %77 = bitcast i64* %__dnew.i.i.i.i560 to i8*
  call void @llvm.lifetime.start.p0i8(i64 8, i8* nonnull %77) #22
  store i64 29, i64* %__dnew.i.i.i.i560, align 8, !tbaa !19
  %call5.i.i.i10.i573 = invoke i8* @_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE9_M_createERmm(%"class.std::__cxx11::basic_string"* nonnull dereferenceable(32) %ref.tmp61, i64* nonnull align 8 dereferenceable(8) %__dnew.i.i.i.i560, i64 0)
          to label %call5.i.i.i10.i.noexc572 unwind label %lpad63

call5.i.i.i10.i.noexc572:                         ; preds = %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit559
  %_M_p.i13.i.i.i.i563 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp61, i64 0, i32 0, i32 0
  store i8* %call5.i.i.i10.i573, i8** %_M_p.i13.i.i.i.i563, align 8, !tbaa !31
  %78 = load i64, i64* %__dnew.i.i.i.i560, align 8, !tbaa !19
  %_M_allocated_capacity.i.i.i.i.i564 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp61, i64 0, i32 2, i32 0
  store i64 %78, i64* %_M_allocated_capacity.i.i.i.i.i564, align 8, !tbaa !33
  call void @llvm.memcpy.p0i8.p0i8.i64(i8* nonnull align 1 dereferenceable(29) %call5.i.i.i10.i573, i8* nonnull align 1 dereferenceable(29) getelementptr inbounds ([30 x i8], [30 x i8]* @.str.4, i64 0, i64 0), i64 29, i1 false) #22
  %_M_string_length.i.i.i.i.i.i570 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp61, i64 0, i32 1
  store i64 %78, i64* %_M_string_length.i.i.i.i.i.i570, align 8, !tbaa !34
  %79 = load i8*, i8** %_M_p.i13.i.i.i.i563, align 8, !tbaa !31
  %arrayidx.i.i.i.i.i571 = getelementptr inbounds i8, i8* %79, i64 %78
  store i8 0, i8* %arrayidx.i.i.i.i.i571, align 1, !tbaa !33
  call void @llvm.lifetime.end.p0i8(i64 8, i8* nonnull %77) #22
  %call67 = invoke nonnull align 8 dereferenceable(8) %"class.codegen::ParamValue"** @_ZNSt3mapINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEPN7codegen10ParamValueESt4lessIS5_ESaISt4pairIKS5_S8_EEEixEOS5_(%"class.std::map"* nonnull dereferenceable(48) %testParam, %"class.std::__cxx11::basic_string"* nonnull align 8 dereferenceable(32) %ref.tmp61)
          to label %invoke.cont66 unwind label %lpad65

invoke.cont66:                                    ; preds = %call5.i.i.i10.i.noexc572
  store %"class.codegen::ParamValue"* %p_sortCols, %"class.codegen::ParamValue"** %call67, align 8, !tbaa !37
  %80 = load i8*, i8** %_M_p.i13.i.i.i.i563, align 8, !tbaa !31
  %cmp.i.i.i577 = icmp eq i8* %80, %76
  br i1 %cmp.i.i.i577, label %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit579, label %if.then.i.i578

if.then.i.i578:                                   ; preds = %invoke.cont66
  call void @_ZdlPv(i8* %80) #22
  br label %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit579

_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit579: ; preds = %invoke.cont66, %if.then.i.i578
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %73) #22
  %81 = bitcast %"class.std::__cxx11::basic_string"* %ref.tmp72 to i8*
  call void @llvm.lifetime.start.p0i8(i64 32, i8* nonnull %81) #22
  %82 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp72, i64 0, i32 2
  %83 = bitcast %"class.std::__cxx11::basic_string"* %ref.tmp72 to %union.anon**
  store %union.anon* %82, %union.anon** %83, align 8, !tbaa !29
  %84 = bitcast %union.anon* %82 to i8*
  %85 = bitcast i64* %__dnew.i.i.i.i585 to i8*
  call void @llvm.lifetime.start.p0i8(i64 8, i8* nonnull %85) #22
  store i64 29, i64* %__dnew.i.i.i.i585, align 8, !tbaa !19
  %call5.i.i.i10.i598 = invoke i8* @_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE9_M_createERmm(%"class.std::__cxx11::basic_string"* nonnull dereferenceable(32) %ref.tmp72, i64* nonnull align 8 dereferenceable(8) %__dnew.i.i.i.i585, i64 0)
          to label %call5.i.i.i10.i.noexc597 unwind label %lpad74

call5.i.i.i10.i.noexc597:                         ; preds = %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit579
  %_M_p.i13.i.i.i.i588 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp72, i64 0, i32 0, i32 0
  store i8* %call5.i.i.i10.i598, i8** %_M_p.i13.i.i.i.i588, align 8, !tbaa !31
  %86 = load i64, i64* %__dnew.i.i.i.i585, align 8, !tbaa !19
  %_M_allocated_capacity.i.i.i.i.i589 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp72, i64 0, i32 2, i32 0
  store i64 %86, i64* %_M_allocated_capacity.i.i.i.i.i589, align 8, !tbaa !33
  call void @llvm.memcpy.p0i8.p0i8.i64(i8* nonnull align 1 dereferenceable(29) %call5.i.i.i10.i598, i8* nonnull align 1 dereferenceable(29) getelementptr inbounds ([30 x i8], [30 x i8]* @.str.5, i64 0, i64 0), i64 29, i1 false) #22
  %_M_string_length.i.i.i.i.i.i595 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp72, i64 0, i32 1
  store i64 %86, i64* %_M_string_length.i.i.i.i.i.i595, align 8, !tbaa !34
  %87 = load i8*, i8** %_M_p.i13.i.i.i.i588, align 8, !tbaa !31
  %arrayidx.i.i.i.i.i596 = getelementptr inbounds i8, i8* %87, i64 %86
  store i8 0, i8* %arrayidx.i.i.i.i.i596, align 1, !tbaa !33
  call void @llvm.lifetime.end.p0i8(i64 8, i8* nonnull %85) #22
  %call78 = invoke nonnull align 8 dereferenceable(8) %"class.codegen::ParamValue"** @_ZNSt3mapINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEPN7codegen10ParamValueESt4lessIS5_ESaISt4pairIKS5_S8_EEEixEOS5_(%"class.std::map"* nonnull dereferenceable(48) %testParam, %"class.std::__cxx11::basic_string"* nonnull align 8 dereferenceable(32) %ref.tmp72)
          to label %invoke.cont77 unwind label %lpad76

invoke.cont77:                                    ; preds = %call5.i.i.i10.i.noexc597
  store %"class.codegen::ParamValue"* %p_sortAscendings, %"class.codegen::ParamValue"** %call78, align 8, !tbaa !37
  %88 = load i8*, i8** %_M_p.i13.i.i.i.i588, align 8, !tbaa !31
  %cmp.i.i.i602 = icmp eq i8* %88, %84
  br i1 %cmp.i.i.i602, label %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit604, label %if.then.i.i603

if.then.i.i603:                                   ; preds = %invoke.cont77
  call void @_ZdlPv(i8* %88) #22
  br label %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit604

_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit604: ; preds = %invoke.cont77, %if.then.i.i603
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %81) #22
  %89 = bitcast %"class.std::__cxx11::basic_string"* %ref.tmp83 to i8*
  call void @llvm.lifetime.start.p0i8(i64 32, i8* nonnull %89) #22
  %90 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp83, i64 0, i32 2
  %91 = bitcast %"class.std::__cxx11::basic_string"* %ref.tmp83 to %union.anon**
  store %union.anon* %90, %union.anon** %91, align 8, !tbaa !29
  %92 = bitcast %union.anon* %90 to i8*
  %93 = bitcast i64* %__dnew.i.i.i.i610 to i8*
  call void @llvm.lifetime.start.p0i8(i64 8, i8* nonnull %93) #22
  store i64 29, i64* %__dnew.i.i.i.i610, align 8, !tbaa !19
  %call5.i.i.i10.i623 = invoke i8* @_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE9_M_createERmm(%"class.std::__cxx11::basic_string"* nonnull dereferenceable(32) %ref.tmp83, i64* nonnull align 8 dereferenceable(8) %__dnew.i.i.i.i610, i64 0)
          to label %call5.i.i.i10.i.noexc622 unwind label %lpad85

call5.i.i.i10.i.noexc622:                         ; preds = %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit604
  %_M_p.i13.i.i.i.i613 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp83, i64 0, i32 0, i32 0
  store i8* %call5.i.i.i10.i623, i8** %_M_p.i13.i.i.i.i613, align 8, !tbaa !31
  %94 = load i64, i64* %__dnew.i.i.i.i610, align 8, !tbaa !19
  %_M_allocated_capacity.i.i.i.i.i614 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp83, i64 0, i32 2, i32 0
  store i64 %94, i64* %_M_allocated_capacity.i.i.i.i.i614, align 8, !tbaa !33
  call void @llvm.memcpy.p0i8.p0i8.i64(i8* nonnull align 1 dereferenceable(29) %call5.i.i.i10.i623, i8* nonnull align 1 dereferenceable(29) getelementptr inbounds ([30 x i8], [30 x i8]* @.str.6, i64 0, i64 0), i64 29, i1 false) #22
  %_M_string_length.i.i.i.i.i.i620 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp83, i64 0, i32 1
  store i64 %94, i64* %_M_string_length.i.i.i.i.i.i620, align 8, !tbaa !34
  %95 = load i8*, i8** %_M_p.i13.i.i.i.i613, align 8, !tbaa !31
  %arrayidx.i.i.i.i.i621 = getelementptr inbounds i8, i8* %95, i64 %94
  store i8 0, i8* %arrayidx.i.i.i.i.i621, align 1, !tbaa !33
  call void @llvm.lifetime.end.p0i8(i64 8, i8* nonnull %93) #22
  %call89 = invoke nonnull align 8 dereferenceable(8) %"class.codegen::ParamValue"** @_ZNSt3mapINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEPN7codegen10ParamValueESt4lessIS5_ESaISt4pairIKS5_S8_EEEixEOS5_(%"class.std::map"* nonnull dereferenceable(48) %testParam, %"class.std::__cxx11::basic_string"* nonnull align 8 dereferenceable(32) %ref.tmp83)
          to label %invoke.cont88 unwind label %lpad87

invoke.cont88:                                    ; preds = %call5.i.i.i10.i.noexc622
  store %"class.codegen::ParamValue"* %p_sortNullFirsts, %"class.codegen::ParamValue"** %call89, align 8, !tbaa !37
  %96 = load i8*, i8** %_M_p.i13.i.i.i.i613, align 8, !tbaa !31
  %cmp.i.i.i627 = icmp eq i8* %96, %92
  br i1 %cmp.i.i.i627, label %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit629, label %if.then.i.i628

if.then.i.i628:                                   ; preds = %invoke.cont88
  call void @_ZdlPv(i8* %96) #22
  br label %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit629

_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit629: ; preds = %invoke.cont88, %if.then.i.i628
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %89) #22
  %97 = bitcast %"class.std::__cxx11::basic_string"* %ref.tmp94 to i8*
  call void @llvm.lifetime.start.p0i8(i64 32, i8* nonnull %97) #22
  %98 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp94, i64 0, i32 2
  %99 = bitcast %"class.std::__cxx11::basic_string"* %ref.tmp94 to %union.anon**
  store %union.anon* %98, %union.anon** %99, align 8, !tbaa !29
  %100 = bitcast %union.anon* %98 to i8*
  %101 = bitcast i64* %__dnew.i.i.i.i635 to i8*
  call void @llvm.lifetime.start.p0i8(i64 8, i8* nonnull %101) #22
  store i64 29, i64* %__dnew.i.i.i.i635, align 8, !tbaa !19
  %call5.i.i.i10.i648 = invoke i8* @_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE9_M_createERmm(%"class.std::__cxx11::basic_string"* nonnull dereferenceable(32) %ref.tmp94, i64* nonnull align 8 dereferenceable(8) %__dnew.i.i.i.i635, i64 0)
          to label %call5.i.i.i10.i.noexc647 unwind label %lpad96

call5.i.i.i10.i.noexc647:                         ; preds = %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit629
  %_M_p.i13.i.i.i.i638 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp94, i64 0, i32 0, i32 0
  store i8* %call5.i.i.i10.i648, i8** %_M_p.i13.i.i.i.i638, align 8, !tbaa !31
  %102 = load i64, i64* %__dnew.i.i.i.i635, align 8, !tbaa !19
  %_M_allocated_capacity.i.i.i.i.i639 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp94, i64 0, i32 2, i32 0
  store i64 %102, i64* %_M_allocated_capacity.i.i.i.i.i639, align 8, !tbaa !33
  call void @llvm.memcpy.p0i8.p0i8.i64(i8* nonnull align 1 dereferenceable(29) %call5.i.i.i10.i648, i8* nonnull align 1 dereferenceable(29) getelementptr inbounds ([30 x i8], [30 x i8]* @.str.7, i64 0, i64 0), i64 29, i1 false) #22
  %_M_string_length.i.i.i.i.i.i645 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp94, i64 0, i32 1
  store i64 %102, i64* %_M_string_length.i.i.i.i.i.i645, align 8, !tbaa !34
  %103 = load i8*, i8** %_M_p.i13.i.i.i.i638, align 8, !tbaa !31
  %arrayidx.i.i.i.i.i646 = getelementptr inbounds i8, i8* %103, i64 %102
  store i8 0, i8* %arrayidx.i.i.i.i.i646, align 1, !tbaa !33
  call void @llvm.lifetime.end.p0i8(i64 8, i8* nonnull %101) #22
  %call100 = invoke nonnull align 8 dereferenceable(8) %"class.codegen::ParamValue"** @_ZNSt3mapINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEPN7codegen10ParamValueESt4lessIS5_ESaISt4pairIKS5_S8_EEEixEOS5_(%"class.std::map"* nonnull dereferenceable(48) %testParam, %"class.std::__cxx11::basic_string"* nonnull align 8 dereferenceable(32) %ref.tmp94)
          to label %invoke.cont99 unwind label %lpad98

invoke.cont99:                                    ; preds = %call5.i.i.i10.i.noexc647
  store %"class.codegen::ParamValue"* %p_sortColCount, %"class.codegen::ParamValue"** %call100, align 8, !tbaa !37
  %104 = load i8*, i8** %_M_p.i13.i.i.i.i638, align 8, !tbaa !31
  %cmp.i.i.i652 = icmp eq i8* %104, %100
  br i1 %cmp.i.i.i652, label %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit654, label %if.then.i.i653

if.then.i.i653:                                   ; preds = %invoke.cont99
  call void @_ZdlPv(i8* %104) #22
  br label %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit654

_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit654: ; preds = %invoke.cont99, %if.then.i.i653
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %97) #22
  %105 = bitcast %"class.std::__cxx11::basic_string"* %ref.tmp105 to i8*
  call void @llvm.lifetime.start.p0i8(i64 32, i8* nonnull %105) #22
  %106 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp105, i64 0, i32 2
  %107 = bitcast %"class.std::__cxx11::basic_string"* %ref.tmp105 to %union.anon**
  store %union.anon* %106, %union.anon** %107, align 8, !tbaa !29
  %108 = bitcast %union.anon* %106 to i8*
  %109 = bitcast i64* %__dnew.i.i.i.i660 to i8*
  call void @llvm.lifetime.start.p0i8(i64 8, i8* nonnull %109) #22
  store i64 26, i64* %__dnew.i.i.i.i660, align 8, !tbaa !19
  %call5.i.i.i10.i673 = invoke i8* @_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE9_M_createERmm(%"class.std::__cxx11::basic_string"* nonnull dereferenceable(32) %ref.tmp105, i64* nonnull align 8 dereferenceable(8) %__dnew.i.i.i.i660, i64 0)
          to label %call5.i.i.i10.i.noexc672 unwind label %lpad107

call5.i.i.i10.i.noexc672:                         ; preds = %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit654
  %_M_p.i13.i.i.i.i663 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp105, i64 0, i32 0, i32 0
  store i8* %call5.i.i.i10.i673, i8** %_M_p.i13.i.i.i.i663, align 8, !tbaa !31
  %110 = load i64, i64* %__dnew.i.i.i.i660, align 8, !tbaa !19
  %_M_allocated_capacity.i.i.i.i.i664 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp105, i64 0, i32 2, i32 0
  store i64 %110, i64* %_M_allocated_capacity.i.i.i.i.i664, align 8, !tbaa !33
  call void @llvm.memcpy.p0i8.p0i8.i64(i8* nonnull align 1 dereferenceable(26) %call5.i.i.i10.i673, i8* nonnull align 1 dereferenceable(26) getelementptr inbounds ([27 x i8], [27 x i8]* @.str.8, i64 0, i64 0), i64 26, i1 false) #22
  %_M_string_length.i.i.i.i.i.i670 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp105, i64 0, i32 1
  store i64 %110, i64* %_M_string_length.i.i.i.i.i.i670, align 8, !tbaa !34
  %111 = load i8*, i8** %_M_p.i13.i.i.i.i663, align 8, !tbaa !31
  %arrayidx.i.i.i.i.i671 = getelementptr inbounds i8, i8* %111, i64 %110
  store i8 0, i8* %arrayidx.i.i.i.i.i671, align 1, !tbaa !33
  call void @llvm.lifetime.end.p0i8(i64 8, i8* nonnull %109) #22
  %call111 = invoke nonnull align 8 dereferenceable(8) %"class.codegen::ParamValue"** @_ZNSt3mapINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEPN7codegen10ParamValueESt4lessIS5_ESaISt4pairIKS5_S8_EEEixEOS5_(%"class.std::map"* nonnull dereferenceable(48) %testParam, %"class.std::__cxx11::basic_string"* nonnull align 8 dereferenceable(32) %ref.tmp105)
          to label %invoke.cont110 unwind label %lpad109

invoke.cont110:                                   ; preds = %call5.i.i.i10.i.noexc672
  store %"class.codegen::ParamValue"* %p_sortCols, %"class.codegen::ParamValue"** %call111, align 8, !tbaa !37
  %112 = load i8*, i8** %_M_p.i13.i.i.i.i663, align 8, !tbaa !31
  %cmp.i.i.i677 = icmp eq i8* %112, %108
  br i1 %cmp.i.i.i677, label %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit679, label %if.then.i.i678

if.then.i.i678:                                   ; preds = %invoke.cont110
  call void @_ZdlPv(i8* %112) #22
  br label %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit679

_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit679: ; preds = %invoke.cont110, %if.then.i.i678
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %105) #22
  %113 = bitcast %"class.std::__cxx11::basic_string"* %ref.tmp116 to i8*
  call void @llvm.lifetime.start.p0i8(i64 32, i8* nonnull %113) #22
  %114 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp116, i64 0, i32 2
  %115 = bitcast %"class.std::__cxx11::basic_string"* %ref.tmp116 to %union.anon**
  store %union.anon* %114, %union.anon** %115, align 8, !tbaa !29
  %116 = bitcast %union.anon* %114 to i8*
  %117 = bitcast i64* %__dnew.i.i.i.i685 to i8*
  call void @llvm.lifetime.start.p0i8(i64 8, i8* nonnull %117) #22
  store i64 26, i64* %__dnew.i.i.i.i685, align 8, !tbaa !19
  %call5.i.i.i10.i698 = invoke i8* @_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE9_M_createERmm(%"class.std::__cxx11::basic_string"* nonnull dereferenceable(32) %ref.tmp116, i64* nonnull align 8 dereferenceable(8) %__dnew.i.i.i.i685, i64 0)
          to label %call5.i.i.i10.i.noexc697 unwind label %lpad118

call5.i.i.i10.i.noexc697:                         ; preds = %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit679
  %_M_p.i13.i.i.i.i688 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp116, i64 0, i32 0, i32 0
  store i8* %call5.i.i.i10.i698, i8** %_M_p.i13.i.i.i.i688, align 8, !tbaa !31
  %118 = load i64, i64* %__dnew.i.i.i.i685, align 8, !tbaa !19
  %_M_allocated_capacity.i.i.i.i.i689 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp116, i64 0, i32 2, i32 0
  store i64 %118, i64* %_M_allocated_capacity.i.i.i.i.i689, align 8, !tbaa !33
  call void @llvm.memcpy.p0i8.p0i8.i64(i8* nonnull align 1 dereferenceable(26) %call5.i.i.i10.i698, i8* nonnull align 1 dereferenceable(26) getelementptr inbounds ([27 x i8], [27 x i8]* @.str.9, i64 0, i64 0), i64 26, i1 false) #22
  %_M_string_length.i.i.i.i.i.i695 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp116, i64 0, i32 1
  store i64 %118, i64* %_M_string_length.i.i.i.i.i.i695, align 8, !tbaa !34
  %119 = load i8*, i8** %_M_p.i13.i.i.i.i688, align 8, !tbaa !31
  %arrayidx.i.i.i.i.i696 = getelementptr inbounds i8, i8* %119, i64 %118
  store i8 0, i8* %arrayidx.i.i.i.i.i696, align 1, !tbaa !33
  call void @llvm.lifetime.end.p0i8(i64 8, i8* nonnull %117) #22
  %call122 = invoke nonnull align 8 dereferenceable(8) %"class.codegen::ParamValue"** @_ZNSt3mapINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEPN7codegen10ParamValueESt4lessIS5_ESaISt4pairIKS5_S8_EEEixEOS5_(%"class.std::map"* nonnull dereferenceable(48) %testParam, %"class.std::__cxx11::basic_string"* nonnull align 8 dereferenceable(32) %ref.tmp116)
          to label %invoke.cont121 unwind label %lpad120

invoke.cont121:                                   ; preds = %call5.i.i.i10.i.noexc697
  store %"class.codegen::ParamValue"* %p_sortColTypes, %"class.codegen::ParamValue"** %call122, align 8, !tbaa !37
  %120 = load i8*, i8** %_M_p.i13.i.i.i.i688, align 8, !tbaa !31
  %cmp.i.i.i702 = icmp eq i8* %120, %116
  br i1 %cmp.i.i.i702, label %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit704, label %if.then.i.i703

if.then.i.i703:                                   ; preds = %invoke.cont121
  call void @_ZdlPv(i8* %120) #22
  br label %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit704

_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit704: ; preds = %invoke.cont121, %if.then.i.i703
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %113) #22
  %121 = bitcast %"class.std::__cxx11::basic_string"* %ref.tmp127 to i8*
  call void @llvm.lifetime.start.p0i8(i64 32, i8* nonnull %121) #22
  %122 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp127, i64 0, i32 2
  %123 = bitcast %"class.std::__cxx11::basic_string"* %ref.tmp127 to %union.anon**
  store %union.anon* %122, %union.anon** %123, align 8, !tbaa !29
  %124 = bitcast %union.anon* %122 to i8*
  %125 = bitcast i64* %__dnew.i.i.i.i710 to i8*
  call void @llvm.lifetime.start.p0i8(i64 8, i8* nonnull %125) #22
  store i64 26, i64* %__dnew.i.i.i.i710, align 8, !tbaa !19
  %call5.i.i.i10.i723 = invoke i8* @_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE9_M_createERmm(%"class.std::__cxx11::basic_string"* nonnull dereferenceable(32) %ref.tmp127, i64* nonnull align 8 dereferenceable(8) %__dnew.i.i.i.i710, i64 0)
          to label %call5.i.i.i10.i.noexc722 unwind label %lpad129

call5.i.i.i10.i.noexc722:                         ; preds = %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit704
  %_M_p.i13.i.i.i.i713 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp127, i64 0, i32 0, i32 0
  store i8* %call5.i.i.i10.i723, i8** %_M_p.i13.i.i.i.i713, align 8, !tbaa !31
  %126 = load i64, i64* %__dnew.i.i.i.i710, align 8, !tbaa !19
  %_M_allocated_capacity.i.i.i.i.i714 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp127, i64 0, i32 2, i32 0
  store i64 %126, i64* %_M_allocated_capacity.i.i.i.i.i714, align 8, !tbaa !33
  call void @llvm.memcpy.p0i8.p0i8.i64(i8* nonnull align 1 dereferenceable(26) %call5.i.i.i10.i723, i8* nonnull align 1 dereferenceable(26) getelementptr inbounds ([27 x i8], [27 x i8]* @.str.10, i64 0, i64 0), i64 26, i1 false) #22
  %_M_string_length.i.i.i.i.i.i720 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp127, i64 0, i32 1
  store i64 %126, i64* %_M_string_length.i.i.i.i.i.i720, align 8, !tbaa !34
  %127 = load i8*, i8** %_M_p.i13.i.i.i.i713, align 8, !tbaa !31
  %arrayidx.i.i.i.i.i721 = getelementptr inbounds i8, i8* %127, i64 %126
  store i8 0, i8* %arrayidx.i.i.i.i.i721, align 1, !tbaa !33
  call void @llvm.lifetime.end.p0i8(i64 8, i8* nonnull %125) #22
  %call133 = invoke nonnull align 8 dereferenceable(8) %"class.codegen::ParamValue"** @_ZNSt3mapINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEPN7codegen10ParamValueESt4lessIS5_ESaISt4pairIKS5_S8_EEEixEOS5_(%"class.std::map"* nonnull dereferenceable(48) %testParam, %"class.std::__cxx11::basic_string"* nonnull align 8 dereferenceable(32) %ref.tmp127)
          to label %invoke.cont132 unwind label %lpad131

invoke.cont132:                                   ; preds = %call5.i.i.i10.i.noexc722
  store %"class.codegen::ParamValue"* %p_sortAscendings, %"class.codegen::ParamValue"** %call133, align 8, !tbaa !37
  %128 = load i8*, i8** %_M_p.i13.i.i.i.i713, align 8, !tbaa !31
  %cmp.i.i.i727 = icmp eq i8* %128, %124
  br i1 %cmp.i.i.i727, label %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit729, label %if.then.i.i728

if.then.i.i728:                                   ; preds = %invoke.cont132
  call void @_ZdlPv(i8* %128) #22
  br label %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit729

_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit729: ; preds = %invoke.cont132, %if.then.i.i728
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %121) #22
  %129 = bitcast %"class.std::__cxx11::basic_string"* %ref.tmp138 to i8*
  call void @llvm.lifetime.start.p0i8(i64 32, i8* nonnull %129) #22
  %130 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp138, i64 0, i32 2
  %131 = bitcast %"class.std::__cxx11::basic_string"* %ref.tmp138 to %union.anon**
  store %union.anon* %130, %union.anon** %131, align 8, !tbaa !29
  %132 = bitcast %union.anon* %130 to i8*
  %133 = bitcast i64* %__dnew.i.i.i.i735 to i8*
  call void @llvm.lifetime.start.p0i8(i64 8, i8* nonnull %133) #22
  store i64 26, i64* %__dnew.i.i.i.i735, align 8, !tbaa !19
  %call5.i.i.i10.i748 = invoke i8* @_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE9_M_createERmm(%"class.std::__cxx11::basic_string"* nonnull dereferenceable(32) %ref.tmp138, i64* nonnull align 8 dereferenceable(8) %__dnew.i.i.i.i735, i64 0)
          to label %call5.i.i.i10.i.noexc747 unwind label %lpad140

call5.i.i.i10.i.noexc747:                         ; preds = %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit729
  %_M_p.i13.i.i.i.i738 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp138, i64 0, i32 0, i32 0
  store i8* %call5.i.i.i10.i748, i8** %_M_p.i13.i.i.i.i738, align 8, !tbaa !31
  %134 = load i64, i64* %__dnew.i.i.i.i735, align 8, !tbaa !19
  %_M_allocated_capacity.i.i.i.i.i739 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp138, i64 0, i32 2, i32 0
  store i64 %134, i64* %_M_allocated_capacity.i.i.i.i.i739, align 8, !tbaa !33
  call void @llvm.memcpy.p0i8.p0i8.i64(i8* nonnull align 1 dereferenceable(26) %call5.i.i.i10.i748, i8* nonnull align 1 dereferenceable(26) getelementptr inbounds ([27 x i8], [27 x i8]* @.str.11, i64 0, i64 0), i64 26, i1 false) #22
  %_M_string_length.i.i.i.i.i.i745 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp138, i64 0, i32 1
  store i64 %134, i64* %_M_string_length.i.i.i.i.i.i745, align 8, !tbaa !34
  %135 = load i8*, i8** %_M_p.i13.i.i.i.i738, align 8, !tbaa !31
  %arrayidx.i.i.i.i.i746 = getelementptr inbounds i8, i8* %135, i64 %134
  store i8 0, i8* %arrayidx.i.i.i.i.i746, align 1, !tbaa !33
  call void @llvm.lifetime.end.p0i8(i64 8, i8* nonnull %133) #22
  %call144 = invoke nonnull align 8 dereferenceable(8) %"class.codegen::ParamValue"** @_ZNSt3mapINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEPN7codegen10ParamValueESt4lessIS5_ESaISt4pairIKS5_S8_EEEixEOS5_(%"class.std::map"* nonnull dereferenceable(48) %testParam, %"class.std::__cxx11::basic_string"* nonnull align 8 dereferenceable(32) %ref.tmp138)
          to label %invoke.cont143 unwind label %lpad142

invoke.cont143:                                   ; preds = %call5.i.i.i10.i.noexc747
  store %"class.codegen::ParamValue"* %p_sortNullFirsts, %"class.codegen::ParamValue"** %call144, align 8, !tbaa !37
  %136 = load i8*, i8** %_M_p.i13.i.i.i.i738, align 8, !tbaa !31
  %cmp.i.i.i752 = icmp eq i8* %136, %132
  br i1 %cmp.i.i.i752, label %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit754, label %if.then.i.i753

if.then.i.i753:                                   ; preds = %invoke.cont143
  call void @_ZdlPv(i8* %136) #22
  br label %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit754

_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit754: ; preds = %invoke.cont143, %if.then.i.i753
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %129) #22
  %137 = bitcast %"class.std::__cxx11::basic_string"* %ref.tmp149 to i8*
  call void @llvm.lifetime.start.p0i8(i64 32, i8* nonnull %137) #22
  %138 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp149, i64 0, i32 2
  %139 = bitcast %"class.std::__cxx11::basic_string"* %ref.tmp149 to %union.anon**
  store %union.anon* %138, %union.anon** %139, align 8, !tbaa !29
  %140 = bitcast %union.anon* %138 to i8*
  %141 = bitcast i64* %__dnew.i.i.i.i760 to i8*
  call void @llvm.lifetime.start.p0i8(i64 8, i8* nonnull %141) #22
  store i64 26, i64* %__dnew.i.i.i.i760, align 8, !tbaa !19
  %call5.i.i.i10.i773 = invoke i8* @_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE9_M_createERmm(%"class.std::__cxx11::basic_string"* nonnull dereferenceable(32) %ref.tmp149, i64* nonnull align 8 dereferenceable(8) %__dnew.i.i.i.i760, i64 0)
          to label %call5.i.i.i10.i.noexc772 unwind label %lpad151

call5.i.i.i10.i.noexc772:                         ; preds = %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit754
  %_M_p.i13.i.i.i.i763 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp149, i64 0, i32 0, i32 0
  store i8* %call5.i.i.i10.i773, i8** %_M_p.i13.i.i.i.i763, align 8, !tbaa !31
  %142 = load i64, i64* %__dnew.i.i.i.i760, align 8, !tbaa !19
  %_M_allocated_capacity.i.i.i.i.i764 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp149, i64 0, i32 2, i32 0
  store i64 %142, i64* %_M_allocated_capacity.i.i.i.i.i764, align 8, !tbaa !33
  call void @llvm.memcpy.p0i8.p0i8.i64(i8* nonnull align 1 dereferenceable(26) %call5.i.i.i10.i773, i8* nonnull align 1 dereferenceable(26) getelementptr inbounds ([27 x i8], [27 x i8]* @.str.12, i64 0, i64 0), i64 26, i1 false) #22
  %_M_string_length.i.i.i.i.i.i770 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp149, i64 0, i32 1
  store i64 %142, i64* %_M_string_length.i.i.i.i.i.i770, align 8, !tbaa !34
  %143 = load i8*, i8** %_M_p.i13.i.i.i.i763, align 8, !tbaa !31
  %arrayidx.i.i.i.i.i771 = getelementptr inbounds i8, i8* %143, i64 %142
  store i8 0, i8* %arrayidx.i.i.i.i.i771, align 1, !tbaa !33
  call void @llvm.lifetime.end.p0i8(i64 8, i8* nonnull %141) #22
  %call155 = invoke nonnull align 8 dereferenceable(8) %"class.codegen::ParamValue"** @_ZNSt3mapINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEPN7codegen10ParamValueESt4lessIS5_ESaISt4pairIKS5_S8_EEEixEOS5_(%"class.std::map"* nonnull dereferenceable(48) %testParam, %"class.std::__cxx11::basic_string"* nonnull align 8 dereferenceable(32) %ref.tmp149)
          to label %invoke.cont154 unwind label %lpad153

invoke.cont154:                                   ; preds = %call5.i.i.i10.i.noexc772
  store %"class.codegen::ParamValue"* %p_sortColCount, %"class.codegen::ParamValue"** %call155, align 8, !tbaa !37
  %144 = load i8*, i8** %_M_p.i13.i.i.i.i763, align 8, !tbaa !31
  %cmp.i.i.i777 = icmp eq i8* %144, %140
  br i1 %cmp.i.i.i777, label %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit779, label %if.then.i.i778

if.then.i.i778:                                   ; preds = %invoke.cont154
  call void @_ZdlPv(i8* %144) #22
  br label %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit779

_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit779: ; preds = %invoke.cont154, %if.then.i.i778
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %137) #22
  %145 = bitcast %"class.std::__cxx11::basic_string"* %ref.tmp160 to i8*
  call void @llvm.lifetime.start.p0i8(i64 32, i8* nonnull %145) #22
  %146 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp160, i64 0, i32 2
  %147 = bitcast %"class.std::__cxx11::basic_string"* %ref.tmp160 to %union.anon**
  store %union.anon* %146, %union.anon** %147, align 8, !tbaa !29
  %148 = bitcast %union.anon* %146 to i8*
  %149 = bitcast i64* %__dnew.i.i.i.i785 to i8*
  call void @llvm.lifetime.start.p0i8(i64 8, i8* nonnull %149) #22
  store i64 25, i64* %__dnew.i.i.i.i785, align 8, !tbaa !19
  %call5.i.i.i10.i798 = invoke i8* @_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE9_M_createERmm(%"class.std::__cxx11::basic_string"* nonnull dereferenceable(32) %ref.tmp160, i64* nonnull align 8 dereferenceable(8) %__dnew.i.i.i.i785, i64 0)
          to label %call5.i.i.i10.i.noexc797 unwind label %lpad162

call5.i.i.i10.i.noexc797:                         ; preds = %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit779
  %_M_p.i13.i.i.i.i788 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp160, i64 0, i32 0, i32 0
  store i8* %call5.i.i.i10.i798, i8** %_M_p.i13.i.i.i.i788, align 8, !tbaa !31
  %150 = load i64, i64* %__dnew.i.i.i.i785, align 8, !tbaa !19
  %_M_allocated_capacity.i.i.i.i.i789 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp160, i64 0, i32 2, i32 0
  store i64 %150, i64* %_M_allocated_capacity.i.i.i.i.i789, align 8, !tbaa !33
  call void @llvm.memcpy.p0i8.p0i8.i64(i8* nonnull align 1 dereferenceable(25) %call5.i.i.i10.i798, i8* nonnull align 1 dereferenceable(25) getelementptr inbounds ([26 x i8], [26 x i8]* @.str.13, i64 0, i64 0), i64 25, i1 false) #22
  %_M_string_length.i.i.i.i.i.i795 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp160, i64 0, i32 1
  store i64 %150, i64* %_M_string_length.i.i.i.i.i.i795, align 8, !tbaa !34
  %151 = load i8*, i8** %_M_p.i13.i.i.i.i788, align 8, !tbaa !31
  %arrayidx.i.i.i.i.i796 = getelementptr inbounds i8, i8* %151, i64 %150
  store i8 0, i8* %arrayidx.i.i.i.i.i796, align 1, !tbaa !33
  call void @llvm.lifetime.end.p0i8(i64 8, i8* nonnull %149) #22
  %call166 = invoke nonnull align 8 dereferenceable(8) %"class.codegen::ParamValue"** @_ZNSt3mapINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEPN7codegen10ParamValueESt4lessIS5_ESaISt4pairIKS5_S8_EEEixEOS5_(%"class.std::map"* nonnull dereferenceable(48) %testParam, %"class.std::__cxx11::basic_string"* nonnull align 8 dereferenceable(32) %ref.tmp160)
          to label %invoke.cont165 unwind label %lpad164

invoke.cont165:                                   ; preds = %call5.i.i.i10.i.noexc797
  store %"class.codegen::ParamValue"* %p_sourceTypes, %"class.codegen::ParamValue"** %call166, align 8, !tbaa !37
  %152 = load i8*, i8** %_M_p.i13.i.i.i.i788, align 8, !tbaa !31
  %cmp.i.i.i802 = icmp eq i8* %152, %148
  br i1 %cmp.i.i.i802, label %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit804, label %if.then.i.i803

if.then.i.i803:                                   ; preds = %invoke.cont165
  call void @_ZdlPv(i8* %152) #22
  br label %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit804

_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit804: ; preds = %invoke.cont165, %if.then.i.i803
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %145) #22
  %153 = bitcast %"class.std::__cxx11::basic_string"* %ref.tmp171 to i8*
  call void @llvm.lifetime.start.p0i8(i64 32, i8* nonnull %153) #22
  %154 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp171, i64 0, i32 2
  %155 = bitcast %"class.std::__cxx11::basic_string"* %ref.tmp171 to %union.anon**
  store %union.anon* %154, %union.anon** %155, align 8, !tbaa !29
  %156 = bitcast %union.anon* %154 to i8*
  %157 = bitcast i64* %__dnew.i.i.i.i810 to i8*
  call void @llvm.lifetime.start.p0i8(i64 8, i8* nonnull %157) #22
  store i64 25, i64* %__dnew.i.i.i.i810, align 8, !tbaa !19
  %call5.i.i.i10.i823 = invoke i8* @_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE9_M_createERmm(%"class.std::__cxx11::basic_string"* nonnull dereferenceable(32) %ref.tmp171, i64* nonnull align 8 dereferenceable(8) %__dnew.i.i.i.i810, i64 0)
          to label %call5.i.i.i10.i.noexc822 unwind label %lpad173

call5.i.i.i10.i.noexc822:                         ; preds = %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit804
  %_M_p.i13.i.i.i.i813 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp171, i64 0, i32 0, i32 0
  store i8* %call5.i.i.i10.i823, i8** %_M_p.i13.i.i.i.i813, align 8, !tbaa !31
  %158 = load i64, i64* %__dnew.i.i.i.i810, align 8, !tbaa !19
  %_M_allocated_capacity.i.i.i.i.i814 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp171, i64 0, i32 2, i32 0
  store i64 %158, i64* %_M_allocated_capacity.i.i.i.i.i814, align 8, !tbaa !33
  call void @llvm.memcpy.p0i8.p0i8.i64(i8* nonnull align 1 dereferenceable(25) %call5.i.i.i10.i823, i8* nonnull align 1 dereferenceable(25) getelementptr inbounds ([26 x i8], [26 x i8]* @.str.14, i64 0, i64 0), i64 25, i1 false) #22
  %_M_string_length.i.i.i.i.i.i820 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp171, i64 0, i32 1
  store i64 %158, i64* %_M_string_length.i.i.i.i.i.i820, align 8, !tbaa !34
  %159 = load i8*, i8** %_M_p.i13.i.i.i.i813, align 8, !tbaa !31
  %arrayidx.i.i.i.i.i821 = getelementptr inbounds i8, i8* %159, i64 %158
  store i8 0, i8* %arrayidx.i.i.i.i.i821, align 1, !tbaa !33
  call void @llvm.lifetime.end.p0i8(i64 8, i8* nonnull %157) #22
  %call177 = invoke nonnull align 8 dereferenceable(8) %"class.codegen::ParamValue"** @_ZNSt3mapINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEPN7codegen10ParamValueESt4lessIS5_ESaISt4pairIKS5_S8_EEEixEOS5_(%"class.std::map"* nonnull dereferenceable(48) %testParam, %"class.std::__cxx11::basic_string"* nonnull align 8 dereferenceable(32) %ref.tmp171)
          to label %invoke.cont176 unwind label %lpad175

invoke.cont176:                                   ; preds = %call5.i.i.i10.i.noexc822
  store %"class.codegen::ParamValue"* %p_outputCols, %"class.codegen::ParamValue"** %call177, align 8, !tbaa !37
  %160 = load i8*, i8** %_M_p.i13.i.i.i.i813, align 8, !tbaa !31
  %cmp.i.i.i827 = icmp eq i8* %160, %156
  br i1 %cmp.i.i.i827, label %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit829, label %if.then.i.i828

if.then.i.i828:                                   ; preds = %invoke.cont176
  call void @_ZdlPv(i8* %160) #22
  br label %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit829

_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit829: ; preds = %invoke.cont176, %if.then.i.i828
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %153) #22
  %161 = bitcast %"class.std::__cxx11::basic_string"* %ref.tmp182 to i8*
  call void @llvm.lifetime.start.p0i8(i64 32, i8* nonnull %161) #22
  %162 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp182, i64 0, i32 2
  %163 = bitcast %"class.std::__cxx11::basic_string"* %ref.tmp182 to %union.anon**
  store %union.anon* %162, %union.anon** %163, align 8, !tbaa !29
  %164 = bitcast %union.anon* %162 to i8*
  %165 = bitcast i64* %__dnew.i.i.i.i835 to i8*
  call void @llvm.lifetime.start.p0i8(i64 8, i8* nonnull %165) #22
  store i64 25, i64* %__dnew.i.i.i.i835, align 8, !tbaa !19
  %call5.i.i.i10.i848 = invoke i8* @_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE9_M_createERmm(%"class.std::__cxx11::basic_string"* nonnull dereferenceable(32) %ref.tmp182, i64* nonnull align 8 dereferenceable(8) %__dnew.i.i.i.i835, i64 0)
          to label %call5.i.i.i10.i.noexc847 unwind label %lpad184

call5.i.i.i10.i.noexc847:                         ; preds = %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit829
  %_M_p.i13.i.i.i.i838 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp182, i64 0, i32 0, i32 0
  store i8* %call5.i.i.i10.i848, i8** %_M_p.i13.i.i.i.i838, align 8, !tbaa !31
  %166 = load i64, i64* %__dnew.i.i.i.i835, align 8, !tbaa !19
  %_M_allocated_capacity.i.i.i.i.i839 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp182, i64 0, i32 2, i32 0
  store i64 %166, i64* %_M_allocated_capacity.i.i.i.i.i839, align 8, !tbaa !33
  call void @llvm.memcpy.p0i8.p0i8.i64(i8* nonnull align 1 dereferenceable(25) %call5.i.i.i10.i848, i8* nonnull align 1 dereferenceable(25) getelementptr inbounds ([26 x i8], [26 x i8]* @.str.15, i64 0, i64 0), i64 25, i1 false) #22
  %_M_string_length.i.i.i.i.i.i845 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp182, i64 0, i32 1
  store i64 %166, i64* %_M_string_length.i.i.i.i.i.i845, align 8, !tbaa !34
  %167 = load i8*, i8** %_M_p.i13.i.i.i.i838, align 8, !tbaa !31
  %arrayidx.i.i.i.i.i846 = getelementptr inbounds i8, i8* %167, i64 %166
  store i8 0, i8* %arrayidx.i.i.i.i.i846, align 1, !tbaa !33
  call void @llvm.lifetime.end.p0i8(i64 8, i8* nonnull %165) #22
  %call188 = invoke nonnull align 8 dereferenceable(8) %"class.codegen::ParamValue"** @_ZNSt3mapINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEPN7codegen10ParamValueESt4lessIS5_ESaISt4pairIKS5_S8_EEEixEOS5_(%"class.std::map"* nonnull dereferenceable(48) %testParam, %"class.std::__cxx11::basic_string"* nonnull align 8 dereferenceable(32) %ref.tmp182)
          to label %invoke.cont187 unwind label %lpad186

invoke.cont187:                                   ; preds = %call5.i.i.i10.i.noexc847
  store %"class.codegen::ParamValue"* %p_outputColCount, %"class.codegen::ParamValue"** %call188, align 8, !tbaa !37
  %168 = load i8*, i8** %_M_p.i13.i.i.i.i838, align 8, !tbaa !31
  %cmp.i.i.i852 = icmp eq i8* %168, %164
  br i1 %cmp.i.i.i852, label %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit854, label %if.then.i.i853

if.then.i.i853:                                   ; preds = %invoke.cont187
  call void @_ZdlPv(i8* %168) #22
  br label %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit854

_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit854: ; preds = %invoke.cont187, %if.then.i.i853
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %161) #22
  %169 = bitcast %"class.std::__cxx11::basic_string"* %ref.tmp193 to i8*
  call void @llvm.lifetime.start.p0i8(i64 32, i8* nonnull %169) #22
  %170 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp193, i64 0, i32 2
  %171 = bitcast %"class.std::__cxx11::basic_string"* %ref.tmp193 to %union.anon**
  store %union.anon* %170, %union.anon** %171, align 8, !tbaa !29
  %172 = bitcast %union.anon* %170 to i8*
  %173 = bitcast i64* %__dnew.i.i.i.i860 to i8*
  call void @llvm.lifetime.start.p0i8(i64 8, i8* nonnull %173) #22
  store i64 23, i64* %__dnew.i.i.i.i860, align 8, !tbaa !19
  %call5.i.i.i10.i873 = invoke i8* @_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE9_M_createERmm(%"class.std::__cxx11::basic_string"* nonnull dereferenceable(32) %ref.tmp193, i64* nonnull align 8 dereferenceable(8) %__dnew.i.i.i.i860, i64 0)
          to label %call5.i.i.i10.i.noexc872 unwind label %lpad195

call5.i.i.i10.i.noexc872:                         ; preds = %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit854
  %_M_p.i13.i.i.i.i863 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp193, i64 0, i32 0, i32 0
  store i8* %call5.i.i.i10.i873, i8** %_M_p.i13.i.i.i.i863, align 8, !tbaa !31
  %174 = load i64, i64* %__dnew.i.i.i.i860, align 8, !tbaa !19
  %_M_allocated_capacity.i.i.i.i.i864 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp193, i64 0, i32 2, i32 0
  store i64 %174, i64* %_M_allocated_capacity.i.i.i.i.i864, align 8, !tbaa !33
  call void @llvm.memcpy.p0i8.p0i8.i64(i8* nonnull align 1 dereferenceable(23) %call5.i.i.i10.i873, i8* nonnull align 1 dereferenceable(23) getelementptr inbounds ([24 x i8], [24 x i8]* @.str.16, i64 0, i64 0), i64 23, i1 false) #22
  %_M_string_length.i.i.i.i.i.i870 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp193, i64 0, i32 1
  store i64 %174, i64* %_M_string_length.i.i.i.i.i.i870, align 8, !tbaa !34
  %175 = load i8*, i8** %_M_p.i13.i.i.i.i863, align 8, !tbaa !31
  %arrayidx.i.i.i.i.i871 = getelementptr inbounds i8, i8* %175, i64 %174
  store i8 0, i8* %arrayidx.i.i.i.i.i871, align 1, !tbaa !33
  call void @llvm.lifetime.end.p0i8(i64 8, i8* nonnull %173) #22
  %call199 = invoke nonnull align 8 dereferenceable(8) %"class.codegen::ParamValue"** @_ZNSt3mapINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEPN7codegen10ParamValueESt4lessIS5_ESaISt4pairIKS5_S8_EEEixEOS5_(%"class.std::map"* nonnull dereferenceable(48) %testParam, %"class.std::__cxx11::basic_string"* nonnull align 8 dereferenceable(32) %ref.tmp193)
          to label %invoke.cont198 unwind label %lpad197

invoke.cont198:                                   ; preds = %call5.i.i.i10.i.noexc872
  store %"class.codegen::ParamValue"* %p_outputCols, %"class.codegen::ParamValue"** %call199, align 8, !tbaa !37
  %176 = load i8*, i8** %_M_p.i13.i.i.i.i863, align 8, !tbaa !31
  %cmp.i.i.i877 = icmp eq i8* %176, %172
  br i1 %cmp.i.i.i877, label %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit879, label %if.then.i.i878

if.then.i.i878:                                   ; preds = %invoke.cont198
  call void @_ZdlPv(i8* %176) #22
  br label %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit879

_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit879: ; preds = %invoke.cont198, %if.then.i.i878
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %169) #22
  %177 = bitcast %"class.std::__cxx11::basic_string"* %ref.tmp204 to i8*
  call void @llvm.lifetime.start.p0i8(i64 32, i8* nonnull %177) #22
  %178 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp204, i64 0, i32 2
  %179 = bitcast %"class.std::__cxx11::basic_string"* %ref.tmp204 to %union.anon**
  store %union.anon* %178, %union.anon** %179, align 8, !tbaa !29
  %180 = bitcast %union.anon* %178 to i8*
  %181 = bitcast i64* %__dnew.i.i.i.i885 to i8*
  call void @llvm.lifetime.start.p0i8(i64 8, i8* nonnull %181) #22
  store i64 23, i64* %__dnew.i.i.i.i885, align 8, !tbaa !19
  %call5.i.i.i10.i898 = invoke i8* @_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE9_M_createERmm(%"class.std::__cxx11::basic_string"* nonnull dereferenceable(32) %ref.tmp204, i64* nonnull align 8 dereferenceable(8) %__dnew.i.i.i.i885, i64 0)
          to label %call5.i.i.i10.i.noexc897 unwind label %lpad206

call5.i.i.i10.i.noexc897:                         ; preds = %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit879
  %_M_p.i13.i.i.i.i888 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp204, i64 0, i32 0, i32 0
  store i8* %call5.i.i.i10.i898, i8** %_M_p.i13.i.i.i.i888, align 8, !tbaa !31
  %182 = load i64, i64* %__dnew.i.i.i.i885, align 8, !tbaa !19
  %_M_allocated_capacity.i.i.i.i.i889 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp204, i64 0, i32 2, i32 0
  store i64 %182, i64* %_M_allocated_capacity.i.i.i.i.i889, align 8, !tbaa !33
  call void @llvm.memcpy.p0i8.p0i8.i64(i8* nonnull align 1 dereferenceable(23) %call5.i.i.i10.i898, i8* nonnull align 1 dereferenceable(23) getelementptr inbounds ([24 x i8], [24 x i8]* @.str.17, i64 0, i64 0), i64 23, i1 false) #22
  %_M_string_length.i.i.i.i.i.i895 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp204, i64 0, i32 1
  store i64 %182, i64* %_M_string_length.i.i.i.i.i.i895, align 8, !tbaa !34
  %183 = load i8*, i8** %_M_p.i13.i.i.i.i888, align 8, !tbaa !31
  %arrayidx.i.i.i.i.i896 = getelementptr inbounds i8, i8* %183, i64 %182
  store i8 0, i8* %arrayidx.i.i.i.i.i896, align 1, !tbaa !33
  call void @llvm.lifetime.end.p0i8(i64 8, i8* nonnull %181) #22
  %call210 = invoke nonnull align 8 dereferenceable(8) %"class.codegen::ParamValue"** @_ZNSt3mapINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEPN7codegen10ParamValueESt4lessIS5_ESaISt4pairIKS5_S8_EEEixEOS5_(%"class.std::map"* nonnull dereferenceable(48) %testParam, %"class.std::__cxx11::basic_string"* nonnull align 8 dereferenceable(32) %ref.tmp204)
          to label %invoke.cont209 unwind label %lpad208

invoke.cont209:                                   ; preds = %call5.i.i.i10.i.noexc897
  store %"class.codegen::ParamValue"* %p_outputColCount, %"class.codegen::ParamValue"** %call210, align 8, !tbaa !37
  %184 = load i8*, i8** %_M_p.i13.i.i.i.i888, align 8, !tbaa !31
  %cmp.i.i.i902 = icmp eq i8* %184, %180
  br i1 %cmp.i.i.i902, label %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit904, label %if.then.i.i903

if.then.i.i903:                                   ; preds = %invoke.cont209
  call void @_ZdlPv(i8* %184) #22
  br label %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit904

_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit904: ; preds = %invoke.cont209, %if.then.i.i903
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %177) #22
  %185 = bitcast %"class.std::__cxx11::basic_string"* %ref.tmp215 to i8*
  call void @llvm.lifetime.start.p0i8(i64 32, i8* nonnull %185) #22
  %186 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp215, i64 0, i32 2
  %187 = bitcast %"class.std::__cxx11::basic_string"* %ref.tmp215 to %union.anon**
  store %union.anon* %186, %union.anon** %187, align 8, !tbaa !29
  %188 = bitcast %union.anon* %186 to i8*
  %189 = bitcast i64* %__dnew.i.i.i.i910 to i8*
  call void @llvm.lifetime.start.p0i8(i64 8, i8* nonnull %189) #22
  store i64 23, i64* %__dnew.i.i.i.i910, align 8, !tbaa !19
  %call5.i.i.i10.i923 = invoke i8* @_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE9_M_createERmm(%"class.std::__cxx11::basic_string"* nonnull dereferenceable(32) %ref.tmp215, i64* nonnull align 8 dereferenceable(8) %__dnew.i.i.i.i910, i64 0)
          to label %call5.i.i.i10.i.noexc922 unwind label %lpad217

call5.i.i.i10.i.noexc922:                         ; preds = %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit904
  %_M_p.i13.i.i.i.i913 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp215, i64 0, i32 0, i32 0
  store i8* %call5.i.i.i10.i923, i8** %_M_p.i13.i.i.i.i913, align 8, !tbaa !31
  %190 = load i64, i64* %__dnew.i.i.i.i910, align 8, !tbaa !19
  %_M_allocated_capacity.i.i.i.i.i914 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp215, i64 0, i32 2, i32 0
  store i64 %190, i64* %_M_allocated_capacity.i.i.i.i.i914, align 8, !tbaa !33
  call void @llvm.memcpy.p0i8.p0i8.i64(i8* nonnull align 1 dereferenceable(23) %call5.i.i.i10.i923, i8* nonnull align 1 dereferenceable(23) getelementptr inbounds ([24 x i8], [24 x i8]* @.str.18, i64 0, i64 0), i64 23, i1 false) #22
  %_M_string_length.i.i.i.i.i.i920 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp215, i64 0, i32 1
  store i64 %190, i64* %_M_string_length.i.i.i.i.i.i920, align 8, !tbaa !34
  %191 = load i8*, i8** %_M_p.i13.i.i.i.i913, align 8, !tbaa !31
  %arrayidx.i.i.i.i.i921 = getelementptr inbounds i8, i8* %191, i64 %190
  store i8 0, i8* %arrayidx.i.i.i.i.i921, align 1, !tbaa !33
  call void @llvm.lifetime.end.p0i8(i64 8, i8* nonnull %189) #22
  %call221 = invoke nonnull align 8 dereferenceable(8) %"class.codegen::ParamValue"** @_ZNSt3mapINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEPN7codegen10ParamValueESt4lessIS5_ESaISt4pairIKS5_S8_EEEixEOS5_(%"class.std::map"* nonnull dereferenceable(48) %testParam, %"class.std::__cxx11::basic_string"* nonnull align 8 dereferenceable(32) %ref.tmp215)
          to label %invoke.cont220 unwind label %lpad219

invoke.cont220:                                   ; preds = %call5.i.i.i10.i.noexc922
  store %"class.codegen::ParamValue"* %p_sourceTypes, %"class.codegen::ParamValue"** %call221, align 8, !tbaa !37
  %192 = load i8*, i8** %_M_p.i13.i.i.i.i913, align 8, !tbaa !31
  %cmp.i.i.i927 = icmp eq i8* %192, %188
  br i1 %cmp.i.i.i927, label %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit929, label %if.then.i.i928

if.then.i.i928:                                   ; preds = %invoke.cont220
  call void @_ZdlPv(i8* %192) #22
  br label %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit929

_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit929: ; preds = %invoke.cont220, %if.then.i.i928
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %185) #22
  %call.i930 = invoke i8* @_ZN4llvm3sys14DynamicLibrary19getPermanentLibraryEPKcPNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEE(i8* getelementptr inbounds ([45 x i8], [45 x i8]* @.str.19, i64 0, i64 0), %"class.std::__cxx11::basic_string"* null)
          to label %invoke.cont229 unwind label %lpad19

invoke.cont229:                                   ; preds = %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit929
  %193 = bitcast %"class.codegen::Hammer"* %hammer1 to i8*
  call void @llvm.lifetime.start.p0i8(i64 152, i8* nonnull %193) #22
  %_M_t.i932 = getelementptr inbounds %"class.std::map", %"class.std::map"* %agg.tmp230, i64 0, i32 0
  %194 = getelementptr inbounds %"class.std::map", %"class.std::map"* %agg.tmp230, i64 0, i32 0, i32 0, i32 0, i32 0, i32 0
  %195 = getelementptr inbounds i8, i8* %194, i64 8
  %_M_color.i.i.i.i933 = bitcast i8* %195 to i32*
  store i32 0, i32* %_M_color.i.i.i.i933, align 8, !tbaa !6
  %_M_parent.i.i.i.i.i934 = getelementptr inbounds i8, i8* %194, i64 16
  %196 = bitcast i8* %_M_parent.i.i.i.i.i934 to %"struct.std::_Rb_tree_node_base"**
  store %"struct.std::_Rb_tree_node_base"* null, %"struct.std::_Rb_tree_node_base"** %196, align 8, !tbaa !12
  %_M_left.i.i.i.i.i935 = getelementptr inbounds i8, i8* %194, i64 24
  %197 = bitcast i8* %_M_left.i.i.i.i.i935 to i8**
  store i8* %195, i8** %197, align 8, !tbaa !13
  %_M_right.i.i.i.i.i936 = getelementptr inbounds i8, i8* %194, i64 32
  %198 = bitcast i8* %_M_right.i.i.i.i.i936 to i8**
  store i8* %195, i8** %198, align 8, !tbaa !14
  %_M_node_count.i.i.i.i.i937 = getelementptr inbounds i8, i8* %194, i64 40
  %199 = bitcast i8* %_M_node_count.i.i.i.i.i937 to i64*
  store i64 0, i64* %199, align 8, !tbaa !15
  %200 = load %"struct.std::_Rb_tree_node_base"*, %"struct.std::_Rb_tree_node_base"** %2, align 8, !tbaa !12
  %cmp.not.i.i938 = icmp eq %"struct.std::_Rb_tree_node_base"* %200, null
  br i1 %cmp.not.i.i938, label %invoke.cont231, label %if.then.i.i939

if.then.i.i939:                                   ; preds = %invoke.cont229
  %201 = bitcast %"struct.std::_Rb_tree_node_base"* %200 to %"struct.std::_Rb_tree_node"*
  %202 = bitcast %"struct.std::_Rb_tree<std::__cxx11::basic_string<char>, std::pair<const std::__cxx11::basic_string<char>, codegen::ParamValue *>, std::_Select1st<std::pair<const std::__cxx11::basic_string<char>, codegen::ParamValue *>>, std::less<std::__cxx11::basic_string<char>>>::_Alloc_node"* %__an.i.i.i to i8*
  call void @llvm.lifetime.start.p0i8(i64 8, i8* nonnull %202) #22
  %_M_t.i.i.i.i = getelementptr inbounds %"struct.std::_Rb_tree<std::__cxx11::basic_string<char>, std::pair<const std::__cxx11::basic_string<char>, codegen::ParamValue *>, std::_Select1st<std::pair<const std::__cxx11::basic_string<char>, codegen::ParamValue *>>, std::less<std::__cxx11::basic_string<char>>>::_Alloc_node", %"struct.std::_Rb_tree<std::__cxx11::basic_string<char>, std::pair<const std::__cxx11::basic_string<char>, codegen::ParamValue *>, std::_Select1st<std::pair<const std::__cxx11::basic_string<char>, codegen::ParamValue *>>, std::less<std::__cxx11::basic_string<char>>>::_Alloc_node"* %__an.i.i.i, i64 0, i32 0
  store %"class.std::_Rb_tree"* %_M_t.i932, %"class.std::_Rb_tree"** %_M_t.i.i.i.i, align 8, !tbaa !37
  %_M_header.i.i.i.i.i = bitcast i8* %195 to %"struct.std::_Rb_tree_node_base"*
  %call3.i.i11.i.i940 = invoke %"struct.std::_Rb_tree_node"* @_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE7_M_copyINSH_11_Alloc_nodeEEEPSt13_Rb_tree_nodeISB_EPKSL_PSt18_Rb_tree_node_baseRT_(%"class.std::_Rb_tree"* nonnull dereferenceable(48) %_M_t.i932, %"struct.std::_Rb_tree_node"* nonnull %201, %"struct.std::_Rb_tree_node_base"* nonnull %_M_header.i.i.i.i.i, %"struct.std::_Rb_tree<std::__cxx11::basic_string<char>, std::pair<const std::__cxx11::basic_string<char>, codegen::ParamValue *>, std::_Select1st<std::pair<const std::__cxx11::basic_string<char>, codegen::ParamValue *>>, std::less<std::__cxx11::basic_string<char>>>::_Alloc_node"* nonnull align 8 dereferenceable(8) %__an.i.i.i)
          to label %call3.i.i11.i.i.noexc unwind label %lpad228

call3.i.i11.i.i.noexc:                            ; preds = %if.then.i.i939
  %203 = getelementptr %"struct.std::_Rb_tree_node", %"struct.std::_Rb_tree_node"* %call3.i.i11.i.i940, i64 0, i32 0
  br label %while.cond.i.i17.i.i.i.i

while.cond.i.i17.i.i.i.i:                         ; preds = %while.cond.i.i17.i.i.i.i, %call3.i.i11.i.i.noexc
  %__x.addr.0.i.i15.i.i.i.i = phi %"struct.std::_Rb_tree_node_base"* [ %203, %call3.i.i11.i.i.noexc ], [ %204, %while.cond.i.i17.i.i.i.i ]
  %_M_left.i.i.i.i.i.i = getelementptr inbounds %"struct.std::_Rb_tree_node_base", %"struct.std::_Rb_tree_node_base"* %__x.addr.0.i.i15.i.i.i.i, i64 0, i32 2
  %204 = load %"struct.std::_Rb_tree_node_base"*, %"struct.std::_Rb_tree_node_base"** %_M_left.i.i.i.i.i.i, align 8, !tbaa !38
  %cmp.not.i.i16.i.i.i.i = icmp eq %"struct.std::_Rb_tree_node_base"* %204, null
  br i1 %cmp.not.i.i16.i.i.i.i, label %_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE10_S_minimumEPSt18_Rb_tree_node_base.exit.i.i.i.i, label %while.cond.i.i17.i.i.i.i, !llvm.loop !39

_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE10_S_minimumEPSt18_Rb_tree_node_base.exit.i.i.i.i: ; preds = %while.cond.i.i17.i.i.i.i
  %205 = bitcast i8* %_M_left.i.i.i.i.i935 to %"struct.std::_Rb_tree_node_base"**
  store %"struct.std::_Rb_tree_node_base"* %__x.addr.0.i.i15.i.i.i.i, %"struct.std::_Rb_tree_node_base"** %205, align 8, !tbaa !37
  br label %while.cond.i.i.i.i.i.i

while.cond.i.i.i.i.i.i:                           ; preds = %while.cond.i.i.i.i.i.i, %_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE10_S_minimumEPSt18_Rb_tree_node_base.exit.i.i.i.i
  %__x.addr.0.i.i.i.i.i.i = phi %"struct.std::_Rb_tree_node_base"* [ %203, %_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE10_S_minimumEPSt18_Rb_tree_node_base.exit.i.i.i.i ], [ %206, %while.cond.i.i.i.i.i.i ]
  %_M_right.i.i.i.i.i.i = getelementptr inbounds %"struct.std::_Rb_tree_node_base", %"struct.std::_Rb_tree_node_base"* %__x.addr.0.i.i.i.i.i.i, i64 0, i32 3
  %206 = load %"struct.std::_Rb_tree_node_base"*, %"struct.std::_Rb_tree_node_base"** %_M_right.i.i.i.i.i.i, align 8, !tbaa !40
  %cmp.not.i.i.i.i.i.i = icmp eq %"struct.std::_Rb_tree_node_base"* %206, null
  br i1 %cmp.not.i.i.i.i.i.i, label %invoke.cont.i.i, label %while.cond.i.i.i.i.i.i, !llvm.loop !41

invoke.cont.i.i:                                  ; preds = %while.cond.i.i.i.i.i.i
  %207 = bitcast i8* %_M_right.i.i.i.i.i936 to %"struct.std::_Rb_tree_node_base"**
  store %"struct.std::_Rb_tree_node_base"* %__x.addr.0.i.i.i.i.i.i, %"struct.std::_Rb_tree_node_base"** %207, align 8, !tbaa !37
  %208 = load i64, i64* %5, align 8, !tbaa !15
  store i64 %208, i64* %199, align 8, !tbaa !15
  call void @llvm.lifetime.end.p0i8(i64 8, i8* nonnull %202) #22
  store %"struct.std::_Rb_tree_node_base"* %203, %"struct.std::_Rb_tree_node_base"** %196, align 8, !tbaa !37
  br label %invoke.cont231

invoke.cont231:                                   ; preds = %invoke.cont.i.i, %invoke.cont229
  invoke void @_ZN7codegen6HammerC1EN4llvm9StringRefESt3mapINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEPNS_10ParamValueESt4lessIS9_ESaISt4pairIKS9_SB_EEE(%"class.codegen::Hammer"* nonnull dereferenceable(152) %hammer1, i8* getelementptr inbounds ([20 x i8], [20 x i8]* @.str.20, i64 0, i64 0), i64 19, %"class.std::map"* nonnull %agg.tmp230)
          to label %invoke.cont233 unwind label %lpad232

invoke.cont233:                                   ; preds = %invoke.cont231
  %209 = bitcast i8* %_M_parent.i.i.i.i.i934 to %"struct.std::_Rb_tree_node"**
  %210 = load %"struct.std::_Rb_tree_node"*, %"struct.std::_Rb_tree_node"** %209, align 8, !tbaa !12
  invoke void @_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE8_M_eraseEPSt13_Rb_tree_nodeISB_E(%"class.std::_Rb_tree"* nonnull dereferenceable(48) %_M_t.i932, %"struct.std::_Rb_tree_node"* %210)
          to label %invoke.cont237 unwind label %lpad.i.i943

lpad.i.i943:                                      ; preds = %invoke.cont233
  %211 = landingpad { i8*, i32 }
          catch i8* null
  %212 = extractvalue { i8*, i32 } %211, 0
  call void @__clang_call_terminate(i8* %212) #23
  unreachable

invoke.cont237:                                   ; preds = %invoke.cont233
  %213 = bitcast %"class.codegen::Hammer"* %hammer2 to i8*
  call void @llvm.lifetime.start.p0i8(i64 152, i8* nonnull %213) #22
  %_M_t.i949 = getelementptr inbounds %"class.std::map", %"class.std::map"* %agg.tmp238, i64 0, i32 0
  %214 = getelementptr inbounds %"class.std::map", %"class.std::map"* %agg.tmp238, i64 0, i32 0, i32 0, i32 0, i32 0, i32 0
  %215 = getelementptr inbounds i8, i8* %214, i64 8
  %_M_color.i.i.i.i950 = bitcast i8* %215 to i32*
  store i32 0, i32* %_M_color.i.i.i.i950, align 8, !tbaa !6
  %_M_parent.i.i.i.i.i951 = getelementptr inbounds i8, i8* %214, i64 16
  %216 = bitcast i8* %_M_parent.i.i.i.i.i951 to %"struct.std::_Rb_tree_node_base"**
  store %"struct.std::_Rb_tree_node_base"* null, %"struct.std::_Rb_tree_node_base"** %216, align 8, !tbaa !12
  %_M_left.i.i.i.i.i952 = getelementptr inbounds i8, i8* %214, i64 24
  %217 = bitcast i8* %_M_left.i.i.i.i.i952 to i8**
  store i8* %215, i8** %217, align 8, !tbaa !13
  %_M_right.i.i.i.i.i953 = getelementptr inbounds i8, i8* %214, i64 32
  %218 = bitcast i8* %_M_right.i.i.i.i.i953 to i8**
  store i8* %215, i8** %218, align 8, !tbaa !14
  %_M_node_count.i.i.i.i.i954 = getelementptr inbounds i8, i8* %214, i64 40
  %219 = bitcast i8* %_M_node_count.i.i.i.i.i954 to i64*
  store i64 0, i64* %219, align 8, !tbaa !15
  %220 = load %"struct.std::_Rb_tree_node_base"*, %"struct.std::_Rb_tree_node_base"** %2, align 8, !tbaa !12
  %cmp.not.i.i956 = icmp eq %"struct.std::_Rb_tree_node_base"* %220, null
  br i1 %cmp.not.i.i956, label %invoke.cont239, label %if.then.i.i959

if.then.i.i959:                                   ; preds = %invoke.cont237
  %221 = bitcast %"struct.std::_Rb_tree_node_base"* %220 to %"struct.std::_Rb_tree_node"*
  %222 = bitcast %"struct.std::_Rb_tree<std::__cxx11::basic_string<char>, std::pair<const std::__cxx11::basic_string<char>, codegen::ParamValue *>, std::_Select1st<std::pair<const std::__cxx11::basic_string<char>, codegen::ParamValue *>>, std::less<std::__cxx11::basic_string<char>>>::_Alloc_node"* %__an.i.i.i948 to i8*
  call void @llvm.lifetime.start.p0i8(i64 8, i8* nonnull %222) #22
  %_M_t.i.i.i.i957 = getelementptr inbounds %"struct.std::_Rb_tree<std::__cxx11::basic_string<char>, std::pair<const std::__cxx11::basic_string<char>, codegen::ParamValue *>, std::_Select1st<std::pair<const std::__cxx11::basic_string<char>, codegen::ParamValue *>>, std::less<std::__cxx11::basic_string<char>>>::_Alloc_node", %"struct.std::_Rb_tree<std::__cxx11::basic_string<char>, std::pair<const std::__cxx11::basic_string<char>, codegen::ParamValue *>, std::_Select1st<std::pair<const std::__cxx11::basic_string<char>, codegen::ParamValue *>>, std::less<std::__cxx11::basic_string<char>>>::_Alloc_node"* %__an.i.i.i948, i64 0, i32 0
  store %"class.std::_Rb_tree"* %_M_t.i949, %"class.std::_Rb_tree"** %_M_t.i.i.i.i957, align 8, !tbaa !37
  %_M_header.i.i.i.i.i958 = bitcast i8* %215 to %"struct.std::_Rb_tree_node_base"*
  %call3.i.i11.i.i972 = invoke %"struct.std::_Rb_tree_node"* @_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE7_M_copyINSH_11_Alloc_nodeEEEPSt13_Rb_tree_nodeISB_EPKSL_PSt18_Rb_tree_node_baseRT_(%"class.std::_Rb_tree"* nonnull dereferenceable(48) %_M_t.i949, %"struct.std::_Rb_tree_node"* nonnull %221, %"struct.std::_Rb_tree_node_base"* nonnull %_M_header.i.i.i.i.i958, %"struct.std::_Rb_tree<std::__cxx11::basic_string<char>, std::pair<const std::__cxx11::basic_string<char>, codegen::ParamValue *>, std::_Select1st<std::pair<const std::__cxx11::basic_string<char>, codegen::ParamValue *>>, std::less<std::__cxx11::basic_string<char>>>::_Alloc_node"* nonnull align 8 dereferenceable(8) %__an.i.i.i948)
          to label %call3.i.i11.i.i.noexc971 unwind label %lpad236

call3.i.i11.i.i.noexc971:                         ; preds = %if.then.i.i959
  %223 = getelementptr %"struct.std::_Rb_tree_node", %"struct.std::_Rb_tree_node"* %call3.i.i11.i.i972, i64 0, i32 0
  br label %while.cond.i.i17.i.i.i.i963

while.cond.i.i17.i.i.i.i963:                      ; preds = %while.cond.i.i17.i.i.i.i963, %call3.i.i11.i.i.noexc971
  %__x.addr.0.i.i15.i.i.i.i960 = phi %"struct.std::_Rb_tree_node_base"* [ %223, %call3.i.i11.i.i.noexc971 ], [ %224, %while.cond.i.i17.i.i.i.i963 ]
  %_M_left.i.i.i.i.i.i961 = getelementptr inbounds %"struct.std::_Rb_tree_node_base", %"struct.std::_Rb_tree_node_base"* %__x.addr.0.i.i15.i.i.i.i960, i64 0, i32 2
  %224 = load %"struct.std::_Rb_tree_node_base"*, %"struct.std::_Rb_tree_node_base"** %_M_left.i.i.i.i.i.i961, align 8, !tbaa !38
  %cmp.not.i.i16.i.i.i.i962 = icmp eq %"struct.std::_Rb_tree_node_base"* %224, null
  br i1 %cmp.not.i.i16.i.i.i.i962, label %_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE10_S_minimumEPSt18_Rb_tree_node_base.exit.i.i.i.i964, label %while.cond.i.i17.i.i.i.i963, !llvm.loop !39

_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE10_S_minimumEPSt18_Rb_tree_node_base.exit.i.i.i.i964: ; preds = %while.cond.i.i17.i.i.i.i963
  %225 = bitcast i8* %_M_left.i.i.i.i.i952 to %"struct.std::_Rb_tree_node_base"**
  store %"struct.std::_Rb_tree_node_base"* %__x.addr.0.i.i15.i.i.i.i960, %"struct.std::_Rb_tree_node_base"** %225, align 8, !tbaa !37
  br label %while.cond.i.i.i.i.i.i968

while.cond.i.i.i.i.i.i968:                        ; preds = %while.cond.i.i.i.i.i.i968, %_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE10_S_minimumEPSt18_Rb_tree_node_base.exit.i.i.i.i964
  %__x.addr.0.i.i.i.i.i.i965 = phi %"struct.std::_Rb_tree_node_base"* [ %223, %_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE10_S_minimumEPSt18_Rb_tree_node_base.exit.i.i.i.i964 ], [ %226, %while.cond.i.i.i.i.i.i968 ]
  %_M_right.i.i.i.i.i.i966 = getelementptr inbounds %"struct.std::_Rb_tree_node_base", %"struct.std::_Rb_tree_node_base"* %__x.addr.0.i.i.i.i.i.i965, i64 0, i32 3
  %226 = load %"struct.std::_Rb_tree_node_base"*, %"struct.std::_Rb_tree_node_base"** %_M_right.i.i.i.i.i.i966, align 8, !tbaa !40
  %cmp.not.i.i.i.i.i.i967 = icmp eq %"struct.std::_Rb_tree_node_base"* %226, null
  br i1 %cmp.not.i.i.i.i.i.i967, label %invoke.cont.i.i970, label %while.cond.i.i.i.i.i.i968, !llvm.loop !41

invoke.cont.i.i970:                               ; preds = %while.cond.i.i.i.i.i.i968
  %227 = bitcast i8* %_M_right.i.i.i.i.i953 to %"struct.std::_Rb_tree_node_base"**
  store %"struct.std::_Rb_tree_node_base"* %__x.addr.0.i.i.i.i.i.i965, %"struct.std::_Rb_tree_node_base"** %227, align 8, !tbaa !37
  %228 = load i64, i64* %5, align 8, !tbaa !15
  store i64 %228, i64* %219, align 8, !tbaa !15
  call void @llvm.lifetime.end.p0i8(i64 8, i8* nonnull %222) #22
  store %"struct.std::_Rb_tree_node_base"* %223, %"struct.std::_Rb_tree_node_base"** %216, align 8, !tbaa !37
  br label %invoke.cont239

invoke.cont239:                                   ; preds = %invoke.cont.i.i970, %invoke.cont237
  invoke void @_ZN7codegen6HammerC1EN4llvm9StringRefESt3mapINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEPNS_10ParamValueESt4lessIS9_ESaISt4pairIKS9_SB_EEE(%"class.codegen::Hammer"* nonnull dereferenceable(152) %hammer2, i8* getelementptr inbounds ([27 x i8], [27 x i8]* @.str.21, i64 0, i64 0), i64 26, %"class.std::map"* nonnull %agg.tmp238)
          to label %invoke.cont241 unwind label %lpad240

invoke.cont241:                                   ; preds = %invoke.cont239
  %229 = bitcast i8* %_M_parent.i.i.i.i.i951 to %"struct.std::_Rb_tree_node"**
  %230 = load %"struct.std::_Rb_tree_node"*, %"struct.std::_Rb_tree_node"** %229, align 8, !tbaa !12
  invoke void @_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE8_M_eraseEPSt13_Rb_tree_nodeISB_E(%"class.std::_Rb_tree"* nonnull dereferenceable(48) %_M_t.i949, %"struct.std::_Rb_tree_node"* %230)
          to label %_ZNSt3mapINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEPN7codegen10ParamValueESt4lessIS5_ESaISt4pairIKS5_S8_EEED2Ev.exit977 unwind label %lpad.i.i976

lpad.i.i976:                                      ; preds = %invoke.cont241
  %231 = landingpad { i8*, i32 }
          catch i8* null
  %232 = extractvalue { i8*, i32 } %231, 0
  call void @__clang_call_terminate(i8* %232) #23
  unreachable

_ZNSt3mapINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEPN7codegen10ParamValueESt4lessIS5_ESaISt4pairIKS5_S8_EEED2Ev.exit977: ; preds = %invoke.cont241
  %call245 = invoke zeroext i1 @_ZN7codegen6Hammer6hardenEv(%"class.codegen::Hammer"* nonnull dereferenceable(152) %hammer1)
          to label %invoke.cont244 unwind label %lpad243

invoke.cont244:                                   ; preds = %_ZNSt3mapINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEPN7codegen10ParamValueESt4lessIS5_ESaISt4pairIKS5_S8_EEED2Ev.exit977
  %call247 = invoke zeroext i1 @_ZN7codegen6Hammer6hardenEv(%"class.codegen::Hammer"* nonnull dereferenceable(152) %hammer2)
          to label %invoke.cont246 unwind label %lpad243

invoke.cont246:                                   ; preds = %invoke.cont244
  %call2.i.i.i.i.i.i978 = invoke noalias nonnull i8* @_Znwm(i64 24) #24
          to label %invoke.cont250 unwind label %lpad249

invoke.cont250:                                   ; preds = %invoke.cont246
  %_M_storage.i.i4.i.i = getelementptr inbounds i8, i8* %call2.i.i.i.i.i.i978, i64 16
  %233 = bitcast i8* %_M_storage.i.i4.i.i to %"class.codegen::Hammer"**
  store %"class.codegen::Hammer"* %hammer2, %"class.codegen::Hammer"** %233, align 8, !tbaa !37
  %234 = bitcast i8* %call2.i.i.i.i.i.i978 to %"struct.std::__detail::_List_node_base"*
  call void @_ZNSt8__detail15_List_node_base7_M_hookEPS0_(%"struct.std::__detail::_List_node_base"* nonnull dereferenceable(16) %234, %"struct.std::__detail::_List_node_base"* nonnull %7) #22
  %235 = load i64, i64* %8, align 8, !tbaa !19
  %add.i.i.i = add i64 %235, 1
  store i64 %add.i.i.i, i64* %8, align 8, !tbaa !19
  %236 = bitcast %"class.codegen::HammerConfig"* %hammerConfig to i8*
  call void @llvm.lifetime.start.p0i8(i64 1616, i8* nonnull %236) #22
  %237 = getelementptr inbounds [32 x i8], [32 x i8]* %ALL.i, i64 0, i64 0
  call void @llvm.lifetime.start.p0i8(i64 32, i8* nonnull %237) #22
  call void @llvm.memcpy.p0i8.p0i8.i64(i8* nonnull align 16 dereferenceable(32) %237, i8* nonnull align 16 dereferenceable(32) getelementptr inbounds (<{ i8, [31 x i8] }>, <{ i8, [31 x i8] }>* @__const.HammerConfig.ALL, i64 0, i32 0), i64 32, i1 false)
  %func_conf.i = getelementptr inbounds %"class.codegen::HammerConfig", %"class.codegen::HammerConfig"* %hammerConfig, i64 0, i32 0
  store i8* %237, i8** %func_conf.i, align 8, !tbaa !42
  %module_conf.i = getelementptr inbounds %"class.codegen::HammerConfig", %"class.codegen::HammerConfig"* %hammerConfig, i64 0, i32 1
  store i8* %237, i8** %module_conf.i, align 8, !tbaa !44
  invoke void @_ZN7codegen12HammerConfig14init_func_passEv(%"class.codegen::HammerConfig"* nonnull dereferenceable(1616) %hammerConfig)
          to label %.noexc unwind label %lpad252

.noexc:                                           ; preds = %invoke.cont250
  invoke void @_ZN7codegen12HammerConfig16init_module_passEv(%"class.codegen::HammerConfig"* nonnull dereferenceable(1616) %hammerConfig)
          to label %invoke.cont253 unwind label %lpad252

invoke.cont253:                                   ; preds = %.noexc
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %237) #22
  %238 = bitcast %"class.std::unique_ptr.123"* %jitter to i8*
  call void @llvm.lifetime.start.p0i8(i64 8, i8* nonnull %238) #22
  %239 = getelementptr inbounds %"class.std::__cxx11::list", %"class.std::__cxx11::list"* %agg.tmp254, i64 0, i32 0, i32 0, i32 0, i32 0
  %_M_next.i.i14.i = getelementptr inbounds %"class.std::__cxx11::list", %"class.std::__cxx11::list"* %agg.tmp254, i64 0, i32 0, i32 0, i32 0, i32 0, i32 0
  store %"struct.std::__detail::_List_node_base"* %239, %"struct.std::__detail::_List_node_base"** %_M_next.i.i14.i, align 8, !tbaa !16
  %_M_prev.i.i.i980 = getelementptr inbounds %"class.std::__cxx11::list", %"class.std::__cxx11::list"* %agg.tmp254, i64 0, i32 0, i32 0, i32 0, i32 0, i32 1
  store %"struct.std::__detail::_List_node_base"* %239, %"struct.std::__detail::_List_node_base"** %_M_prev.i.i.i980, align 8, !tbaa !18
  %_M_storage.i.i.i.i.i981 = getelementptr inbounds %"class.std::__cxx11::list", %"class.std::__cxx11::list"* %agg.tmp254, i64 0, i32 0, i32 0, i32 0, i32 1
  %240 = bitcast %"struct.__gnu_cxx::__aligned_membuf"* %_M_storage.i.i.i.i.i981 to i64*
  store i64 0, i64* %240, align 8, !tbaa !19
  %241 = load %"struct.std::__detail::_List_node_base"*, %"struct.std::__detail::_List_node_base"** %_M_next.i.i.i, align 8, !tbaa !16
  %cmp.i.not8.i.i = icmp eq %"struct.std::__detail::_List_node_base"* %241, %7
  br i1 %cmp.i.not8.i.i, label %invoke.cont256, label %for.body.i.i

for.body.i.i:                                     ; preds = %invoke.cont253, %call2.i.i.i.i.i.i.i.noexc.i
  %__first.sroa.0.09.i.i = phi %"struct.std::__detail::_List_node_base"* [ %247, %call2.i.i.i.i.i.i.i.noexc.i ], [ %241, %invoke.cont253 ]
  %call2.i.i.i.i.i.i.i13.i = invoke noalias nonnull i8* @_Znwm(i64 24) #24
          to label %call2.i.i.i.i.i.i.i.noexc.i unwind label %lpad.i

call2.i.i.i.i.i.i.i.noexc.i:                      ; preds = %for.body.i.i
  %_M_storage.i.i.i.i983 = getelementptr inbounds %"struct.std::__detail::_List_node_base", %"struct.std::__detail::_List_node_base"* %__first.sroa.0.09.i.i, i64 1
  %242 = bitcast %"struct.std::__detail::_List_node_base"* %_M_storage.i.i.i.i983 to %"class.codegen::Hammer"**
  %_M_storage.i.i4.i.i.i.i = getelementptr inbounds i8, i8* %call2.i.i.i.i.i.i.i13.i, i64 16
  %243 = bitcast i8* %_M_storage.i.i4.i.i.i.i to %"class.codegen::Hammer"**
  %244 = load %"class.codegen::Hammer"*, %"class.codegen::Hammer"** %242, align 8, !tbaa !37
  store %"class.codegen::Hammer"* %244, %"class.codegen::Hammer"** %243, align 8, !tbaa !37
  %245 = bitcast i8* %call2.i.i.i.i.i.i.i13.i to %"struct.std::__detail::_List_node_base"*
  call void @_ZNSt8__detail15_List_node_base7_M_hookEPS0_(%"struct.std::__detail::_List_node_base"* nonnull dereferenceable(16) %245, %"struct.std::__detail::_List_node_base"* nonnull %239) #22
  %246 = load i64, i64* %240, align 8, !tbaa !19
  %add.i.i.i.i.i = add i64 %246, 1
  store i64 %add.i.i.i.i.i, i64* %240, align 8, !tbaa !19
  %_M_next.i.i12.i = getelementptr inbounds %"struct.std::__detail::_List_node_base", %"struct.std::__detail::_List_node_base"* %__first.sroa.0.09.i.i, i64 0, i32 0
  %247 = load %"struct.std::__detail::_List_node_base"*, %"struct.std::__detail::_List_node_base"** %_M_next.i.i12.i, align 8, !tbaa !16
  %cmp.i.not.i.i = icmp eq %"struct.std::__detail::_List_node_base"* %247, %7
  br i1 %cmp.i.not.i.i, label %invoke.cont256, label %for.body.i.i, !llvm.loop !45

lpad.i:                                           ; preds = %for.body.i.i
  %248 = landingpad { i8*, i32 }
          cleanup
  %249 = load %"struct.std::__detail::_List_node_base"*, %"struct.std::__detail::_List_node_base"** %_M_next.i.i14.i, align 8, !tbaa !16
  %cmp.not16.i.i.i = icmp eq %"struct.std::__detail::_List_node_base"* %249, %239
  br i1 %cmp.not16.i.i.i, label %ehcleanup327, label %invoke.cont6.i.i.i

invoke.cont6.i.i.i:                               ; preds = %lpad.i, %invoke.cont6.i.i.i
  %__cur.017.i.i.i = phi %"struct.std::__detail::_List_node_base"* [ %250, %invoke.cont6.i.i.i ], [ %249, %lpad.i ]
  %_M_next4.i.i.i = getelementptr inbounds %"struct.std::__detail::_List_node_base", %"struct.std::__detail::_List_node_base"* %__cur.017.i.i.i, i64 0, i32 0
  %250 = load %"struct.std::__detail::_List_node_base"*, %"struct.std::__detail::_List_node_base"** %_M_next4.i.i.i, align 8, !tbaa !16
  %251 = bitcast %"struct.std::__detail::_List_node_base"* %__cur.017.i.i.i to i8*
  call void @_ZdlPv(i8* %251) #22
  %cmp.not.i.i.i = icmp eq %"struct.std::__detail::_List_node_base"* %250, %239
  br i1 %cmp.not.i.i.i, label %ehcleanup327, label %invoke.cont6.i.i.i, !llvm.loop !46

invoke.cont256:                                   ; preds = %call2.i.i.i.i.i.i.i.noexc.i, %invoke.cont253
  invoke void @_ZN7codegen6Hammer13create_jitterENSt7__cxx114listIPS0_SaIS3_EEERNS_12HammerConfigE(%"class.std::unique_ptr.123"* nonnull sret(%"class.std::unique_ptr.123") align 8 %jitter, %"class.codegen::Hammer"* nonnull dereferenceable(152) %hammer1, %"class.std::__cxx11::list"* nonnull %agg.tmp254, %"class.codegen::HammerConfig"* nonnull align 8 dereferenceable(1616) %hammerConfig)
          to label %invoke.cont258 unwind label %lpad257

invoke.cont258:                                   ; preds = %invoke.cont256
  %252 = load %"struct.std::__detail::_List_node_base"*, %"struct.std::__detail::_List_node_base"** %_M_next.i.i14.i, align 8, !tbaa !16
  %cmp.not16.i.i985 = icmp eq %"struct.std::__detail::_List_node_base"* %252, %239
  br i1 %cmp.not16.i.i985, label %invoke.cont264, label %invoke.cont6.i.i989

invoke.cont6.i.i989:                              ; preds = %invoke.cont258, %invoke.cont6.i.i989
  %__cur.017.i.i986 = phi %"struct.std::__detail::_List_node_base"* [ %253, %invoke.cont6.i.i989 ], [ %252, %invoke.cont258 ]
  %_M_next4.i.i987 = getelementptr inbounds %"struct.std::__detail::_List_node_base", %"struct.std::__detail::_List_node_base"* %__cur.017.i.i986, i64 0, i32 0
  %253 = load %"struct.std::__detail::_List_node_base"*, %"struct.std::__detail::_List_node_base"** %_M_next4.i.i987, align 8, !tbaa !16
  %254 = bitcast %"struct.std::__detail::_List_node_base"* %__cur.017.i.i986 to i8*
  call void @_ZdlPv(i8* %254) #22
  %cmp.not.i.i988 = icmp eq %"struct.std::__detail::_List_node_base"* %253, %239
  br i1 %cmp.not.i.i988, label %invoke.cont264, label %invoke.cont6.i.i989, !llvm.loop !46

invoke.cont264:                                   ; preds = %invoke.cont6.i.i989, %invoke.cont258
  %255 = getelementptr inbounds %"class.llvm::Expected", %"class.llvm::Expected"* %ref.tmp260, i64 0, i32 0, i32 0, i32 0, i64 0
  call void @llvm.lifetime.start.p0i8(i64 24, i8* nonnull %255) #22
  %_M_head_impl.i.i.i.i.i.i.i991 = getelementptr inbounds %"class.std::unique_ptr.123", %"class.std::unique_ptr.123"* %jitter, i64 0, i32 0, i32 0, i32 0, i32 0, i32 0
  %256 = load %"class.llvm::orc::LLJIT"*, %"class.llvm::orc::LLJIT"** %_M_head_impl.i.i.i.i.i.i.i991, align 8, !tbaa !37
  %Main.i = getelementptr inbounds %"class.llvm::orc::LLJIT", %"class.llvm::orc::LLJIT"* %256, i64 0, i32 2
  %257 = load %"class.llvm::orc::JITDylib"*, %"class.llvm::orc::JITDylib"** %Main.i, align 8, !tbaa !47, !noalias !85
  invoke void @_ZN4llvm3orc5LLJIT6lookupERNS0_8JITDylibENS_9StringRefE(%"class.llvm::Expected"* nonnull sret(%"class.llvm::Expected") align 8 %ref.tmp260, %"class.llvm::orc::LLJIT"* nonnull dereferenceable(568) %256, %"class.llvm::orc::JITDylib"* nonnull align 8 dereferenceable(272) %257, i8* getelementptr inbounds ([28 x i8], [28 x i8]* @.str.22, i64 0, i64 0), i64 27)
          to label %invoke.cont265 unwind label %lpad263

invoke.cont265:                                   ; preds = %invoke.cont264
  %HasError.i.i = getelementptr inbounds %"class.llvm::Expected", %"class.llvm::Expected"* %ref.tmp260, i64 0, i32 1
  %bf.load.i.i = load i8, i8* %HasError.i.i, align 8
  %258 = and i8 %bf.load.i.i, 1
  %bf.cast.not.i.i = icmp eq i8 %258, 0
  br i1 %bf.cast.not.i.i, label %invoke.cont277, label %cond.false.i.i

cond.false.i.i:                                   ; preds = %invoke.cont265
  call void @__assert_fail(i8* getelementptr inbounds ([54 x i8], [54 x i8]* @.str.33, i64 0, i64 0), i8* getelementptr inbounds ([42 x i8], [42 x i8]* @.str.34, i64 0, i64 0), i32 631, i8* getelementptr inbounds ([116 x i8], [116 x i8]* @__PRETTY_FUNCTION__._ZN4llvm8ExpectedINS_18JITEvaluatedSymbolEE10getStorageEv, i64 0, i64 0)) #23
  unreachable

invoke.cont277:                                   ; preds = %invoke.cont265
  %Address.i = bitcast %"class.llvm::Expected"* %ref.tmp260 to i64*
  %259 = load i64, i64* %Address.i, align 8, !tbaa !88
  call void @llvm.lifetime.end.p0i8(i64 24, i8* nonnull %255) #22
  %260 = getelementptr inbounds %"class.llvm::Expected", %"class.llvm::Expected"* %ref.tmp273, i64 0, i32 0, i32 0, i32 0, i64 0
  call void @llvm.lifetime.start.p0i8(i64 24, i8* nonnull %260) #22
  %261 = load %"class.llvm::orc::LLJIT"*, %"class.llvm::orc::LLJIT"** %_M_head_impl.i.i.i.i.i.i.i991, align 8, !tbaa !37
  %Main.i1011 = getelementptr inbounds %"class.llvm::orc::LLJIT", %"class.llvm::orc::LLJIT"* %261, i64 0, i32 2
  %262 = load %"class.llvm::orc::JITDylib"*, %"class.llvm::orc::JITDylib"** %Main.i1011, align 8, !tbaa !47, !noalias !92
  invoke void @_ZN4llvm3orc5LLJIT6lookupERNS0_8JITDylibENS_9StringRefE(%"class.llvm::Expected"* nonnull sret(%"class.llvm::Expected") align 8 %ref.tmp273, %"class.llvm::orc::LLJIT"* nonnull dereferenceable(568) %261, %"class.llvm::orc::JITDylib"* nonnull align 8 dereferenceable(272) %262, i8* getelementptr inbounds ([25 x i8], [25 x i8]* @.str.23, i64 0, i64 0), i64 24)
          to label %invoke.cont278 unwind label %lpad276

invoke.cont278:                                   ; preds = %invoke.cont277
  %HasError.i.i1014 = getelementptr inbounds %"class.llvm::Expected", %"class.llvm::Expected"* %ref.tmp273, i64 0, i32 1
  %bf.load.i.i1015 = load i8, i8* %HasError.i.i1014, align 8
  %263 = and i8 %bf.load.i.i1015, 1
  %bf.cast.not.i.i1016 = icmp eq i8 %263, 0
  br i1 %bf.cast.not.i.i1016, label %invoke.cont290, label %cond.false.i.i1017

cond.false.i.i1017:                               ; preds = %invoke.cont278
  call void @__assert_fail(i8* getelementptr inbounds ([54 x i8], [54 x i8]* @.str.33, i64 0, i64 0), i8* getelementptr inbounds ([42 x i8], [42 x i8]* @.str.34, i64 0, i64 0), i32 631, i8* getelementptr inbounds ([116 x i8], [116 x i8]* @__PRETTY_FUNCTION__._ZN4llvm8ExpectedINS_18JITEvaluatedSymbolEE10getStorageEv, i64 0, i64 0)) #23
  unreachable

invoke.cont290:                                   ; preds = %invoke.cont278
  %Address.i1019 = bitcast %"class.llvm::Expected"* %ref.tmp273 to i64*
  %264 = load i64, i64* %Address.i1019, align 8, !tbaa !88
  call void @llvm.lifetime.end.p0i8(i64 24, i8* nonnull %260) #22
  %265 = getelementptr inbounds %"class.llvm::Expected", %"class.llvm::Expected"* %ref.tmp286, i64 0, i32 0, i32 0, i32 0, i64 0
  call void @llvm.lifetime.start.p0i8(i64 24, i8* nonnull %265) #22
  %266 = load %"class.llvm::orc::LLJIT"*, %"class.llvm::orc::LLJIT"** %_M_head_impl.i.i.i.i.i.i.i991, align 8, !tbaa !37
  %Main.i1035 = getelementptr inbounds %"class.llvm::orc::LLJIT", %"class.llvm::orc::LLJIT"* %266, i64 0, i32 2
  %267 = load %"class.llvm::orc::JITDylib"*, %"class.llvm::orc::JITDylib"** %Main.i1035, align 8, !tbaa !47, !noalias !95
  invoke void @_ZN4llvm3orc5LLJIT6lookupERNS0_8JITDylibENS_9StringRefE(%"class.llvm::Expected"* nonnull sret(%"class.llvm::Expected") align 8 %ref.tmp286, %"class.llvm::orc::LLJIT"* nonnull dereferenceable(568) %266, %"class.llvm::orc::JITDylib"* nonnull align 8 dereferenceable(272) %267, i8* getelementptr inbounds ([24 x i8], [24 x i8]* @.str.24, i64 0, i64 0), i64 23)
          to label %invoke.cont291 unwind label %lpad289

invoke.cont291:                                   ; preds = %invoke.cont290
  %HasError.i.i1038 = getelementptr inbounds %"class.llvm::Expected", %"class.llvm::Expected"* %ref.tmp286, i64 0, i32 1
  %bf.load.i.i1039 = load i8, i8* %HasError.i.i1038, align 8
  %268 = and i8 %bf.load.i.i1039, 1
  %bf.cast.not.i.i1040 = icmp eq i8 %268, 0
  br i1 %bf.cast.not.i.i1040, label %invoke.cont303, label %cond.false.i.i1041

cond.false.i.i1041:                               ; preds = %invoke.cont291
  call void @__assert_fail(i8* getelementptr inbounds ([54 x i8], [54 x i8]* @.str.33, i64 0, i64 0), i8* getelementptr inbounds ([42 x i8], [42 x i8]* @.str.34, i64 0, i64 0), i32 631, i8* getelementptr inbounds ([116 x i8], [116 x i8]* @__PRETTY_FUNCTION__._ZN4llvm8ExpectedINS_18JITEvaluatedSymbolEE10getStorageEv, i64 0, i64 0)) #23
  unreachable

invoke.cont303:                                   ; preds = %invoke.cont291
  %Address.i1043 = bitcast %"class.llvm::Expected"* %ref.tmp286 to i64*
  %269 = load i64, i64* %Address.i1043, align 8, !tbaa !88
  call void @llvm.lifetime.end.p0i8(i64 24, i8* nonnull %265) #22
  %270 = getelementptr inbounds %"class.llvm::Expected", %"class.llvm::Expected"* %ref.tmp299, i64 0, i32 0, i32 0, i32 0, i64 0
  call void @llvm.lifetime.start.p0i8(i64 24, i8* nonnull %270) #22
  %271 = load %"class.llvm::orc::LLJIT"*, %"class.llvm::orc::LLJIT"** %_M_head_impl.i.i.i.i.i.i.i991, align 8, !tbaa !37
  %Main.i1064 = getelementptr inbounds %"class.llvm::orc::LLJIT", %"class.llvm::orc::LLJIT"* %271, i64 0, i32 2
  %272 = load %"class.llvm::orc::JITDylib"*, %"class.llvm::orc::JITDylib"** %Main.i1064, align 8, !tbaa !47, !noalias !98
  invoke void @_ZN4llvm3orc5LLJIT6lookupERNS0_8JITDylibENS_9StringRefE(%"class.llvm::Expected"* nonnull sret(%"class.llvm::Expected") align 8 %ref.tmp299, %"class.llvm::orc::LLJIT"* nonnull dereferenceable(568) %271, %"class.llvm::orc::JITDylib"* nonnull align 8 dereferenceable(272) %272, i8* getelementptr inbounds ([22 x i8], [22 x i8]* @.str.25, i64 0, i64 0), i64 21)
          to label %invoke.cont304 unwind label %lpad302

invoke.cont304:                                   ; preds = %invoke.cont303
  %HasError.i.i1067 = getelementptr inbounds %"class.llvm::Expected", %"class.llvm::Expected"* %ref.tmp299, i64 0, i32 1
  %bf.load.i.i1068 = load i8, i8* %HasError.i.i1067, align 8
  %273 = and i8 %bf.load.i.i1068, 1
  %bf.cast.not.i.i1069 = icmp eq i8 %273, 0
  br i1 %bf.cast.not.i.i1069, label %_ZN4llvm8ExpectedINS_18JITEvaluatedSymbolEED2Ev.exit1088, label %cond.false.i.i1070

cond.false.i.i1070:                               ; preds = %invoke.cont304
  call void @__assert_fail(i8* getelementptr inbounds ([54 x i8], [54 x i8]* @.str.33, i64 0, i64 0), i8* getelementptr inbounds ([42 x i8], [42 x i8]* @.str.34, i64 0, i64 0), i32 631, i8* getelementptr inbounds ([116 x i8], [116 x i8]* @__PRETTY_FUNCTION__._ZN4llvm8ExpectedINS_18JITEvaluatedSymbolEE10getStorageEv, i64 0, i64 0)) #23
  unreachable

_ZN4llvm8ExpectedINS_18JITEvaluatedSymbolEED2Ev.exit1088: ; preds = %invoke.cont304
  %Address.i1072 = bitcast %"class.llvm::Expected"* %ref.tmp299 to i64*
  %274 = load i64, i64* %Address.i1072, align 8, !tbaa !88
  call void @llvm.lifetime.end.p0i8(i64 24, i8* nonnull %270) #22
  %call314 = invoke noalias nonnull dereferenceable(40) i8* @_Znwm(i64 40) #25
          to label %_ZNSt10unique_ptrIN4llvm3orc5LLJITESt14default_deleteIS2_EED2Ev.exit1098 unwind label %lpad312

_ZNSt10unique_ptrIN4llvm3orc5LLJITESt14default_deleteIS2_EED2Ev.exit1098: ; preds = %_ZN4llvm8ExpectedINS_18JITEvaluatedSymbolEED2Ev.exit1088
  %275 = inttoptr i64 %274 to void (i64, i32*, i32, i64, i32*, i32, i32)*
  %276 = inttoptr i64 %269 to void (i64, i32*, i32*, i32, i32)*
  %277 = inttoptr i64 %264 to void (i64, i32*, i32*, i32*, i32*, i32, i32, i32)*
  %278 = inttoptr i64 %259 to i64 (i32*, i32, i32*, i32, i32*, i32*, i32*, i32)*
  %createSortFunc315 = getelementptr inbounds i8, i8* %call314, i64 8
  %279 = bitcast i8* %createSortFunc315 to i64 (i32*, i32, i32*, i32, i32*, i32*, i32*, i32)**
  store i64 (i32*, i32, i32*, i32, i32*, i32*, i32*, i32)* %278, i64 (i32*, i32, i32*, i32, i32*, i32*, i32*, i32)** %279, align 8, !tbaa !101
  %sortFunc316 = getelementptr inbounds i8, i8* %call314, i64 16
  %280 = bitcast i8* %sortFunc316 to void (i64, i32*, i32*, i32*, i32*, i32, i32, i32)**
  store void (i64, i32*, i32*, i32*, i32*, i32, i32, i32)* %277, void (i64, i32*, i32*, i32*, i32*, i32, i32, i32)** %280, align 8, !tbaa !103
  %allocColumnsFunc317 = getelementptr inbounds i8, i8* %call314, i64 24
  %281 = bitcast i8* %allocColumnsFunc317 to void (i64, i32*, i32*, i32, i32)**
  store void (i64, i32*, i32*, i32, i32)* %276, void (i64, i32*, i32*, i32, i32)** %281, align 8, !tbaa !104
  %getResultFunc318 = getelementptr inbounds i8, i8* %call314, i64 32
  %282 = bitcast i8* %getResultFunc318 to void (i64, i32*, i32, i64, i32*, i32, i32)**
  store void (i64, i32*, i32, i64, i32*, i32, i32)* %275, void (i64, i32*, i32, i64, i32*, i32, i32)** %282, align 8, !tbaa !105
  %283 = load %"class.llvm::orc::LLJIT"*, %"class.llvm::orc::LLJIT"** %_M_head_impl.i.i.i.i.i.i.i991, align 8, !tbaa !37
  %jitter320 = bitcast i8* %call314 to %"class.llvm::orc::LLJIT"**
  store %"class.llvm::orc::LLJIT"* %283, %"class.llvm::orc::LLJIT"** %jitter320, align 8, !tbaa !106
  call void @llvm.lifetime.end.p0i8(i64 8, i8* nonnull %238) #22
  call void @llvm.lifetime.end.p0i8(i64 1616, i8* nonnull %236) #22
  call void @_ZN7codegen6HammerD2Ev(%"class.codegen::Hammer"* nonnull dereferenceable(152) %hammer2) #22
  call void @llvm.lifetime.end.p0i8(i64 152, i8* nonnull %213) #22
  call void @_ZN7codegen6HammerD2Ev(%"class.codegen::Hammer"* nonnull dereferenceable(152) %hammer1) #22
  call void @llvm.lifetime.end.p0i8(i64 152, i8* nonnull %193) #22
  call void @llvm.lifetime.end.p0i8(i64 24, i8* nonnull %31) #22
  call void @llvm.lifetime.end.p0i8(i64 24, i8* nonnull %29) #22
  call void @llvm.lifetime.end.p0i8(i64 24, i8* nonnull %27) #22
  call void @llvm.lifetime.end.p0i8(i64 24, i8* nonnull %25) #22
  call void @llvm.lifetime.end.p0i8(i64 24, i8* nonnull %23) #22
  call void @llvm.lifetime.end.p0i8(i64 24, i8* nonnull %21) #22
  call void @llvm.lifetime.end.p0i8(i64 24, i8* nonnull %19) #22
  call void @llvm.lifetime.end.p0i8(i64 24, i8* nonnull %17) #22
  call void @llvm.lifetime.end.p0i8(i64 24, i8* nonnull %15) #22
  call void @llvm.stackrestore(i8* %10)
  %284 = load %"struct.std::__detail::_List_node_base"*, %"struct.std::__detail::_List_node_base"** %_M_next.i.i.i, align 8, !tbaa !16
  %cmp.not16.i.i1100 = icmp eq %"struct.std::__detail::_List_node_base"* %284, %7
  br i1 %cmp.not16.i.i1100, label %_ZNSt7__cxx1110_List_baseIPN7codegen6HammerESaIS3_EED2Ev.exit1105, label %invoke.cont6.i.i1104

invoke.cont6.i.i1104:                             ; preds = %_ZNSt10unique_ptrIN4llvm3orc5LLJITESt14default_deleteIS2_EED2Ev.exit1098, %invoke.cont6.i.i1104
  %__cur.017.i.i1101 = phi %"struct.std::__detail::_List_node_base"* [ %285, %invoke.cont6.i.i1104 ], [ %284, %_ZNSt10unique_ptrIN4llvm3orc5LLJITESt14default_deleteIS2_EED2Ev.exit1098 ]
  %_M_next4.i.i1102 = getelementptr inbounds %"struct.std::__detail::_List_node_base", %"struct.std::__detail::_List_node_base"* %__cur.017.i.i1101, i64 0, i32 0
  %285 = load %"struct.std::__detail::_List_node_base"*, %"struct.std::__detail::_List_node_base"** %_M_next4.i.i1102, align 8, !tbaa !16
  %286 = bitcast %"struct.std::__detail::_List_node_base"* %__cur.017.i.i1101 to i8*
  call void @_ZdlPv(i8* %286) #22
  %cmp.not.i.i1103 = icmp eq %"struct.std::__detail::_List_node_base"* %285, %7
  br i1 %cmp.not.i.i1103, label %_ZNSt7__cxx1110_List_baseIPN7codegen6HammerESaIS3_EED2Ev.exit1105, label %invoke.cont6.i.i1104, !llvm.loop !46

_ZNSt7__cxx1110_List_baseIPN7codegen6HammerESaIS3_EED2Ev.exit1105: ; preds = %invoke.cont6.i.i1104, %_ZNSt10unique_ptrIN4llvm3orc5LLJITESt14default_deleteIS2_EED2Ev.exit1098
  call void @llvm.lifetime.end.p0i8(i64 24, i8* nonnull %6) #22
  %_M_t.i1106 = getelementptr inbounds %"class.std::map", %"class.std::map"* %testParam, i64 0, i32 0
  %287 = bitcast i8* %_M_parent.i.i.i.i.i to %"struct.std::_Rb_tree_node"**
  %288 = load %"struct.std::_Rb_tree_node"*, %"struct.std::_Rb_tree_node"** %287, align 8, !tbaa !12
  invoke void @_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE8_M_eraseEPSt13_Rb_tree_nodeISB_E(%"class.std::_Rb_tree"* nonnull dereferenceable(48) %_M_t.i1106, %"struct.std::_Rb_tree_node"* %288)
          to label %_ZNSt3mapINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEPN7codegen10ParamValueESt4lessIS5_ESaISt4pairIKS5_S8_EEED2Ev.exit1109 unwind label %lpad.i.i1108

lpad.i.i1108:                                     ; preds = %_ZNSt7__cxx1110_List_baseIPN7codegen6HammerESaIS3_EED2Ev.exit1105
  %289 = landingpad { i8*, i32 }
          catch i8* null
  %290 = extractvalue { i8*, i32 } %289, 0
  call void @__clang_call_terminate(i8* %290) #23
  unreachable

_ZNSt3mapINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEPN7codegen10ParamValueESt4lessIS5_ESaISt4pairIKS5_S8_EEED2Ev.exit1109: ; preds = %_ZNSt7__cxx1110_List_baseIPN7codegen6HammerESaIS3_EED2Ev.exit1105
  %291 = ptrtoint i8* %call314 to i64
  call void @llvm.lifetime.end.p0i8(i64 48, i8* nonnull %0) #22
  ret i64 %291

lpad19:                                           ; preds = %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit929
  %292 = landingpad { i8*, i32 }
          cleanup
  br label %ehcleanup333

lpad22:                                           ; preds = %for.cond.cleanup
  %293 = landingpad { i8*, i32 }
          cleanup
  br label %ehcleanup

lpad24:                                           ; preds = %call5.i.i.i10.i.noexc
  %294 = landingpad { i8*, i32 }
          cleanup
  %295 = load i8*, i8** %_M_p.i13.i.i.i.i, align 8, !tbaa !31
  %cmp.i.i.i1112 = icmp eq i8* %295, %36
  br i1 %cmp.i.i.i1112, label %ehcleanup, label %if.then.i.i1113

if.then.i.i1113:                                  ; preds = %lpad24
  call void @_ZdlPv(i8* %295) #22
  br label %ehcleanup

ehcleanup:                                        ; preds = %if.then.i.i1113, %lpad24, %lpad22
  %.pn = phi { i8*, i32 } [ %293, %lpad22 ], [ %294, %lpad24 ], [ %294, %if.then.i.i1113 ]
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %33) #22
  br label %ehcleanup333

lpad30:                                           ; preds = %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit
  %296 = landingpad { i8*, i32 }
          cleanup
  br label %ehcleanup36

lpad32:                                           ; preds = %call5.i.i.i10.i.noexc508
  %297 = landingpad { i8*, i32 }
          cleanup
  %298 = load i8*, i8** %_M_p.i13.i.i.i.i499, align 8, !tbaa !31
  %cmp.i.i.i1117 = icmp eq i8* %298, %52
  br i1 %cmp.i.i.i1117, label %ehcleanup36, label %if.then.i.i1118

if.then.i.i1118:                                  ; preds = %lpad32
  call void @_ZdlPv(i8* %298) #22
  br label %ehcleanup36

ehcleanup36:                                      ; preds = %if.then.i.i1118, %lpad32, %lpad30
  %.pn372 = phi { i8*, i32 } [ %296, %lpad30 ], [ %297, %lpad32 ], [ %297, %if.then.i.i1118 ]
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %49) #22
  br label %ehcleanup333

lpad41:                                           ; preds = %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit515
  %299 = landingpad { i8*, i32 }
          cleanup
  br label %ehcleanup47

lpad43:                                           ; preds = %call5.i.i.i10.i.noexc528
  %300 = landingpad { i8*, i32 }
          cleanup
  %301 = load i8*, i8** %_M_p.i13.i.i.i.i519, align 8, !tbaa !31
  %cmp.i.i.i1092 = icmp eq i8* %301, %60
  br i1 %cmp.i.i.i1092, label %ehcleanup47, label %if.then.i.i1093

if.then.i.i1093:                                  ; preds = %lpad43
  call void @_ZdlPv(i8* %301) #22
  br label %ehcleanup47

ehcleanup47:                                      ; preds = %if.then.i.i1093, %lpad43, %lpad41
  %.pn374 = phi { i8*, i32 } [ %299, %lpad41 ], [ %300, %lpad43 ], [ %300, %if.then.i.i1093 ]
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %57) #22
  br label %ehcleanup333

lpad52:                                           ; preds = %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit535
  %302 = landingpad { i8*, i32 }
          cleanup
  br label %ehcleanup58

lpad54:                                           ; preds = %call5.i.i.i10.i.noexc552
  %303 = landingpad { i8*, i32 }
          cleanup
  %304 = load i8*, i8** %_M_p.i13.i.i.i.i543, align 8, !tbaa !31
  %cmp.i.i.i1075 = icmp eq i8* %304, %68
  br i1 %cmp.i.i.i1075, label %ehcleanup58, label %if.then.i.i1076

if.then.i.i1076:                                  ; preds = %lpad54
  call void @_ZdlPv(i8* %304) #22
  br label %ehcleanup58

ehcleanup58:                                      ; preds = %if.then.i.i1076, %lpad54, %lpad52
  %.pn376 = phi { i8*, i32 } [ %302, %lpad52 ], [ %303, %lpad54 ], [ %303, %if.then.i.i1076 ]
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %65) #22
  br label %ehcleanup333

lpad63:                                           ; preds = %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit559
  %305 = landingpad { i8*, i32 }
          cleanup
  br label %ehcleanup69

lpad65:                                           ; preds = %call5.i.i.i10.i.noexc572
  %306 = landingpad { i8*, i32 }
          cleanup
  %307 = load i8*, i8** %_M_p.i13.i.i.i.i563, align 8, !tbaa !31
  %cmp.i.i.i1058 = icmp eq i8* %307, %76
  br i1 %cmp.i.i.i1058, label %ehcleanup69, label %if.then.i.i1059

if.then.i.i1059:                                  ; preds = %lpad65
  call void @_ZdlPv(i8* %307) #22
  br label %ehcleanup69

ehcleanup69:                                      ; preds = %if.then.i.i1059, %lpad65, %lpad63
  %.pn378 = phi { i8*, i32 } [ %305, %lpad63 ], [ %306, %lpad65 ], [ %306, %if.then.i.i1059 ]
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %73) #22
  br label %ehcleanup333

lpad74:                                           ; preds = %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit579
  %308 = landingpad { i8*, i32 }
          cleanup
  br label %ehcleanup80

lpad76:                                           ; preds = %call5.i.i.i10.i.noexc597
  %309 = landingpad { i8*, i32 }
          cleanup
  %310 = load i8*, i8** %_M_p.i13.i.i.i.i588, align 8, !tbaa !31
  %cmp.i.i.i907 = icmp eq i8* %310, %84
  br i1 %cmp.i.i.i907, label %ehcleanup80, label %if.then.i.i908

if.then.i.i908:                                   ; preds = %lpad76
  call void @_ZdlPv(i8* %310) #22
  br label %ehcleanup80

ehcleanup80:                                      ; preds = %if.then.i.i908, %lpad76, %lpad74
  %.pn380 = phi { i8*, i32 } [ %308, %lpad74 ], [ %309, %lpad76 ], [ %309, %if.then.i.i908 ]
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %81) #22
  br label %ehcleanup333

lpad85:                                           ; preds = %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit604
  %311 = landingpad { i8*, i32 }
          cleanup
  br label %ehcleanup91

lpad87:                                           ; preds = %call5.i.i.i10.i.noexc622
  %312 = landingpad { i8*, i32 }
          cleanup
  %313 = load i8*, i8** %_M_p.i13.i.i.i.i613, align 8, !tbaa !31
  %cmp.i.i.i882 = icmp eq i8* %313, %92
  br i1 %cmp.i.i.i882, label %ehcleanup91, label %if.then.i.i883

if.then.i.i883:                                   ; preds = %lpad87
  call void @_ZdlPv(i8* %313) #22
  br label %ehcleanup91

ehcleanup91:                                      ; preds = %if.then.i.i883, %lpad87, %lpad85
  %.pn382 = phi { i8*, i32 } [ %311, %lpad85 ], [ %312, %lpad87 ], [ %312, %if.then.i.i883 ]
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %89) #22
  br label %ehcleanup333

lpad96:                                           ; preds = %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit629
  %314 = landingpad { i8*, i32 }
          cleanup
  br label %ehcleanup102

lpad98:                                           ; preds = %call5.i.i.i10.i.noexc647
  %315 = landingpad { i8*, i32 }
          cleanup
  %316 = load i8*, i8** %_M_p.i13.i.i.i.i638, align 8, !tbaa !31
  %cmp.i.i.i857 = icmp eq i8* %316, %100
  br i1 %cmp.i.i.i857, label %ehcleanup102, label %if.then.i.i858

if.then.i.i858:                                   ; preds = %lpad98
  call void @_ZdlPv(i8* %316) #22
  br label %ehcleanup102

ehcleanup102:                                     ; preds = %if.then.i.i858, %lpad98, %lpad96
  %.pn384 = phi { i8*, i32 } [ %314, %lpad96 ], [ %315, %lpad98 ], [ %315, %if.then.i.i858 ]
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %97) #22
  br label %ehcleanup333

lpad107:                                          ; preds = %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit654
  %317 = landingpad { i8*, i32 }
          cleanup
  br label %ehcleanup113

lpad109:                                          ; preds = %call5.i.i.i10.i.noexc672
  %318 = landingpad { i8*, i32 }
          cleanup
  %319 = load i8*, i8** %_M_p.i13.i.i.i.i663, align 8, !tbaa !31
  %cmp.i.i.i832 = icmp eq i8* %319, %108
  br i1 %cmp.i.i.i832, label %ehcleanup113, label %if.then.i.i833

if.then.i.i833:                                   ; preds = %lpad109
  call void @_ZdlPv(i8* %319) #22
  br label %ehcleanup113

ehcleanup113:                                     ; preds = %if.then.i.i833, %lpad109, %lpad107
  %.pn386 = phi { i8*, i32 } [ %317, %lpad107 ], [ %318, %lpad109 ], [ %318, %if.then.i.i833 ]
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %105) #22
  br label %ehcleanup333

lpad118:                                          ; preds = %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit679
  %320 = landingpad { i8*, i32 }
          cleanup
  br label %ehcleanup124

lpad120:                                          ; preds = %call5.i.i.i10.i.noexc697
  %321 = landingpad { i8*, i32 }
          cleanup
  %322 = load i8*, i8** %_M_p.i13.i.i.i.i688, align 8, !tbaa !31
  %cmp.i.i.i807 = icmp eq i8* %322, %116
  br i1 %cmp.i.i.i807, label %ehcleanup124, label %if.then.i.i808

if.then.i.i808:                                   ; preds = %lpad120
  call void @_ZdlPv(i8* %322) #22
  br label %ehcleanup124

ehcleanup124:                                     ; preds = %if.then.i.i808, %lpad120, %lpad118
  %.pn388 = phi { i8*, i32 } [ %320, %lpad118 ], [ %321, %lpad120 ], [ %321, %if.then.i.i808 ]
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %113) #22
  br label %ehcleanup333

lpad129:                                          ; preds = %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit704
  %323 = landingpad { i8*, i32 }
          cleanup
  br label %ehcleanup135

lpad131:                                          ; preds = %call5.i.i.i10.i.noexc722
  %324 = landingpad { i8*, i32 }
          cleanup
  %325 = load i8*, i8** %_M_p.i13.i.i.i.i713, align 8, !tbaa !31
  %cmp.i.i.i782 = icmp eq i8* %325, %124
  br i1 %cmp.i.i.i782, label %ehcleanup135, label %if.then.i.i783

if.then.i.i783:                                   ; preds = %lpad131
  call void @_ZdlPv(i8* %325) #22
  br label %ehcleanup135

ehcleanup135:                                     ; preds = %if.then.i.i783, %lpad131, %lpad129
  %.pn390 = phi { i8*, i32 } [ %323, %lpad129 ], [ %324, %lpad131 ], [ %324, %if.then.i.i783 ]
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %121) #22
  br label %ehcleanup333

lpad140:                                          ; preds = %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit729
  %326 = landingpad { i8*, i32 }
          cleanup
  br label %ehcleanup146

lpad142:                                          ; preds = %call5.i.i.i10.i.noexc747
  %327 = landingpad { i8*, i32 }
          cleanup
  %328 = load i8*, i8** %_M_p.i13.i.i.i.i738, align 8, !tbaa !31
  %cmp.i.i.i757 = icmp eq i8* %328, %132
  br i1 %cmp.i.i.i757, label %ehcleanup146, label %if.then.i.i758

if.then.i.i758:                                   ; preds = %lpad142
  call void @_ZdlPv(i8* %328) #22
  br label %ehcleanup146

ehcleanup146:                                     ; preds = %if.then.i.i758, %lpad142, %lpad140
  %.pn392 = phi { i8*, i32 } [ %326, %lpad140 ], [ %327, %lpad142 ], [ %327, %if.then.i.i758 ]
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %129) #22
  br label %ehcleanup333

lpad151:                                          ; preds = %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit754
  %329 = landingpad { i8*, i32 }
          cleanup
  br label %ehcleanup157

lpad153:                                          ; preds = %call5.i.i.i10.i.noexc772
  %330 = landingpad { i8*, i32 }
          cleanup
  %331 = load i8*, i8** %_M_p.i13.i.i.i.i763, align 8, !tbaa !31
  %cmp.i.i.i732 = icmp eq i8* %331, %140
  br i1 %cmp.i.i.i732, label %ehcleanup157, label %if.then.i.i733

if.then.i.i733:                                   ; preds = %lpad153
  call void @_ZdlPv(i8* %331) #22
  br label %ehcleanup157

ehcleanup157:                                     ; preds = %if.then.i.i733, %lpad153, %lpad151
  %.pn394 = phi { i8*, i32 } [ %329, %lpad151 ], [ %330, %lpad153 ], [ %330, %if.then.i.i733 ]
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %137) #22
  br label %ehcleanup333

lpad162:                                          ; preds = %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit779
  %332 = landingpad { i8*, i32 }
          cleanup
  br label %ehcleanup168

lpad164:                                          ; preds = %call5.i.i.i10.i.noexc797
  %333 = landingpad { i8*, i32 }
          cleanup
  %334 = load i8*, i8** %_M_p.i13.i.i.i.i788, align 8, !tbaa !31
  %cmp.i.i.i707 = icmp eq i8* %334, %148
  br i1 %cmp.i.i.i707, label %ehcleanup168, label %if.then.i.i708

if.then.i.i708:                                   ; preds = %lpad164
  call void @_ZdlPv(i8* %334) #22
  br label %ehcleanup168

ehcleanup168:                                     ; preds = %if.then.i.i708, %lpad164, %lpad162
  %.pn396 = phi { i8*, i32 } [ %332, %lpad162 ], [ %333, %lpad164 ], [ %333, %if.then.i.i708 ]
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %145) #22
  br label %ehcleanup333

lpad173:                                          ; preds = %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit804
  %335 = landingpad { i8*, i32 }
          cleanup
  br label %ehcleanup179

lpad175:                                          ; preds = %call5.i.i.i10.i.noexc822
  %336 = landingpad { i8*, i32 }
          cleanup
  %337 = load i8*, i8** %_M_p.i13.i.i.i.i813, align 8, !tbaa !31
  %cmp.i.i.i682 = icmp eq i8* %337, %156
  br i1 %cmp.i.i.i682, label %ehcleanup179, label %if.then.i.i683

if.then.i.i683:                                   ; preds = %lpad175
  call void @_ZdlPv(i8* %337) #22
  br label %ehcleanup179

ehcleanup179:                                     ; preds = %if.then.i.i683, %lpad175, %lpad173
  %.pn398 = phi { i8*, i32 } [ %335, %lpad173 ], [ %336, %lpad175 ], [ %336, %if.then.i.i683 ]
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %153) #22
  br label %ehcleanup333

lpad184:                                          ; preds = %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit829
  %338 = landingpad { i8*, i32 }
          cleanup
  br label %ehcleanup190

lpad186:                                          ; preds = %call5.i.i.i10.i.noexc847
  %339 = landingpad { i8*, i32 }
          cleanup
  %340 = load i8*, i8** %_M_p.i13.i.i.i.i838, align 8, !tbaa !31
  %cmp.i.i.i657 = icmp eq i8* %340, %164
  br i1 %cmp.i.i.i657, label %ehcleanup190, label %if.then.i.i658

if.then.i.i658:                                   ; preds = %lpad186
  call void @_ZdlPv(i8* %340) #22
  br label %ehcleanup190

ehcleanup190:                                     ; preds = %if.then.i.i658, %lpad186, %lpad184
  %.pn400 = phi { i8*, i32 } [ %338, %lpad184 ], [ %339, %lpad186 ], [ %339, %if.then.i.i658 ]
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %161) #22
  br label %ehcleanup333

lpad195:                                          ; preds = %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit854
  %341 = landingpad { i8*, i32 }
          cleanup
  br label %ehcleanup201

lpad197:                                          ; preds = %call5.i.i.i10.i.noexc872
  %342 = landingpad { i8*, i32 }
          cleanup
  %343 = load i8*, i8** %_M_p.i13.i.i.i.i863, align 8, !tbaa !31
  %cmp.i.i.i632 = icmp eq i8* %343, %172
  br i1 %cmp.i.i.i632, label %ehcleanup201, label %if.then.i.i633

if.then.i.i633:                                   ; preds = %lpad197
  call void @_ZdlPv(i8* %343) #22
  br label %ehcleanup201

ehcleanup201:                                     ; preds = %if.then.i.i633, %lpad197, %lpad195
  %.pn402 = phi { i8*, i32 } [ %341, %lpad195 ], [ %342, %lpad197 ], [ %342, %if.then.i.i633 ]
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %169) #22
  br label %ehcleanup333

lpad206:                                          ; preds = %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit879
  %344 = landingpad { i8*, i32 }
          cleanup
  br label %ehcleanup212

lpad208:                                          ; preds = %call5.i.i.i10.i.noexc897
  %345 = landingpad { i8*, i32 }
          cleanup
  %346 = load i8*, i8** %_M_p.i13.i.i.i.i888, align 8, !tbaa !31
  %cmp.i.i.i607 = icmp eq i8* %346, %180
  br i1 %cmp.i.i.i607, label %ehcleanup212, label %if.then.i.i608

if.then.i.i608:                                   ; preds = %lpad208
  call void @_ZdlPv(i8* %346) #22
  br label %ehcleanup212

ehcleanup212:                                     ; preds = %if.then.i.i608, %lpad208, %lpad206
  %.pn404 = phi { i8*, i32 } [ %344, %lpad206 ], [ %345, %lpad208 ], [ %345, %if.then.i.i608 ]
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %177) #22
  br label %ehcleanup333

lpad217:                                          ; preds = %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit904
  %347 = landingpad { i8*, i32 }
          cleanup
  br label %ehcleanup223

lpad219:                                          ; preds = %call5.i.i.i10.i.noexc922
  %348 = landingpad { i8*, i32 }
          cleanup
  %349 = load i8*, i8** %_M_p.i13.i.i.i.i913, align 8, !tbaa !31
  %cmp.i.i.i582 = icmp eq i8* %349, %188
  br i1 %cmp.i.i.i582, label %ehcleanup223, label %if.then.i.i583

if.then.i.i583:                                   ; preds = %lpad219
  call void @_ZdlPv(i8* %349) #22
  br label %ehcleanup223

ehcleanup223:                                     ; preds = %if.then.i.i583, %lpad219, %lpad217
  %.pn406 = phi { i8*, i32 } [ %347, %lpad217 ], [ %348, %lpad219 ], [ %348, %if.then.i.i583 ]
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %185) #22
  br label %ehcleanup333

lpad228:                                          ; preds = %if.then.i.i939
  %350 = landingpad { i8*, i32 }
          cleanup
  br label %ehcleanup332

lpad232:                                          ; preds = %invoke.cont231
  %351 = landingpad { i8*, i32 }
          cleanup
  %352 = bitcast i8* %_M_parent.i.i.i.i.i934 to %"struct.std::_Rb_tree_node"**
  %353 = load %"struct.std::_Rb_tree_node"*, %"struct.std::_Rb_tree_node"** %352, align 8, !tbaa !12
  invoke void @_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE8_M_eraseEPSt13_Rb_tree_nodeISB_E(%"class.std::_Rb_tree"* nonnull dereferenceable(48) %_M_t.i932, %"struct.std::_Rb_tree_node"* %353)
          to label %ehcleanup332 unwind label %lpad.i.i538

lpad.i.i538:                                      ; preds = %lpad232
  %354 = landingpad { i8*, i32 }
          catch i8* null
  %355 = extractvalue { i8*, i32 } %354, 0
  call void @__clang_call_terminate(i8* %355) #23
  unreachable

lpad236:                                          ; preds = %if.then.i.i959
  %356 = landingpad { i8*, i32 }
          cleanup
  br label %ehcleanup330

lpad240:                                          ; preds = %invoke.cont239
  %357 = landingpad { i8*, i32 }
          cleanup
  %358 = bitcast i8* %_M_parent.i.i.i.i.i951 to %"struct.std::_Rb_tree_node"**
  %359 = load %"struct.std::_Rb_tree_node"*, %"struct.std::_Rb_tree_node"** %358, align 8, !tbaa !12
  invoke void @_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE8_M_eraseEPSt13_Rb_tree_nodeISB_E(%"class.std::_Rb_tree"* nonnull dereferenceable(48) %_M_t.i949, %"struct.std::_Rb_tree_node"* %359)
          to label %ehcleanup330 unwind label %lpad.i.i494

lpad.i.i494:                                      ; preds = %lpad240
  %360 = landingpad { i8*, i32 }
          catch i8* null
  %361 = extractvalue { i8*, i32 } %360, 0
  call void @__clang_call_terminate(i8* %361) #23
  unreachable

lpad243:                                          ; preds = %invoke.cont244, %_ZNSt3mapINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEPN7codegen10ParamValueESt4lessIS5_ESaISt4pairIKS5_S8_EEED2Ev.exit977
  %362 = landingpad { i8*, i32 }
          cleanup
  br label %ehcleanup329

lpad249:                                          ; preds = %invoke.cont246
  %363 = landingpad { i8*, i32 }
          cleanup
  br label %ehcleanup329

lpad252:                                          ; preds = %.noexc, %invoke.cont250
  %364 = landingpad { i8*, i32 }
          cleanup
  br label %ehcleanup328

lpad257:                                          ; preds = %invoke.cont256
  %365 = landingpad { i8*, i32 }
          cleanup
  %366 = load %"struct.std::__detail::_List_node_base"*, %"struct.std::__detail::_List_node_base"** %_M_next.i.i14.i, align 8, !tbaa !16
  %cmp.not16.i.i485 = icmp eq %"struct.std::__detail::_List_node_base"* %366, %239
  br i1 %cmp.not16.i.i485, label %ehcleanup327, label %invoke.cont6.i.i489

invoke.cont6.i.i489:                              ; preds = %lpad257, %invoke.cont6.i.i489
  %__cur.017.i.i486 = phi %"struct.std::__detail::_List_node_base"* [ %367, %invoke.cont6.i.i489 ], [ %366, %lpad257 ]
  %_M_next4.i.i487 = getelementptr inbounds %"struct.std::__detail::_List_node_base", %"struct.std::__detail::_List_node_base"* %__cur.017.i.i486, i64 0, i32 0
  %367 = load %"struct.std::__detail::_List_node_base"*, %"struct.std::__detail::_List_node_base"** %_M_next4.i.i487, align 8, !tbaa !16
  %368 = bitcast %"struct.std::__detail::_List_node_base"* %__cur.017.i.i486 to i8*
  call void @_ZdlPv(i8* %368) #22
  %cmp.not.i.i488 = icmp eq %"struct.std::__detail::_List_node_base"* %367, %239
  br i1 %cmp.not.i.i488, label %ehcleanup327, label %invoke.cont6.i.i489, !llvm.loop !46

lpad263:                                          ; preds = %invoke.cont264
  %369 = landingpad { i8*, i32 }
          cleanup
  call void @llvm.lifetime.end.p0i8(i64 24, i8* nonnull %255) #22
  br label %ehcleanup325

lpad276:                                          ; preds = %invoke.cont277
  %370 = landingpad { i8*, i32 }
          cleanup
  call void @llvm.lifetime.end.p0i8(i64 24, i8* nonnull %260) #22
  br label %ehcleanup325

lpad289:                                          ; preds = %invoke.cont290
  %371 = landingpad { i8*, i32 }
          cleanup
  call void @llvm.lifetime.end.p0i8(i64 24, i8* nonnull %265) #22
  br label %ehcleanup325

lpad302:                                          ; preds = %invoke.cont303
  %372 = landingpad { i8*, i32 }
          cleanup
  call void @llvm.lifetime.end.p0i8(i64 24, i8* nonnull %270) #22
  br label %ehcleanup325

lpad312:                                          ; preds = %_ZN4llvm8ExpectedINS_18JITEvaluatedSymbolEED2Ev.exit1088
  %373 = landingpad { i8*, i32 }
          cleanup
  br label %ehcleanup325

ehcleanup325:                                     ; preds = %lpad276, %lpad302, %lpad312, %lpad289, %lpad263
  %.pn416.pn.pn.pn = phi { i8*, i32 } [ %369, %lpad263 ], [ %370, %lpad276 ], [ %371, %lpad289 ], [ %373, %lpad312 ], [ %372, %lpad302 ]
  %374 = load %"class.llvm::orc::LLJIT"*, %"class.llvm::orc::LLJIT"** %_M_head_impl.i.i.i.i.i.i.i991, align 8, !tbaa !37
  %cmp.not.i = icmp eq %"class.llvm::orc::LLJIT"* %374, null
  br i1 %cmp.not.i, label %ehcleanup327, label %_ZNKSt14default_deleteIN4llvm3orc5LLJITEEclEPS2_.exit.i

_ZNKSt14default_deleteIN4llvm3orc5LLJITEEclEPS2_.exit.i: ; preds = %ehcleanup325
  call void @_ZN4llvm3orc5LLJITD1Ev(%"class.llvm::orc::LLJIT"* nonnull dereferenceable(568) %374) #22
  %375 = bitcast %"class.llvm::orc::LLJIT"* %374 to i8*
  call void @_ZdlPv(i8* %375) #26
  br label %ehcleanup327

ehcleanup327:                                     ; preds = %invoke.cont6.i.i.i, %invoke.cont6.i.i489, %_ZNKSt14default_deleteIN4llvm3orc5LLJITEEclEPS2_.exit.i, %ehcleanup325, %lpad257, %lpad.i
  %.pn416.pn.pn.pn.pn = phi { i8*, i32 } [ %248, %lpad.i ], [ %365, %lpad257 ], [ %.pn416.pn.pn.pn, %ehcleanup325 ], [ %.pn416.pn.pn.pn, %_ZNKSt14default_deleteIN4llvm3orc5LLJITEEclEPS2_.exit.i ], [ %365, %invoke.cont6.i.i489 ], [ %248, %invoke.cont6.i.i.i ]
  call void @llvm.lifetime.end.p0i8(i64 8, i8* nonnull %238) #22
  br label %ehcleanup328

ehcleanup328:                                     ; preds = %ehcleanup327, %lpad252
  %.pn416.pn.pn.pn.pn.pn = phi { i8*, i32 } [ %.pn416.pn.pn.pn.pn, %ehcleanup327 ], [ %364, %lpad252 ]
  call void @llvm.lifetime.end.p0i8(i64 1616, i8* nonnull %236) #22
  br label %ehcleanup329

ehcleanup329:                                     ; preds = %ehcleanup328, %lpad249, %lpad243
  %.pn416.pn.pn.pn.pn.pn.pn = phi { i8*, i32 } [ %.pn416.pn.pn.pn.pn.pn, %ehcleanup328 ], [ %363, %lpad249 ], [ %362, %lpad243 ]
  call void @_ZN7codegen6HammerD2Ev(%"class.codegen::Hammer"* nonnull dereferenceable(152) %hammer2) #22
  br label %ehcleanup330

ehcleanup330:                                     ; preds = %lpad240, %ehcleanup329, %lpad236
  %.pn416.pn.pn.pn.pn.pn.pn.pn = phi { i8*, i32 } [ %.pn416.pn.pn.pn.pn.pn.pn, %ehcleanup329 ], [ %356, %lpad236 ], [ %357, %lpad240 ]
  call void @llvm.lifetime.end.p0i8(i64 152, i8* nonnull %213) #22
  call void @_ZN7codegen6HammerD2Ev(%"class.codegen::Hammer"* nonnull dereferenceable(152) %hammer1) #22
  br label %ehcleanup332

ehcleanup332:                                     ; preds = %lpad232, %ehcleanup330, %lpad228
  %.pn416.pn.pn.pn.pn.pn.pn.pn.pn = phi { i8*, i32 } [ %.pn416.pn.pn.pn.pn.pn.pn.pn, %ehcleanup330 ], [ %350, %lpad228 ], [ %351, %lpad232 ]
  call void @llvm.lifetime.end.p0i8(i64 152, i8* nonnull %193) #22
  br label %ehcleanup333

ehcleanup333:                                     ; preds = %ehcleanup332, %ehcleanup223, %ehcleanup212, %ehcleanup201, %ehcleanup190, %ehcleanup179, %ehcleanup168, %ehcleanup157, %ehcleanup146, %ehcleanup135, %ehcleanup124, %ehcleanup113, %ehcleanup102, %ehcleanup91, %ehcleanup80, %ehcleanup69, %ehcleanup58, %ehcleanup47, %ehcleanup36, %ehcleanup, %lpad19
  %.pn416.pn.pn.pn.pn.pn.pn.pn.pn.pn = phi { i8*, i32 } [ %.pn416.pn.pn.pn.pn.pn.pn.pn.pn, %ehcleanup332 ], [ %292, %lpad19 ], [ %.pn406, %ehcleanup223 ], [ %.pn404, %ehcleanup212 ], [ %.pn402, %ehcleanup201 ], [ %.pn400, %ehcleanup190 ], [ %.pn398, %ehcleanup179 ], [ %.pn396, %ehcleanup168 ], [ %.pn394, %ehcleanup157 ], [ %.pn392, %ehcleanup146 ], [ %.pn390, %ehcleanup135 ], [ %.pn388, %ehcleanup124 ], [ %.pn386, %ehcleanup113 ], [ %.pn384, %ehcleanup102 ], [ %.pn382, %ehcleanup91 ], [ %.pn380, %ehcleanup80 ], [ %.pn378, %ehcleanup69 ], [ %.pn376, %ehcleanup58 ], [ %.pn374, %ehcleanup47 ], [ %.pn372, %ehcleanup36 ], [ %.pn, %ehcleanup ]
  call void @llvm.lifetime.end.p0i8(i64 24, i8* nonnull %31) #22
  call void @llvm.lifetime.end.p0i8(i64 24, i8* nonnull %29) #22
  call void @llvm.lifetime.end.p0i8(i64 24, i8* nonnull %27) #22
  call void @llvm.lifetime.end.p0i8(i64 24, i8* nonnull %25) #22
  call void @llvm.lifetime.end.p0i8(i64 24, i8* nonnull %23) #22
  call void @llvm.lifetime.end.p0i8(i64 24, i8* nonnull %21) #22
  call void @llvm.lifetime.end.p0i8(i64 24, i8* nonnull %19) #22
  call void @llvm.lifetime.end.p0i8(i64 24, i8* nonnull %17) #22
  call void @llvm.lifetime.end.p0i8(i64 24, i8* nonnull %15) #22
  %376 = load %"struct.std::__detail::_List_node_base"*, %"struct.std::__detail::_List_node_base"** %_M_next.i.i.i, align 8, !tbaa !16
  %cmp.not16.i.i = icmp eq %"struct.std::__detail::_List_node_base"* %376, %7
  br i1 %cmp.not16.i.i, label %_ZNSt7__cxx1110_List_baseIPN7codegen6HammerESaIS3_EED2Ev.exit, label %invoke.cont6.i.i

invoke.cont6.i.i:                                 ; preds = %ehcleanup333, %invoke.cont6.i.i
  %__cur.017.i.i = phi %"struct.std::__detail::_List_node_base"* [ %377, %invoke.cont6.i.i ], [ %376, %ehcleanup333 ]
  %_M_next4.i.i = getelementptr inbounds %"struct.std::__detail::_List_node_base", %"struct.std::__detail::_List_node_base"* %__cur.017.i.i, i64 0, i32 0
  %377 = load %"struct.std::__detail::_List_node_base"*, %"struct.std::__detail::_List_node_base"** %_M_next4.i.i, align 8, !tbaa !16
  %378 = bitcast %"struct.std::__detail::_List_node_base"* %__cur.017.i.i to i8*
  call void @_ZdlPv(i8* %378) #22
  %cmp.not.i.i = icmp eq %"struct.std::__detail::_List_node_base"* %377, %7
  br i1 %cmp.not.i.i, label %_ZNSt7__cxx1110_List_baseIPN7codegen6HammerESaIS3_EED2Ev.exit, label %invoke.cont6.i.i, !llvm.loop !46

_ZNSt7__cxx1110_List_baseIPN7codegen6HammerESaIS3_EED2Ev.exit: ; preds = %invoke.cont6.i.i, %ehcleanup333
  call void @llvm.lifetime.end.p0i8(i64 24, i8* nonnull %6) #22
  %_M_t.i = getelementptr inbounds %"class.std::map", %"class.std::map"* %testParam, i64 0, i32 0
  %379 = bitcast i8* %_M_parent.i.i.i.i.i to %"struct.std::_Rb_tree_node"**
  %380 = load %"struct.std::_Rb_tree_node"*, %"struct.std::_Rb_tree_node"** %379, align 8, !tbaa !12
  invoke void @_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE8_M_eraseEPSt13_Rb_tree_nodeISB_E(%"class.std::_Rb_tree"* nonnull dereferenceable(48) %_M_t.i, %"struct.std::_Rb_tree_node"* %380)
          to label %_ZNSt3mapINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEPN7codegen10ParamValueESt4lessIS5_ESaISt4pairIKS5_S8_EEED2Ev.exit unwind label %lpad.i.i

lpad.i.i:                                         ; preds = %_ZNSt7__cxx1110_List_baseIPN7codegen6HammerESaIS3_EED2Ev.exit
  %381 = landingpad { i8*, i32 }
          catch i8* null
  %382 = extractvalue { i8*, i32 } %381, 0
  call void @__clang_call_terminate(i8* %382) #23
  unreachable

_ZNSt3mapINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEPN7codegen10ParamValueESt4lessIS5_ESaISt4pairIKS5_S8_EEED2Ev.exit: ; preds = %_ZNSt7__cxx1110_List_baseIPN7codegen6HammerESaIS3_EED2Ev.exit
  call void @llvm.lifetime.end.p0i8(i64 48, i8* nonnull %0) #22
  resume { i8*, i32 } %.pn416.pn.pn.pn.pn.pn.pn.pn.pn.pn
}

; Function Attrs: argmemonly nofree nosync nounwind willreturn
declare void @llvm.lifetime.start.p0i8(i64 immarg, i8* nocapture) #4

; Function Attrs: nofree nosync nounwind willreturn
declare i8* @llvm.stacksave() #5

; Function Attrs: argmemonly nofree nosync nounwind willreturn
declare void @llvm.lifetime.end.p0i8(i64 immarg, i8* nocapture) #4

declare dso_local i32 @__gxx_personality_v0(...)

; Function Attrs: uwtable
define linkonce_odr dso_local nonnull align 8 dereferenceable(8) %"class.codegen::ParamValue"** @_ZNSt3mapINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEPN7codegen10ParamValueESt4lessIS5_ESaISt4pairIKS5_S8_EEEixEOS5_(%"class.std::map"* nonnull dereferenceable(48) %this, %"class.std::__cxx11::basic_string"* nonnull align 8 dereferenceable(32) %__k) local_unnamed_addr #3 comdat align 2 personality i32 (...)* @__gxx_personality_v0 {
entry:
  %ref.tmp9 = alloca %"class.std::tuple.436", align 8
  %ref.tmp11 = alloca %"class.std::tuple.439", align 1
  %0 = getelementptr inbounds %"class.std::map", %"class.std::map"* %this, i64 0, i32 0, i32 0, i32 0, i32 0, i32 0
  %_M_parent.i.i.i = getelementptr inbounds i8, i8* %0, i64 16
  %1 = bitcast i8* %_M_parent.i.i.i to %"struct.std::_Rb_tree_node"**
  %2 = load %"struct.std::_Rb_tree_node"*, %"struct.std::_Rb_tree_node"** %1, align 8, !tbaa !12
  %add.ptr.i.i.i = getelementptr inbounds i8, i8* %0, i64 8
  %_M_header.i.i.i = bitcast i8* %add.ptr.i.i.i to %"struct.std::_Rb_tree_node_base"*
  %cmp.not11.i.i.i = icmp eq %"struct.std::_Rb_tree_node"* %2, null
  br i1 %cmp.not11.i.i.i, label %if.then, label %while.body.lr.ph.i.i.i

while.body.lr.ph.i.i.i:                           ; preds = %entry
  %_M_string_length.i14.i.i.i.i.i.i = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %__k, i64 0, i32 1
  %3 = load i64, i64* %_M_string_length.i14.i.i.i.i.i.i, align 8, !tbaa !34
  %_M_p.i.i.i.i.i.i.i.i = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %__k, i64 0, i32 0, i32 0
  %4 = load i8*, i8** %_M_p.i.i.i.i.i.i.i.i, align 8
  br label %while.body.i.i.i

while.body.i.i.i:                                 ; preds = %if.end.i.i.i, %while.body.lr.ph.i.i.i
  %__x.addr.013.i.i.i = phi %"struct.std::_Rb_tree_node"* [ %2, %while.body.lr.ph.i.i.i ], [ %__x.addr.1.i.i.i, %if.end.i.i.i ]
  %__y.addr.012.i.i.i = phi %"struct.std::_Rb_tree_node_base"* [ %_M_header.i.i.i, %while.body.lr.ph.i.i.i ], [ %__y.addr.1.i.i.i, %if.end.i.i.i ]
  %_M_string_length.i.i.i.i.i.i.i = getelementptr inbounds %"struct.std::_Rb_tree_node", %"struct.std::_Rb_tree_node"* %__x.addr.013.i.i.i, i64 0, i32 1, i32 0, i64 8
  %5 = bitcast i8* %_M_string_length.i.i.i.i.i.i.i to i64*
  %6 = load i64, i64* %5, align 8, !tbaa !34
  %cmp.i15.i.i.i.i.i.i = icmp ugt i64 %6, %3
  %.sroa.speculated.i.i.i.i.i.i = select i1 %cmp.i15.i.i.i.i.i.i, i64 %3, i64 %6
  %cmp.i13.i.i.i.i.i.i = icmp eq i64 %.sroa.speculated.i.i.i.i.i.i, 0
  br i1 %cmp.i13.i.i.i.i.i.i, label %if.then.i.i.i.i.i.i, label %_ZNSt11char_traitsIcE7compareEPKcS2_m.exit.i.i.i.i.i.i

_ZNSt11char_traitsIcE7compareEPKcS2_m.exit.i.i.i.i.i.i: ; preds = %while.body.i.i.i
  %_M_storage.i.i.i.i.i.i = getelementptr inbounds %"struct.std::_Rb_tree_node", %"struct.std::_Rb_tree_node"* %__x.addr.013.i.i.i, i64 0, i32 1
  %_M_p.i.i.i.i.i.i.i = bitcast %"struct.__gnu_cxx::__aligned_membuf.409"* %_M_storage.i.i.i.i.i.i to i8**
  %7 = load i8*, i8** %_M_p.i.i.i.i.i.i.i, align 8, !tbaa !31
  %call.i.i.i.i.i.i.i = tail call i32 @memcmp(i8* %7, i8* %4, i64 %.sroa.speculated.i.i.i.i.i.i) #22
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
  %__x.addr.1.i.i.i = load %"struct.std::_Rb_tree_node"*, %"struct.std::_Rb_tree_node"** %__x.addr.1.in.i.i.i, align 8, !tbaa !37
  %cmp.not.i.i.i = icmp eq %"struct.std::_Rb_tree_node"* %__x.addr.1.i.i.i, null
  br i1 %cmp.not.i.i.i, label %_ZNSt3mapINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEPN7codegen10ParamValueESt4lessIS5_ESaISt4pairIKS5_S8_EEE11lower_boundERSC_.exit, label %while.body.i.i.i, !llvm.loop !107

_ZNSt3mapINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEPN7codegen10ParamValueESt4lessIS5_ESaISt4pairIKS5_S8_EEE11lower_boundERSC_.exit: ; preds = %if.end.i.i.i
  %cmp.i = icmp eq %"struct.std::_Rb_tree_node_base"* %__y.addr.1.i.i.i, %_M_header.i.i.i
  br i1 %cmp.i, label %if.then, label %lor.rhs

lor.rhs:                                          ; preds = %_ZNSt3mapINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEPN7codegen10ParamValueESt4lessIS5_ESaISt4pairIKS5_S8_EEE11lower_boundERSC_.exit
  %_M_string_length.i14.i.i.i = getelementptr inbounds %"struct.std::_Rb_tree_node_base", %"struct.std::_Rb_tree_node_base"* %__y.addr.1.i.i.i, i64 1, i32 1
  %11 = bitcast %"struct.std::_Rb_tree_node_base"** %_M_string_length.i14.i.i.i to i64*
  %12 = load i64, i64* %11, align 8, !tbaa !34
  %cmp.i15.i.i.i = icmp ugt i64 %3, %12
  %.sroa.speculated.i.i.i = select i1 %cmp.i15.i.i.i, i64 %12, i64 %3
  %cmp.i13.i.i.i = icmp eq i64 %.sroa.speculated.i.i.i, 0
  br i1 %cmp.i13.i.i.i, label %if.then.i.i.i21, label %_ZNSt11char_traitsIcE7compareEPKcS2_m.exit.i.i.i

_ZNSt11char_traitsIcE7compareEPKcS2_m.exit.i.i.i: ; preds = %lor.rhs
  %_M_storage.i.i22 = getelementptr inbounds %"struct.std::_Rb_tree_node_base", %"struct.std::_Rb_tree_node_base"* %__y.addr.1.i.i.i, i64 1
  %_M_p.i.i.i.i.i = bitcast %"struct.std::_Rb_tree_node_base"* %_M_storage.i.i22 to i8**
  %13 = load i8*, i8** %_M_p.i.i.i.i.i, align 8, !tbaa !31
  %call.i.i.i.i = tail call i32 @memcmp(i8* %4, i8* %13, i64 %.sroa.speculated.i.i.i) #22
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
  %16 = bitcast %"class.std::tuple.436"* %ref.tmp9 to i8*
  call void @llvm.lifetime.start.p0i8(i64 8, i8* nonnull %16) #22
  %_M_head_impl.i.i.i.i = getelementptr inbounds %"class.std::tuple.436", %"class.std::tuple.436"* %ref.tmp9, i64 0, i32 0, i32 0, i32 0
  store %"class.std::__cxx11::basic_string"* %__k, %"class.std::__cxx11::basic_string"** %_M_head_impl.i.i.i.i, align 8, !tbaa !37, !alias.scope !108
  %17 = getelementptr inbounds %"class.std::tuple.439", %"class.std::tuple.439"* %ref.tmp11, i64 0, i32 0
  call void @llvm.lifetime.start.p0i8(i64 1, i8* nonnull %17) #22
  %call13 = call %"struct.std::_Rb_tree_node_base"* @_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE22_M_emplace_hint_uniqueIJRKSt21piecewise_construct_tSt5tupleIJOS5_EESM_IJEEEEESt17_Rb_tree_iteratorISB_ESt23_Rb_tree_const_iteratorISB_EDpOT_(%"class.std::_Rb_tree"* nonnull dereferenceable(48) %_M_t, %"struct.std::_Rb_tree_node_base"* %__y.addr.0.lcssa.i.i.i31, %"struct.std::piecewise_construct_t"* nonnull align 1 dereferenceable(1) @_ZStL19piecewise_construct, %"class.std::tuple.436"* nonnull align 8 dereferenceable(8) %ref.tmp9, %"class.std::tuple.439"* nonnull align 1 dereferenceable(1) %ref.tmp11)
  call void @llvm.lifetime.end.p0i8(i64 1, i8* nonnull %17) #22
  call void @llvm.lifetime.end.p0i8(i64 8, i8* nonnull %16) #22
  br label %if.end

if.end:                                           ; preds = %if.then.i.i.i21, %if.then, %_ZNKSt4lessINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEEclERKS5_S8_.exit
  %__i.sroa.0.0 = phi %"struct.std::_Rb_tree_node_base"* [ %call13, %if.then ], [ %__y.addr.1.i.i.i, %_ZNKSt4lessINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEEclERKS5_S8_.exit ], [ %__y.addr.1.i.i.i, %if.then.i.i.i21 ]
  %second = getelementptr inbounds %"struct.std::_Rb_tree_node_base", %"struct.std::_Rb_tree_node_base"* %__i.sroa.0.0, i64 2
  %18 = bitcast %"struct.std::_Rb_tree_node_base"* %second to %"class.codegen::ParamValue"**
  ret %"class.codegen::ParamValue"** %18
}

declare dso_local void @_ZN7codegen6HammerC1EN4llvm9StringRefESt3mapINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEPNS_10ParamValueESt4lessIS9_ESaISt4pairIKS9_SB_EEE(%"class.codegen::Hammer"* nonnull dereferenceable(152), i8*, i64, %"class.std::map"*) unnamed_addr #0

declare dso_local zeroext i1 @_ZN7codegen6Hammer6hardenEv(%"class.codegen::Hammer"* nonnull dereferenceable(152)) local_unnamed_addr #0

declare dso_local void @_ZN7codegen6Hammer13create_jitterENSt7__cxx114listIPS0_SaIS3_EEERNS_12HammerConfigE(%"class.std::unique_ptr.123"* sret(%"class.std::unique_ptr.123") align 8, %"class.codegen::Hammer"* nonnull dereferenceable(152), %"class.std::__cxx11::list"*, %"class.codegen::HammerConfig"* nonnull align 8 dereferenceable(1616)) local_unnamed_addr #0

; Function Attrs: nobuiltin nofree allocsize(0)
declare dso_local nonnull i8* @_Znwm(i64) local_unnamed_addr #6

; Function Attrs: inlinehint nounwind uwtable
define linkonce_odr dso_local void @_ZN7codegen6HammerD2Ev(%"class.codegen::Hammer"* nonnull dereferenceable(152) %this) unnamed_addr #7 comdat align 2 personality i32 (...)* @__gxx_personality_v0 {
entry:
  %params = getelementptr inbounds %"class.codegen::Hammer", %"class.codegen::Hammer"* %this, i64 0, i32 6
  %_M_t.i = getelementptr inbounds %"class.std::map", %"class.std::map"* %params, i64 0, i32 0
  %0 = getelementptr inbounds %"class.std::map", %"class.std::map"* %params, i64 0, i32 0, i32 0, i32 0, i32 0, i32 0
  %_M_parent.i.i.i = getelementptr inbounds i8, i8* %0, i64 16
  %1 = bitcast i8* %_M_parent.i.i.i to %"struct.std::_Rb_tree_node"**
  %2 = load %"struct.std::_Rb_tree_node"*, %"struct.std::_Rb_tree_node"** %1, align 8, !tbaa !12
  invoke void @_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE8_M_eraseEPSt13_Rb_tree_nodeISB_E(%"class.std::_Rb_tree"* nonnull dereferenceable(48) %_M_t.i, %"struct.std::_Rb_tree_node"* %2)
          to label %_ZNSt3mapINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEPN7codegen10ParamValueESt4lessIS5_ESaISt4pairIKS5_S8_EEED2Ev.exit unwind label %lpad.i.i

lpad.i.i:                                         ; preds = %entry
  %3 = landingpad { i8*, i32 }
          catch i8* null
  %4 = extractvalue { i8*, i32 } %3, 0
  tail call void @__clang_call_terminate(i8* %4) #23
  unreachable

_ZNSt3mapINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEPN7codegen10ParamValueESt4lessIS5_ESaISt4pairIKS5_S8_EEED2Ev.exit: ; preds = %entry
  %_M_head_impl.i.i.i.i.i.i = getelementptr inbounds %"class.codegen::Hammer", %"class.codegen::Hammer"* %this, i64 0, i32 5, i32 0, i32 0, i32 0, i32 0, i32 0
  %5 = load %"class.llvm::Module"*, %"class.llvm::Module"** %_M_head_impl.i.i.i.i.i.i, align 8, !tbaa !37
  %cmp.not.i = icmp eq %"class.llvm::Module"* %5, null
  br i1 %cmp.not.i, label %_ZNSt10unique_ptrIN4llvm6ModuleESt14default_deleteIS1_EED2Ev.exit, label %_ZNKSt14default_deleteIN4llvm6ModuleEEclEPS1_.exit.i

_ZNKSt14default_deleteIN4llvm6ModuleEEclEPS1_.exit.i: ; preds = %_ZNSt3mapINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEPN7codegen10ParamValueESt4lessIS5_ESaISt4pairIKS5_S8_EEED2Ev.exit
  tail call void @_ZN4llvm6ModuleD1Ev(%"class.llvm::Module"* nonnull dereferenceable(744) %5) #22
  %6 = bitcast %"class.llvm::Module"* %5 to i8*
  tail call void @_ZdlPv(i8* %6) #26
  br label %_ZNSt10unique_ptrIN4llvm6ModuleESt14default_deleteIS1_EED2Ev.exit

_ZNSt10unique_ptrIN4llvm6ModuleESt14default_deleteIS1_EED2Ev.exit: ; preds = %_ZNSt3mapINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEPN7codegen10ParamValueESt4lessIS5_ESaISt4pairIKS5_S8_EEED2Ev.exit, %_ZNKSt14default_deleteIN4llvm6ModuleEEclEPS1_.exit.i
  store %"class.llvm::Module"* null, %"class.llvm::Module"** %_M_head_impl.i.i.i.i.i.i, align 8, !tbaa !37
  %_M_head_impl.i.i.i.i.i.i2 = getelementptr inbounds %"class.codegen::Hammer", %"class.codegen::Hammer"* %this, i64 0, i32 4, i32 0, i32 0, i32 0, i32 0, i32 0
  %7 = load %"class.llvm::legacy::FunctionPassManager"*, %"class.llvm::legacy::FunctionPassManager"** %_M_head_impl.i.i.i.i.i.i2, align 8, !tbaa !37
  %cmp.not.i3 = icmp eq %"class.llvm::legacy::FunctionPassManager"* %7, null
  br i1 %cmp.not.i3, label %_ZNSt10unique_ptrIN4llvm6legacy19FunctionPassManagerESt14default_deleteIS2_EED2Ev.exit, label %_ZNKSt14default_deleteIN4llvm6legacy19FunctionPassManagerEEclEPS2_.exit.i

_ZNKSt14default_deleteIN4llvm6legacy19FunctionPassManagerEEclEPS2_.exit.i: ; preds = %_ZNSt10unique_ptrIN4llvm6ModuleESt14default_deleteIS1_EED2Ev.exit
  %8 = bitcast %"class.llvm::legacy::FunctionPassManager"* %7 to void (%"class.llvm::legacy::FunctionPassManager"*)***
  %vtable.i.i = load void (%"class.llvm::legacy::FunctionPassManager"*)**, void (%"class.llvm::legacy::FunctionPassManager"*)*** %8, align 8, !tbaa !111
  %vfn.i.i = getelementptr inbounds void (%"class.llvm::legacy::FunctionPassManager"*)*, void (%"class.llvm::legacy::FunctionPassManager"*)** %vtable.i.i, i64 1
  %9 = load void (%"class.llvm::legacy::FunctionPassManager"*)*, void (%"class.llvm::legacy::FunctionPassManager"*)** %vfn.i.i, align 8
  tail call void %9(%"class.llvm::legacy::FunctionPassManager"* nonnull dereferenceable(24) %7) #22
  br label %_ZNSt10unique_ptrIN4llvm6legacy19FunctionPassManagerESt14default_deleteIS2_EED2Ev.exit

_ZNSt10unique_ptrIN4llvm6legacy19FunctionPassManagerESt14default_deleteIS2_EED2Ev.exit: ; preds = %_ZNSt10unique_ptrIN4llvm6ModuleESt14default_deleteIS1_EED2Ev.exit, %_ZNKSt14default_deleteIN4llvm6legacy19FunctionPassManagerEEclEPS2_.exit.i
  store %"class.llvm::legacy::FunctionPassManager"* null, %"class.llvm::legacy::FunctionPassManager"** %_M_head_impl.i.i.i.i.i.i2, align 8, !tbaa !37
  %_M_head_impl.i.i.i.i.i.i4 = getelementptr inbounds %"class.codegen::Hammer", %"class.codegen::Hammer"* %this, i64 0, i32 3, i32 0, i32 0, i32 0, i32 0, i32 0
  %10 = load %"class.llvm::IRBuilder"*, %"class.llvm::IRBuilder"** %_M_head_impl.i.i.i.i.i.i4, align 8, !tbaa !37
  %cmp.not.i5 = icmp eq %"class.llvm::IRBuilder"* %10, null
  br i1 %cmp.not.i5, label %_ZNSt10unique_ptrIN4llvm9IRBuilderINS0_14ConstantFolderENS0_24IRBuilderDefaultInserterEEESt14default_deleteIS4_EED2Ev.exit, label %delete.notnull.i.i

delete.notnull.i.i:                               ; preds = %_ZNSt10unique_ptrIN4llvm6legacy19FunctionPassManagerESt14default_deleteIS2_EED2Ev.exit
  %Inserter.i.i.i = getelementptr inbounds %"class.llvm::IRBuilder", %"class.llvm::IRBuilder"* %10, i64 0, i32 2
  tail call void @_ZN4llvm24IRBuilderDefaultInserterD1Ev(%"class.llvm::IRBuilderDefaultInserter"* nonnull dereferenceable(8) %Inserter.i.i.i) #22
  %11 = getelementptr inbounds %"class.llvm::IRBuilder", %"class.llvm::IRBuilder"* %10, i64 0, i32 1, i32 0
  tail call void @_ZN4llvm15IRBuilderFolderD2Ev(%"class.llvm::IRBuilderFolder"* nonnull dereferenceable(8) %11) #22
  %12 = getelementptr inbounds %"class.llvm::IRBuilder", %"class.llvm::IRBuilder"* %10, i64 0, i32 0, i32 0, i32 0
  %BeginX.i.i.i.i.i.i.i = getelementptr inbounds %"class.llvm::IRBuilder", %"class.llvm::IRBuilder"* %10, i64 0, i32 0, i32 0, i32 0, i32 0, i32 0, i32 0, i32 0
  %13 = load i8*, i8** %BeginX.i.i.i.i.i.i.i, align 8, !tbaa !113
  %add.ptr2.i.i.i.i.i.i.i.i = getelementptr inbounds %"class.llvm::SmallVectorImpl.379", %"class.llvm::SmallVectorImpl.379"* %12, i64 1, i32 0, i32 0
  %add.ptr.i.i.i.i.i.i.i.i = bitcast %"class.llvm::SmallVectorTemplateCommon.381"* %add.ptr2.i.i.i.i.i.i.i.i to i8*
  %cmp.i.i.i.i.i.i.i = icmp eq i8* %13, %add.ptr.i.i.i.i.i.i.i.i
  br i1 %cmp.i.i.i.i.i.i.i, label %_ZNKSt14default_deleteIN4llvm9IRBuilderINS0_14ConstantFolderENS0_24IRBuilderDefaultInserterEEEEclEPS4_.exit.i, label %if.then.i.i.i.i.i.i

if.then.i.i.i.i.i.i:                              ; preds = %delete.notnull.i.i
  tail call void @free(i8* %13) #22
  br label %_ZNKSt14default_deleteIN4llvm9IRBuilderINS0_14ConstantFolderENS0_24IRBuilderDefaultInserterEEEEclEPS4_.exit.i

_ZNKSt14default_deleteIN4llvm9IRBuilderINS0_14ConstantFolderENS0_24IRBuilderDefaultInserterEEEEclEPS4_.exit.i: ; preds = %if.then.i.i.i.i.i.i, %delete.notnull.i.i
  %14 = bitcast %"class.llvm::IRBuilder"* %10 to i8*
  tail call void @_ZdlPv(i8* %14) #26
  br label %_ZNSt10unique_ptrIN4llvm9IRBuilderINS0_14ConstantFolderENS0_24IRBuilderDefaultInserterEEESt14default_deleteIS4_EED2Ev.exit

_ZNSt10unique_ptrIN4llvm9IRBuilderINS0_14ConstantFolderENS0_24IRBuilderDefaultInserterEEESt14default_deleteIS4_EED2Ev.exit: ; preds = %_ZNSt10unique_ptrIN4llvm6legacy19FunctionPassManagerESt14default_deleteIS2_EED2Ev.exit, %_ZNKSt14default_deleteIN4llvm9IRBuilderINS0_14ConstantFolderENS0_24IRBuilderDefaultInserterEEEEclEPS4_.exit.i
  store %"class.llvm::IRBuilder"* null, %"class.llvm::IRBuilder"** %_M_head_impl.i.i.i.i.i.i4, align 8, !tbaa !37
  %_M_head_impl.i.i.i.i.i.i6 = getelementptr inbounds %"class.codegen::Hammer", %"class.codegen::Hammer"* %this, i64 0, i32 2, i32 0, i32 0, i32 0, i32 0, i32 0
  %15 = load %"class.llvm::StringRef"*, %"class.llvm::StringRef"** %_M_head_impl.i.i.i.i.i.i6, align 8, !tbaa !37
  %cmp.not.i7 = icmp eq %"class.llvm::StringRef"* %15, null
  br i1 %cmp.not.i7, label %_ZNSt10unique_ptrIN4llvm9StringRefESt14default_deleteIS1_EED2Ev.exit, label %_ZNKSt14default_deleteIN4llvm9StringRefEEclEPS1_.exit.i

_ZNKSt14default_deleteIN4llvm9StringRefEEclEPS1_.exit.i: ; preds = %_ZNSt10unique_ptrIN4llvm9IRBuilderINS0_14ConstantFolderENS0_24IRBuilderDefaultInserterEEESt14default_deleteIS4_EED2Ev.exit
  %16 = bitcast %"class.llvm::StringRef"* %15 to i8*
  tail call void @_ZdlPv(i8* %16) #26
  br label %_ZNSt10unique_ptrIN4llvm9StringRefESt14default_deleteIS1_EED2Ev.exit

_ZNSt10unique_ptrIN4llvm9StringRefESt14default_deleteIS1_EED2Ev.exit: ; preds = %_ZNSt10unique_ptrIN4llvm9IRBuilderINS0_14ConstantFolderENS0_24IRBuilderDefaultInserterEEESt14default_deleteIS4_EED2Ev.exit, %_ZNKSt14default_deleteIN4llvm9StringRefEEclEPS1_.exit.i
  store %"class.llvm::StringRef"* null, %"class.llvm::StringRef"** %_M_head_impl.i.i.i.i.i.i6, align 8, !tbaa !37
  %_M_head_impl.i.i.i.i.i.i8 = getelementptr inbounds %"class.codegen::Hammer", %"class.codegen::Hammer"* %this, i64 0, i32 1, i32 0, i32 0, i32 0, i32 0, i32 0
  %17 = load %"class.llvm::LLVMContext"*, %"class.llvm::LLVMContext"** %_M_head_impl.i.i.i.i.i.i8, align 8, !tbaa !37
  %cmp.not.i9 = icmp eq %"class.llvm::LLVMContext"* %17, null
  br i1 %cmp.not.i9, label %_ZNSt10unique_ptrIN4llvm11LLVMContextESt14default_deleteIS1_EED2Ev.exit, label %_ZNKSt14default_deleteIN4llvm11LLVMContextEEclEPS1_.exit.i

_ZNKSt14default_deleteIN4llvm11LLVMContextEEclEPS1_.exit.i: ; preds = %_ZNSt10unique_ptrIN4llvm9StringRefESt14default_deleteIS1_EED2Ev.exit
  tail call void @_ZN4llvm11LLVMContextD1Ev(%"class.llvm::LLVMContext"* nonnull dereferenceable(8) %17) #22
  %18 = bitcast %"class.llvm::LLVMContext"* %17 to i8*
  tail call void @_ZdlPv(i8* %18) #26
  br label %_ZNSt10unique_ptrIN4llvm11LLVMContextESt14default_deleteIS1_EED2Ev.exit

_ZNSt10unique_ptrIN4llvm11LLVMContextESt14default_deleteIS1_EED2Ev.exit: ; preds = %_ZNSt10unique_ptrIN4llvm9StringRefESt14default_deleteIS1_EED2Ev.exit, %_ZNKSt14default_deleteIN4llvm11LLVMContextEEclEPS1_.exit.i
  store %"class.llvm::LLVMContext"* null, %"class.llvm::LLVMContext"** %_M_head_impl.i.i.i.i.i.i8, align 8, !tbaa !37
  %_M_manager.i.i = getelementptr inbounds %"class.codegen::Hammer", %"class.codegen::Hammer"* %this, i64 0, i32 0, i32 1, i32 0, i32 1
  %19 = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %_M_manager.i.i, align 8, !tbaa !115
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
  tail call void @__clang_call_terminate(i8* %21) #23
  unreachable

_ZNSt14_Function_baseD2Ev.exit.i:                 ; preds = %if.then.i.i, %_ZNSt10unique_ptrIN4llvm11LLVMContextESt14default_deleteIS1_EED2Ev.exit
  %_M_p.i.i.i.i.i = getelementptr inbounds %"class.codegen::Hammer", %"class.codegen::Hammer"* %this, i64 0, i32 0, i32 0, i32 0, i32 0
  %22 = load i8*, i8** %_M_p.i.i.i.i.i, align 8, !tbaa !31
  %23 = getelementptr inbounds %"class.codegen::Hammer", %"class.codegen::Hammer"* %this, i64 0, i32 0, i32 0, i32 2
  %arraydecay.i.i.i.i.i = bitcast %union.anon* %23 to i8*
  %cmp.i.i.i.i = icmp eq i8* %22, %arraydecay.i.i.i.i.i
  br i1 %cmp.i.i.i.i, label %_ZN4llvm11ExitOnErrorD2Ev.exit, label %if.then.i.i.i

if.then.i.i.i:                                    ; preds = %_ZNSt14_Function_baseD2Ev.exit.i
  tail call void @_ZdlPv(i8* %22) #22
  br label %_ZN4llvm11ExitOnErrorD2Ev.exit

_ZN4llvm11ExitOnErrorD2Ev.exit:                   ; preds = %_ZNSt14_Function_baseD2Ev.exit.i, %if.then.i.i.i
  ret void
}

; Function Attrs: nofree nosync nounwind willreturn
declare void @llvm.stackrestore(i8*) #5

; Function Attrs: uwtable mustprogress
define dso_local i64 @_Z18sortCreateOperatorlPiiS_iS_S_S_i(i64 %contextAddress, i32* %sourceTypes, i32 %typeCount, i32* %outputCols, i32 %outputColCount, i32* %sortCols, i32* %sortAscendings, i32* %sortNullFirsts, i32 %sortColCount) local_unnamed_addr #8 {
entry:
  %cmp.not = icmp eq i64 %contextAddress, 0
  br i1 %cmp.not, label %if.else, label %if.then

if.then:                                          ; preds = %entry
  %0 = inttoptr i64 %contextAddress to %struct.JitSortContext*
  %createSortFunc1 = getelementptr inbounds %struct.JitSortContext, %struct.JitSortContext* %0, i64 0, i32 1
  %1 = load i64 (i32*, i32, i32*, i32, i32*, i32*, i32*, i32)*, i64 (i32*, i32, i32*, i32, i32*, i32*, i32*, i32)** %createSortFunc1, align 8, !tbaa !101
  %call = tail call i64 %1(i32* %sourceTypes, i32 %typeCount, i32* %outputCols, i32 %outputColCount, i32* %sortCols, i32* %sortAscendings, i32* %sortNullFirsts, i32 %sortColCount)
  br label %if.end

if.else:                                          ; preds = %entry
  %call2 = tail call i64 @_Z10createSortPiiS_iS_S_S_i(i32* %sourceTypes, i32 %typeCount, i32* %outputCols, i32 %outputColCount, i32* %sortCols, i32* %sortAscendings, i32* %sortNullFirsts, i32 %sortColCount)
  br label %if.end

if.end:                                           ; preds = %if.else, %if.then
  %sortAddress.0 = phi i64 [ %call, %if.then ], [ %call2, %if.else ]
  ret i64 %sortAddress.0
}

declare dso_local i64 @_Z10createSortPiiS_iS_S_S_i(i32*, i32, i32*, i32, i32*, i32*, i32*, i32) local_unnamed_addr #0

; Function Attrs: uwtable mustprogress
define dso_local void @_Z12sortAddInputllPlS_iPii(i64 %contextAddress, i64 %sortAddress, i64* %datas, i64* %nulls, i32 %pageCount, i32* %rowCounts, i32 %totalRowCount) local_unnamed_addr #8 {
entry:
  %0 = inttoptr i64 %sortAddress to %class.Sort*
  %pagesIndex.i = getelementptr inbounds %class.Sort, %class.Sort* %0, i64 0, i32 9
  %1 = load %class.PagesIndex*, %class.PagesIndex** %pagesIndex.i, align 8, !tbaa !117
  tail call void @_ZN10PagesIndex9addTablesEPlS0_iPii(%class.PagesIndex* nonnull dereferenceable(44) %1, i64* %datas, i64* %nulls, i32 %pageCount, i32* %rowCounts, i32 %totalRowCount)
  ret void
}

declare dso_local void @_ZN10PagesIndex9addTablesEPlS0_iPii(%class.PagesIndex* nonnull dereferenceable(44), i64*, i64*, i32, i32*, i32) local_unnamed_addr #0

; Function Attrs: uwtable mustprogress
define dso_local void @_Z11sortExecutell(i64 %contextAddress, i64 %sortAddress) local_unnamed_addr #8 {
entry:
  %0 = inttoptr i64 %sortAddress to %class.Sort*
  %sourceTypes.i = getelementptr inbounds %class.Sort, %class.Sort* %0, i64 0, i32 1
  %1 = load i32*, i32** %sourceTypes.i, align 8, !tbaa !119
  %sortCols.i = getelementptr inbounds %class.Sort, %class.Sort* %0, i64 0, i32 5
  %2 = load i32*, i32** %sortCols.i, align 8, !tbaa !120
  %sortColCount.i = getelementptr inbounds %class.Sort, %class.Sort* %0, i64 0, i32 8
  %3 = load i32, i32* %sortColCount.i, align 8, !tbaa !121
  %pagesIndex.i = getelementptr inbounds %class.Sort, %class.Sort* %0, i64 0, i32 9
  %4 = load %class.PagesIndex*, %class.PagesIndex** %pagesIndex.i, align 8, !tbaa !117
  %tableCount.i = getelementptr inbounds %class.PagesIndex, %class.PagesIndex* %4, i64 0, i32 7
  %5 = load i32, i32* %tableCount.i, align 8, !tbaa !122
  %cmp = icmp eq i32 %5, 0
  br i1 %cmp, label %cleanup, label %if.end

if.end:                                           ; preds = %entry
  %positionCount.i = getelementptr inbounds %class.PagesIndex, %class.PagesIndex* %4, i64 0, i32 4
  %6 = load i32, i32* %positionCount.i, align 8, !tbaa !124
  %7 = zext i32 %3 to i64
  %8 = tail call i8* @llvm.stacksave()
  %vla = alloca i32, i64 %7, align 16
  %cmp659 = icmp sgt i32 %3, 0
  br i1 %cmp659, label %for.body.preheader, label %for.cond.cleanup

for.body.preheader:                               ; preds = %if.end
  %9 = add nsw i64 %7, -1
  %xtraiter = and i64 %7, 3
  %10 = icmp ult i64 %9, 3
  br i1 %10, label %for.cond.cleanup.loopexit.unr-lcssa, label %for.body.preheader.new

for.body.preheader.new:                           ; preds = %for.body.preheader
  %unroll_iter = and i64 %7, 4294967292
  br label %for.body

for.cond.cleanup.loopexit.unr-lcssa:              ; preds = %for.body, %for.body.preheader
  %indvars.iv.unr = phi i64 [ 0, %for.body.preheader ], [ %indvars.iv.next.3, %for.body ]
  %lcmp.mod.not = icmp eq i64 %xtraiter, 0
  br i1 %lcmp.mod.not, label %for.cond.cleanup, label %for.body.epil

for.body.epil:                                    ; preds = %for.cond.cleanup.loopexit.unr-lcssa, %for.body.epil
  %indvars.iv.epil = phi i64 [ %indvars.iv.next.epil, %for.body.epil ], [ %indvars.iv.unr, %for.cond.cleanup.loopexit.unr-lcssa ]
  %epil.iter = phi i64 [ %epil.iter.sub, %for.body.epil ], [ %xtraiter, %for.cond.cleanup.loopexit.unr-lcssa ]
  %arrayidx.epil = getelementptr inbounds i32, i32* %2, i64 %indvars.iv.epil
  %11 = load i32, i32* %arrayidx.epil, align 4, !tbaa !2
  %idxprom7.epil = sext i32 %11 to i64
  %arrayidx8.epil = getelementptr inbounds i32, i32* %1, i64 %idxprom7.epil
  %12 = load i32, i32* %arrayidx8.epil, align 4, !tbaa !2
  %arrayidx10.epil = getelementptr inbounds i32, i32* %vla, i64 %indvars.iv.epil
  store i32 %12, i32* %arrayidx10.epil, align 4, !tbaa !2
  %indvars.iv.next.epil = add nuw nsw i64 %indvars.iv.epil, 1
  %epil.iter.sub = add i64 %epil.iter, -1
  %epil.iter.cmp.not = icmp eq i64 %epil.iter.sub, 0
  br i1 %epil.iter.cmp.not, label %for.cond.cleanup, label %for.body.epil, !llvm.loop !125

for.cond.cleanup:                                 ; preds = %for.cond.cleanup.loopexit.unr-lcssa, %for.body.epil, %if.end
  %cmp11.not = icmp eq i64 %contextAddress, 0
  br i1 %cmp11.not, label %if.else, label %if.then12

for.body:                                         ; preds = %for.body, %for.body.preheader.new
  %indvars.iv = phi i64 [ 0, %for.body.preheader.new ], [ %indvars.iv.next.3, %for.body ]
  %niter = phi i64 [ %unroll_iter, %for.body.preheader.new ], [ %niter.nsub.3, %for.body ]
  %arrayidx = getelementptr inbounds i32, i32* %2, i64 %indvars.iv
  %13 = load i32, i32* %arrayidx, align 4, !tbaa !2
  %idxprom7 = sext i32 %13 to i64
  %arrayidx8 = getelementptr inbounds i32, i32* %1, i64 %idxprom7
  %14 = load i32, i32* %arrayidx8, align 4, !tbaa !2
  %arrayidx10 = getelementptr inbounds i32, i32* %vla, i64 %indvars.iv
  store i32 %14, i32* %arrayidx10, align 16, !tbaa !2
  %indvars.iv.next = or i64 %indvars.iv, 1
  %arrayidx.1 = getelementptr inbounds i32, i32* %2, i64 %indvars.iv.next
  %15 = load i32, i32* %arrayidx.1, align 4, !tbaa !2
  %idxprom7.1 = sext i32 %15 to i64
  %arrayidx8.1 = getelementptr inbounds i32, i32* %1, i64 %idxprom7.1
  %16 = load i32, i32* %arrayidx8.1, align 4, !tbaa !2
  %arrayidx10.1 = getelementptr inbounds i32, i32* %vla, i64 %indvars.iv.next
  store i32 %16, i32* %arrayidx10.1, align 4, !tbaa !2
  %indvars.iv.next.1 = or i64 %indvars.iv, 2
  %arrayidx.2 = getelementptr inbounds i32, i32* %2, i64 %indvars.iv.next.1
  %17 = load i32, i32* %arrayidx.2, align 4, !tbaa !2
  %idxprom7.2 = sext i32 %17 to i64
  %arrayidx8.2 = getelementptr inbounds i32, i32* %1, i64 %idxprom7.2
  %18 = load i32, i32* %arrayidx8.2, align 4, !tbaa !2
  %arrayidx10.2 = getelementptr inbounds i32, i32* %vla, i64 %indvars.iv.next.1
  store i32 %18, i32* %arrayidx10.2, align 8, !tbaa !2
  %indvars.iv.next.2 = or i64 %indvars.iv, 3
  %arrayidx.3 = getelementptr inbounds i32, i32* %2, i64 %indvars.iv.next.2
  %19 = load i32, i32* %arrayidx.3, align 4, !tbaa !2
  %idxprom7.3 = sext i32 %19 to i64
  %arrayidx8.3 = getelementptr inbounds i32, i32* %1, i64 %idxprom7.3
  %20 = load i32, i32* %arrayidx8.3, align 4, !tbaa !2
  %arrayidx10.3 = getelementptr inbounds i32, i32* %vla, i64 %indvars.iv.next.2
  store i32 %20, i32* %arrayidx10.3, align 4, !tbaa !2
  %indvars.iv.next.3 = add nuw nsw i64 %indvars.iv, 4
  %niter.nsub.3 = add i64 %niter, -4
  %niter.ncmp.3 = icmp eq i64 %niter.nsub.3, 0
  br i1 %niter.ncmp.3, label %for.cond.cleanup.loopexit.unr-lcssa, label %for.body, !llvm.loop !126

if.then12:                                        ; preds = %for.cond.cleanup
  %21 = inttoptr i64 %contextAddress to %struct.JitSortContext*
  %sortFunc13 = getelementptr inbounds %struct.JitSortContext, %struct.JitSortContext* %21, i64 0, i32 2
  %22 = load void (i64, i32*, i32*, i32*, i32*, i32, i32, i32)*, void (i64, i32*, i32*, i32*, i32*, i32, i32, i32)** %sortFunc13, align 8, !tbaa !103
  %23 = ptrtoint %class.PagesIndex* %4 to i64
  %sortAscendings.i58 = getelementptr inbounds %class.Sort, %class.Sort* %0, i64 0, i32 6
  %24 = load i32*, i32** %sortAscendings.i58, align 8, !tbaa !127
  %sortNullFirsts.i57 = getelementptr inbounds %class.Sort, %class.Sort* %0, i64 0, i32 7
  %25 = load i32*, i32** %sortNullFirsts.i57, align 8, !tbaa !128
  call void %22(i64 %23, i32* %2, i32* nonnull %vla, i32* %24, i32* %25, i32 %3, i32 0, i32 %6)
  br label %if.end19

if.else:                                          ; preds = %for.cond.cleanup
  %26 = ptrtoint %class.PagesIndex* %4 to i64
  %sortAscendings.i = getelementptr inbounds %class.Sort, %class.Sort* %0, i64 0, i32 6
  %27 = load i32*, i32** %sortAscendings.i, align 8, !tbaa !127
  %sortNullFirsts.i = getelementptr inbounds %class.Sort, %class.Sort* %0, i64 0, i32 7
  %28 = load i32*, i32** %sortNullFirsts.i, align 8, !tbaa !128
  call void @_Z9quickSortlPiS_S_S_iii(i64 %26, i32* %2, i32* nonnull %vla, i32* %27, i32* %28, i32 %3, i32 0, i32 %6)
  br label %if.end19

if.end19:                                         ; preds = %if.else, %if.then12
  call void @llvm.stackrestore(i8* %8)
  br label %cleanup

cleanup:                                          ; preds = %entry, %if.end19
  ret void
}

declare dso_local void @_Z9quickSortlPiS_S_S_iii(i64, i32*, i32*, i32*, i32*, i32, i32, i32) local_unnamed_addr #0

; Function Attrs: norecurse nounwind readonly uwtable willreturn mustprogress
define dso_local i32 @_Z14getMaxRowCountPiS_i(i32* nocapture readonly %sourceTypes, i32* nocapture readonly %outputCols, i32 %outputColsCount) local_unnamed_addr #9 {
entry:
  %cmp26 = icmp sgt i32 %outputColsCount, 0
  br i1 %cmp26, label %for.body.preheader, label %for.cond.cleanup

for.body.preheader:                               ; preds = %entry
  %wide.trip.count = zext i32 %outputColsCount to i64
  %xtraiter = and i64 %wide.trip.count, 1
  %0 = icmp eq i32 %outputColsCount, 1
  br i1 %0, label %for.cond.cleanup.loopexit.unr-lcssa, label %for.body.preheader.new

for.body.preheader.new:                           ; preds = %for.body.preheader
  %unroll_iter = and i64 %wide.trip.count, 4294967294
  br label %for.body

for.cond.cleanup.loopexit.unr-lcssa:              ; preds = %for.inc.1, %for.body.preheader
  %rowSize.1.lcssa.ph = phi i32 [ undef, %for.body.preheader ], [ %rowSize.1.1, %for.inc.1 ]
  %indvars.iv.unr = phi i64 [ 0, %for.body.preheader ], [ %indvars.iv.next.1, %for.inc.1 ]
  %rowSize.028.unr = phi i32 [ 0, %for.body.preheader ], [ %rowSize.1.1, %for.inc.1 ]
  %lcmp.mod.not = icmp eq i64 %xtraiter, 0
  br i1 %lcmp.mod.not, label %for.cond.cleanup, label %for.body.epil

for.body.epil:                                    ; preds = %for.cond.cleanup.loopexit.unr-lcssa
  %arrayidx.epil = getelementptr inbounds i32, i32* %outputCols, i64 %indvars.iv.unr
  %1 = load i32, i32* %arrayidx.epil, align 4, !tbaa !2
  %idxprom1.epil = sext i32 %1 to i64
  %arrayidx2.epil = getelementptr inbounds i32, i32* %sourceTypes, i64 %idxprom1.epil
  %2 = load i32, i32* %arrayidx2.epil, align 4, !tbaa !2
  switch i32 %2, label %for.cond.cleanup [
    i32 1, label %sw.bb.epil
    i32 2, label %sw.bb5.epil
    i32 3, label %sw.bb10.epil
  ]

sw.bb10.epil:                                     ; preds = %for.body.epil
  %add13.epil = add i32 %rowSize.028.unr, 9
  br label %for.cond.cleanup

sw.bb5.epil:                                      ; preds = %for.body.epil
  %add8.epil = add i32 %rowSize.028.unr, 9
  br label %for.cond.cleanup

sw.bb.epil:                                       ; preds = %for.body.epil
  %add3.epil = add i32 %rowSize.028.unr, 5
  br label %for.cond.cleanup

for.cond.cleanup:                                 ; preds = %for.cond.cleanup.loopexit.unr-lcssa, %for.body.epil, %sw.bb10.epil, %sw.bb5.epil, %sw.bb.epil, %entry
  %rowSize.0.lcssa = phi i32 [ 0, %entry ], [ %rowSize.1.lcssa.ph, %for.cond.cleanup.loopexit.unr-lcssa ], [ %rowSize.028.unr, %for.body.epil ], [ %add13.epil, %sw.bb10.epil ], [ %add8.epil, %sw.bb5.epil ], [ %add3.epil, %sw.bb.epil ]
  %3 = load i32, i32* @DEFAULT_MAX_PAGE_SIZE_IN_BYTES, align 4, !tbaa !2
  %add15 = add i32 %rowSize.0.lcssa, -1
  %sub = add i32 %add15, %3
  %div = sdiv i32 %sub, %rowSize.0.lcssa
  ret i32 %div

for.body:                                         ; preds = %for.inc.1, %for.body.preheader.new
  %indvars.iv = phi i64 [ 0, %for.body.preheader.new ], [ %indvars.iv.next.1, %for.inc.1 ]
  %rowSize.028 = phi i32 [ 0, %for.body.preheader.new ], [ %rowSize.1.1, %for.inc.1 ]
  %niter = phi i64 [ %unroll_iter, %for.body.preheader.new ], [ %niter.nsub.1, %for.inc.1 ]
  %arrayidx = getelementptr inbounds i32, i32* %outputCols, i64 %indvars.iv
  %4 = load i32, i32* %arrayidx, align 4, !tbaa !2
  %idxprom1 = sext i32 %4 to i64
  %arrayidx2 = getelementptr inbounds i32, i32* %sourceTypes, i64 %idxprom1
  %5 = load i32, i32* %arrayidx2, align 4, !tbaa !2
  switch i32 %5, label %for.inc [
    i32 1, label %sw.bb
    i32 2, label %sw.bb5
    i32 3, label %sw.bb10
  ]

sw.bb:                                            ; preds = %for.body
  %add3 = add i32 %rowSize.028, 5
  br label %for.inc

sw.bb5:                                           ; preds = %for.body
  %add8 = add i32 %rowSize.028, 9
  br label %for.inc

sw.bb10:                                          ; preds = %for.body
  %add13 = add i32 %rowSize.028, 9
  br label %for.inc

for.inc:                                          ; preds = %sw.bb, %sw.bb5, %sw.bb10, %for.body
  %rowSize.1 = phi i32 [ %rowSize.028, %for.body ], [ %add13, %sw.bb10 ], [ %add8, %sw.bb5 ], [ %add3, %sw.bb ]
  %indvars.iv.next = or i64 %indvars.iv, 1
  %arrayidx.1 = getelementptr inbounds i32, i32* %outputCols, i64 %indvars.iv.next
  %6 = load i32, i32* %arrayidx.1, align 4, !tbaa !2
  %idxprom1.1 = sext i32 %6 to i64
  %arrayidx2.1 = getelementptr inbounds i32, i32* %sourceTypes, i64 %idxprom1.1
  %7 = load i32, i32* %arrayidx2.1, align 4, !tbaa !2
  switch i32 %7, label %for.inc.1 [
    i32 1, label %sw.bb.1
    i32 2, label %sw.bb5.1
    i32 3, label %sw.bb10.1
  ]

sw.bb10.1:                                        ; preds = %for.inc
  %add13.1 = add i32 %rowSize.1, 9
  br label %for.inc.1

sw.bb5.1:                                         ; preds = %for.inc
  %add8.1 = add i32 %rowSize.1, 9
  br label %for.inc.1

sw.bb.1:                                          ; preds = %for.inc
  %add3.1 = add i32 %rowSize.1, 5
  br label %for.inc.1

for.inc.1:                                        ; preds = %sw.bb.1, %sw.bb5.1, %sw.bb10.1, %for.inc
  %rowSize.1.1 = phi i32 [ %rowSize.1, %for.inc ], [ %add13.1, %sw.bb10.1 ], [ %add8.1, %sw.bb5.1 ], [ %add3.1, %sw.bb.1 ]
  %indvars.iv.next.1 = add nuw nsw i64 %indvars.iv, 2
  %niter.nsub.1 = add i64 %niter, -2
  %niter.ncmp.1 = icmp eq i64 %niter.nsub.1, 0
  br i1 %niter.ncmp.1, label %for.cond.cleanup.loopexit.unr-lcssa, label %for.body, !llvm.loop !129
}

; Function Attrs: norecurse nounwind readnone uwtable willreturn mustprogress
define dso_local i32 @_Z13getTableCountii(i32 %positionCount, i32 %maxRowCount) local_unnamed_addr #10 {
entry:
  %add = add i32 %positionCount, -1
  %sub = add i32 %add, %maxRowCount
  %div = sdiv i32 %sub, %maxRowCount
  ret i32 %div
}

; Function Attrs: uwtable
define dso_local %class.Table** @_Z13sortGetOutputllPi(i64 %contextAddress, i64 %sortAddress, i32* nocapture %tableCountAddr) local_unnamed_addr #3 personality i8* bitcast (i32 (...)* @__gxx_personality_v0 to i8*) {
entry:
  %0 = inttoptr i64 %contextAddress to %struct.JitSortContext*
  %1 = inttoptr i64 %sortAddress to %class.Sort*
  %pagesIndex.i = getelementptr inbounds %class.Sort, %class.Sort* %1, i64 0, i32 9
  %2 = load %class.PagesIndex*, %class.PagesIndex** %pagesIndex.i, align 8, !tbaa !117
  %positionCount.i = getelementptr inbounds %class.PagesIndex, %class.PagesIndex* %2, i64 0, i32 4
  %3 = load i32, i32* %positionCount.i, align 8, !tbaa !124
  %outputColsCount.i = getelementptr inbounds %class.Sort, %class.Sort* %1, i64 0, i32 4
  %4 = load i32, i32* %outputColsCount.i, align 8, !tbaa !130
  %outputCols.i = getelementptr inbounds %class.Sort, %class.Sort* %1, i64 0, i32 3
  %5 = load i32*, i32** %outputCols.i, align 8, !tbaa !131
  %sourceTypes.i = getelementptr inbounds %class.Sort, %class.Sort* %1, i64 0, i32 1
  %6 = load i32*, i32** %sourceTypes.i, align 8, !tbaa !119
  %cmp26.i = icmp sgt i32 %4, 0
  br i1 %cmp26.i, label %for.body.preheader.i, label %_Z14getMaxRowCountPiS_i.exit

for.body.preheader.i:                             ; preds = %entry
  %wide.trip.count.i = zext i32 %4 to i64
  %xtraiter = and i64 %wide.trip.count.i, 1
  %7 = icmp eq i32 %4, 1
  br i1 %7, label %_Z14getMaxRowCountPiS_i.exit.loopexit.unr-lcssa, label %for.body.preheader.i.new

for.body.preheader.i.new:                         ; preds = %for.body.preheader.i
  %unroll_iter = and i64 %wide.trip.count.i, 4294967294
  br label %for.body.i

for.body.i:                                       ; preds = %for.inc.i.1, %for.body.preheader.i.new
  %indvars.iv.i = phi i64 [ 0, %for.body.preheader.i.new ], [ %indvars.iv.next.i.1, %for.inc.i.1 ]
  %rowSize.028.i = phi i32 [ 0, %for.body.preheader.i.new ], [ %rowSize.1.i.1, %for.inc.i.1 ]
  %niter = phi i64 [ %unroll_iter, %for.body.preheader.i.new ], [ %niter.nsub.1, %for.inc.i.1 ]
  %arrayidx.i = getelementptr inbounds i32, i32* %5, i64 %indvars.iv.i
  %8 = load i32, i32* %arrayidx.i, align 4, !tbaa !2
  %idxprom1.i = sext i32 %8 to i64
  %arrayidx2.i = getelementptr inbounds i32, i32* %6, i64 %idxprom1.i
  %9 = load i32, i32* %arrayidx2.i, align 4, !tbaa !2
  switch i32 %9, label %for.inc.i [
    i32 1, label %sw.bb.i
    i32 2, label %sw.bb5.i
    i32 3, label %sw.bb10.i
  ]

sw.bb.i:                                          ; preds = %for.body.i
  %add3.i = add i32 %rowSize.028.i, 5
  br label %for.inc.i

sw.bb5.i:                                         ; preds = %for.body.i
  %add8.i = add i32 %rowSize.028.i, 9
  br label %for.inc.i

sw.bb10.i:                                        ; preds = %for.body.i
  %add13.i = add i32 %rowSize.028.i, 9
  br label %for.inc.i

for.inc.i:                                        ; preds = %sw.bb10.i, %sw.bb5.i, %sw.bb.i, %for.body.i
  %rowSize.1.i = phi i32 [ %rowSize.028.i, %for.body.i ], [ %add13.i, %sw.bb10.i ], [ %add8.i, %sw.bb5.i ], [ %add3.i, %sw.bb.i ]
  %indvars.iv.next.i = or i64 %indvars.iv.i, 1
  %arrayidx.i.1 = getelementptr inbounds i32, i32* %5, i64 %indvars.iv.next.i
  %10 = load i32, i32* %arrayidx.i.1, align 4, !tbaa !2
  %idxprom1.i.1 = sext i32 %10 to i64
  %arrayidx2.i.1 = getelementptr inbounds i32, i32* %6, i64 %idxprom1.i.1
  %11 = load i32, i32* %arrayidx2.i.1, align 4, !tbaa !2
  switch i32 %11, label %for.inc.i.1 [
    i32 1, label %sw.bb.i.1
    i32 2, label %sw.bb5.i.1
    i32 3, label %sw.bb10.i.1
  ]

_Z14getMaxRowCountPiS_i.exit.loopexit.unr-lcssa:  ; preds = %for.inc.i.1, %for.body.preheader.i
  %rowSize.1.i.lcssa.ph = phi i32 [ undef, %for.body.preheader.i ], [ %rowSize.1.i.1, %for.inc.i.1 ]
  %indvars.iv.i.unr = phi i64 [ 0, %for.body.preheader.i ], [ %indvars.iv.next.i.1, %for.inc.i.1 ]
  %rowSize.028.i.unr = phi i32 [ 0, %for.body.preheader.i ], [ %rowSize.1.i.1, %for.inc.i.1 ]
  %lcmp.mod.not = icmp eq i64 %xtraiter, 0
  br i1 %lcmp.mod.not, label %_Z14getMaxRowCountPiS_i.exit, label %for.body.i.epil

for.body.i.epil:                                  ; preds = %_Z14getMaxRowCountPiS_i.exit.loopexit.unr-lcssa
  %arrayidx.i.epil = getelementptr inbounds i32, i32* %5, i64 %indvars.iv.i.unr
  %12 = load i32, i32* %arrayidx.i.epil, align 4, !tbaa !2
  %idxprom1.i.epil = sext i32 %12 to i64
  %arrayidx2.i.epil = getelementptr inbounds i32, i32* %6, i64 %idxprom1.i.epil
  %13 = load i32, i32* %arrayidx2.i.epil, align 4, !tbaa !2
  switch i32 %13, label %_Z14getMaxRowCountPiS_i.exit [
    i32 1, label %sw.bb.i.epil
    i32 2, label %sw.bb5.i.epil
    i32 3, label %sw.bb10.i.epil
  ]

sw.bb10.i.epil:                                   ; preds = %for.body.i.epil
  %add13.i.epil = add i32 %rowSize.028.i.unr, 9
  br label %_Z14getMaxRowCountPiS_i.exit

sw.bb5.i.epil:                                    ; preds = %for.body.i.epil
  %add8.i.epil = add i32 %rowSize.028.i.unr, 9
  br label %_Z14getMaxRowCountPiS_i.exit

sw.bb.i.epil:                                     ; preds = %for.body.i.epil
  %add3.i.epil = add i32 %rowSize.028.i.unr, 5
  br label %_Z14getMaxRowCountPiS_i.exit

_Z14getMaxRowCountPiS_i.exit:                     ; preds = %_Z14getMaxRowCountPiS_i.exit.loopexit.unr-lcssa, %for.body.i.epil, %sw.bb10.i.epil, %sw.bb5.i.epil, %sw.bb.i.epil, %entry
  %rowSize.0.lcssa.i = phi i32 [ 0, %entry ], [ %rowSize.1.i.lcssa.ph, %_Z14getMaxRowCountPiS_i.exit.loopexit.unr-lcssa ], [ %rowSize.028.i.unr, %for.body.i.epil ], [ %add13.i.epil, %sw.bb10.i.epil ], [ %add8.i.epil, %sw.bb5.i.epil ], [ %add3.i.epil, %sw.bb.i.epil ]
  %14 = load i32, i32* @DEFAULT_MAX_PAGE_SIZE_IN_BYTES, align 4, !tbaa !2
  %add15.i = add i32 %rowSize.0.lcssa.i, -1
  %sub.i96 = add i32 %add15.i, %14
  %div.i97 = sdiv i32 %sub.i96, %rowSize.0.lcssa.i
  %add.i = add i32 %3, -1
  %sub.i = add i32 %add.i, %div.i97
  %div.i = sdiv i32 %sub.i, %div.i97
  store i32 %div.i, i32* %tableCountAddr, align 4, !tbaa !2
  %cmp = icmp eq i32 %div.i, 0
  br i1 %cmp, label %cleanup, label %if.end

if.end:                                           ; preds = %_Z14getMaxRowCountPiS_i.exit
  %conv = sext i32 %div.i to i64
  %mul = shl nsw i64 %conv, 3
  %call7 = tail call noalias i8* @malloc(i64 %mul) #22
  %15 = bitcast i8* %call7 to %class.Table**
  %cmp8105 = icmp sgt i32 %div.i, 0
  br i1 %cmp8105, label %for.body.lr.ph, label %cleanup

for.body.lr.ph:                                   ; preds = %if.end
  %conv.i = zext i32 %4 to i64
  %16 = shl nuw nsw i64 %conv.i, 2
  %cmp11.not = icmp eq i64 %contextAddress, 0
  %allocColumnsFunc13 = getelementptr inbounds %struct.JitSortContext, %struct.JitSortContext* %0, i64 0, i32 3
  %getResultFunc14 = getelementptr inbounds %struct.JitSortContext, %struct.JitSortContext* %0, i64 0, i32 4
  %17 = ptrtoint %class.PagesIndex* %2 to i64
  %wide.trip.count = zext i32 %div.i to i64
  br i1 %cmp11.not, label %for.body.us, label %for.body

for.body.us:                                      ; preds = %for.body.lr.ph, %invoke.cont.us
  %indvars.iv = phi i64 [ %indvars.iv.next, %invoke.cont.us ], [ 0, %for.body.lr.ph ]
  %position.0107.us = phi i32 [ %add.us, %invoke.cont.us ], [ 0, %for.body.lr.ph ]
  %sub.us = sub nsw i32 %3, %position.0107.us
  %cmp.i.us = icmp slt i32 %sub.us, %div.i97
  %.sroa.speculated.us = select i1 %cmp.i.us, i32 %sub.us, i32 %div.i97
  %call10.us = tail call noalias nonnull dereferenceable(64) i8* @_Znwm(i64 64) #25
  %18 = bitcast i8* %call10.us to i32 (...)***
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [4 x i8*] }, { [4 x i8*] }* @_ZTV5Table, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %18, align 8, !tbaa !111
  %data.i.us = getelementptr inbounds i8, i8* %call10.us, i64 16
  tail call void @llvm.memset.p0i8.i64(i8* nonnull align 8 dereferenceable(24) %data.i.us, i8 0, i64 24, i1 false) #22
  %positionCount2.i.us = getelementptr inbounds i8, i8* %call10.us, i64 48
  %19 = bitcast i8* %positionCount2.i.us to i32*
  store i32 %.sroa.speculated.us, i32* %19, align 8, !tbaa !132
  %columnCount3.i.us = getelementptr inbounds i8, i8* %call10.us, i64 52
  %20 = bitcast i8* %columnCount3.i.us to i32*
  store i32 %4, i32* %20, align 4, !tbaa !136
  %call.i95.us = invoke noalias nonnull i8* @_Znam(i64 %16) #25
          to label %invoke.cont.us unwind label %lpad.us-lcssa.us

invoke.cont.us:                                   ; preds = %for.body.us
  %types.i.us = getelementptr inbounds i8, i8* %call10.us, i64 40
  %21 = bitcast i8* %types.i.us to i8**
  store i8* %call.i95.us, i8** %21, align 8, !tbaa !137
  %columnSize.i.us = getelementptr inbounds i8, i8* %call10.us, i64 56
  %22 = bitcast i8* %columnSize.i.us to i32*
  store i32 0, i32* %22, align 8, !tbaa !138
  %23 = ptrtoint i8* %call10.us to i64
  tail call void @_Z12allocColumnslPiS_ii(i64 %23, i32* %6, i32* %5, i32 %4, i32 %.sroa.speculated.us)
  tail call void @_Z9getResultlPiilS_ii(i64 %17, i32* %5, i32 %4, i64 %23, i32* %6, i32 %position.0107.us, i32 %.sroa.speculated.us)
  %add.us = add nsw i32 %.sroa.speculated.us, %position.0107.us
  %arrayidx.us = getelementptr inbounds %class.Table*, %class.Table** %15, i64 %indvars.iv
  %24 = bitcast %class.Table** %arrayidx.us to i8**
  store i8* %call10.us, i8** %24, align 8, !tbaa !37
  %indvars.iv.next = add nuw nsw i64 %indvars.iv, 1
  %exitcond.not = icmp eq i64 %indvars.iv.next, %wide.trip.count
  br i1 %exitcond.not, label %cleanup, label %for.body.us, !llvm.loop !139

lpad.us-lcssa.us:                                 ; preds = %for.body.us
  %lpad.us-lcssa109.us = landingpad { i8*, i32 }
          cleanup
  br label %lpad

for.body:                                         ; preds = %for.body.lr.ph, %invoke.cont
  %indvars.iv114 = phi i64 [ %indvars.iv.next115, %invoke.cont ], [ 0, %for.body.lr.ph ]
  %position.0107 = phi i32 [ %add, %invoke.cont ], [ 0, %for.body.lr.ph ]
  %sub = sub nsw i32 %3, %position.0107
  %cmp.i = icmp slt i32 %sub, %div.i97
  %.sroa.speculated = select i1 %cmp.i, i32 %sub, i32 %div.i97
  %call10 = tail call noalias nonnull dereferenceable(64) i8* @_Znwm(i64 64) #25
  %25 = bitcast i8* %call10 to i32 (...)***
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [4 x i8*] }, { [4 x i8*] }* @_ZTV5Table, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %25, align 8, !tbaa !111
  %data.i = getelementptr inbounds i8, i8* %call10, i64 16
  tail call void @llvm.memset.p0i8.i64(i8* nonnull align 8 dereferenceable(24) %data.i, i8 0, i64 24, i1 false) #22
  %positionCount2.i = getelementptr inbounds i8, i8* %call10, i64 48
  %26 = bitcast i8* %positionCount2.i to i32*
  store i32 %.sroa.speculated, i32* %26, align 8, !tbaa !132
  %columnCount3.i = getelementptr inbounds i8, i8* %call10, i64 52
  %27 = bitcast i8* %columnCount3.i to i32*
  store i32 %4, i32* %27, align 4, !tbaa !136
  %call.i95 = invoke noalias nonnull i8* @_Znam(i64 %16) #25
          to label %invoke.cont unwind label %lpad.us-lcssa

invoke.cont:                                      ; preds = %for.body
  %types.i = getelementptr inbounds i8, i8* %call10, i64 40
  %28 = bitcast i8* %types.i to i8**
  store i8* %call.i95, i8** %28, align 8, !tbaa !137
  %columnSize.i = getelementptr inbounds i8, i8* %call10, i64 56
  %29 = bitcast i8* %columnSize.i to i32*
  store i32 0, i32* %29, align 8, !tbaa !138
  %30 = load void (i64, i32*, i32*, i32, i32)*, void (i64, i32*, i32*, i32, i32)** %allocColumnsFunc13, align 8, !tbaa !104
  %31 = load void (i64, i32*, i32, i64, i32*, i32, i32)*, void (i64, i32*, i32, i64, i32*, i32, i32)** %getResultFunc14, align 8, !tbaa !105
  %32 = ptrtoint i8* %call10 to i64
  tail call void %30(i64 %32, i32* %6, i32* %5, i32 %4, i32 %.sroa.speculated)
  tail call void %31(i64 %17, i32* %5, i32 %4, i64 %32, i32* %6, i32 %position.0107, i32 %.sroa.speculated)
  %add = add nsw i32 %.sroa.speculated, %position.0107
  %arrayidx = getelementptr inbounds %class.Table*, %class.Table** %15, i64 %indvars.iv114
  %33 = bitcast %class.Table** %arrayidx to i8**
  store i8* %call10, i8** %33, align 8, !tbaa !37
  %indvars.iv.next115 = add nuw nsw i64 %indvars.iv114, 1
  %exitcond117.not = icmp eq i64 %indvars.iv.next115, %wide.trip.count
  br i1 %exitcond117.not, label %cleanup, label %for.body, !llvm.loop !139

lpad.us-lcssa:                                    ; preds = %for.body
  %lpad.us-lcssa109 = landingpad { i8*, i32 }
          cleanup
  br label %lpad

lpad:                                             ; preds = %lpad.us-lcssa.us, %lpad.us-lcssa
  %call10.lcssa = phi i8* [ %call10, %lpad.us-lcssa ], [ %call10.us, %lpad.us-lcssa.us ]
  %34 = phi { i8*, i32 } [ %lpad.us-lcssa109, %lpad.us-lcssa ], [ %lpad.us-lcssa109.us, %lpad.us-lcssa.us ]
  tail call void @_ZdlPv(i8* nonnull %call10.lcssa) #26
  resume { i8*, i32 } %34

cleanup:                                          ; preds = %invoke.cont, %invoke.cont.us, %if.end, %_Z14getMaxRowCountPiS_i.exit
  %retval.0 = phi %class.Table** [ null, %_Z14getMaxRowCountPiS_i.exit ], [ %15, %if.end ], [ %15, %invoke.cont.us ], [ %15, %invoke.cont ]
  ret %class.Table** %retval.0

sw.bb10.i.1:                                      ; preds = %for.inc.i
  %add13.i.1 = add i32 %rowSize.1.i, 9
  br label %for.inc.i.1

sw.bb5.i.1:                                       ; preds = %for.inc.i
  %add8.i.1 = add i32 %rowSize.1.i, 9
  br label %for.inc.i.1

sw.bb.i.1:                                        ; preds = %for.inc.i
  %add3.i.1 = add i32 %rowSize.1.i, 5
  br label %for.inc.i.1

for.inc.i.1:                                      ; preds = %sw.bb.i.1, %sw.bb5.i.1, %sw.bb10.i.1, %for.inc.i
  %rowSize.1.i.1 = phi i32 [ %rowSize.1.i, %for.inc.i ], [ %add13.i.1, %sw.bb10.i.1 ], [ %add8.i.1, %sw.bb5.i.1 ], [ %add3.i.1, %sw.bb.i.1 ]
  %indvars.iv.next.i.1 = add nuw nsw i64 %indvars.iv.i, 2
  %niter.nsub.1 = add i64 %niter, -2
  %niter.ncmp.1 = icmp eq i64 %niter.nsub.1, 0
  br i1 %niter.ncmp.1, label %_Z14getMaxRowCountPiS_i.exit.loopexit.unr-lcssa, label %for.body.i, !llvm.loop !129
}

; Function Attrs: inaccessiblememonly nofree nounwind willreturn
declare dso_local noalias noundef i8* @malloc(i64) local_unnamed_addr #11

; Function Attrs: nobuiltin nounwind
declare dso_local void @_ZdlPv(i8*) local_unnamed_addr #12

declare dso_local void @_Z12allocColumnslPiS_ii(i64, i32*, i32*, i32, i32) local_unnamed_addr #0

declare dso_local void @_Z9getResultlPiilS_ii(i64, i32*, i32, i64, i32*, i32, i32) local_unnamed_addr #0

; Function Attrs: noinline noreturn nounwind
define linkonce_odr hidden void @__clang_call_terminate(i8* %0) local_unnamed_addr #13 comdat {
  %2 = tail call i8* @__cxa_begin_catch(i8* %0) #22
  tail call void @_ZSt9terminatev() #23
  unreachable
}

declare dso_local i8* @__cxa_begin_catch(i8*) local_unnamed_addr

declare dso_local void @_ZSt9terminatev() local_unnamed_addr

declare dso_local i8* @_ZN4llvm3sys14DynamicLibrary19getPermanentLibraryEPKcPNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEE(i8*, %"class.std::__cxx11::basic_string"*) local_unnamed_addr #0

; Function Attrs: uwtable mustprogress
define linkonce_odr dso_local %"struct.std::_Rb_tree_node"* @_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE7_M_copyINSH_11_Alloc_nodeEEEPSt13_Rb_tree_nodeISB_EPKSL_PSt18_Rb_tree_node_baseRT_(%"class.std::_Rb_tree"* nonnull dereferenceable(48) %this, %"struct.std::_Rb_tree_node"* %__x, %"struct.std::_Rb_tree_node_base"* %__p, %"struct.std::_Rb_tree<std::__cxx11::basic_string<char>, std::pair<const std::__cxx11::basic_string<char>, codegen::ParamValue *>, std::_Select1st<std::pair<const std::__cxx11::basic_string<char>, codegen::ParamValue *>>, std::less<std::__cxx11::basic_string<char>>>::_Alloc_node"* nonnull align 8 dereferenceable(8) %__node_gen) local_unnamed_addr #8 comdat align 2 personality i8* bitcast (i32 (...)* @__gxx_personality_v0 to i8*) {
entry:
  %_M_storage.i.i = getelementptr inbounds %"struct.std::_Rb_tree_node", %"struct.std::_Rb_tree_node"* %__x, i64 0, i32 1
  %0 = bitcast %"struct.__gnu_cxx::__aligned_membuf.409"* %_M_storage.i.i to %"struct.std::pair.410"*
  %_M_t.i.i = getelementptr inbounds %"struct.std::_Rb_tree<std::__cxx11::basic_string<char>, std::pair<const std::__cxx11::basic_string<char>, codegen::ParamValue *>, std::_Select1st<std::pair<const std::__cxx11::basic_string<char>, codegen::ParamValue *>>, std::less<std::__cxx11::basic_string<char>>>::_Alloc_node", %"struct.std::_Rb_tree<std::__cxx11::basic_string<char>, std::pair<const std::__cxx11::basic_string<char>, codegen::ParamValue *>, std::_Select1st<std::pair<const std::__cxx11::basic_string<char>, codegen::ParamValue *>>, std::less<std::__cxx11::basic_string<char>>>::_Alloc_node"* %__node_gen, i64 0, i32 0
  %1 = load %"class.std::_Rb_tree"*, %"class.std::_Rb_tree"** %_M_t.i.i, align 8, !tbaa !140
  %call2.i.i.i.i.i.i = tail call noalias nonnull i8* @_Znwm(i64 72) #24
  %2 = bitcast i8* %call2.i.i.i.i.i.i to %"struct.std::_Rb_tree_node"*
  tail call void @_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE17_M_construct_nodeIJRKSB_EEEvPSt13_Rb_tree_nodeISB_EDpOT_(%"class.std::_Rb_tree"* nonnull dereferenceable(48) %1, %"struct.std::_Rb_tree_node"* nonnull %2, %"struct.std::pair.410"* nonnull align 8 dereferenceable(40) %0)
  %_M_color.i = getelementptr inbounds %"struct.std::_Rb_tree_node", %"struct.std::_Rb_tree_node"* %__x, i64 0, i32 0, i32 0
  %3 = load i32, i32* %_M_color.i, align 8, !tbaa !142
  %_M_color3.i = bitcast i8* %call2.i.i.i.i.i.i to i32*
  store i32 %3, i32* %_M_color3.i, align 8, !tbaa !142
  %_M_left.i = getelementptr inbounds i8, i8* %call2.i.i.i.i.i.i, i64 16
  tail call void @llvm.memset.p0i8.i64(i8* nonnull align 8 dereferenceable(16) %_M_left.i, i8 0, i64 16, i1 false)
  %4 = bitcast i8* %call2.i.i.i.i.i.i to %"struct.std::_Rb_tree_node_base"*
  %_M_parent = getelementptr inbounds i8, i8* %call2.i.i.i.i.i.i, i64 8
  %5 = bitcast i8* %_M_parent to %"struct.std::_Rb_tree_node_base"**
  store %"struct.std::_Rb_tree_node_base"* %__p, %"struct.std::_Rb_tree_node_base"** %5, align 8, !tbaa !143
  %_M_right = getelementptr inbounds %"struct.std::_Rb_tree_node", %"struct.std::_Rb_tree_node"* %__x, i64 0, i32 0, i32 3
  %6 = load %"struct.std::_Rb_tree_node_base"*, %"struct.std::_Rb_tree_node_base"** %_M_right, align 8, !tbaa !40
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
  store %"struct.std::_Rb_tree_node_base"* %8, %"struct.std::_Rb_tree_node_base"** %9, align 8, !tbaa !40
  br label %if.end

lpad:                                             ; preds = %if.then
  %10 = landingpad { i8*, i32 }
          catch i8* null
  br label %catch

if.end:                                           ; preds = %invoke.cont, %entry
  %__x.addr.0.in.in60 = getelementptr inbounds %"struct.std::_Rb_tree_node", %"struct.std::_Rb_tree_node"* %__x, i64 0, i32 0, i32 2
  %__x.addr.0.in61 = bitcast %"struct.std::_Rb_tree_node_base"** %__x.addr.0.in.in60 to %"struct.std::_Rb_tree_node"**
  %__x.addr.062 = load %"struct.std::_Rb_tree_node"*, %"struct.std::_Rb_tree_node"** %__x.addr.0.in61, align 8, !tbaa !38
  %cmp.not63 = icmp eq %"struct.std::_Rb_tree_node"* %__x.addr.062, null
  br i1 %cmp.not63, label %try.cont, label %while.body

while.body:                                       ; preds = %if.end, %if.end17
  %__x.addr.065 = phi %"struct.std::_Rb_tree_node"* [ %__x.addr.0, %if.end17 ], [ %__x.addr.062, %if.end ]
  %__p.addr.064 = phi %"struct.std::_Rb_tree_node_base"* [ %15, %if.end17 ], [ %4, %if.end ]
  %11 = load %"class.std::_Rb_tree"*, %"class.std::_Rb_tree"** %_M_t.i.i, align 8, !tbaa !140
  %call2.i.i.i.i.i.i5357 = invoke noalias nonnull i8* @_Znwm(i64 72) #24
          to label %call2.i.i.i.i.i.i53.noexc unwind label %lpad6

call2.i.i.i.i.i.i53.noexc:                        ; preds = %while.body
  %_M_storage.i.i51 = getelementptr inbounds %"struct.std::_Rb_tree_node", %"struct.std::_Rb_tree_node"* %__x.addr.065, i64 0, i32 1
  %12 = bitcast %"struct.__gnu_cxx::__aligned_membuf.409"* %_M_storage.i.i51 to %"struct.std::pair.410"*
  %13 = bitcast i8* %call2.i.i.i.i.i.i5357 to %"struct.std::_Rb_tree_node"*
  invoke void @_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE17_M_construct_nodeIJRKSB_EEEvPSt13_Rb_tree_nodeISB_EDpOT_(%"class.std::_Rb_tree"* nonnull dereferenceable(48) %11, %"struct.std::_Rb_tree_node"* nonnull %13, %"struct.std::pair.410"* nonnull align 8 dereferenceable(40) %12)
          to label %invoke.cont7 unwind label %lpad6

invoke.cont7:                                     ; preds = %call2.i.i.i.i.i.i53.noexc
  %_M_color.i54 = getelementptr inbounds %"struct.std::_Rb_tree_node", %"struct.std::_Rb_tree_node"* %__x.addr.065, i64 0, i32 0, i32 0
  %14 = load i32, i32* %_M_color.i54, align 8, !tbaa !142
  %_M_color3.i55 = bitcast i8* %call2.i.i.i.i.i.i5357 to i32*
  store i32 %14, i32* %_M_color3.i55, align 8, !tbaa !142
  %_M_left.i56 = getelementptr inbounds i8, i8* %call2.i.i.i.i.i.i5357, i64 16
  tail call void @llvm.memset.p0i8.i64(i8* nonnull align 8 dereferenceable(16) %_M_left.i56, i8 0, i64 16, i1 false)
  %15 = bitcast i8* %call2.i.i.i.i.i.i5357 to %"struct.std::_Rb_tree_node_base"*
  %_M_left = getelementptr inbounds %"struct.std::_Rb_tree_node_base", %"struct.std::_Rb_tree_node_base"* %__p.addr.064, i64 0, i32 2
  %16 = bitcast %"struct.std::_Rb_tree_node_base"** %_M_left to i8**
  store i8* %call2.i.i.i.i.i.i5357, i8** %16, align 8, !tbaa !38
  %_M_parent9 = getelementptr inbounds i8, i8* %call2.i.i.i.i.i.i5357, i64 8
  %17 = bitcast i8* %_M_parent9 to %"struct.std::_Rb_tree_node_base"**
  store %"struct.std::_Rb_tree_node_base"* %__p.addr.064, %"struct.std::_Rb_tree_node_base"** %17, align 8, !tbaa !143
  %_M_right10 = getelementptr inbounds %"struct.std::_Rb_tree_node", %"struct.std::_Rb_tree_node"* %__x.addr.065, i64 0, i32 0, i32 3
  %18 = load %"struct.std::_Rb_tree_node_base"*, %"struct.std::_Rb_tree_node_base"** %_M_right10, align 8, !tbaa !40
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
  store %"struct.std::_Rb_tree_node_base"* %20, %"struct.std::_Rb_tree_node_base"** %21, align 8, !tbaa !40
  br label %if.end17

lpad6:                                            ; preds = %call2.i.i.i.i.i.i53.noexc, %while.body, %if.then12
  %22 = landingpad { i8*, i32 }
          catch i8* null
  br label %catch

catch:                                            ; preds = %lpad6, %lpad
  %.pn = phi { i8*, i32 } [ %22, %lpad6 ], [ %10, %lpad ]
  %exn.slot.0 = extractvalue { i8*, i32 } %.pn, 0
  %23 = tail call i8* @__cxa_begin_catch(i8* %exn.slot.0) #22
  invoke void @_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE8_M_eraseEPSt13_Rb_tree_nodeISB_E(%"class.std::_Rb_tree"* nonnull dereferenceable(48) %this, %"struct.std::_Rb_tree_node"* nonnull %2)
          to label %invoke.cont20 unwind label %lpad19

invoke.cont20:                                    ; preds = %catch
  invoke void @__cxa_rethrow() #27
          to label %unreachable unwind label %lpad19

if.end17:                                         ; preds = %invoke.cont14, %invoke.cont7
  %__x.addr.0.in.in = getelementptr inbounds %"struct.std::_Rb_tree_node", %"struct.std::_Rb_tree_node"* %__x.addr.065, i64 0, i32 0, i32 2
  %__x.addr.0.in = bitcast %"struct.std::_Rb_tree_node_base"** %__x.addr.0.in.in to %"struct.std::_Rb_tree_node"**
  %__x.addr.0 = load %"struct.std::_Rb_tree_node"*, %"struct.std::_Rb_tree_node"** %__x.addr.0.in, align 8, !tbaa !38
  %cmp.not = icmp eq %"struct.std::_Rb_tree_node"* %__x.addr.0, null
  br i1 %cmp.not, label %try.cont, label %while.body, !llvm.loop !144

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
  tail call void @__clang_call_terminate(i8* %26) #23
  unreachable

unreachable:                                      ; preds = %invoke.cont20
  unreachable
}

; Function Attrs: uwtable
define linkonce_odr dso_local void @_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE8_M_eraseEPSt13_Rb_tree_nodeISB_E(%"class.std::_Rb_tree"* nonnull dereferenceable(48) %this, %"struct.std::_Rb_tree_node"* %__x) local_unnamed_addr #3 comdat align 2 personality i32 (...)* @__gxx_personality_v0 {
entry:
  %cmp.not7 = icmp eq %"struct.std::_Rb_tree_node"* %__x, null
  br i1 %cmp.not7, label %while.end, label %while.body

while.body:                                       ; preds = %entry, %_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE12_M_drop_nodeEPSt13_Rb_tree_nodeISB_E.exit
  %__x.addr.08 = phi %"struct.std::_Rb_tree_node"* [ %3, %_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE12_M_drop_nodeEPSt13_Rb_tree_nodeISB_E.exit ], [ %__x, %entry ]
  %_M_right.i = getelementptr inbounds %"struct.std::_Rb_tree_node", %"struct.std::_Rb_tree_node"* %__x.addr.08, i64 0, i32 0, i32 3
  %0 = bitcast %"struct.std::_Rb_tree_node_base"** %_M_right.i to %"struct.std::_Rb_tree_node"**
  %1 = load %"struct.std::_Rb_tree_node"*, %"struct.std::_Rb_tree_node"** %0, align 8, !tbaa !40
  tail call void @_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE8_M_eraseEPSt13_Rb_tree_nodeISB_E(%"class.std::_Rb_tree"* nonnull dereferenceable(48) %this, %"struct.std::_Rb_tree_node"* %1)
  %_M_left.i = getelementptr inbounds %"struct.std::_Rb_tree_node", %"struct.std::_Rb_tree_node"* %__x.addr.08, i64 0, i32 0, i32 2
  %2 = bitcast %"struct.std::_Rb_tree_node_base"** %_M_left.i to %"struct.std::_Rb_tree_node"**
  %3 = load %"struct.std::_Rb_tree_node"*, %"struct.std::_Rb_tree_node"** %2, align 8, !tbaa !38
  %_M_storage.i.i.i = getelementptr inbounds %"struct.std::_Rb_tree_node", %"struct.std::_Rb_tree_node"* %__x.addr.08, i64 0, i32 1
  %_M_p.i.i.i.i.i.i.i.i.i = bitcast %"struct.__gnu_cxx::__aligned_membuf.409"* %_M_storage.i.i.i to i8**
  %4 = load i8*, i8** %_M_p.i.i.i.i.i.i.i.i.i, align 8, !tbaa !31
  %5 = getelementptr inbounds %"struct.std::_Rb_tree_node", %"struct.std::_Rb_tree_node"* %__x.addr.08, i64 0, i32 1, i32 0, i64 16
  %cmp.i.i.i.i.i.i.i.i = icmp eq i8* %4, %5
  br i1 %cmp.i.i.i.i.i.i.i.i, label %_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE12_M_drop_nodeEPSt13_Rb_tree_nodeISB_E.exit, label %if.then.i.i.i.i.i.i.i

if.then.i.i.i.i.i.i.i:                            ; preds = %while.body
  tail call void @_ZdlPv(i8* %4) #22
  br label %_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE12_M_drop_nodeEPSt13_Rb_tree_nodeISB_E.exit

_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE12_M_drop_nodeEPSt13_Rb_tree_nodeISB_E.exit: ; preds = %while.body, %if.then.i.i.i.i.i.i.i
  %6 = bitcast %"struct.std::_Rb_tree_node"* %__x.addr.08 to i8*
  tail call void @_ZdlPv(i8* nonnull %6) #22
  %cmp.not = icmp eq %"struct.std::_Rb_tree_node"* %3, null
  br i1 %cmp.not, label %while.end, label %while.body, !llvm.loop !145

while.end:                                        ; preds = %_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE12_M_drop_nodeEPSt13_Rb_tree_nodeISB_E.exit, %entry
  ret void
}

declare dso_local void @__cxa_rethrow() local_unnamed_addr

declare dso_local void @__cxa_end_catch() local_unnamed_addr

; Function Attrs: uwtable
define linkonce_odr dso_local void @_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE17_M_construct_nodeIJRKSB_EEEvPSt13_Rb_tree_nodeISB_EDpOT_(%"class.std::_Rb_tree"* nonnull dereferenceable(48) %this, %"struct.std::_Rb_tree_node"* %__node, %"struct.std::pair.410"* nonnull align 8 dereferenceable(40) %__args) local_unnamed_addr #3 comdat align 2 personality i8* bitcast (i32 (...)* @__gxx_personality_v0 to i8*) {
entry:
  %__dnew.i.i.i.i.i.i.i = alloca i64, align 8
  %_M_storage.i = getelementptr inbounds %"struct.std::_Rb_tree_node", %"struct.std::_Rb_tree_node"* %__node, i64 0, i32 1
  %0 = getelementptr inbounds %"struct.std::_Rb_tree_node", %"struct.std::_Rb_tree_node"* %__node, i64 0, i32 1, i32 0, i64 16
  %1 = bitcast %"struct.__gnu_cxx::__aligned_membuf.409"* %_M_storage.i to i8**
  store i8* %0, i8** %1, align 8, !tbaa !29
  %_M_p.i15.i.i.i.i = getelementptr inbounds %"struct.std::pair.410", %"struct.std::pair.410"* %__args, i64 0, i32 0, i32 0, i32 0
  %2 = load i8*, i8** %_M_p.i15.i.i.i.i, align 8, !tbaa !31
  %_M_string_length.i.i.i.i.i = getelementptr inbounds %"struct.std::pair.410", %"struct.std::pair.410"* %__args, i64 0, i32 0, i32 1
  %3 = load i64, i64* %_M_string_length.i.i.i.i.i, align 8, !tbaa !34
  %4 = bitcast i64* %__dnew.i.i.i.i.i.i.i to i8*
  call void @llvm.lifetime.start.p0i8(i64 8, i8* nonnull %4) #22
  store i64 %3, i64* %__dnew.i.i.i.i.i.i.i, align 8, !tbaa !19
  %cmp3.i.i.i.i.i.i.i = icmp ugt i64 %3, 15
  br i1 %cmp3.i.i.i.i.i.i.i, label %if.then4.i.i.i.i.i.i.i, label %if.end6.i.i.i.i.i.i.i

if.then4.i.i.i.i.i.i.i:                           ; preds = %entry
  %first.i.i.i = bitcast %"struct.__gnu_cxx::__aligned_membuf.409"* %_M_storage.i to %"class.std::__cxx11::basic_string"*
  %call5.i.i.i14.i.i.i.i12 = invoke i8* @_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE9_M_createERmm(%"class.std::__cxx11::basic_string"* nonnull dereferenceable(32) %first.i.i.i, i64* nonnull align 8 dereferenceable(8) %__dnew.i.i.i.i.i.i.i, i64 0)
          to label %call5.i.i.i14.i.i.i.i.noexc unwind label %lpad

call5.i.i.i14.i.i.i.i.noexc:                      ; preds = %if.then4.i.i.i.i.i.i.i
  store i8* %call5.i.i.i14.i.i.i.i12, i8** %1, align 8, !tbaa !31
  %5 = load i64, i64* %__dnew.i.i.i.i.i.i.i, align 8, !tbaa !19
  %6 = bitcast i8* %0 to i64*
  store i64 %5, i64* %6, align 8, !tbaa !33
  br label %if.end6.i.i.i.i.i.i.i

if.end6.i.i.i.i.i.i.i:                            ; preds = %entry, %call5.i.i.i14.i.i.i.i.noexc
  %7 = phi i8* [ %call5.i.i.i14.i.i.i.i12, %call5.i.i.i14.i.i.i.i.noexc ], [ %0, %entry ]
  switch i64 %3, label %if.end.i.i.i.i.i.i.i.i.i.i [
    i64 1, label %if.then.i.i.i.i.i.i.i.i.i
    i64 0, label %try.cont
  ]

if.then.i.i.i.i.i.i.i.i.i:                        ; preds = %if.end6.i.i.i.i.i.i.i
  %8 = load i8, i8* %2, align 1, !tbaa !33
  store i8 %8, i8* %7, align 1, !tbaa !33
  br label %try.cont

if.end.i.i.i.i.i.i.i.i.i.i:                       ; preds = %if.end6.i.i.i.i.i.i.i
  call void @llvm.memcpy.p0i8.p0i8.i64(i8* align 1 %7, i8* align 1 %2, i64 %3, i1 false) #22
  br label %try.cont

lpad:                                             ; preds = %if.then4.i.i.i.i.i.i.i
  %9 = landingpad { i8*, i32 }
          catch i8* null
  %10 = extractvalue { i8*, i32 } %9, 0
  %11 = call i8* @__cxa_begin_catch(i8* %10) #22
  %12 = bitcast %"struct.std::_Rb_tree_node"* %__node to i8*
  call void @_ZdlPv(i8* %12) #22
  invoke void @__cxa_rethrow() #27
          to label %unreachable unwind label %lpad5

lpad5:                                            ; preds = %lpad
  %13 = landingpad { i8*, i32 }
          cleanup
  invoke void @__cxa_end_catch()
          to label %eh.resume unwind label %terminate.lpad

try.cont:                                         ; preds = %if.end.i.i.i.i.i.i.i.i.i.i, %if.then.i.i.i.i.i.i.i.i.i, %if.end6.i.i.i.i.i.i.i
  %14 = load i64, i64* %__dnew.i.i.i.i.i.i.i, align 8, !tbaa !19
  %_M_string_length.i.i.i.i.i.i.i.i.i = getelementptr inbounds %"struct.std::_Rb_tree_node", %"struct.std::_Rb_tree_node"* %__node, i64 0, i32 1, i32 0, i64 8
  %15 = bitcast i8* %_M_string_length.i.i.i.i.i.i.i.i.i to i64*
  store i64 %14, i64* %15, align 8, !tbaa !34
  %16 = load i8*, i8** %1, align 8, !tbaa !31
  %arrayidx.i.i.i.i.i.i.i.i = getelementptr inbounds i8, i8* %16, i64 %14
  store i8 0, i8* %arrayidx.i.i.i.i.i.i.i.i, align 1, !tbaa !33
  call void @llvm.lifetime.end.p0i8(i64 8, i8* nonnull %4) #22
  %second.i.i.i = getelementptr inbounds %"struct.std::_Rb_tree_node", %"struct.std::_Rb_tree_node"* %__node, i64 0, i32 1, i32 0, i64 32
  %17 = bitcast i8* %second.i.i.i to %"class.codegen::ParamValue"**
  %second3.i.i.i = getelementptr inbounds %"struct.std::pair.410", %"struct.std::pair.410"* %__args, i64 0, i32 1
  %18 = load %"class.codegen::ParamValue"*, %"class.codegen::ParamValue"** %second3.i.i.i, align 8, !tbaa !146
  store %"class.codegen::ParamValue"* %18, %"class.codegen::ParamValue"** %17, align 8, !tbaa !146
  ret void

eh.resume:                                        ; preds = %lpad5
  resume { i8*, i32 } %13

terminate.lpad:                                   ; preds = %lpad5
  %19 = landingpad { i8*, i32 }
          catch i8* null
  %20 = extractvalue { i8*, i32 } %19, 0
  call void @__clang_call_terminate(i8* %20) #23
  unreachable

unreachable:                                      ; preds = %lpad
  unreachable
}

declare dso_local i8* @_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE9_M_createERmm(%"class.std::__cxx11::basic_string"* nonnull dereferenceable(32), i64* nonnull align 8 dereferenceable(8), i64) local_unnamed_addr #0

; Function Attrs: argmemonly nofree nosync nounwind willreturn
declare void @llvm.memcpy.p0i8.p0i8.i64(i8* noalias nocapture writeonly, i8* noalias nocapture readonly, i64, i1 immarg) #4

declare dso_local void @_ZN7codegen12HammerConfig14init_func_passEv(%"class.codegen::HammerConfig"* nonnull dereferenceable(1616)) local_unnamed_addr #0

declare dso_local void @_ZN7codegen12HammerConfig16init_module_passEv(%"class.codegen::HammerConfig"* nonnull dereferenceable(1616)) local_unnamed_addr #0

; Function Attrs: uwtable
define linkonce_odr dso_local void @_ZN4llvm3orc5LLJIT6lookupERNS0_8JITDylibENS_9StringRefE(%"class.llvm::Expected"* noalias sret(%"class.llvm::Expected") align 8 %agg.result, %"class.llvm::orc::LLJIT"* nonnull dereferenceable(568) %this, %"class.llvm::orc::JITDylib"* nonnull align 8 dereferenceable(272) %JD, i8* %UnmangledName.coerce0, i64 %UnmangledName.coerce1) local_unnamed_addr #3 comdat align 2 personality i8* bitcast (i32 (...)* @__gxx_personality_v0 to i8*) {
entry:
  %ref.tmp2.i.i = alloca i32, align 4
  %agg.tmp.i = alloca %"class.llvm::orc::SymbolStringPtr", align 8
  %ref.tmp = alloca %"class.std::__cxx11::basic_string", align 8
  %0 = bitcast %"class.std::__cxx11::basic_string"* %ref.tmp to i8*
  call void @llvm.lifetime.start.p0i8(i64 32, i8* nonnull %0) #22
  call void @_ZNK4llvm3orc5LLJIT6mangleB5cxx11ENS_9StringRefE(%"class.std::__cxx11::basic_string"* nonnull sret(%"class.std::__cxx11::basic_string") align 8 %ref.tmp, %"class.llvm::orc::LLJIT"* nonnull dereferenceable(568) %this, i8* %UnmangledName.coerce0, i64 %UnmangledName.coerce1)
  %_M_p.i.i.i = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp, i64 0, i32 0, i32 0
  %1 = load i8*, i8** %_M_p.i.i.i, align 8, !tbaa !31
  %_M_string_length.i.i = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp, i64 0, i32 1
  %2 = load i64, i64* %_M_string_length.i.i, align 8, !tbaa !34
  %3 = bitcast %"class.llvm::orc::SymbolStringPtr"* %agg.tmp.i to i8*
  call void @llvm.lifetime.start.p0i8(i64 8, i8* nonnull %3)
  %_M_head_impl.i.i.i.i.i.i.i.i = getelementptr inbounds %"class.llvm::orc::LLJIT", %"class.llvm::orc::LLJIT"* %this, i64 0, i32 0, i32 0, i32 0, i32 0, i32 0, i32 0
  %4 = load %"class.llvm::orc::ExecutionSession"*, %"class.llvm::orc::ExecutionSession"** %_M_head_impl.i.i.i.i.i.i.i.i, align 8, !tbaa !37, !noalias !148
  call void @llvm.experimental.noalias.scope.decl(metadata !151)
  %_M_ptr.i.i.i.i = getelementptr inbounds %"class.llvm::orc::ExecutionSession", %"class.llvm::orc::ExecutionSession"* %4, i64 0, i32 2, i32 0, i32 0
  %5 = load %"class.llvm::orc::SymbolStringPool"*, %"class.llvm::orc::SymbolStringPool"** %_M_ptr.i.i.i.i, align 8, !tbaa !154, !noalias !151
  call void @llvm.experimental.noalias.scope.decl(metadata !157), !noalias !151
  br i1 icmp ne (i8* bitcast (i32 (i32*, void (i8*)*)* @__pthread_key_create to i8*), i8* null), label %_ZL20__gthread_mutex_lockP15pthread_mutex_t.exit.i.i.i.i, label %_ZNSt10lock_guardISt5mutexEC2ERS0_.exit.i.i

_ZL20__gthread_mutex_lockP15pthread_mutex_t.exit.i.i.i.i: ; preds = %entry
  %_M_mutex.i.i.i.i = getelementptr inbounds %"class.llvm::orc::SymbolStringPool", %"class.llvm::orc::SymbolStringPool"* %5, i64 0, i32 0, i32 0, i32 0
  %call1.i.i.i.i.i = call i32 @pthread_mutex_lock(%union.pthread_mutex_t* nonnull %_M_mutex.i.i.i.i) #22, !noalias !160
  %tobool.not.i.i.i.i = icmp eq i32 %call1.i.i.i.i.i, 0
  br i1 %tobool.not.i.i.i.i, label %_ZNSt10lock_guardISt5mutexEC2ERS0_.exit.i.i, label %if.then.i.i.i.i

if.then.i.i.i.i:                                  ; preds = %_ZL20__gthread_mutex_lockP15pthread_mutex_t.exit.i.i.i.i
  invoke void @_ZSt20__throw_system_errori(i32 %call1.i.i.i.i.i) #27
          to label %.noexc8 unwind label %lpad

.noexc8:                                          ; preds = %if.then.i.i.i.i
  unreachable

_ZNSt10lock_guardISt5mutexEC2ERS0_.exit.i.i:      ; preds = %_ZL20__gthread_mutex_lockP15pthread_mutex_t.exit.i.i.i.i, %entry
  %Pool.i.i = getelementptr inbounds %"class.llvm::orc::SymbolStringPool", %"class.llvm::orc::SymbolStringPool"* %5, i64 0, i32 1
  %6 = bitcast i32* %ref.tmp2.i.i to i8*
  call void @llvm.lifetime.start.p0i8(i64 4, i8* nonnull %6) #22, !noalias !160
  store i32 0, i32* %ref.tmp2.i.i, align 4, !tbaa !2, !noalias !160
  %call.i.i = invoke { %"class.llvm::StringMapEntryBase"**, i8 } @_ZN4llvm9StringMapISt6atomicImENS_15MallocAllocatorEE11try_emplaceIJiEEESt4pairINS_17StringMapIteratorIS2_EEbENS_9StringRefEDpOT_(%"class.llvm::StringMap.141"* nonnull dereferenceable(25) %Pool.i.i, i8* %1, i64 %2, i32* nonnull align 4 dereferenceable(4) %ref.tmp2.i.i)
          to label %invoke.cont.i.i unwind label %lpad.i.i, !noalias !160

invoke.cont.i.i:                                  ; preds = %_ZNSt10lock_guardISt5mutexEC2ERS0_.exit.i.i
  %7 = extractvalue { %"class.llvm::StringMapEntryBase"**, i8 } %call.i.i, 0
  call void @llvm.lifetime.end.p0i8(i64 4, i8* nonnull %6) #22, !noalias !160
  %8 = bitcast %"class.llvm::StringMapEntryBase"** %7 to %"class.llvm::StringMapEntry"**
  %9 = load %"class.llvm::StringMapEntry"*, %"class.llvm::StringMapEntry"** %8, align 8, !tbaa !37, !noalias !160
  %S2.i.i.i = getelementptr inbounds %"class.llvm::orc::SymbolStringPtr", %"class.llvm::orc::SymbolStringPtr"* %agg.tmp.i, i64 0, i32 0
  store %"class.llvm::StringMapEntry"* %9, %"class.llvm::StringMapEntry"** %S2.i.i.i, align 8, !tbaa !161, !alias.scope !160
  %10 = ptrtoint %"class.llvm::StringMapEntry"* %9 to i64
  %sub.i.i.i.i = add i64 %10, -1
  %cmp.i.i.i.i = icmp ult i64 %sub.i.i.i.i, -32
  br i1 %cmp.i.i.i.i, label %if.then.i.i.i, label %invoke.cont11.i.i

if.then.i.i.i:                                    ; preds = %invoke.cont.i.i
  %_M_i.i.i.i.i = getelementptr inbounds %"class.llvm::StringMapEntry", %"class.llvm::StringMapEntry"* %9, i64 0, i32 0, i32 1, i32 0, i32 0
  %11 = atomicrmw add i64* %_M_i.i.i.i.i, i64 1 seq_cst, !noalias !160
  br label %invoke.cont11.i.i

invoke.cont11.i.i:                                ; preds = %if.then.i.i.i, %invoke.cont.i.i
  br i1 icmp ne (i8* bitcast (i32 (i32*, void (i8*)*)* @__pthread_key_create to i8*), i8* null), label %if.then.i.i.i25.i.i, label %.noexc

if.then.i.i.i25.i.i:                              ; preds = %invoke.cont11.i.i
  %_M_mutex.i.i23.i.i = getelementptr inbounds %"class.llvm::orc::SymbolStringPool", %"class.llvm::orc::SymbolStringPool"* %5, i64 0, i32 0, i32 0, i32 0
  %call1.i.i.i24.i.i = call i32 @pthread_mutex_unlock(%union.pthread_mutex_t* nonnull %_M_mutex.i.i23.i.i) #22, !noalias !160
  br label %.noexc

lpad.i.i:                                         ; preds = %_ZNSt10lock_guardISt5mutexEC2ERS0_.exit.i.i
  %12 = landingpad { i8*, i32 }
          cleanup
  call void @llvm.lifetime.end.p0i8(i64 4, i8* nonnull %6) #22, !noalias !160
  br i1 icmp ne (i8* bitcast (i32 (i32*, void (i8*)*)* @__pthread_key_create to i8*), i8* null), label %if.then.i.i.i.i.i, label %lpad.body

if.then.i.i.i.i.i:                                ; preds = %lpad.i.i
  %_M_mutex.i.i20.i.i = getelementptr inbounds %"class.llvm::orc::SymbolStringPool", %"class.llvm::orc::SymbolStringPool"* %5, i64 0, i32 0, i32 0, i32 0
  %call1.i.i.i21.i.i = call i32 @pthread_mutex_unlock(%union.pthread_mutex_t* nonnull %_M_mutex.i.i20.i.i) #22, !noalias !160
  br label %lpad.body

.noexc:                                           ; preds = %if.then.i.i.i25.i.i, %invoke.cont11.i.i
  invoke void @_ZN4llvm3orc5LLJIT19lookupLinkerMangledERNS0_8JITDylibENS0_15SymbolStringPtrE(%"class.llvm::Expected"* sret(%"class.llvm::Expected") align 8 %agg.result, %"class.llvm::orc::LLJIT"* nonnull dereferenceable(568) %this, %"class.llvm::orc::JITDylib"* nonnull align 8 dereferenceable(272) %JD, %"class.llvm::orc::SymbolStringPtr"* nonnull %agg.tmp.i)
          to label %invoke.cont.i unwind label %lpad.i

invoke.cont.i:                                    ; preds = %.noexc
  %13 = load %"class.llvm::StringMapEntry"*, %"class.llvm::StringMapEntry"** %S2.i.i.i, align 8, !tbaa !161, !noalias !148
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
  %17 = load %"class.llvm::StringMapEntry"*, %"class.llvm::StringMapEntry"** %S2.i.i.i, align 8, !tbaa !161, !noalias !148
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
  %20 = load i8*, i8** %_M_p.i.i.i, align 8, !tbaa !31
  %21 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp, i64 0, i32 2
  %arraydecay.i.i.i.i12 = bitcast %union.anon* %21 to i8*
  %cmp.i.i.i13 = icmp eq i8* %20, %arraydecay.i.i.i.i12
  br i1 %cmp.i.i.i13, label %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit16, label %if.then.i.i14

if.then.i.i14:                                    ; preds = %invoke.cont3
  call void @_ZdlPv(i8* %20) #22
  br label %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit16

_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit16: ; preds = %invoke.cont3, %if.then.i.i14
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %0) #22
  ret void

lpad:                                             ; preds = %if.then.i.i.i.i
  %22 = landingpad { i8*, i32 }
          cleanup
  br label %lpad.body

lpad.body:                                        ; preds = %lpad, %if.then.i.i.i.i.i, %lpad.i.i, %lpad.i, %if.then.i.i7
  %eh.lpad-body = phi { i8*, i32 } [ %16, %if.then.i.i7 ], [ %16, %lpad.i ], [ %22, %lpad ], [ %12, %if.then.i.i.i.i.i ], [ %12, %lpad.i.i ]
  %23 = load i8*, i8** %_M_p.i.i.i, align 8, !tbaa !31
  %24 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %ref.tmp, i64 0, i32 2
  %arraydecay.i.i.i.i = bitcast %union.anon* %24 to i8*
  %cmp.i.i.i = icmp eq i8* %23, %arraydecay.i.i.i.i
  br i1 %cmp.i.i.i, label %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit, label %if.then.i.i

if.then.i.i:                                      ; preds = %lpad.body
  call void @_ZdlPv(i8* %23) #22
  br label %_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit

_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEED2Ev.exit: ; preds = %lpad.body, %if.then.i.i
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %0) #22
  resume { i8*, i32 } %eh.lpad-body
}

declare dso_local void @_ZNK4llvm3orc5LLJIT6mangleB5cxx11ENS_9StringRefE(%"class.std::__cxx11::basic_string"* sret(%"class.std::__cxx11::basic_string") align 8, %"class.llvm::orc::LLJIT"* nonnull dereferenceable(568), i8*, i64) local_unnamed_addr #0

declare dso_local void @_ZN4llvm3orc5LLJIT19lookupLinkerMangledERNS0_8JITDylibENS0_15SymbolStringPtrE(%"class.llvm::Expected"* sret(%"class.llvm::Expected") align 8, %"class.llvm::orc::LLJIT"* nonnull dereferenceable(568), %"class.llvm::orc::JITDylib"* nonnull align 8 dereferenceable(272), %"class.llvm::orc::SymbolStringPtr"*) local_unnamed_addr #0

; Function Attrs: uwtable
define linkonce_odr dso_local { %"class.llvm::StringMapEntryBase"**, i8 } @_ZN4llvm9StringMapISt6atomicImENS_15MallocAllocatorEE11try_emplaceIJiEEESt4pairINS_17StringMapIteratorIS2_EEbENS_9StringRefEDpOT_(%"class.llvm::StringMap.141"* nonnull dereferenceable(25) %this, i8* %Key.coerce0, i64 %Key.coerce1, i32* nonnull align 4 dereferenceable(4) %Args) local_unnamed_addr #3 comdat align 2 {
entry:
  %0 = getelementptr inbounds %"class.llvm::StringMap.141", %"class.llvm::StringMap.141"* %this, i64 0, i32 0
  %call = tail call i32 @_ZN4llvm13StringMapImpl15LookupBucketForENS_9StringRefE(%"class.llvm::StringMapImpl"* nonnull dereferenceable(24) %0, i8* %Key.coerce0, i64 %Key.coerce1)
  %TheTable = getelementptr inbounds %"class.llvm::StringMap.141", %"class.llvm::StringMap.141"* %this, i64 0, i32 0, i32 0
  %1 = load %"class.llvm::StringMapEntryBase"**, %"class.llvm::StringMapEntryBase"*** %TheTable, align 8, !tbaa !163
  %idxprom = zext i32 %call to i64
  %arrayidx = getelementptr inbounds %"class.llvm::StringMapEntryBase"*, %"class.llvm::StringMapEntryBase"** %1, i64 %idxprom
  %2 = load %"class.llvm::StringMapEntryBase"*, %"class.llvm::StringMapEntryBase"** %arrayidx, align 8, !tbaa !37
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
  %.pre = load %"class.llvm::StringMapEntryBase"*, %"class.llvm::StringMapEntryBase"** %incdec.ptr.i.i.i39, align 8, !tbaa !37
  br label %while.cond.i.i.i38, !llvm.loop !165

if.then8:                                         ; preds = %entry
  %NumTombstones = getelementptr inbounds %"class.llvm::StringMap.141", %"class.llvm::StringMap.141"* %this, i64 0, i32 0, i32 3
  %4 = load i32, i32* %NumTombstones, align 8, !tbaa !166
  %dec = add i32 %4, -1
  store i32 %dec, i32* %NumTombstones, align 8, !tbaa !166
  br label %if.end9

if.end9:                                          ; preds = %entry, %if.then8
  %add1.i = add i64 %Key.coerce1, 17
  %call.i.i = tail call noalias nonnull i8* @_ZN4llvm15allocate_bufferEmm(i64 %add1.i, i64 8)
  %keyLength2.i.i.i.i = bitcast i8* %call.i.i to i64*
  store i64 %Key.coerce1, i64* %keyLength2.i.i.i.i, align 8, !tbaa !167
  %5 = load i32, i32* %Args, align 4, !tbaa !2
  %conv.i.i.i = sext i32 %5 to i64
  %_M_i.i.i.i.i.i = getelementptr inbounds i8, i8* %call.i.i, i64 8
  %6 = bitcast i8* %_M_i.i.i.i.i.i to i64*
  store i64 %conv.i.i.i, i64* %6, align 8, !tbaa !169
  %add.ptr.i.i = getelementptr inbounds i8, i8* %call.i.i, i64 16
  %cmp.not.i = icmp eq i64 %Key.coerce1, 0
  br i1 %cmp.not.i, label %_ZN4llvm14StringMapEntryISt6atomicImEE6CreateINS_15MallocAllocatorEJiEEEPS3_NS_9StringRefERT_DpOT0_.exit, label %if.then.i

if.then.i:                                        ; preds = %if.end9
  tail call void @llvm.memcpy.p0i8.p0i8.i64(i8* nonnull align 1 %add.ptr.i.i, i8* align 1 %Key.coerce0, i64 %Key.coerce1, i1 false)
  br label %_ZN4llvm14StringMapEntryISt6atomicImEE6CreateINS_15MallocAllocatorEJiEEEPS3_NS_9StringRefERT_DpOT0_.exit

_ZN4llvm14StringMapEntryISt6atomicImEE6CreateINS_15MallocAllocatorEJiEEEPS3_NS_9StringRefERT_DpOT0_.exit: ; preds = %if.end9, %if.then.i
  %arrayidx.i = getelementptr inbounds i8, i8* %add.ptr.i.i, i64 %Key.coerce1
  store i8 0, i8* %arrayidx.i, align 1, !tbaa !33
  %7 = bitcast %"class.llvm::StringMapEntryBase"** %arrayidx to i8**
  store i8* %call.i.i, i8** %7, align 8, !tbaa !37
  %NumItems = getelementptr inbounds %"class.llvm::StringMap.141", %"class.llvm::StringMap.141"* %this, i64 0, i32 0, i32 2
  %8 = load i32, i32* %NumItems, align 4, !tbaa !171
  %inc = add i32 %8, 1
  store i32 %inc, i32* %NumItems, align 4, !tbaa !171
  %NumTombstones14 = getelementptr inbounds %"class.llvm::StringMap.141", %"class.llvm::StringMap.141"* %this, i64 0, i32 0, i32 3
  %9 = load i32, i32* %NumTombstones14, align 8, !tbaa !166
  %add = add i32 %9, %inc
  %NumBuckets = getelementptr inbounds %"class.llvm::StringMap.141", %"class.llvm::StringMap.141"* %this, i64 0, i32 0, i32 1
  %10 = load i32, i32* %NumBuckets, align 8, !tbaa !172
  %cmp15.not = icmp ugt i32 %add, %10
  br i1 %cmp15.not, label %cond.false, label %cond.end

cond.false:                                       ; preds = %_ZN4llvm14StringMapEntryISt6atomicImEE6CreateINS_15MallocAllocatorEJiEEEPS3_NS_9StringRefERT_DpOT0_.exit
  tail call void @__assert_fail(i8* getelementptr inbounds ([39 x i8], [39 x i8]* @.str.27, i64 0, i64 0), i8* getelementptr inbounds ([42 x i8], [42 x i8]* @.str.28, i64 0, i64 0), i32 326, i8* getelementptr inbounds ([206 x i8], [206 x i8]* @__PRETTY_FUNCTION__._ZN4llvm9StringMapISt6atomicImENS_15MallocAllocatorEE11try_emplaceIJiEEESt4pairINS_17StringMapIteratorIS2_EEbENS_9StringRefEDpOT_, i64 0, i64 0)) #23
  unreachable

cond.end:                                         ; preds = %_ZN4llvm14StringMapEntryISt6atomicImEE6CreateINS_15MallocAllocatorEJiEEEPS3_NS_9StringRefERT_DpOT0_.exit
  %call16 = tail call i32 @_ZN4llvm13StringMapImpl11RehashTableEj(%"class.llvm::StringMapImpl"* nonnull dereferenceable(24) %0, i32 %call)
  %11 = load %"class.llvm::StringMapEntryBase"**, %"class.llvm::StringMapEntryBase"*** %TheTable, align 8, !tbaa !163
  %idx.ext19 = zext i32 %call16 to i64
  %add.ptr20 = getelementptr inbounds %"class.llvm::StringMapEntryBase"*, %"class.llvm::StringMapEntryBase"** %11, i64 %idx.ext19
  br label %while.cond.i.i.i

while.cond.i.i.i:                                 ; preds = %while.body.i.i.i, %cond.end
  %ref.tmp17.sroa.0.0 = phi %"class.llvm::StringMapEntryBase"** [ %add.ptr20, %cond.end ], [ %incdec.ptr.i.i.i, %while.body.i.i.i ]
  %12 = load %"class.llvm::StringMapEntryBase"*, %"class.llvm::StringMapEntryBase"** %ref.tmp17.sroa.0.0, align 8, !tbaa !37
  %magicptr.i.i.i = ptrtoint %"class.llvm::StringMapEntryBase"* %12 to i64
  switch i64 %magicptr.i.i.i, label %cleanup [
    i64 0, label %while.body.i.i.i
    i64 -8, label %while.body.i.i.i
  ]

while.body.i.i.i:                                 ; preds = %while.cond.i.i.i, %while.cond.i.i.i
  %incdec.ptr.i.i.i = getelementptr inbounds %"class.llvm::StringMapEntryBase"*, %"class.llvm::StringMapEntryBase"** %ref.tmp17.sroa.0.0, i64 1
  br label %while.cond.i.i.i, !llvm.loop !165

cleanup:                                          ; preds = %while.cond.i.i.i, %while.cond.i.i.i38
  %ref.tmp.sroa.0.0.pn = phi %"class.llvm::StringMapEntryBase"** [ %ref.tmp.sroa.0.0, %while.cond.i.i.i38 ], [ %ref.tmp17.sroa.0.0, %while.cond.i.i.i ]
  %.pn = phi i8 [ 0, %while.cond.i.i.i38 ], [ 1, %while.cond.i.i.i ]
  %.fca.0.insert.i42.pn = insertvalue { %"class.llvm::StringMapEntryBase"**, i8 } undef, %"class.llvm::StringMapEntryBase"** %ref.tmp.sroa.0.0.pn, 0
  %call5.pn = insertvalue { %"class.llvm::StringMapEntryBase"**, i8 } %.fca.0.insert.i42.pn, i8 %.pn, 1
  ret { %"class.llvm::StringMapEntryBase"**, i8 } %call5.pn
}

; Function Attrs: noreturn
declare dso_local void @_ZSt20__throw_system_errori(i32) local_unnamed_addr #14

; Function Attrs: nounwind
declare extern_weak dso_local i32 @pthread_mutex_lock(%union.pthread_mutex_t*) local_unnamed_addr #1

; Function Attrs: nounwind
declare extern_weak dso_local i32 @__pthread_key_create(i32*, void (i8*)*) #1

declare dso_local i32 @_ZN4llvm13StringMapImpl15LookupBucketForENS_9StringRefE(%"class.llvm::StringMapImpl"* nonnull dereferenceable(24), i8*, i64) local_unnamed_addr #0

; Function Attrs: noreturn nounwind
declare dso_local void @__assert_fail(i8*, i8*, i32, i8*) local_unnamed_addr #15

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
declare dso_local void @free(i8* nocapture noundef) local_unnamed_addr #16

; Function Attrs: nounwind
declare dso_local void @_ZN4llvm11LLVMContextD1Ev(%"class.llvm::LLVMContext"* nonnull dereferenceable(8)) unnamed_addr #1

; Function Attrs: nobuiltin nofree allocsize(0)
declare dso_local nonnull i8* @_Znam(i64) local_unnamed_addr #6

; Function Attrs: nounwind uwtable
define linkonce_odr dso_local void @_ZN5TableD2Ev(%class.Table* nonnull dereferenceable(60) %this) unnamed_addr #17 comdat align 2 personality i32 (...)* @__gxx_personality_v0 {
entry:
  %0 = getelementptr inbounds %class.Table, %class.Table* %this, i64 0, i32 0
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [4 x i8*] }, { [4 x i8*] }* @_ZTV5Table, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %0, align 8, !tbaa !111
  %types = getelementptr inbounds %class.Table, %class.Table* %this, i64 0, i32 4
  %1 = load i32*, i32** %types, align 8, !tbaa !137
  %isnull = icmp eq i32* %1, null
  br i1 %isnull, label %delete.end, label %delete.notnull

delete.notnull:                                   ; preds = %entry
  %2 = bitcast i32* %1 to i8*
  tail call void @_ZdaPv(i8* %2) #26
  br label %delete.end

delete.end:                                       ; preds = %delete.notnull, %entry
  %_M_start.i.i = getelementptr inbounds %class.Table, %class.Table* %this, i64 0, i32 3, i32 0, i32 0, i32 0
  %3 = load %class.Column**, %class.Column*** %_M_start.i.i, align 8, !tbaa !173
  %tobool.not.i.i.i = icmp eq %class.Column** %3, null
  br i1 %tobool.not.i.i.i, label %_ZNSt6vectorIP6ColumnSaIS1_EED2Ev.exit, label %if.then.i.i.i

if.then.i.i.i:                                    ; preds = %delete.end
  %4 = bitcast %class.Column** %3 to i8*
  tail call void @_ZdlPv(i8* nonnull %4) #22
  br label %_ZNSt6vectorIP6ColumnSaIS1_EED2Ev.exit

_ZNSt6vectorIP6ColumnSaIS1_EED2Ev.exit:           ; preds = %delete.end, %if.then.i.i.i
  ret void
}

; Function Attrs: nounwind uwtable
define linkonce_odr dso_local void @_ZN5TableD0Ev(%class.Table* nonnull dereferenceable(60) %this) unnamed_addr #17 comdat align 2 personality i32 (...)* @__gxx_personality_v0 {
entry:
  %0 = getelementptr inbounds %class.Table, %class.Table* %this, i64 0, i32 0
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [4 x i8*] }, { [4 x i8*] }* @_ZTV5Table, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %0, align 8, !tbaa !111
  %types.i = getelementptr inbounds %class.Table, %class.Table* %this, i64 0, i32 4
  %1 = load i32*, i32** %types.i, align 8, !tbaa !137
  %isnull.i = icmp eq i32* %1, null
  br i1 %isnull.i, label %delete.end.i, label %delete.notnull.i

delete.notnull.i:                                 ; preds = %entry
  %2 = bitcast i32* %1 to i8*
  tail call void @_ZdaPv(i8* %2) #26
  br label %delete.end.i

delete.end.i:                                     ; preds = %delete.notnull.i, %entry
  %_M_start.i.i.i = getelementptr inbounds %class.Table, %class.Table* %this, i64 0, i32 3, i32 0, i32 0, i32 0
  %3 = load %class.Column**, %class.Column*** %_M_start.i.i.i, align 8, !tbaa !173
  %tobool.not.i.i.i.i = icmp eq %class.Column** %3, null
  br i1 %tobool.not.i.i.i.i, label %_ZN5TableD2Ev.exit, label %if.then.i.i.i.i

if.then.i.i.i.i:                                  ; preds = %delete.end.i
  %4 = bitcast %class.Column** %3 to i8*
  tail call void @_ZdlPv(i8* nonnull %4) #22
  br label %_ZN5TableD2Ev.exit

_ZN5TableD2Ev.exit:                               ; preds = %delete.end.i, %if.then.i.i.i.i
  %5 = bitcast %class.Table* %this to i8*
  tail call void @_ZdlPv(i8* nonnull %5) #26
  ret void
}

; Function Attrs: nobuiltin nounwind
declare dso_local void @_ZdaPv(i8*) local_unnamed_addr #12

; Function Attrs: argmemonly nofree nosync nounwind willreturn writeonly
declare void @llvm.memset.p0i8.i64(i8* nocapture writeonly, i8, i64, i1 immarg) #18

; Function Attrs: uwtable
define linkonce_odr dso_local %"struct.std::_Rb_tree_node_base"* @_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE22_M_emplace_hint_uniqueIJRKSt21piecewise_construct_tSt5tupleIJOS5_EESM_IJEEEEESt17_Rb_tree_iteratorISB_ESt23_Rb_tree_const_iteratorISB_EDpOT_(%"class.std::_Rb_tree"* nonnull dereferenceable(48) %this, %"struct.std::_Rb_tree_node_base"* %__pos.coerce, %"struct.std::piecewise_construct_t"* nonnull align 1 dereferenceable(1) %__args, %"class.std::tuple.436"* nonnull align 8 dereferenceable(8) %__args1, %"class.std::tuple.439"* nonnull align 1 dereferenceable(1) %__args3) local_unnamed_addr #3 comdat align 2 personality i8* bitcast (i32 (...)* @__gxx_personality_v0 to i8*) {
entry:
  %call2.i.i.i.i = tail call noalias nonnull i8* @_Znwm(i64 72) #24
  %_M_storage.i.i.i35 = getelementptr inbounds i8, i8* %call2.i.i.i.i, i64 32
  %_M_head_impl.i.i.i.i.i.i.i.i = getelementptr inbounds %"class.std::tuple.436", %"class.std::tuple.436"* %__args1, i64 0, i32 0, i32 0, i32 0
  %0 = load %"class.std::__cxx11::basic_string"*, %"class.std::__cxx11::basic_string"** %_M_head_impl.i.i.i.i.i.i.i.i, align 8, !tbaa !176
  %1 = getelementptr inbounds i8, i8* %call2.i.i.i.i, i64 48
  %2 = bitcast i8* %_M_storage.i.i.i35 to i8**
  store i8* %1, i8** %2, align 8, !tbaa !29
  %_M_p.i.i25.i.i.i.i.i.i.i = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %0, i64 0, i32 0, i32 0
  %3 = load i8*, i8** %_M_p.i.i25.i.i.i.i.i.i.i, align 8, !tbaa !31
  %4 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %0, i64 0, i32 2
  %arraydecay.i.i.i.i.i.i.i.i.i = bitcast %union.anon* %4 to i8*
  %cmp.i.i.i.i.i.i.i.i36 = icmp eq i8* %3, %arraydecay.i.i.i.i.i.i.i.i.i
  br i1 %cmp.i.i.i.i.i.i.i.i36, label %if.then.i.i.i.i.i.i.i37, label %if.else.i.i.i.i.i.i.i

if.then.i.i.i.i.i.i.i37:                          ; preds = %entry
  tail call void @llvm.memcpy.p0i8.p0i8.i64(i8* nonnull align 1 dereferenceable(16) %1, i8* nonnull align 8 dereferenceable(16) %3, i64 16, i1 false) #22
  br label %invoke.cont

if.else.i.i.i.i.i.i.i:                            ; preds = %entry
  store i8* %3, i8** %2, align 8, !tbaa !31
  %_M_allocated_capacity.i.i.i.i.i.i.i = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %0, i64 0, i32 2, i32 0
  %5 = load i64, i64* %_M_allocated_capacity.i.i.i.i.i.i.i, align 8, !tbaa !33
  %6 = bitcast i8* %1 to i64*
  store i64 %5, i64* %6, align 8, !tbaa !33
  br label %invoke.cont

invoke.cont:                                      ; preds = %if.else.i.i.i.i.i.i.i, %if.then.i.i.i.i.i.i.i37
  %_M_string_length.i22.i.i.i.i.i.i.i = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %0, i64 0, i32 1
  %7 = load i64, i64* %_M_string_length.i22.i.i.i.i.i.i.i, align 8, !tbaa !34
  %8 = getelementptr inbounds i8, i8* %call2.i.i.i.i, i64 40
  %9 = bitcast i8* %8 to i64*
  store i64 %7, i64* %9, align 8, !tbaa !34
  %10 = bitcast %"class.std::__cxx11::basic_string"* %0 to %union.anon**
  store %union.anon* %4, %union.anon** %10, align 8, !tbaa !31
  store i64 0, i64* %_M_string_length.i22.i.i.i.i.i.i.i, align 8, !tbaa !34
  store i8 0, i8* %arraydecay.i.i.i.i.i.i.i.i.i, align 8, !tbaa !33
  %11 = getelementptr inbounds i8, i8* %call2.i.i.i.i, i64 64
  %12 = bitcast i8* %11 to %"class.codegen::ParamValue"**
  store %"class.codegen::ParamValue"* null, %"class.codegen::ParamValue"** %12, align 8, !tbaa !146
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
  %16 = load i64, i64* %9, align 8, !tbaa !34
  %_M_string_length.i14.i.i.i.i = getelementptr inbounds %"struct.std::_Rb_tree_node_base", %"struct.std::_Rb_tree_node_base"* %14, i64 1, i32 1
  %17 = bitcast %"struct.std::_Rb_tree_node_base"** %_M_string_length.i14.i.i.i.i to i64*
  %18 = load i64, i64* %17, align 8, !tbaa !34
  %cmp.i15.i.i.i.i = icmp ugt i64 %16, %18
  %.sroa.speculated.i.i.i.i = select i1 %cmp.i15.i.i.i.i, i64 %18, i64 %16
  %cmp.i13.i.i.i.i = icmp eq i64 %.sroa.speculated.i.i.i.i, 0
  br i1 %cmp.i13.i.i.i.i, label %if.then.i.i.i.i, label %_ZNSt11char_traitsIcE7compareEPKcS2_m.exit.i.i.i.i

_ZNSt11char_traitsIcE7compareEPKcS2_m.exit.i.i.i.i: ; preds = %lor.rhs.i
  %_M_storage.i.i.i13.i = getelementptr inbounds %"struct.std::_Rb_tree_node_base", %"struct.std::_Rb_tree_node_base"* %14, i64 1
  %_M_p.i.i.i.i.i.i = bitcast %"struct.std::_Rb_tree_node_base"* %_M_storage.i.i.i13.i to i8**
  %19 = load i8*, i8** %_M_p.i.i.i.i.i.i, align 8, !tbaa !31
  %20 = load i8*, i8** %2, align 8, !tbaa !31
  %call.i.i.i.i.i = tail call i32 @memcmp(i8* %20, i8* %19, i64 %.sroa.speculated.i.i.i.i) #22
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
  tail call void @_ZSt29_Rb_tree_insert_and_rebalancebPSt18_Rb_tree_node_baseS0_RS_(i1 zeroext %23, %"struct.std::_Rb_tree_node_base"* nonnull %24, %"struct.std::_Rb_tree_node_base"* nonnull %14, %"struct.std::_Rb_tree_node_base"* nonnull align 8 dereferenceable(32) %_M_header.i) #22
  %_M_node_count.i = getelementptr inbounds i8, i8* %25, i64 40
  %26 = bitcast i8* %_M_node_count.i to i64*
  %27 = load i64, i64* %26, align 8, !tbaa !15
  %inc.i = add i64 %27, 1
  store i64 %inc.i, i64* %26, align 8, !tbaa !15
  br label %cleanup

lpad:                                             ; preds = %invoke.cont
  %28 = landingpad { i8*, i32 }
          catch i8* null
  %29 = extractvalue { i8*, i32 } %28, 0
  %30 = tail call i8* @__cxa_begin_catch(i8* %29) #22
  %31 = load i8*, i8** %2, align 8, !tbaa !31
  %cmp.i.i.i.i.i.i.i.i41 = icmp eq i8* %31, %1
  br i1 %cmp.i.i.i.i.i.i.i.i41, label %_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE12_M_drop_nodeEPSt13_Rb_tree_nodeISB_E.exit43, label %if.then.i.i.i.i.i.i.i42

if.then.i.i.i.i.i.i.i42:                          ; preds = %lpad
  tail call void @_ZdlPv(i8* %31) #22
  br label %_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE12_M_drop_nodeEPSt13_Rb_tree_nodeISB_E.exit43

_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE12_M_drop_nodeEPSt13_Rb_tree_nodeISB_E.exit43: ; preds = %lpad, %if.then.i.i.i.i.i.i.i42
  tail call void @_ZdlPv(i8* nonnull %call2.i.i.i.i) #22
  invoke void @__cxa_rethrow() #27
          to label %unreachable unwind label %lpad18

if.end:                                           ; preds = %invoke.cont11
  %32 = load i8*, i8** %2, align 8, !tbaa !31
  %cmp.i.i.i.i.i.i.i.i = icmp eq i8* %32, %1
  br i1 %cmp.i.i.i.i.i.i.i.i, label %_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE12_M_drop_nodeEPSt13_Rb_tree_nodeISB_E.exit, label %if.then.i.i.i.i.i.i.i

if.then.i.i.i.i.i.i.i:                            ; preds = %if.end
  tail call void @_ZdlPv(i8* %32) #22
  br label %_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE12_M_drop_nodeEPSt13_Rb_tree_nodeISB_E.exit

_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE12_M_drop_nodeEPSt13_Rb_tree_nodeISB_E.exit: ; preds = %if.end, %if.then.i.i.i.i.i.i.i
  tail call void @_ZdlPv(i8* nonnull %call2.i.i.i.i) #22
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
  tail call void @__clang_call_terminate(i8* %35) #23
  unreachable

unreachable:                                      ; preds = %_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE12_M_drop_nodeEPSt13_Rb_tree_nodeISB_E.exit43
  unreachable
}

; Function Attrs: argmemonly nofree nounwind readonly willreturn
declare dso_local i32 @memcmp(i8* nocapture, i8* nocapture, i64) local_unnamed_addr #19

; Function Attrs: uwtable
define linkonce_odr dso_local { %"struct.std::_Rb_tree_node_base"*, %"struct.std::_Rb_tree_node_base"* } @_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE29_M_get_insert_hint_unique_posESt23_Rb_tree_const_iteratorISB_ERS7_(%"class.std::_Rb_tree"* nonnull dereferenceable(48) %this, %"struct.std::_Rb_tree_node_base"* %__position.coerce, %"class.std::__cxx11::basic_string"* nonnull align 8 dereferenceable(32) %__k) local_unnamed_addr #3 comdat align 2 personality i32 (...)* @__gxx_personality_v0 {
entry:
  %0 = getelementptr inbounds %"class.std::_Rb_tree", %"class.std::_Rb_tree"* %this, i64 0, i32 0, i32 0, i32 0, i32 0
  %add.ptr.i = getelementptr inbounds i8, i8* %0, i64 8
  %_M_header.i = bitcast i8* %add.ptr.i to %"struct.std::_Rb_tree_node_base"*
  %cmp = icmp eq %"struct.std::_Rb_tree_node_base"* %_M_header.i, %__position.coerce
  br i1 %cmp, label %if.then, label %if.else12

if.then:                                          ; preds = %entry
  %_M_node_count.i = getelementptr inbounds i8, i8* %0, i64 40
  %1 = bitcast i8* %_M_node_count.i to i64*
  %2 = load i64, i64* %1, align 8, !tbaa !15
  %cmp5.not = icmp eq i64 %2, 0
  br i1 %cmp5.not, label %if.else, label %land.lhs.true

land.lhs.true:                                    ; preds = %if.then
  %_M_right.i94 = getelementptr inbounds i8, i8* %0, i64 32
  %3 = bitcast i8* %_M_right.i94 to %"struct.std::_Rb_tree_node_base"**
  %4 = load %"struct.std::_Rb_tree_node_base"*, %"struct.std::_Rb_tree_node_base"** %3, align 8, !tbaa !37
  %_M_string_length.i.i.i.i121 = getelementptr inbounds %"struct.std::_Rb_tree_node_base", %"struct.std::_Rb_tree_node_base"* %4, i64 1, i32 1
  %5 = bitcast %"struct.std::_Rb_tree_node_base"** %_M_string_length.i.i.i.i121 to i64*
  %6 = load i64, i64* %5, align 8, !tbaa !34
  %_M_string_length.i14.i.i.i122 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %__k, i64 0, i32 1
  %7 = load i64, i64* %_M_string_length.i14.i.i.i122, align 8, !tbaa !34
  %cmp.i15.i.i.i123 = icmp ugt i64 %6, %7
  %.sroa.speculated.i.i.i124 = select i1 %cmp.i15.i.i.i123, i64 %7, i64 %6
  %cmp.i13.i.i.i125 = icmp eq i64 %.sroa.speculated.i.i.i124, 0
  br i1 %cmp.i13.i.i.i125, label %if.then.i.i.i133, label %_ZNSt11char_traitsIcE7compareEPKcS2_m.exit.i.i.i130

_ZNSt11char_traitsIcE7compareEPKcS2_m.exit.i.i.i130: ; preds = %land.lhs.true
  %_M_storage.i.i.i117 = getelementptr inbounds %"struct.std::_Rb_tree_node_base", %"struct.std::_Rb_tree_node_base"* %4, i64 1
  %_M_p.i.i.i.i.i126 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %__k, i64 0, i32 0, i32 0
  %8 = load i8*, i8** %_M_p.i.i.i.i.i126, align 8, !tbaa !31
  %_M_p.i.i.i.i127 = bitcast %"struct.std::_Rb_tree_node_base"* %_M_storage.i.i.i117 to i8**
  %9 = load i8*, i8** %_M_p.i.i.i.i127, align 8, !tbaa !31
  %call.i.i.i.i128 = tail call i32 @memcmp(i8* %9, i8* %8, i64 %.sroa.speculated.i.i.i124) #22
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
  %14 = load i64, i64* %_M_string_length.i.i.i.i175, align 8, !tbaa !34
  %_M_string_length.i14.i.i.i176 = getelementptr inbounds %"struct.std::_Rb_tree_node_base", %"struct.std::_Rb_tree_node_base"* %__position.coerce, i64 1, i32 1
  %15 = bitcast %"struct.std::_Rb_tree_node_base"** %_M_string_length.i14.i.i.i176 to i64*
  %16 = load i64, i64* %15, align 8, !tbaa !34
  %cmp.i15.i.i.i177 = icmp ugt i64 %14, %16
  %.sroa.speculated.i.i.i178 = select i1 %cmp.i15.i.i.i177, i64 %16, i64 %14
  %cmp.i13.i.i.i179 = icmp eq i64 %.sroa.speculated.i.i.i178, 0
  br i1 %cmp.i13.i.i.i179, label %if.then.i.i.i187, label %_ZNSt11char_traitsIcE7compareEPKcS2_m.exit.i.i.i184

_ZNSt11char_traitsIcE7compareEPKcS2_m.exit.i.i.i184: ; preds = %if.else12
  %_M_p.i.i.i.i.i180 = bitcast %"struct.std::_Rb_tree_node_base"* %_M_storage.i.i.i169 to i8**
  %17 = load i8*, i8** %_M_p.i.i.i.i.i180, align 8, !tbaa !31
  %_M_p.i.i.i.i181 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %__k, i64 0, i32 0, i32 0
  %18 = load i8*, i8** %_M_p.i.i.i.i181, align 8, !tbaa !31
  %call.i.i.i.i182 = tail call i32 @memcmp(i8* %18, i8* %17, i64 %.sroa.speculated.i.i.i178) #22
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
  %22 = load %"struct.std::_Rb_tree_node_base"*, %"struct.std::_Rb_tree_node_base"** %21, align 8, !tbaa !37
  %cmp21 = icmp eq %"struct.std::_Rb_tree_node_base"* %22, %__position.coerce
  br i1 %cmp21, label %cleanup80, label %if.else25

if.else25:                                        ; preds = %if.then18
  %call.i168 = tail call %"struct.std::_Rb_tree_node_base"* @_ZSt18_Rb_tree_decrementPSt18_Rb_tree_node_base(%"struct.std::_Rb_tree_node_base"* nonnull %__position.coerce) #28
  %_M_string_length.i.i.i.i145 = getelementptr inbounds %"struct.std::_Rb_tree_node_base", %"struct.std::_Rb_tree_node_base"* %call.i168, i64 1, i32 1
  %23 = bitcast %"struct.std::_Rb_tree_node_base"** %_M_string_length.i.i.i.i145 to i64*
  %24 = load i64, i64* %23, align 8, !tbaa !34
  %cmp.i15.i.i.i147 = icmp ugt i64 %24, %14
  %.sroa.speculated.i.i.i148 = select i1 %cmp.i15.i.i.i147, i64 %14, i64 %24
  %cmp.i13.i.i.i149 = icmp eq i64 %.sroa.speculated.i.i.i148, 0
  br i1 %cmp.i13.i.i.i149, label %if.then.i.i.i157, label %_ZNSt11char_traitsIcE7compareEPKcS2_m.exit.i.i.i154

_ZNSt11char_traitsIcE7compareEPKcS2_m.exit.i.i.i154: ; preds = %if.else25
  %_M_storage.i.i.i165 = getelementptr inbounds %"struct.std::_Rb_tree_node_base", %"struct.std::_Rb_tree_node_base"* %call.i168, i64 1
  %_M_p.i.i.i.i.i150 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %__k, i64 0, i32 0, i32 0
  %25 = load i8*, i8** %_M_p.i.i.i.i.i150, align 8, !tbaa !31
  %_M_p.i.i.i.i151 = bitcast %"struct.std::_Rb_tree_node_base"* %_M_storage.i.i.i165 to i8**
  %26 = load i8*, i8** %_M_p.i.i.i.i151, align 8, !tbaa !31
  %call.i.i.i.i152 = tail call i32 @memcmp(i8* %26, i8* %25, i64 %.sroa.speculated.i.i.i148) #22
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
  %30 = load %"struct.std::_Rb_tree_node"*, %"struct.std::_Rb_tree_node"** %29, align 8, !tbaa !40
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
  %33 = load i8*, i8** %_M_p.i.i.i.i.i104, align 8, !tbaa !31
  %_M_p.i.i.i.i105 = bitcast %"struct.std::_Rb_tree_node_base"* %_M_storage.i.i.i169 to i8**
  %34 = load i8*, i8** %_M_p.i.i.i.i105, align 8, !tbaa !31
  %call.i.i.i.i106 = tail call i32 @memcmp(i8* %34, i8* %33, i64 %.sroa.speculated.i.i.i178) #22
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
  %38 = load %"struct.std::_Rb_tree_node_base"*, %"struct.std::_Rb_tree_node_base"** %37, align 8, !tbaa !37
  %cmp53 = icmp eq %"struct.std::_Rb_tree_node_base"* %38, %__position.coerce
  br i1 %cmp53, label %cleanup80, label %if.else57

if.else57:                                        ; preds = %if.then50
  %call.i = tail call %"struct.std::_Rb_tree_node_base"* @_ZSt18_Rb_tree_incrementPSt18_Rb_tree_node_base(%"struct.std::_Rb_tree_node_base"* nonnull %__position.coerce) #28
  %_M_string_length.i14.i.i.i = getelementptr inbounds %"struct.std::_Rb_tree_node_base", %"struct.std::_Rb_tree_node_base"* %call.i, i64 1, i32 1
  %39 = bitcast %"struct.std::_Rb_tree_node_base"** %_M_string_length.i14.i.i.i to i64*
  %40 = load i64, i64* %39, align 8, !tbaa !34
  %cmp.i15.i.i.i = icmp ugt i64 %14, %40
  %.sroa.speculated.i.i.i = select i1 %cmp.i15.i.i.i, i64 %40, i64 %14
  %cmp.i13.i.i.i = icmp eq i64 %.sroa.speculated.i.i.i, 0
  br i1 %cmp.i13.i.i.i, label %if.then.i.i.i, label %_ZNSt11char_traitsIcE7compareEPKcS2_m.exit.i.i.i

_ZNSt11char_traitsIcE7compareEPKcS2_m.exit.i.i.i: ; preds = %if.else57
  %_M_storage.i.i.i = getelementptr inbounds %"struct.std::_Rb_tree_node_base", %"struct.std::_Rb_tree_node_base"* %call.i, i64 1
  %_M_p.i.i.i.i.i = bitcast %"struct.std::_Rb_tree_node_base"* %_M_storage.i.i.i to i8**
  %41 = load i8*, i8** %_M_p.i.i.i.i.i, align 8, !tbaa !31
  %_M_p.i.i.i.i = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %__k, i64 0, i32 0, i32 0
  %42 = load i8*, i8** %_M_p.i.i.i.i, align 8, !tbaa !31
  %call.i.i.i.i = tail call i32 @memcmp(i8* %42, i8* %41, i64 %.sroa.speculated.i.i.i) #22
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
  %46 = load %"struct.std::_Rb_tree_node"*, %"struct.std::_Rb_tree_node"** %45, align 8, !tbaa !40
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
define linkonce_odr dso_local { %"struct.std::_Rb_tree_node_base"*, %"struct.std::_Rb_tree_node_base"* } @_ZNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE24_M_get_insert_unique_posERS7_(%"class.std::_Rb_tree"* nonnull dereferenceable(48) %this, %"class.std::__cxx11::basic_string"* nonnull align 8 dereferenceable(32) %__k) local_unnamed_addr #3 comdat align 2 personality i32 (...)* @__gxx_personality_v0 {
entry:
  %0 = getelementptr inbounds %"class.std::_Rb_tree", %"class.std::_Rb_tree"* %this, i64 0, i32 0, i32 0, i32 0, i32 0
  %_M_parent.i = getelementptr inbounds i8, i8* %0, i64 16
  %1 = bitcast i8* %_M_parent.i to %"struct.std::_Rb_tree_node"**
  %add.ptr.i = getelementptr inbounds i8, i8* %0, i64 8
  %_M_header.i = bitcast i8* %add.ptr.i to %"struct.std::_Rb_tree_node_base"*
  %__x.075 = load %"struct.std::_Rb_tree_node"*, %"struct.std::_Rb_tree_node"** %1, align 8, !tbaa !37
  %cmp.not76 = icmp eq %"struct.std::_Rb_tree_node"* %__x.075, null
  br i1 %cmp.not76, label %if.then, label %while.body.lr.ph

while.body.lr.ph:                                 ; preds = %entry
  %_M_string_length.i.i.i.i35 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %__k, i64 0, i32 1
  %2 = load i64, i64* %_M_string_length.i.i.i.i35, align 8, !tbaa !34
  %_M_p.i.i.i.i41 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %__k, i64 0, i32 0, i32 0
  %3 = load i8*, i8** %_M_p.i.i.i.i41, align 8
  br label %while.body

while.body:                                       ; preds = %while.body.backedge, %while.body.lr.ph
  %__x.077 = phi %"struct.std::_Rb_tree_node"* [ %__x.075, %while.body.lr.ph ], [ %__x.077.be, %while.body.backedge ]
  %_M_string_length.i14.i.i.i36 = getelementptr inbounds %"struct.std::_Rb_tree_node", %"struct.std::_Rb_tree_node"* %__x.077, i64 0, i32 1, i32 0, i64 8
  %4 = bitcast i8* %_M_string_length.i14.i.i.i36 to i64*
  %5 = load i64, i64* %4, align 8, !tbaa !34
  %cmp.i15.i.i.i37 = icmp ugt i64 %2, %5
  %.sroa.speculated.i.i.i38 = select i1 %cmp.i15.i.i.i37, i64 %5, i64 %2
  %cmp.i13.i.i.i39 = icmp eq i64 %.sroa.speculated.i.i.i38, 0
  br i1 %cmp.i13.i.i.i39, label %if.then.i.i.i47, label %_ZNSt11char_traitsIcE7compareEPKcS2_m.exit.i.i.i44

_ZNSt11char_traitsIcE7compareEPKcS2_m.exit.i.i.i44: ; preds = %while.body
  %_M_storage.i.i.i28 = getelementptr inbounds %"struct.std::_Rb_tree_node", %"struct.std::_Rb_tree_node"* %__x.077, i64 0, i32 1
  %_M_p.i.i.i.i.i40 = bitcast %"struct.__gnu_cxx::__aligned_membuf.409"* %_M_storage.i.i.i28 to i8**
  %6 = load i8*, i8** %_M_p.i.i.i.i.i40, align 8, !tbaa !31
  %call.i.i.i.i42 = tail call i32 @memcmp(i8* %3, i8* %6, i64 %.sroa.speculated.i.i.i38) #22
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
  %__x.0 = load %"struct.std::_Rb_tree_node"*, %"struct.std::_Rb_tree_node"** %cond.in, align 8, !tbaa !37
  %cmp.not = icmp eq %"struct.std::_Rb_tree_node"* %__x.0, null
  br i1 %cmp.not, label %while.end, label %while.body.backedge

while.body.backedge:                              ; preds = %cond.end, %cond.end.thread
  %__x.077.be = phi %"struct.std::_Rb_tree_node"* [ %__x.0, %cond.end ], [ %__x.089, %cond.end.thread ]
  br label %while.body, !llvm.loop !178

cond.end.thread:                                  ; preds = %_ZNKSt4lessINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEEclERKS5_S8_.exit52, %if.then.i.i.i47
  %_M_right.i = getelementptr inbounds %"struct.std::_Rb_tree_node", %"struct.std::_Rb_tree_node"* %__x.077, i64 0, i32 0, i32 3
  %cond.in88 = bitcast %"struct.std::_Rb_tree_node_base"** %_M_right.i to %"struct.std::_Rb_tree_node"**
  %__x.089 = load %"struct.std::_Rb_tree_node"*, %"struct.std::_Rb_tree_node"** %cond.in88, align 8, !tbaa !37
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
  %12 = load %"struct.std::_Rb_tree_node_base"*, %"struct.std::_Rb_tree_node_base"** %11, align 8, !tbaa !13
  %cmp.i = icmp eq %"struct.std::_Rb_tree_node_base"* %__y.0.lcssa85, %12
  br i1 %cmp.i, label %cleanup, label %if.else

if.else:                                          ; preds = %if.then
  %call.i = tail call %"struct.std::_Rb_tree_node_base"* @_ZSt18_Rb_tree_decrementPSt18_Rb_tree_node_base(%"struct.std::_Rb_tree_node_base"* nonnull %__y.0.lcssa85) #28
  br label %if.end12

if.end12:                                         ; preds = %while.end.thread92, %if.else
  %__y.0.lcssa84 = phi %"struct.std::_Rb_tree_node_base"* [ %__y.0.lcssa85, %if.else ], [ %9, %while.end.thread92 ]
  %__j.sroa.0.0 = phi %"struct.std::_Rb_tree_node_base"* [ %call.i, %if.else ], [ %9, %while.end.thread92 ]
  %_M_string_length.i.i.i.i = getelementptr inbounds %"struct.std::_Rb_tree_node_base", %"struct.std::_Rb_tree_node_base"* %__j.sroa.0.0, i64 1, i32 1
  %13 = bitcast %"struct.std::_Rb_tree_node_base"** %_M_string_length.i.i.i.i to i64*
  %14 = load i64, i64* %13, align 8, !tbaa !34
  %_M_string_length.i14.i.i.i = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %__k, i64 0, i32 1
  %15 = load i64, i64* %_M_string_length.i14.i.i.i, align 8, !tbaa !34
  %cmp.i15.i.i.i = icmp ugt i64 %14, %15
  %.sroa.speculated.i.i.i = select i1 %cmp.i15.i.i.i, i64 %15, i64 %14
  %cmp.i13.i.i.i = icmp eq i64 %.sroa.speculated.i.i.i, 0
  br i1 %cmp.i13.i.i.i, label %if.then.i.i.i, label %_ZNSt11char_traitsIcE7compareEPKcS2_m.exit.i.i.i

_ZNSt11char_traitsIcE7compareEPKcS2_m.exit.i.i.i: ; preds = %if.end12
  %_M_storage.i.i.i = getelementptr inbounds %"struct.std::_Rb_tree_node_base", %"struct.std::_Rb_tree_node_base"* %__j.sroa.0.0, i64 1
  %_M_p.i.i.i.i.i = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %__k, i64 0, i32 0, i32 0
  %16 = load i8*, i8** %_M_p.i.i.i.i.i, align 8, !tbaa !31
  %_M_p.i.i.i.i = bitcast %"struct.std::_Rb_tree_node_base"* %_M_storage.i.i.i to i8**
  %17 = load i8*, i8** %_M_p.i.i.i.i, align 8, !tbaa !31
  %call.i.i.i.i = tail call i32 @memcmp(i8* %17, i8* %16, i64 %.sroa.speculated.i.i.i) #22
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
declare dso_local %"struct.std::_Rb_tree_node_base"* @_ZSt18_Rb_tree_decrementPSt18_Rb_tree_node_base(%"struct.std::_Rb_tree_node_base"*) local_unnamed_addr #20

; Function Attrs: nounwind readonly willreturn
declare dso_local %"struct.std::_Rb_tree_node_base"* @_ZSt18_Rb_tree_incrementPSt18_Rb_tree_node_base(%"struct.std::_Rb_tree_node_base"*) local_unnamed_addr #20

; Function Attrs: nounwind
declare dso_local void @_ZSt29_Rb_tree_insert_and_rebalancebPSt18_Rb_tree_node_baseS0_RS_(i1 zeroext, %"struct.std::_Rb_tree_node_base"*, %"struct.std::_Rb_tree_node_base"*, %"struct.std::_Rb_tree_node_base"* nonnull align 8 dereferenceable(32)) local_unnamed_addr #1

; Function Attrs: nounwind
declare dso_local void @_ZNSt8__detail15_List_node_base7_M_hookEPS0_(%"struct.std::__detail::_List_node_base"* nonnull dereferenceable(16), %"struct.std::__detail::_List_node_base"*) local_unnamed_addr #1

; Function Attrs: nounwind
declare dso_local void @_ZN4llvm3orc5LLJITD1Ev(%"class.llvm::orc::LLJIT"* nonnull dereferenceable(568)) unnamed_addr #1

; Function Attrs: uwtable
define internal void @_GLOBAL__sub_I_sort_api.cpp() #3 section ".text.startup" {
entry:
  tail call void @_ZNSt8ios_base4InitC1Ev(%"class.std::ios_base::Init"* nonnull dereferenceable(1) @_ZStL8__ioinit)
  %0 = tail call i32 @__cxa_atexit(void (i8*)* bitcast (void (%"class.std::ios_base::Init"*)* @_ZNSt8ios_base4InitD1Ev to void (i8*)*), i8* getelementptr inbounds (%"class.std::ios_base::Init", %"class.std::ios_base::Init"* @_ZStL8__ioinit, i64 0, i32 0), i8* nonnull @__dso_handle) #22
  ret void
}

; Function Attrs: inaccessiblememonly nofree nosync nounwind willreturn
declare void @llvm.experimental.noalias.scope.decl(metadata) #21

attributes #0 = { "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "no-infs-fp-math"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #1 = { nounwind "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "no-infs-fp-math"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #2 = { nofree nounwind }
attributes #3 = { uwtable "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "min-legal-vector-width"="0" "no-infs-fp-math"="false" "no-jump-tables"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #4 = { argmemonly nofree nosync nounwind willreturn }
attributes #5 = { nofree nosync nounwind willreturn }
attributes #6 = { nobuiltin nofree allocsize(0) "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "no-infs-fp-math"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #7 = { inlinehint nounwind uwtable "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "min-legal-vector-width"="0" "no-infs-fp-math"="false" "no-jump-tables"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #8 = { uwtable mustprogress "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "min-legal-vector-width"="0" "no-infs-fp-math"="false" "no-jump-tables"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #9 = { norecurse nounwind readonly uwtable willreturn mustprogress "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "min-legal-vector-width"="0" "no-infs-fp-math"="false" "no-jump-tables"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #10 = { norecurse nounwind readnone uwtable willreturn mustprogress "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "min-legal-vector-width"="0" "no-infs-fp-math"="false" "no-jump-tables"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #11 = { inaccessiblememonly nofree nounwind willreturn "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "no-infs-fp-math"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #12 = { nobuiltin nounwind "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "no-infs-fp-math"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #13 = { noinline noreturn nounwind }
attributes #14 = { noreturn "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "no-infs-fp-math"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #15 = { noreturn nounwind "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "no-infs-fp-math"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #16 = { inaccessiblemem_or_argmemonly nounwind willreturn "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "no-infs-fp-math"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #17 = { nounwind uwtable "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "min-legal-vector-width"="0" "no-infs-fp-math"="false" "no-jump-tables"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #18 = { argmemonly nofree nosync nounwind willreturn writeonly }
attributes #19 = { argmemonly nofree nounwind readonly willreturn "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "no-infs-fp-math"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #20 = { nounwind readonly willreturn "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "no-infs-fp-math"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #21 = { inaccessiblememonly nofree nosync nounwind willreturn }
attributes #22 = { nounwind }
attributes #23 = { noreturn nounwind }
attributes #24 = { allocsize(0) }
attributes #25 = { builtin allocsize(0) }
attributes #26 = { builtin nounwind }
attributes #27 = { noreturn }
attributes #28 = { nounwind readonly willreturn }

!llvm.module.flags = !{!0}
!llvm.ident = !{!1}

!0 = !{i32 1, !"wchar_size", i32 4}
!1 = !{!"Ubuntu clang version 12.0.0-++20210225092633+e0e6b1e39e7e-1~exp1~20210225083352.50"}
!2 = !{!3, !3, i64 0}
!3 = !{!"int", !4, i64 0}
!4 = !{!"omnipotent char", !5, i64 0}
!5 = !{!"Simple C++ TBAA"}
!6 = !{!7, !9, i64 0}
!7 = !{!"_ZTSSt15_Rb_tree_header", !8, i64 0, !11, i64 32}
!8 = !{!"_ZTSSt18_Rb_tree_node_base", !9, i64 0, !10, i64 8, !10, i64 16, !10, i64 24}
!9 = !{!"_ZTSSt14_Rb_tree_color", !4, i64 0}
!10 = !{!"any pointer", !4, i64 0}
!11 = !{!"long", !4, i64 0}
!12 = !{!7, !10, i64 8}
!13 = !{!7, !10, i64 16}
!14 = !{!7, !10, i64 24}
!15 = !{!7, !11, i64 32}
!16 = !{!17, !10, i64 0}
!17 = !{!"_ZTSNSt8__detail15_List_node_baseE", !10, i64 0, !10, i64 8}
!18 = !{!17, !10, i64 8}
!19 = !{!11, !11, i64 0}
!20 = distinct !{!20, !21}
!21 = !{!"llvm.loop.unroll.disable"}
!22 = !{!23, !10, i64 0}
!23 = !{!"_ZTSN7codegen10ParamValueE", !10, i64 0, !3, i64 8, !24, i64 12, !25, i64 16}
!24 = !{!"_ZTSN7codegen9ParamTypeE", !4, i64 0}
!25 = !{!"bool", !4, i64 0}
!26 = !{!23, !3, i64 8}
!27 = !{!23, !24, i64 12}
!28 = !{!23, !25, i64 16}
!29 = !{!30, !10, i64 0}
!30 = !{!"_ZTSNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE12_Alloc_hiderE", !10, i64 0}
!31 = !{!32, !10, i64 0}
!32 = !{!"_ZTSNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEE", !30, i64 0, !11, i64 8, !4, i64 16}
!33 = !{!4, !4, i64 0}
!34 = !{!32, !11, i64 8}
!35 = distinct !{!35, !36}
!36 = !{!"llvm.loop.mustprogress"}
!37 = !{!10, !10, i64 0}
!38 = !{!8, !10, i64 16}
!39 = distinct !{!39, !36}
!40 = !{!8, !10, i64 24}
!41 = distinct !{!41, !36}
!42 = !{!43, !10, i64 0}
!43 = !{!"_ZTSN7codegen12HammerConfigE", !10, i64 0, !10, i64 8, !4, i64 16, !4, i64 816}
!44 = !{!43, !10, i64 8}
!45 = distinct !{!45, !36}
!46 = distinct !{!46, !36}
!47 = !{!48, !10, i64 16}
!48 = !{!"_ZTSN4llvm3orc5LLJITE", !49, i64 0, !52, i64 8, !10, i64 16, !55, i64 24, !63, i64 464, !70, i64 520, !73, i64 528, !76, i64 536, !79, i64 544, !82, i64 552, !82, i64 560}
!49 = !{!"_ZTSSt10unique_ptrIN4llvm3orc16ExecutionSessionESt14default_deleteIS2_EE", !50, i64 0}
!50 = !{!"_ZTSSt15__uniq_ptr_implIN4llvm3orc16ExecutionSessionESt14default_deleteIS2_EE", !51, i64 0}
!51 = !{!"_ZTSSt5tupleIJPN4llvm3orc16ExecutionSessionESt14default_deleteIS2_EEE"}
!52 = !{!"_ZTSSt10unique_ptrIN4llvm3orc5LLJIT15PlatformSupportESt14default_deleteIS3_EE", !53, i64 0}
!53 = !{!"_ZTSSt15__uniq_ptr_implIN4llvm3orc5LLJIT15PlatformSupportESt14default_deleteIS3_EE", !54, i64 0}
!54 = !{!"_ZTSSt5tupleIJPN4llvm3orc5LLJIT15PlatformSupportESt14default_deleteIS3_EEE"}
!55 = !{!"_ZTSN4llvm10DataLayoutE", !25, i64 0, !3, i64 4, !56, i64 8, !3, i64 12, !3, i64 16, !56, i64 20, !57, i64 24, !58, i64 28, !59, i64 32, !60, i64 64, !32, i64 208, !61, i64 240, !10, i64 384, !62, i64 392}
!56 = !{!"_ZTSN4llvm10MaybeAlignE"}
!57 = !{!"_ZTSN4llvm10DataLayout20FunctionPtrAlignTypeE", !4, i64 0}
!58 = !{!"_ZTSN4llvm10DataLayout13ManglingModeTE", !4, i64 0}
!59 = !{!"_ZTSN4llvm11SmallVectorIhLj8EEE"}
!60 = !{!"_ZTSN4llvm11SmallVectorINS_15LayoutAlignElemELj16EEE"}
!61 = !{!"_ZTSN4llvm11SmallVectorINS_16PointerAlignElemELj8EEE"}
!62 = !{!"_ZTSN4llvm11SmallVectorIjLj8EEE"}
!63 = !{!"_ZTSN4llvm6TripleE", !32, i64 0, !64, i64 32, !65, i64 36, !66, i64 40, !67, i64 44, !68, i64 48, !69, i64 52}
!64 = !{!"_ZTSN4llvm6Triple8ArchTypeE", !4, i64 0}
!65 = !{!"_ZTSN4llvm6Triple11SubArchTypeE", !4, i64 0}
!66 = !{!"_ZTSN4llvm6Triple10VendorTypeE", !4, i64 0}
!67 = !{!"_ZTSN4llvm6Triple6OSTypeE", !4, i64 0}
!68 = !{!"_ZTSN4llvm6Triple15EnvironmentTypeE", !4, i64 0}
!69 = !{!"_ZTSN4llvm6Triple16ObjectFormatTypeE", !4, i64 0}
!70 = !{!"_ZTSSt10unique_ptrIN4llvm10ThreadPoolESt14default_deleteIS1_EE", !71, i64 0}
!71 = !{!"_ZTSSt15__uniq_ptr_implIN4llvm10ThreadPoolESt14default_deleteIS1_EE", !72, i64 0}
!72 = !{!"_ZTSSt5tupleIJPN4llvm10ThreadPoolESt14default_deleteIS1_EEE"}
!73 = !{!"_ZTSSt10unique_ptrIN4llvm3orc11ObjectLayerESt14default_deleteIS2_EE", !74, i64 0}
!74 = !{!"_ZTSSt15__uniq_ptr_implIN4llvm3orc11ObjectLayerESt14default_deleteIS2_EE", !75, i64 0}
!75 = !{!"_ZTSSt5tupleIJPN4llvm3orc11ObjectLayerESt14default_deleteIS2_EEE"}
!76 = !{!"_ZTSSt10unique_ptrIN4llvm3orc20ObjectTransformLayerESt14default_deleteIS2_EE", !77, i64 0}
!77 = !{!"_ZTSSt15__uniq_ptr_implIN4llvm3orc20ObjectTransformLayerESt14default_deleteIS2_EE", !78, i64 0}
!78 = !{!"_ZTSSt5tupleIJPN4llvm3orc20ObjectTransformLayerESt14default_deleteIS2_EEE"}
!79 = !{!"_ZTSSt10unique_ptrIN4llvm3orc14IRCompileLayerESt14default_deleteIS2_EE", !80, i64 0}
!80 = !{!"_ZTSSt15__uniq_ptr_implIN4llvm3orc14IRCompileLayerESt14default_deleteIS2_EE", !81, i64 0}
!81 = !{!"_ZTSSt5tupleIJPN4llvm3orc14IRCompileLayerESt14default_deleteIS2_EEE"}
!82 = !{!"_ZTSSt10unique_ptrIN4llvm3orc16IRTransformLayerESt14default_deleteIS2_EE", !83, i64 0}
!83 = !{!"_ZTSSt15__uniq_ptr_implIN4llvm3orc16IRTransformLayerESt14default_deleteIS2_EE", !84, i64 0}
!84 = !{!"_ZTSSt5tupleIJPN4llvm3orc16IRTransformLayerESt14default_deleteIS2_EEE"}
!85 = !{!86}
!86 = distinct !{!86, !87, !"_ZN4llvm3orc5LLJIT6lookupENS_9StringRefE: %agg.result"}
!87 = distinct !{!87, !"_ZN4llvm3orc5LLJIT6lookupENS_9StringRefE"}
!88 = !{!89, !11, i64 0}
!89 = !{!"_ZTSN4llvm18JITEvaluatedSymbolE", !11, i64 0, !90, i64 8}
!90 = !{!"_ZTSN4llvm14JITSymbolFlagsE", !4, i64 0, !91, i64 1}
!91 = !{!"_ZTSN4llvm14JITSymbolFlags9FlagNamesE", !4, i64 0}
!92 = !{!93}
!93 = distinct !{!93, !94, !"_ZN4llvm3orc5LLJIT6lookupENS_9StringRefE: %agg.result"}
!94 = distinct !{!94, !"_ZN4llvm3orc5LLJIT6lookupENS_9StringRefE"}
!95 = !{!96}
!96 = distinct !{!96, !97, !"_ZN4llvm3orc5LLJIT6lookupENS_9StringRefE: %agg.result"}
!97 = distinct !{!97, !"_ZN4llvm3orc5LLJIT6lookupENS_9StringRefE"}
!98 = !{!99}
!99 = distinct !{!99, !100, !"_ZN4llvm3orc5LLJIT6lookupENS_9StringRefE: %agg.result"}
!100 = distinct !{!100, !"_ZN4llvm3orc5LLJIT6lookupENS_9StringRefE"}
!101 = !{!102, !10, i64 8}
!102 = !{!"_ZTS14JitSortContext", !10, i64 0, !10, i64 8, !10, i64 16, !10, i64 24, !10, i64 32}
!103 = !{!102, !10, i64 16}
!104 = !{!102, !10, i64 24}
!105 = !{!102, !10, i64 32}
!106 = !{!102, !10, i64 0}
!107 = distinct !{!107, !36}
!108 = !{!109}
!109 = distinct !{!109, !110, !"_ZSt16forward_as_tupleIJNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEEESt5tupleIJDpOT_EES9_: %agg.result"}
!110 = distinct !{!110, !"_ZSt16forward_as_tupleIJNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEEESt5tupleIJDpOT_EES9_"}
!111 = !{!112, !112, i64 0}
!112 = !{!"vtable pointer", !5, i64 0}
!113 = !{!114, !10, i64 0}
!114 = !{!"_ZTSN4llvm15SmallVectorBaseIjEE", !10, i64 0, !3, i64 8, !3, i64 12}
!115 = !{!116, !10, i64 16}
!116 = !{!"_ZTSSt14_Function_base", !4, i64 0, !10, i64 16}
!117 = !{!118, !10, i64 72}
!118 = !{!"_ZTS4Sort", !10, i64 8, !3, i64 16, !10, i64 24, !3, i64 32, !10, i64 40, !10, i64 48, !10, i64 56, !3, i64 64, !10, i64 72}
!119 = !{!118, !10, i64 8}
!120 = !{!118, !10, i64 40}
!121 = !{!118, !3, i64 64}
!122 = !{!123, !3, i64 40}
!123 = !{!"_ZTS10PagesIndex", !10, i64 0, !3, i64 8, !10, i64 16, !3, i64 24, !10, i64 32, !3, i64 40}
!124 = !{!123, !3, i64 24}
!125 = distinct !{!125, !21}
!126 = distinct !{!126, !36}
!127 = !{!118, !10, i64 48}
!128 = !{!118, !10, i64 56}
!129 = distinct !{!129, !36}
!130 = !{!118, !3, i64 32}
!131 = !{!118, !10, i64 24}
!132 = !{!133, !3, i64 48}
!133 = !{!"_ZTS5Table", !134, i64 8, !135, i64 16, !10, i64 40, !3, i64 48, !3, i64 52, !3, i64 56}
!134 = !{!"_ZTS6Layout"}
!135 = !{!"_ZTSSt6vectorIP6ColumnSaIS1_EE"}
!136 = !{!133, !3, i64 52}
!137 = !{!133, !10, i64 40}
!138 = !{!133, !3, i64 56}
!139 = distinct !{!139, !36}
!140 = !{!141, !10, i64 0}
!141 = !{!"_ZTSNSt8_Rb_treeINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESt4pairIKS5_PN7codegen10ParamValueEESt10_Select1stISB_ESt4lessIS5_ESaISB_EE11_Alloc_nodeE", !10, i64 0}
!142 = !{!8, !9, i64 0}
!143 = !{!8, !10, i64 8}
!144 = distinct !{!144, !36}
!145 = distinct !{!145, !36}
!146 = !{!147, !10, i64 32}
!147 = !{!"_ZTSSt4pairIKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEPN7codegen10ParamValueEE", !32, i64 0, !10, i64 32}
!148 = !{!149}
!149 = distinct !{!149, !150, !"_ZN4llvm3orc5LLJIT19lookupLinkerMangledERNS0_8JITDylibENS_9StringRefE: %agg.result"}
!150 = distinct !{!150, !"_ZN4llvm3orc5LLJIT19lookupLinkerMangledERNS0_8JITDylibENS_9StringRefE"}
!151 = !{!152}
!152 = distinct !{!152, !153, !"_ZN4llvm3orc16ExecutionSession6internENS_9StringRefE: %agg.result"}
!153 = distinct !{!153, !"_ZN4llvm3orc16ExecutionSession6internENS_9StringRefE"}
!154 = !{!155, !10, i64 0}
!155 = !{!"_ZTSSt12__shared_ptrIN4llvm3orc16SymbolStringPoolELN9__gnu_cxx12_Lock_policyE2EE", !10, i64 0, !156, i64 8}
!156 = !{!"_ZTSSt14__shared_countILN9__gnu_cxx12_Lock_policyE2EE", !10, i64 0}
!157 = !{!158}
!158 = distinct !{!158, !159, !"_ZN4llvm3orc16SymbolStringPool6internENS_9StringRefE: %agg.result"}
!159 = distinct !{!159, !"_ZN4llvm3orc16SymbolStringPool6internENS_9StringRefE"}
!160 = !{!158, !152}
!161 = !{!162, !10, i64 0}
!162 = !{!"_ZTSN4llvm3orc15SymbolStringPtrE", !10, i64 0}
!163 = !{!164, !10, i64 0}
!164 = !{!"_ZTSN4llvm13StringMapImplE", !10, i64 0, !3, i64 8, !3, i64 12, !3, i64 16, !3, i64 20}
!165 = distinct !{!165, !36}
!166 = !{!164, !3, i64 16}
!167 = !{!168, !11, i64 0}
!168 = !{!"_ZTSN4llvm18StringMapEntryBaseE", !11, i64 0}
!169 = !{!170, !11, i64 0}
!170 = !{!"_ZTSSt13__atomic_baseImE", !11, i64 0}
!171 = !{!164, !3, i64 12}
!172 = !{!164, !3, i64 8}
!173 = !{!174, !10, i64 0}
!174 = !{!"_ZTSSt12_Vector_baseIP6ColumnSaIS1_EE", !175, i64 0}
!175 = !{!"_ZTSNSt12_Vector_baseIP6ColumnSaIS1_EE12_Vector_implE", !10, i64 0, !10, i64 8, !10, i64 16}
!176 = !{!177, !10, i64 0}
!177 = !{!"_ZTSSt10_Head_baseILm0EONSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEELb0EE", !10, i64 0}
!178 = distinct !{!178, !36}
