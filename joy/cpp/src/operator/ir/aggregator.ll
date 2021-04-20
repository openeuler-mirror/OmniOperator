; ModuleID = '/usr/code/olk_dev/omni_runtime_joy/omni-cache/joy/cpp/src/operator/ir/../aggregator.cpp'
source_filename = "/usr/code/olk_dev/omni_runtime_joy/omni-cache/joy/cpp/src/operator/ir/../aggregator.cpp"
target datalayout = "e-m:e-p270:32:32-p271:32:32-p272:64:64-i64:64-f80:128-n8:16:32:64-S128"
target triple = "x86_64-pc-linux-gnu"

%"class.std::ios_base::Init" = type { i8 }
%class.SumAggregator = type { %class.Aggregator }
%class.Aggregator = type { i32 (...)**, i32, i32, %"class.std::unordered_map" }
%"class.std::unordered_map" = type { %"class.std::_Hashtable" }
%"class.std::_Hashtable" = type { %"struct.std::__detail::_Hash_node_base"**, i64, %"struct.std::__detail::_Hash_node_base", i64, %"struct.std::__detail::_Prime_rehash_policy", %"struct.std::__detail::_Hash_node_base"* }
%"struct.std::__detail::_Hash_node_base" = type { %"struct.std::__detail::_Hash_node_base"* }
%"struct.std::__detail::_Prime_rehash_policy" = type { float, i64 }
%"struct.std::pair" = type { i64, %"class.std::vector" }
%"class.std::vector" = type { %"struct.std::_Vector_base" }
%"struct.std::_Vector_base" = type { %"struct.std::_Vector_base<GroupByColumn, std::allocator<GroupByColumn>>::_Vector_impl" }
%"struct.std::_Vector_base<GroupByColumn, std::allocator<GroupByColumn>>::_Vector_impl" = type { %struct.GroupByColumn*, %struct.GroupByColumn*, %struct.GroupByColumn* }
%struct.GroupByColumn = type { i32, i8* }
%"struct.std::__detail::_Hash_node" = type { %"struct.std::__detail::_Hash_node_value_base" }
%"struct.std::__detail::_Hash_node_value_base" = type { %"struct.std::__detail::_Hash_node_base", %"struct.__gnu_cxx::__aligned_buffer" }
%"struct.__gnu_cxx::__aligned_buffer" = type { %"union.std::aligned_storage<32, 8>::type" }
%"union.std::aligned_storage<32, 8>::type" = type { [32 x i8] }

$_ZN13SumAggregatorD0Ev = comdat any

$_ZN13SumAggregator7processEmPvN3opt10ColumnTypeE = comdat any

$__clang_call_terminate = comdat any

$_ZN10AggregatorD2Ev = comdat any

$_ZN10AggregatorD0Ev = comdat any

$_ZNSt10_HashtableImSt4pairIKmSt6vectorI13GroupByColumnSaIS3_EEESaIS6_ENSt8__detail10_Select1stESt8equal_toImESt4hashImENS8_18_Mod_range_hashingENS8_20_Default_ranged_hashENS8_20_Prime_rehash_policyENS8_17_Hashtable_traitsILb0ELb0ELb1EEEE10_M_emplaceIJS6_EEES0_INS8_14_Node_iteratorIS6_Lb0ELb0EEEbESt17integral_constantIbLb1EEDpOT_ = comdat any

$_ZNSt10_HashtableImSt4pairIKmSt6vectorI13GroupByColumnSaIS3_EEESaIS6_ENSt8__detail10_Select1stESt8equal_toImESt4hashImENS8_18_Mod_range_hashingENS8_20_Default_ranged_hashENS8_20_Prime_rehash_policyENS8_17_Hashtable_traitsILb0ELb0ELb1EEEE21_M_insert_unique_nodeEmmPNS8_10_Hash_nodeIS6_Lb0EEE = comdat any

$_ZTS10Aggregator = comdat any

$_ZTI10Aggregator = comdat any

$_ZTV10Aggregator = comdat any

@_ZStL8__ioinit = internal global %"class.std::ios_base::Init" zeroinitializer, align 1
@__dso_handle = external hidden global i8
@.str = private unnamed_addr constant [29 x i8] c"[%s][%s][%d]:Null Pointer %d\00", align 1
@.str.1 = private unnamed_addr constant [88 x i8] c"/usr/code/olk_dev/omni_runtime_joy/omni-cache/joy/cpp/src/operator/ir/../aggregator.cpp\00", align 1
@__FUNCTION__.sumProcessInt32 = private unnamed_addr constant [16 x i8] c"sumProcessInt32\00", align 1
@__FUNCTION__.sumProcessInt64 = private unnamed_addr constant [16 x i8] c"sumProcessInt64\00", align 1
@__FUNCTION__.sumProcessDouble = private unnamed_addr constant [17 x i8] c"sumProcessDouble\00", align 1
@_ZTV13SumAggregator = dso_local unnamed_addr constant { [6 x i8*] } { [6 x i8*] [i8* null, i8* bitcast ({ i8*, i8*, i8* }* @_ZTI13SumAggregator to i8*), i8* bitcast (void (%class.Aggregator*)* @_ZN10AggregatorD2Ev to i8*), i8* bitcast (void (%class.SumAggregator*)* @_ZN13SumAggregatorD0Ev to i8*), i8* bitcast (void (%class.SumAggregator*, i64, i8*, i32)* @_ZN13SumAggregator7processEmPvN3opt10ColumnTypeE to i8*), i8* bitcast (void (%class.SumAggregator*, i64, i8*, i32, i32)* @_ZN13SumAggregator7processEmPvij to i8*)] }, align 8
@_ZTVN10__cxxabiv120__si_class_type_infoE = external dso_local global i8*
@_ZTS13SumAggregator = dso_local constant [16 x i8] c"13SumAggregator\00", align 1
@_ZTVN10__cxxabiv117__class_type_infoE = external dso_local global i8*
@_ZTS10Aggregator = linkonce_odr dso_local constant [13 x i8] c"10Aggregator\00", comdat, align 1
@_ZTI10Aggregator = linkonce_odr dso_local constant { i8*, i8* } { i8* bitcast (i8** getelementptr inbounds (i8*, i8** @_ZTVN10__cxxabiv117__class_type_infoE, i64 2) to i8*), i8* getelementptr inbounds ([13 x i8], [13 x i8]* @_ZTS10Aggregator, i32 0, i32 0) }, comdat, align 8
@_ZTI13SumAggregator = dso_local constant { i8*, i8*, i8* } { i8* bitcast (i8** getelementptr inbounds (i8*, i8** @_ZTVN10__cxxabiv120__si_class_type_infoE, i64 2) to i8*), i8* getelementptr inbounds ([16 x i8], [16 x i8]* @_ZTS13SumAggregator, i32 0, i32 0), i8* bitcast ({ i8*, i8* }* @_ZTI10Aggregator to i8*) }, align 8
@_ZTV10Aggregator = linkonce_odr dso_local unnamed_addr constant { [6 x i8*] } { [6 x i8*] [i8* null, i8* bitcast ({ i8*, i8* }* @_ZTI10Aggregator to i8*), i8* bitcast (void (%class.Aggregator*)* @_ZN10AggregatorD2Ev to i8*), i8* bitcast (void (%class.Aggregator*)* @_ZN10AggregatorD0Ev to i8*), i8* bitcast (void ()* @__cxa_pure_virtual to i8*), i8* bitcast (void ()* @__cxa_pure_virtual to i8*)] }, comdat, align 8
@llvm.global_ctors = appending global [1 x { i32, void ()*, i8* }] [{ i32, void ()*, i8* } { i32 65535, void ()* @_GLOBAL__sub_I_aggregator.cpp, i8* null }]

declare dso_local void @_ZNSt8ios_base4InitC1Ev(%"class.std::ios_base::Init"* nonnull dereferenceable(1)) unnamed_addr #0

; Function Attrs: nounwind
declare dso_local void @_ZNSt8ios_base4InitD1Ev(%"class.std::ios_base::Init"* nonnull dereferenceable(1)) unnamed_addr #1

; Function Attrs: nofree nounwind
declare dso_local i32 @__cxa_atexit(void (i8*)*, i8*, i8*) local_unnamed_addr #2

; Function Attrs: uwtable
define dso_local void @_ZN13SumAggregator7processEmPvij(%class.SumAggregator* nonnull dereferenceable(72) %this, i64 %key, i8* nocapture readonly %colPtr, i32 %type, i32 %offset) unnamed_addr #3 align 2 personality i8* bitcast (i32 (...)* @__gxx_personality_v0 to i8*) {
entry:
  %ref.tmp17 = alloca %"struct.std::pair", align 8
  %ref.tmp62 = alloca %"struct.std::pair", align 8
  %ref.tmp111 = alloca %"struct.std::pair", align 8
  switch i32 %type, label %sw.epilog [
    i32 1, label %sw.bb
    i32 2, label %sw.bb29
    i32 3, label %sw.bb78
  ]

sw.bb:                                            ; preds = %entry
  %0 = bitcast i8* %colPtr to i32*
  %idx.ext = zext i32 %offset to i64
  %add.ptr = getelementptr inbounds i32, i32* %0, i64 %idx.ext
  %state = getelementptr inbounds %class.SumAggregator, %class.SumAggregator* %this, i64 0, i32 0, i32 3
  %_M_bucket_count.i.i.i = getelementptr inbounds %class.SumAggregator, %class.SumAggregator* %this, i64 0, i32 0, i32 3, i32 0, i32 1
  %1 = load i64, i64* %_M_bucket_count.i.i.i, align 8, !tbaa !2
  %rem.i.i.i.i.i = urem i64 %key, %1
  %_M_buckets.i.i.i.i = getelementptr inbounds %"class.std::unordered_map", %"class.std::unordered_map"* %state, i64 0, i32 0, i32 0
  %2 = load %"struct.std::__detail::_Hash_node_base"**, %"struct.std::__detail::_Hash_node_base"*** %_M_buckets.i.i.i.i, align 8, !tbaa !11
  %arrayidx.i.i.i.i = getelementptr inbounds %"struct.std::__detail::_Hash_node_base"*, %"struct.std::__detail::_Hash_node_base"** %2, i64 %rem.i.i.i.i.i
  %3 = load %"struct.std::__detail::_Hash_node_base"*, %"struct.std::__detail::_Hash_node_base"** %arrayidx.i.i.i.i, align 8, !tbaa !12
  %tobool.not.i.i.i.i = icmp eq %"struct.std::__detail::_Hash_node_base"* %3, null
  br i1 %tobool.not.i.i.i.i, label %if.else, label %if.end.i.i.i.i

if.end.i.i.i.i:                                   ; preds = %sw.bb
  %4 = bitcast %"struct.std::__detail::_Hash_node_base"* %3 to %"struct.std::__detail::_Hash_node"**
  %5 = load %"struct.std::__detail::_Hash_node"*, %"struct.std::__detail::_Hash_node"** %4, align 8, !tbaa !13
  %_M_storage.i.i.i.i23.i.i.i.i = getelementptr inbounds %"struct.std::__detail::_Hash_node", %"struct.std::__detail::_Hash_node"* %5, i64 0, i32 0, i32 1
  %first.i.i.i.i.i24.i.i.i.i = bitcast %"struct.__gnu_cxx::__aligned_buffer"* %_M_storage.i.i.i.i23.i.i.i.i to i64*
  %6 = load i64, i64* %first.i.i.i.i.i24.i.i.i.i, align 8, !tbaa !14
  %cmp.i.i.i25.i.i.i.i = icmp eq i64 %6, %key
  br i1 %cmp.i.i.i25.i.i.i.i, label %if.then, label %if.end3.i.i.i.i

for.cond.i.i.i.i:                                 ; preds = %lor.lhs.false.i.i.i.i
  %cmp.i.i.i.i.i.i.i = icmp eq i64 %9, %key
  br i1 %cmp.i.i.i.i.i.i.i, label %if.then.loopexit, label %if.end3.i.i.i.i

if.end3.i.i.i.i:                                  ; preds = %if.end.i.i.i.i, %for.cond.i.i.i.i
  %__p.026.i.i.i.i = phi %"struct.std::__detail::_Hash_node"* [ %8, %for.cond.i.i.i.i ], [ %5, %if.end.i.i.i.i ]
  %_M_nxt4.i.i.i.i = getelementptr inbounds %"struct.std::__detail::_Hash_node", %"struct.std::__detail::_Hash_node"* %__p.026.i.i.i.i, i64 0, i32 0, i32 0, i32 0
  %7 = load %"struct.std::__detail::_Hash_node_base"*, %"struct.std::__detail::_Hash_node_base"** %_M_nxt4.i.i.i.i, align 8, !tbaa !13
  %tobool5.not.i.i.i.i = icmp eq %"struct.std::__detail::_Hash_node_base"* %7, null
  %8 = bitcast %"struct.std::__detail::_Hash_node_base"* %7 to %"struct.std::__detail::_Hash_node"*
  br i1 %tobool5.not.i.i.i.i, label %if.else, label %lor.lhs.false.i.i.i.i

lor.lhs.false.i.i.i.i:                            ; preds = %if.end3.i.i.i.i
  %_M_storage.i.i.i.i21.i.i.i.i = getelementptr inbounds %"struct.std::__detail::_Hash_node_base", %"struct.std::__detail::_Hash_node_base"* %7, i64 1
  %first.i.i.i.i.i22.i.i.i.i = bitcast %"struct.std::__detail::_Hash_node_base"* %_M_storage.i.i.i.i21.i.i.i.i to i64*
  %9 = load i64, i64* %first.i.i.i.i.i22.i.i.i.i, align 8, !tbaa !14
  %rem.i.i.i.i.i.i.i = urem i64 %9, %1
  %cmp.not.i.i.i.i = icmp eq i64 %rem.i.i.i.i.i.i.i, %rem.i.i.i.i.i
  br i1 %cmp.not.i.i.i.i, label %for.cond.i.i.i.i, label %if.else

if.then.loopexit:                                 ; preds = %for.cond.i.i.i.i
  %10 = bitcast %"struct.std::__detail::_Hash_node_base"* %7 to %"struct.std::__detail::_Hash_node"*
  br label %if.then

if.then:                                          ; preds = %if.then.loopexit, %if.end.i.i.i.i
  %retval.sroa.0.0.i.i = phi %"struct.std::__detail::_Hash_node"* [ %5, %if.end.i.i.i.i ], [ %10, %if.then.loopexit ]
  %11 = load i32, i32* %add.ptr, align 4, !tbaa !15
  %second = getelementptr inbounds %"struct.std::__detail::_Hash_node", %"struct.std::__detail::_Hash_node"* %retval.sroa.0.0.i.i, i64 0, i32 0, i32 1, i32 0, i32 0, i64 8
  %_M_start.i = bitcast i8* %second to %struct.GroupByColumn**
  %12 = load %struct.GroupByColumn*, %struct.GroupByColumn** %_M_start.i, align 8, !tbaa !17
  %val = getelementptr inbounds %struct.GroupByColumn, %struct.GroupByColumn* %12, i64 0, i32 1
  %13 = bitcast i8** %val to i32**
  %14 = load i32*, i32** %13, align 8, !tbaa !20
  %15 = load i32, i32* %14, align 4, !tbaa !15
  %add = add nsw i32 %15, %11
  store i32 %add, i32* %14, align 4, !tbaa !15
  br label %sw.epilog

if.else:                                          ; preds = %if.end3.i.i.i.i, %lor.lhs.false.i.i.i.i, %sw.bb
  %call11 = tail call noalias nonnull dereferenceable(4) i8* @_Znwm(i64 4) #14
  %16 = bitcast i8* %call11 to i32*
  %17 = load i32, i32* %add.ptr, align 4, !tbaa !15
  store i32 %17, i32* %16, align 4, !tbaa !15
  %call2.i.i.i.i.i179 = tail call noalias nonnull i8* @_Znwm(i64 16) #15
  %c.sroa.0.0..sroa_idx512 = bitcast i8* %call2.i.i.i.i.i179 to i32*
  store i32 1, i32* %c.sroa.0.0..sroa_idx512, align 8, !tbaa.struct !23
  %c.sroa.6519.0..sroa_idx523 = getelementptr inbounds i8, i8* %call2.i.i.i.i.i179, i64 8
  %18 = bitcast i8* %c.sroa.6519.0..sroa_idx523 to i8**
  store i8* %call11, i8** %18, align 8, !tbaa.struct !25
  %19 = bitcast %"struct.std::pair"* %ref.tmp17 to i8*
  call void @llvm.lifetime.start.p0i8(i64 32, i8* nonnull %19) #16
  %first.i180 = getelementptr inbounds %"struct.std::pair", %"struct.std::pair"* %ref.tmp17, i64 0, i32 0
  store i64 %key, i64* %first.i180, align 8, !tbaa !26
  %second.i181 = getelementptr inbounds %"struct.std::pair", %"struct.std::pair"* %ref.tmp17, i64 0, i32 1
  %20 = bitcast %"class.std::vector"* %second.i181 to i8*
  call void @llvm.memset.p0i8.i64(i8* nonnull align 8 dereferenceable(24) %20, i8 0, i64 24, i1 false) #16
  %call2.i.i.i.i3.i22.i.i208 = invoke noalias nonnull i8* @_Znwm(i64 16) #15
          to label %call2.i.i.i.i3.i22.i.i.noexc207 unwind label %lpad18

call2.i.i.i.i3.i22.i.i.noexc207:                  ; preds = %if.else
  %21 = bitcast %"class.std::vector"* %second.i181 to i8**
  store i8* %call2.i.i.i.i3.i22.i.i208, i8** %21, align 8, !tbaa !17
  %_M_finish.i.i.i.i195 = getelementptr inbounds %"struct.std::pair", %"struct.std::pair"* %ref.tmp17, i64 0, i32 1, i32 0, i32 0, i32 1
  %22 = bitcast %struct.GroupByColumn** %_M_finish.i.i.i.i195 to i8**
  %add.ptr.i.i.i.i196 = getelementptr inbounds i8, i8* %call2.i.i.i.i3.i22.i.i208, i64 16
  %_M_end_of_storage.i.i.i.i197 = getelementptr inbounds %"struct.std::pair", %"struct.std::pair"* %ref.tmp17, i64 0, i32 1, i32 0, i32 0, i32 2
  %23 = bitcast %struct.GroupByColumn** %_M_end_of_storage.i.i.i.i197 to i8**
  store i8* %add.ptr.i.i.i.i196, i8** %23, align 8, !tbaa !29
  tail call void @llvm.memcpy.p0i8.p0i8.i64(i8* nonnull align 8 dereferenceable(16) %call2.i.i.i.i3.i22.i.i208, i8* nonnull align 8 dereferenceable(16) %call2.i.i.i.i.i179, i64 16, i1 false) #16
  store i8* %add.ptr.i.i.i.i196, i8** %22, align 8, !tbaa !30
  %24 = getelementptr inbounds %"class.std::unordered_map", %"class.std::unordered_map"* %state, i64 0, i32 0
  %call3.i.i210 = invoke { %"struct.std::__detail::_Hash_node"*, i8 } @_ZNSt10_HashtableImSt4pairIKmSt6vectorI13GroupByColumnSaIS3_EEESaIS6_ENSt8__detail10_Select1stESt8equal_toImESt4hashImENS8_18_Mod_range_hashingENS8_20_Default_ranged_hashENS8_20_Prime_rehash_policyENS8_17_Hashtable_traitsILb0ELb0ELb1EEEE10_M_emplaceIJS6_EEES0_INS8_14_Node_iteratorIS6_Lb0ELb0EEEbESt17integral_constantIbLb1EEDpOT_(%"class.std::_Hashtable"* nonnull dereferenceable(56) %24, %"struct.std::pair"* nonnull align 8 dereferenceable(32) %ref.tmp17)
          to label %invoke.cont21 unwind label %lpad20

invoke.cont21:                                    ; preds = %call2.i.i.i.i3.i22.i.i.noexc207
  %_M_start.i.i.i212 = getelementptr inbounds %"struct.std::pair", %"struct.std::pair"* %ref.tmp17, i64 0, i32 1, i32 0, i32 0, i32 0
  %25 = load %struct.GroupByColumn*, %struct.GroupByColumn** %_M_start.i.i.i212, align 8, !tbaa !17
  %tobool.not.i.i.i.i213 = icmp eq %struct.GroupByColumn* %25, null
  br i1 %tobool.not.i.i.i.i213, label %_ZNSt6vectorI13GroupByColumnSaIS0_EED2Ev.exit219, label %if.then.i.i.i.i214

if.then.i.i.i.i214:                               ; preds = %invoke.cont21
  %26 = bitcast %struct.GroupByColumn* %25 to i8*
  call void @_ZdlPv(i8* nonnull %26) #16
  br label %_ZNSt6vectorI13GroupByColumnSaIS0_EED2Ev.exit219

_ZNSt6vectorI13GroupByColumnSaIS0_EED2Ev.exit219: ; preds = %invoke.cont21, %if.then.i.i.i.i214
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %19) #16
  call void @_ZdlPv(i8* nonnull %call2.i.i.i.i.i179) #16
  br label %sw.epilog

lpad18:                                           ; preds = %if.else
  %27 = landingpad { i8*, i32 }
          cleanup
  br label %if.then.i.i.i226

lpad20:                                           ; preds = %call2.i.i.i.i3.i22.i.i.noexc207
  %28 = landingpad { i8*, i32 }
          cleanup
  %_M_start.i.i.i220 = getelementptr inbounds %"struct.std::pair", %"struct.std::pair"* %ref.tmp17, i64 0, i32 1, i32 0, i32 0, i32 0
  %29 = load %struct.GroupByColumn*, %struct.GroupByColumn** %_M_start.i.i.i220, align 8, !tbaa !17
  %tobool.not.i.i.i.i221 = icmp eq %struct.GroupByColumn* %29, null
  br i1 %tobool.not.i.i.i.i221, label %if.then.i.i.i226, label %if.then.i.i.i.i222

if.then.i.i.i.i222:                               ; preds = %lpad20
  %30 = bitcast %struct.GroupByColumn* %29 to i8*
  call void @_ZdlPv(i8* nonnull %30) #16
  br label %if.then.i.i.i226

if.then.i.i.i226:                                 ; preds = %if.then.i.i.i.i222, %lpad20, %lpad18
  %.pn160 = phi { i8*, i32 } [ %27, %lpad18 ], [ %28, %lpad20 ], [ %28, %if.then.i.i.i.i222 ]
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %19) #16
  br label %eh.resume

sw.bb29:                                          ; preds = %entry
  %31 = bitcast i8* %colPtr to i64*
  %idx.ext31 = zext i32 %offset to i64
  %add.ptr32 = getelementptr inbounds i64, i64* %31, i64 %idx.ext31
  %state34 = getelementptr inbounds %class.SumAggregator, %class.SumAggregator* %this, i64 0, i32 0, i32 3
  %_M_bucket_count.i.i.i228 = getelementptr inbounds %class.SumAggregator, %class.SumAggregator* %this, i64 0, i32 0, i32 3, i32 0, i32 1
  %32 = load i64, i64* %_M_bucket_count.i.i.i228, align 8, !tbaa !2
  %rem.i.i.i.i.i229 = urem i64 %key, %32
  %_M_buckets.i.i.i.i230 = getelementptr inbounds %"class.std::unordered_map", %"class.std::unordered_map"* %state34, i64 0, i32 0, i32 0
  %33 = load %"struct.std::__detail::_Hash_node_base"**, %"struct.std::__detail::_Hash_node_base"*** %_M_buckets.i.i.i.i230, align 8, !tbaa !11
  %arrayidx.i.i.i.i231 = getelementptr inbounds %"struct.std::__detail::_Hash_node_base"*, %"struct.std::__detail::_Hash_node_base"** %33, i64 %rem.i.i.i.i.i229
  %34 = load %"struct.std::__detail::_Hash_node_base"*, %"struct.std::__detail::_Hash_node_base"** %arrayidx.i.i.i.i231, align 8, !tbaa !12
  %tobool.not.i.i.i.i232 = icmp eq %"struct.std::__detail::_Hash_node_base"* %34, null
  br i1 %tobool.not.i.i.i.i232, label %if.else50, label %if.end.i.i.i.i236

if.end.i.i.i.i236:                                ; preds = %sw.bb29
  %35 = bitcast %"struct.std::__detail::_Hash_node_base"* %34 to %"struct.std::__detail::_Hash_node"**
  %36 = load %"struct.std::__detail::_Hash_node"*, %"struct.std::__detail::_Hash_node"** %35, align 8, !tbaa !13
  %_M_storage.i.i.i.i23.i.i.i.i233 = getelementptr inbounds %"struct.std::__detail::_Hash_node", %"struct.std::__detail::_Hash_node"* %36, i64 0, i32 0, i32 1
  %first.i.i.i.i.i24.i.i.i.i234 = bitcast %"struct.__gnu_cxx::__aligned_buffer"* %_M_storage.i.i.i.i23.i.i.i.i233 to i64*
  %37 = load i64, i64* %first.i.i.i.i.i24.i.i.i.i234, align 8, !tbaa !14
  %cmp.i.i.i25.i.i.i.i235 = icmp eq i64 %37, %key
  br i1 %cmp.i.i.i25.i.i.i.i235, label %if.then44, label %if.end3.i.i.i.i242

for.cond.i.i.i.i238:                              ; preds = %lor.lhs.false.i.i.i.i247
  %cmp.i.i.i.i.i.i.i237 = icmp eq i64 %40, %key
  br i1 %cmp.i.i.i.i.i.i.i237, label %if.then44.loopexit, label %if.end3.i.i.i.i242

if.end3.i.i.i.i242:                               ; preds = %if.end.i.i.i.i236, %for.cond.i.i.i.i238
  %__p.026.i.i.i.i239 = phi %"struct.std::__detail::_Hash_node"* [ %39, %for.cond.i.i.i.i238 ], [ %36, %if.end.i.i.i.i236 ]
  %_M_nxt4.i.i.i.i240 = getelementptr inbounds %"struct.std::__detail::_Hash_node", %"struct.std::__detail::_Hash_node"* %__p.026.i.i.i.i239, i64 0, i32 0, i32 0, i32 0
  %38 = load %"struct.std::__detail::_Hash_node_base"*, %"struct.std::__detail::_Hash_node_base"** %_M_nxt4.i.i.i.i240, align 8, !tbaa !13
  %tobool5.not.i.i.i.i241 = icmp eq %"struct.std::__detail::_Hash_node_base"* %38, null
  %39 = bitcast %"struct.std::__detail::_Hash_node_base"* %38 to %"struct.std::__detail::_Hash_node"*
  br i1 %tobool5.not.i.i.i.i241, label %if.else50, label %lor.lhs.false.i.i.i.i247

lor.lhs.false.i.i.i.i247:                         ; preds = %if.end3.i.i.i.i242
  %_M_storage.i.i.i.i21.i.i.i.i243 = getelementptr inbounds %"struct.std::__detail::_Hash_node_base", %"struct.std::__detail::_Hash_node_base"* %38, i64 1
  %first.i.i.i.i.i22.i.i.i.i244 = bitcast %"struct.std::__detail::_Hash_node_base"* %_M_storage.i.i.i.i21.i.i.i.i243 to i64*
  %40 = load i64, i64* %first.i.i.i.i.i22.i.i.i.i244, align 8, !tbaa !14
  %rem.i.i.i.i.i.i.i245 = urem i64 %40, %32
  %cmp.not.i.i.i.i246 = icmp eq i64 %rem.i.i.i.i.i.i.i245, %rem.i.i.i.i.i229
  br i1 %cmp.not.i.i.i.i246, label %for.cond.i.i.i.i238, label %if.else50

if.then44.loopexit:                               ; preds = %for.cond.i.i.i.i238
  %41 = bitcast %"struct.std::__detail::_Hash_node_base"* %38 to %"struct.std::__detail::_Hash_node"*
  br label %if.then44

if.then44:                                        ; preds = %if.then44.loopexit, %if.end.i.i.i.i236
  %retval.sroa.0.0.i.i249 = phi %"struct.std::__detail::_Hash_node"* [ %36, %if.end.i.i.i.i236 ], [ %41, %if.then44.loopexit ]
  %42 = load i64, i64* %add.ptr32, align 8, !tbaa !14
  %second46 = getelementptr inbounds %"struct.std::__detail::_Hash_node", %"struct.std::__detail::_Hash_node"* %retval.sroa.0.0.i.i249, i64 0, i32 0, i32 1, i32 0, i32 0, i64 8
  %_M_start.i303 = bitcast i8* %second46 to %struct.GroupByColumn**
  %43 = load %struct.GroupByColumn*, %struct.GroupByColumn** %_M_start.i303, align 8, !tbaa !17
  %val48 = getelementptr inbounds %struct.GroupByColumn, %struct.GroupByColumn* %43, i64 0, i32 1
  %44 = bitcast i8** %val48 to i64**
  %45 = load i64*, i64** %44, align 8, !tbaa !20
  %46 = load i64, i64* %45, align 8, !tbaa !14
  %add49 = add nsw i64 %46, %42
  store i64 %add49, i64* %45, align 8, !tbaa !14
  br label %sw.epilog

if.else50:                                        ; preds = %if.end3.i.i.i.i242, %lor.lhs.false.i.i.i.i247, %sw.bb29
  %call55 = tail call noalias nonnull dereferenceable(8) i8* @_Znwm(i64 8) #14
  %47 = bitcast i8* %call55 to i64*
  %48 = load i64, i64* %add.ptr32, align 8, !tbaa !14
  store i64 %48, i64* %47, align 8, !tbaa !14
  %call2.i.i.i.i.i382 = tail call noalias nonnull i8* @_Znwm(i64 16) #15
  %c56.sroa.0.0..sroa_idx471 = bitcast i8* %call2.i.i.i.i.i382 to i32*
  store i32 2, i32* %c56.sroa.0.0..sroa_idx471, align 8, !tbaa.struct !23
  %c56.sroa.6478.0..sroa_idx482 = getelementptr inbounds i8, i8* %call2.i.i.i.i.i382, i64 8
  %49 = bitcast i8* %c56.sroa.6478.0..sroa_idx482 to i8**
  store i8* %call55, i8** %49, align 8, !tbaa.struct !25
  %50 = bitcast %"struct.std::pair"* %ref.tmp62 to i8*
  call void @llvm.lifetime.start.p0i8(i64 32, i8* nonnull %50) #16
  %first.i384 = getelementptr inbounds %"struct.std::pair", %"struct.std::pair"* %ref.tmp62, i64 0, i32 0
  store i64 %key, i64* %first.i384, align 8, !tbaa !26
  %second.i385 = getelementptr inbounds %"struct.std::pair", %"struct.std::pair"* %ref.tmp62, i64 0, i32 1
  %51 = bitcast %"class.std::vector"* %second.i385 to i8*
  call void @llvm.memset.p0i8.i64(i8* nonnull align 8 dereferenceable(24) %51, i8 0, i64 24, i1 false) #16
  %call2.i.i.i.i3.i22.i.i412 = invoke noalias nonnull i8* @_Znwm(i64 16) #15
          to label %call2.i.i.i.i3.i22.i.i.noexc411 unwind label %lpad63

call2.i.i.i.i3.i22.i.i.noexc411:                  ; preds = %if.else50
  %52 = bitcast %"class.std::vector"* %second.i385 to i8**
  store i8* %call2.i.i.i.i3.i22.i.i412, i8** %52, align 8, !tbaa !17
  %_M_finish.i.i.i.i399 = getelementptr inbounds %"struct.std::pair", %"struct.std::pair"* %ref.tmp62, i64 0, i32 1, i32 0, i32 0, i32 1
  %53 = bitcast %struct.GroupByColumn** %_M_finish.i.i.i.i399 to i8**
  %add.ptr.i.i.i.i400 = getelementptr inbounds i8, i8* %call2.i.i.i.i3.i22.i.i412, i64 16
  %_M_end_of_storage.i.i.i.i401 = getelementptr inbounds %"struct.std::pair", %"struct.std::pair"* %ref.tmp62, i64 0, i32 1, i32 0, i32 0, i32 2
  %54 = bitcast %struct.GroupByColumn** %_M_end_of_storage.i.i.i.i401 to i8**
  store i8* %add.ptr.i.i.i.i400, i8** %54, align 8, !tbaa !29
  tail call void @llvm.memcpy.p0i8.p0i8.i64(i8* nonnull align 8 dereferenceable(16) %call2.i.i.i.i3.i22.i.i412, i8* nonnull align 8 dereferenceable(16) %call2.i.i.i.i.i382, i64 16, i1 false) #16
  store i8* %add.ptr.i.i.i.i400, i8** %53, align 8, !tbaa !30
  %55 = getelementptr inbounds %"class.std::unordered_map", %"class.std::unordered_map"* %state34, i64 0, i32 0
  %call3.i.i414 = invoke { %"struct.std::__detail::_Hash_node"*, i8 } @_ZNSt10_HashtableImSt4pairIKmSt6vectorI13GroupByColumnSaIS3_EEESaIS6_ENSt8__detail10_Select1stESt8equal_toImESt4hashImENS8_18_Mod_range_hashingENS8_20_Default_ranged_hashENS8_20_Prime_rehash_policyENS8_17_Hashtable_traitsILb0ELb0ELb1EEEE10_M_emplaceIJS6_EEES0_INS8_14_Node_iteratorIS6_Lb0ELb0EEEbESt17integral_constantIbLb1EEDpOT_(%"class.std::_Hashtable"* nonnull dereferenceable(56) %55, %"struct.std::pair"* nonnull align 8 dereferenceable(32) %ref.tmp62)
          to label %invoke.cont66 unwind label %lpad65

invoke.cont66:                                    ; preds = %call2.i.i.i.i3.i22.i.i.noexc411
  %_M_start.i.i.i416 = getelementptr inbounds %"struct.std::pair", %"struct.std::pair"* %ref.tmp62, i64 0, i32 1, i32 0, i32 0, i32 0
  %56 = load %struct.GroupByColumn*, %struct.GroupByColumn** %_M_start.i.i.i416, align 8, !tbaa !17
  %tobool.not.i.i.i.i417 = icmp eq %struct.GroupByColumn* %56, null
  br i1 %tobool.not.i.i.i.i417, label %_ZNSt6vectorI13GroupByColumnSaIS0_EED2Ev.exit423, label %if.then.i.i.i.i418

if.then.i.i.i.i418:                               ; preds = %invoke.cont66
  %57 = bitcast %struct.GroupByColumn* %56 to i8*
  call void @_ZdlPv(i8* nonnull %57) #16
  br label %_ZNSt6vectorI13GroupByColumnSaIS0_EED2Ev.exit423

_ZNSt6vectorI13GroupByColumnSaIS0_EED2Ev.exit423: ; preds = %invoke.cont66, %if.then.i.i.i.i418
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %50) #16
  call void @_ZdlPv(i8* nonnull %call2.i.i.i.i.i382) #16
  br label %sw.epilog

lpad63:                                           ; preds = %if.else50
  %58 = landingpad { i8*, i32 }
          cleanup
  br label %if.then.i.i.i334

lpad65:                                           ; preds = %call2.i.i.i.i3.i22.i.i.noexc411
  %59 = landingpad { i8*, i32 }
          cleanup
  %_M_start.i.i.i424 = getelementptr inbounds %"struct.std::pair", %"struct.std::pair"* %ref.tmp62, i64 0, i32 1, i32 0, i32 0, i32 0
  %60 = load %struct.GroupByColumn*, %struct.GroupByColumn** %_M_start.i.i.i424, align 8, !tbaa !17
  %tobool.not.i.i.i.i425 = icmp eq %struct.GroupByColumn* %60, null
  br i1 %tobool.not.i.i.i.i425, label %if.then.i.i.i334, label %if.then.i.i.i.i426

if.then.i.i.i.i426:                               ; preds = %lpad65
  %61 = bitcast %struct.GroupByColumn* %60 to i8*
  call void @_ZdlPv(i8* nonnull %61) #16
  br label %if.then.i.i.i334

if.then.i.i.i334:                                 ; preds = %if.then.i.i.i.i426, %lpad65, %lpad63
  %.pn156 = phi { i8*, i32 } [ %58, %lpad63 ], [ %59, %lpad65 ], [ %59, %if.then.i.i.i.i426 ]
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %50) #16
  br label %eh.resume

sw.bb78:                                          ; preds = %entry
  %62 = bitcast i8* %colPtr to double*
  %idx.ext80 = zext i32 %offset to i64
  %add.ptr81 = getelementptr inbounds double, double* %62, i64 %idx.ext80
  %state83 = getelementptr inbounds %class.SumAggregator, %class.SumAggregator* %this, i64 0, i32 0, i32 3
  %_M_bucket_count.i.i.i309 = getelementptr inbounds %class.SumAggregator, %class.SumAggregator* %this, i64 0, i32 0, i32 3, i32 0, i32 1
  %63 = load i64, i64* %_M_bucket_count.i.i.i309, align 8, !tbaa !2
  %rem.i.i.i.i.i310 = urem i64 %key, %63
  %_M_buckets.i.i.i.i311 = getelementptr inbounds %"class.std::unordered_map", %"class.std::unordered_map"* %state83, i64 0, i32 0, i32 0
  %64 = load %"struct.std::__detail::_Hash_node_base"**, %"struct.std::__detail::_Hash_node_base"*** %_M_buckets.i.i.i.i311, align 8, !tbaa !11
  %arrayidx.i.i.i.i312 = getelementptr inbounds %"struct.std::__detail::_Hash_node_base"*, %"struct.std::__detail::_Hash_node_base"** %64, i64 %rem.i.i.i.i.i310
  %65 = load %"struct.std::__detail::_Hash_node_base"*, %"struct.std::__detail::_Hash_node_base"** %arrayidx.i.i.i.i312, align 8, !tbaa !12
  %tobool.not.i.i.i.i313 = icmp eq %"struct.std::__detail::_Hash_node_base"* %65, null
  br i1 %tobool.not.i.i.i.i313, label %if.else99, label %if.end.i.i.i.i317

if.end.i.i.i.i317:                                ; preds = %sw.bb78
  %66 = bitcast %"struct.std::__detail::_Hash_node_base"* %65 to %"struct.std::__detail::_Hash_node"**
  %67 = load %"struct.std::__detail::_Hash_node"*, %"struct.std::__detail::_Hash_node"** %66, align 8, !tbaa !13
  %_M_storage.i.i.i.i23.i.i.i.i314 = getelementptr inbounds %"struct.std::__detail::_Hash_node", %"struct.std::__detail::_Hash_node"* %67, i64 0, i32 0, i32 1
  %first.i.i.i.i.i24.i.i.i.i315 = bitcast %"struct.__gnu_cxx::__aligned_buffer"* %_M_storage.i.i.i.i23.i.i.i.i314 to i64*
  %68 = load i64, i64* %first.i.i.i.i.i24.i.i.i.i315, align 8, !tbaa !14
  %cmp.i.i.i25.i.i.i.i316 = icmp eq i64 %68, %key
  br i1 %cmp.i.i.i25.i.i.i.i316, label %if.then93, label %if.end3.i.i.i.i323

for.cond.i.i.i.i319:                              ; preds = %lor.lhs.false.i.i.i.i328
  %cmp.i.i.i.i.i.i.i318 = icmp eq i64 %71, %key
  br i1 %cmp.i.i.i.i.i.i.i318, label %if.then93.loopexit, label %if.end3.i.i.i.i323

if.end3.i.i.i.i323:                               ; preds = %if.end.i.i.i.i317, %for.cond.i.i.i.i319
  %__p.026.i.i.i.i320 = phi %"struct.std::__detail::_Hash_node"* [ %70, %for.cond.i.i.i.i319 ], [ %67, %if.end.i.i.i.i317 ]
  %_M_nxt4.i.i.i.i321 = getelementptr inbounds %"struct.std::__detail::_Hash_node", %"struct.std::__detail::_Hash_node"* %__p.026.i.i.i.i320, i64 0, i32 0, i32 0, i32 0
  %69 = load %"struct.std::__detail::_Hash_node_base"*, %"struct.std::__detail::_Hash_node_base"** %_M_nxt4.i.i.i.i321, align 8, !tbaa !13
  %tobool5.not.i.i.i.i322 = icmp eq %"struct.std::__detail::_Hash_node_base"* %69, null
  %70 = bitcast %"struct.std::__detail::_Hash_node_base"* %69 to %"struct.std::__detail::_Hash_node"*
  br i1 %tobool5.not.i.i.i.i322, label %if.else99, label %lor.lhs.false.i.i.i.i328

lor.lhs.false.i.i.i.i328:                         ; preds = %if.end3.i.i.i.i323
  %_M_storage.i.i.i.i21.i.i.i.i324 = getelementptr inbounds %"struct.std::__detail::_Hash_node_base", %"struct.std::__detail::_Hash_node_base"* %69, i64 1
  %first.i.i.i.i.i22.i.i.i.i325 = bitcast %"struct.std::__detail::_Hash_node_base"* %_M_storage.i.i.i.i21.i.i.i.i324 to i64*
  %71 = load i64, i64* %first.i.i.i.i.i22.i.i.i.i325, align 8, !tbaa !14
  %rem.i.i.i.i.i.i.i326 = urem i64 %71, %63
  %cmp.not.i.i.i.i327 = icmp eq i64 %rem.i.i.i.i.i.i.i326, %rem.i.i.i.i.i310
  br i1 %cmp.not.i.i.i.i327, label %for.cond.i.i.i.i319, label %if.else99

if.then93.loopexit:                               ; preds = %for.cond.i.i.i.i319
  %72 = bitcast %"struct.std::__detail::_Hash_node_base"* %69 to %"struct.std::__detail::_Hash_node"*
  br label %if.then93

if.then93:                                        ; preds = %if.then93.loopexit, %if.end.i.i.i.i317
  %retval.sroa.0.0.i.i330 = phi %"struct.std::__detail::_Hash_node"* [ %67, %if.end.i.i.i.i317 ], [ %72, %if.then93.loopexit ]
  %73 = load double, double* %add.ptr81, align 8, !tbaa !31
  %second95 = getelementptr inbounds %"struct.std::__detail::_Hash_node", %"struct.std::__detail::_Hash_node"* %retval.sroa.0.0.i.i330, i64 0, i32 0, i32 1, i32 0, i32 0, i64 8
  %_M_start.i304 = bitcast i8* %second95 to %struct.GroupByColumn**
  %74 = load %struct.GroupByColumn*, %struct.GroupByColumn** %_M_start.i304, align 8, !tbaa !17
  %val97 = getelementptr inbounds %struct.GroupByColumn, %struct.GroupByColumn* %74, i64 0, i32 1
  %75 = bitcast i8** %val97 to double**
  %76 = load double*, double** %75, align 8, !tbaa !20
  %77 = load double, double* %76, align 8, !tbaa !31
  %add98 = fadd double %73, %77
  store double %add98, double* %76, align 8, !tbaa !31
  br label %sw.epilog

if.else99:                                        ; preds = %if.end3.i.i.i.i323, %lor.lhs.false.i.i.i.i328, %sw.bb78
  %call104 = tail call noalias nonnull dereferenceable(8) i8* @_Znwm(i64 8) #14
  %78 = bitcast i8* %call104 to double*
  %79 = load double, double* %add.ptr81, align 8, !tbaa !31
  store double %79, double* %78, align 8, !tbaa !31
  %call2.i.i.i.i.i301 = tail call noalias nonnull i8* @_Znwm(i64 16) #15
  %c105.sroa.0.0..sroa_idx430 = bitcast i8* %call2.i.i.i.i.i301 to i32*
  store i32 3, i32* %c105.sroa.0.0..sroa_idx430, align 8, !tbaa.struct !23
  %c105.sroa.6437.0..sroa_idx441 = getelementptr inbounds i8, i8* %call2.i.i.i.i.i301, i64 8
  %80 = bitcast i8* %c105.sroa.6437.0..sroa_idx441 to i8**
  store i8* %call104, i8** %80, align 8, !tbaa.struct !25
  %81 = bitcast %"struct.std::pair"* %ref.tmp111 to i8*
  call void @llvm.lifetime.start.p0i8(i64 32, i8* nonnull %81) #16
  %first.i = getelementptr inbounds %"struct.std::pair", %"struct.std::pair"* %ref.tmp111, i64 0, i32 0
  store i64 %key, i64* %first.i, align 8, !tbaa !26
  %second.i = getelementptr inbounds %"struct.std::pair", %"struct.std::pair"* %ref.tmp111, i64 0, i32 1
  %82 = bitcast %"class.std::vector"* %second.i to i8*
  call void @llvm.memset.p0i8.i64(i8* nonnull align 8 dereferenceable(24) %82, i8 0, i64 24, i1 false) #16
  %call2.i.i.i.i3.i22.i.i177 = invoke noalias nonnull i8* @_Znwm(i64 16) #15
          to label %call2.i.i.i.i3.i22.i.i.noexc unwind label %lpad112

call2.i.i.i.i3.i22.i.i.noexc:                     ; preds = %if.else99
  %83 = bitcast %"class.std::vector"* %second.i to i8**
  store i8* %call2.i.i.i.i3.i22.i.i177, i8** %83, align 8, !tbaa !17
  %_M_finish.i.i.i.i = getelementptr inbounds %"struct.std::pair", %"struct.std::pair"* %ref.tmp111, i64 0, i32 1, i32 0, i32 0, i32 1
  %84 = bitcast %struct.GroupByColumn** %_M_finish.i.i.i.i to i8**
  %add.ptr.i.i.i.i = getelementptr inbounds i8, i8* %call2.i.i.i.i3.i22.i.i177, i64 16
  %_M_end_of_storage.i.i.i.i = getelementptr inbounds %"struct.std::pair", %"struct.std::pair"* %ref.tmp111, i64 0, i32 1, i32 0, i32 0, i32 2
  %85 = bitcast %struct.GroupByColumn** %_M_end_of_storage.i.i.i.i to i8**
  store i8* %add.ptr.i.i.i.i, i8** %85, align 8, !tbaa !29
  tail call void @llvm.memcpy.p0i8.p0i8.i64(i8* nonnull align 8 dereferenceable(16) %call2.i.i.i.i3.i22.i.i177, i8* nonnull align 8 dereferenceable(16) %call2.i.i.i.i.i301, i64 16, i1 false) #16
  store i8* %add.ptr.i.i.i.i, i8** %84, align 8, !tbaa !30
  %86 = getelementptr inbounds %"class.std::unordered_map", %"class.std::unordered_map"* %state83, i64 0, i32 0
  %call3.i.i174 = invoke { %"struct.std::__detail::_Hash_node"*, i8 } @_ZNSt10_HashtableImSt4pairIKmSt6vectorI13GroupByColumnSaIS3_EEESaIS6_ENSt8__detail10_Select1stESt8equal_toImESt4hashImENS8_18_Mod_range_hashingENS8_20_Default_ranged_hashENS8_20_Prime_rehash_policyENS8_17_Hashtable_traitsILb0ELb0ELb1EEEE10_M_emplaceIJS6_EEES0_INS8_14_Node_iteratorIS6_Lb0ELb0EEEbESt17integral_constantIbLb1EEDpOT_(%"class.std::_Hashtable"* nonnull dereferenceable(56) %86, %"struct.std::pair"* nonnull align 8 dereferenceable(32) %ref.tmp111)
          to label %invoke.cont115 unwind label %lpad114

invoke.cont115:                                   ; preds = %call2.i.i.i.i3.i22.i.i.noexc
  %_M_start.i.i.i170 = getelementptr inbounds %"struct.std::pair", %"struct.std::pair"* %ref.tmp111, i64 0, i32 1, i32 0, i32 0, i32 0
  %87 = load %struct.GroupByColumn*, %struct.GroupByColumn** %_M_start.i.i.i170, align 8, !tbaa !17
  %tobool.not.i.i.i.i171 = icmp eq %struct.GroupByColumn* %87, null
  br i1 %tobool.not.i.i.i.i171, label %_ZNSt6vectorI13GroupByColumnSaIS0_EED2Ev.exit169, label %if.then.i.i.i.i172

if.then.i.i.i.i172:                               ; preds = %invoke.cont115
  %88 = bitcast %struct.GroupByColumn* %87 to i8*
  call void @_ZdlPv(i8* nonnull %88) #16
  br label %_ZNSt6vectorI13GroupByColumnSaIS0_EED2Ev.exit169

_ZNSt6vectorI13GroupByColumnSaIS0_EED2Ev.exit169: ; preds = %invoke.cont115, %if.then.i.i.i.i172
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %81) #16
  call void @_ZdlPv(i8* nonnull %call2.i.i.i.i.i301) #16
  br label %sw.epilog

lpad112:                                          ; preds = %if.else99
  %89 = landingpad { i8*, i32 }
          cleanup
  br label %if.then.i.i.i

lpad114:                                          ; preds = %call2.i.i.i.i3.i22.i.i.noexc
  %90 = landingpad { i8*, i32 }
          cleanup
  %_M_start.i.i.i = getelementptr inbounds %"struct.std::pair", %"struct.std::pair"* %ref.tmp111, i64 0, i32 1, i32 0, i32 0, i32 0
  %91 = load %struct.GroupByColumn*, %struct.GroupByColumn** %_M_start.i.i.i, align 8, !tbaa !17
  %tobool.not.i.i.i.i165 = icmp eq %struct.GroupByColumn* %91, null
  br i1 %tobool.not.i.i.i.i165, label %if.then.i.i.i, label %if.then.i.i.i.i

if.then.i.i.i.i:                                  ; preds = %lpad114
  %92 = bitcast %struct.GroupByColumn* %91 to i8*
  call void @_ZdlPv(i8* nonnull %92) #16
  br label %if.then.i.i.i

if.then.i.i.i:                                    ; preds = %if.then.i.i.i.i, %lpad114, %lpad112
  %.pn = phi { i8*, i32 } [ %89, %lpad112 ], [ %90, %lpad114 ], [ %90, %if.then.i.i.i.i ]
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %81) #16
  br label %eh.resume

sw.epilog:                                        ; preds = %if.then93, %_ZNSt6vectorI13GroupByColumnSaIS0_EED2Ev.exit169, %if.then44, %_ZNSt6vectorI13GroupByColumnSaIS0_EED2Ev.exit423, %if.then, %_ZNSt6vectorI13GroupByColumnSaIS0_EED2Ev.exit219, %entry
  ret void

eh.resume:                                        ; preds = %if.then.i.i.i, %if.then.i.i.i334, %if.then.i.i.i226
  %call2.i.i.i.i.i301.sink = phi i8* [ %call2.i.i.i.i.i301, %if.then.i.i.i ], [ %call2.i.i.i.i.i382, %if.then.i.i.i334 ], [ %call2.i.i.i.i.i179, %if.then.i.i.i226 ]
  %.pn.pn.pn.pn = phi { i8*, i32 } [ %.pn, %if.then.i.i.i ], [ %.pn156, %if.then.i.i.i334 ], [ %.pn160, %if.then.i.i.i226 ]
  call void @_ZdlPv(i8* nonnull %call2.i.i.i.i.i301.sink) #16
  resume { i8*, i32 } %.pn.pn.pn.pn
}

; Function Attrs: argmemonly nofree nosync nounwind willreturn
declare void @llvm.lifetime.start.p0i8(i64 immarg, i8* nocapture) #4

; Function Attrs: argmemonly nofree nosync nounwind willreturn
declare void @llvm.lifetime.end.p0i8(i64 immarg, i8* nocapture) #4

; Function Attrs: nobuiltin nofree allocsize(0)
declare dso_local nonnull i8* @_Znwm(i64) local_unnamed_addr #5

declare dso_local i32 @__gxx_personality_v0(...)

; Function Attrs: uwtable
define dso_local void @sumProcessInt32(%class.SumAggregator* %aggregator, i64 %key, i8* nocapture readonly %columnPtr, i32 %offset) local_unnamed_addr #3 personality i8* bitcast (i32 (...)* @__gxx_personality_v0 to i8*) {
entry:
  %ref.tmp18 = alloca %"struct.std::pair", align 8
  %cmp = icmp eq %class.SumAggregator* %aggregator, null
  br i1 %cmp, label %do.body, label %if.end

do.body:                                          ; preds = %entry
  %call = tail call i32 (i8*, ...) @printf(i8* nonnull dereferenceable(1) getelementptr inbounds ([29 x i8], [29 x i8]* @.str, i64 0, i64 0), i8* getelementptr inbounds ([88 x i8], [88 x i8]* @.str.1, i64 0, i64 0), i8* getelementptr inbounds ([16 x i8], [16 x i8]* @__FUNCTION__.sumProcessInt32, i64 0, i64 0), i32 62, i32 %offset)
  %putchar = tail call i32 @putchar(i32 10)
  br label %if.end

if.end:                                           ; preds = %do.body, %entry
  %state.i = getelementptr inbounds %class.SumAggregator, %class.SumAggregator* %aggregator, i64 0, i32 0, i32 3
  %_M_bucket_count.i.i.i = getelementptr inbounds %class.SumAggregator, %class.SumAggregator* %aggregator, i64 0, i32 0, i32 3, i32 0, i32 1
  %0 = load i64, i64* %_M_bucket_count.i.i.i, align 8, !tbaa !2
  %rem.i.i.i.i.i = urem i64 %key, %0
  %_M_buckets.i.i.i.i = getelementptr inbounds %"class.std::unordered_map", %"class.std::unordered_map"* %state.i, i64 0, i32 0, i32 0
  %1 = load %"struct.std::__detail::_Hash_node_base"**, %"struct.std::__detail::_Hash_node_base"*** %_M_buckets.i.i.i.i, align 8, !tbaa !11
  %arrayidx.i.i.i.i = getelementptr inbounds %"struct.std::__detail::_Hash_node_base"*, %"struct.std::__detail::_Hash_node_base"** %1, i64 %rem.i.i.i.i.i
  %2 = load %"struct.std::__detail::_Hash_node_base"*, %"struct.std::__detail::_Hash_node_base"** %arrayidx.i.i.i.i, align 8, !tbaa !12
  %tobool.not.i.i.i.i = icmp eq %"struct.std::__detail::_Hash_node_base"* %2, null
  br i1 %tobool.not.i.i.i.i, label %if.else, label %if.end.i.i.i.i

if.end.i.i.i.i:                                   ; preds = %if.end
  %3 = bitcast %"struct.std::__detail::_Hash_node_base"* %2 to %"struct.std::__detail::_Hash_node"**
  %4 = load %"struct.std::__detail::_Hash_node"*, %"struct.std::__detail::_Hash_node"** %3, align 8, !tbaa !13
  %_M_storage.i.i.i.i23.i.i.i.i = getelementptr inbounds %"struct.std::__detail::_Hash_node", %"struct.std::__detail::_Hash_node"* %4, i64 0, i32 0, i32 1
  %first.i.i.i.i.i24.i.i.i.i = bitcast %"struct.__gnu_cxx::__aligned_buffer"* %_M_storage.i.i.i.i23.i.i.i.i to i64*
  %5 = load i64, i64* %first.i.i.i.i.i24.i.i.i.i, align 8, !tbaa !14
  %cmp.i.i.i25.i.i.i.i = icmp eq i64 %5, %key
  br i1 %cmp.i.i.i25.i.i.i.i, label %if.then10, label %if.end3.i.i.i.i

for.cond.i.i.i.i:                                 ; preds = %lor.lhs.false.i.i.i.i
  %cmp.i.i.i.i.i.i.i = icmp eq i64 %8, %key
  br i1 %cmp.i.i.i.i.i.i.i, label %if.then10.loopexit, label %if.end3.i.i.i.i

if.end3.i.i.i.i:                                  ; preds = %if.end.i.i.i.i, %for.cond.i.i.i.i
  %__p.026.i.i.i.i = phi %"struct.std::__detail::_Hash_node"* [ %7, %for.cond.i.i.i.i ], [ %4, %if.end.i.i.i.i ]
  %_M_nxt4.i.i.i.i = getelementptr inbounds %"struct.std::__detail::_Hash_node", %"struct.std::__detail::_Hash_node"* %__p.026.i.i.i.i, i64 0, i32 0, i32 0, i32 0
  %6 = load %"struct.std::__detail::_Hash_node_base"*, %"struct.std::__detail::_Hash_node_base"** %_M_nxt4.i.i.i.i, align 8, !tbaa !13
  %tobool5.not.i.i.i.i = icmp eq %"struct.std::__detail::_Hash_node_base"* %6, null
  %7 = bitcast %"struct.std::__detail::_Hash_node_base"* %6 to %"struct.std::__detail::_Hash_node"*
  br i1 %tobool5.not.i.i.i.i, label %if.else, label %lor.lhs.false.i.i.i.i

lor.lhs.false.i.i.i.i:                            ; preds = %if.end3.i.i.i.i
  %_M_storage.i.i.i.i21.i.i.i.i = getelementptr inbounds %"struct.std::__detail::_Hash_node_base", %"struct.std::__detail::_Hash_node_base"* %6, i64 1
  %first.i.i.i.i.i22.i.i.i.i = bitcast %"struct.std::__detail::_Hash_node_base"* %_M_storage.i.i.i.i21.i.i.i.i to i64*
  %8 = load i64, i64* %first.i.i.i.i.i22.i.i.i.i, align 8, !tbaa !14
  %rem.i.i.i.i.i.i.i = urem i64 %8, %0
  %cmp.not.i.i.i.i = icmp eq i64 %rem.i.i.i.i.i.i.i, %rem.i.i.i.i.i
  br i1 %cmp.not.i.i.i.i, label %for.cond.i.i.i.i, label %if.else

if.then10.loopexit:                               ; preds = %for.cond.i.i.i.i
  %9 = bitcast %"struct.std::__detail::_Hash_node_base"* %6 to %"struct.std::__detail::_Hash_node"*
  br label %if.then10

if.then10:                                        ; preds = %if.then10.loopexit, %if.end.i.i.i.i
  %retval.sroa.0.0.i.i = phi %"struct.std::__detail::_Hash_node"* [ %4, %if.end.i.i.i.i ], [ %9, %if.then10.loopexit ]
  %10 = bitcast i8* %columnPtr to i32*
  %idx.ext = sext i32 %offset to i64
  %add.ptr = getelementptr inbounds i32, i32* %10, i64 %idx.ext
  %11 = load i32, i32* %add.ptr, align 4, !tbaa !15
  %second = getelementptr inbounds %"struct.std::__detail::_Hash_node", %"struct.std::__detail::_Hash_node"* %retval.sroa.0.0.i.i, i64 0, i32 0, i32 1, i32 0, i32 0, i64 8
  %_M_start.i = bitcast i8* %second to %struct.GroupByColumn**
  %12 = load %struct.GroupByColumn*, %struct.GroupByColumn** %_M_start.i, align 8, !tbaa !17
  %val = getelementptr inbounds %struct.GroupByColumn, %struct.GroupByColumn* %12, i64 0, i32 1
  %13 = bitcast i8** %val to i32**
  %14 = load i32*, i32** %13, align 8, !tbaa !20
  %15 = load i32, i32* %14, align 4, !tbaa !15
  %add = add nsw i32 %15, %11
  store i32 %add, i32* %14, align 4, !tbaa !15
  br label %if.end28

if.else:                                          ; preds = %if.end3.i.i.i.i, %lor.lhs.false.i.i.i.i, %if.end
  %16 = bitcast i8* %columnPtr to i32*
  %idx.ext107 = sext i32 %offset to i64
  %add.ptr108 = getelementptr inbounds i32, i32* %16, i64 %idx.ext107
  %call14 = tail call noalias nonnull dereferenceable(4) i8* @_Znwm(i64 4) #14
  %17 = bitcast i8* %call14 to i32*
  %18 = load i32, i32* %add.ptr108, align 4, !tbaa !15
  store i32 %18, i32* %17, align 4, !tbaa !15
  %call2.i.i.i.i.i62 = tail call noalias nonnull i8* @_Znwm(i64 16) #15
  %c.sroa.0.0..sroa_idx65 = bitcast i8* %call2.i.i.i.i.i62 to i32*
  store i32 1, i32* %c.sroa.0.0..sroa_idx65, align 8, !tbaa.struct !23
  %c.sroa.672.0..sroa_idx76 = getelementptr inbounds i8, i8* %call2.i.i.i.i.i62, i64 8
  %19 = bitcast i8* %c.sroa.672.0..sroa_idx76 to i8**
  store i8* %call14, i8** %19, align 8, !tbaa.struct !25
  %20 = bitcast %"struct.std::pair"* %ref.tmp18 to i8*
  call void @llvm.lifetime.start.p0i8(i64 32, i8* nonnull %20) #16
  %first.i = getelementptr inbounds %"struct.std::pair", %"struct.std::pair"* %ref.tmp18, i64 0, i32 0
  store i64 %key, i64* %first.i, align 8, !tbaa !26
  %second.i = getelementptr inbounds %"struct.std::pair", %"struct.std::pair"* %ref.tmp18, i64 0, i32 1
  %21 = bitcast %"class.std::vector"* %second.i to i8*
  call void @llvm.memset.p0i8.i64(i8* nonnull align 8 dereferenceable(24) %21, i8 0, i64 24, i1 false) #16
  %call2.i.i.i.i3.i22.i.i60 = invoke noalias nonnull i8* @_Znwm(i64 16) #15
          to label %call2.i.i.i.i3.i22.i.i.noexc unwind label %lpad19

call2.i.i.i.i3.i22.i.i.noexc:                     ; preds = %if.else
  %22 = bitcast %"class.std::vector"* %second.i to i8**
  store i8* %call2.i.i.i.i3.i22.i.i60, i8** %22, align 8, !tbaa !17
  %_M_finish.i.i.i.i = getelementptr inbounds %"struct.std::pair", %"struct.std::pair"* %ref.tmp18, i64 0, i32 1, i32 0, i32 0, i32 1
  %23 = bitcast %struct.GroupByColumn** %_M_finish.i.i.i.i to i8**
  %add.ptr.i.i.i.i = getelementptr inbounds i8, i8* %call2.i.i.i.i3.i22.i.i60, i64 16
  %_M_end_of_storage.i.i.i.i = getelementptr inbounds %"struct.std::pair", %"struct.std::pair"* %ref.tmp18, i64 0, i32 1, i32 0, i32 0, i32 2
  %24 = bitcast %struct.GroupByColumn** %_M_end_of_storage.i.i.i.i to i8**
  store i8* %add.ptr.i.i.i.i, i8** %24, align 8, !tbaa !29
  tail call void @llvm.memcpy.p0i8.p0i8.i64(i8* nonnull align 8 dereferenceable(16) %call2.i.i.i.i3.i22.i.i60, i8* nonnull align 8 dereferenceable(16) %call2.i.i.i.i.i62, i64 16, i1 false) #16
  store i8* %add.ptr.i.i.i.i, i8** %23, align 8, !tbaa !30
  %25 = getelementptr inbounds %"class.std::unordered_map", %"class.std::unordered_map"* %state.i, i64 0, i32 0
  %call3.i.i57 = invoke { %"struct.std::__detail::_Hash_node"*, i8 } @_ZNSt10_HashtableImSt4pairIKmSt6vectorI13GroupByColumnSaIS3_EEESaIS6_ENSt8__detail10_Select1stESt8equal_toImESt4hashImENS8_18_Mod_range_hashingENS8_20_Default_ranged_hashENS8_20_Prime_rehash_policyENS8_17_Hashtable_traitsILb0ELb0ELb1EEEE10_M_emplaceIJS6_EEES0_INS8_14_Node_iteratorIS6_Lb0ELb0EEEbESt17integral_constantIbLb1EEDpOT_(%"class.std::_Hashtable"* nonnull dereferenceable(56) %25, %"struct.std::pair"* nonnull align 8 dereferenceable(32) %ref.tmp18)
          to label %invoke.cont22 unwind label %lpad21

invoke.cont22:                                    ; preds = %call2.i.i.i.i3.i22.i.i.noexc
  %_M_start.i.i.i53 = getelementptr inbounds %"struct.std::pair", %"struct.std::pair"* %ref.tmp18, i64 0, i32 1, i32 0, i32 0, i32 0
  %26 = load %struct.GroupByColumn*, %struct.GroupByColumn** %_M_start.i.i.i53, align 8, !tbaa !17
  %tobool.not.i.i.i.i54 = icmp eq %struct.GroupByColumn* %26, null
  br i1 %tobool.not.i.i.i.i54, label %_ZNSt6vectorI13GroupByColumnSaIS0_EED2Ev.exit52, label %if.then.i.i.i.i55

if.then.i.i.i.i55:                                ; preds = %invoke.cont22
  %27 = bitcast %struct.GroupByColumn* %26 to i8*
  call void @_ZdlPv(i8* nonnull %27) #16
  br label %_ZNSt6vectorI13GroupByColumnSaIS0_EED2Ev.exit52

_ZNSt6vectorI13GroupByColumnSaIS0_EED2Ev.exit52:  ; preds = %invoke.cont22, %if.then.i.i.i.i55
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %20) #16
  call void @_ZdlPv(i8* nonnull %call2.i.i.i.i.i62) #16
  br label %if.end28

lpad19:                                           ; preds = %if.else
  %28 = landingpad { i8*, i32 }
          cleanup
  br label %if.then.i.i.i

lpad21:                                           ; preds = %call2.i.i.i.i3.i22.i.i.noexc
  %29 = landingpad { i8*, i32 }
          cleanup
  %_M_start.i.i.i = getelementptr inbounds %"struct.std::pair", %"struct.std::pair"* %ref.tmp18, i64 0, i32 1, i32 0, i32 0, i32 0
  %30 = load %struct.GroupByColumn*, %struct.GroupByColumn** %_M_start.i.i.i, align 8, !tbaa !17
  %tobool.not.i.i.i.i48 = icmp eq %struct.GroupByColumn* %30, null
  br i1 %tobool.not.i.i.i.i48, label %if.then.i.i.i, label %if.then.i.i.i.i

if.then.i.i.i.i:                                  ; preds = %lpad21
  %31 = bitcast %struct.GroupByColumn* %30 to i8*
  call void @_ZdlPv(i8* nonnull %31) #16
  br label %if.then.i.i.i

if.then.i.i.i:                                    ; preds = %if.then.i.i.i.i, %lpad21, %lpad19
  %.pn = phi { i8*, i32 } [ %28, %lpad19 ], [ %29, %lpad21 ], [ %29, %if.then.i.i.i.i ]
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %20) #16
  call void @_ZdlPv(i8* nonnull %call2.i.i.i.i.i62) #16
  resume { i8*, i32 } %.pn

if.end28:                                         ; preds = %_ZNSt6vectorI13GroupByColumnSaIS0_EED2Ev.exit52, %if.then10
  ret void
}

; Function Attrs: nofree nounwind
declare dso_local noundef i32 @printf(i8* nocapture noundef readonly, ...) local_unnamed_addr #6

; Function Attrs: uwtable
define dso_local void @sumProcessInt64(%class.SumAggregator* %aggregator, i64 %key, i8* nocapture readonly %columnPtr, i32 %offset) local_unnamed_addr #3 personality i8* bitcast (i32 (...)* @__gxx_personality_v0 to i8*) {
entry:
  %ref.tmp18 = alloca %"struct.std::pair", align 8
  %cmp = icmp eq %class.SumAggregator* %aggregator, null
  br i1 %cmp, label %do.body, label %if.end

do.body:                                          ; preds = %entry
  %call = tail call i32 (i8*, ...) @printf(i8* nonnull dereferenceable(1) getelementptr inbounds ([29 x i8], [29 x i8]* @.str, i64 0, i64 0), i8* getelementptr inbounds ([88 x i8], [88 x i8]* @.str.1, i64 0, i64 0), i8* getelementptr inbounds ([16 x i8], [16 x i8]* @__FUNCTION__.sumProcessInt64, i64 0, i64 0), i32 84, i32 %offset)
  %putchar = tail call i32 @putchar(i32 10)
  br label %if.end

if.end:                                           ; preds = %do.body, %entry
  %state.i = getelementptr inbounds %class.SumAggregator, %class.SumAggregator* %aggregator, i64 0, i32 0, i32 3
  %_M_bucket_count.i.i.i = getelementptr inbounds %class.SumAggregator, %class.SumAggregator* %aggregator, i64 0, i32 0, i32 3, i32 0, i32 1
  %0 = load i64, i64* %_M_bucket_count.i.i.i, align 8, !tbaa !2
  %rem.i.i.i.i.i = urem i64 %key, %0
  %_M_buckets.i.i.i.i = getelementptr inbounds %"class.std::unordered_map", %"class.std::unordered_map"* %state.i, i64 0, i32 0, i32 0
  %1 = load %"struct.std::__detail::_Hash_node_base"**, %"struct.std::__detail::_Hash_node_base"*** %_M_buckets.i.i.i.i, align 8, !tbaa !11
  %arrayidx.i.i.i.i = getelementptr inbounds %"struct.std::__detail::_Hash_node_base"*, %"struct.std::__detail::_Hash_node_base"** %1, i64 %rem.i.i.i.i.i
  %2 = load %"struct.std::__detail::_Hash_node_base"*, %"struct.std::__detail::_Hash_node_base"** %arrayidx.i.i.i.i, align 8, !tbaa !12
  %tobool.not.i.i.i.i = icmp eq %"struct.std::__detail::_Hash_node_base"* %2, null
  br i1 %tobool.not.i.i.i.i, label %if.else, label %if.end.i.i.i.i

if.end.i.i.i.i:                                   ; preds = %if.end
  %3 = bitcast %"struct.std::__detail::_Hash_node_base"* %2 to %"struct.std::__detail::_Hash_node"**
  %4 = load %"struct.std::__detail::_Hash_node"*, %"struct.std::__detail::_Hash_node"** %3, align 8, !tbaa !13
  %_M_storage.i.i.i.i23.i.i.i.i = getelementptr inbounds %"struct.std::__detail::_Hash_node", %"struct.std::__detail::_Hash_node"* %4, i64 0, i32 0, i32 1
  %first.i.i.i.i.i24.i.i.i.i = bitcast %"struct.__gnu_cxx::__aligned_buffer"* %_M_storage.i.i.i.i23.i.i.i.i to i64*
  %5 = load i64, i64* %first.i.i.i.i.i24.i.i.i.i, align 8, !tbaa !14
  %cmp.i.i.i25.i.i.i.i = icmp eq i64 %5, %key
  br i1 %cmp.i.i.i25.i.i.i.i, label %if.then10, label %if.end3.i.i.i.i

for.cond.i.i.i.i:                                 ; preds = %lor.lhs.false.i.i.i.i
  %cmp.i.i.i.i.i.i.i = icmp eq i64 %8, %key
  br i1 %cmp.i.i.i.i.i.i.i, label %if.then10.loopexit, label %if.end3.i.i.i.i

if.end3.i.i.i.i:                                  ; preds = %if.end.i.i.i.i, %for.cond.i.i.i.i
  %__p.026.i.i.i.i = phi %"struct.std::__detail::_Hash_node"* [ %7, %for.cond.i.i.i.i ], [ %4, %if.end.i.i.i.i ]
  %_M_nxt4.i.i.i.i = getelementptr inbounds %"struct.std::__detail::_Hash_node", %"struct.std::__detail::_Hash_node"* %__p.026.i.i.i.i, i64 0, i32 0, i32 0, i32 0
  %6 = load %"struct.std::__detail::_Hash_node_base"*, %"struct.std::__detail::_Hash_node_base"** %_M_nxt4.i.i.i.i, align 8, !tbaa !13
  %tobool5.not.i.i.i.i = icmp eq %"struct.std::__detail::_Hash_node_base"* %6, null
  %7 = bitcast %"struct.std::__detail::_Hash_node_base"* %6 to %"struct.std::__detail::_Hash_node"*
  br i1 %tobool5.not.i.i.i.i, label %if.else, label %lor.lhs.false.i.i.i.i

lor.lhs.false.i.i.i.i:                            ; preds = %if.end3.i.i.i.i
  %_M_storage.i.i.i.i21.i.i.i.i = getelementptr inbounds %"struct.std::__detail::_Hash_node_base", %"struct.std::__detail::_Hash_node_base"* %6, i64 1
  %first.i.i.i.i.i22.i.i.i.i = bitcast %"struct.std::__detail::_Hash_node_base"* %_M_storage.i.i.i.i21.i.i.i.i to i64*
  %8 = load i64, i64* %first.i.i.i.i.i22.i.i.i.i, align 8, !tbaa !14
  %rem.i.i.i.i.i.i.i = urem i64 %8, %0
  %cmp.not.i.i.i.i = icmp eq i64 %rem.i.i.i.i.i.i.i, %rem.i.i.i.i.i
  br i1 %cmp.not.i.i.i.i, label %for.cond.i.i.i.i, label %if.else

if.then10.loopexit:                               ; preds = %for.cond.i.i.i.i
  %9 = bitcast %"struct.std::__detail::_Hash_node_base"* %6 to %"struct.std::__detail::_Hash_node"*
  br label %if.then10

if.then10:                                        ; preds = %if.then10.loopexit, %if.end.i.i.i.i
  %retval.sroa.0.0.i.i = phi %"struct.std::__detail::_Hash_node"* [ %4, %if.end.i.i.i.i ], [ %9, %if.then10.loopexit ]
  %10 = bitcast i8* %columnPtr to i64*
  %idx.ext = sext i32 %offset to i64
  %add.ptr = getelementptr inbounds i64, i64* %10, i64 %idx.ext
  %11 = load i64, i64* %add.ptr, align 8, !tbaa !14
  %second = getelementptr inbounds %"struct.std::__detail::_Hash_node", %"struct.std::__detail::_Hash_node"* %retval.sroa.0.0.i.i, i64 0, i32 0, i32 1, i32 0, i32 0, i64 8
  %_M_start.i = bitcast i8* %second to %struct.GroupByColumn**
  %12 = load %struct.GroupByColumn*, %struct.GroupByColumn** %_M_start.i, align 8, !tbaa !17
  %val = getelementptr inbounds %struct.GroupByColumn, %struct.GroupByColumn* %12, i64 0, i32 1
  %13 = bitcast i8** %val to i64**
  %14 = load i64*, i64** %13, align 8, !tbaa !20
  %15 = load i64, i64* %14, align 8, !tbaa !14
  %add = add nsw i64 %15, %11
  store i64 %add, i64* %14, align 8, !tbaa !14
  br label %if.end28

if.else:                                          ; preds = %if.end3.i.i.i.i, %lor.lhs.false.i.i.i.i, %if.end
  %16 = bitcast i8* %columnPtr to i64*
  %idx.ext107 = sext i32 %offset to i64
  %add.ptr108 = getelementptr inbounds i64, i64* %16, i64 %idx.ext107
  %call14 = tail call noalias nonnull dereferenceable(8) i8* @_Znwm(i64 8) #14
  %17 = bitcast i8* %call14 to i64*
  %18 = load i64, i64* %add.ptr108, align 8, !tbaa !14
  store i64 %18, i64* %17, align 8, !tbaa !14
  %call2.i.i.i.i.i62 = tail call noalias nonnull i8* @_Znwm(i64 16) #15
  %c.sroa.0.0..sroa_idx65 = bitcast i8* %call2.i.i.i.i.i62 to i32*
  store i32 2, i32* %c.sroa.0.0..sroa_idx65, align 8, !tbaa.struct !23
  %c.sroa.672.0..sroa_idx76 = getelementptr inbounds i8, i8* %call2.i.i.i.i.i62, i64 8
  %19 = bitcast i8* %c.sroa.672.0..sroa_idx76 to i8**
  store i8* %call14, i8** %19, align 8, !tbaa.struct !25
  %20 = bitcast %"struct.std::pair"* %ref.tmp18 to i8*
  call void @llvm.lifetime.start.p0i8(i64 32, i8* nonnull %20) #16
  %first.i = getelementptr inbounds %"struct.std::pair", %"struct.std::pair"* %ref.tmp18, i64 0, i32 0
  store i64 %key, i64* %first.i, align 8, !tbaa !26
  %second.i = getelementptr inbounds %"struct.std::pair", %"struct.std::pair"* %ref.tmp18, i64 0, i32 1
  %21 = bitcast %"class.std::vector"* %second.i to i8*
  call void @llvm.memset.p0i8.i64(i8* nonnull align 8 dereferenceable(24) %21, i8 0, i64 24, i1 false) #16
  %call2.i.i.i.i3.i22.i.i60 = invoke noalias nonnull i8* @_Znwm(i64 16) #15
          to label %call2.i.i.i.i3.i22.i.i.noexc unwind label %lpad19

call2.i.i.i.i3.i22.i.i.noexc:                     ; preds = %if.else
  %22 = bitcast %"class.std::vector"* %second.i to i8**
  store i8* %call2.i.i.i.i3.i22.i.i60, i8** %22, align 8, !tbaa !17
  %_M_finish.i.i.i.i = getelementptr inbounds %"struct.std::pair", %"struct.std::pair"* %ref.tmp18, i64 0, i32 1, i32 0, i32 0, i32 1
  %23 = bitcast %struct.GroupByColumn** %_M_finish.i.i.i.i to i8**
  %add.ptr.i.i.i.i = getelementptr inbounds i8, i8* %call2.i.i.i.i3.i22.i.i60, i64 16
  %_M_end_of_storage.i.i.i.i = getelementptr inbounds %"struct.std::pair", %"struct.std::pair"* %ref.tmp18, i64 0, i32 1, i32 0, i32 0, i32 2
  %24 = bitcast %struct.GroupByColumn** %_M_end_of_storage.i.i.i.i to i8**
  store i8* %add.ptr.i.i.i.i, i8** %24, align 8, !tbaa !29
  tail call void @llvm.memcpy.p0i8.p0i8.i64(i8* nonnull align 8 dereferenceable(16) %call2.i.i.i.i3.i22.i.i60, i8* nonnull align 8 dereferenceable(16) %call2.i.i.i.i.i62, i64 16, i1 false) #16
  store i8* %add.ptr.i.i.i.i, i8** %23, align 8, !tbaa !30
  %25 = getelementptr inbounds %"class.std::unordered_map", %"class.std::unordered_map"* %state.i, i64 0, i32 0
  %call3.i.i57 = invoke { %"struct.std::__detail::_Hash_node"*, i8 } @_ZNSt10_HashtableImSt4pairIKmSt6vectorI13GroupByColumnSaIS3_EEESaIS6_ENSt8__detail10_Select1stESt8equal_toImESt4hashImENS8_18_Mod_range_hashingENS8_20_Default_ranged_hashENS8_20_Prime_rehash_policyENS8_17_Hashtable_traitsILb0ELb0ELb1EEEE10_M_emplaceIJS6_EEES0_INS8_14_Node_iteratorIS6_Lb0ELb0EEEbESt17integral_constantIbLb1EEDpOT_(%"class.std::_Hashtable"* nonnull dereferenceable(56) %25, %"struct.std::pair"* nonnull align 8 dereferenceable(32) %ref.tmp18)
          to label %invoke.cont22 unwind label %lpad21

invoke.cont22:                                    ; preds = %call2.i.i.i.i3.i22.i.i.noexc
  %_M_start.i.i.i53 = getelementptr inbounds %"struct.std::pair", %"struct.std::pair"* %ref.tmp18, i64 0, i32 1, i32 0, i32 0, i32 0
  %26 = load %struct.GroupByColumn*, %struct.GroupByColumn** %_M_start.i.i.i53, align 8, !tbaa !17
  %tobool.not.i.i.i.i54 = icmp eq %struct.GroupByColumn* %26, null
  br i1 %tobool.not.i.i.i.i54, label %_ZNSt6vectorI13GroupByColumnSaIS0_EED2Ev.exit52, label %if.then.i.i.i.i55

if.then.i.i.i.i55:                                ; preds = %invoke.cont22
  %27 = bitcast %struct.GroupByColumn* %26 to i8*
  call void @_ZdlPv(i8* nonnull %27) #16
  br label %_ZNSt6vectorI13GroupByColumnSaIS0_EED2Ev.exit52

_ZNSt6vectorI13GroupByColumnSaIS0_EED2Ev.exit52:  ; preds = %invoke.cont22, %if.then.i.i.i.i55
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %20) #16
  call void @_ZdlPv(i8* nonnull %call2.i.i.i.i.i62) #16
  br label %if.end28

lpad19:                                           ; preds = %if.else
  %28 = landingpad { i8*, i32 }
          cleanup
  br label %if.then.i.i.i

lpad21:                                           ; preds = %call2.i.i.i.i3.i22.i.i.noexc
  %29 = landingpad { i8*, i32 }
          cleanup
  %_M_start.i.i.i = getelementptr inbounds %"struct.std::pair", %"struct.std::pair"* %ref.tmp18, i64 0, i32 1, i32 0, i32 0, i32 0
  %30 = load %struct.GroupByColumn*, %struct.GroupByColumn** %_M_start.i.i.i, align 8, !tbaa !17
  %tobool.not.i.i.i.i48 = icmp eq %struct.GroupByColumn* %30, null
  br i1 %tobool.not.i.i.i.i48, label %if.then.i.i.i, label %if.then.i.i.i.i

if.then.i.i.i.i:                                  ; preds = %lpad21
  %31 = bitcast %struct.GroupByColumn* %30 to i8*
  call void @_ZdlPv(i8* nonnull %31) #16
  br label %if.then.i.i.i

if.then.i.i.i:                                    ; preds = %if.then.i.i.i.i, %lpad21, %lpad19
  %.pn = phi { i8*, i32 } [ %28, %lpad19 ], [ %29, %lpad21 ], [ %29, %if.then.i.i.i.i ]
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %20) #16
  call void @_ZdlPv(i8* nonnull %call2.i.i.i.i.i62) #16
  resume { i8*, i32 } %.pn

if.end28:                                         ; preds = %_ZNSt6vectorI13GroupByColumnSaIS0_EED2Ev.exit52, %if.then10
  ret void
}

; Function Attrs: uwtable
define dso_local void @sumProcessDouble(%class.SumAggregator* %aggregator, i64 %key, i8* nocapture readonly %columnPtr, i32 %offset) local_unnamed_addr #3 personality i8* bitcast (i32 (...)* @__gxx_personality_v0 to i8*) {
entry:
  %ref.tmp18 = alloca %"struct.std::pair", align 8
  %cmp = icmp eq %class.SumAggregator* %aggregator, null
  br i1 %cmp, label %do.body, label %if.end

do.body:                                          ; preds = %entry
  %call = tail call i32 (i8*, ...) @printf(i8* nonnull dereferenceable(1) getelementptr inbounds ([29 x i8], [29 x i8]* @.str, i64 0, i64 0), i8* getelementptr inbounds ([88 x i8], [88 x i8]* @.str.1, i64 0, i64 0), i8* getelementptr inbounds ([17 x i8], [17 x i8]* @__FUNCTION__.sumProcessDouble, i64 0, i64 0), i32 106, i32 %offset)
  %putchar = tail call i32 @putchar(i32 10)
  br label %if.end

if.end:                                           ; preds = %do.body, %entry
  %state.i = getelementptr inbounds %class.SumAggregator, %class.SumAggregator* %aggregator, i64 0, i32 0, i32 3
  %_M_bucket_count.i.i.i = getelementptr inbounds %class.SumAggregator, %class.SumAggregator* %aggregator, i64 0, i32 0, i32 3, i32 0, i32 1
  %0 = load i64, i64* %_M_bucket_count.i.i.i, align 8, !tbaa !2
  %rem.i.i.i.i.i = urem i64 %key, %0
  %_M_buckets.i.i.i.i = getelementptr inbounds %"class.std::unordered_map", %"class.std::unordered_map"* %state.i, i64 0, i32 0, i32 0
  %1 = load %"struct.std::__detail::_Hash_node_base"**, %"struct.std::__detail::_Hash_node_base"*** %_M_buckets.i.i.i.i, align 8, !tbaa !11
  %arrayidx.i.i.i.i = getelementptr inbounds %"struct.std::__detail::_Hash_node_base"*, %"struct.std::__detail::_Hash_node_base"** %1, i64 %rem.i.i.i.i.i
  %2 = load %"struct.std::__detail::_Hash_node_base"*, %"struct.std::__detail::_Hash_node_base"** %arrayidx.i.i.i.i, align 8, !tbaa !12
  %tobool.not.i.i.i.i = icmp eq %"struct.std::__detail::_Hash_node_base"* %2, null
  br i1 %tobool.not.i.i.i.i, label %if.else, label %if.end.i.i.i.i

if.end.i.i.i.i:                                   ; preds = %if.end
  %3 = bitcast %"struct.std::__detail::_Hash_node_base"* %2 to %"struct.std::__detail::_Hash_node"**
  %4 = load %"struct.std::__detail::_Hash_node"*, %"struct.std::__detail::_Hash_node"** %3, align 8, !tbaa !13
  %_M_storage.i.i.i.i23.i.i.i.i = getelementptr inbounds %"struct.std::__detail::_Hash_node", %"struct.std::__detail::_Hash_node"* %4, i64 0, i32 0, i32 1
  %first.i.i.i.i.i24.i.i.i.i = bitcast %"struct.__gnu_cxx::__aligned_buffer"* %_M_storage.i.i.i.i23.i.i.i.i to i64*
  %5 = load i64, i64* %first.i.i.i.i.i24.i.i.i.i, align 8, !tbaa !14
  %cmp.i.i.i25.i.i.i.i = icmp eq i64 %5, %key
  br i1 %cmp.i.i.i25.i.i.i.i, label %if.then10, label %if.end3.i.i.i.i

for.cond.i.i.i.i:                                 ; preds = %lor.lhs.false.i.i.i.i
  %cmp.i.i.i.i.i.i.i = icmp eq i64 %8, %key
  br i1 %cmp.i.i.i.i.i.i.i, label %if.then10.loopexit, label %if.end3.i.i.i.i

if.end3.i.i.i.i:                                  ; preds = %if.end.i.i.i.i, %for.cond.i.i.i.i
  %__p.026.i.i.i.i = phi %"struct.std::__detail::_Hash_node"* [ %7, %for.cond.i.i.i.i ], [ %4, %if.end.i.i.i.i ]
  %_M_nxt4.i.i.i.i = getelementptr inbounds %"struct.std::__detail::_Hash_node", %"struct.std::__detail::_Hash_node"* %__p.026.i.i.i.i, i64 0, i32 0, i32 0, i32 0
  %6 = load %"struct.std::__detail::_Hash_node_base"*, %"struct.std::__detail::_Hash_node_base"** %_M_nxt4.i.i.i.i, align 8, !tbaa !13
  %tobool5.not.i.i.i.i = icmp eq %"struct.std::__detail::_Hash_node_base"* %6, null
  %7 = bitcast %"struct.std::__detail::_Hash_node_base"* %6 to %"struct.std::__detail::_Hash_node"*
  br i1 %tobool5.not.i.i.i.i, label %if.else, label %lor.lhs.false.i.i.i.i

lor.lhs.false.i.i.i.i:                            ; preds = %if.end3.i.i.i.i
  %_M_storage.i.i.i.i21.i.i.i.i = getelementptr inbounds %"struct.std::__detail::_Hash_node_base", %"struct.std::__detail::_Hash_node_base"* %6, i64 1
  %first.i.i.i.i.i22.i.i.i.i = bitcast %"struct.std::__detail::_Hash_node_base"* %_M_storage.i.i.i.i21.i.i.i.i to i64*
  %8 = load i64, i64* %first.i.i.i.i.i22.i.i.i.i, align 8, !tbaa !14
  %rem.i.i.i.i.i.i.i = urem i64 %8, %0
  %cmp.not.i.i.i.i = icmp eq i64 %rem.i.i.i.i.i.i.i, %rem.i.i.i.i.i
  br i1 %cmp.not.i.i.i.i, label %for.cond.i.i.i.i, label %if.else

if.then10.loopexit:                               ; preds = %for.cond.i.i.i.i
  %9 = bitcast %"struct.std::__detail::_Hash_node_base"* %6 to %"struct.std::__detail::_Hash_node"*
  br label %if.then10

if.then10:                                        ; preds = %if.then10.loopexit, %if.end.i.i.i.i
  %retval.sroa.0.0.i.i = phi %"struct.std::__detail::_Hash_node"* [ %4, %if.end.i.i.i.i ], [ %9, %if.then10.loopexit ]
  %10 = bitcast i8* %columnPtr to double*
  %idx.ext = sext i32 %offset to i64
  %add.ptr = getelementptr inbounds double, double* %10, i64 %idx.ext
  %11 = load double, double* %add.ptr, align 8, !tbaa !31
  %second = getelementptr inbounds %"struct.std::__detail::_Hash_node", %"struct.std::__detail::_Hash_node"* %retval.sroa.0.0.i.i, i64 0, i32 0, i32 1, i32 0, i32 0, i64 8
  %_M_start.i = bitcast i8* %second to %struct.GroupByColumn**
  %12 = load %struct.GroupByColumn*, %struct.GroupByColumn** %_M_start.i, align 8, !tbaa !17
  %val = getelementptr inbounds %struct.GroupByColumn, %struct.GroupByColumn* %12, i64 0, i32 1
  %13 = bitcast i8** %val to double**
  %14 = load double*, double** %13, align 8, !tbaa !20
  %15 = load double, double* %14, align 8, !tbaa !31
  %add = fadd double %11, %15
  store double %add, double* %14, align 8, !tbaa !31
  br label %if.end28

if.else:                                          ; preds = %if.end3.i.i.i.i, %lor.lhs.false.i.i.i.i, %if.end
  %16 = bitcast i8* %columnPtr to double*
  %idx.ext107 = sext i32 %offset to i64
  %add.ptr108 = getelementptr inbounds double, double* %16, i64 %idx.ext107
  %call14 = tail call noalias nonnull dereferenceable(8) i8* @_Znwm(i64 8) #14
  %17 = bitcast i8* %call14 to double*
  %18 = load double, double* %add.ptr108, align 8, !tbaa !31
  store double %18, double* %17, align 8, !tbaa !31
  %call2.i.i.i.i.i62 = tail call noalias nonnull i8* @_Znwm(i64 16) #15
  %c.sroa.0.0..sroa_idx65 = bitcast i8* %call2.i.i.i.i.i62 to i32*
  store i32 3, i32* %c.sroa.0.0..sroa_idx65, align 8, !tbaa.struct !23
  %c.sroa.672.0..sroa_idx76 = getelementptr inbounds i8, i8* %call2.i.i.i.i.i62, i64 8
  %19 = bitcast i8* %c.sroa.672.0..sroa_idx76 to i8**
  store i8* %call14, i8** %19, align 8, !tbaa.struct !25
  %20 = bitcast %"struct.std::pair"* %ref.tmp18 to i8*
  call void @llvm.lifetime.start.p0i8(i64 32, i8* nonnull %20) #16
  %first.i = getelementptr inbounds %"struct.std::pair", %"struct.std::pair"* %ref.tmp18, i64 0, i32 0
  store i64 %key, i64* %first.i, align 8, !tbaa !26
  %second.i = getelementptr inbounds %"struct.std::pair", %"struct.std::pair"* %ref.tmp18, i64 0, i32 1
  %21 = bitcast %"class.std::vector"* %second.i to i8*
  call void @llvm.memset.p0i8.i64(i8* nonnull align 8 dereferenceable(24) %21, i8 0, i64 24, i1 false) #16
  %call2.i.i.i.i3.i22.i.i60 = invoke noalias nonnull i8* @_Znwm(i64 16) #15
          to label %call2.i.i.i.i3.i22.i.i.noexc unwind label %lpad19

call2.i.i.i.i3.i22.i.i.noexc:                     ; preds = %if.else
  %22 = bitcast %"class.std::vector"* %second.i to i8**
  store i8* %call2.i.i.i.i3.i22.i.i60, i8** %22, align 8, !tbaa !17
  %_M_finish.i.i.i.i = getelementptr inbounds %"struct.std::pair", %"struct.std::pair"* %ref.tmp18, i64 0, i32 1, i32 0, i32 0, i32 1
  %23 = bitcast %struct.GroupByColumn** %_M_finish.i.i.i.i to i8**
  %add.ptr.i.i.i.i = getelementptr inbounds i8, i8* %call2.i.i.i.i3.i22.i.i60, i64 16
  %_M_end_of_storage.i.i.i.i = getelementptr inbounds %"struct.std::pair", %"struct.std::pair"* %ref.tmp18, i64 0, i32 1, i32 0, i32 0, i32 2
  %24 = bitcast %struct.GroupByColumn** %_M_end_of_storage.i.i.i.i to i8**
  store i8* %add.ptr.i.i.i.i, i8** %24, align 8, !tbaa !29
  tail call void @llvm.memcpy.p0i8.p0i8.i64(i8* nonnull align 8 dereferenceable(16) %call2.i.i.i.i3.i22.i.i60, i8* nonnull align 8 dereferenceable(16) %call2.i.i.i.i.i62, i64 16, i1 false) #16
  store i8* %add.ptr.i.i.i.i, i8** %23, align 8, !tbaa !30
  %25 = getelementptr inbounds %"class.std::unordered_map", %"class.std::unordered_map"* %state.i, i64 0, i32 0
  %call3.i.i57 = invoke { %"struct.std::__detail::_Hash_node"*, i8 } @_ZNSt10_HashtableImSt4pairIKmSt6vectorI13GroupByColumnSaIS3_EEESaIS6_ENSt8__detail10_Select1stESt8equal_toImESt4hashImENS8_18_Mod_range_hashingENS8_20_Default_ranged_hashENS8_20_Prime_rehash_policyENS8_17_Hashtable_traitsILb0ELb0ELb1EEEE10_M_emplaceIJS6_EEES0_INS8_14_Node_iteratorIS6_Lb0ELb0EEEbESt17integral_constantIbLb1EEDpOT_(%"class.std::_Hashtable"* nonnull dereferenceable(56) %25, %"struct.std::pair"* nonnull align 8 dereferenceable(32) %ref.tmp18)
          to label %invoke.cont22 unwind label %lpad21

invoke.cont22:                                    ; preds = %call2.i.i.i.i3.i22.i.i.noexc
  %_M_start.i.i.i53 = getelementptr inbounds %"struct.std::pair", %"struct.std::pair"* %ref.tmp18, i64 0, i32 1, i32 0, i32 0, i32 0
  %26 = load %struct.GroupByColumn*, %struct.GroupByColumn** %_M_start.i.i.i53, align 8, !tbaa !17
  %tobool.not.i.i.i.i54 = icmp eq %struct.GroupByColumn* %26, null
  br i1 %tobool.not.i.i.i.i54, label %_ZNSt6vectorI13GroupByColumnSaIS0_EED2Ev.exit52, label %if.then.i.i.i.i55

if.then.i.i.i.i55:                                ; preds = %invoke.cont22
  %27 = bitcast %struct.GroupByColumn* %26 to i8*
  call void @_ZdlPv(i8* nonnull %27) #16
  br label %_ZNSt6vectorI13GroupByColumnSaIS0_EED2Ev.exit52

_ZNSt6vectorI13GroupByColumnSaIS0_EED2Ev.exit52:  ; preds = %invoke.cont22, %if.then.i.i.i.i55
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %20) #16
  call void @_ZdlPv(i8* nonnull %call2.i.i.i.i.i62) #16
  br label %if.end28

lpad19:                                           ; preds = %if.else
  %28 = landingpad { i8*, i32 }
          cleanup
  br label %if.then.i.i.i

lpad21:                                           ; preds = %call2.i.i.i.i3.i22.i.i.noexc
  %29 = landingpad { i8*, i32 }
          cleanup
  %_M_start.i.i.i = getelementptr inbounds %"struct.std::pair", %"struct.std::pair"* %ref.tmp18, i64 0, i32 1, i32 0, i32 0, i32 0
  %30 = load %struct.GroupByColumn*, %struct.GroupByColumn** %_M_start.i.i.i, align 8, !tbaa !17
  %tobool.not.i.i.i.i48 = icmp eq %struct.GroupByColumn* %30, null
  br i1 %tobool.not.i.i.i.i48, label %if.then.i.i.i, label %if.then.i.i.i.i

if.then.i.i.i.i:                                  ; preds = %lpad21
  %31 = bitcast %struct.GroupByColumn* %30 to i8*
  call void @_ZdlPv(i8* nonnull %31) #16
  br label %if.then.i.i.i

if.then.i.i.i:                                    ; preds = %if.then.i.i.i.i, %lpad21, %lpad19
  %.pn = phi { i8*, i32 } [ %28, %lpad19 ], [ %29, %lpad21 ], [ %29, %if.then.i.i.i.i ]
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %20) #16
  call void @_ZdlPv(i8* nonnull %call2.i.i.i.i.i62) #16
  resume { i8*, i32 } %.pn

if.end28:                                         ; preds = %_ZNSt6vectorI13GroupByColumnSaIS0_EED2Ev.exit52, %if.then10
  ret void
}

; Function Attrs: nounwind uwtable
define linkonce_odr dso_local void @_ZN13SumAggregatorD0Ev(%class.SumAggregator* nonnull dereferenceable(72) %this) unnamed_addr #7 comdat align 2 {
entry:
  %0 = getelementptr inbounds %class.SumAggregator, %class.SumAggregator* %this, i64 0, i32 0
  tail call void @_ZN10AggregatorD2Ev(%class.Aggregator* nonnull dereferenceable(72) %0) #16
  %1 = bitcast %class.SumAggregator* %this to i8*
  tail call void @_ZdlPv(i8* nonnull %1) #17
  ret void
}

; Function Attrs: nounwind uwtable willreturn mustprogress
define linkonce_odr dso_local void @_ZN13SumAggregator7processEmPvN3opt10ColumnTypeE(%class.SumAggregator* nonnull dereferenceable(72) %this, i64 %key, i8* %valuePtr, i32 %type) unnamed_addr #8 comdat align 2 {
entry:
  ret void
}

; Function Attrs: noreturn
declare dso_local void @_ZSt17__throw_bad_allocv() local_unnamed_addr #9

; Function Attrs: argmemonly nofree nosync nounwind willreturn
declare void @llvm.memcpy.p0i8.p0i8.i64(i8* noalias nocapture writeonly, i8* noalias nocapture readonly, i64, i1 immarg) #4

; Function Attrs: noinline noreturn nounwind
define linkonce_odr hidden void @__clang_call_terminate(i8* %0) local_unnamed_addr #10 comdat {
  %2 = tail call i8* @__cxa_begin_catch(i8* %0) #16
  tail call void @_ZSt9terminatev() #18
  unreachable
}

declare dso_local i8* @__cxa_begin_catch(i8*) local_unnamed_addr

declare dso_local void @_ZSt9terminatev() local_unnamed_addr

; Function Attrs: nobuiltin nounwind
declare dso_local void @_ZdlPv(i8*) local_unnamed_addr #11

; Function Attrs: nounwind uwtable
define linkonce_odr dso_local void @_ZN10AggregatorD2Ev(%class.Aggregator* nonnull dereferenceable(72) %this) unnamed_addr #7 comdat align 2 personality i32 (...)* @__gxx_personality_v0 {
entry:
  %0 = getelementptr inbounds %class.Aggregator, %class.Aggregator* %this, i64 0, i32 0
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [6 x i8*] }, { [6 x i8*] }* @_ZTV10Aggregator, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %0, align 8, !tbaa !33
  %state = getelementptr inbounds %class.Aggregator, %class.Aggregator* %this, i64 0, i32 3
  %_M_nxt.i.i.i = getelementptr inbounds %class.Aggregator, %class.Aggregator* %this, i64 0, i32 3, i32 0, i32 2, i32 0
  %1 = bitcast %"struct.std::__detail::_Hash_node_base"** %_M_nxt.i.i.i to %"struct.std::__detail::_Hash_node"**
  %__begin1.sroa.0.020 = load %"struct.std::__detail::_Hash_node"*, %"struct.std::__detail::_Hash_node"** %1, align 8, !tbaa !13
  %cmp.i.not21 = icmp eq %"struct.std::__detail::_Hash_node"* %__begin1.sroa.0.020, null
  br i1 %cmp.i.not21, label %_ZNSt10_HashtableImSt4pairIKmSt6vectorI13GroupByColumnSaIS3_EEESaIS6_ENSt8__detail10_Select1stESt8equal_toImESt4hashImENS8_18_Mod_range_hashingENS8_20_Default_ranged_hashENS8_20_Prime_rehash_policyENS8_17_Hashtable_traitsILb0ELb0ELb1EEEE5clearEv.exit.i.i, label %for.body

for.cond.cleanup:                                 ; preds = %delete.end
  %.pre = load %"struct.std::__detail::_Hash_node"*, %"struct.std::__detail::_Hash_node"** %1, align 8, !tbaa !35
  %tobool.not5.i.i.i = icmp eq %"struct.std::__detail::_Hash_node"* %.pre, null
  br i1 %tobool.not5.i.i.i, label %_ZNSt10_HashtableImSt4pairIKmSt6vectorI13GroupByColumnSaIS3_EEESaIS6_ENSt8__detail10_Select1stESt8equal_toImESt4hashImENS8_18_Mod_range_hashingENS8_20_Default_ranged_hashENS8_20_Prime_rehash_policyENS8_17_Hashtable_traitsILb0ELb0ELb1EEEE5clearEv.exit.i.i, label %while.body.i.i.i

while.body.i.i.i:                                 ; preds = %for.cond.cleanup, %_ZNSt8__detail16_Hashtable_allocISaINS_10_Hash_nodeISt4pairIKmSt6vectorI13GroupByColumnSaIS5_EEELb0EEEEE18_M_deallocate_nodeEPS9_.exit.i.i.i
  %__n.addr.06.i.i.i = phi %"struct.std::__detail::_Hash_node"* [ %3, %_ZNSt8__detail16_Hashtable_allocISaINS_10_Hash_nodeISt4pairIKmSt6vectorI13GroupByColumnSaIS5_EEELb0EEEEE18_M_deallocate_nodeEPS9_.exit.i.i.i ], [ %.pre, %for.cond.cleanup ]
  %2 = bitcast %"struct.std::__detail::_Hash_node"* %__n.addr.06.i.i.i to %"struct.std::__detail::_Hash_node"**
  %3 = load %"struct.std::__detail::_Hash_node"*, %"struct.std::__detail::_Hash_node"** %2, align 8, !tbaa !13
  %_M_start.i.i.i.i.i.i.i.i.i = getelementptr inbounds %"struct.std::__detail::_Hash_node", %"struct.std::__detail::_Hash_node"* %__n.addr.06.i.i.i, i64 0, i32 0, i32 1, i32 0, i32 0, i64 8
  %4 = bitcast i8* %_M_start.i.i.i.i.i.i.i.i.i to %struct.GroupByColumn**
  %5 = load %struct.GroupByColumn*, %struct.GroupByColumn** %4, align 8, !tbaa !17
  %tobool.not.i.i.i.i.i.i.i.i.i.i = icmp eq %struct.GroupByColumn* %5, null
  br i1 %tobool.not.i.i.i.i.i.i.i.i.i.i, label %_ZNSt8__detail16_Hashtable_allocISaINS_10_Hash_nodeISt4pairIKmSt6vectorI13GroupByColumnSaIS5_EEELb0EEEEE18_M_deallocate_nodeEPS9_.exit.i.i.i, label %if.then.i.i.i.i.i.i.i.i.i.i

if.then.i.i.i.i.i.i.i.i.i.i:                      ; preds = %while.body.i.i.i
  %6 = bitcast %struct.GroupByColumn* %5 to i8*
  tail call void @_ZdlPv(i8* nonnull %6) #16
  br label %_ZNSt8__detail16_Hashtable_allocISaINS_10_Hash_nodeISt4pairIKmSt6vectorI13GroupByColumnSaIS5_EEELb0EEEEE18_M_deallocate_nodeEPS9_.exit.i.i.i

_ZNSt8__detail16_Hashtable_allocISaINS_10_Hash_nodeISt4pairIKmSt6vectorI13GroupByColumnSaIS5_EEELb0EEEEE18_M_deallocate_nodeEPS9_.exit.i.i.i: ; preds = %if.then.i.i.i.i.i.i.i.i.i.i, %while.body.i.i.i
  %7 = bitcast %"struct.std::__detail::_Hash_node"* %__n.addr.06.i.i.i to i8*
  tail call void @_ZdlPv(i8* nonnull %7) #16
  %tobool.not.i.i.i = icmp eq %"struct.std::__detail::_Hash_node"* %3, null
  br i1 %tobool.not.i.i.i, label %_ZNSt10_HashtableImSt4pairIKmSt6vectorI13GroupByColumnSaIS3_EEESaIS6_ENSt8__detail10_Select1stESt8equal_toImESt4hashImENS8_18_Mod_range_hashingENS8_20_Default_ranged_hashENS8_20_Prime_rehash_policyENS8_17_Hashtable_traitsILb0ELb0ELb1EEEE5clearEv.exit.i.i, label %while.body.i.i.i, !llvm.loop !36

_ZNSt10_HashtableImSt4pairIKmSt6vectorI13GroupByColumnSaIS3_EEESaIS6_ENSt8__detail10_Select1stESt8equal_toImESt4hashImENS8_18_Mod_range_hashingENS8_20_Default_ranged_hashENS8_20_Prime_rehash_policyENS8_17_Hashtable_traitsILb0ELb0ELb1EEEE5clearEv.exit.i.i: ; preds = %_ZNSt8__detail16_Hashtable_allocISaINS_10_Hash_nodeISt4pairIKmSt6vectorI13GroupByColumnSaIS5_EEELb0EEEEE18_M_deallocate_nodeEPS9_.exit.i.i.i, %entry, %for.cond.cleanup
  %8 = bitcast %"class.std::unordered_map"* %state to i8**
  %9 = load i8*, i8** %8, align 8, !tbaa !11
  %_M_bucket_count.i.i = getelementptr inbounds %class.Aggregator, %class.Aggregator* %this, i64 0, i32 3, i32 0, i32 1
  %10 = load i64, i64* %_M_bucket_count.i.i, align 8, !tbaa !2
  %mul.i.i = shl i64 %10, 3
  tail call void @llvm.memset.p0i8.i64(i8* align 8 %9, i8 0, i64 %mul.i.i, i1 false) #16
  %11 = bitcast %"struct.std::__detail::_Hash_node_base"** %_M_nxt.i.i.i to i8*
  %.pre23 = load i8*, i8** %8, align 8, !tbaa !11
  %.pre24 = load i64, i64* %_M_bucket_count.i.i, align 8, !tbaa !2
  %phi.bo = shl i64 %.pre24, 3
  tail call void @llvm.memset.p0i8.i64(i8* align 8 %.pre23, i8 0, i64 %phi.bo, i1 false) #16
  tail call void @llvm.memset.p0i8.i64(i8* nonnull align 8 dereferenceable(16) %11, i8 0, i64 16, i1 false) #16
  %_M_buckets.i.i.i = getelementptr inbounds %"class.std::unordered_map", %"class.std::unordered_map"* %state, i64 0, i32 0, i32 0
  %12 = load %"struct.std::__detail::_Hash_node_base"**, %"struct.std::__detail::_Hash_node_base"*** %_M_buckets.i.i.i, align 8, !tbaa !11
  %_M_single_bucket.i.i.i.i.i = getelementptr inbounds %class.Aggregator, %class.Aggregator* %this, i64 0, i32 3, i32 0, i32 5
  %cmp.i.i.i.i.i = icmp eq %"struct.std::__detail::_Hash_node_base"** %_M_single_bucket.i.i.i.i.i, %12
  br i1 %cmp.i.i.i.i.i, label %_ZNSt13unordered_mapImSt6vectorI13GroupByColumnSaIS1_EESt4hashImESt8equal_toImESaISt4pairIKmS3_EEED2Ev.exit, label %if.end.i.i.i.i

if.end.i.i.i.i:                                   ; preds = %_ZNSt10_HashtableImSt4pairIKmSt6vectorI13GroupByColumnSaIS3_EEESaIS6_ENSt8__detail10_Select1stESt8equal_toImESt4hashImENS8_18_Mod_range_hashingENS8_20_Default_ranged_hashENS8_20_Prime_rehash_policyENS8_17_Hashtable_traitsILb0ELb0ELb1EEEE5clearEv.exit.i.i
  %13 = bitcast %"struct.std::__detail::_Hash_node_base"** %12 to i8*
  tail call void @_ZdlPv(i8* %13) #16
  br label %_ZNSt13unordered_mapImSt6vectorI13GroupByColumnSaIS1_EESt4hashImESt8equal_toImESaISt4pairIKmS3_EEED2Ev.exit

_ZNSt13unordered_mapImSt6vectorI13GroupByColumnSaIS1_EESt4hashImESt8equal_toImESaISt4pairIKmS3_EEED2Ev.exit: ; preds = %_ZNSt10_HashtableImSt4pairIKmSt6vectorI13GroupByColumnSaIS3_EEESaIS6_ENSt8__detail10_Select1stESt8equal_toImESt4hashImENS8_18_Mod_range_hashingENS8_20_Default_ranged_hashENS8_20_Prime_rehash_policyENS8_17_Hashtable_traitsILb0ELb0ELb1EEEE5clearEv.exit.i.i, %if.end.i.i.i.i
  ret void

for.body:                                         ; preds = %entry, %delete.end
  %__begin1.sroa.0.022 = phi %"struct.std::__detail::_Hash_node"* [ %__begin1.sroa.0.0, %delete.end ], [ %__begin1.sroa.0.020, %entry ]
  %second = getelementptr inbounds %"struct.std::__detail::_Hash_node", %"struct.std::__detail::_Hash_node"* %__begin1.sroa.0.022, i64 0, i32 0, i32 1, i32 0, i32 0, i64 8
  %_M_start.i = bitcast i8* %second to %struct.GroupByColumn**
  %14 = load %struct.GroupByColumn*, %struct.GroupByColumn** %_M_start.i, align 8, !tbaa !17
  %val = getelementptr inbounds %struct.GroupByColumn, %struct.GroupByColumn* %14, i64 0, i32 1
  %15 = load i8*, i8** %val, align 8, !tbaa !20
  %isnull = icmp eq i8* %15, null
  br i1 %isnull, label %delete.end, label %delete.notnull

delete.notnull:                                   ; preds = %for.body
  tail call void @_ZdlPv(i8* nonnull %15) #17
  br label %delete.end

delete.end:                                       ; preds = %delete.notnull, %for.body
  %16 = bitcast %"struct.std::__detail::_Hash_node"* %__begin1.sroa.0.022 to %"struct.std::__detail::_Hash_node"**
  %__begin1.sroa.0.0 = load %"struct.std::__detail::_Hash_node"*, %"struct.std::__detail::_Hash_node"** %16, align 8, !tbaa !13
  %cmp.i.not = icmp eq %"struct.std::__detail::_Hash_node"* %__begin1.sroa.0.0, null
  br i1 %cmp.i.not, label %for.cond.cleanup, label %for.body
}

; Function Attrs: nounwind uwtable
define linkonce_odr dso_local void @_ZN10AggregatorD0Ev(%class.Aggregator* nonnull dereferenceable(72) %this) unnamed_addr #7 comdat align 2 {
entry:
  tail call void @llvm.trap() #18
  unreachable
}

declare dso_local void @__cxa_pure_virtual() unnamed_addr

; Function Attrs: argmemonly nofree nosync nounwind willreturn writeonly
declare void @llvm.memset.p0i8.i64(i8* nocapture writeonly, i8, i64, i1 immarg) #12

; Function Attrs: cold noreturn nounwind
declare void @llvm.trap() #13

declare dso_local void @__cxa_rethrow() local_unnamed_addr

declare dso_local void @__cxa_end_catch() local_unnamed_addr

; Function Attrs: uwtable
define linkonce_odr dso_local { %"struct.std::__detail::_Hash_node"*, i8 } @_ZNSt10_HashtableImSt4pairIKmSt6vectorI13GroupByColumnSaIS3_EEESaIS6_ENSt8__detail10_Select1stESt8equal_toImESt4hashImENS8_18_Mod_range_hashingENS8_20_Default_ranged_hashENS8_20_Prime_rehash_policyENS8_17_Hashtable_traitsILb0ELb0ELb1EEEE10_M_emplaceIJS6_EEES0_INS8_14_Node_iteratorIS6_Lb0ELb0EEEbESt17integral_constantIbLb1EEDpOT_(%"class.std::_Hashtable"* nonnull dereferenceable(56) %this, %"struct.std::pair"* nonnull align 8 dereferenceable(32) %__args) local_unnamed_addr #3 comdat align 2 personality i8* bitcast (i32 (...)* @__gxx_personality_v0 to i8*) {
invoke.cont:
  %call2.i.i.i = tail call noalias nonnull i8* @_Znwm(i64 40) #15
  %0 = bitcast i8* %call2.i.i.i to %"struct.std::__detail::_Hash_node"*
  %_M_nxt.i.i.i.i = bitcast i8* %call2.i.i.i to %"struct.std::__detail::_Hash_node_base"**
  store %"struct.std::__detail::_Hash_node_base"* null, %"struct.std::__detail::_Hash_node_base"** %_M_nxt.i.i.i.i, align 8, !tbaa !13
  %_M_storage.i.i = getelementptr inbounds i8, i8* %call2.i.i.i, i64 8
  %first.i.i.i.i = bitcast i8* %_M_storage.i.i to i64*
  %first2.i.i.i.i = getelementptr inbounds %"struct.std::pair", %"struct.std::pair"* %__args, i64 0, i32 0
  %1 = load i64, i64* %first2.i.i.i.i, align 8, !tbaa !14
  store i64 %1, i64* %first.i.i.i.i, align 8, !tbaa !26
  %second.i.i.i.i = getelementptr inbounds i8, i8* %call2.i.i.i, i64 16
  %_M_start.i.i.i.i.i.i.i = bitcast i8* %second.i.i.i.i to %struct.GroupByColumn**
  %_M_start2.i.i.i.i.i.i.i = getelementptr inbounds %"struct.std::pair", %"struct.std::pair"* %__args, i64 0, i32 1, i32 0, i32 0, i32 0
  %2 = load %struct.GroupByColumn*, %struct.GroupByColumn** %_M_start2.i.i.i.i.i.i.i, align 8, !tbaa !12
  store %struct.GroupByColumn* %2, %struct.GroupByColumn** %_M_start.i.i.i.i.i.i.i, align 8, !tbaa !12
  %_M_finish.i.i.i.i.i.i.i = getelementptr inbounds i8, i8* %call2.i.i.i, i64 24
  %_M_finish3.i.i.i.i.i.i.i = getelementptr inbounds %"struct.std::pair", %"struct.std::pair"* %__args, i64 0, i32 1, i32 0, i32 0, i32 1
  %_M_end_of_storage4.i.i.i.i.i.i.i = getelementptr inbounds %"struct.std::pair", %"struct.std::pair"* %__args, i64 0, i32 1, i32 0, i32 0, i32 2
  %3 = bitcast %struct.GroupByColumn** %_M_finish3.i.i.i.i.i.i.i to <2 x %struct.GroupByColumn*>*
  %4 = load <2 x %struct.GroupByColumn*>, <2 x %struct.GroupByColumn*>* %3, align 8, !tbaa !12
  %5 = bitcast %struct.GroupByColumn** %_M_start2.i.i.i.i.i.i.i to <2 x %struct.GroupByColumn*>*
  store <2 x %struct.GroupByColumn*> zeroinitializer, <2 x %struct.GroupByColumn*>* %5, align 8, !tbaa !12
  %6 = bitcast i8* %_M_finish.i.i.i.i.i.i.i to <2 x %struct.GroupByColumn*>*
  store <2 x %struct.GroupByColumn*> %4, <2 x %struct.GroupByColumn*>* %6, align 8, !tbaa !12
  store %struct.GroupByColumn* null, %struct.GroupByColumn** %_M_end_of_storage4.i.i.i.i.i.i.i, align 8, !tbaa !12
  %_M_bucket_count.i = getelementptr inbounds %"class.std::_Hashtable", %"class.std::_Hashtable"* %this, i64 0, i32 1
  %7 = load i64, i64* %_M_bucket_count.i, align 8, !tbaa !2
  %rem.i.i.i = urem i64 %1, %7
  %_M_buckets.i.i = getelementptr inbounds %"class.std::_Hashtable", %"class.std::_Hashtable"* %this, i64 0, i32 0
  %8 = load %"struct.std::__detail::_Hash_node_base"**, %"struct.std::__detail::_Hash_node_base"*** %_M_buckets.i.i, align 8, !tbaa !11
  %arrayidx.i.i = getelementptr inbounds %"struct.std::__detail::_Hash_node_base"*, %"struct.std::__detail::_Hash_node_base"** %8, i64 %rem.i.i.i
  %9 = load %"struct.std::__detail::_Hash_node_base"*, %"struct.std::__detail::_Hash_node_base"** %arrayidx.i.i, align 8, !tbaa !12
  %tobool.not.i.i = icmp eq %"struct.std::__detail::_Hash_node_base"* %9, null
  br i1 %tobool.not.i.i, label %cleanup.cont, label %if.end.i.i

if.end.i.i:                                       ; preds = %invoke.cont
  %10 = bitcast %"struct.std::__detail::_Hash_node_base"* %9 to %"struct.std::__detail::_Hash_node"**
  %11 = load %"struct.std::__detail::_Hash_node"*, %"struct.std::__detail::_Hash_node"** %10, align 8, !tbaa !13
  %_M_storage.i.i.i.i23.i.i = getelementptr inbounds %"struct.std::__detail::_Hash_node", %"struct.std::__detail::_Hash_node"* %11, i64 0, i32 0, i32 1
  %first.i.i.i.i.i24.i.i = bitcast %"struct.__gnu_cxx::__aligned_buffer"* %_M_storage.i.i.i.i23.i.i to i64*
  %12 = load i64, i64* %first.i.i.i.i.i24.i.i, align 8, !tbaa !14
  %cmp.i.i.i25.i.i = icmp eq i64 %1, %12
  br i1 %cmp.i.i.i25.i.i, label %if.then, label %if.end3.i.i

for.cond.i.i:                                     ; preds = %lor.lhs.false.i.i
  %cmp.i.i.i.i.i = icmp eq i64 %1, %15
  br i1 %cmp.i.i.i.i.i, label %if.then.loopexit, label %if.end3.i.i

if.end3.i.i:                                      ; preds = %if.end.i.i, %for.cond.i.i
  %__p.026.i.i = phi %"struct.std::__detail::_Hash_node"* [ %14, %for.cond.i.i ], [ %11, %if.end.i.i ]
  %_M_nxt4.i.i = getelementptr inbounds %"struct.std::__detail::_Hash_node", %"struct.std::__detail::_Hash_node"* %__p.026.i.i, i64 0, i32 0, i32 0, i32 0
  %13 = load %"struct.std::__detail::_Hash_node_base"*, %"struct.std::__detail::_Hash_node_base"** %_M_nxt4.i.i, align 8, !tbaa !13
  %tobool5.not.i.i = icmp eq %"struct.std::__detail::_Hash_node_base"* %13, null
  %14 = bitcast %"struct.std::__detail::_Hash_node_base"* %13 to %"struct.std::__detail::_Hash_node"*
  br i1 %tobool5.not.i.i, label %cleanup.cont, label %lor.lhs.false.i.i

lor.lhs.false.i.i:                                ; preds = %if.end3.i.i
  %_M_storage.i.i.i.i21.i.i = getelementptr inbounds %"struct.std::__detail::_Hash_node_base", %"struct.std::__detail::_Hash_node_base"* %13, i64 1
  %first.i.i.i.i.i22.i.i = bitcast %"struct.std::__detail::_Hash_node_base"* %_M_storage.i.i.i.i21.i.i to i64*
  %15 = load i64, i64* %first.i.i.i.i.i22.i.i, align 8, !tbaa !14
  %rem.i.i.i.i.i = urem i64 %15, %7
  %cmp.not.i.i = icmp eq i64 %rem.i.i.i.i.i, %rem.i.i.i
  br i1 %cmp.not.i.i, label %for.cond.i.i, label %cleanup.cont

if.then.loopexit:                                 ; preds = %for.cond.i.i
  %16 = bitcast %"struct.std::__detail::_Hash_node_base"* %13 to %"struct.std::__detail::_Hash_node"*
  br label %if.then

if.then:                                          ; preds = %if.then.loopexit, %if.end.i.i
  %retval.0.i = phi %"struct.std::__detail::_Hash_node"* [ %11, %if.end.i.i ], [ %16, %if.then.loopexit ]
  %tobool.not.i.i.i.i.i.i.i = icmp eq %struct.GroupByColumn* %2, null
  br i1 %tobool.not.i.i.i.i.i.i.i, label %cleanup, label %if.then.i.i.i.i.i.i.i

if.then.i.i.i.i.i.i.i:                            ; preds = %if.then
  %17 = bitcast %struct.GroupByColumn* %2 to i8*
  tail call void @_ZdlPv(i8* nonnull %17) #16
  br label %cleanup

cleanup:                                          ; preds = %if.then.i.i.i.i.i.i.i, %if.then
  tail call void @_ZdlPv(i8* nonnull %call2.i.i.i) #16
  br label %cleanup19

cleanup.cont:                                     ; preds = %if.end3.i.i, %lor.lhs.false.i.i, %invoke.cont
  %call15 = tail call %"struct.std::__detail::_Hash_node"* @_ZNSt10_HashtableImSt4pairIKmSt6vectorI13GroupByColumnSaIS3_EEESaIS6_ENSt8__detail10_Select1stESt8equal_toImESt4hashImENS8_18_Mod_range_hashingENS8_20_Default_ranged_hashENS8_20_Prime_rehash_policyENS8_17_Hashtable_traitsILb0ELb0ELb1EEEE21_M_insert_unique_nodeEmmPNS8_10_Hash_nodeIS6_Lb0EEE(%"class.std::_Hashtable"* nonnull dereferenceable(56) %this, i64 %rem.i.i.i, i64 %1, %"struct.std::__detail::_Hash_node"* nonnull %0)
  br label %cleanup19

cleanup19:                                        ; preds = %cleanup, %cleanup.cont
  %retval.sroa.0.1 = phi %"struct.std::__detail::_Hash_node"* [ %call15, %cleanup.cont ], [ %retval.0.i, %cleanup ]
  %retval.sroa.3.1 = phi i8 [ 1, %cleanup.cont ], [ 0, %cleanup ]
  %.fca.0.insert = insertvalue { %"struct.std::__detail::_Hash_node"*, i8 } undef, %"struct.std::__detail::_Hash_node"* %retval.sroa.0.1, 0
  %.fca.1.insert = insertvalue { %"struct.std::__detail::_Hash_node"*, i8 } %.fca.0.insert, i8 %retval.sroa.3.1, 1
  ret { %"struct.std::__detail::_Hash_node"*, i8 } %.fca.1.insert
}

; Function Attrs: uwtable
define linkonce_odr dso_local %"struct.std::__detail::_Hash_node"* @_ZNSt10_HashtableImSt4pairIKmSt6vectorI13GroupByColumnSaIS3_EEESaIS6_ENSt8__detail10_Select1stESt8equal_toImESt4hashImENS8_18_Mod_range_hashingENS8_20_Default_ranged_hashENS8_20_Prime_rehash_policyENS8_17_Hashtable_traitsILb0ELb0ELb1EEEE21_M_insert_unique_nodeEmmPNS8_10_Hash_nodeIS6_Lb0EEE(%"class.std::_Hashtable"* nonnull dereferenceable(56) %this, i64 %__bkt, i64 %__code, %"struct.std::__detail::_Hash_node"* %__node) local_unnamed_addr #3 comdat align 2 personality i8* bitcast (i32 (...)* @__gxx_personality_v0 to i8*) {
entry:
  %_M_rehash_policy = getelementptr inbounds %"class.std::_Hashtable", %"class.std::_Hashtable"* %this, i64 0, i32 4
  %_M_next_resize.i = getelementptr inbounds %"class.std::_Hashtable", %"class.std::_Hashtable"* %this, i64 0, i32 4, i32 1
  %0 = load i64, i64* %_M_next_resize.i, align 8, !tbaa !38
  %_M_bucket_count = getelementptr inbounds %"class.std::_Hashtable", %"class.std::_Hashtable"* %this, i64 0, i32 1
  %1 = load i64, i64* %_M_bucket_count, align 8, !tbaa !2
  %_M_element_count = getelementptr inbounds %"class.std::_Hashtable", %"class.std::_Hashtable"* %this, i64 0, i32 3
  %2 = load i64, i64* %_M_element_count, align 8, !tbaa !39
  %call3 = tail call { i8, i64 } @_ZNKSt8__detail20_Prime_rehash_policy14_M_need_rehashEmmm(%"struct.std::__detail::_Prime_rehash_policy"* nonnull dereferenceable(16) %_M_rehash_policy, i64 %1, i64 %2, i64 1)
  %3 = extractvalue { i8, i64 } %call3, 0
  %4 = and i8 %3, 1
  %tobool.not = icmp eq i8 %4, 0
  br i1 %tobool.not, label %entry.if.end_crit_edge, label %if.then

entry.if.end_crit_edge:                           ; preds = %entry
  %_M_buckets.i.phi.trans.insert = getelementptr inbounds %"class.std::_Hashtable", %"class.std::_Hashtable"* %this, i64 0, i32 0
  %.pre = load %"struct.std::__detail::_Hash_node_base"**, %"struct.std::__detail::_Hash_node_base"*** %_M_buckets.i.phi.trans.insert, align 8, !tbaa !11
  br label %if.end

if.then:                                          ; preds = %entry
  %5 = extractvalue { i8, i64 } %call3, 1
  %cmp.i.i = icmp eq i64 %5, 1
  br i1 %cmp.i.i, label %if.then.i.i, label %if.end.i.i, !prof !40

if.then.i.i:                                      ; preds = %if.then
  %_M_single_bucket.i.i = getelementptr inbounds %"class.std::_Hashtable", %"class.std::_Hashtable"* %this, i64 0, i32 5
  store %"struct.std::__detail::_Hash_node_base"* null, %"struct.std::__detail::_Hash_node_base"** %_M_single_bucket.i.i, align 8, !tbaa !41
  br label %_ZNSt10_HashtableImSt4pairIKmSt6vectorI13GroupByColumnSaIS3_EEESaIS6_ENSt8__detail10_Select1stESt8equal_toImESt4hashImENS8_18_Mod_range_hashingENS8_20_Default_ranged_hashENS8_20_Prime_rehash_policyENS8_17_Hashtable_traitsILb0ELb0ELb1EEEE19_M_allocate_bucketsEm.exit.i

if.end.i.i:                                       ; preds = %if.then
  %cmp.i.i.i.i.i = icmp ugt i64 %5, 2305843009213693951
  br i1 %cmp.i.i.i.i.i, label %if.then.i.i.i.i.i, label %_ZNSt8__detail16_Hashtable_allocISaINS_10_Hash_nodeISt4pairIKmSt6vectorI13GroupByColumnSaIS5_EEELb0EEEEE19_M_allocate_bucketsEm.exit.i.i

if.then.i.i.i.i.i:                                ; preds = %if.end.i.i
  invoke void @_ZSt17__throw_bad_allocv() #19
          to label %.noexc unwind label %lpad.i

.noexc:                                           ; preds = %if.then.i.i.i.i.i
  unreachable

_ZNSt8__detail16_Hashtable_allocISaINS_10_Hash_nodeISt4pairIKmSt6vectorI13GroupByColumnSaIS5_EEELb0EEEEE19_M_allocate_bucketsEm.exit.i.i: ; preds = %if.end.i.i
  %mul.i.i.i.i.i = shl nuw i64 %5, 3
  %call2.i.i10.i.i.i33 = invoke noalias nonnull i8* @_Znwm(i64 %mul.i.i.i.i.i) #15
          to label %call2.i.i10.i.i.i.noexc unwind label %lpad.i

call2.i.i10.i.i.i.noexc:                          ; preds = %_ZNSt8__detail16_Hashtable_allocISaINS_10_Hash_nodeISt4pairIKmSt6vectorI13GroupByColumnSaIS5_EEELb0EEEEE19_M_allocate_bucketsEm.exit.i.i
  %6 = bitcast i8* %call2.i.i10.i.i.i33 to %"struct.std::__detail::_Hash_node_base"**
  tail call void @llvm.memset.p0i8.i64(i8* nonnull align 8 %call2.i.i10.i.i.i33, i8 0, i64 %mul.i.i.i.i.i, i1 false)
  br label %_ZNSt10_HashtableImSt4pairIKmSt6vectorI13GroupByColumnSaIS3_EEESaIS6_ENSt8__detail10_Select1stESt8equal_toImESt4hashImENS8_18_Mod_range_hashingENS8_20_Default_ranged_hashENS8_20_Prime_rehash_policyENS8_17_Hashtable_traitsILb0ELb0ELb1EEEE19_M_allocate_bucketsEm.exit.i

_ZNSt10_HashtableImSt4pairIKmSt6vectorI13GroupByColumnSaIS3_EEESaIS6_ENSt8__detail10_Select1stESt8equal_toImESt4hashImENS8_18_Mod_range_hashingENS8_20_Default_ranged_hashENS8_20_Prime_rehash_policyENS8_17_Hashtable_traitsILb0ELb0ELb1EEEE19_M_allocate_bucketsEm.exit.i: ; preds = %call2.i.i10.i.i.i.noexc, %if.then.i.i
  %retval.0.i.i = phi %"struct.std::__detail::_Hash_node_base"** [ %_M_single_bucket.i.i, %if.then.i.i ], [ %6, %call2.i.i10.i.i.i.noexc ]
  %_M_nxt.i.i = getelementptr inbounds %"class.std::_Hashtable", %"class.std::_Hashtable"* %this, i64 0, i32 2, i32 0
  %7 = bitcast %"struct.std::__detail::_Hash_node_base"** %_M_nxt.i.i to %"struct.std::__detail::_Hash_node"**
  %8 = load %"struct.std::__detail::_Hash_node"*, %"struct.std::__detail::_Hash_node"** %7, align 8, !tbaa !35
  %_M_before_begin.i = getelementptr inbounds %"class.std::_Hashtable", %"class.std::_Hashtable"* %this, i64 0, i32 2
  %_M_nxt.i = getelementptr inbounds %"struct.std::__detail::_Hash_node_base", %"struct.std::__detail::_Hash_node_base"* %_M_before_begin.i, i64 0, i32 0
  store %"struct.std::__detail::_Hash_node_base"* null, %"struct.std::__detail::_Hash_node_base"** %_M_nxt.i, align 8, !tbaa !35
  %tobool.not47.i = icmp eq %"struct.std::__detail::_Hash_node"* %8, null
  br i1 %tobool.not47.i, label %while.end.i, label %while.body.i

while.body.i:                                     ; preds = %_ZNSt10_HashtableImSt4pairIKmSt6vectorI13GroupByColumnSaIS3_EEESaIS6_ENSt8__detail10_Select1stESt8equal_toImESt4hashImENS8_18_Mod_range_hashingENS8_20_Default_ranged_hashENS8_20_Prime_rehash_policyENS8_17_Hashtable_traitsILb0ELb0ELb1EEEE19_M_allocate_bucketsEm.exit.i, %if.end22.i
  %__p.049.i = phi %"struct.std::__detail::_Hash_node"* [ %10, %if.end22.i ], [ %8, %_ZNSt10_HashtableImSt4pairIKmSt6vectorI13GroupByColumnSaIS3_EEESaIS6_ENSt8__detail10_Select1stESt8equal_toImESt4hashImENS8_18_Mod_range_hashingENS8_20_Default_ranged_hashENS8_20_Prime_rehash_policyENS8_17_Hashtable_traitsILb0ELb0ELb1EEEE19_M_allocate_bucketsEm.exit.i ]
  %__bbegin_bkt.048.i = phi i64 [ %__bbegin_bkt.1.i, %if.end22.i ], [ 0, %_ZNSt10_HashtableImSt4pairIKmSt6vectorI13GroupByColumnSaIS3_EEESaIS6_ENSt8__detail10_Select1stESt8equal_toImESt4hashImENS8_18_Mod_range_hashingENS8_20_Default_ranged_hashENS8_20_Prime_rehash_policyENS8_17_Hashtable_traitsILb0ELb0ELb1EEEE19_M_allocate_bucketsEm.exit.i ]
  %9 = bitcast %"struct.std::__detail::_Hash_node"* %__p.049.i to %"struct.std::__detail::_Hash_node"**
  %10 = load %"struct.std::__detail::_Hash_node"*, %"struct.std::__detail::_Hash_node"** %9, align 8, !tbaa !13
  %_M_storage.i.i.i.i = getelementptr inbounds %"struct.std::__detail::_Hash_node", %"struct.std::__detail::_Hash_node"* %__p.049.i, i64 0, i32 0, i32 1
  %first.i.i.i.i.i = bitcast %"struct.__gnu_cxx::__aligned_buffer"* %_M_storage.i.i.i.i to i64*
  %11 = load i64, i64* %first.i.i.i.i.i, align 8, !tbaa !14
  %rem.i.i.i31 = urem i64 %11, %5
  %arrayidx.i = getelementptr inbounds %"struct.std::__detail::_Hash_node_base"*, %"struct.std::__detail::_Hash_node_base"** %retval.0.i.i, i64 %rem.i.i.i31
  %12 = load %"struct.std::__detail::_Hash_node_base"*, %"struct.std::__detail::_Hash_node_base"** %arrayidx.i, align 8, !tbaa !12
  %tobool5.not.i = icmp eq %"struct.std::__detail::_Hash_node_base"* %12, null
  br i1 %tobool5.not.i, label %if.then.i, label %if.else.i

if.then.i:                                        ; preds = %while.body.i
  %13 = load %"struct.std::__detail::_Hash_node_base"*, %"struct.std::__detail::_Hash_node_base"** %_M_nxt.i, align 8, !tbaa !35
  %14 = getelementptr %"struct.std::__detail::_Hash_node", %"struct.std::__detail::_Hash_node"* %__p.049.i, i64 0, i32 0, i32 0
  %_M_nxt8.i = getelementptr inbounds %"struct.std::__detail::_Hash_node", %"struct.std::__detail::_Hash_node"* %__p.049.i, i64 0, i32 0, i32 0, i32 0
  store %"struct.std::__detail::_Hash_node_base"* %13, %"struct.std::__detail::_Hash_node_base"** %_M_nxt8.i, align 8, !tbaa !13
  store %"struct.std::__detail::_Hash_node_base"* %14, %"struct.std::__detail::_Hash_node_base"** %_M_nxt.i, align 8, !tbaa !35
  store %"struct.std::__detail::_Hash_node_base"* %_M_before_begin.i, %"struct.std::__detail::_Hash_node_base"** %arrayidx.i, align 8, !tbaa !12
  %15 = load %"struct.std::__detail::_Hash_node_base"*, %"struct.std::__detail::_Hash_node_base"** %_M_nxt8.i, align 8, !tbaa !13
  %tobool14.not.i = icmp eq %"struct.std::__detail::_Hash_node_base"* %15, null
  br i1 %tobool14.not.i, label %if.end22.i, label %if.then15.i

if.then15.i:                                      ; preds = %if.then.i
  %arrayidx16.i = getelementptr inbounds %"struct.std::__detail::_Hash_node_base"*, %"struct.std::__detail::_Hash_node_base"** %retval.0.i.i, i64 %__bbegin_bkt.048.i
  store %"struct.std::__detail::_Hash_node_base"* %14, %"struct.std::__detail::_Hash_node_base"** %arrayidx16.i, align 8, !tbaa !12
  br label %if.end22.i

if.else.i:                                        ; preds = %while.body.i
  %_M_nxt18.i = getelementptr inbounds %"struct.std::__detail::_Hash_node_base", %"struct.std::__detail::_Hash_node_base"* %12, i64 0, i32 0
  %16 = load %"struct.std::__detail::_Hash_node_base"*, %"struct.std::__detail::_Hash_node_base"** %_M_nxt18.i, align 8, !tbaa !13
  %17 = getelementptr %"struct.std::__detail::_Hash_node", %"struct.std::__detail::_Hash_node"* %__p.049.i, i64 0, i32 0, i32 0
  %_M_nxt19.i = getelementptr inbounds %"struct.std::__detail::_Hash_node", %"struct.std::__detail::_Hash_node"* %__p.049.i, i64 0, i32 0, i32 0, i32 0
  store %"struct.std::__detail::_Hash_node_base"* %16, %"struct.std::__detail::_Hash_node_base"** %_M_nxt19.i, align 8, !tbaa !13
  %18 = load %"struct.std::__detail::_Hash_node_base"*, %"struct.std::__detail::_Hash_node_base"** %arrayidx.i, align 8, !tbaa !12
  %_M_nxt21.i = getelementptr inbounds %"struct.std::__detail::_Hash_node_base", %"struct.std::__detail::_Hash_node_base"* %18, i64 0, i32 0
  store %"struct.std::__detail::_Hash_node_base"* %17, %"struct.std::__detail::_Hash_node_base"** %_M_nxt21.i, align 8, !tbaa !13
  br label %if.end22.i

if.end22.i:                                       ; preds = %if.else.i, %if.then15.i, %if.then.i
  %__bbegin_bkt.1.i = phi i64 [ %__bbegin_bkt.048.i, %if.else.i ], [ %rem.i.i.i31, %if.then15.i ], [ %rem.i.i.i31, %if.then.i ]
  %tobool.not.i = icmp eq %"struct.std::__detail::_Hash_node"* %10, null
  br i1 %tobool.not.i, label %while.end.i, label %while.body.i, !llvm.loop !42

while.end.i:                                      ; preds = %if.end22.i, %_ZNSt10_HashtableImSt4pairIKmSt6vectorI13GroupByColumnSaIS3_EEESaIS6_ENSt8__detail10_Select1stESt8equal_toImESt4hashImENS8_18_Mod_range_hashingENS8_20_Default_ranged_hashENS8_20_Prime_rehash_policyENS8_17_Hashtable_traitsILb0ELb0ELb1EEEE19_M_allocate_bucketsEm.exit.i
  %_M_buckets.i.i = getelementptr inbounds %"class.std::_Hashtable", %"class.std::_Hashtable"* %this, i64 0, i32 0
  %19 = load %"struct.std::__detail::_Hash_node_base"**, %"struct.std::__detail::_Hash_node_base"*** %_M_buckets.i.i, align 8, !tbaa !11
  %_M_single_bucket.i.i.i.i = getelementptr inbounds %"class.std::_Hashtable", %"class.std::_Hashtable"* %this, i64 0, i32 5
  %cmp.i.i.i.i = icmp eq %"struct.std::__detail::_Hash_node_base"** %_M_single_bucket.i.i.i.i, %19
  br i1 %cmp.i.i.i.i, label %invoke.cont4, label %if.end.i.i.i

if.end.i.i.i:                                     ; preds = %while.end.i
  %20 = bitcast %"struct.std::__detail::_Hash_node_base"** %19 to i8*
  tail call void @_ZdlPv(i8* %20) #16
  br label %invoke.cont4

lpad.i:                                           ; preds = %_ZNSt8__detail16_Hashtable_allocISaINS_10_Hash_nodeISt4pairIKmSt6vectorI13GroupByColumnSaIS5_EEELb0EEEEE19_M_allocate_bucketsEm.exit.i.i, %if.then.i.i.i.i.i
  %21 = landingpad { i8*, i32 }
          catch i8* null
  %22 = extractvalue { i8*, i32 } %21, 0
  %23 = tail call i8* @__cxa_begin_catch(i8* %22) #16
  store i64 %0, i64* %_M_next_resize.i, align 8, !tbaa !38
  invoke void @__cxa_rethrow() #19
          to label %unreachable.i unwind label %lpad2.i

lpad2.i:                                          ; preds = %lpad.i
  %24 = landingpad { i8*, i32 }
          catch i8* null
  invoke void @__cxa_end_catch()
          to label %lpad.body unwind label %terminate.lpad.i

terminate.lpad.i:                                 ; preds = %lpad2.i
  %25 = landingpad { i8*, i32 }
          catch i8* null
  %26 = extractvalue { i8*, i32 } %25, 0
  tail call void @__clang_call_terminate(i8* %26) #18
  unreachable

unreachable.i:                                    ; preds = %lpad.i
  unreachable

invoke.cont4:                                     ; preds = %if.end.i.i.i, %while.end.i
  store i64 %5, i64* %_M_bucket_count, align 8, !tbaa !2
  store %"struct.std::__detail::_Hash_node_base"** %retval.0.i.i, %"struct.std::__detail::_Hash_node_base"*** %_M_buckets.i.i, align 8, !tbaa !11
  %rem.i.i.i = urem i64 %__code, %5
  br label %if.end

lpad.body:                                        ; preds = %lpad2.i
  %27 = extractvalue { i8*, i32 } %24, 0
  %28 = tail call i8* @__cxa_begin_catch(i8* %27) #16
  %_M_start.i.i.i.i.i.i = getelementptr inbounds %"struct.std::__detail::_Hash_node", %"struct.std::__detail::_Hash_node"* %__node, i64 0, i32 0, i32 1, i32 0, i32 0, i64 8
  %29 = bitcast i8* %_M_start.i.i.i.i.i.i to %struct.GroupByColumn**
  %30 = load %struct.GroupByColumn*, %struct.GroupByColumn** %29, align 8, !tbaa !17
  %tobool.not.i.i.i.i.i.i.i = icmp eq %struct.GroupByColumn* %30, null
  br i1 %tobool.not.i.i.i.i.i.i.i, label %invoke.cont14, label %if.then.i.i.i.i.i.i.i

if.then.i.i.i.i.i.i.i:                            ; preds = %lpad.body
  %31 = bitcast %struct.GroupByColumn* %30 to i8*
  tail call void @_ZdlPv(i8* nonnull %31) #16
  br label %invoke.cont14

invoke.cont14:                                    ; preds = %if.then.i.i.i.i.i.i.i, %lpad.body
  %32 = bitcast %"struct.std::__detail::_Hash_node"* %__node to i8*
  tail call void @_ZdlPv(i8* %32) #16
  invoke void @__cxa_rethrow() #19
          to label %unreachable unwind label %lpad13

if.end:                                           ; preds = %entry.if.end_crit_edge, %invoke.cont4
  %33 = phi %"struct.std::__detail::_Hash_node_base"** [ %.pre, %entry.if.end_crit_edge ], [ %retval.0.i.i, %invoke.cont4 ]
  %__bkt.addr.0 = phi i64 [ %__bkt, %entry.if.end_crit_edge ], [ %rem.i.i.i, %invoke.cont4 ]
  %_M_buckets.i = getelementptr inbounds %"class.std::_Hashtable", %"class.std::_Hashtable"* %this, i64 0, i32 0
  %arrayidx.i34 = getelementptr inbounds %"struct.std::__detail::_Hash_node_base"*, %"struct.std::__detail::_Hash_node_base"** %33, i64 %__bkt.addr.0
  %34 = load %"struct.std::__detail::_Hash_node_base"*, %"struct.std::__detail::_Hash_node_base"** %arrayidx.i34, align 8, !tbaa !12
  %tobool.not.i35 = icmp eq %"struct.std::__detail::_Hash_node_base"* %34, null
  br i1 %tobool.not.i35, label %if.else.i40, label %if.then.i37

if.then.i37:                                      ; preds = %if.end
  %_M_nxt.i36 = getelementptr inbounds %"struct.std::__detail::_Hash_node_base", %"struct.std::__detail::_Hash_node_base"* %34, i64 0, i32 0
  %35 = load %"struct.std::__detail::_Hash_node_base"*, %"struct.std::__detail::_Hash_node_base"** %_M_nxt.i36, align 8, !tbaa !13
  %36 = getelementptr %"struct.std::__detail::_Hash_node", %"struct.std::__detail::_Hash_node"* %__node, i64 0, i32 0, i32 0
  %_M_nxt4.i = getelementptr inbounds %"struct.std::__detail::_Hash_node", %"struct.std::__detail::_Hash_node"* %__node, i64 0, i32 0, i32 0, i32 0
  store %"struct.std::__detail::_Hash_node_base"* %35, %"struct.std::__detail::_Hash_node_base"** %_M_nxt4.i, align 8, !tbaa !13
  %37 = load %"struct.std::__detail::_Hash_node_base"*, %"struct.std::__detail::_Hash_node_base"** %arrayidx.i34, align 8, !tbaa !12
  %_M_nxt7.i = getelementptr inbounds %"struct.std::__detail::_Hash_node_base", %"struct.std::__detail::_Hash_node_base"* %37, i64 0, i32 0
  store %"struct.std::__detail::_Hash_node_base"* %36, %"struct.std::__detail::_Hash_node_base"** %_M_nxt7.i, align 8, !tbaa !13
  br label %_ZNSt10_HashtableImSt4pairIKmSt6vectorI13GroupByColumnSaIS3_EEESaIS6_ENSt8__detail10_Select1stESt8equal_toImESt4hashImENS8_18_Mod_range_hashingENS8_20_Default_ranged_hashENS8_20_Prime_rehash_policyENS8_17_Hashtable_traitsILb0ELb0ELb1EEEE22_M_insert_bucket_beginEmPNS8_10_Hash_nodeIS6_Lb0EEE.exit

if.else.i40:                                      ; preds = %if.end
  %_M_before_begin.i38 = getelementptr inbounds %"class.std::_Hashtable", %"class.std::_Hashtable"* %this, i64 0, i32 2
  %_M_nxt8.i39 = getelementptr inbounds %"struct.std::__detail::_Hash_node_base", %"struct.std::__detail::_Hash_node_base"* %_M_before_begin.i38, i64 0, i32 0
  %38 = load %"struct.std::__detail::_Hash_node_base"*, %"struct.std::__detail::_Hash_node_base"** %_M_nxt8.i39, align 8, !tbaa !35
  %39 = getelementptr %"struct.std::__detail::_Hash_node", %"struct.std::__detail::_Hash_node"* %__node, i64 0, i32 0, i32 0
  %_M_nxt9.i = getelementptr inbounds %"struct.std::__detail::_Hash_node", %"struct.std::__detail::_Hash_node"* %__node, i64 0, i32 0, i32 0, i32 0
  store %"struct.std::__detail::_Hash_node_base"* %38, %"struct.std::__detail::_Hash_node_base"** %_M_nxt9.i, align 8, !tbaa !13
  store %"struct.std::__detail::_Hash_node_base"* %39, %"struct.std::__detail::_Hash_node_base"** %_M_nxt8.i39, align 8, !tbaa !35
  %40 = load %"struct.std::__detail::_Hash_node_base"*, %"struct.std::__detail::_Hash_node_base"** %_M_nxt9.i, align 8, !tbaa !13
  %tobool13.not.i = icmp eq %"struct.std::__detail::_Hash_node_base"* %40, null
  br i1 %tobool13.not.i, label %if.end.i, label %if.then14.i

if.then14.i:                                      ; preds = %if.else.i40
  %41 = load i64, i64* %_M_bucket_count, align 8, !tbaa !2
  %_M_storage.i.i.i.i.i = getelementptr inbounds %"struct.std::__detail::_Hash_node_base", %"struct.std::__detail::_Hash_node_base"* %40, i64 1
  %first.i.i.i.i.i.i = bitcast %"struct.std::__detail::_Hash_node_base"* %_M_storage.i.i.i.i.i to i64*
  %42 = load i64, i64* %first.i.i.i.i.i.i, align 8, !tbaa !14
  %rem.i.i.i.i = urem i64 %42, %41
  %arrayidx17.i = getelementptr inbounds %"struct.std::__detail::_Hash_node_base"*, %"struct.std::__detail::_Hash_node_base"** %33, i64 %rem.i.i.i.i
  store %"struct.std::__detail::_Hash_node_base"* %39, %"struct.std::__detail::_Hash_node_base"** %arrayidx17.i, align 8, !tbaa !12
  %.pre.i = load %"struct.std::__detail::_Hash_node_base"**, %"struct.std::__detail::_Hash_node_base"*** %_M_buckets.i, align 8, !tbaa !11
  br label %if.end.i

if.end.i:                                         ; preds = %if.then14.i, %if.else.i40
  %43 = phi %"struct.std::__detail::_Hash_node_base"** [ %.pre.i, %if.then14.i ], [ %33, %if.else.i40 ]
  %arrayidx20.i = getelementptr inbounds %"struct.std::__detail::_Hash_node_base"*, %"struct.std::__detail::_Hash_node_base"** %43, i64 %__bkt.addr.0
  store %"struct.std::__detail::_Hash_node_base"* %_M_before_begin.i38, %"struct.std::__detail::_Hash_node_base"** %arrayidx20.i, align 8, !tbaa !12
  br label %_ZNSt10_HashtableImSt4pairIKmSt6vectorI13GroupByColumnSaIS3_EEESaIS6_ENSt8__detail10_Select1stESt8equal_toImESt4hashImENS8_18_Mod_range_hashingENS8_20_Default_ranged_hashENS8_20_Prime_rehash_policyENS8_17_Hashtable_traitsILb0ELb0ELb1EEEE22_M_insert_bucket_beginEmPNS8_10_Hash_nodeIS6_Lb0EEE.exit

_ZNSt10_HashtableImSt4pairIKmSt6vectorI13GroupByColumnSaIS3_EEESaIS6_ENSt8__detail10_Select1stESt8equal_toImESt4hashImENS8_18_Mod_range_hashingENS8_20_Default_ranged_hashENS8_20_Prime_rehash_policyENS8_17_Hashtable_traitsILb0ELb0ELb1EEEE22_M_insert_bucket_beginEmPNS8_10_Hash_nodeIS6_Lb0EEE.exit: ; preds = %if.then.i37, %if.end.i
  %44 = load i64, i64* %_M_element_count, align 8, !tbaa !39
  %inc = add i64 %44, 1
  store i64 %inc, i64* %_M_element_count, align 8, !tbaa !39
  ret %"struct.std::__detail::_Hash_node"* %__node

lpad13:                                           ; preds = %invoke.cont14
  %45 = landingpad { i8*, i32 }
          cleanup
  invoke void @__cxa_end_catch()
          to label %invoke.cont15 unwind label %terminate.lpad

invoke.cont15:                                    ; preds = %lpad13
  resume { i8*, i32 } %45

terminate.lpad:                                   ; preds = %lpad13
  %46 = landingpad { i8*, i32 }
          catch i8* null
  %47 = extractvalue { i8*, i32 } %46, 0
  tail call void @__clang_call_terminate(i8* %47) #18
  unreachable

unreachable:                                      ; preds = %invoke.cont14
  unreachable
}

declare dso_local { i8, i64 } @_ZNKSt8__detail20_Prime_rehash_policy14_M_need_rehashEmmm(%"struct.std::__detail::_Prime_rehash_policy"* nonnull dereferenceable(16), i64, i64, i64) local_unnamed_addr #0

; Function Attrs: uwtable
define internal void @_GLOBAL__sub_I_aggregator.cpp() #3 section ".text.startup" {
entry:
  tail call void @_ZNSt8ios_base4InitC1Ev(%"class.std::ios_base::Init"* nonnull dereferenceable(1) @_ZStL8__ioinit)
  %0 = tail call i32 @__cxa_atexit(void (i8*)* bitcast (void (%"class.std::ios_base::Init"*)* @_ZNSt8ios_base4InitD1Ev to void (i8*)*), i8* getelementptr inbounds (%"class.std::ios_base::Init", %"class.std::ios_base::Init"* @_ZStL8__ioinit, i64 0, i32 0), i8* nonnull @__dso_handle) #16
  ret void
}

; Function Attrs: nofree nounwind
declare noundef i32 @putchar(i32 noundef) local_unnamed_addr #2

attributes #0 = { "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "no-infs-fp-math"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #1 = { nounwind "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "no-infs-fp-math"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #2 = { nofree nounwind }
attributes #3 = { uwtable "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "min-legal-vector-width"="0" "no-infs-fp-math"="false" "no-jump-tables"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #4 = { argmemonly nofree nosync nounwind willreturn }
attributes #5 = { nobuiltin nofree allocsize(0) "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "no-infs-fp-math"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #6 = { nofree nounwind "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "no-infs-fp-math"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #7 = { nounwind uwtable "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "min-legal-vector-width"="0" "no-infs-fp-math"="false" "no-jump-tables"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #8 = { nounwind uwtable willreturn mustprogress "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "min-legal-vector-width"="0" "no-infs-fp-math"="false" "no-jump-tables"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #9 = { noreturn "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "no-infs-fp-math"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #10 = { noinline noreturn nounwind }
attributes #11 = { nobuiltin nounwind "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "no-infs-fp-math"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #12 = { argmemonly nofree nosync nounwind willreturn writeonly }
attributes #13 = { cold noreturn nounwind }
attributes #14 = { builtin allocsize(0) }
attributes #15 = { allocsize(0) }
attributes #16 = { nounwind }
attributes #17 = { builtin nounwind }
attributes #18 = { noreturn nounwind }
attributes #19 = { noreturn }

!llvm.module.flags = !{!0}
!llvm.ident = !{!1}

!0 = !{i32 1, !"wchar_size", i32 4}
!1 = !{!"Ubuntu clang version 12.0.0-++20210225092633+e0e6b1e39e7e-1~exp1~20210225083352.50"}
!2 = !{!3, !7, i64 8}
!3 = !{!"_ZTSSt10_HashtableImSt4pairIKmSt6vectorI13GroupByColumnSaIS3_EEESaIS6_ENSt8__detail10_Select1stESt8equal_toImESt4hashImENS8_18_Mod_range_hashingENS8_20_Default_ranged_hashENS8_20_Prime_rehash_policyENS8_17_Hashtable_traitsILb0ELb0ELb1EEEE", !4, i64 0, !7, i64 8, !8, i64 16, !7, i64 24, !9, i64 32, !4, i64 48}
!4 = !{!"any pointer", !5, i64 0}
!5 = !{!"omnipotent char", !6, i64 0}
!6 = !{!"Simple C++ TBAA"}
!7 = !{!"long", !5, i64 0}
!8 = !{!"_ZTSNSt8__detail15_Hash_node_baseE", !4, i64 0}
!9 = !{!"_ZTSNSt8__detail20_Prime_rehash_policyE", !10, i64 0, !7, i64 8}
!10 = !{!"float", !5, i64 0}
!11 = !{!3, !4, i64 0}
!12 = !{!4, !4, i64 0}
!13 = !{!8, !4, i64 0}
!14 = !{!7, !7, i64 0}
!15 = !{!16, !16, i64 0}
!16 = !{!"int", !5, i64 0}
!17 = !{!18, !4, i64 0}
!18 = !{!"_ZTSSt12_Vector_baseI13GroupByColumnSaIS0_EE", !19, i64 0}
!19 = !{!"_ZTSNSt12_Vector_baseI13GroupByColumnSaIS0_EE12_Vector_implE", !4, i64 0, !4, i64 8, !4, i64 16}
!20 = !{!21, !4, i64 8}
!21 = !{!"_ZTS13GroupByColumn", !22, i64 0, !4, i64 8}
!22 = !{!"_ZTSN3opt10ColumnTypeE", !5, i64 0}
!23 = !{i64 0, i64 4, !24, i64 8, i64 8, !12}
!24 = !{!22, !22, i64 0}
!25 = !{i64 0, i64 8, !12}
!26 = !{!27, !7, i64 0}
!27 = !{!"_ZTSSt4pairIKmSt6vectorI13GroupByColumnSaIS2_EEE", !7, i64 0, !28, i64 8}
!28 = !{!"_ZTSSt6vectorI13GroupByColumnSaIS0_EE"}
!29 = !{!18, !4, i64 16}
!30 = !{!18, !4, i64 8}
!31 = !{!32, !32, i64 0}
!32 = !{!"double", !5, i64 0}
!33 = !{!34, !34, i64 0}
!34 = !{!"vtable pointer", !6, i64 0}
!35 = !{!3, !4, i64 16}
!36 = distinct !{!36, !37}
!37 = !{!"llvm.loop.mustprogress"}
!38 = !{!9, !7, i64 8}
!39 = !{!3, !7, i64 24}
!40 = !{!"branch_weights", i32 1, i32 2000}
!41 = !{!3, !4, i64 48}
!42 = distinct !{!42, !37}
