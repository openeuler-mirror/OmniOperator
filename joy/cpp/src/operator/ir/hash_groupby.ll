; ModuleID = '/home/omni-cache/joy/cpp/src/operator/ir/../hash_groupby.cpp'
source_filename = "/home/omni-cache/joy/cpp/src/operator/ir/../hash_groupby.cpp"
target datalayout = "e-m:e-p270:32:32-p271:32:32-p272:64:64-i64:64-f80:128-n8:16:32:64-S128"
target triple = "x86_64-pc-linux-gnu"

%"class.std::ios_base::Init" = type { i8 }
%"struct.std::chrono::duration" = type { float }
%class.HashGroupBy = type { %class.OpTemplate, %"class.std::vector", %"class.std::vector", %"class.std::vector.0", %"class.std::unordered_map", i32* }
%class.OpTemplate = type { i32 (...)** }
%"class.std::vector" = type { %"struct.std::_Vector_base" }
%"struct.std::_Vector_base" = type { %"struct.std::_Vector_base<ColumnIndex, std::allocator<ColumnIndex>>::_Vector_impl" }
%"struct.std::_Vector_base<ColumnIndex, std::allocator<ColumnIndex>>::_Vector_impl" = type { %struct.ColumnIndex*, %struct.ColumnIndex*, %struct.ColumnIndex* }
%struct.ColumnIndex = type { i32, i32 }
%"class.std::vector.0" = type { %"struct.std::_Vector_base.1" }
%"struct.std::_Vector_base.1" = type { %"struct.std::_Vector_base<Aggregator *, std::allocator<Aggregator *>>::_Vector_impl" }
%"struct.std::_Vector_base<Aggregator *, std::allocator<Aggregator *>>::_Vector_impl" = type { %class.Aggregator**, %class.Aggregator**, %class.Aggregator** }
%class.Aggregator = type { i32 (...)**, i32, i32, %"class.std::unordered_map" }
%"class.std::unordered_map" = type { %"class.std::_Hashtable" }
%"class.std::_Hashtable" = type { %"struct.std::__detail::_Hash_node_base"**, i64, %"struct.std::__detail::_Hash_node_base", i64, %"struct.std::__detail::_Prime_rehash_policy", %"struct.std::__detail::_Hash_node_base"* }
%"struct.std::__detail::_Hash_node_base" = type { %"struct.std::__detail::_Hash_node_base"* }
%"struct.std::__detail::_Prime_rehash_policy" = type { float, i64 }
%class.Table = type <{ i32 (...)**, %class.Layout, [7 x i8], %"class.std::vector.12", i32*, i32, i32, i32, [4 x i8] }>
%class.Layout = type { i8 }
%"class.std::vector.12" = type { %"struct.std::_Vector_base.13" }
%"struct.std::_Vector_base.13" = type { %"struct.std::_Vector_base<Column *, std::allocator<Column *>>::_Vector_impl" }
%"struct.std::_Vector_base<Column *, std::allocator<Column *>>::_Vector_impl" = type { %class.Column**, %class.Column**, %class.Column** }
%class.Column = type { i32 (...)**, i8*, i32*, i32, i64 }
%"struct.std::pair.28" = type { i64, %"class.std::vector.17" }
%"class.std::vector.17" = type { %"struct.std::_Vector_base.18" }
%"struct.std::_Vector_base.18" = type { %"struct.std::_Vector_base<GroupByColumn, std::allocator<GroupByColumn>>::_Vector_impl" }
%"struct.std::_Vector_base<GroupByColumn, std::allocator<GroupByColumn>>::_Vector_impl" = type { %struct.GroupByColumn*, %struct.GroupByColumn*, %struct.GroupByColumn* }
%struct.GroupByColumn = type { i32, i8* }
%"struct.std::__detail::_Hash_node" = type { %"struct.std::__detail::_Hash_node_value_base" }
%"struct.std::__detail::_Hash_node_value_base" = type { %"struct.std::__detail::_Hash_node_base", %"struct.__gnu_cxx::__aligned_buffer" }
%"struct.__gnu_cxx::__aligned_buffer" = type { %"union.std::aligned_storage<32, 8>::type" }
%"union.std::aligned_storage<32, 8>::type" = type { [32 x i8] }
%class.SumAggregator = type { %class.Aggregator }
%struct.Iterator = type { %"struct.std::__detail::_Node_iterator", %"class.std::vector.33" }
%"struct.std::__detail::_Node_iterator" = type { %"struct.std::__detail::_Node_iterator_base" }
%"struct.std::__detail::_Node_iterator_base" = type { %"struct.std::__detail::_Hash_node"* }
%"class.std::vector.33" = type { %"struct.std::_Vector_base.34" }
%"struct.std::_Vector_base.34" = type { %"struct.std::_Vector_base<std::__detail::_Node_iterator<std::pair<const unsigned long, std::vector<GroupByColumn>>, false, false>, std::allocator<std::__detail::_Node_iterator<std::pair<const unsigned long, std::vector<GroupByColumn>>, false, false>>>::_Vector_impl" }
%"struct.std::_Vector_base<std::__detail::_Node_iterator<std::pair<const unsigned long, std::vector<GroupByColumn>>, false, false>, std::allocator<std::__detail::_Node_iterator<std::pair<const unsigned long, std::vector<GroupByColumn>>, false, false>>>::_Vector_impl" = type { %"struct.std::__detail::_Node_iterator"*, %"struct.std::__detail::_Node_iterator"*, %"struct.std::__detail::_Node_iterator"* }
%"class.std::vector.38" = type { %"struct.std::_Vector_base.39" }
%"struct.std::_Vector_base.39" = type { %"struct.std::_Vector_base<Table *, std::allocator<Table *>>::_Vector_impl" }
%"struct.std::_Vector_base<Table *, std::allocator<Table *>>::_Vector_impl" = type { %class.Table**, %class.Table**, %class.Table** }
%class.anon = type { %"class.std::_Hashtable"* }
%"struct.std::__detail::_Hashtable_alloc" = type { i8 }

$_ZN11HashGroupByC2ESt6vectorI11ColumnIndexSaIS1_EES3_S0_IP10AggregatorSaIS5_EE = comdat any

$_ZN11HashGroupBy9getResultEv = comdat any

$_ZN11HashGroupByD2Ev = comdat any

$_ZN11HashGroupByD0Ev = comdat any

$__clang_call_terminate = comdat any

$_ZN6ColumnD2Ev = comdat any

$_ZN6ColumnD0Ev = comdat any

$_ZN5TableD2Ev = comdat any

$_ZN5TableD0Ev = comdat any

$_ZNSt10_HashtableImSt4pairIKmSt6vectorI13GroupByColumnSaIS3_EEESaIS6_ENSt8__detail10_Select1stESt8equal_toImESt4hashImENS8_18_Mod_range_hashingENS8_20_Default_ranged_hashENS8_20_Prime_rehash_policyENS8_17_Hashtable_traitsILb0ELb0ELb1EEEE9_M_assignIZNSJ_C1ERKSJ_EUlPKNS8_10_Hash_nodeIS6_Lb0EEEE_EEvSM_RKT_ = comdat any

$_ZNSt8__detail16_Hashtable_allocISaINS_10_Hash_nodeISt4pairIKmSt6vectorI13GroupByColumnSaIS5_EEELb0EEEEE16_M_allocate_nodeIJRKS8_EEEPS9_DpOT_ = comdat any

$_ZNSt10_HashtableImSt4pairIKmSt6vectorI13GroupByColumnSaIS3_EEESaIS6_ENSt8__detail10_Select1stESt8equal_toImESt4hashImENS8_18_Mod_range_hashingENS8_20_Default_ranged_hashENS8_20_Prime_rehash_policyENS8_17_Hashtable_traitsILb0ELb0ELb1EEEE10_M_emplaceIJS6_EEES0_INS8_14_Node_iteratorIS6_Lb0ELb0EEEbESt17integral_constantIbLb1EEDpOT_ = comdat any

$_ZNSt10_HashtableImSt4pairIKmSt6vectorI13GroupByColumnSaIS3_EEESaIS6_ENSt8__detail10_Select1stESt8equal_toImESt4hashImENS8_18_Mod_range_hashingENS8_20_Default_ranged_hashENS8_20_Prime_rehash_policyENS8_17_Hashtable_traitsILb0ELb0ELb1EEEE21_M_insert_unique_nodeEmmPNS8_10_Hash_nodeIS6_Lb0EEE = comdat any

$_ZTS10OpTemplate = comdat any

$_ZTI10OpTemplate = comdat any

$_ZTV6Column = comdat any

$_ZTS6Column = comdat any

$_ZTI6Column = comdat any

$_ZTV5Table = comdat any

$_ZTS5Table = comdat any

$_ZTI5Table = comdat any

@_ZStL8__ioinit = internal global %"class.std::ios_base::Init" zeroinitializer, align 1
@__dso_handle = external hidden global i8
@.str = private unnamed_addr constant [29 x i8] c"[%s][%s][%d]:No such type %d\00", align 1
@.str.1 = private unnamed_addr constant [61 x i8] c"/home/omni-cache/joy/cpp/src/operator/ir/../hash_groupby.cpp\00", align 1
@__FUNCTION__.processAgg = private unnamed_addr constant [11 x i8] c"processAgg\00", align 1
@.str.3 = private unnamed_addr constant [35 x i8] c"[%s][%s][%d]:No such aggregator %d\00", align 1
@g_total_execute_time = dso_local local_unnamed_addr global %"struct.std::chrono::duration" zeroinitializer, align 4
@_ZTV11HashGroupBy = dso_local unnamed_addr constant { [9 x i8*] } { [9 x i8*] [i8* null, i8* bitcast ({ i8*, i8*, i8* }* @_ZTI11HashGroupBy to i8*), i8* bitcast (void (%class.HashGroupBy*, %class.Table*)* @_ZN11HashGroupBy7preloopEP5Table to i8*), i8* bitcast (void (%class.HashGroupBy*, %class.Table*, i32)* @_ZN11HashGroupBy6inloopEP5Tablej to i8*), i8* bitcast (void (%class.HashGroupBy*, %class.Table*)* @_ZN11HashGroupBy8postloopEP5Table to i8*), i8* bitcast (void (%class.HashGroupBy*, %class.Table*, i32)* @_ZN11HashGroupBy7processEP5Tablej to i8*), i8* bitcast (%class.Table* (%class.HashGroupBy*)* @_ZN11HashGroupBy9getResultEv to i8*), i8* bitcast (void (%class.HashGroupBy*)* @_ZN11HashGroupByD2Ev to i8*), i8* bitcast (void (%class.HashGroupBy*)* @_ZN11HashGroupByD0Ev to i8*)] }, align 8
@_ZTVN10__cxxabiv120__si_class_type_infoE = external dso_local global i8*
@_ZTS11HashGroupBy = dso_local constant [14 x i8] c"11HashGroupBy\00", align 1
@_ZTVN10__cxxabiv117__class_type_infoE = external dso_local global i8*
@_ZTS10OpTemplate = linkonce_odr dso_local constant [13 x i8] c"10OpTemplate\00", comdat, align 1
@_ZTI10OpTemplate = linkonce_odr dso_local constant { i8*, i8* } { i8* bitcast (i8** getelementptr inbounds (i8*, i8** @_ZTVN10__cxxabiv117__class_type_infoE, i64 2) to i8*), i8* getelementptr inbounds ([13 x i8], [13 x i8]* @_ZTS10OpTemplate, i32 0, i32 0) }, comdat, align 8
@_ZTI11HashGroupBy = dso_local constant { i8*, i8*, i8* } { i8* bitcast (i8** getelementptr inbounds (i8*, i8** @_ZTVN10__cxxabiv120__si_class_type_infoE, i64 2) to i8*), i8* getelementptr inbounds ([14 x i8], [14 x i8]* @_ZTS11HashGroupBy, i32 0, i32 0), i8* bitcast ({ i8*, i8* }* @_ZTI10OpTemplate to i8*) }, align 8
@_ZTV6Column = linkonce_odr dso_local unnamed_addr constant { [4 x i8*] } { [4 x i8*] [i8* null, i8* bitcast ({ i8*, i8* }* @_ZTI6Column to i8*), i8* bitcast (void (%class.Column*)* @_ZN6ColumnD2Ev to i8*), i8* bitcast (void (%class.Column*)* @_ZN6ColumnD0Ev to i8*)] }, comdat, align 8
@_ZTS6Column = linkonce_odr dso_local constant [8 x i8] c"6Column\00", comdat, align 1
@_ZTI6Column = linkonce_odr dso_local constant { i8*, i8* } { i8* bitcast (i8** getelementptr inbounds (i8*, i8** @_ZTVN10__cxxabiv117__class_type_infoE, i64 2) to i8*), i8* getelementptr inbounds ([8 x i8], [8 x i8]* @_ZTS6Column, i32 0, i32 0) }, comdat, align 8
@_ZTV5Table = linkonce_odr dso_local unnamed_addr constant { [4 x i8*] } { [4 x i8*] [i8* null, i8* bitcast ({ i8*, i8* }* @_ZTI5Table to i8*), i8* bitcast (void (%class.Table*)* @_ZN5TableD2Ev to i8*), i8* bitcast (void (%class.Table*)* @_ZN5TableD0Ev to i8*)] }, comdat, align 8
@_ZTS5Table = linkonce_odr dso_local constant [7 x i8] c"5Table\00", comdat, align 1
@_ZTI5Table = linkonce_odr dso_local constant { i8*, i8* } { i8* bitcast (i8** getelementptr inbounds (i8*, i8** @_ZTVN10__cxxabiv117__class_type_infoE, i64 2) to i8*), i8* getelementptr inbounds ([7 x i8], [7 x i8]* @_ZTS5Table, i32 0, i32 0) }, comdat, align 8
@llvm.global_ctors = appending global [1 x { i32, void ()*, i8* }] [{ i32, void ()*, i8* } { i32 65535, void ()* @_GLOBAL__sub_I_hash_groupby.cpp, i8* null }]

declare dso_local void @_ZNSt8ios_base4InitC1Ev(%"class.std::ios_base::Init"* nonnull dereferenceable(1)) unnamed_addr #0

; Function Attrs: nounwind
declare dso_local void @_ZNSt8ios_base4InitD1Ev(%"class.std::ios_base::Init"* nonnull dereferenceable(1)) unnamed_addr #1

; Function Attrs: nofree nounwind
declare dso_local i32 @__cxa_atexit(void (i8*)*, i8*, i8*) local_unnamed_addr #2

; Function Attrs: nofree uwtable mustprogress
define dso_local void @_ZN11HashGroupBy7preloopEP5Table(%class.HashGroupBy* nocapture nonnull dereferenceable(144) %this, %class.Table* nocapture readnone %table) unnamed_addr #3 align 2 {
entry:
  %_M_finish.i = getelementptr inbounds %class.HashGroupBy, %class.HashGroupBy* %this, i64 0, i32 1, i32 0, i32 0, i32 1
  %0 = load %struct.ColumnIndex*, %struct.ColumnIndex** %_M_finish.i, align 8, !tbaa !2
  %_M_start.i = getelementptr inbounds %class.HashGroupBy, %class.HashGroupBy* %this, i64 0, i32 1, i32 0, i32 0, i32 0
  %1 = load %struct.ColumnIndex*, %struct.ColumnIndex** %_M_start.i, align 8, !tbaa !8
  %sub.ptr.lhs.cast.i = ptrtoint %struct.ColumnIndex* %0 to i64
  %sub.ptr.rhs.cast.i = ptrtoint %struct.ColumnIndex* %1 to i64
  %sub.ptr.sub.i = sub i64 %sub.ptr.lhs.cast.i, %sub.ptr.rhs.cast.i
  %sub.ptr.div.i = ashr exact i64 %sub.ptr.sub.i, 3
  %_M_finish.i4 = getelementptr inbounds %class.HashGroupBy, %class.HashGroupBy* %this, i64 0, i32 2, i32 0, i32 0, i32 1
  %2 = load %struct.ColumnIndex*, %struct.ColumnIndex** %_M_finish.i4, align 8, !tbaa !2
  %_M_start.i5 = getelementptr inbounds %class.HashGroupBy, %class.HashGroupBy* %this, i64 0, i32 2, i32 0, i32 0, i32 0
  %3 = load %struct.ColumnIndex*, %struct.ColumnIndex** %_M_start.i5, align 8, !tbaa !8
  %sub.ptr.lhs.cast.i6 = ptrtoint %struct.ColumnIndex* %2 to i64
  %sub.ptr.rhs.cast.i7 = ptrtoint %struct.ColumnIndex* %3 to i64
  %sub.ptr.sub.i8 = sub i64 %sub.ptr.lhs.cast.i6, %sub.ptr.rhs.cast.i7
  %sub.ptr.div.i9 = ashr exact i64 %sub.ptr.sub.i8, 3
  %add = add nsw i64 %sub.ptr.div.i9, %sub.ptr.div.i
  %4 = tail call { i64, i1 } @llvm.umul.with.overflow.i64(i64 %add, i64 4)
  %5 = extractvalue { i64, i1 } %4, 1
  %6 = extractvalue { i64, i1 } %4, 0
  %7 = select i1 %5, i64 -1, i64 %6
  %call3 = tail call noalias nonnull i8* @_Znam(i64 %7) #18
  %inputColTypes = getelementptr inbounds %class.HashGroupBy, %class.HashGroupBy* %this, i64 0, i32 5
  %8 = bitcast i32** %inputColTypes to i8**
  store i8* %call3, i8** %8, align 8, !tbaa !9
  ret void
}

; Function Attrs: nofree nosync nounwind readnone speculatable willreturn
declare { i64, i1 } @llvm.umul.with.overflow.i64(i64, i64) #4

; Function Attrs: nobuiltin nofree allocsize(0)
declare dso_local nonnull i8* @_Znam(i64) local_unnamed_addr #5

; Function Attrs: uwtable
define dso_local void @_ZN11HashGroupBy6inloopEP5Tablej(%class.HashGroupBy* nonnull dereferenceable(144) %this, %class.Table* nocapture readonly %table, i32 %rowIndex) unnamed_addr #6 align 2 personality i8* bitcast (i32 (...)* @__gxx_personality_v0 to i8*) {
entry:
  %__val.addr.i = alloca double, align 8
  %ref.tmp74 = alloca %"struct.std::pair.28", align 8
  %_M_start.i = getelementptr inbounds %class.HashGroupBy, %class.HashGroupBy* %this, i64 0, i32 1, i32 0, i32 0, i32 0
  %0 = load %struct.ColumnIndex*, %struct.ColumnIndex** %_M_start.i, align 8, !tbaa !19
  %_M_finish.i195 = getelementptr inbounds %class.HashGroupBy, %class.HashGroupBy* %this, i64 0, i32 1, i32 0, i32 0, i32 1
  %1 = load %struct.ColumnIndex*, %struct.ColumnIndex** %_M_finish.i195, align 8, !tbaa !19
  %cmp.i.not313 = icmp eq %struct.ColumnIndex* %0, %1
  br i1 %cmp.i.not313, label %for.cond.cleanup, label %for.body.lr.ph

for.body.lr.ph:                                   ; preds = %entry
  %_M_start.i.i212 = getelementptr inbounds %class.Table, %class.Table* %table, i64 0, i32 3, i32 0, i32 0, i32 0
  %idx.ext10.i231 = zext i32 %rowIndex to i64
  %2 = bitcast double* %__val.addr.i to i8*
  br label %for.body

for.cond.cleanup.loopexit:                        ; preds = %sw.epilog
  %phi.cast = ptrtoint %struct.GroupByColumn* %groupByTuple.sroa.11.1 to i64
  br label %for.cond.cleanup

for.cond.cleanup:                                 ; preds = %for.cond.cleanup.loopexit, %entry
  %groupByTuple.sroa.11.0.lcssa = phi i64 [ 0, %entry ], [ %phi.cast, %for.cond.cleanup.loopexit ]
  %groupByTuple.sroa.0.0.lcssa = phi %struct.GroupByColumn* [ null, %entry ], [ %groupByTuple.sroa.0.1, %for.cond.cleanup.loopexit ]
  %combinedHash.0.lcssa = phi i64 [ 0, %entry ], [ %add.i, %for.cond.cleanup.loopexit ]
  %groupedRows = getelementptr inbounds %class.HashGroupBy, %class.HashGroupBy* %this, i64 0, i32 4
  %_M_bucket_count.i.i.i = getelementptr inbounds %class.HashGroupBy, %class.HashGroupBy* %this, i64 0, i32 4, i32 0, i32 1
  %3 = load i64, i64* %_M_bucket_count.i.i.i, align 8, !tbaa !20
  %rem.i.i.i.i.i = urem i64 %combinedHash.0.lcssa, %3
  %_M_buckets.i.i.i.i = getelementptr inbounds %"class.std::unordered_map", %"class.std::unordered_map"* %groupedRows, i64 0, i32 0, i32 0
  %4 = load %"struct.std::__detail::_Hash_node_base"**, %"struct.std::__detail::_Hash_node_base"*** %_M_buckets.i.i.i.i, align 8, !tbaa !21
  %arrayidx.i.i.i.i = getelementptr inbounds %"struct.std::__detail::_Hash_node_base"*, %"struct.std::__detail::_Hash_node_base"** %4, i64 %rem.i.i.i.i.i
  %5 = load %"struct.std::__detail::_Hash_node_base"*, %"struct.std::__detail::_Hash_node_base"** %arrayidx.i.i.i.i, align 8, !tbaa !19
  %tobool.not.i.i.i.i206 = icmp eq %"struct.std::__detail::_Hash_node_base"* %5, null
  br i1 %tobool.not.i.i.i.i206, label %if.then, label %if.end.i.i.i.i

if.end.i.i.i.i:                                   ; preds = %for.cond.cleanup
  %6 = bitcast %"struct.std::__detail::_Hash_node_base"* %5 to %"struct.std::__detail::_Hash_node"**
  %7 = load %"struct.std::__detail::_Hash_node"*, %"struct.std::__detail::_Hash_node"** %6, align 8, !tbaa !22
  %_M_storage.i.i.i.i23.i.i.i.i = getelementptr inbounds %"struct.std::__detail::_Hash_node", %"struct.std::__detail::_Hash_node"* %7, i64 0, i32 0, i32 1
  %first.i.i.i.i.i24.i.i.i.i = bitcast %"struct.__gnu_cxx::__aligned_buffer"* %_M_storage.i.i.i.i23.i.i.i.i to i64*
  %8 = load i64, i64* %first.i.i.i.i.i24.i.i.i.i, align 8, !tbaa !23
  %cmp.i.i.i25.i.i.i.i = icmp eq i64 %combinedHash.0.lcssa, %8
  br i1 %cmp.i.i.i25.i.i.i.i, label %if.end, label %if.end3.i.i.i.i

for.cond.i.i.i.i:                                 ; preds = %lor.lhs.false.i.i.i.i
  %cmp.i.i.i.i.i.i.i = icmp eq i64 %combinedHash.0.lcssa, %11
  br i1 %cmp.i.i.i.i.i.i.i, label %if.end, label %if.end3.i.i.i.i

if.end3.i.i.i.i:                                  ; preds = %if.end.i.i.i.i, %for.cond.i.i.i.i
  %__p.026.i.i.i.i = phi %"struct.std::__detail::_Hash_node"* [ %10, %for.cond.i.i.i.i ], [ %7, %if.end.i.i.i.i ]
  %_M_nxt4.i.i.i.i = getelementptr inbounds %"struct.std::__detail::_Hash_node", %"struct.std::__detail::_Hash_node"* %__p.026.i.i.i.i, i64 0, i32 0, i32 0, i32 0
  %9 = load %"struct.std::__detail::_Hash_node_base"*, %"struct.std::__detail::_Hash_node_base"** %_M_nxt4.i.i.i.i, align 8, !tbaa !22
  %tobool5.not.i.i.i.i = icmp eq %"struct.std::__detail::_Hash_node_base"* %9, null
  %10 = bitcast %"struct.std::__detail::_Hash_node_base"* %9 to %"struct.std::__detail::_Hash_node"*
  br i1 %tobool5.not.i.i.i.i, label %if.then, label %lor.lhs.false.i.i.i.i

lor.lhs.false.i.i.i.i:                            ; preds = %if.end3.i.i.i.i
  %_M_storage.i.i.i.i21.i.i.i.i = getelementptr inbounds %"struct.std::__detail::_Hash_node_base", %"struct.std::__detail::_Hash_node_base"* %9, i64 1
  %first.i.i.i.i.i22.i.i.i.i = bitcast %"struct.std::__detail::_Hash_node_base"* %_M_storage.i.i.i.i21.i.i.i.i to i64*
  %11 = load i64, i64* %first.i.i.i.i.i22.i.i.i.i, align 8, !tbaa !23
  %rem.i.i.i.i.i.i.i = urem i64 %11, %3
  %cmp.not.i.i.i.i = icmp eq i64 %rem.i.i.i.i.i.i.i, %rem.i.i.i.i.i
  br i1 %cmp.not.i.i.i.i, label %for.cond.i.i.i.i, label %if.then

for.body:                                         ; preds = %for.body.lr.ph, %sw.epilog
  %combinedHash.0319 = phi i64 [ 0, %for.body.lr.ph ], [ %add.i, %sw.epilog ]
  %groupByTuple.sroa.0.0317 = phi %struct.GroupByColumn* [ null, %for.body.lr.ph ], [ %groupByTuple.sroa.0.1, %sw.epilog ]
  %groupByTuple.sroa.11.0316 = phi %struct.GroupByColumn* [ null, %for.body.lr.ph ], [ %groupByTuple.sroa.11.1, %sw.epilog ]
  %groupByTuple.sroa.18.0315 = phi %struct.GroupByColumn* [ null, %for.body.lr.ph ], [ %groupByTuple.sroa.18.1, %sw.epilog ]
  %__begin1.sroa.0.0314 = phi %struct.ColumnIndex* [ %0, %for.body.lr.ph ], [ %incdec.ptr.i, %sw.epilog ]
  %c.sroa.0.0..sroa_idx = getelementptr inbounds %struct.ColumnIndex, %struct.ColumnIndex* %__begin1.sroa.0.0314, i64 0, i32 0
  %c.sroa.0.0.copyload = load i32, i32* %c.sroa.0.0..sroa_idx, align 4, !tbaa.struct !24
  %conv.i211 = zext i32 %c.sroa.0.0.copyload to i64
  %12 = load %class.Column**, %class.Column*** %_M_start.i.i212, align 8, !tbaa !29
  %add.ptr.i.i213 = getelementptr inbounds %class.Column*, %class.Column** %12, i64 %conv.i211
  %13 = load %class.Column*, %class.Column** %add.ptr.i.i213, align 8, !tbaa !19
  %type.i214 = getelementptr inbounds %class.Column, %class.Column* %13, i64 0, i32 3
  %14 = load i32, i32* %type.i214, align 8, !tbaa !32
  switch i32 %14, label %_ZN6Column8getValueEj.exit235 [
    i32 1, label %sw.bb.i225
    i32 2, label %sw.bb2.i229
    i32 3, label %sw.bb7.i233
  ]

sw.bb.i225:                                       ; preds = %for.body
  %data.i222 = getelementptr inbounds %class.Column, %class.Column* %13, i64 0, i32 1
  %15 = bitcast i8** %data.i222 to i32**
  %16 = load i32*, i32** %15, align 8, !tbaa !34
  %add.ptr.i224 = getelementptr inbounds i32, i32* %16, i64 %idx.ext10.i231
  %17 = bitcast i32* %add.ptr.i224 to i8*
  br label %_ZN6Column8getValueEj.exit235

sw.bb2.i229:                                      ; preds = %for.body
  %data4.i226 = getelementptr inbounds %class.Column, %class.Column* %13, i64 0, i32 1
  %18 = bitcast i8** %data4.i226 to i64**
  %19 = load i64*, i64** %18, align 8, !tbaa !34
  %add.ptr6.i228 = getelementptr inbounds i64, i64* %19, i64 %idx.ext10.i231
  %20 = bitcast i64* %add.ptr6.i228 to i8*
  br label %_ZN6Column8getValueEj.exit235

sw.bb7.i233:                                      ; preds = %for.body
  %data9.i230 = getelementptr inbounds %class.Column, %class.Column* %13, i64 0, i32 1
  %21 = bitcast i8** %data9.i230 to double**
  %22 = load double*, double** %21, align 8, !tbaa !34
  %add.ptr11.i232 = getelementptr inbounds double, double* %22, i64 %idx.ext10.i231
  %23 = bitcast double* %add.ptr11.i232 to i8*
  br label %_ZN6Column8getValueEj.exit235

_ZN6Column8getValueEj.exit235:                    ; preds = %for.body, %sw.bb.i225, %sw.bb2.i229, %sw.bb7.i233
  %res.0.i234 = phi i8* [ null, %for.body ], [ %23, %sw.bb7.i233 ], [ %20, %sw.bb2.i229 ], [ %17, %sw.bb.i225 ]
  %cmp.not.i = icmp eq %struct.GroupByColumn* %groupByTuple.sroa.11.0316, %groupByTuple.sroa.18.0315
  br i1 %cmp.not.i, label %if.else.i, label %if.then.i

if.then.i:                                        ; preds = %_ZN6Column8getValueEj.exit235
  %groupCol.sroa.0.0..sroa_idx = getelementptr inbounds %struct.GroupByColumn, %struct.GroupByColumn* %groupByTuple.sroa.11.0316, i64 0, i32 0
  store i32 %14, i32* %groupCol.sroa.0.0..sroa_idx, align 8, !tbaa.struct !35
  %groupCol.sroa.6254.0..sroa_idx257 = getelementptr inbounds %struct.GroupByColumn, %struct.GroupByColumn* %groupByTuple.sroa.11.0316, i64 0, i32 1
  store i8* %res.0.i234, i8** %groupCol.sroa.6254.0..sroa_idx257, align 8, !tbaa.struct !36
  br label %invoke.cont17

if.else.i:                                        ; preds = %_ZN6Column8getValueEj.exit235
  %sub.ptr.lhs.cast.i28.i.i.i = ptrtoint %struct.GroupByColumn* %groupByTuple.sroa.11.0316 to i64
  %sub.ptr.rhs.cast.i29.i.i.i = ptrtoint %struct.GroupByColumn* %groupByTuple.sroa.0.0317 to i64
  %sub.ptr.sub.i30.i.i.i = sub i64 %sub.ptr.lhs.cast.i28.i.i.i, %sub.ptr.rhs.cast.i29.i.i.i
  %sub.ptr.div.i31.i.i.i = ashr exact i64 %sub.ptr.sub.i30.i.i.i, 4
  %cmp.i.i.i.i = icmp eq i64 %sub.ptr.sub.i30.i.i.i, 0
  %.sroa.speculated.i.i.i = select i1 %cmp.i.i.i.i, i64 1, i64 %sub.ptr.div.i31.i.i.i
  %add.i.i.i = add nsw i64 %.sroa.speculated.i.i.i, %sub.ptr.div.i31.i.i.i
  %cmp7.i.i.i = icmp ult i64 %add.i.i.i, %sub.ptr.div.i31.i.i.i
  %cmp9.i.i.i = icmp ugt i64 %add.i.i.i, 1152921504606846975
  %or.cond.i.i.i = or i1 %cmp7.i.i.i, %cmp9.i.i.i
  %cond.i.i.i = select i1 %or.cond.i.i.i, i64 1152921504606846975, i64 %add.i.i.i
  %mul.i.i.i.i.i = shl nuw i64 %cond.i.i.i, 4
  %call2.i.i.i.i.i241 = invoke noalias nonnull i8* @_Znwm(i64 %mul.i.i.i.i.i) #19
          to label %call2.i.i.i.i.i.noexc unwind label %lpad16

call2.i.i.i.i.i.noexc:                            ; preds = %if.else.i
  %24 = bitcast i8* %call2.i.i.i.i.i241 to %struct.GroupByColumn*
  %groupCol.sroa.0.0..sroa_idx247 = getelementptr inbounds %struct.GroupByColumn, %struct.GroupByColumn* %24, i64 %sub.ptr.div.i31.i.i.i, i32 0
  store i32 %14, i32* %groupCol.sroa.0.0..sroa_idx247, align 8, !tbaa.struct !35
  %groupCol.sroa.6254.0..sroa_idx258 = getelementptr inbounds %struct.GroupByColumn, %struct.GroupByColumn* %24, i64 %sub.ptr.div.i31.i.i.i, i32 1
  store i8* %res.0.i234, i8** %groupCol.sroa.6254.0..sroa_idx258, align 8, !tbaa.struct !36
  br i1 %cmp.i.i.i.i, label %invoke.cont15.i.i, label %if.then.i.i.i.i.i.i.i.i76.i.i

if.then.i.i.i.i.i.i.i.i76.i.i:                    ; preds = %call2.i.i.i.i.i.noexc
  %25 = bitcast %struct.GroupByColumn* %groupByTuple.sroa.0.0317 to i8*
  call void @llvm.memmove.p0i8.p0i8.i64(i8* nonnull align 8 %call2.i.i.i.i.i241, i8* align 8 %25, i64 %sub.ptr.sub.i30.i.i.i, i1 false) #20
  br label %invoke.cont15.i.i

invoke.cont15.i.i:                                ; preds = %call2.i.i.i.i.i.noexc, %if.then.i.i.i.i.i.i.i.i76.i.i
  %add.ptr.i.i.i.i.i.i.i.i78.i.i = getelementptr inbounds %struct.GroupByColumn, %struct.GroupByColumn* %24, i64 %sub.ptr.div.i31.i.i.i
  %tobool.not.i68.i.i = icmp eq %struct.GroupByColumn* %groupByTuple.sroa.0.0317, null
  br i1 %tobool.not.i68.i.i, label %_ZNSt6vectorI13GroupByColumnSaIS0_EE17_M_realloc_insertIJRKS0_EEEvN9__gnu_cxx17__normal_iteratorIPS0_S2_EEDpOT_.exit.i, label %if.then.i69.i.i

if.then.i69.i.i:                                  ; preds = %invoke.cont15.i.i
  %26 = bitcast %struct.GroupByColumn* %groupByTuple.sroa.0.0317 to i8*
  call void @_ZdlPv(i8* nonnull %26) #20
  br label %_ZNSt6vectorI13GroupByColumnSaIS0_EE17_M_realloc_insertIJRKS0_EEEvN9__gnu_cxx17__normal_iteratorIPS0_S2_EEDpOT_.exit.i

_ZNSt6vectorI13GroupByColumnSaIS0_EE17_M_realloc_insertIJRKS0_EEEvN9__gnu_cxx17__normal_iteratorIPS0_S2_EEDpOT_.exit.i: ; preds = %if.then.i69.i.i, %invoke.cont15.i.i
  %add.ptr39.i.i = getelementptr inbounds %struct.GroupByColumn, %struct.GroupByColumn* %24, i64 %cond.i.i.i
  br label %invoke.cont17

invoke.cont17:                                    ; preds = %_ZNSt6vectorI13GroupByColumnSaIS0_EE17_M_realloc_insertIJRKS0_EEEvN9__gnu_cxx17__normal_iteratorIPS0_S2_EEDpOT_.exit.i, %if.then.i
  %groupByTuple.sroa.18.1 = phi %struct.GroupByColumn* [ %add.ptr39.i.i, %_ZNSt6vectorI13GroupByColumnSaIS0_EE17_M_realloc_insertIJRKS0_EEEvN9__gnu_cxx17__normal_iteratorIPS0_S2_EEDpOT_.exit.i ], [ %groupByTuple.sroa.18.0315, %if.then.i ]
  %add.ptr.i.i.i.i.i.i.i.i78.i.i.pn = phi %struct.GroupByColumn* [ %add.ptr.i.i.i.i.i.i.i.i78.i.i, %_ZNSt6vectorI13GroupByColumnSaIS0_EE17_M_realloc_insertIJRKS0_EEEvN9__gnu_cxx17__normal_iteratorIPS0_S2_EEDpOT_.exit.i ], [ %groupByTuple.sroa.11.0316, %if.then.i ]
  %groupByTuple.sroa.0.1 = phi %struct.GroupByColumn* [ %24, %_ZNSt6vectorI13GroupByColumnSaIS0_EE17_M_realloc_insertIJRKS0_EEEvN9__gnu_cxx17__normal_iteratorIPS0_S2_EEDpOT_.exit.i ], [ %groupByTuple.sroa.0.0317, %if.then.i ]
  %groupByTuple.sroa.11.1 = getelementptr inbounds %struct.GroupByColumn, %struct.GroupByColumn* %add.ptr.i.i.i.i.i.i.i.i78.i.i.pn, i64 1
  switch i32 %14, label %sw.epilog [
    i32 1, label %sw.bb
    i32 2, label %sw.bb25
    i32 3, label %sw.bb37
  ]

lpad16:                                           ; preds = %if.else.i
  %27 = landingpad { i8*, i32 }
          cleanup
  br label %ehcleanup116

sw.bb:                                            ; preds = %invoke.cont17
  %28 = bitcast i8* %res.0.i234 to i32*
  %29 = load i32, i32* %28, align 4, !tbaa !25
  %conv.i236 = sext i32 %29 to i64
  br label %sw.epilog

sw.bb25:                                          ; preds = %invoke.cont17
  %30 = bitcast i8* %res.0.i234 to i64*
  %31 = load i64, i64* %30, align 8, !tbaa !23
  br label %sw.epilog

sw.bb37:                                          ; preds = %invoke.cont17
  %32 = bitcast i8* %res.0.i234 to double*
  %33 = load double, double* %32, align 8, !tbaa !37
  call void @llvm.lifetime.start.p0i8(i64 8, i8* nonnull %2)
  store double %33, double* %__val.addr.i, align 8, !tbaa !37
  %cmp.i217 = fcmp une double %33, 0.000000e+00
  br i1 %cmp.i217, label %cond.true.i, label %_ZNKSt4hashIdEclEd.exit

cond.true.i:                                      ; preds = %sw.bb37
  %call.i.i2.i = invoke i64 @_ZSt11_Hash_bytesPKvmm(i8* nonnull %2, i64 8, i64 3339675911)
          to label %_ZNKSt4hashIdEclEd.exit unwind label %terminate.lpad.i

terminate.lpad.i:                                 ; preds = %cond.true.i
  %34 = landingpad { i8*, i32 }
          catch i8* null
  %35 = extractvalue { i8*, i32 } %34, 0
  call void @__clang_call_terminate(i8* %35) #21
  unreachable

_ZNKSt4hashIdEclEd.exit:                          ; preds = %sw.bb37, %cond.true.i
  %cond.i = phi i64 [ 0, %sw.bb37 ], [ %call.i.i2.i, %cond.true.i ]
  call void @llvm.lifetime.end.p0i8(i64 8, i8* nonnull %2)
  br label %sw.epilog

sw.epilog:                                        ; preds = %invoke.cont17, %_ZNKSt4hashIdEclEd.exit, %sw.bb25, %sw.bb
  %hash.0 = phi i64 [ 0, %invoke.cont17 ], [ %cond.i, %_ZNKSt4hashIdEclEd.exit ], [ %31, %sw.bb25 ], [ %conv.i236, %sw.bb ]
  %mul.i = mul i64 %combinedHash.0319, 31
  %add.i = add i64 %hash.0, %mul.i
  %incdec.ptr.i = getelementptr inbounds %struct.ColumnIndex, %struct.ColumnIndex* %__begin1.sroa.0.0314, i64 1
  %cmp.i.not = icmp eq %struct.ColumnIndex* %incdec.ptr.i, %1
  br i1 %cmp.i.not, label %for.cond.cleanup.loopexit, label %for.body

if.then:                                          ; preds = %if.end3.i.i.i.i, %lor.lhs.false.i.i.i.i, %for.cond.cleanup
  %36 = bitcast %"struct.std::pair.28"* %ref.tmp74 to i8*
  call void @llvm.lifetime.start.p0i8(i64 32, i8* nonnull %36) #20
  %first.i = getelementptr inbounds %"struct.std::pair.28", %"struct.std::pair.28"* %ref.tmp74, i64 0, i32 0
  store i64 %combinedHash.0.lcssa, i64* %first.i, align 8, !tbaa !39
  %second.i = getelementptr inbounds %"struct.std::pair.28", %"struct.std::pair.28"* %ref.tmp74, i64 0, i32 1
  %sub.ptr.rhs.cast.i.i.i = ptrtoint %struct.GroupByColumn* %groupByTuple.sroa.0.0.lcssa to i64
  %sub.ptr.sub.i.i.i = sub i64 %groupByTuple.sroa.11.0.lcssa, %sub.ptr.rhs.cast.i.i.i
  %sub.ptr.div.i.i.i = ashr exact i64 %sub.ptr.sub.i.i.i, 4
  %37 = bitcast %"class.std::vector.17"* %second.i to i8*
  call void @llvm.memset.p0i8.i64(i8* nonnull align 8 dereferenceable(24) %37, i8 0, i64 24, i1 false) #20
  %cmp.not.i.i.i.i.i = icmp eq i64 %sub.ptr.sub.i.i.i, 0
  br i1 %cmp.not.i.i.i.i.i, label %invoke.cont.i.i, label %cond.true.i.i.i.i.i

cond.true.i.i.i.i.i:                              ; preds = %if.then
  %cmp.i.i.i.i.i.i.i209 = icmp slt i64 %sub.ptr.sub.i.i.i, 0
  br i1 %cmp.i.i.i.i.i.i.i209, label %if.then.i.i.i.i.i.i.i, label %_ZNSt16allocator_traitsISaI13GroupByColumnEE8allocateERS1_m.exit.i.i.i.i.i

if.then.i.i.i.i.i.i.i:                            ; preds = %cond.true.i.i.i.i.i
  invoke void @_ZSt17__throw_bad_allocv() #22
          to label %.noexc unwind label %lpad75

.noexc:                                           ; preds = %if.then.i.i.i.i.i.i.i
  unreachable

_ZNSt16allocator_traitsISaI13GroupByColumnEE8allocateERS1_m.exit.i.i.i.i.i: ; preds = %cond.true.i.i.i.i.i
  %call2.i.i.i.i3.i22.i.i210 = invoke noalias nonnull i8* @_Znwm(i64 %sub.ptr.sub.i.i.i) #19
          to label %call2.i.i.i.i3.i22.i.i.noexc unwind label %lpad75

call2.i.i.i.i3.i22.i.i.noexc:                     ; preds = %_ZNSt16allocator_traitsISaI13GroupByColumnEE8allocateERS1_m.exit.i.i.i.i.i
  %38 = bitcast i8* %call2.i.i.i.i3.i22.i.i210 to %struct.GroupByColumn*
  br label %invoke.cont.i.i

invoke.cont.i.i:                                  ; preds = %call2.i.i.i.i3.i22.i.i.noexc, %if.then
  %cond.i.i.i.i.i = phi %struct.GroupByColumn* [ %38, %call2.i.i.i.i3.i22.i.i.noexc ], [ null, %if.then ]
  %_M_start.i.i.i.i = getelementptr inbounds %"class.std::vector.17", %"class.std::vector.17"* %second.i, i64 0, i32 0, i32 0, i32 0
  store %struct.GroupByColumn* %cond.i.i.i.i.i, %struct.GroupByColumn** %_M_start.i.i.i.i, align 8, !tbaa !42
  %_M_finish.i.i.i.i = getelementptr inbounds %"struct.std::pair.28", %"struct.std::pair.28"* %ref.tmp74, i64 0, i32 1, i32 0, i32 0, i32 1
  store %struct.GroupByColumn* %cond.i.i.i.i.i, %struct.GroupByColumn** %_M_finish.i.i.i.i, align 8, !tbaa !45
  %add.ptr.i.i.i.i = getelementptr inbounds %struct.GroupByColumn, %struct.GroupByColumn* %cond.i.i.i.i.i, i64 %sub.ptr.div.i.i.i
  %_M_end_of_storage.i.i.i.i = getelementptr inbounds %"struct.std::pair.28", %"struct.std::pair.28"* %ref.tmp74, i64 0, i32 1, i32 0, i32 0, i32 2
  store %struct.GroupByColumn* %add.ptr.i.i.i.i, %struct.GroupByColumn** %_M_end_of_storage.i.i.i.i, align 8, !tbaa !46
  br i1 %cmp.not.i.i.i.i.i, label %invoke.cont76, label %if.then.i.i.i.i.i.i.i.i.i

if.then.i.i.i.i.i.i.i.i.i:                        ; preds = %invoke.cont.i.i
  %39 = bitcast %struct.GroupByColumn* %cond.i.i.i.i.i to i8*
  %40 = bitcast %struct.GroupByColumn* %groupByTuple.sroa.0.0.lcssa to i8*
  call void @llvm.memcpy.p0i8.p0i8.i64(i8* align 8 %39, i8* align 8 %40, i64 %sub.ptr.sub.i.i.i, i1 false) #20
  br label %invoke.cont76

invoke.cont76:                                    ; preds = %if.then.i.i.i.i.i.i.i.i.i, %invoke.cont.i.i
  store %struct.GroupByColumn* %add.ptr.i.i.i.i, %struct.GroupByColumn** %_M_finish.i.i.i.i, align 8, !tbaa !45
  %41 = getelementptr inbounds %"class.std::unordered_map", %"class.std::unordered_map"* %groupedRows, i64 0, i32 0
  %call3.i.i207 = invoke { %"struct.std::__detail::_Hash_node"*, i8 } @_ZNSt10_HashtableImSt4pairIKmSt6vectorI13GroupByColumnSaIS3_EEESaIS6_ENSt8__detail10_Select1stESt8equal_toImESt4hashImENS8_18_Mod_range_hashingENS8_20_Default_ranged_hashENS8_20_Prime_rehash_policyENS8_17_Hashtable_traitsILb0ELb0ELb1EEEE10_M_emplaceIJS6_EEES0_INS8_14_Node_iteratorIS6_Lb0ELb0EEEbESt17integral_constantIbLb1EEDpOT_(%"class.std::_Hashtable"* nonnull dereferenceable(56) %41, %"struct.std::pair.28"* nonnull align 8 dereferenceable(32) %ref.tmp74)
          to label %invoke.cont78 unwind label %lpad77

invoke.cont78:                                    ; preds = %invoke.cont76
  %_M_start.i.i.i202 = getelementptr inbounds %"struct.std::pair.28", %"struct.std::pair.28"* %ref.tmp74, i64 0, i32 1, i32 0, i32 0, i32 0
  %42 = load %struct.GroupByColumn*, %struct.GroupByColumn** %_M_start.i.i.i202, align 8, !tbaa !42
  %tobool.not.i.i.i.i203 = icmp eq %struct.GroupByColumn* %42, null
  br i1 %tobool.not.i.i.i.i203, label %_ZNSt4pairIKmSt6vectorI13GroupByColumnSaIS2_EEED2Ev.exit205, label %if.then.i.i.i.i204

if.then.i.i.i.i204:                               ; preds = %invoke.cont78
  %43 = bitcast %struct.GroupByColumn* %42 to i8*
  call void @_ZdlPv(i8* nonnull %43) #20
  br label %_ZNSt4pairIKmSt6vectorI13GroupByColumnSaIS2_EEED2Ev.exit205

_ZNSt4pairIKmSt6vectorI13GroupByColumnSaIS2_EEED2Ev.exit205: ; preds = %invoke.cont78, %if.then.i.i.i.i204
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %36) #20
  br label %if.end

lpad75:                                           ; preds = %_ZNSt16allocator_traitsISaI13GroupByColumnEE8allocateERS1_m.exit.i.i.i.i.i, %if.then.i.i.i.i.i.i.i
  %44 = landingpad { i8*, i32 }
          cleanup
  br label %ehcleanup81

lpad77:                                           ; preds = %invoke.cont76
  %45 = landingpad { i8*, i32 }
          cleanup
  %_M_start.i.i.i = getelementptr inbounds %"struct.std::pair.28", %"struct.std::pair.28"* %ref.tmp74, i64 0, i32 1, i32 0, i32 0, i32 0
  %46 = load %struct.GroupByColumn*, %struct.GroupByColumn** %_M_start.i.i.i, align 8, !tbaa !42
  %tobool.not.i.i.i.i = icmp eq %struct.GroupByColumn* %46, null
  br i1 %tobool.not.i.i.i.i, label %ehcleanup81, label %if.then.i.i.i.i

if.then.i.i.i.i:                                  ; preds = %lpad77
  %47 = bitcast %struct.GroupByColumn* %46 to i8*
  call void @_ZdlPv(i8* nonnull %47) #20
  br label %ehcleanup81

ehcleanup81:                                      ; preds = %if.then.i.i.i.i, %lpad77, %lpad75
  %.pn = phi { i8*, i32 } [ %44, %lpad75 ], [ %45, %lpad77 ], [ %45, %if.then.i.i.i.i ]
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %36) #20
  br label %ehcleanup116

if.end:                                           ; preds = %for.cond.i.i.i.i, %if.end.i.i.i.i, %_ZNSt4pairIKmSt6vectorI13GroupByColumnSaIS2_EEED2Ev.exit205
  %_M_finish.i196 = getelementptr inbounds %class.HashGroupBy, %class.HashGroupBy* %this, i64 0, i32 2, i32 0, i32 0, i32 1
  %48 = load %struct.ColumnIndex*, %struct.ColumnIndex** %_M_finish.i196, align 8, !tbaa !2
  %_M_start.i197 = getelementptr inbounds %class.HashGroupBy, %class.HashGroupBy* %this, i64 0, i32 2, i32 0, i32 0, i32 0
  %49 = load %struct.ColumnIndex*, %struct.ColumnIndex** %_M_start.i197, align 8, !tbaa !8
  %cmp310.not = icmp eq %struct.ColumnIndex* %48, %49
  br i1 %cmp310.not, label %for.cond.cleanup85, label %for.body86.lr.ph

for.body86.lr.ph:                                 ; preds = %if.end
  %_M_start.i.i183 = getelementptr inbounds %class.Table, %class.Table* %table, i64 0, i32 3, i32 0, i32 0, i32 0
  %idx.ext10.i = zext i32 %rowIndex to i64
  %_M_start.i177 = getelementptr inbounds %class.HashGroupBy, %class.HashGroupBy* %this, i64 0, i32 3, i32 0, i32 0, i32 0
  br label %for.body86

for.cond.cleanup85:                               ; preds = %invoke.cont110, %if.end
  %tobool.not.i.i.i192 = icmp eq %struct.GroupByColumn* %groupByTuple.sroa.0.0.lcssa, null
  br i1 %tobool.not.i.i.i192, label %_ZNSt6vectorI13GroupByColumnSaIS0_EED2Ev.exit194, label %if.then.i.i.i193

if.then.i.i.i193:                                 ; preds = %for.cond.cleanup85
  %50 = bitcast %struct.GroupByColumn* %groupByTuple.sroa.0.0.lcssa to i8*
  call void @_ZdlPv(i8* nonnull %50) #20
  br label %_ZNSt6vectorI13GroupByColumnSaIS0_EED2Ev.exit194

_ZNSt6vectorI13GroupByColumnSaIS0_EED2Ev.exit194: ; preds = %for.cond.cleanup85, %if.then.i.i.i193
  ret void

for.body86:                                       ; preds = %for.body86.lr.ph, %invoke.cont110
  %indvars.iv = phi i64 [ 0, %for.body86.lr.ph ], [ %indvars.iv.next, %invoke.cont110 ]
  %51 = load %struct.ColumnIndex*, %struct.ColumnIndex** %_M_finish.i195, align 8, !tbaa !2
  %52 = load %struct.ColumnIndex*, %struct.ColumnIndex** %_M_start.i, align 8, !tbaa !8
  %sub.ptr.lhs.cast.i187 = ptrtoint %struct.ColumnIndex* %51 to i64
  %sub.ptr.rhs.cast.i188 = ptrtoint %struct.ColumnIndex* %52 to i64
  %sub.ptr.sub.i189 = sub i64 %sub.ptr.lhs.cast.i187, %sub.ptr.rhs.cast.i188
  %53 = lshr exact i64 %sub.ptr.sub.i189, 3
  %conv91 = add i64 %indvars.iv, %53
  %conv.i182 = and i64 %conv91, 4294967295
  %54 = load %class.Column**, %class.Column*** %_M_start.i.i183, align 8, !tbaa !29
  %add.ptr.i.i184 = getelementptr inbounds %class.Column*, %class.Column** %54, i64 %conv.i182
  %55 = load %class.Column*, %class.Column** %add.ptr.i.i184, align 8, !tbaa !19
  %type.i181 = getelementptr inbounds %class.Column, %class.Column* %55, i64 0, i32 3
  %56 = load i32, i32* %type.i181, align 8, !tbaa !32
  switch i32 %56, label %_ZN6Column8getValueEj.exit [
    i32 1, label %sw.bb.i
    i32 2, label %sw.bb2.i
    i32 3, label %sw.bb7.i
  ]

sw.bb.i:                                          ; preds = %for.body86
  %data.i = getelementptr inbounds %class.Column, %class.Column* %55, i64 0, i32 1
  %57 = bitcast i8** %data.i to i32**
  %58 = load i32*, i32** %57, align 8, !tbaa !34
  %add.ptr.i178 = getelementptr inbounds i32, i32* %58, i64 %idx.ext10.i
  %59 = bitcast i32* %add.ptr.i178 to i8*
  br label %_ZN6Column8getValueEj.exit

sw.bb2.i:                                         ; preds = %for.body86
  %data4.i = getelementptr inbounds %class.Column, %class.Column* %55, i64 0, i32 1
  %60 = bitcast i8** %data4.i to i64**
  %61 = load i64*, i64** %60, align 8, !tbaa !34
  %add.ptr6.i = getelementptr inbounds i64, i64* %61, i64 %idx.ext10.i
  %62 = bitcast i64* %add.ptr6.i to i8*
  br label %_ZN6Column8getValueEj.exit

sw.bb7.i:                                         ; preds = %for.body86
  %data9.i = getelementptr inbounds %class.Column, %class.Column* %55, i64 0, i32 1
  %63 = bitcast i8** %data9.i to double**
  %64 = load double*, double** %63, align 8, !tbaa !34
  %add.ptr11.i = getelementptr inbounds double, double* %64, i64 %idx.ext10.i
  %65 = bitcast double* %add.ptr11.i to i8*
  br label %_ZN6Column8getValueEj.exit

_ZN6Column8getValueEj.exit:                       ; preds = %for.body86, %sw.bb.i, %sw.bb2.i, %sw.bb7.i
  %res.0.i = phi i8* [ null, %for.body86 ], [ %65, %sw.bb7.i ], [ %62, %sw.bb2.i ], [ %59, %sw.bb.i ]
  %66 = load %class.Aggregator**, %class.Aggregator*** %_M_start.i177, align 8, !tbaa !47
  %add.ptr.i = getelementptr inbounds %class.Aggregator*, %class.Aggregator** %66, i64 %indvars.iv
  %67 = load %class.Aggregator*, %class.Aggregator** %add.ptr.i, align 8, !tbaa !19
  %68 = bitcast %class.Aggregator* %67 to void (%class.Aggregator*, i64, i8*, i32)***
  %vtable = load void (%class.Aggregator*, i64, i8*, i32)**, void (%class.Aggregator*, i64, i8*, i32)*** %68, align 8, !tbaa !50
  %vfn = getelementptr inbounds void (%class.Aggregator*, i64, i8*, i32)*, void (%class.Aggregator*, i64, i8*, i32)** %vtable, i64 2
  %69 = load void (%class.Aggregator*, i64, i8*, i32)*, void (%class.Aggregator*, i64, i8*, i32)** %vfn, align 8
  invoke void %69(%class.Aggregator* nonnull dereferenceable(72) %67, i64 %combinedHash.0.lcssa, i8* %res.0.i, i32 %56)
          to label %invoke.cont110 unwind label %lpad103

invoke.cont110:                                   ; preds = %_ZN6Column8getValueEj.exit
  %indvars.iv.next = add nuw i64 %indvars.iv, 1
  %70 = load %struct.ColumnIndex*, %struct.ColumnIndex** %_M_finish.i196, align 8, !tbaa !2
  %71 = load %struct.ColumnIndex*, %struct.ColumnIndex** %_M_start.i197, align 8, !tbaa !8
  %sub.ptr.lhs.cast.i198 = ptrtoint %struct.ColumnIndex* %70 to i64
  %sub.ptr.rhs.cast.i199 = ptrtoint %struct.ColumnIndex* %71 to i64
  %sub.ptr.sub.i200 = sub i64 %sub.ptr.lhs.cast.i198, %sub.ptr.rhs.cast.i199
  %sub.ptr.div.i201 = ashr exact i64 %sub.ptr.sub.i200, 3
  %cmp = icmp ugt i64 %sub.ptr.div.i201, %indvars.iv.next
  br i1 %cmp, label %for.body86, label %for.cond.cleanup85, !llvm.loop !52

lpad103:                                          ; preds = %_ZN6Column8getValueEj.exit
  %72 = landingpad { i8*, i32 }
          cleanup
  br label %ehcleanup116

ehcleanup116:                                     ; preds = %lpad103, %ehcleanup81, %lpad16
  %groupByTuple.sroa.0.0301 = phi %struct.GroupByColumn* [ %groupByTuple.sroa.0.0317, %lpad16 ], [ %groupByTuple.sroa.0.0.lcssa, %lpad103 ], [ %groupByTuple.sroa.0.0.lcssa, %ehcleanup81 ]
  %.pn173.pn.pn = phi { i8*, i32 } [ %27, %lpad16 ], [ %72, %lpad103 ], [ %.pn, %ehcleanup81 ]
  %tobool.not.i.i.i = icmp eq %struct.GroupByColumn* %groupByTuple.sroa.0.0301, null
  br i1 %tobool.not.i.i.i, label %_ZNSt6vectorI13GroupByColumnSaIS0_EED2Ev.exit, label %if.then.i.i.i

if.then.i.i.i:                                    ; preds = %ehcleanup116
  %73 = bitcast %struct.GroupByColumn* %groupByTuple.sroa.0.0301 to i8*
  call void @_ZdlPv(i8* nonnull %73) #20
  br label %_ZNSt6vectorI13GroupByColumnSaIS0_EED2Ev.exit

_ZNSt6vectorI13GroupByColumnSaIS0_EED2Ev.exit:    ; preds = %ehcleanup116, %if.then.i.i.i
  resume { i8*, i32 } %.pn173.pn.pn
}

; Function Attrs: argmemonly nofree nosync nounwind willreturn
declare void @llvm.lifetime.start.p0i8(i64 immarg, i8* nocapture) #7

; Function Attrs: argmemonly nofree nosync nounwind willreturn
declare void @llvm.memcpy.p0i8.p0i8.i64(i8* noalias nocapture writeonly, i8* noalias nocapture readonly, i64, i1 immarg) #7

declare dso_local i32 @__gxx_personality_v0(...)

; Function Attrs: nobuiltin nofree allocsize(0)
declare dso_local nonnull i8* @_Znwm(i64) local_unnamed_addr #5

; Function Attrs: argmemonly nofree nosync nounwind willreturn
declare void @llvm.lifetime.end.p0i8(i64 immarg, i8* nocapture) #7

; Function Attrs: norecurse nounwind readnone uwtable willreturn mustprogress
define dso_local void @_ZN11HashGroupBy8postloopEP5Table(%class.HashGroupBy* nocapture nonnull dereferenceable(144) %this, %class.Table* nocapture %table) unnamed_addr #8 align 2 {
entry:
  ret void
}

; Function Attrs: uwtable mustprogress
define dso_local void @processAgg(i64 %key, %"class.std::vector.0"* nocapture nonnull readonly align 8 dereferenceable(24) %aggs, i32* nocapture readonly %aggTypes, i32 %aggNum, i32* nocapture readonly %types, i32* nocapture readonly %aggIdx, i8** nocapture readonly %head, i32 %offset) local_unnamed_addr #9 {
entry:
  %cmp52 = icmp sgt i32 %aggNum, 0
  br i1 %cmp52, label %for.body.lr.ph, label %for.cond.cleanup

for.body.lr.ph:                                   ; preds = %entry
  %_M_start.i48 = getelementptr inbounds %"class.std::vector.0", %"class.std::vector.0"* %aggs, i64 0, i32 0, i32 0, i32 0
  %wide.trip.count = zext i32 %aggNum to i64
  br label %for.body

for.cond.cleanup:                                 ; preds = %sw.epilog24, %entry
  ret void

for.body:                                         ; preds = %for.body.lr.ph, %sw.epilog24
  %indvars.iv = phi i64 [ 0, %for.body.lr.ph ], [ %indvars.iv.next, %sw.epilog24 ]
  %arrayidx = getelementptr inbounds i32, i32* %aggIdx, i64 %indvars.iv
  %0 = load i32, i32* %arrayidx, align 4, !tbaa !25
  %idxprom1 = sext i32 %0 to i64
  %arrayidx2 = getelementptr inbounds i32, i32* %types, i64 %idxprom1
  %1 = load i32, i32* %arrayidx2, align 4, !tbaa !25
  %arrayidx4 = getelementptr inbounds i8*, i8** %head, i64 %idxprom1
  %2 = load i8*, i8** %arrayidx4, align 8, !tbaa !19
  %arrayidx6 = getelementptr inbounds i32, i32* %aggTypes, i64 %indvars.iv
  %3 = load i32, i32* %arrayidx6, align 4, !tbaa !25
  %cond = icmp eq i32 %3, 0
  br i1 %cond, label %sw.bb, label %do.body17

sw.bb:                                            ; preds = %for.body
  switch i32 %1, label %do.body [
    i32 1, label %sw.bb7
    i32 2, label %sw.bb8
    i32 3, label %sw.bb11
  ]

sw.bb7:                                           ; preds = %sw.bb
  %4 = load %class.Aggregator**, %class.Aggregator*** %_M_start.i48, align 8, !tbaa !47
  %add.ptr.i = getelementptr inbounds %class.Aggregator*, %class.Aggregator** %4, i64 %indvars.iv
  %5 = bitcast %class.Aggregator** %add.ptr.i to %class.SumAggregator**
  %6 = load %class.SumAggregator*, %class.SumAggregator** %5, align 8, !tbaa !19
  tail call void @sumProcessInt32(%class.SumAggregator* %6, i64 %key, i8* %2, i32 %offset)
  br label %sw.epilog24

sw.bb8:                                           ; preds = %sw.bb
  %7 = load %class.Aggregator**, %class.Aggregator*** %_M_start.i48, align 8, !tbaa !47
  %add.ptr.i51 = getelementptr inbounds %class.Aggregator*, %class.Aggregator** %7, i64 %indvars.iv
  %8 = bitcast %class.Aggregator** %add.ptr.i51 to %class.SumAggregator**
  %9 = load %class.SumAggregator*, %class.SumAggregator** %8, align 8, !tbaa !19
  tail call void @sumProcessInt64(%class.SumAggregator* %9, i64 %key, i8* %2, i32 %offset)
  br label %sw.epilog24

sw.bb11:                                          ; preds = %sw.bb
  %10 = load %class.Aggregator**, %class.Aggregator*** %_M_start.i48, align 8, !tbaa !47
  %add.ptr.i49 = getelementptr inbounds %class.Aggregator*, %class.Aggregator** %10, i64 %indvars.iv
  %11 = bitcast %class.Aggregator** %add.ptr.i49 to %class.SumAggregator**
  %12 = load %class.SumAggregator*, %class.SumAggregator** %11, align 8, !tbaa !19
  tail call void @sumProcessDouble(%class.SumAggregator* %12, i64 %key, i8* %2, i32 %offset)
  br label %sw.epilog24

do.body:                                          ; preds = %sw.bb
  %call14 = tail call i32 (i8*, ...) @printf(i8* nonnull dereferenceable(1) getelementptr inbounds ([29 x i8], [29 x i8]* @.str, i64 0, i64 0), i8* getelementptr inbounds ([61 x i8], [61 x i8]* @.str.1, i64 0, i64 0), i8* getelementptr inbounds ([11 x i8], [11 x i8]* @__FUNCTION__.processAgg, i64 0, i64 0), i32 116, i32 %1)
  %putchar47 = tail call i32 @putchar(i32 10)
  br label %sw.epilog24

do.body17:                                        ; preds = %for.body
  %call20 = tail call i32 (i8*, ...) @printf(i8* nonnull dereferenceable(1) getelementptr inbounds ([35 x i8], [35 x i8]* @.str.3, i64 0, i64 0), i8* getelementptr inbounds ([61 x i8], [61 x i8]* @.str.1, i64 0, i64 0), i8* getelementptr inbounds ([11 x i8], [11 x i8]* @__FUNCTION__.processAgg, i64 0, i64 0), i32 124, i32 %3)
  %putchar = tail call i32 @putchar(i32 10)
  br label %sw.epilog24

sw.epilog24:                                      ; preds = %sw.bb7, %sw.bb8, %sw.bb11, %do.body, %do.body17
  %indvars.iv.next = add nuw nsw i64 %indvars.iv, 1
  %exitcond.not = icmp eq i64 %indvars.iv.next, %wide.trip.count
  br i1 %exitcond.not, label %for.cond.cleanup, label %for.body, !llvm.loop !54
}

declare dso_local void @sumProcessInt32(%class.SumAggregator*, i64, i8*, i32) local_unnamed_addr #0

declare dso_local void @sumProcessInt64(%class.SumAggregator*, i64, i8*, i32) local_unnamed_addr #0

declare dso_local void @sumProcessDouble(%class.SumAggregator*, i64, i8*, i32) local_unnamed_addr #0

; Function Attrs: nofree nounwind
declare dso_local noundef i32 @printf(i8* nocapture noundef readonly, ...) local_unnamed_addr #10

; Function Attrs: uwtable
define dso_local void @_ZN11HashGroupBy6inloopEPPcjPiiS2_iS2_iS2_(%class.HashGroupBy* nonnull dereferenceable(144) %this, i8** nocapture readonly %head, i32 %offset, i32* nocapture readonly %types, i32 %colNum, i32* nocapture readonly %groupByColIdx, i32 %groupByColNum, i32* nocapture readonly %aggColIdx, i32 %aggColNum, i32* nocapture readonly %aggFuncTypes) local_unnamed_addr #6 align 2 personality i8* bitcast (i32 (...)* @__gxx_personality_v0 to i8*) {
entry:
  %__val.addr.i = alloca double, align 8
  %ref.tmp85 = alloca %"struct.std::pair.28", align 8
  %cmp264 = icmp sgt i32 %groupByColNum, 0
  br i1 %cmp264, label %for.body.lr.ph, label %for.cond.cleanup

for.body.lr.ph:                                   ; preds = %entry
  %idx.ext17 = zext i32 %offset to i64
  %0 = bitcast double* %__val.addr.i to i8*
  %wide.trip.count275 = zext i32 %groupByColNum to i64
  br label %for.body

for.cond.cleanup:                                 ; preds = %sw.epilog, %entry
  %combinedHash.sroa.9.0.lcssa = phi i64 [ 0, %entry ], [ %idx.ext17, %sw.epilog ]
  %combinedHash.sroa.0.0.lcssa = phi i64 [ 0, %entry ], [ %add.i, %sw.epilog ]
  %groupedRows = getelementptr inbounds %class.HashGroupBy, %class.HashGroupBy* %this, i64 0, i32 4
  %_M_bucket_count.i.i.i = getelementptr inbounds %class.HashGroupBy, %class.HashGroupBy* %this, i64 0, i32 4, i32 0, i32 1
  %1 = load i64, i64* %_M_bucket_count.i.i.i, align 8, !tbaa !20
  %rem.i.i.i.i.i = urem i64 %combinedHash.sroa.0.0.lcssa, %1
  %_M_buckets.i.i.i.i = getelementptr inbounds %"class.std::unordered_map", %"class.std::unordered_map"* %groupedRows, i64 0, i32 0, i32 0
  %2 = load %"struct.std::__detail::_Hash_node_base"**, %"struct.std::__detail::_Hash_node_base"*** %_M_buckets.i.i.i.i, align 8, !tbaa !21
  %arrayidx.i.i.i.i = getelementptr inbounds %"struct.std::__detail::_Hash_node_base"*, %"struct.std::__detail::_Hash_node_base"** %2, i64 %rem.i.i.i.i.i
  %3 = load %"struct.std::__detail::_Hash_node_base"*, %"struct.std::__detail::_Hash_node_base"** %arrayidx.i.i.i.i, align 8, !tbaa !19
  %tobool.not.i.i.i.i = icmp eq %"struct.std::__detail::_Hash_node_base"* %3, null
  br i1 %tobool.not.i.i.i.i, label %if.then, label %if.end.i.i.i.i

if.end.i.i.i.i:                                   ; preds = %for.cond.cleanup
  %4 = bitcast %"struct.std::__detail::_Hash_node_base"* %3 to %"struct.std::__detail::_Hash_node"**
  %5 = load %"struct.std::__detail::_Hash_node"*, %"struct.std::__detail::_Hash_node"** %4, align 8, !tbaa !22
  %_M_storage.i.i.i.i23.i.i.i.i = getelementptr inbounds %"struct.std::__detail::_Hash_node", %"struct.std::__detail::_Hash_node"* %5, i64 0, i32 0, i32 1
  %first.i.i.i.i.i24.i.i.i.i = bitcast %"struct.__gnu_cxx::__aligned_buffer"* %_M_storage.i.i.i.i23.i.i.i.i to i64*
  %6 = load i64, i64* %first.i.i.i.i.i24.i.i.i.i, align 8, !tbaa !23
  %cmp.i.i.i25.i.i.i.i = icmp eq i64 %combinedHash.sroa.0.0.lcssa, %6
  br i1 %cmp.i.i.i25.i.i.i.i, label %if.end, label %if.end3.i.i.i.i

for.cond.i.i.i.i:                                 ; preds = %lor.lhs.false.i.i.i.i
  %cmp.i.i.i.i.i.i.i = icmp eq i64 %combinedHash.sroa.0.0.lcssa, %9
  br i1 %cmp.i.i.i.i.i.i.i, label %if.end, label %if.end3.i.i.i.i

if.end3.i.i.i.i:                                  ; preds = %if.end.i.i.i.i, %for.cond.i.i.i.i
  %__p.026.i.i.i.i = phi %"struct.std::__detail::_Hash_node"* [ %8, %for.cond.i.i.i.i ], [ %5, %if.end.i.i.i.i ]
  %_M_nxt4.i.i.i.i = getelementptr inbounds %"struct.std::__detail::_Hash_node", %"struct.std::__detail::_Hash_node"* %__p.026.i.i.i.i, i64 0, i32 0, i32 0, i32 0
  %7 = load %"struct.std::__detail::_Hash_node_base"*, %"struct.std::__detail::_Hash_node_base"** %_M_nxt4.i.i.i.i, align 8, !tbaa !22
  %tobool5.not.i.i.i.i = icmp eq %"struct.std::__detail::_Hash_node_base"* %7, null
  %8 = bitcast %"struct.std::__detail::_Hash_node_base"* %7 to %"struct.std::__detail::_Hash_node"*
  br i1 %tobool5.not.i.i.i.i, label %if.then, label %lor.lhs.false.i.i.i.i

lor.lhs.false.i.i.i.i:                            ; preds = %if.end3.i.i.i.i
  %_M_storage.i.i.i.i21.i.i.i.i = getelementptr inbounds %"struct.std::__detail::_Hash_node_base", %"struct.std::__detail::_Hash_node_base"* %7, i64 1
  %first.i.i.i.i.i22.i.i.i.i = bitcast %"struct.std::__detail::_Hash_node_base"* %_M_storage.i.i.i.i21.i.i.i.i to i64*
  %9 = load i64, i64* %first.i.i.i.i.i22.i.i.i.i, align 8, !tbaa !23
  %rem.i.i.i.i.i.i.i = urem i64 %9, %1
  %cmp.not.i.i.i.i = icmp eq i64 %rem.i.i.i.i.i.i.i, %rem.i.i.i.i.i
  br i1 %cmp.not.i.i.i.i, label %for.cond.i.i.i.i, label %if.then

for.body:                                         ; preds = %for.body.lr.ph, %sw.epilog
  %indvars.iv273 = phi i64 [ 0, %for.body.lr.ph ], [ %indvars.iv.next274, %sw.epilog ]
  %combinedHash.sroa.0.0265 = phi i64 [ 0, %for.body.lr.ph ], [ %add.i, %sw.epilog ]
  %arrayidx = getelementptr inbounds i32, i32* %groupByColIdx, i64 %indvars.iv273
  %10 = load i32, i32* %arrayidx, align 4, !tbaa !25
  %idxprom2 = zext i32 %10 to i64
  %arrayidx3 = getelementptr inbounds i32, i32* %types, i64 %idxprom2
  %11 = load i32, i32* %arrayidx3, align 4, !tbaa !25
  switch i32 %11, label %sw.epilog [
    i32 1, label %sw.bb
    i32 2, label %sw.bb6
    i32 3, label %sw.bb13
  ]

sw.bb:                                            ; preds = %for.body
  %arrayidx5 = getelementptr inbounds i8*, i8** %head, i64 %idxprom2
  %12 = bitcast i8** %arrayidx5 to i32**
  %13 = load i32*, i32** %12, align 8, !tbaa !19
  %add.ptr = getelementptr inbounds i32, i32* %13, i64 %idx.ext17
  %14 = load i32, i32* %add.ptr, align 4, !tbaa !25
  %conv.i = sext i32 %14 to i64
  br label %sw.epilog

sw.bb6:                                           ; preds = %for.body
  %arrayidx9 = getelementptr inbounds i8*, i8** %head, i64 %idxprom2
  %15 = bitcast i8** %arrayidx9 to i64**
  %16 = load i64*, i64** %15, align 8, !tbaa !19
  %add.ptr11 = getelementptr inbounds i64, i64* %16, i64 %idx.ext17
  %17 = load i64, i64* %add.ptr11, align 8, !tbaa !23
  br label %sw.epilog

sw.bb13:                                          ; preds = %for.body
  %arrayidx16 = getelementptr inbounds i8*, i8** %head, i64 %idxprom2
  %18 = bitcast i8** %arrayidx16 to double**
  %19 = load double*, double** %18, align 8, !tbaa !19
  %add.ptr18 = getelementptr inbounds double, double* %19, i64 %idx.ext17
  %20 = load double, double* %add.ptr18, align 8, !tbaa !37
  call void @llvm.lifetime.start.p0i8(i64 8, i8* nonnull %0)
  store double %20, double* %__val.addr.i, align 8, !tbaa !37
  %cmp.i171 = fcmp une double %20, 0.000000e+00
  br i1 %cmp.i171, label %cond.true.i, label %_ZNKSt4hashIdEclEd.exit

cond.true.i:                                      ; preds = %sw.bb13
  %call.i.i2.i = invoke i64 @_ZSt11_Hash_bytesPKvmm(i8* nonnull %0, i64 8, i64 3339675911)
          to label %_ZNKSt4hashIdEclEd.exit unwind label %terminate.lpad.i

terminate.lpad.i:                                 ; preds = %cond.true.i
  %21 = landingpad { i8*, i32 }
          catch i8* null
  %22 = extractvalue { i8*, i32 } %21, 0
  call void @__clang_call_terminate(i8* %22) #21
  unreachable

_ZNKSt4hashIdEclEd.exit:                          ; preds = %sw.bb13, %cond.true.i
  %cond.i = phi i64 [ 0, %sw.bb13 ], [ %call.i.i2.i, %cond.true.i ]
  call void @llvm.lifetime.end.p0i8(i64 8, i8* nonnull %0)
  br label %sw.epilog

sw.epilog:                                        ; preds = %for.body, %_ZNKSt4hashIdEclEd.exit, %sw.bb6, %sw.bb
  %hash.0 = phi i64 [ 0, %for.body ], [ %cond.i, %_ZNKSt4hashIdEclEd.exit ], [ %17, %sw.bb6 ], [ %conv.i, %sw.bb ]
  %mul.i = mul i64 %combinedHash.sroa.0.0265, 31
  %add.i = add i64 %hash.0, %mul.i
  %indvars.iv.next274 = add nuw nsw i64 %indvars.iv273, 1
  %exitcond276.not = icmp eq i64 %indvars.iv.next274, %wide.trip.count275
  br i1 %exitcond276.not, label %for.cond.cleanup, label %for.body, !llvm.loop !55

if.then:                                          ; preds = %if.end3.i.i.i.i, %lor.lhs.false.i.i.i.i, %for.cond.cleanup
  br i1 %cmp264, label %for.body36.preheader, label %for.cond.cleanup35

for.body36.preheader:                             ; preds = %if.then
  %wide.trip.count = zext i32 %groupByColNum to i64
  br label %for.body36

for.cond.cleanup35.loopexit:                      ; preds = %invoke.cont78
  %phi.cast = ptrtoint %struct.GroupByColumn* %groupByTuple.sroa.11.1 to i64
  br label %for.cond.cleanup35

for.cond.cleanup35:                               ; preds = %for.cond.cleanup35.loopexit, %if.then
  %groupByTuple.sroa.0.0.lcssa = phi %struct.GroupByColumn* [ null, %if.then ], [ %groupByTuple.sroa.0.1, %for.cond.cleanup35.loopexit ]
  %groupByTuple.sroa.11.0.lcssa = phi i64 [ 0, %if.then ], [ %phi.cast, %for.cond.cleanup35.loopexit ]
  %23 = bitcast %"struct.std::pair.28"* %ref.tmp85 to i8*
  call void @llvm.lifetime.start.p0i8(i64 32, i8* nonnull %23) #20
  %first.i = getelementptr inbounds %"struct.std::pair.28", %"struct.std::pair.28"* %ref.tmp85, i64 0, i32 0
  store i64 %combinedHash.sroa.0.0.lcssa, i64* %first.i, align 8, !tbaa !39
  %second.i = getelementptr inbounds %"struct.std::pair.28", %"struct.std::pair.28"* %ref.tmp85, i64 0, i32 1
  %sub.ptr.rhs.cast.i.i.i = ptrtoint %struct.GroupByColumn* %groupByTuple.sroa.0.0.lcssa to i64
  %sub.ptr.sub.i.i.i = sub i64 %groupByTuple.sroa.11.0.lcssa, %sub.ptr.rhs.cast.i.i.i
  %sub.ptr.div.i.i.i = ashr exact i64 %sub.ptr.sub.i.i.i, 4
  %24 = bitcast %"class.std::vector.17"* %second.i to i8*
  call void @llvm.memset.p0i8.i64(i8* nonnull align 8 dereferenceable(24) %24, i8 0, i64 24, i1 false) #20
  %cmp.not.i.i.i.i.i = icmp eq i64 %sub.ptr.sub.i.i.i, 0
  br i1 %cmp.not.i.i.i.i.i, label %invoke.cont.i.i175, label %cond.true.i.i.i.i.i

cond.true.i.i.i.i.i:                              ; preds = %for.cond.cleanup35
  %cmp.i.i.i.i.i.i.i174 = icmp slt i64 %sub.ptr.sub.i.i.i, 0
  br i1 %cmp.i.i.i.i.i.i.i174, label %if.then.i.i.i.i.i.i.i, label %_ZNSt16allocator_traitsISaI13GroupByColumnEE8allocateERS1_m.exit.i.i.i.i.i

if.then.i.i.i.i.i.i.i:                            ; preds = %cond.true.i.i.i.i.i
  invoke void @_ZSt17__throw_bad_allocv() #22
          to label %.noexc unwind label %lpad87

.noexc:                                           ; preds = %if.then.i.i.i.i.i.i.i
  unreachable

_ZNSt16allocator_traitsISaI13GroupByColumnEE8allocateERS1_m.exit.i.i.i.i.i: ; preds = %cond.true.i.i.i.i.i
  %call2.i.i.i.i3.i22.i.i176 = invoke noalias nonnull i8* @_Znwm(i64 %sub.ptr.sub.i.i.i) #19
          to label %call2.i.i.i.i3.i22.i.i.noexc unwind label %lpad87

call2.i.i.i.i3.i22.i.i.noexc:                     ; preds = %_ZNSt16allocator_traitsISaI13GroupByColumnEE8allocateERS1_m.exit.i.i.i.i.i
  %25 = bitcast i8* %call2.i.i.i.i3.i22.i.i176 to %struct.GroupByColumn*
  br label %invoke.cont.i.i175

invoke.cont.i.i175:                               ; preds = %call2.i.i.i.i3.i22.i.i.noexc, %for.cond.cleanup35
  %cond.i.i.i.i.i = phi %struct.GroupByColumn* [ %25, %call2.i.i.i.i3.i22.i.i.noexc ], [ null, %for.cond.cleanup35 ]
  %_M_start.i.i.i.i = getelementptr inbounds %"class.std::vector.17", %"class.std::vector.17"* %second.i, i64 0, i32 0, i32 0, i32 0
  store %struct.GroupByColumn* %cond.i.i.i.i.i, %struct.GroupByColumn** %_M_start.i.i.i.i, align 8, !tbaa !42
  %_M_finish.i.i.i.i = getelementptr inbounds %"struct.std::pair.28", %"struct.std::pair.28"* %ref.tmp85, i64 0, i32 1, i32 0, i32 0, i32 1
  store %struct.GroupByColumn* %cond.i.i.i.i.i, %struct.GroupByColumn** %_M_finish.i.i.i.i, align 8, !tbaa !45
  %add.ptr.i.i.i.i = getelementptr inbounds %struct.GroupByColumn, %struct.GroupByColumn* %cond.i.i.i.i.i, i64 %sub.ptr.div.i.i.i
  %_M_end_of_storage.i.i.i.i = getelementptr inbounds %"struct.std::pair.28", %"struct.std::pair.28"* %ref.tmp85, i64 0, i32 1, i32 0, i32 0, i32 2
  store %struct.GroupByColumn* %add.ptr.i.i.i.i, %struct.GroupByColumn** %_M_end_of_storage.i.i.i.i, align 8, !tbaa !46
  br i1 %cmp.not.i.i.i.i.i, label %invoke.cont88, label %if.then.i.i.i.i.i.i.i.i.i

if.then.i.i.i.i.i.i.i.i.i:                        ; preds = %invoke.cont.i.i175
  %26 = bitcast %struct.GroupByColumn* %cond.i.i.i.i.i to i8*
  %27 = bitcast %struct.GroupByColumn* %groupByTuple.sroa.0.0.lcssa to i8*
  call void @llvm.memcpy.p0i8.p0i8.i64(i8* align 8 %26, i8* align 8 %27, i64 %sub.ptr.sub.i.i.i, i1 false) #20
  br label %invoke.cont88

for.body36:                                       ; preds = %for.body36.preheader, %invoke.cont78
  %indvars.iv = phi i64 [ 0, %for.body36.preheader ], [ %indvars.iv.next, %invoke.cont78 ]
  %groupByTuple.sroa.18.0261 = phi %struct.GroupByColumn* [ null, %for.body36.preheader ], [ %groupByTuple.sroa.18.1, %invoke.cont78 ]
  %groupByTuple.sroa.11.0260 = phi %struct.GroupByColumn* [ null, %for.body36.preheader ], [ %groupByTuple.sroa.11.1, %invoke.cont78 ]
  %groupByTuple.sroa.0.0258 = phi %struct.GroupByColumn* [ null, %for.body36.preheader ], [ %groupByTuple.sroa.0.1, %invoke.cont78 ]
  %arrayidx40 = getelementptr inbounds i32, i32* %groupByColIdx, i64 %indvars.iv
  %28 = load i32, i32* %arrayidx40, align 4, !tbaa !25
  %idxprom41 = zext i32 %28 to i64
  %arrayidx42 = getelementptr inbounds i32, i32* %types, i64 %idxprom41
  %29 = load i32, i32* %arrayidx42, align 4, !tbaa !25
  switch i32 %29, label %sw.epilog74 [
    i32 1, label %sw.bb43
    i32 2, label %sw.bb51
    i32 3, label %sw.bb62
  ]

sw.bb43:                                          ; preds = %for.body36
  %arrayidx46 = getelementptr inbounds i8*, i8** %head, i64 %idxprom41
  %30 = bitcast i8** %arrayidx46 to i32**
  %31 = load i32*, i32** %30, align 8, !tbaa !19
  %call50 = invoke i8* @omni_allocate(i64 4)
          to label %invoke.cont unwind label %lpad

invoke.cont:                                      ; preds = %sw.bb43
  %add.ptr49 = getelementptr inbounds i32, i32* %31, i64 %combinedHash.sroa.9.0.lcssa
  %32 = bitcast i8* %call50 to i32*
  %33 = load i32, i32* %add.ptr49, align 4, !tbaa !25
  store i32 %33, i32* %32, align 4, !tbaa !25
  br label %sw.epilog74

lpad:                                             ; preds = %sw.bb43
  %34 = landingpad { i8*, i32 }
          cleanup
  br label %ehcleanup94

sw.bb51:                                          ; preds = %for.body36
  %arrayidx54 = getelementptr inbounds i8*, i8** %head, i64 %idxprom41
  %35 = bitcast i8** %arrayidx54 to i64**
  %36 = load i64*, i64** %35, align 8, !tbaa !19
  %call61 = invoke i8* @omni_allocate(i64 8)
          to label %invoke.cont60 unwind label %lpad59

invoke.cont60:                                    ; preds = %sw.bb51
  %add.ptr57 = getelementptr inbounds i64, i64* %36, i64 %combinedHash.sroa.9.0.lcssa
  %37 = bitcast i8* %call61 to i64*
  %38 = load i64, i64* %add.ptr57, align 8, !tbaa !23
  store i64 %38, i64* %37, align 8, !tbaa !23
  br label %sw.epilog74

lpad59:                                           ; preds = %sw.bb51
  %39 = landingpad { i8*, i32 }
          cleanup
  br label %ehcleanup94

sw.bb62:                                          ; preds = %for.body36
  %arrayidx65 = getelementptr inbounds i8*, i8** %head, i64 %idxprom41
  %40 = bitcast i8** %arrayidx65 to double**
  %41 = load double*, double** %40, align 8, !tbaa !19
  %call72 = invoke i8* @omni_allocate(i64 8)
          to label %invoke.cont71 unwind label %lpad70

invoke.cont71:                                    ; preds = %sw.bb62
  %add.ptr68 = getelementptr inbounds double, double* %41, i64 %combinedHash.sroa.9.0.lcssa
  %42 = bitcast i8* %call72 to double*
  %43 = load double, double* %add.ptr68, align 8, !tbaa !37
  store double %43, double* %42, align 8, !tbaa !37
  br label %sw.epilog74

lpad70:                                           ; preds = %sw.bb62
  %44 = landingpad { i8*, i32 }
          cleanup
  br label %ehcleanup94

sw.epilog74:                                      ; preds = %for.body36, %invoke.cont71, %invoke.cont60, %invoke.cont
  %rowPtr37.0 = phi i8* [ null, %for.body36 ], [ %call72, %invoke.cont71 ], [ %call61, %invoke.cont60 ], [ %call50, %invoke.cont ]
  %45 = load i32, i32* %arrayidx42, align 4, !tbaa !25
  %cmp.not.i = icmp eq %struct.GroupByColumn* %groupByTuple.sroa.11.0260, %groupByTuple.sroa.18.0261
  br i1 %cmp.not.i, label %if.else.i, label %if.then.i

if.then.i:                                        ; preds = %sw.epilog74
  %groupCol.sroa.0.0..sroa_idx = getelementptr inbounds %struct.GroupByColumn, %struct.GroupByColumn* %groupByTuple.sroa.11.0260, i64 0, i32 0
  store i32 %45, i32* %groupCol.sroa.0.0..sroa_idx, align 8, !tbaa.struct !35
  %groupCol.sroa.6186.0..sroa_idx189 = getelementptr inbounds %struct.GroupByColumn, %struct.GroupByColumn* %groupByTuple.sroa.11.0260, i64 0, i32 1
  store i8* %rowPtr37.0, i8** %groupCol.sroa.6186.0..sroa_idx189, align 8, !tbaa.struct !36
  br label %invoke.cont78

if.else.i:                                        ; preds = %sw.epilog74
  %sub.ptr.lhs.cast.i28.i.i.i = ptrtoint %struct.GroupByColumn* %groupByTuple.sroa.18.0261 to i64
  %sub.ptr.rhs.cast.i29.i.i.i = ptrtoint %struct.GroupByColumn* %groupByTuple.sroa.0.0258 to i64
  %sub.ptr.sub.i30.i.i.i = sub i64 %sub.ptr.lhs.cast.i28.i.i.i, %sub.ptr.rhs.cast.i29.i.i.i
  %sub.ptr.div.i31.i.i.i = ashr exact i64 %sub.ptr.sub.i30.i.i.i, 4
  %cmp.i.i.i.i = icmp eq i64 %sub.ptr.sub.i30.i.i.i, 0
  %.sroa.speculated.i.i.i = select i1 %cmp.i.i.i.i, i64 1, i64 %sub.ptr.div.i31.i.i.i
  %add.i.i.i = add nsw i64 %.sroa.speculated.i.i.i, %sub.ptr.div.i31.i.i.i
  %cmp7.i.i.i = icmp ult i64 %add.i.i.i, %sub.ptr.div.i31.i.i.i
  %cmp9.i.i.i = icmp ugt i64 %add.i.i.i, 1152921504606846975
  %or.cond.i.i.i = or i1 %cmp7.i.i.i, %cmp9.i.i.i
  %cond.i.i.i = select i1 %or.cond.i.i.i, i64 1152921504606846975, i64 %add.i.i.i
  %mul.i.i.i.i.i = shl nuw i64 %cond.i.i.i, 4
  %call2.i.i.i.i.i172 = invoke noalias nonnull i8* @_Znwm(i64 %mul.i.i.i.i.i) #19
          to label %call2.i.i.i.i.i.noexc unwind label %lpad77

call2.i.i.i.i.i.noexc:                            ; preds = %if.else.i
  %46 = bitcast i8* %call2.i.i.i.i.i172 to %struct.GroupByColumn*
  %groupCol.sroa.0.0..sroa_idx179 = getelementptr inbounds %struct.GroupByColumn, %struct.GroupByColumn* %46, i64 %sub.ptr.div.i31.i.i.i, i32 0
  store i32 %45, i32* %groupCol.sroa.0.0..sroa_idx179, align 8, !tbaa.struct !35
  %groupCol.sroa.6186.0..sroa_idx190 = getelementptr inbounds %struct.GroupByColumn, %struct.GroupByColumn* %46, i64 %sub.ptr.div.i31.i.i.i, i32 1
  store i8* %rowPtr37.0, i8** %groupCol.sroa.6186.0..sroa_idx190, align 8, !tbaa.struct !36
  br i1 %cmp.i.i.i.i, label %invoke.cont15.i.i, label %if.then.i.i.i.i.i.i.i.i76.i.i

if.then.i.i.i.i.i.i.i.i76.i.i:                    ; preds = %call2.i.i.i.i.i.noexc
  %47 = bitcast %struct.GroupByColumn* %groupByTuple.sroa.0.0258 to i8*
  call void @llvm.memmove.p0i8.p0i8.i64(i8* nonnull align 8 %call2.i.i.i.i.i172, i8* align 8 %47, i64 %sub.ptr.sub.i30.i.i.i, i1 false) #20
  br label %invoke.cont15.i.i

invoke.cont15.i.i:                                ; preds = %call2.i.i.i.i.i.noexc, %if.then.i.i.i.i.i.i.i.i76.i.i
  %add.ptr.i.i.i.i.i.i.i.i78.i.i = getelementptr inbounds %struct.GroupByColumn, %struct.GroupByColumn* %46, i64 %sub.ptr.div.i31.i.i.i
  %tobool.not.i68.i.i = icmp eq %struct.GroupByColumn* %groupByTuple.sroa.0.0258, null
  br i1 %tobool.not.i68.i.i, label %_ZNSt6vectorI13GroupByColumnSaIS0_EE17_M_realloc_insertIJRKS0_EEEvN9__gnu_cxx17__normal_iteratorIPS0_S2_EEDpOT_.exit.i, label %if.then.i69.i.i

if.then.i69.i.i:                                  ; preds = %invoke.cont15.i.i
  %48 = bitcast %struct.GroupByColumn* %groupByTuple.sroa.0.0258 to i8*
  call void @_ZdlPv(i8* nonnull %48) #20
  br label %_ZNSt6vectorI13GroupByColumnSaIS0_EE17_M_realloc_insertIJRKS0_EEEvN9__gnu_cxx17__normal_iteratorIPS0_S2_EEDpOT_.exit.i

_ZNSt6vectorI13GroupByColumnSaIS0_EE17_M_realloc_insertIJRKS0_EEEvN9__gnu_cxx17__normal_iteratorIPS0_S2_EEDpOT_.exit.i: ; preds = %if.then.i69.i.i, %invoke.cont15.i.i
  %add.ptr39.i.i = getelementptr inbounds %struct.GroupByColumn, %struct.GroupByColumn* %46, i64 %cond.i.i.i
  br label %invoke.cont78

invoke.cont78:                                    ; preds = %_ZNSt6vectorI13GroupByColumnSaIS0_EE17_M_realloc_insertIJRKS0_EEEvN9__gnu_cxx17__normal_iteratorIPS0_S2_EEDpOT_.exit.i, %if.then.i
  %groupByTuple.sroa.0.1 = phi %struct.GroupByColumn* [ %46, %_ZNSt6vectorI13GroupByColumnSaIS0_EE17_M_realloc_insertIJRKS0_EEEvN9__gnu_cxx17__normal_iteratorIPS0_S2_EEDpOT_.exit.i ], [ %groupByTuple.sroa.0.0258, %if.then.i ]
  %add.ptr.i.i.i.i.i.i.i.i78.i.i.pn = phi %struct.GroupByColumn* [ %add.ptr.i.i.i.i.i.i.i.i78.i.i, %_ZNSt6vectorI13GroupByColumnSaIS0_EE17_M_realloc_insertIJRKS0_EEEvN9__gnu_cxx17__normal_iteratorIPS0_S2_EEDpOT_.exit.i ], [ %groupByTuple.sroa.11.0260, %if.then.i ]
  %groupByTuple.sroa.18.1 = phi %struct.GroupByColumn* [ %add.ptr39.i.i, %_ZNSt6vectorI13GroupByColumnSaIS0_EE17_M_realloc_insertIJRKS0_EEEvN9__gnu_cxx17__normal_iteratorIPS0_S2_EEDpOT_.exit.i ], [ %groupByTuple.sroa.18.0261, %if.then.i ]
  %groupByTuple.sroa.11.1 = getelementptr inbounds %struct.GroupByColumn, %struct.GroupByColumn* %add.ptr.i.i.i.i.i.i.i.i78.i.i.pn, i64 1
  %indvars.iv.next = add nuw nsw i64 %indvars.iv, 1
  %exitcond.not = icmp eq i64 %indvars.iv.next, %wide.trip.count
  br i1 %exitcond.not, label %for.cond.cleanup35.loopexit, label %for.body36, !llvm.loop !56

lpad77:                                           ; preds = %if.else.i
  %49 = landingpad { i8*, i32 }
          cleanup
  br label %ehcleanup94

invoke.cont88:                                    ; preds = %if.then.i.i.i.i.i.i.i.i.i, %invoke.cont.i.i175
  store %struct.GroupByColumn* %add.ptr.i.i.i.i, %struct.GroupByColumn** %_M_finish.i.i.i.i, align 8, !tbaa !45
  %50 = getelementptr inbounds %"class.std::unordered_map", %"class.std::unordered_map"* %groupedRows, i64 0, i32 0
  %call3.i.i170 = invoke { %"struct.std::__detail::_Hash_node"*, i8 } @_ZNSt10_HashtableImSt4pairIKmSt6vectorI13GroupByColumnSaIS3_EEESaIS6_ENSt8__detail10_Select1stESt8equal_toImESt4hashImENS8_18_Mod_range_hashingENS8_20_Default_ranged_hashENS8_20_Prime_rehash_policyENS8_17_Hashtable_traitsILb0ELb0ELb1EEEE10_M_emplaceIJS6_EEES0_INS8_14_Node_iteratorIS6_Lb0ELb0EEEbESt17integral_constantIbLb1EEDpOT_(%"class.std::_Hashtable"* nonnull dereferenceable(56) %50, %"struct.std::pair.28"* nonnull align 8 dereferenceable(32) %ref.tmp85)
          to label %invoke.cont90 unwind label %lpad89

invoke.cont90:                                    ; preds = %invoke.cont88
  %_M_start.i.i.i166 = getelementptr inbounds %"struct.std::pair.28", %"struct.std::pair.28"* %ref.tmp85, i64 0, i32 1, i32 0, i32 0, i32 0
  %51 = load %struct.GroupByColumn*, %struct.GroupByColumn** %_M_start.i.i.i166, align 8, !tbaa !42
  %tobool.not.i.i.i.i167 = icmp eq %struct.GroupByColumn* %51, null
  br i1 %tobool.not.i.i.i.i167, label %_ZNSt4pairIKmSt6vectorI13GroupByColumnSaIS2_EEED2Ev.exit169, label %if.then.i.i.i.i168

if.then.i.i.i.i168:                               ; preds = %invoke.cont90
  %52 = bitcast %struct.GroupByColumn* %51 to i8*
  call void @_ZdlPv(i8* nonnull %52) #20
  br label %_ZNSt4pairIKmSt6vectorI13GroupByColumnSaIS2_EEED2Ev.exit169

_ZNSt4pairIKmSt6vectorI13GroupByColumnSaIS2_EEED2Ev.exit169: ; preds = %invoke.cont90, %if.then.i.i.i.i168
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %23) #20
  %tobool.not.i.i.i163 = icmp eq %struct.GroupByColumn* %groupByTuple.sroa.0.0.lcssa, null
  br i1 %tobool.not.i.i.i163, label %if.end, label %if.then.i.i.i164

if.then.i.i.i164:                                 ; preds = %_ZNSt4pairIKmSt6vectorI13GroupByColumnSaIS2_EEED2Ev.exit169
  %53 = bitcast %struct.GroupByColumn* %groupByTuple.sroa.0.0.lcssa to i8*
  call void @_ZdlPv(i8* nonnull %53) #20
  br label %if.end

lpad87:                                           ; preds = %_ZNSt16allocator_traitsISaI13GroupByColumnEE8allocateERS1_m.exit.i.i.i.i.i, %if.then.i.i.i.i.i.i.i
  %54 = landingpad { i8*, i32 }
          cleanup
  br label %ehcleanup93

lpad89:                                           ; preds = %invoke.cont88
  %55 = landingpad { i8*, i32 }
          cleanup
  %_M_start.i.i.i = getelementptr inbounds %"struct.std::pair.28", %"struct.std::pair.28"* %ref.tmp85, i64 0, i32 1, i32 0, i32 0, i32 0
  %56 = load %struct.GroupByColumn*, %struct.GroupByColumn** %_M_start.i.i.i, align 8, !tbaa !42
  %tobool.not.i.i.i.i161 = icmp eq %struct.GroupByColumn* %56, null
  br i1 %tobool.not.i.i.i.i161, label %ehcleanup93, label %if.then.i.i.i.i

if.then.i.i.i.i:                                  ; preds = %lpad89
  %57 = bitcast %struct.GroupByColumn* %56 to i8*
  call void @_ZdlPv(i8* nonnull %57) #20
  br label %ehcleanup93

ehcleanup93:                                      ; preds = %if.then.i.i.i.i, %lpad89, %lpad87
  %.pn = phi { i8*, i32 } [ %54, %lpad87 ], [ %55, %lpad89 ], [ %55, %if.then.i.i.i.i ]
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %23) #20
  br label %ehcleanup94

ehcleanup94:                                      ; preds = %lpad, %lpad59, %lpad70, %lpad77, %ehcleanup93
  %groupByTuple.sroa.0.0252 = phi %struct.GroupByColumn* [ %groupByTuple.sroa.0.0.lcssa, %ehcleanup93 ], [ %groupByTuple.sroa.0.0258, %lpad77 ], [ %groupByTuple.sroa.0.0258, %lpad70 ], [ %groupByTuple.sroa.0.0258, %lpad59 ], [ %groupByTuple.sroa.0.0258, %lpad ]
  %.pn158.pn = phi { i8*, i32 } [ %.pn, %ehcleanup93 ], [ %49, %lpad77 ], [ %44, %lpad70 ], [ %39, %lpad59 ], [ %34, %lpad ]
  %tobool.not.i.i.i = icmp eq %struct.GroupByColumn* %groupByTuple.sroa.0.0252, null
  br i1 %tobool.not.i.i.i, label %_ZNSt6vectorI13GroupByColumnSaIS0_EED2Ev.exit, label %if.then.i.i.i

if.then.i.i.i:                                    ; preds = %ehcleanup94
  %58 = bitcast %struct.GroupByColumn* %groupByTuple.sroa.0.0252 to i8*
  call void @_ZdlPv(i8* nonnull %58) #20
  br label %_ZNSt6vectorI13GroupByColumnSaIS0_EED2Ev.exit

_ZNSt6vectorI13GroupByColumnSaIS0_EED2Ev.exit:    ; preds = %ehcleanup94, %if.then.i.i.i
  resume { i8*, i32 } %.pn158.pn

if.end:                                           ; preds = %for.cond.i.i.i.i, %if.then.i.i.i164, %_ZNSt4pairIKmSt6vectorI13GroupByColumnSaIS2_EEED2Ev.exit169, %if.end.i.i.i.i
  %aggregators = getelementptr inbounds %class.HashGroupBy, %class.HashGroupBy* %this, i64 0, i32 3
  call void @processAgg(i64 %combinedHash.sroa.0.0.lcssa, %"class.std::vector.0"* nonnull align 8 dereferenceable(24) %aggregators, i32* %aggFuncTypes, i32 %aggColNum, i32* %types, i32* %aggColIdx, i8** %head, i32 %offset)
  ret void
}

; Function Attrs: argmemonly nofree nosync nounwind willreturn writeonly
declare void @llvm.memset.p0i8.i64(i8* nocapture writeonly, i8, i64, i1 immarg) #11

declare dso_local i8* @omni_allocate(i64) local_unnamed_addr #0

; Function Attrs: uwtable mustprogress
define dso_local void @_ZN11HashGroupBy7processEP5Tablej(%class.HashGroupBy* nonnull dereferenceable(144) %this, %class.Table* %table, i32 %rowCount) unnamed_addr #9 align 2 {
entry:
  %0 = bitcast %class.HashGroupBy* %this to void (%class.HashGroupBy*, %class.Table*)***
  %vtable = load void (%class.HashGroupBy*, %class.Table*)**, void (%class.HashGroupBy*, %class.Table*)*** %0, align 8, !tbaa !50
  %1 = load void (%class.HashGroupBy*, %class.Table*)*, void (%class.HashGroupBy*, %class.Table*)** %vtable, align 8
  tail call void %1(%class.HashGroupBy* nonnull dereferenceable(144) %this, %class.Table* %table)
  %_M_finish.i.i = getelementptr inbounds %class.Table, %class.Table* %table, i64 0, i32 3, i32 0, i32 0, i32 1
  %2 = load %class.Column**, %class.Column*** %_M_finish.i.i, align 8, !tbaa !57
  %_M_start.i.i = getelementptr inbounds %class.Table, %class.Table* %table, i64 0, i32 3, i32 0, i32 0, i32 0
  %3 = load %class.Column**, %class.Column*** %_M_start.i.i, align 8, !tbaa !29
  %sub.ptr.lhs.cast.i.i = ptrtoint %class.Column** %2 to i64
  %sub.ptr.rhs.cast.i.i = ptrtoint %class.Column** %3 to i64
  %sub.ptr.sub.i.i = sub i64 %sub.ptr.lhs.cast.i.i, %sub.ptr.rhs.cast.i.i
  %4 = lshr exact i64 %sub.ptr.sub.i.i, 3
  %conv.i = trunc i64 %4 to i32
  %5 = and i64 %sub.ptr.sub.i.i, 34359738360
  %call3.i = tail call noalias nonnull i8* @_Znam(i64 %5) #18
  %6 = bitcast i8* %call3.i to i8**
  %cmp17.not.i = icmp eq i32 %conv.i, 0
  br i1 %cmp17.not.i, label %_ZN5Table8getHeadsEv.exit, label %for.body.preheader.i

for.body.preheader.i:                             ; preds = %entry
  %wide.trip.count.i = and i64 %4, 4294967295
  %7 = add nsw i64 %wide.trip.count.i, -1
  %xtraiter133 = and i64 %4, 3
  %8 = icmp ult i64 %7, 3
  br i1 %8, label %_ZN5Table8getHeadsEv.exit.loopexit.unr-lcssa, label %for.body.preheader.i.new

for.body.preheader.i.new:                         ; preds = %for.body.preheader.i
  %unroll_iter135 = sub nsw i64 %wide.trip.count.i, %xtraiter133
  br label %for.body.i

for.body.i:                                       ; preds = %for.body.i, %for.body.preheader.i.new
  %indvars.iv.i = phi i64 [ 0, %for.body.preheader.i.new ], [ %indvars.iv.next.i.3, %for.body.i ]
  %niter136 = phi i64 [ %unroll_iter135, %for.body.preheader.i.new ], [ %niter136.nsub.3, %for.body.i ]
  %add.ptr.i.i = getelementptr inbounds %class.Column*, %class.Column** %3, i64 %indvars.iv.i
  %9 = load %class.Column*, %class.Column** %add.ptr.i.i, align 8, !tbaa !19
  %data.i.i = getelementptr inbounds %class.Column, %class.Column* %9, i64 0, i32 1
  %10 = load i8*, i8** %data.i.i, align 8, !tbaa !34
  %arrayidx.i = getelementptr inbounds i8*, i8** %6, i64 %indvars.iv.i
  store i8* %10, i8** %arrayidx.i, align 8, !tbaa !19
  %indvars.iv.next.i = or i64 %indvars.iv.i, 1
  %add.ptr.i.i.1 = getelementptr inbounds %class.Column*, %class.Column** %3, i64 %indvars.iv.next.i
  %11 = load %class.Column*, %class.Column** %add.ptr.i.i.1, align 8, !tbaa !19
  %data.i.i.1 = getelementptr inbounds %class.Column, %class.Column* %11, i64 0, i32 1
  %12 = load i8*, i8** %data.i.i.1, align 8, !tbaa !34
  %arrayidx.i.1 = getelementptr inbounds i8*, i8** %6, i64 %indvars.iv.next.i
  store i8* %12, i8** %arrayidx.i.1, align 8, !tbaa !19
  %indvars.iv.next.i.1 = or i64 %indvars.iv.i, 2
  %add.ptr.i.i.2 = getelementptr inbounds %class.Column*, %class.Column** %3, i64 %indvars.iv.next.i.1
  %13 = load %class.Column*, %class.Column** %add.ptr.i.i.2, align 8, !tbaa !19
  %data.i.i.2 = getelementptr inbounds %class.Column, %class.Column* %13, i64 0, i32 1
  %14 = load i8*, i8** %data.i.i.2, align 8, !tbaa !34
  %arrayidx.i.2 = getelementptr inbounds i8*, i8** %6, i64 %indvars.iv.next.i.1
  store i8* %14, i8** %arrayidx.i.2, align 8, !tbaa !19
  %indvars.iv.next.i.2 = or i64 %indvars.iv.i, 3
  %add.ptr.i.i.3 = getelementptr inbounds %class.Column*, %class.Column** %3, i64 %indvars.iv.next.i.2
  %15 = load %class.Column*, %class.Column** %add.ptr.i.i.3, align 8, !tbaa !19
  %data.i.i.3 = getelementptr inbounds %class.Column, %class.Column* %15, i64 0, i32 1
  %16 = load i8*, i8** %data.i.i.3, align 8, !tbaa !34
  %arrayidx.i.3 = getelementptr inbounds i8*, i8** %6, i64 %indvars.iv.next.i.2
  store i8* %16, i8** %arrayidx.i.3, align 8, !tbaa !19
  %indvars.iv.next.i.3 = add nuw nsw i64 %indvars.iv.i, 4
  %niter136.nsub.3 = add i64 %niter136, -4
  %niter136.ncmp.3 = icmp eq i64 %niter136.nsub.3, 0
  br i1 %niter136.ncmp.3, label %_ZN5Table8getHeadsEv.exit.loopexit.unr-lcssa, label %for.body.i, !llvm.loop !58

_ZN5Table8getHeadsEv.exit.loopexit.unr-lcssa:     ; preds = %for.body.i, %for.body.preheader.i
  %indvars.iv.i.unr = phi i64 [ 0, %for.body.preheader.i ], [ %indvars.iv.next.i.3, %for.body.i ]
  %lcmp.mod134.not = icmp eq i64 %xtraiter133, 0
  br i1 %lcmp.mod134.not, label %_ZN5Table8getHeadsEv.exit, label %for.body.i.epil

for.body.i.epil:                                  ; preds = %_ZN5Table8getHeadsEv.exit.loopexit.unr-lcssa, %for.body.i.epil
  %indvars.iv.i.epil = phi i64 [ %indvars.iv.next.i.epil, %for.body.i.epil ], [ %indvars.iv.i.unr, %_ZN5Table8getHeadsEv.exit.loopexit.unr-lcssa ]
  %epil.iter = phi i64 [ %epil.iter.sub, %for.body.i.epil ], [ %xtraiter133, %_ZN5Table8getHeadsEv.exit.loopexit.unr-lcssa ]
  %add.ptr.i.i.epil = getelementptr inbounds %class.Column*, %class.Column** %3, i64 %indvars.iv.i.epil
  %17 = load %class.Column*, %class.Column** %add.ptr.i.i.epil, align 8, !tbaa !19
  %data.i.i.epil = getelementptr inbounds %class.Column, %class.Column* %17, i64 0, i32 1
  %18 = load i8*, i8** %data.i.i.epil, align 8, !tbaa !34
  %arrayidx.i.epil = getelementptr inbounds i8*, i8** %6, i64 %indvars.iv.i.epil
  store i8* %18, i8** %arrayidx.i.epil, align 8, !tbaa !19
  %indvars.iv.next.i.epil = add nuw nsw i64 %indvars.iv.i.epil, 1
  %epil.iter.sub = add i64 %epil.iter, -1
  %epil.iter.cmp.not = icmp eq i64 %epil.iter.sub, 0
  br i1 %epil.iter.cmp.not, label %_ZN5Table8getHeadsEv.exit, label %for.body.i.epil, !llvm.loop !59

_ZN5Table8getHeadsEv.exit:                        ; preds = %_ZN5Table8getHeadsEv.exit.loopexit.unr-lcssa, %for.body.i.epil, %entry
  %types.i = getelementptr inbounds %class.Table, %class.Table* %table, i64 0, i32 4
  %19 = load i32*, i32** %types.i, align 8, !tbaa !61
  %_M_finish.i = getelementptr inbounds %class.HashGroupBy, %class.HashGroupBy* %this, i64 0, i32 1, i32 0, i32 0, i32 1
  %20 = load %struct.ColumnIndex*, %struct.ColumnIndex** %_M_finish.i, align 8, !tbaa !2
  %_M_start.i = getelementptr inbounds %class.HashGroupBy, %class.HashGroupBy* %this, i64 0, i32 1, i32 0, i32 0, i32 0
  %21 = load %struct.ColumnIndex*, %struct.ColumnIndex** %_M_start.i, align 8, !tbaa !8
  %sub.ptr.lhs.cast.i = ptrtoint %struct.ColumnIndex* %20 to i64
  %sub.ptr.rhs.cast.i = ptrtoint %struct.ColumnIndex* %21 to i64
  %sub.ptr.sub.i = sub i64 %sub.ptr.lhs.cast.i, %sub.ptr.rhs.cast.i
  %22 = lshr exact i64 %sub.ptr.sub.i, 3
  %conv = trunc i64 %22 to i32
  %sext = shl i64 %sub.ptr.sub.i, 29
  %conv5 = ashr exact i64 %sext, 32
  %23 = tail call { i64, i1 } @llvm.umul.with.overflow.i64(i64 %conv5, i64 4)
  %24 = extractvalue { i64, i1 } %23, 1
  %25 = extractvalue { i64, i1 } %23, 0
  %26 = select i1 %24, i64 -1, i64 %25
  %call6 = tail call noalias nonnull i8* @_Znam(i64 %26) #18
  %27 = bitcast i8* %call6 to i32*
  %_M_finish.i112 = getelementptr inbounds %class.HashGroupBy, %class.HashGroupBy* %this, i64 0, i32 2, i32 0, i32 0, i32 1
  %28 = load %struct.ColumnIndex*, %struct.ColumnIndex** %_M_finish.i112, align 8, !tbaa !2
  %_M_start.i113 = getelementptr inbounds %class.HashGroupBy, %class.HashGroupBy* %this, i64 0, i32 2, i32 0, i32 0, i32 0
  %29 = load %struct.ColumnIndex*, %struct.ColumnIndex** %_M_start.i113, align 8, !tbaa !8
  %sub.ptr.lhs.cast.i114 = ptrtoint %struct.ColumnIndex* %28 to i64
  %sub.ptr.rhs.cast.i115 = ptrtoint %struct.ColumnIndex* %29 to i64
  %sub.ptr.sub.i116 = sub i64 %sub.ptr.lhs.cast.i114, %sub.ptr.rhs.cast.i115
  %30 = lshr exact i64 %sub.ptr.sub.i116, 3
  %conv8 = trunc i64 %30 to i32
  %sext102 = shl i64 %sub.ptr.sub.i116, 29
  %conv9 = ashr exact i64 %sext102, 32
  %31 = tail call { i64, i1 } @llvm.umul.with.overflow.i64(i64 %conv9, i64 4)
  %32 = extractvalue { i64, i1 } %31, 1
  %33 = extractvalue { i64, i1 } %31, 0
  %34 = select i1 %32, i64 -1, i64 %33
  %call10 = tail call noalias nonnull i8* @_Znam(i64 %34) #18
  %35 = bitcast i8* %call10 to i32*
  %call12 = tail call noalias nonnull i8* @_Znam(i64 %34) #18
  %36 = bitcast i8* %call12 to i32*
  %cmp122 = icmp sgt i32 %conv, 0
  br i1 %cmp122, label %for.body.lr.ph, label %for.cond23.preheader

for.body.lr.ph:                                   ; preds = %_ZN5Table8getHeadsEv.exit
  %inputColTypes = getelementptr inbounds %class.HashGroupBy, %class.HashGroupBy* %this, i64 0, i32 5
  %37 = load i32*, i32** %inputColTypes, align 8, !tbaa !9
  %wide.trip.count127 = and i64 %22, 4294967295
  %xtraiter129 = and i64 %22, 1
  %38 = icmp eq i64 %wide.trip.count127, 1
  br i1 %38, label %for.cond23.preheader.loopexit.unr-lcssa, label %for.body.lr.ph.new

for.body.lr.ph.new:                               ; preds = %for.body.lr.ph
  %unroll_iter131 = sub nsw i64 %wide.trip.count127, %xtraiter129
  br label %for.body

for.cond23.preheader.loopexit.unr-lcssa:          ; preds = %for.body, %for.body.lr.ph
  %indvars.iv125.unr = phi i64 [ 0, %for.body.lr.ph ], [ %indvars.iv.next126.1, %for.body ]
  %lcmp.mod130.not = icmp eq i64 %xtraiter129, 0
  br i1 %lcmp.mod130.not, label %for.cond23.preheader, label %for.body.epil

for.body.epil:                                    ; preds = %for.cond23.preheader.loopexit.unr-lcssa
  %idx.epil = getelementptr inbounds %struct.ColumnIndex, %struct.ColumnIndex* %21, i64 %indvars.iv125.unr, i32 0
  %39 = load i32, i32* %idx.epil, align 4, !tbaa !65
  %arrayidx.epil = getelementptr inbounds i32, i32* %27, i64 %indvars.iv125.unr
  store i32 %39, i32* %arrayidx.epil, align 4, !tbaa !25
  %40 = load i32, i32* %idx.epil, align 4, !tbaa !65
  %idxprom20.epil = zext i32 %40 to i64
  %arrayidx21.epil = getelementptr inbounds i32, i32* %37, i64 %idxprom20.epil
  store i32 0, i32* %arrayidx21.epil, align 4, !tbaa !25
  br label %for.cond23.preheader

for.cond23.preheader:                             ; preds = %for.body.epil, %for.cond23.preheader.loopexit.unr-lcssa, %_ZN5Table8getHeadsEv.exit
  %cmp24120 = icmp sgt i32 %conv8, 0
  br i1 %cmp24120, label %for.body26.lr.ph, label %for.cond49.preheader

for.body26.lr.ph:                                 ; preds = %for.cond23.preheader
  %inputColTypes33 = getelementptr inbounds %class.HashGroupBy, %class.HashGroupBy* %this, i64 0, i32 5
  %41 = load i32*, i32** %inputColTypes33, align 8, !tbaa !9
  %_M_start.i103 = getelementptr inbounds %class.HashGroupBy, %class.HashGroupBy* %this, i64 0, i32 3, i32 0, i32 0, i32 0
  %42 = load %class.Aggregator**, %class.Aggregator*** %_M_start.i103, align 8, !tbaa !47
  %wide.trip.count = and i64 %30, 4294967295
  %xtraiter = and i64 %30, 1
  %43 = icmp eq i64 %wide.trip.count, 1
  br i1 %43, label %for.cond49.preheader.loopexit.unr-lcssa, label %for.body26.lr.ph.new

for.body26.lr.ph.new:                             ; preds = %for.body26.lr.ph
  %unroll_iter = sub nsw i64 %wide.trip.count, %xtraiter
  br label %for.body26

for.body:                                         ; preds = %for.body, %for.body.lr.ph.new
  %indvars.iv125 = phi i64 [ 0, %for.body.lr.ph.new ], [ %indvars.iv.next126.1, %for.body ]
  %niter132 = phi i64 [ %unroll_iter131, %for.body.lr.ph.new ], [ %niter132.nsub.1, %for.body ]
  %idx = getelementptr inbounds %struct.ColumnIndex, %struct.ColumnIndex* %21, i64 %indvars.iv125, i32 0
  %44 = load i32, i32* %idx, align 4, !tbaa !65
  %arrayidx = getelementptr inbounds i32, i32* %27, i64 %indvars.iv125
  store i32 %44, i32* %arrayidx, align 4, !tbaa !25
  %45 = load i32, i32* %idx, align 4, !tbaa !65
  %idxprom20 = zext i32 %45 to i64
  %arrayidx21 = getelementptr inbounds i32, i32* %37, i64 %idxprom20
  store i32 0, i32* %arrayidx21, align 4, !tbaa !25
  %indvars.iv.next126 = or i64 %indvars.iv125, 1
  %idx.1 = getelementptr inbounds %struct.ColumnIndex, %struct.ColumnIndex* %21, i64 %indvars.iv.next126, i32 0
  %46 = load i32, i32* %idx.1, align 4, !tbaa !65
  %arrayidx.1 = getelementptr inbounds i32, i32* %27, i64 %indvars.iv.next126
  store i32 %46, i32* %arrayidx.1, align 4, !tbaa !25
  %47 = load i32, i32* %idx.1, align 4, !tbaa !65
  %idxprom20.1 = zext i32 %47 to i64
  %arrayidx21.1 = getelementptr inbounds i32, i32* %37, i64 %idxprom20.1
  store i32 0, i32* %arrayidx21.1, align 4, !tbaa !25
  %indvars.iv.next126.1 = add nuw nsw i64 %indvars.iv125, 2
  %niter132.nsub.1 = add i64 %niter132, -2
  %niter132.ncmp.1 = icmp eq i64 %niter132.nsub.1, 0
  br i1 %niter132.ncmp.1, label %for.cond23.preheader.loopexit.unr-lcssa, label %for.body, !llvm.loop !67

for.cond49.preheader.loopexit.unr-lcssa:          ; preds = %for.body26, %for.body26.lr.ph
  %indvars.iv.unr = phi i64 [ 0, %for.body26.lr.ph ], [ %indvars.iv.next.1, %for.body26 ]
  %lcmp.mod.not = icmp eq i64 %xtraiter, 0
  br i1 %lcmp.mod.not, label %for.cond49.preheader, label %for.body26.epil

for.body26.epil:                                  ; preds = %for.cond49.preheader.loopexit.unr-lcssa
  %idx30.epil = getelementptr inbounds %struct.ColumnIndex, %struct.ColumnIndex* %29, i64 %indvars.iv.unr, i32 0
  %48 = load i32, i32* %idx30.epil, align 4, !tbaa !65
  %arrayidx32.epil = getelementptr inbounds i32, i32* %35, i64 %indvars.iv.unr
  store i32 %48, i32* %arrayidx32.epil, align 4, !tbaa !25
  %49 = load i32, i32* %idx30.epil, align 4, !tbaa !65
  %idxprom38.epil = zext i32 %49 to i64
  %arrayidx39.epil = getelementptr inbounds i32, i32* %41, i64 %idxprom38.epil
  store i32 1, i32* %arrayidx39.epil, align 4, !tbaa !25
  %add.ptr.i.epil = getelementptr inbounds %class.Aggregator*, %class.Aggregator** %42, i64 %indvars.iv.unr
  %50 = load %class.Aggregator*, %class.Aggregator** %add.ptr.i.epil, align 8, !tbaa !19
  %type.i.epil = getelementptr inbounds %class.Aggregator, %class.Aggregator* %50, i64 0, i32 1
  %51 = load i32, i32* %type.i.epil, align 8, !tbaa !68
  %arrayidx44.epil = getelementptr inbounds i32, i32* %36, i64 %indvars.iv.unr
  store i32 %51, i32* %arrayidx44.epil, align 4, !tbaa !25
  br label %for.cond49.preheader

for.cond49.preheader:                             ; preds = %for.body26.epil, %for.cond49.preheader.loopexit.unr-lcssa, %for.cond23.preheader
  %cmp50118.not = icmp eq i32 %rowCount, 0
  br i1 %cmp50118.not, label %for.cond.cleanup51, label %for.body52

for.body26:                                       ; preds = %for.body26, %for.body26.lr.ph.new
  %indvars.iv = phi i64 [ 0, %for.body26.lr.ph.new ], [ %indvars.iv.next.1, %for.body26 ]
  %niter = phi i64 [ %unroll_iter, %for.body26.lr.ph.new ], [ %niter.nsub.1, %for.body26 ]
  %idx30 = getelementptr inbounds %struct.ColumnIndex, %struct.ColumnIndex* %29, i64 %indvars.iv, i32 0
  %52 = load i32, i32* %idx30, align 4, !tbaa !65
  %arrayidx32 = getelementptr inbounds i32, i32* %35, i64 %indvars.iv
  store i32 %52, i32* %arrayidx32, align 4, !tbaa !25
  %53 = load i32, i32* %idx30, align 4, !tbaa !65
  %idxprom38 = zext i32 %53 to i64
  %arrayidx39 = getelementptr inbounds i32, i32* %41, i64 %idxprom38
  store i32 1, i32* %arrayidx39, align 4, !tbaa !25
  %add.ptr.i = getelementptr inbounds %class.Aggregator*, %class.Aggregator** %42, i64 %indvars.iv
  %54 = load %class.Aggregator*, %class.Aggregator** %add.ptr.i, align 8, !tbaa !19
  %type.i = getelementptr inbounds %class.Aggregator, %class.Aggregator* %54, i64 0, i32 1
  %55 = load i32, i32* %type.i, align 8, !tbaa !68
  %arrayidx44 = getelementptr inbounds i32, i32* %36, i64 %indvars.iv
  store i32 %55, i32* %arrayidx44, align 4, !tbaa !25
  %indvars.iv.next = or i64 %indvars.iv, 1
  %idx30.1 = getelementptr inbounds %struct.ColumnIndex, %struct.ColumnIndex* %29, i64 %indvars.iv.next, i32 0
  %56 = load i32, i32* %idx30.1, align 4, !tbaa !65
  %arrayidx32.1 = getelementptr inbounds i32, i32* %35, i64 %indvars.iv.next
  store i32 %56, i32* %arrayidx32.1, align 4, !tbaa !25
  %57 = load i32, i32* %idx30.1, align 4, !tbaa !65
  %idxprom38.1 = zext i32 %57 to i64
  %arrayidx39.1 = getelementptr inbounds i32, i32* %41, i64 %idxprom38.1
  store i32 1, i32* %arrayidx39.1, align 4, !tbaa !25
  %add.ptr.i.1 = getelementptr inbounds %class.Aggregator*, %class.Aggregator** %42, i64 %indvars.iv.next
  %58 = load %class.Aggregator*, %class.Aggregator** %add.ptr.i.1, align 8, !tbaa !19
  %type.i.1 = getelementptr inbounds %class.Aggregator, %class.Aggregator* %58, i64 0, i32 1
  %59 = load i32, i32* %type.i.1, align 8, !tbaa !68
  %arrayidx44.1 = getelementptr inbounds i32, i32* %36, i64 %indvars.iv.next
  store i32 %59, i32* %arrayidx44.1, align 4, !tbaa !25
  %indvars.iv.next.1 = add nuw nsw i64 %indvars.iv, 2
  %niter.nsub.1 = add i64 %niter, -2
  %niter.ncmp.1 = icmp eq i64 %niter.nsub.1, 0
  br i1 %niter.ncmp.1, label %for.cond49.preheader.loopexit.unr-lcssa, label %for.body26, !llvm.loop !71

for.cond.cleanup51:                               ; preds = %for.body52, %for.cond49.preheader
  %vtable56 = load void (%class.HashGroupBy*, %class.Table*)**, void (%class.HashGroupBy*, %class.Table*)*** %0, align 8, !tbaa !50
  %vfn57 = getelementptr inbounds void (%class.HashGroupBy*, %class.Table*)*, void (%class.HashGroupBy*, %class.Table*)** %vtable56, i64 2
  %60 = load void (%class.HashGroupBy*, %class.Table*)*, void (%class.HashGroupBy*, %class.Table*)** %vfn57, align 8
  tail call void %60(%class.HashGroupBy* nonnull dereferenceable(144) %this, %class.Table* nonnull %table)
  tail call void @_ZdaPv(i8* nonnull %call6) #23
  tail call void @_ZdaPv(i8* nonnull %call10) #23
  tail call void @_ZdaPv(i8* nonnull %call12) #23
  ret void

for.body52:                                       ; preds = %for.cond49.preheader, %for.body52
  %i48.0119 = phi i32 [ %inc54, %for.body52 ], [ 0, %for.cond49.preheader ]
  tail call void @_ZN11HashGroupBy6inloopEPPcjPiiS2_iS2_iS2_(%class.HashGroupBy* nonnull dereferenceable(144) %this, i8** nonnull %6, i32 %i48.0119, i32* %19, i32 undef, i32* nonnull %27, i32 %conv, i32* nonnull %35, i32 %conv8, i32* nonnull %36)
  %inc54 = add nuw i32 %i48.0119, 1
  %exitcond.not = icmp eq i32 %inc54, %rowCount
  br i1 %exitcond.not, label %for.cond.cleanup51, label %for.body52, !llvm.loop !72
}

; Function Attrs: nobuiltin nounwind
declare dso_local void @_ZdaPv(i8*) local_unnamed_addr #12

; Function Attrs: uwtable
define dso_local void @JIT_hashGroupByExecute(%class.HashGroupBy* %op, %class.Table* %table) local_unnamed_addr #6 {
entry:
  %call = tail call i64 @_ZNSt6chrono3_V212system_clock3nowEv() #20
  %positionCount.i = getelementptr inbounds %class.Table, %class.Table* %table, i64 0, i32 5
  %0 = load i32, i32* %positionCount.i, align 8, !tbaa !73
  %1 = bitcast %class.HashGroupBy* %op to void (%class.HashGroupBy*, %class.Table*, i32)***
  %vtable = load void (%class.HashGroupBy*, %class.Table*, i32)**, void (%class.HashGroupBy*, %class.Table*, i32)*** %1, align 8, !tbaa !50
  %vfn = getelementptr inbounds void (%class.HashGroupBy*, %class.Table*, i32)*, void (%class.HashGroupBy*, %class.Table*, i32)** %vtable, i64 3
  %2 = load void (%class.HashGroupBy*, %class.Table*, i32)*, void (%class.HashGroupBy*, %class.Table*, i32)** %vfn, align 8
  tail call void %2(%class.HashGroupBy* nonnull dereferenceable(144) %op, %class.Table* nonnull %table, i32 %0)
  %call3 = tail call i64 @_ZNSt6chrono3_V212system_clock3nowEv() #20
  %sub.i.i31 = sub nsw i64 %call3, %call
  %conv.i.i.i25 = sitofp i64 %sub.i.i31 to float
  %div.i.i.i26 = fdiv float %conv.i.i.i25, 1.000000e+09
  %3 = load float, float* getelementptr inbounds (%"struct.std::chrono::duration", %"struct.std::chrono::duration"* @g_total_execute_time, i64 0, i32 0), align 4, !tbaa !74
  %add.i = fadd float %3, %div.i.i.i26
  store float %add.i, float* getelementptr inbounds (%"struct.std::chrono::duration", %"struct.std::chrono::duration"* @g_total_execute_time, i64 0, i32 0), align 4, !tbaa !74
  ret void
}

; Function Attrs: nounwind
declare dso_local i64 @_ZNSt6chrono3_V212system_clock3nowEv() local_unnamed_addr #1

; Function Attrs: uwtable
define dso_local void @_ZN11HashGroupBy15constructColumnEP5TablePijjiR8Iterator(%class.HashGroupBy* nocapture nonnull readonly dereferenceable(144) %this, %class.Table* %table, i32* nocapture readonly %types, i32 %groupByColSize, i32 %aggColSize, i32 %tableRowSize, %struct.Iterator* nocapture nonnull align 8 dereferenceable(32) %iterator) local_unnamed_addr #6 align 2 personality i8* bitcast (i32 (...)* @__gxx_personality_v0 to i8*) {
entry:
  %cmp685.not = icmp eq i32 %groupByColSize, 0
  br i1 %cmp685.not, label %for.cond22.preheader.thread, label %for.body.lr.ph

for.body.lr.ph:                                   ; preds = %entry
  %conv15 = sext i32 %tableRowSize to i64
  %mul16 = shl nsw i64 %conv15, 3
  %types.i434 = getelementptr inbounds %class.Table, %class.Table* %table, i64 0, i32 4
  %columnSize.i435 = getelementptr inbounds %class.Table, %class.Table* %table, i64 0, i32 7
  %_M_finish.i.i439 = getelementptr inbounds %class.Table, %class.Table* %table, i64 0, i32 3, i32 0, i32 0, i32 1
  %_M_end_of_storage.i.i440 = getelementptr inbounds %class.Table, %class.Table* %table, i64 0, i32 3, i32 0, i32 0, i32 2
  %_M_start.i27.i.i.i.i444 = getelementptr inbounds %class.Table, %class.Table* %table, i64 0, i32 3, i32 0, i32 0, i32 0
  %mul = shl nsw i64 %conv15, 2
  %wide.trip.count711 = zext i32 %groupByColSize to i64
  br label %for.body

for.cond22.preheader:                             ; preds = %for.inc
  %_M_cur.i = getelementptr inbounds %struct.Iterator, %struct.Iterator* %iterator, i64 0, i32 0, i32 0, i32 0
  %cmp23682 = icmp sgt i32 %tableRowSize, 0
  br i1 %cmp23682, label %land.rhs.lr.ph, label %for.end75

for.cond22.preheader.thread:                      ; preds = %entry
  %_M_cur.i718 = getelementptr inbounds %struct.Iterator, %struct.Iterator* %iterator, i64 0, i32 0, i32 0, i32 0
  %cmp23682719 = icmp sgt i32 %tableRowSize, 0
  br i1 %cmp23682719, label %land.rhs.preheader, label %for.end75

land.rhs.lr.ph:                                   ; preds = %for.cond22.preheader
  %_M_start.i.i = getelementptr inbounds %class.Table, %class.Table* %table, i64 0, i32 3, i32 0, i32 0, i32 0
  %0 = bitcast %struct.Iterator* %iterator to %"struct.std::__detail::_Hash_node"***
  br i1 %cmp685.not, label %land.rhs.preheader, label %land.rhs.us.preheader

land.rhs.preheader:                               ; preds = %for.cond22.preheader.thread, %land.rhs.lr.ph
  %_M_cur.i720727 = phi %"struct.std::__detail::_Hash_node"** [ %_M_cur.i, %land.rhs.lr.ph ], [ %_M_cur.i718, %for.cond22.preheader.thread ]
  %.pre713 = load %"struct.std::__detail::_Hash_node"*, %"struct.std::__detail::_Hash_node"** %_M_cur.i720727, align 8, !tbaa !76
  br label %land.rhs

land.rhs.us.preheader:                            ; preds = %land.rhs.lr.ph
  %wide.trip.count706 = zext i32 %tableRowSize to i64
  %.pre = load %"struct.std::__detail::_Hash_node"*, %"struct.std::__detail::_Hash_node"** %_M_cur.i, align 8, !tbaa !76
  %wide.trip.count702 = zext i32 %groupByColSize to i64
  br label %land.rhs.us

land.rhs.us:                                      ; preds = %land.rhs.us.preheader, %for.cond29.for.cond.cleanup31_crit_edge.us
  %1 = phi %"struct.std::__detail::_Hash_node"* [ %.pre, %land.rhs.us.preheader ], [ %23, %for.cond29.for.cond.cleanup31_crit_edge.us ]
  %indvars.iv704 = phi i64 [ 0, %land.rhs.us.preheader ], [ %indvars.iv.next705, %for.cond29.for.cond.cleanup31_crit_edge.us ]
  %cmp.i.not.us = icmp eq %"struct.std::__detail::_Hash_node"* %1, null
  br i1 %cmp.i.not.us, label %for.end75, label %for.body32.lr.ph.us

for.body32.us:                                    ; preds = %for.body32.lr.ph.us, %sw.epilog66.us
  %indvars.iv700 = phi i64 [ 0, %for.body32.lr.ph.us ], [ %indvars.iv.next701, %sw.epilog66.us ]
  %add.ptr.i.i.us = getelementptr inbounds %class.Column*, %class.Column** %22, i64 %indvars.iv700
  %2 = load %class.Column*, %class.Column** %add.ptr.i.i.us, align 8, !tbaa !19
  %type.i554.us = getelementptr inbounds %class.Column, %class.Column* %2, i64 0, i32 3
  %3 = load i32, i32* %type.i554.us, align 8, !tbaa !32
  switch i32 %3, label %sw.epilog66.us [
    i32 1, label %sw.bb35.us
    i32 2, label %sw.bb44.us
    i32 3, label %sw.bb55.us
  ]

sw.bb55.us:                                       ; preds = %for.body32.us
  %data.i652.us = getelementptr inbounds %class.Column, %class.Column* %2, i64 0, i32 1
  %4 = bitcast i8** %data.i652.us to double**
  %5 = load double*, double** %4, align 8, !tbaa !34
  %6 = load %struct.GroupByColumn*, %struct.GroupByColumn** %_M_start.i649.us, align 8, !tbaa !42
  %val63.us = getelementptr inbounds %struct.GroupByColumn, %struct.GroupByColumn* %6, i64 %indvars.iv700, i32 1
  %7 = bitcast i8** %val63.us to double**
  %8 = load double*, double** %7, align 8, !tbaa !78
  %9 = load double, double* %8, align 8, !tbaa !37
  %arrayidx65.us = getelementptr inbounds double, double* %5, i64 %indvars.iv704
  store double %9, double* %arrayidx65.us, align 8, !tbaa !37
  br label %sw.epilog66.us

sw.bb44.us:                                       ; preds = %for.body32.us
  %data.i656.us = getelementptr inbounds %class.Column, %class.Column* %2, i64 0, i32 1
  %10 = bitcast i8** %data.i656.us to i64**
  %11 = load i64*, i64** %10, align 8, !tbaa !34
  %12 = load %struct.GroupByColumn*, %struct.GroupByColumn** %_M_start.i649.us, align 8, !tbaa !42
  %val52.us = getelementptr inbounds %struct.GroupByColumn, %struct.GroupByColumn* %12, i64 %indvars.iv700, i32 1
  %13 = bitcast i8** %val52.us to i64**
  %14 = load i64*, i64** %13, align 8, !tbaa !78
  %15 = load i64, i64* %14, align 8, !tbaa !23
  %arrayidx54.us = getelementptr inbounds i64, i64* %11, i64 %indvars.iv704
  store i64 %15, i64* %arrayidx54.us, align 8, !tbaa !23
  br label %sw.epilog66.us

sw.bb35.us:                                       ; preds = %for.body32.us
  %data.i627.us = getelementptr inbounds %class.Column, %class.Column* %2, i64 0, i32 1
  %16 = bitcast i8** %data.i627.us to i32**
  %17 = load i32*, i32** %16, align 8, !tbaa !34
  %18 = load %struct.GroupByColumn*, %struct.GroupByColumn** %_M_start.i657.us, align 8, !tbaa !42
  %val.us = getelementptr inbounds %struct.GroupByColumn, %struct.GroupByColumn* %18, i64 %indvars.iv700, i32 1
  %19 = bitcast i8** %val.us to i32**
  %20 = load i32*, i32** %19, align 8, !tbaa !78
  %21 = load i32, i32* %20, align 4, !tbaa !25
  %arrayidx43.us = getelementptr inbounds i32, i32* %17, i64 %indvars.iv704
  store i32 %21, i32* %arrayidx43.us, align 4, !tbaa !25
  br label %sw.epilog66.us

sw.epilog66.us:                                   ; preds = %sw.bb35.us, %sw.bb44.us, %sw.bb55.us, %for.body32.us
  %indvars.iv.next701 = add nuw nsw i64 %indvars.iv700, 1
  %exitcond703.not = icmp eq i64 %indvars.iv.next701, %wide.trip.count702
  br i1 %exitcond703.not, label %for.cond29.for.cond.cleanup31_crit_edge.us, label %for.body32.us, !llvm.loop !80

for.body32.lr.ph.us:                              ; preds = %land.rhs.us
  %22 = load %class.Column**, %class.Column*** %_M_start.i.i, align 8, !tbaa !29
  %second60.us = getelementptr inbounds %"struct.std::__detail::_Hash_node", %"struct.std::__detail::_Hash_node"* %1, i64 0, i32 0, i32 1, i32 0, i32 0, i64 8
  %_M_start.i649.us = bitcast i8* %second60.us to %struct.GroupByColumn**
  %second.us = getelementptr inbounds %"struct.std::__detail::_Hash_node", %"struct.std::__detail::_Hash_node"* %1, i64 0, i32 0, i32 1, i32 0, i32 0, i64 8
  %_M_start.i657.us = bitcast i8* %second.us to %struct.GroupByColumn**
  br label %for.body32.us

for.cond29.for.cond.cleanup31_crit_edge.us:       ; preds = %sw.epilog66.us
  %indvars.iv.next705 = add nuw nsw i64 %indvars.iv704, 1
  %retval.sroa.0.0.copyload.i670.us = load %"struct.std::__detail::_Hash_node"**, %"struct.std::__detail::_Hash_node"*** %0, align 8
  %23 = load %"struct.std::__detail::_Hash_node"*, %"struct.std::__detail::_Hash_node"** %retval.sroa.0.0.copyload.i670.us, align 8, !tbaa !22
  store %"struct.std::__detail::_Hash_node"* %23, %"struct.std::__detail::_Hash_node"** %_M_cur.i, align 8, !tbaa !76
  %exitcond707.not = icmp eq i64 %indvars.iv.next705, %wide.trip.count706
  br i1 %exitcond707.not, label %for.end75, label %land.rhs.us, !llvm.loop !81

for.body:                                         ; preds = %for.body.lr.ph, %for.inc
  %indvars.iv709 = phi i64 [ 0, %for.body.lr.ph ], [ %indvars.iv.next710, %for.inc ]
  %arrayidx = getelementptr inbounds i32, i32* %types, i64 %indvars.iv709
  %24 = load i32, i32* %arrayidx, align 4, !tbaa !25
  switch i32 %24, label %for.inc [
    i32 1, label %sw.bb
    i32 2, label %sw.bb4
    i32 3, label %sw.bb13
  ]

sw.bb:                                            ; preds = %for.body
  %call = tail call i8* @omni_allocate(i64 %mul)
  %call2 = tail call noalias nonnull dereferenceable(40) i8* @_Znwm(i64 40) #18
  %25 = bitcast i8* %call2 to i32 (...)***
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [4 x i8*] }, { [4 x i8*] }* @_ZTV6Column, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %25, align 8, !tbaa !50
  %data.i = getelementptr inbounds i8, i8* %call2, i64 8
  %26 = bitcast i8* %data.i to i8**
  store i8* %call, i8** %26, align 8, !tbaa !34
  %type.i = getelementptr inbounds i8, i8* %call2, i64 24
  %27 = bitcast i8* %type.i to i32*
  store i32 1, i32* %27, align 8, !tbaa !32
  %size.i = getelementptr inbounds i8, i8* %call2, i64 32
  %28 = bitcast i8* %size.i to i64*
  store i64 %conv15, i64* %28, align 8, !tbaa !82
  %29 = load i32*, i32** %types.i434, align 8, !tbaa !61
  %30 = load i32, i32* %columnSize.i435, align 8, !tbaa !83
  %idxprom.i326 = zext i32 %30 to i64
  %arrayidx.i327 = getelementptr inbounds i32, i32* %29, i64 %idxprom.i326
  store i32 1, i32* %arrayidx.i327, align 4, !tbaa !27
  %inc.i328 = add i32 %30, 1
  store i32 %inc.i328, i32* %columnSize.i435, align 8, !tbaa !83
  %31 = load %class.Column**, %class.Column*** %_M_finish.i.i439, align 8, !tbaa !57
  %32 = load %class.Column**, %class.Column*** %_M_end_of_storage.i.i440, align 8, !tbaa !84
  %cmp.not.i.i331 = icmp eq %class.Column** %31, %32
  br i1 %cmp.not.i.i331, label %if.else.i.i347, label %if.then.i.i333

if.then.i.i333:                                   ; preds = %sw.bb
  %33 = bitcast %class.Column** %31 to i8**
  store i8* %call2, i8** %33, align 8, !tbaa !19
  %34 = load %class.Column**, %class.Column*** %_M_finish.i.i439, align 8, !tbaa !57
  %incdec.ptr.i.i332 = getelementptr inbounds %class.Column*, %class.Column** %34, i64 1
  br label %for.inc.sink.split

if.else.i.i347:                                   ; preds = %sw.bb
  %35 = load %class.Column**, %class.Column*** %_M_start.i27.i.i.i.i444, align 8, !tbaa !29
  %sub.ptr.lhs.cast.i28.i.i.i.i335 = ptrtoint %class.Column** %31 to i64
  %sub.ptr.rhs.cast.i29.i.i.i.i336 = ptrtoint %class.Column** %35 to i64
  %sub.ptr.sub.i30.i.i.i.i337 = sub i64 %sub.ptr.lhs.cast.i28.i.i.i.i335, %sub.ptr.rhs.cast.i29.i.i.i.i336
  %sub.ptr.div.i31.i.i.i.i338 = ashr exact i64 %sub.ptr.sub.i30.i.i.i.i337, 3
  %cmp.i.i.i.i.i339 = icmp eq i64 %sub.ptr.sub.i30.i.i.i.i337, 0
  %.sroa.speculated.i.i.i.i340 = select i1 %cmp.i.i.i.i.i339, i64 1, i64 %sub.ptr.div.i31.i.i.i.i338
  %add.i.i.i.i341 = add nsw i64 %.sroa.speculated.i.i.i.i340, %sub.ptr.div.i31.i.i.i.i338
  %cmp7.i.i.i.i342 = icmp ult i64 %add.i.i.i.i341, %sub.ptr.div.i31.i.i.i.i338
  %cmp9.i.i.i.i343 = icmp ugt i64 %add.i.i.i.i341, 2305843009213693951
  %or.cond.i.i.i.i344 = or i1 %cmp7.i.i.i.i342, %cmp9.i.i.i.i343
  %cond.i.i.i.i345 = select i1 %or.cond.i.i.i.i344, i64 2305843009213693951, i64 %add.i.i.i.i341
  %cmp.not.i.i.i.i346 = icmp eq i64 %cond.i.i.i.i345, 0
  br i1 %cmp.not.i.i.i.i346, label %invoke.cont.i.i.i358, label %_ZNSt16allocator_traitsISaIP6ColumnEE8allocateERS2_m.exit.i.i.i.i353

_ZNSt16allocator_traitsISaIP6ColumnEE8allocateERS2_m.exit.i.i.i.i353: ; preds = %if.else.i.i347
  %mul.i.i.i.i.i.i348 = shl nuw i64 %cond.i.i.i.i345, 3
  %call2.i.i.i.i.i.i349 = tail call noalias nonnull i8* @_Znwm(i64 %mul.i.i.i.i.i.i348) #19
  %36 = bitcast i8* %call2.i.i.i.i.i.i349 to %class.Column**
  %.pre.i.i.i350 = load %class.Column**, %class.Column*** %_M_start.i27.i.i.i.i444, align 8, !tbaa !29
  %.pre83.i.i.i351 = ptrtoint %class.Column** %.pre.i.i.i350 to i64
  %.pre84.i.i.i352 = sub i64 %sub.ptr.lhs.cast.i28.i.i.i.i335, %.pre83.i.i.i351
  br label %invoke.cont.i.i.i358

invoke.cont.i.i.i358:                             ; preds = %_ZNSt16allocator_traitsISaIP6ColumnEE8allocateERS2_m.exit.i.i.i.i353, %if.else.i.i347
  %sub.ptr.sub.i.i.i.i.i.i.i.i74.pre-phi.i.i.i354 = phi i64 [ %.pre84.i.i.i352, %_ZNSt16allocator_traitsISaIP6ColumnEE8allocateERS2_m.exit.i.i.i.i353 ], [ %sub.ptr.sub.i30.i.i.i.i337, %if.else.i.i347 ]
  %37 = phi %class.Column** [ %.pre.i.i.i350, %_ZNSt16allocator_traitsISaIP6ColumnEE8allocateERS2_m.exit.i.i.i.i353 ], [ %35, %if.else.i.i347 ]
  %cond.i67.i.i.i355 = phi %class.Column** [ %36, %_ZNSt16allocator_traitsISaIP6ColumnEE8allocateERS2_m.exit.i.i.i.i353 ], [ null, %if.else.i.i347 ]
  %add.ptr.i.i.i356 = getelementptr inbounds %class.Column*, %class.Column** %cond.i67.i.i.i355, i64 %sub.ptr.div.i31.i.i.i.i338
  %38 = bitcast %class.Column** %add.ptr.i.i.i356 to i8**
  store i8* %call2, i8** %38, align 8, !tbaa !19
  %tobool.not.i.i.i.i.i.i.i.i75.i.i.i357 = icmp eq i64 %sub.ptr.sub.i.i.i.i.i.i.i.i74.pre-phi.i.i.i354, 0
  br i1 %tobool.not.i.i.i.i.i.i.i.i75.i.i.i357, label %invoke.cont10.i.i.i366, label %if.then.i.i.i.i.i.i.i.i76.i.i.i359

if.then.i.i.i.i.i.i.i.i76.i.i.i359:               ; preds = %invoke.cont.i.i.i358
  %39 = bitcast %class.Column** %cond.i67.i.i.i355 to i8*
  %40 = bitcast %class.Column** %37 to i8*
  tail call void @llvm.memmove.p0i8.p0i8.i64(i8* align 8 %39, i8* align 8 %40, i64 %sub.ptr.sub.i.i.i.i.i.i.i.i74.pre-phi.i.i.i354, i1 false) #20
  br label %invoke.cont10.i.i.i366

invoke.cont10.i.i.i366:                           ; preds = %if.then.i.i.i.i.i.i.i.i76.i.i.i359, %invoke.cont.i.i.i358
  %sub.ptr.div.i.i.i.i.i.i.i.i77.i.i.i360 = ashr exact i64 %sub.ptr.sub.i.i.i.i.i.i.i.i74.pre-phi.i.i.i354, 3
  %add.ptr.i.i.i.i.i.i.i.i78.i.i.i361 = getelementptr inbounds %class.Column*, %class.Column** %cond.i67.i.i.i355, i64 1
  %incdec.ptr.i.i.i362 = getelementptr inbounds %class.Column*, %class.Column** %add.ptr.i.i.i.i.i.i.i.i78.i.i.i361, i64 %sub.ptr.div.i.i.i.i.i.i.i.i77.i.i.i360
  %41 = load %class.Column**, %class.Column*** %_M_finish.i.i439, align 8, !tbaa !57
  %sub.ptr.lhs.cast.i.i.i.i.i.i.i.i.i.i.i363 = ptrtoint %class.Column** %41 to i64
  %sub.ptr.sub.i.i.i.i.i.i.i.i.i.i.i364 = sub i64 %sub.ptr.lhs.cast.i.i.i.i.i.i.i.i.i.i.i363, %sub.ptr.lhs.cast.i28.i.i.i.i335
  %tobool.not.i.i.i.i.i.i.i.i.i.i.i365 = icmp eq i64 %sub.ptr.sub.i.i.i.i.i.i.i.i.i.i.i364, 0
  br i1 %tobool.not.i.i.i.i.i.i.i.i.i.i.i365, label %invoke.cont15.i.i.i369, label %if.then.i.i.i.i.i.i.i.i.i.i.i367

if.then.i.i.i.i.i.i.i.i.i.i.i367:                 ; preds = %invoke.cont10.i.i.i366
  %42 = bitcast %class.Column** %incdec.ptr.i.i.i362 to i8*
  %43 = bitcast %class.Column** %31 to i8*
  tail call void @llvm.memmove.p0i8.p0i8.i64(i8* nonnull align 8 %42, i8* align 8 %43, i64 %sub.ptr.sub.i.i.i.i.i.i.i.i.i.i.i364, i1 false) #20
  br label %invoke.cont15.i.i.i369

invoke.cont15.i.i.i369:                           ; preds = %if.then.i.i.i.i.i.i.i.i.i.i.i367, %invoke.cont10.i.i.i366
  %tobool.not.i68.i.i.i368 = icmp eq %class.Column** %37, null
  br i1 %tobool.not.i68.i.i.i368, label %_ZNSt6vectorIP6ColumnSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i.i374, label %if.then.i69.i.i.i370

if.then.i69.i.i.i370:                             ; preds = %invoke.cont15.i.i.i369
  %44 = bitcast %class.Column** %37 to i8*
  tail call void @_ZdlPv(i8* nonnull %44) #20
  br label %_ZNSt6vectorIP6ColumnSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i.i374

_ZNSt6vectorIP6ColumnSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i.i374: ; preds = %if.then.i69.i.i.i370, %invoke.cont15.i.i.i369
  %sub.ptr.div.i.i.i.i.i.i.i.i.i.i.i371 = ashr exact i64 %sub.ptr.sub.i.i.i.i.i.i.i.i.i.i.i364, 3
  %add.ptr.i.i.i.i.i.i.i.i.i.i.i372 = getelementptr inbounds %class.Column*, %class.Column** %incdec.ptr.i.i.i362, i64 %sub.ptr.div.i.i.i.i.i.i.i.i.i.i.i371
  store %class.Column** %cond.i67.i.i.i355, %class.Column*** %_M_start.i27.i.i.i.i444, align 8, !tbaa !29
  store %class.Column** %add.ptr.i.i.i.i.i.i.i.i.i.i.i372, %class.Column*** %_M_finish.i.i439, align 8, !tbaa !57
  %add.ptr39.i.i.i373 = getelementptr inbounds %class.Column*, %class.Column** %cond.i67.i.i.i355, i64 %cond.i.i.i.i345
  br label %for.inc.sink.split

sw.bb4:                                           ; preds = %for.body
  %call8 = tail call i8* @omni_allocate(i64 %mul16)
  %call9 = tail call noalias nonnull dereferenceable(40) i8* @_Znwm(i64 40) #18
  %45 = bitcast i8* %call9 to i32 (...)***
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [4 x i8*] }, { [4 x i8*] }* @_ZTV6Column, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %45, align 8, !tbaa !50
  %data.i376 = getelementptr inbounds i8, i8* %call9, i64 8
  %46 = bitcast i8* %data.i376 to i8**
  store i8* %call8, i8** %46, align 8, !tbaa !34
  %type.i377 = getelementptr inbounds i8, i8* %call9, i64 24
  %47 = bitcast i8* %type.i377 to i32*
  store i32 2, i32* %47, align 8, !tbaa !32
  %size.i378 = getelementptr inbounds i8, i8* %call9, i64 32
  %48 = bitcast i8* %size.i378 to i64*
  store i64 %conv15, i64* %48, align 8, !tbaa !82
  %49 = load i32*, i32** %types.i434, align 8, !tbaa !61
  %50 = load i32, i32* %columnSize.i435, align 8, !tbaa !83
  %idxprom.i381 = zext i32 %50 to i64
  %arrayidx.i382 = getelementptr inbounds i32, i32* %49, i64 %idxprom.i381
  store i32 2, i32* %arrayidx.i382, align 4, !tbaa !27
  %inc.i383 = add i32 %50, 1
  store i32 %inc.i383, i32* %columnSize.i435, align 8, !tbaa !83
  %51 = load %class.Column**, %class.Column*** %_M_finish.i.i439, align 8, !tbaa !57
  %52 = load %class.Column**, %class.Column*** %_M_end_of_storage.i.i440, align 8, !tbaa !84
  %cmp.not.i.i386 = icmp eq %class.Column** %51, %52
  br i1 %cmp.not.i.i386, label %if.else.i.i402, label %if.then.i.i388

if.then.i.i388:                                   ; preds = %sw.bb4
  %53 = bitcast %class.Column** %51 to i8**
  store i8* %call9, i8** %53, align 8, !tbaa !19
  %54 = load %class.Column**, %class.Column*** %_M_finish.i.i439, align 8, !tbaa !57
  %incdec.ptr.i.i387 = getelementptr inbounds %class.Column*, %class.Column** %54, i64 1
  br label %for.inc.sink.split

if.else.i.i402:                                   ; preds = %sw.bb4
  %55 = load %class.Column**, %class.Column*** %_M_start.i27.i.i.i.i444, align 8, !tbaa !29
  %sub.ptr.lhs.cast.i28.i.i.i.i390 = ptrtoint %class.Column** %51 to i64
  %sub.ptr.rhs.cast.i29.i.i.i.i391 = ptrtoint %class.Column** %55 to i64
  %sub.ptr.sub.i30.i.i.i.i392 = sub i64 %sub.ptr.lhs.cast.i28.i.i.i.i390, %sub.ptr.rhs.cast.i29.i.i.i.i391
  %sub.ptr.div.i31.i.i.i.i393 = ashr exact i64 %sub.ptr.sub.i30.i.i.i.i392, 3
  %cmp.i.i.i.i.i394 = icmp eq i64 %sub.ptr.sub.i30.i.i.i.i392, 0
  %.sroa.speculated.i.i.i.i395 = select i1 %cmp.i.i.i.i.i394, i64 1, i64 %sub.ptr.div.i31.i.i.i.i393
  %add.i.i.i.i396 = add nsw i64 %.sroa.speculated.i.i.i.i395, %sub.ptr.div.i31.i.i.i.i393
  %cmp7.i.i.i.i397 = icmp ult i64 %add.i.i.i.i396, %sub.ptr.div.i31.i.i.i.i393
  %cmp9.i.i.i.i398 = icmp ugt i64 %add.i.i.i.i396, 2305843009213693951
  %or.cond.i.i.i.i399 = or i1 %cmp7.i.i.i.i397, %cmp9.i.i.i.i398
  %cond.i.i.i.i400 = select i1 %or.cond.i.i.i.i399, i64 2305843009213693951, i64 %add.i.i.i.i396
  %cmp.not.i.i.i.i401 = icmp eq i64 %cond.i.i.i.i400, 0
  br i1 %cmp.not.i.i.i.i401, label %invoke.cont.i.i.i413, label %_ZNSt16allocator_traitsISaIP6ColumnEE8allocateERS2_m.exit.i.i.i.i408

_ZNSt16allocator_traitsISaIP6ColumnEE8allocateERS2_m.exit.i.i.i.i408: ; preds = %if.else.i.i402
  %mul.i.i.i.i.i.i403 = shl nuw i64 %cond.i.i.i.i400, 3
  %call2.i.i.i.i.i.i404 = tail call noalias nonnull i8* @_Znwm(i64 %mul.i.i.i.i.i.i403) #19
  %56 = bitcast i8* %call2.i.i.i.i.i.i404 to %class.Column**
  %.pre.i.i.i405 = load %class.Column**, %class.Column*** %_M_start.i27.i.i.i.i444, align 8, !tbaa !29
  %.pre83.i.i.i406 = ptrtoint %class.Column** %.pre.i.i.i405 to i64
  %.pre84.i.i.i407 = sub i64 %sub.ptr.lhs.cast.i28.i.i.i.i390, %.pre83.i.i.i406
  br label %invoke.cont.i.i.i413

invoke.cont.i.i.i413:                             ; preds = %_ZNSt16allocator_traitsISaIP6ColumnEE8allocateERS2_m.exit.i.i.i.i408, %if.else.i.i402
  %sub.ptr.sub.i.i.i.i.i.i.i.i74.pre-phi.i.i.i409 = phi i64 [ %.pre84.i.i.i407, %_ZNSt16allocator_traitsISaIP6ColumnEE8allocateERS2_m.exit.i.i.i.i408 ], [ %sub.ptr.sub.i30.i.i.i.i392, %if.else.i.i402 ]
  %57 = phi %class.Column** [ %.pre.i.i.i405, %_ZNSt16allocator_traitsISaIP6ColumnEE8allocateERS2_m.exit.i.i.i.i408 ], [ %55, %if.else.i.i402 ]
  %cond.i67.i.i.i410 = phi %class.Column** [ %56, %_ZNSt16allocator_traitsISaIP6ColumnEE8allocateERS2_m.exit.i.i.i.i408 ], [ null, %if.else.i.i402 ]
  %add.ptr.i.i.i411 = getelementptr inbounds %class.Column*, %class.Column** %cond.i67.i.i.i410, i64 %sub.ptr.div.i31.i.i.i.i393
  %58 = bitcast %class.Column** %add.ptr.i.i.i411 to i8**
  store i8* %call9, i8** %58, align 8, !tbaa !19
  %tobool.not.i.i.i.i.i.i.i.i75.i.i.i412 = icmp eq i64 %sub.ptr.sub.i.i.i.i.i.i.i.i74.pre-phi.i.i.i409, 0
  br i1 %tobool.not.i.i.i.i.i.i.i.i75.i.i.i412, label %invoke.cont10.i.i.i421, label %if.then.i.i.i.i.i.i.i.i76.i.i.i414

if.then.i.i.i.i.i.i.i.i76.i.i.i414:               ; preds = %invoke.cont.i.i.i413
  %59 = bitcast %class.Column** %cond.i67.i.i.i410 to i8*
  %60 = bitcast %class.Column** %57 to i8*
  tail call void @llvm.memmove.p0i8.p0i8.i64(i8* align 8 %59, i8* align 8 %60, i64 %sub.ptr.sub.i.i.i.i.i.i.i.i74.pre-phi.i.i.i409, i1 false) #20
  br label %invoke.cont10.i.i.i421

invoke.cont10.i.i.i421:                           ; preds = %if.then.i.i.i.i.i.i.i.i76.i.i.i414, %invoke.cont.i.i.i413
  %sub.ptr.div.i.i.i.i.i.i.i.i77.i.i.i415 = ashr exact i64 %sub.ptr.sub.i.i.i.i.i.i.i.i74.pre-phi.i.i.i409, 3
  %add.ptr.i.i.i.i.i.i.i.i78.i.i.i416 = getelementptr inbounds %class.Column*, %class.Column** %cond.i67.i.i.i410, i64 1
  %incdec.ptr.i.i.i417 = getelementptr inbounds %class.Column*, %class.Column** %add.ptr.i.i.i.i.i.i.i.i78.i.i.i416, i64 %sub.ptr.div.i.i.i.i.i.i.i.i77.i.i.i415
  %61 = load %class.Column**, %class.Column*** %_M_finish.i.i439, align 8, !tbaa !57
  %sub.ptr.lhs.cast.i.i.i.i.i.i.i.i.i.i.i418 = ptrtoint %class.Column** %61 to i64
  %sub.ptr.sub.i.i.i.i.i.i.i.i.i.i.i419 = sub i64 %sub.ptr.lhs.cast.i.i.i.i.i.i.i.i.i.i.i418, %sub.ptr.lhs.cast.i28.i.i.i.i390
  %tobool.not.i.i.i.i.i.i.i.i.i.i.i420 = icmp eq i64 %sub.ptr.sub.i.i.i.i.i.i.i.i.i.i.i419, 0
  br i1 %tobool.not.i.i.i.i.i.i.i.i.i.i.i420, label %invoke.cont15.i.i.i424, label %if.then.i.i.i.i.i.i.i.i.i.i.i422

if.then.i.i.i.i.i.i.i.i.i.i.i422:                 ; preds = %invoke.cont10.i.i.i421
  %62 = bitcast %class.Column** %incdec.ptr.i.i.i417 to i8*
  %63 = bitcast %class.Column** %51 to i8*
  tail call void @llvm.memmove.p0i8.p0i8.i64(i8* nonnull align 8 %62, i8* align 8 %63, i64 %sub.ptr.sub.i.i.i.i.i.i.i.i.i.i.i419, i1 false) #20
  br label %invoke.cont15.i.i.i424

invoke.cont15.i.i.i424:                           ; preds = %if.then.i.i.i.i.i.i.i.i.i.i.i422, %invoke.cont10.i.i.i421
  %tobool.not.i68.i.i.i423 = icmp eq %class.Column** %57, null
  br i1 %tobool.not.i68.i.i.i423, label %_ZNSt6vectorIP6ColumnSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i.i429, label %if.then.i69.i.i.i425

if.then.i69.i.i.i425:                             ; preds = %invoke.cont15.i.i.i424
  %64 = bitcast %class.Column** %57 to i8*
  tail call void @_ZdlPv(i8* nonnull %64) #20
  br label %_ZNSt6vectorIP6ColumnSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i.i429

_ZNSt6vectorIP6ColumnSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i.i429: ; preds = %if.then.i69.i.i.i425, %invoke.cont15.i.i.i424
  %sub.ptr.div.i.i.i.i.i.i.i.i.i.i.i426 = ashr exact i64 %sub.ptr.sub.i.i.i.i.i.i.i.i.i.i.i419, 3
  %add.ptr.i.i.i.i.i.i.i.i.i.i.i427 = getelementptr inbounds %class.Column*, %class.Column** %incdec.ptr.i.i.i417, i64 %sub.ptr.div.i.i.i.i.i.i.i.i.i.i.i426
  store %class.Column** %cond.i67.i.i.i410, %class.Column*** %_M_start.i27.i.i.i.i444, align 8, !tbaa !29
  store %class.Column** %add.ptr.i.i.i.i.i.i.i.i.i.i.i427, %class.Column*** %_M_finish.i.i439, align 8, !tbaa !57
  %add.ptr39.i.i.i428 = getelementptr inbounds %class.Column*, %class.Column** %cond.i67.i.i.i410, i64 %cond.i.i.i.i400
  br label %for.inc.sink.split

sw.bb13:                                          ; preds = %for.body
  %call17 = tail call i8* @omni_allocate(i64 %mul16)
  %call18 = tail call noalias nonnull dereferenceable(40) i8* @_Znwm(i64 40) #18
  %65 = bitcast i8* %call18 to i32 (...)***
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [4 x i8*] }, { [4 x i8*] }* @_ZTV6Column, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %65, align 8, !tbaa !50
  %data.i431 = getelementptr inbounds i8, i8* %call18, i64 8
  %66 = bitcast i8* %data.i431 to i8**
  store i8* %call17, i8** %66, align 8, !tbaa !34
  %type.i432 = getelementptr inbounds i8, i8* %call18, i64 24
  %67 = bitcast i8* %type.i432 to i32*
  store i32 3, i32* %67, align 8, !tbaa !32
  %size.i433 = getelementptr inbounds i8, i8* %call18, i64 32
  %68 = bitcast i8* %size.i433 to i64*
  store i64 %conv15, i64* %68, align 8, !tbaa !82
  %69 = load i32*, i32** %types.i434, align 8, !tbaa !61
  %70 = load i32, i32* %columnSize.i435, align 8, !tbaa !83
  %idxprom.i436 = zext i32 %70 to i64
  %arrayidx.i437 = getelementptr inbounds i32, i32* %69, i64 %idxprom.i436
  store i32 3, i32* %arrayidx.i437, align 4, !tbaa !27
  %inc.i438 = add i32 %70, 1
  store i32 %inc.i438, i32* %columnSize.i435, align 8, !tbaa !83
  %71 = load %class.Column**, %class.Column*** %_M_finish.i.i439, align 8, !tbaa !57
  %72 = load %class.Column**, %class.Column*** %_M_end_of_storage.i.i440, align 8, !tbaa !84
  %cmp.not.i.i441 = icmp eq %class.Column** %71, %72
  br i1 %cmp.not.i.i441, label %if.else.i.i457, label %if.then.i.i443

if.then.i.i443:                                   ; preds = %sw.bb13
  %73 = bitcast %class.Column** %71 to i8**
  store i8* %call18, i8** %73, align 8, !tbaa !19
  %74 = load %class.Column**, %class.Column*** %_M_finish.i.i439, align 8, !tbaa !57
  %incdec.ptr.i.i442 = getelementptr inbounds %class.Column*, %class.Column** %74, i64 1
  br label %for.inc.sink.split

if.else.i.i457:                                   ; preds = %sw.bb13
  %75 = load %class.Column**, %class.Column*** %_M_start.i27.i.i.i.i444, align 8, !tbaa !29
  %sub.ptr.lhs.cast.i28.i.i.i.i445 = ptrtoint %class.Column** %71 to i64
  %sub.ptr.rhs.cast.i29.i.i.i.i446 = ptrtoint %class.Column** %75 to i64
  %sub.ptr.sub.i30.i.i.i.i447 = sub i64 %sub.ptr.lhs.cast.i28.i.i.i.i445, %sub.ptr.rhs.cast.i29.i.i.i.i446
  %sub.ptr.div.i31.i.i.i.i448 = ashr exact i64 %sub.ptr.sub.i30.i.i.i.i447, 3
  %cmp.i.i.i.i.i449 = icmp eq i64 %sub.ptr.sub.i30.i.i.i.i447, 0
  %.sroa.speculated.i.i.i.i450 = select i1 %cmp.i.i.i.i.i449, i64 1, i64 %sub.ptr.div.i31.i.i.i.i448
  %add.i.i.i.i451 = add nsw i64 %.sroa.speculated.i.i.i.i450, %sub.ptr.div.i31.i.i.i.i448
  %cmp7.i.i.i.i452 = icmp ult i64 %add.i.i.i.i451, %sub.ptr.div.i31.i.i.i.i448
  %cmp9.i.i.i.i453 = icmp ugt i64 %add.i.i.i.i451, 2305843009213693951
  %or.cond.i.i.i.i454 = or i1 %cmp7.i.i.i.i452, %cmp9.i.i.i.i453
  %cond.i.i.i.i455 = select i1 %or.cond.i.i.i.i454, i64 2305843009213693951, i64 %add.i.i.i.i451
  %cmp.not.i.i.i.i456 = icmp eq i64 %cond.i.i.i.i455, 0
  br i1 %cmp.not.i.i.i.i456, label %invoke.cont.i.i.i468, label %_ZNSt16allocator_traitsISaIP6ColumnEE8allocateERS2_m.exit.i.i.i.i463

_ZNSt16allocator_traitsISaIP6ColumnEE8allocateERS2_m.exit.i.i.i.i463: ; preds = %if.else.i.i457
  %mul.i.i.i.i.i.i458 = shl nuw i64 %cond.i.i.i.i455, 3
  %call2.i.i.i.i.i.i459 = tail call noalias nonnull i8* @_Znwm(i64 %mul.i.i.i.i.i.i458) #19
  %76 = bitcast i8* %call2.i.i.i.i.i.i459 to %class.Column**
  %.pre.i.i.i460 = load %class.Column**, %class.Column*** %_M_start.i27.i.i.i.i444, align 8, !tbaa !29
  %.pre83.i.i.i461 = ptrtoint %class.Column** %.pre.i.i.i460 to i64
  %.pre84.i.i.i462 = sub i64 %sub.ptr.lhs.cast.i28.i.i.i.i445, %.pre83.i.i.i461
  br label %invoke.cont.i.i.i468

invoke.cont.i.i.i468:                             ; preds = %_ZNSt16allocator_traitsISaIP6ColumnEE8allocateERS2_m.exit.i.i.i.i463, %if.else.i.i457
  %sub.ptr.sub.i.i.i.i.i.i.i.i74.pre-phi.i.i.i464 = phi i64 [ %.pre84.i.i.i462, %_ZNSt16allocator_traitsISaIP6ColumnEE8allocateERS2_m.exit.i.i.i.i463 ], [ %sub.ptr.sub.i30.i.i.i.i447, %if.else.i.i457 ]
  %77 = phi %class.Column** [ %.pre.i.i.i460, %_ZNSt16allocator_traitsISaIP6ColumnEE8allocateERS2_m.exit.i.i.i.i463 ], [ %75, %if.else.i.i457 ]
  %cond.i67.i.i.i465 = phi %class.Column** [ %76, %_ZNSt16allocator_traitsISaIP6ColumnEE8allocateERS2_m.exit.i.i.i.i463 ], [ null, %if.else.i.i457 ]
  %add.ptr.i.i.i466 = getelementptr inbounds %class.Column*, %class.Column** %cond.i67.i.i.i465, i64 %sub.ptr.div.i31.i.i.i.i448
  %78 = bitcast %class.Column** %add.ptr.i.i.i466 to i8**
  store i8* %call18, i8** %78, align 8, !tbaa !19
  %tobool.not.i.i.i.i.i.i.i.i75.i.i.i467 = icmp eq i64 %sub.ptr.sub.i.i.i.i.i.i.i.i74.pre-phi.i.i.i464, 0
  br i1 %tobool.not.i.i.i.i.i.i.i.i75.i.i.i467, label %invoke.cont10.i.i.i476, label %if.then.i.i.i.i.i.i.i.i76.i.i.i469

if.then.i.i.i.i.i.i.i.i76.i.i.i469:               ; preds = %invoke.cont.i.i.i468
  %79 = bitcast %class.Column** %cond.i67.i.i.i465 to i8*
  %80 = bitcast %class.Column** %77 to i8*
  tail call void @llvm.memmove.p0i8.p0i8.i64(i8* align 8 %79, i8* align 8 %80, i64 %sub.ptr.sub.i.i.i.i.i.i.i.i74.pre-phi.i.i.i464, i1 false) #20
  br label %invoke.cont10.i.i.i476

invoke.cont10.i.i.i476:                           ; preds = %if.then.i.i.i.i.i.i.i.i76.i.i.i469, %invoke.cont.i.i.i468
  %sub.ptr.div.i.i.i.i.i.i.i.i77.i.i.i470 = ashr exact i64 %sub.ptr.sub.i.i.i.i.i.i.i.i74.pre-phi.i.i.i464, 3
  %add.ptr.i.i.i.i.i.i.i.i78.i.i.i471 = getelementptr inbounds %class.Column*, %class.Column** %cond.i67.i.i.i465, i64 1
  %incdec.ptr.i.i.i472 = getelementptr inbounds %class.Column*, %class.Column** %add.ptr.i.i.i.i.i.i.i.i78.i.i.i471, i64 %sub.ptr.div.i.i.i.i.i.i.i.i77.i.i.i470
  %81 = load %class.Column**, %class.Column*** %_M_finish.i.i439, align 8, !tbaa !57
  %sub.ptr.lhs.cast.i.i.i.i.i.i.i.i.i.i.i473 = ptrtoint %class.Column** %81 to i64
  %sub.ptr.sub.i.i.i.i.i.i.i.i.i.i.i474 = sub i64 %sub.ptr.lhs.cast.i.i.i.i.i.i.i.i.i.i.i473, %sub.ptr.lhs.cast.i28.i.i.i.i445
  %tobool.not.i.i.i.i.i.i.i.i.i.i.i475 = icmp eq i64 %sub.ptr.sub.i.i.i.i.i.i.i.i.i.i.i474, 0
  br i1 %tobool.not.i.i.i.i.i.i.i.i.i.i.i475, label %invoke.cont15.i.i.i479, label %if.then.i.i.i.i.i.i.i.i.i.i.i477

if.then.i.i.i.i.i.i.i.i.i.i.i477:                 ; preds = %invoke.cont10.i.i.i476
  %82 = bitcast %class.Column** %incdec.ptr.i.i.i472 to i8*
  %83 = bitcast %class.Column** %71 to i8*
  tail call void @llvm.memmove.p0i8.p0i8.i64(i8* nonnull align 8 %82, i8* align 8 %83, i64 %sub.ptr.sub.i.i.i.i.i.i.i.i.i.i.i474, i1 false) #20
  br label %invoke.cont15.i.i.i479

invoke.cont15.i.i.i479:                           ; preds = %if.then.i.i.i.i.i.i.i.i.i.i.i477, %invoke.cont10.i.i.i476
  %tobool.not.i68.i.i.i478 = icmp eq %class.Column** %77, null
  br i1 %tobool.not.i68.i.i.i478, label %_ZNSt6vectorIP6ColumnSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i.i484, label %if.then.i69.i.i.i480

if.then.i69.i.i.i480:                             ; preds = %invoke.cont15.i.i.i479
  %84 = bitcast %class.Column** %77 to i8*
  tail call void @_ZdlPv(i8* nonnull %84) #20
  br label %_ZNSt6vectorIP6ColumnSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i.i484

_ZNSt6vectorIP6ColumnSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i.i484: ; preds = %if.then.i69.i.i.i480, %invoke.cont15.i.i.i479
  %sub.ptr.div.i.i.i.i.i.i.i.i.i.i.i481 = ashr exact i64 %sub.ptr.sub.i.i.i.i.i.i.i.i.i.i.i474, 3
  %add.ptr.i.i.i.i.i.i.i.i.i.i.i482 = getelementptr inbounds %class.Column*, %class.Column** %incdec.ptr.i.i.i472, i64 %sub.ptr.div.i.i.i.i.i.i.i.i.i.i.i481
  store %class.Column** %cond.i67.i.i.i465, %class.Column*** %_M_start.i27.i.i.i.i444, align 8, !tbaa !29
  store %class.Column** %add.ptr.i.i.i.i.i.i.i.i.i.i.i482, %class.Column*** %_M_finish.i.i439, align 8, !tbaa !57
  %add.ptr39.i.i.i483 = getelementptr inbounds %class.Column*, %class.Column** %cond.i67.i.i.i465, i64 %cond.i.i.i.i455
  br label %for.inc.sink.split

for.inc.sink.split:                               ; preds = %if.then.i.i333, %_ZNSt6vectorIP6ColumnSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i.i374, %if.then.i.i388, %_ZNSt6vectorIP6ColumnSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i.i429, %if.then.i.i443, %_ZNSt6vectorIP6ColumnSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i.i484
  %_M_end_of_storage.i.i440.sink = phi %class.Column*** [ %_M_end_of_storage.i.i440, %_ZNSt6vectorIP6ColumnSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i.i484 ], [ %_M_finish.i.i439, %if.then.i.i443 ], [ %_M_end_of_storage.i.i440, %_ZNSt6vectorIP6ColumnSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i.i429 ], [ %_M_finish.i.i439, %if.then.i.i388 ], [ %_M_end_of_storage.i.i440, %_ZNSt6vectorIP6ColumnSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i.i374 ], [ %_M_finish.i.i439, %if.then.i.i333 ]
  %add.ptr39.i.i.i483.sink = phi %class.Column** [ %add.ptr39.i.i.i483, %_ZNSt6vectorIP6ColumnSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i.i484 ], [ %incdec.ptr.i.i442, %if.then.i.i443 ], [ %add.ptr39.i.i.i428, %_ZNSt6vectorIP6ColumnSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i.i429 ], [ %incdec.ptr.i.i387, %if.then.i.i388 ], [ %add.ptr39.i.i.i373, %_ZNSt6vectorIP6ColumnSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i.i374 ], [ %incdec.ptr.i.i332, %if.then.i.i333 ]
  store %class.Column** %add.ptr39.i.i.i483.sink, %class.Column*** %_M_end_of_storage.i.i440.sink, align 8, !tbaa !19
  br label %for.inc

for.inc:                                          ; preds = %for.inc.sink.split, %for.body
  %indvars.iv.next710 = add nuw nsw i64 %indvars.iv709, 1
  %exitcond712.not = icmp eq i64 %indvars.iv.next710, %wide.trip.count711
  br i1 %exitcond712.not, label %for.cond22.preheader, label %for.body, !llvm.loop !85

land.rhs:                                         ; preds = %land.rhs.preheader, %for.cond.cleanup31
  %85 = phi %"struct.std::__detail::_Hash_node"* [ %86, %for.cond.cleanup31 ], [ %.pre713, %land.rhs.preheader ]
  %rIdx.0683 = phi i32 [ %inc70, %for.cond.cleanup31 ], [ 0, %land.rhs.preheader ]
  %cmp.i.not = icmp eq %"struct.std::__detail::_Hash_node"* %85, null
  br i1 %cmp.i.not, label %for.end75, label %for.cond.cleanup31

for.cond.cleanup31:                               ; preds = %land.rhs
  %retval.sroa.0.0.copyload.i670 = bitcast %"struct.std::__detail::_Hash_node"* %85 to %"struct.std::__detail::_Hash_node"**
  %inc70 = add nuw nsw i32 %rIdx.0683, 1
  %86 = load %"struct.std::__detail::_Hash_node"*, %"struct.std::__detail::_Hash_node"** %retval.sroa.0.0.copyload.i670, align 8, !tbaa !22
  store %"struct.std::__detail::_Hash_node"* %86, %"struct.std::__detail::_Hash_node"** %_M_cur.i720727, align 8, !tbaa !76
  %exitcond708.not = icmp eq i32 %inc70, %tableRowSize
  br i1 %exitcond708.not, label %for.end75, label %land.rhs, !llvm.loop !81

for.end75:                                        ; preds = %land.rhs.us, %for.cond29.for.cond.cleanup31_crit_edge.us, %land.rhs, %for.cond.cleanup31, %for.cond22.preheader.thread, %for.cond22.preheader
  %cmp23682721 = phi i1 [ false, %for.cond22.preheader.thread ], [ false, %for.cond22.preheader ], [ true, %for.cond.cleanup31 ], [ true, %land.rhs ], [ true, %for.cond29.for.cond.cleanup31_crit_edge.us ], [ true, %land.rhs.us ]
  %cmp78677.not = icmp eq i32 %aggColSize, 0
  br i1 %cmp78677.not, label %for.cond.cleanup79, label %for.body80.lr.ph

for.body80.lr.ph:                                 ; preds = %for.end75
  %conv181 = sext i32 %tableRowSize to i64
  %mul182 = shl nsw i64 %conv181, 3
  %_M_start.i500 = getelementptr inbounds %struct.Iterator, %struct.Iterator* %iterator, i64 0, i32 1, i32 0, i32 0, i32 0
  %types.i = getelementptr inbounds %class.Table, %class.Table* %table, i64 0, i32 4
  %columnSize.i = getelementptr inbounds %class.Table, %class.Table* %table, i64 0, i32 7
  %_M_finish.i.i = getelementptr inbounds %class.Table, %class.Table* %table, i64 0, i32 3, i32 0, i32 0, i32 1
  %_M_end_of_storage.i.i = getelementptr inbounds %class.Table, %class.Table* %table, i64 0, i32 3, i32 0, i32 0, i32 2
  %_M_start.i27.i.i.i.i = getelementptr inbounds %class.Table, %class.Table* %table, i64 0, i32 3, i32 0, i32 0, i32 0
  %mul86 = shl nsw i64 %conv181, 2
  %wide.trip.count698 = zext i32 %aggColSize to i64
  %wide.trip.count = zext i32 %tableRowSize to i64
  %wide.trip.count690 = zext i32 %tableRowSize to i64
  %wide.trip.count694 = zext i32 %tableRowSize to i64
  br label %for.body80

for.cond.cleanup79:                               ; preds = %for.inc230, %for.end75
  ret void

for.body80:                                       ; preds = %for.body80.lr.ph, %for.inc230
  %indvars.iv696 = phi i64 [ 0, %for.body80.lr.ph ], [ %indvars.iv.next697, %for.inc230 ]
  %87 = trunc i64 %indvars.iv696 to i32
  %add = add i32 %87, %groupByColSize
  %idxprom81 = zext i32 %add to i64
  %arrayidx82 = getelementptr inbounds i32, i32* %types, i64 %idxprom81
  %88 = load i32, i32* %arrayidx82, align 4, !tbaa !25
  switch i32 %88, label %for.inc230 [
    i32 1, label %sw.bb83
    i32 2, label %sw.bb130
    i32 3, label %sw.bb179
  ]

sw.bb83:                                          ; preds = %for.body80
  %call87 = tail call i8* @omni_allocate(i64 %mul86)
  %89 = bitcast i8* %call87 to i32*
  br i1 %cmp23682721, label %land.rhs91.lr.ph, label %for.end122

land.rhs91.lr.ph:                                 ; preds = %sw.bb83
  %90 = load %"struct.std::__detail::_Node_iterator"*, %"struct.std::__detail::_Node_iterator"** %_M_start.i500, align 8, !tbaa !86
  %add.ptr.i648 = getelementptr inbounds %"struct.std::__detail::_Node_iterator", %"struct.std::__detail::_Node_iterator"* %90, i64 %indvars.iv696
  %_M_cur.i641 = getelementptr inbounds %"struct.std::__detail::_Node_iterator", %"struct.std::__detail::_Node_iterator"* %add.ptr.i648, i64 0, i32 0, i32 0
  %91 = bitcast %"struct.std::__detail::_Node_iterator"* %add.ptr.i648 to %"struct.std::__detail::_Hash_node"***
  %.pre717 = load %"struct.std::__detail::_Hash_node"*, %"struct.std::__detail::_Hash_node"** %_M_cur.i641, align 8, !tbaa !76
  br label %land.rhs91

land.rhs91:                                       ; preds = %land.rhs91.lr.ph, %for.body103
  %92 = phi %"struct.std::__detail::_Hash_node"* [ %.pre717, %land.rhs91.lr.ph ], [ %97, %for.body103 ]
  %indvars.iv692 = phi i64 [ 0, %land.rhs91.lr.ph ], [ %indvars.iv.next693, %for.body103 ]
  %cmp.i643.not = icmp eq %"struct.std::__detail::_Hash_node"* %92, null
  br i1 %cmp.i643.not, label %for.end122, label %for.body103

for.body103:                                      ; preds = %land.rhs91
  %second108 = getelementptr inbounds %"struct.std::__detail::_Hash_node", %"struct.std::__detail::_Hash_node"* %92, i64 0, i32 0, i32 1, i32 0, i32 0, i64 8
  %_M_start.i637 = bitcast i8* %second108 to %struct.GroupByColumn**
  %93 = load %struct.GroupByColumn*, %struct.GroupByColumn** %_M_start.i637, align 8, !tbaa !42
  %val110 = getelementptr inbounds %struct.GroupByColumn, %struct.GroupByColumn* %93, i64 0, i32 1
  %94 = bitcast i8** %val110 to i32**
  %95 = load i32*, i32** %94, align 8, !tbaa !78
  %96 = load i32, i32* %95, align 4, !tbaa !25
  %indvars.iv.next693 = add nuw nsw i64 %indvars.iv692, 1
  %arrayidx114 = getelementptr inbounds i32, i32* %89, i64 %indvars.iv692
  store i32 %96, i32* %arrayidx114, align 4, !tbaa !25
  %retval.sroa.0.0.copyload.i633669 = load %"struct.std::__detail::_Hash_node"**, %"struct.std::__detail::_Hash_node"*** %91, align 8
  %97 = load %"struct.std::__detail::_Hash_node"*, %"struct.std::__detail::_Hash_node"** %retval.sroa.0.0.copyload.i633669, align 8, !tbaa !22
  store %"struct.std::__detail::_Hash_node"* %97, %"struct.std::__detail::_Hash_node"** %_M_cur.i641, align 8, !tbaa !76
  %exitcond695.not = icmp eq i64 %indvars.iv.next693, %wide.trip.count694
  br i1 %exitcond695.not, label %for.end122, label %land.rhs91, !llvm.loop !89

for.end122:                                       ; preds = %land.rhs91, %for.body103, %sw.bb83
  %call123 = tail call noalias nonnull dereferenceable(40) i8* @_Znwm(i64 40) #18
  %98 = bitcast i8* %call123 to i32 (...)***
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [4 x i8*] }, { [4 x i8*] }* @_ZTV6Column, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %98, align 8, !tbaa !50
  %data.i629 = getelementptr inbounds i8, i8* %call123, i64 8
  %99 = bitcast i8* %data.i629 to i8**
  store i8* %call87, i8** %99, align 8, !tbaa !34
  %type.i630 = getelementptr inbounds i8, i8* %call123, i64 24
  %100 = bitcast i8* %type.i630 to i32*
  store i32 1, i32* %100, align 8, !tbaa !32
  %size.i631 = getelementptr inbounds i8, i8* %call123, i64 32
  %101 = bitcast i8* %size.i631 to i64*
  store i64 %conv181, i64* %101, align 8, !tbaa !82
  %102 = load i32*, i32** %types.i, align 8, !tbaa !61
  %103 = load i32, i32* %columnSize.i, align 8, !tbaa !83
  %idxprom.i577 = zext i32 %103 to i64
  %arrayidx.i578 = getelementptr inbounds i32, i32* %102, i64 %idxprom.i577
  store i32 1, i32* %arrayidx.i578, align 4, !tbaa !27
  %inc.i579 = add i32 %103, 1
  store i32 %inc.i579, i32* %columnSize.i, align 8, !tbaa !83
  %104 = load %class.Column**, %class.Column*** %_M_finish.i.i, align 8, !tbaa !57
  %105 = load %class.Column**, %class.Column*** %_M_end_of_storage.i.i, align 8, !tbaa !84
  %cmp.not.i.i582 = icmp eq %class.Column** %104, %105
  br i1 %cmp.not.i.i582, label %if.else.i.i598, label %if.then.i.i584

if.then.i.i584:                                   ; preds = %for.end122
  %106 = bitcast %class.Column** %104 to i8**
  store i8* %call123, i8** %106, align 8, !tbaa !19
  %107 = load %class.Column**, %class.Column*** %_M_finish.i.i, align 8, !tbaa !57
  %incdec.ptr.i.i583 = getelementptr inbounds %class.Column*, %class.Column** %107, i64 1
  br label %for.inc230.sink.split

if.else.i.i598:                                   ; preds = %for.end122
  %108 = load %class.Column**, %class.Column*** %_M_start.i27.i.i.i.i, align 8, !tbaa !29
  %sub.ptr.lhs.cast.i28.i.i.i.i586 = ptrtoint %class.Column** %104 to i64
  %sub.ptr.rhs.cast.i29.i.i.i.i587 = ptrtoint %class.Column** %108 to i64
  %sub.ptr.sub.i30.i.i.i.i588 = sub i64 %sub.ptr.lhs.cast.i28.i.i.i.i586, %sub.ptr.rhs.cast.i29.i.i.i.i587
  %sub.ptr.div.i31.i.i.i.i589 = ashr exact i64 %sub.ptr.sub.i30.i.i.i.i588, 3
  %cmp.i.i.i.i.i590 = icmp eq i64 %sub.ptr.sub.i30.i.i.i.i588, 0
  %.sroa.speculated.i.i.i.i591 = select i1 %cmp.i.i.i.i.i590, i64 1, i64 %sub.ptr.div.i31.i.i.i.i589
  %add.i.i.i.i592 = add nsw i64 %.sroa.speculated.i.i.i.i591, %sub.ptr.div.i31.i.i.i.i589
  %cmp7.i.i.i.i593 = icmp ult i64 %add.i.i.i.i592, %sub.ptr.div.i31.i.i.i.i589
  %cmp9.i.i.i.i594 = icmp ugt i64 %add.i.i.i.i592, 2305843009213693951
  %or.cond.i.i.i.i595 = or i1 %cmp7.i.i.i.i593, %cmp9.i.i.i.i594
  %cond.i.i.i.i596 = select i1 %or.cond.i.i.i.i595, i64 2305843009213693951, i64 %add.i.i.i.i592
  %cmp.not.i.i.i.i597 = icmp eq i64 %cond.i.i.i.i596, 0
  br i1 %cmp.not.i.i.i.i597, label %invoke.cont.i.i.i609, label %_ZNSt16allocator_traitsISaIP6ColumnEE8allocateERS2_m.exit.i.i.i.i604

_ZNSt16allocator_traitsISaIP6ColumnEE8allocateERS2_m.exit.i.i.i.i604: ; preds = %if.else.i.i598
  %mul.i.i.i.i.i.i599 = shl nuw i64 %cond.i.i.i.i596, 3
  %call2.i.i.i.i.i.i600 = tail call noalias nonnull i8* @_Znwm(i64 %mul.i.i.i.i.i.i599) #19
  %109 = bitcast i8* %call2.i.i.i.i.i.i600 to %class.Column**
  %.pre.i.i.i601 = load %class.Column**, %class.Column*** %_M_start.i27.i.i.i.i, align 8, !tbaa !29
  %.pre83.i.i.i602 = ptrtoint %class.Column** %.pre.i.i.i601 to i64
  %.pre84.i.i.i603 = sub i64 %sub.ptr.lhs.cast.i28.i.i.i.i586, %.pre83.i.i.i602
  br label %invoke.cont.i.i.i609

invoke.cont.i.i.i609:                             ; preds = %_ZNSt16allocator_traitsISaIP6ColumnEE8allocateERS2_m.exit.i.i.i.i604, %if.else.i.i598
  %sub.ptr.sub.i.i.i.i.i.i.i.i74.pre-phi.i.i.i605 = phi i64 [ %.pre84.i.i.i603, %_ZNSt16allocator_traitsISaIP6ColumnEE8allocateERS2_m.exit.i.i.i.i604 ], [ %sub.ptr.sub.i30.i.i.i.i588, %if.else.i.i598 ]
  %110 = phi %class.Column** [ %.pre.i.i.i601, %_ZNSt16allocator_traitsISaIP6ColumnEE8allocateERS2_m.exit.i.i.i.i604 ], [ %108, %if.else.i.i598 ]
  %cond.i67.i.i.i606 = phi %class.Column** [ %109, %_ZNSt16allocator_traitsISaIP6ColumnEE8allocateERS2_m.exit.i.i.i.i604 ], [ null, %if.else.i.i598 ]
  %add.ptr.i.i.i607 = getelementptr inbounds %class.Column*, %class.Column** %cond.i67.i.i.i606, i64 %sub.ptr.div.i31.i.i.i.i589
  %111 = bitcast %class.Column** %add.ptr.i.i.i607 to i8**
  store i8* %call123, i8** %111, align 8, !tbaa !19
  %tobool.not.i.i.i.i.i.i.i.i75.i.i.i608 = icmp eq i64 %sub.ptr.sub.i.i.i.i.i.i.i.i74.pre-phi.i.i.i605, 0
  br i1 %tobool.not.i.i.i.i.i.i.i.i75.i.i.i608, label %invoke.cont10.i.i.i617, label %if.then.i.i.i.i.i.i.i.i76.i.i.i610

if.then.i.i.i.i.i.i.i.i76.i.i.i610:               ; preds = %invoke.cont.i.i.i609
  %112 = bitcast %class.Column** %cond.i67.i.i.i606 to i8*
  %113 = bitcast %class.Column** %110 to i8*
  tail call void @llvm.memmove.p0i8.p0i8.i64(i8* align 8 %112, i8* align 8 %113, i64 %sub.ptr.sub.i.i.i.i.i.i.i.i74.pre-phi.i.i.i605, i1 false) #20
  br label %invoke.cont10.i.i.i617

invoke.cont10.i.i.i617:                           ; preds = %if.then.i.i.i.i.i.i.i.i76.i.i.i610, %invoke.cont.i.i.i609
  %sub.ptr.div.i.i.i.i.i.i.i.i77.i.i.i611 = ashr exact i64 %sub.ptr.sub.i.i.i.i.i.i.i.i74.pre-phi.i.i.i605, 3
  %add.ptr.i.i.i.i.i.i.i.i78.i.i.i612 = getelementptr inbounds %class.Column*, %class.Column** %cond.i67.i.i.i606, i64 1
  %incdec.ptr.i.i.i613 = getelementptr inbounds %class.Column*, %class.Column** %add.ptr.i.i.i.i.i.i.i.i78.i.i.i612, i64 %sub.ptr.div.i.i.i.i.i.i.i.i77.i.i.i611
  %114 = load %class.Column**, %class.Column*** %_M_finish.i.i, align 8, !tbaa !57
  %sub.ptr.lhs.cast.i.i.i.i.i.i.i.i.i.i.i614 = ptrtoint %class.Column** %114 to i64
  %sub.ptr.sub.i.i.i.i.i.i.i.i.i.i.i615 = sub i64 %sub.ptr.lhs.cast.i.i.i.i.i.i.i.i.i.i.i614, %sub.ptr.lhs.cast.i28.i.i.i.i586
  %tobool.not.i.i.i.i.i.i.i.i.i.i.i616 = icmp eq i64 %sub.ptr.sub.i.i.i.i.i.i.i.i.i.i.i615, 0
  br i1 %tobool.not.i.i.i.i.i.i.i.i.i.i.i616, label %invoke.cont15.i.i.i620, label %if.then.i.i.i.i.i.i.i.i.i.i.i618

if.then.i.i.i.i.i.i.i.i.i.i.i618:                 ; preds = %invoke.cont10.i.i.i617
  %115 = bitcast %class.Column** %incdec.ptr.i.i.i613 to i8*
  %116 = bitcast %class.Column** %104 to i8*
  tail call void @llvm.memmove.p0i8.p0i8.i64(i8* nonnull align 8 %115, i8* align 8 %116, i64 %sub.ptr.sub.i.i.i.i.i.i.i.i.i.i.i615, i1 false) #20
  br label %invoke.cont15.i.i.i620

invoke.cont15.i.i.i620:                           ; preds = %if.then.i.i.i.i.i.i.i.i.i.i.i618, %invoke.cont10.i.i.i617
  %tobool.not.i68.i.i.i619 = icmp eq %class.Column** %110, null
  br i1 %tobool.not.i68.i.i.i619, label %_ZNSt6vectorIP6ColumnSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i.i625, label %if.then.i69.i.i.i621

if.then.i69.i.i.i621:                             ; preds = %invoke.cont15.i.i.i620
  %117 = bitcast %class.Column** %110 to i8*
  tail call void @_ZdlPv(i8* nonnull %117) #20
  br label %_ZNSt6vectorIP6ColumnSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i.i625

_ZNSt6vectorIP6ColumnSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i.i625: ; preds = %if.then.i69.i.i.i621, %invoke.cont15.i.i.i620
  %sub.ptr.div.i.i.i.i.i.i.i.i.i.i.i622 = ashr exact i64 %sub.ptr.sub.i.i.i.i.i.i.i.i.i.i.i615, 3
  %add.ptr.i.i.i.i.i.i.i.i.i.i.i623 = getelementptr inbounds %class.Column*, %class.Column** %incdec.ptr.i.i.i613, i64 %sub.ptr.div.i.i.i.i.i.i.i.i.i.i.i622
  store %class.Column** %cond.i67.i.i.i606, %class.Column*** %_M_start.i27.i.i.i.i, align 8, !tbaa !29
  store %class.Column** %add.ptr.i.i.i.i.i.i.i.i.i.i.i623, %class.Column*** %_M_finish.i.i, align 8, !tbaa !57
  %add.ptr39.i.i.i624 = getelementptr inbounds %class.Column*, %class.Column** %cond.i67.i.i.i606, i64 %cond.i.i.i.i596
  br label %for.inc230.sink.split

sw.bb130:                                         ; preds = %for.body80
  %call134 = tail call i8* @omni_allocate(i64 %mul182)
  %118 = bitcast i8* %call134 to i64*
  br i1 %cmp23682721, label %land.rhs138.lr.ph, label %for.end171

land.rhs138.lr.ph:                                ; preds = %sw.bb130
  %119 = load %"struct.std::__detail::_Node_iterator"*, %"struct.std::__detail::_Node_iterator"** %_M_start.i500, align 8, !tbaa !86
  %add.ptr.i574 = getelementptr inbounds %"struct.std::__detail::_Node_iterator", %"struct.std::__detail::_Node_iterator"* %119, i64 %indvars.iv696
  %_M_cur.i567 = getelementptr inbounds %"struct.std::__detail::_Node_iterator", %"struct.std::__detail::_Node_iterator"* %add.ptr.i574, i64 0, i32 0, i32 0
  %120 = bitcast %"struct.std::__detail::_Node_iterator"* %add.ptr.i574 to %"struct.std::__detail::_Hash_node"***
  %.pre716 = load %"struct.std::__detail::_Hash_node"*, %"struct.std::__detail::_Hash_node"** %_M_cur.i567, align 8, !tbaa !76
  br label %land.rhs138

land.rhs138:                                      ; preds = %land.rhs138.lr.ph, %for.body152
  %121 = phi %"struct.std::__detail::_Hash_node"* [ %.pre716, %land.rhs138.lr.ph ], [ %126, %for.body152 ]
  %indvars.iv688 = phi i64 [ 0, %land.rhs138.lr.ph ], [ %indvars.iv.next689, %for.body152 ]
  %cmp.i569.not = icmp eq %"struct.std::__detail::_Hash_node"* %121, null
  br i1 %cmp.i569.not, label %for.end171, label %for.body152

for.body152:                                      ; preds = %land.rhs138
  %second157 = getelementptr inbounds %"struct.std::__detail::_Hash_node", %"struct.std::__detail::_Hash_node"* %121, i64 0, i32 0, i32 1, i32 0, i32 0, i64 8
  %_M_start.i563 = bitcast i8* %second157 to %struct.GroupByColumn**
  %122 = load %struct.GroupByColumn*, %struct.GroupByColumn** %_M_start.i563, align 8, !tbaa !42
  %val159 = getelementptr inbounds %struct.GroupByColumn, %struct.GroupByColumn* %122, i64 0, i32 1
  %123 = bitcast i8** %val159 to i64**
  %124 = load i64*, i64** %123, align 8, !tbaa !78
  %125 = load i64, i64* %124, align 8, !tbaa !23
  %indvars.iv.next689 = add nuw nsw i64 %indvars.iv688, 1
  %arrayidx163 = getelementptr inbounds i64, i64* %118, i64 %indvars.iv688
  store i64 %125, i64* %arrayidx163, align 8, !tbaa !23
  %retval.sroa.0.0.copyload.i559668 = load %"struct.std::__detail::_Hash_node"**, %"struct.std::__detail::_Hash_node"*** %120, align 8
  %126 = load %"struct.std::__detail::_Hash_node"*, %"struct.std::__detail::_Hash_node"** %retval.sroa.0.0.copyload.i559668, align 8, !tbaa !22
  store %"struct.std::__detail::_Hash_node"* %126, %"struct.std::__detail::_Hash_node"** %_M_cur.i567, align 8, !tbaa !76
  %exitcond691.not = icmp eq i64 %indvars.iv.next689, %wide.trip.count690
  br i1 %exitcond691.not, label %for.end171, label %land.rhs138, !llvm.loop !90

for.end171:                                       ; preds = %land.rhs138, %for.body152, %sw.bb130
  %call172 = tail call noalias nonnull dereferenceable(40) i8* @_Znwm(i64 40) #18
  %127 = bitcast i8* %call172 to i32 (...)***
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [4 x i8*] }, { [4 x i8*] }* @_ZTV6Column, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %127, align 8, !tbaa !50
  %data.i555 = getelementptr inbounds i8, i8* %call172, i64 8
  %128 = bitcast i8* %data.i555 to i8**
  store i8* %call134, i8** %128, align 8, !tbaa !34
  %type.i556 = getelementptr inbounds i8, i8* %call172, i64 24
  %129 = bitcast i8* %type.i556 to i32*
  store i32 2, i32* %129, align 8, !tbaa !32
  %size.i557 = getelementptr inbounds i8, i8* %call172, i64 32
  %130 = bitcast i8* %size.i557 to i64*
  store i64 %conv181, i64* %130, align 8, !tbaa !82
  %131 = load i32*, i32** %types.i, align 8, !tbaa !61
  %132 = load i32, i32* %columnSize.i, align 8, !tbaa !83
  %idxprom.i504 = zext i32 %132 to i64
  %arrayidx.i505 = getelementptr inbounds i32, i32* %131, i64 %idxprom.i504
  store i32 2, i32* %arrayidx.i505, align 4, !tbaa !27
  %inc.i506 = add i32 %132, 1
  store i32 %inc.i506, i32* %columnSize.i, align 8, !tbaa !83
  %133 = load %class.Column**, %class.Column*** %_M_finish.i.i, align 8, !tbaa !57
  %134 = load %class.Column**, %class.Column*** %_M_end_of_storage.i.i, align 8, !tbaa !84
  %cmp.not.i.i509 = icmp eq %class.Column** %133, %134
  br i1 %cmp.not.i.i509, label %if.else.i.i525, label %if.then.i.i511

if.then.i.i511:                                   ; preds = %for.end171
  %135 = bitcast %class.Column** %133 to i8**
  store i8* %call172, i8** %135, align 8, !tbaa !19
  %136 = load %class.Column**, %class.Column*** %_M_finish.i.i, align 8, !tbaa !57
  %incdec.ptr.i.i510 = getelementptr inbounds %class.Column*, %class.Column** %136, i64 1
  br label %for.inc230.sink.split

if.else.i.i525:                                   ; preds = %for.end171
  %137 = load %class.Column**, %class.Column*** %_M_start.i27.i.i.i.i, align 8, !tbaa !29
  %sub.ptr.lhs.cast.i28.i.i.i.i513 = ptrtoint %class.Column** %133 to i64
  %sub.ptr.rhs.cast.i29.i.i.i.i514 = ptrtoint %class.Column** %137 to i64
  %sub.ptr.sub.i30.i.i.i.i515 = sub i64 %sub.ptr.lhs.cast.i28.i.i.i.i513, %sub.ptr.rhs.cast.i29.i.i.i.i514
  %sub.ptr.div.i31.i.i.i.i516 = ashr exact i64 %sub.ptr.sub.i30.i.i.i.i515, 3
  %cmp.i.i.i.i.i517 = icmp eq i64 %sub.ptr.sub.i30.i.i.i.i515, 0
  %.sroa.speculated.i.i.i.i518 = select i1 %cmp.i.i.i.i.i517, i64 1, i64 %sub.ptr.div.i31.i.i.i.i516
  %add.i.i.i.i519 = add nsw i64 %.sroa.speculated.i.i.i.i518, %sub.ptr.div.i31.i.i.i.i516
  %cmp7.i.i.i.i520 = icmp ult i64 %add.i.i.i.i519, %sub.ptr.div.i31.i.i.i.i516
  %cmp9.i.i.i.i521 = icmp ugt i64 %add.i.i.i.i519, 2305843009213693951
  %or.cond.i.i.i.i522 = or i1 %cmp7.i.i.i.i520, %cmp9.i.i.i.i521
  %cond.i.i.i.i523 = select i1 %or.cond.i.i.i.i522, i64 2305843009213693951, i64 %add.i.i.i.i519
  %cmp.not.i.i.i.i524 = icmp eq i64 %cond.i.i.i.i523, 0
  br i1 %cmp.not.i.i.i.i524, label %invoke.cont.i.i.i536, label %_ZNSt16allocator_traitsISaIP6ColumnEE8allocateERS2_m.exit.i.i.i.i531

_ZNSt16allocator_traitsISaIP6ColumnEE8allocateERS2_m.exit.i.i.i.i531: ; preds = %if.else.i.i525
  %mul.i.i.i.i.i.i526 = shl nuw i64 %cond.i.i.i.i523, 3
  %call2.i.i.i.i.i.i527 = tail call noalias nonnull i8* @_Znwm(i64 %mul.i.i.i.i.i.i526) #19
  %138 = bitcast i8* %call2.i.i.i.i.i.i527 to %class.Column**
  %.pre.i.i.i528 = load %class.Column**, %class.Column*** %_M_start.i27.i.i.i.i, align 8, !tbaa !29
  %.pre83.i.i.i529 = ptrtoint %class.Column** %.pre.i.i.i528 to i64
  %.pre84.i.i.i530 = sub i64 %sub.ptr.lhs.cast.i28.i.i.i.i513, %.pre83.i.i.i529
  br label %invoke.cont.i.i.i536

invoke.cont.i.i.i536:                             ; preds = %_ZNSt16allocator_traitsISaIP6ColumnEE8allocateERS2_m.exit.i.i.i.i531, %if.else.i.i525
  %sub.ptr.sub.i.i.i.i.i.i.i.i74.pre-phi.i.i.i532 = phi i64 [ %.pre84.i.i.i530, %_ZNSt16allocator_traitsISaIP6ColumnEE8allocateERS2_m.exit.i.i.i.i531 ], [ %sub.ptr.sub.i30.i.i.i.i515, %if.else.i.i525 ]
  %139 = phi %class.Column** [ %.pre.i.i.i528, %_ZNSt16allocator_traitsISaIP6ColumnEE8allocateERS2_m.exit.i.i.i.i531 ], [ %137, %if.else.i.i525 ]
  %cond.i67.i.i.i533 = phi %class.Column** [ %138, %_ZNSt16allocator_traitsISaIP6ColumnEE8allocateERS2_m.exit.i.i.i.i531 ], [ null, %if.else.i.i525 ]
  %add.ptr.i.i.i534 = getelementptr inbounds %class.Column*, %class.Column** %cond.i67.i.i.i533, i64 %sub.ptr.div.i31.i.i.i.i516
  %140 = bitcast %class.Column** %add.ptr.i.i.i534 to i8**
  store i8* %call172, i8** %140, align 8, !tbaa !19
  %tobool.not.i.i.i.i.i.i.i.i75.i.i.i535 = icmp eq i64 %sub.ptr.sub.i.i.i.i.i.i.i.i74.pre-phi.i.i.i532, 0
  br i1 %tobool.not.i.i.i.i.i.i.i.i75.i.i.i535, label %invoke.cont10.i.i.i544, label %if.then.i.i.i.i.i.i.i.i76.i.i.i537

if.then.i.i.i.i.i.i.i.i76.i.i.i537:               ; preds = %invoke.cont.i.i.i536
  %141 = bitcast %class.Column** %cond.i67.i.i.i533 to i8*
  %142 = bitcast %class.Column** %139 to i8*
  tail call void @llvm.memmove.p0i8.p0i8.i64(i8* align 8 %141, i8* align 8 %142, i64 %sub.ptr.sub.i.i.i.i.i.i.i.i74.pre-phi.i.i.i532, i1 false) #20
  br label %invoke.cont10.i.i.i544

invoke.cont10.i.i.i544:                           ; preds = %if.then.i.i.i.i.i.i.i.i76.i.i.i537, %invoke.cont.i.i.i536
  %sub.ptr.div.i.i.i.i.i.i.i.i77.i.i.i538 = ashr exact i64 %sub.ptr.sub.i.i.i.i.i.i.i.i74.pre-phi.i.i.i532, 3
  %add.ptr.i.i.i.i.i.i.i.i78.i.i.i539 = getelementptr inbounds %class.Column*, %class.Column** %cond.i67.i.i.i533, i64 1
  %incdec.ptr.i.i.i540 = getelementptr inbounds %class.Column*, %class.Column** %add.ptr.i.i.i.i.i.i.i.i78.i.i.i539, i64 %sub.ptr.div.i.i.i.i.i.i.i.i77.i.i.i538
  %143 = load %class.Column**, %class.Column*** %_M_finish.i.i, align 8, !tbaa !57
  %sub.ptr.lhs.cast.i.i.i.i.i.i.i.i.i.i.i541 = ptrtoint %class.Column** %143 to i64
  %sub.ptr.sub.i.i.i.i.i.i.i.i.i.i.i542 = sub i64 %sub.ptr.lhs.cast.i.i.i.i.i.i.i.i.i.i.i541, %sub.ptr.lhs.cast.i28.i.i.i.i513
  %tobool.not.i.i.i.i.i.i.i.i.i.i.i543 = icmp eq i64 %sub.ptr.sub.i.i.i.i.i.i.i.i.i.i.i542, 0
  br i1 %tobool.not.i.i.i.i.i.i.i.i.i.i.i543, label %invoke.cont15.i.i.i547, label %if.then.i.i.i.i.i.i.i.i.i.i.i545

if.then.i.i.i.i.i.i.i.i.i.i.i545:                 ; preds = %invoke.cont10.i.i.i544
  %144 = bitcast %class.Column** %incdec.ptr.i.i.i540 to i8*
  %145 = bitcast %class.Column** %133 to i8*
  tail call void @llvm.memmove.p0i8.p0i8.i64(i8* nonnull align 8 %144, i8* align 8 %145, i64 %sub.ptr.sub.i.i.i.i.i.i.i.i.i.i.i542, i1 false) #20
  br label %invoke.cont15.i.i.i547

invoke.cont15.i.i.i547:                           ; preds = %if.then.i.i.i.i.i.i.i.i.i.i.i545, %invoke.cont10.i.i.i544
  %tobool.not.i68.i.i.i546 = icmp eq %class.Column** %139, null
  br i1 %tobool.not.i68.i.i.i546, label %_ZNSt6vectorIP6ColumnSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i.i552, label %if.then.i69.i.i.i548

if.then.i69.i.i.i548:                             ; preds = %invoke.cont15.i.i.i547
  %146 = bitcast %class.Column** %139 to i8*
  tail call void @_ZdlPv(i8* nonnull %146) #20
  br label %_ZNSt6vectorIP6ColumnSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i.i552

_ZNSt6vectorIP6ColumnSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i.i552: ; preds = %if.then.i69.i.i.i548, %invoke.cont15.i.i.i547
  %sub.ptr.div.i.i.i.i.i.i.i.i.i.i.i549 = ashr exact i64 %sub.ptr.sub.i.i.i.i.i.i.i.i.i.i.i542, 3
  %add.ptr.i.i.i.i.i.i.i.i.i.i.i550 = getelementptr inbounds %class.Column*, %class.Column** %incdec.ptr.i.i.i540, i64 %sub.ptr.div.i.i.i.i.i.i.i.i.i.i.i549
  store %class.Column** %cond.i67.i.i.i533, %class.Column*** %_M_start.i27.i.i.i.i, align 8, !tbaa !29
  store %class.Column** %add.ptr.i.i.i.i.i.i.i.i.i.i.i550, %class.Column*** %_M_finish.i.i, align 8, !tbaa !57
  %add.ptr39.i.i.i551 = getelementptr inbounds %class.Column*, %class.Column** %cond.i67.i.i.i533, i64 %cond.i.i.i.i523
  br label %for.inc230.sink.split

sw.bb179:                                         ; preds = %for.body80
  %call183 = tail call i8* @omni_allocate(i64 %mul182)
  %147 = bitcast i8* %call183 to double*
  br i1 %cmp23682721, label %land.rhs187.lr.ph, label %for.end220

land.rhs187.lr.ph:                                ; preds = %sw.bb179
  %148 = load %"struct.std::__detail::_Node_iterator"*, %"struct.std::__detail::_Node_iterator"** %_M_start.i500, align 8, !tbaa !86
  %add.ptr.i501 = getelementptr inbounds %"struct.std::__detail::_Node_iterator", %"struct.std::__detail::_Node_iterator"* %148, i64 %indvars.iv696
  %_M_cur.i495 = getelementptr inbounds %"struct.std::__detail::_Node_iterator", %"struct.std::__detail::_Node_iterator"* %add.ptr.i501, i64 0, i32 0, i32 0
  %149 = bitcast %"struct.std::__detail::_Node_iterator"* %add.ptr.i501 to %"struct.std::__detail::_Hash_node"***
  %.pre715 = load %"struct.std::__detail::_Hash_node"*, %"struct.std::__detail::_Hash_node"** %_M_cur.i495, align 8, !tbaa !76
  br label %land.rhs187

land.rhs187:                                      ; preds = %land.rhs187.lr.ph, %for.body201
  %150 = phi %"struct.std::__detail::_Hash_node"* [ %.pre715, %land.rhs187.lr.ph ], [ %155, %for.body201 ]
  %indvars.iv = phi i64 [ 0, %land.rhs187.lr.ph ], [ %indvars.iv.next, %for.body201 ]
  %cmp.i497.not = icmp eq %"struct.std::__detail::_Hash_node"* %150, null
  br i1 %cmp.i497.not, label %for.end220, label %for.body201

for.body201:                                      ; preds = %land.rhs187
  %second206 = getelementptr inbounds %"struct.std::__detail::_Hash_node", %"struct.std::__detail::_Hash_node"* %150, i64 0, i32 0, i32 1, i32 0, i32 0, i64 8
  %_M_start.i492 = bitcast i8* %second206 to %struct.GroupByColumn**
  %151 = load %struct.GroupByColumn*, %struct.GroupByColumn** %_M_start.i492, align 8, !tbaa !42
  %val208 = getelementptr inbounds %struct.GroupByColumn, %struct.GroupByColumn* %151, i64 0, i32 1
  %152 = bitcast i8** %val208 to double**
  %153 = load double*, double** %152, align 8, !tbaa !78
  %154 = load double, double* %153, align 8, !tbaa !37
  %indvars.iv.next = add nuw nsw i64 %indvars.iv, 1
  %arrayidx212 = getelementptr inbounds double, double* %147, i64 %indvars.iv
  store double %154, double* %arrayidx212, align 8, !tbaa !37
  %retval.sroa.0.0.copyload.i490667 = load %"struct.std::__detail::_Hash_node"**, %"struct.std::__detail::_Hash_node"*** %149, align 8
  %155 = load %"struct.std::__detail::_Hash_node"*, %"struct.std::__detail::_Hash_node"** %retval.sroa.0.0.copyload.i490667, align 8, !tbaa !22
  store %"struct.std::__detail::_Hash_node"* %155, %"struct.std::__detail::_Hash_node"** %_M_cur.i495, align 8, !tbaa !76
  %exitcond.not = icmp eq i64 %indvars.iv.next, %wide.trip.count
  br i1 %exitcond.not, label %for.end220, label %land.rhs187, !llvm.loop !91

for.end220:                                       ; preds = %land.rhs187, %for.body201, %sw.bb179
  %call221 = tail call noalias nonnull dereferenceable(40) i8* @_Znwm(i64 40) #18
  %156 = bitcast i8* %call221 to i32 (...)***
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [4 x i8*] }, { [4 x i8*] }* @_ZTV6Column, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %156, align 8, !tbaa !50
  %data.i486 = getelementptr inbounds i8, i8* %call221, i64 8
  %157 = bitcast i8* %data.i486 to i8**
  store i8* %call183, i8** %157, align 8, !tbaa !34
  %type.i487 = getelementptr inbounds i8, i8* %call221, i64 24
  %158 = bitcast i8* %type.i487 to i32*
  store i32 3, i32* %158, align 8, !tbaa !32
  %size.i488 = getelementptr inbounds i8, i8* %call221, i64 32
  %159 = bitcast i8* %size.i488 to i64*
  store i64 %conv181, i64* %159, align 8, !tbaa !82
  %160 = load i32*, i32** %types.i, align 8, !tbaa !61
  %161 = load i32, i32* %columnSize.i, align 8, !tbaa !83
  %idxprom.i = zext i32 %161 to i64
  %arrayidx.i = getelementptr inbounds i32, i32* %160, i64 %idxprom.i
  store i32 3, i32* %arrayidx.i, align 4, !tbaa !27
  %inc.i = add i32 %161, 1
  store i32 %inc.i, i32* %columnSize.i, align 8, !tbaa !83
  %162 = load %class.Column**, %class.Column*** %_M_finish.i.i, align 8, !tbaa !57
  %163 = load %class.Column**, %class.Column*** %_M_end_of_storage.i.i, align 8, !tbaa !84
  %cmp.not.i.i = icmp eq %class.Column** %162, %163
  br i1 %cmp.not.i.i, label %if.else.i.i, label %if.then.i.i

if.then.i.i:                                      ; preds = %for.end220
  %164 = bitcast %class.Column** %162 to i8**
  store i8* %call221, i8** %164, align 8, !tbaa !19
  %165 = load %class.Column**, %class.Column*** %_M_finish.i.i, align 8, !tbaa !57
  %incdec.ptr.i.i = getelementptr inbounds %class.Column*, %class.Column** %165, i64 1
  br label %for.inc230.sink.split

if.else.i.i:                                      ; preds = %for.end220
  %166 = load %class.Column**, %class.Column*** %_M_start.i27.i.i.i.i, align 8, !tbaa !29
  %sub.ptr.lhs.cast.i28.i.i.i.i = ptrtoint %class.Column** %162 to i64
  %sub.ptr.rhs.cast.i29.i.i.i.i = ptrtoint %class.Column** %166 to i64
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
  %call2.i.i.i.i.i.i = tail call noalias nonnull i8* @_Znwm(i64 %mul.i.i.i.i.i.i) #19
  %167 = bitcast i8* %call2.i.i.i.i.i.i to %class.Column**
  %.pre.i.i.i = load %class.Column**, %class.Column*** %_M_start.i27.i.i.i.i, align 8, !tbaa !29
  %.pre83.i.i.i = ptrtoint %class.Column** %.pre.i.i.i to i64
  %.pre84.i.i.i = sub i64 %sub.ptr.lhs.cast.i28.i.i.i.i, %.pre83.i.i.i
  br label %invoke.cont.i.i.i

invoke.cont.i.i.i:                                ; preds = %_ZNSt16allocator_traitsISaIP6ColumnEE8allocateERS2_m.exit.i.i.i.i, %if.else.i.i
  %sub.ptr.sub.i.i.i.i.i.i.i.i74.pre-phi.i.i.i = phi i64 [ %.pre84.i.i.i, %_ZNSt16allocator_traitsISaIP6ColumnEE8allocateERS2_m.exit.i.i.i.i ], [ %sub.ptr.sub.i30.i.i.i.i, %if.else.i.i ]
  %168 = phi %class.Column** [ %.pre.i.i.i, %_ZNSt16allocator_traitsISaIP6ColumnEE8allocateERS2_m.exit.i.i.i.i ], [ %166, %if.else.i.i ]
  %cond.i67.i.i.i = phi %class.Column** [ %167, %_ZNSt16allocator_traitsISaIP6ColumnEE8allocateERS2_m.exit.i.i.i.i ], [ null, %if.else.i.i ]
  %add.ptr.i.i.i = getelementptr inbounds %class.Column*, %class.Column** %cond.i67.i.i.i, i64 %sub.ptr.div.i31.i.i.i.i
  %169 = bitcast %class.Column** %add.ptr.i.i.i to i8**
  store i8* %call221, i8** %169, align 8, !tbaa !19
  %tobool.not.i.i.i.i.i.i.i.i75.i.i.i = icmp eq i64 %sub.ptr.sub.i.i.i.i.i.i.i.i74.pre-phi.i.i.i, 0
  br i1 %tobool.not.i.i.i.i.i.i.i.i75.i.i.i, label %invoke.cont10.i.i.i, label %if.then.i.i.i.i.i.i.i.i76.i.i.i

if.then.i.i.i.i.i.i.i.i76.i.i.i:                  ; preds = %invoke.cont.i.i.i
  %170 = bitcast %class.Column** %cond.i67.i.i.i to i8*
  %171 = bitcast %class.Column** %168 to i8*
  tail call void @llvm.memmove.p0i8.p0i8.i64(i8* align 8 %170, i8* align 8 %171, i64 %sub.ptr.sub.i.i.i.i.i.i.i.i74.pre-phi.i.i.i, i1 false) #20
  br label %invoke.cont10.i.i.i

invoke.cont10.i.i.i:                              ; preds = %if.then.i.i.i.i.i.i.i.i76.i.i.i, %invoke.cont.i.i.i
  %sub.ptr.div.i.i.i.i.i.i.i.i77.i.i.i = ashr exact i64 %sub.ptr.sub.i.i.i.i.i.i.i.i74.pre-phi.i.i.i, 3
  %add.ptr.i.i.i.i.i.i.i.i78.i.i.i = getelementptr inbounds %class.Column*, %class.Column** %cond.i67.i.i.i, i64 1
  %incdec.ptr.i.i.i = getelementptr inbounds %class.Column*, %class.Column** %add.ptr.i.i.i.i.i.i.i.i78.i.i.i, i64 %sub.ptr.div.i.i.i.i.i.i.i.i77.i.i.i
  %172 = load %class.Column**, %class.Column*** %_M_finish.i.i, align 8, !tbaa !57
  %sub.ptr.lhs.cast.i.i.i.i.i.i.i.i.i.i.i = ptrtoint %class.Column** %172 to i64
  %sub.ptr.sub.i.i.i.i.i.i.i.i.i.i.i = sub i64 %sub.ptr.lhs.cast.i.i.i.i.i.i.i.i.i.i.i, %sub.ptr.lhs.cast.i28.i.i.i.i
  %tobool.not.i.i.i.i.i.i.i.i.i.i.i = icmp eq i64 %sub.ptr.sub.i.i.i.i.i.i.i.i.i.i.i, 0
  br i1 %tobool.not.i.i.i.i.i.i.i.i.i.i.i, label %invoke.cont15.i.i.i, label %if.then.i.i.i.i.i.i.i.i.i.i.i

if.then.i.i.i.i.i.i.i.i.i.i.i:                    ; preds = %invoke.cont10.i.i.i
  %173 = bitcast %class.Column** %incdec.ptr.i.i.i to i8*
  %174 = bitcast %class.Column** %162 to i8*
  tail call void @llvm.memmove.p0i8.p0i8.i64(i8* nonnull align 8 %173, i8* align 8 %174, i64 %sub.ptr.sub.i.i.i.i.i.i.i.i.i.i.i, i1 false) #20
  br label %invoke.cont15.i.i.i

invoke.cont15.i.i.i:                              ; preds = %if.then.i.i.i.i.i.i.i.i.i.i.i, %invoke.cont10.i.i.i
  %tobool.not.i68.i.i.i = icmp eq %class.Column** %168, null
  br i1 %tobool.not.i68.i.i.i, label %_ZNSt6vectorIP6ColumnSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i.i, label %if.then.i69.i.i.i

if.then.i69.i.i.i:                                ; preds = %invoke.cont15.i.i.i
  %175 = bitcast %class.Column** %168 to i8*
  tail call void @_ZdlPv(i8* nonnull %175) #20
  br label %_ZNSt6vectorIP6ColumnSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i.i

_ZNSt6vectorIP6ColumnSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i.i: ; preds = %if.then.i69.i.i.i, %invoke.cont15.i.i.i
  %sub.ptr.div.i.i.i.i.i.i.i.i.i.i.i = ashr exact i64 %sub.ptr.sub.i.i.i.i.i.i.i.i.i.i.i, 3
  %add.ptr.i.i.i.i.i.i.i.i.i.i.i = getelementptr inbounds %class.Column*, %class.Column** %incdec.ptr.i.i.i, i64 %sub.ptr.div.i.i.i.i.i.i.i.i.i.i.i
  store %class.Column** %cond.i67.i.i.i, %class.Column*** %_M_start.i27.i.i.i.i, align 8, !tbaa !29
  store %class.Column** %add.ptr.i.i.i.i.i.i.i.i.i.i.i, %class.Column*** %_M_finish.i.i, align 8, !tbaa !57
  %add.ptr39.i.i.i = getelementptr inbounds %class.Column*, %class.Column** %cond.i67.i.i.i, i64 %cond.i.i.i.i
  br label %for.inc230.sink.split

for.inc230.sink.split:                            ; preds = %if.then.i.i584, %_ZNSt6vectorIP6ColumnSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i.i625, %if.then.i.i511, %_ZNSt6vectorIP6ColumnSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i.i552, %if.then.i.i, %_ZNSt6vectorIP6ColumnSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i.i
  %_M_end_of_storage.i.i.sink = phi %class.Column*** [ %_M_end_of_storage.i.i, %_ZNSt6vectorIP6ColumnSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i.i ], [ %_M_finish.i.i, %if.then.i.i ], [ %_M_end_of_storage.i.i, %_ZNSt6vectorIP6ColumnSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i.i552 ], [ %_M_finish.i.i, %if.then.i.i511 ], [ %_M_end_of_storage.i.i, %_ZNSt6vectorIP6ColumnSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i.i625 ], [ %_M_finish.i.i, %if.then.i.i584 ]
  %add.ptr39.i.i.i.sink = phi %class.Column** [ %add.ptr39.i.i.i, %_ZNSt6vectorIP6ColumnSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i.i ], [ %incdec.ptr.i.i, %if.then.i.i ], [ %add.ptr39.i.i.i551, %_ZNSt6vectorIP6ColumnSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i.i552 ], [ %incdec.ptr.i.i510, %if.then.i.i511 ], [ %add.ptr39.i.i.i624, %_ZNSt6vectorIP6ColumnSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i.i625 ], [ %incdec.ptr.i.i583, %if.then.i.i584 ]
  store %class.Column** %add.ptr39.i.i.i.sink, %class.Column*** %_M_end_of_storage.i.i.sink, align 8, !tbaa !19
  br label %for.inc230

for.inc230:                                       ; preds = %for.inc230.sink.split, %for.body80
  %indvars.iv.next697 = add nuw nsw i64 %indvars.iv696, 1
  %exitcond699.not = icmp eq i64 %indvars.iv.next697, %wide.trip.count698
  br i1 %exitcond699.not, label %for.cond.cleanup79, label %for.body80, !llvm.loop !92
}

; Function Attrs: nobuiltin nounwind
declare dso_local void @_ZdlPv(i8*) local_unnamed_addr #12

; Function Attrs: uwtable
define dso_local i32 @_ZN11HashGroupBy9getResultERSt6vectorIP5TableSaIS2_EE(%class.HashGroupBy* nocapture nonnull readonly dereferenceable(144) %this, %"class.std::vector.38"* nocapture nonnull align 8 dereferenceable(24) %result) local_unnamed_addr #6 align 2 personality i8* bitcast (i32 (...)* @__gxx_personality_v0 to i8*) {
entry:
  %iterator = alloca %struct.Iterator, align 8
  %_M_finish.i = getelementptr inbounds %class.HashGroupBy, %class.HashGroupBy* %this, i64 0, i32 1, i32 0, i32 0, i32 1
  %0 = load %struct.ColumnIndex*, %struct.ColumnIndex** %_M_finish.i, align 8, !tbaa !2
  %_M_start.i = getelementptr inbounds %class.HashGroupBy, %class.HashGroupBy* %this, i64 0, i32 1, i32 0, i32 0, i32 0
  %1 = load %struct.ColumnIndex*, %struct.ColumnIndex** %_M_start.i, align 8, !tbaa !8
  %sub.ptr.lhs.cast.i = ptrtoint %struct.ColumnIndex* %0 to i64
  %sub.ptr.rhs.cast.i = ptrtoint %struct.ColumnIndex* %1 to i64
  %sub.ptr.sub.i = sub i64 %sub.ptr.lhs.cast.i, %sub.ptr.rhs.cast.i
  %2 = lshr exact i64 %sub.ptr.sub.i, 3
  %conv = trunc i64 %2 to i32
  %_M_finish.i185 = getelementptr inbounds %class.HashGroupBy, %class.HashGroupBy* %this, i64 0, i32 2, i32 0, i32 0, i32 1
  %3 = load %struct.ColumnIndex*, %struct.ColumnIndex** %_M_finish.i185, align 8, !tbaa !2
  %_M_start.i186 = getelementptr inbounds %class.HashGroupBy, %class.HashGroupBy* %this, i64 0, i32 2, i32 0, i32 0, i32 0
  %4 = load %struct.ColumnIndex*, %struct.ColumnIndex** %_M_start.i186, align 8, !tbaa !8
  %sub.ptr.lhs.cast.i187 = ptrtoint %struct.ColumnIndex* %3 to i64
  %sub.ptr.rhs.cast.i188 = ptrtoint %struct.ColumnIndex* %4 to i64
  %sub.ptr.sub.i189 = sub i64 %sub.ptr.lhs.cast.i187, %sub.ptr.rhs.cast.i188
  %5 = lshr exact i64 %sub.ptr.sub.i189, 3
  %conv3 = trunc i64 %5 to i32
  %add = add i32 %conv3, %conv
  %conv4 = zext i32 %add to i64
  %mul = shl nuw nsw i64 %conv4, 2
  %call5 = tail call i8* @omni_allocate(i64 %mul)
  %6 = bitcast i8* %call5 to i32*
  %7 = load %struct.ColumnIndex*, %struct.ColumnIndex** %_M_start.i, align 8, !tbaa !19
  %8 = load %struct.ColumnIndex*, %struct.ColumnIndex** %_M_finish.i, align 8, !tbaa !19
  %cmp.i.not264 = icmp eq %struct.ColumnIndex* %7, %8
  br i1 %cmp.i.not264, label %for.cond.cleanup, label %for.body

for.cond.cleanup.loopexit:                        ; preds = %sw.epilog
  %phi.cast = and i64 %indvars.iv.next274, 4294967295
  br label %for.cond.cleanup

for.cond.cleanup:                                 ; preds = %for.cond.cleanup.loopexit, %entry
  %rowSize.0.lcssa = phi i32 [ 0, %entry ], [ %rowSize.1, %for.cond.cleanup.loopexit ]
  %idx.0.lcssa = phi i64 [ 0, %entry ], [ %phi.cast, %for.cond.cleanup.loopexit ]
  %9 = load %struct.ColumnIndex*, %struct.ColumnIndex** %_M_start.i186, align 8, !tbaa !19
  %10 = load %struct.ColumnIndex*, %struct.ColumnIndex** %_M_finish.i185, align 8, !tbaa !19
  %cmp.i225.not260 = icmp eq %struct.ColumnIndex* %9, %10
  br i1 %cmp.i225.not260, label %for.cond.cleanup35, label %for.body36

for.body:                                         ; preds = %entry, %sw.epilog
  %indvars.iv273 = phi i64 [ %indvars.iv.next274, %sw.epilog ], [ 0, %entry ]
  %rowSize.0266 = phi i32 [ %rowSize.1, %sw.epilog ], [ 0, %entry ]
  %__begin1.sroa.0.0265 = phi %struct.ColumnIndex* [ %incdec.ptr.i211, %sw.epilog ], [ %7, %entry ]
  %type = getelementptr inbounds %struct.ColumnIndex, %struct.ColumnIndex* %__begin1.sroa.0.0265, i64 0, i32 1
  %11 = load i32, i32* %type, align 4, !tbaa !93
  %indvars.iv.next274 = add nuw nsw i64 %indvars.iv273, 1
  %arrayidx = getelementptr inbounds i32, i32* %6, i64 %indvars.iv273
  store i32 %11, i32* %arrayidx, align 4, !tbaa !25
  switch i32 %11, label %sw.epilog [
    i32 1, label %sw.bb
    i32 2, label %sw.bb16
    i32 3, label %sw.bb20
  ]

sw.bb:                                            ; preds = %for.body
  %add14 = add i32 %rowSize.0266, 4
  br label %sw.epilog

sw.bb16:                                          ; preds = %for.body
  %add18 = add i32 %rowSize.0266, 8
  br label %sw.epilog

sw.bb20:                                          ; preds = %for.body
  %add22 = add i32 %rowSize.0266, 8
  br label %sw.epilog

sw.epilog:                                        ; preds = %for.body, %sw.bb20, %sw.bb16, %sw.bb
  %rowSize.1 = phi i32 [ %rowSize.0266, %for.body ], [ %add22, %sw.bb20 ], [ %add18, %sw.bb16 ], [ %add14, %sw.bb ]
  %incdec.ptr.i211 = getelementptr inbounds %struct.ColumnIndex, %struct.ColumnIndex* %__begin1.sroa.0.0265, i64 1
  %cmp.i.not = icmp eq %struct.ColumnIndex* %incdec.ptr.i211, %8
  br i1 %cmp.i.not, label %for.cond.cleanup.loopexit, label %for.body

for.cond.cleanup35:                               ; preds = %sw.epilog57, %for.cond.cleanup
  %rowSize.2.lcssa = phi i32 [ %rowSize.0.lcssa, %for.cond.cleanup ], [ %rowSize.3, %sw.epilog57 ]
  %div = sdiv i32 1048576, %rowSize.2.lcssa
  %_M_element_count.i.i222 = getelementptr inbounds %class.HashGroupBy, %class.HashGroupBy* %this, i64 0, i32 4, i32 0, i32 3
  %12 = load i64, i64* %_M_element_count.i.i222, align 8, !tbaa !94
  %conv62 = uitofp i64 %12 to double
  %conv63 = sitofp i32 %div to double
  %div64 = fdiv double %conv62, %conv63
  %13 = tail call double @llvm.ceil.f64(double %div64)
  %conv65 = fptosi double %13 to i32
  %14 = bitcast %struct.Iterator* %iterator to i8*
  call void @llvm.lifetime.start.p0i8(i64 32, i8* nonnull %14) #20
  %15 = getelementptr inbounds %struct.Iterator, %struct.Iterator* %iterator, i64 0, i32 1
  %16 = bitcast %"class.std::vector.33"* %15 to i8*
  call void @llvm.memset.p0i8.i64(i8* nonnull align 8 dereferenceable(32) %16, i8 0, i64 24, i1 false) #20
  %_M_nxt.i.i.i221 = getelementptr inbounds %class.HashGroupBy, %class.HashGroupBy* %this, i64 0, i32 4, i32 0, i32 2, i32 0
  %17 = bitcast %"struct.std::__detail::_Hash_node_base"** %_M_nxt.i.i.i221 to %"struct.std::__detail::_Hash_node"**
  %18 = load %"struct.std::__detail::_Hash_node"*, %"struct.std::__detail::_Hash_node"** %17, align 8, !tbaa !95
  %ref.tmp.sroa.0.0..sroa_idx = getelementptr inbounds %struct.Iterator, %struct.Iterator* %iterator, i64 0, i32 0, i32 0, i32 0
  store %"struct.std::__detail::_Hash_node"* %18, %"struct.std::__detail::_Hash_node"** %ref.tmp.sroa.0.0..sroa_idx, align 8
  %_M_start.i220 = getelementptr inbounds %class.HashGroupBy, %class.HashGroupBy* %this, i64 0, i32 3, i32 0, i32 0, i32 0
  %19 = load %class.Aggregator**, %class.Aggregator*** %_M_start.i220, align 8, !tbaa !19
  %_M_finish.i219 = getelementptr inbounds %class.HashGroupBy, %class.HashGroupBy* %this, i64 0, i32 3, i32 0, i32 0, i32 1
  %20 = load %class.Aggregator**, %class.Aggregator*** %_M_finish.i219, align 8, !tbaa !19
  %cmp.i215.not258 = icmp eq %class.Aggregator** %19, %20
  br i1 %cmp.i215.not258, label %for.cond92.preheader, label %for.body80.lr.ph

for.body80.lr.ph:                                 ; preds = %for.cond.cleanup35
  %_M_finish.i.i = getelementptr inbounds %struct.Iterator, %struct.Iterator* %iterator, i64 0, i32 1, i32 0, i32 0, i32 1
  %_M_end_of_storage.i.i = getelementptr inbounds %struct.Iterator, %struct.Iterator* %iterator, i64 0, i32 1, i32 0, i32 0, i32 2
  %_M_start.i27.i.i = getelementptr inbounds %struct.Iterator, %struct.Iterator* %iterator, i64 0, i32 1, i32 0, i32 0, i32 0
  br label %for.body80

for.body36:                                       ; preds = %for.cond.cleanup, %sw.epilog57
  %indvars.iv = phi i64 [ %indvars.iv.next, %sw.epilog57 ], [ %idx.0.lcssa, %for.cond.cleanup ]
  %rowSize.2262 = phi i32 [ %rowSize.3, %sw.epilog57 ], [ %rowSize.0.lcssa, %for.cond.cleanup ]
  %__begin127.sroa.0.0261 = phi %struct.ColumnIndex* [ %incdec.ptr.i217, %sw.epilog57 ], [ %9, %for.cond.cleanup ]
  %type39 = getelementptr inbounds %struct.ColumnIndex, %struct.ColumnIndex* %__begin127.sroa.0.0261, i64 0, i32 1
  %21 = load i32, i32* %type39, align 4, !tbaa !93
  %indvars.iv.next = add nuw nsw i64 %indvars.iv, 1
  %arrayidx42 = getelementptr inbounds i32, i32* %6, i64 %indvars.iv
  store i32 %21, i32* %arrayidx42, align 4, !tbaa !25
  switch i32 %21, label %sw.epilog57 [
    i32 1, label %sw.bb44
    i32 2, label %sw.bb48
    i32 3, label %sw.bb52
  ]

sw.bb44:                                          ; preds = %for.body36
  %add46 = add i32 %rowSize.2262, 4
  br label %sw.epilog57

sw.bb48:                                          ; preds = %for.body36
  %add50 = add i32 %rowSize.2262, 8
  br label %sw.epilog57

sw.bb52:                                          ; preds = %for.body36
  %add54 = add i32 %rowSize.2262, 8
  br label %sw.epilog57

sw.epilog57:                                      ; preds = %for.body36, %sw.bb52, %sw.bb48, %sw.bb44
  %rowSize.3 = phi i32 [ %rowSize.2262, %for.body36 ], [ %add54, %sw.bb52 ], [ %add50, %sw.bb48 ], [ %add46, %sw.bb44 ]
  %incdec.ptr.i217 = getelementptr inbounds %struct.ColumnIndex, %struct.ColumnIndex* %__begin127.sroa.0.0261, i64 1
  %cmp.i225.not = icmp eq %struct.ColumnIndex* %incdec.ptr.i217, %10
  br i1 %cmp.i225.not, label %for.cond.cleanup35, label %for.body36

for.cond92.preheader:                             ; preds = %invoke.cont87, %for.cond.cleanup35
  %cmp255 = icmp sgt i32 %conv65, 0
  br i1 %cmp255, label %for.body94.lr.ph, label %for.cond.cleanup93

for.body94.lr.ph:                                 ; preds = %for.cond92.preheader
  %_M_finish.i196 = getelementptr inbounds %"class.std::vector.38", %"class.std::vector.38"* %result, i64 0, i32 0, i32 0, i32 1
  %_M_end_of_storage.i = getelementptr inbounds %"class.std::vector.38", %"class.std::vector.38"* %result, i64 0, i32 0, i32 0, i32 2
  %_M_start.i27.i.i.i = getelementptr inbounds %"class.std::vector.38", %"class.std::vector.38"* %result, i64 0, i32 0, i32 0, i32 0
  br label %for.body94

for.body80:                                       ; preds = %invoke.cont87.for.body80_crit_edge, %for.body80.lr.ph
  %22 = phi %"struct.std::__detail::_Node_iterator"* [ null, %for.body80.lr.ph ], [ %.pre, %invoke.cont87.for.body80_crit_edge ]
  %23 = phi %"struct.std::__detail::_Node_iterator"* [ null, %for.body80.lr.ph ], [ %125, %invoke.cont87.for.body80_crit_edge ]
  %__begin171.sroa.0.0259 = phi %class.Aggregator** [ %19, %for.body80.lr.ph ], [ %incdec.ptr.i203, %invoke.cont87.for.body80_crit_edge ]
  %24 = ptrtoint %"struct.std::__detail::_Node_iterator"* %22 to i64
  %25 = load %class.Aggregator*, %class.Aggregator** %__begin171.sroa.0.0259, align 8, !tbaa !19
  %_M_nxt.i.i.i = getelementptr inbounds %class.Aggregator, %class.Aggregator* %25, i64 0, i32 3, i32 0, i32 2, i32 0
  %26 = bitcast %"struct.std::__detail::_Hash_node_base"** %_M_nxt.i.i.i to %"struct.std::__detail::_Hash_node"**
  %27 = load %"struct.std::__detail::_Hash_node"*, %"struct.std::__detail::_Hash_node"** %26, align 8, !tbaa !95
  %cmp.not.i.i = icmp eq %"struct.std::__detail::_Node_iterator"* %23, %22
  br i1 %cmp.not.i.i, label %if.else.i.i, label %if.then.i.i

if.then.i.i:                                      ; preds = %for.body80
  %28 = bitcast %"struct.std::__detail::_Node_iterator"* %23 to i64*
  %29 = ptrtoint %"struct.std::__detail::_Hash_node"* %27 to i64
  store i64 %29, i64* %28, align 8
  %incdec.ptr.i.i204 = getelementptr inbounds %"struct.std::__detail::_Node_iterator", %"struct.std::__detail::_Node_iterator"* %23, i64 1
  store %"struct.std::__detail::_Node_iterator"* %incdec.ptr.i.i204, %"struct.std::__detail::_Node_iterator"** %_M_finish.i.i, align 8, !tbaa !96
  br label %invoke.cont87

if.else.i.i:                                      ; preds = %for.body80
  %30 = load %"struct.std::__detail::_Node_iterator"*, %"struct.std::__detail::_Node_iterator"** %_M_start.i27.i.i, align 8, !tbaa !86
  %31 = ptrtoint %"struct.std::__detail::_Node_iterator"* %30 to i64
  %sub.ptr.lhs.cast.i28.i.i = ptrtoint %"struct.std::__detail::_Node_iterator"* %22 to i64
  %sub.ptr.rhs.cast.i29.i.i = ptrtoint %"struct.std::__detail::_Node_iterator"* %30 to i64
  %sub.ptr.sub.i30.i.i = sub i64 %sub.ptr.lhs.cast.i28.i.i, %sub.ptr.rhs.cast.i29.i.i
  %sub.ptr.div.i31.i.i = ashr exact i64 %sub.ptr.sub.i30.i.i, 3
  %cmp.i.i.i = icmp eq i64 %sub.ptr.sub.i30.i.i, 0
  %.sroa.speculated.i.i = select i1 %cmp.i.i.i, i64 1, i64 %sub.ptr.div.i31.i.i
  %add.i.i = add nsw i64 %.sroa.speculated.i.i, %sub.ptr.div.i31.i.i
  %cmp7.i.i = icmp ult i64 %add.i.i, %sub.ptr.div.i31.i.i
  %cmp9.i.i = icmp ugt i64 %add.i.i, 2305843009213693951
  %or.cond.i.i = or i1 %cmp7.i.i, %cmp9.i.i
  %cond.i.i = select i1 %or.cond.i.i, i64 2305843009213693951, i64 %add.i.i
  %cmp.not.i.i205 = icmp eq i64 %cond.i.i, 0
  br i1 %cmp.not.i.i205, label %invoke.cont.i, label %_ZNSt16allocator_traitsISaINSt8__detail14_Node_iteratorISt4pairIKmSt6vectorI13GroupByColumnSaIS5_EEELb0ELb0EEEEE8allocateERSA_m.exit.i.i

_ZNSt16allocator_traitsISaINSt8__detail14_Node_iteratorISt4pairIKmSt6vectorI13GroupByColumnSaIS5_EEELb0ELb0EEEEE8allocateERSA_m.exit.i.i: ; preds = %if.else.i.i
  %mul.i.i.i.i = shl nuw i64 %cond.i.i, 3
  %call2.i.i.i.i208 = invoke noalias nonnull i8* @_Znwm(i64 %mul.i.i.i.i) #19
          to label %call2.i.i.i.i.noexc unwind label %lpad

call2.i.i.i.i.noexc:                              ; preds = %_ZNSt16allocator_traitsISaINSt8__detail14_Node_iteratorISt4pairIKmSt6vectorI13GroupByColumnSaIS5_EEELb0ELb0EEEEE8allocateERSA_m.exit.i.i
  %32 = bitcast i8* %call2.i.i.i.i208 to %"struct.std::__detail::_Node_iterator"*
  br label %invoke.cont.i

invoke.cont.i:                                    ; preds = %call2.i.i.i.i.noexc, %if.else.i.i
  %cond.i67.i = phi %"struct.std::__detail::_Node_iterator"* [ %32, %call2.i.i.i.i.noexc ], [ null, %if.else.i.i ]
  %add.ptr.i = getelementptr inbounds %"struct.std::__detail::_Node_iterator", %"struct.std::__detail::_Node_iterator"* %cond.i67.i, i64 %sub.ptr.div.i31.i.i
  %33 = bitcast %"struct.std::__detail::_Node_iterator"* %add.ptr.i to i64*
  %34 = ptrtoint %"struct.std::__detail::_Hash_node"* %27 to i64
  store i64 %34, i64* %33, align 8
  %cmp.i.i.not22.i.i.i.i72.i = icmp eq %"struct.std::__detail::_Node_iterator"* %30, %22
  br i1 %cmp.i.i.not22.i.i.i.i72.i, label %invoke.cont10.i.thread, label %for.body.i.i.i.i78.i.preheader

for.body.i.i.i.i78.i.preheader:                   ; preds = %invoke.cont.i
  %scevgep303 = getelementptr %"struct.std::__detail::_Node_iterator", %"struct.std::__detail::_Node_iterator"* %22, i64 -1
  %35 = ptrtoint %"struct.std::__detail::_Node_iterator"* %scevgep303 to i64
  %36 = sub i64 %35, %31
  %37 = lshr i64 %36, 3
  %38 = add nuw nsw i64 %37, 1
  %min.iters.check307 = icmp ult i64 %36, 24
  br i1 %min.iters.check307, label %for.body.i.i.i.i78.i.preheader342, label %vector.memcheck309

vector.memcheck309:                               ; preds = %for.body.i.i.i.i78.i.preheader
  %scevgep312 = getelementptr %"struct.std::__detail::_Node_iterator", %"struct.std::__detail::_Node_iterator"* %cond.i67.i, i64 1
  %scevgep313 = getelementptr %"struct.std::__detail::_Node_iterator", %"struct.std::__detail::_Node_iterator"* %22, i64 -1
  %39 = ptrtoint %"struct.std::__detail::_Node_iterator"* %scevgep313 to i64
  %40 = sub i64 %39, %31
  %41 = lshr i64 %40, 3
  %scevgep317 = getelementptr %"struct.std::__detail::_Node_iterator", %"struct.std::__detail::_Node_iterator"* %scevgep312, i64 %41
  %scevgep319 = getelementptr %"struct.std::__detail::_Node_iterator", %"struct.std::__detail::_Node_iterator"* %30, i64 1
  %scevgep320 = getelementptr %"struct.std::__detail::_Node_iterator", %"struct.std::__detail::_Node_iterator"* %scevgep319, i64 %41
  %bound0322 = icmp ult %"struct.std::__detail::_Node_iterator"* %cond.i67.i, %scevgep320
  %bound1323 = icmp ult %"struct.std::__detail::_Node_iterator"* %30, %scevgep317
  %found.conflict324 = and i1 %bound0322, %bound1323
  br i1 %found.conflict324, label %for.body.i.i.i.i78.i.preheader342, label %vector.ph310

vector.ph310:                                     ; preds = %vector.memcheck309
  %n.vec327 = and i64 %38, 4611686018427387900
  %ind.end331 = getelementptr %"struct.std::__detail::_Node_iterator", %"struct.std::__detail::_Node_iterator"* %cond.i67.i, i64 %n.vec327
  %ind.end333 = getelementptr %"struct.std::__detail::_Node_iterator", %"struct.std::__detail::_Node_iterator"* %30, i64 %n.vec327
  %42 = add nsw i64 %n.vec327, -4
  %43 = lshr exact i64 %42, 2
  %44 = add nuw nsw i64 %43, 1
  %xtraiter = and i64 %44, 3
  %45 = icmp ult i64 %42, 12
  br i1 %45, label %middle.block300.unr-lcssa, label %vector.ph310.new

vector.ph310.new:                                 ; preds = %vector.ph310
  %unroll_iter = and i64 %44, 9223372036854775804
  br label %vector.body302

vector.body302:                                   ; preds = %vector.body302, %vector.ph310.new
  %index328 = phi i64 [ 0, %vector.ph310.new ], [ %index.next329.3, %vector.body302 ]
  %niter = phi i64 [ %unroll_iter, %vector.ph310.new ], [ %niter.nsub.3, %vector.body302 ]
  %next.gep335 = getelementptr %"struct.std::__detail::_Node_iterator", %"struct.std::__detail::_Node_iterator"* %cond.i67.i, i64 %index328
  %next.gep337 = getelementptr %"struct.std::__detail::_Node_iterator", %"struct.std::__detail::_Node_iterator"* %30, i64 %index328
  %46 = bitcast %"struct.std::__detail::_Node_iterator"* %next.gep337 to <2 x i64>*
  %wide.load339 = load <2 x i64>, <2 x i64>* %46, align 8, !alias.scope !97
  %47 = getelementptr %"struct.std::__detail::_Node_iterator", %"struct.std::__detail::_Node_iterator"* %next.gep337, i64 2
  %48 = bitcast %"struct.std::__detail::_Node_iterator"* %47 to <2 x i64>*
  %wide.load340 = load <2 x i64>, <2 x i64>* %48, align 8, !alias.scope !97
  %49 = bitcast %"struct.std::__detail::_Node_iterator"* %next.gep335 to <2 x i64>*
  store <2 x i64> %wide.load339, <2 x i64>* %49, align 8, !alias.scope !100, !noalias !97
  %50 = getelementptr %"struct.std::__detail::_Node_iterator", %"struct.std::__detail::_Node_iterator"* %next.gep335, i64 2
  %51 = bitcast %"struct.std::__detail::_Node_iterator"* %50 to <2 x i64>*
  store <2 x i64> %wide.load340, <2 x i64>* %51, align 8, !alias.scope !100, !noalias !97
  %index.next329 = or i64 %index328, 4
  %next.gep335.1 = getelementptr %"struct.std::__detail::_Node_iterator", %"struct.std::__detail::_Node_iterator"* %cond.i67.i, i64 %index.next329
  %next.gep337.1 = getelementptr %"struct.std::__detail::_Node_iterator", %"struct.std::__detail::_Node_iterator"* %30, i64 %index.next329
  %52 = bitcast %"struct.std::__detail::_Node_iterator"* %next.gep337.1 to <2 x i64>*
  %wide.load339.1 = load <2 x i64>, <2 x i64>* %52, align 8, !alias.scope !97
  %53 = getelementptr %"struct.std::__detail::_Node_iterator", %"struct.std::__detail::_Node_iterator"* %next.gep337.1, i64 2
  %54 = bitcast %"struct.std::__detail::_Node_iterator"* %53 to <2 x i64>*
  %wide.load340.1 = load <2 x i64>, <2 x i64>* %54, align 8, !alias.scope !97
  %55 = bitcast %"struct.std::__detail::_Node_iterator"* %next.gep335.1 to <2 x i64>*
  store <2 x i64> %wide.load339.1, <2 x i64>* %55, align 8, !alias.scope !100, !noalias !97
  %56 = getelementptr %"struct.std::__detail::_Node_iterator", %"struct.std::__detail::_Node_iterator"* %next.gep335.1, i64 2
  %57 = bitcast %"struct.std::__detail::_Node_iterator"* %56 to <2 x i64>*
  store <2 x i64> %wide.load340.1, <2 x i64>* %57, align 8, !alias.scope !100, !noalias !97
  %index.next329.1 = or i64 %index328, 8
  %next.gep335.2 = getelementptr %"struct.std::__detail::_Node_iterator", %"struct.std::__detail::_Node_iterator"* %cond.i67.i, i64 %index.next329.1
  %next.gep337.2 = getelementptr %"struct.std::__detail::_Node_iterator", %"struct.std::__detail::_Node_iterator"* %30, i64 %index.next329.1
  %58 = bitcast %"struct.std::__detail::_Node_iterator"* %next.gep337.2 to <2 x i64>*
  %wide.load339.2 = load <2 x i64>, <2 x i64>* %58, align 8, !alias.scope !97
  %59 = getelementptr %"struct.std::__detail::_Node_iterator", %"struct.std::__detail::_Node_iterator"* %next.gep337.2, i64 2
  %60 = bitcast %"struct.std::__detail::_Node_iterator"* %59 to <2 x i64>*
  %wide.load340.2 = load <2 x i64>, <2 x i64>* %60, align 8, !alias.scope !97
  %61 = bitcast %"struct.std::__detail::_Node_iterator"* %next.gep335.2 to <2 x i64>*
  store <2 x i64> %wide.load339.2, <2 x i64>* %61, align 8, !alias.scope !100, !noalias !97
  %62 = getelementptr %"struct.std::__detail::_Node_iterator", %"struct.std::__detail::_Node_iterator"* %next.gep335.2, i64 2
  %63 = bitcast %"struct.std::__detail::_Node_iterator"* %62 to <2 x i64>*
  store <2 x i64> %wide.load340.2, <2 x i64>* %63, align 8, !alias.scope !100, !noalias !97
  %index.next329.2 = or i64 %index328, 12
  %next.gep335.3 = getelementptr %"struct.std::__detail::_Node_iterator", %"struct.std::__detail::_Node_iterator"* %cond.i67.i, i64 %index.next329.2
  %next.gep337.3 = getelementptr %"struct.std::__detail::_Node_iterator", %"struct.std::__detail::_Node_iterator"* %30, i64 %index.next329.2
  %64 = bitcast %"struct.std::__detail::_Node_iterator"* %next.gep337.3 to <2 x i64>*
  %wide.load339.3 = load <2 x i64>, <2 x i64>* %64, align 8, !alias.scope !97
  %65 = getelementptr %"struct.std::__detail::_Node_iterator", %"struct.std::__detail::_Node_iterator"* %next.gep337.3, i64 2
  %66 = bitcast %"struct.std::__detail::_Node_iterator"* %65 to <2 x i64>*
  %wide.load340.3 = load <2 x i64>, <2 x i64>* %66, align 8, !alias.scope !97
  %67 = bitcast %"struct.std::__detail::_Node_iterator"* %next.gep335.3 to <2 x i64>*
  store <2 x i64> %wide.load339.3, <2 x i64>* %67, align 8, !alias.scope !100, !noalias !97
  %68 = getelementptr %"struct.std::__detail::_Node_iterator", %"struct.std::__detail::_Node_iterator"* %next.gep335.3, i64 2
  %69 = bitcast %"struct.std::__detail::_Node_iterator"* %68 to <2 x i64>*
  store <2 x i64> %wide.load340.3, <2 x i64>* %69, align 8, !alias.scope !100, !noalias !97
  %index.next329.3 = add i64 %index328, 16
  %niter.nsub.3 = add i64 %niter, -4
  %niter.ncmp.3 = icmp eq i64 %niter.nsub.3, 0
  br i1 %niter.ncmp.3, label %middle.block300.unr-lcssa, label %vector.body302, !llvm.loop !102

middle.block300.unr-lcssa:                        ; preds = %vector.body302, %vector.ph310
  %index328.unr = phi i64 [ 0, %vector.ph310 ], [ %index.next329.3, %vector.body302 ]
  %lcmp.mod.not = icmp eq i64 %xtraiter, 0
  br i1 %lcmp.mod.not, label %middle.block300, label %vector.body302.epil

vector.body302.epil:                              ; preds = %middle.block300.unr-lcssa, %vector.body302.epil
  %index328.epil = phi i64 [ %index.next329.epil, %vector.body302.epil ], [ %index328.unr, %middle.block300.unr-lcssa ]
  %epil.iter = phi i64 [ %epil.iter.sub, %vector.body302.epil ], [ %xtraiter, %middle.block300.unr-lcssa ]
  %next.gep335.epil = getelementptr %"struct.std::__detail::_Node_iterator", %"struct.std::__detail::_Node_iterator"* %cond.i67.i, i64 %index328.epil
  %next.gep337.epil = getelementptr %"struct.std::__detail::_Node_iterator", %"struct.std::__detail::_Node_iterator"* %30, i64 %index328.epil
  %70 = bitcast %"struct.std::__detail::_Node_iterator"* %next.gep337.epil to <2 x i64>*
  %wide.load339.epil = load <2 x i64>, <2 x i64>* %70, align 8, !alias.scope !97
  %71 = getelementptr %"struct.std::__detail::_Node_iterator", %"struct.std::__detail::_Node_iterator"* %next.gep337.epil, i64 2
  %72 = bitcast %"struct.std::__detail::_Node_iterator"* %71 to <2 x i64>*
  %wide.load340.epil = load <2 x i64>, <2 x i64>* %72, align 8, !alias.scope !97
  %73 = bitcast %"struct.std::__detail::_Node_iterator"* %next.gep335.epil to <2 x i64>*
  store <2 x i64> %wide.load339.epil, <2 x i64>* %73, align 8, !alias.scope !100, !noalias !97
  %74 = getelementptr %"struct.std::__detail::_Node_iterator", %"struct.std::__detail::_Node_iterator"* %next.gep335.epil, i64 2
  %75 = bitcast %"struct.std::__detail::_Node_iterator"* %74 to <2 x i64>*
  store <2 x i64> %wide.load340.epil, <2 x i64>* %75, align 8, !alias.scope !100, !noalias !97
  %index.next329.epil = add i64 %index328.epil, 4
  %epil.iter.sub = add i64 %epil.iter, -1
  %epil.iter.cmp.not = icmp eq i64 %epil.iter.sub, 0
  br i1 %epil.iter.cmp.not, label %middle.block300, label %vector.body302.epil, !llvm.loop !104

middle.block300:                                  ; preds = %vector.body302.epil, %middle.block300.unr-lcssa
  %cmp.n334 = icmp eq i64 %38, %n.vec327
  %cast.cmo = add nsw i64 %n.vec327, -1
  %ind.escape = getelementptr %"struct.std::__detail::_Node_iterator", %"struct.std::__detail::_Node_iterator"* %cond.i67.i, i64 %cast.cmo
  br i1 %cmp.n334, label %invoke.cont10.i, label %for.body.i.i.i.i78.i.preheader342

for.body.i.i.i.i78.i.preheader342:                ; preds = %vector.memcheck309, %for.body.i.i.i.i78.i.preheader, %middle.block300
  %__cur.024.i.i.i.i73.i.ph = phi %"struct.std::__detail::_Node_iterator"* [ %cond.i67.i, %vector.memcheck309 ], [ %cond.i67.i, %for.body.i.i.i.i78.i.preheader ], [ %ind.end331, %middle.block300 ]
  %__first.sroa.0.023.i.i.i.i74.i.ph = phi %"struct.std::__detail::_Node_iterator"* [ %30, %vector.memcheck309 ], [ %30, %for.body.i.i.i.i78.i.preheader ], [ %ind.end333, %middle.block300 ]
  br label %for.body.i.i.i.i78.i

invoke.cont10.i.thread:                           ; preds = %invoke.cont.i
  %incdec.ptr.i206277 = getelementptr inbounds %"struct.std::__detail::_Node_iterator", %"struct.std::__detail::_Node_iterator"* %cond.i67.i, i64 1
  br label %invoke.cont15.i

for.body.i.i.i.i78.i:                             ; preds = %for.body.i.i.i.i78.i.preheader342, %for.body.i.i.i.i78.i
  %__cur.024.i.i.i.i73.i = phi %"struct.std::__detail::_Node_iterator"* [ %incdec.ptr.i.i.i.i76.i, %for.body.i.i.i.i78.i ], [ %__cur.024.i.i.i.i73.i.ph, %for.body.i.i.i.i78.i.preheader342 ]
  %__first.sroa.0.023.i.i.i.i74.i = phi %"struct.std::__detail::_Node_iterator"* [ %incdec.ptr.i.i.i.i.i75.i, %for.body.i.i.i.i78.i ], [ %__first.sroa.0.023.i.i.i.i74.i.ph, %for.body.i.i.i.i78.i.preheader342 ]
  %76 = bitcast %"struct.std::__detail::_Node_iterator"* %__first.sroa.0.023.i.i.i.i74.i to i64*
  %77 = bitcast %"struct.std::__detail::_Node_iterator"* %__cur.024.i.i.i.i73.i to i64*
  %78 = load i64, i64* %76, align 8
  store i64 %78, i64* %77, align 8
  %incdec.ptr.i.i.i.i.i75.i = getelementptr inbounds %"struct.std::__detail::_Node_iterator", %"struct.std::__detail::_Node_iterator"* %__first.sroa.0.023.i.i.i.i74.i, i64 1
  %incdec.ptr.i.i.i.i76.i = getelementptr inbounds %"struct.std::__detail::_Node_iterator", %"struct.std::__detail::_Node_iterator"* %__cur.024.i.i.i.i73.i, i64 1
  %cmp.i.i.not.i.i.i.i77.i = icmp eq %"struct.std::__detail::_Node_iterator"* %incdec.ptr.i.i.i.i.i75.i, %22
  br i1 %cmp.i.i.not.i.i.i.i77.i, label %invoke.cont10.i, label %for.body.i.i.i.i78.i, !llvm.loop !105

invoke.cont10.i:                                  ; preds = %for.body.i.i.i.i78.i, %middle.block300
  %__cur.024.i.i.i.i73.i.lcssa = phi %"struct.std::__detail::_Node_iterator"* [ %ind.escape, %middle.block300 ], [ %__cur.024.i.i.i.i73.i, %for.body.i.i.i.i78.i ]
  %.pre275 = load %"struct.std::__detail::_Node_iterator"*, %"struct.std::__detail::_Node_iterator"** %_M_finish.i.i, align 8, !tbaa !96
  %incdec.ptr.i206 = getelementptr inbounds %"struct.std::__detail::_Node_iterator", %"struct.std::__detail::_Node_iterator"* %__cur.024.i.i.i.i73.i.lcssa, i64 2
  %cmp.i.i.not22.i.i.i.i.i = icmp eq %"struct.std::__detail::_Node_iterator"* %.pre275, %22
  br i1 %cmp.i.i.not22.i.i.i.i.i, label %invoke.cont15.i, label %for.body.i.i.i.i.i.preheader

for.body.i.i.i.i.i.preheader:                     ; preds = %invoke.cont10.i
  %scevgep = getelementptr %"struct.std::__detail::_Node_iterator", %"struct.std::__detail::_Node_iterator"* %.pre275, i64 -1
  %79 = ptrtoint %"struct.std::__detail::_Node_iterator"* %scevgep to i64
  %80 = sub i64 %79, %24
  %81 = lshr i64 %80, 3
  %82 = add nuw nsw i64 %81, 1
  %min.iters.check = icmp ult i64 %80, 24
  br i1 %min.iters.check, label %for.body.i.i.i.i.i.preheader341, label %vector.memcheck

vector.memcheck:                                  ; preds = %for.body.i.i.i.i.i.preheader
  %scevgep284 = getelementptr %"struct.std::__detail::_Node_iterator", %"struct.std::__detail::_Node_iterator"* %__cur.024.i.i.i.i73.i.lcssa, i64 3
  %scevgep285 = getelementptr %"struct.std::__detail::_Node_iterator", %"struct.std::__detail::_Node_iterator"* %.pre275, i64 -1
  %83 = ptrtoint %"struct.std::__detail::_Node_iterator"* %scevgep285 to i64
  %84 = sub i64 %83, %24
  %85 = lshr i64 %84, 3
  %scevgep289 = getelementptr %"struct.std::__detail::_Node_iterator", %"struct.std::__detail::_Node_iterator"* %scevgep284, i64 %85
  %scevgep291 = getelementptr %"struct.std::__detail::_Node_iterator", %"struct.std::__detail::_Node_iterator"* %22, i64 1
  %scevgep292 = getelementptr %"struct.std::__detail::_Node_iterator", %"struct.std::__detail::_Node_iterator"* %scevgep291, i64 %85
  %bound0 = icmp ult %"struct.std::__detail::_Node_iterator"* %incdec.ptr.i206, %scevgep292
  %bound1 = icmp ult %"struct.std::__detail::_Node_iterator"* %22, %scevgep289
  %found.conflict = and i1 %bound0, %bound1
  br i1 %found.conflict, label %for.body.i.i.i.i.i.preheader341, label %vector.ph

vector.ph:                                        ; preds = %vector.memcheck
  %n.vec = and i64 %82, 4611686018427387900
  %ind.end = getelementptr %"struct.std::__detail::_Node_iterator", %"struct.std::__detail::_Node_iterator"* %incdec.ptr.i206, i64 %n.vec
  %ind.end295 = getelementptr %"struct.std::__detail::_Node_iterator", %"struct.std::__detail::_Node_iterator"* %22, i64 %n.vec
  %86 = add nsw i64 %n.vec, -4
  %87 = lshr exact i64 %86, 2
  %88 = add nuw nsw i64 %87, 1
  %xtraiter345 = and i64 %88, 3
  %89 = icmp ult i64 %86, 12
  br i1 %89, label %middle.block.unr-lcssa, label %vector.ph.new

vector.ph.new:                                    ; preds = %vector.ph
  %unroll_iter348 = and i64 %88, 9223372036854775804
  br label %vector.body

vector.body:                                      ; preds = %vector.body, %vector.ph.new
  %index = phi i64 [ 0, %vector.ph.new ], [ %index.next.3, %vector.body ]
  %niter349 = phi i64 [ %unroll_iter348, %vector.ph.new ], [ %niter349.nsub.3, %vector.body ]
  %next.gep = getelementptr %"struct.std::__detail::_Node_iterator", %"struct.std::__detail::_Node_iterator"* %incdec.ptr.i206, i64 %index
  %next.gep297 = getelementptr %"struct.std::__detail::_Node_iterator", %"struct.std::__detail::_Node_iterator"* %22, i64 %index
  %90 = bitcast %"struct.std::__detail::_Node_iterator"* %next.gep297 to <2 x i64>*
  %wide.load = load <2 x i64>, <2 x i64>* %90, align 8, !alias.scope !106
  %91 = getelementptr %"struct.std::__detail::_Node_iterator", %"struct.std::__detail::_Node_iterator"* %next.gep297, i64 2
  %92 = bitcast %"struct.std::__detail::_Node_iterator"* %91 to <2 x i64>*
  %wide.load299 = load <2 x i64>, <2 x i64>* %92, align 8, !alias.scope !106
  %93 = bitcast %"struct.std::__detail::_Node_iterator"* %next.gep to <2 x i64>*
  store <2 x i64> %wide.load, <2 x i64>* %93, align 8, !alias.scope !109, !noalias !106
  %94 = getelementptr %"struct.std::__detail::_Node_iterator", %"struct.std::__detail::_Node_iterator"* %next.gep, i64 2
  %95 = bitcast %"struct.std::__detail::_Node_iterator"* %94 to <2 x i64>*
  store <2 x i64> %wide.load299, <2 x i64>* %95, align 8, !alias.scope !109, !noalias !106
  %index.next = or i64 %index, 4
  %next.gep.1 = getelementptr %"struct.std::__detail::_Node_iterator", %"struct.std::__detail::_Node_iterator"* %incdec.ptr.i206, i64 %index.next
  %next.gep297.1 = getelementptr %"struct.std::__detail::_Node_iterator", %"struct.std::__detail::_Node_iterator"* %22, i64 %index.next
  %96 = bitcast %"struct.std::__detail::_Node_iterator"* %next.gep297.1 to <2 x i64>*
  %wide.load.1 = load <2 x i64>, <2 x i64>* %96, align 8, !alias.scope !106
  %97 = getelementptr %"struct.std::__detail::_Node_iterator", %"struct.std::__detail::_Node_iterator"* %next.gep297.1, i64 2
  %98 = bitcast %"struct.std::__detail::_Node_iterator"* %97 to <2 x i64>*
  %wide.load299.1 = load <2 x i64>, <2 x i64>* %98, align 8, !alias.scope !106
  %99 = bitcast %"struct.std::__detail::_Node_iterator"* %next.gep.1 to <2 x i64>*
  store <2 x i64> %wide.load.1, <2 x i64>* %99, align 8, !alias.scope !109, !noalias !106
  %100 = getelementptr %"struct.std::__detail::_Node_iterator", %"struct.std::__detail::_Node_iterator"* %next.gep.1, i64 2
  %101 = bitcast %"struct.std::__detail::_Node_iterator"* %100 to <2 x i64>*
  store <2 x i64> %wide.load299.1, <2 x i64>* %101, align 8, !alias.scope !109, !noalias !106
  %index.next.1 = or i64 %index, 8
  %next.gep.2 = getelementptr %"struct.std::__detail::_Node_iterator", %"struct.std::__detail::_Node_iterator"* %incdec.ptr.i206, i64 %index.next.1
  %next.gep297.2 = getelementptr %"struct.std::__detail::_Node_iterator", %"struct.std::__detail::_Node_iterator"* %22, i64 %index.next.1
  %102 = bitcast %"struct.std::__detail::_Node_iterator"* %next.gep297.2 to <2 x i64>*
  %wide.load.2 = load <2 x i64>, <2 x i64>* %102, align 8, !alias.scope !106
  %103 = getelementptr %"struct.std::__detail::_Node_iterator", %"struct.std::__detail::_Node_iterator"* %next.gep297.2, i64 2
  %104 = bitcast %"struct.std::__detail::_Node_iterator"* %103 to <2 x i64>*
  %wide.load299.2 = load <2 x i64>, <2 x i64>* %104, align 8, !alias.scope !106
  %105 = bitcast %"struct.std::__detail::_Node_iterator"* %next.gep.2 to <2 x i64>*
  store <2 x i64> %wide.load.2, <2 x i64>* %105, align 8, !alias.scope !109, !noalias !106
  %106 = getelementptr %"struct.std::__detail::_Node_iterator", %"struct.std::__detail::_Node_iterator"* %next.gep.2, i64 2
  %107 = bitcast %"struct.std::__detail::_Node_iterator"* %106 to <2 x i64>*
  store <2 x i64> %wide.load299.2, <2 x i64>* %107, align 8, !alias.scope !109, !noalias !106
  %index.next.2 = or i64 %index, 12
  %next.gep.3 = getelementptr %"struct.std::__detail::_Node_iterator", %"struct.std::__detail::_Node_iterator"* %incdec.ptr.i206, i64 %index.next.2
  %next.gep297.3 = getelementptr %"struct.std::__detail::_Node_iterator", %"struct.std::__detail::_Node_iterator"* %22, i64 %index.next.2
  %108 = bitcast %"struct.std::__detail::_Node_iterator"* %next.gep297.3 to <2 x i64>*
  %wide.load.3 = load <2 x i64>, <2 x i64>* %108, align 8, !alias.scope !106
  %109 = getelementptr %"struct.std::__detail::_Node_iterator", %"struct.std::__detail::_Node_iterator"* %next.gep297.3, i64 2
  %110 = bitcast %"struct.std::__detail::_Node_iterator"* %109 to <2 x i64>*
  %wide.load299.3 = load <2 x i64>, <2 x i64>* %110, align 8, !alias.scope !106
  %111 = bitcast %"struct.std::__detail::_Node_iterator"* %next.gep.3 to <2 x i64>*
  store <2 x i64> %wide.load.3, <2 x i64>* %111, align 8, !alias.scope !109, !noalias !106
  %112 = getelementptr %"struct.std::__detail::_Node_iterator", %"struct.std::__detail::_Node_iterator"* %next.gep.3, i64 2
  %113 = bitcast %"struct.std::__detail::_Node_iterator"* %112 to <2 x i64>*
  store <2 x i64> %wide.load299.3, <2 x i64>* %113, align 8, !alias.scope !109, !noalias !106
  %index.next.3 = add i64 %index, 16
  %niter349.nsub.3 = add i64 %niter349, -4
  %niter349.ncmp.3 = icmp eq i64 %niter349.nsub.3, 0
  br i1 %niter349.ncmp.3, label %middle.block.unr-lcssa, label %vector.body, !llvm.loop !111

middle.block.unr-lcssa:                           ; preds = %vector.body, %vector.ph
  %index.unr = phi i64 [ 0, %vector.ph ], [ %index.next.3, %vector.body ]
  %lcmp.mod347.not = icmp eq i64 %xtraiter345, 0
  br i1 %lcmp.mod347.not, label %middle.block, label %vector.body.epil

vector.body.epil:                                 ; preds = %middle.block.unr-lcssa, %vector.body.epil
  %index.epil = phi i64 [ %index.next.epil, %vector.body.epil ], [ %index.unr, %middle.block.unr-lcssa ]
  %epil.iter346 = phi i64 [ %epil.iter346.sub, %vector.body.epil ], [ %xtraiter345, %middle.block.unr-lcssa ]
  %next.gep.epil = getelementptr %"struct.std::__detail::_Node_iterator", %"struct.std::__detail::_Node_iterator"* %incdec.ptr.i206, i64 %index.epil
  %next.gep297.epil = getelementptr %"struct.std::__detail::_Node_iterator", %"struct.std::__detail::_Node_iterator"* %22, i64 %index.epil
  %114 = bitcast %"struct.std::__detail::_Node_iterator"* %next.gep297.epil to <2 x i64>*
  %wide.load.epil = load <2 x i64>, <2 x i64>* %114, align 8, !alias.scope !106
  %115 = getelementptr %"struct.std::__detail::_Node_iterator", %"struct.std::__detail::_Node_iterator"* %next.gep297.epil, i64 2
  %116 = bitcast %"struct.std::__detail::_Node_iterator"* %115 to <2 x i64>*
  %wide.load299.epil = load <2 x i64>, <2 x i64>* %116, align 8, !alias.scope !106
  %117 = bitcast %"struct.std::__detail::_Node_iterator"* %next.gep.epil to <2 x i64>*
  store <2 x i64> %wide.load.epil, <2 x i64>* %117, align 8, !alias.scope !109, !noalias !106
  %118 = getelementptr %"struct.std::__detail::_Node_iterator", %"struct.std::__detail::_Node_iterator"* %next.gep.epil, i64 2
  %119 = bitcast %"struct.std::__detail::_Node_iterator"* %118 to <2 x i64>*
  store <2 x i64> %wide.load299.epil, <2 x i64>* %119, align 8, !alias.scope !109, !noalias !106
  %index.next.epil = add i64 %index.epil, 4
  %epil.iter346.sub = add i64 %epil.iter346, -1
  %epil.iter346.cmp.not = icmp eq i64 %epil.iter346.sub, 0
  br i1 %epil.iter346.cmp.not, label %middle.block, label %vector.body.epil, !llvm.loop !112

middle.block:                                     ; preds = %vector.body.epil, %middle.block.unr-lcssa
  %cmp.n = icmp eq i64 %82, %n.vec
  br i1 %cmp.n, label %invoke.cont15.i, label %for.body.i.i.i.i.i.preheader341

for.body.i.i.i.i.i.preheader341:                  ; preds = %vector.memcheck, %for.body.i.i.i.i.i.preheader, %middle.block
  %__cur.024.i.i.i.i.i.ph = phi %"struct.std::__detail::_Node_iterator"* [ %incdec.ptr.i206, %vector.memcheck ], [ %incdec.ptr.i206, %for.body.i.i.i.i.i.preheader ], [ %ind.end, %middle.block ]
  %__first.sroa.0.023.i.i.i.i.i.ph = phi %"struct.std::__detail::_Node_iterator"* [ %22, %vector.memcheck ], [ %22, %for.body.i.i.i.i.i.preheader ], [ %ind.end295, %middle.block ]
  br label %for.body.i.i.i.i.i

for.body.i.i.i.i.i:                               ; preds = %for.body.i.i.i.i.i.preheader341, %for.body.i.i.i.i.i
  %__cur.024.i.i.i.i.i = phi %"struct.std::__detail::_Node_iterator"* [ %incdec.ptr.i.i.i.i.i, %for.body.i.i.i.i.i ], [ %__cur.024.i.i.i.i.i.ph, %for.body.i.i.i.i.i.preheader341 ]
  %__first.sroa.0.023.i.i.i.i.i = phi %"struct.std::__detail::_Node_iterator"* [ %incdec.ptr.i.i.i.i.i.i, %for.body.i.i.i.i.i ], [ %__first.sroa.0.023.i.i.i.i.i.ph, %for.body.i.i.i.i.i.preheader341 ]
  %120 = bitcast %"struct.std::__detail::_Node_iterator"* %__first.sroa.0.023.i.i.i.i.i to i64*
  %121 = bitcast %"struct.std::__detail::_Node_iterator"* %__cur.024.i.i.i.i.i to i64*
  %122 = load i64, i64* %120, align 8
  store i64 %122, i64* %121, align 8
  %incdec.ptr.i.i.i.i.i.i = getelementptr inbounds %"struct.std::__detail::_Node_iterator", %"struct.std::__detail::_Node_iterator"* %__first.sroa.0.023.i.i.i.i.i, i64 1
  %incdec.ptr.i.i.i.i.i = getelementptr inbounds %"struct.std::__detail::_Node_iterator", %"struct.std::__detail::_Node_iterator"* %__cur.024.i.i.i.i.i, i64 1
  %cmp.i.i.not.i.i.i.i.i = icmp eq %"struct.std::__detail::_Node_iterator"* %incdec.ptr.i.i.i.i.i.i, %.pre275
  br i1 %cmp.i.i.not.i.i.i.i.i, label %invoke.cont15.i, label %for.body.i.i.i.i.i, !llvm.loop !113

invoke.cont15.i:                                  ; preds = %for.body.i.i.i.i.i, %middle.block, %invoke.cont10.i.thread, %invoke.cont10.i
  %__cur.0.lcssa.i.i.i.i.i = phi %"struct.std::__detail::_Node_iterator"* [ %incdec.ptr.i206, %invoke.cont10.i ], [ %incdec.ptr.i206277, %invoke.cont10.i.thread ], [ %ind.end, %middle.block ], [ %incdec.ptr.i.i.i.i.i, %for.body.i.i.i.i.i ]
  %123 = load %"struct.std::__detail::_Node_iterator"*, %"struct.std::__detail::_Node_iterator"** %_M_start.i27.i.i, align 8, !tbaa !86
  %tobool.not.i68.i = icmp eq %"struct.std::__detail::_Node_iterator"* %123, null
  br i1 %tobool.not.i68.i, label %.noexc, label %if.then.i69.i

if.then.i69.i:                                    ; preds = %invoke.cont15.i
  %124 = bitcast %"struct.std::__detail::_Node_iterator"* %123 to i8*
  tail call void @_ZdlPv(i8* nonnull %124) #20
  br label %.noexc

.noexc:                                           ; preds = %if.then.i69.i, %invoke.cont15.i
  store %"struct.std::__detail::_Node_iterator"* %cond.i67.i, %"struct.std::__detail::_Node_iterator"** %_M_start.i27.i.i, align 8, !tbaa !86
  store %"struct.std::__detail::_Node_iterator"* %__cur.0.lcssa.i.i.i.i.i, %"struct.std::__detail::_Node_iterator"** %_M_finish.i.i, align 8, !tbaa !96
  %add.ptr39.i = getelementptr inbounds %"struct.std::__detail::_Node_iterator", %"struct.std::__detail::_Node_iterator"* %cond.i67.i, i64 %cond.i.i
  store %"struct.std::__detail::_Node_iterator"* %add.ptr39.i, %"struct.std::__detail::_Node_iterator"** %_M_end_of_storage.i.i, align 8, !tbaa !114
  br label %invoke.cont87

invoke.cont87:                                    ; preds = %.noexc, %if.then.i.i
  %125 = phi %"struct.std::__detail::_Node_iterator"* [ %__cur.0.lcssa.i.i.i.i.i, %.noexc ], [ %incdec.ptr.i.i204, %if.then.i.i ]
  %incdec.ptr.i203 = getelementptr inbounds %class.Aggregator*, %class.Aggregator** %__begin171.sroa.0.0259, i64 1
  %cmp.i215.not = icmp eq %class.Aggregator** %incdec.ptr.i203, %20
  br i1 %cmp.i215.not, label %for.cond92.preheader, label %invoke.cont87.for.body80_crit_edge

invoke.cont87.for.body80_crit_edge:               ; preds = %invoke.cont87
  %.pre = load %"struct.std::__detail::_Node_iterator"*, %"struct.std::__detail::_Node_iterator"** %_M_end_of_storage.i.i, align 8, !tbaa !114
  br label %for.body80

lpad:                                             ; preds = %_ZNSt16allocator_traitsISaINSt8__detail14_Node_iteratorISt4pairIKmSt6vectorI13GroupByColumnSaIS5_EEELb0ELb0EEEEE8allocateERSA_m.exit.i.i
  %126 = landingpad { i8*, i32 }
          cleanup
  br label %ehcleanup118

for.cond.cleanup93:                               ; preds = %invoke.cont109, %for.cond92.preheader
  %127 = ptrtoint i8* %call5 to i64
  invoke void @omni_release(i64 %127)
          to label %invoke.cont117 unwind label %lpad116

for.body94:                                       ; preds = %for.body94.lr.ph, %invoke.cont109
  %i91.0257 = phi i32 [ 0, %for.body94.lr.ph ], [ %inc113, %invoke.cont109 ]
  %currentPosition.0256 = phi i32 [ 0, %for.body94.lr.ph ], [ %add110, %invoke.cont109 ]
  %128 = load i64, i64* %_M_element_count.i.i222, align 8, !tbaa !94
  %129 = trunc i64 %128 to i32
  %conv99 = sub i32 %129, %currentPosition.0256
  %cmp.i202 = icmp slt i32 %conv99, %div
  %.sroa.speculated = select i1 %cmp.i202, i32 %conv99, i32 %div
  %call105 = invoke noalias nonnull dereferenceable(64) i8* @_Znwm(i64 64) #18
          to label %invoke.cont104 unwind label %lpad103

invoke.cont104:                                   ; preds = %for.body94
  %130 = bitcast i8* %call105 to i32 (...)***
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [4 x i8*] }, { [4 x i8*] }* @_ZTV5Table, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %130, align 8, !tbaa !50
  %data.i = getelementptr inbounds i8, i8* %call105, i64 16
  tail call void @llvm.memset.p0i8.i64(i8* nonnull align 8 dereferenceable(24) %data.i, i8 0, i64 24, i1 false) #20
  %positionCount2.i = getelementptr inbounds i8, i8* %call105, i64 48
  %131 = bitcast i8* %positionCount2.i to i32*
  store i32 %.sroa.speculated, i32* %131, align 8, !tbaa !73
  %columnCount3.i = getelementptr inbounds i8, i8* %call105, i64 52
  %132 = bitcast i8* %columnCount3.i to i32*
  store i32 %add, i32* %132, align 4, !tbaa !115
  %call.i200 = invoke noalias nonnull i8* @_Znam(i64 %mul) #18
          to label %invoke.cont107 unwind label %lpad106

invoke.cont107:                                   ; preds = %invoke.cont104
  %133 = bitcast i8* %call105 to %class.Table*
  %types.i = getelementptr inbounds i8, i8* %call105, i64 40
  %134 = bitcast i8* %types.i to i8**
  store i8* %call.i200, i8** %134, align 8, !tbaa !61
  %columnSize.i = getelementptr inbounds i8, i8* %call105, i64 56
  %135 = bitcast i8* %columnSize.i to i32*
  store i32 0, i32* %135, align 8, !tbaa !83
  invoke void @_ZN11HashGroupBy15constructColumnEP5TablePijjiR8Iterator(%class.HashGroupBy* nonnull dereferenceable(144) %this, %class.Table* nonnull %133, i32* %6, i32 %conv, i32 %conv3, i32 %.sroa.speculated, %struct.Iterator* nonnull align 8 dereferenceable(32) %iterator)
          to label %invoke.cont108 unwind label %lpad103

invoke.cont108:                                   ; preds = %invoke.cont107
  %136 = load %class.Table**, %class.Table*** %_M_finish.i196, align 8, !tbaa !116
  %137 = load %class.Table**, %class.Table*** %_M_end_of_storage.i, align 8, !tbaa !119
  %cmp.not.i = icmp eq %class.Table** %136, %137
  br i1 %cmp.not.i, label %if.else.i, label %if.then.i

if.then.i:                                        ; preds = %invoke.cont108
  %138 = bitcast %class.Table** %136 to i8**
  store i8* %call105, i8** %138, align 8, !tbaa !19
  %139 = load %class.Table**, %class.Table*** %_M_finish.i196, align 8, !tbaa !116
  %incdec.ptr.i = getelementptr inbounds %class.Table*, %class.Table** %139, i64 1
  br label %invoke.cont109

if.else.i:                                        ; preds = %invoke.cont108
  %140 = load %class.Table**, %class.Table*** %_M_start.i27.i.i.i, align 8, !tbaa !120
  %sub.ptr.lhs.cast.i28.i.i.i = ptrtoint %class.Table** %136 to i64
  %sub.ptr.rhs.cast.i29.i.i.i = ptrtoint %class.Table** %140 to i64
  %sub.ptr.sub.i30.i.i.i = sub i64 %sub.ptr.lhs.cast.i28.i.i.i, %sub.ptr.rhs.cast.i29.i.i.i
  %sub.ptr.div.i31.i.i.i = ashr exact i64 %sub.ptr.sub.i30.i.i.i, 3
  %cmp.i.i.i.i = icmp eq i64 %sub.ptr.sub.i30.i.i.i, 0
  %.sroa.speculated.i.i.i = select i1 %cmp.i.i.i.i, i64 1, i64 %sub.ptr.div.i31.i.i.i
  %add.i.i.i = add nsw i64 %.sroa.speculated.i.i.i, %sub.ptr.div.i31.i.i.i
  %cmp7.i.i.i = icmp ult i64 %add.i.i.i, %sub.ptr.div.i31.i.i.i
  %cmp9.i.i.i = icmp ugt i64 %add.i.i.i, 2305843009213693951
  %or.cond.i.i.i = or i1 %cmp7.i.i.i, %cmp9.i.i.i
  %cond.i.i.i = select i1 %or.cond.i.i.i, i64 2305843009213693951, i64 %add.i.i.i
  %cmp.not.i.i.i = icmp eq i64 %cond.i.i.i, 0
  br i1 %cmp.not.i.i.i, label %invoke.cont.i.i, label %_ZNSt16allocator_traitsISaIP5TableEE8allocateERS2_m.exit.i.i.i

_ZNSt16allocator_traitsISaIP5TableEE8allocateERS2_m.exit.i.i.i: ; preds = %if.else.i
  %mul.i.i.i.i.i = shl nuw i64 %cond.i.i.i, 3
  %call2.i.i.i.i.i197 = invoke noalias nonnull i8* @_Znwm(i64 %mul.i.i.i.i.i) #19
          to label %call2.i.i.i.i.i.noexc unwind label %lpad103

call2.i.i.i.i.i.noexc:                            ; preds = %_ZNSt16allocator_traitsISaIP5TableEE8allocateERS2_m.exit.i.i.i
  %141 = bitcast i8* %call2.i.i.i.i.i197 to %class.Table**
  %.pre.i.i = load %class.Table**, %class.Table*** %_M_start.i27.i.i.i, align 8, !tbaa !120
  %.pre83.i.i = ptrtoint %class.Table** %.pre.i.i to i64
  %.pre84.i.i = sub i64 %sub.ptr.lhs.cast.i28.i.i.i, %.pre83.i.i
  br label %invoke.cont.i.i

invoke.cont.i.i:                                  ; preds = %call2.i.i.i.i.i.noexc, %if.else.i
  %sub.ptr.sub.i.i.i.i.i.i.i.i74.pre-phi.i.i = phi i64 [ %.pre84.i.i, %call2.i.i.i.i.i.noexc ], [ %sub.ptr.sub.i30.i.i.i, %if.else.i ]
  %142 = phi %class.Table** [ %.pre.i.i, %call2.i.i.i.i.i.noexc ], [ %140, %if.else.i ]
  %cond.i67.i.i = phi %class.Table** [ %141, %call2.i.i.i.i.i.noexc ], [ null, %if.else.i ]
  %add.ptr.i.i = getelementptr inbounds %class.Table*, %class.Table** %cond.i67.i.i, i64 %sub.ptr.div.i31.i.i.i
  %143 = bitcast %class.Table** %add.ptr.i.i to i8**
  store i8* %call105, i8** %143, align 8, !tbaa !19
  %tobool.not.i.i.i.i.i.i.i.i75.i.i = icmp eq i64 %sub.ptr.sub.i.i.i.i.i.i.i.i74.pre-phi.i.i, 0
  br i1 %tobool.not.i.i.i.i.i.i.i.i75.i.i, label %invoke.cont10.i.i, label %if.then.i.i.i.i.i.i.i.i76.i.i

if.then.i.i.i.i.i.i.i.i76.i.i:                    ; preds = %invoke.cont.i.i
  %144 = bitcast %class.Table** %cond.i67.i.i to i8*
  %145 = bitcast %class.Table** %142 to i8*
  tail call void @llvm.memmove.p0i8.p0i8.i64(i8* align 8 %144, i8* align 8 %145, i64 %sub.ptr.sub.i.i.i.i.i.i.i.i74.pre-phi.i.i, i1 false) #20
  br label %invoke.cont10.i.i

invoke.cont10.i.i:                                ; preds = %if.then.i.i.i.i.i.i.i.i76.i.i, %invoke.cont.i.i
  %sub.ptr.div.i.i.i.i.i.i.i.i77.i.i = ashr exact i64 %sub.ptr.sub.i.i.i.i.i.i.i.i74.pre-phi.i.i, 3
  %add.ptr.i.i.i.i.i.i.i.i78.i.i = getelementptr inbounds %class.Table*, %class.Table** %cond.i67.i.i, i64 1
  %incdec.ptr.i.i = getelementptr inbounds %class.Table*, %class.Table** %add.ptr.i.i.i.i.i.i.i.i78.i.i, i64 %sub.ptr.div.i.i.i.i.i.i.i.i77.i.i
  %146 = load %class.Table**, %class.Table*** %_M_finish.i196, align 8, !tbaa !116
  %sub.ptr.lhs.cast.i.i.i.i.i.i.i.i.i.i = ptrtoint %class.Table** %146 to i64
  %sub.ptr.sub.i.i.i.i.i.i.i.i.i.i = sub i64 %sub.ptr.lhs.cast.i.i.i.i.i.i.i.i.i.i, %sub.ptr.lhs.cast.i28.i.i.i
  %tobool.not.i.i.i.i.i.i.i.i.i.i = icmp eq i64 %sub.ptr.sub.i.i.i.i.i.i.i.i.i.i, 0
  br i1 %tobool.not.i.i.i.i.i.i.i.i.i.i, label %invoke.cont15.i.i, label %if.then.i.i.i.i.i.i.i.i.i.i

if.then.i.i.i.i.i.i.i.i.i.i:                      ; preds = %invoke.cont10.i.i
  %147 = bitcast %class.Table** %incdec.ptr.i.i to i8*
  %148 = bitcast %class.Table** %136 to i8*
  tail call void @llvm.memmove.p0i8.p0i8.i64(i8* nonnull align 8 %147, i8* align 8 %148, i64 %sub.ptr.sub.i.i.i.i.i.i.i.i.i.i, i1 false) #20
  br label %invoke.cont15.i.i

invoke.cont15.i.i:                                ; preds = %if.then.i.i.i.i.i.i.i.i.i.i, %invoke.cont10.i.i
  %tobool.not.i68.i.i = icmp eq %class.Table** %142, null
  br i1 %tobool.not.i68.i.i, label %_ZNSt6vectorIP5TableSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i, label %if.then.i69.i.i

if.then.i69.i.i:                                  ; preds = %invoke.cont15.i.i
  %149 = bitcast %class.Table** %142 to i8*
  tail call void @_ZdlPv(i8* nonnull %149) #20
  br label %_ZNSt6vectorIP5TableSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i

_ZNSt6vectorIP5TableSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i: ; preds = %if.then.i69.i.i, %invoke.cont15.i.i
  %sub.ptr.div.i.i.i.i.i.i.i.i.i.i = ashr exact i64 %sub.ptr.sub.i.i.i.i.i.i.i.i.i.i, 3
  %add.ptr.i.i.i.i.i.i.i.i.i.i = getelementptr inbounds %class.Table*, %class.Table** %incdec.ptr.i.i, i64 %sub.ptr.div.i.i.i.i.i.i.i.i.i.i
  store %class.Table** %cond.i67.i.i, %class.Table*** %_M_start.i27.i.i.i, align 8, !tbaa !120
  store %class.Table** %add.ptr.i.i.i.i.i.i.i.i.i.i, %class.Table*** %_M_finish.i196, align 8, !tbaa !116
  %add.ptr39.i.i = getelementptr inbounds %class.Table*, %class.Table** %cond.i67.i.i, i64 %cond.i.i.i
  br label %invoke.cont109

invoke.cont109:                                   ; preds = %_ZNSt6vectorIP5TableSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i, %if.then.i
  %_M_end_of_storage.i.sink = phi %class.Table*** [ %_M_end_of_storage.i, %_ZNSt6vectorIP5TableSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i ], [ %_M_finish.i196, %if.then.i ]
  %add.ptr39.i.i.sink = phi %class.Table** [ %add.ptr39.i.i, %_ZNSt6vectorIP5TableSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i ], [ %incdec.ptr.i, %if.then.i ]
  store %class.Table** %add.ptr39.i.i.sink, %class.Table*** %_M_end_of_storage.i.sink, align 8, !tbaa !19
  %add110 = add nsw i32 %currentPosition.0256, %div
  %inc113 = add nuw nsw i32 %i91.0257, 1
  %exitcond.not = icmp eq i32 %inc113, %conv65
  br i1 %exitcond.not, label %for.cond.cleanup93, label %for.body94, !llvm.loop !121

lpad103:                                          ; preds = %_ZNSt16allocator_traitsISaIP5TableEE8allocateERS2_m.exit.i.i.i, %invoke.cont107, %for.body94
  %150 = landingpad { i8*, i32 }
          cleanup
  br label %ehcleanup118

lpad106:                                          ; preds = %invoke.cont104
  %151 = landingpad { i8*, i32 }
          cleanup
  tail call void @_ZdlPv(i8* nonnull %call105) #23
  br label %ehcleanup118

invoke.cont117:                                   ; preds = %for.cond.cleanup93
  %_M_start.i.i.i191 = getelementptr inbounds %struct.Iterator, %struct.Iterator* %iterator, i64 0, i32 1, i32 0, i32 0, i32 0
  %152 = load %"struct.std::__detail::_Node_iterator"*, %"struct.std::__detail::_Node_iterator"** %_M_start.i.i.i191, align 8, !tbaa !86
  %tobool.not.i.i.i.i192 = icmp eq %"struct.std::__detail::_Node_iterator"* %152, null
  br i1 %tobool.not.i.i.i.i192, label %_ZN8IteratorD2Ev.exit194, label %if.then.i.i.i.i193

if.then.i.i.i.i193:                               ; preds = %invoke.cont117
  %153 = bitcast %"struct.std::__detail::_Node_iterator"* %152 to i8*
  tail call void @_ZdlPv(i8* nonnull %153) #20
  br label %_ZN8IteratorD2Ev.exit194

_ZN8IteratorD2Ev.exit194:                         ; preds = %invoke.cont117, %if.then.i.i.i.i193
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %14) #20
  ret i32 %conv65

lpad116:                                          ; preds = %for.cond.cleanup93
  %154 = landingpad { i8*, i32 }
          cleanup
  br label %ehcleanup118

ehcleanup118:                                     ; preds = %lpad103, %lpad106, %lpad116, %lpad
  %.pn182 = phi { i8*, i32 } [ %126, %lpad ], [ %154, %lpad116 ], [ %150, %lpad103 ], [ %151, %lpad106 ]
  %_M_start.i.i.i = getelementptr inbounds %struct.Iterator, %struct.Iterator* %iterator, i64 0, i32 1, i32 0, i32 0, i32 0
  %155 = load %"struct.std::__detail::_Node_iterator"*, %"struct.std::__detail::_Node_iterator"** %_M_start.i.i.i, align 8, !tbaa !86
  %tobool.not.i.i.i.i = icmp eq %"struct.std::__detail::_Node_iterator"* %155, null
  br i1 %tobool.not.i.i.i.i, label %_ZN8IteratorD2Ev.exit, label %if.then.i.i.i.i

if.then.i.i.i.i:                                  ; preds = %ehcleanup118
  %156 = bitcast %"struct.std::__detail::_Node_iterator"* %155 to i8*
  tail call void @_ZdlPv(i8* nonnull %156) #20
  br label %_ZN8IteratorD2Ev.exit

_ZN8IteratorD2Ev.exit:                            ; preds = %ehcleanup118, %if.then.i.i.i.i
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %14) #20
  resume { i8*, i32 } %.pn182
}

; Function Attrs: nofree nosync nounwind readnone speculatable willreturn
declare double @llvm.ceil.f64(double) #4

declare dso_local void @omni_release(i64) local_unnamed_addr #0

; Function Attrs: uwtable
define dso_local void @_ZN11HashGroupBy15constructColumnEP5Tablejij(%class.HashGroupBy* nocapture nonnull readonly dereferenceable(144) %this, %class.Table* %table, i32 %type, i32 %columnIdx, i32 %outputColType) local_unnamed_addr #6 align 2 personality i8* bitcast (i32 (...)* @__gxx_personality_v0 to i8*) {
entry:
  %ref.tmp5.i.i = alloca %class.anon, align 8
  %rows = alloca %"class.std::unordered_map", align 8
  switch i32 %outputColType, label %if.end255 [
    i32 0, label %if.then
    i32 1, label %if.then93
  ]

if.then:                                          ; preds = %entry
  %_M_element_count.i.i = getelementptr inbounds %class.HashGroupBy, %class.HashGroupBy* %this, i64 0, i32 4, i32 0, i32 3
  %0 = load i64, i64* %_M_element_count.i.i, align 8, !tbaa !94
  switch i32 %type, label %if.end255 [
    i32 1, label %sw.bb
    i32 2, label %sw.bb18
    i32 3, label %sw.bb55
  ]

sw.bb:                                            ; preds = %if.then
  %conv2 = and i64 %0, 4294967295
  %mul = shl nuw nsw i64 %conv2, 2
  %call3 = tail call i8* @omni_allocate(i64 %mul)
  %1 = bitcast i8* %call3 to i32*
  %_M_nxt.i.i.i = getelementptr inbounds %class.HashGroupBy, %class.HashGroupBy* %this, i64 0, i32 4, i32 0, i32 2, i32 0
  %2 = bitcast %"struct.std::__detail::_Hash_node_base"** %_M_nxt.i.i.i to %"struct.std::__detail::_Hash_node"**
  %__begin3.sroa.0.0696 = load %"struct.std::__detail::_Hash_node"*, %"struct.std::__detail::_Hash_node"** %2, align 8, !tbaa !22
  %cmp.i.not697 = icmp eq %"struct.std::__detail::_Hash_node"* %__begin3.sroa.0.0696, null
  br i1 %cmp.i.not697, label %for.cond.cleanup, label %for.body.lr.ph

for.body.lr.ph:                                   ; preds = %sw.bb
  %conv12 = sext i32 %columnIdx to i64
  br label %for.body

for.cond.cleanup:                                 ; preds = %for.body, %sw.bb
  %call16 = tail call noalias nonnull dereferenceable(40) i8* @_Znwm(i64 40) #18
  %3 = bitcast i8* %call16 to i32 (...)***
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [4 x i8*] }, { [4 x i8*] }* @_ZTV6Column, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %3, align 8, !tbaa !50
  %data.i = getelementptr inbounds i8, i8* %call16, i64 8
  %4 = bitcast i8* %data.i to i8**
  store i8* %call3, i8** %4, align 8, !tbaa !34
  %type.i = getelementptr inbounds i8, i8* %call16, i64 24
  %5 = bitcast i8* %type.i to i32*
  store i32 1, i32* %5, align 8, !tbaa !32
  %size.i = getelementptr inbounds i8, i8* %call16, i64 32
  %6 = bitcast i8* %size.i to i64*
  store i64 %conv2, i64* %6, align 8, !tbaa !82
  %types.i = getelementptr inbounds %class.Table, %class.Table* %table, i64 0, i32 4
  %7 = load i32*, i32** %types.i, align 8, !tbaa !61
  %columnSize.i = getelementptr inbounds %class.Table, %class.Table* %table, i64 0, i32 7
  %8 = load i32, i32* %columnSize.i, align 8, !tbaa !83
  %idxprom.i = zext i32 %8 to i64
  %arrayidx.i = getelementptr inbounds i32, i32* %7, i64 %idxprom.i
  store i32 1, i32* %arrayidx.i, align 4, !tbaa !27
  %inc.i = add i32 %8, 1
  store i32 %inc.i, i32* %columnSize.i, align 8, !tbaa !83
  %_M_finish.i.i = getelementptr inbounds %class.Table, %class.Table* %table, i64 0, i32 3, i32 0, i32 0, i32 1
  %9 = load %class.Column**, %class.Column*** %_M_finish.i.i, align 8, !tbaa !57
  %_M_end_of_storage.i.i = getelementptr inbounds %class.Table, %class.Table* %table, i64 0, i32 3, i32 0, i32 0, i32 2
  %10 = load %class.Column**, %class.Column*** %_M_end_of_storage.i.i, align 8, !tbaa !84
  %cmp.not.i.i = icmp eq %class.Column** %9, %10
  br i1 %cmp.not.i.i, label %if.else.i.i, label %if.then.i.i

if.then.i.i:                                      ; preds = %for.cond.cleanup
  %11 = bitcast %class.Column** %9 to i8**
  store i8* %call16, i8** %11, align 8, !tbaa !19
  %12 = load %class.Column**, %class.Column*** %_M_finish.i.i, align 8, !tbaa !57
  %incdec.ptr.i.i = getelementptr inbounds %class.Column*, %class.Column** %12, i64 1
  store %class.Column** %incdec.ptr.i.i, %class.Column*** %_M_finish.i.i, align 8, !tbaa !57
  br label %if.end255

if.else.i.i:                                      ; preds = %for.cond.cleanup
  %_M_start.i27.i.i.i.i = getelementptr inbounds %class.Table, %class.Table* %table, i64 0, i32 3, i32 0, i32 0, i32 0
  %13 = load %class.Column**, %class.Column*** %_M_start.i27.i.i.i.i, align 8, !tbaa !29
  %sub.ptr.lhs.cast.i28.i.i.i.i = ptrtoint %class.Column** %9 to i64
  %sub.ptr.rhs.cast.i29.i.i.i.i = ptrtoint %class.Column** %13 to i64
  %sub.ptr.sub.i30.i.i.i.i = sub i64 %sub.ptr.lhs.cast.i28.i.i.i.i, %sub.ptr.rhs.cast.i29.i.i.i.i
  %sub.ptr.div.i31.i.i.i.i = ashr exact i64 %sub.ptr.sub.i30.i.i.i.i, 3
  %cmp.i.i.i.i.i346 = icmp eq i64 %sub.ptr.sub.i30.i.i.i.i, 0
  %.sroa.speculated.i.i.i.i = select i1 %cmp.i.i.i.i.i346, i64 1, i64 %sub.ptr.div.i31.i.i.i.i
  %add.i.i.i.i = add nsw i64 %.sroa.speculated.i.i.i.i, %sub.ptr.div.i31.i.i.i.i
  %cmp7.i.i.i.i = icmp ult i64 %add.i.i.i.i, %sub.ptr.div.i31.i.i.i.i
  %cmp9.i.i.i.i = icmp ugt i64 %add.i.i.i.i, 2305843009213693951
  %or.cond.i.i.i.i = or i1 %cmp7.i.i.i.i, %cmp9.i.i.i.i
  %cond.i.i.i.i = select i1 %or.cond.i.i.i.i, i64 2305843009213693951, i64 %add.i.i.i.i
  %cmp.not.i.i.i.i = icmp eq i64 %cond.i.i.i.i, 0
  br i1 %cmp.not.i.i.i.i, label %invoke.cont.i.i.i, label %_ZNSt16allocator_traitsISaIP6ColumnEE8allocateERS2_m.exit.i.i.i.i

_ZNSt16allocator_traitsISaIP6ColumnEE8allocateERS2_m.exit.i.i.i.i: ; preds = %if.else.i.i
  %mul.i.i.i.i.i.i = shl nuw i64 %cond.i.i.i.i, 3
  %call2.i.i.i.i.i.i = tail call noalias nonnull i8* @_Znwm(i64 %mul.i.i.i.i.i.i) #19
  %14 = bitcast i8* %call2.i.i.i.i.i.i to %class.Column**
  %.pre.i.i.i = load %class.Column**, %class.Column*** %_M_start.i27.i.i.i.i, align 8, !tbaa !29
  %.pre83.i.i.i = ptrtoint %class.Column** %.pre.i.i.i to i64
  %.pre84.i.i.i = sub i64 %sub.ptr.lhs.cast.i28.i.i.i.i, %.pre83.i.i.i
  br label %invoke.cont.i.i.i

invoke.cont.i.i.i:                                ; preds = %_ZNSt16allocator_traitsISaIP6ColumnEE8allocateERS2_m.exit.i.i.i.i, %if.else.i.i
  %sub.ptr.sub.i.i.i.i.i.i.i.i74.pre-phi.i.i.i = phi i64 [ %.pre84.i.i.i, %_ZNSt16allocator_traitsISaIP6ColumnEE8allocateERS2_m.exit.i.i.i.i ], [ %sub.ptr.sub.i30.i.i.i.i, %if.else.i.i ]
  %15 = phi %class.Column** [ %.pre.i.i.i, %_ZNSt16allocator_traitsISaIP6ColumnEE8allocateERS2_m.exit.i.i.i.i ], [ %13, %if.else.i.i ]
  %cond.i67.i.i.i = phi %class.Column** [ %14, %_ZNSt16allocator_traitsISaIP6ColumnEE8allocateERS2_m.exit.i.i.i.i ], [ null, %if.else.i.i ]
  %add.ptr.i.i.i = getelementptr inbounds %class.Column*, %class.Column** %cond.i67.i.i.i, i64 %sub.ptr.div.i31.i.i.i.i
  %16 = bitcast %class.Column** %add.ptr.i.i.i to i8**
  store i8* %call16, i8** %16, align 8, !tbaa !19
  %tobool.not.i.i.i.i.i.i.i.i75.i.i.i = icmp eq i64 %sub.ptr.sub.i.i.i.i.i.i.i.i74.pre-phi.i.i.i, 0
  br i1 %tobool.not.i.i.i.i.i.i.i.i75.i.i.i, label %invoke.cont10.i.i.i, label %if.then.i.i.i.i.i.i.i.i76.i.i.i

if.then.i.i.i.i.i.i.i.i76.i.i.i:                  ; preds = %invoke.cont.i.i.i
  %17 = bitcast %class.Column** %cond.i67.i.i.i to i8*
  %18 = bitcast %class.Column** %15 to i8*
  tail call void @llvm.memmove.p0i8.p0i8.i64(i8* align 8 %17, i8* align 8 %18, i64 %sub.ptr.sub.i.i.i.i.i.i.i.i74.pre-phi.i.i.i, i1 false) #20
  br label %invoke.cont10.i.i.i

invoke.cont10.i.i.i:                              ; preds = %if.then.i.i.i.i.i.i.i.i76.i.i.i, %invoke.cont.i.i.i
  %sub.ptr.div.i.i.i.i.i.i.i.i77.i.i.i = ashr exact i64 %sub.ptr.sub.i.i.i.i.i.i.i.i74.pre-phi.i.i.i, 3
  %add.ptr.i.i.i.i.i.i.i.i78.i.i.i = getelementptr inbounds %class.Column*, %class.Column** %cond.i67.i.i.i, i64 %sub.ptr.div.i.i.i.i.i.i.i.i77.i.i.i
  %incdec.ptr.i.i.i = getelementptr inbounds %class.Column*, %class.Column** %add.ptr.i.i.i.i.i.i.i.i78.i.i.i, i64 1
  %19 = load %class.Column**, %class.Column*** %_M_finish.i.i, align 8, !tbaa !57
  %sub.ptr.lhs.cast.i.i.i.i.i.i.i.i.i.i.i = ptrtoint %class.Column** %19 to i64
  %sub.ptr.sub.i.i.i.i.i.i.i.i.i.i.i = sub i64 %sub.ptr.lhs.cast.i.i.i.i.i.i.i.i.i.i.i, %sub.ptr.lhs.cast.i28.i.i.i.i
  %tobool.not.i.i.i.i.i.i.i.i.i.i.i347 = icmp eq i64 %sub.ptr.sub.i.i.i.i.i.i.i.i.i.i.i, 0
  br i1 %tobool.not.i.i.i.i.i.i.i.i.i.i.i347, label %invoke.cont15.i.i.i, label %if.then.i.i.i.i.i.i.i.i.i.i.i348

if.then.i.i.i.i.i.i.i.i.i.i.i348:                 ; preds = %invoke.cont10.i.i.i
  %20 = bitcast %class.Column** %incdec.ptr.i.i.i to i8*
  %21 = bitcast %class.Column** %9 to i8*
  tail call void @llvm.memmove.p0i8.p0i8.i64(i8* nonnull align 8 %20, i8* align 8 %21, i64 %sub.ptr.sub.i.i.i.i.i.i.i.i.i.i.i, i1 false) #20
  br label %invoke.cont15.i.i.i

invoke.cont15.i.i.i:                              ; preds = %if.then.i.i.i.i.i.i.i.i.i.i.i348, %invoke.cont10.i.i.i
  %tobool.not.i68.i.i.i = icmp eq %class.Column** %15, null
  br i1 %tobool.not.i68.i.i.i, label %_ZNSt6vectorIP6ColumnSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i.i, label %if.then.i69.i.i.i

if.then.i69.i.i.i:                                ; preds = %invoke.cont15.i.i.i
  %22 = bitcast %class.Column** %15 to i8*
  tail call void @_ZdlPv(i8* nonnull %22) #20
  br label %_ZNSt6vectorIP6ColumnSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i.i

_ZNSt6vectorIP6ColumnSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i.i: ; preds = %if.then.i69.i.i.i, %invoke.cont15.i.i.i
  %sub.ptr.div.i.i.i.i.i.i.i.i.i.i.i = ashr exact i64 %sub.ptr.sub.i.i.i.i.i.i.i.i.i.i.i, 3
  %add.ptr.i.i.i.i.i.i.i.i.i.i.i = getelementptr inbounds %class.Column*, %class.Column** %incdec.ptr.i.i.i, i64 %sub.ptr.div.i.i.i.i.i.i.i.i.i.i.i
  store %class.Column** %cond.i67.i.i.i, %class.Column*** %_M_start.i27.i.i.i.i, align 8, !tbaa !29
  store %class.Column** %add.ptr.i.i.i.i.i.i.i.i.i.i.i, %class.Column*** %_M_finish.i.i, align 8, !tbaa !57
  %add.ptr39.i.i.i = getelementptr inbounds %class.Column*, %class.Column** %cond.i67.i.i.i, i64 %cond.i.i.i.i
  store %class.Column** %add.ptr39.i.i.i, %class.Column*** %_M_end_of_storage.i.i, align 8, !tbaa !84
  br label %if.end255

for.body:                                         ; preds = %for.body.lr.ph, %for.body
  %indvars.iv = phi i64 [ 0, %for.body.lr.ph ], [ %indvars.iv.next, %for.body ]
  %__begin3.sroa.0.0699 = phi %"struct.std::__detail::_Hash_node"* [ %__begin3.sroa.0.0696, %for.body.lr.ph ], [ %__begin3.sroa.0.0, %for.body ]
  %second = getelementptr inbounds %"struct.std::__detail::_Hash_node", %"struct.std::__detail::_Hash_node"* %__begin3.sroa.0.0699, i64 0, i32 0, i32 1, i32 0, i32 0, i64 8
  %_M_start.i = bitcast i8* %second to %struct.GroupByColumn**
  %23 = load %struct.GroupByColumn*, %struct.GroupByColumn** %_M_start.i, align 8, !tbaa !42
  %val = getelementptr inbounds %struct.GroupByColumn, %struct.GroupByColumn* %23, i64 %conv12, i32 1
  %24 = bitcast i8** %val to i32**
  %25 = load i32*, i32** %24, align 8, !tbaa !78
  %26 = load i32, i32* %25, align 4, !tbaa !25
  %indvars.iv.next = add nuw i64 %indvars.iv, 1
  %arrayidx = getelementptr inbounds i32, i32* %1, i64 %indvars.iv
  store i32 %26, i32* %arrayidx, align 4, !tbaa !25
  %27 = bitcast %"struct.std::__detail::_Hash_node"* %__begin3.sroa.0.0699 to %"struct.std::__detail::_Hash_node"**
  %__begin3.sroa.0.0 = load %"struct.std::__detail::_Hash_node"*, %"struct.std::__detail::_Hash_node"** %27, align 8, !tbaa !22
  %cmp.i.not = icmp eq %"struct.std::__detail::_Hash_node"* %__begin3.sroa.0.0, null
  br i1 %cmp.i.not, label %for.cond.cleanup, label %for.body

sw.bb18:                                          ; preds = %if.then
  %conv20 = and i64 %0, 4294967295
  %mul21 = shl nuw nsw i64 %conv20, 3
  %call22 = tail call i8* @omni_allocate(i64 %mul21)
  %28 = bitcast i8* %call22 to i64*
  %_M_nxt.i.i.i402 = getelementptr inbounds %class.HashGroupBy, %class.HashGroupBy* %this, i64 0, i32 4, i32 0, i32 2, i32 0
  %29 = bitcast %"struct.std::__detail::_Hash_node_base"** %_M_nxt.i.i.i402 to %"struct.std::__detail::_Hash_node"**
  %__begin326.sroa.0.0700 = load %"struct.std::__detail::_Hash_node"*, %"struct.std::__detail::_Hash_node"** %29, align 8, !tbaa !22
  %cmp.i468.not701 = icmp eq %"struct.std::__detail::_Hash_node"* %__begin326.sroa.0.0700, null
  br i1 %cmp.i468.not701, label %for.cond.cleanup36, label %for.body37.lr.ph

for.body37.lr.ph:                                 ; preds = %sw.bb18
  %conv41 = sext i32 %columnIdx to i64
  br label %for.body37

for.cond.cleanup36:                               ; preds = %for.body37, %sw.bb18
  %call51 = tail call noalias nonnull dereferenceable(40) i8* @_Znwm(i64 40) #18
  %30 = bitcast i8* %call51 to i32 (...)***
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [4 x i8*] }, { [4 x i8*] }* @_ZTV6Column, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %30, align 8, !tbaa !50
  %data.i532 = getelementptr inbounds i8, i8* %call51, i64 8
  %31 = bitcast i8* %data.i532 to i8**
  store i8* %call22, i8** %31, align 8, !tbaa !34
  %type.i533 = getelementptr inbounds i8, i8* %call51, i64 24
  %32 = bitcast i8* %type.i533 to i32*
  store i32 2, i32* %32, align 8, !tbaa !32
  %size.i534 = getelementptr inbounds i8, i8* %call51, i64 32
  %33 = bitcast i8* %size.i534 to i64*
  store i64 %conv20, i64* %33, align 8, !tbaa !82
  %types.i535 = getelementptr inbounds %class.Table, %class.Table* %table, i64 0, i32 4
  %34 = load i32*, i32** %types.i535, align 8, !tbaa !61
  %columnSize.i536 = getelementptr inbounds %class.Table, %class.Table* %table, i64 0, i32 7
  %35 = load i32, i32* %columnSize.i536, align 8, !tbaa !83
  %idxprom.i537 = zext i32 %35 to i64
  %arrayidx.i538 = getelementptr inbounds i32, i32* %34, i64 %idxprom.i537
  store i32 2, i32* %arrayidx.i538, align 4, !tbaa !27
  %inc.i539 = add i32 %35, 1
  store i32 %inc.i539, i32* %columnSize.i536, align 8, !tbaa !83
  %_M_finish.i.i540 = getelementptr inbounds %class.Table, %class.Table* %table, i64 0, i32 3, i32 0, i32 0, i32 1
  %36 = load %class.Column**, %class.Column*** %_M_finish.i.i540, align 8, !tbaa !57
  %_M_end_of_storage.i.i541 = getelementptr inbounds %class.Table, %class.Table* %table, i64 0, i32 3, i32 0, i32 0, i32 2
  %37 = load %class.Column**, %class.Column*** %_M_end_of_storage.i.i541, align 8, !tbaa !84
  %cmp.not.i.i542 = icmp eq %class.Column** %36, %37
  br i1 %cmp.not.i.i542, label %if.else.i.i558, label %if.then.i.i544

if.then.i.i544:                                   ; preds = %for.cond.cleanup36
  %38 = bitcast %class.Column** %36 to i8**
  store i8* %call51, i8** %38, align 8, !tbaa !19
  %39 = load %class.Column**, %class.Column*** %_M_finish.i.i540, align 8, !tbaa !57
  %incdec.ptr.i.i543 = getelementptr inbounds %class.Column*, %class.Column** %39, i64 1
  store %class.Column** %incdec.ptr.i.i543, %class.Column*** %_M_finish.i.i540, align 8, !tbaa !57
  br label %if.end255

if.else.i.i558:                                   ; preds = %for.cond.cleanup36
  %_M_start.i27.i.i.i.i545 = getelementptr inbounds %class.Table, %class.Table* %table, i64 0, i32 3, i32 0, i32 0, i32 0
  %40 = load %class.Column**, %class.Column*** %_M_start.i27.i.i.i.i545, align 8, !tbaa !29
  %sub.ptr.lhs.cast.i28.i.i.i.i546 = ptrtoint %class.Column** %36 to i64
  %sub.ptr.rhs.cast.i29.i.i.i.i547 = ptrtoint %class.Column** %40 to i64
  %sub.ptr.sub.i30.i.i.i.i548 = sub i64 %sub.ptr.lhs.cast.i28.i.i.i.i546, %sub.ptr.rhs.cast.i29.i.i.i.i547
  %sub.ptr.div.i31.i.i.i.i549 = ashr exact i64 %sub.ptr.sub.i30.i.i.i.i548, 3
  %cmp.i.i.i.i.i550 = icmp eq i64 %sub.ptr.sub.i30.i.i.i.i548, 0
  %.sroa.speculated.i.i.i.i551 = select i1 %cmp.i.i.i.i.i550, i64 1, i64 %sub.ptr.div.i31.i.i.i.i549
  %add.i.i.i.i552 = add nsw i64 %.sroa.speculated.i.i.i.i551, %sub.ptr.div.i31.i.i.i.i549
  %cmp7.i.i.i.i553 = icmp ult i64 %add.i.i.i.i552, %sub.ptr.div.i31.i.i.i.i549
  %cmp9.i.i.i.i554 = icmp ugt i64 %add.i.i.i.i552, 2305843009213693951
  %or.cond.i.i.i.i555 = or i1 %cmp7.i.i.i.i553, %cmp9.i.i.i.i554
  %cond.i.i.i.i556 = select i1 %or.cond.i.i.i.i555, i64 2305843009213693951, i64 %add.i.i.i.i552
  %cmp.not.i.i.i.i557 = icmp eq i64 %cond.i.i.i.i556, 0
  br i1 %cmp.not.i.i.i.i557, label %invoke.cont.i.i.i569, label %_ZNSt16allocator_traitsISaIP6ColumnEE8allocateERS2_m.exit.i.i.i.i564

_ZNSt16allocator_traitsISaIP6ColumnEE8allocateERS2_m.exit.i.i.i.i564: ; preds = %if.else.i.i558
  %mul.i.i.i.i.i.i559 = shl nuw i64 %cond.i.i.i.i556, 3
  %call2.i.i.i.i.i.i560 = tail call noalias nonnull i8* @_Znwm(i64 %mul.i.i.i.i.i.i559) #19
  %41 = bitcast i8* %call2.i.i.i.i.i.i560 to %class.Column**
  %.pre.i.i.i561 = load %class.Column**, %class.Column*** %_M_start.i27.i.i.i.i545, align 8, !tbaa !29
  %.pre83.i.i.i562 = ptrtoint %class.Column** %.pre.i.i.i561 to i64
  %.pre84.i.i.i563 = sub i64 %sub.ptr.lhs.cast.i28.i.i.i.i546, %.pre83.i.i.i562
  br label %invoke.cont.i.i.i569

invoke.cont.i.i.i569:                             ; preds = %_ZNSt16allocator_traitsISaIP6ColumnEE8allocateERS2_m.exit.i.i.i.i564, %if.else.i.i558
  %sub.ptr.sub.i.i.i.i.i.i.i.i74.pre-phi.i.i.i565 = phi i64 [ %.pre84.i.i.i563, %_ZNSt16allocator_traitsISaIP6ColumnEE8allocateERS2_m.exit.i.i.i.i564 ], [ %sub.ptr.sub.i30.i.i.i.i548, %if.else.i.i558 ]
  %42 = phi %class.Column** [ %.pre.i.i.i561, %_ZNSt16allocator_traitsISaIP6ColumnEE8allocateERS2_m.exit.i.i.i.i564 ], [ %40, %if.else.i.i558 ]
  %cond.i67.i.i.i566 = phi %class.Column** [ %41, %_ZNSt16allocator_traitsISaIP6ColumnEE8allocateERS2_m.exit.i.i.i.i564 ], [ null, %if.else.i.i558 ]
  %add.ptr.i.i.i567 = getelementptr inbounds %class.Column*, %class.Column** %cond.i67.i.i.i566, i64 %sub.ptr.div.i31.i.i.i.i549
  %43 = bitcast %class.Column** %add.ptr.i.i.i567 to i8**
  store i8* %call51, i8** %43, align 8, !tbaa !19
  %tobool.not.i.i.i.i.i.i.i.i75.i.i.i568 = icmp eq i64 %sub.ptr.sub.i.i.i.i.i.i.i.i74.pre-phi.i.i.i565, 0
  br i1 %tobool.not.i.i.i.i.i.i.i.i75.i.i.i568, label %invoke.cont10.i.i.i577, label %if.then.i.i.i.i.i.i.i.i76.i.i.i570

if.then.i.i.i.i.i.i.i.i76.i.i.i570:               ; preds = %invoke.cont.i.i.i569
  %44 = bitcast %class.Column** %cond.i67.i.i.i566 to i8*
  %45 = bitcast %class.Column** %42 to i8*
  tail call void @llvm.memmove.p0i8.p0i8.i64(i8* align 8 %44, i8* align 8 %45, i64 %sub.ptr.sub.i.i.i.i.i.i.i.i74.pre-phi.i.i.i565, i1 false) #20
  br label %invoke.cont10.i.i.i577

invoke.cont10.i.i.i577:                           ; preds = %if.then.i.i.i.i.i.i.i.i76.i.i.i570, %invoke.cont.i.i.i569
  %sub.ptr.div.i.i.i.i.i.i.i.i77.i.i.i571 = ashr exact i64 %sub.ptr.sub.i.i.i.i.i.i.i.i74.pre-phi.i.i.i565, 3
  %add.ptr.i.i.i.i.i.i.i.i78.i.i.i572 = getelementptr inbounds %class.Column*, %class.Column** %cond.i67.i.i.i566, i64 %sub.ptr.div.i.i.i.i.i.i.i.i77.i.i.i571
  %incdec.ptr.i.i.i573 = getelementptr inbounds %class.Column*, %class.Column** %add.ptr.i.i.i.i.i.i.i.i78.i.i.i572, i64 1
  %46 = load %class.Column**, %class.Column*** %_M_finish.i.i540, align 8, !tbaa !57
  %sub.ptr.lhs.cast.i.i.i.i.i.i.i.i.i.i.i574 = ptrtoint %class.Column** %46 to i64
  %sub.ptr.sub.i.i.i.i.i.i.i.i.i.i.i575 = sub i64 %sub.ptr.lhs.cast.i.i.i.i.i.i.i.i.i.i.i574, %sub.ptr.lhs.cast.i28.i.i.i.i546
  %tobool.not.i.i.i.i.i.i.i.i.i.i.i576 = icmp eq i64 %sub.ptr.sub.i.i.i.i.i.i.i.i.i.i.i575, 0
  br i1 %tobool.not.i.i.i.i.i.i.i.i.i.i.i576, label %invoke.cont15.i.i.i580, label %if.then.i.i.i.i.i.i.i.i.i.i.i578

if.then.i.i.i.i.i.i.i.i.i.i.i578:                 ; preds = %invoke.cont10.i.i.i577
  %47 = bitcast %class.Column** %incdec.ptr.i.i.i573 to i8*
  %48 = bitcast %class.Column** %36 to i8*
  tail call void @llvm.memmove.p0i8.p0i8.i64(i8* nonnull align 8 %47, i8* align 8 %48, i64 %sub.ptr.sub.i.i.i.i.i.i.i.i.i.i.i575, i1 false) #20
  br label %invoke.cont15.i.i.i580

invoke.cont15.i.i.i580:                           ; preds = %if.then.i.i.i.i.i.i.i.i.i.i.i578, %invoke.cont10.i.i.i577
  %tobool.not.i68.i.i.i579 = icmp eq %class.Column** %42, null
  br i1 %tobool.not.i68.i.i.i579, label %_ZNSt6vectorIP6ColumnSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i.i585, label %if.then.i69.i.i.i581

if.then.i69.i.i.i581:                             ; preds = %invoke.cont15.i.i.i580
  %49 = bitcast %class.Column** %42 to i8*
  tail call void @_ZdlPv(i8* nonnull %49) #20
  br label %_ZNSt6vectorIP6ColumnSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i.i585

_ZNSt6vectorIP6ColumnSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i.i585: ; preds = %if.then.i69.i.i.i581, %invoke.cont15.i.i.i580
  %sub.ptr.div.i.i.i.i.i.i.i.i.i.i.i582 = ashr exact i64 %sub.ptr.sub.i.i.i.i.i.i.i.i.i.i.i575, 3
  %add.ptr.i.i.i.i.i.i.i.i.i.i.i583 = getelementptr inbounds %class.Column*, %class.Column** %incdec.ptr.i.i.i573, i64 %sub.ptr.div.i.i.i.i.i.i.i.i.i.i.i582
  store %class.Column** %cond.i67.i.i.i566, %class.Column*** %_M_start.i27.i.i.i.i545, align 8, !tbaa !29
  store %class.Column** %add.ptr.i.i.i.i.i.i.i.i.i.i.i583, %class.Column*** %_M_finish.i.i540, align 8, !tbaa !57
  %add.ptr39.i.i.i584 = getelementptr inbounds %class.Column*, %class.Column** %cond.i67.i.i.i566, i64 %cond.i.i.i.i556
  store %class.Column** %add.ptr39.i.i.i584, %class.Column*** %_M_end_of_storage.i.i541, align 8, !tbaa !84
  br label %if.end255

for.body37:                                       ; preds = %for.body37.lr.ph, %for.body37
  %indvars.iv720 = phi i64 [ 0, %for.body37.lr.ph ], [ %indvars.iv.next721, %for.body37 ]
  %__begin326.sroa.0.0703 = phi %"struct.std::__detail::_Hash_node"* [ %__begin326.sroa.0.0700, %for.body37.lr.ph ], [ %__begin326.sroa.0.0, %for.body37 ]
  %second40 = getelementptr inbounds %"struct.std::__detail::_Hash_node", %"struct.std::__detail::_Hash_node"* %__begin326.sroa.0.0703, i64 0, i32 0, i32 1, i32 0, i32 0, i64 8
  %_M_start.i588 = bitcast i8* %second40 to %struct.GroupByColumn**
  %50 = load %struct.GroupByColumn*, %struct.GroupByColumn** %_M_start.i588, align 8, !tbaa !42
  %val43 = getelementptr inbounds %struct.GroupByColumn, %struct.GroupByColumn* %50, i64 %conv41, i32 1
  %51 = bitcast i8** %val43 to i64**
  %52 = load i64*, i64** %51, align 8, !tbaa !78
  %53 = load i64, i64* %52, align 8, !tbaa !23
  %indvars.iv.next721 = add nuw i64 %indvars.iv720, 1
  %arrayidx47 = getelementptr inbounds i64, i64* %28, i64 %indvars.iv720
  store i64 %53, i64* %arrayidx47, align 8, !tbaa !23
  %54 = bitcast %"struct.std::__detail::_Hash_node"* %__begin326.sroa.0.0703 to %"struct.std::__detail::_Hash_node"**
  %__begin326.sroa.0.0 = load %"struct.std::__detail::_Hash_node"*, %"struct.std::__detail::_Hash_node"** %54, align 8, !tbaa !22
  %cmp.i468.not = icmp eq %"struct.std::__detail::_Hash_node"* %__begin326.sroa.0.0, null
  br i1 %cmp.i468.not, label %for.cond.cleanup36, label %for.body37

sw.bb55:                                          ; preds = %if.then
  %conv57 = and i64 %0, 4294967295
  %mul58 = shl nuw nsw i64 %conv57, 3
  %call59 = tail call i8* @omni_allocate(i64 %mul58)
  %55 = bitcast i8* %call59 to double*
  %_M_nxt.i.i.i665 = getelementptr inbounds %class.HashGroupBy, %class.HashGroupBy* %this, i64 0, i32 4, i32 0, i32 2, i32 0
  %56 = bitcast %"struct.std::__detail::_Hash_node_base"** %_M_nxt.i.i.i665 to %"struct.std::__detail::_Hash_node"**
  %__begin363.sroa.0.0704 = load %"struct.std::__detail::_Hash_node"*, %"struct.std::__detail::_Hash_node"** %56, align 8, !tbaa !22
  %cmp.i671.not705 = icmp eq %"struct.std::__detail::_Hash_node"* %__begin363.sroa.0.0704, null
  br i1 %cmp.i671.not705, label %for.cond.cleanup73, label %for.body74.lr.ph

for.body74.lr.ph:                                 ; preds = %sw.bb55
  %conv78 = sext i32 %columnIdx to i64
  br label %for.body74

for.cond.cleanup73:                               ; preds = %for.body74, %sw.bb55
  %call88 = tail call noalias nonnull dereferenceable(40) i8* @_Znwm(i64 40) #18
  %57 = bitcast i8* %call88 to i32 (...)***
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [4 x i8*] }, { [4 x i8*] }* @_ZTV6Column, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %57, align 8, !tbaa !50
  %data.i666 = getelementptr inbounds i8, i8* %call88, i64 8
  %58 = bitcast i8* %data.i666 to i8**
  store i8* %call59, i8** %58, align 8, !tbaa !34
  %type.i667 = getelementptr inbounds i8, i8* %call88, i64 24
  %59 = bitcast i8* %type.i667 to i32*
  store i32 3, i32* %59, align 8, !tbaa !32
  %size.i668 = getelementptr inbounds i8, i8* %call88, i64 32
  %60 = bitcast i8* %size.i668 to i64*
  store i64 %conv57, i64* %60, align 8, !tbaa !82
  %types.i612 = getelementptr inbounds %class.Table, %class.Table* %table, i64 0, i32 4
  %61 = load i32*, i32** %types.i612, align 8, !tbaa !61
  %columnSize.i613 = getelementptr inbounds %class.Table, %class.Table* %table, i64 0, i32 7
  %62 = load i32, i32* %columnSize.i613, align 8, !tbaa !83
  %idxprom.i614 = zext i32 %62 to i64
  %arrayidx.i615 = getelementptr inbounds i32, i32* %61, i64 %idxprom.i614
  store i32 3, i32* %arrayidx.i615, align 4, !tbaa !27
  %inc.i616 = add i32 %62, 1
  store i32 %inc.i616, i32* %columnSize.i613, align 8, !tbaa !83
  %_M_finish.i.i617 = getelementptr inbounds %class.Table, %class.Table* %table, i64 0, i32 3, i32 0, i32 0, i32 1
  %63 = load %class.Column**, %class.Column*** %_M_finish.i.i617, align 8, !tbaa !57
  %_M_end_of_storage.i.i618 = getelementptr inbounds %class.Table, %class.Table* %table, i64 0, i32 3, i32 0, i32 0, i32 2
  %64 = load %class.Column**, %class.Column*** %_M_end_of_storage.i.i618, align 8, !tbaa !84
  %cmp.not.i.i619 = icmp eq %class.Column** %63, %64
  br i1 %cmp.not.i.i619, label %if.else.i.i635, label %if.then.i.i621

if.then.i.i621:                                   ; preds = %for.cond.cleanup73
  %65 = bitcast %class.Column** %63 to i8**
  store i8* %call88, i8** %65, align 8, !tbaa !19
  %66 = load %class.Column**, %class.Column*** %_M_finish.i.i617, align 8, !tbaa !57
  %incdec.ptr.i.i620 = getelementptr inbounds %class.Column*, %class.Column** %66, i64 1
  store %class.Column** %incdec.ptr.i.i620, %class.Column*** %_M_finish.i.i617, align 8, !tbaa !57
  br label %if.end255

if.else.i.i635:                                   ; preds = %for.cond.cleanup73
  %_M_start.i27.i.i.i.i622 = getelementptr inbounds %class.Table, %class.Table* %table, i64 0, i32 3, i32 0, i32 0, i32 0
  %67 = load %class.Column**, %class.Column*** %_M_start.i27.i.i.i.i622, align 8, !tbaa !29
  %sub.ptr.lhs.cast.i28.i.i.i.i623 = ptrtoint %class.Column** %63 to i64
  %sub.ptr.rhs.cast.i29.i.i.i.i624 = ptrtoint %class.Column** %67 to i64
  %sub.ptr.sub.i30.i.i.i.i625 = sub i64 %sub.ptr.lhs.cast.i28.i.i.i.i623, %sub.ptr.rhs.cast.i29.i.i.i.i624
  %sub.ptr.div.i31.i.i.i.i626 = ashr exact i64 %sub.ptr.sub.i30.i.i.i.i625, 3
  %cmp.i.i.i.i.i627 = icmp eq i64 %sub.ptr.sub.i30.i.i.i.i625, 0
  %.sroa.speculated.i.i.i.i628 = select i1 %cmp.i.i.i.i.i627, i64 1, i64 %sub.ptr.div.i31.i.i.i.i626
  %add.i.i.i.i629 = add nsw i64 %.sroa.speculated.i.i.i.i628, %sub.ptr.div.i31.i.i.i.i626
  %cmp7.i.i.i.i630 = icmp ult i64 %add.i.i.i.i629, %sub.ptr.div.i31.i.i.i.i626
  %cmp9.i.i.i.i631 = icmp ugt i64 %add.i.i.i.i629, 2305843009213693951
  %or.cond.i.i.i.i632 = or i1 %cmp7.i.i.i.i630, %cmp9.i.i.i.i631
  %cond.i.i.i.i633 = select i1 %or.cond.i.i.i.i632, i64 2305843009213693951, i64 %add.i.i.i.i629
  %cmp.not.i.i.i.i634 = icmp eq i64 %cond.i.i.i.i633, 0
  br i1 %cmp.not.i.i.i.i634, label %invoke.cont.i.i.i646, label %_ZNSt16allocator_traitsISaIP6ColumnEE8allocateERS2_m.exit.i.i.i.i641

_ZNSt16allocator_traitsISaIP6ColumnEE8allocateERS2_m.exit.i.i.i.i641: ; preds = %if.else.i.i635
  %mul.i.i.i.i.i.i636 = shl nuw i64 %cond.i.i.i.i633, 3
  %call2.i.i.i.i.i.i637 = tail call noalias nonnull i8* @_Znwm(i64 %mul.i.i.i.i.i.i636) #19
  %68 = bitcast i8* %call2.i.i.i.i.i.i637 to %class.Column**
  %.pre.i.i.i638 = load %class.Column**, %class.Column*** %_M_start.i27.i.i.i.i622, align 8, !tbaa !29
  %.pre83.i.i.i639 = ptrtoint %class.Column** %.pre.i.i.i638 to i64
  %.pre84.i.i.i640 = sub i64 %sub.ptr.lhs.cast.i28.i.i.i.i623, %.pre83.i.i.i639
  br label %invoke.cont.i.i.i646

invoke.cont.i.i.i646:                             ; preds = %_ZNSt16allocator_traitsISaIP6ColumnEE8allocateERS2_m.exit.i.i.i.i641, %if.else.i.i635
  %sub.ptr.sub.i.i.i.i.i.i.i.i74.pre-phi.i.i.i642 = phi i64 [ %.pre84.i.i.i640, %_ZNSt16allocator_traitsISaIP6ColumnEE8allocateERS2_m.exit.i.i.i.i641 ], [ %sub.ptr.sub.i30.i.i.i.i625, %if.else.i.i635 ]
  %69 = phi %class.Column** [ %.pre.i.i.i638, %_ZNSt16allocator_traitsISaIP6ColumnEE8allocateERS2_m.exit.i.i.i.i641 ], [ %67, %if.else.i.i635 ]
  %cond.i67.i.i.i643 = phi %class.Column** [ %68, %_ZNSt16allocator_traitsISaIP6ColumnEE8allocateERS2_m.exit.i.i.i.i641 ], [ null, %if.else.i.i635 ]
  %add.ptr.i.i.i644 = getelementptr inbounds %class.Column*, %class.Column** %cond.i67.i.i.i643, i64 %sub.ptr.div.i31.i.i.i.i626
  %70 = bitcast %class.Column** %add.ptr.i.i.i644 to i8**
  store i8* %call88, i8** %70, align 8, !tbaa !19
  %tobool.not.i.i.i.i.i.i.i.i75.i.i.i645 = icmp eq i64 %sub.ptr.sub.i.i.i.i.i.i.i.i74.pre-phi.i.i.i642, 0
  br i1 %tobool.not.i.i.i.i.i.i.i.i75.i.i.i645, label %invoke.cont10.i.i.i654, label %if.then.i.i.i.i.i.i.i.i76.i.i.i647

if.then.i.i.i.i.i.i.i.i76.i.i.i647:               ; preds = %invoke.cont.i.i.i646
  %71 = bitcast %class.Column** %cond.i67.i.i.i643 to i8*
  %72 = bitcast %class.Column** %69 to i8*
  tail call void @llvm.memmove.p0i8.p0i8.i64(i8* align 8 %71, i8* align 8 %72, i64 %sub.ptr.sub.i.i.i.i.i.i.i.i74.pre-phi.i.i.i642, i1 false) #20
  br label %invoke.cont10.i.i.i654

invoke.cont10.i.i.i654:                           ; preds = %if.then.i.i.i.i.i.i.i.i76.i.i.i647, %invoke.cont.i.i.i646
  %sub.ptr.div.i.i.i.i.i.i.i.i77.i.i.i648 = ashr exact i64 %sub.ptr.sub.i.i.i.i.i.i.i.i74.pre-phi.i.i.i642, 3
  %add.ptr.i.i.i.i.i.i.i.i78.i.i.i649 = getelementptr inbounds %class.Column*, %class.Column** %cond.i67.i.i.i643, i64 %sub.ptr.div.i.i.i.i.i.i.i.i77.i.i.i648
  %incdec.ptr.i.i.i650 = getelementptr inbounds %class.Column*, %class.Column** %add.ptr.i.i.i.i.i.i.i.i78.i.i.i649, i64 1
  %73 = load %class.Column**, %class.Column*** %_M_finish.i.i617, align 8, !tbaa !57
  %sub.ptr.lhs.cast.i.i.i.i.i.i.i.i.i.i.i651 = ptrtoint %class.Column** %73 to i64
  %sub.ptr.sub.i.i.i.i.i.i.i.i.i.i.i652 = sub i64 %sub.ptr.lhs.cast.i.i.i.i.i.i.i.i.i.i.i651, %sub.ptr.lhs.cast.i28.i.i.i.i623
  %tobool.not.i.i.i.i.i.i.i.i.i.i.i653 = icmp eq i64 %sub.ptr.sub.i.i.i.i.i.i.i.i.i.i.i652, 0
  br i1 %tobool.not.i.i.i.i.i.i.i.i.i.i.i653, label %invoke.cont15.i.i.i657, label %if.then.i.i.i.i.i.i.i.i.i.i.i655

if.then.i.i.i.i.i.i.i.i.i.i.i655:                 ; preds = %invoke.cont10.i.i.i654
  %74 = bitcast %class.Column** %incdec.ptr.i.i.i650 to i8*
  %75 = bitcast %class.Column** %63 to i8*
  tail call void @llvm.memmove.p0i8.p0i8.i64(i8* nonnull align 8 %74, i8* align 8 %75, i64 %sub.ptr.sub.i.i.i.i.i.i.i.i.i.i.i652, i1 false) #20
  br label %invoke.cont15.i.i.i657

invoke.cont15.i.i.i657:                           ; preds = %if.then.i.i.i.i.i.i.i.i.i.i.i655, %invoke.cont10.i.i.i654
  %tobool.not.i68.i.i.i656 = icmp eq %class.Column** %69, null
  br i1 %tobool.not.i68.i.i.i656, label %_ZNSt6vectorIP6ColumnSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i.i662, label %if.then.i69.i.i.i658

if.then.i69.i.i.i658:                             ; preds = %invoke.cont15.i.i.i657
  %76 = bitcast %class.Column** %69 to i8*
  tail call void @_ZdlPv(i8* nonnull %76) #20
  br label %_ZNSt6vectorIP6ColumnSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i.i662

_ZNSt6vectorIP6ColumnSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i.i662: ; preds = %if.then.i69.i.i.i658, %invoke.cont15.i.i.i657
  %sub.ptr.div.i.i.i.i.i.i.i.i.i.i.i659 = ashr exact i64 %sub.ptr.sub.i.i.i.i.i.i.i.i.i.i.i652, 3
  %add.ptr.i.i.i.i.i.i.i.i.i.i.i660 = getelementptr inbounds %class.Column*, %class.Column** %incdec.ptr.i.i.i650, i64 %sub.ptr.div.i.i.i.i.i.i.i.i.i.i.i659
  store %class.Column** %cond.i67.i.i.i643, %class.Column*** %_M_start.i27.i.i.i.i622, align 8, !tbaa !29
  store %class.Column** %add.ptr.i.i.i.i.i.i.i.i.i.i.i660, %class.Column*** %_M_finish.i.i617, align 8, !tbaa !57
  %add.ptr39.i.i.i661 = getelementptr inbounds %class.Column*, %class.Column** %cond.i67.i.i.i643, i64 %cond.i.i.i.i633
  store %class.Column** %add.ptr39.i.i.i661, %class.Column*** %_M_end_of_storage.i.i618, align 8, !tbaa !84
  br label %if.end255

for.body74:                                       ; preds = %for.body74.lr.ph, %for.body74
  %indvars.iv722 = phi i64 [ 0, %for.body74.lr.ph ], [ %indvars.iv.next723, %for.body74 ]
  %__begin363.sroa.0.0707 = phi %"struct.std::__detail::_Hash_node"* [ %__begin363.sroa.0.0704, %for.body74.lr.ph ], [ %__begin363.sroa.0.0, %for.body74 ]
  %second77 = getelementptr inbounds %"struct.std::__detail::_Hash_node", %"struct.std::__detail::_Hash_node"* %__begin363.sroa.0.0707, i64 0, i32 0, i32 1, i32 0, i32 0, i64 8
  %_M_start.i609 = bitcast i8* %second77 to %struct.GroupByColumn**
  %77 = load %struct.GroupByColumn*, %struct.GroupByColumn** %_M_start.i609, align 8, !tbaa !42
  %val80 = getelementptr inbounds %struct.GroupByColumn, %struct.GroupByColumn* %77, i64 %conv78, i32 1
  %78 = bitcast i8** %val80 to double**
  %79 = load double*, double** %78, align 8, !tbaa !78
  %80 = load double, double* %79, align 8, !tbaa !37
  %indvars.iv.next723 = add nuw i64 %indvars.iv722, 1
  %arrayidx84 = getelementptr inbounds double, double* %55, i64 %indvars.iv722
  store double %80, double* %arrayidx84, align 8, !tbaa !37
  %81 = bitcast %"struct.std::__detail::_Hash_node"* %__begin363.sroa.0.0707 to %"struct.std::__detail::_Hash_node"**
  %__begin363.sroa.0.0 = load %"struct.std::__detail::_Hash_node"*, %"struct.std::__detail::_Hash_node"** %81, align 8, !tbaa !22
  %cmp.i671.not = icmp eq %"struct.std::__detail::_Hash_node"* %__begin363.sroa.0.0, null
  br i1 %cmp.i671.not, label %for.cond.cleanup73, label %for.body74

if.then93:                                        ; preds = %entry
  %82 = bitcast %"class.std::unordered_map"* %rows to i8*
  call void @llvm.lifetime.start.p0i8(i64 56, i8* nonnull %82) #20
  %conv94 = sext i32 %columnIdx to i64
  %_M_start.i606 = getelementptr inbounds %class.HashGroupBy, %class.HashGroupBy* %this, i64 0, i32 3, i32 0, i32 0, i32 0
  %83 = load %class.Aggregator**, %class.Aggregator*** %_M_start.i606, align 8, !tbaa !47
  %add.ptr.i607 = getelementptr inbounds %class.Aggregator*, %class.Aggregator** %83, i64 %conv94
  %84 = load %class.Aggregator*, %class.Aggregator** %add.ptr.i607, align 8, !tbaa !19
  %_M_h.i = getelementptr inbounds %"class.std::unordered_map", %"class.std::unordered_map"* %rows, i64 0, i32 0
  %_M_h2.i = getelementptr inbounds %class.Aggregator, %class.Aggregator* %84, i64 0, i32 3, i32 0
  %_M_buckets.i.i = getelementptr inbounds %"class.std::unordered_map", %"class.std::unordered_map"* %rows, i64 0, i32 0, i32 0
  store %"struct.std::__detail::_Hash_node_base"** null, %"struct.std::__detail::_Hash_node_base"*** %_M_buckets.i.i, align 8, !tbaa !21
  %_M_bucket_count.i.i = getelementptr inbounds %"class.std::unordered_map", %"class.std::unordered_map"* %rows, i64 0, i32 0, i32 1
  %_M_bucket_count2.i.i = getelementptr inbounds %class.Aggregator, %class.Aggregator* %84, i64 0, i32 3, i32 0, i32 1
  %85 = load i64, i64* %_M_bucket_count2.i.i, align 8, !tbaa !20
  store i64 %85, i64* %_M_bucket_count.i.i, align 8, !tbaa !20
  %_M_nxt.i.i.i603 = getelementptr inbounds %"class.std::unordered_map", %"class.std::unordered_map"* %rows, i64 0, i32 0, i32 2, i32 0
  store %"struct.std::__detail::_Hash_node_base"* null, %"struct.std::__detail::_Hash_node_base"** %_M_nxt.i.i.i603, align 8, !tbaa !22
  %_M_element_count.i.i604 = getelementptr inbounds %"class.std::unordered_map", %"class.std::unordered_map"* %rows, i64 0, i32 0, i32 3
  %_M_element_count3.i.i = getelementptr inbounds %class.Aggregator, %class.Aggregator* %84, i64 0, i32 3, i32 0, i32 3
  %86 = load i64, i64* %_M_element_count3.i.i, align 8, !tbaa !94
  store i64 %86, i64* %_M_element_count.i.i604, align 8, !tbaa !94
  %_M_rehash_policy.i.i = getelementptr inbounds %"class.std::unordered_map", %"class.std::unordered_map"* %rows, i64 0, i32 0, i32 4
  %_M_rehash_policy4.i.i = getelementptr inbounds %class.Aggregator, %class.Aggregator* %84, i64 0, i32 3, i32 0, i32 4
  %87 = bitcast %"struct.std::__detail::_Prime_rehash_policy"* %_M_rehash_policy.i.i to i8*
  %88 = bitcast %"struct.std::__detail::_Prime_rehash_policy"* %_M_rehash_policy4.i.i to i8*
  call void @llvm.memcpy.p0i8.p0i8.i64(i8* nonnull align 8 dereferenceable(16) %87, i8* nonnull align 8 dereferenceable(16) %88, i64 16, i1 false), !tbaa.struct !122
  %_M_single_bucket.i.i = getelementptr inbounds %"class.std::unordered_map", %"class.std::unordered_map"* %rows, i64 0, i32 0, i32 5
  store %"struct.std::__detail::_Hash_node_base"* null, %"struct.std::__detail::_Hash_node_base"** %_M_single_bucket.i.i, align 8, !tbaa !124
  %89 = bitcast %class.anon* %ref.tmp5.i.i to i8*
  call void @llvm.lifetime.start.p0i8(i64 8, i8* nonnull %89) #20
  %90 = getelementptr inbounds %class.anon, %class.anon* %ref.tmp5.i.i, i64 0, i32 0
  store %"class.std::_Hashtable"* %_M_h.i, %"class.std::_Hashtable"** %90, align 8, !tbaa !125
  call void @_ZNSt10_HashtableImSt4pairIKmSt6vectorI13GroupByColumnSaIS3_EEESaIS6_ENSt8__detail10_Select1stESt8equal_toImESt4hashImENS8_18_Mod_range_hashingENS8_20_Default_ranged_hashENS8_20_Prime_rehash_policyENS8_17_Hashtable_traitsILb0ELb0ELb1EEEE9_M_assignIZNSJ_C1ERKSJ_EUlPKNS8_10_Hash_nodeIS6_Lb0EEEE_EEvSM_RKT_(%"class.std::_Hashtable"* nonnull dereferenceable(56) %_M_h.i, %"class.std::_Hashtable"* nonnull align 8 dereferenceable(56) %_M_h2.i, %class.anon* nonnull align 8 dereferenceable(8) %ref.tmp5.i.i)
  call void @llvm.lifetime.end.p0i8(i64 8, i8* nonnull %89) #20
  %91 = load %class.Aggregator**, %class.Aggregator*** %_M_start.i606, align 8, !tbaa !47
  %add.ptr.i602 = getelementptr inbounds %class.Aggregator*, %class.Aggregator** %91, i64 %conv94
  %92 = load %class.Aggregator*, %class.Aggregator** %add.ptr.i602, align 8, !tbaa !19
  %_M_element_count.i.i600 = getelementptr inbounds %class.Aggregator, %class.Aggregator* %92, i64 0, i32 3, i32 0, i32 3
  %93 = load i64, i64* %_M_element_count.i.i600, align 8, !tbaa !94
  switch i32 %type, label %if.then93.sw.epilog251_crit_edge [
    i32 1, label %sw.bb106
    i32 2, label %sw.bb152
    i32 3, label %sw.bb201
  ]

if.then93.sw.epilog251_crit_edge:                 ; preds = %if.then93
  %.pre = bitcast %"struct.std::__detail::_Hash_node_base"** %_M_nxt.i.i.i603 to %"struct.std::__detail::_Hash_node"**
  br label %sw.epilog251

sw.bb106:                                         ; preds = %if.then93
  %conv108 = and i64 %93, 4294967295
  %mul109 = shl nuw nsw i64 %conv108, 2
  %call112 = invoke i8* @omni_allocate(i64 %mul109)
          to label %invoke.cont111 unwind label %lpad110

invoke.cont111:                                   ; preds = %sw.bb106
  %94 = bitcast i8* %call112 to i32*
  %95 = bitcast %"struct.std::__detail::_Hash_node_base"** %_M_nxt.i.i.i603 to %"struct.std::__detail::_Hash_node"**
  %__begin4.sroa.0.0708 = load %"struct.std::__detail::_Hash_node"*, %"struct.std::__detail::_Hash_node"** %95, align 8, !tbaa !22
  %cmp.i598.not709 = icmp eq %"struct.std::__detail::_Hash_node"* %__begin4.sroa.0.0708, null
  br i1 %cmp.i598.not709, label %for.cond.cleanup122, label %for.body123

for.cond.cleanup122:                              ; preds = %for.body123, %invoke.cont111
  %call144 = invoke noalias nonnull dereferenceable(40) i8* @_Znwm(i64 40) #18
          to label %invoke.cont143 unwind label %lpad142

lpad110:                                          ; preds = %sw.bb106
  %96 = landingpad { i8*, i32 }
          cleanup
  br label %ehcleanup252

for.body123:                                      ; preds = %invoke.cont111, %for.body123
  %indvars.iv724 = phi i64 [ %indvars.iv.next725, %for.body123 ], [ 0, %invoke.cont111 ]
  %__begin4.sroa.0.0711 = phi %"struct.std::__detail::_Hash_node"* [ %__begin4.sroa.0.0, %for.body123 ], [ %__begin4.sroa.0.0708, %invoke.cont111 ]
  %second126 = getelementptr inbounds %"struct.std::__detail::_Hash_node", %"struct.std::__detail::_Hash_node"* %__begin4.sroa.0.0711, i64 0, i32 0, i32 1, i32 0, i32 0, i64 8
  %_M_start.i594 = bitcast i8* %second126 to %struct.GroupByColumn**
  %97 = load %struct.GroupByColumn*, %struct.GroupByColumn** %_M_start.i594, align 8, !tbaa !42
  %val128 = getelementptr inbounds %struct.GroupByColumn, %struct.GroupByColumn* %97, i64 0, i32 1
  %98 = bitcast i8** %val128 to i32**
  %99 = load i32*, i32** %98, align 8, !tbaa !78
  %100 = load i32, i32* %99, align 4, !tbaa !25
  %indvars.iv.next725 = add nuw i64 %indvars.iv724, 1
  %arrayidx134 = getelementptr inbounds i32, i32* %94, i64 %indvars.iv724
  store i32 %100, i32* %arrayidx134, align 4, !tbaa !25
  %101 = bitcast %"struct.std::__detail::_Hash_node"* %__begin4.sroa.0.0711 to %"struct.std::__detail::_Hash_node"**
  %__begin4.sroa.0.0 = load %"struct.std::__detail::_Hash_node"*, %"struct.std::__detail::_Hash_node"** %101, align 8, !tbaa !22
  %cmp.i598.not = icmp eq %"struct.std::__detail::_Hash_node"* %__begin4.sroa.0.0, null
  br i1 %cmp.i598.not, label %for.cond.cleanup122, label %for.body123

invoke.cont143:                                   ; preds = %for.cond.cleanup122
  %102 = bitcast i8* %call144 to i32 (...)***
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [4 x i8*] }, { [4 x i8*] }* @_ZTV6Column, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %102, align 8, !tbaa !50
  %data.i590 = getelementptr inbounds i8, i8* %call144, i64 8
  %103 = bitcast i8* %data.i590 to i8**
  store i8* %call112, i8** %103, align 8, !tbaa !34
  %type.i591 = getelementptr inbounds i8, i8* %call144, i64 24
  %104 = bitcast i8* %type.i591 to i32*
  store i32 1, i32* %104, align 8, !tbaa !32
  %size.i592 = getelementptr inbounds i8, i8* %call144, i64 32
  %105 = bitcast i8* %size.i592 to i64*
  store i64 %conv108, i64* %105, align 8, !tbaa !82
  %types.i479 = getelementptr inbounds %class.Table, %class.Table* %table, i64 0, i32 4
  %106 = load i32*, i32** %types.i479, align 8, !tbaa !61
  %columnSize.i480 = getelementptr inbounds %class.Table, %class.Table* %table, i64 0, i32 7
  %107 = load i32, i32* %columnSize.i480, align 8, !tbaa !83
  %idxprom.i481 = zext i32 %107 to i64
  %arrayidx.i482 = getelementptr inbounds i32, i32* %106, i64 %idxprom.i481
  store i32 1, i32* %arrayidx.i482, align 4, !tbaa !27
  %inc.i483 = add i32 %107, 1
  store i32 %inc.i483, i32* %columnSize.i480, align 8, !tbaa !83
  %_M_finish.i.i484 = getelementptr inbounds %class.Table, %class.Table* %table, i64 0, i32 3, i32 0, i32 0, i32 1
  %108 = load %class.Column**, %class.Column*** %_M_finish.i.i484, align 8, !tbaa !57
  %_M_end_of_storage.i.i485 = getelementptr inbounds %class.Table, %class.Table* %table, i64 0, i32 3, i32 0, i32 0, i32 2
  %109 = load %class.Column**, %class.Column*** %_M_end_of_storage.i.i485, align 8, !tbaa !84
  %cmp.not.i.i486 = icmp eq %class.Column** %108, %109
  br i1 %cmp.not.i.i486, label %if.else.i.i502, label %if.then.i.i488

if.then.i.i488:                                   ; preds = %invoke.cont143
  %110 = bitcast %class.Column** %108 to i8**
  store i8* %call144, i8** %110, align 8, !tbaa !19
  %111 = load %class.Column**, %class.Column*** %_M_finish.i.i484, align 8, !tbaa !57
  %incdec.ptr.i.i487 = getelementptr inbounds %class.Column*, %class.Column** %111, i64 1
  store %class.Column** %incdec.ptr.i.i487, %class.Column*** %_M_finish.i.i484, align 8, !tbaa !57
  br label %sw.epilog251

if.else.i.i502:                                   ; preds = %invoke.cont143
  %_M_start.i27.i.i.i.i489 = getelementptr inbounds %class.Table, %class.Table* %table, i64 0, i32 3, i32 0, i32 0, i32 0
  %112 = load %class.Column**, %class.Column*** %_M_start.i27.i.i.i.i489, align 8, !tbaa !29
  %sub.ptr.lhs.cast.i28.i.i.i.i490 = ptrtoint %class.Column** %108 to i64
  %sub.ptr.rhs.cast.i29.i.i.i.i491 = ptrtoint %class.Column** %112 to i64
  %sub.ptr.sub.i30.i.i.i.i492 = sub i64 %sub.ptr.lhs.cast.i28.i.i.i.i490, %sub.ptr.rhs.cast.i29.i.i.i.i491
  %sub.ptr.div.i31.i.i.i.i493 = ashr exact i64 %sub.ptr.sub.i30.i.i.i.i492, 3
  %cmp.i.i.i.i.i494 = icmp eq i64 %sub.ptr.sub.i30.i.i.i.i492, 0
  %.sroa.speculated.i.i.i.i495 = select i1 %cmp.i.i.i.i.i494, i64 1, i64 %sub.ptr.div.i31.i.i.i.i493
  %add.i.i.i.i496 = add nsw i64 %.sroa.speculated.i.i.i.i495, %sub.ptr.div.i31.i.i.i.i493
  %cmp7.i.i.i.i497 = icmp ult i64 %add.i.i.i.i496, %sub.ptr.div.i31.i.i.i.i493
  %cmp9.i.i.i.i498 = icmp ugt i64 %add.i.i.i.i496, 2305843009213693951
  %or.cond.i.i.i.i499 = or i1 %cmp7.i.i.i.i497, %cmp9.i.i.i.i498
  %cond.i.i.i.i500 = select i1 %or.cond.i.i.i.i499, i64 2305843009213693951, i64 %add.i.i.i.i496
  %cmp.not.i.i.i.i501 = icmp eq i64 %cond.i.i.i.i500, 0
  br i1 %cmp.not.i.i.i.i501, label %invoke.cont.i.i.i513, label %_ZNSt16allocator_traitsISaIP6ColumnEE8allocateERS2_m.exit.i.i.i.i508

_ZNSt16allocator_traitsISaIP6ColumnEE8allocateERS2_m.exit.i.i.i.i508: ; preds = %if.else.i.i502
  %mul.i.i.i.i.i.i503 = shl nuw i64 %cond.i.i.i.i500, 3
  %call2.i.i.i.i.i.i504530 = invoke noalias nonnull i8* @_Znwm(i64 %mul.i.i.i.i.i.i503) #19
          to label %call2.i.i.i.i.i.i504.noexc unwind label %lpad142

call2.i.i.i.i.i.i504.noexc:                       ; preds = %_ZNSt16allocator_traitsISaIP6ColumnEE8allocateERS2_m.exit.i.i.i.i508
  %113 = bitcast i8* %call2.i.i.i.i.i.i504530 to %class.Column**
  %.pre.i.i.i505 = load %class.Column**, %class.Column*** %_M_start.i27.i.i.i.i489, align 8, !tbaa !29
  %.pre83.i.i.i506 = ptrtoint %class.Column** %.pre.i.i.i505 to i64
  %.pre84.i.i.i507 = sub i64 %sub.ptr.lhs.cast.i28.i.i.i.i490, %.pre83.i.i.i506
  br label %invoke.cont.i.i.i513

invoke.cont.i.i.i513:                             ; preds = %call2.i.i.i.i.i.i504.noexc, %if.else.i.i502
  %sub.ptr.sub.i.i.i.i.i.i.i.i74.pre-phi.i.i.i509 = phi i64 [ %.pre84.i.i.i507, %call2.i.i.i.i.i.i504.noexc ], [ %sub.ptr.sub.i30.i.i.i.i492, %if.else.i.i502 ]
  %114 = phi %class.Column** [ %.pre.i.i.i505, %call2.i.i.i.i.i.i504.noexc ], [ %112, %if.else.i.i502 ]
  %cond.i67.i.i.i510 = phi %class.Column** [ %113, %call2.i.i.i.i.i.i504.noexc ], [ null, %if.else.i.i502 ]
  %add.ptr.i.i.i511 = getelementptr inbounds %class.Column*, %class.Column** %cond.i67.i.i.i510, i64 %sub.ptr.div.i31.i.i.i.i493
  %115 = bitcast %class.Column** %add.ptr.i.i.i511 to i8**
  store i8* %call144, i8** %115, align 8, !tbaa !19
  %tobool.not.i.i.i.i.i.i.i.i75.i.i.i512 = icmp eq i64 %sub.ptr.sub.i.i.i.i.i.i.i.i74.pre-phi.i.i.i509, 0
  br i1 %tobool.not.i.i.i.i.i.i.i.i75.i.i.i512, label %invoke.cont10.i.i.i521, label %if.then.i.i.i.i.i.i.i.i76.i.i.i514

if.then.i.i.i.i.i.i.i.i76.i.i.i514:               ; preds = %invoke.cont.i.i.i513
  %116 = bitcast %class.Column** %cond.i67.i.i.i510 to i8*
  %117 = bitcast %class.Column** %114 to i8*
  call void @llvm.memmove.p0i8.p0i8.i64(i8* align 8 %116, i8* align 8 %117, i64 %sub.ptr.sub.i.i.i.i.i.i.i.i74.pre-phi.i.i.i509, i1 false) #20
  br label %invoke.cont10.i.i.i521

invoke.cont10.i.i.i521:                           ; preds = %if.then.i.i.i.i.i.i.i.i76.i.i.i514, %invoke.cont.i.i.i513
  %sub.ptr.div.i.i.i.i.i.i.i.i77.i.i.i515 = ashr exact i64 %sub.ptr.sub.i.i.i.i.i.i.i.i74.pre-phi.i.i.i509, 3
  %add.ptr.i.i.i.i.i.i.i.i78.i.i.i516 = getelementptr inbounds %class.Column*, %class.Column** %cond.i67.i.i.i510, i64 %sub.ptr.div.i.i.i.i.i.i.i.i77.i.i.i515
  %incdec.ptr.i.i.i517 = getelementptr inbounds %class.Column*, %class.Column** %add.ptr.i.i.i.i.i.i.i.i78.i.i.i516, i64 1
  %118 = load %class.Column**, %class.Column*** %_M_finish.i.i484, align 8, !tbaa !57
  %sub.ptr.lhs.cast.i.i.i.i.i.i.i.i.i.i.i518 = ptrtoint %class.Column** %118 to i64
  %sub.ptr.sub.i.i.i.i.i.i.i.i.i.i.i519 = sub i64 %sub.ptr.lhs.cast.i.i.i.i.i.i.i.i.i.i.i518, %sub.ptr.lhs.cast.i28.i.i.i.i490
  %tobool.not.i.i.i.i.i.i.i.i.i.i.i520 = icmp eq i64 %sub.ptr.sub.i.i.i.i.i.i.i.i.i.i.i519, 0
  br i1 %tobool.not.i.i.i.i.i.i.i.i.i.i.i520, label %invoke.cont15.i.i.i524, label %if.then.i.i.i.i.i.i.i.i.i.i.i522

if.then.i.i.i.i.i.i.i.i.i.i.i522:                 ; preds = %invoke.cont10.i.i.i521
  %119 = bitcast %class.Column** %incdec.ptr.i.i.i517 to i8*
  %120 = bitcast %class.Column** %108 to i8*
  call void @llvm.memmove.p0i8.p0i8.i64(i8* nonnull align 8 %119, i8* align 8 %120, i64 %sub.ptr.sub.i.i.i.i.i.i.i.i.i.i.i519, i1 false) #20
  br label %invoke.cont15.i.i.i524

invoke.cont15.i.i.i524:                           ; preds = %if.then.i.i.i.i.i.i.i.i.i.i.i522, %invoke.cont10.i.i.i521
  %tobool.not.i68.i.i.i523 = icmp eq %class.Column** %114, null
  br i1 %tobool.not.i68.i.i.i523, label %_ZNSt6vectorIP6ColumnSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i.i529, label %if.then.i69.i.i.i525

if.then.i69.i.i.i525:                             ; preds = %invoke.cont15.i.i.i524
  %121 = bitcast %class.Column** %114 to i8*
  call void @_ZdlPv(i8* nonnull %121) #20
  br label %_ZNSt6vectorIP6ColumnSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i.i529

_ZNSt6vectorIP6ColumnSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i.i529: ; preds = %if.then.i69.i.i.i525, %invoke.cont15.i.i.i524
  %sub.ptr.div.i.i.i.i.i.i.i.i.i.i.i526 = ashr exact i64 %sub.ptr.sub.i.i.i.i.i.i.i.i.i.i.i519, 3
  %add.ptr.i.i.i.i.i.i.i.i.i.i.i527 = getelementptr inbounds %class.Column*, %class.Column** %incdec.ptr.i.i.i517, i64 %sub.ptr.div.i.i.i.i.i.i.i.i.i.i.i526
  store %class.Column** %cond.i67.i.i.i510, %class.Column*** %_M_start.i27.i.i.i.i489, align 8, !tbaa !29
  store %class.Column** %add.ptr.i.i.i.i.i.i.i.i.i.i.i527, %class.Column*** %_M_finish.i.i484, align 8, !tbaa !57
  %add.ptr39.i.i.i528 = getelementptr inbounds %class.Column*, %class.Column** %cond.i67.i.i.i510, i64 %cond.i.i.i.i500
  store %class.Column** %add.ptr39.i.i.i528, %class.Column*** %_M_end_of_storage.i.i485, align 8, !tbaa !84
  br label %sw.epilog251

lpad142:                                          ; preds = %_ZNSt16allocator_traitsISaIP6ColumnEE8allocateERS2_m.exit.i.i.i.i508, %for.cond.cleanup122
  %122 = landingpad { i8*, i32 }
          cleanup
  br label %ehcleanup252

sw.bb152:                                         ; preds = %if.then93
  %conv154 = and i64 %93, 4294967295
  %mul155 = shl nuw nsw i64 %conv154, 3
  %call158 = invoke i8* @omni_allocate(i64 %mul155)
          to label %invoke.cont157 unwind label %lpad156

invoke.cont157:                                   ; preds = %sw.bb152
  %123 = bitcast i8* %call158 to i64*
  %124 = bitcast %"struct.std::__detail::_Hash_node_base"** %_M_nxt.i.i.i603 to %"struct.std::__detail::_Hash_node"**
  %__begin4161.sroa.0.0712 = load %"struct.std::__detail::_Hash_node"*, %"struct.std::__detail::_Hash_node"** %124, align 8, !tbaa !22
  %cmp.i477.not713 = icmp eq %"struct.std::__detail::_Hash_node"* %__begin4161.sroa.0.0712, null
  br i1 %cmp.i477.not713, label %for.cond.cleanup171, label %for.body172

for.cond.cleanup171:                              ; preds = %for.body172, %invoke.cont157
  %call193 = invoke noalias nonnull dereferenceable(40) i8* @_Znwm(i64 40) #18
          to label %invoke.cont192 unwind label %lpad191

lpad156:                                          ; preds = %sw.bb152
  %125 = landingpad { i8*, i32 }
          cleanup
  br label %ehcleanup252

for.body172:                                      ; preds = %invoke.cont157, %for.body172
  %indvars.iv726 = phi i64 [ %indvars.iv.next727, %for.body172 ], [ 0, %invoke.cont157 ]
  %__begin4161.sroa.0.0715 = phi %"struct.std::__detail::_Hash_node"* [ %__begin4161.sroa.0.0, %for.body172 ], [ %__begin4161.sroa.0.0712, %invoke.cont157 ]
  %second175 = getelementptr inbounds %"struct.std::__detail::_Hash_node", %"struct.std::__detail::_Hash_node"* %__begin4161.sroa.0.0715, i64 0, i32 0, i32 1, i32 0, i32 0, i64 8
  %_M_start.i473 = bitcast i8* %second175 to %struct.GroupByColumn**
  %126 = load %struct.GroupByColumn*, %struct.GroupByColumn** %_M_start.i473, align 8, !tbaa !42
  %val177 = getelementptr inbounds %struct.GroupByColumn, %struct.GroupByColumn* %126, i64 0, i32 1
  %127 = bitcast i8** %val177 to i64**
  %128 = load i64*, i64** %127, align 8, !tbaa !78
  %129 = load i64, i64* %128, align 8, !tbaa !23
  %indvars.iv.next727 = add nuw i64 %indvars.iv726, 1
  %arrayidx183 = getelementptr inbounds i64, i64* %123, i64 %indvars.iv726
  store i64 %129, i64* %arrayidx183, align 8, !tbaa !23
  %130 = bitcast %"struct.std::__detail::_Hash_node"* %__begin4161.sroa.0.0715 to %"struct.std::__detail::_Hash_node"**
  %__begin4161.sroa.0.0 = load %"struct.std::__detail::_Hash_node"*, %"struct.std::__detail::_Hash_node"** %130, align 8, !tbaa !22
  %cmp.i477.not = icmp eq %"struct.std::__detail::_Hash_node"* %__begin4161.sroa.0.0, null
  br i1 %cmp.i477.not, label %for.cond.cleanup171, label %for.body172

invoke.cont192:                                   ; preds = %for.cond.cleanup171
  %131 = bitcast i8* %call193 to i32 (...)***
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [4 x i8*] }, { [4 x i8*] }* @_ZTV6Column, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %131, align 8, !tbaa !50
  %data.i469 = getelementptr inbounds i8, i8* %call193, i64 8
  %132 = bitcast i8* %data.i469 to i8**
  store i8* %call158, i8** %132, align 8, !tbaa !34
  %type.i470 = getelementptr inbounds i8, i8* %call193, i64 24
  %133 = bitcast i8* %type.i470 to i32*
  store i32 2, i32* %133, align 8, !tbaa !32
  %size.i471 = getelementptr inbounds i8, i8* %call193, i64 32
  %134 = bitcast i8* %size.i471 to i64*
  store i64 %conv154, i64* %134, align 8, !tbaa !82
  %types.i413 = getelementptr inbounds %class.Table, %class.Table* %table, i64 0, i32 4
  %135 = load i32*, i32** %types.i413, align 8, !tbaa !61
  %columnSize.i414 = getelementptr inbounds %class.Table, %class.Table* %table, i64 0, i32 7
  %136 = load i32, i32* %columnSize.i414, align 8, !tbaa !83
  %idxprom.i415 = zext i32 %136 to i64
  %arrayidx.i416 = getelementptr inbounds i32, i32* %135, i64 %idxprom.i415
  store i32 2, i32* %arrayidx.i416, align 4, !tbaa !27
  %inc.i417 = add i32 %136, 1
  store i32 %inc.i417, i32* %columnSize.i414, align 8, !tbaa !83
  %_M_finish.i.i418 = getelementptr inbounds %class.Table, %class.Table* %table, i64 0, i32 3, i32 0, i32 0, i32 1
  %137 = load %class.Column**, %class.Column*** %_M_finish.i.i418, align 8, !tbaa !57
  %_M_end_of_storage.i.i419 = getelementptr inbounds %class.Table, %class.Table* %table, i64 0, i32 3, i32 0, i32 0, i32 2
  %138 = load %class.Column**, %class.Column*** %_M_end_of_storage.i.i419, align 8, !tbaa !84
  %cmp.not.i.i420 = icmp eq %class.Column** %137, %138
  br i1 %cmp.not.i.i420, label %if.else.i.i436, label %if.then.i.i422

if.then.i.i422:                                   ; preds = %invoke.cont192
  %139 = bitcast %class.Column** %137 to i8**
  store i8* %call193, i8** %139, align 8, !tbaa !19
  %140 = load %class.Column**, %class.Column*** %_M_finish.i.i418, align 8, !tbaa !57
  %incdec.ptr.i.i421 = getelementptr inbounds %class.Column*, %class.Column** %140, i64 1
  store %class.Column** %incdec.ptr.i.i421, %class.Column*** %_M_finish.i.i418, align 8, !tbaa !57
  br label %sw.epilog251

if.else.i.i436:                                   ; preds = %invoke.cont192
  %_M_start.i27.i.i.i.i423 = getelementptr inbounds %class.Table, %class.Table* %table, i64 0, i32 3, i32 0, i32 0, i32 0
  %141 = load %class.Column**, %class.Column*** %_M_start.i27.i.i.i.i423, align 8, !tbaa !29
  %sub.ptr.lhs.cast.i28.i.i.i.i424 = ptrtoint %class.Column** %137 to i64
  %sub.ptr.rhs.cast.i29.i.i.i.i425 = ptrtoint %class.Column** %141 to i64
  %sub.ptr.sub.i30.i.i.i.i426 = sub i64 %sub.ptr.lhs.cast.i28.i.i.i.i424, %sub.ptr.rhs.cast.i29.i.i.i.i425
  %sub.ptr.div.i31.i.i.i.i427 = ashr exact i64 %sub.ptr.sub.i30.i.i.i.i426, 3
  %cmp.i.i.i.i.i428 = icmp eq i64 %sub.ptr.sub.i30.i.i.i.i426, 0
  %.sroa.speculated.i.i.i.i429 = select i1 %cmp.i.i.i.i.i428, i64 1, i64 %sub.ptr.div.i31.i.i.i.i427
  %add.i.i.i.i430 = add nsw i64 %.sroa.speculated.i.i.i.i429, %sub.ptr.div.i31.i.i.i.i427
  %cmp7.i.i.i.i431 = icmp ult i64 %add.i.i.i.i430, %sub.ptr.div.i31.i.i.i.i427
  %cmp9.i.i.i.i432 = icmp ugt i64 %add.i.i.i.i430, 2305843009213693951
  %or.cond.i.i.i.i433 = or i1 %cmp7.i.i.i.i431, %cmp9.i.i.i.i432
  %cond.i.i.i.i434 = select i1 %or.cond.i.i.i.i433, i64 2305843009213693951, i64 %add.i.i.i.i430
  %cmp.not.i.i.i.i435 = icmp eq i64 %cond.i.i.i.i434, 0
  br i1 %cmp.not.i.i.i.i435, label %invoke.cont.i.i.i447, label %_ZNSt16allocator_traitsISaIP6ColumnEE8allocateERS2_m.exit.i.i.i.i442

_ZNSt16allocator_traitsISaIP6ColumnEE8allocateERS2_m.exit.i.i.i.i442: ; preds = %if.else.i.i436
  %mul.i.i.i.i.i.i437 = shl nuw i64 %cond.i.i.i.i434, 3
  %call2.i.i.i.i.i.i438464 = invoke noalias nonnull i8* @_Znwm(i64 %mul.i.i.i.i.i.i437) #19
          to label %call2.i.i.i.i.i.i438.noexc unwind label %lpad191

call2.i.i.i.i.i.i438.noexc:                       ; preds = %_ZNSt16allocator_traitsISaIP6ColumnEE8allocateERS2_m.exit.i.i.i.i442
  %142 = bitcast i8* %call2.i.i.i.i.i.i438464 to %class.Column**
  %.pre.i.i.i439 = load %class.Column**, %class.Column*** %_M_start.i27.i.i.i.i423, align 8, !tbaa !29
  %.pre83.i.i.i440 = ptrtoint %class.Column** %.pre.i.i.i439 to i64
  %.pre84.i.i.i441 = sub i64 %sub.ptr.lhs.cast.i28.i.i.i.i424, %.pre83.i.i.i440
  br label %invoke.cont.i.i.i447

invoke.cont.i.i.i447:                             ; preds = %call2.i.i.i.i.i.i438.noexc, %if.else.i.i436
  %sub.ptr.sub.i.i.i.i.i.i.i.i74.pre-phi.i.i.i443 = phi i64 [ %.pre84.i.i.i441, %call2.i.i.i.i.i.i438.noexc ], [ %sub.ptr.sub.i30.i.i.i.i426, %if.else.i.i436 ]
  %143 = phi %class.Column** [ %.pre.i.i.i439, %call2.i.i.i.i.i.i438.noexc ], [ %141, %if.else.i.i436 ]
  %cond.i67.i.i.i444 = phi %class.Column** [ %142, %call2.i.i.i.i.i.i438.noexc ], [ null, %if.else.i.i436 ]
  %add.ptr.i.i.i445 = getelementptr inbounds %class.Column*, %class.Column** %cond.i67.i.i.i444, i64 %sub.ptr.div.i31.i.i.i.i427
  %144 = bitcast %class.Column** %add.ptr.i.i.i445 to i8**
  store i8* %call193, i8** %144, align 8, !tbaa !19
  %tobool.not.i.i.i.i.i.i.i.i75.i.i.i446 = icmp eq i64 %sub.ptr.sub.i.i.i.i.i.i.i.i74.pre-phi.i.i.i443, 0
  br i1 %tobool.not.i.i.i.i.i.i.i.i75.i.i.i446, label %invoke.cont10.i.i.i455, label %if.then.i.i.i.i.i.i.i.i76.i.i.i448

if.then.i.i.i.i.i.i.i.i76.i.i.i448:               ; preds = %invoke.cont.i.i.i447
  %145 = bitcast %class.Column** %cond.i67.i.i.i444 to i8*
  %146 = bitcast %class.Column** %143 to i8*
  call void @llvm.memmove.p0i8.p0i8.i64(i8* align 8 %145, i8* align 8 %146, i64 %sub.ptr.sub.i.i.i.i.i.i.i.i74.pre-phi.i.i.i443, i1 false) #20
  br label %invoke.cont10.i.i.i455

invoke.cont10.i.i.i455:                           ; preds = %if.then.i.i.i.i.i.i.i.i76.i.i.i448, %invoke.cont.i.i.i447
  %sub.ptr.div.i.i.i.i.i.i.i.i77.i.i.i449 = ashr exact i64 %sub.ptr.sub.i.i.i.i.i.i.i.i74.pre-phi.i.i.i443, 3
  %add.ptr.i.i.i.i.i.i.i.i78.i.i.i450 = getelementptr inbounds %class.Column*, %class.Column** %cond.i67.i.i.i444, i64 %sub.ptr.div.i.i.i.i.i.i.i.i77.i.i.i449
  %incdec.ptr.i.i.i451 = getelementptr inbounds %class.Column*, %class.Column** %add.ptr.i.i.i.i.i.i.i.i78.i.i.i450, i64 1
  %147 = load %class.Column**, %class.Column*** %_M_finish.i.i418, align 8, !tbaa !57
  %sub.ptr.lhs.cast.i.i.i.i.i.i.i.i.i.i.i452 = ptrtoint %class.Column** %147 to i64
  %sub.ptr.sub.i.i.i.i.i.i.i.i.i.i.i453 = sub i64 %sub.ptr.lhs.cast.i.i.i.i.i.i.i.i.i.i.i452, %sub.ptr.lhs.cast.i28.i.i.i.i424
  %tobool.not.i.i.i.i.i.i.i.i.i.i.i454 = icmp eq i64 %sub.ptr.sub.i.i.i.i.i.i.i.i.i.i.i453, 0
  br i1 %tobool.not.i.i.i.i.i.i.i.i.i.i.i454, label %invoke.cont15.i.i.i458, label %if.then.i.i.i.i.i.i.i.i.i.i.i456

if.then.i.i.i.i.i.i.i.i.i.i.i456:                 ; preds = %invoke.cont10.i.i.i455
  %148 = bitcast %class.Column** %incdec.ptr.i.i.i451 to i8*
  %149 = bitcast %class.Column** %137 to i8*
  call void @llvm.memmove.p0i8.p0i8.i64(i8* nonnull align 8 %148, i8* align 8 %149, i64 %sub.ptr.sub.i.i.i.i.i.i.i.i.i.i.i453, i1 false) #20
  br label %invoke.cont15.i.i.i458

invoke.cont15.i.i.i458:                           ; preds = %if.then.i.i.i.i.i.i.i.i.i.i.i456, %invoke.cont10.i.i.i455
  %tobool.not.i68.i.i.i457 = icmp eq %class.Column** %143, null
  br i1 %tobool.not.i68.i.i.i457, label %_ZNSt6vectorIP6ColumnSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i.i463, label %if.then.i69.i.i.i459

if.then.i69.i.i.i459:                             ; preds = %invoke.cont15.i.i.i458
  %150 = bitcast %class.Column** %143 to i8*
  call void @_ZdlPv(i8* nonnull %150) #20
  br label %_ZNSt6vectorIP6ColumnSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i.i463

_ZNSt6vectorIP6ColumnSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i.i463: ; preds = %if.then.i69.i.i.i459, %invoke.cont15.i.i.i458
  %sub.ptr.div.i.i.i.i.i.i.i.i.i.i.i460 = ashr exact i64 %sub.ptr.sub.i.i.i.i.i.i.i.i.i.i.i453, 3
  %add.ptr.i.i.i.i.i.i.i.i.i.i.i461 = getelementptr inbounds %class.Column*, %class.Column** %incdec.ptr.i.i.i451, i64 %sub.ptr.div.i.i.i.i.i.i.i.i.i.i.i460
  store %class.Column** %cond.i67.i.i.i444, %class.Column*** %_M_start.i27.i.i.i.i423, align 8, !tbaa !29
  store %class.Column** %add.ptr.i.i.i.i.i.i.i.i.i.i.i461, %class.Column*** %_M_finish.i.i418, align 8, !tbaa !57
  %add.ptr39.i.i.i462 = getelementptr inbounds %class.Column*, %class.Column** %cond.i67.i.i.i444, i64 %cond.i.i.i.i434
  store %class.Column** %add.ptr39.i.i.i462, %class.Column*** %_M_end_of_storage.i.i419, align 8, !tbaa !84
  br label %sw.epilog251

lpad191:                                          ; preds = %_ZNSt16allocator_traitsISaIP6ColumnEE8allocateERS2_m.exit.i.i.i.i442, %for.cond.cleanup171
  %151 = landingpad { i8*, i32 }
          cleanup
  br label %ehcleanup252

sw.bb201:                                         ; preds = %if.then93
  %conv203 = and i64 %93, 4294967295
  %mul204 = shl nuw nsw i64 %conv203, 3
  %call207 = invoke i8* @omni_allocate(i64 %mul204)
          to label %invoke.cont206 unwind label %lpad205

invoke.cont206:                                   ; preds = %sw.bb201
  %152 = bitcast i8* %call207 to double*
  %153 = bitcast %"struct.std::__detail::_Hash_node_base"** %_M_nxt.i.i.i603 to %"struct.std::__detail::_Hash_node"**
  %__begin4210.sroa.0.0716 = load %"struct.std::__detail::_Hash_node"*, %"struct.std::__detail::_Hash_node"** %153, align 8, !tbaa !22
  %cmp.i411.not717 = icmp eq %"struct.std::__detail::_Hash_node"* %__begin4210.sroa.0.0716, null
  br i1 %cmp.i411.not717, label %for.cond.cleanup220, label %for.body221

for.cond.cleanup220:                              ; preds = %for.body221, %invoke.cont206
  %call242 = invoke noalias nonnull dereferenceable(40) i8* @_Znwm(i64 40) #18
          to label %invoke.cont241 unwind label %lpad240

lpad205:                                          ; preds = %sw.bb201
  %154 = landingpad { i8*, i32 }
          cleanup
  br label %ehcleanup252

for.body221:                                      ; preds = %invoke.cont206, %for.body221
  %indvars.iv728 = phi i64 [ %indvars.iv.next729, %for.body221 ], [ 0, %invoke.cont206 ]
  %__begin4210.sroa.0.0719 = phi %"struct.std::__detail::_Hash_node"* [ %__begin4210.sroa.0.0, %for.body221 ], [ %__begin4210.sroa.0.0716, %invoke.cont206 ]
  %second224 = getelementptr inbounds %"struct.std::__detail::_Hash_node", %"struct.std::__detail::_Hash_node"* %__begin4210.sroa.0.0719, i64 0, i32 0, i32 1, i32 0, i32 0, i64 8
  %_M_start.i407 = bitcast i8* %second224 to %struct.GroupByColumn**
  %155 = load %struct.GroupByColumn*, %struct.GroupByColumn** %_M_start.i407, align 8, !tbaa !42
  %val226 = getelementptr inbounds %struct.GroupByColumn, %struct.GroupByColumn* %155, i64 0, i32 1
  %156 = bitcast i8** %val226 to double**
  %157 = load double*, double** %156, align 8, !tbaa !78
  %158 = load double, double* %157, align 8, !tbaa !37
  %indvars.iv.next729 = add nuw i64 %indvars.iv728, 1
  %arrayidx232 = getelementptr inbounds double, double* %152, i64 %indvars.iv728
  store double %158, double* %arrayidx232, align 8, !tbaa !37
  %159 = bitcast %"struct.std::__detail::_Hash_node"* %__begin4210.sroa.0.0719 to %"struct.std::__detail::_Hash_node"**
  %__begin4210.sroa.0.0 = load %"struct.std::__detail::_Hash_node"*, %"struct.std::__detail::_Hash_node"** %159, align 8, !tbaa !22
  %cmp.i411.not = icmp eq %"struct.std::__detail::_Hash_node"* %__begin4210.sroa.0.0, null
  br i1 %cmp.i411.not, label %for.cond.cleanup220, label %for.body221

invoke.cont241:                                   ; preds = %for.cond.cleanup220
  %160 = bitcast i8* %call242 to i32 (...)***
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [4 x i8*] }, { [4 x i8*] }* @_ZTV6Column, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %160, align 8, !tbaa !50
  %data.i403 = getelementptr inbounds i8, i8* %call242, i64 8
  %161 = bitcast i8* %data.i403 to i8**
  store i8* %call207, i8** %161, align 8, !tbaa !34
  %type.i404 = getelementptr inbounds i8, i8* %call242, i64 24
  %162 = bitcast i8* %type.i404 to i32*
  store i32 3, i32* %162, align 8, !tbaa !32
  %size.i405 = getelementptr inbounds i8, i8* %call242, i64 32
  %163 = bitcast i8* %size.i405 to i64*
  store i64 %conv203, i64* %163, align 8, !tbaa !82
  %types.i349 = getelementptr inbounds %class.Table, %class.Table* %table, i64 0, i32 4
  %164 = load i32*, i32** %types.i349, align 8, !tbaa !61
  %columnSize.i350 = getelementptr inbounds %class.Table, %class.Table* %table, i64 0, i32 7
  %165 = load i32, i32* %columnSize.i350, align 8, !tbaa !83
  %idxprom.i351 = zext i32 %165 to i64
  %arrayidx.i352 = getelementptr inbounds i32, i32* %164, i64 %idxprom.i351
  store i32 3, i32* %arrayidx.i352, align 4, !tbaa !27
  %inc.i353 = add i32 %165, 1
  store i32 %inc.i353, i32* %columnSize.i350, align 8, !tbaa !83
  %_M_finish.i.i354 = getelementptr inbounds %class.Table, %class.Table* %table, i64 0, i32 3, i32 0, i32 0, i32 1
  %166 = load %class.Column**, %class.Column*** %_M_finish.i.i354, align 8, !tbaa !57
  %_M_end_of_storage.i.i355 = getelementptr inbounds %class.Table, %class.Table* %table, i64 0, i32 3, i32 0, i32 0, i32 2
  %167 = load %class.Column**, %class.Column*** %_M_end_of_storage.i.i355, align 8, !tbaa !84
  %cmp.not.i.i356 = icmp eq %class.Column** %166, %167
  br i1 %cmp.not.i.i356, label %if.else.i.i372, label %if.then.i.i358

if.then.i.i358:                                   ; preds = %invoke.cont241
  %168 = bitcast %class.Column** %166 to i8**
  store i8* %call242, i8** %168, align 8, !tbaa !19
  %169 = load %class.Column**, %class.Column*** %_M_finish.i.i354, align 8, !tbaa !57
  %incdec.ptr.i.i357 = getelementptr inbounds %class.Column*, %class.Column** %169, i64 1
  store %class.Column** %incdec.ptr.i.i357, %class.Column*** %_M_finish.i.i354, align 8, !tbaa !57
  br label %sw.epilog251

if.else.i.i372:                                   ; preds = %invoke.cont241
  %_M_start.i27.i.i.i.i359 = getelementptr inbounds %class.Table, %class.Table* %table, i64 0, i32 3, i32 0, i32 0, i32 0
  %170 = load %class.Column**, %class.Column*** %_M_start.i27.i.i.i.i359, align 8, !tbaa !29
  %sub.ptr.lhs.cast.i28.i.i.i.i360 = ptrtoint %class.Column** %166 to i64
  %sub.ptr.rhs.cast.i29.i.i.i.i361 = ptrtoint %class.Column** %170 to i64
  %sub.ptr.sub.i30.i.i.i.i362 = sub i64 %sub.ptr.lhs.cast.i28.i.i.i.i360, %sub.ptr.rhs.cast.i29.i.i.i.i361
  %sub.ptr.div.i31.i.i.i.i363 = ashr exact i64 %sub.ptr.sub.i30.i.i.i.i362, 3
  %cmp.i.i.i.i.i364 = icmp eq i64 %sub.ptr.sub.i30.i.i.i.i362, 0
  %.sroa.speculated.i.i.i.i365 = select i1 %cmp.i.i.i.i.i364, i64 1, i64 %sub.ptr.div.i31.i.i.i.i363
  %add.i.i.i.i366 = add nsw i64 %.sroa.speculated.i.i.i.i365, %sub.ptr.div.i31.i.i.i.i363
  %cmp7.i.i.i.i367 = icmp ult i64 %add.i.i.i.i366, %sub.ptr.div.i31.i.i.i.i363
  %cmp9.i.i.i.i368 = icmp ugt i64 %add.i.i.i.i366, 2305843009213693951
  %or.cond.i.i.i.i369 = or i1 %cmp7.i.i.i.i367, %cmp9.i.i.i.i368
  %cond.i.i.i.i370 = select i1 %or.cond.i.i.i.i369, i64 2305843009213693951, i64 %add.i.i.i.i366
  %cmp.not.i.i.i.i371 = icmp eq i64 %cond.i.i.i.i370, 0
  br i1 %cmp.not.i.i.i.i371, label %invoke.cont.i.i.i383, label %_ZNSt16allocator_traitsISaIP6ColumnEE8allocateERS2_m.exit.i.i.i.i378

_ZNSt16allocator_traitsISaIP6ColumnEE8allocateERS2_m.exit.i.i.i.i378: ; preds = %if.else.i.i372
  %mul.i.i.i.i.i.i373 = shl nuw i64 %cond.i.i.i.i370, 3
  %call2.i.i.i.i.i.i374400 = invoke noalias nonnull i8* @_Znwm(i64 %mul.i.i.i.i.i.i373) #19
          to label %call2.i.i.i.i.i.i374.noexc unwind label %lpad240

call2.i.i.i.i.i.i374.noexc:                       ; preds = %_ZNSt16allocator_traitsISaIP6ColumnEE8allocateERS2_m.exit.i.i.i.i378
  %171 = bitcast i8* %call2.i.i.i.i.i.i374400 to %class.Column**
  %.pre.i.i.i375 = load %class.Column**, %class.Column*** %_M_start.i27.i.i.i.i359, align 8, !tbaa !29
  %.pre83.i.i.i376 = ptrtoint %class.Column** %.pre.i.i.i375 to i64
  %.pre84.i.i.i377 = sub i64 %sub.ptr.lhs.cast.i28.i.i.i.i360, %.pre83.i.i.i376
  br label %invoke.cont.i.i.i383

invoke.cont.i.i.i383:                             ; preds = %call2.i.i.i.i.i.i374.noexc, %if.else.i.i372
  %sub.ptr.sub.i.i.i.i.i.i.i.i74.pre-phi.i.i.i379 = phi i64 [ %.pre84.i.i.i377, %call2.i.i.i.i.i.i374.noexc ], [ %sub.ptr.sub.i30.i.i.i.i362, %if.else.i.i372 ]
  %172 = phi %class.Column** [ %.pre.i.i.i375, %call2.i.i.i.i.i.i374.noexc ], [ %170, %if.else.i.i372 ]
  %cond.i67.i.i.i380 = phi %class.Column** [ %171, %call2.i.i.i.i.i.i374.noexc ], [ null, %if.else.i.i372 ]
  %add.ptr.i.i.i381 = getelementptr inbounds %class.Column*, %class.Column** %cond.i67.i.i.i380, i64 %sub.ptr.div.i31.i.i.i.i363
  %173 = bitcast %class.Column** %add.ptr.i.i.i381 to i8**
  store i8* %call242, i8** %173, align 8, !tbaa !19
  %tobool.not.i.i.i.i.i.i.i.i75.i.i.i382 = icmp eq i64 %sub.ptr.sub.i.i.i.i.i.i.i.i74.pre-phi.i.i.i379, 0
  br i1 %tobool.not.i.i.i.i.i.i.i.i75.i.i.i382, label %invoke.cont10.i.i.i391, label %if.then.i.i.i.i.i.i.i.i76.i.i.i384

if.then.i.i.i.i.i.i.i.i76.i.i.i384:               ; preds = %invoke.cont.i.i.i383
  %174 = bitcast %class.Column** %cond.i67.i.i.i380 to i8*
  %175 = bitcast %class.Column** %172 to i8*
  call void @llvm.memmove.p0i8.p0i8.i64(i8* align 8 %174, i8* align 8 %175, i64 %sub.ptr.sub.i.i.i.i.i.i.i.i74.pre-phi.i.i.i379, i1 false) #20
  br label %invoke.cont10.i.i.i391

invoke.cont10.i.i.i391:                           ; preds = %if.then.i.i.i.i.i.i.i.i76.i.i.i384, %invoke.cont.i.i.i383
  %sub.ptr.div.i.i.i.i.i.i.i.i77.i.i.i385 = ashr exact i64 %sub.ptr.sub.i.i.i.i.i.i.i.i74.pre-phi.i.i.i379, 3
  %add.ptr.i.i.i.i.i.i.i.i78.i.i.i386 = getelementptr inbounds %class.Column*, %class.Column** %cond.i67.i.i.i380, i64 %sub.ptr.div.i.i.i.i.i.i.i.i77.i.i.i385
  %incdec.ptr.i.i.i387 = getelementptr inbounds %class.Column*, %class.Column** %add.ptr.i.i.i.i.i.i.i.i78.i.i.i386, i64 1
  %176 = load %class.Column**, %class.Column*** %_M_finish.i.i354, align 8, !tbaa !57
  %sub.ptr.lhs.cast.i.i.i.i.i.i.i.i.i.i.i388 = ptrtoint %class.Column** %176 to i64
  %sub.ptr.sub.i.i.i.i.i.i.i.i.i.i.i389 = sub i64 %sub.ptr.lhs.cast.i.i.i.i.i.i.i.i.i.i.i388, %sub.ptr.lhs.cast.i28.i.i.i.i360
  %tobool.not.i.i.i.i.i.i.i.i.i.i.i390 = icmp eq i64 %sub.ptr.sub.i.i.i.i.i.i.i.i.i.i.i389, 0
  br i1 %tobool.not.i.i.i.i.i.i.i.i.i.i.i390, label %invoke.cont15.i.i.i394, label %if.then.i.i.i.i.i.i.i.i.i.i.i392

if.then.i.i.i.i.i.i.i.i.i.i.i392:                 ; preds = %invoke.cont10.i.i.i391
  %177 = bitcast %class.Column** %incdec.ptr.i.i.i387 to i8*
  %178 = bitcast %class.Column** %166 to i8*
  call void @llvm.memmove.p0i8.p0i8.i64(i8* nonnull align 8 %177, i8* align 8 %178, i64 %sub.ptr.sub.i.i.i.i.i.i.i.i.i.i.i389, i1 false) #20
  br label %invoke.cont15.i.i.i394

invoke.cont15.i.i.i394:                           ; preds = %if.then.i.i.i.i.i.i.i.i.i.i.i392, %invoke.cont10.i.i.i391
  %tobool.not.i68.i.i.i393 = icmp eq %class.Column** %172, null
  br i1 %tobool.not.i68.i.i.i393, label %_ZNSt6vectorIP6ColumnSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i.i399, label %if.then.i69.i.i.i395

if.then.i69.i.i.i395:                             ; preds = %invoke.cont15.i.i.i394
  %179 = bitcast %class.Column** %172 to i8*
  call void @_ZdlPv(i8* nonnull %179) #20
  br label %_ZNSt6vectorIP6ColumnSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i.i399

_ZNSt6vectorIP6ColumnSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i.i399: ; preds = %if.then.i69.i.i.i395, %invoke.cont15.i.i.i394
  %sub.ptr.div.i.i.i.i.i.i.i.i.i.i.i396 = ashr exact i64 %sub.ptr.sub.i.i.i.i.i.i.i.i.i.i.i389, 3
  %add.ptr.i.i.i.i.i.i.i.i.i.i.i397 = getelementptr inbounds %class.Column*, %class.Column** %incdec.ptr.i.i.i387, i64 %sub.ptr.div.i.i.i.i.i.i.i.i.i.i.i396
  store %class.Column** %cond.i67.i.i.i380, %class.Column*** %_M_start.i27.i.i.i.i359, align 8, !tbaa !29
  store %class.Column** %add.ptr.i.i.i.i.i.i.i.i.i.i.i397, %class.Column*** %_M_finish.i.i354, align 8, !tbaa !57
  %add.ptr39.i.i.i398 = getelementptr inbounds %class.Column*, %class.Column** %cond.i67.i.i.i380, i64 %cond.i.i.i.i370
  store %class.Column** %add.ptr39.i.i.i398, %class.Column*** %_M_end_of_storage.i.i355, align 8, !tbaa !84
  br label %sw.epilog251

lpad240:                                          ; preds = %_ZNSt16allocator_traitsISaIP6ColumnEE8allocateERS2_m.exit.i.i.i.i378, %for.cond.cleanup220
  %180 = landingpad { i8*, i32 }
          cleanup
  br label %ehcleanup252

sw.epilog251:                                     ; preds = %if.then93.sw.epilog251_crit_edge, %_ZNSt6vectorIP6ColumnSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i.i399, %if.then.i.i358, %_ZNSt6vectorIP6ColumnSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i.i463, %if.then.i.i422, %_ZNSt6vectorIP6ColumnSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i.i529, %if.then.i.i488
  %.pre-phi = phi %"struct.std::__detail::_Hash_node"** [ %.pre, %if.then93.sw.epilog251_crit_edge ], [ %153, %_ZNSt6vectorIP6ColumnSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i.i399 ], [ %153, %if.then.i.i358 ], [ %124, %_ZNSt6vectorIP6ColumnSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i.i463 ], [ %124, %if.then.i.i422 ], [ %95, %_ZNSt6vectorIP6ColumnSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i.i529 ], [ %95, %if.then.i.i488 ]
  %181 = load %"struct.std::__detail::_Hash_node"*, %"struct.std::__detail::_Hash_node"** %.pre-phi, align 8, !tbaa !95
  %tobool.not5.i.i.i.i330 = icmp eq %"struct.std::__detail::_Hash_node"* %181, null
  br i1 %tobool.not5.i.i.i.i330, label %_ZNSt10_HashtableImSt4pairIKmSt6vectorI13GroupByColumnSaIS3_EEESaIS6_ENSt8__detail10_Select1stESt8equal_toImESt4hashImENS8_18_Mod_range_hashingENS8_20_Default_ranged_hashENS8_20_Prime_rehash_policyENS8_17_Hashtable_traitsILb0ELb0ELb1EEEE5clearEv.exit.i.i343, label %while.body.i.i.i.i334

while.body.i.i.i.i334:                            ; preds = %sw.epilog251, %_ZNSt8__detail16_Hashtable_allocISaINS_10_Hash_nodeISt4pairIKmSt6vectorI13GroupByColumnSaIS5_EEELb0EEEEE18_M_deallocate_nodeEPS9_.exit.i.i.i.i337
  %__n.addr.06.i.i.i.i331 = phi %"struct.std::__detail::_Hash_node"* [ %183, %_ZNSt8__detail16_Hashtable_allocISaINS_10_Hash_nodeISt4pairIKmSt6vectorI13GroupByColumnSaIS5_EEELb0EEEEE18_M_deallocate_nodeEPS9_.exit.i.i.i.i337 ], [ %181, %sw.epilog251 ]
  %182 = bitcast %"struct.std::__detail::_Hash_node"* %__n.addr.06.i.i.i.i331 to %"struct.std::__detail::_Hash_node"**
  %183 = load %"struct.std::__detail::_Hash_node"*, %"struct.std::__detail::_Hash_node"** %182, align 8, !tbaa !22
  %_M_start.i.i.i.i.i.i.i.i.i.i332 = getelementptr inbounds %"struct.std::__detail::_Hash_node", %"struct.std::__detail::_Hash_node"* %__n.addr.06.i.i.i.i331, i64 0, i32 0, i32 1, i32 0, i32 0, i64 8
  %184 = bitcast i8* %_M_start.i.i.i.i.i.i.i.i.i.i332 to %struct.GroupByColumn**
  %185 = load %struct.GroupByColumn*, %struct.GroupByColumn** %184, align 8, !tbaa !42
  %tobool.not.i.i.i.i.i.i.i.i.i.i.i333 = icmp eq %struct.GroupByColumn* %185, null
  br i1 %tobool.not.i.i.i.i.i.i.i.i.i.i.i333, label %_ZNSt8__detail16_Hashtable_allocISaINS_10_Hash_nodeISt4pairIKmSt6vectorI13GroupByColumnSaIS5_EEELb0EEEEE18_M_deallocate_nodeEPS9_.exit.i.i.i.i337, label %if.then.i.i.i.i.i.i.i.i.i.i.i335

if.then.i.i.i.i.i.i.i.i.i.i.i335:                 ; preds = %while.body.i.i.i.i334
  %186 = bitcast %struct.GroupByColumn* %185 to i8*
  call void @_ZdlPv(i8* nonnull %186) #20
  br label %_ZNSt8__detail16_Hashtable_allocISaINS_10_Hash_nodeISt4pairIKmSt6vectorI13GroupByColumnSaIS5_EEELb0EEEEE18_M_deallocate_nodeEPS9_.exit.i.i.i.i337

_ZNSt8__detail16_Hashtable_allocISaINS_10_Hash_nodeISt4pairIKmSt6vectorI13GroupByColumnSaIS5_EEELb0EEEEE18_M_deallocate_nodeEPS9_.exit.i.i.i.i337: ; preds = %if.then.i.i.i.i.i.i.i.i.i.i.i335, %while.body.i.i.i.i334
  %187 = bitcast %"struct.std::__detail::_Hash_node"* %__n.addr.06.i.i.i.i331 to i8*
  call void @_ZdlPv(i8* nonnull %187) #20
  %tobool.not.i.i.i.i336 = icmp eq %"struct.std::__detail::_Hash_node"* %183, null
  br i1 %tobool.not.i.i.i.i336, label %_ZNSt10_HashtableImSt4pairIKmSt6vectorI13GroupByColumnSaIS3_EEESaIS6_ENSt8__detail10_Select1stESt8equal_toImESt4hashImENS8_18_Mod_range_hashingENS8_20_Default_ranged_hashENS8_20_Prime_rehash_policyENS8_17_Hashtable_traitsILb0ELb0ELb1EEEE5clearEv.exit.i.i343, label %while.body.i.i.i.i334, !llvm.loop !127

_ZNSt10_HashtableImSt4pairIKmSt6vectorI13GroupByColumnSaIS3_EEESaIS6_ENSt8__detail10_Select1stESt8equal_toImESt4hashImENS8_18_Mod_range_hashingENS8_20_Default_ranged_hashENS8_20_Prime_rehash_policyENS8_17_Hashtable_traitsILb0ELb0ELb1EEEE5clearEv.exit.i.i343: ; preds = %_ZNSt8__detail16_Hashtable_allocISaINS_10_Hash_nodeISt4pairIKmSt6vectorI13GroupByColumnSaIS5_EEELb0EEEEE18_M_deallocate_nodeEPS9_.exit.i.i.i.i337, %sw.epilog251
  %188 = bitcast %"class.std::unordered_map"* %rows to i8**
  %189 = load i8*, i8** %188, align 8, !tbaa !21
  %190 = load i64, i64* %_M_bucket_count.i.i, align 8, !tbaa !20
  %mul.i.i.i339 = shl i64 %190, 3
  call void @llvm.memset.p0i8.i64(i8* align 8 %189, i8 0, i64 %mul.i.i.i339, i1 false) #20
  %191 = bitcast %"struct.std::__detail::_Hash_node_base"** %_M_nxt.i.i.i603 to i8*
  call void @llvm.memset.p0i8.i64(i8* nonnull align 8 dereferenceable(16) %191, i8 0, i64 16, i1 false) #20
  %192 = load %"struct.std::__detail::_Hash_node_base"**, %"struct.std::__detail::_Hash_node_base"*** %_M_buckets.i.i, align 8, !tbaa !21
  %cmp.i.i.i.i.i342 = icmp eq %"struct.std::__detail::_Hash_node_base"** %_M_single_bucket.i.i, %192
  br i1 %cmp.i.i.i.i.i342, label %_ZNSt13unordered_mapImSt6vectorI13GroupByColumnSaIS1_EESt4hashImESt8equal_toImESaISt4pairIKmS3_EEED2Ev.exit345, label %if.end.i.i.i.i344

if.end.i.i.i.i344:                                ; preds = %_ZNSt10_HashtableImSt4pairIKmSt6vectorI13GroupByColumnSaIS3_EEESaIS6_ENSt8__detail10_Select1stESt8equal_toImESt4hashImENS8_18_Mod_range_hashingENS8_20_Default_ranged_hashENS8_20_Prime_rehash_policyENS8_17_Hashtable_traitsILb0ELb0ELb1EEEE5clearEv.exit.i.i343
  %193 = bitcast %"struct.std::__detail::_Hash_node_base"** %192 to i8*
  call void @_ZdlPv(i8* %193) #20
  br label %_ZNSt13unordered_mapImSt6vectorI13GroupByColumnSaIS1_EESt4hashImESt8equal_toImESaISt4pairIKmS3_EEED2Ev.exit345

_ZNSt13unordered_mapImSt6vectorI13GroupByColumnSaIS1_EESt4hashImESt8equal_toImESaISt4pairIKmS3_EEED2Ev.exit345: ; preds = %_ZNSt10_HashtableImSt4pairIKmSt6vectorI13GroupByColumnSaIS3_EEESaIS6_ENSt8__detail10_Select1stESt8equal_toImESt4hashImENS8_18_Mod_range_hashingENS8_20_Default_ranged_hashENS8_20_Prime_rehash_policyENS8_17_Hashtable_traitsILb0ELb0ELb1EEEE5clearEv.exit.i.i343, %if.end.i.i.i.i344
  call void @llvm.lifetime.end.p0i8(i64 56, i8* nonnull %82) #20
  br label %if.end255

ehcleanup252:                                     ; preds = %lpad205, %lpad240, %lpad156, %lpad191, %lpad110, %lpad142
  %.pn.pn = phi { i8*, i32 } [ %122, %lpad142 ], [ %96, %lpad110 ], [ %151, %lpad191 ], [ %125, %lpad156 ], [ %180, %lpad240 ], [ %154, %lpad205 ]
  %194 = bitcast %"struct.std::__detail::_Hash_node_base"** %_M_nxt.i.i.i603 to %"struct.std::__detail::_Hash_node"**
  %195 = load %"struct.std::__detail::_Hash_node"*, %"struct.std::__detail::_Hash_node"** %194, align 8, !tbaa !95
  %tobool.not5.i.i.i.i = icmp eq %"struct.std::__detail::_Hash_node"* %195, null
  br i1 %tobool.not5.i.i.i.i, label %_ZNSt10_HashtableImSt4pairIKmSt6vectorI13GroupByColumnSaIS3_EEESaIS6_ENSt8__detail10_Select1stESt8equal_toImESt4hashImENS8_18_Mod_range_hashingENS8_20_Default_ranged_hashENS8_20_Prime_rehash_policyENS8_17_Hashtable_traitsILb0ELb0ELb1EEEE5clearEv.exit.i.i, label %while.body.i.i.i.i

while.body.i.i.i.i:                               ; preds = %ehcleanup252, %_ZNSt8__detail16_Hashtable_allocISaINS_10_Hash_nodeISt4pairIKmSt6vectorI13GroupByColumnSaIS5_EEELb0EEEEE18_M_deallocate_nodeEPS9_.exit.i.i.i.i
  %__n.addr.06.i.i.i.i = phi %"struct.std::__detail::_Hash_node"* [ %197, %_ZNSt8__detail16_Hashtable_allocISaINS_10_Hash_nodeISt4pairIKmSt6vectorI13GroupByColumnSaIS5_EEELb0EEEEE18_M_deallocate_nodeEPS9_.exit.i.i.i.i ], [ %195, %ehcleanup252 ]
  %196 = bitcast %"struct.std::__detail::_Hash_node"* %__n.addr.06.i.i.i.i to %"struct.std::__detail::_Hash_node"**
  %197 = load %"struct.std::__detail::_Hash_node"*, %"struct.std::__detail::_Hash_node"** %196, align 8, !tbaa !22
  %_M_start.i.i.i.i.i.i.i.i.i.i = getelementptr inbounds %"struct.std::__detail::_Hash_node", %"struct.std::__detail::_Hash_node"* %__n.addr.06.i.i.i.i, i64 0, i32 0, i32 1, i32 0, i32 0, i64 8
  %198 = bitcast i8* %_M_start.i.i.i.i.i.i.i.i.i.i to %struct.GroupByColumn**
  %199 = load %struct.GroupByColumn*, %struct.GroupByColumn** %198, align 8, !tbaa !42
  %tobool.not.i.i.i.i.i.i.i.i.i.i.i = icmp eq %struct.GroupByColumn* %199, null
  br i1 %tobool.not.i.i.i.i.i.i.i.i.i.i.i, label %_ZNSt8__detail16_Hashtable_allocISaINS_10_Hash_nodeISt4pairIKmSt6vectorI13GroupByColumnSaIS5_EEELb0EEEEE18_M_deallocate_nodeEPS9_.exit.i.i.i.i, label %if.then.i.i.i.i.i.i.i.i.i.i.i

if.then.i.i.i.i.i.i.i.i.i.i.i:                    ; preds = %while.body.i.i.i.i
  %200 = bitcast %struct.GroupByColumn* %199 to i8*
  call void @_ZdlPv(i8* nonnull %200) #20
  br label %_ZNSt8__detail16_Hashtable_allocISaINS_10_Hash_nodeISt4pairIKmSt6vectorI13GroupByColumnSaIS5_EEELb0EEEEE18_M_deallocate_nodeEPS9_.exit.i.i.i.i

_ZNSt8__detail16_Hashtable_allocISaINS_10_Hash_nodeISt4pairIKmSt6vectorI13GroupByColumnSaIS5_EEELb0EEEEE18_M_deallocate_nodeEPS9_.exit.i.i.i.i: ; preds = %if.then.i.i.i.i.i.i.i.i.i.i.i, %while.body.i.i.i.i
  %201 = bitcast %"struct.std::__detail::_Hash_node"* %__n.addr.06.i.i.i.i to i8*
  call void @_ZdlPv(i8* nonnull %201) #20
  %tobool.not.i.i.i.i = icmp eq %"struct.std::__detail::_Hash_node"* %197, null
  br i1 %tobool.not.i.i.i.i, label %_ZNSt10_HashtableImSt4pairIKmSt6vectorI13GroupByColumnSaIS3_EEESaIS6_ENSt8__detail10_Select1stESt8equal_toImESt4hashImENS8_18_Mod_range_hashingENS8_20_Default_ranged_hashENS8_20_Prime_rehash_policyENS8_17_Hashtable_traitsILb0ELb0ELb1EEEE5clearEv.exit.i.i, label %while.body.i.i.i.i, !llvm.loop !127

_ZNSt10_HashtableImSt4pairIKmSt6vectorI13GroupByColumnSaIS3_EEESaIS6_ENSt8__detail10_Select1stESt8equal_toImESt4hashImENS8_18_Mod_range_hashingENS8_20_Default_ranged_hashENS8_20_Prime_rehash_policyENS8_17_Hashtable_traitsILb0ELb0ELb1EEEE5clearEv.exit.i.i: ; preds = %_ZNSt8__detail16_Hashtable_allocISaINS_10_Hash_nodeISt4pairIKmSt6vectorI13GroupByColumnSaIS5_EEELb0EEEEE18_M_deallocate_nodeEPS9_.exit.i.i.i.i, %ehcleanup252
  %202 = bitcast %"class.std::unordered_map"* %rows to i8**
  %203 = load i8*, i8** %202, align 8, !tbaa !21
  %204 = load i64, i64* %_M_bucket_count.i.i, align 8, !tbaa !20
  %mul.i.i.i = shl i64 %204, 3
  call void @llvm.memset.p0i8.i64(i8* align 8 %203, i8 0, i64 %mul.i.i.i, i1 false) #20
  %205 = bitcast %"struct.std::__detail::_Hash_node_base"** %_M_nxt.i.i.i603 to i8*
  call void @llvm.memset.p0i8.i64(i8* nonnull align 8 dereferenceable(16) %205, i8 0, i64 16, i1 false) #20
  %206 = load %"struct.std::__detail::_Hash_node_base"**, %"struct.std::__detail::_Hash_node_base"*** %_M_buckets.i.i, align 8, !tbaa !21
  %cmp.i.i.i.i.i = icmp eq %"struct.std::__detail::_Hash_node_base"** %_M_single_bucket.i.i, %206
  br i1 %cmp.i.i.i.i.i, label %_ZNSt13unordered_mapImSt6vectorI13GroupByColumnSaIS1_EESt4hashImESt8equal_toImESaISt4pairIKmS3_EEED2Ev.exit, label %if.end.i.i.i.i

if.end.i.i.i.i:                                   ; preds = %_ZNSt10_HashtableImSt4pairIKmSt6vectorI13GroupByColumnSaIS3_EEESaIS6_ENSt8__detail10_Select1stESt8equal_toImESt4hashImENS8_18_Mod_range_hashingENS8_20_Default_ranged_hashENS8_20_Prime_rehash_policyENS8_17_Hashtable_traitsILb0ELb0ELb1EEEE5clearEv.exit.i.i
  %207 = bitcast %"struct.std::__detail::_Hash_node_base"** %206 to i8*
  call void @_ZdlPv(i8* %207) #20
  br label %_ZNSt13unordered_mapImSt6vectorI13GroupByColumnSaIS1_EESt4hashImESt8equal_toImESaISt4pairIKmS3_EEED2Ev.exit

_ZNSt13unordered_mapImSt6vectorI13GroupByColumnSaIS1_EESt4hashImESt8equal_toImESaISt4pairIKmS3_EEED2Ev.exit: ; preds = %_ZNSt10_HashtableImSt4pairIKmSt6vectorI13GroupByColumnSaIS3_EEESaIS6_ENSt8__detail10_Select1stESt8equal_toImESt4hashImENS8_18_Mod_range_hashingENS8_20_Default_ranged_hashENS8_20_Prime_rehash_policyENS8_17_Hashtable_traitsILb0ELb0ELb1EEEE5clearEv.exit.i.i, %if.end.i.i.i.i
  call void @llvm.lifetime.end.p0i8(i64 56, i8* nonnull %82) #20
  resume { i8*, i32 } %.pn.pn

if.end255:                                        ; preds = %_ZNSt6vectorIP6ColumnSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i.i662, %if.then.i.i621, %_ZNSt6vectorIP6ColumnSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i.i585, %if.then.i.i544, %_ZNSt6vectorIP6ColumnSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i.i, %if.then.i.i, %entry, %if.then, %_ZNSt13unordered_mapImSt6vectorI13GroupByColumnSaIS1_EESt4hashImESt8equal_toImESaISt4pairIKmS3_EEED2Ev.exit345
  ret void
}

; Function Attrs: uwtable
define dso_local nonnull %class.HashGroupBy* @_Z17createHashGroupByRSt6vectorI11ColumnIndexSaIS0_EES3_RS_IP10AggregatorSaIS5_EE(%"class.std::vector"* nocapture nonnull readonly align 8 dereferenceable(24) %groupByIndex, %"class.std::vector"* nocapture nonnull readonly align 8 dereferenceable(24) %aggIndex, %"class.std::vector.0"* nocapture nonnull readonly align 8 dereferenceable(24) %aggs) local_unnamed_addr #6 personality i8* bitcast (i32 (...)* @__gxx_personality_v0 to i8*) {
entry:
  %agg.tmp = alloca %"class.std::vector", align 8
  %agg.tmp1 = alloca %"class.std::vector", align 8
  %agg.tmp4 = alloca %"class.std::vector.0", align 8
  %call = tail call noalias nonnull dereferenceable(144) i8* @_Znwm(i64 144) #18
  %0 = bitcast i8* %call to %class.HashGroupBy*
  %_M_finish.i.i = getelementptr inbounds %"class.std::vector", %"class.std::vector"* %groupByIndex, i64 0, i32 0, i32 0, i32 1
  %1 = load %struct.ColumnIndex*, %struct.ColumnIndex** %_M_finish.i.i, align 8, !tbaa !2
  %_M_start.i.i = getelementptr inbounds %"class.std::vector", %"class.std::vector"* %groupByIndex, i64 0, i32 0, i32 0, i32 0
  %2 = load %struct.ColumnIndex*, %struct.ColumnIndex** %_M_start.i.i, align 8, !tbaa !8
  %sub.ptr.lhs.cast.i.i = ptrtoint %struct.ColumnIndex* %1 to i64
  %sub.ptr.rhs.cast.i.i = ptrtoint %struct.ColumnIndex* %2 to i64
  %sub.ptr.sub.i.i = sub i64 %sub.ptr.lhs.cast.i.i, %sub.ptr.rhs.cast.i.i
  %sub.ptr.div.i.i = ashr exact i64 %sub.ptr.sub.i.i, 3
  %3 = bitcast %"class.std::vector"* %agg.tmp to i8*
  call void @llvm.memset.p0i8.i64(i8* nonnull align 8 dereferenceable(24) %3, i8 0, i64 24, i1 false) #20
  %cmp.not.i.i.i.i = icmp eq i64 %sub.ptr.sub.i.i, 0
  br i1 %cmp.not.i.i.i.i, label %invoke.cont.i, label %cond.true.i.i.i.i

cond.true.i.i.i.i:                                ; preds = %entry
  %cmp.i.i.i.i.i.i = icmp slt i64 %sub.ptr.sub.i.i, 0
  br i1 %cmp.i.i.i.i.i.i, label %if.then.i.i.i.i.i.i, label %_ZNSt16allocator_traitsISaI11ColumnIndexEE8allocateERS1_m.exit.i.i.i.i

if.then.i.i.i.i.i.i:                              ; preds = %cond.true.i.i.i.i
  invoke void @_ZSt17__throw_bad_allocv() #22
          to label %.noexc unwind label %lpad

.noexc:                                           ; preds = %if.then.i.i.i.i.i.i
  unreachable

_ZNSt16allocator_traitsISaI11ColumnIndexEE8allocateERS1_m.exit.i.i.i.i: ; preds = %cond.true.i.i.i.i
  %call2.i.i.i.i3.i22.i18 = invoke noalias nonnull i8* @_Znwm(i64 %sub.ptr.sub.i.i) #19
          to label %call2.i.i.i.i3.i22.i.noexc unwind label %lpad

call2.i.i.i.i3.i22.i.noexc:                       ; preds = %_ZNSt16allocator_traitsISaI11ColumnIndexEE8allocateERS1_m.exit.i.i.i.i
  %4 = bitcast i8* %call2.i.i.i.i3.i22.i18 to %struct.ColumnIndex*
  %.pre = load %struct.ColumnIndex*, %struct.ColumnIndex** %_M_start.i.i, align 8, !tbaa !19
  %.pre99 = load %struct.ColumnIndex*, %struct.ColumnIndex** %_M_finish.i.i, align 8, !tbaa !19
  %.pre104 = ptrtoint %struct.ColumnIndex* %.pre99 to i64
  %.pre105 = ptrtoint %struct.ColumnIndex* %.pre to i64
  %.pre106 = sub i64 %.pre104, %.pre105
  br label %invoke.cont.i

invoke.cont.i:                                    ; preds = %call2.i.i.i.i3.i22.i.noexc, %entry
  %sub.ptr.sub.i.i.i.i.i.i.i.i.pre-phi = phi i64 [ %.pre106, %call2.i.i.i.i3.i22.i.noexc ], [ 0, %entry ]
  %5 = phi %struct.ColumnIndex* [ %.pre, %call2.i.i.i.i3.i22.i.noexc ], [ %2, %entry ]
  %cond.i.i.i.i = phi %struct.ColumnIndex* [ %4, %call2.i.i.i.i3.i22.i.noexc ], [ null, %entry ]
  %_M_start.i.i.i = getelementptr inbounds %"class.std::vector", %"class.std::vector"* %agg.tmp, i64 0, i32 0, i32 0, i32 0
  store %struct.ColumnIndex* %cond.i.i.i.i, %struct.ColumnIndex** %_M_start.i.i.i, align 8, !tbaa !8
  %_M_finish.i.i.i = getelementptr inbounds %"class.std::vector", %"class.std::vector"* %agg.tmp, i64 0, i32 0, i32 0, i32 1
  %add.ptr.i.i.i = getelementptr inbounds %struct.ColumnIndex, %struct.ColumnIndex* %cond.i.i.i.i, i64 %sub.ptr.div.i.i
  %_M_end_of_storage.i.i.i = getelementptr inbounds %"class.std::vector", %"class.std::vector"* %agg.tmp, i64 0, i32 0, i32 0, i32 2
  store %struct.ColumnIndex* %add.ptr.i.i.i, %struct.ColumnIndex** %_M_end_of_storage.i.i.i, align 8, !tbaa !128
  %tobool.not.i.i.i.i.i.i.i.i = icmp eq i64 %sub.ptr.sub.i.i.i.i.i.i.i.i.pre-phi, 0
  br i1 %tobool.not.i.i.i.i.i.i.i.i, label %invoke.cont, label %if.then.i.i.i.i.i.i.i.i

if.then.i.i.i.i.i.i.i.i:                          ; preds = %invoke.cont.i
  %6 = bitcast %struct.ColumnIndex* %cond.i.i.i.i to i8*
  %7 = bitcast %struct.ColumnIndex* %5 to i8*
  tail call void @llvm.memmove.p0i8.p0i8.i64(i8* align 4 %6, i8* align 4 %7, i64 %sub.ptr.sub.i.i.i.i.i.i.i.i.pre-phi, i1 false) #20
  br label %invoke.cont

invoke.cont:                                      ; preds = %if.then.i.i.i.i.i.i.i.i, %invoke.cont.i
  %sub.ptr.div.i.i.i.i.i.i.i.i = ashr exact i64 %sub.ptr.sub.i.i.i.i.i.i.i.i.pre-phi, 3
  %add.ptr.i.i.i.i.i.i.i.i = getelementptr inbounds %struct.ColumnIndex, %struct.ColumnIndex* %cond.i.i.i.i, i64 %sub.ptr.div.i.i.i.i.i.i.i.i
  store %struct.ColumnIndex* %add.ptr.i.i.i.i.i.i.i.i, %struct.ColumnIndex** %_M_finish.i.i.i, align 8, !tbaa !2
  %_M_finish.i.i19 = getelementptr inbounds %"class.std::vector", %"class.std::vector"* %aggIndex, i64 0, i32 0, i32 0, i32 1
  %8 = load %struct.ColumnIndex*, %struct.ColumnIndex** %_M_finish.i.i19, align 8, !tbaa !2
  %_M_start.i.i20 = getelementptr inbounds %"class.std::vector", %"class.std::vector"* %aggIndex, i64 0, i32 0, i32 0, i32 0
  %9 = load %struct.ColumnIndex*, %struct.ColumnIndex** %_M_start.i.i20, align 8, !tbaa !8
  %sub.ptr.lhs.cast.i.i21 = ptrtoint %struct.ColumnIndex* %8 to i64
  %sub.ptr.rhs.cast.i.i22 = ptrtoint %struct.ColumnIndex* %9 to i64
  %sub.ptr.sub.i.i23 = sub i64 %sub.ptr.lhs.cast.i.i21, %sub.ptr.rhs.cast.i.i22
  %sub.ptr.div.i.i24 = ashr exact i64 %sub.ptr.sub.i.i23, 3
  %10 = bitcast %"class.std::vector"* %agg.tmp1 to i8*
  call void @llvm.memset.p0i8.i64(i8* nonnull align 8 dereferenceable(24) %10, i8 0, i64 24, i1 false) #20
  %cmp.not.i.i.i.i25 = icmp eq i64 %sub.ptr.sub.i.i23, 0
  br i1 %cmp.not.i.i.i.i25, label %invoke.cont.i39, label %cond.true.i.i.i.i27

cond.true.i.i.i.i27:                              ; preds = %invoke.cont
  %cmp.i.i.i.i.i.i26 = icmp slt i64 %sub.ptr.sub.i.i23, 0
  br i1 %cmp.i.i.i.i.i.i26, label %if.then.i.i.i.i.i.i28, label %_ZNSt16allocator_traitsISaI11ColumnIndexEE8allocateERS1_m.exit.i.i.i.i29

if.then.i.i.i.i.i.i28:                            ; preds = %cond.true.i.i.i.i27
  invoke void @_ZSt17__throw_bad_allocv() #22
          to label %.noexc43 unwind label %lpad2

.noexc43:                                         ; preds = %if.then.i.i.i.i.i.i28
  unreachable

_ZNSt16allocator_traitsISaI11ColumnIndexEE8allocateERS1_m.exit.i.i.i.i29: ; preds = %cond.true.i.i.i.i27
  %call2.i.i.i.i3.i22.i45 = invoke noalias nonnull i8* @_Znwm(i64 %sub.ptr.sub.i.i23) #19
          to label %call2.i.i.i.i3.i22.i.noexc44 unwind label %lpad2

call2.i.i.i.i3.i22.i.noexc44:                     ; preds = %_ZNSt16allocator_traitsISaI11ColumnIndexEE8allocateERS1_m.exit.i.i.i.i29
  %11 = bitcast i8* %call2.i.i.i.i3.i22.i45 to %struct.ColumnIndex*
  %.pre100 = load %struct.ColumnIndex*, %struct.ColumnIndex** %_M_start.i.i20, align 8, !tbaa !19
  %.pre101 = load %struct.ColumnIndex*, %struct.ColumnIndex** %_M_finish.i.i19, align 8, !tbaa !19
  %.pre107 = ptrtoint %struct.ColumnIndex* %.pre101 to i64
  %.pre108 = ptrtoint %struct.ColumnIndex* %.pre100 to i64
  %.pre109 = sub i64 %.pre107, %.pre108
  br label %invoke.cont.i39

invoke.cont.i39:                                  ; preds = %call2.i.i.i.i3.i22.i.noexc44, %invoke.cont
  %sub.ptr.sub.i.i.i.i.i.i.i.i37.pre-phi = phi i64 [ %.pre109, %call2.i.i.i.i3.i22.i.noexc44 ], [ 0, %invoke.cont ]
  %12 = phi %struct.ColumnIndex* [ %.pre100, %call2.i.i.i.i3.i22.i.noexc44 ], [ %9, %invoke.cont ]
  %cond.i.i.i.i30 = phi %struct.ColumnIndex* [ %11, %call2.i.i.i.i3.i22.i.noexc44 ], [ null, %invoke.cont ]
  %_M_start.i.i.i31 = getelementptr inbounds %"class.std::vector", %"class.std::vector"* %agg.tmp1, i64 0, i32 0, i32 0, i32 0
  store %struct.ColumnIndex* %cond.i.i.i.i30, %struct.ColumnIndex** %_M_start.i.i.i31, align 8, !tbaa !8
  %_M_finish.i.i.i32 = getelementptr inbounds %"class.std::vector", %"class.std::vector"* %agg.tmp1, i64 0, i32 0, i32 0, i32 1
  %add.ptr.i.i.i33 = getelementptr inbounds %struct.ColumnIndex, %struct.ColumnIndex* %cond.i.i.i.i30, i64 %sub.ptr.div.i.i24
  %_M_end_of_storage.i.i.i34 = getelementptr inbounds %"class.std::vector", %"class.std::vector"* %agg.tmp1, i64 0, i32 0, i32 0, i32 2
  store %struct.ColumnIndex* %add.ptr.i.i.i33, %struct.ColumnIndex** %_M_end_of_storage.i.i.i34, align 8, !tbaa !128
  %tobool.not.i.i.i.i.i.i.i.i38 = icmp eq i64 %sub.ptr.sub.i.i.i.i.i.i.i.i37.pre-phi, 0
  br i1 %tobool.not.i.i.i.i.i.i.i.i38, label %invoke.cont3, label %if.then.i.i.i.i.i.i.i.i40

if.then.i.i.i.i.i.i.i.i40:                        ; preds = %invoke.cont.i39
  %13 = bitcast %struct.ColumnIndex* %cond.i.i.i.i30 to i8*
  %14 = bitcast %struct.ColumnIndex* %12 to i8*
  tail call void @llvm.memmove.p0i8.p0i8.i64(i8* align 4 %13, i8* align 4 %14, i64 %sub.ptr.sub.i.i.i.i.i.i.i.i37.pre-phi, i1 false) #20
  br label %invoke.cont3

invoke.cont3:                                     ; preds = %if.then.i.i.i.i.i.i.i.i40, %invoke.cont.i39
  %sub.ptr.div.i.i.i.i.i.i.i.i41 = ashr exact i64 %sub.ptr.sub.i.i.i.i.i.i.i.i37.pre-phi, 3
  %add.ptr.i.i.i.i.i.i.i.i42 = getelementptr inbounds %struct.ColumnIndex, %struct.ColumnIndex* %cond.i.i.i.i30, i64 %sub.ptr.div.i.i.i.i.i.i.i.i41
  store %struct.ColumnIndex* %add.ptr.i.i.i.i.i.i.i.i42, %struct.ColumnIndex** %_M_finish.i.i.i32, align 8, !tbaa !2
  %_M_finish.i.i47 = getelementptr inbounds %"class.std::vector.0", %"class.std::vector.0"* %aggs, i64 0, i32 0, i32 0, i32 1
  %15 = load %class.Aggregator**, %class.Aggregator*** %_M_finish.i.i47, align 8, !tbaa !129
  %_M_start.i.i48 = getelementptr inbounds %"class.std::vector.0", %"class.std::vector.0"* %aggs, i64 0, i32 0, i32 0, i32 0
  %16 = load %class.Aggregator**, %class.Aggregator*** %_M_start.i.i48, align 8, !tbaa !47
  %sub.ptr.lhs.cast.i.i49 = ptrtoint %class.Aggregator** %15 to i64
  %sub.ptr.rhs.cast.i.i50 = ptrtoint %class.Aggregator** %16 to i64
  %sub.ptr.sub.i.i51 = sub i64 %sub.ptr.lhs.cast.i.i49, %sub.ptr.rhs.cast.i.i50
  %sub.ptr.div.i.i52 = ashr exact i64 %sub.ptr.sub.i.i51, 3
  %17 = bitcast %"class.std::vector.0"* %agg.tmp4 to i8*
  call void @llvm.memset.p0i8.i64(i8* nonnull align 8 dereferenceable(24) %17, i8 0, i64 24, i1 false) #20
  %cmp.not.i.i.i.i53 = icmp eq i64 %sub.ptr.sub.i.i51, 0
  br i1 %cmp.not.i.i.i.i53, label %invoke.cont.i66, label %cond.true.i.i.i.i55

cond.true.i.i.i.i55:                              ; preds = %invoke.cont3
  %cmp.i.i.i.i.i.i54 = icmp slt i64 %sub.ptr.sub.i.i51, 0
  br i1 %cmp.i.i.i.i.i.i54, label %if.then.i.i.i.i.i.i56, label %_ZNSt16allocator_traitsISaIP10AggregatorEE8allocateERS2_m.exit.i.i.i.i

if.then.i.i.i.i.i.i56:                            ; preds = %cond.true.i.i.i.i55
  invoke void @_ZSt17__throw_bad_allocv() #22
          to label %.noexc70 unwind label %lpad5

.noexc70:                                         ; preds = %if.then.i.i.i.i.i.i56
  unreachable

_ZNSt16allocator_traitsISaIP10AggregatorEE8allocateERS2_m.exit.i.i.i.i: ; preds = %cond.true.i.i.i.i55
  %call2.i.i.i.i3.i22.i72 = invoke noalias nonnull i8* @_Znwm(i64 %sub.ptr.sub.i.i51) #19
          to label %call2.i.i.i.i3.i22.i.noexc71 unwind label %lpad5

call2.i.i.i.i3.i22.i.noexc71:                     ; preds = %_ZNSt16allocator_traitsISaIP10AggregatorEE8allocateERS2_m.exit.i.i.i.i
  %18 = bitcast i8* %call2.i.i.i.i3.i22.i72 to %class.Aggregator**
  %.pre102 = load %class.Aggregator**, %class.Aggregator*** %_M_start.i.i48, align 8, !tbaa !19
  %.pre103 = load %class.Aggregator**, %class.Aggregator*** %_M_finish.i.i47, align 8, !tbaa !19
  %.pre110 = ptrtoint %class.Aggregator** %.pre103 to i64
  %.pre111 = ptrtoint %class.Aggregator** %.pre102 to i64
  %.pre112 = sub i64 %.pre110, %.pre111
  br label %invoke.cont.i66

invoke.cont.i66:                                  ; preds = %call2.i.i.i.i3.i22.i.noexc71, %invoke.cont3
  %sub.ptr.sub.i.i.i.i.i.i.i.i64.pre-phi = phi i64 [ %.pre112, %call2.i.i.i.i3.i22.i.noexc71 ], [ 0, %invoke.cont3 ]
  %19 = phi %class.Aggregator** [ %.pre102, %call2.i.i.i.i3.i22.i.noexc71 ], [ %16, %invoke.cont3 ]
  %cond.i.i.i.i57 = phi %class.Aggregator** [ %18, %call2.i.i.i.i3.i22.i.noexc71 ], [ null, %invoke.cont3 ]
  %_M_start.i.i.i58 = getelementptr inbounds %"class.std::vector.0", %"class.std::vector.0"* %agg.tmp4, i64 0, i32 0, i32 0, i32 0
  store %class.Aggregator** %cond.i.i.i.i57, %class.Aggregator*** %_M_start.i.i.i58, align 8, !tbaa !47
  %_M_finish.i.i.i59 = getelementptr inbounds %"class.std::vector.0", %"class.std::vector.0"* %agg.tmp4, i64 0, i32 0, i32 0, i32 1
  %add.ptr.i.i.i60 = getelementptr inbounds %class.Aggregator*, %class.Aggregator** %cond.i.i.i.i57, i64 %sub.ptr.div.i.i52
  %_M_end_of_storage.i.i.i61 = getelementptr inbounds %"class.std::vector.0", %"class.std::vector.0"* %agg.tmp4, i64 0, i32 0, i32 0, i32 2
  store %class.Aggregator** %add.ptr.i.i.i60, %class.Aggregator*** %_M_end_of_storage.i.i.i61, align 8, !tbaa !130
  %tobool.not.i.i.i.i.i.i.i.i65 = icmp eq i64 %sub.ptr.sub.i.i.i.i.i.i.i.i64.pre-phi, 0
  br i1 %tobool.not.i.i.i.i.i.i.i.i65, label %invoke.cont6, label %if.then.i.i.i.i.i.i.i.i67

if.then.i.i.i.i.i.i.i.i67:                        ; preds = %invoke.cont.i66
  %20 = bitcast %class.Aggregator** %cond.i.i.i.i57 to i8*
  %21 = bitcast %class.Aggregator** %19 to i8*
  tail call void @llvm.memmove.p0i8.p0i8.i64(i8* align 8 %20, i8* align 8 %21, i64 %sub.ptr.sub.i.i.i.i.i.i.i.i64.pre-phi, i1 false) #20
  br label %invoke.cont6

invoke.cont6:                                     ; preds = %if.then.i.i.i.i.i.i.i.i67, %invoke.cont.i66
  %sub.ptr.div.i.i.i.i.i.i.i.i68 = ashr exact i64 %sub.ptr.sub.i.i.i.i.i.i.i.i64.pre-phi, 3
  %add.ptr.i.i.i.i.i.i.i.i69 = getelementptr inbounds %class.Aggregator*, %class.Aggregator** %cond.i.i.i.i57, i64 %sub.ptr.div.i.i.i.i.i.i.i.i68
  store %class.Aggregator** %add.ptr.i.i.i.i.i.i.i.i69, %class.Aggregator*** %_M_finish.i.i.i59, align 8, !tbaa !129
  invoke void @_ZN11HashGroupByC2ESt6vectorI11ColumnIndexSaIS1_EES3_S0_IP10AggregatorSaIS5_EE(%class.HashGroupBy* nonnull dereferenceable(144) %0, %"class.std::vector"* nonnull %agg.tmp, %"class.std::vector"* nonnull %agg.tmp1, %"class.std::vector.0"* nonnull %agg.tmp4)
          to label %invoke.cont8 unwind label %lpad7

invoke.cont8:                                     ; preds = %invoke.cont6
  %22 = load %class.Aggregator**, %class.Aggregator*** %_M_start.i.i.i58, align 8, !tbaa !47
  %tobool.not.i.i.i = icmp eq %class.Aggregator** %22, null
  br i1 %tobool.not.i.i.i, label %_ZNSt6vectorIP10AggregatorSaIS1_EED2Ev.exit, label %if.then.i.i.i

if.then.i.i.i:                                    ; preds = %invoke.cont8
  %23 = bitcast %class.Aggregator** %22 to i8*
  call void @_ZdlPv(i8* nonnull %23) #20
  br label %_ZNSt6vectorIP10AggregatorSaIS1_EED2Ev.exit

_ZNSt6vectorIP10AggregatorSaIS1_EED2Ev.exit:      ; preds = %invoke.cont8, %if.then.i.i.i
  %24 = load %struct.ColumnIndex*, %struct.ColumnIndex** %_M_start.i.i.i31, align 8, !tbaa !8
  %tobool.not.i.i.i76 = icmp eq %struct.ColumnIndex* %24, null
  br i1 %tobool.not.i.i.i76, label %_ZNSt6vectorI11ColumnIndexSaIS0_EED2Ev.exit, label %if.then.i.i.i78

if.then.i.i.i78:                                  ; preds = %_ZNSt6vectorIP10AggregatorSaIS1_EED2Ev.exit
  %25 = bitcast %struct.ColumnIndex* %24 to i8*
  call void @_ZdlPv(i8* nonnull %25) #20
  br label %_ZNSt6vectorI11ColumnIndexSaIS0_EED2Ev.exit

_ZNSt6vectorI11ColumnIndexSaIS0_EED2Ev.exit:      ; preds = %_ZNSt6vectorIP10AggregatorSaIS1_EED2Ev.exit, %if.then.i.i.i78
  %26 = load %struct.ColumnIndex*, %struct.ColumnIndex** %_M_start.i.i.i, align 8, !tbaa !8
  %tobool.not.i.i.i80 = icmp eq %struct.ColumnIndex* %26, null
  br i1 %tobool.not.i.i.i80, label %_ZNSt6vectorI11ColumnIndexSaIS0_EED2Ev.exit83, label %if.then.i.i.i82

if.then.i.i.i82:                                  ; preds = %_ZNSt6vectorI11ColumnIndexSaIS0_EED2Ev.exit
  %27 = bitcast %struct.ColumnIndex* %26 to i8*
  call void @_ZdlPv(i8* nonnull %27) #20
  br label %_ZNSt6vectorI11ColumnIndexSaIS0_EED2Ev.exit83

_ZNSt6vectorI11ColumnIndexSaIS0_EED2Ev.exit83:    ; preds = %_ZNSt6vectorI11ColumnIndexSaIS0_EED2Ev.exit, %if.then.i.i.i82
  ret %class.HashGroupBy* %0

lpad:                                             ; preds = %_ZNSt16allocator_traitsISaI11ColumnIndexEE8allocateERS1_m.exit.i.i.i.i, %if.then.i.i.i.i.i.i
  %28 = landingpad { i8*, i32 }
          cleanup
  br label %cleanup.action

lpad2:                                            ; preds = %_ZNSt16allocator_traitsISaI11ColumnIndexEE8allocateERS1_m.exit.i.i.i.i29, %if.then.i.i.i.i.i.i28
  %29 = landingpad { i8*, i32 }
          cleanup
  br label %ehcleanup9

lpad5:                                            ; preds = %_ZNSt16allocator_traitsISaIP10AggregatorEE8allocateERS2_m.exit.i.i.i.i, %if.then.i.i.i.i.i.i56
  %30 = landingpad { i8*, i32 }
          cleanup
  br label %ehcleanup

lpad7:                                            ; preds = %invoke.cont6
  %31 = landingpad { i8*, i32 }
          cleanup
  %32 = load %class.Aggregator**, %class.Aggregator*** %_M_start.i.i.i58, align 8, !tbaa !47
  %tobool.not.i.i.i85 = icmp eq %class.Aggregator** %32, null
  br i1 %tobool.not.i.i.i85, label %ehcleanup, label %if.then.i.i.i87

if.then.i.i.i87:                                  ; preds = %lpad7
  %33 = bitcast %class.Aggregator** %32 to i8*
  call void @_ZdlPv(i8* nonnull %33) #20
  br label %ehcleanup

ehcleanup:                                        ; preds = %if.then.i.i.i87, %lpad7, %lpad5
  %.pn = phi { i8*, i32 } [ %30, %lpad5 ], [ %31, %lpad7 ], [ %31, %if.then.i.i.i87 ]
  %34 = load %struct.ColumnIndex*, %struct.ColumnIndex** %_M_start.i.i.i31, align 8, !tbaa !8
  %tobool.not.i.i.i90 = icmp eq %struct.ColumnIndex* %34, null
  br i1 %tobool.not.i.i.i90, label %ehcleanup9, label %if.then.i.i.i92

if.then.i.i.i92:                                  ; preds = %ehcleanup
  %35 = bitcast %struct.ColumnIndex* %34 to i8*
  call void @_ZdlPv(i8* nonnull %35) #20
  br label %ehcleanup9

ehcleanup9:                                       ; preds = %if.then.i.i.i92, %ehcleanup, %lpad2
  %.pn.pn = phi { i8*, i32 } [ %29, %lpad2 ], [ %.pn, %ehcleanup ], [ %.pn, %if.then.i.i.i92 ]
  %36 = load %struct.ColumnIndex*, %struct.ColumnIndex** %_M_start.i.i.i, align 8, !tbaa !8
  %tobool.not.i.i.i95 = icmp eq %struct.ColumnIndex* %36, null
  br i1 %tobool.not.i.i.i95, label %cleanup.action, label %if.then.i.i.i97

if.then.i.i.i97:                                  ; preds = %ehcleanup9
  %37 = bitcast %struct.ColumnIndex* %36 to i8*
  call void @_ZdlPv(i8* nonnull %37) #20
  br label %cleanup.action

cleanup.action:                                   ; preds = %if.then.i.i.i97, %ehcleanup9, %lpad
  %.pn.pn.pn = phi { i8*, i32 } [ %28, %lpad ], [ %.pn.pn, %ehcleanup9 ], [ %.pn.pn, %if.then.i.i.i97 ]
  call void @_ZdlPv(i8* nonnull %call) #23
  resume { i8*, i32 } %.pn.pn.pn
}

; Function Attrs: uwtable
define linkonce_odr dso_local void @_ZN11HashGroupByC2ESt6vectorI11ColumnIndexSaIS1_EES3_S0_IP10AggregatorSaIS5_EE(%class.HashGroupBy* nonnull dereferenceable(144) %this, %"class.std::vector"* %groupByCols, %"class.std::vector"* %aggCols, %"class.std::vector.0"* %aggregators) unnamed_addr #6 comdat align 2 personality i8* bitcast (i32 (...)* @__gxx_personality_v0 to i8*) {
entry:
  %0 = getelementptr inbounds %class.HashGroupBy, %class.HashGroupBy* %this, i64 0, i32 0, i32 0
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [9 x i8*] }, { [9 x i8*] }* @_ZTV11HashGroupBy, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %0, align 8, !tbaa !50
  %groupByCols2 = getelementptr inbounds %class.HashGroupBy, %class.HashGroupBy* %this, i64 0, i32 1
  %_M_finish.i.i = getelementptr inbounds %"class.std::vector", %"class.std::vector"* %groupByCols, i64 0, i32 0, i32 0, i32 1
  %1 = load %struct.ColumnIndex*, %struct.ColumnIndex** %_M_finish.i.i, align 8, !tbaa !2
  %_M_start.i.i13 = getelementptr inbounds %"class.std::vector", %"class.std::vector"* %groupByCols, i64 0, i32 0, i32 0, i32 0
  %2 = load %struct.ColumnIndex*, %struct.ColumnIndex** %_M_start.i.i13, align 8, !tbaa !8
  %sub.ptr.lhs.cast.i.i = ptrtoint %struct.ColumnIndex* %1 to i64
  %sub.ptr.rhs.cast.i.i = ptrtoint %struct.ColumnIndex* %2 to i64
  %sub.ptr.sub.i.i = sub i64 %sub.ptr.lhs.cast.i.i, %sub.ptr.rhs.cast.i.i
  %sub.ptr.div.i.i = ashr exact i64 %sub.ptr.sub.i.i, 3
  %3 = bitcast %"class.std::vector"* %groupByCols2 to i8*
  tail call void @llvm.memset.p0i8.i64(i8* nonnull align 8 dereferenceable(24) %3, i8 0, i64 24, i1 false) #20
  %cmp.not.i.i.i.i = icmp eq i64 %sub.ptr.sub.i.i, 0
  br i1 %cmp.not.i.i.i.i, label %invoke.cont.i, label %cond.true.i.i.i.i

cond.true.i.i.i.i:                                ; preds = %entry
  %cmp.i.i.i.i.i.i = icmp slt i64 %sub.ptr.sub.i.i, 0
  br i1 %cmp.i.i.i.i.i.i, label %if.then.i.i.i.i.i.i, label %_ZNSt16allocator_traitsISaI11ColumnIndexEE8allocateERS1_m.exit.i.i.i.i

if.then.i.i.i.i.i.i:                              ; preds = %cond.true.i.i.i.i
  tail call void @_ZSt17__throw_bad_allocv() #22
  unreachable

_ZNSt16allocator_traitsISaI11ColumnIndexEE8allocateERS1_m.exit.i.i.i.i: ; preds = %cond.true.i.i.i.i
  %call2.i.i.i.i3.i22.i14 = tail call noalias nonnull i8* @_Znwm(i64 %sub.ptr.sub.i.i) #19
  %4 = bitcast i8* %call2.i.i.i.i3.i22.i14 to %struct.ColumnIndex*
  br label %invoke.cont.i

invoke.cont.i:                                    ; preds = %_ZNSt16allocator_traitsISaI11ColumnIndexEE8allocateERS1_m.exit.i.i.i.i, %entry
  %cond.i.i.i.i = phi %struct.ColumnIndex* [ %4, %_ZNSt16allocator_traitsISaI11ColumnIndexEE8allocateERS1_m.exit.i.i.i.i ], [ null, %entry ]
  %_M_start.i.i.i = getelementptr inbounds %"class.std::vector", %"class.std::vector"* %groupByCols2, i64 0, i32 0, i32 0, i32 0
  store %struct.ColumnIndex* %cond.i.i.i.i, %struct.ColumnIndex** %_M_start.i.i.i, align 8, !tbaa !8
  %_M_finish.i.i.i = getelementptr inbounds %class.HashGroupBy, %class.HashGroupBy* %this, i64 0, i32 1, i32 0, i32 0, i32 1
  store %struct.ColumnIndex* %cond.i.i.i.i, %struct.ColumnIndex** %_M_finish.i.i.i, align 8, !tbaa !2
  %add.ptr.i.i.i = getelementptr inbounds %struct.ColumnIndex, %struct.ColumnIndex* %cond.i.i.i.i, i64 %sub.ptr.div.i.i
  %_M_end_of_storage.i.i.i = getelementptr inbounds %class.HashGroupBy, %class.HashGroupBy* %this, i64 0, i32 1, i32 0, i32 0, i32 2
  store %struct.ColumnIndex* %add.ptr.i.i.i, %struct.ColumnIndex** %_M_end_of_storage.i.i.i, align 8, !tbaa !128
  %5 = load %struct.ColumnIndex*, %struct.ColumnIndex** %_M_start.i.i13, align 8, !tbaa !19
  %6 = load %struct.ColumnIndex*, %struct.ColumnIndex** %_M_finish.i.i, align 8, !tbaa !19
  %sub.ptr.lhs.cast.i.i.i.i.i.i.i.i = ptrtoint %struct.ColumnIndex* %6 to i64
  %sub.ptr.rhs.cast.i.i.i.i.i.i.i.i = ptrtoint %struct.ColumnIndex* %5 to i64
  %sub.ptr.sub.i.i.i.i.i.i.i.i = sub i64 %sub.ptr.lhs.cast.i.i.i.i.i.i.i.i, %sub.ptr.rhs.cast.i.i.i.i.i.i.i.i
  %tobool.not.i.i.i.i.i.i.i.i = icmp eq i64 %sub.ptr.sub.i.i.i.i.i.i.i.i, 0
  br i1 %tobool.not.i.i.i.i.i.i.i.i, label %invoke.cont, label %if.then.i.i.i.i.i.i.i.i

if.then.i.i.i.i.i.i.i.i:                          ; preds = %invoke.cont.i
  %7 = bitcast %struct.ColumnIndex* %cond.i.i.i.i to i8*
  %8 = bitcast %struct.ColumnIndex* %5 to i8*
  tail call void @llvm.memmove.p0i8.p0i8.i64(i8* align 4 %7, i8* align 4 %8, i64 %sub.ptr.sub.i.i.i.i.i.i.i.i, i1 false) #20
  br label %invoke.cont

invoke.cont:                                      ; preds = %if.then.i.i.i.i.i.i.i.i, %invoke.cont.i
  %sub.ptr.div.i.i.i.i.i.i.i.i = ashr exact i64 %sub.ptr.sub.i.i.i.i.i.i.i.i, 3
  %add.ptr.i.i.i.i.i.i.i.i = getelementptr inbounds %struct.ColumnIndex, %struct.ColumnIndex* %cond.i.i.i.i, i64 %sub.ptr.div.i.i.i.i.i.i.i.i
  store %struct.ColumnIndex* %add.ptr.i.i.i.i.i.i.i.i, %struct.ColumnIndex** %_M_finish.i.i.i, align 8, !tbaa !2
  %aggCols3 = getelementptr inbounds %class.HashGroupBy, %class.HashGroupBy* %this, i64 0, i32 2
  %_M_finish.i.i15 = getelementptr inbounds %"class.std::vector", %"class.std::vector"* %aggCols, i64 0, i32 0, i32 0, i32 1
  %9 = load %struct.ColumnIndex*, %struct.ColumnIndex** %_M_finish.i.i15, align 8, !tbaa !2
  %_M_start.i.i16 = getelementptr inbounds %"class.std::vector", %"class.std::vector"* %aggCols, i64 0, i32 0, i32 0, i32 0
  %10 = load %struct.ColumnIndex*, %struct.ColumnIndex** %_M_start.i.i16, align 8, !tbaa !8
  %sub.ptr.lhs.cast.i.i17 = ptrtoint %struct.ColumnIndex* %9 to i64
  %sub.ptr.rhs.cast.i.i18 = ptrtoint %struct.ColumnIndex* %10 to i64
  %sub.ptr.sub.i.i19 = sub i64 %sub.ptr.lhs.cast.i.i17, %sub.ptr.rhs.cast.i.i18
  %sub.ptr.div.i.i20 = ashr exact i64 %sub.ptr.sub.i.i19, 3
  %11 = bitcast %"class.std::vector"* %aggCols3 to i8*
  tail call void @llvm.memset.p0i8.i64(i8* nonnull align 8 dereferenceable(24) %11, i8 0, i64 24, i1 false) #20
  %cmp.not.i.i.i.i21 = icmp eq i64 %sub.ptr.sub.i.i19, 0
  br i1 %cmp.not.i.i.i.i21, label %invoke.cont.i35, label %cond.true.i.i.i.i23

cond.true.i.i.i.i23:                              ; preds = %invoke.cont
  %cmp.i.i.i.i.i.i22 = icmp slt i64 %sub.ptr.sub.i.i19, 0
  br i1 %cmp.i.i.i.i.i.i22, label %if.then.i.i.i.i.i.i24, label %_ZNSt16allocator_traitsISaI11ColumnIndexEE8allocateERS1_m.exit.i.i.i.i25

if.then.i.i.i.i.i.i24:                            ; preds = %cond.true.i.i.i.i23
  invoke void @_ZSt17__throw_bad_allocv() #22
          to label %.noexc39 unwind label %lpad4

.noexc39:                                         ; preds = %if.then.i.i.i.i.i.i24
  unreachable

_ZNSt16allocator_traitsISaI11ColumnIndexEE8allocateERS1_m.exit.i.i.i.i25: ; preds = %cond.true.i.i.i.i23
  %call2.i.i.i.i3.i22.i41 = invoke noalias nonnull i8* @_Znwm(i64 %sub.ptr.sub.i.i19) #19
          to label %call2.i.i.i.i3.i22.i.noexc40 unwind label %lpad4

call2.i.i.i.i3.i22.i.noexc40:                     ; preds = %_ZNSt16allocator_traitsISaI11ColumnIndexEE8allocateERS1_m.exit.i.i.i.i25
  %12 = bitcast i8* %call2.i.i.i.i3.i22.i41 to %struct.ColumnIndex*
  br label %invoke.cont.i35

invoke.cont.i35:                                  ; preds = %call2.i.i.i.i3.i22.i.noexc40, %invoke.cont
  %cond.i.i.i.i26 = phi %struct.ColumnIndex* [ %12, %call2.i.i.i.i3.i22.i.noexc40 ], [ null, %invoke.cont ]
  %_M_start.i.i.i27 = getelementptr inbounds %"class.std::vector", %"class.std::vector"* %aggCols3, i64 0, i32 0, i32 0, i32 0
  store %struct.ColumnIndex* %cond.i.i.i.i26, %struct.ColumnIndex** %_M_start.i.i.i27, align 8, !tbaa !8
  %_M_finish.i.i.i28 = getelementptr inbounds %class.HashGroupBy, %class.HashGroupBy* %this, i64 0, i32 2, i32 0, i32 0, i32 1
  store %struct.ColumnIndex* %cond.i.i.i.i26, %struct.ColumnIndex** %_M_finish.i.i.i28, align 8, !tbaa !2
  %add.ptr.i.i.i29 = getelementptr inbounds %struct.ColumnIndex, %struct.ColumnIndex* %cond.i.i.i.i26, i64 %sub.ptr.div.i.i20
  %_M_end_of_storage.i.i.i30 = getelementptr inbounds %class.HashGroupBy, %class.HashGroupBy* %this, i64 0, i32 2, i32 0, i32 0, i32 2
  store %struct.ColumnIndex* %add.ptr.i.i.i29, %struct.ColumnIndex** %_M_end_of_storage.i.i.i30, align 8, !tbaa !128
  %13 = load %struct.ColumnIndex*, %struct.ColumnIndex** %_M_start.i.i16, align 8, !tbaa !19
  %14 = load %struct.ColumnIndex*, %struct.ColumnIndex** %_M_finish.i.i15, align 8, !tbaa !19
  %sub.ptr.lhs.cast.i.i.i.i.i.i.i.i31 = ptrtoint %struct.ColumnIndex* %14 to i64
  %sub.ptr.rhs.cast.i.i.i.i.i.i.i.i32 = ptrtoint %struct.ColumnIndex* %13 to i64
  %sub.ptr.sub.i.i.i.i.i.i.i.i33 = sub i64 %sub.ptr.lhs.cast.i.i.i.i.i.i.i.i31, %sub.ptr.rhs.cast.i.i.i.i.i.i.i.i32
  %tobool.not.i.i.i.i.i.i.i.i34 = icmp eq i64 %sub.ptr.sub.i.i.i.i.i.i.i.i33, 0
  br i1 %tobool.not.i.i.i.i.i.i.i.i34, label %invoke.cont5, label %if.then.i.i.i.i.i.i.i.i36

if.then.i.i.i.i.i.i.i.i36:                        ; preds = %invoke.cont.i35
  %15 = bitcast %struct.ColumnIndex* %cond.i.i.i.i26 to i8*
  %16 = bitcast %struct.ColumnIndex* %13 to i8*
  tail call void @llvm.memmove.p0i8.p0i8.i64(i8* align 4 %15, i8* align 4 %16, i64 %sub.ptr.sub.i.i.i.i.i.i.i.i33, i1 false) #20
  br label %invoke.cont5

invoke.cont5:                                     ; preds = %if.then.i.i.i.i.i.i.i.i36, %invoke.cont.i35
  %sub.ptr.div.i.i.i.i.i.i.i.i37 = ashr exact i64 %sub.ptr.sub.i.i.i.i.i.i.i.i33, 3
  %add.ptr.i.i.i.i.i.i.i.i38 = getelementptr inbounds %struct.ColumnIndex, %struct.ColumnIndex* %cond.i.i.i.i26, i64 %sub.ptr.div.i.i.i.i.i.i.i.i37
  store %struct.ColumnIndex* %add.ptr.i.i.i.i.i.i.i.i38, %struct.ColumnIndex** %_M_finish.i.i.i28, align 8, !tbaa !2
  %aggregators6 = getelementptr inbounds %class.HashGroupBy, %class.HashGroupBy* %this, i64 0, i32 3
  %_M_finish.i.i43 = getelementptr inbounds %"class.std::vector.0", %"class.std::vector.0"* %aggregators, i64 0, i32 0, i32 0, i32 1
  %17 = load %class.Aggregator**, %class.Aggregator*** %_M_finish.i.i43, align 8, !tbaa !129
  %_M_start.i.i44 = getelementptr inbounds %"class.std::vector.0", %"class.std::vector.0"* %aggregators, i64 0, i32 0, i32 0, i32 0
  %18 = load %class.Aggregator**, %class.Aggregator*** %_M_start.i.i44, align 8, !tbaa !47
  %sub.ptr.lhs.cast.i.i45 = ptrtoint %class.Aggregator** %17 to i64
  %sub.ptr.rhs.cast.i.i46 = ptrtoint %class.Aggregator** %18 to i64
  %sub.ptr.sub.i.i47 = sub i64 %sub.ptr.lhs.cast.i.i45, %sub.ptr.rhs.cast.i.i46
  %sub.ptr.div.i.i48 = ashr exact i64 %sub.ptr.sub.i.i47, 3
  %19 = bitcast %"class.std::vector.0"* %aggregators6 to i8*
  tail call void @llvm.memset.p0i8.i64(i8* nonnull align 8 dereferenceable(24) %19, i8 0, i64 24, i1 false) #20
  %cmp.not.i.i.i.i49 = icmp eq i64 %sub.ptr.sub.i.i47, 0
  br i1 %cmp.not.i.i.i.i49, label %invoke.cont.i62, label %cond.true.i.i.i.i51

cond.true.i.i.i.i51:                              ; preds = %invoke.cont5
  %cmp.i.i.i.i.i.i50 = icmp slt i64 %sub.ptr.sub.i.i47, 0
  br i1 %cmp.i.i.i.i.i.i50, label %if.then.i.i.i.i.i.i52, label %_ZNSt16allocator_traitsISaIP10AggregatorEE8allocateERS2_m.exit.i.i.i.i

if.then.i.i.i.i.i.i52:                            ; preds = %cond.true.i.i.i.i51
  invoke void @_ZSt17__throw_bad_allocv() #22
          to label %.noexc66 unwind label %lpad7

.noexc66:                                         ; preds = %if.then.i.i.i.i.i.i52
  unreachable

_ZNSt16allocator_traitsISaIP10AggregatorEE8allocateERS2_m.exit.i.i.i.i: ; preds = %cond.true.i.i.i.i51
  %call2.i.i.i.i3.i22.i68 = invoke noalias nonnull i8* @_Znwm(i64 %sub.ptr.sub.i.i47) #19
          to label %call2.i.i.i.i3.i22.i.noexc67 unwind label %lpad7

call2.i.i.i.i3.i22.i.noexc67:                     ; preds = %_ZNSt16allocator_traitsISaIP10AggregatorEE8allocateERS2_m.exit.i.i.i.i
  %20 = bitcast i8* %call2.i.i.i.i3.i22.i68 to %class.Aggregator**
  br label %invoke.cont.i62

invoke.cont.i62:                                  ; preds = %call2.i.i.i.i3.i22.i.noexc67, %invoke.cont5
  %cond.i.i.i.i53 = phi %class.Aggregator** [ %20, %call2.i.i.i.i3.i22.i.noexc67 ], [ null, %invoke.cont5 ]
  %_M_start.i.i.i54 = getelementptr inbounds %"class.std::vector.0", %"class.std::vector.0"* %aggregators6, i64 0, i32 0, i32 0, i32 0
  store %class.Aggregator** %cond.i.i.i.i53, %class.Aggregator*** %_M_start.i.i.i54, align 8, !tbaa !47
  %_M_finish.i.i.i55 = getelementptr inbounds %class.HashGroupBy, %class.HashGroupBy* %this, i64 0, i32 3, i32 0, i32 0, i32 1
  store %class.Aggregator** %cond.i.i.i.i53, %class.Aggregator*** %_M_finish.i.i.i55, align 8, !tbaa !129
  %add.ptr.i.i.i56 = getelementptr inbounds %class.Aggregator*, %class.Aggregator** %cond.i.i.i.i53, i64 %sub.ptr.div.i.i48
  %_M_end_of_storage.i.i.i57 = getelementptr inbounds %class.HashGroupBy, %class.HashGroupBy* %this, i64 0, i32 3, i32 0, i32 0, i32 2
  store %class.Aggregator** %add.ptr.i.i.i56, %class.Aggregator*** %_M_end_of_storage.i.i.i57, align 8, !tbaa !130
  %21 = load %class.Aggregator**, %class.Aggregator*** %_M_start.i.i44, align 8, !tbaa !19
  %22 = load %class.Aggregator**, %class.Aggregator*** %_M_finish.i.i43, align 8, !tbaa !19
  %sub.ptr.lhs.cast.i.i.i.i.i.i.i.i58 = ptrtoint %class.Aggregator** %22 to i64
  %sub.ptr.rhs.cast.i.i.i.i.i.i.i.i59 = ptrtoint %class.Aggregator** %21 to i64
  %sub.ptr.sub.i.i.i.i.i.i.i.i60 = sub i64 %sub.ptr.lhs.cast.i.i.i.i.i.i.i.i58, %sub.ptr.rhs.cast.i.i.i.i.i.i.i.i59
  %tobool.not.i.i.i.i.i.i.i.i61 = icmp eq i64 %sub.ptr.sub.i.i.i.i.i.i.i.i60, 0
  br i1 %tobool.not.i.i.i.i.i.i.i.i61, label %invoke.cont8, label %if.then.i.i.i.i.i.i.i.i63

if.then.i.i.i.i.i.i.i.i63:                        ; preds = %invoke.cont.i62
  %23 = bitcast %class.Aggregator** %cond.i.i.i.i53 to i8*
  %24 = bitcast %class.Aggregator** %21 to i8*
  tail call void @llvm.memmove.p0i8.p0i8.i64(i8* align 8 %23, i8* align 8 %24, i64 %sub.ptr.sub.i.i.i.i.i.i.i.i60, i1 false) #20
  br label %invoke.cont8

invoke.cont8:                                     ; preds = %if.then.i.i.i.i.i.i.i.i63, %invoke.cont.i62
  %sub.ptr.div.i.i.i.i.i.i.i.i64 = ashr exact i64 %sub.ptr.sub.i.i.i.i.i.i.i.i60, 3
  %add.ptr.i.i.i.i.i.i.i.i65 = getelementptr inbounds %class.Aggregator*, %class.Aggregator** %cond.i.i.i.i53, i64 %sub.ptr.div.i.i.i.i.i.i.i.i64
  store %class.Aggregator** %add.ptr.i.i.i.i.i.i.i.i65, %class.Aggregator*** %_M_finish.i.i.i55, align 8, !tbaa !129
  %_M_buckets.i.i = getelementptr inbounds %class.HashGroupBy, %class.HashGroupBy* %this, i64 0, i32 4, i32 0, i32 0
  %_M_single_bucket.i.i = getelementptr inbounds %class.HashGroupBy, %class.HashGroupBy* %this, i64 0, i32 4, i32 0, i32 5
  store %"struct.std::__detail::_Hash_node_base"** %_M_single_bucket.i.i, %"struct.std::__detail::_Hash_node_base"*** %_M_buckets.i.i, align 8, !tbaa !21
  %_M_bucket_count.i.i = getelementptr inbounds %class.HashGroupBy, %class.HashGroupBy* %this, i64 0, i32 4, i32 0, i32 1
  store i64 1, i64* %_M_bucket_count.i.i, align 8, !tbaa !20
  %_M_nxt.i.i.i = getelementptr inbounds %class.HashGroupBy, %class.HashGroupBy* %this, i64 0, i32 4, i32 0, i32 2, i32 0
  %_M_max_load_factor.i.i.i = getelementptr inbounds %class.HashGroupBy, %class.HashGroupBy* %this, i64 0, i32 4, i32 0, i32 4, i32 0
  %25 = bitcast %"struct.std::__detail::_Hash_node_base"** %_M_nxt.i.i.i to i8*
  tail call void @llvm.memset.p0i8.i64(i8* nonnull align 8 dereferenceable(16) %25, i8 0, i64 16, i1 false) #20
  store float 1.000000e+00, float* %_M_max_load_factor.i.i.i, align 8, !tbaa !131
  %_M_next_resize.i.i.i = getelementptr inbounds %class.HashGroupBy, %class.HashGroupBy* %this, i64 0, i32 4, i32 0, i32 4, i32 1
  %26 = bitcast i64* %_M_next_resize.i.i.i to i8*
  tail call void @llvm.memset.p0i8.i64(i8* nonnull align 8 dereferenceable(16) %26, i8 0, i64 16, i1 false) #20
  ret void

lpad4:                                            ; preds = %_ZNSt16allocator_traitsISaI11ColumnIndexEE8allocateERS1_m.exit.i.i.i.i25, %if.then.i.i.i.i.i.i24
  %27 = landingpad { i8*, i32 }
          cleanup
  br label %ehcleanup

lpad7:                                            ; preds = %_ZNSt16allocator_traitsISaIP10AggregatorEE8allocateERS2_m.exit.i.i.i.i, %if.then.i.i.i.i.i.i52
  %28 = landingpad { i8*, i32 }
          cleanup
  %29 = load %struct.ColumnIndex*, %struct.ColumnIndex** %_M_start.i.i.i27, align 8, !tbaa !8
  %tobool.not.i.i.i70 = icmp eq %struct.ColumnIndex* %29, null
  br i1 %tobool.not.i.i.i70, label %ehcleanup, label %if.then.i.i.i72

if.then.i.i.i72:                                  ; preds = %lpad7
  %30 = bitcast %struct.ColumnIndex* %29 to i8*
  tail call void @_ZdlPv(i8* nonnull %30) #20
  br label %ehcleanup

ehcleanup:                                        ; preds = %if.then.i.i.i72, %lpad7, %lpad4
  %.pn = phi { i8*, i32 } [ %27, %lpad4 ], [ %28, %lpad7 ], [ %28, %if.then.i.i.i72 ]
  %31 = load %struct.ColumnIndex*, %struct.ColumnIndex** %_M_start.i.i.i, align 8, !tbaa !8
  %tobool.not.i.i.i = icmp eq %struct.ColumnIndex* %31, null
  br i1 %tobool.not.i.i.i, label %ehcleanup9, label %if.then.i.i.i

if.then.i.i.i:                                    ; preds = %ehcleanup
  %32 = bitcast %struct.ColumnIndex* %31 to i8*
  tail call void @_ZdlPv(i8* nonnull %32) #20
  br label %ehcleanup9

ehcleanup9:                                       ; preds = %if.then.i.i.i, %ehcleanup
  resume { i8*, i32 } %.pn
}

; Function Attrs: nounwind uwtable willreturn mustprogress
define linkonce_odr dso_local %class.Table* @_ZN11HashGroupBy9getResultEv(%class.HashGroupBy* nonnull dereferenceable(144) %this) unnamed_addr #13 comdat align 2 {
entry:
  unreachable
}

; Function Attrs: nounwind uwtable
define linkonce_odr dso_local void @_ZN11HashGroupByD2Ev(%class.HashGroupBy* nonnull dereferenceable(144) %this) unnamed_addr #14 comdat align 2 personality i32 (...)* @__gxx_personality_v0 {
entry:
  %0 = getelementptr inbounds %class.HashGroupBy, %class.HashGroupBy* %this, i64 0, i32 0, i32 0
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [9 x i8*] }, { [9 x i8*] }* @_ZTV11HashGroupBy, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %0, align 8, !tbaa !50
  %groupedRows = getelementptr inbounds %class.HashGroupBy, %class.HashGroupBy* %this, i64 0, i32 4
  %_M_nxt.i.i.i = getelementptr inbounds %class.HashGroupBy, %class.HashGroupBy* %this, i64 0, i32 4, i32 0, i32 2, i32 0
  %1 = bitcast %"struct.std::__detail::_Hash_node_base"** %_M_nxt.i.i.i to %"struct.std::__detail::_Hash_node"**
  %__begin1.sroa.0.0103 = load %"struct.std::__detail::_Hash_node"*, %"struct.std::__detail::_Hash_node"** %1, align 8, !tbaa !22
  %cmp.i.not104 = icmp eq %"struct.std::__detail::_Hash_node"* %__begin1.sroa.0.0103, null
  br i1 %cmp.i.not104, label %_ZNSt13unordered_mapImSt6vectorI13GroupByColumnSaIS1_EESt4hashImESt8equal_toImESaISt4pairIKmS3_EEE5clearEv.exit, label %for.body

for.cond.cleanup:                                 ; preds = %for.cond.cleanup14
  %.pre = load %"struct.std::__detail::_Hash_node"*, %"struct.std::__detail::_Hash_node"** %1, align 8, !tbaa !95
  %tobool.not5.i.i.i = icmp eq %"struct.std::__detail::_Hash_node"* %.pre, null
  br i1 %tobool.not5.i.i.i, label %_ZNSt13unordered_mapImSt6vectorI13GroupByColumnSaIS1_EESt4hashImESt8equal_toImESaISt4pairIKmS3_EEE5clearEv.exit, label %while.body.i.i.i

while.body.i.i.i:                                 ; preds = %for.cond.cleanup, %_ZNSt8__detail16_Hashtable_allocISaINS_10_Hash_nodeISt4pairIKmSt6vectorI13GroupByColumnSaIS5_EEELb0EEEEE18_M_deallocate_nodeEPS9_.exit.i.i.i
  %__n.addr.06.i.i.i = phi %"struct.std::__detail::_Hash_node"* [ %3, %_ZNSt8__detail16_Hashtable_allocISaINS_10_Hash_nodeISt4pairIKmSt6vectorI13GroupByColumnSaIS5_EEELb0EEEEE18_M_deallocate_nodeEPS9_.exit.i.i.i ], [ %.pre, %for.cond.cleanup ]
  %2 = bitcast %"struct.std::__detail::_Hash_node"* %__n.addr.06.i.i.i to %"struct.std::__detail::_Hash_node"**
  %3 = load %"struct.std::__detail::_Hash_node"*, %"struct.std::__detail::_Hash_node"** %2, align 8, !tbaa !22
  %_M_start.i.i.i.i.i.i.i.i.i = getelementptr inbounds %"struct.std::__detail::_Hash_node", %"struct.std::__detail::_Hash_node"* %__n.addr.06.i.i.i, i64 0, i32 0, i32 1, i32 0, i32 0, i64 8
  %4 = bitcast i8* %_M_start.i.i.i.i.i.i.i.i.i to %struct.GroupByColumn**
  %5 = load %struct.GroupByColumn*, %struct.GroupByColumn** %4, align 8, !tbaa !42
  %tobool.not.i.i.i.i.i.i.i.i.i.i = icmp eq %struct.GroupByColumn* %5, null
  br i1 %tobool.not.i.i.i.i.i.i.i.i.i.i, label %_ZNSt8__detail16_Hashtable_allocISaINS_10_Hash_nodeISt4pairIKmSt6vectorI13GroupByColumnSaIS5_EEELb0EEEEE18_M_deallocate_nodeEPS9_.exit.i.i.i, label %if.then.i.i.i.i.i.i.i.i.i.i

if.then.i.i.i.i.i.i.i.i.i.i:                      ; preds = %while.body.i.i.i
  %6 = bitcast %struct.GroupByColumn* %5 to i8*
  tail call void @_ZdlPv(i8* nonnull %6) #20
  br label %_ZNSt8__detail16_Hashtable_allocISaINS_10_Hash_nodeISt4pairIKmSt6vectorI13GroupByColumnSaIS5_EEELb0EEEEE18_M_deallocate_nodeEPS9_.exit.i.i.i

_ZNSt8__detail16_Hashtable_allocISaINS_10_Hash_nodeISt4pairIKmSt6vectorI13GroupByColumnSaIS5_EEELb0EEEEE18_M_deallocate_nodeEPS9_.exit.i.i.i: ; preds = %if.then.i.i.i.i.i.i.i.i.i.i, %while.body.i.i.i
  %7 = bitcast %"struct.std::__detail::_Hash_node"* %__n.addr.06.i.i.i to i8*
  tail call void @_ZdlPv(i8* nonnull %7) #20
  %tobool.not.i.i.i76 = icmp eq %"struct.std::__detail::_Hash_node"* %3, null
  br i1 %tobool.not.i.i.i76, label %_ZNSt13unordered_mapImSt6vectorI13GroupByColumnSaIS1_EESt4hashImESt8equal_toImESaISt4pairIKmS3_EEE5clearEv.exit, label %while.body.i.i.i, !llvm.loop !127

_ZNSt13unordered_mapImSt6vectorI13GroupByColumnSaIS1_EESt4hashImESt8equal_toImESaISt4pairIKmS3_EEE5clearEv.exit: ; preds = %_ZNSt8__detail16_Hashtable_allocISaINS_10_Hash_nodeISt4pairIKmSt6vectorI13GroupByColumnSaIS5_EEELb0EEEEE18_M_deallocate_nodeEPS9_.exit.i.i.i, %entry, %for.cond.cleanup
  %8 = bitcast %"class.std::unordered_map"* %groupedRows to i8**
  %9 = load i8*, i8** %8, align 8, !tbaa !21
  %_M_bucket_count.i.i = getelementptr inbounds %class.HashGroupBy, %class.HashGroupBy* %this, i64 0, i32 4, i32 0, i32 1
  %10 = load i64, i64* %_M_bucket_count.i.i, align 8, !tbaa !20
  %mul.i.i = shl i64 %10, 3
  tail call void @llvm.memset.p0i8.i64(i8* align 8 %9, i8 0, i64 %mul.i.i, i1 false) #20
  %11 = bitcast %"struct.std::__detail::_Hash_node_base"** %_M_nxt.i.i.i to i8*
  tail call void @llvm.memset.p0i8.i64(i8* nonnull align 8 dereferenceable(16) %11, i8 0, i64 16, i1 false) #20
  %_M_start.i = getelementptr inbounds %class.HashGroupBy, %class.HashGroupBy* %this, i64 0, i32 3, i32 0, i32 0, i32 0
  %12 = load %class.Aggregator**, %class.Aggregator*** %_M_start.i, align 8, !tbaa !19
  %_M_finish.i = getelementptr inbounds %class.HashGroupBy, %class.HashGroupBy* %this, i64 0, i32 3, i32 0, i32 0, i32 1
  %13 = load %class.Aggregator**, %class.Aggregator*** %_M_finish.i, align 8, !tbaa !19
  %cmp.i83.not99 = icmp eq %class.Aggregator** %12, %13
  br i1 %cmp.i83.not99, label %for.cond.cleanup41, label %for.body42

for.body:                                         ; preds = %entry, %for.cond.cleanup14
  %__begin1.sroa.0.0105 = phi %"struct.std::__detail::_Hash_node"* [ %__begin1.sroa.0.0, %for.cond.cleanup14 ], [ %__begin1.sroa.0.0103, %entry ]
  %second = getelementptr inbounds %"struct.std::__detail::_Hash_node", %"struct.std::__detail::_Hash_node"* %__begin1.sroa.0.0105, i64 0, i32 0, i32 1, i32 0, i32 0, i64 8
  %_M_start.i77 = bitcast i8* %second to %struct.GroupByColumn**
  %14 = load %struct.GroupByColumn*, %struct.GroupByColumn** %_M_start.i77, align 8, !tbaa !19
  %_M_finish.i78 = getelementptr inbounds %"struct.std::__detail::_Hash_node", %"struct.std::__detail::_Hash_node"* %__begin1.sroa.0.0105, i64 0, i32 0, i32 1, i32 0, i32 0, i64 16
  %15 = bitcast i8* %_M_finish.i78 to %struct.GroupByColumn**
  %16 = load %struct.GroupByColumn*, %struct.GroupByColumn** %15, align 8, !tbaa !19
  %cmp.i79.not101 = icmp eq %struct.GroupByColumn* %14, %16
  br i1 %cmp.i79.not101, label %for.cond.cleanup14, label %for.body15

for.cond.cleanup14:                               ; preds = %sw.epilog, %for.body
  %17 = bitcast %"struct.std::__detail::_Hash_node"* %__begin1.sroa.0.0105 to %"struct.std::__detail::_Hash_node"**
  %__begin1.sroa.0.0 = load %"struct.std::__detail::_Hash_node"*, %"struct.std::__detail::_Hash_node"** %17, align 8, !tbaa !22
  %cmp.i.not = icmp eq %"struct.std::__detail::_Hash_node"* %__begin1.sroa.0.0, null
  br i1 %cmp.i.not, label %for.cond.cleanup, label %for.body

for.body15:                                       ; preds = %for.body, %sw.epilog
  %__begin2.sroa.0.0102 = phi %struct.GroupByColumn* [ %incdec.ptr.i85, %sw.epilog ], [ %14, %for.body ]
  %type = getelementptr inbounds %struct.GroupByColumn, %struct.GroupByColumn* %__begin2.sroa.0.0102, i64 0, i32 0
  %18 = load i32, i32* %type, align 8, !tbaa !132
  switch i32 %18, label %sw.epilog [
    i32 1, label %sw.bb
    i32 2, label %sw.bb17
    i32 3, label %sw.bb22
  ]

sw.bb:                                            ; preds = %for.body15
  %val = getelementptr inbounds %struct.GroupByColumn, %struct.GroupByColumn* %__begin2.sroa.0.0102, i64 0, i32 1
  %19 = load i8*, i8** %val, align 8, !tbaa !78
  %isnull = icmp eq i8* %19, null
  br i1 %isnull, label %sw.epilog, label %delete.notnull

delete.notnull:                                   ; preds = %sw.bb
  tail call void @_ZdlPv(i8* nonnull %19) #23
  br label %sw.epilog

sw.bb17:                                          ; preds = %for.body15
  %val18 = getelementptr inbounds %struct.GroupByColumn, %struct.GroupByColumn* %__begin2.sroa.0.0102, i64 0, i32 1
  %20 = load i8*, i8** %val18, align 8, !tbaa !78
  %isnull19 = icmp eq i8* %20, null
  br i1 %isnull19, label %sw.epilog, label %delete.notnull20

delete.notnull20:                                 ; preds = %sw.bb17
  tail call void @_ZdlPv(i8* nonnull %20) #23
  br label %sw.epilog

sw.bb22:                                          ; preds = %for.body15
  %val23 = getelementptr inbounds %struct.GroupByColumn, %struct.GroupByColumn* %__begin2.sroa.0.0102, i64 0, i32 1
  %21 = load i8*, i8** %val23, align 8, !tbaa !78
  %isnull24 = icmp eq i8* %21, null
  br i1 %isnull24, label %sw.epilog, label %delete.notnull25

delete.notnull25:                                 ; preds = %sw.bb22
  tail call void @_ZdlPv(i8* nonnull %21) #23
  br label %sw.epilog

sw.epilog:                                        ; preds = %for.body15, %sw.bb22, %delete.notnull25, %sw.bb17, %delete.notnull20, %sw.bb, %delete.notnull
  %incdec.ptr.i85 = getelementptr inbounds %struct.GroupByColumn, %struct.GroupByColumn* %__begin2.sroa.0.0102, i64 1
  %cmp.i79.not = icmp eq %struct.GroupByColumn* %incdec.ptr.i85, %16
  br i1 %cmp.i79.not, label %for.cond.cleanup14, label %for.body15

for.cond.cleanup41:                               ; preds = %delete.end46, %_ZNSt13unordered_mapImSt6vectorI13GroupByColumnSaIS1_EESt4hashImESt8equal_toImESaISt4pairIKmS3_EEE5clearEv.exit
  %inputColTypes = getelementptr inbounds %class.HashGroupBy, %class.HashGroupBy* %this, i64 0, i32 5
  %22 = load i32*, i32** %inputColTypes, align 8, !tbaa !9
  %isnull50 = icmp eq i32* %22, null
  br i1 %isnull50, label %delete.end52, label %delete.notnull51

for.body42:                                       ; preds = %_ZNSt13unordered_mapImSt6vectorI13GroupByColumnSaIS1_EESt4hashImESt8equal_toImESaISt4pairIKmS3_EEE5clearEv.exit, %delete.end46
  %__begin133.sroa.0.0100 = phi %class.Aggregator** [ %incdec.ptr.i, %delete.end46 ], [ %12, %_ZNSt13unordered_mapImSt6vectorI13GroupByColumnSaIS1_EESt4hashImESt8equal_toImESaISt4pairIKmS3_EEE5clearEv.exit ]
  %23 = load %class.Aggregator*, %class.Aggregator** %__begin133.sroa.0.0100, align 8, !tbaa !19
  %isnull44 = icmp eq %class.Aggregator* %23, null
  br i1 %isnull44, label %delete.end46, label %delete.notnull45

delete.notnull45:                                 ; preds = %for.body42
  %24 = bitcast %class.Aggregator* %23 to void (%class.Aggregator*)***
  %vtable = load void (%class.Aggregator*)**, void (%class.Aggregator*)*** %24, align 8, !tbaa !50
  %vfn = getelementptr inbounds void (%class.Aggregator*)*, void (%class.Aggregator*)** %vtable, i64 1
  %25 = load void (%class.Aggregator*)*, void (%class.Aggregator*)** %vfn, align 8
  tail call void %25(%class.Aggregator* nonnull dereferenceable(72) %23) #20
  br label %delete.end46

delete.end46:                                     ; preds = %delete.notnull45, %for.body42
  %incdec.ptr.i = getelementptr inbounds %class.Aggregator*, %class.Aggregator** %__begin133.sroa.0.0100, i64 1
  %cmp.i83.not = icmp eq %class.Aggregator** %incdec.ptr.i, %13
  br i1 %cmp.i83.not, label %for.cond.cleanup41, label %for.body42

delete.notnull51:                                 ; preds = %for.cond.cleanup41
  %26 = bitcast i32* %22 to i8*
  tail call void @_ZdaPv(i8* %26) #23
  br label %delete.end52

delete.end52:                                     ; preds = %delete.notnull51, %for.cond.cleanup41
  %27 = load %"struct.std::__detail::_Hash_node"*, %"struct.std::__detail::_Hash_node"** %1, align 8, !tbaa !95
  %tobool.not5.i.i.i.i = icmp eq %"struct.std::__detail::_Hash_node"* %27, null
  br i1 %tobool.not5.i.i.i.i, label %_ZNSt10_HashtableImSt4pairIKmSt6vectorI13GroupByColumnSaIS3_EEESaIS6_ENSt8__detail10_Select1stESt8equal_toImESt4hashImENS8_18_Mod_range_hashingENS8_20_Default_ranged_hashENS8_20_Prime_rehash_policyENS8_17_Hashtable_traitsILb0ELb0ELb1EEEE5clearEv.exit.i.i, label %while.body.i.i.i.i

while.body.i.i.i.i:                               ; preds = %delete.end52, %_ZNSt8__detail16_Hashtable_allocISaINS_10_Hash_nodeISt4pairIKmSt6vectorI13GroupByColumnSaIS5_EEELb0EEEEE18_M_deallocate_nodeEPS9_.exit.i.i.i.i
  %__n.addr.06.i.i.i.i = phi %"struct.std::__detail::_Hash_node"* [ %29, %_ZNSt8__detail16_Hashtable_allocISaINS_10_Hash_nodeISt4pairIKmSt6vectorI13GroupByColumnSaIS5_EEELb0EEEEE18_M_deallocate_nodeEPS9_.exit.i.i.i.i ], [ %27, %delete.end52 ]
  %28 = bitcast %"struct.std::__detail::_Hash_node"* %__n.addr.06.i.i.i.i to %"struct.std::__detail::_Hash_node"**
  %29 = load %"struct.std::__detail::_Hash_node"*, %"struct.std::__detail::_Hash_node"** %28, align 8, !tbaa !22
  %_M_start.i.i.i.i.i.i.i.i.i.i = getelementptr inbounds %"struct.std::__detail::_Hash_node", %"struct.std::__detail::_Hash_node"* %__n.addr.06.i.i.i.i, i64 0, i32 0, i32 1, i32 0, i32 0, i64 8
  %30 = bitcast i8* %_M_start.i.i.i.i.i.i.i.i.i.i to %struct.GroupByColumn**
  %31 = load %struct.GroupByColumn*, %struct.GroupByColumn** %30, align 8, !tbaa !42
  %tobool.not.i.i.i.i.i.i.i.i.i.i.i = icmp eq %struct.GroupByColumn* %31, null
  br i1 %tobool.not.i.i.i.i.i.i.i.i.i.i.i, label %_ZNSt8__detail16_Hashtable_allocISaINS_10_Hash_nodeISt4pairIKmSt6vectorI13GroupByColumnSaIS5_EEELb0EEEEE18_M_deallocate_nodeEPS9_.exit.i.i.i.i, label %if.then.i.i.i.i.i.i.i.i.i.i.i

if.then.i.i.i.i.i.i.i.i.i.i.i:                    ; preds = %while.body.i.i.i.i
  %32 = bitcast %struct.GroupByColumn* %31 to i8*
  tail call void @_ZdlPv(i8* nonnull %32) #20
  br label %_ZNSt8__detail16_Hashtable_allocISaINS_10_Hash_nodeISt4pairIKmSt6vectorI13GroupByColumnSaIS5_EEELb0EEEEE18_M_deallocate_nodeEPS9_.exit.i.i.i.i

_ZNSt8__detail16_Hashtable_allocISaINS_10_Hash_nodeISt4pairIKmSt6vectorI13GroupByColumnSaIS5_EEELb0EEEEE18_M_deallocate_nodeEPS9_.exit.i.i.i.i: ; preds = %if.then.i.i.i.i.i.i.i.i.i.i.i, %while.body.i.i.i.i
  %33 = bitcast %"struct.std::__detail::_Hash_node"* %__n.addr.06.i.i.i.i to i8*
  tail call void @_ZdlPv(i8* nonnull %33) #20
  %tobool.not.i.i.i.i = icmp eq %"struct.std::__detail::_Hash_node"* %29, null
  br i1 %tobool.not.i.i.i.i, label %_ZNSt10_HashtableImSt4pairIKmSt6vectorI13GroupByColumnSaIS3_EEESaIS6_ENSt8__detail10_Select1stESt8equal_toImESt4hashImENS8_18_Mod_range_hashingENS8_20_Default_ranged_hashENS8_20_Prime_rehash_policyENS8_17_Hashtable_traitsILb0ELb0ELb1EEEE5clearEv.exit.i.i, label %while.body.i.i.i.i, !llvm.loop !127

_ZNSt10_HashtableImSt4pairIKmSt6vectorI13GroupByColumnSaIS3_EEESaIS6_ENSt8__detail10_Select1stESt8equal_toImESt4hashImENS8_18_Mod_range_hashingENS8_20_Default_ranged_hashENS8_20_Prime_rehash_policyENS8_17_Hashtable_traitsILb0ELb0ELb1EEEE5clearEv.exit.i.i: ; preds = %_ZNSt8__detail16_Hashtable_allocISaINS_10_Hash_nodeISt4pairIKmSt6vectorI13GroupByColumnSaIS5_EEELb0EEEEE18_M_deallocate_nodeEPS9_.exit.i.i.i.i, %delete.end52
  %34 = load i8*, i8** %8, align 8, !tbaa !21
  %35 = load i64, i64* %_M_bucket_count.i.i, align 8, !tbaa !20
  %mul.i.i.i = shl i64 %35, 3
  tail call void @llvm.memset.p0i8.i64(i8* align 8 %34, i8 0, i64 %mul.i.i.i, i1 false) #20
  tail call void @llvm.memset.p0i8.i64(i8* nonnull align 8 dereferenceable(16) %11, i8 0, i64 16, i1 false) #20
  %_M_buckets.i.i.i = getelementptr inbounds %"class.std::unordered_map", %"class.std::unordered_map"* %groupedRows, i64 0, i32 0, i32 0
  %36 = load %"struct.std::__detail::_Hash_node_base"**, %"struct.std::__detail::_Hash_node_base"*** %_M_buckets.i.i.i, align 8, !tbaa !21
  %_M_single_bucket.i.i.i.i.i = getelementptr inbounds %class.HashGroupBy, %class.HashGroupBy* %this, i64 0, i32 4, i32 0, i32 5
  %cmp.i.i.i.i.i = icmp eq %"struct.std::__detail::_Hash_node_base"** %_M_single_bucket.i.i.i.i.i, %36
  br i1 %cmp.i.i.i.i.i, label %_ZNSt13unordered_mapImSt6vectorI13GroupByColumnSaIS1_EESt4hashImESt8equal_toImESaISt4pairIKmS3_EEED2Ev.exit, label %if.end.i.i.i.i

if.end.i.i.i.i:                                   ; preds = %_ZNSt10_HashtableImSt4pairIKmSt6vectorI13GroupByColumnSaIS3_EEESaIS6_ENSt8__detail10_Select1stESt8equal_toImESt4hashImENS8_18_Mod_range_hashingENS8_20_Default_ranged_hashENS8_20_Prime_rehash_policyENS8_17_Hashtable_traitsILb0ELb0ELb1EEEE5clearEv.exit.i.i
  %37 = bitcast %"struct.std::__detail::_Hash_node_base"** %36 to i8*
  tail call void @_ZdlPv(i8* %37) #20
  br label %_ZNSt13unordered_mapImSt6vectorI13GroupByColumnSaIS1_EESt4hashImESt8equal_toImESaISt4pairIKmS3_EEED2Ev.exit

_ZNSt13unordered_mapImSt6vectorI13GroupByColumnSaIS1_EESt4hashImESt8equal_toImESaISt4pairIKmS3_EEED2Ev.exit: ; preds = %_ZNSt10_HashtableImSt4pairIKmSt6vectorI13GroupByColumnSaIS3_EEESaIS6_ENSt8__detail10_Select1stESt8equal_toImESt4hashImENS8_18_Mod_range_hashingENS8_20_Default_ranged_hashENS8_20_Prime_rehash_policyENS8_17_Hashtable_traitsILb0ELb0ELb1EEEE5clearEv.exit.i.i, %if.end.i.i.i.i
  %38 = load %class.Aggregator**, %class.Aggregator*** %_M_start.i, align 8, !tbaa !47
  %tobool.not.i.i.i73 = icmp eq %class.Aggregator** %38, null
  br i1 %tobool.not.i.i.i73, label %_ZNSt6vectorIP10AggregatorSaIS1_EED2Ev.exit, label %if.then.i.i.i74

if.then.i.i.i74:                                  ; preds = %_ZNSt13unordered_mapImSt6vectorI13GroupByColumnSaIS1_EESt4hashImESt8equal_toImESaISt4pairIKmS3_EEED2Ev.exit
  %39 = bitcast %class.Aggregator** %38 to i8*
  tail call void @_ZdlPv(i8* nonnull %39) #20
  br label %_ZNSt6vectorIP10AggregatorSaIS1_EED2Ev.exit

_ZNSt6vectorIP10AggregatorSaIS1_EED2Ev.exit:      ; preds = %_ZNSt13unordered_mapImSt6vectorI13GroupByColumnSaIS1_EESt4hashImESt8equal_toImESaISt4pairIKmS3_EEED2Ev.exit, %if.then.i.i.i74
  %_M_start.i.i68 = getelementptr inbounds %class.HashGroupBy, %class.HashGroupBy* %this, i64 0, i32 2, i32 0, i32 0, i32 0
  %40 = load %struct.ColumnIndex*, %struct.ColumnIndex** %_M_start.i.i68, align 8, !tbaa !8
  %tobool.not.i.i.i69 = icmp eq %struct.ColumnIndex* %40, null
  br i1 %tobool.not.i.i.i69, label %_ZNSt6vectorI11ColumnIndexSaIS0_EED2Ev.exit71, label %if.then.i.i.i70

if.then.i.i.i70:                                  ; preds = %_ZNSt6vectorIP10AggregatorSaIS1_EED2Ev.exit
  %41 = bitcast %struct.ColumnIndex* %40 to i8*
  tail call void @_ZdlPv(i8* nonnull %41) #20
  br label %_ZNSt6vectorI11ColumnIndexSaIS0_EED2Ev.exit71

_ZNSt6vectorI11ColumnIndexSaIS0_EED2Ev.exit71:    ; preds = %_ZNSt6vectorIP10AggregatorSaIS1_EED2Ev.exit, %if.then.i.i.i70
  %_M_start.i.i = getelementptr inbounds %class.HashGroupBy, %class.HashGroupBy* %this, i64 0, i32 1, i32 0, i32 0, i32 0
  %42 = load %struct.ColumnIndex*, %struct.ColumnIndex** %_M_start.i.i, align 8, !tbaa !8
  %tobool.not.i.i.i = icmp eq %struct.ColumnIndex* %42, null
  br i1 %tobool.not.i.i.i, label %_ZNSt6vectorI11ColumnIndexSaIS0_EED2Ev.exit, label %if.then.i.i.i

if.then.i.i.i:                                    ; preds = %_ZNSt6vectorI11ColumnIndexSaIS0_EED2Ev.exit71
  %43 = bitcast %struct.ColumnIndex* %42 to i8*
  tail call void @_ZdlPv(i8* nonnull %43) #20
  br label %_ZNSt6vectorI11ColumnIndexSaIS0_EED2Ev.exit

_ZNSt6vectorI11ColumnIndexSaIS0_EED2Ev.exit:      ; preds = %_ZNSt6vectorI11ColumnIndexSaIS0_EED2Ev.exit71, %if.then.i.i.i
  ret void
}

; Function Attrs: nounwind uwtable
define linkonce_odr dso_local void @_ZN11HashGroupByD0Ev(%class.HashGroupBy* nonnull dereferenceable(144) %this) unnamed_addr #14 comdat align 2 {
entry:
  tail call void @_ZN11HashGroupByD2Ev(%class.HashGroupBy* nonnull dereferenceable(144) %this) #20
  %0 = bitcast %class.HashGroupBy* %this to i8*
  tail call void @_ZdlPv(i8* nonnull %0) #23
  ret void
}

; Function Attrs: noinline noreturn nounwind
define linkonce_odr hidden void @__clang_call_terminate(i8* %0) local_unnamed_addr #15 comdat {
  %2 = tail call i8* @__cxa_begin_catch(i8* %0) #20
  tail call void @_ZSt9terminatev() #21
  unreachable
}

declare dso_local i8* @__cxa_begin_catch(i8*) local_unnamed_addr

declare dso_local void @_ZSt9terminatev() local_unnamed_addr

declare dso_local i64 @_ZSt11_Hash_bytesPKvmm(i8*, i64, i64) local_unnamed_addr #0

; Function Attrs: noreturn
declare dso_local void @_ZSt17__throw_bad_allocv() local_unnamed_addr #16

; Function Attrs: argmemonly nofree nosync nounwind willreturn
declare void @llvm.memmove.p0i8.p0i8.i64(i8* nocapture writeonly, i8* nocapture readonly, i64, i1 immarg) #7

declare dso_local void @__cxa_rethrow() local_unnamed_addr

declare dso_local void @__cxa_end_catch() local_unnamed_addr

; Function Attrs: nounwind uwtable willreturn
define linkonce_odr dso_local void @_ZN6ColumnD2Ev(%class.Column* nonnull dereferenceable(40) %this) unnamed_addr #17 comdat align 2 {
entry:
  ret void
}

; Function Attrs: nounwind uwtable
define linkonce_odr dso_local void @_ZN6ColumnD0Ev(%class.Column* nonnull dereferenceable(40) %this) unnamed_addr #14 comdat align 2 {
entry:
  %0 = bitcast %class.Column* %this to i8*
  tail call void @_ZdlPv(i8* nonnull %0) #23
  ret void
}

; Function Attrs: nounwind uwtable
define linkonce_odr dso_local void @_ZN5TableD2Ev(%class.Table* nonnull dereferenceable(60) %this) unnamed_addr #14 comdat align 2 personality i32 (...)* @__gxx_personality_v0 {
entry:
  %0 = getelementptr inbounds %class.Table, %class.Table* %this, i64 0, i32 0
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [4 x i8*] }, { [4 x i8*] }* @_ZTV5Table, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %0, align 8, !tbaa !50
  %types = getelementptr inbounds %class.Table, %class.Table* %this, i64 0, i32 4
  %1 = load i32*, i32** %types, align 8, !tbaa !61
  %isnull = icmp eq i32* %1, null
  br i1 %isnull, label %delete.end, label %delete.notnull

delete.notnull:                                   ; preds = %entry
  %2 = bitcast i32* %1 to i8*
  tail call void @_ZdaPv(i8* %2) #23
  br label %delete.end

delete.end:                                       ; preds = %delete.notnull, %entry
  %_M_start.i.i = getelementptr inbounds %class.Table, %class.Table* %this, i64 0, i32 3, i32 0, i32 0, i32 0
  %3 = load %class.Column**, %class.Column*** %_M_start.i.i, align 8, !tbaa !29
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
define linkonce_odr dso_local void @_ZN5TableD0Ev(%class.Table* nonnull dereferenceable(60) %this) unnamed_addr #14 comdat align 2 personality i32 (...)* @__gxx_personality_v0 {
entry:
  %0 = getelementptr inbounds %class.Table, %class.Table* %this, i64 0, i32 0
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [4 x i8*] }, { [4 x i8*] }* @_ZTV5Table, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %0, align 8, !tbaa !50
  %types.i = getelementptr inbounds %class.Table, %class.Table* %this, i64 0, i32 4
  %1 = load i32*, i32** %types.i, align 8, !tbaa !61
  %isnull.i = icmp eq i32* %1, null
  br i1 %isnull.i, label %delete.end.i, label %delete.notnull.i

delete.notnull.i:                                 ; preds = %entry
  %2 = bitcast i32* %1 to i8*
  tail call void @_ZdaPv(i8* %2) #23
  br label %delete.end.i

delete.end.i:                                     ; preds = %delete.notnull.i, %entry
  %_M_start.i.i.i = getelementptr inbounds %class.Table, %class.Table* %this, i64 0, i32 3, i32 0, i32 0, i32 0
  %3 = load %class.Column**, %class.Column*** %_M_start.i.i.i, align 8, !tbaa !29
  %tobool.not.i.i.i.i = icmp eq %class.Column** %3, null
  br i1 %tobool.not.i.i.i.i, label %_ZN5TableD2Ev.exit, label %if.then.i.i.i.i

if.then.i.i.i.i:                                  ; preds = %delete.end.i
  %4 = bitcast %class.Column** %3 to i8*
  tail call void @_ZdlPv(i8* nonnull %4) #20
  br label %_ZN5TableD2Ev.exit

_ZN5TableD2Ev.exit:                               ; preds = %delete.end.i, %if.then.i.i.i.i
  %5 = bitcast %class.Table* %this to i8*
  tail call void @_ZdlPv(i8* nonnull %5) #23
  ret void
}

; Function Attrs: uwtable
define linkonce_odr dso_local void @_ZNSt10_HashtableImSt4pairIKmSt6vectorI13GroupByColumnSaIS3_EEESaIS6_ENSt8__detail10_Select1stESt8equal_toImESt4hashImENS8_18_Mod_range_hashingENS8_20_Default_ranged_hashENS8_20_Prime_rehash_policyENS8_17_Hashtable_traitsILb0ELb0ELb1EEEE9_M_assignIZNSJ_C1ERKSJ_EUlPKNS8_10_Hash_nodeIS6_Lb0EEEE_EEvSM_RKT_(%"class.std::_Hashtable"* nonnull dereferenceable(56) %this, %"class.std::_Hashtable"* nonnull align 8 dereferenceable(56) %__ht, %class.anon* nonnull align 8 dereferenceable(8) %__node_gen) local_unnamed_addr #6 comdat align 2 personality i8* bitcast (i32 (...)* @__gxx_personality_v0 to i8*) {
entry:
  %_M_buckets = getelementptr inbounds %"class.std::_Hashtable", %"class.std::_Hashtable"* %this, i64 0, i32 0
  %0 = load %"struct.std::__detail::_Hash_node_base"**, %"struct.std::__detail::_Hash_node_base"*** %_M_buckets, align 8, !tbaa !21
  %tobool.not = icmp eq %"struct.std::__detail::_Hash_node_base"** %0, null
  br i1 %tobool.not, label %if.then, label %if.end

if.then:                                          ; preds = %entry
  %_M_bucket_count = getelementptr inbounds %"class.std::_Hashtable", %"class.std::_Hashtable"* %this, i64 0, i32 1
  %1 = load i64, i64* %_M_bucket_count, align 8, !tbaa !20
  %cmp.i = icmp eq i64 %1, 1
  br i1 %cmp.i, label %if.then.i, label %if.end.i, !prof !133

if.then.i:                                        ; preds = %if.then
  %_M_single_bucket.i = getelementptr inbounds %"class.std::_Hashtable", %"class.std::_Hashtable"* %this, i64 0, i32 5
  store %"struct.std::__detail::_Hash_node_base"* null, %"struct.std::__detail::_Hash_node_base"** %_M_single_bucket.i, align 8, !tbaa !124
  br label %_ZNSt10_HashtableImSt4pairIKmSt6vectorI13GroupByColumnSaIS3_EEESaIS6_ENSt8__detail10_Select1stESt8equal_toImESt4hashImENS8_18_Mod_range_hashingENS8_20_Default_ranged_hashENS8_20_Prime_rehash_policyENS8_17_Hashtable_traitsILb0ELb0ELb1EEEE19_M_allocate_bucketsEm.exit

if.end.i:                                         ; preds = %if.then
  %cmp.i.i.i.i = icmp ugt i64 %1, 2305843009213693951
  br i1 %cmp.i.i.i.i, label %if.then.i.i.i.i, label %_ZNSt8__detail16_Hashtable_allocISaINS_10_Hash_nodeISt4pairIKmSt6vectorI13GroupByColumnSaIS5_EEELb0EEEEE19_M_allocate_bucketsEm.exit.i

if.then.i.i.i.i:                                  ; preds = %if.end.i
  tail call void @_ZSt17__throw_bad_allocv() #22
  unreachable

_ZNSt8__detail16_Hashtable_allocISaINS_10_Hash_nodeISt4pairIKmSt6vectorI13GroupByColumnSaIS5_EEELb0EEEEE19_M_allocate_bucketsEm.exit.i: ; preds = %if.end.i
  %mul.i.i.i.i = shl nuw i64 %1, 3
  %call2.i.i10.i.i = tail call noalias nonnull i8* @_Znwm(i64 %mul.i.i.i.i) #19
  %2 = bitcast i8* %call2.i.i10.i.i to %"struct.std::__detail::_Hash_node_base"**
  tail call void @llvm.memset.p0i8.i64(i8* nonnull align 8 %call2.i.i10.i.i, i8 0, i64 %mul.i.i.i.i, i1 false)
  br label %_ZNSt10_HashtableImSt4pairIKmSt6vectorI13GroupByColumnSaIS3_EEESaIS6_ENSt8__detail10_Select1stESt8equal_toImESt4hashImENS8_18_Mod_range_hashingENS8_20_Default_ranged_hashENS8_20_Prime_rehash_policyENS8_17_Hashtable_traitsILb0ELb0ELb1EEEE19_M_allocate_bucketsEm.exit

_ZNSt10_HashtableImSt4pairIKmSt6vectorI13GroupByColumnSaIS3_EEESaIS6_ENSt8__detail10_Select1stESt8equal_toImESt4hashImENS8_18_Mod_range_hashingENS8_20_Default_ranged_hashENS8_20_Prime_rehash_policyENS8_17_Hashtable_traitsILb0ELb0ELb1EEEE19_M_allocate_bucketsEm.exit: ; preds = %if.then.i, %_ZNSt8__detail16_Hashtable_allocISaINS_10_Hash_nodeISt4pairIKmSt6vectorI13GroupByColumnSaIS5_EEELb0EEEEE19_M_allocate_bucketsEm.exit.i
  %retval.0.i = phi %"struct.std::__detail::_Hash_node_base"** [ %_M_single_bucket.i, %if.then.i ], [ %2, %_ZNSt8__detail16_Hashtable_allocISaINS_10_Hash_nodeISt4pairIKmSt6vectorI13GroupByColumnSaIS5_EEELb0EEEEE19_M_allocate_bucketsEm.exit.i ]
  store %"struct.std::__detail::_Hash_node_base"** %retval.0.i, %"struct.std::__detail::_Hash_node_base"*** %_M_buckets, align 8, !tbaa !21
  br label %if.end

if.end:                                           ; preds = %_ZNSt10_HashtableImSt4pairIKmSt6vectorI13GroupByColumnSaIS3_EEESaIS6_ENSt8__detail10_Select1stESt8equal_toImESt4hashImENS8_18_Mod_range_hashingENS8_20_Default_ranged_hashENS8_20_Prime_rehash_policyENS8_17_Hashtable_traitsILb0ELb0ELb1EEEE19_M_allocate_bucketsEm.exit, %entry
  %__buckets.0 = phi %"struct.std::__detail::_Hash_node_base"** [ null, %entry ], [ %retval.0.i, %_ZNSt10_HashtableImSt4pairIKmSt6vectorI13GroupByColumnSaIS3_EEESaIS6_ENSt8__detail10_Select1stESt8equal_toImESt4hashImENS8_18_Mod_range_hashingENS8_20_Default_ranged_hashENS8_20_Prime_rehash_policyENS8_17_Hashtable_traitsILb0ELb0ELb1EEEE19_M_allocate_bucketsEm.exit ]
  %_M_nxt = getelementptr inbounds %"class.std::_Hashtable", %"class.std::_Hashtable"* %__ht, i64 0, i32 2, i32 0
  %3 = load %"struct.std::__detail::_Hash_node_base"*, %"struct.std::__detail::_Hash_node_base"** %_M_nxt, align 8, !tbaa !95
  %tobool3.not = icmp eq %"struct.std::__detail::_Hash_node_base"* %3, null
  br i1 %tobool3.not, label %cleanup, label %if.end5

if.end5:                                          ; preds = %if.end
  %4 = bitcast %class.anon* %__node_gen to %"struct.std::__detail::_Hashtable_alloc"**
  %5 = load %"struct.std::__detail::_Hashtable_alloc"*, %"struct.std::__detail::_Hashtable_alloc"** %4, align 8, !tbaa !125
  %_M_storage.i.i.i = getelementptr inbounds %"struct.std::__detail::_Hash_node_base", %"struct.std::__detail::_Hash_node_base"* %3, i64 1
  %6 = bitcast %"struct.std::__detail::_Hash_node_base"* %_M_storage.i.i.i to %"struct.std::pair.28"*
  %call2.i69 = invoke %"struct.std::__detail::_Hash_node"* @_ZNSt8__detail16_Hashtable_allocISaINS_10_Hash_nodeISt4pairIKmSt6vectorI13GroupByColumnSaIS5_EEELb0EEEEE16_M_allocate_nodeIJRKS8_EEEPS9_DpOT_(%"struct.std::__detail::_Hashtable_alloc"* nonnull dereferenceable(1) %5, %"struct.std::pair.28"* nonnull align 8 dereferenceable(32) %6)
          to label %invoke.cont8 unwind label %lpad7

invoke.cont8:                                     ; preds = %if.end5
  %7 = getelementptr %"struct.std::__detail::_Hash_node", %"struct.std::__detail::_Hash_node"* %call2.i69, i64 0, i32 0, i32 0
  %_M_before_begin11 = getelementptr inbounds %"class.std::_Hashtable", %"class.std::_Hashtable"* %this, i64 0, i32 2
  %_M_nxt12 = getelementptr inbounds %"struct.std::__detail::_Hash_node_base", %"struct.std::__detail::_Hash_node_base"* %_M_before_begin11, i64 0, i32 0
  store %"struct.std::__detail::_Hash_node_base"* %7, %"struct.std::__detail::_Hash_node_base"** %_M_nxt12, align 8, !tbaa !95
  %8 = load %"struct.std::__detail::_Hash_node_base"**, %"struct.std::__detail::_Hash_node_base"*** %_M_buckets, align 8, !tbaa !21
  %_M_bucket_count.i = getelementptr inbounds %"class.std::_Hashtable", %"class.std::_Hashtable"* %this, i64 0, i32 1
  %9 = load i64, i64* %_M_bucket_count.i, align 8, !tbaa !20
  %_M_storage.i.i.i.i = getelementptr inbounds %"struct.std::__detail::_Hash_node", %"struct.std::__detail::_Hash_node"* %call2.i69, i64 0, i32 0, i32 1
  %first.i.i.i.i.i = bitcast %"struct.__gnu_cxx::__aligned_buffer"* %_M_storage.i.i.i.i to i64*
  %10 = load i64, i64* %first.i.i.i.i.i, align 8, !tbaa !23
  %rem.i.i.i = urem i64 %10, %9
  %arrayidx = getelementptr inbounds %"struct.std::__detail::_Hash_node_base"*, %"struct.std::__detail::_Hash_node_base"** %8, i64 %rem.i.i.i
  store %"struct.std::__detail::_Hash_node_base"* %_M_before_begin11, %"struct.std::__detail::_Hash_node_base"** %arrayidx, align 8, !tbaa !19
  %__ht_n.0.in78 = bitcast %"struct.std::__detail::_Hash_node_base"* %3 to %"struct.std::__detail::_Hash_node"**
  %__ht_n.079 = load %"struct.std::__detail::_Hash_node"*, %"struct.std::__detail::_Hash_node"** %__ht_n.0.in78, align 8, !tbaa !22
  %tobool17.not80 = icmp eq %"struct.std::__detail::_Hash_node"* %__ht_n.079, null
  br i1 %tobool17.not80, label %cleanup, label %for.body

for.body:                                         ; preds = %invoke.cont8, %if.end30
  %__ht_n.082 = phi %"struct.std::__detail::_Hash_node"* [ %__ht_n.0, %if.end30 ], [ %__ht_n.079, %invoke.cont8 ]
  %__prev_n.081 = phi %"struct.std::__detail::_Hash_node_base"* [ %13, %if.end30 ], [ %7, %invoke.cont8 ]
  %11 = load %"struct.std::__detail::_Hashtable_alloc"*, %"struct.std::__detail::_Hashtable_alloc"** %4, align 8, !tbaa !125
  %_M_storage.i.i.i71 = getelementptr inbounds %"struct.std::__detail::_Hash_node", %"struct.std::__detail::_Hash_node"* %__ht_n.082, i64 0, i32 0, i32 1
  %12 = bitcast %"struct.__gnu_cxx::__aligned_buffer"* %_M_storage.i.i.i71 to %"struct.std::pair.28"*
  %call2.i72 = invoke %"struct.std::__detail::_Hash_node"* @_ZNSt8__detail16_Hashtable_allocISaINS_10_Hash_nodeISt4pairIKmSt6vectorI13GroupByColumnSaIS5_EEELb0EEEEE16_M_allocate_nodeIJRKS8_EEEPS9_DpOT_(%"struct.std::__detail::_Hashtable_alloc"* nonnull dereferenceable(1) %11, %"struct.std::pair.28"* nonnull align 8 dereferenceable(32) %12)
          to label %invoke.cont19 unwind label %lpad18

invoke.cont19:                                    ; preds = %for.body
  %13 = getelementptr %"struct.std::__detail::_Hash_node", %"struct.std::__detail::_Hash_node"* %call2.i72, i64 0, i32 0, i32 0
  %_M_nxt21 = getelementptr inbounds %"struct.std::__detail::_Hash_node_base", %"struct.std::__detail::_Hash_node_base"* %__prev_n.081, i64 0, i32 0
  store %"struct.std::__detail::_Hash_node_base"* %13, %"struct.std::__detail::_Hash_node_base"** %_M_nxt21, align 8, !tbaa !22
  %14 = load i64, i64* %_M_bucket_count.i, align 8, !tbaa !20
  %_M_storage.i.i.i.i75 = getelementptr inbounds %"struct.std::__detail::_Hash_node", %"struct.std::__detail::_Hash_node"* %call2.i72, i64 0, i32 0, i32 1
  %first.i.i.i.i.i76 = bitcast %"struct.__gnu_cxx::__aligned_buffer"* %_M_storage.i.i.i.i75 to i64*
  %15 = load i64, i64* %first.i.i.i.i.i76, align 8, !tbaa !23
  %rem.i.i.i77 = urem i64 %15, %14
  %16 = load %"struct.std::__detail::_Hash_node_base"**, %"struct.std::__detail::_Hash_node_base"*** %_M_buckets, align 8, !tbaa !21
  %arrayidx25 = getelementptr inbounds %"struct.std::__detail::_Hash_node_base"*, %"struct.std::__detail::_Hash_node_base"** %16, i64 %rem.i.i.i77
  %17 = load %"struct.std::__detail::_Hash_node_base"*, %"struct.std::__detail::_Hash_node_base"** %arrayidx25, align 8, !tbaa !19
  %tobool26.not = icmp eq %"struct.std::__detail::_Hash_node_base"* %17, null
  br i1 %tobool26.not, label %if.then27, label %if.end30

if.then27:                                        ; preds = %invoke.cont19
  store %"struct.std::__detail::_Hash_node_base"* %__prev_n.081, %"struct.std::__detail::_Hash_node_base"** %arrayidx25, align 8, !tbaa !19
  br label %if.end30

lpad7:                                            ; preds = %if.end5
  %18 = landingpad { i8*, i32 }
          catch i8* null
  br label %ehcleanup

lpad18:                                           ; preds = %for.body
  %19 = landingpad { i8*, i32 }
          catch i8* null
  br label %ehcleanup

if.end30:                                         ; preds = %if.then27, %invoke.cont19
  %__ht_n.0.in = bitcast %"struct.std::__detail::_Hash_node"* %__ht_n.082 to %"struct.std::__detail::_Hash_node"**
  %__ht_n.0 = load %"struct.std::__detail::_Hash_node"*, %"struct.std::__detail::_Hash_node"** %__ht_n.0.in, align 8, !tbaa !22
  %tobool17.not = icmp eq %"struct.std::__detail::_Hash_node"* %__ht_n.0, null
  br i1 %tobool17.not, label %cleanup, label %for.body, !llvm.loop !134

ehcleanup:                                        ; preds = %lpad18, %lpad7
  %.pn = phi { i8*, i32 } [ %19, %lpad18 ], [ %18, %lpad7 ]
  %exn.slot.0 = extractvalue { i8*, i32 } %.pn, 0
  %20 = tail call i8* @__cxa_begin_catch(i8* %exn.slot.0) #20
  %_M_nxt.i.i = getelementptr inbounds %"class.std::_Hashtable", %"class.std::_Hashtable"* %this, i64 0, i32 2, i32 0
  %21 = bitcast %"struct.std::__detail::_Hash_node_base"** %_M_nxt.i.i to %"struct.std::__detail::_Hash_node"**
  %22 = load %"struct.std::__detail::_Hash_node"*, %"struct.std::__detail::_Hash_node"** %21, align 8, !tbaa !95
  %tobool.not5.i.i = icmp eq %"struct.std::__detail::_Hash_node"* %22, null
  br i1 %tobool.not5.i.i, label %_ZNSt10_HashtableImSt4pairIKmSt6vectorI13GroupByColumnSaIS3_EEESaIS6_ENSt8__detail10_Select1stESt8equal_toImESt4hashImENS8_18_Mod_range_hashingENS8_20_Default_ranged_hashENS8_20_Prime_rehash_policyENS8_17_Hashtable_traitsILb0ELb0ELb1EEEE5clearEv.exit, label %while.body.i.i

while.body.i.i:                                   ; preds = %ehcleanup, %_ZNSt8__detail16_Hashtable_allocISaINS_10_Hash_nodeISt4pairIKmSt6vectorI13GroupByColumnSaIS5_EEELb0EEEEE18_M_deallocate_nodeEPS9_.exit.i.i
  %__n.addr.06.i.i = phi %"struct.std::__detail::_Hash_node"* [ %24, %_ZNSt8__detail16_Hashtable_allocISaINS_10_Hash_nodeISt4pairIKmSt6vectorI13GroupByColumnSaIS5_EEELb0EEEEE18_M_deallocate_nodeEPS9_.exit.i.i ], [ %22, %ehcleanup ]
  %23 = bitcast %"struct.std::__detail::_Hash_node"* %__n.addr.06.i.i to %"struct.std::__detail::_Hash_node"**
  %24 = load %"struct.std::__detail::_Hash_node"*, %"struct.std::__detail::_Hash_node"** %23, align 8, !tbaa !22
  %_M_start.i.i.i.i.i.i.i.i = getelementptr inbounds %"struct.std::__detail::_Hash_node", %"struct.std::__detail::_Hash_node"* %__n.addr.06.i.i, i64 0, i32 0, i32 1, i32 0, i32 0, i64 8
  %25 = bitcast i8* %_M_start.i.i.i.i.i.i.i.i to %struct.GroupByColumn**
  %26 = load %struct.GroupByColumn*, %struct.GroupByColumn** %25, align 8, !tbaa !42
  %tobool.not.i.i.i.i.i.i.i.i.i = icmp eq %struct.GroupByColumn* %26, null
  br i1 %tobool.not.i.i.i.i.i.i.i.i.i, label %_ZNSt8__detail16_Hashtable_allocISaINS_10_Hash_nodeISt4pairIKmSt6vectorI13GroupByColumnSaIS5_EEELb0EEEEE18_M_deallocate_nodeEPS9_.exit.i.i, label %if.then.i.i.i.i.i.i.i.i.i

if.then.i.i.i.i.i.i.i.i.i:                        ; preds = %while.body.i.i
  %27 = bitcast %struct.GroupByColumn* %26 to i8*
  tail call void @_ZdlPv(i8* nonnull %27) #20
  br label %_ZNSt8__detail16_Hashtable_allocISaINS_10_Hash_nodeISt4pairIKmSt6vectorI13GroupByColumnSaIS5_EEELb0EEEEE18_M_deallocate_nodeEPS9_.exit.i.i

_ZNSt8__detail16_Hashtable_allocISaINS_10_Hash_nodeISt4pairIKmSt6vectorI13GroupByColumnSaIS5_EEELb0EEEEE18_M_deallocate_nodeEPS9_.exit.i.i: ; preds = %if.then.i.i.i.i.i.i.i.i.i, %while.body.i.i
  %28 = bitcast %"struct.std::__detail::_Hash_node"* %__n.addr.06.i.i to i8*
  tail call void @_ZdlPv(i8* nonnull %28) #20
  %tobool.not.i.i = icmp eq %"struct.std::__detail::_Hash_node"* %24, null
  br i1 %tobool.not.i.i, label %_ZNSt10_HashtableImSt4pairIKmSt6vectorI13GroupByColumnSaIS3_EEESaIS6_ENSt8__detail10_Select1stESt8equal_toImESt4hashImENS8_18_Mod_range_hashingENS8_20_Default_ranged_hashENS8_20_Prime_rehash_policyENS8_17_Hashtable_traitsILb0ELb0ELb1EEEE5clearEv.exit, label %while.body.i.i, !llvm.loop !127

_ZNSt10_HashtableImSt4pairIKmSt6vectorI13GroupByColumnSaIS3_EEESaIS6_ENSt8__detail10_Select1stESt8equal_toImESt4hashImENS8_18_Mod_range_hashingENS8_20_Default_ranged_hashENS8_20_Prime_rehash_policyENS8_17_Hashtable_traitsILb0ELb0ELb1EEEE5clearEv.exit: ; preds = %_ZNSt8__detail16_Hashtable_allocISaINS_10_Hash_nodeISt4pairIKmSt6vectorI13GroupByColumnSaIS5_EEELb0EEEEE18_M_deallocate_nodeEPS9_.exit.i.i, %ehcleanup
  %29 = bitcast %"class.std::_Hashtable"* %this to i8**
  %30 = load i8*, i8** %29, align 8, !tbaa !21
  %_M_bucket_count.i70 = getelementptr inbounds %"class.std::_Hashtable", %"class.std::_Hashtable"* %this, i64 0, i32 1
  %31 = load i64, i64* %_M_bucket_count.i70, align 8, !tbaa !20
  %mul.i = shl i64 %31, 3
  tail call void @llvm.memset.p0i8.i64(i8* align 8 %30, i8 0, i64 %mul.i, i1 false) #20
  %32 = bitcast %"struct.std::__detail::_Hash_node_base"** %_M_nxt.i.i to i8*
  tail call void @llvm.memset.p0i8.i64(i8* nonnull align 8 dereferenceable(16) %32, i8 0, i64 16, i1 false) #20
  %tobool33.not = icmp eq %"struct.std::__detail::_Hash_node_base"** %__buckets.0, null
  br i1 %tobool33.not, label %if.end37, label %if.then34

if.then34:                                        ; preds = %_ZNSt10_HashtableImSt4pairIKmSt6vectorI13GroupByColumnSaIS3_EEESaIS6_ENSt8__detail10_Select1stESt8equal_toImESt4hashImENS8_18_Mod_range_hashingENS8_20_Default_ranged_hashENS8_20_Prime_rehash_policyENS8_17_Hashtable_traitsILb0ELb0ELb1EEEE5clearEv.exit
  %33 = load %"struct.std::__detail::_Hash_node_base"**, %"struct.std::__detail::_Hash_node_base"*** %_M_buckets, align 8, !tbaa !21
  %_M_single_bucket.i.i.i = getelementptr inbounds %"class.std::_Hashtable", %"class.std::_Hashtable"* %this, i64 0, i32 5
  %cmp.i.i.i = icmp eq %"struct.std::__detail::_Hash_node_base"** %_M_single_bucket.i.i.i, %33
  br i1 %cmp.i.i.i, label %if.end37, label %if.end.i.i

if.end.i.i:                                       ; preds = %if.then34
  %34 = bitcast %"struct.std::__detail::_Hash_node_base"** %33 to i8*
  tail call void @_ZdlPv(i8* %34) #20
  br label %if.end37

lpad35:                                           ; preds = %if.end37
  %35 = landingpad { i8*, i32 }
          cleanup
  invoke void @__cxa_end_catch()
          to label %invoke.cont39 unwind label %terminate.lpad

if.end37:                                         ; preds = %if.end.i.i, %if.then34, %_ZNSt10_HashtableImSt4pairIKmSt6vectorI13GroupByColumnSaIS3_EEESaIS6_ENSt8__detail10_Select1stESt8equal_toImESt4hashImENS8_18_Mod_range_hashingENS8_20_Default_ranged_hashENS8_20_Prime_rehash_policyENS8_17_Hashtable_traitsILb0ELb0ELb1EEEE5clearEv.exit
  invoke void @__cxa_rethrow() #22
          to label %unreachable unwind label %lpad35

invoke.cont39:                                    ; preds = %lpad35
  resume { i8*, i32 } %35

cleanup:                                          ; preds = %if.end30, %invoke.cont8, %if.end
  ret void

terminate.lpad:                                   ; preds = %lpad35
  %36 = landingpad { i8*, i32 }
          catch i8* null
  %37 = extractvalue { i8*, i32 } %36, 0
  tail call void @__clang_call_terminate(i8* %37) #21
  unreachable

unreachable:                                      ; preds = %if.end37
  unreachable
}

; Function Attrs: uwtable
define linkonce_odr dso_local %"struct.std::__detail::_Hash_node"* @_ZNSt8__detail16_Hashtable_allocISaINS_10_Hash_nodeISt4pairIKmSt6vectorI13GroupByColumnSaIS5_EEELb0EEEEE16_M_allocate_nodeIJRKS8_EEEPS9_DpOT_(%"struct.std::__detail::_Hashtable_alloc"* nonnull dereferenceable(1) %this, %"struct.std::pair.28"* nonnull align 8 dereferenceable(32) %__args) local_unnamed_addr #6 comdat align 2 personality i8* bitcast (i32 (...)* @__gxx_personality_v0 to i8*) {
invoke.cont:
  %call2.i.i = tail call noalias nonnull i8* @_Znwm(i64 40) #19
  %_M_nxt.i.i.i = bitcast i8* %call2.i.i to %"struct.std::__detail::_Hash_node_base"**
  store %"struct.std::__detail::_Hash_node_base"* null, %"struct.std::__detail::_Hash_node_base"** %_M_nxt.i.i.i, align 8, !tbaa !22
  %_M_storage.i = getelementptr inbounds i8, i8* %call2.i.i, i64 8
  %first.i.i.i = bitcast i8* %_M_storage.i to i64*
  %first2.i.i.i = getelementptr inbounds %"struct.std::pair.28", %"struct.std::pair.28"* %__args, i64 0, i32 0
  %0 = load i64, i64* %first2.i.i.i, align 8, !tbaa !39
  store i64 %0, i64* %first.i.i.i, align 8, !tbaa !39
  %second.i.i.i = getelementptr inbounds i8, i8* %call2.i.i, i64 16
  %_M_finish.i.i.i.i.i = getelementptr inbounds %"struct.std::pair.28", %"struct.std::pair.28"* %__args, i64 0, i32 1, i32 0, i32 0, i32 1
  %1 = load %struct.GroupByColumn*, %struct.GroupByColumn** %_M_finish.i.i.i.i.i, align 8, !tbaa !45
  %_M_start.i.i.i.i.i = getelementptr inbounds %"struct.std::pair.28", %"struct.std::pair.28"* %__args, i64 0, i32 1, i32 0, i32 0, i32 0
  %2 = load %struct.GroupByColumn*, %struct.GroupByColumn** %_M_start.i.i.i.i.i, align 8, !tbaa !42
  %sub.ptr.lhs.cast.i.i.i.i.i = ptrtoint %struct.GroupByColumn* %1 to i64
  %sub.ptr.rhs.cast.i.i.i.i.i = ptrtoint %struct.GroupByColumn* %2 to i64
  %sub.ptr.sub.i.i.i.i.i = sub i64 %sub.ptr.lhs.cast.i.i.i.i.i, %sub.ptr.rhs.cast.i.i.i.i.i
  %sub.ptr.div.i.i.i.i.i = ashr exact i64 %sub.ptr.sub.i.i.i.i.i, 4
  tail call void @llvm.memset.p0i8.i64(i8* nonnull align 8 dereferenceable(24) %second.i.i.i, i8 0, i64 24, i1 false) #20
  %cmp.not.i.i.i.i.i.i.i = icmp eq i64 %sub.ptr.sub.i.i.i.i.i, 0
  br i1 %cmp.not.i.i.i.i.i.i.i, label %invoke.cont.i.i.i.i, label %cond.true.i.i.i.i.i.i.i

cond.true.i.i.i.i.i.i.i:                          ; preds = %invoke.cont
  %cmp.i.i.i.i.i.i.i.i.i = icmp slt i64 %sub.ptr.sub.i.i.i.i.i, 0
  br i1 %cmp.i.i.i.i.i.i.i.i.i, label %if.then.i.i.i.i.i.i.i.i.i, label %_ZNSt16allocator_traitsISaI13GroupByColumnEE8allocateERS1_m.exit.i.i.i.i.i.i.i

if.then.i.i.i.i.i.i.i.i.i:                        ; preds = %cond.true.i.i.i.i.i.i.i
  invoke void @_ZSt17__throw_bad_allocv() #22
          to label %.noexc unwind label %lpad7

.noexc:                                           ; preds = %if.then.i.i.i.i.i.i.i.i.i
  unreachable

_ZNSt16allocator_traitsISaI13GroupByColumnEE8allocateERS1_m.exit.i.i.i.i.i.i.i: ; preds = %cond.true.i.i.i.i.i.i.i
  %call2.i.i.i.i3.i22.i.i.i.i26 = invoke noalias nonnull i8* @_Znwm(i64 %sub.ptr.sub.i.i.i.i.i) #19
          to label %call2.i.i.i.i3.i22.i.i.i.i.noexc unwind label %lpad7

call2.i.i.i.i3.i22.i.i.i.i.noexc:                 ; preds = %_ZNSt16allocator_traitsISaI13GroupByColumnEE8allocateERS1_m.exit.i.i.i.i.i.i.i
  %3 = bitcast i8* %call2.i.i.i.i3.i22.i.i.i.i26 to %struct.GroupByColumn*
  %.pre = load %struct.GroupByColumn*, %struct.GroupByColumn** %_M_start.i.i.i.i.i, align 8, !tbaa !19
  %.pre27 = load %struct.GroupByColumn*, %struct.GroupByColumn** %_M_finish.i.i.i.i.i, align 8, !tbaa !19
  %.pre28 = ptrtoint %struct.GroupByColumn* %.pre27 to i64
  %.pre29 = ptrtoint %struct.GroupByColumn* %.pre to i64
  %.pre30 = sub i64 %.pre28, %.pre29
  br label %invoke.cont.i.i.i.i

invoke.cont.i.i.i.i:                              ; preds = %call2.i.i.i.i3.i22.i.i.i.i.noexc, %invoke.cont
  %sub.ptr.sub.i.i.i.i.i.i.i.i.i.i.i.pre-phi = phi i64 [ %.pre30, %call2.i.i.i.i3.i22.i.i.i.i.noexc ], [ 0, %invoke.cont ]
  %4 = phi %struct.GroupByColumn* [ %.pre, %call2.i.i.i.i3.i22.i.i.i.i.noexc ], [ %2, %invoke.cont ]
  %cond.i.i.i.i.i.i.i = phi %struct.GroupByColumn* [ %3, %call2.i.i.i.i3.i22.i.i.i.i.noexc ], [ null, %invoke.cont ]
  %_M_start.i.i.i.i.i.i = bitcast i8* %second.i.i.i to %struct.GroupByColumn**
  store %struct.GroupByColumn* %cond.i.i.i.i.i.i.i, %struct.GroupByColumn** %_M_start.i.i.i.i.i.i, align 8, !tbaa !42
  %_M_finish.i.i.i.i.i.i = getelementptr inbounds i8, i8* %call2.i.i, i64 24
  %5 = bitcast i8* %_M_finish.i.i.i.i.i.i to %struct.GroupByColumn**
  store %struct.GroupByColumn* %cond.i.i.i.i.i.i.i, %struct.GroupByColumn** %5, align 8, !tbaa !45
  %add.ptr.i.i.i.i.i.i = getelementptr inbounds %struct.GroupByColumn, %struct.GroupByColumn* %cond.i.i.i.i.i.i.i, i64 %sub.ptr.div.i.i.i.i.i
  %_M_end_of_storage.i.i.i.i.i.i = getelementptr inbounds i8, i8* %call2.i.i, i64 32
  %6 = bitcast i8* %_M_end_of_storage.i.i.i.i.i.i to %struct.GroupByColumn**
  store %struct.GroupByColumn* %add.ptr.i.i.i.i.i.i, %struct.GroupByColumn** %6, align 8, !tbaa !46
  %tobool.not.i.i.i.i.i.i.i.i.i.i.i = icmp eq i64 %sub.ptr.sub.i.i.i.i.i.i.i.i.i.i.i.pre-phi, 0
  br i1 %tobool.not.i.i.i.i.i.i.i.i.i.i.i, label %invoke.cont8, label %if.then.i.i.i.i.i.i.i.i.i.i.i

if.then.i.i.i.i.i.i.i.i.i.i.i:                    ; preds = %invoke.cont.i.i.i.i
  %7 = bitcast %struct.GroupByColumn* %cond.i.i.i.i.i.i.i to i8*
  %8 = bitcast %struct.GroupByColumn* %4 to i8*
  tail call void @llvm.memmove.p0i8.p0i8.i64(i8* align 8 %7, i8* align 8 %8, i64 %sub.ptr.sub.i.i.i.i.i.i.i.i.i.i.i.pre-phi, i1 false) #20
  br label %invoke.cont8

invoke.cont8:                                     ; preds = %if.then.i.i.i.i.i.i.i.i.i.i.i, %invoke.cont.i.i.i.i
  %9 = bitcast i8* %call2.i.i to %"struct.std::__detail::_Hash_node"*
  %sub.ptr.div.i.i.i.i.i.i.i.i.i.i.i = ashr exact i64 %sub.ptr.sub.i.i.i.i.i.i.i.i.i.i.i.pre-phi, 4
  %add.ptr.i.i.i.i.i.i.i.i.i.i.i = getelementptr inbounds %struct.GroupByColumn, %struct.GroupByColumn* %cond.i.i.i.i.i.i.i, i64 %sub.ptr.div.i.i.i.i.i.i.i.i.i.i.i
  store %struct.GroupByColumn* %add.ptr.i.i.i.i.i.i.i.i.i.i.i, %struct.GroupByColumn** %5, align 8, !tbaa !45
  ret %"struct.std::__detail::_Hash_node"* %9

lpad7:                                            ; preds = %_ZNSt16allocator_traitsISaI13GroupByColumnEE8allocateERS1_m.exit.i.i.i.i.i.i.i, %if.then.i.i.i.i.i.i.i.i.i
  %10 = landingpad { i8*, i32 }
          catch i8* null
  %exn.slot.0 = extractvalue { i8*, i32 } %10, 0
  %11 = tail call i8* @__cxa_begin_catch(i8* %exn.slot.0) #20
  tail call void @_ZdlPv(i8* nonnull %call2.i.i) #20
  invoke void @__cxa_rethrow() #22
          to label %unreachable unwind label %lpad9

lpad9:                                            ; preds = %lpad7
  %12 = landingpad { i8*, i32 }
          cleanup
  invoke void @__cxa_end_catch()
          to label %invoke.cont14 unwind label %terminate.lpad

invoke.cont14:                                    ; preds = %lpad9
  resume { i8*, i32 } %12

terminate.lpad:                                   ; preds = %lpad9
  %13 = landingpad { i8*, i32 }
          catch i8* null
  %14 = extractvalue { i8*, i32 } %13, 0
  tail call void @__clang_call_terminate(i8* %14) #21
  unreachable

unreachable:                                      ; preds = %lpad7
  unreachable
}

; Function Attrs: uwtable
define linkonce_odr dso_local { %"struct.std::__detail::_Hash_node"*, i8 } @_ZNSt10_HashtableImSt4pairIKmSt6vectorI13GroupByColumnSaIS3_EEESaIS6_ENSt8__detail10_Select1stESt8equal_toImESt4hashImENS8_18_Mod_range_hashingENS8_20_Default_ranged_hashENS8_20_Prime_rehash_policyENS8_17_Hashtable_traitsILb0ELb0ELb1EEEE10_M_emplaceIJS6_EEES0_INS8_14_Node_iteratorIS6_Lb0ELb0EEEbESt17integral_constantIbLb1EEDpOT_(%"class.std::_Hashtable"* nonnull dereferenceable(56) %this, %"struct.std::pair.28"* nonnull align 8 dereferenceable(32) %__args) local_unnamed_addr #6 comdat align 2 personality i8* bitcast (i32 (...)* @__gxx_personality_v0 to i8*) {
invoke.cont:
  %call2.i.i.i = tail call noalias nonnull i8* @_Znwm(i64 40) #19
  %0 = bitcast i8* %call2.i.i.i to %"struct.std::__detail::_Hash_node"*
  %_M_nxt.i.i.i.i = bitcast i8* %call2.i.i.i to %"struct.std::__detail::_Hash_node_base"**
  store %"struct.std::__detail::_Hash_node_base"* null, %"struct.std::__detail::_Hash_node_base"** %_M_nxt.i.i.i.i, align 8, !tbaa !22
  %_M_storage.i.i = getelementptr inbounds i8, i8* %call2.i.i.i, i64 8
  %first.i.i.i.i = bitcast i8* %_M_storage.i.i to i64*
  %first2.i.i.i.i = getelementptr inbounds %"struct.std::pair.28", %"struct.std::pair.28"* %__args, i64 0, i32 0
  %1 = load i64, i64* %first2.i.i.i.i, align 8, !tbaa !23
  store i64 %1, i64* %first.i.i.i.i, align 8, !tbaa !39
  %second.i.i.i.i = getelementptr inbounds i8, i8* %call2.i.i.i, i64 16
  %_M_start.i.i.i.i.i.i.i = bitcast i8* %second.i.i.i.i to %struct.GroupByColumn**
  %_M_start2.i.i.i.i.i.i.i = getelementptr inbounds %"struct.std::pair.28", %"struct.std::pair.28"* %__args, i64 0, i32 1, i32 0, i32 0, i32 0
  %2 = load %struct.GroupByColumn*, %struct.GroupByColumn** %_M_start2.i.i.i.i.i.i.i, align 8, !tbaa !19
  store %struct.GroupByColumn* %2, %struct.GroupByColumn** %_M_start.i.i.i.i.i.i.i, align 8, !tbaa !19
  %_M_finish.i.i.i.i.i.i.i = getelementptr inbounds i8, i8* %call2.i.i.i, i64 24
  %_M_finish3.i.i.i.i.i.i.i = getelementptr inbounds %"struct.std::pair.28", %"struct.std::pair.28"* %__args, i64 0, i32 1, i32 0, i32 0, i32 1
  %_M_end_of_storage4.i.i.i.i.i.i.i = getelementptr inbounds %"struct.std::pair.28", %"struct.std::pair.28"* %__args, i64 0, i32 1, i32 0, i32 0, i32 2
  %3 = bitcast %struct.GroupByColumn** %_M_finish3.i.i.i.i.i.i.i to <2 x %struct.GroupByColumn*>*
  %4 = load <2 x %struct.GroupByColumn*>, <2 x %struct.GroupByColumn*>* %3, align 8, !tbaa !19
  %5 = bitcast %struct.GroupByColumn** %_M_start2.i.i.i.i.i.i.i to <2 x %struct.GroupByColumn*>*
  store <2 x %struct.GroupByColumn*> zeroinitializer, <2 x %struct.GroupByColumn*>* %5, align 8, !tbaa !19
  %6 = bitcast i8* %_M_finish.i.i.i.i.i.i.i to <2 x %struct.GroupByColumn*>*
  store <2 x %struct.GroupByColumn*> %4, <2 x %struct.GroupByColumn*>* %6, align 8, !tbaa !19
  store %struct.GroupByColumn* null, %struct.GroupByColumn** %_M_end_of_storage4.i.i.i.i.i.i.i, align 8, !tbaa !19
  %_M_bucket_count.i = getelementptr inbounds %"class.std::_Hashtable", %"class.std::_Hashtable"* %this, i64 0, i32 1
  %7 = load i64, i64* %_M_bucket_count.i, align 8, !tbaa !20
  %rem.i.i.i = urem i64 %1, %7
  %_M_buckets.i.i = getelementptr inbounds %"class.std::_Hashtable", %"class.std::_Hashtable"* %this, i64 0, i32 0
  %8 = load %"struct.std::__detail::_Hash_node_base"**, %"struct.std::__detail::_Hash_node_base"*** %_M_buckets.i.i, align 8, !tbaa !21
  %arrayidx.i.i = getelementptr inbounds %"struct.std::__detail::_Hash_node_base"*, %"struct.std::__detail::_Hash_node_base"** %8, i64 %rem.i.i.i
  %9 = load %"struct.std::__detail::_Hash_node_base"*, %"struct.std::__detail::_Hash_node_base"** %arrayidx.i.i, align 8, !tbaa !19
  %tobool.not.i.i = icmp eq %"struct.std::__detail::_Hash_node_base"* %9, null
  br i1 %tobool.not.i.i, label %cleanup.cont, label %if.end.i.i

if.end.i.i:                                       ; preds = %invoke.cont
  %10 = bitcast %"struct.std::__detail::_Hash_node_base"* %9 to %"struct.std::__detail::_Hash_node"**
  %11 = load %"struct.std::__detail::_Hash_node"*, %"struct.std::__detail::_Hash_node"** %10, align 8, !tbaa !22
  %_M_storage.i.i.i.i23.i.i = getelementptr inbounds %"struct.std::__detail::_Hash_node", %"struct.std::__detail::_Hash_node"* %11, i64 0, i32 0, i32 1
  %first.i.i.i.i.i24.i.i = bitcast %"struct.__gnu_cxx::__aligned_buffer"* %_M_storage.i.i.i.i23.i.i to i64*
  %12 = load i64, i64* %first.i.i.i.i.i24.i.i, align 8, !tbaa !23
  %cmp.i.i.i25.i.i = icmp eq i64 %1, %12
  br i1 %cmp.i.i.i25.i.i, label %if.then, label %if.end3.i.i

for.cond.i.i:                                     ; preds = %lor.lhs.false.i.i
  %cmp.i.i.i.i.i = icmp eq i64 %1, %15
  br i1 %cmp.i.i.i.i.i, label %if.then.loopexit, label %if.end3.i.i

if.end3.i.i:                                      ; preds = %if.end.i.i, %for.cond.i.i
  %__p.026.i.i = phi %"struct.std::__detail::_Hash_node"* [ %14, %for.cond.i.i ], [ %11, %if.end.i.i ]
  %_M_nxt4.i.i = getelementptr inbounds %"struct.std::__detail::_Hash_node", %"struct.std::__detail::_Hash_node"* %__p.026.i.i, i64 0, i32 0, i32 0, i32 0
  %13 = load %"struct.std::__detail::_Hash_node_base"*, %"struct.std::__detail::_Hash_node_base"** %_M_nxt4.i.i, align 8, !tbaa !22
  %tobool5.not.i.i = icmp eq %"struct.std::__detail::_Hash_node_base"* %13, null
  %14 = bitcast %"struct.std::__detail::_Hash_node_base"* %13 to %"struct.std::__detail::_Hash_node"*
  br i1 %tobool5.not.i.i, label %cleanup.cont, label %lor.lhs.false.i.i

lor.lhs.false.i.i:                                ; preds = %if.end3.i.i
  %_M_storage.i.i.i.i21.i.i = getelementptr inbounds %"struct.std::__detail::_Hash_node_base", %"struct.std::__detail::_Hash_node_base"* %13, i64 1
  %first.i.i.i.i.i22.i.i = bitcast %"struct.std::__detail::_Hash_node_base"* %_M_storage.i.i.i.i21.i.i to i64*
  %15 = load i64, i64* %first.i.i.i.i.i22.i.i, align 8, !tbaa !23
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
  tail call void @_ZdlPv(i8* nonnull %17) #20
  br label %cleanup

cleanup:                                          ; preds = %if.then.i.i.i.i.i.i.i, %if.then
  tail call void @_ZdlPv(i8* nonnull %call2.i.i.i) #20
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
define linkonce_odr dso_local %"struct.std::__detail::_Hash_node"* @_ZNSt10_HashtableImSt4pairIKmSt6vectorI13GroupByColumnSaIS3_EEESaIS6_ENSt8__detail10_Select1stESt8equal_toImESt4hashImENS8_18_Mod_range_hashingENS8_20_Default_ranged_hashENS8_20_Prime_rehash_policyENS8_17_Hashtable_traitsILb0ELb0ELb1EEEE21_M_insert_unique_nodeEmmPNS8_10_Hash_nodeIS6_Lb0EEE(%"class.std::_Hashtable"* nonnull dereferenceable(56) %this, i64 %__bkt, i64 %__code, %"struct.std::__detail::_Hash_node"* %__node) local_unnamed_addr #6 comdat align 2 personality i8* bitcast (i32 (...)* @__gxx_personality_v0 to i8*) {
entry:
  %_M_rehash_policy = getelementptr inbounds %"class.std::_Hashtable", %"class.std::_Hashtable"* %this, i64 0, i32 4
  %_M_next_resize.i = getelementptr inbounds %"class.std::_Hashtable", %"class.std::_Hashtable"* %this, i64 0, i32 4, i32 1
  %0 = load i64, i64* %_M_next_resize.i, align 8, !tbaa !135
  %_M_bucket_count = getelementptr inbounds %"class.std::_Hashtable", %"class.std::_Hashtable"* %this, i64 0, i32 1
  %1 = load i64, i64* %_M_bucket_count, align 8, !tbaa !20
  %_M_element_count = getelementptr inbounds %"class.std::_Hashtable", %"class.std::_Hashtable"* %this, i64 0, i32 3
  %2 = load i64, i64* %_M_element_count, align 8, !tbaa !94
  %call3 = tail call { i8, i64 } @_ZNKSt8__detail20_Prime_rehash_policy14_M_need_rehashEmmm(%"struct.std::__detail::_Prime_rehash_policy"* nonnull dereferenceable(16) %_M_rehash_policy, i64 %1, i64 %2, i64 1)
  %3 = extractvalue { i8, i64 } %call3, 0
  %4 = and i8 %3, 1
  %tobool.not = icmp eq i8 %4, 0
  br i1 %tobool.not, label %entry.if.end_crit_edge, label %if.then

entry.if.end_crit_edge:                           ; preds = %entry
  %_M_buckets.i.phi.trans.insert = getelementptr inbounds %"class.std::_Hashtable", %"class.std::_Hashtable"* %this, i64 0, i32 0
  %.pre = load %"struct.std::__detail::_Hash_node_base"**, %"struct.std::__detail::_Hash_node_base"*** %_M_buckets.i.phi.trans.insert, align 8, !tbaa !21
  br label %if.end

if.then:                                          ; preds = %entry
  %5 = extractvalue { i8, i64 } %call3, 1
  %cmp.i.i = icmp eq i64 %5, 1
  br i1 %cmp.i.i, label %if.then.i.i, label %if.end.i.i, !prof !133

if.then.i.i:                                      ; preds = %if.then
  %_M_single_bucket.i.i = getelementptr inbounds %"class.std::_Hashtable", %"class.std::_Hashtable"* %this, i64 0, i32 5
  store %"struct.std::__detail::_Hash_node_base"* null, %"struct.std::__detail::_Hash_node_base"** %_M_single_bucket.i.i, align 8, !tbaa !124
  br label %_ZNSt10_HashtableImSt4pairIKmSt6vectorI13GroupByColumnSaIS3_EEESaIS6_ENSt8__detail10_Select1stESt8equal_toImESt4hashImENS8_18_Mod_range_hashingENS8_20_Default_ranged_hashENS8_20_Prime_rehash_policyENS8_17_Hashtable_traitsILb0ELb0ELb1EEEE19_M_allocate_bucketsEm.exit.i

if.end.i.i:                                       ; preds = %if.then
  %cmp.i.i.i.i.i = icmp ugt i64 %5, 2305843009213693951
  br i1 %cmp.i.i.i.i.i, label %if.then.i.i.i.i.i, label %_ZNSt8__detail16_Hashtable_allocISaINS_10_Hash_nodeISt4pairIKmSt6vectorI13GroupByColumnSaIS5_EEELb0EEEEE19_M_allocate_bucketsEm.exit.i.i

if.then.i.i.i.i.i:                                ; preds = %if.end.i.i
  invoke void @_ZSt17__throw_bad_allocv() #22
          to label %.noexc unwind label %lpad.i

.noexc:                                           ; preds = %if.then.i.i.i.i.i
  unreachable

_ZNSt8__detail16_Hashtable_allocISaINS_10_Hash_nodeISt4pairIKmSt6vectorI13GroupByColumnSaIS5_EEELb0EEEEE19_M_allocate_bucketsEm.exit.i.i: ; preds = %if.end.i.i
  %mul.i.i.i.i.i = shl nuw i64 %5, 3
  %call2.i.i10.i.i.i33 = invoke noalias nonnull i8* @_Znwm(i64 %mul.i.i.i.i.i) #19
          to label %call2.i.i10.i.i.i.noexc unwind label %lpad.i

call2.i.i10.i.i.i.noexc:                          ; preds = %_ZNSt8__detail16_Hashtable_allocISaINS_10_Hash_nodeISt4pairIKmSt6vectorI13GroupByColumnSaIS5_EEELb0EEEEE19_M_allocate_bucketsEm.exit.i.i
  %6 = bitcast i8* %call2.i.i10.i.i.i33 to %"struct.std::__detail::_Hash_node_base"**
  tail call void @llvm.memset.p0i8.i64(i8* nonnull align 8 %call2.i.i10.i.i.i33, i8 0, i64 %mul.i.i.i.i.i, i1 false)
  br label %_ZNSt10_HashtableImSt4pairIKmSt6vectorI13GroupByColumnSaIS3_EEESaIS6_ENSt8__detail10_Select1stESt8equal_toImESt4hashImENS8_18_Mod_range_hashingENS8_20_Default_ranged_hashENS8_20_Prime_rehash_policyENS8_17_Hashtable_traitsILb0ELb0ELb1EEEE19_M_allocate_bucketsEm.exit.i

_ZNSt10_HashtableImSt4pairIKmSt6vectorI13GroupByColumnSaIS3_EEESaIS6_ENSt8__detail10_Select1stESt8equal_toImESt4hashImENS8_18_Mod_range_hashingENS8_20_Default_ranged_hashENS8_20_Prime_rehash_policyENS8_17_Hashtable_traitsILb0ELb0ELb1EEEE19_M_allocate_bucketsEm.exit.i: ; preds = %call2.i.i10.i.i.i.noexc, %if.then.i.i
  %retval.0.i.i = phi %"struct.std::__detail::_Hash_node_base"** [ %_M_single_bucket.i.i, %if.then.i.i ], [ %6, %call2.i.i10.i.i.i.noexc ]
  %_M_nxt.i.i = getelementptr inbounds %"class.std::_Hashtable", %"class.std::_Hashtable"* %this, i64 0, i32 2, i32 0
  %7 = bitcast %"struct.std::__detail::_Hash_node_base"** %_M_nxt.i.i to %"struct.std::__detail::_Hash_node"**
  %8 = load %"struct.std::__detail::_Hash_node"*, %"struct.std::__detail::_Hash_node"** %7, align 8, !tbaa !95
  %_M_before_begin.i = getelementptr inbounds %"class.std::_Hashtable", %"class.std::_Hashtable"* %this, i64 0, i32 2
  %_M_nxt.i = getelementptr inbounds %"struct.std::__detail::_Hash_node_base", %"struct.std::__detail::_Hash_node_base"* %_M_before_begin.i, i64 0, i32 0
  store %"struct.std::__detail::_Hash_node_base"* null, %"struct.std::__detail::_Hash_node_base"** %_M_nxt.i, align 8, !tbaa !95
  %tobool.not47.i = icmp eq %"struct.std::__detail::_Hash_node"* %8, null
  br i1 %tobool.not47.i, label %while.end.i, label %while.body.i

while.body.i:                                     ; preds = %_ZNSt10_HashtableImSt4pairIKmSt6vectorI13GroupByColumnSaIS3_EEESaIS6_ENSt8__detail10_Select1stESt8equal_toImESt4hashImENS8_18_Mod_range_hashingENS8_20_Default_ranged_hashENS8_20_Prime_rehash_policyENS8_17_Hashtable_traitsILb0ELb0ELb1EEEE19_M_allocate_bucketsEm.exit.i, %if.end22.i
  %__p.049.i = phi %"struct.std::__detail::_Hash_node"* [ %10, %if.end22.i ], [ %8, %_ZNSt10_HashtableImSt4pairIKmSt6vectorI13GroupByColumnSaIS3_EEESaIS6_ENSt8__detail10_Select1stESt8equal_toImESt4hashImENS8_18_Mod_range_hashingENS8_20_Default_ranged_hashENS8_20_Prime_rehash_policyENS8_17_Hashtable_traitsILb0ELb0ELb1EEEE19_M_allocate_bucketsEm.exit.i ]
  %__bbegin_bkt.048.i = phi i64 [ %__bbegin_bkt.1.i, %if.end22.i ], [ 0, %_ZNSt10_HashtableImSt4pairIKmSt6vectorI13GroupByColumnSaIS3_EEESaIS6_ENSt8__detail10_Select1stESt8equal_toImESt4hashImENS8_18_Mod_range_hashingENS8_20_Default_ranged_hashENS8_20_Prime_rehash_policyENS8_17_Hashtable_traitsILb0ELb0ELb1EEEE19_M_allocate_bucketsEm.exit.i ]
  %9 = bitcast %"struct.std::__detail::_Hash_node"* %__p.049.i to %"struct.std::__detail::_Hash_node"**
  %10 = load %"struct.std::__detail::_Hash_node"*, %"struct.std::__detail::_Hash_node"** %9, align 8, !tbaa !22
  %_M_storage.i.i.i.i = getelementptr inbounds %"struct.std::__detail::_Hash_node", %"struct.std::__detail::_Hash_node"* %__p.049.i, i64 0, i32 0, i32 1
  %first.i.i.i.i.i = bitcast %"struct.__gnu_cxx::__aligned_buffer"* %_M_storage.i.i.i.i to i64*
  %11 = load i64, i64* %first.i.i.i.i.i, align 8, !tbaa !23
  %rem.i.i.i31 = urem i64 %11, %5
  %arrayidx.i = getelementptr inbounds %"struct.std::__detail::_Hash_node_base"*, %"struct.std::__detail::_Hash_node_base"** %retval.0.i.i, i64 %rem.i.i.i31
  %12 = load %"struct.std::__detail::_Hash_node_base"*, %"struct.std::__detail::_Hash_node_base"** %arrayidx.i, align 8, !tbaa !19
  %tobool5.not.i = icmp eq %"struct.std::__detail::_Hash_node_base"* %12, null
  br i1 %tobool5.not.i, label %if.then.i, label %if.else.i

if.then.i:                                        ; preds = %while.body.i
  %13 = load %"struct.std::__detail::_Hash_node_base"*, %"struct.std::__detail::_Hash_node_base"** %_M_nxt.i, align 8, !tbaa !95
  %14 = getelementptr %"struct.std::__detail::_Hash_node", %"struct.std::__detail::_Hash_node"* %__p.049.i, i64 0, i32 0, i32 0
  %_M_nxt8.i = getelementptr inbounds %"struct.std::__detail::_Hash_node", %"struct.std::__detail::_Hash_node"* %__p.049.i, i64 0, i32 0, i32 0, i32 0
  store %"struct.std::__detail::_Hash_node_base"* %13, %"struct.std::__detail::_Hash_node_base"** %_M_nxt8.i, align 8, !tbaa !22
  store %"struct.std::__detail::_Hash_node_base"* %14, %"struct.std::__detail::_Hash_node_base"** %_M_nxt.i, align 8, !tbaa !95
  store %"struct.std::__detail::_Hash_node_base"* %_M_before_begin.i, %"struct.std::__detail::_Hash_node_base"** %arrayidx.i, align 8, !tbaa !19
  %15 = load %"struct.std::__detail::_Hash_node_base"*, %"struct.std::__detail::_Hash_node_base"** %_M_nxt8.i, align 8, !tbaa !22
  %tobool14.not.i = icmp eq %"struct.std::__detail::_Hash_node_base"* %15, null
  br i1 %tobool14.not.i, label %if.end22.i, label %if.then15.i

if.then15.i:                                      ; preds = %if.then.i
  %arrayidx16.i = getelementptr inbounds %"struct.std::__detail::_Hash_node_base"*, %"struct.std::__detail::_Hash_node_base"** %retval.0.i.i, i64 %__bbegin_bkt.048.i
  store %"struct.std::__detail::_Hash_node_base"* %14, %"struct.std::__detail::_Hash_node_base"** %arrayidx16.i, align 8, !tbaa !19
  br label %if.end22.i

if.else.i:                                        ; preds = %while.body.i
  %_M_nxt18.i = getelementptr inbounds %"struct.std::__detail::_Hash_node_base", %"struct.std::__detail::_Hash_node_base"* %12, i64 0, i32 0
  %16 = load %"struct.std::__detail::_Hash_node_base"*, %"struct.std::__detail::_Hash_node_base"** %_M_nxt18.i, align 8, !tbaa !22
  %17 = getelementptr %"struct.std::__detail::_Hash_node", %"struct.std::__detail::_Hash_node"* %__p.049.i, i64 0, i32 0, i32 0
  %_M_nxt19.i = getelementptr inbounds %"struct.std::__detail::_Hash_node", %"struct.std::__detail::_Hash_node"* %__p.049.i, i64 0, i32 0, i32 0, i32 0
  store %"struct.std::__detail::_Hash_node_base"* %16, %"struct.std::__detail::_Hash_node_base"** %_M_nxt19.i, align 8, !tbaa !22
  %18 = load %"struct.std::__detail::_Hash_node_base"*, %"struct.std::__detail::_Hash_node_base"** %arrayidx.i, align 8, !tbaa !19
  %_M_nxt21.i = getelementptr inbounds %"struct.std::__detail::_Hash_node_base", %"struct.std::__detail::_Hash_node_base"* %18, i64 0, i32 0
  store %"struct.std::__detail::_Hash_node_base"* %17, %"struct.std::__detail::_Hash_node_base"** %_M_nxt21.i, align 8, !tbaa !22
  br label %if.end22.i

if.end22.i:                                       ; preds = %if.else.i, %if.then15.i, %if.then.i
  %__bbegin_bkt.1.i = phi i64 [ %__bbegin_bkt.048.i, %if.else.i ], [ %rem.i.i.i31, %if.then15.i ], [ %rem.i.i.i31, %if.then.i ]
  %tobool.not.i = icmp eq %"struct.std::__detail::_Hash_node"* %10, null
  br i1 %tobool.not.i, label %while.end.i, label %while.body.i, !llvm.loop !136

while.end.i:                                      ; preds = %if.end22.i, %_ZNSt10_HashtableImSt4pairIKmSt6vectorI13GroupByColumnSaIS3_EEESaIS6_ENSt8__detail10_Select1stESt8equal_toImESt4hashImENS8_18_Mod_range_hashingENS8_20_Default_ranged_hashENS8_20_Prime_rehash_policyENS8_17_Hashtable_traitsILb0ELb0ELb1EEEE19_M_allocate_bucketsEm.exit.i
  %_M_buckets.i.i = getelementptr inbounds %"class.std::_Hashtable", %"class.std::_Hashtable"* %this, i64 0, i32 0
  %19 = load %"struct.std::__detail::_Hash_node_base"**, %"struct.std::__detail::_Hash_node_base"*** %_M_buckets.i.i, align 8, !tbaa !21
  %_M_single_bucket.i.i.i.i = getelementptr inbounds %"class.std::_Hashtable", %"class.std::_Hashtable"* %this, i64 0, i32 5
  %cmp.i.i.i.i = icmp eq %"struct.std::__detail::_Hash_node_base"** %_M_single_bucket.i.i.i.i, %19
  br i1 %cmp.i.i.i.i, label %invoke.cont4, label %if.end.i.i.i

if.end.i.i.i:                                     ; preds = %while.end.i
  %20 = bitcast %"struct.std::__detail::_Hash_node_base"** %19 to i8*
  tail call void @_ZdlPv(i8* %20) #20
  br label %invoke.cont4

lpad.i:                                           ; preds = %_ZNSt8__detail16_Hashtable_allocISaINS_10_Hash_nodeISt4pairIKmSt6vectorI13GroupByColumnSaIS5_EEELb0EEEEE19_M_allocate_bucketsEm.exit.i.i, %if.then.i.i.i.i.i
  %21 = landingpad { i8*, i32 }
          catch i8* null
  %22 = extractvalue { i8*, i32 } %21, 0
  %23 = tail call i8* @__cxa_begin_catch(i8* %22) #20
  store i64 %0, i64* %_M_next_resize.i, align 8, !tbaa !135
  invoke void @__cxa_rethrow() #22
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
  tail call void @__clang_call_terminate(i8* %26) #21
  unreachable

unreachable.i:                                    ; preds = %lpad.i
  unreachable

invoke.cont4:                                     ; preds = %if.end.i.i.i, %while.end.i
  store i64 %5, i64* %_M_bucket_count, align 8, !tbaa !20
  store %"struct.std::__detail::_Hash_node_base"** %retval.0.i.i, %"struct.std::__detail::_Hash_node_base"*** %_M_buckets.i.i, align 8, !tbaa !21
  %rem.i.i.i = urem i64 %__code, %5
  br label %if.end

lpad.body:                                        ; preds = %lpad2.i
  %27 = extractvalue { i8*, i32 } %24, 0
  %28 = tail call i8* @__cxa_begin_catch(i8* %27) #20
  %_M_start.i.i.i.i.i.i = getelementptr inbounds %"struct.std::__detail::_Hash_node", %"struct.std::__detail::_Hash_node"* %__node, i64 0, i32 0, i32 1, i32 0, i32 0, i64 8
  %29 = bitcast i8* %_M_start.i.i.i.i.i.i to %struct.GroupByColumn**
  %30 = load %struct.GroupByColumn*, %struct.GroupByColumn** %29, align 8, !tbaa !42
  %tobool.not.i.i.i.i.i.i.i = icmp eq %struct.GroupByColumn* %30, null
  br i1 %tobool.not.i.i.i.i.i.i.i, label %invoke.cont14, label %if.then.i.i.i.i.i.i.i

if.then.i.i.i.i.i.i.i:                            ; preds = %lpad.body
  %31 = bitcast %struct.GroupByColumn* %30 to i8*
  tail call void @_ZdlPv(i8* nonnull %31) #20
  br label %invoke.cont14

invoke.cont14:                                    ; preds = %if.then.i.i.i.i.i.i.i, %lpad.body
  %32 = bitcast %"struct.std::__detail::_Hash_node"* %__node to i8*
  tail call void @_ZdlPv(i8* %32) #20
  invoke void @__cxa_rethrow() #22
          to label %unreachable unwind label %lpad13

if.end:                                           ; preds = %entry.if.end_crit_edge, %invoke.cont4
  %33 = phi %"struct.std::__detail::_Hash_node_base"** [ %.pre, %entry.if.end_crit_edge ], [ %retval.0.i.i, %invoke.cont4 ]
  %__bkt.addr.0 = phi i64 [ %__bkt, %entry.if.end_crit_edge ], [ %rem.i.i.i, %invoke.cont4 ]
  %_M_buckets.i = getelementptr inbounds %"class.std::_Hashtable", %"class.std::_Hashtable"* %this, i64 0, i32 0
  %arrayidx.i34 = getelementptr inbounds %"struct.std::__detail::_Hash_node_base"*, %"struct.std::__detail::_Hash_node_base"** %33, i64 %__bkt.addr.0
  %34 = load %"struct.std::__detail::_Hash_node_base"*, %"struct.std::__detail::_Hash_node_base"** %arrayidx.i34, align 8, !tbaa !19
  %tobool.not.i35 = icmp eq %"struct.std::__detail::_Hash_node_base"* %34, null
  br i1 %tobool.not.i35, label %if.else.i40, label %if.then.i37

if.then.i37:                                      ; preds = %if.end
  %_M_nxt.i36 = getelementptr inbounds %"struct.std::__detail::_Hash_node_base", %"struct.std::__detail::_Hash_node_base"* %34, i64 0, i32 0
  %35 = load %"struct.std::__detail::_Hash_node_base"*, %"struct.std::__detail::_Hash_node_base"** %_M_nxt.i36, align 8, !tbaa !22
  %36 = getelementptr %"struct.std::__detail::_Hash_node", %"struct.std::__detail::_Hash_node"* %__node, i64 0, i32 0, i32 0
  %_M_nxt4.i = getelementptr inbounds %"struct.std::__detail::_Hash_node", %"struct.std::__detail::_Hash_node"* %__node, i64 0, i32 0, i32 0, i32 0
  store %"struct.std::__detail::_Hash_node_base"* %35, %"struct.std::__detail::_Hash_node_base"** %_M_nxt4.i, align 8, !tbaa !22
  %37 = load %"struct.std::__detail::_Hash_node_base"*, %"struct.std::__detail::_Hash_node_base"** %arrayidx.i34, align 8, !tbaa !19
  %_M_nxt7.i = getelementptr inbounds %"struct.std::__detail::_Hash_node_base", %"struct.std::__detail::_Hash_node_base"* %37, i64 0, i32 0
  store %"struct.std::__detail::_Hash_node_base"* %36, %"struct.std::__detail::_Hash_node_base"** %_M_nxt7.i, align 8, !tbaa !22
  br label %_ZNSt10_HashtableImSt4pairIKmSt6vectorI13GroupByColumnSaIS3_EEESaIS6_ENSt8__detail10_Select1stESt8equal_toImESt4hashImENS8_18_Mod_range_hashingENS8_20_Default_ranged_hashENS8_20_Prime_rehash_policyENS8_17_Hashtable_traitsILb0ELb0ELb1EEEE22_M_insert_bucket_beginEmPNS8_10_Hash_nodeIS6_Lb0EEE.exit

if.else.i40:                                      ; preds = %if.end
  %_M_before_begin.i38 = getelementptr inbounds %"class.std::_Hashtable", %"class.std::_Hashtable"* %this, i64 0, i32 2
  %_M_nxt8.i39 = getelementptr inbounds %"struct.std::__detail::_Hash_node_base", %"struct.std::__detail::_Hash_node_base"* %_M_before_begin.i38, i64 0, i32 0
  %38 = load %"struct.std::__detail::_Hash_node_base"*, %"struct.std::__detail::_Hash_node_base"** %_M_nxt8.i39, align 8, !tbaa !95
  %39 = getelementptr %"struct.std::__detail::_Hash_node", %"struct.std::__detail::_Hash_node"* %__node, i64 0, i32 0, i32 0
  %_M_nxt9.i = getelementptr inbounds %"struct.std::__detail::_Hash_node", %"struct.std::__detail::_Hash_node"* %__node, i64 0, i32 0, i32 0, i32 0
  store %"struct.std::__detail::_Hash_node_base"* %38, %"struct.std::__detail::_Hash_node_base"** %_M_nxt9.i, align 8, !tbaa !22
  store %"struct.std::__detail::_Hash_node_base"* %39, %"struct.std::__detail::_Hash_node_base"** %_M_nxt8.i39, align 8, !tbaa !95
  %40 = load %"struct.std::__detail::_Hash_node_base"*, %"struct.std::__detail::_Hash_node_base"** %_M_nxt9.i, align 8, !tbaa !22
  %tobool13.not.i = icmp eq %"struct.std::__detail::_Hash_node_base"* %40, null
  br i1 %tobool13.not.i, label %if.end.i, label %if.then14.i

if.then14.i:                                      ; preds = %if.else.i40
  %41 = load i64, i64* %_M_bucket_count, align 8, !tbaa !20
  %_M_storage.i.i.i.i.i = getelementptr inbounds %"struct.std::__detail::_Hash_node_base", %"struct.std::__detail::_Hash_node_base"* %40, i64 1
  %first.i.i.i.i.i.i = bitcast %"struct.std::__detail::_Hash_node_base"* %_M_storage.i.i.i.i.i to i64*
  %42 = load i64, i64* %first.i.i.i.i.i.i, align 8, !tbaa !23
  %rem.i.i.i.i = urem i64 %42, %41
  %arrayidx17.i = getelementptr inbounds %"struct.std::__detail::_Hash_node_base"*, %"struct.std::__detail::_Hash_node_base"** %33, i64 %rem.i.i.i.i
  store %"struct.std::__detail::_Hash_node_base"* %39, %"struct.std::__detail::_Hash_node_base"** %arrayidx17.i, align 8, !tbaa !19
  %.pre.i = load %"struct.std::__detail::_Hash_node_base"**, %"struct.std::__detail::_Hash_node_base"*** %_M_buckets.i, align 8, !tbaa !21
  br label %if.end.i

if.end.i:                                         ; preds = %if.then14.i, %if.else.i40
  %43 = phi %"struct.std::__detail::_Hash_node_base"** [ %.pre.i, %if.then14.i ], [ %33, %if.else.i40 ]
  %arrayidx20.i = getelementptr inbounds %"struct.std::__detail::_Hash_node_base"*, %"struct.std::__detail::_Hash_node_base"** %43, i64 %__bkt.addr.0
  store %"struct.std::__detail::_Hash_node_base"* %_M_before_begin.i38, %"struct.std::__detail::_Hash_node_base"** %arrayidx20.i, align 8, !tbaa !19
  br label %_ZNSt10_HashtableImSt4pairIKmSt6vectorI13GroupByColumnSaIS3_EEESaIS6_ENSt8__detail10_Select1stESt8equal_toImESt4hashImENS8_18_Mod_range_hashingENS8_20_Default_ranged_hashENS8_20_Prime_rehash_policyENS8_17_Hashtable_traitsILb0ELb0ELb1EEEE22_M_insert_bucket_beginEmPNS8_10_Hash_nodeIS6_Lb0EEE.exit

_ZNSt10_HashtableImSt4pairIKmSt6vectorI13GroupByColumnSaIS3_EEESaIS6_ENSt8__detail10_Select1stESt8equal_toImESt4hashImENS8_18_Mod_range_hashingENS8_20_Default_ranged_hashENS8_20_Prime_rehash_policyENS8_17_Hashtable_traitsILb0ELb0ELb1EEEE22_M_insert_bucket_beginEmPNS8_10_Hash_nodeIS6_Lb0EEE.exit: ; preds = %if.then.i37, %if.end.i
  %44 = load i64, i64* %_M_element_count, align 8, !tbaa !94
  %inc = add i64 %44, 1
  store i64 %inc, i64* %_M_element_count, align 8, !tbaa !94
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
  tail call void @__clang_call_terminate(i8* %47) #21
  unreachable

unreachable:                                      ; preds = %invoke.cont14
  unreachable
}

declare dso_local { i8, i64 } @_ZNKSt8__detail20_Prime_rehash_policy14_M_need_rehashEmmm(%"struct.std::__detail::_Prime_rehash_policy"* nonnull dereferenceable(16), i64, i64, i64) local_unnamed_addr #0

; Function Attrs: uwtable
define internal void @_GLOBAL__sub_I_hash_groupby.cpp() #6 section ".text.startup" {
entry:
  tail call void @_ZNSt8ios_base4InitC1Ev(%"class.std::ios_base::Init"* nonnull dereferenceable(1) @_ZStL8__ioinit)
  %0 = tail call i32 @__cxa_atexit(void (i8*)* bitcast (void (%"class.std::ios_base::Init"*)* @_ZNSt8ios_base4InitD1Ev to void (i8*)*), i8* getelementptr inbounds (%"class.std::ios_base::Init", %"class.std::ios_base::Init"* @_ZStL8__ioinit, i64 0, i32 0), i8* nonnull @__dso_handle) #20
  ret void
}

; Function Attrs: nofree nounwind
declare noundef i32 @putchar(i32 noundef) local_unnamed_addr #2

attributes #0 = { "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "no-infs-fp-math"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #1 = { nounwind "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "no-infs-fp-math"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #2 = { nofree nounwind }
attributes #3 = { nofree uwtable mustprogress "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "min-legal-vector-width"="0" "no-infs-fp-math"="false" "no-jump-tables"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #4 = { nofree nosync nounwind readnone speculatable willreturn }
attributes #5 = { nobuiltin nofree allocsize(0) "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "no-infs-fp-math"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #6 = { uwtable "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "min-legal-vector-width"="0" "no-infs-fp-math"="false" "no-jump-tables"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #7 = { argmemonly nofree nosync nounwind willreturn }
attributes #8 = { norecurse nounwind readnone uwtable willreturn mustprogress "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "min-legal-vector-width"="0" "no-infs-fp-math"="false" "no-jump-tables"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #9 = { uwtable mustprogress "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "min-legal-vector-width"="0" "no-infs-fp-math"="false" "no-jump-tables"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #10 = { nofree nounwind "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "no-infs-fp-math"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #11 = { argmemonly nofree nosync nounwind willreturn writeonly }
attributes #12 = { nobuiltin nounwind "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "no-infs-fp-math"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #13 = { nounwind uwtable willreturn mustprogress "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "min-legal-vector-width"="0" "no-infs-fp-math"="false" "no-jump-tables"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #14 = { nounwind uwtable "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "min-legal-vector-width"="0" "no-infs-fp-math"="false" "no-jump-tables"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #15 = { noinline noreturn nounwind }
attributes #16 = { noreturn "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "no-infs-fp-math"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #17 = { nounwind uwtable willreturn "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "min-legal-vector-width"="0" "no-infs-fp-math"="false" "no-jump-tables"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #18 = { builtin allocsize(0) }
attributes #19 = { allocsize(0) }
attributes #20 = { nounwind }
attributes #21 = { noreturn nounwind }
attributes #22 = { noreturn }
attributes #23 = { builtin nounwind }

!llvm.module.flags = !{!0}
!llvm.ident = !{!1}

!0 = !{i32 1, !"wchar_size", i32 4}
!1 = !{!"Ubuntu clang version 12.0.0-++20210301072539+98f06b16a313-1~exp1~20210301183256.51"}
!2 = !{!3, !5, i64 8}
!3 = !{!"_ZTSSt12_Vector_baseI11ColumnIndexSaIS0_EE", !4, i64 0}
!4 = !{!"_ZTSNSt12_Vector_baseI11ColumnIndexSaIS0_EE12_Vector_implE", !5, i64 0, !5, i64 8, !5, i64 16}
!5 = !{!"any pointer", !6, i64 0}
!6 = !{!"omnipotent char", !7, i64 0}
!7 = !{!"Simple C++ TBAA"}
!8 = !{!3, !5, i64 0}
!9 = !{!10, !5, i64 136}
!10 = !{!"_ZTS11HashGroupBy", !11, i64 8, !11, i64 32, !12, i64 56, !13, i64 80, !5, i64 136}
!11 = !{!"_ZTSSt6vectorI11ColumnIndexSaIS0_EE"}
!12 = !{!"_ZTSSt6vectorIP10AggregatorSaIS1_EE"}
!13 = !{!"_ZTSSt13unordered_mapImSt6vectorI13GroupByColumnSaIS1_EESt4hashImESt8equal_toImESaISt4pairIKmS3_EEE", !14, i64 0}
!14 = !{!"_ZTSSt10_HashtableImSt4pairIKmSt6vectorI13GroupByColumnSaIS3_EEESaIS6_ENSt8__detail10_Select1stESt8equal_toImESt4hashImENS8_18_Mod_range_hashingENS8_20_Default_ranged_hashENS8_20_Prime_rehash_policyENS8_17_Hashtable_traitsILb0ELb0ELb1EEEE", !5, i64 0, !15, i64 8, !16, i64 16, !15, i64 24, !17, i64 32, !5, i64 48}
!15 = !{!"long", !6, i64 0}
!16 = !{!"_ZTSNSt8__detail15_Hash_node_baseE", !5, i64 0}
!17 = !{!"_ZTSNSt8__detail20_Prime_rehash_policyE", !18, i64 0, !15, i64 8}
!18 = !{!"float", !6, i64 0}
!19 = !{!5, !5, i64 0}
!20 = !{!14, !15, i64 8}
!21 = !{!14, !5, i64 0}
!22 = !{!16, !5, i64 0}
!23 = !{!15, !15, i64 0}
!24 = !{i64 0, i64 4, !25, i64 4, i64 4, !27}
!25 = !{!26, !26, i64 0}
!26 = !{!"int", !6, i64 0}
!27 = !{!28, !28, i64 0}
!28 = !{!"_ZTSN3opt10ColumnTypeE", !6, i64 0}
!29 = !{!30, !5, i64 0}
!30 = !{!"_ZTSSt12_Vector_baseIP6ColumnSaIS1_EE", !31, i64 0}
!31 = !{!"_ZTSNSt12_Vector_baseIP6ColumnSaIS1_EE12_Vector_implE", !5, i64 0, !5, i64 8, !5, i64 16}
!32 = !{!33, !28, i64 24}
!33 = !{!"_ZTS6Column", !5, i64 8, !5, i64 16, !28, i64 24, !15, i64 32}
!34 = !{!33, !5, i64 8}
!35 = !{i64 0, i64 4, !27, i64 8, i64 8, !19}
!36 = !{i64 0, i64 8, !19}
!37 = !{!38, !38, i64 0}
!38 = !{!"double", !6, i64 0}
!39 = !{!40, !15, i64 0}
!40 = !{!"_ZTSSt4pairIKmSt6vectorI13GroupByColumnSaIS2_EEE", !15, i64 0, !41, i64 8}
!41 = !{!"_ZTSSt6vectorI13GroupByColumnSaIS0_EE"}
!42 = !{!43, !5, i64 0}
!43 = !{!"_ZTSSt12_Vector_baseI13GroupByColumnSaIS0_EE", !44, i64 0}
!44 = !{!"_ZTSNSt12_Vector_baseI13GroupByColumnSaIS0_EE12_Vector_implE", !5, i64 0, !5, i64 8, !5, i64 16}
!45 = !{!43, !5, i64 8}
!46 = !{!43, !5, i64 16}
!47 = !{!48, !5, i64 0}
!48 = !{!"_ZTSSt12_Vector_baseIP10AggregatorSaIS1_EE", !49, i64 0}
!49 = !{!"_ZTSNSt12_Vector_baseIP10AggregatorSaIS1_EE12_Vector_implE", !5, i64 0, !5, i64 8, !5, i64 16}
!50 = !{!51, !51, i64 0}
!51 = !{!"vtable pointer", !7, i64 0}
!52 = distinct !{!52, !53}
!53 = !{!"llvm.loop.mustprogress"}
!54 = distinct !{!54, !53}
!55 = distinct !{!55, !53}
!56 = distinct !{!56, !53}
!57 = !{!30, !5, i64 8}
!58 = distinct !{!58, !53}
!59 = distinct !{!59, !60}
!60 = !{!"llvm.loop.unroll.disable"}
!61 = !{!62, !5, i64 40}
!62 = !{!"_ZTS5Table", !63, i64 8, !64, i64 16, !5, i64 40, !26, i64 48, !26, i64 52, !26, i64 56}
!63 = !{!"_ZTS6Layout"}
!64 = !{!"_ZTSSt6vectorIP6ColumnSaIS1_EE"}
!65 = !{!66, !26, i64 0}
!66 = !{!"_ZTS11ColumnIndex", !26, i64 0, !28, i64 4}
!67 = distinct !{!67, !53}
!68 = !{!69, !70, i64 8}
!69 = !{!"_ZTS10Aggregator", !70, i64 8, !26, i64 12, !13, i64 16}
!70 = !{!"_ZTS13AggregateType", !6, i64 0}
!71 = distinct !{!71, !53}
!72 = distinct !{!72, !53}
!73 = !{!62, !26, i64 48}
!74 = !{!75, !18, i64 0}
!75 = !{!"_ZTSNSt6chrono8durationIfSt5ratioILl1ELl1EEEE", !18, i64 0}
!76 = !{!77, !5, i64 0}
!77 = !{!"_ZTSNSt8__detail19_Node_iterator_baseISt4pairIKmSt6vectorI13GroupByColumnSaIS4_EEELb0EEE", !5, i64 0}
!78 = !{!79, !5, i64 8}
!79 = !{!"_ZTS13GroupByColumn", !28, i64 0, !5, i64 8}
!80 = distinct !{!80, !53}
!81 = distinct !{!81, !53}
!82 = !{!33, !15, i64 32}
!83 = !{!62, !26, i64 56}
!84 = !{!30, !5, i64 16}
!85 = distinct !{!85, !53}
!86 = !{!87, !5, i64 0}
!87 = !{!"_ZTSSt12_Vector_baseINSt8__detail14_Node_iteratorISt4pairIKmSt6vectorI13GroupByColumnSaIS5_EEELb0ELb0EEESaIS9_EE", !88, i64 0}
!88 = !{!"_ZTSNSt12_Vector_baseINSt8__detail14_Node_iteratorISt4pairIKmSt6vectorI13GroupByColumnSaIS5_EEELb0ELb0EEESaIS9_EE12_Vector_implE", !5, i64 0, !5, i64 8, !5, i64 16}
!89 = distinct !{!89, !53}
!90 = distinct !{!90, !53}
!91 = distinct !{!91, !53}
!92 = distinct !{!92, !53}
!93 = !{!66, !28, i64 4}
!94 = !{!14, !15, i64 24}
!95 = !{!14, !5, i64 16}
!96 = !{!87, !5, i64 8}
!97 = !{!98}
!98 = distinct !{!98, !99}
!99 = distinct !{!99, !"LVerDomain"}
!100 = !{!101}
!101 = distinct !{!101, !99}
!102 = distinct !{!102, !53, !103}
!103 = !{!"llvm.loop.isvectorized", i32 1}
!104 = distinct !{!104, !60}
!105 = distinct !{!105, !53, !103}
!106 = !{!107}
!107 = distinct !{!107, !108}
!108 = distinct !{!108, !"LVerDomain"}
!109 = !{!110}
!110 = distinct !{!110, !108}
!111 = distinct !{!111, !53, !103}
!112 = distinct !{!112, !60}
!113 = distinct !{!113, !53, !103}
!114 = !{!87, !5, i64 16}
!115 = !{!62, !26, i64 52}
!116 = !{!117, !5, i64 8}
!117 = !{!"_ZTSSt12_Vector_baseIP5TableSaIS1_EE", !118, i64 0}
!118 = !{!"_ZTSNSt12_Vector_baseIP5TableSaIS1_EE12_Vector_implE", !5, i64 0, !5, i64 8, !5, i64 16}
!119 = !{!117, !5, i64 16}
!120 = !{!117, !5, i64 0}
!121 = distinct !{!121, !53}
!122 = !{i64 0, i64 4, !123, i64 8, i64 8, !23}
!123 = !{!18, !18, i64 0}
!124 = !{!14, !5, i64 48}
!125 = !{!126, !5, i64 0}
!126 = !{!"_ZTSZNSt10_HashtableImSt4pairIKmSt6vectorI13GroupByColumnSaIS3_EEESaIS6_ENSt8__detail10_Select1stESt8equal_toImESt4hashImENS8_18_Mod_range_hashingENS8_20_Default_ranged_hashENS8_20_Prime_rehash_policyENS8_17_Hashtable_traitsILb0ELb0ELb1EEEEC1ERKSJ_EUlPKNS8_10_Hash_nodeIS6_Lb0EEEE_", !5, i64 0}
!127 = distinct !{!127, !53}
!128 = !{!3, !5, i64 16}
!129 = !{!48, !5, i64 8}
!130 = !{!48, !5, i64 16}
!131 = !{!17, !18, i64 0}
!132 = !{!79, !28, i64 0}
!133 = !{!"branch_weights", i32 1, i32 2000}
!134 = distinct !{!134, !53}
!135 = !{!17, !15, i64 8}
!136 = distinct !{!136, !53}
