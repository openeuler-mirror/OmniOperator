; ModuleID = '/usr/code/olk_dev/omni_runtime_joy/omni-cache/joy/cpp/src/operator/ir/../test.cpp'
source_filename = "/usr/code/olk_dev/omni_runtime_joy/omni-cache/joy/cpp/src/operator/ir/../test.cpp"
target datalayout = "e-m:e-p270:32:32-p271:32:32-p272:64:64-i64:64-f80:128-n8:16:32:64-S128"
target triple = "x86_64-pc-linux-gnu"

%"class.std::ios_base::Init" = type { i8 }
%"class.std::basic_ostream" = type { i32 (...)**, %"class.std::basic_ios" }
%"class.std::basic_ios" = type { %"class.std::ios_base", %"class.std::basic_ostream"*, i8, i8, %"class.std::basic_streambuf"*, %"class.std::ctype"*, %"class.std::num_put"*, %"class.std::num_get"* }
%"class.std::ios_base" = type { i32 (...)**, i64, i64, i32, i32, i32, %"struct.std::ios_base::_Callback_list"*, %"struct.std::ios_base::_Words", [8 x %"struct.std::ios_base::_Words"], i32, %"struct.std::ios_base::_Words"*, %"class.std::locale" }
%"struct.std::ios_base::_Callback_list" = type { %"struct.std::ios_base::_Callback_list"*, void (i32, %"class.std::ios_base"*, i32)*, i32, i32 }
%"struct.std::ios_base::_Words" = type { i8*, i64 }
%"class.std::locale" = type { %"class.std::locale::_Impl"* }
%"class.std::locale::_Impl" = type { i32, %"class.std::locale::facet"**, i64, %"class.std::locale::facet"**, i8** }
%"class.std::locale::facet" = type <{ i32 (...)**, i32, [4 x i8] }>
%"class.std::basic_streambuf" = type { i32 (...)**, i8*, i8*, i8*, i8*, i8*, i8*, %"class.std::locale" }
%"class.std::ctype" = type <{ %"class.std::locale::facet.base", [4 x i8], %struct.__locale_struct*, i8, [7 x i8], i32*, i32*, i16*, i8, [256 x i8], [256 x i8], i8, [6 x i8] }>
%"class.std::locale::facet.base" = type <{ i32 (...)**, i32 }>
%struct.__locale_struct = type { [13 x %struct.__locale_data*], i16*, i32*, i32*, [13 x i8*] }
%struct.__locale_data = type opaque
%"class.std::num_put" = type { %"class.std::locale::facet.base", [4 x i8] }
%"class.std::num_get" = type { %"class.std::locale::facet.base", [4 x i8] }
%class.Table = type <{ i32 (...)**, %class.Layout, [7 x i8], %"class.std::vector", i32*, i32, i32, i32, [4 x i8] }>
%class.Layout = type { i8 }
%"class.std::vector" = type { %"struct.std::_Vector_base" }
%"struct.std::_Vector_base" = type { %"struct.std::_Vector_base<Column *, std::allocator<Column *>>::_Vector_impl" }
%"struct.std::_Vector_base<Column *, std::allocator<Column *>>::_Vector_impl" = type { %class.Column**, %class.Column**, %class.Column** }
%class.Column = type { i32 (...)**, i8*, i32*, i32, i64 }
%class.HashGroupBy = type { %class.OpTemplate, %"class.std::vector.0", %"class.std::vector.0", %"class.std::vector.5", %"class.std::unordered_map", i32* }
%class.OpTemplate = type { i32 (...)** }
%"class.std::vector.0" = type { %"struct.std::_Vector_base.1" }
%"struct.std::_Vector_base.1" = type { %"struct.std::_Vector_base<ColumnIndex, std::allocator<ColumnIndex>>::_Vector_impl" }
%"struct.std::_Vector_base<ColumnIndex, std::allocator<ColumnIndex>>::_Vector_impl" = type { %struct.ColumnIndex*, %struct.ColumnIndex*, %struct.ColumnIndex* }
%struct.ColumnIndex = type { i32, i32 }
%"class.std::vector.5" = type { %"struct.std::_Vector_base.6" }
%"struct.std::_Vector_base.6" = type { %"struct.std::_Vector_base<Aggregator *, std::allocator<Aggregator *>>::_Vector_impl" }
%"struct.std::_Vector_base<Aggregator *, std::allocator<Aggregator *>>::_Vector_impl" = type { %class.Aggregator**, %class.Aggregator**, %class.Aggregator** }
%class.Aggregator = type { i32 (...)**, i32, i32, %"class.std::unordered_map" }
%"class.std::unordered_map" = type { %"class.std::_Hashtable" }
%"class.std::_Hashtable" = type { %"struct.std::__detail::_Hash_node_base"**, i64, %"struct.std::__detail::_Hash_node_base", i64, %"struct.std::__detail::_Prime_rehash_policy", %"struct.std::__detail::_Hash_node_base"* }
%"struct.std::__detail::_Hash_node_base" = type { %"struct.std::__detail::_Hash_node_base"* }
%"struct.std::__detail::_Prime_rehash_policy" = type { float, i64 }

$_ZN11HashGroupByC2ESt6vectorI11ColumnIndexSaIS1_EES3_S0_IP10AggregatorSaIS5_EE = comdat any

$_ZN6Column11printColumnEv = comdat any

$_ZN5TableD2Ev = comdat any

$_ZN5TableD0Ev = comdat any

$_ZN6ColumnD2Ev = comdat any

$_ZN6ColumnD0Ev = comdat any

$_ZTV5Table = comdat any

$_ZTS5Table = comdat any

$_ZTI5Table = comdat any

$_ZTV6Column = comdat any

$_ZTS6Column = comdat any

$_ZTI6Column = comdat any

@_ZStL8__ioinit = internal global %"class.std::ios_base::Init" zeroinitializer, align 1
@__dso_handle = external hidden global i8
@_ZSt4cout = external dso_local global %"class.std::basic_ostream", align 8
@.str = private unnamed_addr constant [24 x i8] c" in agg duration time: \00", align 1
@.str.1 = private unnamed_addr constant [4 x i8] c"ms\0A\00", align 1
@.str.2 = private unnamed_addr constant [30 x i8] c"finished groupby page count: \00", align 1
@.str.3 = private unnamed_addr constant [13 x i8] c" page size: \00", align 1
@.str.4 = private unnamed_addr constant [2 x i8] c"\0A\00", align 1
@_ZTV5Table = linkonce_odr dso_local unnamed_addr constant { [4 x i8*] } { [4 x i8*] [i8* null, i8* bitcast ({ i8*, i8* }* @_ZTI5Table to i8*), i8* bitcast (void (%class.Table*)* @_ZN5TableD2Ev to i8*), i8* bitcast (void (%class.Table*)* @_ZN5TableD0Ev to i8*)] }, comdat, align 8
@_ZTVN10__cxxabiv117__class_type_infoE = external dso_local global i8*
@_ZTS5Table = linkonce_odr dso_local constant [7 x i8] c"5Table\00", comdat, align 1
@_ZTI5Table = linkonce_odr dso_local constant { i8*, i8* } { i8* bitcast (i8** getelementptr inbounds (i8*, i8** @_ZTVN10__cxxabiv117__class_type_infoE, i64 2) to i8*), i8* getelementptr inbounds ([7 x i8], [7 x i8]* @_ZTS5Table, i32 0, i32 0) }, comdat, align 8
@_ZTV6Column = linkonce_odr dso_local unnamed_addr constant { [4 x i8*] } { [4 x i8*] [i8* null, i8* bitcast ({ i8*, i8* }* @_ZTI6Column to i8*), i8* bitcast (void (%class.Column*)* @_ZN6ColumnD2Ev to i8*), i8* bitcast (void (%class.Column*)* @_ZN6ColumnD0Ev to i8*)] }, comdat, align 8
@_ZTS6Column = linkonce_odr dso_local constant [8 x i8] c"6Column\00", comdat, align 1
@_ZTI6Column = linkonce_odr dso_local constant { i8*, i8* } { i8* bitcast (i8** getelementptr inbounds (i8*, i8** @_ZTVN10__cxxabiv117__class_type_infoE, i64 2) to i8*), i8* getelementptr inbounds ([8 x i8], [8 x i8]* @_ZTS6Column, i32 0, i32 0) }, comdat, align 8
@_ZTV13SumAggregator = external dso_local unnamed_addr constant { [6 x i8*] }, align 8
@_ZTV11HashGroupBy = external dso_local unnamed_addr constant { [9 x i8*] }, align 8
@.str.6 = private unnamed_addr constant [6 x i8] c"error\00", align 1
@.str.7 = private unnamed_addr constant [2 x i8] c" \00", align 1
@llvm.global_ctors = appending global [1 x { i32, void ()*, i8* }] [{ i32, void ()*, i8* } { i32 65535, void ()* @_GLOBAL__sub_I_test.cpp, i8* null }]

declare dso_local void @_ZNSt8ios_base4InitC1Ev(%"class.std::ios_base::Init"* nonnull dereferenceable(1)) unnamed_addr #0

; Function Attrs: nounwind
declare dso_local void @_ZNSt8ios_base4InitD1Ev(%"class.std::ios_base::Init"* nonnull dereferenceable(1)) unnamed_addr #1

; Function Attrs: nofree nounwind
declare dso_local i32 @__cxa_atexit(void (i8*)*, i8*, i8*) local_unnamed_addr #2

; Function Attrs: uwtable
define dso_local noalias nonnull %class.Table** @_Z9buildDataiiPii(i32 %PAGE_NUM, i32 %DATA_SIZE, i32* nocapture readonly %data_type, i32 %column_count) local_unnamed_addr #3 personality i8* bitcast (i32 (...)* @__gxx_personality_v0 to i8*) {
entry:
  %conv = sext i32 %PAGE_NUM to i64
  %0 = tail call { i64, i1 } @llvm.umul.with.overflow.i64(i64 %conv, i64 8)
  %1 = extractvalue { i64, i1 } %0, 1
  %2 = extractvalue { i64, i1 } %0, 0
  %3 = select i1 %1, i64 -1, i64 %2
  %call = tail call noalias nonnull i8* @_Znam(i64 %3) #13
  %4 = bitcast i8* %call to %class.Table**
  %cmp158 = icmp sgt i32 %PAGE_NUM, 0
  br i1 %cmp158, label %for.body.lr.ph, label %for.cond.cleanup

for.body.lr.ph:                                   ; preds = %entry
  %cmp3156 = icmp sgt i32 %column_count, 0
  %conv7 = sext i32 %DATA_SIZE to i64
  %5 = tail call { i64, i1 } @llvm.umul.with.overflow.i64(i64 %conv7, i64 4)
  %6 = extractvalue { i64, i1 } %5, 1
  %7 = extractvalue { i64, i1 } %5, 0
  %8 = select i1 %6, i64 -1, i64 %7
  %cmp11152 = icmp sgt i32 %DATA_SIZE, 0
  %9 = tail call { i64, i1 } @llvm.umul.with.overflow.i64(i64 %conv7, i64 8)
  %10 = extractvalue { i64, i1 } %9, 1
  %11 = extractvalue { i64, i1 } %9, 0
  %12 = select i1 %10, i64 -1, i64 %11
  %wide.trip.count168 = zext i32 %PAGE_NUM to i64
  %wide.trip.count164 = zext i32 %column_count to i64
  %wide.trip.count = zext i32 %DATA_SIZE to i64
  %13 = and i64 %wide.trip.count, 4294967288
  %14 = add nsw i64 %13, -8
  %15 = lshr exact i64 %14, 3
  %16 = add nuw nsw i64 %15, 1
  %min.iters.check = icmp ult i32 %DATA_SIZE, 8
  %n.vec = and i64 %wide.trip.count, 4294967288
  %xtraiter = and i64 %16, 1
  %17 = icmp eq i64 %14, 0
  %unroll_iter = and i64 %16, 4611686018427387902
  %lcmp.mod.not = icmp eq i64 %xtraiter, 0
  %cmp.n = icmp eq i64 %n.vec, %wide.trip.count
  %xtraiter181 = and i64 %conv7, 1
  %18 = icmp eq i32 %DATA_SIZE, 1
  %unroll_iter183 = and i64 %conv7, -2
  %lcmp.mod182.not = icmp eq i64 %xtraiter181, 0
  br label %for.body

for.cond.cleanup:                                 ; preds = %for.cond.cleanup4, %entry
  ret %class.Table** %4

for.body:                                         ; preds = %for.body.lr.ph, %for.cond.cleanup4
  %indvars.iv166 = phi i64 [ 0, %for.body.lr.ph ], [ %indvars.iv.next167, %for.cond.cleanup4 ]
  %call1 = tail call noalias nonnull dereferenceable(64) i8* @_Znwm(i64 64) #13
  %19 = bitcast i8* %call1 to i32 (...)***
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [4 x i8*] }, { [4 x i8*] }* @_ZTV5Table, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %19, align 8, !tbaa !2
  %data.i = getelementptr inbounds i8, i8* %call1, i64 16
  tail call void @llvm.memset.p0i8.i64(i8* nonnull align 8 dereferenceable(24) %data.i, i8 0, i64 24, i1 false) #14
  %positionCount2.i = getelementptr inbounds i8, i8* %call1, i64 48
  %20 = bitcast i8* %positionCount2.i to i32*
  store i32 %DATA_SIZE, i32* %20, align 8, !tbaa !5
  %columnCount3.i = getelementptr inbounds i8, i8* %call1, i64 52
  %21 = bitcast i8* %columnCount3.i to i32*
  store i32 2, i32* %21, align 4, !tbaa !12
  %call.i93 = invoke noalias nonnull dereferenceable(8) i8* @_Znam(i64 8) #13
          to label %_ZN5TableC2Ejj.exit unwind label %lpad

_ZN5TableC2Ejj.exit:                              ; preds = %for.body
  %types.i = getelementptr inbounds i8, i8* %call1, i64 40
  %22 = bitcast i8* %types.i to i32**
  %23 = bitcast i8* %types.i to i8**
  store i8* %call.i93, i8** %23, align 8, !tbaa !13
  %columnSize.i = getelementptr inbounds i8, i8* %call1, i64 56
  %24 = bitcast i8* %columnSize.i to i32*
  store i32 0, i32* %24, align 8, !tbaa !14
  br i1 %cmp3156, label %for.body5.lr.ph, label %for.cond.cleanup4

for.body5.lr.ph:                                  ; preds = %_ZN5TableC2Ejj.exit
  %_M_finish.i.i102 = getelementptr inbounds i8, i8* %call1, i64 24
  %25 = bitcast i8* %_M_finish.i.i102 to %class.Column***
  %_M_end_of_storage.i.i103 = getelementptr inbounds i8, i8* %call1, i64 32
  %26 = bitcast i8* %_M_end_of_storage.i.i103 to %class.Column***
  %27 = bitcast i8* %data.i to %class.Column***
  br label %for.body5

for.cond.cleanup4:                                ; preds = %for.inc44, %_ZN5TableC2Ejj.exit
  %arrayidx48 = getelementptr inbounds %class.Table*, %class.Table** %4, i64 %indvars.iv166
  %28 = bitcast %class.Table** %arrayidx48 to i8**
  store i8* %call1, i8** %28, align 8, !tbaa !15
  %indvars.iv.next167 = add nuw nsw i64 %indvars.iv166, 1
  %exitcond169.not = icmp eq i64 %indvars.iv.next167, %wide.trip.count168
  br i1 %exitcond169.not, label %for.cond.cleanup, label %for.body, !llvm.loop !16

lpad:                                             ; preds = %for.body
  %29 = landingpad { i8*, i32 }
          cleanup
  tail call void @_ZdlPv(i8* nonnull %call1) #15
  resume { i8*, i32 } %29

for.body5:                                        ; preds = %for.body5.lr.ph, %for.inc44
  %30 = phi %class.Column** [ null, %for.body5.lr.ph ], [ %93, %for.inc44 ]
  %indvars.iv162 = phi i64 [ 0, %for.body5.lr.ph ], [ %indvars.iv.next163, %for.inc44 ]
  %arrayidx = getelementptr inbounds i32, i32* %data_type, i64 %indvars.iv162
  %31 = load i32, i32* %arrayidx, align 4, !tbaa !18
  %cmp6 = icmp eq i32 %31, 1
  br i1 %cmp6, label %if.then, label %if.end

if.then:                                          ; preds = %for.body5
  %call8 = tail call noalias nonnull i8* @_Znam(i64 %8) #13
  %32 = bitcast i8* %call8 to i32*
  br i1 %cmp11152, label %for.body13.preheader, label %for.cond.cleanup12

for.body13.preheader:                             ; preds = %if.then
  br i1 %min.iters.check, label %for.body13.preheader179, label %vector.ph

vector.ph:                                        ; preds = %for.body13.preheader
  br i1 %17, label %middle.block.unr-lcssa, label %vector.body

vector.body:                                      ; preds = %vector.ph, %vector.body
  %index = phi i64 [ %index.next.1, %vector.body ], [ 0, %vector.ph ]
  %vec.ind175 = phi <4 x i32> [ %vec.ind.next178.1, %vector.body ], [ <i32 0, i32 1, i32 2, i32 3>, %vector.ph ]
  %niter = phi i64 [ %niter.nsub.1, %vector.body ], [ %unroll_iter, %vector.ph ]
  %step.add176 = add <4 x i32> %vec.ind175, <i32 4, i32 4, i32 4, i32 4>
  %33 = urem <4 x i32> %vec.ind175, <i32 3, i32 3, i32 3, i32 3>
  %34 = urem <4 x i32> %step.add176, <i32 3, i32 3, i32 3, i32 3>
  %35 = getelementptr inbounds i32, i32* %32, i64 %index
  %36 = bitcast i32* %35 to <4 x i32>*
  store <4 x i32> %33, <4 x i32>* %36, align 4, !tbaa !18
  %37 = getelementptr inbounds i32, i32* %35, i64 4
  %38 = bitcast i32* %37 to <4 x i32>*
  store <4 x i32> %34, <4 x i32>* %38, align 4, !tbaa !18
  %index.next = or i64 %index, 8
  %vec.ind.next178 = add <4 x i32> %vec.ind175, <i32 8, i32 8, i32 8, i32 8>
  %step.add176.1 = add <4 x i32> %vec.ind175, <i32 12, i32 12, i32 12, i32 12>
  %39 = urem <4 x i32> %vec.ind.next178, <i32 3, i32 3, i32 3, i32 3>
  %40 = urem <4 x i32> %step.add176.1, <i32 3, i32 3, i32 3, i32 3>
  %41 = getelementptr inbounds i32, i32* %32, i64 %index.next
  %42 = bitcast i32* %41 to <4 x i32>*
  store <4 x i32> %39, <4 x i32>* %42, align 4, !tbaa !18
  %43 = getelementptr inbounds i32, i32* %41, i64 4
  %44 = bitcast i32* %43 to <4 x i32>*
  store <4 x i32> %40, <4 x i32>* %44, align 4, !tbaa !18
  %index.next.1 = add i64 %index, 16
  %vec.ind.next178.1 = add <4 x i32> %vec.ind175, <i32 16, i32 16, i32 16, i32 16>
  %niter.nsub.1 = add i64 %niter, -2
  %niter.ncmp.1 = icmp eq i64 %niter.nsub.1, 0
  br i1 %niter.ncmp.1, label %middle.block.unr-lcssa, label %vector.body, !llvm.loop !19

middle.block.unr-lcssa:                           ; preds = %vector.body, %vector.ph
  %index.unr = phi i64 [ 0, %vector.ph ], [ %index.next.1, %vector.body ]
  %vec.ind175.unr = phi <4 x i32> [ <i32 0, i32 1, i32 2, i32 3>, %vector.ph ], [ %vec.ind.next178.1, %vector.body ]
  br i1 %lcmp.mod.not, label %middle.block, label %vector.body.epil

vector.body.epil:                                 ; preds = %middle.block.unr-lcssa
  %step.add176.epil = add <4 x i32> %vec.ind175.unr, <i32 4, i32 4, i32 4, i32 4>
  %45 = urem <4 x i32> %vec.ind175.unr, <i32 3, i32 3, i32 3, i32 3>
  %46 = urem <4 x i32> %step.add176.epil, <i32 3, i32 3, i32 3, i32 3>
  %47 = getelementptr inbounds i32, i32* %32, i64 %index.unr
  %48 = bitcast i32* %47 to <4 x i32>*
  store <4 x i32> %45, <4 x i32>* %48, align 4, !tbaa !18
  %49 = getelementptr inbounds i32, i32* %47, i64 4
  %50 = bitcast i32* %49 to <4 x i32>*
  store <4 x i32> %46, <4 x i32>* %50, align 4, !tbaa !18
  br label %middle.block

middle.block:                                     ; preds = %middle.block.unr-lcssa, %vector.body.epil
  br i1 %cmp.n, label %for.cond.cleanup12, label %for.body13.preheader179

for.body13.preheader179:                          ; preds = %for.body13.preheader, %middle.block
  %indvars.iv.ph = phi i64 [ 0, %for.body13.preheader ], [ %n.vec, %middle.block ]
  br label %for.body13

for.cond.cleanup12:                               ; preds = %for.body13, %middle.block, %if.then
  %call16 = tail call noalias nonnull dereferenceable(40) i8* @_Znwm(i64 40) #13
  %51 = bitcast i8* %call16 to i32 (...)***
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [4 x i8*] }, { [4 x i8*] }* @_ZTV6Column, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %51, align 8, !tbaa !2
  %data.i94 = getelementptr inbounds i8, i8* %call16, i64 8
  %52 = bitcast i8* %data.i94 to i8**
  store i8* %call8, i8** %52, align 8, !tbaa !21
  %type.i = getelementptr inbounds i8, i8* %call16, i64 24
  %53 = bitcast i8* %type.i to i32*
  store i32 1, i32* %53, align 8, !tbaa !25
  %size.i = getelementptr inbounds i8, i8* %call16, i64 32
  %54 = bitcast i8* %size.i to i64*
  store i64 %conv7, i64* %54, align 8, !tbaa !26
  %55 = load i32*, i32** %22, align 8, !tbaa !13
  %56 = load i32, i32* %24, align 8, !tbaa !14
  %idxprom.i99 = zext i32 %56 to i64
  %arrayidx.i100 = getelementptr inbounds i32, i32* %55, i64 %idxprom.i99
  store i32 1, i32* %arrayidx.i100, align 4, !tbaa !27
  %inc.i101 = add i32 %56, 1
  store i32 %inc.i101, i32* %24, align 8, !tbaa !14
  %57 = load %class.Column**, %class.Column*** %26, align 8, !tbaa !28
  %cmp.not.i.i104 = icmp eq %class.Column** %30, %57
  br i1 %cmp.not.i.i104, label %if.else.i.i120, label %if.end.thread

if.end.thread:                                    ; preds = %for.cond.cleanup12
  %58 = bitcast %class.Column** %30 to i8**
  store i8* %call16, i8** %58, align 8, !tbaa !15
  %59 = load %class.Column**, %class.Column*** %25, align 8, !tbaa !31
  %incdec.ptr.i.i105 = getelementptr inbounds %class.Column*, %class.Column** %59, i64 1
  store %class.Column** %incdec.ptr.i.i105, %class.Column*** %25, align 8, !tbaa !31
  br label %for.inc44

if.else.i.i120:                                   ; preds = %for.cond.cleanup12
  %60 = load %class.Column**, %class.Column*** %27, align 8, !tbaa !32
  %sub.ptr.lhs.cast.i28.i.i.i.i108 = ptrtoint %class.Column** %30 to i64
  %sub.ptr.rhs.cast.i29.i.i.i.i109 = ptrtoint %class.Column** %60 to i64
  %sub.ptr.sub.i30.i.i.i.i110 = sub i64 %sub.ptr.lhs.cast.i28.i.i.i.i108, %sub.ptr.rhs.cast.i29.i.i.i.i109
  %sub.ptr.div.i31.i.i.i.i111 = ashr exact i64 %sub.ptr.sub.i30.i.i.i.i110, 3
  %cmp.i.i.i.i.i112 = icmp eq i64 %sub.ptr.sub.i30.i.i.i.i110, 0
  %.sroa.speculated.i.i.i.i113 = select i1 %cmp.i.i.i.i.i112, i64 1, i64 %sub.ptr.div.i31.i.i.i.i111
  %add.i.i.i.i114 = add nsw i64 %.sroa.speculated.i.i.i.i113, %sub.ptr.div.i31.i.i.i.i111
  %cmp7.i.i.i.i115 = icmp ult i64 %add.i.i.i.i114, %sub.ptr.div.i31.i.i.i.i111
  %cmp9.i.i.i.i116 = icmp ugt i64 %add.i.i.i.i114, 2305843009213693951
  %or.cond.i.i.i.i117 = or i1 %cmp7.i.i.i.i115, %cmp9.i.i.i.i116
  %cond.i.i.i.i118 = select i1 %or.cond.i.i.i.i117, i64 2305843009213693951, i64 %add.i.i.i.i114
  %cmp.not.i.i.i.i119 = icmp eq i64 %cond.i.i.i.i118, 0
  br i1 %cmp.not.i.i.i.i119, label %invoke.cont.i.i.i131, label %_ZNSt16allocator_traitsISaIP6ColumnEE8allocateERS2_m.exit.i.i.i.i126

_ZNSt16allocator_traitsISaIP6ColumnEE8allocateERS2_m.exit.i.i.i.i126: ; preds = %if.else.i.i120
  %mul.i.i.i.i.i.i121 = shl nuw i64 %cond.i.i.i.i118, 3
  %call2.i.i.i.i.i.i122 = tail call noalias nonnull i8* @_Znwm(i64 %mul.i.i.i.i.i.i121) #16
  %61 = bitcast i8* %call2.i.i.i.i.i.i122 to %class.Column**
  %.pre.i.i.i123 = load %class.Column**, %class.Column*** %27, align 8, !tbaa !32
  %.pre83.i.i.i124 = ptrtoint %class.Column** %.pre.i.i.i123 to i64
  %.pre84.i.i.i125 = sub i64 %sub.ptr.lhs.cast.i28.i.i.i.i108, %.pre83.i.i.i124
  br label %invoke.cont.i.i.i131

invoke.cont.i.i.i131:                             ; preds = %_ZNSt16allocator_traitsISaIP6ColumnEE8allocateERS2_m.exit.i.i.i.i126, %if.else.i.i120
  %sub.ptr.sub.i.i.i.i.i.i.i.i74.pre-phi.i.i.i127 = phi i64 [ %.pre84.i.i.i125, %_ZNSt16allocator_traitsISaIP6ColumnEE8allocateERS2_m.exit.i.i.i.i126 ], [ %sub.ptr.sub.i30.i.i.i.i110, %if.else.i.i120 ]
  %62 = phi %class.Column** [ %.pre.i.i.i123, %_ZNSt16allocator_traitsISaIP6ColumnEE8allocateERS2_m.exit.i.i.i.i126 ], [ %60, %if.else.i.i120 ]
  %cond.i67.i.i.i128 = phi %class.Column** [ %61, %_ZNSt16allocator_traitsISaIP6ColumnEE8allocateERS2_m.exit.i.i.i.i126 ], [ null, %if.else.i.i120 ]
  %add.ptr.i.i.i129 = getelementptr inbounds %class.Column*, %class.Column** %cond.i67.i.i.i128, i64 %sub.ptr.div.i31.i.i.i.i111
  %63 = bitcast %class.Column** %add.ptr.i.i.i129 to i8**
  store i8* %call16, i8** %63, align 8, !tbaa !15
  %tobool.not.i.i.i.i.i.i.i.i75.i.i.i130 = icmp eq i64 %sub.ptr.sub.i.i.i.i.i.i.i.i74.pre-phi.i.i.i127, 0
  br i1 %tobool.not.i.i.i.i.i.i.i.i75.i.i.i130, label %invoke.cont10.i.i.i139, label %if.then.i.i.i.i.i.i.i.i76.i.i.i132

if.then.i.i.i.i.i.i.i.i76.i.i.i132:               ; preds = %invoke.cont.i.i.i131
  %64 = bitcast %class.Column** %cond.i67.i.i.i128 to i8*
  %65 = bitcast %class.Column** %62 to i8*
  tail call void @llvm.memmove.p0i8.p0i8.i64(i8* align 8 %64, i8* align 8 %65, i64 %sub.ptr.sub.i.i.i.i.i.i.i.i74.pre-phi.i.i.i127, i1 false) #14
  br label %invoke.cont10.i.i.i139

invoke.cont10.i.i.i139:                           ; preds = %if.then.i.i.i.i.i.i.i.i76.i.i.i132, %invoke.cont.i.i.i131
  %sub.ptr.div.i.i.i.i.i.i.i.i77.i.i.i133 = ashr exact i64 %sub.ptr.sub.i.i.i.i.i.i.i.i74.pre-phi.i.i.i127, 3
  %add.ptr.i.i.i.i.i.i.i.i78.i.i.i134 = getelementptr inbounds %class.Column*, %class.Column** %cond.i67.i.i.i128, i64 1
  %incdec.ptr.i.i.i135 = getelementptr inbounds %class.Column*, %class.Column** %add.ptr.i.i.i.i.i.i.i.i78.i.i.i134, i64 %sub.ptr.div.i.i.i.i.i.i.i.i77.i.i.i133
  %66 = load %class.Column**, %class.Column*** %25, align 8, !tbaa !31
  %sub.ptr.lhs.cast.i.i.i.i.i.i.i.i.i.i.i136 = ptrtoint %class.Column** %66 to i64
  %sub.ptr.sub.i.i.i.i.i.i.i.i.i.i.i137 = sub i64 %sub.ptr.lhs.cast.i.i.i.i.i.i.i.i.i.i.i136, %sub.ptr.lhs.cast.i28.i.i.i.i108
  %tobool.not.i.i.i.i.i.i.i.i.i.i.i138 = icmp eq i64 %sub.ptr.sub.i.i.i.i.i.i.i.i.i.i.i137, 0
  br i1 %tobool.not.i.i.i.i.i.i.i.i.i.i.i138, label %invoke.cont15.i.i.i142, label %if.then.i.i.i.i.i.i.i.i.i.i.i140

if.then.i.i.i.i.i.i.i.i.i.i.i140:                 ; preds = %invoke.cont10.i.i.i139
  %67 = bitcast %class.Column** %incdec.ptr.i.i.i135 to i8*
  %68 = bitcast %class.Column** %30 to i8*
  tail call void @llvm.memmove.p0i8.p0i8.i64(i8* nonnull align 8 %67, i8* align 8 %68, i64 %sub.ptr.sub.i.i.i.i.i.i.i.i.i.i.i137, i1 false) #14
  br label %invoke.cont15.i.i.i142

invoke.cont15.i.i.i142:                           ; preds = %if.then.i.i.i.i.i.i.i.i.i.i.i140, %invoke.cont10.i.i.i139
  %tobool.not.i68.i.i.i141 = icmp eq %class.Column** %62, null
  br i1 %tobool.not.i68.i.i.i141, label %_ZNSt6vectorIP6ColumnSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i.i147, label %if.then.i69.i.i.i143

if.then.i69.i.i.i143:                             ; preds = %invoke.cont15.i.i.i142
  %69 = bitcast %class.Column** %62 to i8*
  tail call void @_ZdlPv(i8* nonnull %69) #14
  br label %_ZNSt6vectorIP6ColumnSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i.i147

_ZNSt6vectorIP6ColumnSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i.i147: ; preds = %if.then.i69.i.i.i143, %invoke.cont15.i.i.i142
  %sub.ptr.div.i.i.i.i.i.i.i.i.i.i.i144 = ashr exact i64 %sub.ptr.sub.i.i.i.i.i.i.i.i.i.i.i137, 3
  %add.ptr.i.i.i.i.i.i.i.i.i.i.i145 = getelementptr inbounds %class.Column*, %class.Column** %incdec.ptr.i.i.i135, i64 %sub.ptr.div.i.i.i.i.i.i.i.i.i.i.i144
  store %class.Column** %cond.i67.i.i.i128, %class.Column*** %27, align 8, !tbaa !32
  store %class.Column** %add.ptr.i.i.i.i.i.i.i.i.i.i.i145, %class.Column*** %25, align 8, !tbaa !31
  %add.ptr39.i.i.i146 = getelementptr inbounds %class.Column*, %class.Column** %cond.i67.i.i.i128, i64 %cond.i.i.i.i118
  store %class.Column** %add.ptr39.i.i.i146, %class.Column*** %26, align 8, !tbaa !28
  %.pr.pre = load i32, i32* %arrayidx, align 4, !tbaa !18
  br label %if.end

for.body13:                                       ; preds = %for.body13.preheader179, %for.body13
  %indvars.iv = phi i64 [ %indvars.iv.next, %for.body13 ], [ %indvars.iv.ph, %for.body13.preheader179 ]
  %70 = trunc i64 %indvars.iv to i32
  %rem = urem i32 %70, 3
  %arrayidx15 = getelementptr inbounds i32, i32* %32, i64 %indvars.iv
  store i32 %rem, i32* %arrayidx15, align 4, !tbaa !18
  %indvars.iv.next = add nuw nsw i64 %indvars.iv, 1
  %exitcond.not = icmp eq i64 %indvars.iv.next, %wide.trip.count
  br i1 %exitcond.not, label %for.cond.cleanup12, label %for.body13, !llvm.loop !33

if.end:                                           ; preds = %_ZNSt6vectorIP6ColumnSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i.i147, %for.body5
  %71 = phi %class.Column** [ %30, %for.body5 ], [ %add.ptr.i.i.i.i.i.i.i.i.i.i.i145, %_ZNSt6vectorIP6ColumnSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i.i147 ]
  %72 = phi i32 [ %31, %for.body5 ], [ %.pr.pre, %_ZNSt6vectorIP6ColumnSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i.i147 ]
  %cmp22 = icmp eq i32 %72, 2
  br i1 %cmp22, label %if.then23, label %for.inc44

if.then23:                                        ; preds = %if.end
  %call26 = tail call noalias nonnull i8* @_Znam(i64 %12) #13
  %73 = bitcast i8* %call26 to i64*
  br i1 %cmp11152, label %for.body32.preheader, label %for.cond.cleanup31

for.body32.preheader:                             ; preds = %if.then23
  br i1 %18, label %for.cond.cleanup31.loopexit.unr-lcssa, label %for.body32

for.cond.cleanup31.loopexit.unr-lcssa:            ; preds = %for.body32, %for.body32.preheader
  %i27.0155.unr = phi i64 [ 0, %for.body32.preheader ], [ %inc36.1, %for.body32 ]
  br i1 %lcmp.mod182.not, label %for.cond.cleanup31, label %for.body32.epil

for.body32.epil:                                  ; preds = %for.cond.cleanup31.loopexit.unr-lcssa
  %rem33.epil = urem i64 %i27.0155.unr, 3
  %arrayidx34.epil = getelementptr inbounds i64, i64* %73, i64 %i27.0155.unr
  store i64 %rem33.epil, i64* %arrayidx34.epil, align 8, !tbaa !35
  br label %for.cond.cleanup31

for.cond.cleanup31:                               ; preds = %for.body32.epil, %for.cond.cleanup31.loopexit.unr-lcssa, %if.then23
  %call39 = tail call noalias nonnull dereferenceable(40) i8* @_Znwm(i64 40) #13
  %74 = bitcast i8* %call39 to i32 (...)***
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [4 x i8*] }, { [4 x i8*] }* @_ZTV6Column, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %74, align 8, !tbaa !2
  %data.i149 = getelementptr inbounds i8, i8* %call39, i64 8
  %75 = bitcast i8* %data.i149 to i8**
  store i8* %call26, i8** %75, align 8, !tbaa !21
  %type.i150 = getelementptr inbounds i8, i8* %call39, i64 24
  %76 = bitcast i8* %type.i150 to i32*
  store i32 2, i32* %76, align 8, !tbaa !25
  %size.i151 = getelementptr inbounds i8, i8* %call39, i64 32
  %77 = bitcast i8* %size.i151 to i64*
  store i64 %conv7, i64* %77, align 8, !tbaa !26
  %78 = load i32*, i32** %22, align 8, !tbaa !13
  %79 = load i32, i32* %24, align 8, !tbaa !14
  %idxprom.i = zext i32 %79 to i64
  %arrayidx.i = getelementptr inbounds i32, i32* %78, i64 %idxprom.i
  store i32 2, i32* %arrayidx.i, align 4, !tbaa !27
  %inc.i = add i32 %79, 1
  store i32 %inc.i, i32* %24, align 8, !tbaa !14
  %80 = load %class.Column**, %class.Column*** %26, align 8, !tbaa !28
  %cmp.not.i.i = icmp eq %class.Column** %71, %80
  br i1 %cmp.not.i.i, label %if.else.i.i, label %if.then.i.i

if.then.i.i:                                      ; preds = %for.cond.cleanup31
  %81 = bitcast %class.Column** %71 to i8**
  store i8* %call39, i8** %81, align 8, !tbaa !15
  %82 = load %class.Column**, %class.Column*** %25, align 8, !tbaa !31
  %incdec.ptr.i.i = getelementptr inbounds %class.Column*, %class.Column** %82, i64 1
  store %class.Column** %incdec.ptr.i.i, %class.Column*** %25, align 8, !tbaa !31
  br label %for.inc44

if.else.i.i:                                      ; preds = %for.cond.cleanup31
  %83 = load %class.Column**, %class.Column*** %27, align 8, !tbaa !32
  %sub.ptr.lhs.cast.i28.i.i.i.i = ptrtoint %class.Column** %71 to i64
  %sub.ptr.rhs.cast.i29.i.i.i.i = ptrtoint %class.Column** %83 to i64
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
  %call2.i.i.i.i.i.i = tail call noalias nonnull i8* @_Znwm(i64 %mul.i.i.i.i.i.i) #16
  %84 = bitcast i8* %call2.i.i.i.i.i.i to %class.Column**
  %.pre.i.i.i = load %class.Column**, %class.Column*** %27, align 8, !tbaa !32
  %.pre83.i.i.i = ptrtoint %class.Column** %.pre.i.i.i to i64
  %.pre84.i.i.i = sub i64 %sub.ptr.lhs.cast.i28.i.i.i.i, %.pre83.i.i.i
  br label %invoke.cont.i.i.i

invoke.cont.i.i.i:                                ; preds = %_ZNSt16allocator_traitsISaIP6ColumnEE8allocateERS2_m.exit.i.i.i.i, %if.else.i.i
  %sub.ptr.sub.i.i.i.i.i.i.i.i74.pre-phi.i.i.i = phi i64 [ %.pre84.i.i.i, %_ZNSt16allocator_traitsISaIP6ColumnEE8allocateERS2_m.exit.i.i.i.i ], [ %sub.ptr.sub.i30.i.i.i.i, %if.else.i.i ]
  %85 = phi %class.Column** [ %.pre.i.i.i, %_ZNSt16allocator_traitsISaIP6ColumnEE8allocateERS2_m.exit.i.i.i.i ], [ %83, %if.else.i.i ]
  %cond.i67.i.i.i = phi %class.Column** [ %84, %_ZNSt16allocator_traitsISaIP6ColumnEE8allocateERS2_m.exit.i.i.i.i ], [ null, %if.else.i.i ]
  %add.ptr.i.i.i = getelementptr inbounds %class.Column*, %class.Column** %cond.i67.i.i.i, i64 %sub.ptr.div.i31.i.i.i.i
  %86 = bitcast %class.Column** %add.ptr.i.i.i to i8**
  store i8* %call39, i8** %86, align 8, !tbaa !15
  %tobool.not.i.i.i.i.i.i.i.i75.i.i.i = icmp eq i64 %sub.ptr.sub.i.i.i.i.i.i.i.i74.pre-phi.i.i.i, 0
  br i1 %tobool.not.i.i.i.i.i.i.i.i75.i.i.i, label %invoke.cont10.i.i.i, label %if.then.i.i.i.i.i.i.i.i76.i.i.i

if.then.i.i.i.i.i.i.i.i76.i.i.i:                  ; preds = %invoke.cont.i.i.i
  %87 = bitcast %class.Column** %cond.i67.i.i.i to i8*
  %88 = bitcast %class.Column** %85 to i8*
  tail call void @llvm.memmove.p0i8.p0i8.i64(i8* align 8 %87, i8* align 8 %88, i64 %sub.ptr.sub.i.i.i.i.i.i.i.i74.pre-phi.i.i.i, i1 false) #14
  br label %invoke.cont10.i.i.i

invoke.cont10.i.i.i:                              ; preds = %if.then.i.i.i.i.i.i.i.i76.i.i.i, %invoke.cont.i.i.i
  %sub.ptr.div.i.i.i.i.i.i.i.i77.i.i.i = ashr exact i64 %sub.ptr.sub.i.i.i.i.i.i.i.i74.pre-phi.i.i.i, 3
  %add.ptr.i.i.i.i.i.i.i.i78.i.i.i = getelementptr inbounds %class.Column*, %class.Column** %cond.i67.i.i.i, i64 1
  %incdec.ptr.i.i.i = getelementptr inbounds %class.Column*, %class.Column** %add.ptr.i.i.i.i.i.i.i.i78.i.i.i, i64 %sub.ptr.div.i.i.i.i.i.i.i.i77.i.i.i
  %89 = load %class.Column**, %class.Column*** %25, align 8, !tbaa !31
  %sub.ptr.lhs.cast.i.i.i.i.i.i.i.i.i.i.i = ptrtoint %class.Column** %89 to i64
  %sub.ptr.sub.i.i.i.i.i.i.i.i.i.i.i = sub i64 %sub.ptr.lhs.cast.i.i.i.i.i.i.i.i.i.i.i, %sub.ptr.lhs.cast.i28.i.i.i.i
  %tobool.not.i.i.i.i.i.i.i.i.i.i.i = icmp eq i64 %sub.ptr.sub.i.i.i.i.i.i.i.i.i.i.i, 0
  br i1 %tobool.not.i.i.i.i.i.i.i.i.i.i.i, label %invoke.cont15.i.i.i, label %if.then.i.i.i.i.i.i.i.i.i.i.i

if.then.i.i.i.i.i.i.i.i.i.i.i:                    ; preds = %invoke.cont10.i.i.i
  %90 = bitcast %class.Column** %incdec.ptr.i.i.i to i8*
  %91 = bitcast %class.Column** %71 to i8*
  tail call void @llvm.memmove.p0i8.p0i8.i64(i8* nonnull align 8 %90, i8* align 8 %91, i64 %sub.ptr.sub.i.i.i.i.i.i.i.i.i.i.i, i1 false) #14
  br label %invoke.cont15.i.i.i

invoke.cont15.i.i.i:                              ; preds = %if.then.i.i.i.i.i.i.i.i.i.i.i, %invoke.cont10.i.i.i
  %tobool.not.i68.i.i.i = icmp eq %class.Column** %85, null
  br i1 %tobool.not.i68.i.i.i, label %_ZNSt6vectorIP6ColumnSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i.i, label %if.then.i69.i.i.i

if.then.i69.i.i.i:                                ; preds = %invoke.cont15.i.i.i
  %92 = bitcast %class.Column** %85 to i8*
  tail call void @_ZdlPv(i8* nonnull %92) #14
  br label %_ZNSt6vectorIP6ColumnSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i.i

_ZNSt6vectorIP6ColumnSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i.i: ; preds = %if.then.i69.i.i.i, %invoke.cont15.i.i.i
  %sub.ptr.div.i.i.i.i.i.i.i.i.i.i.i = ashr exact i64 %sub.ptr.sub.i.i.i.i.i.i.i.i.i.i.i, 3
  %add.ptr.i.i.i.i.i.i.i.i.i.i.i = getelementptr inbounds %class.Column*, %class.Column** %incdec.ptr.i.i.i, i64 %sub.ptr.div.i.i.i.i.i.i.i.i.i.i.i
  store %class.Column** %cond.i67.i.i.i, %class.Column*** %27, align 8, !tbaa !32
  store %class.Column** %add.ptr.i.i.i.i.i.i.i.i.i.i.i, %class.Column*** %25, align 8, !tbaa !31
  %add.ptr39.i.i.i = getelementptr inbounds %class.Column*, %class.Column** %cond.i67.i.i.i, i64 %cond.i.i.i.i
  store %class.Column** %add.ptr39.i.i.i, %class.Column*** %26, align 8, !tbaa !28
  br label %for.inc44

for.body32:                                       ; preds = %for.body32.preheader, %for.body32
  %i27.0155 = phi i64 [ %inc36.1, %for.body32 ], [ 0, %for.body32.preheader ]
  %niter184 = phi i64 [ %niter184.nsub.1, %for.body32 ], [ %unroll_iter183, %for.body32.preheader ]
  %rem33 = urem i64 %i27.0155, 3
  %arrayidx34 = getelementptr inbounds i64, i64* %73, i64 %i27.0155
  store i64 %rem33, i64* %arrayidx34, align 8, !tbaa !35
  %inc36 = or i64 %i27.0155, 1
  %rem33.1 = urem i64 %inc36, 3
  %arrayidx34.1 = getelementptr inbounds i64, i64* %73, i64 %inc36
  store i64 %rem33.1, i64* %arrayidx34.1, align 8, !tbaa !35
  %inc36.1 = add nuw nsw i64 %i27.0155, 2
  %niter184.nsub.1 = add i64 %niter184, -2
  %niter184.ncmp.1 = icmp eq i64 %niter184.nsub.1, 0
  br i1 %niter184.ncmp.1, label %for.cond.cleanup31.loopexit.unr-lcssa, label %for.body32, !llvm.loop !36

for.inc44:                                        ; preds = %if.end.thread, %_ZNSt6vectorIP6ColumnSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i.i, %if.then.i.i, %if.end
  %93 = phi %class.Column** [ %add.ptr.i.i.i.i.i.i.i.i.i.i.i, %_ZNSt6vectorIP6ColumnSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i.i ], [ %incdec.ptr.i.i, %if.then.i.i ], [ %71, %if.end ], [ %incdec.ptr.i.i105, %if.end.thread ]
  %indvars.iv.next163 = add nuw nsw i64 %indvars.iv162, 1
  %exitcond165.not = icmp eq i64 %indvars.iv.next163, %wide.trip.count164
  br i1 %exitcond165.not, label %for.cond.cleanup4, label %for.body5, !llvm.loop !37
}

; Function Attrs: nofree nosync nounwind readnone speculatable willreturn
declare { i64, i1 } @llvm.umul.with.overflow.i64(i64, i64) #4

; Function Attrs: nobuiltin nofree allocsize(0)
declare dso_local nonnull i8* @_Znam(i64) local_unnamed_addr #5

; Function Attrs: nobuiltin nofree allocsize(0)
declare dso_local nonnull i8* @_Znwm(i64) local_unnamed_addr #5

declare dso_local i32 @__gxx_personality_v0(...)

; Function Attrs: nobuiltin nounwind
declare dso_local void @_ZdlPv(i8*) local_unnamed_addr #6

; Function Attrs: uwtable
define dso_local nonnull %class.HashGroupBy* @_Z13createGroupByv() local_unnamed_addr #3 personality i8* bitcast (i32 (...)* @__gxx_personality_v0 to i8*) {
entry:
  %agg.tmp30 = alloca %"class.std::vector.0", align 8
  %agg.tmp33 = alloca %"class.std::vector.0", align 8
  %agg.tmp36 = alloca %"class.std::vector.5", align 8
  %call2.i.i.i.i4.i = tail call noalias nonnull i8* @_Znwm(i64 16) #16
  %0 = bitcast i8* %call2.i.i.i.i4.i to <2 x i64>*
  store <2 x i64> <i64 4294967296, i64 8589934593>, <2 x i64>* %0, align 4
  %call2.i.i.i.i4.i81 = invoke noalias nonnull i8* @_Znwm(i64 16) #16
          to label %invoke.cont11 unwind label %_ZNSt12_Vector_baseI11ColumnIndexSaIS0_EED2Ev.exit.i89

_ZNSt12_Vector_baseI11ColumnIndexSaIS0_EED2Ev.exit.i89: ; preds = %entry
  %1 = landingpad { i8*, i32 }
          cleanup
  br label %_ZNSt6vectorI11ColumnIndexSaIS0_EED2Ev.exit

invoke.cont11:                                    ; preds = %entry
  %2 = bitcast i8* %call2.i.i.i.i4.i81 to <2 x i64>*
  store <2 x i64> <i64 4294967296, i64 8589934593>, <2 x i64>* %2, align 4
  %call = invoke noalias nonnull dereferenceable(72) i8* @_Znwm(i64 72) #13
          to label %invoke.cont15 unwind label %lpad12

invoke.cont15:                                    ; preds = %invoke.cont11
  %3 = bitcast i8* %call to i32 (...)***
  %type.i.i = getelementptr inbounds i8, i8* %call, i64 8
  %4 = bitcast i8* %type.i.i to i32*
  store i32 0, i32* %4, align 8, !tbaa !38
  %dataType.i.i = getelementptr inbounds i8, i8* %call, i64 12
  %5 = bitcast i8* %dataType.i.i to i32*
  store i32 1, i32* %5, align 4, !tbaa !46
  %_M_buckets.i.i.i.i = getelementptr inbounds i8, i8* %call, i64 16
  %_M_single_bucket.i.i.i.i = getelementptr inbounds i8, i8* %call, i64 64
  %6 = bitcast i8* %_M_buckets.i.i.i.i to i8**
  store i8* %_M_single_bucket.i.i.i.i, i8** %6, align 8, !tbaa !47
  %_M_bucket_count.i.i.i.i = getelementptr inbounds i8, i8* %call, i64 24
  %7 = bitcast i8* %_M_bucket_count.i.i.i.i to i64*
  store i64 1, i64* %7, align 8, !tbaa !48
  %_M_nxt.i.i.i.i.i = getelementptr inbounds i8, i8* %call, i64 32
  %_M_max_load_factor.i.i.i.i.i = getelementptr inbounds i8, i8* %call, i64 48
  %8 = bitcast i8* %_M_max_load_factor.i.i.i.i.i to float*
  tail call void @llvm.memset.p0i8.i64(i8* nonnull align 8 dereferenceable(16) %_M_nxt.i.i.i.i.i, i8 0, i64 16, i1 false) #14
  store float 1.000000e+00, float* %8, align 8, !tbaa !49
  %_M_next_resize.i.i.i.i.i = getelementptr inbounds i8, i8* %call, i64 56
  tail call void @llvm.memset.p0i8.i64(i8* nonnull align 8 dereferenceable(16) %_M_next_resize.i.i.i.i.i, i8 0, i64 16, i1 false) #14
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [6 x i8*] }, { [6 x i8*] }* @_ZTV13SumAggregator, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %3, align 8, !tbaa !2
  %call18 = invoke noalias nonnull dereferenceable(72) i8* @_Znwm(i64 72) #13
          to label %_ZNSt16allocator_traitsISaIP10AggregatorEE8allocateERS2_m.exit.i.i.i.i unwind label %lpad16

_ZNSt16allocator_traitsISaIP10AggregatorEE8allocateERS2_m.exit.i.i.i.i: ; preds = %invoke.cont15
  %9 = bitcast i8* %call18 to i32 (...)***
  %type.i.i100 = getelementptr inbounds i8, i8* %call18, i64 8
  %10 = bitcast i8* %type.i.i100 to i32*
  store i32 0, i32* %10, align 8, !tbaa !38
  %dataType.i.i101 = getelementptr inbounds i8, i8* %call18, i64 12
  %11 = bitcast i8* %dataType.i.i101 to i32*
  store i32 2, i32* %11, align 4, !tbaa !46
  %_M_buckets.i.i.i.i102 = getelementptr inbounds i8, i8* %call18, i64 16
  %_M_single_bucket.i.i.i.i103 = getelementptr inbounds i8, i8* %call18, i64 64
  %12 = bitcast i8* %_M_buckets.i.i.i.i102 to i8**
  store i8* %_M_single_bucket.i.i.i.i103, i8** %12, align 8, !tbaa !47
  %_M_bucket_count.i.i.i.i104 = getelementptr inbounds i8, i8* %call18, i64 24
  %13 = bitcast i8* %_M_bucket_count.i.i.i.i104 to i64*
  store i64 1, i64* %13, align 8, !tbaa !48
  %_M_nxt.i.i.i.i.i105 = getelementptr inbounds i8, i8* %call18, i64 32
  %_M_max_load_factor.i.i.i.i.i106 = getelementptr inbounds i8, i8* %call18, i64 48
  %14 = bitcast i8* %_M_max_load_factor.i.i.i.i.i106 to float*
  tail call void @llvm.memset.p0i8.i64(i8* nonnull align 8 dereferenceable(16) %_M_nxt.i.i.i.i.i105, i8 0, i64 16, i1 false) #14
  store float 1.000000e+00, float* %14, align 8, !tbaa !49
  %_M_next_resize.i.i.i.i.i107 = getelementptr inbounds i8, i8* %call18, i64 56
  tail call void @llvm.memset.p0i8.i64(i8* nonnull align 8 dereferenceable(16) %_M_next_resize.i.i.i.i.i107, i8 0, i64 16, i1 false) #14
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [6 x i8*] }, { [6 x i8*] }* @_ZTV13SumAggregator, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %9, align 8, !tbaa !2
  %call2.i.i.i.i.i.i115 = invoke noalias nonnull i8* @_Znwm(i64 8) #16
          to label %invoke.cont23 unwind label %lpad22

invoke.cont23:                                    ; preds = %_ZNSt16allocator_traitsISaIP10AggregatorEE8allocateERS2_m.exit.i.i.i.i
  %15 = bitcast i8* %call2.i.i.i.i.i.i115 to i8**
  store i8* %call, i8** %15, align 8, !tbaa !15
  %call2.i.i.i.i.i.i162 = invoke noalias nonnull i8* @_Znwm(i64 16) #16
          to label %call2.i.i.i.i.i.i.noexc161 unwind label %lpad25

call2.i.i.i.i.i.i.noexc161:                       ; preds = %invoke.cont23
  %add.ptr.i.i.i142 = getelementptr inbounds i8, i8* %call2.i.i.i.i.i.i162, i64 8
  %16 = bitcast i8* %add.ptr.i.i.i142 to i8**
  store i8* %call18, i8** %16, align 8, !tbaa !15
  %17 = bitcast i8* %call2.i.i.i.i.i.i115 to i64*
  %18 = bitcast i8* %call2.i.i.i.i.i.i162 to i64*
  %19 = load i64, i64* %17, align 8
  store i64 %19, i64* %18, align 8
  tail call void @_ZdlPv(i8* nonnull %call2.i.i.i.i.i.i115) #14
  %call29 = invoke noalias nonnull dereferenceable(144) i8* @_Znwm(i64 144) #13
          to label %_ZNSt16allocator_traitsISaI11ColumnIndexEE8allocateERS1_m.exit.i.i.i.i unwind label %lpad27

_ZNSt16allocator_traitsISaI11ColumnIndexEE8allocateERS1_m.exit.i.i.i.i: ; preds = %call2.i.i.i.i.i.i.noexc161
  %20 = bitcast i8* %call29 to %class.HashGroupBy*
  %21 = bitcast %"class.std::vector.0"* %agg.tmp30 to i8*
  call void @llvm.memset.p0i8.i64(i8* nonnull align 8 dereferenceable(24) %21, i8 0, i64 24, i1 false) #14
  %call2.i.i.i.i3.i22.i169 = invoke noalias nonnull i8* @_Znwm(i64 16) #16
          to label %_ZNSt16allocator_traitsISaI11ColumnIndexEE8allocateERS1_m.exit.i.i.i.i180 unwind label %lpad31

_ZNSt16allocator_traitsISaI11ColumnIndexEE8allocateERS1_m.exit.i.i.i.i180: ; preds = %_ZNSt16allocator_traitsISaI11ColumnIndexEE8allocateERS1_m.exit.i.i.i.i
  %_M_start.i.i.i = getelementptr inbounds %"class.std::vector.0", %"class.std::vector.0"* %agg.tmp30, i64 0, i32 0, i32 0, i32 0
  %22 = bitcast %"class.std::vector.0"* %agg.tmp30 to i8**
  store i8* %call2.i.i.i.i3.i22.i169, i8** %22, align 8, !tbaa !50
  %_M_finish.i.i.i = getelementptr inbounds %"class.std::vector.0", %"class.std::vector.0"* %agg.tmp30, i64 0, i32 0, i32 0, i32 1
  %23 = bitcast %struct.ColumnIndex** %_M_finish.i.i.i to i8**
  %add.ptr.i.i.i168 = getelementptr inbounds i8, i8* %call2.i.i.i.i3.i22.i169, i64 16
  %_M_end_of_storage.i.i.i = getelementptr inbounds %"class.std::vector.0", %"class.std::vector.0"* %agg.tmp30, i64 0, i32 0, i32 0, i32 2
  %24 = bitcast %struct.ColumnIndex** %_M_end_of_storage.i.i.i to i8**
  store i8* %add.ptr.i.i.i168, i8** %24, align 8, !tbaa !53
  tail call void @llvm.memcpy.p0i8.p0i8.i64(i8* nonnull align 4 dereferenceable(16) %call2.i.i.i.i3.i22.i169, i8* nonnull align 4 dereferenceable(16) %call2.i.i.i.i4.i, i64 16, i1 false) #14
  store i8* %add.ptr.i.i.i168, i8** %23, align 8, !tbaa !54
  %25 = bitcast %"class.std::vector.0"* %agg.tmp33 to i8*
  call void @llvm.memset.p0i8.i64(i8* nonnull align 8 dereferenceable(24) %25, i8 0, i64 24, i1 false) #14
  %call2.i.i.i.i3.i22.i196 = invoke noalias nonnull i8* @_Znwm(i64 16) #16
          to label %_ZNSt16allocator_traitsISaIP10AggregatorEE8allocateERS2_m.exit.i.i.i.i208 unwind label %lpad34

_ZNSt16allocator_traitsISaIP10AggregatorEE8allocateERS2_m.exit.i.i.i.i208: ; preds = %_ZNSt16allocator_traitsISaI11ColumnIndexEE8allocateERS1_m.exit.i.i.i.i180
  %_M_start.i.i.i182 = getelementptr inbounds %"class.std::vector.0", %"class.std::vector.0"* %agg.tmp33, i64 0, i32 0, i32 0, i32 0
  %26 = bitcast %"class.std::vector.0"* %agg.tmp33 to i8**
  store i8* %call2.i.i.i.i3.i22.i196, i8** %26, align 8, !tbaa !50
  %_M_finish.i.i.i183 = getelementptr inbounds %"class.std::vector.0", %"class.std::vector.0"* %agg.tmp33, i64 0, i32 0, i32 0, i32 1
  %27 = bitcast %struct.ColumnIndex** %_M_finish.i.i.i183 to i8**
  %add.ptr.i.i.i184 = getelementptr inbounds i8, i8* %call2.i.i.i.i3.i22.i196, i64 16
  %_M_end_of_storage.i.i.i185 = getelementptr inbounds %"class.std::vector.0", %"class.std::vector.0"* %agg.tmp33, i64 0, i32 0, i32 0, i32 2
  %28 = bitcast %struct.ColumnIndex** %_M_end_of_storage.i.i.i185 to i8**
  store i8* %add.ptr.i.i.i184, i8** %28, align 8, !tbaa !53
  tail call void @llvm.memcpy.p0i8.p0i8.i64(i8* nonnull align 4 dereferenceable(16) %call2.i.i.i.i3.i22.i196, i8* nonnull align 4 dereferenceable(16) %call2.i.i.i.i4.i81, i64 16, i1 false) #14
  store i8* %add.ptr.i.i.i184, i8** %27, align 8, !tbaa !54
  %29 = bitcast %"class.std::vector.5"* %agg.tmp36 to i8*
  call void @llvm.memset.p0i8.i64(i8* nonnull align 8 dereferenceable(24) %29, i8 0, i64 24, i1 false) #14
  %call2.i.i.i.i3.i22.i224 = invoke noalias nonnull i8* @_Znwm(i64 16) #16
          to label %invoke.cont38 unwind label %lpad37

invoke.cont38:                                    ; preds = %_ZNSt16allocator_traitsISaIP10AggregatorEE8allocateERS2_m.exit.i.i.i.i208
  %_M_start.i.i.i210 = getelementptr inbounds %"class.std::vector.5", %"class.std::vector.5"* %agg.tmp36, i64 0, i32 0, i32 0, i32 0
  %30 = bitcast %"class.std::vector.5"* %agg.tmp36 to i8**
  store i8* %call2.i.i.i.i3.i22.i224, i8** %30, align 8, !tbaa !55
  %_M_finish.i.i.i211 = getelementptr inbounds %"class.std::vector.5", %"class.std::vector.5"* %agg.tmp36, i64 0, i32 0, i32 0, i32 1
  %add.ptr.i.i.i212 = getelementptr inbounds i8, i8* %call2.i.i.i.i3.i22.i224, i64 16
  %_M_end_of_storage.i.i.i213 = getelementptr inbounds %"class.std::vector.5", %"class.std::vector.5"* %agg.tmp36, i64 0, i32 0, i32 0, i32 2
  %31 = bitcast %class.Aggregator*** %_M_end_of_storage.i.i.i213 to i8**
  store i8* %add.ptr.i.i.i212, i8** %31, align 8, !tbaa !58
  tail call void @llvm.memcpy.p0i8.p0i8.i64(i8* nonnull align 8 dereferenceable(16) %call2.i.i.i.i3.i22.i224, i8* nonnull align 8 dereferenceable(16) %call2.i.i.i.i.i.i162, i64 16, i1 false) #14
  %32 = bitcast %class.Aggregator*** %_M_finish.i.i.i211 to i8**
  store i8* %add.ptr.i.i.i212, i8** %32, align 8, !tbaa !59
  invoke void @_ZN11HashGroupByC2ESt6vectorI11ColumnIndexSaIS1_EES3_S0_IP10AggregatorSaIS5_EE(%class.HashGroupBy* nonnull dereferenceable(144) %20, %"class.std::vector.0"* nonnull %agg.tmp30, %"class.std::vector.0"* nonnull %agg.tmp33, %"class.std::vector.5"* nonnull %agg.tmp36)
          to label %invoke.cont40 unwind label %lpad39

invoke.cont40:                                    ; preds = %invoke.cont38
  %33 = load %class.Aggregator**, %class.Aggregator*** %_M_start.i.i.i210, align 8, !tbaa !55
  %tobool.not.i.i.i226 = icmp eq %class.Aggregator** %33, null
  br i1 %tobool.not.i.i.i226, label %_ZNSt6vectorIP10AggregatorSaIS1_EED2Ev.exit229, label %if.then.i.i.i228

if.then.i.i.i228:                                 ; preds = %invoke.cont40
  %34 = bitcast %class.Aggregator** %33 to i8*
  call void @_ZdlPv(i8* nonnull %34) #14
  br label %_ZNSt6vectorIP10AggregatorSaIS1_EED2Ev.exit229

_ZNSt6vectorIP10AggregatorSaIS1_EED2Ev.exit229:   ; preds = %invoke.cont40, %if.then.i.i.i228
  %35 = load %struct.ColumnIndex*, %struct.ColumnIndex** %_M_start.i.i.i182, align 8, !tbaa !50
  %tobool.not.i.i.i231 = icmp eq %struct.ColumnIndex* %35, null
  br i1 %tobool.not.i.i.i231, label %_ZNSt6vectorI11ColumnIndexSaIS0_EED2Ev.exit235, label %if.then.i.i.i233

if.then.i.i.i233:                                 ; preds = %_ZNSt6vectorIP10AggregatorSaIS1_EED2Ev.exit229
  %36 = bitcast %struct.ColumnIndex* %35 to i8*
  call void @_ZdlPv(i8* nonnull %36) #14
  br label %_ZNSt6vectorI11ColumnIndexSaIS0_EED2Ev.exit235

_ZNSt6vectorI11ColumnIndexSaIS0_EED2Ev.exit235:   ; preds = %_ZNSt6vectorIP10AggregatorSaIS1_EED2Ev.exit229, %if.then.i.i.i233
  %37 = load %struct.ColumnIndex*, %struct.ColumnIndex** %_M_start.i.i.i, align 8, !tbaa !50
  %tobool.not.i.i.i237 = icmp eq %struct.ColumnIndex* %37, null
  br i1 %tobool.not.i.i.i237, label %_ZNSt6vectorI11ColumnIndexSaIS0_EED2Ev.exit258, label %if.then.i.i.i239

if.then.i.i.i239:                                 ; preds = %_ZNSt6vectorI11ColumnIndexSaIS0_EED2Ev.exit235
  %38 = bitcast %struct.ColumnIndex* %37 to i8*
  call void @_ZdlPv(i8* nonnull %38) #14
  br label %_ZNSt6vectorI11ColumnIndexSaIS0_EED2Ev.exit258

_ZNSt6vectorI11ColumnIndexSaIS0_EED2Ev.exit258:   ; preds = %_ZNSt6vectorI11ColumnIndexSaIS0_EED2Ev.exit235, %if.then.i.i.i239
  call void @_ZdlPv(i8* nonnull %call2.i.i.i.i.i.i162) #14
  call void @_ZdlPv(i8* nonnull %call2.i.i.i.i4.i81) #14
  call void @_ZdlPv(i8* nonnull %call2.i.i.i.i4.i) #14
  ret %class.HashGroupBy* %20

lpad12:                                           ; preds = %invoke.cont11
  %39 = landingpad { i8*, i32 }
          cleanup
  br label %_ZNSt6vectorI11ColumnIndexSaIS0_EED2Ev.exit80

lpad16:                                           ; preds = %invoke.cont15
  %40 = landingpad { i8*, i32 }
          cleanup
  br label %_ZNSt6vectorI11ColumnIndexSaIS0_EED2Ev.exit80

lpad22:                                           ; preds = %_ZNSt16allocator_traitsISaIP10AggregatorEE8allocateERS2_m.exit.i.i.i.i
  %41 = landingpad { i8*, i32 }
          cleanup
  br label %_ZNSt6vectorI11ColumnIndexSaIS0_EED2Ev.exit80

lpad25:                                           ; preds = %invoke.cont23
  %42 = landingpad { i8*, i32 }
          cleanup
  br label %if.then.i.i.i94

lpad27:                                           ; preds = %call2.i.i.i.i.i.i.noexc161
  %43 = landingpad { i8*, i32 }
          cleanup
  br label %if.then.i.i.i94

lpad31:                                           ; preds = %_ZNSt16allocator_traitsISaI11ColumnIndexEE8allocateERS1_m.exit.i.i.i.i
  %44 = landingpad { i8*, i32 }
          cleanup
  br label %cleanup.action

lpad34:                                           ; preds = %_ZNSt16allocator_traitsISaI11ColumnIndexEE8allocateERS1_m.exit.i.i.i.i180
  %45 = landingpad { i8*, i32 }
          cleanup
  br label %ehcleanup41

lpad37:                                           ; preds = %_ZNSt16allocator_traitsISaIP10AggregatorEE8allocateERS2_m.exit.i.i.i.i208
  %46 = landingpad { i8*, i32 }
          cleanup
  br label %ehcleanup

lpad39:                                           ; preds = %invoke.cont38
  %47 = landingpad { i8*, i32 }
          cleanup
  %48 = load %class.Aggregator**, %class.Aggregator*** %_M_start.i.i.i210, align 8, !tbaa !55
  %tobool.not.i.i.i260 = icmp eq %class.Aggregator** %48, null
  br i1 %tobool.not.i.i.i260, label %ehcleanup, label %if.then.i.i.i262

if.then.i.i.i262:                                 ; preds = %lpad39
  %49 = bitcast %class.Aggregator** %48 to i8*
  call void @_ZdlPv(i8* nonnull %49) #14
  br label %ehcleanup

ehcleanup:                                        ; preds = %if.then.i.i.i262, %lpad39, %lpad37
  %.pn = phi { i8*, i32 } [ %46, %lpad37 ], [ %47, %lpad39 ], [ %47, %if.then.i.i.i262 ]
  %50 = load %struct.ColumnIndex*, %struct.ColumnIndex** %_M_start.i.i.i182, align 8, !tbaa !50
  %tobool.not.i.i.i109 = icmp eq %struct.ColumnIndex* %50, null
  br i1 %tobool.not.i.i.i109, label %ehcleanup41, label %if.then.i.i.i110

if.then.i.i.i110:                                 ; preds = %ehcleanup
  %51 = bitcast %struct.ColumnIndex* %50 to i8*
  call void @_ZdlPv(i8* nonnull %51) #14
  br label %ehcleanup41

ehcleanup41:                                      ; preds = %if.then.i.i.i110, %ehcleanup, %lpad34
  %.pn.pn = phi { i8*, i32 } [ %45, %lpad34 ], [ %.pn, %ehcleanup ], [ %.pn, %if.then.i.i.i110 ]
  %52 = load %struct.ColumnIndex*, %struct.ColumnIndex** %_M_start.i.i.i, align 8, !tbaa !50
  %tobool.not.i.i.i96 = icmp eq %struct.ColumnIndex* %52, null
  br i1 %tobool.not.i.i.i96, label %cleanup.action, label %if.then.i.i.i97

if.then.i.i.i97:                                  ; preds = %ehcleanup41
  %53 = bitcast %struct.ColumnIndex* %52 to i8*
  call void @_ZdlPv(i8* nonnull %53) #14
  br label %cleanup.action

cleanup.action:                                   ; preds = %if.then.i.i.i97, %ehcleanup41, %lpad31
  %.pn.pn.pn = phi { i8*, i32 } [ %44, %lpad31 ], [ %.pn.pn, %ehcleanup41 ], [ %.pn.pn, %if.then.i.i.i97 ]
  call void @_ZdlPv(i8* nonnull %call29) #15
  br label %if.then.i.i.i94

if.then.i.i.i94:                                  ; preds = %lpad25, %lpad27, %cleanup.action
  %.pn.pn.pn.pn.pn.pn354 = phi { i8*, i32 } [ %43, %lpad27 ], [ %.pn.pn.pn, %cleanup.action ], [ %42, %lpad25 ]
  %54 = phi i8* [ %call2.i.i.i.i.i.i162, %lpad27 ], [ %call2.i.i.i.i.i.i162, %cleanup.action ], [ %call2.i.i.i.i.i.i115, %lpad25 ]
  call void @_ZdlPv(i8* nonnull %54) #14
  br label %_ZNSt6vectorI11ColumnIndexSaIS0_EED2Ev.exit80

_ZNSt6vectorI11ColumnIndexSaIS0_EED2Ev.exit80:    ; preds = %lpad12, %lpad16, %lpad22, %if.then.i.i.i94
  %.pn.pn.pn.pn.pn.pn346 = phi { i8*, i32 } [ %.pn.pn.pn.pn.pn.pn354, %if.then.i.i.i94 ], [ %40, %lpad16 ], [ %41, %lpad22 ], [ %39, %lpad12 ]
  call void @_ZdlPv(i8* nonnull %call2.i.i.i.i4.i81) #14
  br label %_ZNSt6vectorI11ColumnIndexSaIS0_EED2Ev.exit

_ZNSt6vectorI11ColumnIndexSaIS0_EED2Ev.exit:      ; preds = %_ZNSt12_Vector_baseI11ColumnIndexSaIS0_EED2Ev.exit.i89, %_ZNSt6vectorI11ColumnIndexSaIS0_EED2Ev.exit80
  %.pn.pn.pn.pn.pn.pn.pn = phi { i8*, i32 } [ %.pn.pn.pn.pn.pn.pn346, %_ZNSt6vectorI11ColumnIndexSaIS0_EED2Ev.exit80 ], [ %1, %_ZNSt12_Vector_baseI11ColumnIndexSaIS0_EED2Ev.exit.i89 ]
  call void @_ZdlPv(i8* nonnull %call2.i.i.i.i4.i) #14
  resume { i8*, i32 } %.pn.pn.pn.pn.pn.pn.pn
}

; Function Attrs: argmemonly nofree nosync nounwind willreturn
declare void @llvm.memcpy.p0i8.p0i8.i64(i8* noalias nocapture writeonly, i8* noalias nocapture readonly, i64, i1 immarg) #7

; Function Attrs: uwtable
define linkonce_odr dso_local void @_ZN11HashGroupByC2ESt6vectorI11ColumnIndexSaIS1_EES3_S0_IP10AggregatorSaIS5_EE(%class.HashGroupBy* nonnull dereferenceable(144) %this, %"class.std::vector.0"* %groupByCols, %"class.std::vector.0"* %aggCols, %"class.std::vector.5"* %aggregators) unnamed_addr #3 comdat align 2 personality i8* bitcast (i32 (...)* @__gxx_personality_v0 to i8*) {
entry:
  %0 = getelementptr inbounds %class.HashGroupBy, %class.HashGroupBy* %this, i64 0, i32 0, i32 0
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [9 x i8*] }, { [9 x i8*] }* @_ZTV11HashGroupBy, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %0, align 8, !tbaa !2
  %groupByCols2 = getelementptr inbounds %class.HashGroupBy, %class.HashGroupBy* %this, i64 0, i32 1
  %_M_finish.i.i = getelementptr inbounds %"class.std::vector.0", %"class.std::vector.0"* %groupByCols, i64 0, i32 0, i32 0, i32 1
  %1 = load %struct.ColumnIndex*, %struct.ColumnIndex** %_M_finish.i.i, align 8, !tbaa !54
  %_M_start.i.i13 = getelementptr inbounds %"class.std::vector.0", %"class.std::vector.0"* %groupByCols, i64 0, i32 0, i32 0, i32 0
  %2 = load %struct.ColumnIndex*, %struct.ColumnIndex** %_M_start.i.i13, align 8, !tbaa !50
  %sub.ptr.lhs.cast.i.i = ptrtoint %struct.ColumnIndex* %1 to i64
  %sub.ptr.rhs.cast.i.i = ptrtoint %struct.ColumnIndex* %2 to i64
  %sub.ptr.sub.i.i = sub i64 %sub.ptr.lhs.cast.i.i, %sub.ptr.rhs.cast.i.i
  %sub.ptr.div.i.i = ashr exact i64 %sub.ptr.sub.i.i, 3
  %3 = bitcast %"class.std::vector.0"* %groupByCols2 to i8*
  tail call void @llvm.memset.p0i8.i64(i8* nonnull align 8 dereferenceable(24) %3, i8 0, i64 24, i1 false) #14
  %cmp.not.i.i.i.i = icmp eq i64 %sub.ptr.sub.i.i, 0
  br i1 %cmp.not.i.i.i.i, label %invoke.cont.i, label %cond.true.i.i.i.i

cond.true.i.i.i.i:                                ; preds = %entry
  %cmp.i.i.i.i.i.i = icmp slt i64 %sub.ptr.sub.i.i, 0
  br i1 %cmp.i.i.i.i.i.i, label %if.then.i.i.i.i.i.i, label %_ZNSt16allocator_traitsISaI11ColumnIndexEE8allocateERS1_m.exit.i.i.i.i

if.then.i.i.i.i.i.i:                              ; preds = %cond.true.i.i.i.i
  tail call void @_ZSt17__throw_bad_allocv() #17
  unreachable

_ZNSt16allocator_traitsISaI11ColumnIndexEE8allocateERS1_m.exit.i.i.i.i: ; preds = %cond.true.i.i.i.i
  %call2.i.i.i.i3.i22.i14 = tail call noalias nonnull i8* @_Znwm(i64 %sub.ptr.sub.i.i) #16
  %4 = bitcast i8* %call2.i.i.i.i3.i22.i14 to %struct.ColumnIndex*
  br label %invoke.cont.i

invoke.cont.i:                                    ; preds = %_ZNSt16allocator_traitsISaI11ColumnIndexEE8allocateERS1_m.exit.i.i.i.i, %entry
  %cond.i.i.i.i = phi %struct.ColumnIndex* [ %4, %_ZNSt16allocator_traitsISaI11ColumnIndexEE8allocateERS1_m.exit.i.i.i.i ], [ null, %entry ]
  %_M_start.i.i.i = getelementptr inbounds %"class.std::vector.0", %"class.std::vector.0"* %groupByCols2, i64 0, i32 0, i32 0, i32 0
  store %struct.ColumnIndex* %cond.i.i.i.i, %struct.ColumnIndex** %_M_start.i.i.i, align 8, !tbaa !50
  %_M_finish.i.i.i = getelementptr inbounds %class.HashGroupBy, %class.HashGroupBy* %this, i64 0, i32 1, i32 0, i32 0, i32 1
  store %struct.ColumnIndex* %cond.i.i.i.i, %struct.ColumnIndex** %_M_finish.i.i.i, align 8, !tbaa !54
  %add.ptr.i.i.i = getelementptr inbounds %struct.ColumnIndex, %struct.ColumnIndex* %cond.i.i.i.i, i64 %sub.ptr.div.i.i
  %_M_end_of_storage.i.i.i = getelementptr inbounds %class.HashGroupBy, %class.HashGroupBy* %this, i64 0, i32 1, i32 0, i32 0, i32 2
  store %struct.ColumnIndex* %add.ptr.i.i.i, %struct.ColumnIndex** %_M_end_of_storage.i.i.i, align 8, !tbaa !53
  %5 = load %struct.ColumnIndex*, %struct.ColumnIndex** %_M_start.i.i13, align 8, !tbaa !15
  %6 = load %struct.ColumnIndex*, %struct.ColumnIndex** %_M_finish.i.i, align 8, !tbaa !15
  %sub.ptr.lhs.cast.i.i.i.i.i.i.i.i = ptrtoint %struct.ColumnIndex* %6 to i64
  %sub.ptr.rhs.cast.i.i.i.i.i.i.i.i = ptrtoint %struct.ColumnIndex* %5 to i64
  %sub.ptr.sub.i.i.i.i.i.i.i.i = sub i64 %sub.ptr.lhs.cast.i.i.i.i.i.i.i.i, %sub.ptr.rhs.cast.i.i.i.i.i.i.i.i
  %tobool.not.i.i.i.i.i.i.i.i = icmp eq i64 %sub.ptr.sub.i.i.i.i.i.i.i.i, 0
  br i1 %tobool.not.i.i.i.i.i.i.i.i, label %invoke.cont, label %if.then.i.i.i.i.i.i.i.i

if.then.i.i.i.i.i.i.i.i:                          ; preds = %invoke.cont.i
  %7 = bitcast %struct.ColumnIndex* %cond.i.i.i.i to i8*
  %8 = bitcast %struct.ColumnIndex* %5 to i8*
  tail call void @llvm.memmove.p0i8.p0i8.i64(i8* align 4 %7, i8* align 4 %8, i64 %sub.ptr.sub.i.i.i.i.i.i.i.i, i1 false) #14
  br label %invoke.cont

invoke.cont:                                      ; preds = %if.then.i.i.i.i.i.i.i.i, %invoke.cont.i
  %sub.ptr.div.i.i.i.i.i.i.i.i = ashr exact i64 %sub.ptr.sub.i.i.i.i.i.i.i.i, 3
  %add.ptr.i.i.i.i.i.i.i.i = getelementptr inbounds %struct.ColumnIndex, %struct.ColumnIndex* %cond.i.i.i.i, i64 %sub.ptr.div.i.i.i.i.i.i.i.i
  store %struct.ColumnIndex* %add.ptr.i.i.i.i.i.i.i.i, %struct.ColumnIndex** %_M_finish.i.i.i, align 8, !tbaa !54
  %aggCols3 = getelementptr inbounds %class.HashGroupBy, %class.HashGroupBy* %this, i64 0, i32 2
  %_M_finish.i.i15 = getelementptr inbounds %"class.std::vector.0", %"class.std::vector.0"* %aggCols, i64 0, i32 0, i32 0, i32 1
  %9 = load %struct.ColumnIndex*, %struct.ColumnIndex** %_M_finish.i.i15, align 8, !tbaa !54
  %_M_start.i.i16 = getelementptr inbounds %"class.std::vector.0", %"class.std::vector.0"* %aggCols, i64 0, i32 0, i32 0, i32 0
  %10 = load %struct.ColumnIndex*, %struct.ColumnIndex** %_M_start.i.i16, align 8, !tbaa !50
  %sub.ptr.lhs.cast.i.i17 = ptrtoint %struct.ColumnIndex* %9 to i64
  %sub.ptr.rhs.cast.i.i18 = ptrtoint %struct.ColumnIndex* %10 to i64
  %sub.ptr.sub.i.i19 = sub i64 %sub.ptr.lhs.cast.i.i17, %sub.ptr.rhs.cast.i.i18
  %sub.ptr.div.i.i20 = ashr exact i64 %sub.ptr.sub.i.i19, 3
  %11 = bitcast %"class.std::vector.0"* %aggCols3 to i8*
  tail call void @llvm.memset.p0i8.i64(i8* nonnull align 8 dereferenceable(24) %11, i8 0, i64 24, i1 false) #14
  %cmp.not.i.i.i.i21 = icmp eq i64 %sub.ptr.sub.i.i19, 0
  br i1 %cmp.not.i.i.i.i21, label %invoke.cont.i35, label %cond.true.i.i.i.i23

cond.true.i.i.i.i23:                              ; preds = %invoke.cont
  %cmp.i.i.i.i.i.i22 = icmp slt i64 %sub.ptr.sub.i.i19, 0
  br i1 %cmp.i.i.i.i.i.i22, label %if.then.i.i.i.i.i.i24, label %_ZNSt16allocator_traitsISaI11ColumnIndexEE8allocateERS1_m.exit.i.i.i.i25

if.then.i.i.i.i.i.i24:                            ; preds = %cond.true.i.i.i.i23
  invoke void @_ZSt17__throw_bad_allocv() #17
          to label %.noexc39 unwind label %lpad4

.noexc39:                                         ; preds = %if.then.i.i.i.i.i.i24
  unreachable

_ZNSt16allocator_traitsISaI11ColumnIndexEE8allocateERS1_m.exit.i.i.i.i25: ; preds = %cond.true.i.i.i.i23
  %call2.i.i.i.i3.i22.i41 = invoke noalias nonnull i8* @_Znwm(i64 %sub.ptr.sub.i.i19) #16
          to label %call2.i.i.i.i3.i22.i.noexc40 unwind label %lpad4

call2.i.i.i.i3.i22.i.noexc40:                     ; preds = %_ZNSt16allocator_traitsISaI11ColumnIndexEE8allocateERS1_m.exit.i.i.i.i25
  %12 = bitcast i8* %call2.i.i.i.i3.i22.i41 to %struct.ColumnIndex*
  br label %invoke.cont.i35

invoke.cont.i35:                                  ; preds = %call2.i.i.i.i3.i22.i.noexc40, %invoke.cont
  %cond.i.i.i.i26 = phi %struct.ColumnIndex* [ %12, %call2.i.i.i.i3.i22.i.noexc40 ], [ null, %invoke.cont ]
  %_M_start.i.i.i27 = getelementptr inbounds %"class.std::vector.0", %"class.std::vector.0"* %aggCols3, i64 0, i32 0, i32 0, i32 0
  store %struct.ColumnIndex* %cond.i.i.i.i26, %struct.ColumnIndex** %_M_start.i.i.i27, align 8, !tbaa !50
  %_M_finish.i.i.i28 = getelementptr inbounds %class.HashGroupBy, %class.HashGroupBy* %this, i64 0, i32 2, i32 0, i32 0, i32 1
  store %struct.ColumnIndex* %cond.i.i.i.i26, %struct.ColumnIndex** %_M_finish.i.i.i28, align 8, !tbaa !54
  %add.ptr.i.i.i29 = getelementptr inbounds %struct.ColumnIndex, %struct.ColumnIndex* %cond.i.i.i.i26, i64 %sub.ptr.div.i.i20
  %_M_end_of_storage.i.i.i30 = getelementptr inbounds %class.HashGroupBy, %class.HashGroupBy* %this, i64 0, i32 2, i32 0, i32 0, i32 2
  store %struct.ColumnIndex* %add.ptr.i.i.i29, %struct.ColumnIndex** %_M_end_of_storage.i.i.i30, align 8, !tbaa !53
  %13 = load %struct.ColumnIndex*, %struct.ColumnIndex** %_M_start.i.i16, align 8, !tbaa !15
  %14 = load %struct.ColumnIndex*, %struct.ColumnIndex** %_M_finish.i.i15, align 8, !tbaa !15
  %sub.ptr.lhs.cast.i.i.i.i.i.i.i.i31 = ptrtoint %struct.ColumnIndex* %14 to i64
  %sub.ptr.rhs.cast.i.i.i.i.i.i.i.i32 = ptrtoint %struct.ColumnIndex* %13 to i64
  %sub.ptr.sub.i.i.i.i.i.i.i.i33 = sub i64 %sub.ptr.lhs.cast.i.i.i.i.i.i.i.i31, %sub.ptr.rhs.cast.i.i.i.i.i.i.i.i32
  %tobool.not.i.i.i.i.i.i.i.i34 = icmp eq i64 %sub.ptr.sub.i.i.i.i.i.i.i.i33, 0
  br i1 %tobool.not.i.i.i.i.i.i.i.i34, label %invoke.cont5, label %if.then.i.i.i.i.i.i.i.i36

if.then.i.i.i.i.i.i.i.i36:                        ; preds = %invoke.cont.i35
  %15 = bitcast %struct.ColumnIndex* %cond.i.i.i.i26 to i8*
  %16 = bitcast %struct.ColumnIndex* %13 to i8*
  tail call void @llvm.memmove.p0i8.p0i8.i64(i8* align 4 %15, i8* align 4 %16, i64 %sub.ptr.sub.i.i.i.i.i.i.i.i33, i1 false) #14
  br label %invoke.cont5

invoke.cont5:                                     ; preds = %if.then.i.i.i.i.i.i.i.i36, %invoke.cont.i35
  %sub.ptr.div.i.i.i.i.i.i.i.i37 = ashr exact i64 %sub.ptr.sub.i.i.i.i.i.i.i.i33, 3
  %add.ptr.i.i.i.i.i.i.i.i38 = getelementptr inbounds %struct.ColumnIndex, %struct.ColumnIndex* %cond.i.i.i.i26, i64 %sub.ptr.div.i.i.i.i.i.i.i.i37
  store %struct.ColumnIndex* %add.ptr.i.i.i.i.i.i.i.i38, %struct.ColumnIndex** %_M_finish.i.i.i28, align 8, !tbaa !54
  %aggregators6 = getelementptr inbounds %class.HashGroupBy, %class.HashGroupBy* %this, i64 0, i32 3
  %_M_finish.i.i43 = getelementptr inbounds %"class.std::vector.5", %"class.std::vector.5"* %aggregators, i64 0, i32 0, i32 0, i32 1
  %17 = load %class.Aggregator**, %class.Aggregator*** %_M_finish.i.i43, align 8, !tbaa !59
  %_M_start.i.i44 = getelementptr inbounds %"class.std::vector.5", %"class.std::vector.5"* %aggregators, i64 0, i32 0, i32 0, i32 0
  %18 = load %class.Aggregator**, %class.Aggregator*** %_M_start.i.i44, align 8, !tbaa !55
  %sub.ptr.lhs.cast.i.i45 = ptrtoint %class.Aggregator** %17 to i64
  %sub.ptr.rhs.cast.i.i46 = ptrtoint %class.Aggregator** %18 to i64
  %sub.ptr.sub.i.i47 = sub i64 %sub.ptr.lhs.cast.i.i45, %sub.ptr.rhs.cast.i.i46
  %sub.ptr.div.i.i48 = ashr exact i64 %sub.ptr.sub.i.i47, 3
  %19 = bitcast %"class.std::vector.5"* %aggregators6 to i8*
  tail call void @llvm.memset.p0i8.i64(i8* nonnull align 8 dereferenceable(24) %19, i8 0, i64 24, i1 false) #14
  %cmp.not.i.i.i.i49 = icmp eq i64 %sub.ptr.sub.i.i47, 0
  br i1 %cmp.not.i.i.i.i49, label %invoke.cont.i62, label %cond.true.i.i.i.i51

cond.true.i.i.i.i51:                              ; preds = %invoke.cont5
  %cmp.i.i.i.i.i.i50 = icmp slt i64 %sub.ptr.sub.i.i47, 0
  br i1 %cmp.i.i.i.i.i.i50, label %if.then.i.i.i.i.i.i52, label %_ZNSt16allocator_traitsISaIP10AggregatorEE8allocateERS2_m.exit.i.i.i.i

if.then.i.i.i.i.i.i52:                            ; preds = %cond.true.i.i.i.i51
  invoke void @_ZSt17__throw_bad_allocv() #17
          to label %.noexc66 unwind label %lpad7

.noexc66:                                         ; preds = %if.then.i.i.i.i.i.i52
  unreachable

_ZNSt16allocator_traitsISaIP10AggregatorEE8allocateERS2_m.exit.i.i.i.i: ; preds = %cond.true.i.i.i.i51
  %call2.i.i.i.i3.i22.i68 = invoke noalias nonnull i8* @_Znwm(i64 %sub.ptr.sub.i.i47) #16
          to label %call2.i.i.i.i3.i22.i.noexc67 unwind label %lpad7

call2.i.i.i.i3.i22.i.noexc67:                     ; preds = %_ZNSt16allocator_traitsISaIP10AggregatorEE8allocateERS2_m.exit.i.i.i.i
  %20 = bitcast i8* %call2.i.i.i.i3.i22.i68 to %class.Aggregator**
  br label %invoke.cont.i62

invoke.cont.i62:                                  ; preds = %call2.i.i.i.i3.i22.i.noexc67, %invoke.cont5
  %cond.i.i.i.i53 = phi %class.Aggregator** [ %20, %call2.i.i.i.i3.i22.i.noexc67 ], [ null, %invoke.cont5 ]
  %_M_start.i.i.i54 = getelementptr inbounds %"class.std::vector.5", %"class.std::vector.5"* %aggregators6, i64 0, i32 0, i32 0, i32 0
  store %class.Aggregator** %cond.i.i.i.i53, %class.Aggregator*** %_M_start.i.i.i54, align 8, !tbaa !55
  %_M_finish.i.i.i55 = getelementptr inbounds %class.HashGroupBy, %class.HashGroupBy* %this, i64 0, i32 3, i32 0, i32 0, i32 1
  store %class.Aggregator** %cond.i.i.i.i53, %class.Aggregator*** %_M_finish.i.i.i55, align 8, !tbaa !59
  %add.ptr.i.i.i56 = getelementptr inbounds %class.Aggregator*, %class.Aggregator** %cond.i.i.i.i53, i64 %sub.ptr.div.i.i48
  %_M_end_of_storage.i.i.i57 = getelementptr inbounds %class.HashGroupBy, %class.HashGroupBy* %this, i64 0, i32 3, i32 0, i32 0, i32 2
  store %class.Aggregator** %add.ptr.i.i.i56, %class.Aggregator*** %_M_end_of_storage.i.i.i57, align 8, !tbaa !58
  %21 = load %class.Aggregator**, %class.Aggregator*** %_M_start.i.i44, align 8, !tbaa !15
  %22 = load %class.Aggregator**, %class.Aggregator*** %_M_finish.i.i43, align 8, !tbaa !15
  %sub.ptr.lhs.cast.i.i.i.i.i.i.i.i58 = ptrtoint %class.Aggregator** %22 to i64
  %sub.ptr.rhs.cast.i.i.i.i.i.i.i.i59 = ptrtoint %class.Aggregator** %21 to i64
  %sub.ptr.sub.i.i.i.i.i.i.i.i60 = sub i64 %sub.ptr.lhs.cast.i.i.i.i.i.i.i.i58, %sub.ptr.rhs.cast.i.i.i.i.i.i.i.i59
  %tobool.not.i.i.i.i.i.i.i.i61 = icmp eq i64 %sub.ptr.sub.i.i.i.i.i.i.i.i60, 0
  br i1 %tobool.not.i.i.i.i.i.i.i.i61, label %invoke.cont8, label %if.then.i.i.i.i.i.i.i.i63

if.then.i.i.i.i.i.i.i.i63:                        ; preds = %invoke.cont.i62
  %23 = bitcast %class.Aggregator** %cond.i.i.i.i53 to i8*
  %24 = bitcast %class.Aggregator** %21 to i8*
  tail call void @llvm.memmove.p0i8.p0i8.i64(i8* align 8 %23, i8* align 8 %24, i64 %sub.ptr.sub.i.i.i.i.i.i.i.i60, i1 false) #14
  br label %invoke.cont8

invoke.cont8:                                     ; preds = %if.then.i.i.i.i.i.i.i.i63, %invoke.cont.i62
  %sub.ptr.div.i.i.i.i.i.i.i.i64 = ashr exact i64 %sub.ptr.sub.i.i.i.i.i.i.i.i60, 3
  %add.ptr.i.i.i.i.i.i.i.i65 = getelementptr inbounds %class.Aggregator*, %class.Aggregator** %cond.i.i.i.i53, i64 %sub.ptr.div.i.i.i.i.i.i.i.i64
  store %class.Aggregator** %add.ptr.i.i.i.i.i.i.i.i65, %class.Aggregator*** %_M_finish.i.i.i55, align 8, !tbaa !59
  %_M_buckets.i.i = getelementptr inbounds %class.HashGroupBy, %class.HashGroupBy* %this, i64 0, i32 4, i32 0, i32 0
  %_M_single_bucket.i.i = getelementptr inbounds %class.HashGroupBy, %class.HashGroupBy* %this, i64 0, i32 4, i32 0, i32 5
  store %"struct.std::__detail::_Hash_node_base"** %_M_single_bucket.i.i, %"struct.std::__detail::_Hash_node_base"*** %_M_buckets.i.i, align 8, !tbaa !47
  %_M_bucket_count.i.i = getelementptr inbounds %class.HashGroupBy, %class.HashGroupBy* %this, i64 0, i32 4, i32 0, i32 1
  store i64 1, i64* %_M_bucket_count.i.i, align 8, !tbaa !48
  %_M_nxt.i.i.i = getelementptr inbounds %class.HashGroupBy, %class.HashGroupBy* %this, i64 0, i32 4, i32 0, i32 2, i32 0
  %_M_max_load_factor.i.i.i = getelementptr inbounds %class.HashGroupBy, %class.HashGroupBy* %this, i64 0, i32 4, i32 0, i32 4, i32 0
  %25 = bitcast %"struct.std::__detail::_Hash_node_base"** %_M_nxt.i.i.i to i8*
  tail call void @llvm.memset.p0i8.i64(i8* nonnull align 8 dereferenceable(16) %25, i8 0, i64 16, i1 false) #14
  store float 1.000000e+00, float* %_M_max_load_factor.i.i.i, align 8, !tbaa !49
  %_M_next_resize.i.i.i = getelementptr inbounds %class.HashGroupBy, %class.HashGroupBy* %this, i64 0, i32 4, i32 0, i32 4, i32 1
  %26 = bitcast i64* %_M_next_resize.i.i.i to i8*
  tail call void @llvm.memset.p0i8.i64(i8* nonnull align 8 dereferenceable(16) %26, i8 0, i64 16, i1 false) #14
  ret void

lpad4:                                            ; preds = %_ZNSt16allocator_traitsISaI11ColumnIndexEE8allocateERS1_m.exit.i.i.i.i25, %if.then.i.i.i.i.i.i24
  %27 = landingpad { i8*, i32 }
          cleanup
  br label %ehcleanup

lpad7:                                            ; preds = %_ZNSt16allocator_traitsISaIP10AggregatorEE8allocateERS2_m.exit.i.i.i.i, %if.then.i.i.i.i.i.i52
  %28 = landingpad { i8*, i32 }
          cleanup
  %29 = load %struct.ColumnIndex*, %struct.ColumnIndex** %_M_start.i.i.i27, align 8, !tbaa !50
  %tobool.not.i.i.i70 = icmp eq %struct.ColumnIndex* %29, null
  br i1 %tobool.not.i.i.i70, label %ehcleanup, label %if.then.i.i.i72

if.then.i.i.i72:                                  ; preds = %lpad7
  %30 = bitcast %struct.ColumnIndex* %29 to i8*
  tail call void @_ZdlPv(i8* nonnull %30) #14
  br label %ehcleanup

ehcleanup:                                        ; preds = %if.then.i.i.i72, %lpad7, %lpad4
  %.pn = phi { i8*, i32 } [ %27, %lpad4 ], [ %28, %lpad7 ], [ %28, %if.then.i.i.i72 ]
  %31 = load %struct.ColumnIndex*, %struct.ColumnIndex** %_M_start.i.i.i, align 8, !tbaa !50
  %tobool.not.i.i.i = icmp eq %struct.ColumnIndex* %31, null
  br i1 %tobool.not.i.i.i, label %ehcleanup9, label %if.then.i.i.i

if.then.i.i.i:                                    ; preds = %ehcleanup
  %32 = bitcast %struct.ColumnIndex* %31 to i8*
  tail call void @_ZdlPv(i8* nonnull %32) #14
  br label %ehcleanup9

ehcleanup9:                                       ; preds = %if.then.i.i.i, %ehcleanup
  resume { i8*, i32 } %.pn
}

; Function Attrs: uwtable
define dso_local i32 @_Z13test_group_byiiPii(i32 %page_count, i32 %row_count, i32* nocapture readonly %data_type, i32 %column_count) local_unnamed_addr #3 {
entry:
  %call = tail call %class.Table** @_Z9buildDataiiPii(i32 %page_count, i32 %row_count, i32* %data_type, i32 %column_count)
  %call1 = tail call %class.HashGroupBy* @_Z13createGroupByv()
  %call2 = tail call i64 @_ZNSt6chrono3_V212system_clock3nowEv() #14
  %cmp148 = icmp sgt i32 %page_count, 0
  br i1 %cmp148, label %for.body.lr.ph, label %for.cond.cleanup

for.body.lr.ph:                                   ; preds = %entry
  %0 = bitcast %class.HashGroupBy* %call1 to void (%class.HashGroupBy*, %class.Table*, i32)***
  %wide.trip.count156 = zext i32 %page_count to i64
  br label %for.body

for.cond.cleanup:                                 ; preds = %for.body, %entry
  %1 = bitcast %class.HashGroupBy* %call1 to %class.Table* (%class.HashGroupBy*)***
  %vtable4 = load %class.Table* (%class.HashGroupBy*)**, %class.Table* (%class.HashGroupBy*)*** %1, align 8, !tbaa !2
  %vfn5 = getelementptr inbounds %class.Table* (%class.HashGroupBy*)*, %class.Table* (%class.HashGroupBy*)** %vtable4, i64 4
  %2 = load %class.Table* (%class.HashGroupBy*)*, %class.Table* (%class.HashGroupBy*)** %vfn5, align 8
  %call6 = tail call %class.Table* %2(%class.HashGroupBy* nonnull dereferenceable(144) %call1)
  %call7 = tail call i64 @_ZNSt6chrono3_V212system_clock3nowEv() #14
  %sub.i.i = sub nsw i64 %call7, %call2
  %conv.i.i.i = sitofp i64 %sub.i.i to float
  %div.i.i.i = fdiv float %conv.i.i.i, 1.000000e+09
  %mul.i.i = fmul float %div.i.i.i, 1.000000e+03
  %conv.i.i = fptosi float %mul.i.i to i64
  %call1.i = tail call nonnull align 8 dereferenceable(8) %"class.std::basic_ostream"* @_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l(%"class.std::basic_ostream"* nonnull align 8 dereferenceable(8) @_ZSt4cout, i8* nonnull getelementptr inbounds ([24 x i8], [24 x i8]* @.str, i64 0, i64 0), i64 23)
  %call.i = tail call nonnull align 8 dereferenceable(8) %"class.std::basic_ostream"* @_ZNSo9_M_insertIlEERSoT_(%"class.std::basic_ostream"* nonnull dereferenceable(8) @_ZSt4cout, i64 %conv.i.i)
  %call1.i121 = tail call nonnull align 8 dereferenceable(8) %"class.std::basic_ostream"* @_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l(%"class.std::basic_ostream"* nonnull align 8 dereferenceable(8) %call.i, i8* nonnull getelementptr inbounds ([4 x i8], [4 x i8]* @.str.1, i64 0, i64 0), i64 3)
  %columnCount.i122 = getelementptr inbounds %class.Table, %class.Table* %call6, i64 0, i32 6
  %3 = load i32, i32* %columnCount.i122, align 4, !tbaa !12
  %cmp21146.not = icmp eq i32 %3, 0
  br i1 %cmp21146.not, label %for.cond.cleanup22, label %for.body23.lr.ph

for.body23.lr.ph:                                 ; preds = %for.cond.cleanup
  %_M_start.i.i130 = getelementptr inbounds %class.Table, %class.Table* %call6, i64 0, i32 3, i32 0, i32 0, i32 0
  br label %for.body23

for.body:                                         ; preds = %for.body.lr.ph, %for.body
  %indvars.iv154 = phi i64 [ 0, %for.body.lr.ph ], [ %indvars.iv.next155, %for.body ]
  %arrayidx = getelementptr inbounds %class.Table*, %class.Table** %call, i64 %indvars.iv154
  %4 = load %class.Table*, %class.Table** %arrayidx, align 8, !tbaa !15
  %vtable = load void (%class.HashGroupBy*, %class.Table*, i32)**, void (%class.HashGroupBy*, %class.Table*, i32)*** %0, align 8, !tbaa !2
  %vfn = getelementptr inbounds void (%class.HashGroupBy*, %class.Table*, i32)*, void (%class.HashGroupBy*, %class.Table*, i32)** %vtable, i64 3
  %5 = load void (%class.HashGroupBy*, %class.Table*, i32)*, void (%class.HashGroupBy*, %class.Table*, i32)** %vfn, align 8
  tail call void %5(%class.HashGroupBy* nonnull dereferenceable(144) %call1, %class.Table* %4, i32 %row_count)
  %indvars.iv.next155 = add nuw nsw i64 %indvars.iv154, 1
  %exitcond157.not = icmp eq i64 %indvars.iv.next155, %wide.trip.count156
  br i1 %exitcond157.not, label %for.cond.cleanup, label %for.body, !llvm.loop !60

for.cond.cleanup22:                               ; preds = %for.body23, %for.cond.cleanup
  %call1.i124 = tail call nonnull align 8 dereferenceable(8) %"class.std::basic_ostream"* @_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l(%"class.std::basic_ostream"* nonnull align 8 dereferenceable(8) @_ZSt4cout, i8* nonnull getelementptr inbounds ([30 x i8], [30 x i8]* @.str.2, i64 0, i64 0), i64 29)
  %call29 = tail call nonnull align 8 dereferenceable(8) %"class.std::basic_ostream"* @_ZNSolsEi(%"class.std::basic_ostream"* nonnull dereferenceable(8) @_ZSt4cout, i32 %page_count)
  %call1.i126 = tail call nonnull align 8 dereferenceable(8) %"class.std::basic_ostream"* @_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l(%"class.std::basic_ostream"* nonnull align 8 dereferenceable(8) %call29, i8* nonnull getelementptr inbounds ([13 x i8], [13 x i8]* @.str.3, i64 0, i64 0), i64 12)
  %call31 = tail call nonnull align 8 dereferenceable(8) %"class.std::basic_ostream"* @_ZNSolsEi(%"class.std::basic_ostream"* nonnull dereferenceable(8) %call29, i32 %row_count)
  %call1.i128 = tail call nonnull align 8 dereferenceable(8) %"class.std::basic_ostream"* @_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l(%"class.std::basic_ostream"* nonnull align 8 dereferenceable(8) %call31, i8* nonnull getelementptr inbounds ([2 x i8], [2 x i8]* @.str.4, i64 0, i64 0), i64 1)
  br i1 %cmp148, label %for.body37.preheader, label %for.cond60.preheader

for.body37.preheader:                             ; preds = %for.cond.cleanup22
  %wide.trip.count = zext i32 %page_count to i64
  br label %for.body37

for.body23:                                       ; preds = %for.body23.lr.ph, %for.body23
  %indvars.iv152 = phi i64 [ 0, %for.body23.lr.ph ], [ %indvars.iv.next153, %for.body23 ]
  %6 = load %class.Column**, %class.Column*** %_M_start.i.i130, align 8, !tbaa !32
  %add.ptr.i.i131 = getelementptr inbounds %class.Column*, %class.Column** %6, i64 %indvars.iv152
  %7 = load %class.Column*, %class.Column** %add.ptr.i.i131, align 8, !tbaa !15
  tail call void @_ZN6Column11printColumnEv(%class.Column* nonnull dereferenceable(40) %7)
  %indvars.iv.next153 = add nuw nsw i64 %indvars.iv152, 1
  %8 = load i32, i32* %columnCount.i122, align 4, !tbaa !12
  %9 = zext i32 %8 to i64
  %cmp21 = icmp ult i64 %indvars.iv.next153, %9
  br i1 %cmp21, label %for.body23, label %for.cond.cleanup22, !llvm.loop !61

for.cond60.preheader:                             ; preds = %for.inc56, %for.cond.cleanup22
  %10 = load i32, i32* %columnCount.i122, align 4, !tbaa !12
  %cmp62142.not = icmp eq i32 %10, 0
  br i1 %cmp62142.not, label %delete.notnull80, label %for.body64.lr.ph

for.body64.lr.ph:                                 ; preds = %for.cond60.preheader
  %_M_start.i.i = getelementptr inbounds %class.Table, %class.Table* %call6, i64 0, i32 3, i32 0, i32 0, i32 0
  br label %for.body64

for.body37:                                       ; preds = %for.body37.preheader, %for.inc56
  %indvars.iv150 = phi i64 [ 0, %for.body37.preheader ], [ %indvars.iv.next151, %for.inc56 ]
  %arrayidx39 = getelementptr inbounds %class.Table*, %class.Table** %call, i64 %indvars.iv150
  %11 = load %class.Table*, %class.Table** %arrayidx39, align 8, !tbaa !15
  %_M_start.i.i132 = getelementptr inbounds %class.Table, %class.Table* %11, i64 0, i32 3, i32 0, i32 0, i32 0
  %12 = load %class.Column**, %class.Column*** %_M_start.i.i132, align 8, !tbaa !32
  %13 = load %class.Column*, %class.Column** %12, align 8, !tbaa !15
  %data.i133 = getelementptr inbounds %class.Column, %class.Column* %13, i64 0, i32 1
  %14 = load i8*, i8** %data.i133, align 8, !tbaa !21
  %isnull = icmp eq i8* %14, null
  br i1 %isnull, label %delete.end, label %delete.notnull

delete.notnull:                                   ; preds = %for.body37
  tail call void @_ZdaPv(i8* nonnull %14) #15
  %.pre = load %class.Column**, %class.Column*** %_M_start.i.i132, align 8, !tbaa !32
  br label %delete.end

delete.end:                                       ; preds = %delete.notnull, %for.body37
  %15 = phi %class.Column** [ %.pre, %delete.notnull ], [ %12, %for.body37 ]
  %add.ptr.i.i135 = getelementptr inbounds %class.Column*, %class.Column** %15, i64 1
  %16 = load %class.Column*, %class.Column** %add.ptr.i.i135, align 8, !tbaa !15
  %data.i136 = getelementptr inbounds %class.Column, %class.Column* %16, i64 0, i32 1
  %17 = load i8*, i8** %data.i136, align 8, !tbaa !21
  %isnull46 = icmp eq i8* %17, null
  br i1 %isnull46, label %for.inc56, label %delete.notnull47

delete.notnull47:                                 ; preds = %delete.end
  tail call void @_ZdaPv(i8* nonnull %17) #15
  br label %for.inc56

for.inc56:                                        ; preds = %delete.notnull47, %delete.end
  %18 = bitcast %class.Table* %11 to void (%class.Table*)***
  %vtable53 = load void (%class.Table*)**, void (%class.Table*)*** %18, align 8, !tbaa !2
  %vfn54 = getelementptr inbounds void (%class.Table*)*, void (%class.Table*)** %vtable53, i64 1
  %19 = load void (%class.Table*)*, void (%class.Table*)** %vfn54, align 8
  tail call void %19(%class.Table* nonnull dereferenceable(60) %11) #14
  %indvars.iv.next151 = add nuw nsw i64 %indvars.iv150, 1
  %exitcond.not = icmp eq i64 %indvars.iv.next151, %wide.trip.count
  br i1 %exitcond.not, label %for.cond60.preheader, label %for.body37, !llvm.loop !62

for.body64:                                       ; preds = %for.body64.lr.ph, %delete.notnull72
  %indvars.iv = phi i64 [ 0, %for.body64.lr.ph ], [ %indvars.iv.next, %delete.notnull72 ]
  %20 = load %class.Column**, %class.Column*** %_M_start.i.i, align 8, !tbaa !32
  %add.ptr.i.i = getelementptr inbounds %class.Column*, %class.Column** %20, i64 %indvars.iv
  %21 = load %class.Column*, %class.Column** %add.ptr.i.i, align 8, !tbaa !15
  %data.i = getelementptr inbounds %class.Column, %class.Column* %21, i64 0, i32 1
  %22 = load i8*, i8** %data.i, align 8, !tbaa !21
  %isnull68 = icmp eq i8* %22, null
  br i1 %isnull68, label %delete.notnull72, label %delete.notnull69

delete.notnull69:                                 ; preds = %for.body64
  tail call void @_ZdlPv(i8* nonnull %22) #15
  br label %delete.notnull72

delete.notnull72:                                 ; preds = %for.body64, %delete.notnull69
  %23 = bitcast %class.Column* %21 to void (%class.Column*)***
  %vtable73 = load void (%class.Column*)**, void (%class.Column*)*** %23, align 8, !tbaa !2
  %vfn74 = getelementptr inbounds void (%class.Column*)*, void (%class.Column*)** %vtable73, i64 1
  %24 = load void (%class.Column*)*, void (%class.Column*)** %vfn74, align 8
  tail call void %24(%class.Column* nonnull dereferenceable(40) %21) #14
  %indvars.iv.next = add nuw nsw i64 %indvars.iv, 1
  %25 = load i32, i32* %columnCount.i122, align 4, !tbaa !12
  %26 = zext i32 %25 to i64
  %cmp62 = icmp ult i64 %indvars.iv.next, %26
  br i1 %cmp62, label %for.body64, label %delete.notnull80, !llvm.loop !63

delete.notnull80:                                 ; preds = %delete.notnull72, %for.cond60.preheader
  %27 = bitcast %class.Table* %call6 to void (%class.Table*)***
  %vtable81 = load void (%class.Table*)**, void (%class.Table*)*** %27, align 8, !tbaa !2
  %vfn82 = getelementptr inbounds void (%class.Table*)*, void (%class.Table*)** %vtable81, i64 1
  %28 = load void (%class.Table*)*, void (%class.Table*)** %vfn82, align 8
  tail call void %28(%class.Table* nonnull dereferenceable(60) %call6) #14
  ret i32 1234
}

; Function Attrs: nounwind
declare dso_local i64 @_ZNSt6chrono3_V212system_clock3nowEv() local_unnamed_addr #1

; Function Attrs: uwtable mustprogress
define linkonce_odr dso_local void @_ZN6Column11printColumnEv(%class.Column* nonnull dereferenceable(40) %this) local_unnamed_addr #8 comdat align 2 {
entry:
  %size = getelementptr inbounds %class.Column, %class.Column* %this, i64 0, i32 4
  %0 = load i64, i64* %size, align 8, !tbaa !26
  %cmp = icmp eq i64 %0, 0
  br i1 %cmp, label %if.then, label %for.body.lr.ph

if.then:                                          ; preds = %entry
  %call1.i = tail call nonnull align 8 dereferenceable(8) %"class.std::basic_ostream"* @_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l(%"class.std::basic_ostream"* nonnull align 8 dereferenceable(8) @_ZSt4cout, i8* nonnull getelementptr inbounds ([6 x i8], [6 x i8]* @.str.6, i64 0, i64 0), i64 5)
  %vtable.i = load i8*, i8** bitcast (%"class.std::basic_ostream"* @_ZSt4cout to i8**), align 8, !tbaa !2
  %vbase.offset.ptr.i = getelementptr i8, i8* %vtable.i, i64 -24
  %1 = bitcast i8* %vbase.offset.ptr.i to i64*
  %vbase.offset.i = load i64, i64* %1, align 8
  %add.ptr.i = getelementptr inbounds i8, i8* bitcast (%"class.std::basic_ostream"* @_ZSt4cout to i8*), i64 %vbase.offset.i
  %_M_ctype.i = getelementptr inbounds i8, i8* %add.ptr.i, i64 240
  %2 = bitcast i8* %_M_ctype.i to %"class.std::ctype"**
  %3 = load %"class.std::ctype"*, %"class.std::ctype"** %2, align 8, !tbaa !64
  %tobool.not.i63 = icmp eq %"class.std::ctype"* %3, null
  br i1 %tobool.not.i63, label %if.then.i64, label %_ZSt13__check_facetISt5ctypeIcEERKT_PS3_.exit

if.then.i64:                                      ; preds = %if.then
  tail call void @_ZSt16__throw_bad_castv() #17
  unreachable

_ZSt13__check_facetISt5ctypeIcEERKT_PS3_.exit:    ; preds = %if.then
  %_M_widen_ok.i = getelementptr inbounds %"class.std::ctype", %"class.std::ctype"* %3, i64 0, i32 8
  %4 = load i8, i8* %_M_widen_ok.i, align 8, !tbaa !67
  %tobool.not.i = icmp eq i8 %4, 0
  br i1 %tobool.not.i, label %if.end.i, label %if.then.i

if.then.i:                                        ; preds = %_ZSt13__check_facetISt5ctypeIcEERKT_PS3_.exit
  %arrayidx.i = getelementptr inbounds %"class.std::ctype", %"class.std::ctype"* %3, i64 0, i32 9, i64 10
  %5 = load i8, i8* %arrayidx.i, align 1, !tbaa !69
  br label %if.end

if.end.i:                                         ; preds = %_ZSt13__check_facetISt5ctypeIcEERKT_PS3_.exit
  tail call void @_ZNKSt5ctypeIcE13_M_widen_initEv(%"class.std::ctype"* nonnull dereferenceable(570) %3)
  %6 = bitcast %"class.std::ctype"* %3 to i8 (%"class.std::ctype"*, i8)***
  %vtable.i49 = load i8 (%"class.std::ctype"*, i8)**, i8 (%"class.std::ctype"*, i8)*** %6, align 8, !tbaa !2
  %vfn.i = getelementptr inbounds i8 (%"class.std::ctype"*, i8)*, i8 (%"class.std::ctype"*, i8)** %vtable.i49, i64 6
  %7 = load i8 (%"class.std::ctype"*, i8)*, i8 (%"class.std::ctype"*, i8)** %vfn.i, align 8
  %call.i50 = tail call signext i8 %7(%"class.std::ctype"* nonnull dereferenceable(570) %3, i8 signext 10)
  br label %if.end

if.end:                                           ; preds = %if.end.i, %if.then.i
  %retval.0.i = phi i8 [ %5, %if.then.i ], [ %call.i50, %if.end.i ]
  %call1.i31 = tail call nonnull align 8 dereferenceable(8) %"class.std::basic_ostream"* @_ZNSo3putEc(%"class.std::basic_ostream"* nonnull dereferenceable(8) @_ZSt4cout, i8 signext %retval.0.i)
  %call.i = tail call nonnull align 8 dereferenceable(8) %"class.std::basic_ostream"* @_ZNSo5flushEv(%"class.std::basic_ostream"* nonnull dereferenceable(8) %call1.i31)
  %.pre = load i64, i64* %size, align 8, !tbaa !26
  %cmp470.not = icmp eq i64 %.pre, 0
  br i1 %cmp470.not, label %for.cond.cleanup, label %for.body.lr.ph

for.body.lr.ph:                                   ; preds = %entry, %if.end
  %type = getelementptr inbounds %class.Column, %class.Column* %this, i64 0, i32 3
  %data16 = getelementptr inbounds %class.Column, %class.Column* %this, i64 0, i32 1
  %8 = bitcast i8** %data16 to double**
  %9 = bitcast i8** %data16 to i64**
  %10 = bitcast i8** %data16 to i32**
  br label %for.body

for.cond.cleanup:                                 ; preds = %for.inc, %if.end
  %vtable.i33 = load i8*, i8** bitcast (%"class.std::basic_ostream"* @_ZSt4cout to i8**), align 8, !tbaa !2
  %vbase.offset.ptr.i34 = getelementptr i8, i8* %vtable.i33, i64 -24
  %11 = bitcast i8* %vbase.offset.ptr.i34 to i64*
  %vbase.offset.i35 = load i64, i64* %11, align 8
  %add.ptr.i36 = getelementptr inbounds i8, i8* bitcast (%"class.std::basic_ostream"* @_ZSt4cout to i8*), i64 %vbase.offset.i35
  %_M_ctype.i51 = getelementptr inbounds i8, i8* %add.ptr.i36, i64 240
  %12 = bitcast i8* %_M_ctype.i51 to %"class.std::ctype"**
  %13 = load %"class.std::ctype"*, %"class.std::ctype"** %12, align 8, !tbaa !64
  %tobool.not.i66 = icmp eq %"class.std::ctype"* %13, null
  br i1 %tobool.not.i66, label %if.then.i67, label %_ZSt13__check_facetISt5ctypeIcEERKT_PS3_.exit69

if.then.i67:                                      ; preds = %for.cond.cleanup
  tail call void @_ZSt16__throw_bad_castv() #17
  unreachable

_ZSt13__check_facetISt5ctypeIcEERKT_PS3_.exit69:  ; preds = %for.cond.cleanup
  %_M_widen_ok.i53 = getelementptr inbounds %"class.std::ctype", %"class.std::ctype"* %13, i64 0, i32 8
  %14 = load i8, i8* %_M_widen_ok.i53, align 8, !tbaa !67
  %tobool.not.i54 = icmp eq i8 %14, 0
  br i1 %tobool.not.i54, label %if.end.i60, label %if.then.i56

if.then.i56:                                      ; preds = %_ZSt13__check_facetISt5ctypeIcEERKT_PS3_.exit69
  %arrayidx.i55 = getelementptr inbounds %"class.std::ctype", %"class.std::ctype"* %13, i64 0, i32 9, i64 10
  %15 = load i8, i8* %arrayidx.i55, align 1, !tbaa !69
  br label %_ZNKSt5ctypeIcE5widenEc.exit62

if.end.i60:                                       ; preds = %_ZSt13__check_facetISt5ctypeIcEERKT_PS3_.exit69
  tail call void @_ZNKSt5ctypeIcE13_M_widen_initEv(%"class.std::ctype"* nonnull dereferenceable(570) %13)
  %16 = bitcast %"class.std::ctype"* %13 to i8 (%"class.std::ctype"*, i8)***
  %vtable.i57 = load i8 (%"class.std::ctype"*, i8)**, i8 (%"class.std::ctype"*, i8)*** %16, align 8, !tbaa !2
  %vfn.i58 = getelementptr inbounds i8 (%"class.std::ctype"*, i8)*, i8 (%"class.std::ctype"*, i8)** %vtable.i57, i64 6
  %17 = load i8 (%"class.std::ctype"*, i8)*, i8 (%"class.std::ctype"*, i8)** %vfn.i58, align 8
  %call.i59 = tail call signext i8 %17(%"class.std::ctype"* nonnull dereferenceable(570) %13, i8 signext 10)
  br label %_ZNKSt5ctypeIcE5widenEc.exit62

_ZNKSt5ctypeIcE5widenEc.exit62:                   ; preds = %if.then.i56, %if.end.i60
  %retval.0.i61 = phi i8 [ %15, %if.then.i56 ], [ %call.i59, %if.end.i60 ]
  %call1.i38 = tail call nonnull align 8 dereferenceable(8) %"class.std::basic_ostream"* @_ZNSo3putEc(%"class.std::basic_ostream"* nonnull dereferenceable(8) @_ZSt4cout, i8 signext %retval.0.i61)
  %call.i39 = tail call nonnull align 8 dereferenceable(8) %"class.std::basic_ostream"* @_ZNSo5flushEv(%"class.std::basic_ostream"* nonnull dereferenceable(8) %call1.i38)
  ret void

for.body:                                         ; preds = %for.body.lr.ph, %for.inc
  %indvars.iv = phi i64 [ 0, %for.body.lr.ph ], [ %indvars.iv.next, %for.inc ]
  %18 = load i32, i32* %type, align 8, !tbaa !25
  switch i32 %18, label %for.inc [
    i32 1, label %sw.bb
    i32 2, label %sw.bb7
    i32 3, label %sw.bb14
  ]

sw.bb:                                            ; preds = %for.body
  %19 = load i32*, i32** %10, align 8, !tbaa !21
  %add.ptr = getelementptr inbounds i32, i32* %19, i64 %indvars.iv
  %20 = load i32, i32* %add.ptr, align 4, !tbaa !18
  %call5 = tail call nonnull align 8 dereferenceable(8) %"class.std::basic_ostream"* @_ZNSolsEi(%"class.std::basic_ostream"* nonnull dereferenceable(8) @_ZSt4cout, i32 %20)
  br label %for.inc.sink.split

sw.bb7:                                           ; preds = %for.body
  %21 = load i64*, i64** %9, align 8, !tbaa !21
  %add.ptr11 = getelementptr inbounds i64, i64* %21, i64 %indvars.iv
  %22 = load i64, i64* %add.ptr11, align 8, !tbaa !35
  %call.i42 = tail call nonnull align 8 dereferenceable(8) %"class.std::basic_ostream"* @_ZNSo9_M_insertIlEERSoT_(%"class.std::basic_ostream"* nonnull dereferenceable(8) @_ZSt4cout, i64 %22)
  br label %for.inc.sink.split

sw.bb14:                                          ; preds = %for.body
  %23 = load double*, double** %8, align 8, !tbaa !21
  %add.ptr18 = getelementptr inbounds double, double* %23, i64 %indvars.iv
  %24 = load double, double* %add.ptr18, align 8, !tbaa !70
  %call.i45 = tail call nonnull align 8 dereferenceable(8) %"class.std::basic_ostream"* @_ZNSo9_M_insertIdEERSoT_(%"class.std::basic_ostream"* nonnull dereferenceable(8) @_ZSt4cout, double %24)
  br label %for.inc.sink.split

for.inc.sink.split:                               ; preds = %sw.bb14, %sw.bb7, %sw.bb
  %call5.sink = phi %"class.std::basic_ostream"* [ %call5, %sw.bb ], [ %call.i42, %sw.bb7 ], [ %call.i45, %sw.bb14 ]
  %call1.i41 = tail call nonnull align 8 dereferenceable(8) %"class.std::basic_ostream"* @_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l(%"class.std::basic_ostream"* nonnull align 8 dereferenceable(8) %call5.sink, i8* nonnull getelementptr inbounds ([2 x i8], [2 x i8]* @.str.7, i64 0, i64 0), i64 1)
  br label %for.inc

for.inc:                                          ; preds = %for.inc.sink.split, %for.body
  %indvars.iv.next = add nuw i64 %indvars.iv, 1
  %25 = load i64, i64* %size, align 8, !tbaa !26
  %cmp4 = icmp ugt i64 %25, %indvars.iv.next
  br i1 %cmp4, label %for.body, label %for.cond.cleanup, !llvm.loop !72
}

declare dso_local nonnull align 8 dereferenceable(8) %"class.std::basic_ostream"* @_ZNSolsEi(%"class.std::basic_ostream"* nonnull dereferenceable(8), i32) local_unnamed_addr #0

; Function Attrs: nobuiltin nounwind
declare dso_local void @_ZdaPv(i8*) local_unnamed_addr #6

; Function Attrs: nounwind uwtable
define linkonce_odr dso_local void @_ZN5TableD2Ev(%class.Table* nonnull dereferenceable(60) %this) unnamed_addr #9 comdat align 2 personality i32 (...)* @__gxx_personality_v0 {
entry:
  %0 = getelementptr inbounds %class.Table, %class.Table* %this, i64 0, i32 0
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [4 x i8*] }, { [4 x i8*] }* @_ZTV5Table, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %0, align 8, !tbaa !2
  %types = getelementptr inbounds %class.Table, %class.Table* %this, i64 0, i32 4
  %1 = load i32*, i32** %types, align 8, !tbaa !13
  %isnull = icmp eq i32* %1, null
  br i1 %isnull, label %delete.end, label %delete.notnull

delete.notnull:                                   ; preds = %entry
  %2 = bitcast i32* %1 to i8*
  tail call void @_ZdaPv(i8* %2) #15
  br label %delete.end

delete.end:                                       ; preds = %delete.notnull, %entry
  %_M_start.i.i = getelementptr inbounds %class.Table, %class.Table* %this, i64 0, i32 3, i32 0, i32 0, i32 0
  %3 = load %class.Column**, %class.Column*** %_M_start.i.i, align 8, !tbaa !32
  %tobool.not.i.i.i = icmp eq %class.Column** %3, null
  br i1 %tobool.not.i.i.i, label %_ZNSt6vectorIP6ColumnSaIS1_EED2Ev.exit, label %if.then.i.i.i

if.then.i.i.i:                                    ; preds = %delete.end
  %4 = bitcast %class.Column** %3 to i8*
  tail call void @_ZdlPv(i8* nonnull %4) #14
  br label %_ZNSt6vectorIP6ColumnSaIS1_EED2Ev.exit

_ZNSt6vectorIP6ColumnSaIS1_EED2Ev.exit:           ; preds = %delete.end, %if.then.i.i.i
  ret void
}

; Function Attrs: nounwind uwtable
define linkonce_odr dso_local void @_ZN5TableD0Ev(%class.Table* nonnull dereferenceable(60) %this) unnamed_addr #9 comdat align 2 personality i32 (...)* @__gxx_personality_v0 {
entry:
  %0 = getelementptr inbounds %class.Table, %class.Table* %this, i64 0, i32 0
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [4 x i8*] }, { [4 x i8*] }* @_ZTV5Table, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %0, align 8, !tbaa !2
  %types.i = getelementptr inbounds %class.Table, %class.Table* %this, i64 0, i32 4
  %1 = load i32*, i32** %types.i, align 8, !tbaa !13
  %isnull.i = icmp eq i32* %1, null
  br i1 %isnull.i, label %delete.end.i, label %delete.notnull.i

delete.notnull.i:                                 ; preds = %entry
  %2 = bitcast i32* %1 to i8*
  tail call void @_ZdaPv(i8* %2) #15
  br label %delete.end.i

delete.end.i:                                     ; preds = %delete.notnull.i, %entry
  %_M_start.i.i.i = getelementptr inbounds %class.Table, %class.Table* %this, i64 0, i32 3, i32 0, i32 0, i32 0
  %3 = load %class.Column**, %class.Column*** %_M_start.i.i.i, align 8, !tbaa !32
  %tobool.not.i.i.i.i = icmp eq %class.Column** %3, null
  br i1 %tobool.not.i.i.i.i, label %_ZN5TableD2Ev.exit, label %if.then.i.i.i.i

if.then.i.i.i.i:                                  ; preds = %delete.end.i
  %4 = bitcast %class.Column** %3 to i8*
  tail call void @_ZdlPv(i8* nonnull %4) #14
  br label %_ZN5TableD2Ev.exit

_ZN5TableD2Ev.exit:                               ; preds = %delete.end.i, %if.then.i.i.i.i
  %5 = bitcast %class.Table* %this to i8*
  tail call void @_ZdlPv(i8* nonnull %5) #15
  ret void
}

; Function Attrs: nounwind uwtable willreturn
define linkonce_odr dso_local void @_ZN6ColumnD2Ev(%class.Column* nonnull dereferenceable(40) %this) unnamed_addr #10 comdat align 2 {
entry:
  ret void
}

; Function Attrs: nounwind uwtable
define linkonce_odr dso_local void @_ZN6ColumnD0Ev(%class.Column* nonnull dereferenceable(40) %this) unnamed_addr #9 comdat align 2 {
entry:
  %0 = bitcast %class.Column* %this to i8*
  tail call void @_ZdlPv(i8* nonnull %0) #15
  ret void
}

; Function Attrs: noreturn
declare dso_local void @_ZSt17__throw_bad_allocv() local_unnamed_addr #11

; Function Attrs: argmemonly nofree nosync nounwind willreturn
declare void @llvm.memmove.p0i8.p0i8.i64(i8* nocapture writeonly, i8* nocapture readonly, i64, i1 immarg) #7

; Function Attrs: argmemonly nofree nosync nounwind willreturn writeonly
declare void @llvm.memset.p0i8.i64(i8* nocapture writeonly, i8, i64, i1 immarg) #12

declare dso_local nonnull align 8 dereferenceable(8) %"class.std::basic_ostream"* @_ZNSo3putEc(%"class.std::basic_ostream"* nonnull dereferenceable(8), i8 signext) local_unnamed_addr #0

declare dso_local nonnull align 8 dereferenceable(8) %"class.std::basic_ostream"* @_ZNSo5flushEv(%"class.std::basic_ostream"* nonnull dereferenceable(8)) local_unnamed_addr #0

; Function Attrs: noreturn
declare dso_local void @_ZSt16__throw_bad_castv() local_unnamed_addr #11

declare dso_local void @_ZNKSt5ctypeIcE13_M_widen_initEv(%"class.std::ctype"* nonnull dereferenceable(570)) local_unnamed_addr #0

declare dso_local nonnull align 8 dereferenceable(8) %"class.std::basic_ostream"* @_ZNSo9_M_insertIdEERSoT_(%"class.std::basic_ostream"* nonnull dereferenceable(8), double) local_unnamed_addr #0

declare dso_local nonnull align 8 dereferenceable(8) %"class.std::basic_ostream"* @_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l(%"class.std::basic_ostream"* nonnull align 8 dereferenceable(8), i8*, i64) local_unnamed_addr #0

declare dso_local nonnull align 8 dereferenceable(8) %"class.std::basic_ostream"* @_ZNSo9_M_insertIlEERSoT_(%"class.std::basic_ostream"* nonnull dereferenceable(8), i64) local_unnamed_addr #0

; Function Attrs: uwtable
define internal void @_GLOBAL__sub_I_test.cpp() #3 section ".text.startup" {
entry:
  tail call void @_ZNSt8ios_base4InitC1Ev(%"class.std::ios_base::Init"* nonnull dereferenceable(1) @_ZStL8__ioinit)
  %0 = tail call i32 @__cxa_atexit(void (i8*)* bitcast (void (%"class.std::ios_base::Init"*)* @_ZNSt8ios_base4InitD1Ev to void (i8*)*), i8* getelementptr inbounds (%"class.std::ios_base::Init", %"class.std::ios_base::Init"* @_ZStL8__ioinit, i64 0, i32 0), i8* nonnull @__dso_handle) #14
  ret void
}

attributes #0 = { "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "no-infs-fp-math"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #1 = { nounwind "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "no-infs-fp-math"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #2 = { nofree nounwind }
attributes #3 = { uwtable "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "min-legal-vector-width"="0" "no-infs-fp-math"="false" "no-jump-tables"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #4 = { nofree nosync nounwind readnone speculatable willreturn }
attributes #5 = { nobuiltin nofree allocsize(0) "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "no-infs-fp-math"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #6 = { nobuiltin nounwind "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "no-infs-fp-math"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #7 = { argmemonly nofree nosync nounwind willreturn }
attributes #8 = { uwtable mustprogress "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "min-legal-vector-width"="0" "no-infs-fp-math"="false" "no-jump-tables"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #9 = { nounwind uwtable "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "min-legal-vector-width"="0" "no-infs-fp-math"="false" "no-jump-tables"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #10 = { nounwind uwtable willreturn "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "min-legal-vector-width"="0" "no-infs-fp-math"="false" "no-jump-tables"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #11 = { noreturn "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "no-infs-fp-math"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #12 = { argmemonly nofree nosync nounwind willreturn writeonly }
attributes #13 = { builtin allocsize(0) }
attributes #14 = { nounwind }
attributes #15 = { builtin nounwind }
attributes #16 = { allocsize(0) }
attributes #17 = { noreturn }

!llvm.module.flags = !{!0}
!llvm.ident = !{!1}

!0 = !{i32 1, !"wchar_size", i32 4}
!1 = !{!"Ubuntu clang version 12.0.0-++20210225092633+e0e6b1e39e7e-1~exp1~20210225083352.50"}
!2 = !{!3, !3, i64 0}
!3 = !{!"vtable pointer", !4, i64 0}
!4 = !{!"Simple C++ TBAA"}
!5 = !{!6, !11, i64 48}
!6 = !{!"_ZTS5Table", !7, i64 8, !8, i64 16, !9, i64 40, !11, i64 48, !11, i64 52, !11, i64 56}
!7 = !{!"_ZTS6Layout"}
!8 = !{!"_ZTSSt6vectorIP6ColumnSaIS1_EE"}
!9 = !{!"any pointer", !10, i64 0}
!10 = !{!"omnipotent char", !4, i64 0}
!11 = !{!"int", !10, i64 0}
!12 = !{!6, !11, i64 52}
!13 = !{!6, !9, i64 40}
!14 = !{!6, !11, i64 56}
!15 = !{!9, !9, i64 0}
!16 = distinct !{!16, !17}
!17 = !{!"llvm.loop.mustprogress"}
!18 = !{!11, !11, i64 0}
!19 = distinct !{!19, !17, !20}
!20 = !{!"llvm.loop.isvectorized", i32 1}
!21 = !{!22, !9, i64 8}
!22 = !{!"_ZTS6Column", !9, i64 8, !9, i64 16, !23, i64 24, !24, i64 32}
!23 = !{!"_ZTSN3opt10ColumnTypeE", !10, i64 0}
!24 = !{!"long", !10, i64 0}
!25 = !{!22, !23, i64 24}
!26 = !{!22, !24, i64 32}
!27 = !{!23, !23, i64 0}
!28 = !{!29, !9, i64 16}
!29 = !{!"_ZTSSt12_Vector_baseIP6ColumnSaIS1_EE", !30, i64 0}
!30 = !{!"_ZTSNSt12_Vector_baseIP6ColumnSaIS1_EE12_Vector_implE", !9, i64 0, !9, i64 8, !9, i64 16}
!31 = !{!29, !9, i64 8}
!32 = !{!29, !9, i64 0}
!33 = distinct !{!33, !17, !34, !20}
!34 = !{!"llvm.loop.unroll.runtime.disable"}
!35 = !{!24, !24, i64 0}
!36 = distinct !{!36, !17}
!37 = distinct !{!37, !17}
!38 = !{!39, !40, i64 8}
!39 = !{!"_ZTS10Aggregator", !40, i64 8, !11, i64 12, !41, i64 16}
!40 = !{!"_ZTS13AggregateType", !10, i64 0}
!41 = !{!"_ZTSSt13unordered_mapImSt6vectorI13GroupByColumnSaIS1_EESt4hashImESt8equal_toImESaISt4pairIKmS3_EEE", !42, i64 0}
!42 = !{!"_ZTSSt10_HashtableImSt4pairIKmSt6vectorI13GroupByColumnSaIS3_EEESaIS6_ENSt8__detail10_Select1stESt8equal_toImESt4hashImENS8_18_Mod_range_hashingENS8_20_Default_ranged_hashENS8_20_Prime_rehash_policyENS8_17_Hashtable_traitsILb0ELb0ELb1EEEE", !9, i64 0, !24, i64 8, !43, i64 16, !24, i64 24, !44, i64 32, !9, i64 48}
!43 = !{!"_ZTSNSt8__detail15_Hash_node_baseE", !9, i64 0}
!44 = !{!"_ZTSNSt8__detail20_Prime_rehash_policyE", !45, i64 0, !24, i64 8}
!45 = !{!"float", !10, i64 0}
!46 = !{!39, !11, i64 12}
!47 = !{!42, !9, i64 0}
!48 = !{!42, !24, i64 8}
!49 = !{!44, !45, i64 0}
!50 = !{!51, !9, i64 0}
!51 = !{!"_ZTSSt12_Vector_baseI11ColumnIndexSaIS0_EE", !52, i64 0}
!52 = !{!"_ZTSNSt12_Vector_baseI11ColumnIndexSaIS0_EE12_Vector_implE", !9, i64 0, !9, i64 8, !9, i64 16}
!53 = !{!51, !9, i64 16}
!54 = !{!51, !9, i64 8}
!55 = !{!56, !9, i64 0}
!56 = !{!"_ZTSSt12_Vector_baseIP10AggregatorSaIS1_EE", !57, i64 0}
!57 = !{!"_ZTSNSt12_Vector_baseIP10AggregatorSaIS1_EE12_Vector_implE", !9, i64 0, !9, i64 8, !9, i64 16}
!58 = !{!56, !9, i64 16}
!59 = !{!56, !9, i64 8}
!60 = distinct !{!60, !17}
!61 = distinct !{!61, !17}
!62 = distinct !{!62, !17}
!63 = distinct !{!63, !17}
!64 = !{!65, !9, i64 240}
!65 = !{!"_ZTSSt9basic_iosIcSt11char_traitsIcEE", !9, i64 216, !10, i64 224, !66, i64 225, !9, i64 232, !9, i64 240, !9, i64 248, !9, i64 256}
!66 = !{!"bool", !10, i64 0}
!67 = !{!68, !10, i64 56}
!68 = !{!"_ZTSSt5ctypeIcE", !9, i64 16, !66, i64 24, !9, i64 32, !9, i64 40, !9, i64 48, !10, i64 56, !10, i64 57, !10, i64 313, !10, i64 569}
!69 = !{!10, !10, i64 0}
!70 = !{!71, !71, i64 0}
!71 = !{!"double", !10, i64 0}
!72 = distinct !{!72, !17}
