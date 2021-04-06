; ModuleID = '/usr/code/olk_dev/omni_runtime_joy/omni-cache/joy/cpp/src/operator/ir/../sort.cpp'
source_filename = "/usr/code/olk_dev/omni_runtime_joy/omni-cache/joy/cpp/src/operator/ir/../sort.cpp"
target datalayout = "e-m:e-p270:32:32-p271:32:32-p272:64:64-i64:64-f80:128-n8:16:32:64-S128"
target triple = "x86_64-pc-linux-gnu"

%"class.std::ios_base::Init" = type { i8 }
%class.Sort = type { %class.OpTemplate, i32*, i32, i32*, i32, i32*, i32*, i32*, i32, %class.PagesIndex* }
%class.OpTemplate = type { i32 (...)** }
%class.PagesIndex = type <{ i32*, i32, [4 x i8], %"class.std::vector.0", %"class.std::vector.5", i32, [4 x i8] }>
%"class.std::vector.0" = type { %"struct.std::_Vector_base.1" }
%"struct.std::_Vector_base.1" = type { %"struct.std::_Vector_base<long, std::allocator<long>>::_Vector_impl" }
%"struct.std::_Vector_base<long, std::allocator<long>>::_Vector_impl" = type { i64*, i64*, i64* }
%"class.std::vector.5" = type { %"struct.std::_Vector_base.6" }
%"struct.std::_Vector_base.6" = type { %"struct.std::_Vector_base<std::vector<Column *>, std::allocator<std::vector<Column *>>>::_Vector_impl" }
%"struct.std::_Vector_base<std::vector<Column *>, std::allocator<std::vector<Column *>>>::_Vector_impl" = type { %"class.std::vector"*, %"class.std::vector"*, %"class.std::vector"* }
%"class.std::vector" = type { %"struct.std::_Vector_base" }
%"struct.std::_Vector_base" = type { %"struct.std::_Vector_base<Column *, std::allocator<Column *>>::_Vector_impl" }
%"struct.std::_Vector_base<Column *, std::allocator<Column *>>::_Vector_impl" = type { %class.Column**, %class.Column**, %class.Column** }
%class.Column = type { i32 (...)**, i8*, i32*, i32, i64 }
%class.Table = type <{ i32 (...)**, %class.Layout, [7 x i8], %"class.std::vector", i32*, i32, i32, i32, [4 x i8] }>
%class.Layout = type { i8 }
%class.MemoryPool = type { i32 (...)** }

$_ZN4Sort9getResultEv = comdat any

$_ZN6ColumnD2Ev = comdat any

$_ZN6ColumnD0Ev = comdat any

$__clang_call_terminate = comdat any

$_ZNSt6vectorIS_IP6ColumnSaIS1_EESaIS3_EE17_M_realloc_insertIJRKS3_EEEvN9__gnu_cxx17__normal_iteratorIPS3_S5_EEDpOT_ = comdat any

$_ZTS10OpTemplate = comdat any

$_ZTI10OpTemplate = comdat any

$_ZTV6Column = comdat any

$_ZTS6Column = comdat any

$_ZTI6Column = comdat any

@_ZStL8__ioinit = internal global %"class.std::ios_base::Init" zeroinitializer, align 1
@__dso_handle = external hidden global i8
@_ZTV4Sort = dso_local unnamed_addr constant { [9 x i8*] } { [9 x i8*] [i8* null, i8* bitcast ({ i8*, i8*, i8* }* @_ZTI4Sort to i8*), i8* bitcast (void (%class.Sort*, %class.Table*)* @_ZN4Sort7preloopEP5Table to i8*), i8* bitcast (void (%class.Sort*, %class.Table*, i32)* @_ZN4Sort6inloopEP5Tablej to i8*), i8* bitcast (void (%class.Sort*, %class.Table*)* @_ZN4Sort8postloopEP5Table to i8*), i8* bitcast (void (%class.Sort*, %class.Table*, i32)* @_ZN4Sort7processEP5Tablej to i8*), i8* bitcast (%class.Table* (%class.Sort*)* @_ZN4Sort9getResultEv to i8*), i8* bitcast (void (%class.Sort*)* @_ZN4SortD2Ev to i8*), i8* bitcast (void (%class.Sort*)* @_ZN4SortD0Ev to i8*)] }, align 8
@_ZTVN10__cxxabiv120__si_class_type_infoE = external dso_local global i8*
@_ZTS4Sort = dso_local constant [6 x i8] c"4Sort\00", align 1
@_ZTVN10__cxxabiv117__class_type_infoE = external dso_local global i8*
@_ZTS10OpTemplate = linkonce_odr dso_local constant [13 x i8] c"10OpTemplate\00", comdat, align 1
@_ZTI10OpTemplate = linkonce_odr dso_local constant { i8*, i8* } { i8* bitcast (i8** getelementptr inbounds (i8*, i8** @_ZTVN10__cxxabiv117__class_type_infoE, i64 2) to i8*), i8* getelementptr inbounds ([13 x i8], [13 x i8]* @_ZTS10OpTemplate, i32 0, i32 0) }, comdat, align 8
@_ZTI4Sort = dso_local constant { i8*, i8*, i8* } { i8* bitcast (i8** getelementptr inbounds (i8*, i8** @_ZTVN10__cxxabiv120__si_class_type_infoE, i64 2) to i8*), i8* getelementptr inbounds ([6 x i8], [6 x i8]* @_ZTS4Sort, i32 0, i32 0), i8* bitcast ({ i8*, i8* }* @_ZTI10OpTemplate to i8*) }, align 8
@_ZTV6Column = linkonce_odr dso_local unnamed_addr constant { [4 x i8*] } { [4 x i8*] [i8* null, i8* bitcast ({ i8*, i8* }* @_ZTI6Column to i8*), i8* bitcast (void (%class.Column*)* @_ZN6ColumnD2Ev to i8*), i8* bitcast (void (%class.Column*)* @_ZN6ColumnD0Ev to i8*)] }, comdat, align 8
@_ZTS6Column = linkonce_odr dso_local constant [8 x i8] c"6Column\00", comdat, align 1
@_ZTI6Column = linkonce_odr dso_local constant { i8*, i8* } { i8* bitcast (i8** getelementptr inbounds (i8*, i8** @_ZTVN10__cxxabiv117__class_type_infoE, i64 2) to i8*), i8* getelementptr inbounds ([8 x i8], [8 x i8]* @_ZTS6Column, i32 0, i32 0) }, comdat, align 8
@llvm.global_ctors = appending global [1 x { i32, void ()*, i8* }] [{ i32, void ()*, i8* } { i32 65535, void ()* @_GLOBAL__sub_I_sort.cpp, i8* null }]

@_ZN4SortC1EPiiS0_iS0_S0_S0_i = dso_local unnamed_addr alias void (%class.Sort*, i32*, i32, i32*, i32, i32*, i32*, i32*, i32), void (%class.Sort*, i32*, i32, i32*, i32, i32*, i32*, i32*, i32)* @_ZN4SortC2EPiiS0_iS0_S0_S0_i
@_ZN4SortD1Ev = dso_local unnamed_addr alias void (%class.Sort*), void (%class.Sort*)* @_ZN4SortD2Ev
@_ZN10PagesIndexC1EPii = dso_local unnamed_addr alias void (%class.PagesIndex*, i32*, i32), void (%class.PagesIndex*, i32*, i32)* @_ZN10PagesIndexC2EPii

declare dso_local void @_ZNSt8ios_base4InitC1Ev(%"class.std::ios_base::Init"* nonnull dereferenceable(1)) unnamed_addr #0

; Function Attrs: nounwind
declare dso_local void @_ZNSt8ios_base4InitD1Ev(%"class.std::ios_base::Init"* nonnull dereferenceable(1)) unnamed_addr #1

; Function Attrs: nofree nounwind
declare dso_local i32 @__cxa_atexit(void (i8*)*, i8*, i8*) local_unnamed_addr #2

; Function Attrs: norecurse nounwind readnone uwtable willreturn mustprogress
define dso_local i64 @_Z22encodeSyntheticAddressjj(i32 %sliceIndex, i32 %sliceOffset) local_unnamed_addr #3 {
entry:
  %conv = zext i32 %sliceIndex to i64
  %shl = shl nuw i64 %conv, 32
  %conv1 = zext i32 %sliceOffset to i64
  %or = or i64 %shl, %conv1
  ret i64 %or
}

; Function Attrs: norecurse nounwind readnone uwtable willreturn mustprogress
define dso_local i32 @_Z16decodeSliceIndexm(i64 %sliceAddress) local_unnamed_addr #3 {
entry:
  %shr = lshr i64 %sliceAddress, 32
  %conv = trunc i64 %shr to i32
  ret i32 %conv
}

; Function Attrs: norecurse nounwind readnone uwtable willreturn mustprogress
define dso_local i32 @_Z14decodePositionm(i64 %sliceAddress) local_unnamed_addr #3 {
entry:
  %conv = trunc i64 %sliceAddress to i32
  ret i32 %conv
}

; Function Attrs: norecurse nounwind readnone uwtable willreturn mustprogress
define dso_local i32 @_Z13getColumnTypei(i32 %colTypeIdx) local_unnamed_addr #3 {
entry:
  %switch.selectcmp = icmp eq i32 %colTypeIdx, 2
  %switch.select = select i1 %switch.selectcmp, i32 2, i32 1
  %switch.selectcmp9 = icmp eq i32 %colTypeIdx, 3
  %switch.select10 = select i1 %switch.selectcmp9, i32 3, i32 %switch.select
  ret i32 %switch.select10
}

; Function Attrs: norecurse nounwind readnone uwtable willreturn mustprogress
define dso_local i32 @_Z13getColTypeIdxN3opt10ColumnTypeE(i32 %type) local_unnamed_addr #3 {
entry:
  %switch.selectcmp = icmp eq i32 %type, 2
  %switch.select = select i1 %switch.selectcmp, i32 2, i32 1
  %switch.selectcmp9 = icmp eq i32 %type, 3
  %switch.select10 = select i1 %switch.selectcmp9, i32 3, i32 %switch.select
  ret i32 %switch.select10
}

; Function Attrs: uwtable
define dso_local void @_Z12allocColumnslPiS_ij(i64 %outputTableAddr, i32* nocapture readonly %sourceTypes, i32* nocapture readonly %outputCols, i32 %outputColCount, i32 %positionCount) local_unnamed_addr #4 personality i8* bitcast (i32 (...)* @__gxx_personality_v0 to i8*) {
entry:
  %data = alloca i8*, align 8
  %0 = inttoptr i64 %outputTableAddr to %class.Table*
  %call = tail call %class.MemoryPool* @_Z13getMemoryPoolv()
  %1 = bitcast i8** %data to i8*
  call void @llvm.lifetime.start.p0i8(i64 8, i8* nonnull %1) #18
  store i8* null, i8** %data, align 8, !tbaa !2
  %cmp36 = icmp sgt i32 %outputColCount, 0
  br i1 %cmp36, label %for.body.lr.ph, label %for.cond.cleanup

for.body.lr.ph:                                   ; preds = %entry
  %conv11 = zext i32 %positionCount to i64
  %mul12 = shl nuw nsw i64 %conv11, 3
  %2 = bitcast %class.MemoryPool* %call to i32 (%class.MemoryPool*, i64, i8**)***
  %mul = shl nuw nsw i64 %conv11, 2
  %types.i = getelementptr inbounds %class.Table, %class.Table* %0, i64 0, i32 4
  %columnSize.i = getelementptr inbounds %class.Table, %class.Table* %0, i64 0, i32 7
  %_M_finish.i.i = getelementptr inbounds %class.Table, %class.Table* %0, i64 0, i32 3, i32 0, i32 0, i32 1
  %_M_end_of_storage.i.i = getelementptr inbounds %class.Table, %class.Table* %0, i64 0, i32 3, i32 0, i32 0, i32 2
  %_M_start.i27.i.i.i.i = getelementptr inbounds %class.Table, %class.Table* %0, i64 0, i32 3, i32 0, i32 0, i32 0
  %wide.trip.count = zext i32 %outputColCount to i64
  br label %for.body

for.cond.cleanup:                                 ; preds = %_ZN5Table9setColumnEP6ColumnN3opt10ColumnTypeE.exit, %entry
  call void @llvm.lifetime.end.p0i8(i64 8, i8* nonnull %1) #18
  ret void

for.body:                                         ; preds = %for.body.lr.ph, %_ZN5Table9setColumnEP6ColumnN3opt10ColumnTypeE.exit
  %indvars.iv = phi i64 [ 0, %for.body.lr.ph ], [ %indvars.iv.next, %_ZN5Table9setColumnEP6ColumnN3opt10ColumnTypeE.exit ]
  %arrayidx = getelementptr inbounds i32, i32* %outputCols, i64 %indvars.iv
  %3 = load i32, i32* %arrayidx, align 4, !tbaa !6
  %idxprom1 = sext i32 %3 to i64
  %arrayidx2 = getelementptr inbounds i32, i32* %sourceTypes, i64 %idxprom1
  %4 = load i32, i32* %arrayidx2, align 4, !tbaa !6
  switch i32 %4, label %sw.epilog [
    i32 1, label %sw.bb
    i32 2, label %sw.bb4
    i32 3, label %sw.bb10
  ]

sw.bb:                                            ; preds = %for.body
  %vtable = load i32 (%class.MemoryPool*, i64, i8**)**, i32 (%class.MemoryPool*, i64, i8**)*** %2, align 8, !tbaa !8
  %5 = load i32 (%class.MemoryPool*, i64, i8**)*, i32 (%class.MemoryPool*, i64, i8**)** %vtable, align 8
  %call3 = call i32 %5(%class.MemoryPool* nonnull dereferenceable(8) %call, i64 %mul, i8** nonnull %data)
  br label %sw.epilog

sw.bb4:                                           ; preds = %for.body
  %vtable7 = load i32 (%class.MemoryPool*, i64, i8**)**, i32 (%class.MemoryPool*, i64, i8**)*** %2, align 8, !tbaa !8
  %6 = load i32 (%class.MemoryPool*, i64, i8**)*, i32 (%class.MemoryPool*, i64, i8**)** %vtable7, align 8
  %call9 = call i32 %6(%class.MemoryPool* nonnull dereferenceable(8) %call, i64 %mul12, i8** nonnull %data)
  br label %sw.epilog

sw.bb10:                                          ; preds = %for.body
  %vtable13 = load i32 (%class.MemoryPool*, i64, i8**)**, i32 (%class.MemoryPool*, i64, i8**)*** %2, align 8, !tbaa !8
  %7 = load i32 (%class.MemoryPool*, i64, i8**)*, i32 (%class.MemoryPool*, i64, i8**)** %vtable13, align 8
  %call15 = call i32 %7(%class.MemoryPool* nonnull dereferenceable(8) %call, i64 %mul12, i8** nonnull %data)
  br label %sw.epilog

sw.epilog:                                        ; preds = %for.body, %sw.bb10, %sw.bb4, %sw.bb
  %switch.selectcmp.i = icmp eq i32 %4, 2
  %switch.select.i = select i1 %switch.selectcmp.i, i32 2, i32 1
  %switch.selectcmp9.i = icmp eq i32 %4, 3
  %switch.select10.i = select i1 %switch.selectcmp9.i, i32 3, i32 %switch.select.i
  %call17 = call noalias nonnull dereferenceable(40) i8* @_Znwm(i64 40) #19
  %8 = load i8*, i8** %data, align 8, !tbaa !2
  %9 = bitcast i8* %call17 to i32 (...)***
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [4 x i8*] }, { [4 x i8*] }* @_ZTV6Column, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %9, align 8, !tbaa !8
  %data.i = getelementptr inbounds i8, i8* %call17, i64 8
  %10 = bitcast i8* %data.i to i8**
  store i8* %8, i8** %10, align 8, !tbaa !10
  %type.i = getelementptr inbounds i8, i8* %call17, i64 24
  %11 = bitcast i8* %type.i to i32*
  store i32 %switch.select10.i, i32* %11, align 8, !tbaa !14
  %size.i = getelementptr inbounds i8, i8* %call17, i64 32
  %12 = bitcast i8* %size.i to i64*
  store i64 %conv11, i64* %12, align 8, !tbaa !15
  %13 = load i32*, i32** %types.i, align 8, !tbaa !16
  %14 = load i32, i32* %columnSize.i, align 8, !tbaa !20
  %idxprom.i = zext i32 %14 to i64
  %arrayidx.i = getelementptr inbounds i32, i32* %13, i64 %idxprom.i
  store i32 %switch.select10.i, i32* %arrayidx.i, align 4, !tbaa !21
  %inc.i = add i32 %14, 1
  store i32 %inc.i, i32* %columnSize.i, align 8, !tbaa !20
  %15 = load %class.Column**, %class.Column*** %_M_finish.i.i, align 8, !tbaa !22
  %16 = load %class.Column**, %class.Column*** %_M_end_of_storage.i.i, align 8, !tbaa !25
  %cmp.not.i.i = icmp eq %class.Column** %15, %16
  br i1 %cmp.not.i.i, label %if.else.i.i, label %if.then.i.i

if.then.i.i:                                      ; preds = %sw.epilog
  %17 = bitcast %class.Column** %15 to i8**
  store i8* %call17, i8** %17, align 8, !tbaa !2
  %18 = load %class.Column**, %class.Column*** %_M_finish.i.i, align 8, !tbaa !22
  %incdec.ptr.i.i = getelementptr inbounds %class.Column*, %class.Column** %18, i64 1
  br label %_ZN5Table9setColumnEP6ColumnN3opt10ColumnTypeE.exit

if.else.i.i:                                      ; preds = %sw.epilog
  %19 = load %class.Column**, %class.Column*** %_M_start.i27.i.i.i.i, align 8, !tbaa !26
  %sub.ptr.lhs.cast.i28.i.i.i.i = ptrtoint %class.Column** %15 to i64
  %sub.ptr.rhs.cast.i29.i.i.i.i = ptrtoint %class.Column** %19 to i64
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
  %call2.i.i.i.i.i.i = call noalias nonnull i8* @_Znwm(i64 %mul.i.i.i.i.i.i) #20
  %20 = bitcast i8* %call2.i.i.i.i.i.i to %class.Column**
  %.pre.i.i.i = load %class.Column**, %class.Column*** %_M_start.i27.i.i.i.i, align 8, !tbaa !26
  %.pre83.i.i.i = ptrtoint %class.Column** %.pre.i.i.i to i64
  %.pre84.i.i.i = sub i64 %sub.ptr.lhs.cast.i28.i.i.i.i, %.pre83.i.i.i
  br label %invoke.cont.i.i.i

invoke.cont.i.i.i:                                ; preds = %_ZNSt16allocator_traitsISaIP6ColumnEE8allocateERS2_m.exit.i.i.i.i, %if.else.i.i
  %sub.ptr.sub.i.i.i.i.i.i.i.i74.pre-phi.i.i.i = phi i64 [ %.pre84.i.i.i, %_ZNSt16allocator_traitsISaIP6ColumnEE8allocateERS2_m.exit.i.i.i.i ], [ %sub.ptr.sub.i30.i.i.i.i, %if.else.i.i ]
  %21 = phi %class.Column** [ %.pre.i.i.i, %_ZNSt16allocator_traitsISaIP6ColumnEE8allocateERS2_m.exit.i.i.i.i ], [ %19, %if.else.i.i ]
  %cond.i67.i.i.i = phi %class.Column** [ %20, %_ZNSt16allocator_traitsISaIP6ColumnEE8allocateERS2_m.exit.i.i.i.i ], [ null, %if.else.i.i ]
  %add.ptr.i.i.i = getelementptr inbounds %class.Column*, %class.Column** %cond.i67.i.i.i, i64 %sub.ptr.div.i31.i.i.i.i
  %22 = bitcast %class.Column** %add.ptr.i.i.i to i8**
  store i8* %call17, i8** %22, align 8, !tbaa !2
  %tobool.not.i.i.i.i.i.i.i.i75.i.i.i = icmp eq i64 %sub.ptr.sub.i.i.i.i.i.i.i.i74.pre-phi.i.i.i, 0
  br i1 %tobool.not.i.i.i.i.i.i.i.i75.i.i.i, label %invoke.cont10.i.i.i, label %if.then.i.i.i.i.i.i.i.i76.i.i.i

if.then.i.i.i.i.i.i.i.i76.i.i.i:                  ; preds = %invoke.cont.i.i.i
  %23 = bitcast %class.Column** %cond.i67.i.i.i to i8*
  %24 = bitcast %class.Column** %21 to i8*
  call void @llvm.memmove.p0i8.p0i8.i64(i8* align 8 %23, i8* align 8 %24, i64 %sub.ptr.sub.i.i.i.i.i.i.i.i74.pre-phi.i.i.i, i1 false) #18
  br label %invoke.cont10.i.i.i

invoke.cont10.i.i.i:                              ; preds = %if.then.i.i.i.i.i.i.i.i76.i.i.i, %invoke.cont.i.i.i
  %sub.ptr.div.i.i.i.i.i.i.i.i77.i.i.i = ashr exact i64 %sub.ptr.sub.i.i.i.i.i.i.i.i74.pre-phi.i.i.i, 3
  %add.ptr.i.i.i.i.i.i.i.i78.i.i.i = getelementptr inbounds %class.Column*, %class.Column** %cond.i67.i.i.i, i64 1
  %incdec.ptr.i.i.i = getelementptr inbounds %class.Column*, %class.Column** %add.ptr.i.i.i.i.i.i.i.i78.i.i.i, i64 %sub.ptr.div.i.i.i.i.i.i.i.i77.i.i.i
  %25 = load %class.Column**, %class.Column*** %_M_finish.i.i, align 8, !tbaa !22
  %sub.ptr.lhs.cast.i.i.i.i.i.i.i.i.i.i.i = ptrtoint %class.Column** %25 to i64
  %sub.ptr.sub.i.i.i.i.i.i.i.i.i.i.i = sub i64 %sub.ptr.lhs.cast.i.i.i.i.i.i.i.i.i.i.i, %sub.ptr.lhs.cast.i28.i.i.i.i
  %tobool.not.i.i.i.i.i.i.i.i.i.i.i = icmp eq i64 %sub.ptr.sub.i.i.i.i.i.i.i.i.i.i.i, 0
  br i1 %tobool.not.i.i.i.i.i.i.i.i.i.i.i, label %invoke.cont15.i.i.i, label %if.then.i.i.i.i.i.i.i.i.i.i.i

if.then.i.i.i.i.i.i.i.i.i.i.i:                    ; preds = %invoke.cont10.i.i.i
  %26 = bitcast %class.Column** %incdec.ptr.i.i.i to i8*
  %27 = bitcast %class.Column** %15 to i8*
  call void @llvm.memmove.p0i8.p0i8.i64(i8* nonnull align 8 %26, i8* align 8 %27, i64 %sub.ptr.sub.i.i.i.i.i.i.i.i.i.i.i, i1 false) #18
  br label %invoke.cont15.i.i.i

invoke.cont15.i.i.i:                              ; preds = %if.then.i.i.i.i.i.i.i.i.i.i.i, %invoke.cont10.i.i.i
  %tobool.not.i68.i.i.i = icmp eq %class.Column** %21, null
  br i1 %tobool.not.i68.i.i.i, label %_ZNSt6vectorIP6ColumnSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i.i, label %if.then.i69.i.i.i

if.then.i69.i.i.i:                                ; preds = %invoke.cont15.i.i.i
  %28 = bitcast %class.Column** %21 to i8*
  call void @_ZdlPv(i8* nonnull %28) #18
  br label %_ZNSt6vectorIP6ColumnSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i.i

_ZNSt6vectorIP6ColumnSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i.i: ; preds = %if.then.i69.i.i.i, %invoke.cont15.i.i.i
  %sub.ptr.div.i.i.i.i.i.i.i.i.i.i.i = ashr exact i64 %sub.ptr.sub.i.i.i.i.i.i.i.i.i.i.i, 3
  %add.ptr.i.i.i.i.i.i.i.i.i.i.i = getelementptr inbounds %class.Column*, %class.Column** %incdec.ptr.i.i.i, i64 %sub.ptr.div.i.i.i.i.i.i.i.i.i.i.i
  store %class.Column** %cond.i67.i.i.i, %class.Column*** %_M_start.i27.i.i.i.i, align 8, !tbaa !26
  store %class.Column** %add.ptr.i.i.i.i.i.i.i.i.i.i.i, %class.Column*** %_M_finish.i.i, align 8, !tbaa !22
  %add.ptr39.i.i.i = getelementptr inbounds %class.Column*, %class.Column** %cond.i67.i.i.i, i64 %cond.i.i.i.i
  br label %_ZN5Table9setColumnEP6ColumnN3opt10ColumnTypeE.exit

_ZN5Table9setColumnEP6ColumnN3opt10ColumnTypeE.exit: ; preds = %if.then.i.i, %_ZNSt6vectorIP6ColumnSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i.i
  %_M_finish.i.i.sink = phi %class.Column*** [ %_M_finish.i.i, %if.then.i.i ], [ %_M_end_of_storage.i.i, %_ZNSt6vectorIP6ColumnSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i.i ]
  %incdec.ptr.i.i.sink = phi %class.Column** [ %incdec.ptr.i.i, %if.then.i.i ], [ %add.ptr39.i.i.i, %_ZNSt6vectorIP6ColumnSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i.i ]
  store %class.Column** %incdec.ptr.i.i.sink, %class.Column*** %_M_finish.i.i.sink, align 8, !tbaa !2
  %indvars.iv.next = add nuw nsw i64 %indvars.iv, 1
  %exitcond.not = icmp eq i64 %indvars.iv.next, %wide.trip.count
  br i1 %exitcond.not, label %for.cond.cleanup, label %for.body, !llvm.loop !27
}

; Function Attrs: argmemonly nofree nosync nounwind willreturn
declare void @llvm.lifetime.start.p0i8(i64 immarg, i8* nocapture) #5

declare dso_local %class.MemoryPool* @_Z13getMemoryPoolv() local_unnamed_addr #0

; Function Attrs: nobuiltin nofree allocsize(0)
declare dso_local nonnull i8* @_Znwm(i64) local_unnamed_addr #6

declare dso_local i32 @__gxx_personality_v0(...)

; Function Attrs: nobuiltin nounwind
declare dso_local void @_ZdlPv(i8*) local_unnamed_addr #7

; Function Attrs: argmemonly nofree nosync nounwind willreturn
declare void @llvm.lifetime.end.p0i8(i64 immarg, i8* nocapture) #5

; Function Attrs: uwtable
define dso_local void @_ZN4SortC2EPiiS0_iS0_S0_S0_i(%class.Sort* nocapture nonnull dereferenceable(80) %this, i32* %sourceTypes, i32 %typesCount, i32* %outputCols, i32 %outputColsCount, i32* %sortCols, i32* %sortAscendings, i32* %sortNullFirsts, i32 %sortColCount) unnamed_addr #4 align 2 personality i8* bitcast (i32 (...)* @__gxx_personality_v0 to i8*) {
entry:
  %0 = getelementptr inbounds %class.Sort, %class.Sort* %this, i64 0, i32 0, i32 0
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [9 x i8*] }, { [9 x i8*] }* @_ZTV4Sort, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %0, align 8, !tbaa !8
  %sourceTypes2 = getelementptr inbounds %class.Sort, %class.Sort* %this, i64 0, i32 1
  store i32* %sourceTypes, i32** %sourceTypes2, align 8, !tbaa !29
  %typesCount3 = getelementptr inbounds %class.Sort, %class.Sort* %this, i64 0, i32 2
  store i32 %typesCount, i32* %typesCount3, align 8, !tbaa !31
  %outputCols4 = getelementptr inbounds %class.Sort, %class.Sort* %this, i64 0, i32 3
  store i32* %outputCols, i32** %outputCols4, align 8, !tbaa !32
  %outputColsCount5 = getelementptr inbounds %class.Sort, %class.Sort* %this, i64 0, i32 4
  store i32 %outputColsCount, i32* %outputColsCount5, align 8, !tbaa !33
  %sortCols6 = getelementptr inbounds %class.Sort, %class.Sort* %this, i64 0, i32 5
  store i32* %sortCols, i32** %sortCols6, align 8, !tbaa !34
  %sortAscendings7 = getelementptr inbounds %class.Sort, %class.Sort* %this, i64 0, i32 6
  store i32* %sortAscendings, i32** %sortAscendings7, align 8, !tbaa !35
  %sortNullFirsts8 = getelementptr inbounds %class.Sort, %class.Sort* %this, i64 0, i32 7
  store i32* %sortNullFirsts, i32** %sortNullFirsts8, align 8, !tbaa !36
  %sortColCount9 = getelementptr inbounds %class.Sort, %class.Sort* %this, i64 0, i32 8
  store i32 %sortColCount, i32* %sortColCount9, align 8, !tbaa !37
  %call = tail call noalias nonnull dereferenceable(72) i8* @_Znwm(i64 72) #19
  %1 = bitcast i8* %call to %class.PagesIndex*
  invoke void @_ZN10PagesIndexC2EPii(%class.PagesIndex* nonnull dereferenceable(68) %1, i32* %sourceTypes, i32 %typesCount)
          to label %invoke.cont11 unwind label %lpad10

invoke.cont11:                                    ; preds = %entry
  %pagesIndex = getelementptr inbounds %class.Sort, %class.Sort* %this, i64 0, i32 9
  %2 = bitcast %class.PagesIndex** %pagesIndex to i8**
  store i8* %call, i8** %2, align 8, !tbaa !38
  ret void

lpad10:                                           ; preds = %entry
  %3 = landingpad { i8*, i32 }
          cleanup
  tail call void @_ZdlPv(i8* nonnull %call) #21
  resume { i8*, i32 } %3
}

; Function Attrs: nounwind uwtable
define dso_local void @_ZN4SortD2Ev(%class.Sort* nocapture nonnull dereferenceable(80) %this) unnamed_addr #8 align 2 personality i32 (...)* @__gxx_personality_v0 {
entry:
  %0 = getelementptr inbounds %class.Sort, %class.Sort* %this, i64 0, i32 0, i32 0
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [9 x i8*] }, { [9 x i8*] }* @_ZTV4Sort, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %0, align 8, !tbaa !8
  %pagesIndex = getelementptr inbounds %class.Sort, %class.Sort* %this, i64 0, i32 9
  %1 = load %class.PagesIndex*, %class.PagesIndex** %pagesIndex, align 8, !tbaa !38
  %isnull = icmp eq %class.PagesIndex* %1, null
  br i1 %isnull, label %delete.end, label %delete.notnull

delete.notnull:                                   ; preds = %entry
  %_M_start.i.i = getelementptr inbounds %class.PagesIndex, %class.PagesIndex* %1, i64 0, i32 4, i32 0, i32 0, i32 0
  %2 = load %"class.std::vector"*, %"class.std::vector"** %_M_start.i.i, align 8, !tbaa !39
  %_M_finish.i.i = getelementptr inbounds %class.PagesIndex, %class.PagesIndex* %1, i64 0, i32 4, i32 0, i32 0, i32 1
  %3 = load %"class.std::vector"*, %"class.std::vector"** %_M_finish.i.i, align 8, !tbaa !42
  %cmp.not3.i.i.i.i.i = icmp eq %"class.std::vector"* %2, %3
  br i1 %cmp.not3.i.i.i.i.i, label %invoke.cont.i.i, label %for.body.i.i.i.i.i

for.body.i.i.i.i.i:                               ; preds = %delete.notnull, %_ZSt8_DestroyISt6vectorIP6ColumnSaIS2_EEEvPT_.exit.i.i.i.i.i
  %__first.addr.04.i.i.i.i.i = phi %"class.std::vector"* [ %incdec.ptr.i.i.i.i.i, %_ZSt8_DestroyISt6vectorIP6ColumnSaIS2_EEEvPT_.exit.i.i.i.i.i ], [ %2, %delete.notnull ]
  %_M_start.i.i.i.i.i.i.i.i = getelementptr inbounds %"class.std::vector", %"class.std::vector"* %__first.addr.04.i.i.i.i.i, i64 0, i32 0, i32 0, i32 0
  %4 = load %class.Column**, %class.Column*** %_M_start.i.i.i.i.i.i.i.i, align 8, !tbaa !26
  %tobool.not.i.i.i.i.i.i.i.i.i = icmp eq %class.Column** %4, null
  br i1 %tobool.not.i.i.i.i.i.i.i.i.i, label %_ZSt8_DestroyISt6vectorIP6ColumnSaIS2_EEEvPT_.exit.i.i.i.i.i, label %if.then.i.i.i.i.i.i.i.i.i

if.then.i.i.i.i.i.i.i.i.i:                        ; preds = %for.body.i.i.i.i.i
  %5 = bitcast %class.Column** %4 to i8*
  tail call void @_ZdlPv(i8* nonnull %5) #18
  br label %_ZSt8_DestroyISt6vectorIP6ColumnSaIS2_EEEvPT_.exit.i.i.i.i.i

_ZSt8_DestroyISt6vectorIP6ColumnSaIS2_EEEvPT_.exit.i.i.i.i.i: ; preds = %if.then.i.i.i.i.i.i.i.i.i, %for.body.i.i.i.i.i
  %incdec.ptr.i.i.i.i.i = getelementptr inbounds %"class.std::vector", %"class.std::vector"* %__first.addr.04.i.i.i.i.i, i64 1
  %cmp.not.i.i.i.i.i = icmp eq %"class.std::vector"* %incdec.ptr.i.i.i.i.i, %3
  br i1 %cmp.not.i.i.i.i.i, label %invoke.cont.loopexit.i.i, label %for.body.i.i.i.i.i, !llvm.loop !43

invoke.cont.loopexit.i.i:                         ; preds = %_ZSt8_DestroyISt6vectorIP6ColumnSaIS2_EEEvPT_.exit.i.i.i.i.i
  %.pre.i.i = load %"class.std::vector"*, %"class.std::vector"** %_M_start.i.i, align 8, !tbaa !39
  br label %invoke.cont.i.i

invoke.cont.i.i:                                  ; preds = %invoke.cont.loopexit.i.i, %delete.notnull
  %6 = phi %"class.std::vector"* [ %.pre.i.i, %invoke.cont.loopexit.i.i ], [ %2, %delete.notnull ]
  %tobool.not.i.i.i.i = icmp eq %"class.std::vector"* %6, null
  br i1 %tobool.not.i.i.i.i, label %_ZNSt6vectorIS_IP6ColumnSaIS1_EESaIS3_EED2Ev.exit.i, label %if.then.i.i.i.i

if.then.i.i.i.i:                                  ; preds = %invoke.cont.i.i
  %7 = bitcast %"class.std::vector"* %6 to i8*
  tail call void @_ZdlPv(i8* nonnull %7) #18
  br label %_ZNSt6vectorIS_IP6ColumnSaIS1_EESaIS3_EED2Ev.exit.i

_ZNSt6vectorIS_IP6ColumnSaIS1_EESaIS3_EED2Ev.exit.i: ; preds = %if.then.i.i.i.i, %invoke.cont.i.i
  %_M_start.i.i.i = getelementptr inbounds %class.PagesIndex, %class.PagesIndex* %1, i64 0, i32 3, i32 0, i32 0, i32 0
  %8 = load i64*, i64** %_M_start.i.i.i, align 8, !tbaa !44
  %tobool.not.i.i.i2.i = icmp eq i64* %8, null
  br i1 %tobool.not.i.i.i2.i, label %_ZN10PagesIndexD2Ev.exit, label %if.then.i.i.i4.i

if.then.i.i.i4.i:                                 ; preds = %_ZNSt6vectorIS_IP6ColumnSaIS1_EESaIS3_EED2Ev.exit.i
  %9 = bitcast i64* %8 to i8*
  tail call void @_ZdlPv(i8* nonnull %9) #18
  br label %_ZN10PagesIndexD2Ev.exit

_ZN10PagesIndexD2Ev.exit:                         ; preds = %_ZNSt6vectorIS_IP6ColumnSaIS1_EESaIS3_EED2Ev.exit.i, %if.then.i.i.i4.i
  %10 = bitcast %class.PagesIndex* %1 to i8*
  tail call void @_ZdlPv(i8* %10) #21
  br label %delete.end

delete.end:                                       ; preds = %_ZN10PagesIndexD2Ev.exit, %entry
  ret void
}

; Function Attrs: nounwind uwtable
define dso_local void @_ZN4SortD0Ev(%class.Sort* nonnull dereferenceable(80) %this) unnamed_addr #8 align 2 personality i32 (...)* @__gxx_personality_v0 {
entry:
  %0 = getelementptr inbounds %class.Sort, %class.Sort* %this, i64 0, i32 0, i32 0
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [9 x i8*] }, { [9 x i8*] }* @_ZTV4Sort, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %0, align 8, !tbaa !8
  %pagesIndex.i = getelementptr inbounds %class.Sort, %class.Sort* %this, i64 0, i32 9
  %1 = load %class.PagesIndex*, %class.PagesIndex** %pagesIndex.i, align 8, !tbaa !38
  %isnull.i = icmp eq %class.PagesIndex* %1, null
  br i1 %isnull.i, label %_ZN4SortD2Ev.exit, label %delete.notnull.i

delete.notnull.i:                                 ; preds = %entry
  %_M_start.i.i.i = getelementptr inbounds %class.PagesIndex, %class.PagesIndex* %1, i64 0, i32 4, i32 0, i32 0, i32 0
  %2 = load %"class.std::vector"*, %"class.std::vector"** %_M_start.i.i.i, align 8, !tbaa !39
  %_M_finish.i.i.i = getelementptr inbounds %class.PagesIndex, %class.PagesIndex* %1, i64 0, i32 4, i32 0, i32 0, i32 1
  %3 = load %"class.std::vector"*, %"class.std::vector"** %_M_finish.i.i.i, align 8, !tbaa !42
  %cmp.not3.i.i.i.i.i.i = icmp eq %"class.std::vector"* %2, %3
  br i1 %cmp.not3.i.i.i.i.i.i, label %invoke.cont.i.i.i, label %for.body.i.i.i.i.i.i

for.body.i.i.i.i.i.i:                             ; preds = %delete.notnull.i, %_ZSt8_DestroyISt6vectorIP6ColumnSaIS2_EEEvPT_.exit.i.i.i.i.i.i
  %__first.addr.04.i.i.i.i.i.i = phi %"class.std::vector"* [ %incdec.ptr.i.i.i.i.i.i, %_ZSt8_DestroyISt6vectorIP6ColumnSaIS2_EEEvPT_.exit.i.i.i.i.i.i ], [ %2, %delete.notnull.i ]
  %_M_start.i.i.i.i.i.i.i.i.i = getelementptr inbounds %"class.std::vector", %"class.std::vector"* %__first.addr.04.i.i.i.i.i.i, i64 0, i32 0, i32 0, i32 0
  %4 = load %class.Column**, %class.Column*** %_M_start.i.i.i.i.i.i.i.i.i, align 8, !tbaa !26
  %tobool.not.i.i.i.i.i.i.i.i.i.i = icmp eq %class.Column** %4, null
  br i1 %tobool.not.i.i.i.i.i.i.i.i.i.i, label %_ZSt8_DestroyISt6vectorIP6ColumnSaIS2_EEEvPT_.exit.i.i.i.i.i.i, label %if.then.i.i.i.i.i.i.i.i.i.i

if.then.i.i.i.i.i.i.i.i.i.i:                      ; preds = %for.body.i.i.i.i.i.i
  %5 = bitcast %class.Column** %4 to i8*
  tail call void @_ZdlPv(i8* nonnull %5) #18
  br label %_ZSt8_DestroyISt6vectorIP6ColumnSaIS2_EEEvPT_.exit.i.i.i.i.i.i

_ZSt8_DestroyISt6vectorIP6ColumnSaIS2_EEEvPT_.exit.i.i.i.i.i.i: ; preds = %if.then.i.i.i.i.i.i.i.i.i.i, %for.body.i.i.i.i.i.i
  %incdec.ptr.i.i.i.i.i.i = getelementptr inbounds %"class.std::vector", %"class.std::vector"* %__first.addr.04.i.i.i.i.i.i, i64 1
  %cmp.not.i.i.i.i.i.i = icmp eq %"class.std::vector"* %incdec.ptr.i.i.i.i.i.i, %3
  br i1 %cmp.not.i.i.i.i.i.i, label %invoke.cont.loopexit.i.i.i, label %for.body.i.i.i.i.i.i, !llvm.loop !43

invoke.cont.loopexit.i.i.i:                       ; preds = %_ZSt8_DestroyISt6vectorIP6ColumnSaIS2_EEEvPT_.exit.i.i.i.i.i.i
  %.pre.i.i.i = load %"class.std::vector"*, %"class.std::vector"** %_M_start.i.i.i, align 8, !tbaa !39
  br label %invoke.cont.i.i.i

invoke.cont.i.i.i:                                ; preds = %invoke.cont.loopexit.i.i.i, %delete.notnull.i
  %6 = phi %"class.std::vector"* [ %.pre.i.i.i, %invoke.cont.loopexit.i.i.i ], [ %2, %delete.notnull.i ]
  %tobool.not.i.i.i.i.i = icmp eq %"class.std::vector"* %6, null
  br i1 %tobool.not.i.i.i.i.i, label %_ZNSt6vectorIS_IP6ColumnSaIS1_EESaIS3_EED2Ev.exit.i.i, label %if.then.i.i.i.i.i

if.then.i.i.i.i.i:                                ; preds = %invoke.cont.i.i.i
  %7 = bitcast %"class.std::vector"* %6 to i8*
  tail call void @_ZdlPv(i8* nonnull %7) #18
  br label %_ZNSt6vectorIS_IP6ColumnSaIS1_EESaIS3_EED2Ev.exit.i.i

_ZNSt6vectorIS_IP6ColumnSaIS1_EESaIS3_EED2Ev.exit.i.i: ; preds = %if.then.i.i.i.i.i, %invoke.cont.i.i.i
  %_M_start.i.i.i.i = getelementptr inbounds %class.PagesIndex, %class.PagesIndex* %1, i64 0, i32 3, i32 0, i32 0, i32 0
  %8 = load i64*, i64** %_M_start.i.i.i.i, align 8, !tbaa !44
  %tobool.not.i.i.i2.i.i = icmp eq i64* %8, null
  br i1 %tobool.not.i.i.i2.i.i, label %_ZN10PagesIndexD2Ev.exit.i, label %if.then.i.i.i4.i.i

if.then.i.i.i4.i.i:                               ; preds = %_ZNSt6vectorIS_IP6ColumnSaIS1_EESaIS3_EED2Ev.exit.i.i
  %9 = bitcast i64* %8 to i8*
  tail call void @_ZdlPv(i8* nonnull %9) #18
  br label %_ZN10PagesIndexD2Ev.exit.i

_ZN10PagesIndexD2Ev.exit.i:                       ; preds = %if.then.i.i.i4.i.i, %_ZNSt6vectorIS_IP6ColumnSaIS1_EESaIS3_EED2Ev.exit.i.i
  %10 = bitcast %class.PagesIndex* %1 to i8*
  tail call void @_ZdlPv(i8* %10) #21
  br label %_ZN4SortD2Ev.exit

_ZN4SortD2Ev.exit:                                ; preds = %entry, %_ZN10PagesIndexD2Ev.exit.i
  %11 = bitcast %class.Sort* %this to i8*
  tail call void @_ZdlPv(i8* nonnull %11) #21
  ret void
}

; Function Attrs: norecurse nounwind readnone uwtable willreturn mustprogress
define dso_local void @_ZN4Sort7preloopEP5Table(%class.Sort* nocapture nonnull dereferenceable(80) %this, %class.Table* nocapture %table) unnamed_addr #3 align 2 {
entry:
  ret void
}

; Function Attrs: norecurse nounwind readnone uwtable willreturn mustprogress
define dso_local void @_ZN4Sort6inloopEP5Tablej(%class.Sort* nocapture nonnull dereferenceable(80) %this, %class.Table* nocapture %table, i32 %rowIdx) unnamed_addr #3 align 2 {
entry:
  ret void
}

; Function Attrs: norecurse nounwind readnone uwtable willreturn mustprogress
define dso_local void @_ZN4Sort8postloopEP5Table(%class.Sort* nocapture nonnull dereferenceable(80) %this, %class.Table* nocapture %table) unnamed_addr #3 align 2 {
entry:
  ret void
}

; Function Attrs: norecurse nounwind readnone uwtable willreturn mustprogress
define dso_local void @_ZN4Sort7processEP5Tablej(%class.Sort* nocapture nonnull dereferenceable(80) %this, %class.Table* nocapture %table, i32 %rowIdx) unnamed_addr #3 align 2 {
entry:
  ret void
}

; Function Attrs: nofree norecurse nounwind uwtable willreturn mustprogress
define dso_local void @_Z4swapPljj(i64* nocapture %valueAddresses, i32 %a, i32 %b) local_unnamed_addr #9 {
entry:
  %idxprom = zext i32 %a to i64
  %arrayidx = getelementptr inbounds i64, i64* %valueAddresses, i64 %idxprom
  %0 = load i64, i64* %arrayidx, align 8, !tbaa !47
  %idxprom1 = zext i32 %b to i64
  %arrayidx2 = getelementptr inbounds i64, i64* %valueAddresses, i64 %idxprom1
  %1 = load i64, i64* %arrayidx2, align 8, !tbaa !47
  store i64 %1, i64* %arrayidx, align 8, !tbaa !47
  store i64 %0, i64* %arrayidx2, align 8, !tbaa !47
  ret void
}

; Function Attrs: nofree norecurse nounwind uwtable mustprogress
define dso_local void @_Z10vectorSwapPljjj(i64* nocapture %valueAddresses, i32 %from, i32 %l, i32 %s) local_unnamed_addr #10 {
entry:
  %cmp7.not = icmp eq i32 %s, 0
  br i1 %cmp7.not, label %for.cond.cleanup, label %for.body.preheader

for.body.preheader:                               ; preds = %entry
  %min.iters.check = icmp ult i32 %s, 4
  br i1 %min.iters.check, label %for.body.preheader28, label %vector.scevcheck

vector.scevcheck:                                 ; preds = %for.body.preheader
  %0 = add i32 %s, -1
  %1 = xor i32 %from, -1
  %2 = icmp ugt i32 %0, %1
  %3 = xor i32 %l, -1
  %4 = icmp ugt i32 %0, %3
  %5 = or i1 %2, %4
  br i1 %5, label %for.body.preheader28, label %vector.memcheck

vector.memcheck:                                  ; preds = %vector.scevcheck
  %6 = zext i32 %from to i64
  %scevgep = getelementptr i64, i64* %valueAddresses, i64 %6
  %7 = add i32 %s, -1
  %8 = zext i32 %7 to i64
  %9 = add nuw nsw i64 %6, %8
  %10 = add nuw nsw i64 %9, 1
  %scevgep15 = getelementptr i64, i64* %valueAddresses, i64 %10
  %11 = zext i32 %l to i64
  %scevgep17 = getelementptr i64, i64* %valueAddresses, i64 %11
  %12 = add nuw nsw i64 %11, %8
  %13 = add nuw nsw i64 %12, 1
  %scevgep19 = getelementptr i64, i64* %valueAddresses, i64 %13
  %bound0 = icmp ult i64* %scevgep, %scevgep19
  %bound1 = icmp ult i64* %scevgep17, %scevgep15
  %found.conflict = and i1 %bound0, %bound1
  br i1 %found.conflict, label %for.body.preheader28, label %vector.ph

vector.ph:                                        ; preds = %vector.memcheck
  %n.vec = and i32 %s, -4
  %ind.end = add i32 %n.vec, %from
  %ind.end23 = add i32 %n.vec, %l
  %14 = add i32 %n.vec, -4
  %15 = lshr exact i32 %14, 2
  %16 = add nuw nsw i32 %15, 1
  %xtraiter29 = and i32 %16, 1
  %17 = icmp eq i32 %14, 0
  br i1 %17, label %middle.block.unr-lcssa, label %vector.ph.new

vector.ph.new:                                    ; preds = %vector.ph
  %unroll_iter = and i32 %16, 2147483646
  br label %vector.body

vector.body:                                      ; preds = %vector.body, %vector.ph.new
  %index = phi i32 [ 0, %vector.ph.new ], [ %index.next.1, %vector.body ]
  %niter = phi i32 [ %unroll_iter, %vector.ph.new ], [ %niter.nsub.1, %vector.body ]
  %offset.idx = add i32 %index, %from
  %offset.idx24 = add i32 %index, %l
  %18 = zext i32 %offset.idx to i64
  %19 = getelementptr inbounds i64, i64* %valueAddresses, i64 %18
  %20 = bitcast i64* %19 to <2 x i64>*
  %wide.load = load <2 x i64>, <2 x i64>* %20, align 8, !tbaa !47, !alias.scope !48, !noalias !51
  %21 = getelementptr inbounds i64, i64* %19, i64 2
  %22 = bitcast i64* %21 to <2 x i64>*
  %wide.load25 = load <2 x i64>, <2 x i64>* %22, align 8, !tbaa !47, !alias.scope !48, !noalias !51
  %23 = zext i32 %offset.idx24 to i64
  %24 = getelementptr inbounds i64, i64* %valueAddresses, i64 %23
  %25 = bitcast i64* %24 to <2 x i64>*
  %wide.load26 = load <2 x i64>, <2 x i64>* %25, align 8, !tbaa !47, !alias.scope !51
  %26 = getelementptr inbounds i64, i64* %24, i64 2
  %27 = bitcast i64* %26 to <2 x i64>*
  %wide.load27 = load <2 x i64>, <2 x i64>* %27, align 8, !tbaa !47, !alias.scope !51
  %28 = bitcast i64* %19 to <2 x i64>*
  store <2 x i64> %wide.load26, <2 x i64>* %28, align 8, !tbaa !47, !alias.scope !48, !noalias !51
  %29 = bitcast i64* %21 to <2 x i64>*
  store <2 x i64> %wide.load27, <2 x i64>* %29, align 8, !tbaa !47, !alias.scope !48, !noalias !51
  %30 = bitcast i64* %24 to <2 x i64>*
  store <2 x i64> %wide.load, <2 x i64>* %30, align 8, !tbaa !47, !alias.scope !51
  %31 = bitcast i64* %26 to <2 x i64>*
  store <2 x i64> %wide.load25, <2 x i64>* %31, align 8, !tbaa !47, !alias.scope !51
  %index.next = or i32 %index, 4
  %offset.idx.1 = add i32 %index.next, %from
  %offset.idx24.1 = add i32 %index.next, %l
  %32 = zext i32 %offset.idx.1 to i64
  %33 = getelementptr inbounds i64, i64* %valueAddresses, i64 %32
  %34 = bitcast i64* %33 to <2 x i64>*
  %wide.load.1 = load <2 x i64>, <2 x i64>* %34, align 8, !tbaa !47, !alias.scope !48, !noalias !51
  %35 = getelementptr inbounds i64, i64* %33, i64 2
  %36 = bitcast i64* %35 to <2 x i64>*
  %wide.load25.1 = load <2 x i64>, <2 x i64>* %36, align 8, !tbaa !47, !alias.scope !48, !noalias !51
  %37 = zext i32 %offset.idx24.1 to i64
  %38 = getelementptr inbounds i64, i64* %valueAddresses, i64 %37
  %39 = bitcast i64* %38 to <2 x i64>*
  %wide.load26.1 = load <2 x i64>, <2 x i64>* %39, align 8, !tbaa !47, !alias.scope !51
  %40 = getelementptr inbounds i64, i64* %38, i64 2
  %41 = bitcast i64* %40 to <2 x i64>*
  %wide.load27.1 = load <2 x i64>, <2 x i64>* %41, align 8, !tbaa !47, !alias.scope !51
  %42 = bitcast i64* %33 to <2 x i64>*
  store <2 x i64> %wide.load26.1, <2 x i64>* %42, align 8, !tbaa !47, !alias.scope !48, !noalias !51
  %43 = bitcast i64* %35 to <2 x i64>*
  store <2 x i64> %wide.load27.1, <2 x i64>* %43, align 8, !tbaa !47, !alias.scope !48, !noalias !51
  %44 = bitcast i64* %38 to <2 x i64>*
  store <2 x i64> %wide.load.1, <2 x i64>* %44, align 8, !tbaa !47, !alias.scope !51
  %45 = bitcast i64* %40 to <2 x i64>*
  store <2 x i64> %wide.load25.1, <2 x i64>* %45, align 8, !tbaa !47, !alias.scope !51
  %index.next.1 = add i32 %index, 8
  %niter.nsub.1 = add i32 %niter, -2
  %niter.ncmp.1 = icmp eq i32 %niter.nsub.1, 0
  br i1 %niter.ncmp.1, label %middle.block.unr-lcssa, label %vector.body, !llvm.loop !53

middle.block.unr-lcssa:                           ; preds = %vector.body, %vector.ph
  %index.unr = phi i32 [ 0, %vector.ph ], [ %index.next.1, %vector.body ]
  %lcmp.mod30.not = icmp eq i32 %xtraiter29, 0
  br i1 %lcmp.mod30.not, label %middle.block, label %vector.body.epil

vector.body.epil:                                 ; preds = %middle.block.unr-lcssa
  %offset.idx.epil = add i32 %index.unr, %from
  %offset.idx24.epil = add i32 %index.unr, %l
  %46 = zext i32 %offset.idx.epil to i64
  %47 = getelementptr inbounds i64, i64* %valueAddresses, i64 %46
  %48 = bitcast i64* %47 to <2 x i64>*
  %wide.load.epil = load <2 x i64>, <2 x i64>* %48, align 8, !tbaa !47, !alias.scope !48, !noalias !51
  %49 = getelementptr inbounds i64, i64* %47, i64 2
  %50 = bitcast i64* %49 to <2 x i64>*
  %wide.load25.epil = load <2 x i64>, <2 x i64>* %50, align 8, !tbaa !47, !alias.scope !48, !noalias !51
  %51 = zext i32 %offset.idx24.epil to i64
  %52 = getelementptr inbounds i64, i64* %valueAddresses, i64 %51
  %53 = bitcast i64* %52 to <2 x i64>*
  %wide.load26.epil = load <2 x i64>, <2 x i64>* %53, align 8, !tbaa !47, !alias.scope !51
  %54 = getelementptr inbounds i64, i64* %52, i64 2
  %55 = bitcast i64* %54 to <2 x i64>*
  %wide.load27.epil = load <2 x i64>, <2 x i64>* %55, align 8, !tbaa !47, !alias.scope !51
  %56 = bitcast i64* %47 to <2 x i64>*
  store <2 x i64> %wide.load26.epil, <2 x i64>* %56, align 8, !tbaa !47, !alias.scope !48, !noalias !51
  %57 = bitcast i64* %49 to <2 x i64>*
  store <2 x i64> %wide.load27.epil, <2 x i64>* %57, align 8, !tbaa !47, !alias.scope !48, !noalias !51
  %58 = bitcast i64* %52 to <2 x i64>*
  store <2 x i64> %wide.load.epil, <2 x i64>* %58, align 8, !tbaa !47, !alias.scope !51
  %59 = bitcast i64* %54 to <2 x i64>*
  store <2 x i64> %wide.load25.epil, <2 x i64>* %59, align 8, !tbaa !47, !alias.scope !51
  br label %middle.block

middle.block:                                     ; preds = %middle.block.unr-lcssa, %vector.body.epil
  %cmp.n = icmp eq i32 %n.vec, %s
  br i1 %cmp.n, label %for.cond.cleanup, label %for.body.preheader28

for.body.preheader28:                             ; preds = %vector.memcheck, %vector.scevcheck, %for.body.preheader, %middle.block
  %i.010.ph = phi i32 [ 0, %vector.memcheck ], [ 0, %vector.scevcheck ], [ 0, %for.body.preheader ], [ %n.vec, %middle.block ]
  %from.addr.09.ph = phi i32 [ %from, %vector.memcheck ], [ %from, %vector.scevcheck ], [ %from, %for.body.preheader ], [ %ind.end, %middle.block ]
  %l.addr.08.ph = phi i32 [ %l, %vector.memcheck ], [ %l, %vector.scevcheck ], [ %l, %for.body.preheader ], [ %ind.end23, %middle.block ]
  %60 = xor i32 %i.010.ph, -1
  %xtraiter = and i32 %s, 1
  %lcmp.mod.not = icmp eq i32 %xtraiter, 0
  br i1 %lcmp.mod.not, label %for.body.prol.loopexit, label %for.body.prol

for.body.prol:                                    ; preds = %for.body.preheader28
  %idxprom.i.prol = zext i32 %from.addr.09.ph to i64
  %arrayidx.i.prol = getelementptr inbounds i64, i64* %valueAddresses, i64 %idxprom.i.prol
  %61 = load i64, i64* %arrayidx.i.prol, align 8, !tbaa !47
  %idxprom1.i.prol = zext i32 %l.addr.08.ph to i64
  %arrayidx2.i.prol = getelementptr inbounds i64, i64* %valueAddresses, i64 %idxprom1.i.prol
  %62 = load i64, i64* %arrayidx2.i.prol, align 8, !tbaa !47
  store i64 %62, i64* %arrayidx.i.prol, align 8, !tbaa !47
  store i64 %61, i64* %arrayidx2.i.prol, align 8, !tbaa !47
  %inc.prol = or i32 %i.010.ph, 1
  %inc1.prol = add i32 %from.addr.09.ph, 1
  %inc2.prol = add i32 %l.addr.08.ph, 1
  br label %for.body.prol.loopexit

for.body.prol.loopexit:                           ; preds = %for.body.prol, %for.body.preheader28
  %i.010.unr = phi i32 [ %i.010.ph, %for.body.preheader28 ], [ %inc.prol, %for.body.prol ]
  %from.addr.09.unr = phi i32 [ %from.addr.09.ph, %for.body.preheader28 ], [ %inc1.prol, %for.body.prol ]
  %l.addr.08.unr = phi i32 [ %l.addr.08.ph, %for.body.preheader28 ], [ %inc2.prol, %for.body.prol ]
  %63 = sub i32 0, %s
  %64 = icmp eq i32 %60, %63
  br i1 %64, label %for.cond.cleanup, label %for.body

for.cond.cleanup:                                 ; preds = %for.body.prol.loopexit, %for.body, %middle.block, %entry
  ret void

for.body:                                         ; preds = %for.body.prol.loopexit, %for.body
  %i.010 = phi i32 [ %inc.1, %for.body ], [ %i.010.unr, %for.body.prol.loopexit ]
  %from.addr.09 = phi i32 [ %inc1.1, %for.body ], [ %from.addr.09.unr, %for.body.prol.loopexit ]
  %l.addr.08 = phi i32 [ %inc2.1, %for.body ], [ %l.addr.08.unr, %for.body.prol.loopexit ]
  %idxprom.i = zext i32 %from.addr.09 to i64
  %arrayidx.i = getelementptr inbounds i64, i64* %valueAddresses, i64 %idxprom.i
  %65 = load i64, i64* %arrayidx.i, align 8, !tbaa !47
  %idxprom1.i = zext i32 %l.addr.08 to i64
  %arrayidx2.i = getelementptr inbounds i64, i64* %valueAddresses, i64 %idxprom1.i
  %66 = load i64, i64* %arrayidx2.i, align 8, !tbaa !47
  store i64 %66, i64* %arrayidx.i, align 8, !tbaa !47
  store i64 %65, i64* %arrayidx2.i, align 8, !tbaa !47
  %inc1 = add i32 %from.addr.09, 1
  %inc2 = add i32 %l.addr.08, 1
  %idxprom.i.1 = zext i32 %inc1 to i64
  %arrayidx.i.1 = getelementptr inbounds i64, i64* %valueAddresses, i64 %idxprom.i.1
  %67 = load i64, i64* %arrayidx.i.1, align 8, !tbaa !47
  %idxprom1.i.1 = zext i32 %inc2 to i64
  %arrayidx2.i.1 = getelementptr inbounds i64, i64* %valueAddresses, i64 %idxprom1.i.1
  %68 = load i64, i64* %arrayidx2.i.1, align 8, !tbaa !47
  store i64 %68, i64* %arrayidx.i.1, align 8, !tbaa !47
  store i64 %67, i64* %arrayidx2.i.1, align 8, !tbaa !47
  %inc.1 = add nuw i32 %i.010, 2
  %inc1.1 = add i32 %from.addr.09, 2
  %inc2.1 = add i32 %l.addr.08, 2
  %exitcond.not.1 = icmp eq i32 %inc.1, %s
  br i1 %exitcond.not.1, label %for.cond.cleanup, label %for.body, !llvm.loop !55
}

; Function Attrs: uwtable
define dso_local void @_ZN10PagesIndexC2EPii(%class.PagesIndex* nonnull dereferenceable(68) %this, i32* %types, i32 %typeCount) unnamed_addr #4 align 2 personality i8* bitcast (i32 (...)* @__gxx_personality_v0 to i8*) {
entry:
  %column = alloca %"class.std::vector", align 8
  %valueAddresses = getelementptr inbounds %class.PagesIndex, %class.PagesIndex* %this, i64 0, i32 3
  %columns = getelementptr inbounds %class.PagesIndex, %class.PagesIndex* %this, i64 0, i32 4
  %types2 = getelementptr inbounds %class.PagesIndex, %class.PagesIndex* %this, i64 0, i32 0
  %0 = bitcast %"class.std::vector.0"* %valueAddresses to i8*
  call void @llvm.memset.p0i8.i64(i8* nonnull align 8 dereferenceable(48) %0, i8 0, i64 48, i1 false)
  store i32* %types, i32** %types2, align 8, !tbaa !56
  %typeCount3 = getelementptr inbounds %class.PagesIndex, %class.PagesIndex* %this, i64 0, i32 1
  store i32 %typeCount, i32* %typeCount3, align 8, !tbaa !60
  %cmp36.not = icmp eq i32 %typeCount, 0
  br i1 %cmp36.not, label %for.cond.cleanup, label %for.body.lr.ph

for.body.lr.ph:                                   ; preds = %entry
  %1 = bitcast %"class.std::vector"* %column to i8*
  %_M_finish.i20 = getelementptr inbounds %class.PagesIndex, %class.PagesIndex* %this, i64 0, i32 4, i32 0, i32 0, i32 1
  %_M_end_of_storage.i = getelementptr inbounds %class.PagesIndex, %class.PagesIndex* %this, i64 0, i32 4, i32 0, i32 0, i32 2
  %_M_finish.i.i.i.i.i = getelementptr inbounds %"class.std::vector", %"class.std::vector"* %column, i64 0, i32 0, i32 0, i32 1
  %_M_start.i.i.i.i.i = getelementptr inbounds %"class.std::vector", %"class.std::vector"* %column, i64 0, i32 0, i32 0, i32 0
  br label %for.body

for.cond.cleanup.loopexit:                        ; preds = %_ZNSt6vectorIP6ColumnSaIS1_EED2Ev.exit
  %_M_end_of_storage.i.i.phi.trans.insert = getelementptr inbounds %class.PagesIndex, %class.PagesIndex* %this, i64 0, i32 3, i32 0, i32 0, i32 2
  %.pre41 = load i64*, i64** %_M_end_of_storage.i.i.phi.trans.insert, align 8, !tbaa !61
  %_M_start.i.i17.phi.trans.insert = getelementptr inbounds %"class.std::vector.0", %"class.std::vector.0"* %valueAddresses, i64 0, i32 0, i32 0, i32 0
  %.pre42 = load i64*, i64** %_M_start.i.i17.phi.trans.insert, align 8, !tbaa !44
  %phi.cast = ptrtoint i64* %.pre41 to i64
  br label %for.cond.cleanup

for.cond.cleanup:                                 ; preds = %for.cond.cleanup.loopexit, %entry
  %2 = phi i64* [ %.pre42, %for.cond.cleanup.loopexit ], [ null, %entry ]
  %3 = phi i64 [ %phi.cast, %for.cond.cleanup.loopexit ], [ 0, %entry ]
  %positionCount = getelementptr inbounds %class.PagesIndex, %class.PagesIndex* %this, i64 0, i32 5
  store i32 0, i32* %positionCount, align 8, !tbaa !62
  %_M_end_of_storage.i.i = getelementptr inbounds %class.PagesIndex, %class.PagesIndex* %this, i64 0, i32 3, i32 0, i32 0, i32 2
  %_M_start.i.i17 = getelementptr inbounds %"class.std::vector.0", %"class.std::vector.0"* %valueAddresses, i64 0, i32 0, i32 0, i32 0
  %sub.ptr.rhs.cast.i.i = ptrtoint i64* %2 to i64
  %sub.ptr.sub.i.i = sub i64 %3, %sub.ptr.rhs.cast.i.i
  %sub.ptr.div.i.i = ashr exact i64 %sub.ptr.sub.i.i, 3
  %cmp3.i = icmp ult i64 %sub.ptr.div.i.i, 100000000
  br i1 %cmp3.i, label %_ZNSt12_Vector_baseIlSaIlEE11_M_allocateEm.exit.i.i, label %invoke.cont7

_ZNSt12_Vector_baseIlSaIlEE11_M_allocateEm.exit.i.i: ; preds = %for.cond.cleanup
  %_M_finish.i.i = getelementptr inbounds %class.PagesIndex, %class.PagesIndex* %this, i64 0, i32 3, i32 0, i32 0, i32 1
  %4 = load i64*, i64** %_M_finish.i.i, align 8, !tbaa !63
  %sub.ptr.lhs.cast.i41.i = ptrtoint i64* %4 to i64
  %sub.ptr.sub.i43.i = sub i64 %sub.ptr.lhs.cast.i41.i, %sub.ptr.rhs.cast.i.i
  %sub.ptr.div.i44.i = ashr exact i64 %sub.ptr.sub.i43.i, 3
  %call2.i.i.i.i.i18 = invoke noalias nonnull i8* @_Znwm(i64 800000000) #20
          to label %call2.i.i.i.i.i.noexc unwind label %lpad6

call2.i.i.i.i.i.noexc:                            ; preds = %_ZNSt12_Vector_baseIlSaIlEE11_M_allocateEm.exit.i.i
  %5 = bitcast i8* %call2.i.i.i.i.i18 to i64*
  %tobool.not.i.i.i.i.i.i.i.i.i = icmp eq i64 %sub.ptr.sub.i43.i, 0
  br i1 %tobool.not.i.i.i.i.i.i.i.i.i, label %_ZNSt6vectorIlSaIlEE20_M_allocate_and_copyISt13move_iteratorIPlEEES4_mT_S6_.exit.i, label %if.then.i.i.i.i.i.i.i.i.i

if.then.i.i.i.i.i.i.i.i.i:                        ; preds = %call2.i.i.i.i.i.noexc
  %6 = bitcast i64* %2 to i8*
  call void @llvm.memcpy.p0i8.p0i8.i64(i8* nonnull align 8 %call2.i.i.i.i.i18, i8* align 8 %6, i64 %sub.ptr.sub.i43.i, i1 false) #18
  br label %_ZNSt6vectorIlSaIlEE20_M_allocate_and_copyISt13move_iteratorIPlEEES4_mT_S6_.exit.i

_ZNSt6vectorIlSaIlEE20_M_allocate_and_copyISt13move_iteratorIPlEEES4_mT_S6_.exit.i: ; preds = %if.then.i.i.i.i.i.i.i.i.i, %call2.i.i.i.i.i.noexc
  %7 = load i64*, i64** %_M_start.i.i17, align 8, !tbaa !44
  %tobool.not.i.i = icmp eq i64* %7, null
  br i1 %tobool.not.i.i, label %_ZNSt12_Vector_baseIlSaIlEE13_M_deallocateEPlm.exit.i, label %if.then.i.i

if.then.i.i:                                      ; preds = %_ZNSt6vectorIlSaIlEE20_M_allocate_and_copyISt13move_iteratorIPlEEES4_mT_S6_.exit.i
  %8 = bitcast i64* %7 to i8*
  call void @_ZdlPv(i8* nonnull %8) #18
  br label %_ZNSt12_Vector_baseIlSaIlEE13_M_deallocateEPlm.exit.i

_ZNSt12_Vector_baseIlSaIlEE13_M_deallocateEPlm.exit.i: ; preds = %if.then.i.i, %_ZNSt6vectorIlSaIlEE20_M_allocate_and_copyISt13move_iteratorIPlEEES4_mT_S6_.exit.i
  %9 = bitcast %"class.std::vector.0"* %valueAddresses to i8**
  store i8* %call2.i.i.i.i.i18, i8** %9, align 8, !tbaa !44
  %add.ptr.i = getelementptr inbounds i64, i64* %5, i64 %sub.ptr.div.i44.i
  store i64* %add.ptr.i, i64** %_M_finish.i.i, align 8, !tbaa !63
  %add.ptr30.i = getelementptr inbounds i8, i8* %call2.i.i.i.i.i18, i64 800000000
  %10 = bitcast i64** %_M_end_of_storage.i.i to i8**
  store i8* %add.ptr30.i, i8** %10, align 8, !tbaa !61
  br label %invoke.cont7

for.body:                                         ; preds = %_ZNSt6vectorIP6ColumnSaIS1_EED2Ev.exit.for.body_crit_edge, %for.body.lr.ph
  %11 = phi %"class.std::vector"* [ null, %for.body.lr.ph ], [ %.pre38, %_ZNSt6vectorIP6ColumnSaIS1_EED2Ev.exit.for.body_crit_edge ]
  %12 = phi %"class.std::vector"* [ null, %for.body.lr.ph ], [ %.pre, %_ZNSt6vectorIP6ColumnSaIS1_EED2Ev.exit.for.body_crit_edge ]
  %i.037 = phi i32 [ 0, %for.body.lr.ph ], [ %inc, %_ZNSt6vectorIP6ColumnSaIS1_EED2Ev.exit.for.body_crit_edge ]
  call void @llvm.lifetime.start.p0i8(i64 24, i8* nonnull %1) #18
  call void @llvm.memset.p0i8.i64(i8* nonnull align 8 dereferenceable(24) %1, i8 0, i64 24, i1 false) #18
  %cmp.not.i = icmp eq %"class.std::vector"* %12, %11
  br i1 %cmp.not.i, label %if.else.i, label %invoke.cont.i.i.i.i

invoke.cont.i.i.i.i:                              ; preds = %for.body
  %_M_finish.i.i.i.i.i.i = getelementptr inbounds %"class.std::vector", %"class.std::vector"* %12, i64 0, i32 0, i32 0, i32 1
  %13 = bitcast %"class.std::vector"* %12 to i8*
  call void @llvm.memset.p0i8.i64(i8* nonnull align 8 dereferenceable(24) %13, i8 0, i64 24, i1 false)
  %14 = load %class.Column**, %class.Column*** %_M_start.i.i.i.i.i, align 8, !tbaa !2
  %15 = load %class.Column**, %class.Column*** %_M_finish.i.i.i.i.i, align 8, !tbaa !2
  %sub.ptr.lhs.cast.i.i.i.i.i.i.i.i.i.i.i = ptrtoint %class.Column** %15 to i64
  %sub.ptr.rhs.cast.i.i.i.i.i.i.i.i.i.i.i = ptrtoint %class.Column** %14 to i64
  %sub.ptr.sub.i.i.i.i.i.i.i.i.i.i.i = sub i64 %sub.ptr.lhs.cast.i.i.i.i.i.i.i.i.i.i.i, %sub.ptr.rhs.cast.i.i.i.i.i.i.i.i.i.i.i
  %tobool.not.i.i.i.i.i.i.i.i.i.i.i = icmp eq i64 %sub.ptr.sub.i.i.i.i.i.i.i.i.i.i.i, 0
  br i1 %tobool.not.i.i.i.i.i.i.i.i.i.i.i, label %_ZNSt16allocator_traitsISaISt6vectorIP6ColumnSaIS2_EEEE9constructIS4_JRKS4_EEEvRS5_PT_DpOT0_.exit.i, label %if.then.i.i.i.i.i.i.i.i.i.i.i

if.then.i.i.i.i.i.i.i.i.i.i.i:                    ; preds = %invoke.cont.i.i.i.i
  %16 = bitcast %class.Column** %14 to i8*
  call void @llvm.memcpy.p0i8.p0i8.i64(i8* align 536870912 null, i8* align 8 %16, i64 %sub.ptr.sub.i.i.i.i.i.i.i.i.i.i.i, i1 false) #18
  br label %_ZNSt16allocator_traitsISaISt6vectorIP6ColumnSaIS2_EEEE9constructIS4_JRKS4_EEEvRS5_PT_DpOT0_.exit.i

_ZNSt16allocator_traitsISaISt6vectorIP6ColumnSaIS2_EEEE9constructIS4_JRKS4_EEEvRS5_PT_DpOT0_.exit.i: ; preds = %if.then.i.i.i.i.i.i.i.i.i.i.i, %invoke.cont.i.i.i.i
  %sub.ptr.div.i.i.i.i.i.i.i.i.i.i.i = ashr exact i64 %sub.ptr.sub.i.i.i.i.i.i.i.i.i.i.i, 3
  %add.ptr.i.i.i.i.i.i.i.i.i.i.i = getelementptr inbounds %class.Column*, %class.Column** null, i64 %sub.ptr.div.i.i.i.i.i.i.i.i.i.i.i
  store %class.Column** %add.ptr.i.i.i.i.i.i.i.i.i.i.i, %class.Column*** %_M_finish.i.i.i.i.i.i, align 8, !tbaa !22
  %17 = load %"class.std::vector"*, %"class.std::vector"** %_M_finish.i20, align 8, !tbaa !42
  %incdec.ptr.i = getelementptr inbounds %"class.std::vector", %"class.std::vector"* %17, i64 1
  store %"class.std::vector"* %incdec.ptr.i, %"class.std::vector"** %_M_finish.i20, align 8, !tbaa !42
  br label %invoke.cont

if.else.i:                                        ; preds = %for.body
  invoke void @_ZNSt6vectorIS_IP6ColumnSaIS1_EESaIS3_EE17_M_realloc_insertIJRKS3_EEEvN9__gnu_cxx17__normal_iteratorIPS3_S5_EEDpOT_(%"class.std::vector.5"* nonnull dereferenceable(24) %columns, %"class.std::vector"* %11, %"class.std::vector"* nonnull align 8 dereferenceable(24) %column)
          to label %if.else.i.invoke.cont_crit_edge unwind label %lpad

if.else.i.invoke.cont_crit_edge:                  ; preds = %if.else.i
  %.pre40 = load %class.Column**, %class.Column*** %_M_start.i.i.i.i.i, align 8, !tbaa !26
  br label %invoke.cont

invoke.cont:                                      ; preds = %if.else.i.invoke.cont_crit_edge, %_ZNSt16allocator_traitsISaISt6vectorIP6ColumnSaIS2_EEEE9constructIS4_JRKS4_EEEvRS5_PT_DpOT0_.exit.i
  %18 = phi %class.Column** [ %.pre40, %if.else.i.invoke.cont_crit_edge ], [ %14, %_ZNSt16allocator_traitsISaISt6vectorIP6ColumnSaIS2_EEEE9constructIS4_JRKS4_EEEvRS5_PT_DpOT0_.exit.i ]
  %tobool.not.i.i.i25 = icmp eq %class.Column** %18, null
  br i1 %tobool.not.i.i.i25, label %_ZNSt6vectorIP6ColumnSaIS1_EED2Ev.exit, label %if.then.i.i.i27

if.then.i.i.i27:                                  ; preds = %invoke.cont
  %19 = bitcast %class.Column** %18 to i8*
  call void @_ZdlPv(i8* nonnull %19) #18
  br label %_ZNSt6vectorIP6ColumnSaIS1_EED2Ev.exit

_ZNSt6vectorIP6ColumnSaIS1_EED2Ev.exit:           ; preds = %invoke.cont, %if.then.i.i.i27
  call void @llvm.lifetime.end.p0i8(i64 24, i8* nonnull %1) #18
  %inc = add nuw i32 %i.037, 1
  %exitcond.not = icmp eq i32 %inc, %typeCount
  br i1 %exitcond.not, label %for.cond.cleanup.loopexit, label %_ZNSt6vectorIP6ColumnSaIS1_EED2Ev.exit.for.body_crit_edge, !llvm.loop !64

_ZNSt6vectorIP6ColumnSaIS1_EED2Ev.exit.for.body_crit_edge: ; preds = %_ZNSt6vectorIP6ColumnSaIS1_EED2Ev.exit
  %.pre = load %"class.std::vector"*, %"class.std::vector"** %_M_finish.i20, align 8, !tbaa !42
  %.pre38 = load %"class.std::vector"*, %"class.std::vector"** %_M_end_of_storage.i, align 8, !tbaa !65
  br label %for.body

lpad:                                             ; preds = %if.else.i
  %lpad.loopexit33 = landingpad { i8*, i32 }
          cleanup
  %.pre39 = load %class.Column**, %class.Column*** %_M_start.i.i.i.i.i, align 8, !tbaa !26
  %tobool.not.i.i.i29 = icmp eq %class.Column** %.pre39, null
  br i1 %tobool.not.i.i.i29, label %_ZNSt6vectorIP6ColumnSaIS1_EED2Ev.exit32, label %if.then.i.i.i31

if.then.i.i.i31:                                  ; preds = %lpad
  %20 = bitcast %class.Column** %.pre39 to i8*
  call void @_ZdlPv(i8* nonnull %20) #18
  br label %_ZNSt6vectorIP6ColumnSaIS1_EED2Ev.exit32

_ZNSt6vectorIP6ColumnSaIS1_EED2Ev.exit32:         ; preds = %lpad, %if.then.i.i.i31
  call void @llvm.lifetime.end.p0i8(i64 24, i8* nonnull %1) #18
  br label %ehcleanup

invoke.cont7:                                     ; preds = %_ZNSt12_Vector_baseIlSaIlEE13_M_deallocateEPlm.exit.i, %for.cond.cleanup
  ret void

lpad6:                                            ; preds = %_ZNSt12_Vector_baseIlSaIlEE11_M_allocateEm.exit.i.i
  %21 = landingpad { i8*, i32 }
          cleanup
  br label %ehcleanup

ehcleanup:                                        ; preds = %lpad6, %_ZNSt6vectorIP6ColumnSaIS1_EED2Ev.exit32
  %.pn = phi { i8*, i32 } [ %lpad.loopexit33, %_ZNSt6vectorIP6ColumnSaIS1_EED2Ev.exit32 ], [ %21, %lpad6 ]
  %_M_start.i = getelementptr inbounds %"class.std::vector.5", %"class.std::vector.5"* %columns, i64 0, i32 0, i32 0, i32 0
  %22 = load %"class.std::vector"*, %"class.std::vector"** %_M_start.i, align 8, !tbaa !39
  %_M_finish.i = getelementptr inbounds %class.PagesIndex, %class.PagesIndex* %this, i64 0, i32 4, i32 0, i32 0, i32 1
  %23 = load %"class.std::vector"*, %"class.std::vector"** %_M_finish.i, align 8, !tbaa !42
  %cmp.not3.i.i.i.i = icmp eq %"class.std::vector"* %22, %23
  br i1 %cmp.not3.i.i.i.i, label %invoke.cont.i, label %for.body.i.i.i.i

for.body.i.i.i.i:                                 ; preds = %ehcleanup, %_ZSt8_DestroyISt6vectorIP6ColumnSaIS2_EEEvPT_.exit.i.i.i.i
  %__first.addr.04.i.i.i.i = phi %"class.std::vector"* [ %incdec.ptr.i.i.i.i, %_ZSt8_DestroyISt6vectorIP6ColumnSaIS2_EEEvPT_.exit.i.i.i.i ], [ %22, %ehcleanup ]
  %_M_start.i.i.i.i.i.i.i = getelementptr inbounds %"class.std::vector", %"class.std::vector"* %__first.addr.04.i.i.i.i, i64 0, i32 0, i32 0, i32 0
  %24 = load %class.Column**, %class.Column*** %_M_start.i.i.i.i.i.i.i, align 8, !tbaa !26
  %tobool.not.i.i.i.i.i.i.i.i = icmp eq %class.Column** %24, null
  br i1 %tobool.not.i.i.i.i.i.i.i.i, label %_ZSt8_DestroyISt6vectorIP6ColumnSaIS2_EEEvPT_.exit.i.i.i.i, label %if.then.i.i.i.i.i.i.i.i

if.then.i.i.i.i.i.i.i.i:                          ; preds = %for.body.i.i.i.i
  %25 = bitcast %class.Column** %24 to i8*
  call void @_ZdlPv(i8* nonnull %25) #18
  br label %_ZSt8_DestroyISt6vectorIP6ColumnSaIS2_EEEvPT_.exit.i.i.i.i

_ZSt8_DestroyISt6vectorIP6ColumnSaIS2_EEEvPT_.exit.i.i.i.i: ; preds = %if.then.i.i.i.i.i.i.i.i, %for.body.i.i.i.i
  %incdec.ptr.i.i.i.i = getelementptr inbounds %"class.std::vector", %"class.std::vector"* %__first.addr.04.i.i.i.i, i64 1
  %cmp.not.i.i.i.i = icmp eq %"class.std::vector"* %incdec.ptr.i.i.i.i, %23
  br i1 %cmp.not.i.i.i.i, label %invoke.cont.loopexit.i, label %for.body.i.i.i.i, !llvm.loop !43

invoke.cont.loopexit.i:                           ; preds = %_ZSt8_DestroyISt6vectorIP6ColumnSaIS2_EEEvPT_.exit.i.i.i.i
  %.pre.i = load %"class.std::vector"*, %"class.std::vector"** %_M_start.i, align 8, !tbaa !39
  br label %invoke.cont.i

invoke.cont.i:                                    ; preds = %invoke.cont.loopexit.i, %ehcleanup
  %26 = phi %"class.std::vector"* [ %.pre.i, %invoke.cont.loopexit.i ], [ %22, %ehcleanup ]
  %tobool.not.i.i.i15 = icmp eq %"class.std::vector"* %26, null
  br i1 %tobool.not.i.i.i15, label %_ZNSt6vectorIS_IP6ColumnSaIS1_EESaIS3_EED2Ev.exit, label %if.then.i.i.i16

if.then.i.i.i16:                                  ; preds = %invoke.cont.i
  %27 = bitcast %"class.std::vector"* %26 to i8*
  call void @_ZdlPv(i8* nonnull %27) #18
  br label %_ZNSt6vectorIS_IP6ColumnSaIS1_EESaIS3_EED2Ev.exit

_ZNSt6vectorIS_IP6ColumnSaIS1_EESaIS3_EED2Ev.exit: ; preds = %invoke.cont.i, %if.then.i.i.i16
  %_M_start.i.i = getelementptr inbounds %"class.std::vector.0", %"class.std::vector.0"* %valueAddresses, i64 0, i32 0, i32 0, i32 0
  %28 = load i64*, i64** %_M_start.i.i, align 8, !tbaa !44
  %tobool.not.i.i.i = icmp eq i64* %28, null
  br i1 %tobool.not.i.i.i, label %_ZNSt6vectorIlSaIlEED2Ev.exit, label %if.then.i.i.i

if.then.i.i.i:                                    ; preds = %_ZNSt6vectorIS_IP6ColumnSaIS1_EESaIS3_EED2Ev.exit
  %29 = bitcast i64* %28 to i8*
  call void @_ZdlPv(i8* nonnull %29) #18
  br label %_ZNSt6vectorIlSaIlEED2Ev.exit

_ZNSt6vectorIlSaIlEED2Ev.exit:                    ; preds = %_ZNSt6vectorIS_IP6ColumnSaIS1_EESaIS3_EED2Ev.exit, %if.then.i.i.i
  resume { i8*, i32 } %.pn
}

; Function Attrs: uwtable
define dso_local void @_ZN10PagesIndex8addTableEP5Tableij(%class.PagesIndex* nocapture nonnull dereferenceable(68) %this, %class.Table* nocapture readonly %table, i32 %colCount, i32 %positionCount) local_unnamed_addr #4 align 2 personality i32 (...)* @__gxx_personality_v0 {
entry:
  %cmp = icmp eq i32 %positionCount, 0
  br i1 %cmp, label %return, label %if.end

if.end:                                           ; preds = %entry
  %positionCount2 = getelementptr inbounds %class.PagesIndex, %class.PagesIndex* %this, i64 0, i32 5
  %0 = load i32, i32* %positionCount2, align 8, !tbaa !62
  %add = add i32 %0, %positionCount
  store i32 %add, i32* %positionCount2, align 8, !tbaa !62
  %cmp3 = icmp sgt i32 %colCount, 0
  br i1 %cmp3, label %cond.end.thread, label %cond.end

cond.end.thread:                                  ; preds = %if.end
  %_M_start.i = getelementptr inbounds %class.PagesIndex, %class.PagesIndex* %this, i64 0, i32 4, i32 0, i32 0, i32 0
  %1 = load %"class.std::vector"*, %"class.std::vector"** %_M_start.i, align 8, !tbaa !39
  %_M_finish.i29 = getelementptr inbounds %"class.std::vector", %"class.std::vector"* %1, i64 0, i32 0, i32 0, i32 1
  %2 = load %class.Column**, %class.Column*** %_M_finish.i29, align 8, !tbaa !22
  %_M_start.i30 = getelementptr inbounds %"class.std::vector", %"class.std::vector"* %1, i64 0, i32 0, i32 0, i32 0
  %3 = load %class.Column**, %class.Column*** %_M_start.i30, align 8, !tbaa !26
  %sub.ptr.lhs.cast.i = ptrtoint %class.Column** %2 to i64
  %sub.ptr.rhs.cast.i = ptrtoint %class.Column** %3 to i64
  %sub.ptr.sub.i = sub i64 %sub.ptr.lhs.cast.i, %sub.ptr.rhs.cast.i
  %phi.bo = shl i64 %sub.ptr.sub.i, 29
  br label %for.body.lr.ph

cond.end:                                         ; preds = %if.end
  %cmp585.not = icmp eq i32 %colCount, 0
  br i1 %cmp585.not, label %for.body13.lr.ph, label %for.body.lr.ph

for.body.lr.ph:                                   ; preds = %cond.end.thread, %cond.end
  %cond94 = phi i64 [ %phi.bo, %cond.end.thread ], [ 0, %cond.end ]
  %_M_start.i.i = getelementptr inbounds %class.Table, %class.Table* %table, i64 0, i32 3, i32 0, i32 0, i32 0
  %_M_start.i77 = getelementptr inbounds %class.PagesIndex, %class.PagesIndex* %this, i64 0, i32 4, i32 0, i32 0, i32 0
  %wide.trip.count89 = zext i32 %colCount to i64
  br label %for.body

for.body13.lr.ph:                                 ; preds = %_ZNSt6vectorIP6ColumnSaIS1_EE9push_backERKS1_.exit, %cond.end
  %cond95 = phi i64 [ 0, %cond.end ], [ %cond94, %_ZNSt6vectorIP6ColumnSaIS1_EE9push_backERKS1_.exit ]
  %_M_finish.i = getelementptr inbounds %class.PagesIndex, %class.PagesIndex* %this, i64 0, i32 3, i32 0, i32 0, i32 1
  %_M_end_of_storage.i = getelementptr inbounds %class.PagesIndex, %class.PagesIndex* %this, i64 0, i32 3, i32 0, i32 0, i32 2
  %_M_start.i27.i.i.i = getelementptr inbounds %class.PagesIndex, %class.PagesIndex* %this, i64 0, i32 3, i32 0, i32 0, i32 0
  %4 = icmp ugt i32 %positionCount, 1
  %umax = select i1 %4, i32 %positionCount, i32 1
  %wide.trip.count = zext i32 %umax to i64
  %.pre = load i64*, i64** %_M_finish.i, align 8, !tbaa !63
  %.pre91 = load i64*, i64** %_M_end_of_storage.i, align 8, !tbaa !61
  br label %for.body13

for.body:                                         ; preds = %for.body.lr.ph, %_ZNSt6vectorIP6ColumnSaIS1_EE9push_backERKS1_.exit
  %indvars.iv87 = phi i64 [ 0, %for.body.lr.ph ], [ %indvars.iv.next88, %_ZNSt6vectorIP6ColumnSaIS1_EE9push_backERKS1_.exit ]
  %5 = load %class.Column**, %class.Column*** %_M_start.i.i, align 8, !tbaa !26
  %add.ptr.i.i31 = getelementptr inbounds %class.Column*, %class.Column** %5, i64 %indvars.iv87
  %6 = load %class.Column*, %class.Column** %add.ptr.i.i31, align 8, !tbaa !2
  %7 = load %"class.std::vector"*, %"class.std::vector"** %_M_start.i77, align 8, !tbaa !39
  %_M_finish.i33 = getelementptr inbounds %"class.std::vector", %"class.std::vector"* %7, i64 %indvars.iv87, i32 0, i32 0, i32 1
  %8 = load %class.Column**, %class.Column*** %_M_finish.i33, align 8, !tbaa !22
  %_M_end_of_storage.i34 = getelementptr inbounds %"class.std::vector", %"class.std::vector"* %7, i64 %indvars.iv87, i32 0, i32 0, i32 2
  %9 = load %class.Column**, %class.Column*** %_M_end_of_storage.i34, align 8, !tbaa !25
  %cmp.not.i35 = icmp eq %class.Column** %8, %9
  br i1 %cmp.not.i35, label %if.else.i51, label %if.then.i37

if.then.i37:                                      ; preds = %for.body
  store %class.Column* %6, %class.Column** %8, align 8, !tbaa !2
  %10 = load %class.Column**, %class.Column*** %_M_finish.i33, align 8, !tbaa !22
  %incdec.ptr.i36 = getelementptr inbounds %class.Column*, %class.Column** %10, i64 1
  br label %_ZNSt6vectorIP6ColumnSaIS1_EE9push_backERKS1_.exit

if.else.i51:                                      ; preds = %for.body
  %_M_start.i27.i.i.i38 = getelementptr inbounds %"class.std::vector", %"class.std::vector"* %7, i64 %indvars.iv87, i32 0, i32 0, i32 0
  %11 = load %class.Column**, %class.Column*** %_M_start.i27.i.i.i38, align 8, !tbaa !26
  %sub.ptr.lhs.cast.i28.i.i.i39 = ptrtoint %class.Column** %8 to i64
  %sub.ptr.rhs.cast.i29.i.i.i40 = ptrtoint %class.Column** %11 to i64
  %sub.ptr.sub.i30.i.i.i41 = sub i64 %sub.ptr.lhs.cast.i28.i.i.i39, %sub.ptr.rhs.cast.i29.i.i.i40
  %sub.ptr.div.i31.i.i.i42 = ashr exact i64 %sub.ptr.sub.i30.i.i.i41, 3
  %cmp.i.i.i.i43 = icmp eq i64 %sub.ptr.sub.i30.i.i.i41, 0
  %.sroa.speculated.i.i.i44 = select i1 %cmp.i.i.i.i43, i64 1, i64 %sub.ptr.div.i31.i.i.i42
  %add.i.i.i45 = add nsw i64 %.sroa.speculated.i.i.i44, %sub.ptr.div.i31.i.i.i42
  %cmp7.i.i.i46 = icmp ult i64 %add.i.i.i45, %sub.ptr.div.i31.i.i.i42
  %cmp9.i.i.i47 = icmp ugt i64 %add.i.i.i45, 2305843009213693951
  %or.cond.i.i.i48 = or i1 %cmp7.i.i.i46, %cmp9.i.i.i47
  %cond.i.i.i49 = select i1 %or.cond.i.i.i48, i64 2305843009213693951, i64 %add.i.i.i45
  %cmp.not.i.i.i50 = icmp eq i64 %cond.i.i.i49, 0
  br i1 %cmp.not.i.i.i50, label %invoke.cont.i.i61, label %_ZNSt16allocator_traitsISaIP6ColumnEE8allocateERS2_m.exit.i.i.i

_ZNSt16allocator_traitsISaIP6ColumnEE8allocateERS2_m.exit.i.i.i: ; preds = %if.else.i51
  %mul.i.i.i.i.i52 = shl nuw i64 %cond.i.i.i49, 3
  %call2.i.i.i.i.i53 = tail call noalias nonnull i8* @_Znwm(i64 %mul.i.i.i.i.i52) #20
  %12 = bitcast i8* %call2.i.i.i.i.i53 to %class.Column**
  %.pre.i.i54 = load %class.Column**, %class.Column*** %_M_start.i27.i.i.i38, align 8, !tbaa !26
  %.pre83.i.i55 = ptrtoint %class.Column** %.pre.i.i54 to i64
  %.pre84.i.i56 = sub i64 %sub.ptr.lhs.cast.i28.i.i.i39, %.pre83.i.i55
  br label %invoke.cont.i.i61

invoke.cont.i.i61:                                ; preds = %_ZNSt16allocator_traitsISaIP6ColumnEE8allocateERS2_m.exit.i.i.i, %if.else.i51
  %sub.ptr.sub.i.i.i.i.i.i.i.i74.pre-phi.i.i57 = phi i64 [ %.pre84.i.i56, %_ZNSt16allocator_traitsISaIP6ColumnEE8allocateERS2_m.exit.i.i.i ], [ %sub.ptr.sub.i30.i.i.i41, %if.else.i51 ]
  %13 = phi %class.Column** [ %.pre.i.i54, %_ZNSt16allocator_traitsISaIP6ColumnEE8allocateERS2_m.exit.i.i.i ], [ %11, %if.else.i51 ]
  %cond.i67.i.i58 = phi %class.Column** [ %12, %_ZNSt16allocator_traitsISaIP6ColumnEE8allocateERS2_m.exit.i.i.i ], [ null, %if.else.i51 ]
  %add.ptr.i.i59 = getelementptr inbounds %class.Column*, %class.Column** %cond.i67.i.i58, i64 %sub.ptr.div.i31.i.i.i42
  store %class.Column* %6, %class.Column** %add.ptr.i.i59, align 8, !tbaa !2
  %tobool.not.i.i.i.i.i.i.i.i75.i.i60 = icmp eq i64 %sub.ptr.sub.i.i.i.i.i.i.i.i74.pre-phi.i.i57, 0
  br i1 %tobool.not.i.i.i.i.i.i.i.i75.i.i60, label %invoke.cont10.i.i69, label %if.then.i.i.i.i.i.i.i.i76.i.i62

if.then.i.i.i.i.i.i.i.i76.i.i62:                  ; preds = %invoke.cont.i.i61
  %14 = bitcast %class.Column** %cond.i67.i.i58 to i8*
  %15 = bitcast %class.Column** %13 to i8*
  tail call void @llvm.memmove.p0i8.p0i8.i64(i8* align 8 %14, i8* align 8 %15, i64 %sub.ptr.sub.i.i.i.i.i.i.i.i74.pre-phi.i.i57, i1 false) #18
  br label %invoke.cont10.i.i69

invoke.cont10.i.i69:                              ; preds = %if.then.i.i.i.i.i.i.i.i76.i.i62, %invoke.cont.i.i61
  %sub.ptr.div.i.i.i.i.i.i.i.i77.i.i63 = ashr exact i64 %sub.ptr.sub.i.i.i.i.i.i.i.i74.pre-phi.i.i57, 3
  %add.ptr.i.i.i.i.i.i.i.i78.i.i64 = getelementptr inbounds %class.Column*, %class.Column** %cond.i67.i.i58, i64 1
  %incdec.ptr.i.i65 = getelementptr inbounds %class.Column*, %class.Column** %add.ptr.i.i.i.i.i.i.i.i78.i.i64, i64 %sub.ptr.div.i.i.i.i.i.i.i.i77.i.i63
  %16 = load %class.Column**, %class.Column*** %_M_finish.i33, align 8, !tbaa !22
  %sub.ptr.lhs.cast.i.i.i.i.i.i.i.i.i.i66 = ptrtoint %class.Column** %16 to i64
  %sub.ptr.sub.i.i.i.i.i.i.i.i.i.i67 = sub i64 %sub.ptr.lhs.cast.i.i.i.i.i.i.i.i.i.i66, %sub.ptr.lhs.cast.i28.i.i.i39
  %tobool.not.i.i.i.i.i.i.i.i.i.i68 = icmp eq i64 %sub.ptr.sub.i.i.i.i.i.i.i.i.i.i67, 0
  br i1 %tobool.not.i.i.i.i.i.i.i.i.i.i68, label %invoke.cont15.i.i72, label %if.then.i.i.i.i.i.i.i.i.i.i70

if.then.i.i.i.i.i.i.i.i.i.i70:                    ; preds = %invoke.cont10.i.i69
  %17 = bitcast %class.Column** %incdec.ptr.i.i65 to i8*
  %18 = bitcast %class.Column** %8 to i8*
  tail call void @llvm.memmove.p0i8.p0i8.i64(i8* nonnull align 8 %17, i8* align 8 %18, i64 %sub.ptr.sub.i.i.i.i.i.i.i.i.i.i67, i1 false) #18
  br label %invoke.cont15.i.i72

invoke.cont15.i.i72:                              ; preds = %if.then.i.i.i.i.i.i.i.i.i.i70, %invoke.cont10.i.i69
  %tobool.not.i68.i.i71 = icmp eq %class.Column** %13, null
  br i1 %tobool.not.i68.i.i71, label %_ZNSt6vectorIP6ColumnSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i, label %if.then.i69.i.i73

if.then.i69.i.i73:                                ; preds = %invoke.cont15.i.i72
  %19 = bitcast %class.Column** %13 to i8*
  tail call void @_ZdlPv(i8* nonnull %19) #18
  br label %_ZNSt6vectorIP6ColumnSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i

_ZNSt6vectorIP6ColumnSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i: ; preds = %if.then.i69.i.i73, %invoke.cont15.i.i72
  %sub.ptr.div.i.i.i.i.i.i.i.i.i.i74 = ashr exact i64 %sub.ptr.sub.i.i.i.i.i.i.i.i.i.i67, 3
  %add.ptr.i.i.i.i.i.i.i.i.i.i75 = getelementptr inbounds %class.Column*, %class.Column** %incdec.ptr.i.i65, i64 %sub.ptr.div.i.i.i.i.i.i.i.i.i.i74
  store %class.Column** %cond.i67.i.i58, %class.Column*** %_M_start.i27.i.i.i38, align 8, !tbaa !26
  store %class.Column** %add.ptr.i.i.i.i.i.i.i.i.i.i75, %class.Column*** %_M_finish.i33, align 8, !tbaa !22
  %add.ptr39.i.i76 = getelementptr inbounds %class.Column*, %class.Column** %cond.i67.i.i58, i64 %cond.i.i.i49
  br label %_ZNSt6vectorIP6ColumnSaIS1_EE9push_backERKS1_.exit

_ZNSt6vectorIP6ColumnSaIS1_EE9push_backERKS1_.exit: ; preds = %if.then.i37, %_ZNSt6vectorIP6ColumnSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i
  %_M_finish.i33.sink = phi %class.Column*** [ %_M_finish.i33, %if.then.i37 ], [ %_M_end_of_storage.i34, %_ZNSt6vectorIP6ColumnSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i ]
  %incdec.ptr.i36.sink = phi %class.Column** [ %incdec.ptr.i36, %if.then.i37 ], [ %add.ptr39.i.i76, %_ZNSt6vectorIP6ColumnSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i ]
  store %class.Column** %incdec.ptr.i36.sink, %class.Column*** %_M_finish.i33.sink, align 8, !tbaa !2
  %indvars.iv.next88 = add nuw nsw i64 %indvars.iv87, 1
  %exitcond90.not = icmp eq i64 %indvars.iv.next88, %wide.trip.count89
  br i1 %exitcond90.not, label %for.body13.lr.ph, label %for.body, !llvm.loop !66

for.body13:                                       ; preds = %for.body13.lr.ph, %_ZNSt6vectorIlSaIlEE9push_backERKl.exit
  %20 = phi i64* [ %.pre91, %for.body13.lr.ph ], [ %31, %_ZNSt6vectorIlSaIlEE9push_backERKl.exit ]
  %21 = phi i64* [ %.pre, %for.body13.lr.ph ], [ %32, %_ZNSt6vectorIlSaIlEE9push_backERKl.exit ]
  %indvars.iv = phi i64 [ 0, %for.body13.lr.ph ], [ %indvars.iv.next, %_ZNSt6vectorIlSaIlEE9push_backERKl.exit ]
  %or.i = or i64 %cond95, %indvars.iv
  %cmp.not.i = icmp eq i64* %21, %20
  br i1 %cmp.not.i, label %if.else.i, label %if.then.i

if.then.i:                                        ; preds = %for.body13
  store i64 %or.i, i64* %21, align 8, !tbaa !47
  %incdec.ptr.i = getelementptr inbounds i64, i64* %21, i64 1
  store i64* %incdec.ptr.i, i64** %_M_finish.i, align 8, !tbaa !63
  br label %_ZNSt6vectorIlSaIlEE9push_backERKl.exit

if.else.i:                                        ; preds = %for.body13
  %22 = load i64*, i64** %_M_start.i27.i.i.i, align 8, !tbaa !44
  %sub.ptr.lhs.cast.i28.i.i.i = ptrtoint i64* %20 to i64
  %sub.ptr.rhs.cast.i29.i.i.i = ptrtoint i64* %22 to i64
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
  br i1 %cmp.not.i.i.i, label %invoke.cont.i.i, label %_ZNSt16allocator_traitsISaIlEE8allocateERS0_m.exit.i.i.i

_ZNSt16allocator_traitsISaIlEE8allocateERS0_m.exit.i.i.i: ; preds = %if.else.i
  %mul.i.i.i.i.i = shl nuw i64 %cond.i.i.i, 3
  %call2.i.i.i.i.i = tail call noalias nonnull i8* @_Znwm(i64 %mul.i.i.i.i.i) #20
  %23 = bitcast i8* %call2.i.i.i.i.i to i64*
  %.pre.i.i = load i64*, i64** %_M_start.i27.i.i.i, align 8, !tbaa !44
  %.pre83.i.i = ptrtoint i64* %.pre.i.i to i64
  %.pre84.i.i = sub i64 %sub.ptr.lhs.cast.i28.i.i.i, %.pre83.i.i
  br label %invoke.cont.i.i

invoke.cont.i.i:                                  ; preds = %_ZNSt16allocator_traitsISaIlEE8allocateERS0_m.exit.i.i.i, %if.else.i
  %sub.ptr.sub.i.i.i.i.i.i.i.i74.pre-phi.i.i = phi i64 [ %.pre84.i.i, %_ZNSt16allocator_traitsISaIlEE8allocateERS0_m.exit.i.i.i ], [ %sub.ptr.sub.i30.i.i.i, %if.else.i ]
  %24 = phi i64* [ %.pre.i.i, %_ZNSt16allocator_traitsISaIlEE8allocateERS0_m.exit.i.i.i ], [ %22, %if.else.i ]
  %cond.i67.i.i = phi i64* [ %23, %_ZNSt16allocator_traitsISaIlEE8allocateERS0_m.exit.i.i.i ], [ null, %if.else.i ]
  %add.ptr.i.i = getelementptr inbounds i64, i64* %cond.i67.i.i, i64 %sub.ptr.div.i31.i.i.i
  store i64 %or.i, i64* %add.ptr.i.i, align 8, !tbaa !47
  %tobool.not.i.i.i.i.i.i.i.i75.i.i = icmp eq i64 %sub.ptr.sub.i.i.i.i.i.i.i.i74.pre-phi.i.i, 0
  br i1 %tobool.not.i.i.i.i.i.i.i.i75.i.i, label %invoke.cont10.i.i, label %if.then.i.i.i.i.i.i.i.i76.i.i

if.then.i.i.i.i.i.i.i.i76.i.i:                    ; preds = %invoke.cont.i.i
  %25 = bitcast i64* %cond.i67.i.i to i8*
  %26 = bitcast i64* %24 to i8*
  tail call void @llvm.memmove.p0i8.p0i8.i64(i8* align 8 %25, i8* align 8 %26, i64 %sub.ptr.sub.i.i.i.i.i.i.i.i74.pre-phi.i.i, i1 false) #18
  br label %invoke.cont10.i.i

invoke.cont10.i.i:                                ; preds = %if.then.i.i.i.i.i.i.i.i76.i.i, %invoke.cont.i.i
  %sub.ptr.div.i.i.i.i.i.i.i.i77.i.i = ashr exact i64 %sub.ptr.sub.i.i.i.i.i.i.i.i74.pre-phi.i.i, 3
  %add.ptr.i.i.i.i.i.i.i.i78.i.i = getelementptr inbounds i64, i64* %cond.i67.i.i, i64 1
  %incdec.ptr.i.i = getelementptr inbounds i64, i64* %add.ptr.i.i.i.i.i.i.i.i78.i.i, i64 %sub.ptr.div.i.i.i.i.i.i.i.i77.i.i
  %27 = load i64*, i64** %_M_finish.i, align 8, !tbaa !63
  %sub.ptr.lhs.cast.i.i.i.i.i.i.i.i.i.i = ptrtoint i64* %27 to i64
  %sub.ptr.sub.i.i.i.i.i.i.i.i.i.i = sub i64 %sub.ptr.lhs.cast.i.i.i.i.i.i.i.i.i.i, %sub.ptr.lhs.cast.i28.i.i.i
  %tobool.not.i.i.i.i.i.i.i.i.i.i = icmp eq i64 %sub.ptr.sub.i.i.i.i.i.i.i.i.i.i, 0
  br i1 %tobool.not.i.i.i.i.i.i.i.i.i.i, label %invoke.cont15.i.i, label %if.then.i.i.i.i.i.i.i.i.i.i

if.then.i.i.i.i.i.i.i.i.i.i:                      ; preds = %invoke.cont10.i.i
  %28 = bitcast i64* %incdec.ptr.i.i to i8*
  %29 = bitcast i64* %20 to i8*
  tail call void @llvm.memmove.p0i8.p0i8.i64(i8* nonnull align 8 %28, i8* align 8 %29, i64 %sub.ptr.sub.i.i.i.i.i.i.i.i.i.i, i1 false) #18
  br label %invoke.cont15.i.i

invoke.cont15.i.i:                                ; preds = %if.then.i.i.i.i.i.i.i.i.i.i, %invoke.cont10.i.i
  %tobool.not.i68.i.i = icmp eq i64* %24, null
  br i1 %tobool.not.i68.i.i, label %_ZNSt6vectorIlSaIlEE17_M_realloc_insertIJRKlEEEvN9__gnu_cxx17__normal_iteratorIPlS1_EEDpOT_.exit.i, label %if.then.i69.i.i

if.then.i69.i.i:                                  ; preds = %invoke.cont15.i.i
  %30 = bitcast i64* %24 to i8*
  tail call void @_ZdlPv(i8* nonnull %30) #18
  br label %_ZNSt6vectorIlSaIlEE17_M_realloc_insertIJRKlEEEvN9__gnu_cxx17__normal_iteratorIPlS1_EEDpOT_.exit.i

_ZNSt6vectorIlSaIlEE17_M_realloc_insertIJRKlEEEvN9__gnu_cxx17__normal_iteratorIPlS1_EEDpOT_.exit.i: ; preds = %if.then.i69.i.i, %invoke.cont15.i.i
  %sub.ptr.div.i.i.i.i.i.i.i.i.i.i = ashr exact i64 %sub.ptr.sub.i.i.i.i.i.i.i.i.i.i, 3
  %add.ptr.i.i.i.i.i.i.i.i.i.i = getelementptr inbounds i64, i64* %incdec.ptr.i.i, i64 %sub.ptr.div.i.i.i.i.i.i.i.i.i.i
  store i64* %cond.i67.i.i, i64** %_M_start.i27.i.i.i, align 8, !tbaa !44
  store i64* %add.ptr.i.i.i.i.i.i.i.i.i.i, i64** %_M_finish.i, align 8, !tbaa !63
  %add.ptr39.i.i = getelementptr inbounds i64, i64* %cond.i67.i.i, i64 %cond.i.i.i
  store i64* %add.ptr39.i.i, i64** %_M_end_of_storage.i, align 8, !tbaa !61
  br label %_ZNSt6vectorIlSaIlEE9push_backERKl.exit

_ZNSt6vectorIlSaIlEE9push_backERKl.exit:          ; preds = %if.then.i, %_ZNSt6vectorIlSaIlEE17_M_realloc_insertIJRKlEEEvN9__gnu_cxx17__normal_iteratorIPlS1_EEDpOT_.exit.i
  %31 = phi i64* [ %20, %if.then.i ], [ %add.ptr39.i.i, %_ZNSt6vectorIlSaIlEE17_M_realloc_insertIJRKlEEEvN9__gnu_cxx17__normal_iteratorIPlS1_EEDpOT_.exit.i ]
  %32 = phi i64* [ %incdec.ptr.i, %if.then.i ], [ %add.ptr.i.i.i.i.i.i.i.i.i.i, %_ZNSt6vectorIlSaIlEE17_M_realloc_insertIJRKlEEEvN9__gnu_cxx17__normal_iteratorIPlS1_EEDpOT_.exit.i ]
  %indvars.iv.next = add nuw nsw i64 %indvars.iv, 1
  %exitcond.not = icmp eq i64 %indvars.iv.next, %wide.trip.count
  br i1 %exitcond.not, label %return, label %for.body13, !llvm.loop !67

return:                                           ; preds = %_ZNSt6vectorIlSaIlEE9push_backERKl.exit, %entry
  ret void
}

; Function Attrs: norecurse nounwind readonly uwtable willreturn mustprogress
define dso_local i32 @_Z11compareNullPijS_ji(i32* nocapture readonly %leftNulls, i32 %leftPosition, i32* nocapture readonly %rightNulls, i32 %rightPosition, i32 %nullsFirst) local_unnamed_addr #11 {
entry:
  %idxprom = zext i32 %leftPosition to i64
  %arrayidx = getelementptr inbounds i32, i32* %leftNulls, i64 %idxprom
  %0 = load i32, i32* %arrayidx, align 4, !tbaa !6
  %idxprom1 = zext i32 %rightPosition to i64
  %arrayidx2 = getelementptr inbounds i32, i32* %rightNulls, i64 %idxprom1
  %1 = load i32, i32* %arrayidx2, align 4, !tbaa !6
  %tobool = icmp ne i32 %0, 0
  %tobool3 = icmp ne i32 %1, 0
  %or.cond = and i1 %tobool, %tobool3
  br i1 %or.cond, label %cleanup, label %if.end

if.end:                                           ; preds = %entry
  br i1 %tobool, label %if.then5, label %if.end7

if.then5:                                         ; preds = %if.end
  %tobool6.not = icmp eq i32 %nullsFirst, 0
  %cond = select i1 %tobool6.not, i32 1, i32 -1
  br label %cleanup

if.end7:                                          ; preds = %if.end
  br i1 %tobool3, label %if.then9, label %cleanup

if.then9:                                         ; preds = %if.end7
  %tobool10.not = icmp eq i32 %nullsFirst, 0
  %cond11 = select i1 %tobool10.not, i32 1, i32 -1
  br label %cleanup

cleanup:                                          ; preds = %if.end7, %entry, %if.then9, %if.then5
  %retval.0 = phi i32 [ %cond, %if.then5 ], [ %cond11, %if.then9 ], [ 0, %entry ], [ 2, %if.end7 ]
  ret i32 %retval.0
}

; Function Attrs: norecurse nounwind readonly uwtable willreturn mustprogress
define dso_local i32 @_Z15compareIntValuePijS_j(i32* nocapture readonly %leftData, i32 %leftPosition, i32* nocapture readonly %rightData, i32 %rightPosition) local_unnamed_addr #11 {
entry:
  %idxprom = zext i32 %leftPosition to i64
  %arrayidx = getelementptr inbounds i32, i32* %leftData, i64 %idxprom
  %0 = load i32, i32* %arrayidx, align 4, !tbaa !6
  %idxprom1 = zext i32 %rightPosition to i64
  %arrayidx2 = getelementptr inbounds i32, i32* %rightData, i64 %idxprom1
  %1 = load i32, i32* %arrayidx2, align 4, !tbaa !6
  %sub = sub nsw i32 %0, %1
  ret i32 %sub
}

; Function Attrs: norecurse nounwind readonly uwtable willreturn mustprogress
define dso_local i32 @_Z17compareInt64ValuePljS_j(i64* nocapture readonly %leftData, i32 %leftPosition, i64* nocapture readonly %rightData, i32 %rightPosition) local_unnamed_addr #11 {
entry:
  %idxprom = zext i32 %leftPosition to i64
  %arrayidx = getelementptr inbounds i64, i64* %leftData, i64 %idxprom
  %0 = load i64, i64* %arrayidx, align 8, !tbaa !47
  %idxprom1 = zext i32 %rightPosition to i64
  %arrayidx2 = getelementptr inbounds i64, i64* %rightData, i64 %idxprom1
  %1 = load i64, i64* %arrayidx2, align 8, !tbaa !47
  %cmp = icmp sgt i64 %0, %1
  br i1 %cmp, label %cleanup, label %if.else

if.else:                                          ; preds = %entry
  %cmp3 = icmp slt i64 %0, %1
  %spec.select = sext i1 %cmp3 to i32
  ret i32 %spec.select

cleanup:                                          ; preds = %entry
  ret i32 1
}

; Function Attrs: norecurse nounwind readonly uwtable willreturn mustprogress
define dso_local i32 @_Z18compareDoubleValuePdjS_j(double* nocapture readonly %leftData, i32 %leftPosition, double* nocapture readonly %rightData, i32 %rightPosition) local_unnamed_addr #11 {
entry:
  %idxprom = zext i32 %leftPosition to i64
  %arrayidx = getelementptr inbounds double, double* %leftData, i64 %idxprom
  %0 = load double, double* %arrayidx, align 8, !tbaa !68
  %idxprom1 = zext i32 %rightPosition to i64
  %arrayidx2 = getelementptr inbounds double, double* %rightData, i64 %idxprom1
  %1 = load double, double* %arrayidx2, align 8, !tbaa !68
  %cmp = fcmp ogt double %0, %1
  br i1 %cmp, label %cleanup, label %if.else

if.else:                                          ; preds = %entry
  %cmp3 = fcmp olt double %0, %1
  br i1 %cmp3, label %cleanup, label %if.else5

if.else5:                                         ; preds = %if.else
  br label %cleanup

cleanup:                                          ; preds = %if.else, %entry, %if.else5
  %retval.0 = phi i32 [ 0, %if.else5 ], [ 1, %entry ], [ -1, %if.else ]
  ret i32 %retval.0
}

; Function Attrs: norecurse nounwind readonly uwtable willreturn mustprogress
define dso_local i32 @_Z9compareTolPiS_S_S_ijj(i64 %pagesIndexAddr, i32* nocapture readonly %sortCols, i32* nocapture readonly %sortColTypes, i32* nocapture readonly %sortAscendings, i32* nocapture readnone %sortNullFirsts, i32 %sortColCount, i32 %leftPosition, i32 %rightPosition) local_unnamed_addr #11 {
entry:
  %0 = inttoptr i64 %pagesIndexAddr to %class.PagesIndex*
  %conv = zext i32 %leftPosition to i64
  %_M_start.i129 = getelementptr inbounds %class.PagesIndex, %class.PagesIndex* %0, i64 0, i32 3, i32 0, i32 0, i32 0
  %1 = load i64*, i64** %_M_start.i129, align 8, !tbaa !44
  %add.ptr.i130 = getelementptr inbounds i64, i64* %1, i64 %conv
  %2 = load i64, i64* %add.ptr.i130, align 8, !tbaa !47
  %shr.i127 = lshr i64 %2, 32
  %conv5 = zext i32 %rightPosition to i64
  %add.ptr.i125 = getelementptr inbounds i64, i64* %1, i64 %conv5
  %3 = load i64, i64* %add.ptr.i125, align 8, !tbaa !47
  %shr.i = lshr i64 %3, 32
  %cmp9131 = icmp sgt i32 %sortColCount, 0
  br i1 %cmp9131, label %for.body.lr.ph, label %cleanup46

for.body.lr.ph:                                   ; preds = %entry
  %conv.i128 = trunc i64 %shr.i127 to i32
  %conv.i123 = trunc i64 %shr.i to i32
  %cmp = icmp eq i32 %conv.i128, %conv.i123
  %_M_start.i121 = getelementptr inbounds %class.PagesIndex, %class.PagesIndex* %0, i64 0, i32 4, i32 0, i32 0, i32 0
  %4 = load %"class.std::vector"*, %"class.std::vector"** %_M_start.i121, align 8, !tbaa !39
  %idxprom.i = and i64 %2, 4294967295
  %idxprom1.i = and i64 %3, 4294967295
  %wide.trip.count = zext i32 %sortColCount to i64
  br i1 %cmp, label %for.body.us, label %for.body

for.body.us:                                      ; preds = %for.body.lr.ph, %for.cond.us
  %indvars.iv = phi i64 [ %indvars.iv.next, %for.cond.us ], [ 0, %for.body.lr.ph ]
  %arrayidx.us = getelementptr inbounds i32, i32* %sortCols, i64 %indvars.iv
  %5 = load i32, i32* %arrayidx.us, align 4, !tbaa !6
  %conv10.us = sext i32 %5 to i64
  %_M_start.i119.us = getelementptr inbounds %"class.std::vector", %"class.std::vector"* %4, i64 %conv10.us, i32 0, i32 0, i32 0
  %6 = load %class.Column**, %class.Column*** %_M_start.i119.us, align 8, !tbaa !26
  %add.ptr.i120.us = getelementptr inbounds %class.Column*, %class.Column** %6, i64 %shr.i127
  %7 = load %class.Column*, %class.Column** %add.ptr.i120.us, align 8, !tbaa !2
  %data.i118.us = getelementptr inbounds %class.Column, %class.Column* %7, i64 0, i32 1
  %8 = load i8*, i8** %data.i118.us, align 8, !tbaa !10
  %arrayidx17.us = getelementptr inbounds i32, i32* %sortColTypes, i64 %indvars.iv
  %9 = load i32, i32* %arrayidx17.us, align 4, !tbaa !6
  switch i32 %9, label %sw.epilog.us [
    i32 1, label %sw.bb.us
    i32 2, label %sw.bb27.us
    i32 3, label %sw.bb29.us
  ]

for.cond.us:                                      ; preds = %sw.epilog.us
  %exitcond.not = icmp eq i64 %indvars.iv.next, %wide.trip.count
  br i1 %exitcond.not, label %cleanup46, label %for.body.us, !llvm.loop !70

sw.bb29.us:                                       ; preds = %for.body.us
  %10 = bitcast i8* %8 to double*
  %arrayidx.i.us = getelementptr inbounds double, double* %10, i64 %idxprom.i
  %11 = load double, double* %arrayidx.i.us, align 8, !tbaa !68
  %arrayidx2.i.us = getelementptr inbounds double, double* %10, i64 %idxprom1.i
  %12 = load double, double* %arrayidx2.i.us, align 8, !tbaa !68
  %cmp.i.us = fcmp ogt double %11, %12
  br i1 %cmp.i.us, label %sw.epilog.us, label %if.else.i.us

if.else.i.us:                                     ; preds = %sw.bb29.us
  %cmp3.i.us = fcmp olt double %11, %12
  br i1 %cmp3.i.us, label %sw.epilog.us, label %if.else5.i.us

if.else5.i.us:                                    ; preds = %if.else.i.us
  br label %sw.epilog.us

sw.bb27.us:                                       ; preds = %for.body.us
  %13 = bitcast i8* %8 to i64*
  %arrayidx.i104.us = getelementptr inbounds i64, i64* %13, i64 %idxprom.i
  %14 = load i64, i64* %arrayidx.i104.us, align 8, !tbaa !47
  %arrayidx2.i106.us = getelementptr inbounds i64, i64* %13, i64 %idxprom1.i
  %15 = load i64, i64* %arrayidx2.i106.us, align 8, !tbaa !47
  %cmp.i107.us = icmp sgt i64 %14, %15
  br i1 %cmp.i107.us, label %sw.epilog.us, label %if.else.i109.us

if.else.i109.us:                                  ; preds = %sw.bb27.us
  %cmp3.i108.us = icmp slt i64 %14, %15
  %spec.select.i.us = sext i1 %cmp3.i108.us to i32
  br label %sw.epilog.us

sw.bb.us:                                         ; preds = %for.body.us
  %16 = bitcast i8* %8 to i32*
  %arrayidx.i112.us = getelementptr inbounds i32, i32* %16, i64 %idxprom.i
  %17 = load i32, i32* %arrayidx.i112.us, align 4, !tbaa !6
  %arrayidx2.i114.us = getelementptr inbounds i32, i32* %16, i64 %idxprom1.i
  %18 = load i32, i32* %arrayidx2.i114.us, align 4, !tbaa !6
  %sub.i.us = sub nsw i32 %17, %18
  br label %sw.epilog.us

sw.epilog.us:                                     ; preds = %sw.bb.us, %if.else.i109.us, %sw.bb27.us, %if.else5.i.us, %if.else.i.us, %sw.bb29.us, %for.body.us
  %compare.1.us = phi i32 [ 0, %for.body.us ], [ %sub.i.us, %sw.bb.us ], [ %spec.select.i.us, %if.else.i109.us ], [ 1, %sw.bb27.us ], [ 0, %if.else5.i.us ], [ 1, %sw.bb29.us ], [ -1, %if.else.i.us ]
  %arrayidx32.us = getelementptr inbounds i32, i32* %sortAscendings, i64 %indvars.iv
  %19 = load i32, i32* %arrayidx32.us, align 4, !tbaa !6
  %cmp33.us = icmp eq i32 %19, 0
  %sub.us = sub nsw i32 0, %compare.1.us
  %spec.select.us = select i1 %cmp33.us, i32 %sub.us, i32 %compare.1.us
  %cmp36.not.us = icmp eq i32 %spec.select.us, 0
  %indvars.iv.next = add nuw nsw i64 %indvars.iv, 1
  br i1 %cmp36.not.us, label %for.cond.us, label %cleanup46

for.cond:                                         ; preds = %sw.epilog
  %exitcond139.not = icmp eq i64 %indvars.iv.next137, %wide.trip.count
  br i1 %exitcond139.not, label %cleanup46, label %for.body, !llvm.loop !70

for.body:                                         ; preds = %for.body.lr.ph, %for.cond
  %indvars.iv136 = phi i64 [ %indvars.iv.next137, %for.cond ], [ 0, %for.body.lr.ph ]
  %arrayidx = getelementptr inbounds i32, i32* %sortCols, i64 %indvars.iv136
  %20 = load i32, i32* %arrayidx, align 4, !tbaa !6
  %conv10 = sext i32 %20 to i64
  %_M_start.i119 = getelementptr inbounds %"class.std::vector", %"class.std::vector"* %4, i64 %conv10, i32 0, i32 0, i32 0
  %21 = load %class.Column**, %class.Column*** %_M_start.i119, align 8, !tbaa !26
  %add.ptr.i120 = getelementptr inbounds %class.Column*, %class.Column** %21, i64 %shr.i127
  %22 = load %class.Column*, %class.Column** %add.ptr.i120, align 8, !tbaa !2
  %data.i118 = getelementptr inbounds %class.Column, %class.Column* %22, i64 0, i32 1
  %23 = load i8*, i8** %data.i118, align 8, !tbaa !10
  %arrayidx17 = getelementptr inbounds i32, i32* %sortColTypes, i64 %indvars.iv136
  %24 = load i32, i32* %arrayidx17, align 4, !tbaa !6
  %add.ptr.i = getelementptr inbounds %class.Column*, %class.Column** %21, i64 %shr.i
  %25 = load %class.Column*, %class.Column** %add.ptr.i, align 8, !tbaa !2
  %data.i = getelementptr inbounds %class.Column, %class.Column* %25, i64 0, i32 1
  %26 = load i8*, i8** %data.i, align 8, !tbaa !10
  switch i32 %24, label %sw.epilog [
    i32 1, label %sw.bb
    i32 2, label %sw.bb27
    i32 3, label %sw.bb29
  ]

sw.bb:                                            ; preds = %for.body
  %27 = bitcast i8* %23 to i32*
  %28 = bitcast i8* %26 to i32*
  %arrayidx.i112 = getelementptr inbounds i32, i32* %27, i64 %idxprom.i
  %29 = load i32, i32* %arrayidx.i112, align 4, !tbaa !6
  %arrayidx2.i114 = getelementptr inbounds i32, i32* %28, i64 %idxprom1.i
  %30 = load i32, i32* %arrayidx2.i114, align 4, !tbaa !6
  %sub.i = sub nsw i32 %29, %30
  br label %sw.epilog

sw.bb27:                                          ; preds = %for.body
  %31 = bitcast i8* %23 to i64*
  %32 = bitcast i8* %26 to i64*
  %arrayidx.i104 = getelementptr inbounds i64, i64* %31, i64 %idxprom.i
  %33 = load i64, i64* %arrayidx.i104, align 8, !tbaa !47
  %arrayidx2.i106 = getelementptr inbounds i64, i64* %32, i64 %idxprom1.i
  %34 = load i64, i64* %arrayidx2.i106, align 8, !tbaa !47
  %cmp.i107 = icmp sgt i64 %33, %34
  br i1 %cmp.i107, label %sw.epilog, label %if.else.i109

if.else.i109:                                     ; preds = %sw.bb27
  %cmp3.i108 = icmp slt i64 %33, %34
  %spec.select.i = sext i1 %cmp3.i108 to i32
  br label %sw.epilog

sw.bb29:                                          ; preds = %for.body
  %35 = bitcast i8* %23 to double*
  %36 = bitcast i8* %26 to double*
  %arrayidx.i = getelementptr inbounds double, double* %35, i64 %idxprom.i
  %37 = load double, double* %arrayidx.i, align 8, !tbaa !68
  %arrayidx2.i = getelementptr inbounds double, double* %36, i64 %idxprom1.i
  %38 = load double, double* %arrayidx2.i, align 8, !tbaa !68
  %cmp.i = fcmp ogt double %37, %38
  br i1 %cmp.i, label %sw.epilog, label %if.else.i

if.else.i:                                        ; preds = %sw.bb29
  %cmp3.i = fcmp olt double %37, %38
  br i1 %cmp3.i, label %sw.epilog, label %if.else5.i

if.else5.i:                                       ; preds = %if.else.i
  br label %sw.epilog

sw.epilog:                                        ; preds = %if.else5.i, %if.else.i, %sw.bb29, %if.else.i109, %sw.bb27, %for.body, %sw.bb
  %compare.1 = phi i32 [ 0, %for.body ], [ %sub.i, %sw.bb ], [ %spec.select.i, %if.else.i109 ], [ 1, %sw.bb27 ], [ 0, %if.else5.i ], [ 1, %sw.bb29 ], [ -1, %if.else.i ]
  %arrayidx32 = getelementptr inbounds i32, i32* %sortAscendings, i64 %indvars.iv136
  %39 = load i32, i32* %arrayidx32, align 4, !tbaa !6
  %cmp33 = icmp eq i32 %39, 0
  %sub = sub nsw i32 0, %compare.1
  %spec.select = select i1 %cmp33, i32 %sub, i32 %compare.1
  %cmp36.not = icmp eq i32 %spec.select, 0
  %indvars.iv.next137 = add nuw nsw i64 %indvars.iv136, 1
  br i1 %cmp36.not, label %for.cond, label %cleanup46

cleanup46:                                        ; preds = %sw.epilog, %for.cond, %sw.epilog.us, %for.cond.us, %entry
  %compare.3 = phi i32 [ 0, %entry ], [ 0, %for.cond.us ], [ %spec.select.us, %sw.epilog.us ], [ 0, %for.cond ], [ %spec.select, %sw.epilog ]
  ret i32 %compare.3
}

; Function Attrs: norecurse nounwind readonly uwtable willreturn mustprogress
define dso_local i32 @_Z7median3lPiS_S_S_ijjj(i64 %pagesIndexAddr, i32* nocapture readonly %sortCols, i32* nocapture readonly %sortColTypes, i32* nocapture readonly %sortAscendings, i32* nocapture readnone %sortNullFirsts, i32 %sortColCount, i32 %a, i32 %b, i32 %c) local_unnamed_addr #11 {
entry:
  %call = tail call i32 @_Z9compareTolPiS_S_S_ijj(i64 %pagesIndexAddr, i32* %sortCols, i32* %sortColTypes, i32* %sortAscendings, i32* undef, i32 %sortColCount, i32 %a, i32 %b)
  %call1 = tail call i32 @_Z9compareTolPiS_S_S_ijj(i64 %pagesIndexAddr, i32* %sortCols, i32* %sortColTypes, i32* %sortAscendings, i32* undef, i32 %sortColCount, i32 %a, i32 %c)
  %call2 = tail call i32 @_Z9compareTolPiS_S_S_ijj(i64 %pagesIndexAddr, i32* %sortCols, i32* %sortColTypes, i32* %sortAscendings, i32* undef, i32 %sortColCount, i32 %b, i32 %c)
  %cmp = icmp slt i32 %call, 0
  br i1 %cmp, label %cond.true, label %cond.false10

cond.true:                                        ; preds = %entry
  %cmp3 = icmp slt i32 %call2, 0
  br i1 %cmp3, label %cond.end21, label %cond.false

cond.false:                                       ; preds = %cond.true
  %cmp5 = icmp slt i32 %call1, 0
  %cond = select i1 %cmp5, i32 %c, i32 %a
  br label %cond.end21

cond.false10:                                     ; preds = %entry
  %cmp11 = icmp sgt i32 %call2, 0
  br i1 %cmp11, label %cond.end21, label %cond.false13

cond.false13:                                     ; preds = %cond.false10
  %cmp14 = icmp sgt i32 %call1, 0
  %cond18 = select i1 %cmp14, i32 %c, i32 %a
  br label %cond.end21

cond.end21:                                       ; preds = %cond.false10, %cond.true, %cond.false13, %cond.false
  %cond22 = phi i32 [ %cond, %cond.false ], [ %cond18, %cond.false13 ], [ %b, %cond.true ], [ %b, %cond.false10 ]
  ret i32 %cond22
}

; Function Attrs: nofree nounwind uwtable
define dso_local void @_Z9quickSortlPiS_S_S_ijj(i64 %pagesIndexAddr, i32* readonly %sortCols, i32* readonly %sortColTypes, i32* readonly %sortAscendings, i32* readnone %sortNullFirsts, i32 %sortColCount, i32 %from, i32 %to) local_unnamed_addr #12 {
entry:
  %0 = inttoptr i64 %pagesIndexAddr to %class.PagesIndex*
  %valueAddresses.i = getelementptr inbounds %class.PagesIndex, %class.PagesIndex* %0, i64 0, i32 3
  %sub409 = sub i32 %to, %from
  %cmp410 = icmp ult i32 %sub409, 7
  br i1 %cmp410, label %for.cond.preheader, label %if.end.lr.ph

if.end.lr.ph:                                     ; preds = %entry
  %sub15 = add i32 %to, -1
  %_M_start.i357 = getelementptr inbounds %"class.std::vector.0", %"class.std::vector.0"* %valueAddresses.i, i64 0, i32 0, i32 0, i32 0
  %1 = add i32 %to, -1
  br label %if.end

for.cond.preheader:                               ; preds = %if.then110, %entry
  %from.tr.lcssa = phi i32 [ %from, %entry ], [ %sub111, %if.then110 ]
  %cmp1383 = icmp ult i32 %from.tr.lcssa, %to
  br i1 %cmp1383, label %for.cond2.preheader.lr.ph, label %cleanup

for.cond2.preheader.lr.ph:                        ; preds = %for.cond.preheader
  %_M_start.i = getelementptr inbounds %"class.std::vector.0", %"class.std::vector.0"* %valueAddresses.i, i64 0, i32 0, i32 0, i32 0
  %2 = zext i32 %from.tr.lcssa to i64
  br label %for.cond2.preheader

for.cond2.preheader:                              ; preds = %for.cond2.preheader.lr.ph, %for.cond.cleanup7
  %indvars.iv = phi i64 [ %2, %for.cond2.preheader.lr.ph ], [ %indvars.iv.next, %for.cond.cleanup7 ]
  %i.0384 = phi i32 [ %from.tr.lcssa, %for.cond2.preheader.lr.ph ], [ %inc, %for.cond.cleanup7 ]
  %cmp3381 = icmp ugt i32 %i.0384, %from.tr.lcssa
  br i1 %cmp3381, label %land.rhs, label %for.cond.cleanup7

land.rhs:                                         ; preds = %for.cond2.preheader, %for.body8
  %indvars.iv427 = phi i64 [ %indvars.iv.next428, %for.body8 ], [ %indvars.iv, %for.cond2.preheader ]
  %3 = trunc i64 %indvars.iv427 to i32
  %sub4 = add i32 %3, -1
  %call5 = tail call i32 @_Z9compareTolPiS_S_S_ijj(i64 %pagesIndexAddr, i32* %sortCols, i32* %sortColTypes, i32* %sortAscendings, i32* undef, i32 %sortColCount, i32 %sub4, i32 %3)
  %cmp6 = icmp sgt i32 %call5, 0
  br i1 %cmp6, label %for.body8, label %for.cond.cleanup7

for.cond.cleanup7:                                ; preds = %land.rhs, %for.body8, %for.cond2.preheader
  %inc = add nuw i32 %i.0384, 1
  %indvars.iv.next = add nuw nsw i64 %indvars.iv, 1
  %exitcond.not = icmp eq i32 %inc, %to
  br i1 %exitcond.not, label %cleanup, label %for.cond2.preheader, !llvm.loop !71

for.body8:                                        ; preds = %land.rhs
  %4 = load i64*, i64** %_M_start.i, align 8, !tbaa !44
  %arrayidx.i = getelementptr inbounds i64, i64* %4, i64 %indvars.iv427
  %5 = load i64, i64* %arrayidx.i, align 8, !tbaa !47
  %idxprom1.i = zext i32 %sub4 to i64
  %arrayidx2.i = getelementptr inbounds i64, i64* %4, i64 %idxprom1.i
  %6 = load i64, i64* %arrayidx2.i, align 8, !tbaa !47
  store i64 %6, i64* %arrayidx.i, align 8, !tbaa !47
  store i64 %5, i64* %arrayidx2.i, align 8, !tbaa !47
  %cmp3 = icmp ugt i32 %sub4, %from.tr.lcssa
  %indvars.iv.next428 = add nsw i64 %indvars.iv427, -1
  br i1 %cmp3, label %land.rhs, label %for.cond.cleanup7, !llvm.loop !72

if.end:                                           ; preds = %if.end.lr.ph, %if.then110
  %sub413 = phi i32 [ %sub409, %if.end.lr.ph ], [ %sub96, %if.then110 ]
  %from.tr411 = phi i32 [ %from, %if.end.lr.ph ], [ %sub111, %if.then110 ]
  %div = lshr i32 %sub413, 1
  %add = add i32 %div, %from.tr411
  %cmp13.not = icmp eq i32 %sub413, 7
  br i1 %cmp13.not, label %while.cond.preheader, label %if.then14

if.then14:                                        ; preds = %if.end
  %cmp16 = icmp ugt i32 %sub413, 40
  br i1 %cmp16, label %if.then17, label %if.end29

if.then17:                                        ; preds = %if.then14
  %div18 = lshr i32 %sub413, 3
  %add19 = add i32 %div18, %from.tr411
  %mul = shl nuw nsw i32 %div18, 1
  %add20 = add i32 %mul, %from.tr411
  %call.i = tail call i32 @_Z9compareTolPiS_S_S_ijj(i64 %pagesIndexAddr, i32* %sortCols, i32* %sortColTypes, i32* %sortAscendings, i32* undef, i32 %sortColCount, i32 %from.tr411, i32 %add19) #18
  %call1.i = tail call i32 @_Z9compareTolPiS_S_S_ijj(i64 %pagesIndexAddr, i32* %sortCols, i32* %sortColTypes, i32* %sortAscendings, i32* undef, i32 %sortColCount, i32 %from.tr411, i32 %add20) #18
  %call2.i = tail call i32 @_Z9compareTolPiS_S_S_ijj(i64 %pagesIndexAddr, i32* %sortCols, i32* %sortColTypes, i32* %sortAscendings, i32* undef, i32 %sortColCount, i32 %add19, i32 %add20) #18
  %cmp.i308 = icmp slt i32 %call.i, 0
  br i1 %cmp.i308, label %cond.true.i, label %cond.false10.i

cond.true.i:                                      ; preds = %if.then17
  %cmp3.i = icmp slt i32 %call2.i, 0
  br i1 %cmp3.i, label %_Z7median3lPiS_S_S_ijjj.exit, label %cond.false.i

cond.false.i:                                     ; preds = %cond.true.i
  %cmp5.i = icmp slt i32 %call1.i, 0
  %cond.i = select i1 %cmp5.i, i32 %add20, i32 %from.tr411
  br label %_Z7median3lPiS_S_S_ijjj.exit

cond.false10.i:                                   ; preds = %if.then17
  %cmp11.i = icmp sgt i32 %call2.i, 0
  br i1 %cmp11.i, label %_Z7median3lPiS_S_S_ijjj.exit, label %cond.false13.i

cond.false13.i:                                   ; preds = %cond.false10.i
  %cmp14.i = icmp sgt i32 %call1.i, 0
  %cond18.i = select i1 %cmp14.i, i32 %add20, i32 %from.tr411
  br label %_Z7median3lPiS_S_S_ijjj.exit

_Z7median3lPiS_S_S_ijjj.exit:                     ; preds = %cond.true.i, %cond.false.i, %cond.false10.i, %cond.false13.i
  %cond22.i = phi i32 [ %cond.i, %cond.false.i ], [ %cond18.i, %cond.false13.i ], [ %add19, %cond.true.i ], [ %add19, %cond.false10.i ]
  %sub22 = sub i32 %add, %div18
  %add23 = add i32 %add, %div18
  %call.i309 = tail call i32 @_Z9compareTolPiS_S_S_ijj(i64 %pagesIndexAddr, i32* %sortCols, i32* %sortColTypes, i32* %sortAscendings, i32* undef, i32 %sortColCount, i32 %sub22, i32 %add) #18
  %call1.i310 = tail call i32 @_Z9compareTolPiS_S_S_ijj(i64 %pagesIndexAddr, i32* %sortCols, i32* %sortColTypes, i32* %sortAscendings, i32* undef, i32 %sortColCount, i32 %sub22, i32 %add23) #18
  %call2.i311 = tail call i32 @_Z9compareTolPiS_S_S_ijj(i64 %pagesIndexAddr, i32* %sortCols, i32* %sortColTypes, i32* %sortAscendings, i32* undef, i32 %sortColCount, i32 %add, i32 %add23) #18
  %cmp.i312 = icmp slt i32 %call.i309, 0
  br i1 %cmp.i312, label %cond.true.i314, label %cond.false10.i319

cond.true.i314:                                   ; preds = %_Z7median3lPiS_S_S_ijjj.exit
  %cmp3.i313 = icmp slt i32 %call2.i311, 0
  br i1 %cmp3.i313, label %_Z7median3lPiS_S_S_ijjj.exit324, label %cond.false.i317

cond.false.i317:                                  ; preds = %cond.true.i314
  %cmp5.i315 = icmp slt i32 %call1.i310, 0
  %cond.i316 = select i1 %cmp5.i315, i32 %add23, i32 %sub22
  br label %_Z7median3lPiS_S_S_ijjj.exit324

cond.false10.i319:                                ; preds = %_Z7median3lPiS_S_S_ijjj.exit
  %cmp11.i318 = icmp sgt i32 %call2.i311, 0
  br i1 %cmp11.i318, label %_Z7median3lPiS_S_S_ijjj.exit324, label %cond.false13.i322

cond.false13.i322:                                ; preds = %cond.false10.i319
  %cmp14.i320 = icmp sgt i32 %call1.i310, 0
  %cond18.i321 = select i1 %cmp14.i320, i32 %add23, i32 %sub22
  br label %_Z7median3lPiS_S_S_ijjj.exit324

_Z7median3lPiS_S_S_ijjj.exit324:                  ; preds = %cond.true.i314, %cond.false.i317, %cond.false10.i319, %cond.false13.i322
  %cond22.i323 = phi i32 [ %cond.i316, %cond.false.i317 ], [ %cond18.i321, %cond.false13.i322 ], [ %add, %cond.true.i314 ], [ %add, %cond.false10.i319 ]
  %sub26 = sub i32 %sub15, %mul
  %sub27 = sub i32 %sub15, %div18
  %call.i325 = tail call i32 @_Z9compareTolPiS_S_S_ijj(i64 %pagesIndexAddr, i32* %sortCols, i32* %sortColTypes, i32* %sortAscendings, i32* undef, i32 %sortColCount, i32 %sub26, i32 %sub27) #18
  %call1.i326 = tail call i32 @_Z9compareTolPiS_S_S_ijj(i64 %pagesIndexAddr, i32* %sortCols, i32* %sortColTypes, i32* %sortAscendings, i32* undef, i32 %sortColCount, i32 %sub26, i32 %sub15) #18
  %call2.i327 = tail call i32 @_Z9compareTolPiS_S_S_ijj(i64 %pagesIndexAddr, i32* %sortCols, i32* %sortColTypes, i32* %sortAscendings, i32* undef, i32 %sortColCount, i32 %sub27, i32 %sub15) #18
  %cmp.i328 = icmp slt i32 %call.i325, 0
  br i1 %cmp.i328, label %cond.true.i330, label %cond.false10.i335

cond.true.i330:                                   ; preds = %_Z7median3lPiS_S_S_ijjj.exit324
  %cmp3.i329 = icmp slt i32 %call2.i327, 0
  br i1 %cmp3.i329, label %if.end29, label %cond.false.i333

cond.false.i333:                                  ; preds = %cond.true.i330
  %cmp5.i331 = icmp slt i32 %call1.i326, 0
  %cond.i332 = select i1 %cmp5.i331, i32 %sub15, i32 %sub26
  br label %if.end29

cond.false10.i335:                                ; preds = %_Z7median3lPiS_S_S_ijjj.exit324
  %cmp11.i334 = icmp sgt i32 %call2.i327, 0
  br i1 %cmp11.i334, label %if.end29, label %cond.false13.i338

cond.false13.i338:                                ; preds = %cond.false10.i335
  %cmp14.i336 = icmp sgt i32 %call1.i326, 0
  %cond18.i337 = select i1 %cmp14.i336, i32 %sub15, i32 %sub26
  br label %if.end29

if.end29:                                         ; preds = %cond.false13.i338, %cond.false10.i335, %cond.false.i333, %cond.true.i330, %if.then14
  %m.0 = phi i32 [ %add, %if.then14 ], [ %cond22.i323, %cond.true.i330 ], [ %cond22.i323, %cond.false.i333 ], [ %cond22.i323, %cond.false10.i335 ], [ %cond22.i323, %cond.false13.i338 ]
  %l.0 = phi i32 [ %from.tr411, %if.then14 ], [ %cond22.i, %cond.true.i330 ], [ %cond22.i, %cond.false.i333 ], [ %cond22.i, %cond.false10.i335 ], [ %cond22.i, %cond.false13.i338 ]
  %n.0 = phi i32 [ %sub15, %if.then14 ], [ %sub27, %cond.true.i330 ], [ %cond.i332, %cond.false.i333 ], [ %sub27, %cond.false10.i335 ], [ %cond18.i337, %cond.false13.i338 ]
  %call.i341 = tail call i32 @_Z9compareTolPiS_S_S_ijj(i64 %pagesIndexAddr, i32* %sortCols, i32* %sortColTypes, i32* %sortAscendings, i32* undef, i32 %sortColCount, i32 %l.0, i32 %m.0) #18
  %call1.i342 = tail call i32 @_Z9compareTolPiS_S_S_ijj(i64 %pagesIndexAddr, i32* %sortCols, i32* %sortColTypes, i32* %sortAscendings, i32* undef, i32 %sortColCount, i32 %l.0, i32 %n.0) #18
  %call2.i343 = tail call i32 @_Z9compareTolPiS_S_S_ijj(i64 %pagesIndexAddr, i32* %sortCols, i32* %sortColTypes, i32* %sortAscendings, i32* undef, i32 %sortColCount, i32 %m.0, i32 %n.0) #18
  %cmp.i344 = icmp slt i32 %call.i341, 0
  br i1 %cmp.i344, label %cond.true.i346, label %cond.false10.i351

cond.true.i346:                                   ; preds = %if.end29
  %cmp3.i345 = icmp slt i32 %call2.i343, 0
  br i1 %cmp3.i345, label %while.cond.preheader, label %cond.false.i349

cond.false.i349:                                  ; preds = %cond.true.i346
  %cmp5.i347 = icmp slt i32 %call1.i342, 0
  %cond.i348 = select i1 %cmp5.i347, i32 %n.0, i32 %l.0
  br label %while.cond.preheader

cond.false10.i351:                                ; preds = %if.end29
  %cmp11.i350 = icmp sgt i32 %call2.i343, 0
  br i1 %cmp11.i350, label %while.cond.preheader, label %cond.false13.i354

cond.false13.i354:                                ; preds = %cond.false10.i351
  %cmp14.i352 = icmp sgt i32 %call1.i342, 0
  %cond18.i353 = select i1 %cmp14.i352, i32 %n.0, i32 %l.0
  br label %while.cond.preheader

while.cond.preheader:                             ; preds = %cond.false13.i354, %cond.false10.i351, %cond.false.i349, %cond.true.i346, %if.end
  %m.2.ph = phi i32 [ %m.0, %cond.false10.i351 ], [ %m.0, %cond.true.i346 ], [ %cond18.i353, %cond.false13.i354 ], [ %cond.i348, %cond.false.i349 ], [ %add, %if.end ]
  br label %while.cond

while.cond:                                       ; preds = %while.cond.preheader, %if.end75
  %m.2 = phi i32 [ %m.9, %if.end75 ], [ %m.2.ph, %while.cond.preheader ]
  %a.0 = phi i32 [ %a.1.lcssa, %if.end75 ], [ %from.tr411, %while.cond.preheader ]
  %b.0 = phi i32 [ %inc84, %if.end75 ], [ %from.tr411, %while.cond.preheader ]
  %c.0 = phi i32 [ %dec85, %if.end75 ], [ %sub15, %while.cond.preheader ]
  %d.0 = phi i32 [ %d.1400, %if.end75 ], [ %sub15, %while.cond.preheader ]
  %cmp34.not386 = icmp ugt i32 %b.0, %c.0
  br i1 %cmp34.not386, label %while.end, label %land.rhs35

land.rhs35:                                       ; preds = %while.cond, %if.end50
  %b.1389 = phi i32 [ %inc51, %if.end50 ], [ %b.0, %while.cond ]
  %a.1388 = phi i32 [ %a.2, %if.end50 ], [ %a.0, %while.cond ]
  %m.3387 = phi i32 [ %m.5, %if.end50 ], [ %m.2, %while.cond ]
  %call36 = tail call i32 @_Z9compareTolPiS_S_S_ijj(i64 %pagesIndexAddr, i32* %sortCols, i32* %sortColTypes, i32* %sortAscendings, i32* undef, i32 %sortColCount, i32 %b.1389, i32 %m.3387)
  %cmp37 = icmp slt i32 %call36, 1
  br i1 %cmp37, label %while.body39, label %while.end

while.body39:                                     ; preds = %land.rhs35
  %cmp40 = icmp eq i32 %call36, 0
  br i1 %cmp40, label %if.then41, label %if.end50

if.then41:                                        ; preds = %while.body39
  %cmp42 = icmp eq i32 %a.1388, %m.3387
  %cmp44 = icmp eq i32 %b.1389, %m.3387
  %spec.select = select i1 %cmp44, i32 %a.1388, i32 %m.3387
  %m.4 = select i1 %cmp42, i32 %b.1389, i32 %spec.select
  %7 = load i64*, i64** %_M_start.i357, align 8, !tbaa !44
  %inc49 = add i32 %a.1388, 1
  %idxprom.i358 = zext i32 %a.1388 to i64
  %arrayidx.i359 = getelementptr inbounds i64, i64* %7, i64 %idxprom.i358
  %8 = load i64, i64* %arrayidx.i359, align 8, !tbaa !47
  %idxprom1.i360 = zext i32 %b.1389 to i64
  %arrayidx2.i361 = getelementptr inbounds i64, i64* %7, i64 %idxprom1.i360
  %9 = load i64, i64* %arrayidx2.i361, align 8, !tbaa !47
  store i64 %9, i64* %arrayidx.i359, align 8, !tbaa !47
  store i64 %8, i64* %arrayidx2.i361, align 8, !tbaa !47
  br label %if.end50

if.end50:                                         ; preds = %if.then41, %while.body39
  %m.5 = phi i32 [ %m.4, %if.then41 ], [ %m.3387, %while.body39 ]
  %a.2 = phi i32 [ %inc49, %if.then41 ], [ %a.1388, %while.body39 ]
  %inc51 = add i32 %b.1389, 1
  %cmp34.not = icmp ugt i32 %inc51, %c.0
  br i1 %cmp34.not, label %while.end, label %land.rhs35, !llvm.loop !73

while.end:                                        ; preds = %land.rhs35, %if.end50, %while.cond
  %m.3.lcssa = phi i32 [ %m.2, %while.cond ], [ %m.5, %if.end50 ], [ %m.3387, %land.rhs35 ]
  %a.1.lcssa = phi i32 [ %a.0, %while.cond ], [ %a.2, %if.end50 ], [ %a.1388, %land.rhs35 ]
  %b.1.lcssa = phi i32 [ %b.0, %while.cond ], [ %inc51, %if.end50 ], [ %b.1389, %land.rhs35 ]
  %cmp53.not396 = icmp ult i32 %c.0, %b.1.lcssa
  br i1 %cmp53.not396, label %while.end86, label %land.rhs54

land.rhs54:                                       ; preds = %while.end, %if.end70
  %d.1400 = phi i32 [ %d.2, %if.end70 ], [ %d.0, %while.end ]
  %c.1398 = phi i32 [ %dec71, %if.end70 ], [ %c.0, %while.end ]
  %m.6397 = phi i32 [ %m.8, %if.end70 ], [ %m.3.lcssa, %while.end ]
  %call55 = tail call i32 @_Z9compareTolPiS_S_S_ijj(i64 %pagesIndexAddr, i32* %sortCols, i32* %sortColTypes, i32* %sortAscendings, i32* undef, i32 %sortColCount, i32 %c.1398, i32 %m.6397)
  %cmp56 = icmp sgt i32 %call55, -1
  br i1 %cmp56, label %while.body58, label %if.end75

while.body58:                                     ; preds = %land.rhs54
  %cmp59 = icmp eq i32 %call55, 0
  br i1 %cmp59, label %if.then60, label %if.end70

if.then60:                                        ; preds = %while.body58
  %cmp61 = icmp eq i32 %c.1398, %m.6397
  %cmp64 = icmp eq i32 %d.1400, %m.6397
  %spec.select278 = select i1 %cmp64, i32 %c.1398, i32 %m.6397
  %m.7 = select i1 %cmp61, i32 %d.1400, i32 %spec.select278
  %10 = load i64*, i64** %_M_start.i357, align 8, !tbaa !44
  %dec69 = add i32 %d.1400, -1
  %idxprom.i303 = zext i32 %c.1398 to i64
  %arrayidx.i304 = getelementptr inbounds i64, i64* %10, i64 %idxprom.i303
  %11 = load i64, i64* %arrayidx.i304, align 8, !tbaa !47
  %idxprom1.i305 = zext i32 %d.1400 to i64
  %arrayidx2.i306 = getelementptr inbounds i64, i64* %10, i64 %idxprom1.i305
  %12 = load i64, i64* %arrayidx2.i306, align 8, !tbaa !47
  store i64 %12, i64* %arrayidx.i304, align 8, !tbaa !47
  store i64 %11, i64* %arrayidx2.i306, align 8, !tbaa !47
  br label %if.end70

if.end70:                                         ; preds = %if.then60, %while.body58
  %m.8 = phi i32 [ %m.7, %if.then60 ], [ %m.6397, %while.body58 ]
  %d.2 = phi i32 [ %dec69, %if.then60 ], [ %d.1400, %while.body58 ]
  %dec71 = add i32 %c.1398, -1
  %cmp53.not = icmp ult i32 %dec71, %b.1.lcssa
  br i1 %cmp53.not, label %while.end86, label %land.rhs54, !llvm.loop !74

if.end75:                                         ; preds = %land.rhs54
  %cmp76 = icmp eq i32 %b.1.lcssa, %m.6397
  %m.9 = select i1 %cmp76, i32 %d.1400, i32 %m.6397
  %13 = load i64*, i64** %_M_start.i357, align 8, !tbaa !44
  %inc84 = add i32 %b.1.lcssa, 1
  %dec85 = add i32 %c.1398, -1
  %idxprom.i298 = zext i32 %b.1.lcssa to i64
  %arrayidx.i299 = getelementptr inbounds i64, i64* %13, i64 %idxprom.i298
  %14 = load i64, i64* %arrayidx.i299, align 8, !tbaa !47
  %idxprom1.i300 = zext i32 %c.1398 to i64
  %arrayidx2.i301 = getelementptr inbounds i64, i64* %13, i64 %idxprom1.i300
  %15 = load i64, i64* %arrayidx2.i301, align 8, !tbaa !47
  store i64 %15, i64* %arrayidx.i299, align 8, !tbaa !47
  store i64 %14, i64* %arrayidx2.i301, align 8, !tbaa !47
  br label %while.cond

while.end86:                                      ; preds = %while.end, %if.end70
  %c.1.lcssa = phi i32 [ %dec71, %if.end70 ], [ %c.0, %while.end ]
  %d.1.lcssa = phi i32 [ %d.2, %if.end70 ], [ %d.0, %while.end ]
  %sub89 = sub i32 %a.1.lcssa, %from.tr411
  %sub91 = sub i32 %b.1.lcssa, %a.1.lcssa
  %cmp.i296 = icmp ult i32 %sub91, %sub89
  %.sroa.speculated369 = select i1 %cmp.i296, i32 %sub91, i32 %sub89
  %16 = load i64*, i64** %_M_start.i357, align 8, !tbaa !44
  %cmp7.not.i281 = icmp eq i32 %.sroa.speculated369, 0
  br i1 %cmp7.not.i281, label %_Z10vectorSwapPljjj.exit294, label %for.body.i293.preheader

for.body.i293.preheader:                          ; preds = %while.end86
  %sub94 = sub i32 %b.1.lcssa, %.sroa.speculated369
  %min.iters.check474 = icmp ult i32 %.sroa.speculated369, 4
  br i1 %min.iters.check474, label %for.body.i293.preheader517, label %vector.scevcheck482

vector.scevcheck482:                              ; preds = %for.body.i293.preheader
  %17 = add i32 %.sroa.speculated369, -1
  %18 = xor i32 %from.tr411, -1
  %19 = icmp ugt i32 %17, %18
  %20 = add i32 %b.1.lcssa, -1
  %21 = icmp ult i32 %20, %sub94
  %22 = or i1 %19, %21
  br i1 %22, label %for.body.i293.preheader517, label %vector.memcheck484

vector.memcheck484:                               ; preds = %vector.scevcheck482
  %23 = zext i32 %from.tr411 to i64
  %scevgep486 = getelementptr i64, i64* %16, i64 %23
  %scevgep488 = getelementptr i64, i64* %16, i64 1
  %24 = add i32 %.sroa.speculated369, -1
  %25 = zext i32 %24 to i64
  %26 = add nuw nsw i64 %23, %25
  %scevgep489 = getelementptr i64, i64* %scevgep488, i64 %26
  %27 = zext i32 %sub94 to i64
  %scevgep491 = getelementptr i64, i64* %16, i64 %27
  %scevgep493 = getelementptr i64, i64* %16, i64 1
  %28 = add nuw nsw i64 %27, %25
  %scevgep494 = getelementptr i64, i64* %scevgep493, i64 %28
  %bound0496 = icmp ult i64* %scevgep486, %scevgep494
  %bound1497 = icmp ult i64* %scevgep491, %scevgep489
  %found.conflict498 = and i1 %bound0496, %bound1497
  br i1 %found.conflict498, label %for.body.i293.preheader517, label %vector.ph485

vector.ph485:                                     ; preds = %vector.memcheck484
  %n.vec501 = and i32 %.sroa.speculated369, -4
  %ind.end506 = add i32 %from.tr411, %n.vec501
  %ind.end508 = add i32 %sub94, %n.vec501
  %29 = add i32 %n.vec501, -4
  %30 = lshr exact i32 %29, 2
  %31 = add nuw nsw i32 %30, 1
  %xtraiter = and i32 %31, 1
  %32 = icmp eq i32 %29, 0
  br i1 %32, label %middle.block471.unr-lcssa, label %vector.ph485.new

vector.ph485.new:                                 ; preds = %vector.ph485
  %unroll_iter = and i32 %31, 2147483646
  br label %vector.body473

vector.body473:                                   ; preds = %vector.body473, %vector.ph485.new
  %index502 = phi i32 [ 0, %vector.ph485.new ], [ %index.next503.1, %vector.body473 ]
  %niter = phi i32 [ %unroll_iter, %vector.ph485.new ], [ %niter.nsub.1, %vector.body473 ]
  %offset.idx510 = add i32 %from.tr411, %index502
  %offset.idx511 = add i32 %sub94, %index502
  %33 = zext i32 %offset.idx510 to i64
  %34 = getelementptr inbounds i64, i64* %16, i64 %33
  %35 = bitcast i64* %34 to <2 x i64>*
  %wide.load512 = load <2 x i64>, <2 x i64>* %35, align 8, !tbaa !47, !alias.scope !75, !noalias !78
  %36 = getelementptr inbounds i64, i64* %34, i64 2
  %37 = bitcast i64* %36 to <2 x i64>*
  %wide.load513 = load <2 x i64>, <2 x i64>* %37, align 8, !tbaa !47, !alias.scope !75, !noalias !78
  %38 = zext i32 %offset.idx511 to i64
  %39 = getelementptr inbounds i64, i64* %16, i64 %38
  %40 = bitcast i64* %39 to <2 x i64>*
  %wide.load514 = load <2 x i64>, <2 x i64>* %40, align 8, !tbaa !47, !alias.scope !78
  %41 = getelementptr inbounds i64, i64* %39, i64 2
  %42 = bitcast i64* %41 to <2 x i64>*
  %wide.load515 = load <2 x i64>, <2 x i64>* %42, align 8, !tbaa !47, !alias.scope !78
  %43 = bitcast i64* %34 to <2 x i64>*
  store <2 x i64> %wide.load514, <2 x i64>* %43, align 8, !tbaa !47, !alias.scope !75, !noalias !78
  %44 = bitcast i64* %36 to <2 x i64>*
  store <2 x i64> %wide.load515, <2 x i64>* %44, align 8, !tbaa !47, !alias.scope !75, !noalias !78
  %45 = bitcast i64* %39 to <2 x i64>*
  store <2 x i64> %wide.load512, <2 x i64>* %45, align 8, !tbaa !47, !alias.scope !78
  %46 = bitcast i64* %41 to <2 x i64>*
  store <2 x i64> %wide.load513, <2 x i64>* %46, align 8, !tbaa !47, !alias.scope !78
  %index.next503 = or i32 %index502, 4
  %offset.idx510.1 = add i32 %from.tr411, %index.next503
  %offset.idx511.1 = add i32 %sub94, %index.next503
  %47 = zext i32 %offset.idx510.1 to i64
  %48 = getelementptr inbounds i64, i64* %16, i64 %47
  %49 = bitcast i64* %48 to <2 x i64>*
  %wide.load512.1 = load <2 x i64>, <2 x i64>* %49, align 8, !tbaa !47, !alias.scope !75, !noalias !78
  %50 = getelementptr inbounds i64, i64* %48, i64 2
  %51 = bitcast i64* %50 to <2 x i64>*
  %wide.load513.1 = load <2 x i64>, <2 x i64>* %51, align 8, !tbaa !47, !alias.scope !75, !noalias !78
  %52 = zext i32 %offset.idx511.1 to i64
  %53 = getelementptr inbounds i64, i64* %16, i64 %52
  %54 = bitcast i64* %53 to <2 x i64>*
  %wide.load514.1 = load <2 x i64>, <2 x i64>* %54, align 8, !tbaa !47, !alias.scope !78
  %55 = getelementptr inbounds i64, i64* %53, i64 2
  %56 = bitcast i64* %55 to <2 x i64>*
  %wide.load515.1 = load <2 x i64>, <2 x i64>* %56, align 8, !tbaa !47, !alias.scope !78
  %57 = bitcast i64* %48 to <2 x i64>*
  store <2 x i64> %wide.load514.1, <2 x i64>* %57, align 8, !tbaa !47, !alias.scope !75, !noalias !78
  %58 = bitcast i64* %50 to <2 x i64>*
  store <2 x i64> %wide.load515.1, <2 x i64>* %58, align 8, !tbaa !47, !alias.scope !75, !noalias !78
  %59 = bitcast i64* %53 to <2 x i64>*
  store <2 x i64> %wide.load512.1, <2 x i64>* %59, align 8, !tbaa !47, !alias.scope !78
  %60 = bitcast i64* %55 to <2 x i64>*
  store <2 x i64> %wide.load513.1, <2 x i64>* %60, align 8, !tbaa !47, !alias.scope !78
  %index.next503.1 = add i32 %index502, 8
  %niter.nsub.1 = add i32 %niter, -2
  %niter.ncmp.1 = icmp eq i32 %niter.nsub.1, 0
  br i1 %niter.ncmp.1, label %middle.block471.unr-lcssa, label %vector.body473, !llvm.loop !80

middle.block471.unr-lcssa:                        ; preds = %vector.body473, %vector.ph485
  %index502.unr = phi i32 [ 0, %vector.ph485 ], [ %index.next503.1, %vector.body473 ]
  %lcmp.mod.not = icmp eq i32 %xtraiter, 0
  br i1 %lcmp.mod.not, label %middle.block471, label %vector.body473.epil

vector.body473.epil:                              ; preds = %middle.block471.unr-lcssa
  %offset.idx510.epil = add i32 %from.tr411, %index502.unr
  %offset.idx511.epil = add i32 %sub94, %index502.unr
  %61 = zext i32 %offset.idx510.epil to i64
  %62 = getelementptr inbounds i64, i64* %16, i64 %61
  %63 = bitcast i64* %62 to <2 x i64>*
  %wide.load512.epil = load <2 x i64>, <2 x i64>* %63, align 8, !tbaa !47, !alias.scope !75, !noalias !78
  %64 = getelementptr inbounds i64, i64* %62, i64 2
  %65 = bitcast i64* %64 to <2 x i64>*
  %wide.load513.epil = load <2 x i64>, <2 x i64>* %65, align 8, !tbaa !47, !alias.scope !75, !noalias !78
  %66 = zext i32 %offset.idx511.epil to i64
  %67 = getelementptr inbounds i64, i64* %16, i64 %66
  %68 = bitcast i64* %67 to <2 x i64>*
  %wide.load514.epil = load <2 x i64>, <2 x i64>* %68, align 8, !tbaa !47, !alias.scope !78
  %69 = getelementptr inbounds i64, i64* %67, i64 2
  %70 = bitcast i64* %69 to <2 x i64>*
  %wide.load515.epil = load <2 x i64>, <2 x i64>* %70, align 8, !tbaa !47, !alias.scope !78
  %71 = bitcast i64* %62 to <2 x i64>*
  store <2 x i64> %wide.load514.epil, <2 x i64>* %71, align 8, !tbaa !47, !alias.scope !75, !noalias !78
  %72 = bitcast i64* %64 to <2 x i64>*
  store <2 x i64> %wide.load515.epil, <2 x i64>* %72, align 8, !tbaa !47, !alias.scope !75, !noalias !78
  %73 = bitcast i64* %67 to <2 x i64>*
  store <2 x i64> %wide.load512.epil, <2 x i64>* %73, align 8, !tbaa !47, !alias.scope !78
  %74 = bitcast i64* %69 to <2 x i64>*
  store <2 x i64> %wide.load513.epil, <2 x i64>* %74, align 8, !tbaa !47, !alias.scope !78
  br label %middle.block471

middle.block471:                                  ; preds = %middle.block471.unr-lcssa, %vector.body473.epil
  %cmp.n509 = icmp eq i32 %.sroa.speculated369, %n.vec501
  br i1 %cmp.n509, label %_Z10vectorSwapPljjj.exit294, label %for.body.i293.preheader517

for.body.i293.preheader517:                       ; preds = %vector.memcheck484, %vector.scevcheck482, %for.body.i293.preheader, %middle.block471
  %i.010.i282.ph = phi i32 [ 0, %vector.memcheck484 ], [ 0, %vector.scevcheck482 ], [ 0, %for.body.i293.preheader ], [ %n.vec501, %middle.block471 ]
  %from.addr.09.i283.ph = phi i32 [ %from.tr411, %vector.memcheck484 ], [ %from.tr411, %vector.scevcheck482 ], [ %from.tr411, %for.body.i293.preheader ], [ %ind.end506, %middle.block471 ]
  %l.addr.08.i284.ph = phi i32 [ %sub94, %vector.memcheck484 ], [ %sub94, %vector.scevcheck482 ], [ %sub94, %for.body.i293.preheader ], [ %ind.end508, %middle.block471 ]
  %.neg = or i32 %i.010.i282.ph, 1
  %xtraiter529 = and i32 %.sroa.speculated369, 1
  %lcmp.mod530.not = icmp eq i32 %xtraiter529, 0
  br i1 %lcmp.mod530.not, label %for.body.i293.prol.loopexit, label %for.body.i293.prol

for.body.i293.prol:                               ; preds = %for.body.i293.preheader517
  %idxprom.i.i285.prol = zext i32 %from.addr.09.i283.ph to i64
  %arrayidx.i.i286.prol = getelementptr inbounds i64, i64* %16, i64 %idxprom.i.i285.prol
  %75 = load i64, i64* %arrayidx.i.i286.prol, align 8, !tbaa !47
  %idxprom1.i.i287.prol = zext i32 %l.addr.08.i284.ph to i64
  %arrayidx2.i.i288.prol = getelementptr inbounds i64, i64* %16, i64 %idxprom1.i.i287.prol
  %76 = load i64, i64* %arrayidx2.i.i288.prol, align 8, !tbaa !47
  store i64 %76, i64* %arrayidx.i.i286.prol, align 8, !tbaa !47
  store i64 %75, i64* %arrayidx2.i.i288.prol, align 8, !tbaa !47
  %inc.i289.prol = or i32 %i.010.i282.ph, 1
  %inc1.i290.prol = add i32 %from.addr.09.i283.ph, 1
  %inc2.i291.prol = add i32 %l.addr.08.i284.ph, 1
  br label %for.body.i293.prol.loopexit

for.body.i293.prol.loopexit:                      ; preds = %for.body.i293.prol, %for.body.i293.preheader517
  %i.010.i282.unr.ph = phi i32 [ %inc.i289.prol, %for.body.i293.prol ], [ %i.010.i282.ph, %for.body.i293.preheader517 ]
  %from.addr.09.i283.unr.ph = phi i32 [ %inc1.i290.prol, %for.body.i293.prol ], [ %from.addr.09.i283.ph, %for.body.i293.preheader517 ]
  %l.addr.08.i284.unr.ph = phi i32 [ %inc2.i291.prol, %for.body.i293.prol ], [ %l.addr.08.i284.ph, %for.body.i293.preheader517 ]
  %77 = icmp eq i32 %.sroa.speculated369, %.neg
  br i1 %77, label %_Z10vectorSwapPljjj.exit294, label %for.body.i293

for.body.i293:                                    ; preds = %for.body.i293.prol.loopexit, %for.body.i293
  %i.010.i282 = phi i32 [ %inc.i289.1, %for.body.i293 ], [ %i.010.i282.unr.ph, %for.body.i293.prol.loopexit ]
  %from.addr.09.i283 = phi i32 [ %inc1.i290.1, %for.body.i293 ], [ %from.addr.09.i283.unr.ph, %for.body.i293.prol.loopexit ]
  %l.addr.08.i284 = phi i32 [ %inc2.i291.1, %for.body.i293 ], [ %l.addr.08.i284.unr.ph, %for.body.i293.prol.loopexit ]
  %idxprom.i.i285 = zext i32 %from.addr.09.i283 to i64
  %arrayidx.i.i286 = getelementptr inbounds i64, i64* %16, i64 %idxprom.i.i285
  %78 = load i64, i64* %arrayidx.i.i286, align 8, !tbaa !47
  %idxprom1.i.i287 = zext i32 %l.addr.08.i284 to i64
  %arrayidx2.i.i288 = getelementptr inbounds i64, i64* %16, i64 %idxprom1.i.i287
  %79 = load i64, i64* %arrayidx2.i.i288, align 8, !tbaa !47
  store i64 %79, i64* %arrayidx.i.i286, align 8, !tbaa !47
  store i64 %78, i64* %arrayidx2.i.i288, align 8, !tbaa !47
  %inc1.i290 = add i32 %from.addr.09.i283, 1
  %inc2.i291 = add i32 %l.addr.08.i284, 1
  %idxprom.i.i285.1 = zext i32 %inc1.i290 to i64
  %arrayidx.i.i286.1 = getelementptr inbounds i64, i64* %16, i64 %idxprom.i.i285.1
  %80 = load i64, i64* %arrayidx.i.i286.1, align 8, !tbaa !47
  %idxprom1.i.i287.1 = zext i32 %inc2.i291 to i64
  %arrayidx2.i.i288.1 = getelementptr inbounds i64, i64* %16, i64 %idxprom1.i.i287.1
  %81 = load i64, i64* %arrayidx2.i.i288.1, align 8, !tbaa !47
  store i64 %81, i64* %arrayidx.i.i286.1, align 8, !tbaa !47
  store i64 %80, i64* %arrayidx2.i.i288.1, align 8, !tbaa !47
  %inc.i289.1 = add nuw i32 %i.010.i282, 2
  %inc1.i290.1 = add i32 %from.addr.09.i283, 2
  %inc2.i291.1 = add i32 %l.addr.08.i284, 2
  %exitcond.not.i292.1 = icmp eq i32 %inc.i289.1, %.sroa.speculated369
  br i1 %exitcond.not.i292.1, label %_Z10vectorSwapPljjj.exit294, label %for.body.i293, !llvm.loop !81

_Z10vectorSwapPljjj.exit294:                      ; preds = %for.body.i293.prol.loopexit, %for.body.i293, %middle.block471, %while.end86
  %sub96 = sub i32 %d.1.lcssa, %c.1.lcssa
  %82 = xor i32 %d.1.lcssa, -1
  %sub99 = add i32 %82, %to
  %cmp.i = icmp ult i32 %sub99, %sub96
  %.sroa.speculated = select i1 %cmp.i, i32 %sub99, i32 %sub96
  %cmp7.not.i = icmp eq i32 %.sroa.speculated, 0
  br i1 %cmp7.not.i, label %_Z10vectorSwapPljjj.exit, label %for.body.i.preheader

for.body.i.preheader:                             ; preds = %_Z10vectorSwapPljjj.exit294
  %sub102 = sub i32 %to, %.sroa.speculated
  %min.iters.check = icmp ult i32 %.sroa.speculated, 4
  br i1 %min.iters.check, label %for.body.i.preheader516, label %vector.scevcheck

vector.scevcheck:                                 ; preds = %for.body.i.preheader
  %83 = add i32 %.sroa.speculated, -1
  %84 = xor i32 %b.1.lcssa, -1
  %85 = icmp ugt i32 %83, %84
  %86 = icmp ult i32 %1, %sub102
  %87 = or i1 %85, %86
  br i1 %87, label %for.body.i.preheader516, label %vector.memcheck

vector.memcheck:                                  ; preds = %vector.scevcheck
  %88 = zext i32 %b.1.lcssa to i64
  %scevgep = getelementptr i64, i64* %16, i64 %88
  %scevgep456 = getelementptr i64, i64* %16, i64 1
  %89 = add i32 %.sroa.speculated, -1
  %90 = zext i32 %89 to i64
  %91 = add nuw nsw i64 %88, %90
  %scevgep457 = getelementptr i64, i64* %scevgep456, i64 %91
  %92 = zext i32 %sub102 to i64
  %scevgep459 = getelementptr i64, i64* %16, i64 %92
  %scevgep461 = getelementptr i64, i64* %16, i64 1
  %93 = add nuw nsw i64 %92, %90
  %scevgep462 = getelementptr i64, i64* %scevgep461, i64 %93
  %bound0 = icmp ult i64* %scevgep, %scevgep462
  %bound1 = icmp ult i64* %scevgep459, %scevgep457
  %found.conflict = and i1 %bound0, %bound1
  br i1 %found.conflict, label %for.body.i.preheader516, label %vector.ph

vector.ph:                                        ; preds = %vector.memcheck
  %n.vec = and i32 %.sroa.speculated, -4
  %ind.end = add i32 %b.1.lcssa, %n.vec
  %ind.end466 = add i32 %sub102, %n.vec
  %94 = add i32 %n.vec, -4
  %95 = lshr exact i32 %94, 2
  %96 = add nuw nsw i32 %95, 1
  %xtraiter531 = and i32 %96, 1
  %97 = icmp eq i32 %94, 0
  br i1 %97, label %middle.block.unr-lcssa, label %vector.ph.new

vector.ph.new:                                    ; preds = %vector.ph
  %unroll_iter533 = and i32 %96, 2147483646
  br label %vector.body

vector.body:                                      ; preds = %vector.body, %vector.ph.new
  %index = phi i32 [ 0, %vector.ph.new ], [ %index.next.1, %vector.body ]
  %niter534 = phi i32 [ %unroll_iter533, %vector.ph.new ], [ %niter534.nsub.1, %vector.body ]
  %offset.idx = add i32 %b.1.lcssa, %index
  %offset.idx467 = add i32 %sub102, %index
  %98 = zext i32 %offset.idx to i64
  %99 = getelementptr inbounds i64, i64* %16, i64 %98
  %100 = bitcast i64* %99 to <2 x i64>*
  %wide.load = load <2 x i64>, <2 x i64>* %100, align 8, !tbaa !47, !alias.scope !82, !noalias !85
  %101 = getelementptr inbounds i64, i64* %99, i64 2
  %102 = bitcast i64* %101 to <2 x i64>*
  %wide.load468 = load <2 x i64>, <2 x i64>* %102, align 8, !tbaa !47, !alias.scope !82, !noalias !85
  %103 = zext i32 %offset.idx467 to i64
  %104 = getelementptr inbounds i64, i64* %16, i64 %103
  %105 = bitcast i64* %104 to <2 x i64>*
  %wide.load469 = load <2 x i64>, <2 x i64>* %105, align 8, !tbaa !47, !alias.scope !85
  %106 = getelementptr inbounds i64, i64* %104, i64 2
  %107 = bitcast i64* %106 to <2 x i64>*
  %wide.load470 = load <2 x i64>, <2 x i64>* %107, align 8, !tbaa !47, !alias.scope !85
  %108 = bitcast i64* %99 to <2 x i64>*
  store <2 x i64> %wide.load469, <2 x i64>* %108, align 8, !tbaa !47, !alias.scope !82, !noalias !85
  %109 = bitcast i64* %101 to <2 x i64>*
  store <2 x i64> %wide.load470, <2 x i64>* %109, align 8, !tbaa !47, !alias.scope !82, !noalias !85
  %110 = bitcast i64* %104 to <2 x i64>*
  store <2 x i64> %wide.load, <2 x i64>* %110, align 8, !tbaa !47, !alias.scope !85
  %111 = bitcast i64* %106 to <2 x i64>*
  store <2 x i64> %wide.load468, <2 x i64>* %111, align 8, !tbaa !47, !alias.scope !85
  %index.next = or i32 %index, 4
  %offset.idx.1 = add i32 %b.1.lcssa, %index.next
  %offset.idx467.1 = add i32 %sub102, %index.next
  %112 = zext i32 %offset.idx.1 to i64
  %113 = getelementptr inbounds i64, i64* %16, i64 %112
  %114 = bitcast i64* %113 to <2 x i64>*
  %wide.load.1 = load <2 x i64>, <2 x i64>* %114, align 8, !tbaa !47, !alias.scope !82, !noalias !85
  %115 = getelementptr inbounds i64, i64* %113, i64 2
  %116 = bitcast i64* %115 to <2 x i64>*
  %wide.load468.1 = load <2 x i64>, <2 x i64>* %116, align 8, !tbaa !47, !alias.scope !82, !noalias !85
  %117 = zext i32 %offset.idx467.1 to i64
  %118 = getelementptr inbounds i64, i64* %16, i64 %117
  %119 = bitcast i64* %118 to <2 x i64>*
  %wide.load469.1 = load <2 x i64>, <2 x i64>* %119, align 8, !tbaa !47, !alias.scope !85
  %120 = getelementptr inbounds i64, i64* %118, i64 2
  %121 = bitcast i64* %120 to <2 x i64>*
  %wide.load470.1 = load <2 x i64>, <2 x i64>* %121, align 8, !tbaa !47, !alias.scope !85
  %122 = bitcast i64* %113 to <2 x i64>*
  store <2 x i64> %wide.load469.1, <2 x i64>* %122, align 8, !tbaa !47, !alias.scope !82, !noalias !85
  %123 = bitcast i64* %115 to <2 x i64>*
  store <2 x i64> %wide.load470.1, <2 x i64>* %123, align 8, !tbaa !47, !alias.scope !82, !noalias !85
  %124 = bitcast i64* %118 to <2 x i64>*
  store <2 x i64> %wide.load.1, <2 x i64>* %124, align 8, !tbaa !47, !alias.scope !85
  %125 = bitcast i64* %120 to <2 x i64>*
  store <2 x i64> %wide.load468.1, <2 x i64>* %125, align 8, !tbaa !47, !alias.scope !85
  %index.next.1 = add i32 %index, 8
  %niter534.nsub.1 = add i32 %niter534, -2
  %niter534.ncmp.1 = icmp eq i32 %niter534.nsub.1, 0
  br i1 %niter534.ncmp.1, label %middle.block.unr-lcssa, label %vector.body, !llvm.loop !87

middle.block.unr-lcssa:                           ; preds = %vector.body, %vector.ph
  %index.unr = phi i32 [ 0, %vector.ph ], [ %index.next.1, %vector.body ]
  %lcmp.mod532.not = icmp eq i32 %xtraiter531, 0
  br i1 %lcmp.mod532.not, label %middle.block, label %vector.body.epil

vector.body.epil:                                 ; preds = %middle.block.unr-lcssa
  %offset.idx.epil = add i32 %b.1.lcssa, %index.unr
  %offset.idx467.epil = add i32 %sub102, %index.unr
  %126 = zext i32 %offset.idx.epil to i64
  %127 = getelementptr inbounds i64, i64* %16, i64 %126
  %128 = bitcast i64* %127 to <2 x i64>*
  %wide.load.epil = load <2 x i64>, <2 x i64>* %128, align 8, !tbaa !47, !alias.scope !82, !noalias !85
  %129 = getelementptr inbounds i64, i64* %127, i64 2
  %130 = bitcast i64* %129 to <2 x i64>*
  %wide.load468.epil = load <2 x i64>, <2 x i64>* %130, align 8, !tbaa !47, !alias.scope !82, !noalias !85
  %131 = zext i32 %offset.idx467.epil to i64
  %132 = getelementptr inbounds i64, i64* %16, i64 %131
  %133 = bitcast i64* %132 to <2 x i64>*
  %wide.load469.epil = load <2 x i64>, <2 x i64>* %133, align 8, !tbaa !47, !alias.scope !85
  %134 = getelementptr inbounds i64, i64* %132, i64 2
  %135 = bitcast i64* %134 to <2 x i64>*
  %wide.load470.epil = load <2 x i64>, <2 x i64>* %135, align 8, !tbaa !47, !alias.scope !85
  %136 = bitcast i64* %127 to <2 x i64>*
  store <2 x i64> %wide.load469.epil, <2 x i64>* %136, align 8, !tbaa !47, !alias.scope !82, !noalias !85
  %137 = bitcast i64* %129 to <2 x i64>*
  store <2 x i64> %wide.load470.epil, <2 x i64>* %137, align 8, !tbaa !47, !alias.scope !82, !noalias !85
  %138 = bitcast i64* %132 to <2 x i64>*
  store <2 x i64> %wide.load.epil, <2 x i64>* %138, align 8, !tbaa !47, !alias.scope !85
  %139 = bitcast i64* %134 to <2 x i64>*
  store <2 x i64> %wide.load468.epil, <2 x i64>* %139, align 8, !tbaa !47, !alias.scope !85
  br label %middle.block

middle.block:                                     ; preds = %middle.block.unr-lcssa, %vector.body.epil
  %cmp.n = icmp eq i32 %.sroa.speculated, %n.vec
  br i1 %cmp.n, label %_Z10vectorSwapPljjj.exit, label %for.body.i.preheader516

for.body.i.preheader516:                          ; preds = %vector.memcheck, %vector.scevcheck, %for.body.i.preheader, %middle.block
  %i.010.i.ph = phi i32 [ 0, %vector.memcheck ], [ 0, %vector.scevcheck ], [ 0, %for.body.i.preheader ], [ %n.vec, %middle.block ]
  %from.addr.09.i.ph = phi i32 [ %b.1.lcssa, %vector.memcheck ], [ %b.1.lcssa, %vector.scevcheck ], [ %b.1.lcssa, %for.body.i.preheader ], [ %ind.end, %middle.block ]
  %l.addr.08.i.ph = phi i32 [ %sub102, %vector.memcheck ], [ %sub102, %vector.scevcheck ], [ %sub102, %for.body.i.preheader ], [ %ind.end466, %middle.block ]
  %.neg537 = or i32 %i.010.i.ph, 1
  %xtraiter535 = and i32 %.sroa.speculated, 1
  %lcmp.mod536.not = icmp eq i32 %xtraiter535, 0
  br i1 %lcmp.mod536.not, label %for.body.i.prol.loopexit, label %for.body.i.prol

for.body.i.prol:                                  ; preds = %for.body.i.preheader516
  %idxprom.i.i.prol = zext i32 %from.addr.09.i.ph to i64
  %arrayidx.i.i.prol = getelementptr inbounds i64, i64* %16, i64 %idxprom.i.i.prol
  %140 = load i64, i64* %arrayidx.i.i.prol, align 8, !tbaa !47
  %idxprom1.i.i.prol = zext i32 %l.addr.08.i.ph to i64
  %arrayidx2.i.i.prol = getelementptr inbounds i64, i64* %16, i64 %idxprom1.i.i.prol
  %141 = load i64, i64* %arrayidx2.i.i.prol, align 8, !tbaa !47
  store i64 %141, i64* %arrayidx.i.i.prol, align 8, !tbaa !47
  store i64 %140, i64* %arrayidx2.i.i.prol, align 8, !tbaa !47
  %inc.i.prol = or i32 %i.010.i.ph, 1
  %inc1.i.prol = add i32 %from.addr.09.i.ph, 1
  %inc2.i.prol = add i32 %l.addr.08.i.ph, 1
  br label %for.body.i.prol.loopexit

for.body.i.prol.loopexit:                         ; preds = %for.body.i.prol, %for.body.i.preheader516
  %i.010.i.unr.ph = phi i32 [ %inc.i.prol, %for.body.i.prol ], [ %i.010.i.ph, %for.body.i.preheader516 ]
  %from.addr.09.i.unr.ph = phi i32 [ %inc1.i.prol, %for.body.i.prol ], [ %from.addr.09.i.ph, %for.body.i.preheader516 ]
  %l.addr.08.i.unr.ph = phi i32 [ %inc2.i.prol, %for.body.i.prol ], [ %l.addr.08.i.ph, %for.body.i.preheader516 ]
  %142 = icmp eq i32 %.sroa.speculated, %.neg537
  br i1 %142, label %_Z10vectorSwapPljjj.exit, label %for.body.i

for.body.i:                                       ; preds = %for.body.i.prol.loopexit, %for.body.i
  %i.010.i = phi i32 [ %inc.i.1, %for.body.i ], [ %i.010.i.unr.ph, %for.body.i.prol.loopexit ]
  %from.addr.09.i = phi i32 [ %inc1.i.1, %for.body.i ], [ %from.addr.09.i.unr.ph, %for.body.i.prol.loopexit ]
  %l.addr.08.i = phi i32 [ %inc2.i.1, %for.body.i ], [ %l.addr.08.i.unr.ph, %for.body.i.prol.loopexit ]
  %idxprom.i.i = zext i32 %from.addr.09.i to i64
  %arrayidx.i.i = getelementptr inbounds i64, i64* %16, i64 %idxprom.i.i
  %143 = load i64, i64* %arrayidx.i.i, align 8, !tbaa !47
  %idxprom1.i.i = zext i32 %l.addr.08.i to i64
  %arrayidx2.i.i = getelementptr inbounds i64, i64* %16, i64 %idxprom1.i.i
  %144 = load i64, i64* %arrayidx2.i.i, align 8, !tbaa !47
  store i64 %144, i64* %arrayidx.i.i, align 8, !tbaa !47
  store i64 %143, i64* %arrayidx2.i.i, align 8, !tbaa !47
  %inc1.i = add i32 %from.addr.09.i, 1
  %inc2.i = add i32 %l.addr.08.i, 1
  %idxprom.i.i.1 = zext i32 %inc1.i to i64
  %arrayidx.i.i.1 = getelementptr inbounds i64, i64* %16, i64 %idxprom.i.i.1
  %145 = load i64, i64* %arrayidx.i.i.1, align 8, !tbaa !47
  %idxprom1.i.i.1 = zext i32 %inc2.i to i64
  %arrayidx2.i.i.1 = getelementptr inbounds i64, i64* %16, i64 %idxprom1.i.i.1
  %146 = load i64, i64* %arrayidx2.i.i.1, align 8, !tbaa !47
  store i64 %146, i64* %arrayidx.i.i.1, align 8, !tbaa !47
  store i64 %145, i64* %arrayidx2.i.i.1, align 8, !tbaa !47
  %inc.i.1 = add nuw i32 %i.010.i, 2
  %inc1.i.1 = add i32 %from.addr.09.i, 2
  %inc2.i.1 = add i32 %l.addr.08.i, 2
  %exitcond.not.i.1 = icmp eq i32 %inc.i.1, %.sroa.speculated
  br i1 %exitcond.not.i.1, label %_Z10vectorSwapPljjj.exit, label %for.body.i, !llvm.loop !88

_Z10vectorSwapPljjj.exit:                         ; preds = %for.body.i.prol.loopexit, %for.body.i, %middle.block, %_Z10vectorSwapPljjj.exit294
  %cmp104 = icmp ugt i32 %sub91, 1
  br i1 %cmp104, label %if.then105, label %if.end107

if.then105:                                       ; preds = %_Z10vectorSwapPljjj.exit
  %add106 = add i32 %sub91, %from.tr411
  tail call void @_Z9quickSortlPiS_S_S_ijj(i64 %pagesIndexAddr, i32* %sortCols, i32* %sortColTypes, i32* %sortAscendings, i32* %sortNullFirsts, i32 %sortColCount, i32 %from.tr411, i32 %add106)
  br label %if.end107

if.end107:                                        ; preds = %if.then105, %_Z10vectorSwapPljjj.exit
  %cmp109 = icmp ugt i32 %sub96, 1
  br i1 %cmp109, label %if.then110, label %cleanup

if.then110:                                       ; preds = %if.end107
  %sub111 = sub i32 %to, %sub96
  %cmp = icmp ult i32 %sub96, 7
  br i1 %cmp, label %for.cond.preheader, label %if.end

cleanup:                                          ; preds = %if.end107, %for.cond.cleanup7, %for.cond.preheader
  ret void
}

; Function Attrs: nofree norecurse nounwind uwtable mustprogress
define dso_local void @_Z18setIntColumnValuesPljRSt6vectorIP6ColumnSaIS2_EEPi(i64* nocapture readonly %valueAddresses, i32 %positionCount, %"class.std::vector"* nocapture nonnull readonly align 8 dereferenceable(24) %inputTable, i32* nocapture %outputData) local_unnamed_addr #10 {
entry:
  %cmp23.not = icmp eq i32 %positionCount, 0
  br i1 %cmp23.not, label %for.cond.cleanup, label %for.body.lr.ph

for.body.lr.ph:                                   ; preds = %entry
  %_M_start.i = getelementptr inbounds %"class.std::vector", %"class.std::vector"* %inputTable, i64 0, i32 0, i32 0, i32 0
  %0 = load %class.Column**, %class.Column*** %_M_start.i, align 8
  %wide.trip.count = zext i32 %positionCount to i64
  br label %for.body

for.cond.cleanup:                                 ; preds = %if.end, %entry
  ret void

for.body:                                         ; preds = %for.body.lr.ph, %if.end
  %indvars.iv = phi i64 [ 0, %for.body.lr.ph ], [ %indvars.iv.next, %if.end ]
  %inputData.025 = phi i32* [ null, %for.body.lr.ph ], [ %inputData.1, %if.end ]
  %preTableIndex.024 = phi i32 [ -1, %for.body.lr.ph ], [ %preTableIndex.1, %if.end ]
  %arrayidx = getelementptr inbounds i64, i64* %valueAddresses, i64 %indvars.iv
  %1 = load i64, i64* %arrayidx, align 8, !tbaa !47
  %shr.i = lshr i64 %1, 32
  %conv.i = trunc i64 %shr.i to i32
  %cmp2.not = icmp eq i32 %preTableIndex.024, %conv.i
  br i1 %cmp2.not, label %if.end, label %if.then

if.then:                                          ; preds = %for.body
  %conv = ashr i64 %1, 32
  %add.ptr.i = getelementptr inbounds %class.Column*, %class.Column** %0, i64 %conv
  %2 = load %class.Column*, %class.Column** %add.ptr.i, align 8, !tbaa !2
  %data.i = getelementptr inbounds %class.Column, %class.Column* %2, i64 0, i32 1
  %3 = bitcast i8** %data.i to i32**
  %4 = load i32*, i32** %3, align 8, !tbaa !10
  br label %if.end

if.end:                                           ; preds = %if.then, %for.body
  %preTableIndex.1 = phi i32 [ %conv.i, %if.then ], [ %preTableIndex.024, %for.body ]
  %inputData.1 = phi i32* [ %4, %if.then ], [ %inputData.025, %for.body ]
  %sext = shl i64 %1, 32
  %idxprom5 = ashr exact i64 %sext, 32
  %arrayidx6 = getelementptr inbounds i32, i32* %inputData.1, i64 %idxprom5
  %5 = load i32, i32* %arrayidx6, align 4, !tbaa !6
  %arrayidx8 = getelementptr inbounds i32, i32* %outputData, i64 %indvars.iv
  store i32 %5, i32* %arrayidx8, align 4, !tbaa !6
  %indvars.iv.next = add nuw nsw i64 %indvars.iv, 1
  %exitcond.not = icmp eq i64 %indvars.iv.next, %wide.trip.count
  br i1 %exitcond.not, label %for.cond.cleanup, label %for.body, !llvm.loop !89
}

; Function Attrs: nofree norecurse nounwind uwtable mustprogress
define dso_local void @_Z20setInt64ColumnValuesPljRSt6vectorIP6ColumnSaIS2_EES_(i64* nocapture readonly %valueAddresses, i32 %positionCount, %"class.std::vector"* nocapture nonnull readonly align 8 dereferenceable(24) %inputTable, i64* nocapture %outputData) local_unnamed_addr #10 {
entry:
  %cmp23.not = icmp eq i32 %positionCount, 0
  br i1 %cmp23.not, label %for.cond.cleanup, label %for.body.lr.ph

for.body.lr.ph:                                   ; preds = %entry
  %_M_start.i = getelementptr inbounds %"class.std::vector", %"class.std::vector"* %inputTable, i64 0, i32 0, i32 0, i32 0
  %0 = load %class.Column**, %class.Column*** %_M_start.i, align 8
  %wide.trip.count = zext i32 %positionCount to i64
  br label %for.body

for.cond.cleanup:                                 ; preds = %if.end, %entry
  ret void

for.body:                                         ; preds = %for.body.lr.ph, %if.end
  %indvars.iv = phi i64 [ 0, %for.body.lr.ph ], [ %indvars.iv.next, %if.end ]
  %inputData.025 = phi i64* [ null, %for.body.lr.ph ], [ %inputData.1, %if.end ]
  %preTableIndex.024 = phi i32 [ -1, %for.body.lr.ph ], [ %preTableIndex.1, %if.end ]
  %arrayidx = getelementptr inbounds i64, i64* %valueAddresses, i64 %indvars.iv
  %1 = load i64, i64* %arrayidx, align 8, !tbaa !47
  %shr.i = lshr i64 %1, 32
  %conv.i = trunc i64 %shr.i to i32
  %cmp2.not = icmp eq i32 %preTableIndex.024, %conv.i
  br i1 %cmp2.not, label %if.end, label %if.then

if.then:                                          ; preds = %for.body
  %conv = ashr i64 %1, 32
  %add.ptr.i = getelementptr inbounds %class.Column*, %class.Column** %0, i64 %conv
  %2 = load %class.Column*, %class.Column** %add.ptr.i, align 8, !tbaa !2
  %data.i = getelementptr inbounds %class.Column, %class.Column* %2, i64 0, i32 1
  %3 = bitcast i8** %data.i to i64**
  %4 = load i64*, i64** %3, align 8, !tbaa !10
  br label %if.end

if.end:                                           ; preds = %if.then, %for.body
  %preTableIndex.1 = phi i32 [ %conv.i, %if.then ], [ %preTableIndex.024, %for.body ]
  %inputData.1 = phi i64* [ %4, %if.then ], [ %inputData.025, %for.body ]
  %sext = shl i64 %1, 32
  %idxprom5 = ashr exact i64 %sext, 32
  %arrayidx6 = getelementptr inbounds i64, i64* %inputData.1, i64 %idxprom5
  %5 = load i64, i64* %arrayidx6, align 8, !tbaa !47
  %arrayidx8 = getelementptr inbounds i64, i64* %outputData, i64 %indvars.iv
  store i64 %5, i64* %arrayidx8, align 8, !tbaa !47
  %indvars.iv.next = add nuw nsw i64 %indvars.iv, 1
  %exitcond.not = icmp eq i64 %indvars.iv.next, %wide.trip.count
  br i1 %exitcond.not, label %for.cond.cleanup, label %for.body, !llvm.loop !90
}

; Function Attrs: nofree norecurse nounwind uwtable mustprogress
define dso_local void @_Z21setDoubleColumnValuesPljRSt6vectorIP6ColumnSaIS2_EEPd(i64* nocapture readonly %valueAddresses, i32 %positionCount, %"class.std::vector"* nocapture nonnull readonly align 8 dereferenceable(24) %inputTable, double* nocapture %outputData) local_unnamed_addr #10 {
entry:
  %cmp23.not = icmp eq i32 %positionCount, 0
  br i1 %cmp23.not, label %for.cond.cleanup, label %for.body.lr.ph

for.body.lr.ph:                                   ; preds = %entry
  %_M_start.i = getelementptr inbounds %"class.std::vector", %"class.std::vector"* %inputTable, i64 0, i32 0, i32 0, i32 0
  %0 = load %class.Column**, %class.Column*** %_M_start.i, align 8
  %wide.trip.count = zext i32 %positionCount to i64
  br label %for.body

for.cond.cleanup:                                 ; preds = %if.end, %entry
  ret void

for.body:                                         ; preds = %for.body.lr.ph, %if.end
  %indvars.iv = phi i64 [ 0, %for.body.lr.ph ], [ %indvars.iv.next, %if.end ]
  %inputData.025 = phi double* [ null, %for.body.lr.ph ], [ %inputData.1, %if.end ]
  %preTableIndex.024 = phi i32 [ -1, %for.body.lr.ph ], [ %preTableIndex.1, %if.end ]
  %arrayidx = getelementptr inbounds i64, i64* %valueAddresses, i64 %indvars.iv
  %1 = load i64, i64* %arrayidx, align 8, !tbaa !47
  %shr.i = lshr i64 %1, 32
  %conv.i = trunc i64 %shr.i to i32
  %cmp2.not = icmp eq i32 %preTableIndex.024, %conv.i
  br i1 %cmp2.not, label %if.end, label %if.then

if.then:                                          ; preds = %for.body
  %conv = ashr i64 %1, 32
  %add.ptr.i = getelementptr inbounds %class.Column*, %class.Column** %0, i64 %conv
  %2 = load %class.Column*, %class.Column** %add.ptr.i, align 8, !tbaa !2
  %data.i = getelementptr inbounds %class.Column, %class.Column* %2, i64 0, i32 1
  %3 = bitcast i8** %data.i to double**
  %4 = load double*, double** %3, align 8, !tbaa !10
  br label %if.end

if.end:                                           ; preds = %if.then, %for.body
  %preTableIndex.1 = phi i32 [ %conv.i, %if.then ], [ %preTableIndex.024, %for.body ]
  %inputData.1 = phi double* [ %4, %if.then ], [ %inputData.025, %for.body ]
  %sext = shl i64 %1, 32
  %idxprom5 = ashr exact i64 %sext, 32
  %arrayidx6 = getelementptr inbounds double, double* %inputData.1, i64 %idxprom5
  %5 = load double, double* %arrayidx6, align 8, !tbaa !68
  %arrayidx8 = getelementptr inbounds double, double* %outputData, i64 %indvars.iv
  store double %5, double* %arrayidx8, align 8, !tbaa !68
  %indvars.iv.next = add nuw nsw i64 %indvars.iv, 1
  %exitcond.not = icmp eq i64 %indvars.iv.next, %wide.trip.count
  br i1 %exitcond.not, label %for.cond.cleanup, label %for.body, !llvm.loop !91
}

; Function Attrs: nofree norecurse nounwind uwtable mustprogress
define dso_local void @_Z9getResultlPiilS_j(i64 %pagesIndexAddr, i32* nocapture readonly %outputCols, i32 %outputColsCount, i64 %outputTableAddr, i32* nocapture readonly %sourceTypes, i32 %positionCount) local_unnamed_addr #10 {
entry:
  %0 = inttoptr i64 %pagesIndexAddr to %class.PagesIndex*
  %cmp93.not = icmp eq i32 %outputColsCount, 0
  br i1 %cmp93.not, label %for.cond.cleanup, label %for.body.lr.ph

for.body.lr.ph:                                   ; preds = %entry
  %1 = inttoptr i64 %outputTableAddr to %class.Table*
  %_M_start.i.i89 = getelementptr inbounds %class.Table, %class.Table* %1, i64 0, i32 3, i32 0, i32 0, i32 0
  %2 = load %class.Column**, %class.Column*** %_M_start.i.i89, align 8, !tbaa !26
  %_M_start.i87 = getelementptr inbounds %class.PagesIndex, %class.PagesIndex* %0, i64 0, i32 4, i32 0, i32 0, i32 0
  %3 = load %"class.std::vector"*, %"class.std::vector"** %_M_start.i87, align 8, !tbaa !39
  %_M_start.i = getelementptr inbounds %class.PagesIndex, %class.PagesIndex* %0, i64 0, i32 3, i32 0, i32 0, i32 0
  %cmp23.not.i = icmp eq i32 %positionCount, 0
  %wide.trip.count.i = zext i32 %positionCount to i64
  %wide.trip.count = zext i32 %outputColsCount to i64
  br i1 %cmp23.not.i, label %for.body.us.preheader, label %for.body

for.body.us.preheader:                            ; preds = %for.body.lr.ph
  %4 = add nsw i64 %wide.trip.count, -1
  %xtraiter = and i64 %wide.trip.count, 7
  %5 = icmp ult i64 %4, 7
  br i1 %5, label %for.cond.cleanup.loopexit.unr-lcssa, label %for.body.us.preheader.new

for.body.us.preheader.new:                        ; preds = %for.body.us.preheader
  %unroll_iter = and i64 %wide.trip.count, 4294967288
  br label %for.body.us

for.body.us:                                      ; preds = %for.body.us, %for.body.us.preheader.new
  %niter = phi i64 [ %unroll_iter, %for.body.us.preheader.new ], [ %niter.nsub.7, %for.body.us ]
  %niter.nsub.7 = add i64 %niter, -8
  %niter.ncmp.7 = icmp eq i64 %niter.nsub.7, 0
  br i1 %niter.ncmp.7, label %for.cond.cleanup.loopexit.unr-lcssa, label %for.body.us, !llvm.loop !92

for.cond.cleanup.loopexit.unr-lcssa:              ; preds = %for.body.us, %for.body.us.preheader
  %lcmp.mod.not = icmp eq i64 %xtraiter, 0
  br i1 %lcmp.mod.not, label %for.cond.cleanup, label %for.body.us.epil

for.body.us.epil:                                 ; preds = %for.cond.cleanup.loopexit.unr-lcssa, %for.body.us.epil
  %epil.iter = phi i64 [ %epil.iter.sub, %for.body.us.epil ], [ %xtraiter, %for.cond.cleanup.loopexit.unr-lcssa ]
  %epil.iter.sub = add i64 %epil.iter, -1
  %epil.iter.cmp.not = icmp eq i64 %epil.iter.sub, 0
  br i1 %epil.iter.cmp.not, label %for.cond.cleanup, label %for.body.us.epil, !llvm.loop !93

for.cond.cleanup:                                 ; preds = %sw.epilog, %for.cond.cleanup.loopexit.unr-lcssa, %for.body.us.epil, %entry
  ret void

for.body:                                         ; preds = %for.body.lr.ph, %sw.epilog
  %indvars.iv99 = phi i64 [ %indvars.iv.next100, %sw.epilog ], [ 0, %for.body.lr.ph ]
  %add.ptr.i.i90 = getelementptr inbounds %class.Column*, %class.Column** %2, i64 %indvars.iv99
  %6 = load %class.Column*, %class.Column** %add.ptr.i.i90, align 8, !tbaa !2
  %arrayidx = getelementptr inbounds i32, i32* %outputCols, i64 %indvars.iv99
  %7 = load i32, i32* %arrayidx, align 4, !tbaa !6
  %data.i = getelementptr inbounds %class.Column, %class.Column* %6, i64 0, i32 1
  %8 = load i8*, i8** %data.i, align 8, !tbaa !10
  %idxprom4 = sext i32 %7 to i64
  %arrayidx5 = getelementptr inbounds i32, i32* %sourceTypes, i64 %idxprom4
  %9 = load i32, i32* %arrayidx5, align 4, !tbaa !6
  %add.ptr.i = getelementptr inbounds %"class.std::vector", %"class.std::vector"* %3, i64 %idxprom4
  switch i32 %9, label %sw.epilog [
    i32 1, label %for.body.lr.ph.i64
    i32 2, label %for.body.lr.ph.i38
    i32 3, label %sw.bb10
  ]

for.body.lr.ph.i64:                               ; preds = %for.body
  %10 = load i64*, i64** %_M_start.i, align 8, !tbaa !44
  %11 = bitcast i8* %8 to i32*
  %_M_start.i.i62 = getelementptr inbounds %"class.std::vector", %"class.std::vector"* %add.ptr.i, i64 0, i32 0, i32 0, i32 0
  %12 = load %class.Column**, %class.Column*** %_M_start.i.i62, align 8
  br label %for.body.i72

for.body.i72:                                     ; preds = %if.end.i85, %for.body.lr.ph.i64
  %indvars.iv.i65 = phi i64 [ 0, %for.body.lr.ph.i64 ], [ %indvars.iv.next.i83, %if.end.i85 ]
  %inputData.025.i66 = phi i32* [ null, %for.body.lr.ph.i64 ], [ %inputData.1.i78, %if.end.i85 ]
  %preTableIndex.024.i67 = phi i32 [ -1, %for.body.lr.ph.i64 ], [ %preTableIndex.1.i77, %if.end.i85 ]
  %arrayidx.i68 = getelementptr inbounds i64, i64* %10, i64 %indvars.iv.i65
  %13 = load i64, i64* %arrayidx.i68, align 8, !tbaa !47
  %shr.i.i69 = lshr i64 %13, 32
  %conv.i.i70 = trunc i64 %shr.i.i69 to i32
  %cmp2.not.i71 = icmp eq i32 %preTableIndex.024.i67, %conv.i.i70
  br i1 %cmp2.not.i71, label %if.end.i85, label %if.then.i76

if.then.i76:                                      ; preds = %for.body.i72
  %conv.i73 = ashr i64 %13, 32
  %add.ptr.i.i74 = getelementptr inbounds %class.Column*, %class.Column** %12, i64 %conv.i73
  %14 = load %class.Column*, %class.Column** %add.ptr.i.i74, align 8, !tbaa !2
  %data.i.i75 = getelementptr inbounds %class.Column, %class.Column* %14, i64 0, i32 1
  %15 = bitcast i8** %data.i.i75 to i32**
  %16 = load i32*, i32** %15, align 8, !tbaa !10
  br label %if.end.i85

if.end.i85:                                       ; preds = %if.then.i76, %for.body.i72
  %preTableIndex.1.i77 = phi i32 [ %conv.i.i70, %if.then.i76 ], [ %preTableIndex.024.i67, %for.body.i72 ]
  %inputData.1.i78 = phi i32* [ %16, %if.then.i76 ], [ %inputData.025.i66, %for.body.i72 ]
  %sext.i79 = shl i64 %13, 32
  %idxprom5.i80 = ashr exact i64 %sext.i79, 32
  %arrayidx6.i81 = getelementptr inbounds i32, i32* %inputData.1.i78, i64 %idxprom5.i80
  %17 = load i32, i32* %arrayidx6.i81, align 4, !tbaa !6
  %arrayidx8.i82 = getelementptr inbounds i32, i32* %11, i64 %indvars.iv.i65
  store i32 %17, i32* %arrayidx8.i82, align 4, !tbaa !6
  %indvars.iv.next.i83 = add nuw nsw i64 %indvars.iv.i65, 1
  %exitcond.not.i84 = icmp eq i64 %indvars.iv.next.i83, %wide.trip.count.i
  br i1 %exitcond.not.i84, label %sw.epilog, label %for.body.i72, !llvm.loop !89

for.body.lr.ph.i38:                               ; preds = %for.body
  %18 = load i64*, i64** %_M_start.i, align 8, !tbaa !44
  %19 = bitcast i8* %8 to i64*
  %_M_start.i.i36 = getelementptr inbounds %"class.std::vector", %"class.std::vector"* %add.ptr.i, i64 0, i32 0, i32 0, i32 0
  %20 = load %class.Column**, %class.Column*** %_M_start.i.i36, align 8
  br label %for.body.i46

for.body.i46:                                     ; preds = %if.end.i59, %for.body.lr.ph.i38
  %indvars.iv.i39 = phi i64 [ 0, %for.body.lr.ph.i38 ], [ %indvars.iv.next.i57, %if.end.i59 ]
  %inputData.025.i40 = phi i64* [ null, %for.body.lr.ph.i38 ], [ %inputData.1.i52, %if.end.i59 ]
  %preTableIndex.024.i41 = phi i32 [ -1, %for.body.lr.ph.i38 ], [ %preTableIndex.1.i51, %if.end.i59 ]
  %arrayidx.i42 = getelementptr inbounds i64, i64* %18, i64 %indvars.iv.i39
  %21 = load i64, i64* %arrayidx.i42, align 8, !tbaa !47
  %shr.i.i43 = lshr i64 %21, 32
  %conv.i.i44 = trunc i64 %shr.i.i43 to i32
  %cmp2.not.i45 = icmp eq i32 %preTableIndex.024.i41, %conv.i.i44
  br i1 %cmp2.not.i45, label %if.end.i59, label %if.then.i50

if.then.i50:                                      ; preds = %for.body.i46
  %conv.i47 = ashr i64 %21, 32
  %add.ptr.i.i48 = getelementptr inbounds %class.Column*, %class.Column** %20, i64 %conv.i47
  %22 = load %class.Column*, %class.Column** %add.ptr.i.i48, align 8, !tbaa !2
  %data.i.i49 = getelementptr inbounds %class.Column, %class.Column* %22, i64 0, i32 1
  %23 = bitcast i8** %data.i.i49 to i64**
  %24 = load i64*, i64** %23, align 8, !tbaa !10
  br label %if.end.i59

if.end.i59:                                       ; preds = %if.then.i50, %for.body.i46
  %preTableIndex.1.i51 = phi i32 [ %conv.i.i44, %if.then.i50 ], [ %preTableIndex.024.i41, %for.body.i46 ]
  %inputData.1.i52 = phi i64* [ %24, %if.then.i50 ], [ %inputData.025.i40, %for.body.i46 ]
  %sext.i53 = shl i64 %21, 32
  %idxprom5.i54 = ashr exact i64 %sext.i53, 32
  %arrayidx6.i55 = getelementptr inbounds i64, i64* %inputData.1.i52, i64 %idxprom5.i54
  %25 = load i64, i64* %arrayidx6.i55, align 8, !tbaa !47
  %arrayidx8.i56 = getelementptr inbounds i64, i64* %19, i64 %indvars.iv.i39
  store i64 %25, i64* %arrayidx8.i56, align 8, !tbaa !47
  %indvars.iv.next.i57 = add nuw nsw i64 %indvars.iv.i39, 1
  %exitcond.not.i58 = icmp eq i64 %indvars.iv.next.i57, %wide.trip.count.i
  br i1 %exitcond.not.i58, label %sw.epilog, label %for.body.i46, !llvm.loop !90

sw.bb10:                                          ; preds = %for.body
  %26 = load i64*, i64** %_M_start.i, align 8, !tbaa !44
  %27 = bitcast i8* %8 to double*
  %_M_start.i.i = getelementptr inbounds %"class.std::vector", %"class.std::vector"* %add.ptr.i, i64 0, i32 0, i32 0, i32 0
  %28 = load %class.Column**, %class.Column*** %_M_start.i.i, align 8
  br label %for.body.i

for.body.i:                                       ; preds = %if.end.i, %sw.bb10
  %indvars.iv.i = phi i64 [ 0, %sw.bb10 ], [ %indvars.iv.next.i, %if.end.i ]
  %inputData.025.i = phi double* [ null, %sw.bb10 ], [ %inputData.1.i, %if.end.i ]
  %preTableIndex.024.i = phi i32 [ -1, %sw.bb10 ], [ %preTableIndex.1.i, %if.end.i ]
  %arrayidx.i = getelementptr inbounds i64, i64* %26, i64 %indvars.iv.i
  %29 = load i64, i64* %arrayidx.i, align 8, !tbaa !47
  %shr.i.i = lshr i64 %29, 32
  %conv.i.i = trunc i64 %shr.i.i to i32
  %cmp2.not.i = icmp eq i32 %preTableIndex.024.i, %conv.i.i
  br i1 %cmp2.not.i, label %if.end.i, label %if.then.i

if.then.i:                                        ; preds = %for.body.i
  %conv.i = ashr i64 %29, 32
  %add.ptr.i.i = getelementptr inbounds %class.Column*, %class.Column** %28, i64 %conv.i
  %30 = load %class.Column*, %class.Column** %add.ptr.i.i, align 8, !tbaa !2
  %data.i.i = getelementptr inbounds %class.Column, %class.Column* %30, i64 0, i32 1
  %31 = bitcast i8** %data.i.i to double**
  %32 = load double*, double** %31, align 8, !tbaa !10
  br label %if.end.i

if.end.i:                                         ; preds = %if.then.i, %for.body.i
  %preTableIndex.1.i = phi i32 [ %conv.i.i, %if.then.i ], [ %preTableIndex.024.i, %for.body.i ]
  %inputData.1.i = phi double* [ %32, %if.then.i ], [ %inputData.025.i, %for.body.i ]
  %sext.i = shl i64 %29, 32
  %idxprom5.i = ashr exact i64 %sext.i, 32
  %arrayidx6.i = getelementptr inbounds double, double* %inputData.1.i, i64 %idxprom5.i
  %33 = load double, double* %arrayidx6.i, align 8, !tbaa !68
  %arrayidx8.i = getelementptr inbounds double, double* %27, i64 %indvars.iv.i
  store double %33, double* %arrayidx8.i, align 8, !tbaa !68
  %indvars.iv.next.i = add nuw nsw i64 %indvars.iv.i, 1
  %exitcond.not.i = icmp eq i64 %indvars.iv.next.i, %wide.trip.count.i
  br i1 %exitcond.not.i, label %sw.epilog, label %for.body.i, !llvm.loop !91

sw.epilog:                                        ; preds = %if.end.i, %if.end.i59, %if.end.i85, %for.body
  %indvars.iv.next100 = add nuw nsw i64 %indvars.iv99, 1
  %exitcond102.not = icmp eq i64 %indvars.iv.next100, %wide.trip.count
  br i1 %exitcond102.not, label %for.cond.cleanup, label %for.body, !llvm.loop !92
}

; Function Attrs: nounwind uwtable willreturn mustprogress
define linkonce_odr dso_local %class.Table* @_ZN4Sort9getResultEv(%class.Sort* nonnull dereferenceable(80) %this) unnamed_addr #13 comdat align 2 {
entry:
  unreachable
}

; Function Attrs: nounwind uwtable willreturn
define linkonce_odr dso_local void @_ZN6ColumnD2Ev(%class.Column* nonnull dereferenceable(40) %this) unnamed_addr #14 comdat align 2 {
entry:
  ret void
}

; Function Attrs: nounwind uwtable
define linkonce_odr dso_local void @_ZN6ColumnD0Ev(%class.Column* nonnull dereferenceable(40) %this) unnamed_addr #8 comdat align 2 {
entry:
  %0 = bitcast %class.Column* %this to i8*
  tail call void @_ZdlPv(i8* nonnull %0) #21
  ret void
}

; Function Attrs: noinline noreturn nounwind
define linkonce_odr hidden void @__clang_call_terminate(i8* %0) local_unnamed_addr #15 comdat {
  %2 = tail call i8* @__cxa_begin_catch(i8* %0) #18
  tail call void @_ZSt9terminatev() #22
  unreachable
}

declare dso_local i8* @__cxa_begin_catch(i8*) local_unnamed_addr

declare dso_local void @_ZSt9terminatev() local_unnamed_addr

declare dso_local void @__cxa_rethrow() local_unnamed_addr

declare dso_local void @__cxa_end_catch() local_unnamed_addr

; Function Attrs: noreturn
declare dso_local void @_ZSt17__throw_bad_allocv() local_unnamed_addr #16

; Function Attrs: argmemonly nofree nosync nounwind willreturn
declare void @llvm.memmove.p0i8.p0i8.i64(i8* nocapture writeonly, i8* nocapture readonly, i64, i1 immarg) #5

; Function Attrs: uwtable
define linkonce_odr dso_local void @_ZNSt6vectorIS_IP6ColumnSaIS1_EESaIS3_EE17_M_realloc_insertIJRKS3_EEEvN9__gnu_cxx17__normal_iteratorIPS3_S5_EEDpOT_(%"class.std::vector.5"* nonnull dereferenceable(24) %this, %"class.std::vector"* %__position.coerce, %"class.std::vector"* nonnull align 8 dereferenceable(24) %__args) local_unnamed_addr #4 comdat align 2 personality i8* bitcast (i32 (...)* @__gxx_personality_v0 to i8*) {
entry:
  %_M_finish.i26.i = getelementptr inbounds %"class.std::vector.5", %"class.std::vector.5"* %this, i64 0, i32 0, i32 0, i32 1
  %0 = load %"class.std::vector"*, %"class.std::vector"** %_M_finish.i26.i, align 8, !tbaa !42
  %_M_start.i27.i = getelementptr inbounds %"class.std::vector.5", %"class.std::vector.5"* %this, i64 0, i32 0, i32 0, i32 0
  %1 = load %"class.std::vector"*, %"class.std::vector"** %_M_start.i27.i, align 8, !tbaa !39
  %sub.ptr.lhs.cast.i28.i = ptrtoint %"class.std::vector"* %0 to i64
  %sub.ptr.rhs.cast.i29.i = ptrtoint %"class.std::vector"* %1 to i64
  %sub.ptr.sub.i30.i = sub i64 %sub.ptr.lhs.cast.i28.i, %sub.ptr.rhs.cast.i29.i
  %sub.ptr.div.i31.i = sdiv exact i64 %sub.ptr.sub.i30.i, 24
  %2 = icmp eq i64 %sub.ptr.sub.i30.i, 0
  %.sroa.speculated.i = select i1 %2, i64 1, i64 %sub.ptr.div.i31.i
  %add.i = add nsw i64 %.sroa.speculated.i, %sub.ptr.div.i31.i
  %cmp7.i = icmp ult i64 %add.i, %sub.ptr.div.i31.i
  %cmp9.i = icmp ugt i64 %add.i, 768614336404564650
  %or.cond.i = or i1 %cmp7.i, %cmp9.i
  %cond.i = select i1 %or.cond.i, i64 768614336404564650, i64 %add.i
  %sub.ptr.lhs.cast.i = ptrtoint %"class.std::vector"* %__position.coerce to i64
  %sub.ptr.sub.i = sub i64 %sub.ptr.lhs.cast.i, %sub.ptr.rhs.cast.i29.i
  %sub.ptr.div.i = sdiv exact i64 %sub.ptr.sub.i, 24
  %cmp.not.i = icmp eq i64 %cond.i, 0
  br i1 %cmp.not.i, label %_ZNSt12_Vector_baseISt6vectorIP6ColumnSaIS2_EESaIS4_EE11_M_allocateEm.exit, label %_ZNSt16allocator_traitsISaISt6vectorIP6ColumnSaIS2_EEEE8allocateERS5_m.exit.i

_ZNSt16allocator_traitsISaISt6vectorIP6ColumnSaIS2_EEEE8allocateERS5_m.exit.i: ; preds = %entry
  %mul.i.i.i = mul nuw i64 %cond.i, 24
  %call2.i.i.i = tail call noalias nonnull i8* @_Znwm(i64 %mul.i.i.i) #20
  %3 = bitcast i8* %call2.i.i.i to %"class.std::vector"*
  br label %_ZNSt12_Vector_baseISt6vectorIP6ColumnSaIS2_EESaIS4_EE11_M_allocateEm.exit

_ZNSt12_Vector_baseISt6vectorIP6ColumnSaIS2_EESaIS4_EE11_M_allocateEm.exit: ; preds = %entry, %_ZNSt16allocator_traitsISaISt6vectorIP6ColumnSaIS2_EEEE8allocateERS5_m.exit.i
  %cond.i67 = phi %"class.std::vector"* [ %3, %_ZNSt16allocator_traitsISaISt6vectorIP6ColumnSaIS2_EEEE8allocateERS5_m.exit.i ], [ null, %entry ]
  %add.ptr = getelementptr inbounds %"class.std::vector", %"class.std::vector"* %cond.i67, i64 %sub.ptr.div.i
  %_M_finish.i.i.i.i = getelementptr inbounds %"class.std::vector", %"class.std::vector"* %__args, i64 0, i32 0, i32 0, i32 1
  %4 = load %class.Column**, %class.Column*** %_M_finish.i.i.i.i, align 8, !tbaa !22
  %_M_start.i.i.i.i = getelementptr inbounds %"class.std::vector", %"class.std::vector"* %__args, i64 0, i32 0, i32 0, i32 0
  %5 = load %class.Column**, %class.Column*** %_M_start.i.i.i.i, align 8, !tbaa !26
  %sub.ptr.lhs.cast.i.i.i.i = ptrtoint %class.Column** %4 to i64
  %sub.ptr.rhs.cast.i.i.i.i = ptrtoint %class.Column** %5 to i64
  %sub.ptr.sub.i.i.i.i = sub i64 %sub.ptr.lhs.cast.i.i.i.i, %sub.ptr.rhs.cast.i.i.i.i
  %sub.ptr.div.i.i.i.i = ashr exact i64 %sub.ptr.sub.i.i.i.i, 3
  %6 = bitcast %"class.std::vector"* %add.ptr to i8*
  tail call void @llvm.memset.p0i8.i64(i8* nonnull align 8 dereferenceable(24) %6, i8 0, i64 24, i1 false) #18
  %cmp.not.i.i.i.i.i.i = icmp eq i64 %sub.ptr.sub.i.i.i.i, 0
  br i1 %cmp.not.i.i.i.i.i.i, label %invoke.cont.i.i.i, label %cond.true.i.i.i.i.i.i

cond.true.i.i.i.i.i.i:                            ; preds = %_ZNSt12_Vector_baseISt6vectorIP6ColumnSaIS2_EESaIS4_EE11_M_allocateEm.exit
  %cmp.i.i.i.i.i.i.i.i = icmp slt i64 %sub.ptr.sub.i.i.i.i, 0
  br i1 %cmp.i.i.i.i.i.i.i.i, label %if.then.i.i.i.i.i.i.i.i, label %_ZNSt16allocator_traitsISaIP6ColumnEE8allocateERS2_m.exit.i.i.i.i.i.i

if.then.i.i.i.i.i.i.i.i:                          ; preds = %cond.true.i.i.i.i.i.i
  invoke void @_ZSt17__throw_bad_allocv() #23
          to label %.noexc unwind label %if.else

.noexc:                                           ; preds = %if.then.i.i.i.i.i.i.i.i
  unreachable

_ZNSt16allocator_traitsISaIP6ColumnEE8allocateERS2_m.exit.i.i.i.i.i.i: ; preds = %cond.true.i.i.i.i.i.i
  %call2.i.i.i.i3.i22.i.i.i68 = invoke noalias nonnull i8* @_Znwm(i64 %sub.ptr.sub.i.i.i.i) #20
          to label %call2.i.i.i.i3.i22.i.i.i.noexc unwind label %if.else

call2.i.i.i.i3.i22.i.i.i.noexc:                   ; preds = %_ZNSt16allocator_traitsISaIP6ColumnEE8allocateERS2_m.exit.i.i.i.i.i.i
  %7 = bitcast i8* %call2.i.i.i.i3.i22.i.i.i68 to %class.Column**
  %.pre = load %class.Column**, %class.Column*** %_M_start.i.i.i.i, align 8, !tbaa !2
  %.pre104 = load %class.Column**, %class.Column*** %_M_finish.i.i.i.i, align 8, !tbaa !2
  %.pre106 = ptrtoint %class.Column** %.pre104 to i64
  %.pre107 = ptrtoint %class.Column** %.pre to i64
  %.pre108 = sub i64 %.pre106, %.pre107
  br label %invoke.cont.i.i.i

invoke.cont.i.i.i:                                ; preds = %call2.i.i.i.i3.i22.i.i.i.noexc, %_ZNSt12_Vector_baseISt6vectorIP6ColumnSaIS2_EESaIS4_EE11_M_allocateEm.exit
  %sub.ptr.sub.i.i.i.i.i.i.i.i.i.i.pre-phi = phi i64 [ %.pre108, %call2.i.i.i.i3.i22.i.i.i.noexc ], [ 0, %_ZNSt12_Vector_baseISt6vectorIP6ColumnSaIS2_EESaIS4_EE11_M_allocateEm.exit ]
  %8 = phi %class.Column** [ %.pre, %call2.i.i.i.i3.i22.i.i.i.noexc ], [ %5, %_ZNSt12_Vector_baseISt6vectorIP6ColumnSaIS2_EESaIS4_EE11_M_allocateEm.exit ]
  %cond.i.i.i.i.i.i = phi %class.Column** [ %7, %call2.i.i.i.i3.i22.i.i.i.noexc ], [ null, %_ZNSt12_Vector_baseISt6vectorIP6ColumnSaIS2_EESaIS4_EE11_M_allocateEm.exit ]
  %_M_start.i.i.i.i.i = getelementptr inbounds %"class.std::vector", %"class.std::vector"* %add.ptr, i64 0, i32 0, i32 0, i32 0
  store %class.Column** %cond.i.i.i.i.i.i, %class.Column*** %_M_start.i.i.i.i.i, align 8, !tbaa !26
  %_M_finish.i.i.i.i.i = getelementptr inbounds %"class.std::vector", %"class.std::vector"* %cond.i67, i64 %sub.ptr.div.i, i32 0, i32 0, i32 1
  store %class.Column** %cond.i.i.i.i.i.i, %class.Column*** %_M_finish.i.i.i.i.i, align 8, !tbaa !22
  %add.ptr.i.i.i.i.i = getelementptr inbounds %class.Column*, %class.Column** %cond.i.i.i.i.i.i, i64 %sub.ptr.div.i.i.i.i
  %_M_end_of_storage.i.i.i.i.i = getelementptr inbounds %"class.std::vector", %"class.std::vector"* %cond.i67, i64 %sub.ptr.div.i, i32 0, i32 0, i32 2
  store %class.Column** %add.ptr.i.i.i.i.i, %class.Column*** %_M_end_of_storage.i.i.i.i.i, align 8, !tbaa !25
  %tobool.not.i.i.i.i.i.i.i.i.i.i = icmp eq i64 %sub.ptr.sub.i.i.i.i.i.i.i.i.i.i.pre-phi, 0
  br i1 %tobool.not.i.i.i.i.i.i.i.i.i.i, label %invoke.cont, label %if.then.i.i.i.i.i.i.i.i.i.i

if.then.i.i.i.i.i.i.i.i.i.i:                      ; preds = %invoke.cont.i.i.i
  %9 = bitcast %class.Column** %cond.i.i.i.i.i.i to i8*
  %10 = bitcast %class.Column** %8 to i8*
  tail call void @llvm.memmove.p0i8.p0i8.i64(i8* align 8 %9, i8* align 8 %10, i64 %sub.ptr.sub.i.i.i.i.i.i.i.i.i.i.pre-phi, i1 false) #18
  br label %invoke.cont

invoke.cont:                                      ; preds = %if.then.i.i.i.i.i.i.i.i.i.i, %invoke.cont.i.i.i
  %sub.ptr.div.i.i.i.i.i.i.i.i.i.i = ashr exact i64 %sub.ptr.sub.i.i.i.i.i.i.i.i.i.i.pre-phi, 3
  %add.ptr.i.i.i.i.i.i.i.i.i.i = getelementptr inbounds %class.Column*, %class.Column** %cond.i.i.i.i.i.i, i64 %sub.ptr.div.i.i.i.i.i.i.i.i.i.i
  store %class.Column** %add.ptr.i.i.i.i.i.i.i.i.i.i, %class.Column*** %_M_finish.i.i.i.i.i, align 8, !tbaa !22
  %11 = load %"class.std::vector"*, %"class.std::vector"** %_M_start.i27.i, align 8, !tbaa !39
  %cmp.i.i.not22.i.i.i.i = icmp eq %"class.std::vector"* %11, %__position.coerce
  br i1 %cmp.i.i.not22.i.i.i.i, label %invoke.cont10, label %for.body.i.i.i.i

for.body.i.i.i.i:                                 ; preds = %invoke.cont, %for.body.i.i.i.i
  %__cur.024.i.i.i.i = phi %"class.std::vector"* [ %incdec.ptr.i.i.i.i, %for.body.i.i.i.i ], [ %cond.i67, %invoke.cont ]
  %__first.sroa.0.023.i.i.i.i = phi %"class.std::vector"* [ %incdec.ptr.i.i.i.i.i, %for.body.i.i.i.i ], [ %11, %invoke.cont ]
  %12 = bitcast %"class.std::vector"* %__cur.024.i.i.i.i to i8*
  tail call void @llvm.memset.p0i8.i64(i8* nonnull align 8 dereferenceable(24) %12, i8 0, i64 24, i1 false) #18
  %_M_start.i.i.i.i.i.i.i.i = getelementptr inbounds %"class.std::vector", %"class.std::vector"* %__cur.024.i.i.i.i, i64 0, i32 0, i32 0, i32 0
  %_M_start2.i.i.i.i.i.i.i.i = getelementptr inbounds %"class.std::vector", %"class.std::vector"* %__first.sroa.0.023.i.i.i.i, i64 0, i32 0, i32 0, i32 0
  %13 = load %class.Column**, %class.Column*** %_M_start2.i.i.i.i.i.i.i.i, align 8, !tbaa !2
  store %class.Column** %13, %class.Column*** %_M_start.i.i.i.i.i.i.i.i, align 8, !tbaa !2
  store %class.Column** null, %class.Column*** %_M_start2.i.i.i.i.i.i.i.i, align 8, !tbaa !2
  %_M_finish.i.i.i.i.i.i.i.i = getelementptr inbounds %"class.std::vector", %"class.std::vector"* %__cur.024.i.i.i.i, i64 0, i32 0, i32 0, i32 1
  %_M_finish3.i.i.i.i.i.i.i.i = getelementptr inbounds %"class.std::vector", %"class.std::vector"* %__first.sroa.0.023.i.i.i.i, i64 0, i32 0, i32 0, i32 1
  %14 = load %class.Column**, %class.Column*** %_M_finish.i.i.i.i.i.i.i.i, align 8, !tbaa !2
  %15 = load %class.Column**, %class.Column*** %_M_finish3.i.i.i.i.i.i.i.i, align 8, !tbaa !2
  store %class.Column** %15, %class.Column*** %_M_finish.i.i.i.i.i.i.i.i, align 8, !tbaa !2
  store %class.Column** %14, %class.Column*** %_M_finish3.i.i.i.i.i.i.i.i, align 8, !tbaa !2
  %_M_end_of_storage.i.i.i.i.i.i.i.i = getelementptr inbounds %"class.std::vector", %"class.std::vector"* %__cur.024.i.i.i.i, i64 0, i32 0, i32 0, i32 2
  %_M_end_of_storage4.i.i.i.i.i.i.i.i = getelementptr inbounds %"class.std::vector", %"class.std::vector"* %__first.sroa.0.023.i.i.i.i, i64 0, i32 0, i32 0, i32 2
  %16 = load %class.Column**, %class.Column*** %_M_end_of_storage.i.i.i.i.i.i.i.i, align 8, !tbaa !2
  %17 = load %class.Column**, %class.Column*** %_M_end_of_storage4.i.i.i.i.i.i.i.i, align 8, !tbaa !2
  store %class.Column** %17, %class.Column*** %_M_end_of_storage.i.i.i.i.i.i.i.i, align 8, !tbaa !2
  store %class.Column** %16, %class.Column*** %_M_end_of_storage4.i.i.i.i.i.i.i.i, align 8, !tbaa !2
  %incdec.ptr.i.i.i.i.i = getelementptr inbounds %"class.std::vector", %"class.std::vector"* %__first.sroa.0.023.i.i.i.i, i64 1
  %incdec.ptr.i.i.i.i = getelementptr inbounds %"class.std::vector", %"class.std::vector"* %__cur.024.i.i.i.i, i64 1
  %cmp.i.i.not.i.i.i.i = icmp eq %"class.std::vector"* %incdec.ptr.i.i.i.i.i, %__position.coerce
  br i1 %cmp.i.i.not.i.i.i.i, label %invoke.cont10, label %for.body.i.i.i.i, !llvm.loop !95

invoke.cont10:                                    ; preds = %for.body.i.i.i.i, %invoke.cont
  %__cur.0.lcssa.i.i.i.i = phi %"class.std::vector"* [ %cond.i67, %invoke.cont ], [ %incdec.ptr.i.i.i.i, %for.body.i.i.i.i ]
  %incdec.ptr = getelementptr inbounds %"class.std::vector", %"class.std::vector"* %__cur.0.lcssa.i.i.i.i, i64 1
  %18 = load %"class.std::vector"*, %"class.std::vector"** %_M_finish.i26.i, align 8, !tbaa !42
  %cmp.i.i.not22.i.i.i.i84 = icmp eq %"class.std::vector"* %18, %__position.coerce
  br i1 %cmp.i.i.not22.i.i.i.i84, label %invoke.cont15, label %for.body.i.i.i.i96

for.body.i.i.i.i96:                               ; preds = %invoke.cont10, %for.body.i.i.i.i96
  %__cur.024.i.i.i.i85 = phi %"class.std::vector"* [ %incdec.ptr.i.i.i.i94, %for.body.i.i.i.i96 ], [ %incdec.ptr, %invoke.cont10 ]
  %__first.sroa.0.023.i.i.i.i86 = phi %"class.std::vector"* [ %incdec.ptr.i.i.i.i.i93, %for.body.i.i.i.i96 ], [ %__position.coerce, %invoke.cont10 ]
  %19 = bitcast %"class.std::vector"* %__cur.024.i.i.i.i85 to i8*
  tail call void @llvm.memset.p0i8.i64(i8* nonnull align 8 dereferenceable(24) %19, i8 0, i64 24, i1 false) #18
  %_M_start.i.i.i.i.i.i.i.i87 = getelementptr inbounds %"class.std::vector", %"class.std::vector"* %__cur.024.i.i.i.i85, i64 0, i32 0, i32 0, i32 0
  %_M_start2.i.i.i.i.i.i.i.i88 = getelementptr inbounds %"class.std::vector", %"class.std::vector"* %__first.sroa.0.023.i.i.i.i86, i64 0, i32 0, i32 0, i32 0
  %20 = load %class.Column**, %class.Column*** %_M_start2.i.i.i.i.i.i.i.i88, align 8, !tbaa !2
  store %class.Column** %20, %class.Column*** %_M_start.i.i.i.i.i.i.i.i87, align 8, !tbaa !2
  store %class.Column** null, %class.Column*** %_M_start2.i.i.i.i.i.i.i.i88, align 8, !tbaa !2
  %_M_finish.i.i.i.i.i.i.i.i89 = getelementptr inbounds %"class.std::vector", %"class.std::vector"* %__cur.024.i.i.i.i85, i64 0, i32 0, i32 0, i32 1
  %_M_finish3.i.i.i.i.i.i.i.i90 = getelementptr inbounds %"class.std::vector", %"class.std::vector"* %__first.sroa.0.023.i.i.i.i86, i64 0, i32 0, i32 0, i32 1
  %21 = load %class.Column**, %class.Column*** %_M_finish.i.i.i.i.i.i.i.i89, align 8, !tbaa !2
  %22 = load %class.Column**, %class.Column*** %_M_finish3.i.i.i.i.i.i.i.i90, align 8, !tbaa !2
  store %class.Column** %22, %class.Column*** %_M_finish.i.i.i.i.i.i.i.i89, align 8, !tbaa !2
  store %class.Column** %21, %class.Column*** %_M_finish3.i.i.i.i.i.i.i.i90, align 8, !tbaa !2
  %_M_end_of_storage.i.i.i.i.i.i.i.i91 = getelementptr inbounds %"class.std::vector", %"class.std::vector"* %__cur.024.i.i.i.i85, i64 0, i32 0, i32 0, i32 2
  %_M_end_of_storage4.i.i.i.i.i.i.i.i92 = getelementptr inbounds %"class.std::vector", %"class.std::vector"* %__first.sroa.0.023.i.i.i.i86, i64 0, i32 0, i32 0, i32 2
  %23 = load %class.Column**, %class.Column*** %_M_end_of_storage.i.i.i.i.i.i.i.i91, align 8, !tbaa !2
  %24 = load %class.Column**, %class.Column*** %_M_end_of_storage4.i.i.i.i.i.i.i.i92, align 8, !tbaa !2
  store %class.Column** %24, %class.Column*** %_M_end_of_storage.i.i.i.i.i.i.i.i91, align 8, !tbaa !2
  store %class.Column** %23, %class.Column*** %_M_end_of_storage4.i.i.i.i.i.i.i.i92, align 8, !tbaa !2
  %incdec.ptr.i.i.i.i.i93 = getelementptr inbounds %"class.std::vector", %"class.std::vector"* %__first.sroa.0.023.i.i.i.i86, i64 1
  %incdec.ptr.i.i.i.i94 = getelementptr inbounds %"class.std::vector", %"class.std::vector"* %__cur.024.i.i.i.i85, i64 1
  %cmp.i.i.not.i.i.i.i95 = icmp eq %"class.std::vector"* %incdec.ptr.i.i.i.i.i93, %18
  br i1 %cmp.i.i.not.i.i.i.i95, label %invoke.cont15.loopexit, label %for.body.i.i.i.i96, !llvm.loop !95

invoke.cont15.loopexit:                           ; preds = %for.body.i.i.i.i96
  %.pre105 = load %"class.std::vector"*, %"class.std::vector"** %_M_finish.i26.i, align 8, !tbaa !42
  br label %invoke.cont15

invoke.cont15:                                    ; preds = %invoke.cont15.loopexit, %invoke.cont10
  %25 = phi %"class.std::vector"* [ %__position.coerce, %invoke.cont10 ], [ %.pre105, %invoke.cont15.loopexit ]
  %__cur.0.lcssa.i.i.i.i97 = phi %"class.std::vector"* [ %incdec.ptr, %invoke.cont10 ], [ %incdec.ptr.i.i.i.i94, %invoke.cont15.loopexit ]
  %26 = load %"class.std::vector"*, %"class.std::vector"** %_M_start.i27.i, align 8, !tbaa !39
  %cmp.not3.i.i.i74 = icmp eq %"class.std::vector"* %26, %25
  br i1 %cmp.not3.i.i.i74, label %_ZSt8_DestroyIPSt6vectorIP6ColumnSaIS2_EES4_EvT_S6_RSaIT0_E.exit83, label %for.body.i.i.i78

for.body.i.i.i78:                                 ; preds = %invoke.cont15, %_ZSt8_DestroyISt6vectorIP6ColumnSaIS2_EEEvPT_.exit.i.i.i82
  %__first.addr.04.i.i.i75 = phi %"class.std::vector"* [ %incdec.ptr.i.i.i80, %_ZSt8_DestroyISt6vectorIP6ColumnSaIS2_EEEvPT_.exit.i.i.i82 ], [ %26, %invoke.cont15 ]
  %_M_start.i.i.i.i.i.i76 = getelementptr inbounds %"class.std::vector", %"class.std::vector"* %__first.addr.04.i.i.i75, i64 0, i32 0, i32 0, i32 0
  %27 = load %class.Column**, %class.Column*** %_M_start.i.i.i.i.i.i76, align 8, !tbaa !26
  %tobool.not.i.i.i.i.i.i.i77 = icmp eq %class.Column** %27, null
  br i1 %tobool.not.i.i.i.i.i.i.i77, label %_ZSt8_DestroyISt6vectorIP6ColumnSaIS2_EEEvPT_.exit.i.i.i82, label %if.then.i.i.i.i.i.i.i79

if.then.i.i.i.i.i.i.i79:                          ; preds = %for.body.i.i.i78
  %28 = bitcast %class.Column** %27 to i8*
  tail call void @_ZdlPv(i8* nonnull %28) #18
  br label %_ZSt8_DestroyISt6vectorIP6ColumnSaIS2_EEEvPT_.exit.i.i.i82

_ZSt8_DestroyISt6vectorIP6ColumnSaIS2_EEEvPT_.exit.i.i.i82: ; preds = %if.then.i.i.i.i.i.i.i79, %for.body.i.i.i78
  %incdec.ptr.i.i.i80 = getelementptr inbounds %"class.std::vector", %"class.std::vector"* %__first.addr.04.i.i.i75, i64 1
  %cmp.not.i.i.i81 = icmp eq %"class.std::vector"* %incdec.ptr.i.i.i80, %25
  br i1 %cmp.not.i.i.i81, label %_ZSt8_DestroyIPSt6vectorIP6ColumnSaIS2_EES4_EvT_S6_RSaIT0_E.exit83thread-pre-split, label %for.body.i.i.i78, !llvm.loop !43

_ZSt8_DestroyIPSt6vectorIP6ColumnSaIS2_EES4_EvT_S6_RSaIT0_E.exit83thread-pre-split: ; preds = %_ZSt8_DestroyISt6vectorIP6ColumnSaIS2_EEEvPT_.exit.i.i.i82
  %.pr = load %"class.std::vector"*, %"class.std::vector"** %_M_start.i27.i, align 8, !tbaa !39
  br label %_ZSt8_DestroyIPSt6vectorIP6ColumnSaIS2_EES4_EvT_S6_RSaIT0_E.exit83

_ZSt8_DestroyIPSt6vectorIP6ColumnSaIS2_EES4_EvT_S6_RSaIT0_E.exit83: ; preds = %_ZSt8_DestroyIPSt6vectorIP6ColumnSaIS2_EES4_EvT_S6_RSaIT0_E.exit83thread-pre-split, %invoke.cont15
  %29 = phi %"class.std::vector"* [ %.pr, %_ZSt8_DestroyIPSt6vectorIP6ColumnSaIS2_EES4_EvT_S6_RSaIT0_E.exit83thread-pre-split ], [ %25, %invoke.cont15 ]
  %tobool.not.i71 = icmp eq %"class.std::vector"* %29, null
  br i1 %tobool.not.i71, label %_ZNSt12_Vector_baseISt6vectorIP6ColumnSaIS2_EESaIS4_EE13_M_deallocateEPS4_m.exit73, label %if.then.i72

if.then.i72:                                      ; preds = %_ZSt8_DestroyIPSt6vectorIP6ColumnSaIS2_EES4_EvT_S6_RSaIT0_E.exit83
  %30 = bitcast %"class.std::vector"* %29 to i8*
  tail call void @_ZdlPv(i8* nonnull %30) #18
  br label %_ZNSt12_Vector_baseISt6vectorIP6ColumnSaIS2_EESaIS4_EE13_M_deallocateEPS4_m.exit73

_ZNSt12_Vector_baseISt6vectorIP6ColumnSaIS2_EESaIS4_EE13_M_deallocateEPS4_m.exit73: ; preds = %_ZSt8_DestroyIPSt6vectorIP6ColumnSaIS2_EES4_EvT_S6_RSaIT0_E.exit83, %if.then.i72
  %_M_end_of_storage = getelementptr inbounds %"class.std::vector.5", %"class.std::vector.5"* %this, i64 0, i32 0, i32 0, i32 2
  store %"class.std::vector"* %cond.i67, %"class.std::vector"** %_M_start.i27.i, align 8, !tbaa !39
  store %"class.std::vector"* %__cur.0.lcssa.i.i.i.i97, %"class.std::vector"** %_M_finish.i26.i, align 8, !tbaa !42
  %add.ptr39 = getelementptr inbounds %"class.std::vector", %"class.std::vector"* %cond.i67, i64 %cond.i
  store %"class.std::vector"* %add.ptr39, %"class.std::vector"** %_M_end_of_storage, align 8, !tbaa !65
  ret void

lpad19:                                           ; preds = %if.else
  %31 = landingpad { i8*, i32 }
          cleanup
  invoke void @__cxa_end_catch()
          to label %invoke.cont24 unwind label %terminate.lpad

if.else:                                          ; preds = %if.then.i.i.i.i.i.i.i.i, %_ZNSt16allocator_traitsISaIP6ColumnEE8allocateERS2_m.exit.i.i.i.i.i.i
  %32 = landingpad { i8*, i32 }
          catch i8* null
  %33 = extractvalue { i8*, i32 } %32, 0
  %34 = tail call i8* @__cxa_begin_catch(i8* %33) #18
  %35 = bitcast %"class.std::vector"* %cond.i67 to i8*
  tail call void @_ZdlPv(i8* nonnull %35) #18
  invoke void @__cxa_rethrow() #23
          to label %unreachable unwind label %lpad19

invoke.cont24:                                    ; preds = %lpad19
  resume { i8*, i32 } %31

terminate.lpad:                                   ; preds = %lpad19
  %36 = landingpad { i8*, i32 }
          catch i8* null
  %37 = extractvalue { i8*, i32 } %36, 0
  tail call void @__clang_call_terminate(i8* %37) #22
  unreachable

unreachable:                                      ; preds = %if.else
  unreachable
}

; Function Attrs: uwtable
define internal void @_GLOBAL__sub_I_sort.cpp() #4 section ".text.startup" {
entry:
  tail call void @_ZNSt8ios_base4InitC1Ev(%"class.std::ios_base::Init"* nonnull dereferenceable(1) @_ZStL8__ioinit)
  %0 = tail call i32 @__cxa_atexit(void (i8*)* bitcast (void (%"class.std::ios_base::Init"*)* @_ZNSt8ios_base4InitD1Ev to void (i8*)*), i8* getelementptr inbounds (%"class.std::ios_base::Init", %"class.std::ios_base::Init"* @_ZStL8__ioinit, i64 0, i32 0), i8* nonnull @__dso_handle) #18
  ret void
}

; Function Attrs: argmemonly nofree nosync nounwind willreturn writeonly
declare void @llvm.memset.p0i8.i64(i8* nocapture writeonly, i8, i64, i1 immarg) #17

; Function Attrs: argmemonly nofree nosync nounwind willreturn
declare void @llvm.memcpy.p0i8.p0i8.i64(i8* noalias nocapture writeonly, i8* noalias nocapture readonly, i64, i1 immarg) #5

attributes #0 = { "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "no-infs-fp-math"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #1 = { nounwind "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "no-infs-fp-math"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #2 = { nofree nounwind }
attributes #3 = { norecurse nounwind readnone uwtable willreturn mustprogress "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "min-legal-vector-width"="0" "no-infs-fp-math"="false" "no-jump-tables"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #4 = { uwtable "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "min-legal-vector-width"="0" "no-infs-fp-math"="false" "no-jump-tables"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #5 = { argmemonly nofree nosync nounwind willreturn }
attributes #6 = { nobuiltin nofree allocsize(0) "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "no-infs-fp-math"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #7 = { nobuiltin nounwind "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "no-infs-fp-math"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #8 = { nounwind uwtable "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "min-legal-vector-width"="0" "no-infs-fp-math"="false" "no-jump-tables"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #9 = { nofree norecurse nounwind uwtable willreturn mustprogress "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "min-legal-vector-width"="0" "no-infs-fp-math"="false" "no-jump-tables"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #10 = { nofree norecurse nounwind uwtable mustprogress "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "min-legal-vector-width"="0" "no-infs-fp-math"="false" "no-jump-tables"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #11 = { norecurse nounwind readonly uwtable willreturn mustprogress "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "min-legal-vector-width"="0" "no-infs-fp-math"="false" "no-jump-tables"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #12 = { nofree nounwind uwtable "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "min-legal-vector-width"="0" "no-infs-fp-math"="false" "no-jump-tables"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #13 = { nounwind uwtable willreturn mustprogress "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "min-legal-vector-width"="0" "no-infs-fp-math"="false" "no-jump-tables"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #14 = { nounwind uwtable willreturn "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "min-legal-vector-width"="0" "no-infs-fp-math"="false" "no-jump-tables"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #15 = { noinline noreturn nounwind }
attributes #16 = { noreturn "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "no-infs-fp-math"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #17 = { argmemonly nofree nosync nounwind willreturn writeonly }
attributes #18 = { nounwind }
attributes #19 = { builtin allocsize(0) }
attributes #20 = { allocsize(0) }
attributes #21 = { builtin nounwind }
attributes #22 = { noreturn nounwind }
attributes #23 = { noreturn }

!llvm.module.flags = !{!0}
!llvm.ident = !{!1}

!0 = !{i32 1, !"wchar_size", i32 4}
!1 = !{!"Ubuntu clang version 12.0.0-++20210225092633+e0e6b1e39e7e-1~exp1~20210225083352.50"}
!2 = !{!3, !3, i64 0}
!3 = !{!"any pointer", !4, i64 0}
!4 = !{!"omnipotent char", !5, i64 0}
!5 = !{!"Simple C++ TBAA"}
!6 = !{!7, !7, i64 0}
!7 = !{!"int", !4, i64 0}
!8 = !{!9, !9, i64 0}
!9 = !{!"vtable pointer", !5, i64 0}
!10 = !{!11, !3, i64 8}
!11 = !{!"_ZTS6Column", !3, i64 8, !3, i64 16, !12, i64 24, !13, i64 32}
!12 = !{!"_ZTSN3opt10ColumnTypeE", !4, i64 0}
!13 = !{!"long", !4, i64 0}
!14 = !{!11, !12, i64 24}
!15 = !{!11, !13, i64 32}
!16 = !{!17, !3, i64 40}
!17 = !{!"_ZTS5Table", !18, i64 8, !19, i64 16, !3, i64 40, !7, i64 48, !7, i64 52, !7, i64 56}
!18 = !{!"_ZTS6Layout"}
!19 = !{!"_ZTSSt6vectorIP6ColumnSaIS1_EE"}
!20 = !{!17, !7, i64 56}
!21 = !{!12, !12, i64 0}
!22 = !{!23, !3, i64 8}
!23 = !{!"_ZTSSt12_Vector_baseIP6ColumnSaIS1_EE", !24, i64 0}
!24 = !{!"_ZTSNSt12_Vector_baseIP6ColumnSaIS1_EE12_Vector_implE", !3, i64 0, !3, i64 8, !3, i64 16}
!25 = !{!23, !3, i64 16}
!26 = !{!23, !3, i64 0}
!27 = distinct !{!27, !28}
!28 = !{!"llvm.loop.mustprogress"}
!29 = !{!30, !3, i64 8}
!30 = !{!"_ZTS4Sort", !3, i64 8, !7, i64 16, !3, i64 24, !7, i64 32, !3, i64 40, !3, i64 48, !3, i64 56, !7, i64 64, !3, i64 72}
!31 = !{!30, !7, i64 16}
!32 = !{!30, !3, i64 24}
!33 = !{!30, !7, i64 32}
!34 = !{!30, !3, i64 40}
!35 = !{!30, !3, i64 48}
!36 = !{!30, !3, i64 56}
!37 = !{!30, !7, i64 64}
!38 = !{!30, !3, i64 72}
!39 = !{!40, !3, i64 0}
!40 = !{!"_ZTSSt12_Vector_baseISt6vectorIP6ColumnSaIS2_EESaIS4_EE", !41, i64 0}
!41 = !{!"_ZTSNSt12_Vector_baseISt6vectorIP6ColumnSaIS2_EESaIS4_EE12_Vector_implE", !3, i64 0, !3, i64 8, !3, i64 16}
!42 = !{!40, !3, i64 8}
!43 = distinct !{!43, !28}
!44 = !{!45, !3, i64 0}
!45 = !{!"_ZTSSt12_Vector_baseIlSaIlEE", !46, i64 0}
!46 = !{!"_ZTSNSt12_Vector_baseIlSaIlEE12_Vector_implE", !3, i64 0, !3, i64 8, !3, i64 16}
!47 = !{!13, !13, i64 0}
!48 = !{!49}
!49 = distinct !{!49, !50}
!50 = distinct !{!50, !"LVerDomain"}
!51 = !{!52}
!52 = distinct !{!52, !50}
!53 = distinct !{!53, !28, !54}
!54 = !{!"llvm.loop.isvectorized", i32 1}
!55 = distinct !{!55, !28, !54}
!56 = !{!57, !3, i64 0}
!57 = !{!"_ZTS10PagesIndex", !3, i64 0, !7, i64 8, !58, i64 16, !59, i64 40, !7, i64 64}
!58 = !{!"_ZTSSt6vectorIlSaIlEE"}
!59 = !{!"_ZTSSt6vectorIS_IP6ColumnSaIS1_EESaIS3_EE"}
!60 = !{!57, !7, i64 8}
!61 = !{!45, !3, i64 16}
!62 = !{!57, !7, i64 64}
!63 = !{!45, !3, i64 8}
!64 = distinct !{!64, !28}
!65 = !{!40, !3, i64 16}
!66 = distinct !{!66, !28}
!67 = distinct !{!67, !28}
!68 = !{!69, !69, i64 0}
!69 = !{!"double", !4, i64 0}
!70 = distinct !{!70, !28}
!71 = distinct !{!71, !28}
!72 = distinct !{!72, !28}
!73 = distinct !{!73, !28}
!74 = distinct !{!74, !28}
!75 = !{!76}
!76 = distinct !{!76, !77}
!77 = distinct !{!77, !"LVerDomain"}
!78 = !{!79}
!79 = distinct !{!79, !77}
!80 = distinct !{!80, !28, !54}
!81 = distinct !{!81, !28, !54}
!82 = !{!83}
!83 = distinct !{!83, !84}
!84 = distinct !{!84, !"LVerDomain"}
!85 = !{!86}
!86 = distinct !{!86, !84}
!87 = distinct !{!87, !28, !54}
!88 = distinct !{!88, !28, !54}
!89 = distinct !{!89, !28}
!90 = distinct !{!90, !28}
!91 = distinct !{!91, !28}
!92 = distinct !{!92, !28}
!93 = distinct !{!93, !94}
!94 = !{!"llvm.loop.unroll.disable"}
!95 = distinct !{!95, !28}
