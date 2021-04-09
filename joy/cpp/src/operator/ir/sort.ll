; ModuleID = '/home/omni-cache/joy/cpp/src/operator/ir/../sort.cpp'
source_filename = "/home/omni-cache/joy/cpp/src/operator/ir/../sort.cpp"
target datalayout = "e-m:e-p270:32:32-p271:32:32-p272:64:64-i64:64-f80:128-n8:16:32:64-S128"
target triple = "x86_64-pc-linux-gnu"

%"class.std::ios_base::Init" = type { i8 }
%class.Sort = type { %class.OpTemplate, i32*, i32, i32*, i32, i32*, i32*, i32*, i32, %class.PagesIndex* }
%class.OpTemplate = type { i32 (...)** }
%class.PagesIndex = type <{ i32*, i32, [4 x i8], i64*, i32, [4 x i8], %class.Column***, i32, [4 x i8] }>
%class.Column = type { i32 (...)**, i8*, i32*, i32, i64 }
%class.Table = type <{ i32 (...)**, %class.Layout, [7 x i8], %"class.std::vector", i32*, i32, i32, i32, [4 x i8] }>
%class.Layout = type { i8 }
%"class.std::vector" = type { %"struct.std::_Vector_base" }
%"struct.std::_Vector_base" = type { %"struct.std::_Vector_base<Column *, std::allocator<Column *>>::_Vector_impl" }
%"struct.std::_Vector_base<Column *, std::allocator<Column *>>::_Vector_impl" = type { %class.Column**, %class.Column**, %class.Column** }
%class.MemoryPool = type { i32 (...)** }

$_ZN4Sort9getResultEv = comdat any

$_ZN6ColumnD2Ev = comdat any

$_ZN6ColumnD0Ev = comdat any

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
@_ZN10PagesIndexD1Ev = dso_local unnamed_addr alias void (%class.PagesIndex*), void (%class.PagesIndex*)* @_ZN10PagesIndexD2Ev

declare dso_local void @_ZNSt8ios_base4InitC1Ev(%"class.std::ios_base::Init"* nonnull dereferenceable(1)) unnamed_addr #0

; Function Attrs: nounwind
declare dso_local void @_ZNSt8ios_base4InitD1Ev(%"class.std::ios_base::Init"* nonnull dereferenceable(1)) unnamed_addr #1

; Function Attrs: nofree nounwind
declare dso_local i32 @__cxa_atexit(void (i8*)*, i8*, i8*) local_unnamed_addr #2

; Function Attrs: norecurse nounwind readnone uwtable willreturn mustprogress
define dso_local i64 @_Z22encodeSyntheticAddressii(i32 %sliceIndex, i32 %sliceOffset) local_unnamed_addr #3 {
entry:
  %conv2 = zext i32 %sliceIndex to i64
  %shl = shl nuw i64 %conv2, 32
  %conv1 = sext i32 %sliceOffset to i64
  %or = or i64 %shl, %conv1
  ret i64 %or
}

; Function Attrs: norecurse nounwind readnone uwtable willreturn mustprogress
define dso_local i32 @_Z16decodeSliceIndexl(i64 %sliceAddress) local_unnamed_addr #3 {
entry:
  %0 = lshr i64 %sliceAddress, 32
  %conv = trunc i64 %0 to i32
  ret i32 %conv
}

; Function Attrs: norecurse nounwind readnone uwtable willreturn mustprogress
define dso_local i32 @_Z14decodePositionl(i64 %sliceAddress) local_unnamed_addr #3 {
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
define dso_local void @_Z12allocColumnslPiS_ii(i64 %outputTableAddr, i32* nocapture readonly %sourceTypes, i32* nocapture readonly %outputCols, i32 %outputColCount, i32 %positionCount) local_unnamed_addr #4 personality i8* bitcast (i32 (...)* @__gxx_personality_v0 to i8*) {
entry:
  %data = alloca i8*, align 8
  %0 = inttoptr i64 %outputTableAddr to %class.Table*
  %call = tail call %class.MemoryPool* @_Z13getMemoryPoolv()
  %1 = bitcast i8** %data to i8*
  call void @llvm.lifetime.start.p0i8(i64 8, i8* nonnull %1) #19
  store i8* null, i8** %data, align 8, !tbaa !2
  %cmp36 = icmp sgt i32 %outputColCount, 0
  br i1 %cmp36, label %for.body.lr.ph, label %for.cond.cleanup

for.body.lr.ph:                                   ; preds = %entry
  %conv11 = sext i32 %positionCount to i64
  %mul12 = shl nsw i64 %conv11, 3
  %2 = bitcast %class.MemoryPool* %call to i32 (%class.MemoryPool*, i64, i8**)***
  %mul = shl nsw i64 %conv11, 2
  %types.i = getelementptr inbounds %class.Table, %class.Table* %0, i64 0, i32 4
  %columnSize.i = getelementptr inbounds %class.Table, %class.Table* %0, i64 0, i32 7
  %_M_finish.i.i = getelementptr inbounds %class.Table, %class.Table* %0, i64 0, i32 3, i32 0, i32 0, i32 1
  %_M_end_of_storage.i.i = getelementptr inbounds %class.Table, %class.Table* %0, i64 0, i32 3, i32 0, i32 0, i32 2
  %_M_start.i27.i.i.i.i = getelementptr inbounds %class.Table, %class.Table* %0, i64 0, i32 3, i32 0, i32 0, i32 0
  %wide.trip.count = zext i32 %outputColCount to i64
  br label %for.body

for.cond.cleanup:                                 ; preds = %_ZN5Table9setColumnEP6ColumnN3opt10ColumnTypeE.exit, %entry
  call void @llvm.lifetime.end.p0i8(i64 8, i8* nonnull %1) #19
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
  %call17 = call noalias nonnull dereferenceable(40) i8* @_Znwm(i64 40) #20
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
  %call2.i.i.i.i.i.i = call noalias nonnull i8* @_Znwm(i64 %mul.i.i.i.i.i.i) #21
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
  call void @llvm.memmove.p0i8.p0i8.i64(i8* align 8 %23, i8* align 8 %24, i64 %sub.ptr.sub.i.i.i.i.i.i.i.i74.pre-phi.i.i.i, i1 false) #19
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
  call void @llvm.memmove.p0i8.p0i8.i64(i8* nonnull align 8 %26, i8* align 8 %27, i64 %sub.ptr.sub.i.i.i.i.i.i.i.i.i.i.i, i1 false) #19
  br label %invoke.cont15.i.i.i

invoke.cont15.i.i.i:                              ; preds = %if.then.i.i.i.i.i.i.i.i.i.i.i, %invoke.cont10.i.i.i
  %tobool.not.i68.i.i.i = icmp eq %class.Column** %21, null
  br i1 %tobool.not.i68.i.i.i, label %_ZNSt6vectorIP6ColumnSaIS1_EE17_M_realloc_insertIJRKS1_EEEvN9__gnu_cxx17__normal_iteratorIPS1_S3_EEDpOT_.exit.i.i, label %if.then.i69.i.i.i

if.then.i69.i.i.i:                                ; preds = %invoke.cont15.i.i.i
  %28 = bitcast %class.Column** %21 to i8*
  call void @_ZdlPv(i8* nonnull %28) #19
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

; Function Attrs: nofree uwtable
define dso_local void @_ZN4SortC2EPiiS0_iS0_S0_S0_i(%class.Sort* nocapture nonnull dereferenceable(80) %this, i32* %sourceTypes, i32 %typesCount, i32* %outputCols, i32 %outputColsCount, i32* %sortCols, i32* %sortAscendings, i32* %sortNullFirsts, i32 %sortColCount) unnamed_addr #8 align 2 personality i8* bitcast (i32 (...)* @__gxx_personality_v0 to i8*) {
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
  %call = tail call noalias nonnull dereferenceable(48) i8* @_Znwm(i64 48) #20
  %types.i = bitcast i8* %call to i32**
  store i32* %sourceTypes, i32** %types.i, align 8, !tbaa !38
  %typesCount2.i = getelementptr inbounds i8, i8* %call, i64 8
  %1 = bitcast i8* %typesCount2.i to i32*
  store i32 %typesCount, i32* %1, align 8, !tbaa !40
  %pagesIndex = getelementptr inbounds %class.Sort, %class.Sort* %this, i64 0, i32 9
  %2 = bitcast %class.PagesIndex** %pagesIndex to i8**
  store i8* %call, i8** %2, align 8, !tbaa !41
  ret void
}

; Function Attrs: nounwind uwtable
define dso_local void @_ZN4SortD2Ev(%class.Sort* nocapture nonnull dereferenceable(80) %this) unnamed_addr #9 align 2 {
entry:
  %0 = getelementptr inbounds %class.Sort, %class.Sort* %this, i64 0, i32 0, i32 0
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [9 x i8*] }, { [9 x i8*] }* @_ZTV4Sort, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %0, align 8, !tbaa !8
  %pagesIndex = getelementptr inbounds %class.Sort, %class.Sort* %this, i64 0, i32 9
  %1 = load %class.PagesIndex*, %class.PagesIndex** %pagesIndex, align 8, !tbaa !41
  %isnull = icmp eq %class.PagesIndex* %1, null
  br i1 %isnull, label %delete.end, label %delete.notnull

delete.notnull:                                   ; preds = %entry
  %typesCount.i = getelementptr inbounds %class.PagesIndex, %class.PagesIndex* %1, i64 0, i32 1
  %2 = load i32, i32* %typesCount.i, align 8, !tbaa !40
  %cmp24.i = icmp sgt i32 %2, 0
  br i1 %cmp24.i, label %for.cond2.preheader.lr.ph.i, label %_ZN10PagesIndexD2Ev.exit

for.cond2.preheader.lr.ph.i:                      ; preds = %delete.notnull
  %tableCount.i = getelementptr inbounds %class.PagesIndex, %class.PagesIndex* %1, i64 0, i32 7
  %columns.i = getelementptr inbounds %class.PagesIndex, %class.PagesIndex* %1, i64 0, i32 6
  br label %for.cond2.preheader.i

for.cond2.preheader.i:                            ; preds = %for.cond.cleanup4.i, %for.cond2.preheader.lr.ph.i
  %indvars.iv27.i = phi i64 [ 0, %for.cond2.preheader.lr.ph.i ], [ %indvars.iv.next28.i, %for.cond.cleanup4.i ]
  %3 = load i32, i32* %tableCount.i, align 8, !tbaa !42
  %cmp322.i = icmp sgt i32 %3, 0
  br i1 %cmp322.i, label %for.body5.i, label %for.cond.cleanup4.i

for.cond.cleanup4.i:                              ; preds = %for.inc.i, %for.cond2.preheader.i
  %4 = load %class.Column***, %class.Column**** %columns.i, align 8, !tbaa !43
  %arrayidx10.i = getelementptr inbounds %class.Column**, %class.Column*** %4, i64 %indvars.iv27.i
  %5 = bitcast %class.Column*** %arrayidx10.i to i8**
  %6 = load i8*, i8** %5, align 8, !tbaa !2
  tail call void @free(i8* %6) #19
  %indvars.iv.next28.i = add nuw nsw i64 %indvars.iv27.i, 1
  %7 = load i32, i32* %typesCount.i, align 8, !tbaa !40
  %8 = sext i32 %7 to i64
  %cmp.i = icmp slt i64 %indvars.iv.next28.i, %8
  br i1 %cmp.i, label %for.cond2.preheader.i, label %_ZN10PagesIndexD2Ev.exit, !llvm.loop !44

for.body5.i:                                      ; preds = %for.cond2.preheader.i, %for.inc.i
  %9 = phi i32 [ %15, %for.inc.i ], [ %3, %for.cond2.preheader.i ]
  %indvars.iv.i = phi i64 [ %indvars.iv.next.i, %for.inc.i ], [ 0, %for.cond2.preheader.i ]
  %10 = load %class.Column***, %class.Column**** %columns.i, align 8, !tbaa !43
  %arrayidx.i = getelementptr inbounds %class.Column**, %class.Column*** %10, i64 %indvars.iv27.i
  %11 = load %class.Column**, %class.Column*** %arrayidx.i, align 8, !tbaa !2
  %arrayidx7.i = getelementptr inbounds %class.Column*, %class.Column** %11, i64 %indvars.iv.i
  %12 = load %class.Column*, %class.Column** %arrayidx7.i, align 8, !tbaa !2
  %isnull.i = icmp eq %class.Column* %12, null
  br i1 %isnull.i, label %for.inc.i, label %delete.notnull.i

delete.notnull.i:                                 ; preds = %for.body5.i
  %13 = bitcast %class.Column* %12 to void (%class.Column*)***
  %vtable.i = load void (%class.Column*)**, void (%class.Column*)*** %13, align 8, !tbaa !8
  %vfn.i = getelementptr inbounds void (%class.Column*)*, void (%class.Column*)** %vtable.i, i64 1
  %14 = load void (%class.Column*)*, void (%class.Column*)** %vfn.i, align 8
  tail call void %14(%class.Column* nonnull dereferenceable(40) %12) #19
  %.pre.i = load i32, i32* %tableCount.i, align 8, !tbaa !42
  br label %for.inc.i

for.inc.i:                                        ; preds = %delete.notnull.i, %for.body5.i
  %15 = phi i32 [ %9, %for.body5.i ], [ %.pre.i, %delete.notnull.i ]
  %indvars.iv.next.i = add nuw nsw i64 %indvars.iv.i, 1
  %16 = sext i32 %15 to i64
  %cmp3.i = icmp slt i64 %indvars.iv.next.i, %16
  br i1 %cmp3.i, label %for.body5.i, label %for.cond.cleanup4.i, !llvm.loop !45

_ZN10PagesIndexD2Ev.exit:                         ; preds = %for.cond.cleanup4.i, %delete.notnull
  %columns14.i = getelementptr inbounds %class.PagesIndex, %class.PagesIndex* %1, i64 0, i32 6
  %17 = bitcast %class.Column**** %columns14.i to i8**
  %18 = load i8*, i8** %17, align 8, !tbaa !43
  tail call void @free(i8* %18) #19
  %valueAddresses.i = getelementptr inbounds %class.PagesIndex, %class.PagesIndex* %1, i64 0, i32 3
  %19 = bitcast i64** %valueAddresses.i to i8**
  %20 = load i8*, i8** %19, align 8, !tbaa !46
  tail call void @free(i8* %20) #19
  %21 = bitcast %class.PagesIndex* %1 to i8*
  tail call void @_ZdlPv(i8* %21) #22
  br label %delete.end

delete.end:                                       ; preds = %_ZN10PagesIndexD2Ev.exit, %entry
  ret void
}

; Function Attrs: nounwind uwtable
define dso_local void @_ZN4SortD0Ev(%class.Sort* nonnull dereferenceable(80) %this) unnamed_addr #9 align 2 {
entry:
  tail call void @_ZN4SortD2Ev(%class.Sort* nonnull dereferenceable(80) %this) #19
  %0 = bitcast %class.Sort* %this to i8*
  tail call void @_ZdlPv(i8* nonnull %0) #22
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
define dso_local void @_Z4swapPlii(i64* nocapture %valueAddresses, i32 %a, i32 %b) local_unnamed_addr #10 {
entry:
  %idxprom = sext i32 %a to i64
  %arrayidx = getelementptr inbounds i64, i64* %valueAddresses, i64 %idxprom
  %0 = load i64, i64* %arrayidx, align 8, !tbaa !47
  %idxprom1 = sext i32 %b to i64
  %arrayidx2 = getelementptr inbounds i64, i64* %valueAddresses, i64 %idxprom1
  %1 = load i64, i64* %arrayidx2, align 8, !tbaa !47
  store i64 %1, i64* %arrayidx, align 8, !tbaa !47
  store i64 %0, i64* %arrayidx2, align 8, !tbaa !47
  ret void
}

; Function Attrs: nofree norecurse nounwind uwtable mustprogress
define dso_local void @_Z10vectorSwapPliii(i64* nocapture %valueAddresses, i32 %from, i32 %l, i32 %s) local_unnamed_addr #11 {
entry:
  %cmp7 = icmp sgt i32 %s, 0
  br i1 %cmp7, label %for.body.preheader, label %for.cond.cleanup

for.body.preheader:                               ; preds = %entry
  %0 = sext i32 %from to i64
  %1 = sext i32 %l to i64
  %2 = add i32 %s, -1
  %3 = zext i32 %2 to i64
  %4 = add nuw nsw i64 %3, 1
  %min.iters.check = icmp ult i32 %2, 3
  br i1 %min.iters.check, label %for.body.preheader31, label %vector.memcheck

vector.memcheck:                                  ; preds = %for.body.preheader
  %scevgep = getelementptr i64, i64* %valueAddresses, i64 %0
  %5 = add i32 %s, -1
  %6 = zext i32 %5 to i64
  %7 = add nsw i64 %0, %6
  %8 = add nsw i64 %7, 1
  %scevgep16 = getelementptr i64, i64* %valueAddresses, i64 %8
  %scevgep18 = getelementptr i64, i64* %valueAddresses, i64 %1
  %9 = add nsw i64 %1, %6
  %10 = add nsw i64 %9, 1
  %scevgep20 = getelementptr i64, i64* %valueAddresses, i64 %10
  %bound0 = icmp ult i64* %scevgep, %scevgep20
  %bound1 = icmp ult i64* %scevgep18, %scevgep16
  %found.conflict = and i1 %bound0, %bound1
  br i1 %found.conflict, label %for.body.preheader31, label %vector.ph

vector.ph:                                        ; preds = %vector.memcheck
  %n.vec = and i64 %4, 8589934588
  %ind.end = add nsw i64 %n.vec, %1
  %ind.end23 = add nsw i64 %n.vec, %0
  %ind.end25 = trunc i64 %n.vec to i32
  %11 = add nsw i64 %n.vec, -4
  %12 = lshr exact i64 %11, 2
  %13 = add nuw nsw i64 %12, 1
  %xtraiter32 = and i64 %13, 1
  %14 = icmp eq i64 %11, 0
  br i1 %14, label %middle.block.unr-lcssa, label %vector.ph.new

vector.ph.new:                                    ; preds = %vector.ph
  %unroll_iter = and i64 %13, 9223372036854775806
  br label %vector.body

vector.body:                                      ; preds = %vector.body, %vector.ph.new
  %index = phi i64 [ 0, %vector.ph.new ], [ %index.next.1, %vector.body ]
  %niter = phi i64 [ %unroll_iter, %vector.ph.new ], [ %niter.nsub.1, %vector.body ]
  %offset.idx = add i64 %index, %1
  %offset.idx26 = add i64 %index, %0
  %15 = getelementptr inbounds i64, i64* %valueAddresses, i64 %offset.idx26
  %16 = bitcast i64* %15 to <2 x i64>*
  %wide.load = load <2 x i64>, <2 x i64>* %16, align 8, !tbaa !47, !alias.scope !48, !noalias !51
  %17 = getelementptr inbounds i64, i64* %15, i64 2
  %18 = bitcast i64* %17 to <2 x i64>*
  %wide.load28 = load <2 x i64>, <2 x i64>* %18, align 8, !tbaa !47, !alias.scope !48, !noalias !51
  %19 = getelementptr inbounds i64, i64* %valueAddresses, i64 %offset.idx
  %20 = bitcast i64* %19 to <2 x i64>*
  %wide.load29 = load <2 x i64>, <2 x i64>* %20, align 8, !tbaa !47, !alias.scope !51
  %21 = getelementptr inbounds i64, i64* %19, i64 2
  %22 = bitcast i64* %21 to <2 x i64>*
  %wide.load30 = load <2 x i64>, <2 x i64>* %22, align 8, !tbaa !47, !alias.scope !51
  %23 = bitcast i64* %15 to <2 x i64>*
  store <2 x i64> %wide.load29, <2 x i64>* %23, align 8, !tbaa !47, !alias.scope !48, !noalias !51
  %24 = bitcast i64* %17 to <2 x i64>*
  store <2 x i64> %wide.load30, <2 x i64>* %24, align 8, !tbaa !47, !alias.scope !48, !noalias !51
  %25 = bitcast i64* %19 to <2 x i64>*
  store <2 x i64> %wide.load, <2 x i64>* %25, align 8, !tbaa !47, !alias.scope !51
  %26 = bitcast i64* %21 to <2 x i64>*
  store <2 x i64> %wide.load28, <2 x i64>* %26, align 8, !tbaa !47, !alias.scope !51
  %index.next = or i64 %index, 4
  %offset.idx.1 = add i64 %index.next, %1
  %offset.idx26.1 = add i64 %index.next, %0
  %27 = getelementptr inbounds i64, i64* %valueAddresses, i64 %offset.idx26.1
  %28 = bitcast i64* %27 to <2 x i64>*
  %wide.load.1 = load <2 x i64>, <2 x i64>* %28, align 8, !tbaa !47, !alias.scope !48, !noalias !51
  %29 = getelementptr inbounds i64, i64* %27, i64 2
  %30 = bitcast i64* %29 to <2 x i64>*
  %wide.load28.1 = load <2 x i64>, <2 x i64>* %30, align 8, !tbaa !47, !alias.scope !48, !noalias !51
  %31 = getelementptr inbounds i64, i64* %valueAddresses, i64 %offset.idx.1
  %32 = bitcast i64* %31 to <2 x i64>*
  %wide.load29.1 = load <2 x i64>, <2 x i64>* %32, align 8, !tbaa !47, !alias.scope !51
  %33 = getelementptr inbounds i64, i64* %31, i64 2
  %34 = bitcast i64* %33 to <2 x i64>*
  %wide.load30.1 = load <2 x i64>, <2 x i64>* %34, align 8, !tbaa !47, !alias.scope !51
  %35 = bitcast i64* %27 to <2 x i64>*
  store <2 x i64> %wide.load29.1, <2 x i64>* %35, align 8, !tbaa !47, !alias.scope !48, !noalias !51
  %36 = bitcast i64* %29 to <2 x i64>*
  store <2 x i64> %wide.load30.1, <2 x i64>* %36, align 8, !tbaa !47, !alias.scope !48, !noalias !51
  %37 = bitcast i64* %31 to <2 x i64>*
  store <2 x i64> %wide.load.1, <2 x i64>* %37, align 8, !tbaa !47, !alias.scope !51
  %38 = bitcast i64* %33 to <2 x i64>*
  store <2 x i64> %wide.load28.1, <2 x i64>* %38, align 8, !tbaa !47, !alias.scope !51
  %index.next.1 = add i64 %index, 8
  %niter.nsub.1 = add i64 %niter, -2
  %niter.ncmp.1 = icmp eq i64 %niter.nsub.1, 0
  br i1 %niter.ncmp.1, label %middle.block.unr-lcssa, label %vector.body, !llvm.loop !53

middle.block.unr-lcssa:                           ; preds = %vector.body, %vector.ph
  %index.unr = phi i64 [ 0, %vector.ph ], [ %index.next.1, %vector.body ]
  %lcmp.mod33.not = icmp eq i64 %xtraiter32, 0
  br i1 %lcmp.mod33.not, label %middle.block, label %vector.body.epil

vector.body.epil:                                 ; preds = %middle.block.unr-lcssa
  %offset.idx.epil = add i64 %index.unr, %1
  %offset.idx26.epil = add i64 %index.unr, %0
  %39 = getelementptr inbounds i64, i64* %valueAddresses, i64 %offset.idx26.epil
  %40 = bitcast i64* %39 to <2 x i64>*
  %wide.load.epil = load <2 x i64>, <2 x i64>* %40, align 8, !tbaa !47, !alias.scope !48, !noalias !51
  %41 = getelementptr inbounds i64, i64* %39, i64 2
  %42 = bitcast i64* %41 to <2 x i64>*
  %wide.load28.epil = load <2 x i64>, <2 x i64>* %42, align 8, !tbaa !47, !alias.scope !48, !noalias !51
  %43 = getelementptr inbounds i64, i64* %valueAddresses, i64 %offset.idx.epil
  %44 = bitcast i64* %43 to <2 x i64>*
  %wide.load29.epil = load <2 x i64>, <2 x i64>* %44, align 8, !tbaa !47, !alias.scope !51
  %45 = getelementptr inbounds i64, i64* %43, i64 2
  %46 = bitcast i64* %45 to <2 x i64>*
  %wide.load30.epil = load <2 x i64>, <2 x i64>* %46, align 8, !tbaa !47, !alias.scope !51
  %47 = bitcast i64* %39 to <2 x i64>*
  store <2 x i64> %wide.load29.epil, <2 x i64>* %47, align 8, !tbaa !47, !alias.scope !48, !noalias !51
  %48 = bitcast i64* %41 to <2 x i64>*
  store <2 x i64> %wide.load30.epil, <2 x i64>* %48, align 8, !tbaa !47, !alias.scope !48, !noalias !51
  %49 = bitcast i64* %43 to <2 x i64>*
  store <2 x i64> %wide.load.epil, <2 x i64>* %49, align 8, !tbaa !47, !alias.scope !51
  %50 = bitcast i64* %45 to <2 x i64>*
  store <2 x i64> %wide.load28.epil, <2 x i64>* %50, align 8, !tbaa !47, !alias.scope !51
  br label %middle.block

middle.block:                                     ; preds = %middle.block.unr-lcssa, %vector.body.epil
  %cmp.n = icmp eq i64 %4, %n.vec
  br i1 %cmp.n, label %for.cond.cleanup, label %for.body.preheader31

for.body.preheader31:                             ; preds = %vector.memcheck, %for.body.preheader, %middle.block
  %indvars.iv13.ph = phi i64 [ %1, %vector.memcheck ], [ %1, %for.body.preheader ], [ %ind.end, %middle.block ]
  %indvars.iv.ph = phi i64 [ %0, %vector.memcheck ], [ %0, %for.body.preheader ], [ %ind.end23, %middle.block ]
  %i.010.ph = phi i32 [ 0, %vector.memcheck ], [ 0, %for.body.preheader ], [ %ind.end25, %middle.block ]
  %51 = sub i32 %s, %i.010.ph
  %52 = xor i32 %i.010.ph, -1
  %xtraiter = and i32 %51, 1
  %lcmp.mod.not = icmp eq i32 %xtraiter, 0
  br i1 %lcmp.mod.not, label %for.body.prol.loopexit, label %for.body.prol

for.body.prol:                                    ; preds = %for.body.preheader31
  %arrayidx.i.prol = getelementptr inbounds i64, i64* %valueAddresses, i64 %indvars.iv.ph
  %53 = load i64, i64* %arrayidx.i.prol, align 8, !tbaa !47
  %arrayidx2.i.prol = getelementptr inbounds i64, i64* %valueAddresses, i64 %indvars.iv13.ph
  %54 = load i64, i64* %arrayidx2.i.prol, align 8, !tbaa !47
  store i64 %54, i64* %arrayidx.i.prol, align 8, !tbaa !47
  store i64 %53, i64* %arrayidx2.i.prol, align 8, !tbaa !47
  %inc.prol = add nuw nsw i32 %i.010.ph, 1
  %indvars.iv.next.prol = add nsw i64 %indvars.iv.ph, 1
  %indvars.iv.next14.prol = add nsw i64 %indvars.iv13.ph, 1
  br label %for.body.prol.loopexit

for.body.prol.loopexit:                           ; preds = %for.body.prol, %for.body.preheader31
  %indvars.iv13.unr = phi i64 [ %indvars.iv13.ph, %for.body.preheader31 ], [ %indvars.iv.next14.prol, %for.body.prol ]
  %indvars.iv.unr = phi i64 [ %indvars.iv.ph, %for.body.preheader31 ], [ %indvars.iv.next.prol, %for.body.prol ]
  %i.010.unr = phi i32 [ %i.010.ph, %for.body.preheader31 ], [ %inc.prol, %for.body.prol ]
  %55 = sub i32 0, %s
  %56 = icmp eq i32 %52, %55
  br i1 %56, label %for.cond.cleanup, label %for.body

for.cond.cleanup:                                 ; preds = %for.body.prol.loopexit, %for.body, %middle.block, %entry
  ret void

for.body:                                         ; preds = %for.body.prol.loopexit, %for.body
  %indvars.iv13 = phi i64 [ %indvars.iv.next14.1, %for.body ], [ %indvars.iv13.unr, %for.body.prol.loopexit ]
  %indvars.iv = phi i64 [ %indvars.iv.next.1, %for.body ], [ %indvars.iv.unr, %for.body.prol.loopexit ]
  %i.010 = phi i32 [ %inc.1, %for.body ], [ %i.010.unr, %for.body.prol.loopexit ]
  %arrayidx.i = getelementptr inbounds i64, i64* %valueAddresses, i64 %indvars.iv
  %57 = load i64, i64* %arrayidx.i, align 8, !tbaa !47
  %arrayidx2.i = getelementptr inbounds i64, i64* %valueAddresses, i64 %indvars.iv13
  %58 = load i64, i64* %arrayidx2.i, align 8, !tbaa !47
  store i64 %58, i64* %arrayidx.i, align 8, !tbaa !47
  store i64 %57, i64* %arrayidx2.i, align 8, !tbaa !47
  %indvars.iv.next = add nsw i64 %indvars.iv, 1
  %indvars.iv.next14 = add nsw i64 %indvars.iv13, 1
  %arrayidx.i.1 = getelementptr inbounds i64, i64* %valueAddresses, i64 %indvars.iv.next
  %59 = load i64, i64* %arrayidx.i.1, align 8, !tbaa !47
  %arrayidx2.i.1 = getelementptr inbounds i64, i64* %valueAddresses, i64 %indvars.iv.next14
  %60 = load i64, i64* %arrayidx2.i.1, align 8, !tbaa !47
  store i64 %60, i64* %arrayidx.i.1, align 8, !tbaa !47
  store i64 %59, i64* %arrayidx2.i.1, align 8, !tbaa !47
  %inc.1 = add nuw nsw i32 %i.010, 2
  %indvars.iv.next.1 = add nsw i64 %indvars.iv, 2
  %indvars.iv.next14.1 = add nsw i64 %indvars.iv13, 2
  %exitcond.not.1 = icmp eq i32 %inc.1, %s
  br i1 %exitcond.not.1, label %for.cond.cleanup, label %for.body, !llvm.loop !55
}

; Function Attrs: nofree norecurse nounwind uwtable willreturn writeonly
define dso_local void @_ZN10PagesIndexC2EPii(%class.PagesIndex* nocapture nonnull dereferenceable(44) %this, i32* %sourceTypes, i32 %typesCount) unnamed_addr #12 align 2 {
entry:
  %types = getelementptr inbounds %class.PagesIndex, %class.PagesIndex* %this, i64 0, i32 0
  store i32* %sourceTypes, i32** %types, align 8, !tbaa !38
  %typesCount2 = getelementptr inbounds %class.PagesIndex, %class.PagesIndex* %this, i64 0, i32 1
  store i32 %typesCount, i32* %typesCount2, align 8, !tbaa !40
  ret void
}

; Function Attrs: nofree uwtable
define dso_local void @_ZN10PagesIndex9addTablesEPlS0_iPii(%class.PagesIndex* nocapture nonnull dereferenceable(44) %this, i64* nocapture readonly %datas, i64* nocapture readonly %nulls, i32 %pageCount, i32* nocapture readonly %rowCounts, i32 %totalRowCount) local_unnamed_addr #8 align 2 personality i8* bitcast (i32 (...)* @__gxx_personality_v0 to i8*) {
entry:
  %tableCount = getelementptr inbounds %class.PagesIndex, %class.PagesIndex* %this, i64 0, i32 7
  store i32 %pageCount, i32* %tableCount, align 8, !tbaa !42
  %positionCount = getelementptr inbounds %class.PagesIndex, %class.PagesIndex* %this, i64 0, i32 4
  store i32 %totalRowCount, i32* %positionCount, align 8, !tbaa !56
  %conv = sext i32 %totalRowCount to i64
  %mul = shl nsw i64 %conv, 3
  %call = tail call noalias i8* @malloc(i64 %mul) #19
  %valueAddresses = getelementptr inbounds %class.PagesIndex, %class.PagesIndex* %this, i64 0, i32 3
  %0 = bitcast i64** %valueAddresses to i8**
  store i8* %call, i8** %0, align 8, !tbaa !46
  %call2 = tail call noalias dereferenceable_or_null(8) i8* @malloc(i64 8) #19
  %columns = getelementptr inbounds %class.PagesIndex, %class.PagesIndex* %this, i64 0, i32 6
  %1 = bitcast %class.Column**** %columns to i8**
  store i8* %call2, i8** %1, align 8, !tbaa !43
  %typesCount = getelementptr inbounds %class.PagesIndex, %class.PagesIndex* %this, i64 0, i32 1
  %2 = load i32, i32* %typesCount, align 8, !tbaa !40
  %cmp94 = icmp sgt i32 %2, 0
  br i1 %cmp94, label %for.body.lr.ph, label %for.cond8.preheader.thread

for.body.lr.ph:                                   ; preds = %entry
  %conv4 = sext i32 %pageCount to i64
  %mul5 = shl nsw i64 %conv4, 3
  %wide.trip.count122 = zext i32 %2 to i64
  %call6128 = tail call noalias i8* @malloc(i64 %mul5) #19
  %3 = bitcast i8* %call2 to i8**
  store i8* %call6128, i8** %3, align 8, !tbaa !2
  %exitcond123.not129 = icmp eq i32 %2, 1
  br i1 %exitcond123.not129, label %for.cond8.preheader, label %for.body.for.body_crit_edge, !llvm.loop !57

for.cond8.preheader:                              ; preds = %for.body.for.body_crit_edge, %for.body.lr.ph
  %cmp991 = icmp sgt i32 %pageCount, 0
  br i1 %cmp991, label %for.body11.lr.ph, label %for.cond.cleanup10

for.cond8.preheader.thread:                       ; preds = %entry
  %cmp991124 = icmp sgt i32 %pageCount, 0
  br i1 %cmp991124, label %for.body11.preheader, label %for.cond.cleanup10

for.body11.lr.ph:                                 ; preds = %for.cond8.preheader
  %types = getelementptr inbounds %class.PagesIndex, %class.PagesIndex* %this, i64 0, i32 0
  br i1 %cmp94, label %for.body11.us.preheader, label %for.body11.preheader

for.body11.preheader:                             ; preds = %for.cond8.preheader.thread, %for.body11.lr.ph
  %wide.trip.count118 = zext i32 %pageCount to i64
  br label %for.body11

for.body11.us.preheader:                          ; preds = %for.body11.lr.ph
  %4 = zext i32 %2 to i64
  %wide.trip.count108 = zext i32 %pageCount to i64
  %wide.trip.count = zext i32 %2 to i64
  br label %for.body11.us

for.body11.us:                                    ; preds = %for.body11.us.preheader, %for.cond.cleanup41.us
  %indvars.iv105 = phi i64 [ 0, %for.body11.us.preheader ], [ %indvars.iv.next106, %for.cond.cleanup41.us ]
  %valueAddrIdx.092.us = phi i32 [ 0, %for.body11.us.preheader ], [ %valueAddrIdx.1.lcssa.us, %for.cond.cleanup41.us ]
  %arrayidx13.us = getelementptr inbounds i32, i32* %rowCounts, i64 %indvars.iv105
  %5 = load i32, i32* %arrayidx13.us, align 4, !tbaa !6
  %6 = mul nsw i64 %indvars.iv105, %4
  %conv30.us = sext i32 %5 to i64
  br label %for.body21.us

for.cond.cleanup41.us.loopexit:                   ; preds = %for.body42.us, %middle.block135
  %indvars.iv.next102.lcssa = phi i64 [ %ind.end145, %middle.block135 ], [ %indvars.iv.next102, %for.body42.us ]
  %7 = trunc i64 %indvars.iv.next102.lcssa to i32
  br label %for.cond.cleanup41.us

for.cond.cleanup41.us:                            ; preds = %for.cond.cleanup41.us.loopexit, %for.cond17.for.cond39.preheader_crit_edge.us
  %valueAddrIdx.1.lcssa.us = phi i32 [ %valueAddrIdx.092.us, %for.cond17.for.cond39.preheader_crit_edge.us ], [ %7, %for.cond.cleanup41.us.loopexit ]
  %indvars.iv.next106 = add nuw nsw i64 %indvars.iv105, 1
  %exitcond109.not = icmp eq i64 %indvars.iv.next106, %wide.trip.count108
  br i1 %exitcond109.not, label %for.cond.cleanup10, label %for.body11.us, !llvm.loop !58

for.body42.us:                                    ; preds = %for.body42.us.preheader, %for.body42.us
  %indvars.iv101 = phi i64 [ %indvars.iv.next102, %for.body42.us ], [ %indvars.iv101.ph, %for.body42.us.preheader ]
  %indvars.iv99 = phi i64 [ %indvars.iv.next100, %for.body42.us ], [ %indvars.iv99.ph, %for.body42.us.preheader ]
  %or.i.us = or i64 %shl.i.us, %indvars.iv99
  %arrayidx46.us = getelementptr inbounds i64, i64* %23, i64 %indvars.iv101
  store i64 %or.i.us, i64* %arrayidx46.us, align 8, !tbaa !47
  %indvars.iv.next102 = add nsw i64 %indvars.iv101, 1
  %indvars.iv.next100 = add nuw nsw i64 %indvars.iv99, 1
  %exitcond104.not = icmp eq i64 %indvars.iv.next100, %wide.trip.count103
  br i1 %exitcond104.not, label %for.cond.cleanup41.us.loopexit, label %for.body42.us, !llvm.loop !59

for.body21.us:                                    ; preds = %for.body11.us, %for.body21.us
  %indvars.iv = phi i64 [ 0, %for.body11.us ], [ %indvars.iv.next, %for.body21.us ]
  %8 = add nuw nsw i64 %indvars.iv, %6
  %9 = load i32*, i32** %types, align 8, !tbaa !38
  %arrayidx23.us = getelementptr inbounds i32, i32* %9, i64 %indvars.iv
  %10 = load i32, i32* %arrayidx23.us, align 4, !tbaa !6
  %switch.selectcmp.i.us = icmp eq i32 %10, 2
  %switch.select.i.us = select i1 %switch.selectcmp.i.us, i32 2, i32 1
  %switch.selectcmp9.i.us = icmp eq i32 %10, 3
  %switch.select10.i.us = select i1 %switch.selectcmp9.i.us, i32 3, i32 %switch.select.i.us
  %arrayidx26.us = getelementptr inbounds i64, i64* %datas, i64 %8
  %11 = load i64, i64* %arrayidx26.us, align 8, !tbaa !47
  %12 = inttoptr i64 %11 to i8*
  %arrayidx28.us = getelementptr inbounds i64, i64* %nulls, i64 %8
  %13 = load i64, i64* %arrayidx28.us, align 8, !tbaa !47
  %14 = inttoptr i64 %13 to i32*
  %call29.us = tail call noalias nonnull dereferenceable(40) i8* @_Znwm(i64 40) #20
  %15 = bitcast i8* %call29.us to i32 (...)***
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [4 x i8*] }, { [4 x i8*] }* @_ZTV6Column, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %15, align 8, !tbaa !8
  %data.i.us = getelementptr inbounds i8, i8* %call29.us, i64 8
  %16 = bitcast i8* %data.i.us to i8**
  store i8* %12, i8** %16, align 8, !tbaa !10
  %nulls.i.us = getelementptr inbounds i8, i8* %call29.us, i64 16
  %17 = bitcast i8* %nulls.i.us to i32**
  store i32* %14, i32** %17, align 8, !tbaa !61
  %type.i.us = getelementptr inbounds i8, i8* %call29.us, i64 24
  %18 = bitcast i8* %type.i.us to i32*
  store i32 %switch.select10.i.us, i32* %18, align 8, !tbaa !14
  %size.i.us = getelementptr inbounds i8, i8* %call29.us, i64 32
  %19 = bitcast i8* %size.i.us to i64*
  store i64 %conv30.us, i64* %19, align 8, !tbaa !15
  %20 = load %class.Column***, %class.Column**** %columns, align 8, !tbaa !43
  %arrayidx33.us = getelementptr inbounds %class.Column**, %class.Column*** %20, i64 %indvars.iv
  %21 = load %class.Column**, %class.Column*** %arrayidx33.us, align 8, !tbaa !2
  %arrayidx35.us = getelementptr inbounds %class.Column*, %class.Column** %21, i64 %indvars.iv105
  %22 = bitcast %class.Column** %arrayidx35.us to i8**
  store i8* %call29.us, i8** %22, align 8, !tbaa !2
  %indvars.iv.next = add nuw nsw i64 %indvars.iv, 1
  %exitcond.not = icmp eq i64 %indvars.iv.next, %wide.trip.count
  br i1 %exitcond.not, label %for.cond17.for.cond39.preheader_crit_edge.us, label %for.body21.us, !llvm.loop !62

for.cond17.for.cond39.preheader_crit_edge.us:     ; preds = %for.body21.us
  %cmp4088.us = icmp sgt i32 %5, 0
  br i1 %cmp4088.us, label %for.body42.lr.ph.us, label %for.cond.cleanup41.us

for.body42.lr.ph.us:                              ; preds = %for.cond17.for.cond39.preheader_crit_edge.us
  %shl.i.us = shl nuw nsw i64 %indvars.iv105, 32
  %23 = load i64*, i64** %valueAddresses, align 8, !tbaa !46
  %24 = sext i32 %valueAddrIdx.092.us to i64
  %wide.trip.count103 = zext i32 %5 to i64
  %25 = zext i32 %5 to i64
  %min.iters.check138 = icmp ult i32 %5, 4
  br i1 %min.iters.check138, label %for.body42.us.preheader, label %vector.ph139

vector.ph139:                                     ; preds = %for.body42.lr.ph.us
  %n.vec141 = and i64 %25, 4294967292
  %ind.end145 = add nsw i64 %n.vec141, %24
  %broadcast.splatinsert153 = insertelement <2 x i64> poison, i64 %shl.i.us, i32 0
  %broadcast.splat154 = shufflevector <2 x i64> %broadcast.splatinsert153, <2 x i64> poison, <2 x i32> zeroinitializer
  %broadcast.splatinsert155 = insertelement <2 x i64> poison, i64 %shl.i.us, i32 0
  %broadcast.splat156 = shufflevector <2 x i64> %broadcast.splatinsert155, <2 x i64> poison, <2 x i32> zeroinitializer
  %26 = add nsw i64 %n.vec141, -4
  %27 = lshr exact i64 %26, 2
  %28 = add nuw nsw i64 %27, 1
  %xtraiter = and i64 %28, 1
  %29 = icmp eq i64 %26, 0
  br i1 %29, label %middle.block135.unr-lcssa, label %vector.ph139.new

vector.ph139.new:                                 ; preds = %vector.ph139
  %unroll_iter = and i64 %28, 9223372036854775806
  br label %vector.body137

vector.body137:                                   ; preds = %vector.body137, %vector.ph139.new
  %index142 = phi i64 [ 0, %vector.ph139.new ], [ %index.next143.1, %vector.body137 ]
  %vec.ind149 = phi <2 x i64> [ <i64 0, i64 1>, %vector.ph139.new ], [ %vec.ind.next152.1, %vector.body137 ]
  %niter = phi i64 [ %unroll_iter, %vector.ph139.new ], [ %niter.nsub.1, %vector.body137 ]
  %offset.idx148 = add i64 %index142, %24
  %step.add150 = add <2 x i64> %vec.ind149, <i64 2, i64 2>
  %30 = or <2 x i64> %broadcast.splat154, %vec.ind149
  %31 = or <2 x i64> %broadcast.splat156, %step.add150
  %32 = getelementptr inbounds i64, i64* %23, i64 %offset.idx148
  %33 = bitcast i64* %32 to <2 x i64>*
  store <2 x i64> %30, <2 x i64>* %33, align 8, !tbaa !47
  %34 = getelementptr inbounds i64, i64* %32, i64 2
  %35 = bitcast i64* %34 to <2 x i64>*
  store <2 x i64> %31, <2 x i64>* %35, align 8, !tbaa !47
  %index.next143 = or i64 %index142, 4
  %vec.ind.next152 = add <2 x i64> %vec.ind149, <i64 4, i64 4>
  %offset.idx148.1 = add i64 %index.next143, %24
  %step.add150.1 = add <2 x i64> %vec.ind149, <i64 6, i64 6>
  %36 = or <2 x i64> %broadcast.splat154, %vec.ind.next152
  %37 = or <2 x i64> %broadcast.splat156, %step.add150.1
  %38 = getelementptr inbounds i64, i64* %23, i64 %offset.idx148.1
  %39 = bitcast i64* %38 to <2 x i64>*
  store <2 x i64> %36, <2 x i64>* %39, align 8, !tbaa !47
  %40 = getelementptr inbounds i64, i64* %38, i64 2
  %41 = bitcast i64* %40 to <2 x i64>*
  store <2 x i64> %37, <2 x i64>* %41, align 8, !tbaa !47
  %index.next143.1 = add i64 %index142, 8
  %vec.ind.next152.1 = add <2 x i64> %vec.ind149, <i64 8, i64 8>
  %niter.nsub.1 = add i64 %niter, -2
  %niter.ncmp.1 = icmp eq i64 %niter.nsub.1, 0
  br i1 %niter.ncmp.1, label %middle.block135.unr-lcssa, label %vector.body137, !llvm.loop !63

middle.block135.unr-lcssa:                        ; preds = %vector.body137, %vector.ph139
  %index142.unr = phi i64 [ 0, %vector.ph139 ], [ %index.next143.1, %vector.body137 ]
  %vec.ind149.unr = phi <2 x i64> [ <i64 0, i64 1>, %vector.ph139 ], [ %vec.ind.next152.1, %vector.body137 ]
  %lcmp.mod.not = icmp eq i64 %xtraiter, 0
  br i1 %lcmp.mod.not, label %middle.block135, label %vector.body137.epil

vector.body137.epil:                              ; preds = %middle.block135.unr-lcssa
  %offset.idx148.epil = add i64 %index142.unr, %24
  %step.add150.epil = add <2 x i64> %vec.ind149.unr, <i64 2, i64 2>
  %42 = or <2 x i64> %broadcast.splat154, %vec.ind149.unr
  %43 = or <2 x i64> %broadcast.splat156, %step.add150.epil
  %44 = getelementptr inbounds i64, i64* %23, i64 %offset.idx148.epil
  %45 = bitcast i64* %44 to <2 x i64>*
  store <2 x i64> %42, <2 x i64>* %45, align 8, !tbaa !47
  %46 = getelementptr inbounds i64, i64* %44, i64 2
  %47 = bitcast i64* %46 to <2 x i64>*
  store <2 x i64> %43, <2 x i64>* %47, align 8, !tbaa !47
  br label %middle.block135

middle.block135:                                  ; preds = %middle.block135.unr-lcssa, %vector.body137.epil
  %cmp.n147 = icmp eq i64 %n.vec141, %25
  br i1 %cmp.n147, label %for.cond.cleanup41.us.loopexit, label %for.body42.us.preheader

for.body42.us.preheader:                          ; preds = %for.body42.lr.ph.us, %middle.block135
  %indvars.iv101.ph = phi i64 [ %24, %for.body42.lr.ph.us ], [ %ind.end145, %middle.block135 ]
  %indvars.iv99.ph = phi i64 [ 0, %for.body42.lr.ph.us ], [ %n.vec141, %middle.block135 ]
  br label %for.body42.us

for.body.for.body_crit_edge:                      ; preds = %for.body.lr.ph, %for.body.for.body_crit_edge
  %indvars.iv.next121130 = phi i64 [ %indvars.iv.next121, %for.body.for.body_crit_edge ], [ 1, %for.body.lr.ph ]
  %.pre = load %class.Column***, %class.Column**** %columns, align 8, !tbaa !43
  %call6 = tail call noalias i8* @malloc(i64 %mul5) #19
  %arrayidx = getelementptr inbounds %class.Column**, %class.Column*** %.pre, i64 %indvars.iv.next121130
  %48 = bitcast %class.Column*** %arrayidx to i8**
  store i8* %call6, i8** %48, align 8, !tbaa !2
  %indvars.iv.next121 = add nuw nsw i64 %indvars.iv.next121130, 1
  %exitcond123.not = icmp eq i64 %indvars.iv.next121, %wide.trip.count122
  br i1 %exitcond123.not, label %for.cond8.preheader, label %for.body.for.body_crit_edge, !llvm.loop !57

for.cond.cleanup10:                               ; preds = %for.cond.cleanup41, %for.cond.cleanup41.us, %for.cond8.preheader.thread, %for.cond8.preheader
  ret void

for.body11:                                       ; preds = %for.body11.preheader, %for.cond.cleanup41
  %indvars.iv116 = phi i64 [ 0, %for.body11.preheader ], [ %indvars.iv.next117, %for.cond.cleanup41 ]
  %valueAddrIdx.092 = phi i32 [ 0, %for.body11.preheader ], [ %valueAddrIdx.1.lcssa, %for.cond.cleanup41 ]
  %arrayidx13 = getelementptr inbounds i32, i32* %rowCounts, i64 %indvars.iv116
  %49 = load i32, i32* %arrayidx13, align 4, !tbaa !6
  %cmp4088 = icmp sgt i32 %49, 0
  br i1 %cmp4088, label %for.body42.lr.ph, label %for.cond.cleanup41

for.body42.lr.ph:                                 ; preds = %for.body11
  %shl.i = shl nuw nsw i64 %indvars.iv116, 32
  %50 = load i64*, i64** %valueAddresses, align 8, !tbaa !46
  %51 = sext i32 %valueAddrIdx.092 to i64
  %wide.trip.count114 = zext i32 %49 to i64
  %52 = zext i32 %49 to i64
  %min.iters.check = icmp ult i32 %49, 4
  br i1 %min.iters.check, label %for.body42.preheader, label %vector.ph

vector.ph:                                        ; preds = %for.body42.lr.ph
  %n.vec = and i64 %52, 4294967292
  %ind.end = add nsw i64 %n.vec, %51
  %broadcast.splatinsert = insertelement <2 x i64> poison, i64 %shl.i, i32 0
  %broadcast.splat = shufflevector <2 x i64> %broadcast.splatinsert, <2 x i64> poison, <2 x i32> zeroinitializer
  %broadcast.splatinsert133 = insertelement <2 x i64> poison, i64 %shl.i, i32 0
  %broadcast.splat134 = shufflevector <2 x i64> %broadcast.splatinsert133, <2 x i64> poison, <2 x i32> zeroinitializer
  %53 = add nsw i64 %n.vec, -4
  %54 = lshr exact i64 %53, 2
  %55 = add nuw nsw i64 %54, 1
  %xtraiter160 = and i64 %55, 1
  %56 = icmp eq i64 %53, 0
  br i1 %56, label %middle.block.unr-lcssa, label %vector.ph.new

vector.ph.new:                                    ; preds = %vector.ph
  %unroll_iter162 = and i64 %55, 9223372036854775806
  br label %vector.body

vector.body:                                      ; preds = %vector.body, %vector.ph.new
  %index = phi i64 [ 0, %vector.ph.new ], [ %index.next.1, %vector.body ]
  %vec.ind = phi <2 x i64> [ <i64 0, i64 1>, %vector.ph.new ], [ %vec.ind.next.1, %vector.body ]
  %niter163 = phi i64 [ %unroll_iter162, %vector.ph.new ], [ %niter163.nsub.1, %vector.body ]
  %offset.idx = add i64 %index, %51
  %step.add = add <2 x i64> %vec.ind, <i64 2, i64 2>
  %57 = or <2 x i64> %broadcast.splat, %vec.ind
  %58 = or <2 x i64> %broadcast.splat134, %step.add
  %59 = getelementptr inbounds i64, i64* %50, i64 %offset.idx
  %60 = bitcast i64* %59 to <2 x i64>*
  store <2 x i64> %57, <2 x i64>* %60, align 8, !tbaa !47
  %61 = getelementptr inbounds i64, i64* %59, i64 2
  %62 = bitcast i64* %61 to <2 x i64>*
  store <2 x i64> %58, <2 x i64>* %62, align 8, !tbaa !47
  %index.next = or i64 %index, 4
  %vec.ind.next = add <2 x i64> %vec.ind, <i64 4, i64 4>
  %offset.idx.1 = add i64 %index.next, %51
  %step.add.1 = add <2 x i64> %vec.ind, <i64 6, i64 6>
  %63 = or <2 x i64> %broadcast.splat, %vec.ind.next
  %64 = or <2 x i64> %broadcast.splat134, %step.add.1
  %65 = getelementptr inbounds i64, i64* %50, i64 %offset.idx.1
  %66 = bitcast i64* %65 to <2 x i64>*
  store <2 x i64> %63, <2 x i64>* %66, align 8, !tbaa !47
  %67 = getelementptr inbounds i64, i64* %65, i64 2
  %68 = bitcast i64* %67 to <2 x i64>*
  store <2 x i64> %64, <2 x i64>* %68, align 8, !tbaa !47
  %index.next.1 = add i64 %index, 8
  %vec.ind.next.1 = add <2 x i64> %vec.ind, <i64 8, i64 8>
  %niter163.nsub.1 = add i64 %niter163, -2
  %niter163.ncmp.1 = icmp eq i64 %niter163.nsub.1, 0
  br i1 %niter163.ncmp.1, label %middle.block.unr-lcssa, label %vector.body, !llvm.loop !64

middle.block.unr-lcssa:                           ; preds = %vector.body, %vector.ph
  %index.unr = phi i64 [ 0, %vector.ph ], [ %index.next.1, %vector.body ]
  %vec.ind.unr = phi <2 x i64> [ <i64 0, i64 1>, %vector.ph ], [ %vec.ind.next.1, %vector.body ]
  %lcmp.mod161.not = icmp eq i64 %xtraiter160, 0
  br i1 %lcmp.mod161.not, label %middle.block, label %vector.body.epil

vector.body.epil:                                 ; preds = %middle.block.unr-lcssa
  %offset.idx.epil = add i64 %index.unr, %51
  %step.add.epil = add <2 x i64> %vec.ind.unr, <i64 2, i64 2>
  %69 = or <2 x i64> %broadcast.splat, %vec.ind.unr
  %70 = or <2 x i64> %broadcast.splat134, %step.add.epil
  %71 = getelementptr inbounds i64, i64* %50, i64 %offset.idx.epil
  %72 = bitcast i64* %71 to <2 x i64>*
  store <2 x i64> %69, <2 x i64>* %72, align 8, !tbaa !47
  %73 = getelementptr inbounds i64, i64* %71, i64 2
  %74 = bitcast i64* %73 to <2 x i64>*
  store <2 x i64> %70, <2 x i64>* %74, align 8, !tbaa !47
  br label %middle.block

middle.block:                                     ; preds = %middle.block.unr-lcssa, %vector.body.epil
  %cmp.n = icmp eq i64 %n.vec, %52
  br i1 %cmp.n, label %for.cond.cleanup41.loopexit, label %for.body42.preheader

for.body42.preheader:                             ; preds = %for.body42.lr.ph, %middle.block
  %indvars.iv112.ph = phi i64 [ %51, %for.body42.lr.ph ], [ %ind.end, %middle.block ]
  %indvars.iv110.ph = phi i64 [ 0, %for.body42.lr.ph ], [ %n.vec, %middle.block ]
  br label %for.body42

for.cond.cleanup41.loopexit:                      ; preds = %for.body42, %middle.block
  %indvars.iv.next113.lcssa = phi i64 [ %ind.end, %middle.block ], [ %indvars.iv.next113, %for.body42 ]
  %75 = trunc i64 %indvars.iv.next113.lcssa to i32
  br label %for.cond.cleanup41

for.cond.cleanup41:                               ; preds = %for.cond.cleanup41.loopexit, %for.body11
  %valueAddrIdx.1.lcssa = phi i32 [ %valueAddrIdx.092, %for.body11 ], [ %75, %for.cond.cleanup41.loopexit ]
  %indvars.iv.next117 = add nuw nsw i64 %indvars.iv116, 1
  %exitcond119.not = icmp eq i64 %indvars.iv.next117, %wide.trip.count118
  br i1 %exitcond119.not, label %for.cond.cleanup10, label %for.body11, !llvm.loop !58

for.body42:                                       ; preds = %for.body42.preheader, %for.body42
  %indvars.iv112 = phi i64 [ %indvars.iv.next113, %for.body42 ], [ %indvars.iv112.ph, %for.body42.preheader ]
  %indvars.iv110 = phi i64 [ %indvars.iv.next111, %for.body42 ], [ %indvars.iv110.ph, %for.body42.preheader ]
  %or.i = or i64 %shl.i, %indvars.iv110
  %arrayidx46 = getelementptr inbounds i64, i64* %50, i64 %indvars.iv112
  store i64 %or.i, i64* %arrayidx46, align 8, !tbaa !47
  %indvars.iv.next113 = add nsw i64 %indvars.iv112, 1
  %indvars.iv.next111 = add nuw nsw i64 %indvars.iv110, 1
  %exitcond115.not = icmp eq i64 %indvars.iv.next111, %wide.trip.count114
  br i1 %exitcond115.not, label %for.cond.cleanup41.loopexit, label %for.body42, !llvm.loop !65
}

; Function Attrs: inaccessiblememonly nofree nounwind willreturn
declare dso_local noalias noundef i8* @malloc(i64) local_unnamed_addr #13

; Function Attrs: nounwind uwtable
define dso_local void @_ZN10PagesIndexD2Ev(%class.PagesIndex* nocapture nonnull readonly dereferenceable(44) %this) unnamed_addr #9 align 2 {
entry:
  %typesCount = getelementptr inbounds %class.PagesIndex, %class.PagesIndex* %this, i64 0, i32 1
  %0 = load i32, i32* %typesCount, align 8, !tbaa !40
  %cmp24 = icmp sgt i32 %0, 0
  br i1 %cmp24, label %for.cond2.preheader.lr.ph, label %for.cond.cleanup

for.cond2.preheader.lr.ph:                        ; preds = %entry
  %tableCount = getelementptr inbounds %class.PagesIndex, %class.PagesIndex* %this, i64 0, i32 7
  %columns = getelementptr inbounds %class.PagesIndex, %class.PagesIndex* %this, i64 0, i32 6
  br label %for.cond2.preheader

for.cond2.preheader:                              ; preds = %for.cond2.preheader.lr.ph, %for.cond.cleanup4
  %indvars.iv27 = phi i64 [ 0, %for.cond2.preheader.lr.ph ], [ %indvars.iv.next28, %for.cond.cleanup4 ]
  %1 = load i32, i32* %tableCount, align 8, !tbaa !42
  %cmp322 = icmp sgt i32 %1, 0
  br i1 %cmp322, label %for.body5, label %for.cond.cleanup4

for.cond.cleanup:                                 ; preds = %for.cond.cleanup4, %entry
  %columns14 = getelementptr inbounds %class.PagesIndex, %class.PagesIndex* %this, i64 0, i32 6
  %2 = bitcast %class.Column**** %columns14 to i8**
  %3 = load i8*, i8** %2, align 8, !tbaa !43
  tail call void @free(i8* %3) #19
  %valueAddresses = getelementptr inbounds %class.PagesIndex, %class.PagesIndex* %this, i64 0, i32 3
  %4 = bitcast i64** %valueAddresses to i8**
  %5 = load i8*, i8** %4, align 8, !tbaa !46
  tail call void @free(i8* %5) #19
  ret void

for.cond.cleanup4:                                ; preds = %for.inc, %for.cond2.preheader
  %6 = load %class.Column***, %class.Column**** %columns, align 8, !tbaa !43
  %arrayidx10 = getelementptr inbounds %class.Column**, %class.Column*** %6, i64 %indvars.iv27
  %7 = bitcast %class.Column*** %arrayidx10 to i8**
  %8 = load i8*, i8** %7, align 8, !tbaa !2
  tail call void @free(i8* %8) #19
  %indvars.iv.next28 = add nuw nsw i64 %indvars.iv27, 1
  %9 = load i32, i32* %typesCount, align 8, !tbaa !40
  %10 = sext i32 %9 to i64
  %cmp = icmp slt i64 %indvars.iv.next28, %10
  br i1 %cmp, label %for.cond2.preheader, label %for.cond.cleanup, !llvm.loop !44

for.body5:                                        ; preds = %for.cond2.preheader, %for.inc
  %11 = phi i32 [ %17, %for.inc ], [ %1, %for.cond2.preheader ]
  %indvars.iv = phi i64 [ %indvars.iv.next, %for.inc ], [ 0, %for.cond2.preheader ]
  %12 = load %class.Column***, %class.Column**** %columns, align 8, !tbaa !43
  %arrayidx = getelementptr inbounds %class.Column**, %class.Column*** %12, i64 %indvars.iv27
  %13 = load %class.Column**, %class.Column*** %arrayidx, align 8, !tbaa !2
  %arrayidx7 = getelementptr inbounds %class.Column*, %class.Column** %13, i64 %indvars.iv
  %14 = load %class.Column*, %class.Column** %arrayidx7, align 8, !tbaa !2
  %isnull = icmp eq %class.Column* %14, null
  br i1 %isnull, label %for.inc, label %delete.notnull

delete.notnull:                                   ; preds = %for.body5
  %15 = bitcast %class.Column* %14 to void (%class.Column*)***
  %vtable = load void (%class.Column*)**, void (%class.Column*)*** %15, align 8, !tbaa !8
  %vfn = getelementptr inbounds void (%class.Column*)*, void (%class.Column*)** %vtable, i64 1
  %16 = load void (%class.Column*)*, void (%class.Column*)** %vfn, align 8
  tail call void %16(%class.Column* nonnull dereferenceable(40) %14) #19
  %.pre = load i32, i32* %tableCount, align 8, !tbaa !42
  br label %for.inc

for.inc:                                          ; preds = %for.body5, %delete.notnull
  %17 = phi i32 [ %11, %for.body5 ], [ %.pre, %delete.notnull ]
  %indvars.iv.next = add nuw nsw i64 %indvars.iv, 1
  %18 = sext i32 %17 to i64
  %cmp3 = icmp slt i64 %indvars.iv.next, %18
  br i1 %cmp3, label %for.body5, label %for.cond.cleanup4, !llvm.loop !45
}

; Function Attrs: inaccessiblemem_or_argmemonly nounwind willreturn
declare dso_local void @free(i8* nocapture noundef) local_unnamed_addr #14

; Function Attrs: uwtable
define dso_local i64 @_Z10createSortPiiS_iS_S_S_i(i32* %sourceTypes, i32 %typeCount, i32* %outputCols, i32 %outputColCount, i32* %sortCols, i32* %sortAscendings, i32* %sortNullFirsts, i32 %sortColCount) local_unnamed_addr #4 personality i8* bitcast (i32 (...)* @__gxx_personality_v0 to i8*) {
entry:
  %call = tail call noalias nonnull dereferenceable(80) i8* @_Znwm(i64 80) #20
  %0 = bitcast i8* %call to i32 (...)***
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [9 x i8*] }, { [9 x i8*] }* @_ZTV4Sort, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %0, align 8, !tbaa !8
  %sourceTypes2.i = getelementptr inbounds i8, i8* %call, i64 8
  %1 = bitcast i8* %sourceTypes2.i to i32**
  store i32* %sourceTypes, i32** %1, align 8, !tbaa !29
  %typesCount3.i = getelementptr inbounds i8, i8* %call, i64 16
  %2 = bitcast i8* %typesCount3.i to i32*
  store i32 %typeCount, i32* %2, align 8, !tbaa !31
  %outputCols4.i = getelementptr inbounds i8, i8* %call, i64 24
  %3 = bitcast i8* %outputCols4.i to i32**
  store i32* %outputCols, i32** %3, align 8, !tbaa !32
  %outputColsCount5.i = getelementptr inbounds i8, i8* %call, i64 32
  %4 = bitcast i8* %outputColsCount5.i to i32*
  store i32 %outputColCount, i32* %4, align 8, !tbaa !33
  %sortCols6.i = getelementptr inbounds i8, i8* %call, i64 40
  %5 = bitcast i8* %sortCols6.i to i32**
  store i32* %sortCols, i32** %5, align 8, !tbaa !34
  %sortAscendings7.i = getelementptr inbounds i8, i8* %call, i64 48
  %6 = bitcast i8* %sortAscendings7.i to i32**
  store i32* %sortAscendings, i32** %6, align 8, !tbaa !35
  %sortNullFirsts8.i = getelementptr inbounds i8, i8* %call, i64 56
  %7 = bitcast i8* %sortNullFirsts8.i to i32**
  store i32* %sortNullFirsts, i32** %7, align 8, !tbaa !36
  %sortColCount9.i = getelementptr inbounds i8, i8* %call, i64 64
  %8 = bitcast i8* %sortColCount9.i to i32*
  store i32 %sortColCount, i32* %8, align 8, !tbaa !37
  %call.i4 = invoke noalias nonnull dereferenceable(48) i8* @_Znwm(i64 48) #20
          to label %invoke.cont unwind label %lpad

invoke.cont:                                      ; preds = %entry
  %types.i.i = bitcast i8* %call.i4 to i32**
  store i32* %sourceTypes, i32** %types.i.i, align 8, !tbaa !38
  %typesCount2.i.i = getelementptr inbounds i8, i8* %call.i4, i64 8
  %9 = bitcast i8* %typesCount2.i.i to i32*
  store i32 %typeCount, i32* %9, align 8, !tbaa !40
  %pagesIndex.i = getelementptr inbounds i8, i8* %call, i64 72
  %10 = bitcast i8* %pagesIndex.i to i8**
  store i8* %call.i4, i8** %10, align 8, !tbaa !41
  %11 = ptrtoint i8* %call to i64
  ret i64 %11

lpad:                                             ; preds = %entry
  %12 = landingpad { i8*, i32 }
          cleanup
  tail call void @_ZdlPv(i8* nonnull %call) #22
  resume { i8*, i32 } %12
}

; Function Attrs: norecurse nounwind readonly uwtable willreturn mustprogress
define dso_local i32 @_Z11compareNullPiiS_ii(i32* nocapture readonly %leftNulls, i32 %leftPosition, i32* nocapture readonly %rightNulls, i32 %rightPosition, i32 %nullsFirst) local_unnamed_addr #15 {
entry:
  %idxprom = sext i32 %leftPosition to i64
  %arrayidx = getelementptr inbounds i32, i32* %leftNulls, i64 %idxprom
  %0 = load i32, i32* %arrayidx, align 4, !tbaa !6
  %idxprom1 = sext i32 %rightPosition to i64
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
define dso_local i32 @_Z15compareIntValuePiiS_i(i32* nocapture readonly %leftData, i32 %leftPosition, i32* nocapture readonly %rightData, i32 %rightPosition) local_unnamed_addr #15 {
entry:
  %idxprom = sext i32 %leftPosition to i64
  %arrayidx = getelementptr inbounds i32, i32* %leftData, i64 %idxprom
  %0 = load i32, i32* %arrayidx, align 4, !tbaa !6
  %idxprom1 = sext i32 %rightPosition to i64
  %arrayidx2 = getelementptr inbounds i32, i32* %rightData, i64 %idxprom1
  %1 = load i32, i32* %arrayidx2, align 4, !tbaa !6
  %sub = sub nsw i32 %0, %1
  ret i32 %sub
}

; Function Attrs: norecurse nounwind readonly uwtable willreturn mustprogress
define dso_local i32 @_Z17compareInt64ValuePliS_i(i64* nocapture readonly %leftData, i32 %leftPosition, i64* nocapture readonly %rightData, i32 %rightPosition) local_unnamed_addr #15 {
entry:
  %idxprom = sext i32 %leftPosition to i64
  %arrayidx = getelementptr inbounds i64, i64* %leftData, i64 %idxprom
  %0 = load i64, i64* %arrayidx, align 8, !tbaa !47
  %idxprom1 = sext i32 %rightPosition to i64
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
define dso_local i32 @_Z18compareDoubleValuePdiS_i(double* nocapture readonly %leftData, i32 %leftPosition, double* nocapture readonly %rightData, i32 %rightPosition) local_unnamed_addr #15 {
entry:
  %idxprom = sext i32 %leftPosition to i64
  %arrayidx = getelementptr inbounds double, double* %leftData, i64 %idxprom
  %0 = load double, double* %arrayidx, align 8, !tbaa !66
  %idxprom1 = sext i32 %rightPosition to i64
  %arrayidx2 = getelementptr inbounds double, double* %rightData, i64 %idxprom1
  %1 = load double, double* %arrayidx2, align 8, !tbaa !66
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
define dso_local i32 @_Z9compareTolPiS_S_S_iii(i64 %pagesIndexAddr, i32* nocapture readonly %sortCols, i32* nocapture readonly %sortColTypes, i32* nocapture readonly %sortAscendings, i32* nocapture readnone %sortNullFirsts, i32 %sortColCount, i32 %leftPosition, i32 %rightPosition) local_unnamed_addr #15 {
entry:
  %0 = inttoptr i64 %pagesIndexAddr to %class.PagesIndex*
  %valueAddresses.i = getelementptr inbounds %class.PagesIndex, %class.PagesIndex* %0, i64 0, i32 3
  %1 = load i64*, i64** %valueAddresses.i, align 8, !tbaa !46
  %columns.i = getelementptr inbounds %class.PagesIndex, %class.PagesIndex* %0, i64 0, i32 6
  %2 = load %class.Column***, %class.Column**** %columns.i, align 8, !tbaa !43
  %idxprom = sext i32 %leftPosition to i64
  %arrayidx = getelementptr inbounds i64, i64* %1, i64 %idxprom
  %3 = load i64, i64* %arrayidx, align 8, !tbaa !47
  %idxprom4 = sext i32 %rightPosition to i64
  %arrayidx5 = getelementptr inbounds i64, i64* %1, i64 %idxprom4
  %4 = load i64, i64* %arrayidx5, align 8, !tbaa !47
  %cmp8126 = icmp sgt i32 %sortColCount, 0
  br i1 %cmp8126, label %for.body.lr.ph, label %cleanup47

for.body.lr.ph:                                   ; preds = %entry
  %5 = lshr i64 %3, 32
  %conv.i120 = trunc i64 %5 to i32
  %6 = lshr i64 %4, 32
  %conv.i118 = trunc i64 %6 to i32
  %cmp = icmp eq i32 %conv.i120, %conv.i118
  %idxprom13 = ashr i64 %3, 32
  %idxprom22 = ashr i64 %4, 32
  %sext = shl i64 %3, 32
  %idxprom.i = ashr exact i64 %sext, 32
  %sext121 = shl i64 %4, 32
  %idxprom1.i = ashr exact i64 %sext121, 32
  %wide.trip.count = zext i32 %sortColCount to i64
  br i1 %cmp, label %for.body.us, label %for.body

for.body.us:                                      ; preds = %for.body.lr.ph, %for.cond.us
  %indvars.iv = phi i64 [ %indvars.iv.next, %for.cond.us ], [ 0, %for.body.lr.ph ]
  %arrayidx10.us = getelementptr inbounds i32, i32* %sortCols, i64 %indvars.iv
  %7 = load i32, i32* %arrayidx10.us, align 4, !tbaa !6
  %idxprom11.us = sext i32 %7 to i64
  %arrayidx12.us = getelementptr inbounds %class.Column**, %class.Column*** %2, i64 %idxprom11.us
  %8 = load %class.Column**, %class.Column*** %arrayidx12.us, align 8, !tbaa !2
  %arrayidx14.us = getelementptr inbounds %class.Column*, %class.Column** %8, i64 %idxprom13
  %9 = load %class.Column*, %class.Column** %arrayidx14.us, align 8, !tbaa !2
  %data.i117.us = getelementptr inbounds %class.Column, %class.Column* %9, i64 0, i32 1
  %10 = load i8*, i8** %data.i117.us, align 8, !tbaa !10
  %arrayidx18.us = getelementptr inbounds i32, i32* %sortColTypes, i64 %indvars.iv
  %11 = load i32, i32* %arrayidx18.us, align 4, !tbaa !6
  switch i32 %11, label %sw.epilog.us [
    i32 1, label %sw.bb.us
    i32 2, label %sw.bb28.us
    i32 3, label %sw.bb30.us
  ]

for.cond.us:                                      ; preds = %sw.epilog.us
  %exitcond.not = icmp eq i64 %indvars.iv.next, %wide.trip.count
  br i1 %exitcond.not, label %cleanup47, label %for.body.us, !llvm.loop !68

sw.bb30.us:                                       ; preds = %for.body.us
  %12 = bitcast i8* %10 to double*
  %arrayidx.i.us = getelementptr inbounds double, double* %12, i64 %idxprom.i
  %13 = load double, double* %arrayidx.i.us, align 8, !tbaa !66
  %arrayidx2.i.us = getelementptr inbounds double, double* %12, i64 %idxprom1.i
  %14 = load double, double* %arrayidx2.i.us, align 8, !tbaa !66
  %cmp.i.us = fcmp ogt double %13, %14
  br i1 %cmp.i.us, label %sw.epilog.us, label %if.else.i.us

if.else.i.us:                                     ; preds = %sw.bb30.us
  %cmp3.i.us = fcmp olt double %13, %14
  br i1 %cmp3.i.us, label %sw.epilog.us, label %if.else5.i.us

if.else5.i.us:                                    ; preds = %if.else.i.us
  br label %sw.epilog.us

sw.bb28.us:                                       ; preds = %for.body.us
  %15 = bitcast i8* %10 to i64*
  %arrayidx.i105.us = getelementptr inbounds i64, i64* %15, i64 %idxprom.i
  %16 = load i64, i64* %arrayidx.i105.us, align 8, !tbaa !47
  %arrayidx2.i107.us = getelementptr inbounds i64, i64* %15, i64 %idxprom1.i
  %17 = load i64, i64* %arrayidx2.i107.us, align 8, !tbaa !47
  %cmp.i108.us = icmp sgt i64 %16, %17
  br i1 %cmp.i108.us, label %sw.epilog.us, label %if.else.i110.us

if.else.i110.us:                                  ; preds = %sw.bb28.us
  %cmp3.i109.us = icmp slt i64 %16, %17
  %spec.select.i.us = sext i1 %cmp3.i109.us to i32
  br label %sw.epilog.us

sw.bb.us:                                         ; preds = %for.body.us
  %18 = bitcast i8* %10 to i32*
  %arrayidx.i113.us = getelementptr inbounds i32, i32* %18, i64 %idxprom.i
  %19 = load i32, i32* %arrayidx.i113.us, align 4, !tbaa !6
  %arrayidx2.i115.us = getelementptr inbounds i32, i32* %18, i64 %idxprom1.i
  %20 = load i32, i32* %arrayidx2.i115.us, align 4, !tbaa !6
  %sub.i.us = sub nsw i32 %19, %20
  br label %sw.epilog.us

sw.epilog.us:                                     ; preds = %sw.bb.us, %if.else.i110.us, %sw.bb28.us, %if.else5.i.us, %if.else.i.us, %sw.bb30.us, %for.body.us
  %compare.1.us = phi i32 [ 0, %for.body.us ], [ %sub.i.us, %sw.bb.us ], [ %spec.select.i.us, %if.else.i110.us ], [ 1, %sw.bb28.us ], [ 0, %if.else5.i.us ], [ 1, %sw.bb30.us ], [ -1, %if.else.i.us ]
  %arrayidx33.us = getelementptr inbounds i32, i32* %sortAscendings, i64 %indvars.iv
  %21 = load i32, i32* %arrayidx33.us, align 4, !tbaa !6
  %cmp34.us = icmp eq i32 %21, 0
  %sub.us = sub nsw i32 0, %compare.1.us
  %spec.select.us = select i1 %cmp34.us, i32 %sub.us, i32 %compare.1.us
  %cmp37.not.us = icmp eq i32 %spec.select.us, 0
  %indvars.iv.next = add nuw nsw i64 %indvars.iv, 1
  br i1 %cmp37.not.us, label %for.cond.us, label %cleanup47

for.cond:                                         ; preds = %sw.epilog
  %exitcond134.not = icmp eq i64 %indvars.iv.next132, %wide.trip.count
  br i1 %exitcond134.not, label %cleanup47, label %for.body, !llvm.loop !68

for.body:                                         ; preds = %for.body.lr.ph, %for.cond
  %indvars.iv131 = phi i64 [ %indvars.iv.next132, %for.cond ], [ 0, %for.body.lr.ph ]
  %arrayidx10 = getelementptr inbounds i32, i32* %sortCols, i64 %indvars.iv131
  %22 = load i32, i32* %arrayidx10, align 4, !tbaa !6
  %idxprom11 = sext i32 %22 to i64
  %arrayidx12 = getelementptr inbounds %class.Column**, %class.Column*** %2, i64 %idxprom11
  %23 = load %class.Column**, %class.Column*** %arrayidx12, align 8, !tbaa !2
  %arrayidx14 = getelementptr inbounds %class.Column*, %class.Column** %23, i64 %idxprom13
  %24 = load %class.Column*, %class.Column** %arrayidx14, align 8, !tbaa !2
  %data.i117 = getelementptr inbounds %class.Column, %class.Column* %24, i64 0, i32 1
  %25 = load i8*, i8** %data.i117, align 8, !tbaa !10
  %arrayidx18 = getelementptr inbounds i32, i32* %sortColTypes, i64 %indvars.iv131
  %26 = load i32, i32* %arrayidx18, align 4, !tbaa !6
  %arrayidx23 = getelementptr inbounds %class.Column*, %class.Column** %23, i64 %idxprom22
  %27 = load %class.Column*, %class.Column** %arrayidx23, align 8, !tbaa !2
  %data.i = getelementptr inbounds %class.Column, %class.Column* %27, i64 0, i32 1
  %28 = load i8*, i8** %data.i, align 8, !tbaa !10
  switch i32 %26, label %sw.epilog [
    i32 1, label %sw.bb
    i32 2, label %sw.bb28
    i32 3, label %sw.bb30
  ]

sw.bb:                                            ; preds = %for.body
  %29 = bitcast i8* %25 to i32*
  %30 = bitcast i8* %28 to i32*
  %arrayidx.i113 = getelementptr inbounds i32, i32* %29, i64 %idxprom.i
  %31 = load i32, i32* %arrayidx.i113, align 4, !tbaa !6
  %arrayidx2.i115 = getelementptr inbounds i32, i32* %30, i64 %idxprom1.i
  %32 = load i32, i32* %arrayidx2.i115, align 4, !tbaa !6
  %sub.i = sub nsw i32 %31, %32
  br label %sw.epilog

sw.bb28:                                          ; preds = %for.body
  %33 = bitcast i8* %25 to i64*
  %34 = bitcast i8* %28 to i64*
  %arrayidx.i105 = getelementptr inbounds i64, i64* %33, i64 %idxprom.i
  %35 = load i64, i64* %arrayidx.i105, align 8, !tbaa !47
  %arrayidx2.i107 = getelementptr inbounds i64, i64* %34, i64 %idxprom1.i
  %36 = load i64, i64* %arrayidx2.i107, align 8, !tbaa !47
  %cmp.i108 = icmp sgt i64 %35, %36
  br i1 %cmp.i108, label %sw.epilog, label %if.else.i110

if.else.i110:                                     ; preds = %sw.bb28
  %cmp3.i109 = icmp slt i64 %35, %36
  %spec.select.i = sext i1 %cmp3.i109 to i32
  br label %sw.epilog

sw.bb30:                                          ; preds = %for.body
  %37 = bitcast i8* %25 to double*
  %38 = bitcast i8* %28 to double*
  %arrayidx.i = getelementptr inbounds double, double* %37, i64 %idxprom.i
  %39 = load double, double* %arrayidx.i, align 8, !tbaa !66
  %arrayidx2.i = getelementptr inbounds double, double* %38, i64 %idxprom1.i
  %40 = load double, double* %arrayidx2.i, align 8, !tbaa !66
  %cmp.i = fcmp ogt double %39, %40
  br i1 %cmp.i, label %sw.epilog, label %if.else.i

if.else.i:                                        ; preds = %sw.bb30
  %cmp3.i = fcmp olt double %39, %40
  br i1 %cmp3.i, label %sw.epilog, label %if.else5.i

if.else5.i:                                       ; preds = %if.else.i
  br label %sw.epilog

sw.epilog:                                        ; preds = %if.else5.i, %if.else.i, %sw.bb30, %if.else.i110, %sw.bb28, %for.body, %sw.bb
  %compare.1 = phi i32 [ 0, %for.body ], [ %sub.i, %sw.bb ], [ %spec.select.i, %if.else.i110 ], [ 1, %sw.bb28 ], [ 0, %if.else5.i ], [ 1, %sw.bb30 ], [ -1, %if.else.i ]
  %arrayidx33 = getelementptr inbounds i32, i32* %sortAscendings, i64 %indvars.iv131
  %41 = load i32, i32* %arrayidx33, align 4, !tbaa !6
  %cmp34 = icmp eq i32 %41, 0
  %sub = sub nsw i32 0, %compare.1
  %spec.select = select i1 %cmp34, i32 %sub, i32 %compare.1
  %cmp37.not = icmp eq i32 %spec.select, 0
  %indvars.iv.next132 = add nuw nsw i64 %indvars.iv131, 1
  br i1 %cmp37.not, label %for.cond, label %cleanup47

cleanup47:                                        ; preds = %sw.epilog, %for.cond, %sw.epilog.us, %for.cond.us, %entry
  %compare.3 = phi i32 [ 0, %entry ], [ 0, %for.cond.us ], [ %spec.select.us, %sw.epilog.us ], [ 0, %for.cond ], [ %spec.select, %sw.epilog ]
  ret i32 %compare.3
}

; Function Attrs: norecurse nounwind readonly uwtable willreturn mustprogress
define dso_local i32 @_Z7median3lPiS_S_S_iiii(i64 %pagesIndexAddr, i32* nocapture readonly %sortCols, i32* nocapture readonly %sortColTypes, i32* nocapture readonly %sortAscendings, i32* nocapture readnone %sortNullFirsts, i32 %sortColCount, i32 %a, i32 %b, i32 %c) local_unnamed_addr #15 {
entry:
  %call = tail call i32 @_Z9compareTolPiS_S_S_iii(i64 %pagesIndexAddr, i32* %sortCols, i32* %sortColTypes, i32* %sortAscendings, i32* undef, i32 %sortColCount, i32 %a, i32 %b)
  %call1 = tail call i32 @_Z9compareTolPiS_S_S_iii(i64 %pagesIndexAddr, i32* %sortCols, i32* %sortColTypes, i32* %sortAscendings, i32* undef, i32 %sortColCount, i32 %a, i32 %c)
  %call2 = tail call i32 @_Z9compareTolPiS_S_S_iii(i64 %pagesIndexAddr, i32* %sortCols, i32* %sortColTypes, i32* %sortAscendings, i32* undef, i32 %sortColCount, i32 %b, i32 %c)
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
define dso_local void @_Z9quickSortlPiS_S_S_iii(i64 %pagesIndexAddr, i32* readonly %sortCols, i32* readonly %sortColTypes, i32* readonly %sortAscendings, i32* readnone %sortNullFirsts, i32 %sortColCount, i32 %from, i32 %to) local_unnamed_addr #16 {
entry:
  %0 = inttoptr i64 %pagesIndexAddr to %class.PagesIndex*
  %valueAddresses.i = getelementptr inbounds %class.PagesIndex, %class.PagesIndex* %0, i64 0, i32 3
  %1 = load i64*, i64** %valueAddresses.i, align 8, !tbaa !46
  %sub403 = sub nsw i32 %to, %from
  %cmp404 = icmp slt i32 %sub403, 7
  br i1 %cmp404, label %for.cond.preheader, label %if.end.lr.ph

if.end.lr.ph:                                     ; preds = %entry
  %sub15 = add nsw i32 %to, -1
  %2 = sext i32 %to to i64
  %3 = add nsw i64 %2, 1
  br label %if.end

for.cond.preheader:                               ; preds = %if.then105, %entry
  %from.tr.lcssa = phi i32 [ %from, %entry ], [ %sub106, %if.then105 ]
  %.lcssa = phi i64* [ %1, %entry ], [ %148, %if.then105 ]
  %cmp2377 = icmp slt i32 %from.tr.lcssa, %to
  br i1 %cmp2377, label %for.cond3.preheader.preheader, label %cleanup

for.cond3.preheader.preheader:                    ; preds = %for.cond.preheader
  %4 = sext i32 %from.tr.lcssa to i64
  br label %for.cond3.preheader

for.cond3.preheader:                              ; preds = %for.cond3.preheader.preheader, %for.cond.cleanup8
  %indvars.iv = phi i64 [ %4, %for.cond3.preheader.preheader ], [ %indvars.iv.next, %for.cond.cleanup8 ]
  %i.0378 = phi i32 [ %from.tr.lcssa, %for.cond3.preheader.preheader ], [ %inc, %for.cond.cleanup8 ]
  %cmp4375 = icmp sgt i32 %i.0378, %from.tr.lcssa
  br i1 %cmp4375, label %land.rhs, label %for.cond.cleanup8

land.rhs:                                         ; preds = %for.cond3.preheader, %for.body9
  %indvars.iv422 = phi i64 [ %indvars.iv.next423, %for.body9 ], [ %indvars.iv, %for.cond3.preheader ]
  %j.0376 = phi i32 [ %sub5, %for.body9 ], [ %i.0378, %for.cond3.preheader ]
  %indvars.iv.next423 = add nsw i64 %indvars.iv422, -1
  %sub5 = add nsw i32 %j.0376, -1
  %5 = trunc i64 %indvars.iv422 to i32
  %call6 = tail call i32 @_Z9compareTolPiS_S_S_iii(i64 %pagesIndexAddr, i32* %sortCols, i32* %sortColTypes, i32* %sortAscendings, i32* undef, i32 %sortColCount, i32 %sub5, i32 %5)
  %cmp7 = icmp sgt i32 %call6, 0
  br i1 %cmp7, label %for.body9, label %for.cond.cleanup8

for.cond.cleanup8:                                ; preds = %land.rhs, %for.body9, %for.cond3.preheader
  %inc = add nsw i32 %i.0378, 1
  %indvars.iv.next = add nsw i64 %indvars.iv, 1
  %exitcond.not = icmp eq i32 %inc, %to
  br i1 %exitcond.not, label %cleanup, label %for.cond3.preheader, !llvm.loop !69

for.body9:                                        ; preds = %land.rhs
  %arrayidx2.i = getelementptr inbounds i64, i64* %.lcssa, i64 %indvars.iv.next423
  %6 = bitcast i64* %arrayidx2.i to <2 x i64>*
  %7 = load <2 x i64>, <2 x i64>* %6, align 8, !tbaa !47
  %reorder_shuffle = shufflevector <2 x i64> %7, <2 x i64> poison, <2 x i32> <i32 1, i32 0>
  %8 = bitcast i64* %arrayidx2.i to <2 x i64>*
  store <2 x i64> %reorder_shuffle, <2 x i64>* %8, align 8, !tbaa !47
  %cmp4 = icmp sgt i64 %indvars.iv.next423, %4
  br i1 %cmp4, label %land.rhs, label %for.cond.cleanup8, !llvm.loop !70

if.end:                                           ; preds = %if.end.lr.ph, %if.then105
  %sub406 = phi i32 [ %sub403, %if.end.lr.ph ], [ %sub92, %if.then105 ]
  %9 = phi i64* [ %1, %if.end.lr.ph ], [ %148, %if.then105 ]
  %from.tr405 = phi i32 [ %from, %if.end.lr.ph ], [ %sub106, %if.then105 ]
  %div367 = lshr i32 %sub406, 1
  %add = add nsw i32 %div367, %from.tr405
  %cmp13.not = icmp eq i32 %sub406, 7
  br i1 %cmp13.not, label %while.cond.preheader, label %if.then14

if.then14:                                        ; preds = %if.end
  %cmp16 = icmp sgt i32 %sub406, 40
  br i1 %cmp16, label %if.then17, label %if.end29

if.then17:                                        ; preds = %if.then14
  %div18368 = lshr i32 %sub406, 3
  %add19 = add nsw i32 %div18368, %from.tr405
  %mul = shl nuw nsw i32 %div18368, 1
  %add20 = add nsw i32 %mul, %from.tr405
  %call.i = tail call i32 @_Z9compareTolPiS_S_S_iii(i64 %pagesIndexAddr, i32* %sortCols, i32* %sortColTypes, i32* %sortAscendings, i32* undef, i32 %sortColCount, i32 %from.tr405, i32 %add19) #19
  %call1.i = tail call i32 @_Z9compareTolPiS_S_S_iii(i64 %pagesIndexAddr, i32* %sortCols, i32* %sortColTypes, i32* %sortAscendings, i32* undef, i32 %sortColCount, i32 %from.tr405, i32 %add20) #19
  %call2.i = tail call i32 @_Z9compareTolPiS_S_S_iii(i64 %pagesIndexAddr, i32* %sortCols, i32* %sortColTypes, i32* %sortAscendings, i32* undef, i32 %sortColCount, i32 %add19, i32 %add20) #19
  %cmp.i301 = icmp slt i32 %call.i, 0
  br i1 %cmp.i301, label %cond.true.i, label %cond.false10.i

cond.true.i:                                      ; preds = %if.then17
  %cmp3.i = icmp slt i32 %call2.i, 0
  br i1 %cmp3.i, label %_Z7median3lPiS_S_S_iiii.exit, label %cond.false.i

cond.false.i:                                     ; preds = %cond.true.i
  %cmp5.i = icmp slt i32 %call1.i, 0
  %cond.i = select i1 %cmp5.i, i32 %add20, i32 %from.tr405
  br label %_Z7median3lPiS_S_S_iiii.exit

cond.false10.i:                                   ; preds = %if.then17
  %cmp11.i = icmp sgt i32 %call2.i, 0
  br i1 %cmp11.i, label %_Z7median3lPiS_S_S_iiii.exit, label %cond.false13.i

cond.false13.i:                                   ; preds = %cond.false10.i
  %cmp14.i = icmp sgt i32 %call1.i, 0
  %cond18.i = select i1 %cmp14.i, i32 %add20, i32 %from.tr405
  br label %_Z7median3lPiS_S_S_iiii.exit

_Z7median3lPiS_S_S_iiii.exit:                     ; preds = %cond.true.i, %cond.false.i, %cond.false10.i, %cond.false13.i
  %cond22.i = phi i32 [ %cond.i, %cond.false.i ], [ %cond18.i, %cond.false13.i ], [ %add19, %cond.true.i ], [ %add19, %cond.false10.i ]
  %sub22 = sub nsw i32 %add, %div18368
  %add23 = add nsw i32 %add, %div18368
  %call.i302 = tail call i32 @_Z9compareTolPiS_S_S_iii(i64 %pagesIndexAddr, i32* %sortCols, i32* %sortColTypes, i32* %sortAscendings, i32* undef, i32 %sortColCount, i32 %sub22, i32 %add) #19
  %call1.i303 = tail call i32 @_Z9compareTolPiS_S_S_iii(i64 %pagesIndexAddr, i32* %sortCols, i32* %sortColTypes, i32* %sortAscendings, i32* undef, i32 %sortColCount, i32 %sub22, i32 %add23) #19
  %call2.i304 = tail call i32 @_Z9compareTolPiS_S_S_iii(i64 %pagesIndexAddr, i32* %sortCols, i32* %sortColTypes, i32* %sortAscendings, i32* undef, i32 %sortColCount, i32 %add, i32 %add23) #19
  %cmp.i305 = icmp slt i32 %call.i302, 0
  br i1 %cmp.i305, label %cond.true.i307, label %cond.false10.i312

cond.true.i307:                                   ; preds = %_Z7median3lPiS_S_S_iiii.exit
  %cmp3.i306 = icmp slt i32 %call2.i304, 0
  br i1 %cmp3.i306, label %_Z7median3lPiS_S_S_iiii.exit317, label %cond.false.i310

cond.false.i310:                                  ; preds = %cond.true.i307
  %cmp5.i308 = icmp slt i32 %call1.i303, 0
  %cond.i309 = select i1 %cmp5.i308, i32 %add23, i32 %sub22
  br label %_Z7median3lPiS_S_S_iiii.exit317

cond.false10.i312:                                ; preds = %_Z7median3lPiS_S_S_iiii.exit
  %cmp11.i311 = icmp sgt i32 %call2.i304, 0
  br i1 %cmp11.i311, label %_Z7median3lPiS_S_S_iiii.exit317, label %cond.false13.i315

cond.false13.i315:                                ; preds = %cond.false10.i312
  %cmp14.i313 = icmp sgt i32 %call1.i303, 0
  %cond18.i314 = select i1 %cmp14.i313, i32 %add23, i32 %sub22
  br label %_Z7median3lPiS_S_S_iiii.exit317

_Z7median3lPiS_S_S_iiii.exit317:                  ; preds = %cond.true.i307, %cond.false.i310, %cond.false10.i312, %cond.false13.i315
  %cond22.i316 = phi i32 [ %cond.i309, %cond.false.i310 ], [ %cond18.i314, %cond.false13.i315 ], [ %add, %cond.true.i307 ], [ %add, %cond.false10.i312 ]
  %sub26 = sub nsw i32 %sub15, %mul
  %sub27 = sub nsw i32 %sub15, %div18368
  %call.i318 = tail call i32 @_Z9compareTolPiS_S_S_iii(i64 %pagesIndexAddr, i32* %sortCols, i32* %sortColTypes, i32* %sortAscendings, i32* undef, i32 %sortColCount, i32 %sub26, i32 %sub27) #19
  %call1.i319 = tail call i32 @_Z9compareTolPiS_S_S_iii(i64 %pagesIndexAddr, i32* %sortCols, i32* %sortColTypes, i32* %sortAscendings, i32* undef, i32 %sortColCount, i32 %sub26, i32 %sub15) #19
  %call2.i320 = tail call i32 @_Z9compareTolPiS_S_S_iii(i64 %pagesIndexAddr, i32* %sortCols, i32* %sortColTypes, i32* %sortAscendings, i32* undef, i32 %sortColCount, i32 %sub27, i32 %sub15) #19
  %cmp.i321 = icmp slt i32 %call.i318, 0
  br i1 %cmp.i321, label %cond.true.i323, label %cond.false10.i328

cond.true.i323:                                   ; preds = %_Z7median3lPiS_S_S_iiii.exit317
  %cmp3.i322 = icmp slt i32 %call2.i320, 0
  br i1 %cmp3.i322, label %if.end29, label %cond.false.i326

cond.false.i326:                                  ; preds = %cond.true.i323
  %cmp5.i324 = icmp slt i32 %call1.i319, 0
  %cond.i325 = select i1 %cmp5.i324, i32 %sub15, i32 %sub26
  br label %if.end29

cond.false10.i328:                                ; preds = %_Z7median3lPiS_S_S_iiii.exit317
  %cmp11.i327 = icmp sgt i32 %call2.i320, 0
  br i1 %cmp11.i327, label %if.end29, label %cond.false13.i331

cond.false13.i331:                                ; preds = %cond.false10.i328
  %cmp14.i329 = icmp sgt i32 %call1.i319, 0
  %cond18.i330 = select i1 %cmp14.i329, i32 %sub15, i32 %sub26
  br label %if.end29

if.end29:                                         ; preds = %cond.false13.i331, %cond.false10.i328, %cond.false.i326, %cond.true.i323, %if.then14
  %m.0 = phi i32 [ %add, %if.then14 ], [ %cond22.i316, %cond.true.i323 ], [ %cond22.i316, %cond.false.i326 ], [ %cond22.i316, %cond.false10.i328 ], [ %cond22.i316, %cond.false13.i331 ]
  %l.0 = phi i32 [ %from.tr405, %if.then14 ], [ %cond22.i, %cond.true.i323 ], [ %cond22.i, %cond.false.i326 ], [ %cond22.i, %cond.false10.i328 ], [ %cond22.i, %cond.false13.i331 ]
  %n.0 = phi i32 [ %sub15, %if.then14 ], [ %sub27, %cond.true.i323 ], [ %cond.i325, %cond.false.i326 ], [ %sub27, %cond.false10.i328 ], [ %cond18.i330, %cond.false13.i331 ]
  %call.i334 = tail call i32 @_Z9compareTolPiS_S_S_iii(i64 %pagesIndexAddr, i32* %sortCols, i32* %sortColTypes, i32* %sortAscendings, i32* undef, i32 %sortColCount, i32 %l.0, i32 %m.0) #19
  %call1.i335 = tail call i32 @_Z9compareTolPiS_S_S_iii(i64 %pagesIndexAddr, i32* %sortCols, i32* %sortColTypes, i32* %sortAscendings, i32* undef, i32 %sortColCount, i32 %l.0, i32 %n.0) #19
  %call2.i336 = tail call i32 @_Z9compareTolPiS_S_S_iii(i64 %pagesIndexAddr, i32* %sortCols, i32* %sortColTypes, i32* %sortAscendings, i32* undef, i32 %sortColCount, i32 %m.0, i32 %n.0) #19
  %cmp.i337 = icmp slt i32 %call.i334, 0
  br i1 %cmp.i337, label %cond.true.i339, label %cond.false10.i344

cond.true.i339:                                   ; preds = %if.end29
  %cmp3.i338 = icmp slt i32 %call2.i336, 0
  br i1 %cmp3.i338, label %while.cond.preheader, label %cond.false.i342

cond.false.i342:                                  ; preds = %cond.true.i339
  %cmp5.i340 = icmp slt i32 %call1.i335, 0
  %cond.i341 = select i1 %cmp5.i340, i32 %n.0, i32 %l.0
  br label %while.cond.preheader

cond.false10.i344:                                ; preds = %if.end29
  %cmp11.i343 = icmp sgt i32 %call2.i336, 0
  br i1 %cmp11.i343, label %while.cond.preheader, label %cond.false13.i347

cond.false13.i347:                                ; preds = %cond.false10.i344
  %cmp14.i345 = icmp sgt i32 %call1.i335, 0
  %cond18.i346 = select i1 %cmp14.i345, i32 %n.0, i32 %l.0
  br label %while.cond.preheader

while.cond.preheader:                             ; preds = %cond.false13.i347, %cond.false10.i344, %cond.false.i342, %cond.true.i339, %if.end
  %m.2.ph = phi i32 [ %m.0, %cond.false10.i344 ], [ %m.0, %cond.true.i339 ], [ %cond18.i346, %cond.false13.i347 ], [ %cond.i341, %cond.false.i342 ], [ %add, %if.end ]
  br label %while.cond

while.cond:                                       ; preds = %while.cond.preheader, %if.end73
  %m.2 = phi i32 [ %m.9, %if.end73 ], [ %m.2.ph, %while.cond.preheader ]
  %a.0 = phi i32 [ %a.1.lcssa, %if.end73 ], [ %from.tr405, %while.cond.preheader ]
  %b.0 = phi i32 [ %inc81, %if.end73 ], [ %from.tr405, %while.cond.preheader ]
  %c.0 = phi i32 [ %dec82, %if.end73 ], [ %sub15, %while.cond.preheader ]
  %d.0 = phi i32 [ %d.1394, %if.end73 ], [ %sub15, %while.cond.preheader ]
  %cmp34.not380 = icmp sgt i32 %b.0, %c.0
  br i1 %cmp34.not380, label %while.end, label %land.rhs35.preheader

land.rhs35.preheader:                             ; preds = %while.cond
  %10 = sext i32 %b.0 to i64
  %11 = sext i32 %c.0 to i64
  %12 = icmp sgt i64 %10, %11
  %smax = select i1 %12, i64 %10, i64 %11
  %13 = trunc i64 %smax to i32
  %14 = add i32 %13, 1
  br label %land.rhs35

land.rhs35:                                       ; preds = %land.rhs35.preheader, %if.end49
  %indvars.iv424 = phi i64 [ %10, %land.rhs35.preheader ], [ %indvars.iv.next425, %if.end49 ]
  %a.1382 = phi i32 [ %a.0, %land.rhs35.preheader ], [ %a.2, %if.end49 ]
  %m.3381 = phi i32 [ %m.2, %land.rhs35.preheader ], [ %m.5, %if.end49 ]
  %15 = trunc i64 %indvars.iv424 to i32
  %call36 = tail call i32 @_Z9compareTolPiS_S_S_iii(i64 %pagesIndexAddr, i32* %sortCols, i32* %sortColTypes, i32* %sortAscendings, i32* undef, i32 %sortColCount, i32 %15, i32 %m.3381)
  %cmp37 = icmp slt i32 %call36, 1
  br i1 %cmp37, label %while.body39, label %while.end.loopexit.split.loop.exit448

while.body39:                                     ; preds = %land.rhs35
  %cmp40 = icmp eq i32 %call36, 0
  br i1 %cmp40, label %if.then41, label %if.end49

if.then41:                                        ; preds = %while.body39
  %cmp42 = icmp eq i32 %a.1382, %m.3381
  %cmp44 = icmp eq i32 %m.3381, %15
  %spec.select = select i1 %cmp44, i32 %a.1382, i32 %m.3381
  %m.4 = select i1 %cmp42, i32 %15, i32 %spec.select
  %inc48 = add nsw i32 %a.1382, 1
  %idxprom.i350 = sext i32 %a.1382 to i64
  %arrayidx.i351 = getelementptr inbounds i64, i64* %9, i64 %idxprom.i350
  %16 = load i64, i64* %arrayidx.i351, align 8, !tbaa !47
  %arrayidx2.i353 = getelementptr inbounds i64, i64* %9, i64 %indvars.iv424
  %17 = load i64, i64* %arrayidx2.i353, align 8, !tbaa !47
  store i64 %17, i64* %arrayidx.i351, align 8, !tbaa !47
  store i64 %16, i64* %arrayidx2.i353, align 8, !tbaa !47
  br label %if.end49

if.end49:                                         ; preds = %if.then41, %while.body39
  %m.5 = phi i32 [ %m.4, %if.then41 ], [ %m.3381, %while.body39 ]
  %a.2 = phi i32 [ %inc48, %if.then41 ], [ %a.1382, %while.body39 ]
  %indvars.iv.next425 = add nsw i64 %indvars.iv424, 1
  %exitcond426.not = icmp eq i64 %indvars.iv424, %smax
  br i1 %exitcond426.not, label %while.end, label %land.rhs35, !llvm.loop !71

while.end.loopexit.split.loop.exit448:            ; preds = %land.rhs35
  %18 = trunc i64 %indvars.iv424 to i32
  br label %while.end

while.end:                                        ; preds = %if.end49, %while.end.loopexit.split.loop.exit448, %while.cond
  %m.3.lcssa = phi i32 [ %m.2, %while.cond ], [ %m.3381, %while.end.loopexit.split.loop.exit448 ], [ %m.5, %if.end49 ]
  %a.1.lcssa = phi i32 [ %a.0, %while.cond ], [ %a.1382, %while.end.loopexit.split.loop.exit448 ], [ %a.2, %if.end49 ]
  %b.1.lcssa = phi i32 [ %b.0, %while.cond ], [ %18, %while.end.loopexit.split.loop.exit448 ], [ %14, %if.end49 ]
  %cmp52.not390 = icmp slt i32 %c.0, %b.1.lcssa
  br i1 %cmp52.not390, label %while.end83, label %land.rhs53.preheader

land.rhs53.preheader:                             ; preds = %while.end
  %19 = sext i32 %c.0 to i64
  %20 = sext i32 %b.1.lcssa to i64
  br label %land.rhs53

land.rhs53:                                       ; preds = %land.rhs53.preheader, %if.end68
  %indvars.iv427 = phi i64 [ %19, %land.rhs53.preheader ], [ %indvars.iv.next428, %if.end68 ]
  %d.1394 = phi i32 [ %d.0, %land.rhs53.preheader ], [ %d.2, %if.end68 ]
  %m.6391 = phi i32 [ %m.3.lcssa, %land.rhs53.preheader ], [ %m.8, %if.end68 ]
  %21 = trunc i64 %indvars.iv427 to i32
  %call54 = tail call i32 @_Z9compareTolPiS_S_S_iii(i64 %pagesIndexAddr, i32* %sortCols, i32* %sortColTypes, i32* %sortAscendings, i32* undef, i32 %sortColCount, i32 %21, i32 %m.6391)
  %cmp55 = icmp sgt i32 %call54, -1
  br i1 %cmp55, label %while.body57, label %if.end73

while.body57:                                     ; preds = %land.rhs53
  %cmp58 = icmp eq i32 %call54, 0
  br i1 %cmp58, label %if.then59, label %if.end68

if.then59:                                        ; preds = %while.body57
  %cmp60 = icmp eq i32 %m.6391, %21
  %cmp63 = icmp eq i32 %d.1394, %m.6391
  %spec.select276 = select i1 %cmp63, i32 %21, i32 %m.6391
  %m.7 = select i1 %cmp60, i32 %d.1394, i32 %spec.select276
  %dec67 = add nsw i32 %d.1394, -1
  %arrayidx.i298 = getelementptr inbounds i64, i64* %9, i64 %indvars.iv427
  %22 = load i64, i64* %arrayidx.i298, align 8, !tbaa !47
  %idxprom1.i299 = sext i32 %d.1394 to i64
  %arrayidx2.i300 = getelementptr inbounds i64, i64* %9, i64 %idxprom1.i299
  %23 = load i64, i64* %arrayidx2.i300, align 8, !tbaa !47
  store i64 %23, i64* %arrayidx.i298, align 8, !tbaa !47
  store i64 %22, i64* %arrayidx2.i300, align 8, !tbaa !47
  br label %if.end68

if.end68:                                         ; preds = %if.then59, %while.body57
  %m.8 = phi i32 [ %m.7, %if.then59 ], [ %m.6391, %while.body57 ]
  %d.2 = phi i32 [ %dec67, %if.then59 ], [ %d.1394, %while.body57 ]
  %indvars.iv.next428 = add nsw i64 %indvars.iv427, -1
  %cmp52.not.not = icmp sgt i64 %indvars.iv427, %20
  br i1 %cmp52.not.not, label %land.rhs53, label %while.end83.loopexit, !llvm.loop !72

if.end73:                                         ; preds = %land.rhs53
  %24 = trunc i64 %indvars.iv427 to i32
  %cmp74 = icmp eq i32 %b.1.lcssa, %m.6391
  %m.9 = select i1 %cmp74, i32 %d.1394, i32 %m.6391
  %inc81 = add nsw i32 %b.1.lcssa, 1
  %dec82 = add nsw i32 %24, -1
  %arrayidx.i294 = getelementptr inbounds i64, i64* %9, i64 %20
  %25 = load i64, i64* %arrayidx.i294, align 8, !tbaa !47
  %sext = shl i64 %indvars.iv427, 32
  %idxprom1.i295 = ashr exact i64 %sext, 32
  %arrayidx2.i296 = getelementptr inbounds i64, i64* %9, i64 %idxprom1.i295
  %26 = load i64, i64* %arrayidx2.i296, align 8, !tbaa !47
  store i64 %26, i64* %arrayidx.i294, align 8, !tbaa !47
  store i64 %25, i64* %arrayidx2.i296, align 8, !tbaa !47
  br label %while.cond

while.end83.loopexit:                             ; preds = %if.end68
  %27 = trunc i64 %indvars.iv.next428 to i32
  br label %while.end83

while.end83:                                      ; preds = %while.end, %while.end83.loopexit
  %c.1.lcssa = phi i32 [ %27, %while.end83.loopexit ], [ %c.0, %while.end ]
  %d.1.lcssa = phi i32 [ %d.2, %while.end83.loopexit ], [ %d.0, %while.end ]
  %sub86 = sub nsw i32 %a.1.lcssa, %from.tr405
  %sub88 = sub nsw i32 %b.1.lcssa, %a.1.lcssa
  %cmp.i291 = icmp slt i32 %sub88, %sub86
  %.sroa.speculated361 = select i1 %cmp.i291, i32 %sub88, i32 %sub86
  %cmp7.i278 = icmp sgt i32 %.sroa.speculated361, 0
  br i1 %cmp7.i278, label %for.body.preheader.i279, label %_Z10vectorSwapPliii.exit290

for.body.preheader.i279:                          ; preds = %while.end83
  %sub90 = sub nsw i32 %b.1.lcssa, %.sroa.speculated361
  %28 = sext i32 %from.tr405 to i64
  %29 = sext i32 %sub90 to i64
  %30 = add i32 %.sroa.speculated361, -1
  %31 = zext i32 %30 to i64
  %32 = add nuw nsw i64 %31, 1
  %min.iters.check490 = icmp ult i32 %30, 3
  br i1 %min.iters.check490, label %for.body.i289.preheader, label %vector.memcheck492

vector.memcheck492:                               ; preds = %for.body.preheader.i279
  %scevgep494 = getelementptr i64, i64* %9, i64 %28
  %scevgep496 = getelementptr i64, i64* %9, i64 1
  %33 = add i32 %.sroa.speculated361, -1
  %34 = zext i32 %33 to i64
  %35 = add nsw i64 %28, %34
  %scevgep497 = getelementptr i64, i64* %scevgep496, i64 %35
  %scevgep499 = getelementptr i64, i64* %9, i64 %29
  %scevgep501 = getelementptr i64, i64* %9, i64 1
  %36 = sext i32 %b.1.lcssa to i64
  %37 = add nsw i64 %36, %34
  %38 = sext i32 %.sroa.speculated361 to i64
  %39 = sub nsw i64 %37, %38
  %scevgep502 = getelementptr i64, i64* %scevgep501, i64 %39
  %bound0504 = icmp ult i64* %scevgep494, %scevgep502
  %bound1505 = icmp ult i64* %scevgep499, %scevgep497
  %found.conflict506 = and i1 %bound0504, %bound1505
  br i1 %found.conflict506, label %for.body.i289.preheader, label %vector.ph493

vector.ph493:                                     ; preds = %vector.memcheck492
  %n.vec509 = and i64 %32, 8589934588
  %ind.end513 = add nsw i64 %n.vec509, %29
  %ind.end515 = add nsw i64 %n.vec509, %28
  %ind.end517 = trunc i64 %n.vec509 to i32
  %40 = add nsw i64 %n.vec509, -4
  %41 = lshr exact i64 %40, 2
  %42 = add nuw nsw i64 %41, 1
  %xtraiter = and i64 %42, 1
  %43 = icmp eq i64 %40, 0
  br i1 %43, label %middle.block487.unr-lcssa, label %vector.ph493.new

vector.ph493.new:                                 ; preds = %vector.ph493
  %unroll_iter = and i64 %42, 9223372036854775806
  br label %vector.body489

vector.body489:                                   ; preds = %vector.body489, %vector.ph493.new
  %index510 = phi i64 [ 0, %vector.ph493.new ], [ %index.next511.1, %vector.body489 ]
  %niter = phi i64 [ %unroll_iter, %vector.ph493.new ], [ %niter.nsub.1, %vector.body489 ]
  %offset.idx519 = add i64 %index510, %29
  %offset.idx520 = add i64 %index510, %28
  %44 = getelementptr inbounds i64, i64* %9, i64 %offset.idx520
  %45 = bitcast i64* %44 to <2 x i64>*
  %wide.load522 = load <2 x i64>, <2 x i64>* %45, align 8, !tbaa !47, !alias.scope !73, !noalias !76
  %46 = getelementptr inbounds i64, i64* %44, i64 2
  %47 = bitcast i64* %46 to <2 x i64>*
  %wide.load523 = load <2 x i64>, <2 x i64>* %47, align 8, !tbaa !47, !alias.scope !73, !noalias !76
  %48 = getelementptr inbounds i64, i64* %9, i64 %offset.idx519
  %49 = bitcast i64* %48 to <2 x i64>*
  %wide.load524 = load <2 x i64>, <2 x i64>* %49, align 8, !tbaa !47, !alias.scope !76
  %50 = getelementptr inbounds i64, i64* %48, i64 2
  %51 = bitcast i64* %50 to <2 x i64>*
  %wide.load525 = load <2 x i64>, <2 x i64>* %51, align 8, !tbaa !47, !alias.scope !76
  %52 = bitcast i64* %44 to <2 x i64>*
  store <2 x i64> %wide.load524, <2 x i64>* %52, align 8, !tbaa !47, !alias.scope !73, !noalias !76
  %53 = bitcast i64* %46 to <2 x i64>*
  store <2 x i64> %wide.load525, <2 x i64>* %53, align 8, !tbaa !47, !alias.scope !73, !noalias !76
  %54 = bitcast i64* %48 to <2 x i64>*
  store <2 x i64> %wide.load522, <2 x i64>* %54, align 8, !tbaa !47, !alias.scope !76
  %55 = bitcast i64* %50 to <2 x i64>*
  store <2 x i64> %wide.load523, <2 x i64>* %55, align 8, !tbaa !47, !alias.scope !76
  %index.next511 = or i64 %index510, 4
  %offset.idx519.1 = add i64 %index.next511, %29
  %offset.idx520.1 = add i64 %index.next511, %28
  %56 = getelementptr inbounds i64, i64* %9, i64 %offset.idx520.1
  %57 = bitcast i64* %56 to <2 x i64>*
  %wide.load522.1 = load <2 x i64>, <2 x i64>* %57, align 8, !tbaa !47, !alias.scope !73, !noalias !76
  %58 = getelementptr inbounds i64, i64* %56, i64 2
  %59 = bitcast i64* %58 to <2 x i64>*
  %wide.load523.1 = load <2 x i64>, <2 x i64>* %59, align 8, !tbaa !47, !alias.scope !73, !noalias !76
  %60 = getelementptr inbounds i64, i64* %9, i64 %offset.idx519.1
  %61 = bitcast i64* %60 to <2 x i64>*
  %wide.load524.1 = load <2 x i64>, <2 x i64>* %61, align 8, !tbaa !47, !alias.scope !76
  %62 = getelementptr inbounds i64, i64* %60, i64 2
  %63 = bitcast i64* %62 to <2 x i64>*
  %wide.load525.1 = load <2 x i64>, <2 x i64>* %63, align 8, !tbaa !47, !alias.scope !76
  %64 = bitcast i64* %56 to <2 x i64>*
  store <2 x i64> %wide.load524.1, <2 x i64>* %64, align 8, !tbaa !47, !alias.scope !73, !noalias !76
  %65 = bitcast i64* %58 to <2 x i64>*
  store <2 x i64> %wide.load525.1, <2 x i64>* %65, align 8, !tbaa !47, !alias.scope !73, !noalias !76
  %66 = bitcast i64* %60 to <2 x i64>*
  store <2 x i64> %wide.load522.1, <2 x i64>* %66, align 8, !tbaa !47, !alias.scope !76
  %67 = bitcast i64* %62 to <2 x i64>*
  store <2 x i64> %wide.load523.1, <2 x i64>* %67, align 8, !tbaa !47, !alias.scope !76
  %index.next511.1 = add i64 %index510, 8
  %niter.nsub.1 = add i64 %niter, -2
  %niter.ncmp.1 = icmp eq i64 %niter.nsub.1, 0
  br i1 %niter.ncmp.1, label %middle.block487.unr-lcssa, label %vector.body489, !llvm.loop !78

middle.block487.unr-lcssa:                        ; preds = %vector.body489, %vector.ph493
  %index510.unr = phi i64 [ 0, %vector.ph493 ], [ %index.next511.1, %vector.body489 ]
  %lcmp.mod.not = icmp eq i64 %xtraiter, 0
  br i1 %lcmp.mod.not, label %middle.block487, label %vector.body489.epil

vector.body489.epil:                              ; preds = %middle.block487.unr-lcssa
  %offset.idx519.epil = add i64 %index510.unr, %29
  %offset.idx520.epil = add i64 %index510.unr, %28
  %68 = getelementptr inbounds i64, i64* %9, i64 %offset.idx520.epil
  %69 = bitcast i64* %68 to <2 x i64>*
  %wide.load522.epil = load <2 x i64>, <2 x i64>* %69, align 8, !tbaa !47, !alias.scope !73, !noalias !76
  %70 = getelementptr inbounds i64, i64* %68, i64 2
  %71 = bitcast i64* %70 to <2 x i64>*
  %wide.load523.epil = load <2 x i64>, <2 x i64>* %71, align 8, !tbaa !47, !alias.scope !73, !noalias !76
  %72 = getelementptr inbounds i64, i64* %9, i64 %offset.idx519.epil
  %73 = bitcast i64* %72 to <2 x i64>*
  %wide.load524.epil = load <2 x i64>, <2 x i64>* %73, align 8, !tbaa !47, !alias.scope !76
  %74 = getelementptr inbounds i64, i64* %72, i64 2
  %75 = bitcast i64* %74 to <2 x i64>*
  %wide.load525.epil = load <2 x i64>, <2 x i64>* %75, align 8, !tbaa !47, !alias.scope !76
  %76 = bitcast i64* %68 to <2 x i64>*
  store <2 x i64> %wide.load524.epil, <2 x i64>* %76, align 8, !tbaa !47, !alias.scope !73, !noalias !76
  %77 = bitcast i64* %70 to <2 x i64>*
  store <2 x i64> %wide.load525.epil, <2 x i64>* %77, align 8, !tbaa !47, !alias.scope !73, !noalias !76
  %78 = bitcast i64* %72 to <2 x i64>*
  store <2 x i64> %wide.load522.epil, <2 x i64>* %78, align 8, !tbaa !47, !alias.scope !76
  %79 = bitcast i64* %74 to <2 x i64>*
  store <2 x i64> %wide.load523.epil, <2 x i64>* %79, align 8, !tbaa !47, !alias.scope !76
  br label %middle.block487

middle.block487:                                  ; preds = %middle.block487.unr-lcssa, %vector.body489.epil
  %cmp.n518 = icmp eq i64 %32, %n.vec509
  br i1 %cmp.n518, label %_Z10vectorSwapPliii.exit290, label %for.body.i289.preheader

for.body.i289.preheader:                          ; preds = %vector.memcheck492, %for.body.preheader.i279, %middle.block487
  %indvars.iv13.i280.ph = phi i64 [ %29, %vector.memcheck492 ], [ %29, %for.body.preheader.i279 ], [ %ind.end513, %middle.block487 ]
  %indvars.iv.i281.ph = phi i64 [ %28, %vector.memcheck492 ], [ %28, %for.body.preheader.i279 ], [ %ind.end515, %middle.block487 ]
  %i.010.i282.ph = phi i32 [ 0, %vector.memcheck492 ], [ 0, %for.body.preheader.i279 ], [ %ind.end517, %middle.block487 ]
  %80 = sub i32 %.sroa.speculated361, %i.010.i282.ph
  %.neg549 = add i32 %i.010.i282.ph, 1
  %xtraiter541 = and i32 %80, 1
  %lcmp.mod542.not = icmp eq i32 %xtraiter541, 0
  br i1 %lcmp.mod542.not, label %for.body.i289.prol.loopexit, label %for.body.i289.prol

for.body.i289.prol:                               ; preds = %for.body.i289.preheader
  %arrayidx.i.i283.prol = getelementptr inbounds i64, i64* %9, i64 %indvars.iv.i281.ph
  %81 = load i64, i64* %arrayidx.i.i283.prol, align 8, !tbaa !47
  %arrayidx2.i.i284.prol = getelementptr inbounds i64, i64* %9, i64 %indvars.iv13.i280.ph
  %82 = load i64, i64* %arrayidx2.i.i284.prol, align 8, !tbaa !47
  store i64 %82, i64* %arrayidx.i.i283.prol, align 8, !tbaa !47
  store i64 %81, i64* %arrayidx2.i.i284.prol, align 8, !tbaa !47
  %inc.i285.prol = add nuw nsw i32 %i.010.i282.ph, 1
  %indvars.iv.next.i286.prol = add nsw i64 %indvars.iv.i281.ph, 1
  %indvars.iv.next14.i287.prol = add nsw i64 %indvars.iv13.i280.ph, 1
  br label %for.body.i289.prol.loopexit

for.body.i289.prol.loopexit:                      ; preds = %for.body.i289.prol, %for.body.i289.preheader
  %indvars.iv13.i280.unr.ph = phi i64 [ %indvars.iv.next14.i287.prol, %for.body.i289.prol ], [ %indvars.iv13.i280.ph, %for.body.i289.preheader ]
  %indvars.iv.i281.unr.ph = phi i64 [ %indvars.iv.next.i286.prol, %for.body.i289.prol ], [ %indvars.iv.i281.ph, %for.body.i289.preheader ]
  %i.010.i282.unr.ph = phi i32 [ %inc.i285.prol, %for.body.i289.prol ], [ %i.010.i282.ph, %for.body.i289.preheader ]
  %83 = icmp eq i32 %.sroa.speculated361, %.neg549
  br i1 %83, label %_Z10vectorSwapPliii.exit290, label %for.body.i289

for.body.i289:                                    ; preds = %for.body.i289.prol.loopexit, %for.body.i289
  %indvars.iv13.i280 = phi i64 [ %indvars.iv.next14.i287.1, %for.body.i289 ], [ %indvars.iv13.i280.unr.ph, %for.body.i289.prol.loopexit ]
  %indvars.iv.i281 = phi i64 [ %indvars.iv.next.i286.1, %for.body.i289 ], [ %indvars.iv.i281.unr.ph, %for.body.i289.prol.loopexit ]
  %i.010.i282 = phi i32 [ %inc.i285.1, %for.body.i289 ], [ %i.010.i282.unr.ph, %for.body.i289.prol.loopexit ]
  %arrayidx.i.i283 = getelementptr inbounds i64, i64* %9, i64 %indvars.iv.i281
  %84 = load i64, i64* %arrayidx.i.i283, align 8, !tbaa !47
  %arrayidx2.i.i284 = getelementptr inbounds i64, i64* %9, i64 %indvars.iv13.i280
  %85 = load i64, i64* %arrayidx2.i.i284, align 8, !tbaa !47
  store i64 %85, i64* %arrayidx.i.i283, align 8, !tbaa !47
  store i64 %84, i64* %arrayidx2.i.i284, align 8, !tbaa !47
  %indvars.iv.next.i286 = add nsw i64 %indvars.iv.i281, 1
  %indvars.iv.next14.i287 = add nsw i64 %indvars.iv13.i280, 1
  %arrayidx.i.i283.1 = getelementptr inbounds i64, i64* %9, i64 %indvars.iv.next.i286
  %86 = load i64, i64* %arrayidx.i.i283.1, align 8, !tbaa !47
  %arrayidx2.i.i284.1 = getelementptr inbounds i64, i64* %9, i64 %indvars.iv.next14.i287
  %87 = load i64, i64* %arrayidx2.i.i284.1, align 8, !tbaa !47
  store i64 %87, i64* %arrayidx.i.i283.1, align 8, !tbaa !47
  store i64 %86, i64* %arrayidx2.i.i284.1, align 8, !tbaa !47
  %inc.i285.1 = add nuw nsw i32 %i.010.i282, 2
  %indvars.iv.next.i286.1 = add nsw i64 %indvars.iv.i281, 2
  %indvars.iv.next14.i287.1 = add nsw i64 %indvars.iv13.i280, 2
  %exitcond.not.i288.1 = icmp eq i32 %inc.i285.1, %.sroa.speculated361
  br i1 %exitcond.not.i288.1, label %_Z10vectorSwapPliii.exit290, label %for.body.i289, !llvm.loop !79

_Z10vectorSwapPliii.exit290:                      ; preds = %for.body.i289.prol.loopexit, %for.body.i289, %middle.block487, %while.end83
  %sub92 = sub nsw i32 %d.1.lcssa, %c.1.lcssa
  %88 = xor i32 %d.1.lcssa, -1
  %sub95 = add i32 %88, %to
  %cmp.i = icmp slt i32 %sub95, %sub92
  %.sroa.speculated = select i1 %cmp.i, i32 %sub95, i32 %sub92
  %cmp7.i = icmp sgt i32 %.sroa.speculated, 0
  br i1 %cmp7.i, label %for.body.preheader.i, label %_Z10vectorSwapPliii.exit

for.body.preheader.i:                             ; preds = %_Z10vectorSwapPliii.exit290
  %sub97 = sub nsw i32 %to, %.sroa.speculated
  %89 = sext i32 %b.1.lcssa to i64
  %90 = sext i32 %sub97 to i64
  %91 = add i32 %.sroa.speculated, -1
  %92 = zext i32 %91 to i64
  %93 = add nuw nsw i64 %92, 1
  %min.iters.check = icmp ult i32 %91, 3
  br i1 %min.iters.check, label %for.body.i.preheader, label %vector.memcheck

vector.memcheck:                                  ; preds = %for.body.preheader.i
  %scevgep = getelementptr i64, i64* %9, i64 %89
  %scevgep469 = getelementptr i64, i64* %9, i64 1
  %94 = add i32 %.sroa.speculated, -1
  %95 = zext i32 %94 to i64
  %96 = add nsw i64 %89, %95
  %scevgep470 = getelementptr i64, i64* %scevgep469, i64 %96
  %scevgep472 = getelementptr i64, i64* %9, i64 %2
  %97 = sext i32 %.sroa.speculated to i64
  %98 = sub nsw i64 0, %97
  %scevgep473 = getelementptr i64, i64* %scevgep472, i64 %98
  %scevgep475 = getelementptr i64, i64* %9, i64 %3
  %99 = sub nsw i64 %95, %97
  %scevgep476 = getelementptr i64, i64* %scevgep475, i64 %99
  %bound0 = icmp ult i64* %scevgep, %scevgep476
  %bound1 = icmp ult i64* %scevgep473, %scevgep470
  %found.conflict = and i1 %bound0, %bound1
  br i1 %found.conflict, label %for.body.i.preheader, label %vector.ph

vector.ph:                                        ; preds = %vector.memcheck
  %n.vec = and i64 %93, 8589934588
  %ind.end = add nsw i64 %n.vec, %90
  %ind.end479 = add nsw i64 %n.vec, %89
  %ind.end481 = trunc i64 %n.vec to i32
  %100 = add nsw i64 %n.vec, -4
  %101 = lshr exact i64 %100, 2
  %102 = add nuw nsw i64 %101, 1
  %xtraiter543 = and i64 %102, 1
  %103 = icmp eq i64 %100, 0
  br i1 %103, label %middle.block.unr-lcssa, label %vector.ph.new

vector.ph.new:                                    ; preds = %vector.ph
  %unroll_iter545 = and i64 %102, 9223372036854775806
  br label %vector.body

vector.body:                                      ; preds = %vector.body, %vector.ph.new
  %index = phi i64 [ 0, %vector.ph.new ], [ %index.next.1, %vector.body ]
  %niter546 = phi i64 [ %unroll_iter545, %vector.ph.new ], [ %niter546.nsub.1, %vector.body ]
  %offset.idx = add i64 %index, %90
  %offset.idx482 = add i64 %index, %89
  %104 = getelementptr inbounds i64, i64* %9, i64 %offset.idx482
  %105 = bitcast i64* %104 to <2 x i64>*
  %wide.load = load <2 x i64>, <2 x i64>* %105, align 8, !tbaa !47, !alias.scope !80, !noalias !83
  %106 = getelementptr inbounds i64, i64* %104, i64 2
  %107 = bitcast i64* %106 to <2 x i64>*
  %wide.load484 = load <2 x i64>, <2 x i64>* %107, align 8, !tbaa !47, !alias.scope !80, !noalias !83
  %108 = getelementptr inbounds i64, i64* %9, i64 %offset.idx
  %109 = bitcast i64* %108 to <2 x i64>*
  %wide.load485 = load <2 x i64>, <2 x i64>* %109, align 8, !tbaa !47, !alias.scope !83
  %110 = getelementptr inbounds i64, i64* %108, i64 2
  %111 = bitcast i64* %110 to <2 x i64>*
  %wide.load486 = load <2 x i64>, <2 x i64>* %111, align 8, !tbaa !47, !alias.scope !83
  %112 = bitcast i64* %104 to <2 x i64>*
  store <2 x i64> %wide.load485, <2 x i64>* %112, align 8, !tbaa !47, !alias.scope !80, !noalias !83
  %113 = bitcast i64* %106 to <2 x i64>*
  store <2 x i64> %wide.load486, <2 x i64>* %113, align 8, !tbaa !47, !alias.scope !80, !noalias !83
  %114 = bitcast i64* %108 to <2 x i64>*
  store <2 x i64> %wide.load, <2 x i64>* %114, align 8, !tbaa !47, !alias.scope !83
  %115 = bitcast i64* %110 to <2 x i64>*
  store <2 x i64> %wide.load484, <2 x i64>* %115, align 8, !tbaa !47, !alias.scope !83
  %index.next = or i64 %index, 4
  %offset.idx.1 = add i64 %index.next, %90
  %offset.idx482.1 = add i64 %index.next, %89
  %116 = getelementptr inbounds i64, i64* %9, i64 %offset.idx482.1
  %117 = bitcast i64* %116 to <2 x i64>*
  %wide.load.1 = load <2 x i64>, <2 x i64>* %117, align 8, !tbaa !47, !alias.scope !80, !noalias !83
  %118 = getelementptr inbounds i64, i64* %116, i64 2
  %119 = bitcast i64* %118 to <2 x i64>*
  %wide.load484.1 = load <2 x i64>, <2 x i64>* %119, align 8, !tbaa !47, !alias.scope !80, !noalias !83
  %120 = getelementptr inbounds i64, i64* %9, i64 %offset.idx.1
  %121 = bitcast i64* %120 to <2 x i64>*
  %wide.load485.1 = load <2 x i64>, <2 x i64>* %121, align 8, !tbaa !47, !alias.scope !83
  %122 = getelementptr inbounds i64, i64* %120, i64 2
  %123 = bitcast i64* %122 to <2 x i64>*
  %wide.load486.1 = load <2 x i64>, <2 x i64>* %123, align 8, !tbaa !47, !alias.scope !83
  %124 = bitcast i64* %116 to <2 x i64>*
  store <2 x i64> %wide.load485.1, <2 x i64>* %124, align 8, !tbaa !47, !alias.scope !80, !noalias !83
  %125 = bitcast i64* %118 to <2 x i64>*
  store <2 x i64> %wide.load486.1, <2 x i64>* %125, align 8, !tbaa !47, !alias.scope !80, !noalias !83
  %126 = bitcast i64* %120 to <2 x i64>*
  store <2 x i64> %wide.load.1, <2 x i64>* %126, align 8, !tbaa !47, !alias.scope !83
  %127 = bitcast i64* %122 to <2 x i64>*
  store <2 x i64> %wide.load484.1, <2 x i64>* %127, align 8, !tbaa !47, !alias.scope !83
  %index.next.1 = add i64 %index, 8
  %niter546.nsub.1 = add i64 %niter546, -2
  %niter546.ncmp.1 = icmp eq i64 %niter546.nsub.1, 0
  br i1 %niter546.ncmp.1, label %middle.block.unr-lcssa, label %vector.body, !llvm.loop !85

middle.block.unr-lcssa:                           ; preds = %vector.body, %vector.ph
  %index.unr = phi i64 [ 0, %vector.ph ], [ %index.next.1, %vector.body ]
  %lcmp.mod544.not = icmp eq i64 %xtraiter543, 0
  br i1 %lcmp.mod544.not, label %middle.block, label %vector.body.epil

vector.body.epil:                                 ; preds = %middle.block.unr-lcssa
  %offset.idx.epil = add i64 %index.unr, %90
  %offset.idx482.epil = add i64 %index.unr, %89
  %128 = getelementptr inbounds i64, i64* %9, i64 %offset.idx482.epil
  %129 = bitcast i64* %128 to <2 x i64>*
  %wide.load.epil = load <2 x i64>, <2 x i64>* %129, align 8, !tbaa !47, !alias.scope !80, !noalias !83
  %130 = getelementptr inbounds i64, i64* %128, i64 2
  %131 = bitcast i64* %130 to <2 x i64>*
  %wide.load484.epil = load <2 x i64>, <2 x i64>* %131, align 8, !tbaa !47, !alias.scope !80, !noalias !83
  %132 = getelementptr inbounds i64, i64* %9, i64 %offset.idx.epil
  %133 = bitcast i64* %132 to <2 x i64>*
  %wide.load485.epil = load <2 x i64>, <2 x i64>* %133, align 8, !tbaa !47, !alias.scope !83
  %134 = getelementptr inbounds i64, i64* %132, i64 2
  %135 = bitcast i64* %134 to <2 x i64>*
  %wide.load486.epil = load <2 x i64>, <2 x i64>* %135, align 8, !tbaa !47, !alias.scope !83
  %136 = bitcast i64* %128 to <2 x i64>*
  store <2 x i64> %wide.load485.epil, <2 x i64>* %136, align 8, !tbaa !47, !alias.scope !80, !noalias !83
  %137 = bitcast i64* %130 to <2 x i64>*
  store <2 x i64> %wide.load486.epil, <2 x i64>* %137, align 8, !tbaa !47, !alias.scope !80, !noalias !83
  %138 = bitcast i64* %132 to <2 x i64>*
  store <2 x i64> %wide.load.epil, <2 x i64>* %138, align 8, !tbaa !47, !alias.scope !83
  %139 = bitcast i64* %134 to <2 x i64>*
  store <2 x i64> %wide.load484.epil, <2 x i64>* %139, align 8, !tbaa !47, !alias.scope !83
  br label %middle.block

middle.block:                                     ; preds = %middle.block.unr-lcssa, %vector.body.epil
  %cmp.n = icmp eq i64 %93, %n.vec
  br i1 %cmp.n, label %_Z10vectorSwapPliii.exit, label %for.body.i.preheader

for.body.i.preheader:                             ; preds = %vector.memcheck, %for.body.preheader.i, %middle.block
  %indvars.iv13.i.ph = phi i64 [ %90, %vector.memcheck ], [ %90, %for.body.preheader.i ], [ %ind.end, %middle.block ]
  %indvars.iv.i.ph = phi i64 [ %89, %vector.memcheck ], [ %89, %for.body.preheader.i ], [ %ind.end479, %middle.block ]
  %i.010.i.ph = phi i32 [ 0, %vector.memcheck ], [ 0, %for.body.preheader.i ], [ %ind.end481, %middle.block ]
  %140 = sub i32 %.sroa.speculated, %i.010.i.ph
  %.neg = add i32 %i.010.i.ph, 1
  %xtraiter547 = and i32 %140, 1
  %lcmp.mod548.not = icmp eq i32 %xtraiter547, 0
  br i1 %lcmp.mod548.not, label %for.body.i.prol.loopexit, label %for.body.i.prol

for.body.i.prol:                                  ; preds = %for.body.i.preheader
  %arrayidx.i.i.prol = getelementptr inbounds i64, i64* %9, i64 %indvars.iv.i.ph
  %141 = load i64, i64* %arrayidx.i.i.prol, align 8, !tbaa !47
  %arrayidx2.i.i.prol = getelementptr inbounds i64, i64* %9, i64 %indvars.iv13.i.ph
  %142 = load i64, i64* %arrayidx2.i.i.prol, align 8, !tbaa !47
  store i64 %142, i64* %arrayidx.i.i.prol, align 8, !tbaa !47
  store i64 %141, i64* %arrayidx2.i.i.prol, align 8, !tbaa !47
  %inc.i.prol = add nuw nsw i32 %i.010.i.ph, 1
  %indvars.iv.next.i.prol = add nsw i64 %indvars.iv.i.ph, 1
  %indvars.iv.next14.i.prol = add nsw i64 %indvars.iv13.i.ph, 1
  br label %for.body.i.prol.loopexit

for.body.i.prol.loopexit:                         ; preds = %for.body.i.prol, %for.body.i.preheader
  %indvars.iv13.i.unr.ph = phi i64 [ %indvars.iv.next14.i.prol, %for.body.i.prol ], [ %indvars.iv13.i.ph, %for.body.i.preheader ]
  %indvars.iv.i.unr.ph = phi i64 [ %indvars.iv.next.i.prol, %for.body.i.prol ], [ %indvars.iv.i.ph, %for.body.i.preheader ]
  %i.010.i.unr.ph = phi i32 [ %inc.i.prol, %for.body.i.prol ], [ %i.010.i.ph, %for.body.i.preheader ]
  %143 = icmp eq i32 %.sroa.speculated, %.neg
  br i1 %143, label %_Z10vectorSwapPliii.exit, label %for.body.i

for.body.i:                                       ; preds = %for.body.i.prol.loopexit, %for.body.i
  %indvars.iv13.i = phi i64 [ %indvars.iv.next14.i.1, %for.body.i ], [ %indvars.iv13.i.unr.ph, %for.body.i.prol.loopexit ]
  %indvars.iv.i = phi i64 [ %indvars.iv.next.i.1, %for.body.i ], [ %indvars.iv.i.unr.ph, %for.body.i.prol.loopexit ]
  %i.010.i = phi i32 [ %inc.i.1, %for.body.i ], [ %i.010.i.unr.ph, %for.body.i.prol.loopexit ]
  %arrayidx.i.i = getelementptr inbounds i64, i64* %9, i64 %indvars.iv.i
  %144 = load i64, i64* %arrayidx.i.i, align 8, !tbaa !47
  %arrayidx2.i.i = getelementptr inbounds i64, i64* %9, i64 %indvars.iv13.i
  %145 = load i64, i64* %arrayidx2.i.i, align 8, !tbaa !47
  store i64 %145, i64* %arrayidx.i.i, align 8, !tbaa !47
  store i64 %144, i64* %arrayidx2.i.i, align 8, !tbaa !47
  %indvars.iv.next.i = add nsw i64 %indvars.iv.i, 1
  %indvars.iv.next14.i = add nsw i64 %indvars.iv13.i, 1
  %arrayidx.i.i.1 = getelementptr inbounds i64, i64* %9, i64 %indvars.iv.next.i
  %146 = load i64, i64* %arrayidx.i.i.1, align 8, !tbaa !47
  %arrayidx2.i.i.1 = getelementptr inbounds i64, i64* %9, i64 %indvars.iv.next14.i
  %147 = load i64, i64* %arrayidx2.i.i.1, align 8, !tbaa !47
  store i64 %147, i64* %arrayidx.i.i.1, align 8, !tbaa !47
  store i64 %146, i64* %arrayidx2.i.i.1, align 8, !tbaa !47
  %inc.i.1 = add nuw nsw i32 %i.010.i, 2
  %indvars.iv.next.i.1 = add nsw i64 %indvars.iv.i, 2
  %indvars.iv.next14.i.1 = add nsw i64 %indvars.iv13.i, 2
  %exitcond.not.i.1 = icmp eq i32 %inc.i.1, %.sroa.speculated
  br i1 %exitcond.not.i.1, label %_Z10vectorSwapPliii.exit, label %for.body.i, !llvm.loop !86

_Z10vectorSwapPliii.exit:                         ; preds = %for.body.i.prol.loopexit, %for.body.i, %middle.block, %_Z10vectorSwapPliii.exit290
  %cmp99 = icmp sgt i32 %sub88, 1
  br i1 %cmp99, label %if.then100, label %if.end102

if.then100:                                       ; preds = %_Z10vectorSwapPliii.exit
  %add101 = add nsw i32 %sub88, %from.tr405
  tail call void @_Z9quickSortlPiS_S_S_iii(i64 %pagesIndexAddr, i32* %sortCols, i32* %sortColTypes, i32* %sortAscendings, i32* %sortNullFirsts, i32 %sortColCount, i32 %from.tr405, i32 %add101)
  br label %if.end102

if.end102:                                        ; preds = %if.then100, %_Z10vectorSwapPliii.exit
  %cmp104 = icmp sgt i32 %sub92, 1
  br i1 %cmp104, label %if.then105, label %cleanup

if.then105:                                       ; preds = %if.end102
  %sub106 = sub nsw i32 %to, %sub92
  %148 = load i64*, i64** %valueAddresses.i, align 8, !tbaa !46
  %cmp = icmp slt i32 %sub92, 7
  br i1 %cmp, label %for.cond.preheader, label %if.end

cleanup:                                          ; preds = %if.end102, %for.cond.cleanup8, %for.cond.preheader
  ret void
}

; Function Attrs: nofree norecurse nounwind uwtable mustprogress
define dso_local void @_Z18setIntColumnValuesPliPP6ColumnPi(i64* nocapture readonly %valueAddresses, i32 %positionCount, %class.Column** nocapture readonly %inputTable, i32* nocapture %outputData) local_unnamed_addr #11 {
entry:
  %cmp24 = icmp sgt i32 %positionCount, 0
  br i1 %cmp24, label %for.body.preheader, label %for.cond.cleanup

for.body.preheader:                               ; preds = %entry
  %wide.trip.count = zext i32 %positionCount to i64
  br label %for.body

for.cond.cleanup:                                 ; preds = %if.end, %entry
  ret void

for.body:                                         ; preds = %for.body.preheader, %if.end
  %indvars.iv = phi i64 [ 0, %for.body.preheader ], [ %indvars.iv.next, %if.end ]
  %inputData.026 = phi i32* [ null, %for.body.preheader ], [ %inputData.1, %if.end ]
  %preTableIndex.025 = phi i32 [ -1, %for.body.preheader ], [ %preTableIndex.1, %if.end ]
  %arrayidx = getelementptr inbounds i64, i64* %valueAddresses, i64 %indvars.iv
  %0 = load i64, i64* %arrayidx, align 8, !tbaa !47
  %1 = lshr i64 %0, 32
  %conv.i = trunc i64 %1 to i32
  %cmp2.not = icmp eq i32 %preTableIndex.025, %conv.i
  br i1 %cmp2.not, label %if.end, label %if.then

if.then:                                          ; preds = %for.body
  %idxprom3 = ashr i64 %0, 32
  %arrayidx4 = getelementptr inbounds %class.Column*, %class.Column** %inputTable, i64 %idxprom3
  %2 = load %class.Column*, %class.Column** %arrayidx4, align 8, !tbaa !2
  %data.i = getelementptr inbounds %class.Column, %class.Column* %2, i64 0, i32 1
  %3 = bitcast i8** %data.i to i32**
  %4 = load i32*, i32** %3, align 8, !tbaa !10
  br label %if.end

if.end:                                           ; preds = %if.then, %for.body
  %preTableIndex.1 = phi i32 [ %conv.i, %if.then ], [ %preTableIndex.025, %for.body ]
  %inputData.1 = phi i32* [ %4, %if.then ], [ %inputData.026, %for.body ]
  %sext = shl i64 %0, 32
  %idxprom6 = ashr exact i64 %sext, 32
  %arrayidx7 = getelementptr inbounds i32, i32* %inputData.1, i64 %idxprom6
  %5 = load i32, i32* %arrayidx7, align 4, !tbaa !6
  %arrayidx9 = getelementptr inbounds i32, i32* %outputData, i64 %indvars.iv
  store i32 %5, i32* %arrayidx9, align 4, !tbaa !6
  %indvars.iv.next = add nuw nsw i64 %indvars.iv, 1
  %exitcond.not = icmp eq i64 %indvars.iv.next, %wide.trip.count
  br i1 %exitcond.not, label %for.cond.cleanup, label %for.body, !llvm.loop !87
}

; Function Attrs: nofree norecurse nounwind uwtable mustprogress
define dso_local void @_Z20setInt64ColumnValuesPliPP6ColumnS_(i64* nocapture readonly %valueAddresses, i32 %positionCount, %class.Column** nocapture readonly %inputTable, i64* nocapture %outputData) local_unnamed_addr #11 {
entry:
  %cmp24 = icmp sgt i32 %positionCount, 0
  br i1 %cmp24, label %for.body.preheader, label %for.cond.cleanup

for.body.preheader:                               ; preds = %entry
  %wide.trip.count = zext i32 %positionCount to i64
  br label %for.body

for.cond.cleanup:                                 ; preds = %if.end, %entry
  ret void

for.body:                                         ; preds = %for.body.preheader, %if.end
  %indvars.iv = phi i64 [ 0, %for.body.preheader ], [ %indvars.iv.next, %if.end ]
  %inputData.026 = phi i64* [ null, %for.body.preheader ], [ %inputData.1, %if.end ]
  %preTableIndex.025 = phi i32 [ -1, %for.body.preheader ], [ %preTableIndex.1, %if.end ]
  %arrayidx = getelementptr inbounds i64, i64* %valueAddresses, i64 %indvars.iv
  %0 = load i64, i64* %arrayidx, align 8, !tbaa !47
  %1 = lshr i64 %0, 32
  %conv.i = trunc i64 %1 to i32
  %cmp2.not = icmp eq i32 %preTableIndex.025, %conv.i
  br i1 %cmp2.not, label %if.end, label %if.then

if.then:                                          ; preds = %for.body
  %idxprom3 = ashr i64 %0, 32
  %arrayidx4 = getelementptr inbounds %class.Column*, %class.Column** %inputTable, i64 %idxprom3
  %2 = load %class.Column*, %class.Column** %arrayidx4, align 8, !tbaa !2
  %data.i = getelementptr inbounds %class.Column, %class.Column* %2, i64 0, i32 1
  %3 = bitcast i8** %data.i to i64**
  %4 = load i64*, i64** %3, align 8, !tbaa !10
  br label %if.end

if.end:                                           ; preds = %if.then, %for.body
  %preTableIndex.1 = phi i32 [ %conv.i, %if.then ], [ %preTableIndex.025, %for.body ]
  %inputData.1 = phi i64* [ %4, %if.then ], [ %inputData.026, %for.body ]
  %sext = shl i64 %0, 32
  %idxprom6 = ashr exact i64 %sext, 32
  %arrayidx7 = getelementptr inbounds i64, i64* %inputData.1, i64 %idxprom6
  %5 = load i64, i64* %arrayidx7, align 8, !tbaa !47
  %arrayidx9 = getelementptr inbounds i64, i64* %outputData, i64 %indvars.iv
  store i64 %5, i64* %arrayidx9, align 8, !tbaa !47
  %indvars.iv.next = add nuw nsw i64 %indvars.iv, 1
  %exitcond.not = icmp eq i64 %indvars.iv.next, %wide.trip.count
  br i1 %exitcond.not, label %for.cond.cleanup, label %for.body, !llvm.loop !88
}

; Function Attrs: nofree norecurse nounwind uwtable mustprogress
define dso_local void @_Z21setDoubleColumnValuesPliPP6ColumnPd(i64* nocapture readonly %valueAddresses, i32 %positionCount, %class.Column** nocapture readonly %inputTable, double* nocapture %outputData) local_unnamed_addr #11 {
entry:
  %cmp24 = icmp sgt i32 %positionCount, 0
  br i1 %cmp24, label %for.body.preheader, label %for.cond.cleanup

for.body.preheader:                               ; preds = %entry
  %wide.trip.count = zext i32 %positionCount to i64
  br label %for.body

for.cond.cleanup:                                 ; preds = %if.end, %entry
  ret void

for.body:                                         ; preds = %for.body.preheader, %if.end
  %indvars.iv = phi i64 [ 0, %for.body.preheader ], [ %indvars.iv.next, %if.end ]
  %inputData.026 = phi double* [ null, %for.body.preheader ], [ %inputData.1, %if.end ]
  %preTableIndex.025 = phi i32 [ -1, %for.body.preheader ], [ %preTableIndex.1, %if.end ]
  %arrayidx = getelementptr inbounds i64, i64* %valueAddresses, i64 %indvars.iv
  %0 = load i64, i64* %arrayidx, align 8, !tbaa !47
  %1 = lshr i64 %0, 32
  %conv.i = trunc i64 %1 to i32
  %cmp2.not = icmp eq i32 %preTableIndex.025, %conv.i
  br i1 %cmp2.not, label %if.end, label %if.then

if.then:                                          ; preds = %for.body
  %idxprom3 = ashr i64 %0, 32
  %arrayidx4 = getelementptr inbounds %class.Column*, %class.Column** %inputTable, i64 %idxprom3
  %2 = load %class.Column*, %class.Column** %arrayidx4, align 8, !tbaa !2
  %data.i = getelementptr inbounds %class.Column, %class.Column* %2, i64 0, i32 1
  %3 = bitcast i8** %data.i to double**
  %4 = load double*, double** %3, align 8, !tbaa !10
  br label %if.end

if.end:                                           ; preds = %if.then, %for.body
  %preTableIndex.1 = phi i32 [ %conv.i, %if.then ], [ %preTableIndex.025, %for.body ]
  %inputData.1 = phi double* [ %4, %if.then ], [ %inputData.026, %for.body ]
  %sext = shl i64 %0, 32
  %idxprom6 = ashr exact i64 %sext, 32
  %arrayidx7 = getelementptr inbounds double, double* %inputData.1, i64 %idxprom6
  %5 = load double, double* %arrayidx7, align 8, !tbaa !66
  %arrayidx9 = getelementptr inbounds double, double* %outputData, i64 %indvars.iv
  store double %5, double* %arrayidx9, align 8, !tbaa !66
  %indvars.iv.next = add nuw nsw i64 %indvars.iv, 1
  %exitcond.not = icmp eq i64 %indvars.iv.next, %wide.trip.count
  br i1 %exitcond.not, label %for.cond.cleanup, label %for.body, !llvm.loop !89
}

; Function Attrs: nofree norecurse nounwind uwtable mustprogress
define dso_local void @_Z9getResultlPiilS_i(i64 %pagesIndexAddr, i32* nocapture readonly %outputCols, i32 %outputColsCount, i64 %outputTableAddr, i32* nocapture readonly %sourceTypes, i32 %positionCount) local_unnamed_addr #11 {
entry:
  %0 = inttoptr i64 %pagesIndexAddr to %class.PagesIndex*
  %columns.i = getelementptr inbounds %class.PagesIndex, %class.PagesIndex* %0, i64 0, i32 6
  %1 = load %class.Column***, %class.Column**** %columns.i, align 8, !tbaa !43
  %valueAddresses.i = getelementptr inbounds %class.PagesIndex, %class.PagesIndex* %0, i64 0, i32 3
  %2 = load i64*, i64** %valueAddresses.i, align 8, !tbaa !46
  %cmp84 = icmp sgt i32 %outputColsCount, 0
  br i1 %cmp84, label %for.body.lr.ph, label %for.cond.cleanup

for.body.lr.ph:                                   ; preds = %entry
  %3 = inttoptr i64 %outputTableAddr to %class.Table*
  %_M_start.i.i = getelementptr inbounds %class.Table, %class.Table* %3, i64 0, i32 3, i32 0, i32 0, i32 0
  %4 = load %class.Column**, %class.Column*** %_M_start.i.i, align 8, !tbaa !26
  %cmp24.i = icmp sgt i32 %positionCount, 0
  %wide.trip.count.i = zext i32 %positionCount to i64
  %wide.trip.count = zext i32 %outputColsCount to i64
  br i1 %cmp24.i, label %for.body.us, label %for.body.preheader

for.body.preheader:                               ; preds = %for.body.lr.ph
  %5 = add nsw i64 %wide.trip.count, -1
  %xtraiter = and i64 %wide.trip.count, 7
  %6 = icmp ult i64 %5, 7
  br i1 %6, label %for.cond.cleanup.loopexit102.unr-lcssa, label %for.body.preheader.new

for.body.preheader.new:                           ; preds = %for.body.preheader
  %unroll_iter = and i64 %wide.trip.count, 4294967288
  br label %for.body

for.body.us:                                      ; preds = %for.body.lr.ph, %sw.epilog.us
  %indvars.iv = phi i64 [ %indvars.iv.next, %sw.epilog.us ], [ 0, %for.body.lr.ph ]
  %add.ptr.i.i.us = getelementptr inbounds %class.Column*, %class.Column** %4, i64 %indvars.iv
  %7 = load %class.Column*, %class.Column** %add.ptr.i.i.us, align 8, !tbaa !2
  %arrayidx.us = getelementptr inbounds i32, i32* %outputCols, i64 %indvars.iv
  %8 = load i32, i32* %arrayidx.us, align 4, !tbaa !6
  %data.i.us = getelementptr inbounds %class.Column, %class.Column* %7, i64 0, i32 1
  %9 = load i8*, i8** %data.i.us, align 8, !tbaa !10
  %idxprom4.us = sext i32 %8 to i64
  %arrayidx5.us = getelementptr inbounds i32, i32* %sourceTypes, i64 %idxprom4.us
  %10 = load i32, i32* %arrayidx5.us, align 4, !tbaa !6
  %arrayidx7.us = getelementptr inbounds %class.Column**, %class.Column*** %1, i64 %idxprom4.us
  %11 = load %class.Column**, %class.Column*** %arrayidx7.us, align 8, !tbaa !2
  switch i32 %10, label %sw.epilog.us [
    i32 1, label %for.body.i68.us.preheader
    i32 2, label %for.body.i45.us.preheader
    i32 3, label %sw.bb11.us
  ]

sw.bb11.us:                                       ; preds = %for.body.us
  %12 = bitcast i8* %9 to double*
  br label %for.body.i.us

for.body.i.us:                                    ; preds = %if.end.i.us, %sw.bb11.us
  %indvars.iv.i.us = phi i64 [ 0, %sw.bb11.us ], [ %indvars.iv.next.i.us, %if.end.i.us ]
  %inputData.026.i.us = phi double* [ null, %sw.bb11.us ], [ %inputData.1.i.us, %if.end.i.us ]
  %preTableIndex.025.i.us = phi i32 [ -1, %sw.bb11.us ], [ %preTableIndex.1.i.us, %if.end.i.us ]
  %arrayidx.i.us = getelementptr inbounds i64, i64* %2, i64 %indvars.iv.i.us
  %13 = load i64, i64* %arrayidx.i.us, align 8, !tbaa !47
  %14 = lshr i64 %13, 32
  %conv.i.i.us = trunc i64 %14 to i32
  %cmp2.not.i.us = icmp eq i32 %preTableIndex.025.i.us, %conv.i.i.us
  br i1 %cmp2.not.i.us, label %if.end.i.us, label %if.then.i.us

if.then.i.us:                                     ; preds = %for.body.i.us
  %idxprom3.i.us = ashr i64 %13, 32
  %arrayidx4.i.us = getelementptr inbounds %class.Column*, %class.Column** %11, i64 %idxprom3.i.us
  %15 = load %class.Column*, %class.Column** %arrayidx4.i.us, align 8, !tbaa !2
  %data.i.i.us = getelementptr inbounds %class.Column, %class.Column* %15, i64 0, i32 1
  %16 = bitcast i8** %data.i.i.us to double**
  %17 = load double*, double** %16, align 8, !tbaa !10
  br label %if.end.i.us

if.end.i.us:                                      ; preds = %if.then.i.us, %for.body.i.us
  %preTableIndex.1.i.us = phi i32 [ %conv.i.i.us, %if.then.i.us ], [ %preTableIndex.025.i.us, %for.body.i.us ]
  %inputData.1.i.us = phi double* [ %17, %if.then.i.us ], [ %inputData.026.i.us, %for.body.i.us ]
  %sext.i.us = shl i64 %13, 32
  %idxprom6.i.us = ashr exact i64 %sext.i.us, 32
  %arrayidx7.i.us = getelementptr inbounds double, double* %inputData.1.i.us, i64 %idxprom6.i.us
  %18 = load double, double* %arrayidx7.i.us, align 8, !tbaa !66
  %arrayidx9.i.us = getelementptr inbounds double, double* %12, i64 %indvars.iv.i.us
  store double %18, double* %arrayidx9.i.us, align 8, !tbaa !66
  %indvars.iv.next.i.us = add nuw nsw i64 %indvars.iv.i.us, 1
  %exitcond.not.i.us = icmp eq i64 %indvars.iv.next.i.us, %wide.trip.count.i
  br i1 %exitcond.not.i.us, label %sw.epilog.us, label %for.body.i.us, !llvm.loop !89

for.body.i45.us.preheader:                        ; preds = %for.body.us
  %19 = bitcast i8* %9 to i64*
  br label %for.body.i45.us

for.body.i45.us:                                  ; preds = %for.body.i45.us.preheader, %if.end.i58.us
  %indvars.iv.i39.us = phi i64 [ %indvars.iv.next.i56.us, %if.end.i58.us ], [ 0, %for.body.i45.us.preheader ]
  %inputData.026.i40.us = phi i64* [ %inputData.1.i51.us, %if.end.i58.us ], [ null, %for.body.i45.us.preheader ]
  %preTableIndex.025.i41.us = phi i32 [ %preTableIndex.1.i50.us, %if.end.i58.us ], [ -1, %for.body.i45.us.preheader ]
  %arrayidx.i42.us = getelementptr inbounds i64, i64* %2, i64 %indvars.iv.i39.us
  %20 = load i64, i64* %arrayidx.i42.us, align 8, !tbaa !47
  %21 = lshr i64 %20, 32
  %conv.i.i43.us = trunc i64 %21 to i32
  %cmp2.not.i44.us = icmp eq i32 %preTableIndex.025.i41.us, %conv.i.i43.us
  br i1 %cmp2.not.i44.us, label %if.end.i58.us, label %if.then.i49.us

if.then.i49.us:                                   ; preds = %for.body.i45.us
  %idxprom3.i46.us = ashr i64 %20, 32
  %arrayidx4.i47.us = getelementptr inbounds %class.Column*, %class.Column** %11, i64 %idxprom3.i46.us
  %22 = load %class.Column*, %class.Column** %arrayidx4.i47.us, align 8, !tbaa !2
  %data.i.i48.us = getelementptr inbounds %class.Column, %class.Column* %22, i64 0, i32 1
  %23 = bitcast i8** %data.i.i48.us to i64**
  %24 = load i64*, i64** %23, align 8, !tbaa !10
  br label %if.end.i58.us

if.end.i58.us:                                    ; preds = %if.then.i49.us, %for.body.i45.us
  %preTableIndex.1.i50.us = phi i32 [ %conv.i.i43.us, %if.then.i49.us ], [ %preTableIndex.025.i41.us, %for.body.i45.us ]
  %inputData.1.i51.us = phi i64* [ %24, %if.then.i49.us ], [ %inputData.026.i40.us, %for.body.i45.us ]
  %sext.i52.us = shl i64 %20, 32
  %idxprom6.i53.us = ashr exact i64 %sext.i52.us, 32
  %arrayidx7.i54.us = getelementptr inbounds i64, i64* %inputData.1.i51.us, i64 %idxprom6.i53.us
  %25 = load i64, i64* %arrayidx7.i54.us, align 8, !tbaa !47
  %arrayidx9.i55.us = getelementptr inbounds i64, i64* %19, i64 %indvars.iv.i39.us
  store i64 %25, i64* %arrayidx9.i55.us, align 8, !tbaa !47
  %indvars.iv.next.i56.us = add nuw nsw i64 %indvars.iv.i39.us, 1
  %exitcond.not.i57.us = icmp eq i64 %indvars.iv.next.i56.us, %wide.trip.count.i
  br i1 %exitcond.not.i57.us, label %sw.epilog.us, label %for.body.i45.us, !llvm.loop !88

for.body.i68.us.preheader:                        ; preds = %for.body.us
  %26 = bitcast i8* %9 to i32*
  br label %for.body.i68.us

for.body.i68.us:                                  ; preds = %for.body.i68.us.preheader, %if.end.i81.us
  %indvars.iv.i62.us = phi i64 [ %indvars.iv.next.i79.us, %if.end.i81.us ], [ 0, %for.body.i68.us.preheader ]
  %inputData.026.i63.us = phi i32* [ %inputData.1.i74.us, %if.end.i81.us ], [ null, %for.body.i68.us.preheader ]
  %preTableIndex.025.i64.us = phi i32 [ %preTableIndex.1.i73.us, %if.end.i81.us ], [ -1, %for.body.i68.us.preheader ]
  %arrayidx.i65.us = getelementptr inbounds i64, i64* %2, i64 %indvars.iv.i62.us
  %27 = load i64, i64* %arrayidx.i65.us, align 8, !tbaa !47
  %28 = lshr i64 %27, 32
  %conv.i.i66.us = trunc i64 %28 to i32
  %cmp2.not.i67.us = icmp eq i32 %preTableIndex.025.i64.us, %conv.i.i66.us
  br i1 %cmp2.not.i67.us, label %if.end.i81.us, label %if.then.i72.us

if.then.i72.us:                                   ; preds = %for.body.i68.us
  %idxprom3.i69.us = ashr i64 %27, 32
  %arrayidx4.i70.us = getelementptr inbounds %class.Column*, %class.Column** %11, i64 %idxprom3.i69.us
  %29 = load %class.Column*, %class.Column** %arrayidx4.i70.us, align 8, !tbaa !2
  %data.i.i71.us = getelementptr inbounds %class.Column, %class.Column* %29, i64 0, i32 1
  %30 = bitcast i8** %data.i.i71.us to i32**
  %31 = load i32*, i32** %30, align 8, !tbaa !10
  br label %if.end.i81.us

if.end.i81.us:                                    ; preds = %if.then.i72.us, %for.body.i68.us
  %preTableIndex.1.i73.us = phi i32 [ %conv.i.i66.us, %if.then.i72.us ], [ %preTableIndex.025.i64.us, %for.body.i68.us ]
  %inputData.1.i74.us = phi i32* [ %31, %if.then.i72.us ], [ %inputData.026.i63.us, %for.body.i68.us ]
  %sext.i75.us = shl i64 %27, 32
  %idxprom6.i76.us = ashr exact i64 %sext.i75.us, 32
  %arrayidx7.i77.us = getelementptr inbounds i32, i32* %inputData.1.i74.us, i64 %idxprom6.i76.us
  %32 = load i32, i32* %arrayidx7.i77.us, align 4, !tbaa !6
  %arrayidx9.i78.us = getelementptr inbounds i32, i32* %26, i64 %indvars.iv.i62.us
  store i32 %32, i32* %arrayidx9.i78.us, align 4, !tbaa !6
  %indvars.iv.next.i79.us = add nuw nsw i64 %indvars.iv.i62.us, 1
  %exitcond.not.i80.us = icmp eq i64 %indvars.iv.next.i79.us, %wide.trip.count.i
  br i1 %exitcond.not.i80.us, label %sw.epilog.us, label %for.body.i68.us, !llvm.loop !87

sw.epilog.us:                                     ; preds = %if.end.i.us, %if.end.i58.us, %if.end.i81.us, %for.body.us
  %indvars.iv.next = add nuw nsw i64 %indvars.iv, 1
  %exitcond.not = icmp eq i64 %indvars.iv.next, %wide.trip.count
  br i1 %exitcond.not, label %for.cond.cleanup, label %for.body.us, !llvm.loop !90

for.cond.cleanup.loopexit102.unr-lcssa:           ; preds = %for.body, %for.body.preheader
  %lcmp.mod.not = icmp eq i64 %xtraiter, 0
  br i1 %lcmp.mod.not, label %for.cond.cleanup, label %for.body.epil

for.body.epil:                                    ; preds = %for.cond.cleanup.loopexit102.unr-lcssa, %for.body.epil
  %epil.iter = phi i64 [ %epil.iter.sub, %for.body.epil ], [ %xtraiter, %for.cond.cleanup.loopexit102.unr-lcssa ]
  %epil.iter.sub = add i64 %epil.iter, -1
  %epil.iter.cmp.not = icmp eq i64 %epil.iter.sub, 0
  br i1 %epil.iter.cmp.not, label %for.cond.cleanup, label %for.body.epil, !llvm.loop !91

for.cond.cleanup:                                 ; preds = %for.cond.cleanup.loopexit102.unr-lcssa, %for.body.epil, %sw.epilog.us, %entry
  ret void

for.body:                                         ; preds = %for.body, %for.body.preheader.new
  %niter = phi i64 [ %unroll_iter, %for.body.preheader.new ], [ %niter.nsub.7, %for.body ]
  %niter.nsub.7 = add i64 %niter, -8
  %niter.ncmp.7 = icmp eq i64 %niter.nsub.7, 0
  br i1 %niter.ncmp.7, label %for.cond.cleanup.loopexit102.unr-lcssa, label %for.body, !llvm.loop !90
}

; Function Attrs: nounwind uwtable willreturn mustprogress
define linkonce_odr dso_local %class.Table* @_ZN4Sort9getResultEv(%class.Sort* nonnull dereferenceable(80) %this) unnamed_addr #17 comdat align 2 {
entry:
  unreachable
}

; Function Attrs: nounwind uwtable willreturn
define linkonce_odr dso_local void @_ZN6ColumnD2Ev(%class.Column* nonnull dereferenceable(40) %this) unnamed_addr #18 comdat align 2 {
entry:
  ret void
}

; Function Attrs: nounwind uwtable
define linkonce_odr dso_local void @_ZN6ColumnD0Ev(%class.Column* nonnull dereferenceable(40) %this) unnamed_addr #9 comdat align 2 {
entry:
  %0 = bitcast %class.Column* %this to i8*
  tail call void @_ZdlPv(i8* nonnull %0) #22
  ret void
}

; Function Attrs: argmemonly nofree nosync nounwind willreturn
declare void @llvm.memmove.p0i8.p0i8.i64(i8* nocapture writeonly, i8* nocapture readonly, i64, i1 immarg) #5

; Function Attrs: uwtable
define internal void @_GLOBAL__sub_I_sort.cpp() #4 section ".text.startup" {
entry:
  tail call void @_ZNSt8ios_base4InitC1Ev(%"class.std::ios_base::Init"* nonnull dereferenceable(1) @_ZStL8__ioinit)
  %0 = tail call i32 @__cxa_atexit(void (i8*)* bitcast (void (%"class.std::ios_base::Init"*)* @_ZNSt8ios_base4InitD1Ev to void (i8*)*), i8* getelementptr inbounds (%"class.std::ios_base::Init", %"class.std::ios_base::Init"* @_ZStL8__ioinit, i64 0, i32 0), i8* nonnull @__dso_handle) #19
  ret void
}

attributes #0 = { "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "no-infs-fp-math"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #1 = { nounwind "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "no-infs-fp-math"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #2 = { nofree nounwind }
attributes #3 = { norecurse nounwind readnone uwtable willreturn mustprogress "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "min-legal-vector-width"="0" "no-infs-fp-math"="false" "no-jump-tables"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #4 = { uwtable "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "min-legal-vector-width"="0" "no-infs-fp-math"="false" "no-jump-tables"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #5 = { argmemonly nofree nosync nounwind willreturn }
attributes #6 = { nobuiltin nofree allocsize(0) "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "no-infs-fp-math"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #7 = { nobuiltin nounwind "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "no-infs-fp-math"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #8 = { nofree uwtable "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "min-legal-vector-width"="0" "no-infs-fp-math"="false" "no-jump-tables"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #9 = { nounwind uwtable "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "min-legal-vector-width"="0" "no-infs-fp-math"="false" "no-jump-tables"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #10 = { nofree norecurse nounwind uwtable willreturn mustprogress "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "min-legal-vector-width"="0" "no-infs-fp-math"="false" "no-jump-tables"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #11 = { nofree norecurse nounwind uwtable mustprogress "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "min-legal-vector-width"="0" "no-infs-fp-math"="false" "no-jump-tables"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #12 = { nofree norecurse nounwind uwtable willreturn writeonly "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "min-legal-vector-width"="0" "no-infs-fp-math"="false" "no-jump-tables"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #13 = { inaccessiblememonly nofree nounwind willreturn "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "no-infs-fp-math"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #14 = { inaccessiblemem_or_argmemonly nounwind willreturn "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "no-infs-fp-math"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #15 = { norecurse nounwind readonly uwtable willreturn mustprogress "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "min-legal-vector-width"="0" "no-infs-fp-math"="false" "no-jump-tables"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #16 = { nofree nounwind uwtable "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "min-legal-vector-width"="0" "no-infs-fp-math"="false" "no-jump-tables"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #17 = { nounwind uwtable willreturn mustprogress "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "min-legal-vector-width"="0" "no-infs-fp-math"="false" "no-jump-tables"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #18 = { nounwind uwtable willreturn "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "min-legal-vector-width"="0" "no-infs-fp-math"="false" "no-jump-tables"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #19 = { nounwind }
attributes #20 = { builtin allocsize(0) }
attributes #21 = { allocsize(0) }
attributes #22 = { builtin nounwind }

!llvm.module.flags = !{!0}
!llvm.ident = !{!1}

!0 = !{i32 1, !"wchar_size", i32 4}
!1 = !{!"Ubuntu clang version 12.0.0-++20210301072539+98f06b16a313-1~exp1~20210301183256.51"}
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
!38 = !{!39, !3, i64 0}
!39 = !{!"_ZTS10PagesIndex", !3, i64 0, !7, i64 8, !3, i64 16, !7, i64 24, !3, i64 32, !7, i64 40}
!40 = !{!39, !7, i64 8}
!41 = !{!30, !3, i64 72}
!42 = !{!39, !7, i64 40}
!43 = !{!39, !3, i64 32}
!44 = distinct !{!44, !28}
!45 = distinct !{!45, !28}
!46 = !{!39, !3, i64 16}
!47 = !{!13, !13, i64 0}
!48 = !{!49}
!49 = distinct !{!49, !50}
!50 = distinct !{!50, !"LVerDomain"}
!51 = !{!52}
!52 = distinct !{!52, !50}
!53 = distinct !{!53, !28, !54}
!54 = !{!"llvm.loop.isvectorized", i32 1}
!55 = distinct !{!55, !28, !54}
!56 = !{!39, !7, i64 24}
!57 = distinct !{!57, !28}
!58 = distinct !{!58, !28}
!59 = distinct !{!59, !28, !60, !54}
!60 = !{!"llvm.loop.unroll.runtime.disable"}
!61 = !{!11, !3, i64 16}
!62 = distinct !{!62, !28}
!63 = distinct !{!63, !28, !54}
!64 = distinct !{!64, !28, !54}
!65 = distinct !{!65, !28, !60, !54}
!66 = !{!67, !67, i64 0}
!67 = !{!"double", !4, i64 0}
!68 = distinct !{!68, !28}
!69 = distinct !{!69, !28}
!70 = distinct !{!70, !28}
!71 = distinct !{!71, !28}
!72 = distinct !{!72, !28}
!73 = !{!74}
!74 = distinct !{!74, !75}
!75 = distinct !{!75, !"LVerDomain"}
!76 = !{!77}
!77 = distinct !{!77, !75}
!78 = distinct !{!78, !28, !54}
!79 = distinct !{!79, !28, !54}
!80 = !{!81}
!81 = distinct !{!81, !82}
!82 = distinct !{!82, !"LVerDomain"}
!83 = !{!84}
!84 = distinct !{!84, !82}
!85 = distinct !{!85, !28, !54}
!86 = distinct !{!86, !28, !54}
!87 = distinct !{!87, !28}
!88 = distinct !{!88, !28}
!89 = distinct !{!89, !28}
!90 = distinct !{!90, !28}
!91 = distinct !{!91, !92}
!92 = !{!"llvm.loop.unroll.disable"}
