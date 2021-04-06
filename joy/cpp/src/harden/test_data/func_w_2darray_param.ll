; ModuleID = 'func_w_2darray_param.cpp'
source_filename = "func_w_2darray_param.cpp"
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

@_ZStL8__ioinit = internal global %"class.std::ios_base::Init" zeroinitializer, align 1
@__dso_handle = external hidden global i8
@__const.main.column_type = private unnamed_addr constant [3 x i32] [i32 1, i32 2, i32 1], align 4
@_ZSt4cout = external dso_local global %"class.std::basic_ostream", align 8
@.str = private unnamed_addr constant [17 x i8] c" duration time: \00", align 1
@.str.1 = private unnamed_addr constant [4 x i8] c"ms\0A\00", align 1
@.str.2 = private unnamed_addr constant [12 x i8] c"result: %f\0A\00", align 1
@llvm.global_ctors = appending global [1 x { i32, void ()*, i8* }] [{ i32, void ()*, i8* } { i32 65535, void ()* @_GLOBAL__sub_I_func_w_2darray_param.cpp, i8* null }]

declare dso_local void @_ZNSt8ios_base4InitC1Ev(%"class.std::ios_base::Init"* nonnull dereferenceable(1)) unnamed_addr #0

; Function Attrs: nounwind
declare dso_local void @_ZNSt8ios_base4InitD1Ev(%"class.std::ios_base::Init"* nonnull dereferenceable(1)) unnamed_addr #1

; Function Attrs: nofree nounwind
declare dso_local i32 @__cxa_atexit(void (i8*)*, i8*, i8*) local_unnamed_addr #2

; Function Attrs: nounwind uwtable mustprogress
define dso_local i32 @_Z3aggPii(i32* nocapture readonly %column, i32 %size) local_unnamed_addr #3 {
entry:
  %cmp12 = icmp sgt i32 %size, 0
  br i1 %cmp12, label %for.body.preheader, label %for.cond.cleanup

for.body.preheader:                               ; preds = %entry
  %wide.trip.count = zext i32 %size to i64
  br label %for.body

for.cond.cleanup:                                 ; preds = %for.body, %entry
  %sum.0.lcssa = phi i32 [ 0, %entry ], [ %conv6, %for.body ]
  ret i32 %sum.0.lcssa

for.body:                                         ; preds = %for.body.preheader, %for.body
  %indvars.iv = phi i64 [ 0, %for.body.preheader ], [ %indvars.iv.next, %for.body ]
  %sum.013 = phi i32 [ 0, %for.body.preheader ], [ %conv6, %for.body ]
  %arrayidx = getelementptr inbounds i32, i32* %column, i64 %indvars.iv
  %0 = load i32, i32* %arrayidx, align 4, !tbaa !2
  %call = tail call i32 @rand() #10
  %add = add nsw i32 %call, %0
  %conv = sitofp i32 %add to double
  %call1 = tail call i32 @rand() #10
  %mul = mul nsw i32 %call1, 10
  %conv.i = sitofp i32 %mul to double
  %call.i = tail call double @sqrt(double %conv.i) #10
  %add3 = fadd double %call.i, %conv
  %conv4 = sitofp i32 %sum.013 to double
  %add5 = fadd double %add3, %conv4
  %conv6 = fptosi double %add5 to i32
  %indvars.iv.next = add nuw nsw i64 %indvars.iv, 1
  %exitcond.not = icmp eq i64 %indvars.iv.next, %wide.trip.count
  br i1 %exitcond.not, label %for.cond.cleanup, label %for.body, !llvm.loop !6
}

; Function Attrs: argmemonly nofree nosync nounwind willreturn
declare void @llvm.lifetime.start.p0i8(i64 immarg, i8* nocapture) #4

; Function Attrs: nounwind
declare dso_local i32 @rand() local_unnamed_addr #1

; Function Attrs: argmemonly nofree nosync nounwind willreturn
declare void @llvm.lifetime.end.p0i8(i64 immarg, i8* nocapture) #4

; Function Attrs: nounwind uwtable mustprogress
define dso_local double @_Z3aggPdi(double* nocapture readonly %column, i32 %size) local_unnamed_addr #3 {
entry:
  %cmp7 = icmp sgt i32 %size, 0
  br i1 %cmp7, label %for.body.preheader, label %for.cond.cleanup

for.body.preheader:                               ; preds = %entry
  %wide.trip.count = zext i32 %size to i64
  br label %for.body

for.cond.cleanup:                                 ; preds = %for.body, %entry
  %sum.0.lcssa = phi double [ 0.000000e+00, %entry ], [ %add1, %for.body ]
  ret double %sum.0.lcssa

for.body:                                         ; preds = %for.body.preheader, %for.body
  %indvars.iv = phi i64 [ 0, %for.body.preheader ], [ %indvars.iv.next, %for.body ]
  %sum.08 = phi double [ 0.000000e+00, %for.body.preheader ], [ %add1, %for.body ]
  %arrayidx = getelementptr inbounds double, double* %column, i64 %indvars.iv
  %0 = load double, double* %arrayidx, align 8, !tbaa !8
  %call = tail call i32 @rand() #10
  %conv = sitofp i32 %call to double
  %add = fadd double %0, %conv
  %add1 = fadd double %sum.08, %add
  %indvars.iv.next = add nuw nsw i64 %indvars.iv, 1
  %exitcond.not = icmp eq i64 %indvars.iv.next, %wide.trip.count
  br i1 %exitcond.not, label %for.cond.cleanup, label %for.body, !llvm.loop !10
}

; Function Attrs: noinline nounwind uwtable mustprogress
define dso_local double @_Z7processPPvPiii(i8** nocapture readonly %columns, i32* nocapture readonly %y, i32 %z, i32 %row_count) local_unnamed_addr #5 {
entry:
  %cmp36 = icmp sgt i32 %z, 0
  br i1 %cmp36, label %for.body.lr.ph, label %for.cond.cleanup

for.body.lr.ph:                                   ; preds = %entry
  %cmp12.i = icmp sgt i32 %row_count, 0
  %wide.trip.count.i = zext i32 %row_count to i64
  %wide.trip.count = zext i32 %z to i64
  br i1 %cmp12.i, label %for.body.us, label %for.body.preheader

for.body.preheader:                               ; preds = %for.body.lr.ph
  %0 = add nsw i64 %wide.trip.count, -1
  %xtraiter = and i64 %wide.trip.count, 7
  %1 = icmp ult i64 %0, 7
  br i1 %1, label %for.cond.cleanup.loopexit104.unr-lcssa, label %for.body.preheader.new

for.body.preheader.new:                           ; preds = %for.body.preheader
  %unroll_iter = and i64 %wide.trip.count, 4294967288
  br label %for.body

for.body.us:                                      ; preds = %for.body.lr.ph, %for.inc.us
  %indvars.iv = phi i64 [ %indvars.iv.next, %for.inc.us ], [ 0, %for.body.lr.ph ]
  %sum.037.us = phi double [ %sum.2.us, %for.inc.us ], [ 0.000000e+00, %for.body.lr.ph ]
  %arrayidx.us = getelementptr inbounds i32, i32* %y, i64 %indvars.iv
  %2 = load i32, i32* %arrayidx.us, align 4, !tbaa !2
  %cmp1.us = icmp eq i32 %2, 1
  br i1 %cmp1.us, label %if.then.us, label %if.end.us

if.then.us:                                       ; preds = %for.body.us
  %arrayidx3.us = getelementptr inbounds i8*, i8** %columns, i64 %indvars.iv
  %3 = bitcast i8** %arrayidx3.us to i32**
  %4 = load i32*, i32** %3, align 8, !tbaa !11
  br label %for.body.i.us

for.body.i.us:                                    ; preds = %for.body.i.us, %if.then.us
  %indvars.iv.i.us = phi i64 [ 0, %if.then.us ], [ %indvars.iv.next.i.us, %for.body.i.us ]
  %sum.013.i.us = phi i32 [ 0, %if.then.us ], [ %conv6.i.us, %for.body.i.us ]
  %arrayidx.i.us = getelementptr inbounds i32, i32* %4, i64 %indvars.iv.i.us
  %5 = load i32, i32* %arrayidx.i.us, align 4, !tbaa !2
  %call.i.us = tail call i32 @rand() #10
  %add.i.us = add nsw i32 %call.i.us, %5
  %conv.i.us = sitofp i32 %add.i.us to double
  %call1.i.us = tail call i32 @rand() #10
  %mul.i.us = mul nsw i32 %call1.i.us, 10
  %conv.i.i.us = sitofp i32 %mul.i.us to double
  %call.i.i.us = tail call double @sqrt(double %conv.i.i.us) #10
  %add3.i.us = fadd double %call.i.i.us, %conv.i.us
  %conv4.i.us = sitofp i32 %sum.013.i.us to double
  %add5.i.us = fadd double %add3.i.us, %conv4.i.us
  %conv6.i.us = fptosi double %add5.i.us to i32
  %indvars.iv.next.i.us = add nuw nsw i64 %indvars.iv.i.us, 1
  %exitcond.not.i.us = icmp eq i64 %indvars.iv.next.i.us, %wide.trip.count.i
  br i1 %exitcond.not.i.us, label %_Z3aggPii.exit.loopexit.us, label %for.body.i.us, !llvm.loop !6

if.end.us:                                        ; preds = %_Z3aggPii.exit.loopexit.us, %for.body.us
  %6 = phi i32 [ %.pr.us, %_Z3aggPii.exit.loopexit.us ], [ %2, %for.body.us ]
  %sum.1.us = phi double [ %add.us, %_Z3aggPii.exit.loopexit.us ], [ %sum.037.us, %for.body.us ]
  %cmp6.us = icmp eq i32 %6, 2
  br i1 %cmp6.us, label %for.body.i35.us.preheader, label %for.inc.us

for.body.i35.us.preheader:                        ; preds = %if.end.us
  %arrayidx9.us = getelementptr inbounds i8*, i8** %columns, i64 %indvars.iv
  %7 = bitcast i8** %arrayidx9.us to double**
  %8 = load double*, double** %7, align 8, !tbaa !11
  br label %for.body.i35.us

for.body.i35.us:                                  ; preds = %for.body.i35.us.preheader, %for.body.i35.us
  %indvars.iv.i28.us = phi i64 [ %indvars.iv.next.i33.us, %for.body.i35.us ], [ 0, %for.body.i35.us.preheader ]
  %sum.08.i.us = phi double [ %add1.i.us, %for.body.i35.us ], [ 0.000000e+00, %for.body.i35.us.preheader ]
  %arrayidx.i29.us = getelementptr inbounds double, double* %8, i64 %indvars.iv.i28.us
  %9 = load double, double* %arrayidx.i29.us, align 8, !tbaa !8
  %call.i30.us = tail call i32 @rand() #10
  %conv.i31.us = sitofp i32 %call.i30.us to double
  %add.i32.us = fadd double %9, %conv.i31.us
  %add1.i.us = fadd double %sum.08.i.us, %add.i32.us
  %indvars.iv.next.i33.us = add nuw nsw i64 %indvars.iv.i28.us, 1
  %exitcond.not.i34.us = icmp eq i64 %indvars.iv.next.i33.us, %wide.trip.count.i
  br i1 %exitcond.not.i34.us, label %_Z3aggPdi.exit.us, label %for.body.i35.us, !llvm.loop !10

_Z3aggPdi.exit.us:                                ; preds = %for.body.i35.us
  %add11.us = fadd double %sum.1.us, %add1.i.us
  br label %for.inc.us

for.inc.us:                                       ; preds = %_Z3aggPdi.exit.us, %if.end.us
  %sum.2.us = phi double [ %add11.us, %_Z3aggPdi.exit.us ], [ %sum.1.us, %if.end.us ]
  %indvars.iv.next = add nuw nsw i64 %indvars.iv, 1
  %exitcond.not = icmp eq i64 %indvars.iv.next, %wide.trip.count
  br i1 %exitcond.not, label %for.cond.cleanup, label %for.body.us, !llvm.loop !13

_Z3aggPii.exit.loopexit.us:                       ; preds = %for.body.i.us
  %conv.us = sitofp i32 %conv6.i.us to double
  %add.us = fadd double %sum.037.us, %conv.us
  %.pr.us = load i32, i32* %arrayidx.us, align 4, !tbaa !2
  br label %if.end.us

for.cond.cleanup.loopexit104.unr-lcssa:           ; preds = %for.body, %for.body.preheader
  %lcmp.mod.not = icmp eq i64 %xtraiter, 0
  br i1 %lcmp.mod.not, label %for.cond.cleanup, label %for.body.epil

for.body.epil:                                    ; preds = %for.cond.cleanup.loopexit104.unr-lcssa, %for.body.epil
  %epil.iter = phi i64 [ %epil.iter.sub, %for.body.epil ], [ %xtraiter, %for.cond.cleanup.loopexit104.unr-lcssa ]
  %epil.iter.sub = add i64 %epil.iter, -1
  %epil.iter.cmp.not = icmp eq i64 %epil.iter.sub, 0
  br i1 %epil.iter.cmp.not, label %for.cond.cleanup, label %for.body.epil, !llvm.loop !14

for.cond.cleanup:                                 ; preds = %for.cond.cleanup.loopexit104.unr-lcssa, %for.body.epil, %for.inc.us, %entry
  %sum.0.lcssa = phi double [ 0.000000e+00, %entry ], [ %sum.2.us, %for.inc.us ], [ 0.000000e+00, %for.body.epil ], [ 0.000000e+00, %for.cond.cleanup.loopexit104.unr-lcssa ]
  ret double %sum.0.lcssa

for.body:                                         ; preds = %for.body, %for.body.preheader.new
  %niter = phi i64 [ %unroll_iter, %for.body.preheader.new ], [ %niter.nsub.7, %for.body ]
  %niter.nsub.7 = add i64 %niter, -8
  %niter.ncmp.7 = icmp eq i64 %niter.nsub.7, 0
  br i1 %niter.ncmp.7, label %for.cond.cleanup.loopexit104.unr-lcssa, label %for.body, !llvm.loop !13
}

; Function Attrs: norecurse uwtable
define dso_local i32 @main(i32 %argc, i8** nocapture readonly %argv) local_unnamed_addr #6 {
entry:
  %columns = alloca [3 x i8*], align 16
  %column_type = alloca [3 x i32], align 4
  %arrayidx = getelementptr inbounds i8*, i8** %argv, i64 1
  %0 = load i8*, i8** %arrayidx, align 8, !tbaa !11
  %call.i = tail call i64 @strtol(i8* nocapture nonnull %0, i8** null, i32 10) #10
  %conv.i = trunc i64 %call.i to i32
  %1 = and i64 %call.i, 4294967295
  %vla = alloca i32, i64 %1, align 16
  %vla1 = alloca i32, i64 %1, align 16
  %vla2 = alloca i32, i64 %1, align 16
  %cmp62 = icmp sgt i32 %conv.i, 0
  br i1 %cmp62, label %for.body.preheader, label %for.cond.cleanup

for.body.preheader:                               ; preds = %entry
  %wide.trip.count = and i64 %call.i, 4294967295
  %min.iters.check = icmp ult i64 %wide.trip.count, 8
  br i1 %min.iters.check, label %for.body.preheader75, label %vector.ph

vector.ph:                                        ; preds = %for.body.preheader
  %n.mod.vf = and i64 %call.i, 7
  %n.vec = sub nsw i64 %wide.trip.count, %n.mod.vf
  br label %vector.body

vector.body:                                      ; preds = %vector.body, %vector.ph
  %index = phi i64 [ 0, %vector.ph ], [ %index.next, %vector.body ]
  %vec.ind67 = phi <4 x i32> [ <i32 0, i32 1, i32 2, i32 3>, %vector.ph ], [ %vec.ind.next70, %vector.body ]
  %vec.ind71 = phi <4 x i32> [ <i32 0, i32 1, i32 2, i32 3>, %vector.ph ], [ %vec.ind.next74, %vector.body ]
  %2 = getelementptr inbounds i32, i32* %vla, i64 %index
  %step.add68 = add <4 x i32> %vec.ind67, <i32 4, i32 4, i32 4, i32 4>
  %3 = bitcast i32* %2 to <4 x i32>*
  store <4 x i32> %vec.ind67, <4 x i32>* %3, align 16, !tbaa !2
  %4 = getelementptr inbounds i32, i32* %2, i64 4
  %5 = bitcast i32* %4 to <4 x i32>*
  store <4 x i32> %step.add68, <4 x i32>* %5, align 16, !tbaa !2
  %6 = getelementptr inbounds i32, i32* %vla1, i64 %index
  %7 = shl <4 x i32> %vec.ind71, <i32 1, i32 1, i32 1, i32 1>
  %step.add72 = shl <4 x i32> %vec.ind71, <i32 1, i32 1, i32 1, i32 1>
  %8 = add <4 x i32> %step.add72, <i32 8, i32 8, i32 8, i32 8>
  %9 = bitcast i32* %6 to <4 x i32>*
  store <4 x i32> %7, <4 x i32>* %9, align 16, !tbaa !2
  %10 = getelementptr inbounds i32, i32* %6, i64 4
  %11 = bitcast i32* %10 to <4 x i32>*
  store <4 x i32> %8, <4 x i32>* %11, align 16, !tbaa !2
  %12 = mul nsw <4 x i32> %vec.ind67, <i32 3, i32 3, i32 3, i32 3>
  %13 = mul nsw <4 x i32> %step.add68, <i32 3, i32 3, i32 3, i32 3>
  %14 = getelementptr inbounds i32, i32* %vla2, i64 %index
  %15 = bitcast i32* %14 to <4 x i32>*
  store <4 x i32> %12, <4 x i32>* %15, align 16, !tbaa !2
  %16 = getelementptr inbounds i32, i32* %14, i64 4
  %17 = bitcast i32* %16 to <4 x i32>*
  store <4 x i32> %13, <4 x i32>* %17, align 16, !tbaa !2
  %index.next = add i64 %index, 8
  %vec.ind.next70 = add <4 x i32> %vec.ind67, <i32 8, i32 8, i32 8, i32 8>
  %vec.ind.next74 = add <4 x i32> %vec.ind71, <i32 8, i32 8, i32 8, i32 8>
  %18 = icmp eq i64 %index.next, %n.vec
  br i1 %18, label %middle.block, label %vector.body, !llvm.loop !16

middle.block:                                     ; preds = %vector.body
  %cmp.n = icmp eq i64 %n.mod.vf, 0
  br i1 %cmp.n, label %for.cond.cleanup, label %for.body.preheader75

for.body.preheader75:                             ; preds = %for.body.preheader, %middle.block
  %indvars.iv.ph = phi i64 [ 0, %for.body.preheader ], [ %n.vec, %middle.block ]
  br label %for.body

for.cond.cleanup:                                 ; preds = %for.body, %middle.block, %entry
  %19 = bitcast [3 x i8*]* %columns to i8*
  call void @llvm.lifetime.start.p0i8(i64 24, i8* nonnull %19) #10
  %arrayinit.begin = getelementptr inbounds [3 x i8*], [3 x i8*]* %columns, i64 0, i64 0
  %20 = bitcast [3 x i8*]* %columns to i32**
  store i32* %vla, i32** %20, align 16, !tbaa !11
  %arrayinit.element = getelementptr inbounds [3 x i8*], [3 x i8*]* %columns, i64 0, i64 1
  %21 = bitcast i8** %arrayinit.element to i32**
  store i32* %vla1, i32** %21, align 8, !tbaa !11
  %arrayinit.element9 = getelementptr inbounds [3 x i8*], [3 x i8*]* %columns, i64 0, i64 2
  %22 = bitcast i8** %arrayinit.element9 to i32**
  store i32* %vla2, i32** %22, align 16, !tbaa !11
  %23 = bitcast [3 x i32]* %column_type to i8*
  call void @llvm.lifetime.start.p0i8(i64 12, i8* nonnull %23) #10
  call void @llvm.memcpy.p0i8.p0i8.i64(i8* nonnull align 4 dereferenceable(12) %23, i8* nonnull align 4 dereferenceable(12) bitcast ([3 x i32]* @__const.main.column_type to i8*), i64 12, i1 false)
  %call10 = call i64 @_ZNSt6chrono3_V212system_clock3nowEv() #10
  %arraydecay17 = getelementptr inbounds [3 x i32], [3 x i32]* %column_type, i64 0, i64 0
  br label %for.body16

for.body:                                         ; preds = %for.body.preheader75, %for.body
  %indvars.iv = phi i64 [ %indvars.iv.next, %for.body ], [ %indvars.iv.ph, %for.body.preheader75 ]
  %arrayidx3 = getelementptr inbounds i32, i32* %vla, i64 %indvars.iv
  %24 = trunc i64 %indvars.iv to i32
  store i32 %24, i32* %arrayidx3, align 4, !tbaa !2
  %arrayidx5 = getelementptr inbounds i32, i32* %vla1, i64 %indvars.iv
  %indvars.iv.tr = trunc i64 %indvars.iv to i32
  %25 = shl i32 %indvars.iv.tr, 1
  store i32 %25, i32* %arrayidx5, align 4, !tbaa !2
  %mul6 = mul nsw i32 %24, 3
  %arrayidx8 = getelementptr inbounds i32, i32* %vla2, i64 %indvars.iv
  store i32 %mul6, i32* %arrayidx8, align 4, !tbaa !2
  %indvars.iv.next = add nuw nsw i64 %indvars.iv, 1
  %exitcond65.not = icmp eq i64 %indvars.iv.next, %wide.trip.count
  br i1 %exitcond65.not, label %for.cond.cleanup, label %for.body, !llvm.loop !18

for.cond.cleanup15:                               ; preds = %for.body16
  %call22 = call i64 @_ZNSt6chrono3_V212system_clock3nowEv() #10
  %sub.i.i = sub nsw i64 %call22, %call10
  %conv.i.i.i = sitofp i64 %sub.i.i to float
  %div.i.i.i = fdiv float %conv.i.i.i, 1.000000e+09
  %mul.i.i = fmul float %div.i.i.i, 1.000000e+03
  %conv.i.i = fptosi float %mul.i.i to i64
  %call1.i53 = call nonnull align 8 dereferenceable(8) %"class.std::basic_ostream"* @_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l(%"class.std::basic_ostream"* nonnull align 8 dereferenceable(8) @_ZSt4cout, i8* nonnull getelementptr inbounds ([17 x i8], [17 x i8]* @.str, i64 0, i64 0), i64 16)
  %call.i55 = call nonnull align 8 dereferenceable(8) %"class.std::basic_ostream"* @_ZNSo9_M_insertIlEERSoT_(%"class.std::basic_ostream"* nonnull dereferenceable(8) @_ZSt4cout, i64 %conv.i.i)
  %call1.i = call nonnull align 8 dereferenceable(8) %"class.std::basic_ostream"* @_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l(%"class.std::basic_ostream"* nonnull align 8 dereferenceable(8) %call.i55, i8* nonnull getelementptr inbounds ([4 x i8], [4 x i8]* @.str.1, i64 0, i64 0), i64 3)
  %call33 = call i32 (i8*, ...) @printf(i8* nonnull dereferenceable(1) getelementptr inbounds ([12 x i8], [12 x i8]* @.str.2, i64 0, i64 0), double %call18)
  call void @llvm.lifetime.end.p0i8(i64 12, i8* nonnull %23) #10
  call void @llvm.lifetime.end.p0i8(i64 24, i8* nonnull %19) #10
  ret i32 0

for.body16:                                       ; preds = %for.cond.cleanup, %for.body16
  %i12.061 = phi i32 [ 0, %for.cond.cleanup ], [ %inc20, %for.body16 ]
  %call18 = call double @_Z7processPPvPiii(i8** nonnull %arrayinit.begin, i32* nonnull %arraydecay17, i32 3, i32 %conv.i)
  %inc20 = add nuw nsw i32 %i12.061, 1
  %exitcond.not = icmp eq i32 %inc20, 10000
  br i1 %exitcond.not, label %for.cond.cleanup15, label %for.body16, !llvm.loop !20
}

; Function Attrs: argmemonly nofree nosync nounwind willreturn
declare void @llvm.memcpy.p0i8.p0i8.i64(i8* noalias nocapture writeonly, i8* noalias nocapture readonly, i64, i1 immarg) #4

; Function Attrs: nounwind
declare dso_local i64 @_ZNSt6chrono3_V212system_clock3nowEv() local_unnamed_addr #1

; Function Attrs: nofree nounwind
declare dso_local noundef i32 @printf(i8* nocapture noundef readonly, ...) local_unnamed_addr #7

; Function Attrs: nofree nounwind willreturn
declare dso_local double @sqrt(double) local_unnamed_addr #8

; Function Attrs: nofree nounwind willreturn
declare dso_local i64 @strtol(i8* readonly, i8** nocapture, i32) local_unnamed_addr #8

declare dso_local nonnull align 8 dereferenceable(8) %"class.std::basic_ostream"* @_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l(%"class.std::basic_ostream"* nonnull align 8 dereferenceable(8), i8*, i64) local_unnamed_addr #0

declare dso_local nonnull align 8 dereferenceable(8) %"class.std::basic_ostream"* @_ZNSo9_M_insertIlEERSoT_(%"class.std::basic_ostream"* nonnull dereferenceable(8), i64) local_unnamed_addr #0

; Function Attrs: uwtable
define internal void @_GLOBAL__sub_I_func_w_2darray_param.cpp() #9 section ".text.startup" {
entry:
  tail call void @_ZNSt8ios_base4InitC1Ev(%"class.std::ios_base::Init"* nonnull dereferenceable(1) @_ZStL8__ioinit)
  %0 = tail call i32 @__cxa_atexit(void (i8*)* bitcast (void (%"class.std::ios_base::Init"*)* @_ZNSt8ios_base4InitD1Ev to void (i8*)*), i8* getelementptr inbounds (%"class.std::ios_base::Init", %"class.std::ios_base::Init"* @_ZStL8__ioinit, i64 0, i32 0), i8* nonnull @__dso_handle) #10
  ret void
}

attributes #0 = { "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "no-infs-fp-math"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #1 = { nounwind "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "no-infs-fp-math"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #2 = { nofree nounwind }
attributes #3 = { nounwind uwtable mustprogress "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "min-legal-vector-width"="0" "no-infs-fp-math"="false" "no-jump-tables"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #4 = { argmemonly nofree nosync nounwind willreturn }
attributes #5 = { noinline nounwind uwtable mustprogress "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "min-legal-vector-width"="0" "no-infs-fp-math"="false" "no-jump-tables"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #6 = { norecurse uwtable "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "min-legal-vector-width"="0" "no-infs-fp-math"="false" "no-jump-tables"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #7 = { nofree nounwind "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "no-infs-fp-math"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #8 = { nofree nounwind willreturn "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "no-infs-fp-math"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #9 = { uwtable "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "min-legal-vector-width"="0" "no-infs-fp-math"="false" "no-jump-tables"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #10 = { nounwind }

!llvm.module.flags = !{!0}
!llvm.ident = !{!1}

!0 = !{i32 1, !"wchar_size", i32 4}
!1 = !{!"Ubuntu clang version 12.0.0-++20210220023022+2f74c2204827-1~exp1~20210220013739.38"}
!2 = !{!3, !3, i64 0}
!3 = !{!"int", !4, i64 0}
!4 = !{!"omnipotent char", !5, i64 0}
!5 = !{!"Simple C++ TBAA"}
!6 = distinct !{!6, !7}
!7 = !{!"llvm.loop.mustprogress"}
!8 = !{!9, !9, i64 0}
!9 = !{!"double", !4, i64 0}
!10 = distinct !{!10, !7}
!11 = !{!12, !12, i64 0}
!12 = !{!"any pointer", !4, i64 0}
!13 = distinct !{!13, !7}
!14 = distinct !{!14, !15}
!15 = !{!"llvm.loop.unroll.disable"}
!16 = distinct !{!16, !7, !17}
!17 = !{!"llvm.loop.isvectorized", i32 1}
!18 = distinct !{!18, !7, !19, !17}
!19 = !{!"llvm.loop.unroll.runtime.disable"}
!20 = distinct !{!20, !7}
