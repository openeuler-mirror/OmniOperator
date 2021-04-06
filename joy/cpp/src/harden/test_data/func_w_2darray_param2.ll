; ModuleID = 'func_w_2darray_param2.cpp'
source_filename = "func_w_2darray_param2.cpp"
target datalayout = "e-m:e-p270:32:32-p271:32:32-p272:64:64-i64:64-f80:128-n8:16:32:64-S128"
target triple = "x86_64-pc-linux-gnu"

; Function Attrs: noinline norecurse nounwind readonly uwtable willreturn mustprogress
define dso_local double @_Z7processPPvPiii(i8** nocapture readonly %columns, i32* nocapture readonly %y, i32 %z, i32 %row_count) local_unnamed_addr #0 {
entry:
  %cmp28 = icmp sgt i32 %z, 0
  br i1 %cmp28, label %for.body.preheader, label %for.cond.cleanup

for.body.preheader:                               ; preds = %entry
  %wide.trip.count = zext i32 %z to i64
  %xtraiter = and i64 %wide.trip.count, 1
  %0 = icmp eq i32 %z, 1
  br i1 %0, label %for.cond.cleanup.loopexit.unr-lcssa, label %for.body.preheader.new

for.body.preheader.new:                           ; preds = %for.body.preheader
  %unroll_iter = and i64 %wide.trip.count, 4294967294
  br label %for.body

for.cond.cleanup.loopexit.unr-lcssa:              ; preds = %for.inc.1, %for.body.preheader
  %sum.2.lcssa.ph = phi double [ undef, %for.body.preheader ], [ %sum.2.1, %for.inc.1 ]
  %indvars.iv.unr = phi i64 [ 0, %for.body.preheader ], [ %indvars.iv.next.1, %for.inc.1 ]
  %sum.029.unr = phi double [ 0.000000e+00, %for.body.preheader ], [ %sum.2.1, %for.inc.1 ]
  %lcmp.mod.not = icmp eq i64 %xtraiter, 0
  br i1 %lcmp.mod.not, label %for.cond.cleanup, label %for.body.epil

for.body.epil:                                    ; preds = %for.cond.cleanup.loopexit.unr-lcssa
  %arrayidx.epil = getelementptr inbounds i32, i32* %y, i64 %indvars.iv.unr
  %1 = load i32, i32* %arrayidx.epil, align 4, !tbaa !2
  %.off.epil = add i32 %1, -1
  %switch.epil = icmp ult i32 %.off.epil, 2
  br i1 %switch.epil, label %for.inc.sink.split.epil, label %for.cond.cleanup

for.inc.sink.split.epil:                          ; preds = %for.body.epil
  %arrayidx3.epil = getelementptr inbounds i8*, i8** %columns, i64 %indvars.iv.unr
  %2 = bitcast i8** %arrayidx3.epil to i32**
  %3 = load i32*, i32** %2, align 8, !tbaa !6
  %4 = load i32, i32* %3, align 4, !tbaa !2
  %conv.epil = sitofp i32 %4 to double
  %add.epil = fadd double %sum.029.unr, %conv.epil
  br label %for.cond.cleanup

for.cond.cleanup:                                 ; preds = %for.cond.cleanup.loopexit.unr-lcssa, %for.body.epil, %for.inc.sink.split.epil, %entry
  %sum.0.lcssa = phi double [ 0.000000e+00, %entry ], [ %sum.2.lcssa.ph, %for.cond.cleanup.loopexit.unr-lcssa ], [ %sum.029.unr, %for.body.epil ], [ %add.epil, %for.inc.sink.split.epil ]
  ret double %sum.0.lcssa

for.body:                                         ; preds = %for.inc.1, %for.body.preheader.new
  %indvars.iv = phi i64 [ 0, %for.body.preheader.new ], [ %indvars.iv.next.1, %for.inc.1 ]
  %sum.029 = phi double [ 0.000000e+00, %for.body.preheader.new ], [ %sum.2.1, %for.inc.1 ]
  %niter = phi i64 [ %unroll_iter, %for.body.preheader.new ], [ %niter.nsub.1, %for.inc.1 ]
  %arrayidx = getelementptr inbounds i32, i32* %y, i64 %indvars.iv
  %5 = load i32, i32* %arrayidx, align 4, !tbaa !2
  %.off = add i32 %5, -1
  %switch = icmp ult i32 %.off, 2
  br i1 %switch, label %for.inc.sink.split, label %for.inc

for.inc.sink.split:                               ; preds = %for.body
  %arrayidx3 = getelementptr inbounds i8*, i8** %columns, i64 %indvars.iv
  %6 = bitcast i8** %arrayidx3 to i32**
  %7 = load i32*, i32** %6, align 8, !tbaa !6
  %8 = load i32, i32* %7, align 4, !tbaa !2
  %conv = sitofp i32 %8 to double
  %add = fadd double %sum.029, %conv
  br label %for.inc

for.inc:                                          ; preds = %for.body, %for.inc.sink.split
  %sum.2 = phi double [ %sum.029, %for.body ], [ %add, %for.inc.sink.split ]
  %indvars.iv.next = or i64 %indvars.iv, 1
  %arrayidx.1 = getelementptr inbounds i32, i32* %y, i64 %indvars.iv.next
  %9 = load i32, i32* %arrayidx.1, align 4, !tbaa !2
  %.off.1 = add i32 %9, -1
  %switch.1 = icmp ult i32 %.off.1, 2
  br i1 %switch.1, label %for.inc.sink.split.1, label %for.inc.1

for.inc.sink.split.1:                             ; preds = %for.inc
  %arrayidx3.1 = getelementptr inbounds i8*, i8** %columns, i64 %indvars.iv.next
  %10 = bitcast i8** %arrayidx3.1 to i32**
  %11 = load i32*, i32** %10, align 8, !tbaa !6
  %12 = load i32, i32* %11, align 4, !tbaa !2
  %conv.1 = sitofp i32 %12 to double
  %add.1 = fadd double %sum.2, %conv.1
  br label %for.inc.1

for.inc.1:                                        ; preds = %for.inc.sink.split.1, %for.inc
  %sum.2.1 = phi double [ %sum.2, %for.inc ], [ %add.1, %for.inc.sink.split.1 ]
  %indvars.iv.next.1 = add nuw nsw i64 %indvars.iv, 2
  %niter.nsub.1 = add i64 %niter, -2
  %niter.ncmp.1 = icmp eq i64 %niter.nsub.1, 0
  br i1 %niter.ncmp.1, label %for.cond.cleanup.loopexit.unr-lcssa, label %for.body, !llvm.loop !8
}

attributes #0 = { noinline norecurse nounwind readonly uwtable willreturn mustprogress "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "min-legal-vector-width"="0" "no-infs-fp-math"="false" "no-jump-tables"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }

!llvm.module.flags = !{!0}
!llvm.ident = !{!1}

!0 = !{i32 1, !"wchar_size", i32 4}
!1 = !{!"Ubuntu clang version 12.0.0-++20210220023022+2f74c2204827-1~exp1~20210220013739.38"}
!2 = !{!3, !3, i64 0}
!3 = !{!"int", !4, i64 0}
!4 = !{!"omnipotent char", !5, i64 0}
!5 = !{!"Simple C++ TBAA"}
!6 = !{!7, !7, i64 0}
!7 = !{!"any pointer", !4, i64 0}
!8 = distinct !{!8, !9}
!9 = !{!"llvm.loop.mustprogress"}
