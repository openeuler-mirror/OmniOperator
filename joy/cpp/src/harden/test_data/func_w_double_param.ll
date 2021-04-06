; ModuleID = 'func_w_double_param.cpp'
source_filename = "func_w_double_param.cpp"
target datalayout = "e-m:e-p270:32:32-p271:32:32-p272:64:64-i64:64-f80:128-n8:16:32:64-S128"
target triple = "x86_64-pc-linux-gnu"

@.str = private unnamed_addr constant [10 x i8] c"hello %d\0A\00", align 1
@.str.1 = private unnamed_addr constant [29 x i8] c"row_data[%d]: %f, y[%d]: %d\0A\00", align 1
@.str.2 = private unnamed_addr constant [8 x i8] c"sum=%f\0A\00", align 1

; Function Attrs: nofree noinline nounwind uwtable mustprogress
define dso_local double @_Z7processPdPii(double* nocapture readonly %row_data, i32* nocapture readonly %y, i32 %z) local_unnamed_addr #0 {
entry:
  %call = tail call i32 (i8*, ...) @printf(i8* nonnull dereferenceable(1) getelementptr inbounds ([10 x i8], [10 x i8]* @.str, i64 0, i64 0), i32 %z)
  %cmp39 = icmp sgt i32 %z, 0
  br i1 %cmp39, label %for.body.preheader, label %for.cond.cleanup

for.body.preheader:                               ; preds = %entry
  %wide.trip.count = zext i32 %z to i64
  br label %for.body

for.cond.cleanup:                                 ; preds = %if.end16, %entry
  %sum.0.lcssa = phi double [ 0.000000e+00, %entry ], [ %sum.2, %if.end16 ]
  ret double %sum.0.lcssa

for.body:                                         ; preds = %for.body.preheader, %if.end16
  %indvars.iv = phi i64 [ 0, %for.body.preheader ], [ %indvars.iv.next, %if.end16 ]
  %sum.040 = phi double [ 0.000000e+00, %for.body.preheader ], [ %sum.2, %if.end16 ]
  %arrayidx = getelementptr inbounds double, double* %row_data, i64 %indvars.iv
  %0 = load double, double* %arrayidx, align 8, !tbaa !2
  %arrayidx2 = getelementptr inbounds i32, i32* %y, i64 %indvars.iv
  %1 = load i32, i32* %arrayidx2, align 4, !tbaa !6
  %2 = trunc i64 %indvars.iv to i32
  %call3 = tail call i32 (i8*, ...) @printf(i8* nonnull dereferenceable(1) getelementptr inbounds ([29 x i8], [29 x i8]* @.str.1, i64 0, i64 0), i32 %2, double %0, i32 %2, i32 %1)
  %3 = load i32, i32* %arrayidx2, align 4, !tbaa !6
  %.off = add i32 %3, -1
  %switch = icmp ult i32 %.off, 2
  br i1 %switch, label %if.end16.sink.split, label %if.end16

if.end16.sink.split:                              ; preds = %for.body
  %4 = load double, double* %arrayidx, align 8, !tbaa !2
  %add = fadd double %sum.040, %4
  br label %if.end16

if.end16:                                         ; preds = %for.body, %if.end16.sink.split
  %sum.2 = phi double [ %sum.040, %for.body ], [ %add, %if.end16.sink.split ]
  %call17 = tail call i32 (i8*, ...) @printf(i8* nonnull dereferenceable(1) getelementptr inbounds ([8 x i8], [8 x i8]* @.str.2, i64 0, i64 0), double %sum.2)
  %indvars.iv.next = add nuw nsw i64 %indvars.iv, 1
  %exitcond.not = icmp eq i64 %indvars.iv.next, %wide.trip.count
  br i1 %exitcond.not, label %for.cond.cleanup, label %for.body, !llvm.loop !8
}

; Function Attrs: nofree nounwind
declare dso_local noundef i32 @printf(i8* nocapture noundef readonly, ...) local_unnamed_addr #1

attributes #0 = { nofree noinline nounwind uwtable mustprogress "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "min-legal-vector-width"="0" "no-infs-fp-math"="false" "no-jump-tables"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #1 = { nofree nounwind "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "no-infs-fp-math"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }

!llvm.module.flags = !{!0}
!llvm.ident = !{!1}

!0 = !{i32 1, !"wchar_size", i32 4}
!1 = !{!"Ubuntu clang version 12.0.0-++20210220023022+2f74c2204827-1~exp1~20210220013739.38"}
!2 = !{!3, !3, i64 0}
!3 = !{!"double", !4, i64 0}
!4 = !{!"omnipotent char", !5, i64 0}
!5 = !{!"Simple C++ TBAA"}
!6 = !{!7, !7, i64 0}
!7 = !{!"int", !4, i64 0}
!8 = distinct !{!8, !9}
!9 = !{!"llvm.loop.mustprogress"}
