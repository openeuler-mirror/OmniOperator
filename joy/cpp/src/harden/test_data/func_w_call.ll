; ModuleID = 'func_w_call.cpp'
source_filename = "func_w_call.cpp"
target datalayout = "e-m:e-p270:32:32-p271:32:32-p272:64:64-i64:64-f80:128-n8:16:32:64-S128"
target triple = "x86_64-pc-linux-gnu"

@.str = private unnamed_addr constant [10 x i8] c"hello %d\0A\00", align 1
@.str.1 = private unnamed_addr constant [29 x i8] c"row_data[%d]: %d, y[%d]: %d\0A\00", align 1
@.str.2 = private unnamed_addr constant [8 x i8] c"sum=%d\0A\00", align 1

; Function Attrs: norecurse nounwind readonly uwtable willreturn mustprogress
define dso_local i32 @_Z13processColumnPiS_i(i32* nocapture readonly %row_data, i32* nocapture readonly %y, i32 %col_idx) local_unnamed_addr #0 {
entry:
  %idxprom = sext i32 %col_idx to i64
  %arrayidx = getelementptr inbounds i32, i32* %y, i64 %idxprom
  %0 = load i32, i32* %arrayidx, align 4, !tbaa !2
  %.off = add i32 %0, -1
  %switch = icmp ult i32 %.off, 2
  br i1 %switch, label %return.sink.split, label %return

return.sink.split:                                ; preds = %entry
  %arrayidx5 = getelementptr inbounds i32, i32* %row_data, i64 %idxprom
  %1 = load i32, i32* %arrayidx5, align 4, !tbaa !2
  %add6 = add nsw i32 %1, 1
  br label %return

return:                                           ; preds = %entry, %return.sink.split
  %retval.0 = phi i32 [ 0, %entry ], [ %add6, %return.sink.split ]
  ret i32 %retval.0
}

; Function Attrs: nofree noinline nounwind uwtable mustprogress
define dso_local i32 @_Z7processPiS_i(i32* nocapture readonly %row_data, i32* nocapture readonly %y, i32 %z) local_unnamed_addr #1 {
entry:
  %call = tail call i32 (i8*, ...) @printf(i8* nonnull dereferenceable(1) getelementptr inbounds ([10 x i8], [10 x i8]* @.str, i64 0, i64 0), i32 %z)
  %cmp49 = icmp sgt i32 %z, 0
  br i1 %cmp49, label %for.body.preheader, label %for.cond.cleanup

for.body.preheader:                               ; preds = %entry
  %wide.trip.count = zext i32 %z to i64
  br label %for.body

for.cond.cleanup:                                 ; preds = %if.end14, %entry
  %sum.0.lcssa = phi i32 [ 0, %entry ], [ %sum.2, %if.end14 ]
  ret i32 %sum.0.lcssa

for.body:                                         ; preds = %for.body.preheader, %if.end14
  %indvars.iv = phi i64 [ 0, %for.body.preheader ], [ %indvars.iv.next, %if.end14 ]
  %sum.050 = phi i32 [ 0, %for.body.preheader ], [ %sum.2, %if.end14 ]
  %arrayidx = getelementptr inbounds i32, i32* %row_data, i64 %indvars.iv
  %0 = load i32, i32* %arrayidx, align 4, !tbaa !2
  %arrayidx2 = getelementptr inbounds i32, i32* %y, i64 %indvars.iv
  %1 = load i32, i32* %arrayidx2, align 4, !tbaa !2
  %2 = trunc i64 %indvars.iv to i32
  %call3 = tail call i32 (i8*, ...) @printf(i8* nonnull dereferenceable(1) getelementptr inbounds ([29 x i8], [29 x i8]* @.str.1, i64 0, i64 0), i32 %2, i32 %0, i32 %2, i32 %1)
  %3 = load i32, i32* %arrayidx2, align 4, !tbaa !2
  %.off = add i32 %3, -1
  %switch = icmp ult i32 %.off, 2
  br i1 %switch, label %if.end14.sink.split, label %if.end14

if.end14.sink.split:                              ; preds = %for.body
  %4 = load i32, i32* %arrayidx, align 4, !tbaa !2
  %add.i = add nsw i32 %4, 1
  %add = add nsw i32 %add.i, %sum.050
  br label %if.end14

if.end14:                                         ; preds = %for.body, %if.end14.sink.split
  %sum.2 = phi i32 [ %sum.050, %for.body ], [ %add, %if.end14.sink.split ]
  %call15 = tail call i32 (i8*, ...) @printf(i8* nonnull dereferenceable(1) getelementptr inbounds ([8 x i8], [8 x i8]* @.str.2, i64 0, i64 0), i32 %sum.2)
  %indvars.iv.next = add nuw nsw i64 %indvars.iv, 1
  %exitcond.not = icmp eq i64 %indvars.iv.next, %wide.trip.count
  br i1 %exitcond.not, label %for.cond.cleanup, label %for.body, !llvm.loop !6
}

; Function Attrs: nofree nounwind
declare dso_local noundef i32 @printf(i8* nocapture noundef readonly, ...) local_unnamed_addr #2

attributes #0 = { norecurse nounwind readonly uwtable willreturn mustprogress "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "min-legal-vector-width"="0" "no-infs-fp-math"="false" "no-jump-tables"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #1 = { nofree noinline nounwind uwtable mustprogress "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "min-legal-vector-width"="0" "no-infs-fp-math"="false" "no-jump-tables"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #2 = { nofree nounwind "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "no-infs-fp-math"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }

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
