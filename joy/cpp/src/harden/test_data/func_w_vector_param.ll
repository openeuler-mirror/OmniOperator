; ModuleID = 'func_w_vector_param.cpp'
source_filename = "func_w_vector_param.cpp"
target datalayout = "e-m:e-p270:32:32-p271:32:32-p272:64:64-i64:64-f80:128-n8:16:32:64-S128"
target triple = "x86_64-pc-linux-gnu"

%"class.std::vector" = type { %"struct.std::_Vector_base" }
%"struct.std::_Vector_base" = type { %"struct.std::_Vector_base<int, std::allocator<int>>::_Vector_impl" }
%"struct.std::_Vector_base<int, std::allocator<int>>::_Vector_impl" = type { %"struct.std::_Vector_base<int, std::allocator<int>>::_Vector_impl_data" }
%"struct.std::_Vector_base<int, std::allocator<int>>::_Vector_impl_data" = type { i32*, i32*, i32* }

@.str = private unnamed_addr constant [28 x i8] c"processing vector size: %d\0A\00", align 1
@.str.1 = private unnamed_addr constant [15 x i8] c"vector[%d]=%d\0A\00", align 1

; Function Attrs: nofree noinline nounwind uwtable mustprogress
define dso_local void @_Z7processSt6vectorIiSaIiEE(%"class.std::vector"* nocapture readonly %vector) local_unnamed_addr #0 {
entry:
  %_M_finish.i = getelementptr inbounds %"class.std::vector", %"class.std::vector"* %vector, i64 0, i32 0, i32 0, i32 0, i32 1
  %0 = load i32*, i32** %_M_finish.i, align 8, !tbaa !2
  %_M_start.i = getelementptr inbounds %"class.std::vector", %"class.std::vector"* %vector, i64 0, i32 0, i32 0, i32 0, i32 0
  %1 = load i32*, i32** %_M_start.i, align 8, !tbaa !7
  %sub.ptr.lhs.cast.i = ptrtoint i32* %0 to i64
  %sub.ptr.rhs.cast.i = ptrtoint i32* %1 to i64
  %sub.ptr.sub.i = sub i64 %sub.ptr.lhs.cast.i, %sub.ptr.rhs.cast.i
  %sub.ptr.div.i = ashr exact i64 %sub.ptr.sub.i, 2
  %call1 = tail call i32 (i8*, ...) @printf(i8* nonnull dereferenceable(1) getelementptr inbounds ([28 x i8], [28 x i8]* @.str, i64 0, i64 0), i64 %sub.ptr.div.i)
  %2 = load i32*, i32** %_M_finish.i, align 8, !tbaa !2
  %3 = load i32*, i32** %_M_start.i, align 8, !tbaa !7
  %cmp22.not = icmp eq i32* %2, %3
  br i1 %cmp22.not, label %for.cond.cleanup, label %for.body

for.cond.cleanup:                                 ; preds = %for.body, %entry
  ret void

for.body:                                         ; preds = %entry, %for.body
  %indvars.iv = phi i64 [ %indvars.iv.next, %for.body ], [ 0, %entry ]
  %4 = phi i32* [ %8, %for.body ], [ %3, %entry ]
  %add.ptr.i = getelementptr inbounds i32, i32* %4, i64 %indvars.iv
  %5 = load i32, i32* %add.ptr.i, align 4, !tbaa !8
  %6 = trunc i64 %indvars.iv to i32
  %call5 = tail call i32 (i8*, ...) @printf(i8* nonnull dereferenceable(1) getelementptr inbounds ([15 x i8], [15 x i8]* @.str.1, i64 0, i64 0), i32 %6, i32 %5)
  %indvars.iv.next = add nuw i64 %indvars.iv, 1
  %7 = load i32*, i32** %_M_finish.i, align 8, !tbaa !2
  %8 = load i32*, i32** %_M_start.i, align 8, !tbaa !7
  %sub.ptr.lhs.cast.i14 = ptrtoint i32* %7 to i64
  %sub.ptr.rhs.cast.i15 = ptrtoint i32* %8 to i64
  %sub.ptr.sub.i16 = sub i64 %sub.ptr.lhs.cast.i14, %sub.ptr.rhs.cast.i15
  %sub.ptr.div.i17 = ashr exact i64 %sub.ptr.sub.i16, 2
  %cmp = icmp ugt i64 %sub.ptr.div.i17, %indvars.iv.next
  br i1 %cmp, label %for.body, label %for.cond.cleanup, !llvm.loop !10
}

; Function Attrs: nofree nounwind
declare dso_local noundef i32 @printf(i8* nocapture noundef readonly, ...) local_unnamed_addr #1

attributes #0 = { nofree noinline nounwind uwtable mustprogress "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "min-legal-vector-width"="0" "no-infs-fp-math"="false" "no-jump-tables"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #1 = { nofree nounwind "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "no-infs-fp-math"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }

!llvm.module.flags = !{!0}
!llvm.ident = !{!1}

!0 = !{i32 1, !"wchar_size", i32 4}
!1 = !{!"Ubuntu clang version 12.0.0-++20210220023022+2f74c2204827-1~exp1~20210220013739.38"}
!2 = !{!3, !4, i64 8}
!3 = !{!"_ZTSNSt12_Vector_baseIiSaIiEE17_Vector_impl_dataE", !4, i64 0, !4, i64 8, !4, i64 16}
!4 = !{!"any pointer", !5, i64 0}
!5 = !{!"omnipotent char", !6, i64 0}
!6 = !{!"Simple C++ TBAA"}
!7 = !{!3, !4, i64 0}
!8 = !{!9, !9, i64 0}
!9 = !{!"int", !5, i64 0}
!10 = distinct !{!10, !11}
!11 = !{!"llvm.loop.mustprogress"}
