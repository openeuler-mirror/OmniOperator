; ModuleID = '/usr/code/olk_dev/omni_runtime_joy/omni-cache/joy/cpp/src/operator/ir/../../memory_pool/memory_pool.cpp'
source_filename = "/usr/code/olk_dev/omni_runtime_joy/omni-cache/joy/cpp/src/operator/ir/../../memory_pool/memory_pool.cpp"
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
%class.MemoryPool = type { i32 (...)** }
%class.BaseMemoryPoolImpl = type { %class.MemoryPool }
%class.JemallocMemoryPool = type { %class.BaseMemoryPoolImpl }

$_ZN10MemoryPoolD2Ev = comdat any

$_ZN18BaseMemoryPoolImplI17JemallocAllocatorE8allocateElPPh = comdat any

$_ZN18BaseMemoryPoolImplI17JemallocAllocatorE7releaseEPh = comdat any

$_ZN18JemallocMemoryPoolD0Ev = comdat any

$_ZN17JemallocAllocator8allocateElPPh = comdat any

$_ZTV18JemallocMemoryPool = comdat any

$_ZTS18JemallocMemoryPool = comdat any

$_ZTS18BaseMemoryPoolImplI17JemallocAllocatorE = comdat any

$_ZTS10MemoryPool = comdat any

$_ZTI10MemoryPool = comdat any

$_ZTI18BaseMemoryPoolImplI17JemallocAllocatorE = comdat any

$_ZTI18JemallocMemoryPool = comdat any

@_ZStL8__ioinit = internal global %"class.std::ios_base::Init" zeroinitializer, align 1
@__dso_handle = external hidden global i8
@_ZTV18JemallocMemoryPool = linkonce_odr dso_local unnamed_addr constant { [6 x i8*] } { [6 x i8*] [i8* null, i8* bitcast ({ i8*, i8*, i8* }* @_ZTI18JemallocMemoryPool to i8*), i8* bitcast (i32 (%class.BaseMemoryPoolImpl*, i64, i8**)* @_ZN18BaseMemoryPoolImplI17JemallocAllocatorE8allocateElPPh to i8*), i8* bitcast (i32 (%class.BaseMemoryPoolImpl*, i8*)* @_ZN18BaseMemoryPoolImplI17JemallocAllocatorE7releaseEPh to i8*), i8* bitcast (void (%class.MemoryPool*)* @_ZN10MemoryPoolD2Ev to i8*), i8* bitcast (void (%class.JemallocMemoryPool*)* @_ZN18JemallocMemoryPoolD0Ev to i8*)] }, comdat, align 8
@_ZL18jemallocMemoryPool = internal global { i8** } { i8** getelementptr inbounds ({ [6 x i8*] }, { [6 x i8*] }* @_ZTV18JemallocMemoryPool, i32 0, inrange i32 0, i32 2) }, align 8
@_ZTVN10__cxxabiv120__si_class_type_infoE = external dso_local global i8*
@_ZTS18JemallocMemoryPool = linkonce_odr dso_local constant [21 x i8] c"18JemallocMemoryPool\00", comdat, align 1
@_ZTS18BaseMemoryPoolImplI17JemallocAllocatorE = linkonce_odr dso_local constant [42 x i8] c"18BaseMemoryPoolImplI17JemallocAllocatorE\00", comdat, align 1
@_ZTVN10__cxxabiv117__class_type_infoE = external dso_local global i8*
@_ZTS10MemoryPool = linkonce_odr dso_local constant [13 x i8] c"10MemoryPool\00", comdat, align 1
@_ZTI10MemoryPool = linkonce_odr dso_local constant { i8*, i8* } { i8* bitcast (i8** getelementptr inbounds (i8*, i8** @_ZTVN10__cxxabiv117__class_type_infoE, i64 2) to i8*), i8* getelementptr inbounds ([13 x i8], [13 x i8]* @_ZTS10MemoryPool, i32 0, i32 0) }, comdat, align 8
@_ZTI18BaseMemoryPoolImplI17JemallocAllocatorE = linkonce_odr dso_local constant { i8*, i8*, i8* } { i8* bitcast (i8** getelementptr inbounds (i8*, i8** @_ZTVN10__cxxabiv120__si_class_type_infoE, i64 2) to i8*), i8* getelementptr inbounds ([42 x i8], [42 x i8]* @_ZTS18BaseMemoryPoolImplI17JemallocAllocatorE, i32 0, i32 0), i8* bitcast ({ i8*, i8* }* @_ZTI10MemoryPool to i8*) }, comdat, align 8
@_ZTI18JemallocMemoryPool = linkonce_odr dso_local constant { i8*, i8*, i8* } { i8* bitcast (i8** getelementptr inbounds (i8*, i8** @_ZTVN10__cxxabiv120__si_class_type_infoE, i64 2) to i8*), i8* getelementptr inbounds ([21 x i8], [21 x i8]* @_ZTS18JemallocMemoryPool, i32 0, i32 0), i8* bitcast ({ i8*, i8*, i8* }* @_ZTI18BaseMemoryPoolImplI17JemallocAllocatorE to i8*) }, comdat, align 8
@_ZSt4cout = external dso_local global %"class.std::basic_ostream", align 8
@.str = private unnamed_addr constant [19 x i8] c"allocate size is 0\00", align 1
@llvm.global_ctors = appending global [1 x { i32, void ()*, i8* }] [{ i32, void ()*, i8* } { i32 65535, void ()* @_GLOBAL__sub_I_memory_pool.cpp, i8* null }]

declare dso_local void @_ZNSt8ios_base4InitC1Ev(%"class.std::ios_base::Init"* nonnull dereferenceable(1)) unnamed_addr #0

; Function Attrs: nounwind
declare dso_local void @_ZNSt8ios_base4InitD1Ev(%"class.std::ios_base::Init"* nonnull dereferenceable(1)) unnamed_addr #1

; Function Attrs: nofree nounwind
declare dso_local i32 @__cxa_atexit(void (i8*)*, i8*, i8*) local_unnamed_addr #2

; Function Attrs: nounwind uwtable willreturn
define linkonce_odr dso_local void @_ZN10MemoryPoolD2Ev(%class.MemoryPool* nonnull dereferenceable(8) %this) unnamed_addr #3 comdat align 2 {
entry:
  ret void
}

; Function Attrs: norecurse nounwind readnone uwtable willreturn mustprogress
define dso_local %class.MemoryPool* @_Z13getMemoryPoolv() local_unnamed_addr #4 {
entry:
  ret %class.MemoryPool* bitcast ({ i8** }* @_ZL18jemallocMemoryPool to %class.MemoryPool*)
}

; Function Attrs: uwtable mustprogress
define dso_local i8* @omni_allocate(i64 %size) local_unnamed_addr #5 {
entry:
  %buf = alloca i8*, align 8
  %0 = bitcast i8** %buf to i8*
  call void @llvm.lifetime.start.p0i8(i64 8, i8* nonnull %0) #15
  %call.i = call i32 @_ZN17JemallocAllocator8allocateElPPh(i64 %size, i8** nonnull %buf)
  %1 = load i8*, i8** %buf, align 8, !tbaa !2
  call void @llvm.lifetime.end.p0i8(i64 8, i8* nonnull %0) #15
  ret i8* %1
}

; Function Attrs: argmemonly nofree nosync nounwind willreturn
declare void @llvm.lifetime.start.p0i8(i64 immarg, i8* nocapture) #6

; Function Attrs: uwtable mustprogress
define linkonce_odr dso_local i32 @_ZN18BaseMemoryPoolImplI17JemallocAllocatorE8allocateElPPh(%class.BaseMemoryPoolImpl* nonnull dereferenceable(8) %this, i64 %size, i8** %buffer) unnamed_addr #5 comdat align 2 {
entry:
  %call = tail call i32 @_ZN17JemallocAllocator8allocateElPPh(i64 %size, i8** %buffer)
  ret i32 0
}

; Function Attrs: argmemonly nofree nosync nounwind willreturn
declare void @llvm.lifetime.end.p0i8(i64 immarg, i8* nocapture) #6

; Function Attrs: nounwind uwtable willreturn mustprogress
define dso_local void @omni_release(i64 %address) local_unnamed_addr #7 {
entry:
  %0 = inttoptr i64 %address to i8*
  tail call void @free(i8* %0) #15
  ret void
}

; Function Attrs: uwtable willreturn mustprogress
define linkonce_odr dso_local i32 @_ZN18BaseMemoryPoolImplI17JemallocAllocatorE7releaseEPh(%class.BaseMemoryPoolImpl* nonnull dereferenceable(8) %this, i8* %buffer) unnamed_addr #8 comdat align 2 {
entry:
  tail call void @free(i8* %buffer) #15
  ret i32 0
}

; Function Attrs: inlinehint nounwind uwtable
define linkonce_odr dso_local void @_ZN18JemallocMemoryPoolD0Ev(%class.JemallocMemoryPool* nonnull dereferenceable(8) %this) unnamed_addr #9 comdat align 2 {
entry:
  %0 = bitcast %class.JemallocMemoryPool* %this to i8*
  tail call void @_ZdlPv(i8* nonnull %0) #16
  ret void
}

; Function Attrs: uwtable mustprogress
define linkonce_odr dso_local i32 @_ZN17JemallocAllocator8allocateElPPh(i64 %size, i8** %buffer) local_unnamed_addr #5 comdat align 2 {
entry:
  %cmp = icmp eq i64 %size, 0
  br i1 %cmp, label %if.then, label %if.end

if.then:                                          ; preds = %entry
  %call1.i = tail call nonnull align 8 dereferenceable(8) %"class.std::basic_ostream"* @_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l(%"class.std::basic_ostream"* nonnull align 8 dereferenceable(8) @_ZSt4cout, i8* nonnull getelementptr inbounds ([19 x i8], [19 x i8]* @.str, i64 0, i64 0), i64 18)
  %vtable.i = load i8*, i8** bitcast (%"class.std::basic_ostream"* @_ZSt4cout to i8**), align 8, !tbaa !6
  %vbase.offset.ptr.i = getelementptr i8, i8* %vtable.i, i64 -24
  %0 = bitcast i8* %vbase.offset.ptr.i to i64*
  %vbase.offset.i = load i64, i64* %0, align 8
  %add.ptr.i = getelementptr inbounds i8, i8* bitcast (%"class.std::basic_ostream"* @_ZSt4cout to i8*), i64 %vbase.offset.i
  %_M_ctype.i = getelementptr inbounds i8, i8* %add.ptr.i, i64 240
  %1 = bitcast i8* %_M_ctype.i to %"class.std::ctype"**
  %2 = load %"class.std::ctype"*, %"class.std::ctype"** %1, align 8, !tbaa !8
  %tobool.not.i9 = icmp eq %"class.std::ctype"* %2, null
  br i1 %tobool.not.i9, label %if.then.i10, label %_ZSt13__check_facetISt5ctypeIcEERKT_PS3_.exit

if.then.i10:                                      ; preds = %if.then
  tail call void @_ZSt16__throw_bad_castv() #17
  unreachable

_ZSt13__check_facetISt5ctypeIcEERKT_PS3_.exit:    ; preds = %if.then
  %_M_widen_ok.i = getelementptr inbounds %"class.std::ctype", %"class.std::ctype"* %2, i64 0, i32 8
  %3 = load i8, i8* %_M_widen_ok.i, align 8, !tbaa !11
  %tobool.not.i = icmp eq i8 %3, 0
  br i1 %tobool.not.i, label %if.end.i, label %if.then.i

if.then.i:                                        ; preds = %_ZSt13__check_facetISt5ctypeIcEERKT_PS3_.exit
  %arrayidx.i = getelementptr inbounds %"class.std::ctype", %"class.std::ctype"* %2, i64 0, i32 9, i64 10
  %4 = load i8, i8* %arrayidx.i, align 1, !tbaa !13
  br label %_ZNKSt5ctypeIcE5widenEc.exit

if.end.i:                                         ; preds = %_ZSt13__check_facetISt5ctypeIcEERKT_PS3_.exit
  tail call void @_ZNKSt5ctypeIcE13_M_widen_initEv(%"class.std::ctype"* nonnull dereferenceable(570) %2)
  %5 = bitcast %"class.std::ctype"* %2 to i8 (%"class.std::ctype"*, i8)***
  %vtable.i7 = load i8 (%"class.std::ctype"*, i8)**, i8 (%"class.std::ctype"*, i8)*** %5, align 8, !tbaa !6
  %vfn.i = getelementptr inbounds i8 (%"class.std::ctype"*, i8)*, i8 (%"class.std::ctype"*, i8)** %vtable.i7, i64 6
  %6 = load i8 (%"class.std::ctype"*, i8)*, i8 (%"class.std::ctype"*, i8)** %vfn.i, align 8
  %call.i8 = tail call signext i8 %6(%"class.std::ctype"* nonnull dereferenceable(570) %2, i8 signext 10)
  br label %_ZNKSt5ctypeIcE5widenEc.exit

_ZNKSt5ctypeIcE5widenEc.exit:                     ; preds = %if.then.i, %if.end.i
  %retval.0.i = phi i8 [ %4, %if.then.i ], [ %call.i8, %if.end.i ]
  %call1.i5 = tail call nonnull align 8 dereferenceable(8) %"class.std::basic_ostream"* @_ZNSo3putEc(%"class.std::basic_ostream"* nonnull dereferenceable(8) @_ZSt4cout, i8 signext %retval.0.i)
  %call.i = tail call nonnull align 8 dereferenceable(8) %"class.std::basic_ostream"* @_ZNSo5flushEv(%"class.std::basic_ostream"* nonnull dereferenceable(8) %call1.i5)
  br label %return

if.end:                                           ; preds = %entry
  %call2 = tail call noalias i8* @malloc(i64 %size) #18
  store i8* %call2, i8** %buffer, align 8, !tbaa !2
  br label %return

return:                                           ; preds = %if.end, %_ZNKSt5ctypeIcE5widenEc.exit
  %retval.0 = phi i32 [ -1, %_ZNKSt5ctypeIcE5widenEc.exit ], [ 0, %if.end ]
  ret i32 %retval.0
}

; Function Attrs: inaccessiblememonly nofree nounwind willreturn allocsize(0)
declare dso_local noalias noundef i8* @malloc(i64) local_unnamed_addr #10

declare dso_local nonnull align 8 dereferenceable(8) %"class.std::basic_ostream"* @_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l(%"class.std::basic_ostream"* nonnull align 8 dereferenceable(8), i8*, i64) local_unnamed_addr #0

declare dso_local nonnull align 8 dereferenceable(8) %"class.std::basic_ostream"* @_ZNSo3putEc(%"class.std::basic_ostream"* nonnull dereferenceable(8), i8 signext) local_unnamed_addr #0

declare dso_local nonnull align 8 dereferenceable(8) %"class.std::basic_ostream"* @_ZNSo5flushEv(%"class.std::basic_ostream"* nonnull dereferenceable(8)) local_unnamed_addr #0

; Function Attrs: noreturn
declare dso_local void @_ZSt16__throw_bad_castv() local_unnamed_addr #11

declare dso_local void @_ZNKSt5ctypeIcE13_M_widen_initEv(%"class.std::ctype"* nonnull dereferenceable(570)) local_unnamed_addr #0

; Function Attrs: inaccessiblemem_or_argmemonly nounwind willreturn
declare dso_local void @free(i8* nocapture noundef) local_unnamed_addr #12

; Function Attrs: nobuiltin nounwind
declare dso_local void @_ZdlPv(i8*) local_unnamed_addr #13

; Function Attrs: uwtable
define internal void @_GLOBAL__sub_I_memory_pool.cpp() #14 section ".text.startup" {
entry:
  tail call void @_ZNSt8ios_base4InitC1Ev(%"class.std::ios_base::Init"* nonnull dereferenceable(1) @_ZStL8__ioinit)
  %0 = tail call i32 @__cxa_atexit(void (i8*)* bitcast (void (%"class.std::ios_base::Init"*)* @_ZNSt8ios_base4InitD1Ev to void (i8*)*), i8* getelementptr inbounds (%"class.std::ios_base::Init", %"class.std::ios_base::Init"* @_ZStL8__ioinit, i64 0, i32 0), i8* nonnull @__dso_handle) #15
  ret void
}

attributes #0 = { "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "no-infs-fp-math"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #1 = { nounwind "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "no-infs-fp-math"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #2 = { nofree nounwind }
attributes #3 = { nounwind uwtable willreturn "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "min-legal-vector-width"="0" "no-infs-fp-math"="false" "no-jump-tables"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #4 = { norecurse nounwind readnone uwtable willreturn mustprogress "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "min-legal-vector-width"="0" "no-infs-fp-math"="false" "no-jump-tables"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #5 = { uwtable mustprogress "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "min-legal-vector-width"="0" "no-infs-fp-math"="false" "no-jump-tables"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #6 = { argmemonly nofree nosync nounwind willreturn }
attributes #7 = { nounwind uwtable willreturn mustprogress "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "min-legal-vector-width"="0" "no-infs-fp-math"="false" "no-jump-tables"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #8 = { uwtable willreturn mustprogress "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "min-legal-vector-width"="0" "no-infs-fp-math"="false" "no-jump-tables"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #9 = { inlinehint nounwind uwtable "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "min-legal-vector-width"="0" "no-infs-fp-math"="false" "no-jump-tables"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #10 = { inaccessiblememonly nofree nounwind willreturn allocsize(0) "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "no-infs-fp-math"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #11 = { noreturn "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "no-infs-fp-math"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #12 = { inaccessiblemem_or_argmemonly nounwind willreturn "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "no-infs-fp-math"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #13 = { nobuiltin nounwind "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "no-infs-fp-math"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #14 = { uwtable "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "min-legal-vector-width"="0" "no-infs-fp-math"="false" "no-jump-tables"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #15 = { nounwind }
attributes #16 = { builtin nounwind }
attributes #17 = { noreturn }
attributes #18 = { nounwind allocsize(0) }

!llvm.module.flags = !{!0}
!llvm.ident = !{!1}

!0 = !{i32 1, !"wchar_size", i32 4}
!1 = !{!"Ubuntu clang version 12.0.0-++20210225092633+e0e6b1e39e7e-1~exp1~20210225083352.50"}
!2 = !{!3, !3, i64 0}
!3 = !{!"any pointer", !4, i64 0}
!4 = !{!"omnipotent char", !5, i64 0}
!5 = !{!"Simple C++ TBAA"}
!6 = !{!7, !7, i64 0}
!7 = !{!"vtable pointer", !5, i64 0}
!8 = !{!9, !3, i64 240}
!9 = !{!"_ZTSSt9basic_iosIcSt11char_traitsIcEE", !3, i64 216, !4, i64 224, !10, i64 225, !3, i64 232, !3, i64 240, !3, i64 248, !3, i64 256}
!10 = !{!"bool", !4, i64 0}
!11 = !{!12, !4, i64 56}
!12 = !{!"_ZTSSt5ctypeIcE", !3, i64 16, !10, i64 24, !3, i64 32, !3, i64 40, !3, i64 48, !4, i64 56, !4, i64 57, !4, i64 313, !4, i64 569}
!13 = !{!4, !4, i64 0}
