Name: omni-runtime
Version: 0
Release: 1
Summary: RPM package for omni-runtime
License: HUAWEI

#BuildRequires: libc6
#BuildRequires: libgcc-s1
#BuildRequires: libjemalloc2
#BuildRequires: libllvm12
#BuildRequires: libstdc++6

%description
Omni-runtime functionality.

%prep

%build

%install

%files
/opt/lib/libomruntime.so
/opt/lib/ir/aggregator.ll
/opt/lib/ir/hash_groupby.ll
/opt/lib/ir/memory_pool.ll
/opt/lib/ir/sort.ll
/opt/lib/ir/test.ll

%changelog
