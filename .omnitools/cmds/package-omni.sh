#!/usr/bin/env sh
# shellcheck disable=SC2059
# help: package omni-runtime
# flags:
# --deb | optional (default). builds debian package
# --rpm | optional. builds rpm package

set -e

_info "Packaging omni-runtime c++ code"

FLAG_deb="${FLAG_deb:-False}"
FLAG_rpm="${FLAG_rpm:-False}"

if [ "$FLAG_deb" = "False" ] && [ "$FLAG_rpm" = "False" ]; then
  _debug "Packaging debian package by default"
  FLAG_deb=True
fi

if [ ! -f "build/opt/lib/libomni_runtime.so" ]; then
  _error "Can't find omni-runtme libraries, try compiling first with build.sh"
  exit 1
fi

commit=$(git rev-parse HEAD)
commit=$(echo $commit | head -c 7)

pkgFolder="build/omni-runtime_$commit"
if [ ! -d "$pkgFolder" ]; then
  mkdir -p "$pkgFolder"
fi

if [ "$FLAG_deb" = "True" ]; then
  _info "Building deb package for omni-rintime version $commit"
  cp -r .omnitools/packages/debian "$pkgFolder/DEBIAN"
  cp -r build/opt "$pkgFolder"
  dpkg-deb --build --root-owner-group "$pkgFolder"
  _info "Created debian package $pkgFolder.deb"
fi

if [ "$FLAG_rpm" = "True" ]; then
  _info "Building rpm package for omni-rintime version $commit"
  rpmBldRoot="build/rpmbuild"
  arch=$(uname -m)
  rpmBldLibsFolder="$rpmBldRoot/BUILDROOT/omni-runtime-0-1.$arch"
  if [ ! -d "$rpmBldLibsFolder" ]; then
    mkdir -p "$rpmBldLibsFolder"
  fi
  cp -r .omnitools/packages/rpmbuild/SPECS "$rpmBldRoot"
  cp -r build/opt "$rpmBldLibsFolder"
  rpmbuild --define "_topdir `pwd`/$rpmBldRoot" -v -bb "$rpmBldRoot/SPECS/omni-runtime.spec"
  cp "$rpmBldRoot/RPMS/$arch/omni-runtime-0-1.$arch.rpm" "$pkgFolder.rpm"
  _info "Created rpm package $pkgFolder.rpm"
fi

