Name:           journalpump
Version:        %{major_version}
Release:        %{minor_version}.golang%{?dist}
Url:            http://github.com/aiven/journalpump
Summary:        Pump systemd journal messages to Elasticsearch, Kafka, Logplex or CloudWatch
License:        ASL 2.0
Source0:        journalpump-rpm-src.tar.gz
# The sd-journal reader dlopens libsystemd.so.0 at runtime, which the RPM
# dependency generator cannot see; require the soname provide explicitly
# (rather than the systemd-libs package, which rpmlint rejects as an explicit
# library dependency).  The soname provide is arch-coloured "(64bit)", which
# is wrong on 32-bit targets, so the build is constrained to the 64-bit
# architectures the cgo journal reader is built and tested for.
Requires:       libsystemd.so.0()(64bit)
ExclusiveArch:  x86_64 aarch64
BuildRequires:  golang
BuildRequires:  gcc
Obsoletes:      kafkajournalpump

%description
journalpump is a daemon that takes log messages from journald and pumps them
to a given output.  Currently supported outputs are Elasticsearch, Kafka,
Websocket, logplex, AWS CloudWatch and Google Cloud Logging.  It reads
messages from journald and optionally checks if they match a config rule and
forwards them as JSON messages to the desired output.


%prep
%setup -q -n journalpump


%build
# The Go toolchain needs a writable module cache and build cache; defaulting
# them under the build directory keeps the rpmbuild hermetic-ish and avoids
# depending on the invoking user's HOME.  Modules are fetched from the network
# at build time, so this is a maintainer build, not a network-isolated one.
#
# The distro Go may be older than the go.mod directive (Fedora currently ships
# 1.26 and sets GOTOOLCHAIN=local in its go.env); GOTOOLCHAIN=auto lets the
# build fetch the pinned toolchain instead of failing on the version mismatch.
export GOPATH=%{_builddir}/gopath
export GOCACHE=%{_builddir}/gocache
export GOTOOLCHAIN=auto
make build-go VERSION=%{version}-%{release}


%install
%{__install} -Dm0755 build/journalpump %{buildroot}%{_bindir}/journalpump
%{__install} -Dm0644 journalpump.unit %{buildroot}%{_unitdir}/journalpump.service
# Ship a minimal, safe default config so `systemctl start journalpump` works
# zero-config.  It configures no readers or senders, so the daemon starts and
# idles without emitting anything; operators opt in by editing this file.
%{__install} -Dm0644 journalpump.default.json %{buildroot}%{_localstatedir}/lib/journalpump/journalpump.json


%files
%defattr(-,root,root,-)
%doc LICENSE README.rst journalpump.json
%{_bindir}/journalpump
%{_unitdir}/journalpump.service
%dir %{_localstatedir}/lib/journalpump
%config(noreplace) %{_localstatedir}/lib/journalpump/journalpump.json


%changelog
* Fri Mar 25 2016 Oskari Saarenmaa <os@aiven.io> - 1.0.1
- Renamed to journalpump
- We're Python 3 only now

* Mon Jul 27 2015 Hannu Valtonen <hannu.valtonen@ohmu.fi> - 1.0.0
- Initial RPM package spec