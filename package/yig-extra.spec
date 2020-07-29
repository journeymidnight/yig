%global debug_package %{nil}
%global __strip /bin/true

Name:           yig-extra
Version:        %{ver}
Release:        %{rel}%{?dist}

Summary:	Async and timed tasks that are import to yig to function, in the form of Meepo plugins.

Group:		SDS
License:    Apache-2.0
URL:		http://github.com/journeymidnight/yig
Source0:	%{name}-%{version}-%{rel}.tar.gz
BuildRoot:	%(mktemp -ud %{_tmppath}/%{name}-%{version}-%{release}-XXXXXX)
#BuildRequires:
Requires:       libradosstriper1,librados2,meepo

%description


%prep
%setup -q -n %{name}


%build
#The go build still use source code in GOPATH/src/legitlab/yig/
#keep git source tree clean, better ways to build?
#I do not know
make build_extra

%install
rm -rf %{buildroot}
install -D -m 755 delete.so %{buildroot}/etc/meepo/delete.so
install -D -m 644 tools/delete/yig_delete.toml %{buildroot}/etc/meepo/yig_delete.toml
install -D -m 755 migrate.so %{buildroot}/etc/meepo/migrate.so
install -D -m 644 tools/migrate/migrate_scanner.toml %{buildroot}/etc/meepo/migrate_scanner.toml
install -D -m 644 tools/migrate/migrate_worker.toml %{buildroot}/etc/meepo/migrate_worker.toml

%post

%preun

%clean
rm -rf %{buildroot}

%files
%defattr(-,root,root,-)
%config(noreplace) /etc/meepo/yig_delete.toml
%config(noreplace) /etc/meepo/migrate_scanner.toml
%config(noreplace) /etc/meepo/migrate_worker.toml
/etc/meepo/delete.so
/etc/meepo/migrate.so

%changelog
