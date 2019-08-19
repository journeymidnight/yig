%global debug_package %{nil}
%global __strip /bin/true

Name:           yig	
Version:        %{ver}
Release:        %{rel}%{?dist}

Summary:	Yet Index Gateway is a S3-compatible API server whose backend storage is multiple ceph clusters

Group:		SDS
License:        Apache-2.0
URL:		http://github.com/journeymidnight/yig
Source0:	%{name}-%{version}-%{rel}.tar.gz
BuildRoot:	%(mktemp -ud %{_tmppath}/%{name}-%{version}-%{release}-XXXXXX)
#BuildRequires:  
Requires:       libradosstriper1
Requires:       librados2
Requires:       librdkafka1

%description


%prep
%setup -q -n %{name}


%build
#The go build still use source code in GOPATH/src/legitlab/yig/
#keep git source tree clean, better ways to build?
#I do not know
make build_internal

%install
rm -rf %{buildroot}
install -D -m 755 admin %{buildroot}%{_bindir}/yig_admin
install -D -m 755 delete %{buildroot}%{_bindir}/yig_delete_daemon
install -D -m 755 getrediskeys %{buildroot}%{_bindir}/yig_getrediskeys
install -D -m 755 lc     %{buildroot}%{_bindir}/yig_lifecyle_daemon
install -D -m 755 %{_builddir}/yig/build/bin/yig %{buildroot}%{_bindir}/yig
install -D -m 644 package/yig.logrotate %{buildroot}/etc/logrotate.d/yig.logrotate
install -D -m 644 package/access.logrotate %{buildroot}/etc/logrotate.d/access.logrotate
install -D -m 644 package/yig_delete.logrotate %{buildroot}/etc/logrotate.d/yig_delete.logrotate
install -D -m 644 package/yig_lc.logrotate %{buildroot}/etc/logrotate.d/yig_lc.logrotate
install -D -m 644 package/yig.service   %{buildroot}/usr/lib/systemd/system/yig.service
install -D -m 644 package/yig_delete.service   %{buildroot}/usr/lib/systemd/system/yig_delete.service
install -D -m 644 package/yig_lc.service   %{buildroot}/usr/lib/systemd/system/yig_lc.service
install -D -m 644 conf/yig.toml %{buildroot}%{_sysconfdir}/yig/yig.toml
install -d %{buildroot}/var/log/yig/

#ceph confs ?

%post
systemctl enable yig
systemctl enable yig_delete
systemctl enable yig_lc


%preun

%clean
rm -rf %{buildroot}

%files
%defattr(-,root,root,-)
%config(noreplace) /etc/yig/yig.toml
/usr/bin/yig_admin
/usr/bin/yig
/usr/bin/yig_delete_daemon
/usr/bin/yig_getrediskeys
/usr/bin/yig_lifecyle_daemon
/etc/logrotate.d/yig.logrotate
/etc/logrotate.d/access.logrotate
/etc/logrotate.d/yig_delete.logrotate
/etc/logrotate.d/yig_lc.logrotate
%dir /var/log/yig/
/usr/lib/systemd/system/yig.service
/usr/lib/systemd/system/yig_delete.service
/usr/lib/systemd/system/yig_lc.service


%changelog
