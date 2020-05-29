%global debug_package %{nil}
%global __strip /bin/true

Name:           yig-restore
Version:        %{version}
Release:        %{release}

Summary:	yig-restore is a go module of storageclass GLACIER for yig

Source:		%{name}_%{version}-%{release}_linux_amd64.tar.gz
Group:		YIG
License:        Apache-2.0
URL:		http://github.com/fireworkmarks/yig-restore
BuildRoot:	%(mktemp -ud %{_tmppath}/%{name}_%{version}-%{release}-XXXXXX)


%description


%prep
%setup -q -n %{name}_%{version}-%{release}_linux_amd64


%install
rm -rf %{buildroot}
install -D -m 755 yig-restore %{buildroot}%{_bindir}/yig-restore
install -D -m 644 restore.logrotate %{buildroot}/etc/logrotate.d/restore.logrotate
install -D -m 644 yig-restore.service %{buildroot}/usr/lib/systemd/system/yig-restore.service
install -D -m 644 yig-restore.toml %{buildroot}%{_sysconfdir}/yig/yig-restore.toml
install -D -m 644 snappy_restore_plugin.so %{buildroot}%{_sysconfdir}/yig/plugins/snappy_restore_plugin.so
install -d %{buildroot}/var/log/yig/

%post
systemctl enable yig-restore

%preun

%clean
rm -rf %{buildroot}

%files
%defattr(-,root,root,-)
%config(noreplace) /etc/yig/yig-restore.toml
/usr/bin/yig-restore
/etc/logrotate.d/restore.logrotate
/etc/yig/plugins/snappy_restore_plugin.so
%dir /var/log/yig/
/usr/lib/systemd/system/yig-restore.service


%changelog
