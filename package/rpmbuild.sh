#!/bin/bash

PACKAGENAME=$1
echo Building RPMs..
GITROOT=`git rev-parse --show-toplevel`
cd $GITROOT
VER=1.1
echo "Git get full depth..."
git fetch --unshallow
REL=`git rev-parse --short HEAD`git
REL=`git log --oneline|wc -l`.$REL
BUILDROOT=$2
RPMTOPDIR=$GITROOT/$BUILDROOT
echo "Ver: $VER, Release: $REL"


rm -rf $RPMTOPDIR
# Create tarball
mkdir -p $RPMTOPDIR/{SOURCES,SPECS}
git archive --format=tar --prefix=${PACKAGENAME}/ HEAD | gzip -c > $RPMTOPDIR/SOURCES/${PACKAGENAME}-${VER}-${REL}.tar.gz

# Convert git log to RPM's ChangeLog format (shown with rpm -qp --changelog <rpm file>)
sed -e "s/%{ver}/$VER/" -e "s/%{rel}/$REL/" $GITROOT/package/${PACKAGENAME}.spec > $RPMTOPDIR/SPECS/${PACKAGENAME}.spec
git log -n 10 --format="* %cd %aN%n- (%h) %s%d%n" --date=local | sed -r 's/[0-9]+:[0-9]+:[0-9]+ //' >> $RPMTOPDIR/SPECS/${PACKAGENAME}.spec
# Build SRC and binary RPMs
rpmbuild \
--define "_topdir $RPMTOPDIR" \
--define "_rpmdir $PWD" \
--define "_srcrpmdir $PWD" \
--define '_rpmfilename %%{NAME}-%%{VERSION}-%%{RELEASE}.%%{ARCH}.rpm' \
-ba $RPMTOPDIR/SPECS/${PACKAGENAME}.spec

