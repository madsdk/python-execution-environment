#!/bin/sh

SRCDIR=../src/

# Check for version number
if [ "$1" = "" ]; then
	echo "Usage: build.sh version_number";
	exit 1
else
	VERSION="$1"
fi
BUILDDIR=scavenger_daemon-$VERSION

# Create the $BUILDDIR dir
if [ -d $BUILDDIR ]; then 
	rm -rf $BUILDDIR;
fi
mkdir -p $BUILDDIR

# Copy the needed files into the builddir.
cp -r $SRCDIR/* $BUILDDIR/
find $BUILDDIR -name ".git" | xargs rm -rf
find $BUILDDIR -name ".gitignore" | xargs rm 
find $BUILDDIR -name ".pyc" | xargs rm
rm -rf $BUILDDIR/datastore/*
rm -f $BUILDDIR/scavenger.ini 
rm -rf $BUILDDIR/pexecenv/tasks

# Create the archive.
tar cfvz $BUILDDIR.tar.gz $BUILDDIR

exit 0
