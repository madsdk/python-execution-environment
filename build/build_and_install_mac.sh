#!/bin/sh

# Constants.
STACKLESS="http://www.stackless.com/binaries/stackless-27-export.tar.bz2"
PEXECENV="git://github.com/madsdk/python-execution-environment.git"
SCRPC="git://github.com/madsdk/python-single-connection-RPC.git"
DATASTORE="git://github.com/madsdk/python-remote-data-store.git"
EIPC="git://github.com/madsdk/python-easy-ipc.git"
PRESENCE="git://github.com/madsdk/presence.git"

# Check for command line arguments.
if [ "$1" = "" ]; then 
	PREFIX="`pwd`"
else
	PREFIX="$1";
fi

# Install Stackless Python - if necessary.
if [ ! -f $PREFIX/bin/python ]; then 
	if [ ! -f stackless-27-export.tar.bz2 ]; then
		wget $STACKLESS;
	fi
	tar xfj stackless-27-export.tar.bz2;
	cd stackless-27-export;
	CC="gcc -arch i386" ./configure --enable-stacklessfewerregisters --prefix=$PREFIX;
	make;
	make install;
	cd ..
fi

# Update or install components.
# Execution env.
x=python-execution-environment
if [ -d $x ]; then
	read -p "Update (pull from master) $x? [y/n] ";
	if [ $REPLY = "y" ]; then
		cd $x; 
		git pull;
		cd ..
	fi;
else
	git clone $PEXECENV;		
fi

cd $x/build;
./build.sh 1.0
cp -r scavenger_daemon-1.0 $PREFIX/scavenger_daemon
echo "#!/bin/sh" > $PREFIX/scavenger_daemon/start_daemon.sh
echo "PYTHON=\"$PREFIX/bin/python\"" >> $PREFIX/scavenger_daemon/start_daemon.sh
cat start_daemon.sh >> $PREFIX/scavenger_daemon/start_daemon.sh 
chmod +x $PREFIX/scavenger_daemon/start_daemon.sh
cd ../..

# SCRPC.
x=python-single-connection-RPC
if [ -d $x ]; then
	read -p "Update (pull from master) $x? [y/n] ";
	if [ $REPLY = "y" ]; then
		cd $x; 
		git pull;
		cd ..
	fi;
else
	git clone $SCRPC;		
fi

cd $x/build;
./build.sh 1.0
cd scrpc-1.0
$PREFIX/bin/python setup.py install
cd ../../..

# data store.
x=python-remote-data-store
if [ -d $x ]; then
	read -p "Update (pull from master) $x? [y/n] ";
	if [ $REPLY = "y" ]; then
		cd $x; 
		git pull;
		cd ..
	fi;
else
	git clone $DATASTORE;
fi

cd $x/build;
./build.sh 1.0
cd datastore-1.0
$PREFIX/bin/python setup.py install
cd ../../..

# eipc.
x=python-easy-ipc
if [ -d $x ]; then
	read -p "Update (pull from master) $x? [y/n] ";
	if [ $REPLY = "y" ]; then
		cd $x; 
		git pull;
		cd ..
	fi;
else
	git clone $EIPC;
fi

cd $x/build;
./build.sh 1.0
cd eipc-1.0
$PREFIX/bin/python setup.py install
cd ../../..

# Presence lib.
x=presence
if [ -d $x ]; then
	read -p "Update (pull from master) $x? [y/n] ";
	if [ $REPLY = "y" ]; then
		cd $x; 
		git pull;
		cd ..
	fi;
else
	git clone $PRESENCE;
fi

cd $x/builds/python_lib;
./build.sh 1.0
cd presence_lib-1.0
$PREFIX/bin/python setup.py install
cd ../../../..

# Presence daemon (requires qt).
if [ ! -f $PREFIX/scavenger_daemon/presence ]; then
	if [ -f /usr/bin/qmake ]; then 
		cd presence/daemon;
		qmake -spec macx-g++;
		make;
		cp qPresence/presence $PREFIX/scavenger_daemon;
		cd ../..
	else
		echo "You need to install Qt in order to build the Presence daemon.";
	fi
fi

exit 0
