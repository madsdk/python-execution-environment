CORES=1

# Start Presence daemon.
killall -9 presence
./presence -n `hostname` > /dev/null &

# Check command line arguments.
if [ "$1" = "" ]; then 
	$PYTHON main.py dynamic -c $CORES
else if [ "$1" = "dynamic" ]; then
	$PYTHON main.py dynamic -c $CORES
else if [ "$1" = "static" ]; then
	$PYTHON main.py static -c $CORES
else
	echo "Usage: $0 [static|dynamic]";
	killall -9 presence;
	exit 1
fi

# Shut down Presence.
killall -9 presence

exit 0


