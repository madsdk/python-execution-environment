# Copyright (C) 2008, Mads D. Kristensen
# 
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""
This executable script starts the Scavenger daemon.
"""

import sys
from frontends.dynamic import DynamicSurrogate
from frontends.static import StaticSurrogate
from frontends import Config
import logging

def main():
    # Read in the configuration file.
    config = Config("scavenger.ini")

    # Check command line arguments.
    # Check for the existence of the -d debug flag.
    debug = False
    if '-d' in sys.argv:
        debug = True
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s - %(levelname)s: %(message)s\n'\
                                   '\t%(filename)s, %(funcName)s, %(lineno)s',
                            datefmt='%m/%d/%y %H:%M:%S')
    else:
        logging.basicConfig(level=logging.ERROR,
                            format='%(asctime)s - %(levelname)s: %(message)s',
                            datefmt='%m/%d/%y %H:%M:%S')
    logger = logging.getLogger('Scavenger (main.py)')
    
    # Check whether a number of cores to use have been specified (the -c option).
    # Using this option overrides what is in the config file.
    if '-c' in sys.argv:
        index = sys.argv.index('-c')
        try:
            config.set('cpu', 'cores', sys.argv[index+1])
        except:
            logger.fatal('Invalid command line argument, cores', exc_info=True)
            sys.exit(1)
        
    # Create a Scavenger instance.
    try:
        if '-s' in sys.argv:
            scavenger = StaticSurrogate(debug_jail=debug)
        else:
            scavenger = DynamicSurrogate(debug_jail=debug)
    except:
        logger.exception('Error creating Scavenger instance.')
        sys.exit(1)

    try:               
        # Serve the RPC thingy...
        cores = config.getint('cpu', 'cores')
        print 'Scavenger daemon started (using %i core%s)'%(cores, "" if cores == 1 else "s")
        scavenger.serve()
    except KeyboardInterrupt:
        print 'Interrupted by user.'
    finally:
        scavenger.shutdown()
    
    sys.exit(0)

if __name__ == '__main__':
    main()    
