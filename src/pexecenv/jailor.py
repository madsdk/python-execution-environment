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

"""The jailor is responsible for managing the jail. He takes orders from 
the outside and relays them to the prisoners within the jail (the services)."""

from scheduler import Scheduler
from registry import ServiceRegistry
from validator import Validator, ValidationError
from monkey import monkey_header
from eipc import EIPCProcess
import logging
import re

class Jailor(EIPCProcess):
    """
    This class manages the communication between the execution environment
    and the outside world.
    """
    
    TASK_NAME_RE = re.compile('\w+\.\w+\.\w+')

    def __init__(self, pipe, cores, debug = False):
        """
        Constructor.
        @type pipe: EIPC
        @param address: The pipe used for IPC.
        @type cores: int
        @param cores: The number of cores/cpu to utilize when scheduling.
        """
        # Initialize super class.
        super(Jailor, self).__init__(pipe)
        
        # Set up logging.
        if debug:
            logging.basicConfig(level=logging.DEBUG,
                                format='%(asctime)s - %(levelname)s: %(message)s\n'\
                                    '\t%(filename)s, %(funcName)s, %(lineno)s',
                                datefmt='%m/%d/%y %H:%M:%S')
        else:
            logging.basicConfig(level=logging.ERROR,
                                format='%(asctime)s - %(levelname)s: %(message)s',
                                datefmt='%m/%d/%y %H:%M:%S')


        # Register a logger.
        self.__logger = logging.getLogger('jailor')

        # Create the scheduler and registry.
        self.registry = ServiceRegistry()
        self.scheduler = Scheduler(self, cores)

        # Register functions for IPC.
        self.register_function(self.perform_service)
        self.register_function(self.service_exists)
        self.register_function(self.install_service)
        self.register_function(self.fetch_service_code)

        self.__logger.info('Jailor initialized.')

    @classmethod
    def valid_task_name(cls, task_name):
        """
        Checks whether a service name is valid.
        @type task_name: str
        @param task_name: The service name to check for validity.
        @rtype: bool
        @return: Whether or not the service name adheres to the naming convention.
        """
        return cls.TASK_NAME_RE.match(task_name) != None
    
    def perform_service(self, task_name, task_input):
        """
        Starts performing a named service on behalf of the client.
        @type task_name: str
        @param task_name: The service identifier.
        @type task_input: dict (kwargs), tuple (pos args), or any (single argument).
        @param task_input: The input for the given task.
        @rtype: int
        @return: The execution id of the scheduled task.
        """
        # Check the service name.
        if not Jailor.valid_task_name(task_name):
            self.__logger.info('Invalid service name %s used.'%task_name)
            raise Exception('Invalid service name given.')
        
        # Check that the service exists.
        if not self.registry.has_service(task_name):
            self.__logger.info('Call to non-existing service %s'%task_name)
            raise Exception('The named service does not exist.')
        
        # Now start performing the service.
        execid = self.scheduler.schedule(task_name, task_input)
        self.__logger.info('%s scheduled with execid=%i.'%(task_name, execid))
        return execid
    
    def service_exists(self, service_name):
        """
        Checks whether a given service exists.
        @type service_name: str
        @param service_name: The service identifier.
        """
        # Check the service name.
        if not Jailor.valid_service_name(service_name):
            self.__logger.info('Invalid service name %s used.'%service_name)
            raise Exception('Invalid service name given.')

        # Ask the registry whether or not the service is installed.
        return self.registry.has_service(service_name)
        
    def service_callback(self, execution_id, status, args):
        """
        Entry point for service callbacks. This is called by services 
        upon completion or when an error occurs.
        @type execution_id: int
        @param execution_id: The id of the service execution. This is used on 
        the client side to identify the responding service.
        @type status: str
        @param status: The status of the execution. This is: 'DONE' if the service 
        has finished its execution, 'ERROR' if an error has occurred, and 'STATUS' if 
        the service is simply returning some status information about its execution.
        @type args: dict
        @param args: Keyword-based arguments. Depending on the value of the 
        status parameter different keyword arguments are expected. 
        """
        # Log the event.
        self.__logger.info('Callback: execid=%i, status=%s'%(execution_id, status))
    
        # Handle the callback.    
        try:
            # Switch out based on status.
            if status == 'DONE':
                # The service has finished its execution. Return its output to 
                # the client.
                try:
                    self._ipc.service_callback('RESULT', execution_id, args['output'])
                except Exception, excep:
                    self.__logger.exception('Error returning result.')
                    self._ipc.service_callback('ERROR', execution_id, 'Error returning result: %s'%excep.message)
            elif status == 'ERROR':
                # The service has encountered an error. Return the 
                # error message to the client.
                self._ipc.service_callback('ERROR', execution_id, args['error'])
            elif status == 'STATUS':
                # The service is relaying status information about its
                # execution.
                self._ipc.service_callback('STATUS', execution_id, args['message'])
            else:
                # Unknown status - this should not happen.
                raise ValueError('Unknown status (%s)'%status)
        except Exception:
            self.__logger.exception('Callback error encountered.')
                
    def install_service(self, service_name, service_code):
        """
        Installs new service code in the execution environment.
        @type service_name: str
        @param service_name: The name of the service. This name must be on 
        the form name1.name2.name3, e.g., daimi.imaging.scale
        @type service_code: str
        @param service_code: The code of the service. The code will be validated
        by the Locusts code validator and thus must adhere to a lot of different 
        rules.
        @raise Exception: Raised if the code fails to validate.  
        """
        # Check the validity of the service name.
        if not Jailor.valid_service_name(service_name):
            self.__logger.info('Service with invalid name given (%s)'%service_name)
            raise Exception('Invalid service name.')
        
        # Check that the service is not already installed.
        if self.registry.has_service(service_name):
            self.__logger.info('Attempt to re-install service.')
            raise Exception('Service %s already installed.'%service_name)
        
        # Avoid malicious attempts to push __init__.py this way...
        if service_name[-8:] == '__init__':
            self.__logger.info('Attempt to hack by pushing __init__.py')
            raise Exception('Stop trying to hack me!')
        
        # Validate the code.
        v = Validator(service_code)
        try:
            v.validate()
        except ValidationError, error:
            self.__logger.info('Validation error: %s'%error.message)
            raise Exception(error.message)
        
        # Install the code via the registry.
        try:
            self.registry.install_service(service_name, monkey_header+service_code)
            self.__logger.info('Installed service %s'%service_name)
        except Exception, error:
            self.__logger.exception('Error installing valid service code.')
            raise Exception('Error writing service code onto disk. msg=%s'%error.message)
    
    def fetch_service_code(self, service_name):
        """
        Fetches the code of a given service.
        @type service_name: str
        @param service_name: The name of the service.
        @rtype: str
        @return: The service code as a string.
        @raise Exception: Raised if the service can not be found, or if the name is invalid.
        """
        # Check the validity of the service name.
        if not Jailor.valid_service_name(service_name):
            self.__logger.info('Service with invalid name given (%s)'%service_name)
            raise Exception('Invalid service name.')
        
        # Check that the service is in fact installed.
        if not self.registry.has_service(service_name):
            raise Exception('Service %s is not installed.'%service_name)
        
        # Fetch the code.
        return self.registry.fetch_service_code(service_name)
        
    def shutdown(self):
        self.scheduler.stop()
        self.terminate()
