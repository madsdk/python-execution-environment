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
the outside and relays them to the prisoners within the jail (the tasks)."""

from scheduler import Scheduler
from registry import TaskRegistry
from validator import Validator, ValidationError
from monkey import monkey_header
from eipc import EIPCProcess
import logging

class Jailor(EIPCProcess):
    """
    This class manages the communication between the execution environment
    and the outside world.
    """
    
    def __init__(self, pipe, cores, basedir = 'pexecenv', debug = False):
        """
        Constructor.
        @type pipe: EIPC
        @param pipe: The pipe used for IPC.
        @type cores: int
        @param cores: The number of cores/cpu to utilize when scheduling.
        @type basedir: str
        @param basedir: The base directory where task code is stored. 
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
        self.registry = TaskRegistry(basedir)
        self.scheduler = Scheduler(self, cores)

        # Register functions for IPC.
        self.register_function(self.perform_task)
        self.register_function(self.task_exists)
        self.register_function(self.install_task)
        self.register_function(self.fetch_task_code)

        self.__logger.info('Jailor initialized.')
    
    def perform_task(self, task_name, task_input):
        """
        Starts performing a named task on behalf of the client.
        @type task_name: str
        @param task_name: The task identifier.
        @type task_input: dict (kwargs), tuple (pos args), or any (single argument).
        @param task_input: The input for the given task.
        @rtype: int
        @return: The execution id of the scheduled task.
        """        
        # Check that the task exists.
        if not self.registry.has_task(task_name):
            self.__logger.info('Call to non-existing task %s'%task_name)
            raise Exception('The named task does not exist.')
        
        # Now start performing the task.
        execid = self.scheduler.schedule(task_name, task_input)
        self.__logger.info('%s scheduled with execid=%i.'%(task_name, execid))
        return execid
    
    def task_exists(self, task_name):
        """
        Checks whether a given task exists.
        @type task_name: str
        @param task_name: The task identifier.
        """
        # Ask the registry whether or not the task is installed.
        return self.registry.has_task(task_name)
        
    def task_callback(self, execution_id, status, args):
        """
        Entry point for task callbacks. This is called by tasks 
        upon completion or when an error occurs.
        @type execution_id: int
        @param execution_id: The id of the task execution. This is used on 
        the client side to identify the responding task.
        @type status: str
        @param status: The status of the execution. This is: 'DONE' if the task 
        has finished its execution, 'ERROR' if an error has occurred, and 'STATUS' if 
        the task is simply returning some status information about its execution.
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
                # The task has finished its execution. Return its output to 
                # the client.
                try:
                    self._ipc.task_callback('RESULT', execution_id, args['output'])
                except Exception, excep:
                    self.__logger.exception('Error returning result.')
                    self._ipc.task_callback('ERROR', execution_id, 'Error returning result: %s'%excep.message)
            elif status == 'ERROR':
                # The task has encountered an error. Return the 
                # error message to the client.
                self._ipc.task_callback('ERROR', execution_id, args['error'])
            elif status == 'STATUS':
                # The task is relaying status information about its
                # execution.
                self._ipc.task_callback('STATUS', execution_id, args['message'])
            else:
                # Unknown status - this should not happen.
                raise ValueError('Unknown status (%s)'%status)
        except Exception:
            self.__logger.exception('Callback error encountered.')
                
    def install_task(self, task_name, task_code):
        """
        Installs new task code in the execution environment.
        @type task_name: str
        @param task_name: The name of the task. This name must be on 
        the form name1.name2.name3, e.g., daimi.imaging.scale
        @type task_code: str
        @param task_code: The code of the task. The code will be validated
        by the Locusts code validator and thus must adhere to a lot of different 
        rules.
        @raise Exception: Raised if the code fails to validate.  
        """
        # Check the validity of the task name.
        if not TaskRegistry.valid_task_name(task_name):
            self.__logger.info('task with invalid name given (%s)'%task_name)
            raise Exception('Invalid task name.')
        
        # Check that the task is not already installed.
        if self.registry.has_task(task_name):
            self.__logger.info('Attempt to re-install task.')
            raise Exception('task %s already installed.'%task_name)
        
        # Avoid malicious attempts to push __init__.py this way...
        if task_name[-8:] == '__init__':
            self.__logger.info('Attempt to hack by pushing __init__.py')
            raise Exception('Stop trying to hack me!')
        
        # Validate the code.
        v = Validator(task_code)
        try:
            v.validate()
        except ValidationError, error:
            self.__logger.info('Validation error: %s'%error.message)
            raise Exception(error.message)
        
        # Install the code via the registry.
        try:
            self.registry.install_task(task_name, monkey_header+task_code)
            self.__logger.info('Installed task %s'%task_name)
        except Exception, error:
            self.__logger.exception('Error installing valid task code.')
            raise Exception('Error writing task code onto disk. msg=%s'%error.message)
    
    def fetch_task_code(self, task_name):
        """
        Fetches the code of a given task.
        @type task_name: str
        @param task_name: The name of the task.
        @rtype: str
        @return: The task code as a string.
        @raise Exception: Raised if the task can not be found, or if the name is invalid.
        """
        # Check that the task is in fact installed.
        if not self.registry.has_task(task_name):
            raise Exception('task %s is not installed.'%task_name)
        
        # Fetch the code.
        return self.registry.fetch_task_code(task_name)
        
    def shutdown(self):
        self.scheduler.stop()
        self.terminate()
