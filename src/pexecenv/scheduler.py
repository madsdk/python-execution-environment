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
This file contains the Scheduler class and its helpers.
The scheduler is responsible for scheduling stackless tasklets on (possibly)
multiple threads to make sure that multi-core/cpu machines are properly
utilized. 
"""

from __future__ import with_statement
from corescheduler import CoreScheduler
from eipc import EIPC
import logging

class SchedulerException(Exception):
    """Exception raised by the scheduler."""
    def __init__(self, msg):
        Exception.__init__(self, msg)

class Scheduler(object):
    """
    The master scheduler.
    This class creates a number of processes, where stackless tasklets may be
    executed, and schedules between them when new tasks arrive.
    """
    
    PIPE_CHECK_INTERVAL = 0.01

    def __init__(self, jailor, cores):
        """
        Constructor.
        @type jailor: Jailor
        @param jailor: The Jailor instance controlling this scheduler.
        @type cores: int
        @param cores: The number of cores/CPUs to use.
        """
        super(Scheduler, self).__init__()

        # Check the input.
        if cores <= 0:
            raise ValueError('Invalid number of cores (%i)'%cores)
        
        # Store local members.
        self.__cores = cores
        self.__jailor = jailor
        self.__shutdown = False
        
        # Spawn a thread for each core/cpu.
        self.__schedulers = []
        for i in range(0, cores):
            local_ipc, remote_ipc = EIPC.eipc_pair()
            self.__schedulers.append((CoreScheduler(remote_ipc), local_ipc))
            local_ipc.register_function(self.corescheduler_callback, "callback")
            local_ipc.start()
            self.__schedulers[i][0].start()
            
        # Set state variables.
        self.__execution_id = 0
        self.__next_scheduler = 0
        
        # Get a logger.
        self.__logger = logging.getLogger('scheduler')
        self.__logger.info('%i core scheduler(s) spawned'%cores)
    
    def stop(self):
        """Terminates the core schedulers and discards all tasklets."""
        self.__shutdown = True
        for scheduler, _ in self.__schedulers:
            scheduler.terminate()
    
    def schedule(self, task_name, task_input):
        """
        Add the given task to the scheduler.
        This means that the task will be performed a.s.a.p. on one of the
        available CoreSchedulers.
        @type task_name: str
        @param task_name: The id of the task that is to be performed.
        @type task_input: dict
        @param task_input: The task input.
        @rtype: int
        @return: The id of the task execution.
        """
        # Register the execution with one of the core schedulers.
        execid = self.__execution_id
        self.__execution_id += 1
        core_scheduler = self.__next_scheduler
        self.__next_scheduler += 1
        self.__next_scheduler %= self.__cores
        self.__schedulers[core_scheduler][1].schedule(task_name, task_input, execid)

        # Return the execution id to the client.
        return execid
    
    def corescheduler_callback(self, execid, rcode, opt):
        self.__jailor.task_callback(execid, rcode, opt)
                    
