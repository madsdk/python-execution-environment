from __future__ import with_statement
from eipc import EIPC
from scrpc import SCRPC
from threading import Condition, Thread
from thread import allocate_lock
from pexecenv import Jailor
from time import sleep, time
# TODO: Why is the full path for Config needed here!?
from frontends.daemonconfig import Config
from datastore import RemoteDataStore, RemoteDataHandle
import logging

class StaticSurrogate(Thread):
    CALLBACK_TIMEOUT = 5.0
    MAINT_POLL = 1.0
    
    def __init__(self, debug_jail = False):
        super(StaticSurrogate, self).__init__()
        
        # Set member variables.
        self.pending_tasks = {}
        self.pending_tasks_lock = allocate_lock()
        self.activity_count = 0
        self.__shutdown = False
        
        # Get a logger.
        self.__logger = logging.getLogger('scavenger')

        # Get a config handle.
        self._config = Config.get_instance()
        # Check that the "static" section contains a node name.
        if not self._config.has_section('static') or not self._config.has_option('static', 'name'):
            self.__logger.error("Static surrogate name is missing in the config file.")
            raise Exception("Static surrogate name is missing in the config file.")

        # Start the execution environment.
        self._ipc, remote_pipe = EIPC.eipc_pair()
        self._ipc.start()
        self.__exec_env = Jailor(remote_pipe, self._config.getint('cpu', 'cores'), debug=debug_jail)
        self.__exec_env.start()

        # Register the callback function.
        self._ipc.register_function(self.task_callback)

        # Create an RPC server that the clients can connect to.
        try:
            self.rpc_server = SCRPC()
            scavenger_port = self.rpc_server.get_address()[1]
            self.rpc_server.register_function(self.perform_task)
            self.rpc_server.register_function(self.perform_task_intent)
            self.rpc_server.register_function(self.install_task)
            self.rpc_server.register_function(self.has_task)
            self.rpc_server.register_function(self.ping)
            self.__logger.info('StaticSurrogate daemon is listening on port %i'%scavenger_port)
        except Exception, e:
            self.__logger.exception('Error creating RPC server.')
            try:
                self.__exec_env.shutdown()
                if self.rpc_server: self.rpc_server.stop(True)
            except: pass
            raise e

        # Create a remote data store.
        self.remotedatastore = RemoteDataStore(self._config.get('static', 'name'))
        self.rpc_server.register_function(self.remotedatastore.fetch_data, 'resolve_data_handle')
        self.rpc_server.register_function(self.remotedatastore.retain, 'retain_data_handle')
        self.rpc_server.register_function(self.remotedatastore.expire, 'expire_data_handle')
        self.rpc_server.register_function(self.remotedatastore.store_data, 'store_data')

        # Start the maintenance thread.
        self.start()
     
    def shutdown(self):
        self.__shutdown = True
        self.__exec_env.shutdown()
        self.rpc_server.stop()
    
    def ping(self, flaf):
        """
        Simple rpc function that can be used to check whether the connection is alive.
        """
        return flaf    

    def task_callback(self, rcode, eid, output):
        # Find the Condition object that the worker thread is waiting on.  
        with self.pending_tasks_lock:
            try:
                cond = self.pending_tasks[eid]
            except KeyError:
                # The execution id was unknown. This means that the operation has timed out.
                return
            
            # Store the return code and output for the caller to fetch.
            self.pending_tasks[eid] = (rcode, output)
                
        # Now the return code and output has been placed so that the waiting
        # thread can access it. Time to awaken the sleeper...
        cond.acquire()
        cond.notify()
        cond.release()

    def _resolve_data_handles_in_input(self, task_input):
        if type(task_input) == dict:
            # Keyword arguments.
            for key, value in task_input.items():
                if type(value) == RemoteDataHandle:
                    task_input[key] = self.remotedatastore.resolve_data_handle(value)
        elif type(task_input) in (tuple, list):
            # Positional arguments.
            new_list = []
            for value in task_input:
                if type(value) == RemoteDataHandle:
                    new_list.append(self.remotedatastore.resolve_data_handle(value))
                else:
                    new_list.append(value)
            task_input = new_list
        else:
            # Single argument.
            if type(task_input) == RemoteDataHandle:
                task_input = self.remotedatastore.resolve_data_handle(task_input)
        return task_input

    def change_activity(self, increment):
        with self.pending_tasks_lock:
            self.activity_count += increment

    def perform_task_intent(self, failure):
        if failure:
            # There was intent to call the function but it was never in fact called.
            self.change_activity(-1)
        else:
            # Someone has shown intent of calling this funtion.
            self.change_activity(1)
        
    def perform_task(self, task_name, task_input, timeout = 120, store = False, profile = False):
        print 'perform %s'%task_name #DEBUG

        # Check the task input for data handles that should be resolved.
        task_input = self._resolve_data_handles_in_input(task_input)
        
        # Start performing the task.
        with self.pending_tasks_lock:
            if profile:
                start = time()
                start_activity = self.activity_count
            try:
                # Send the message to the execution env.
                eid = self._ipc.perform_task(task_name, task_input)
                # Create a Condition object that this worker thread can wait on until 
                # the execution of the task is done.
                cond = Condition()
                cond.acquire()
                # Update the pending tasks table.
                self.pending_tasks[eid] = cond
            except Exception, error:
                err_msg = 'Error registering task with execution environment.'
                raise Exception(err_msg, error)
        
        # Wait for the task to finish -- or for the timer to expire...
        cond.wait(timeout)
        if profile:
            stop = time()
            stop_activity = self.activity_count

        # Check whether the result has been stored in pending_tasks.
        # If not this means that the timeout was reached.
        self.change_activity(-1)
        with self.pending_tasks_lock:
            try:
                flaf = self.pending_tasks.pop(eid)
            except KeyError, error:
                del cond
                err_msg = 'This should never happen ;-)'
                raise Exception(err_msg, error)
    
        if type(flaf) == tuple:
            # The result (or an error message is there).
            cond.release()
            del cond
            rcode, output = flaf
            if rcode == 'RESULT':
                if store:
                    # We have been asked to store the result here.
                    if type(output) == tuple:
                        # Store the output values as individual remote data handles. 
                        new_output = []
                        for item in output:
                            new_output.append(self.remotedatastore.store_data(item))
                        new_output = tuple(new_output)
                    else:
                        new_output = self.remotedatastore.store_data(output)

                    if profile:
                        cores = self._config.getint('cpu', 'cores')
                        activity_level = float(start_activity/cores + stop_activity/cores) / 2
                        if activity_level < 1: activity_level = 1.0
                        complexity = ((stop - start) * self._config.getfloat('cpu', 'strength')) / activity_level
                        return (new_output, complexity)
                    else:
                        return new_output
                else:
                    if profile:
                        cores = self._config.getint('cpu', 'cores')
                        activity_level = float(start_activity/cores + stop_activity/cores) / 2
                        if activity_level < 1: activity_level = 1.0
                        complexity = ((stop - start) * self._config.getfloat('cpu', 'strength')) / activity_level
                        return (output, complexity)
                    else:
                        return output
            elif rcode == 'ERROR':
                err_msg = 'Exception thrown within task: %s'%output
                raise Exception(err_msg)
            else:
                err_msg = 'Unknown return code: %s'%rcode
                raise Exception(err_msg)        
        else:
            # The condition object is still there... a timeout must have occurred.
            cond.release()
            del cond
            err_msg = 'Timeout while performing task.'
            raise Exception(err_msg)

    def install_task(self, task_name, task_code):
        try:
            self._ipc.install_task(task_name, task_code)
        except Exception, error:
            err_msg = 'Error installing task. %s'%error.message
            raise Exception(err_msg, error)

    def has_task(self, task_name):
        try:
            return self._ipc.task_exists(task_name)
        except Exception, error:
            raise Exception('Error checking for task. %s'%error.message, error)

    def serve(self):
        self.rpc_server.run()

    def run(self):
        # Thread body - this is used for any periodic maintenance etc.
#        cpu_strength = self._config.getfloat('cpu', 'strength')
#        cpu_cores = self._config.getint('cpu', 'cores')
#        network_speed = self._config.getint('network', 'speed')
        period_count = 0
        while not self.__shutdown:            
            # Cleanup the data store every 10th period.
            if period_count % 10 == 0:
                self.remotedatastore.cleanup()

            # Wait for another second...
            period_count += 1
            sleep(StaticSurrogate.MAINT_POLL)
