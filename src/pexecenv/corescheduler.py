from multiprocessing import Process, Queue
from time import sleep
from Queue import Empty as QueueEmptyException
import stackless

class CoreScheduler(Process):
    """A CoreScheduler schedules stackless tasks within a single thread, 
    i.e., on a single core/CPU."""
    
    STEP_SIZE = 1000000
    SLEEP_TIME = 0.01
    MAX_SINS = 1000
    
    def __init__(self, eipc_handle):
        """
        Constructor.
        @type eipc_handle: eipc.EIPC
        @param eipc_handle: An IPC handle that the corescheduler may use to communicate
        with the scheduler.
        """
        super(CoreScheduler, self).__init__()
        self.__ipc = eipc_handle
        self.__ipc.register_function(self.schedule)
        self.__ipc.start()
        self.__scheduling_queue = Queue()
        self.__sinners = {} # Sinners are tasklets that use too many resources :-)

    def perform_service(self, service_name, service_input, execid):
        try:
            # Load the service if necessary.
            service_module = __import__('see.services.'+service_name, {}, {}, ['perform'], 0)
            # Perform the service.
            if type(service_input) == dict:
                output = service_module.perform(**service_input)
            elif type(service_input) in (tuple, list):
                output = service_module.perform(*service_input)
            else:
                output = service_module.perform(service_input)
        except TaskletExit:
            # The tasklet has been killed.
            try:
                t = stackless.getcurrent()
                atomic = t.set_atomic(True)
                if t in self.__sinners: 
                    self.__sinners.pop(t)
                try:
                    self.__ipc.callback(execid, 'ERROR', {'error':'Service was killed.'})
                finally:
                    t.set_atomic(atomic)
                    try: del service_module 
                    except: pass
            except: #IGNORE:W0704
                pass
            return
        except Exception, excep: #IGNORE:W0703
            # The service execution has thrown an exception. Pass this
            # exception on to the initiator so that the bug hunt may begin. 
            try:
                t = stackless.getcurrent()
                atomic = t.set_atomic(True)
                if t in self.__sinners: 
                    self.__sinners.pop(t)
                try:
                    self.__ipc.callback(execid, 'ERROR', {'error':excep.message})
                finally:
                    t.set_atomic(atomic)
                    try: del service_module 
                    except: pass
            except: #IGNORE:W0704
                pass
            return
        
        # The service has been successfully performed.
        try:
            t = stackless.getcurrent()
            if t in self.__sinners: 
                self.__sinners.pop(t)
            atomic = t.set_atomic(True)
            try:
                self.__ipc.callback(execid, 'DONE', {'output':output})
            finally:
                t.set_atomic(atomic)
                try: del service_module 
                except: pass
        except: #IGNORE:W0704
            pass
                
    def kill_tasklet(self, tasklet):
        tasklet.kill()
                      
    def schedule(self, service_module, service_input, execid):
        self.__scheduling_queue.put((service_module, service_input, execid))
          
    def run(self):
        """Main process function."""
        while True:
            # Check whether any new services should be scheduled.
            while not self.__scheduling_queue.empty():
                try:
                    service_module, service_input, execid = self.__scheduling_queue.get_nowait()
                    stackless.tasklet(self.perform_service)(service_module, service_input, execid)
                except QueueEmptyException:
                    break
                                
            # Schedule currently active tasklets - if any.
            if stackless.getruncount() != 1:
                # Run for the next STEP_SIZE instructions.
                tasklet = stackless.run(CoreScheduler.STEP_SIZE)
                if tasklet:
                    # Check this task against the "sinners" registry.
                    if tasklet in self.__sinners:
                        # The tasklet has been added to sinners already. Check how many times it has been pre-empted.
                        if self.__sinners[tasklet] == CoreScheduler.MAX_SINS:
                            # This sinner must be killed. Try to kill it nicely by raising a TaskletExit exception.
                            print "killing tasklet" #DEBUG
                            self.__sinners[tasklet] = -1
                            stackless.tasklet(self.kill_tasklet)(tasklet)
                        elif self.__sinners[tasklet] == -1:
                            # The bastard caught the TaskletExit exception! Silently remove him from the runables queue.
                            print "bastard!" #DEBUG
                            self.__sinners.pop(tasklet)
                        else:
                            # Give him another chance.
                            self.__sinners[tasklet] += 1
                            tasklet.insert()
                    else: # if tasklet in self.__sinners
                        # Insert the tasklet into the sinners registry.
                        self.__sinners[tasklet] = 1
                        tasklet.insert()
            else:
                # Otherwise sleep for a little while.
                sleep(CoreScheduler.SLEEP_TIME)
                
