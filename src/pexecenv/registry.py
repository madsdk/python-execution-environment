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
This file contains the implementation of the task registry.
"""

import os

class TaskRegistry:
    """
    The task registry keeps track of installed tasks within the
    execution environment.
    """

    class NamingError(Exception):
        def __init__(self, message):
            super(TaskRegistry.NamingError, self).__init__(message)
    
    class FileAccessError(Exception):
        def __init__(self, message, exception):
            super(TaskRegistry.FileAccessError, self).__init__(message, exception)
    
    def __init__(self, basedir):
        # Set member vars.
        self.__tasks = set()
        self._basedir = basedir

        # Build the task registry by scanning the 'tasks' directory.
        # Start by checking that the 'tasks' directory exists.
        tasks_dir = self._basedir + os.path.sep + 'tasks'
        if not os.path.exists(tasks_dir):
            try:
                os.mkdir(tasks_dir, 0755)
                with open(tasks_dir + os.path.sep + '__init__.py', 'w') as _:
                    pass
            except Exception, e:
                # TODO: add logging
                raise TaskRegistry.FileAccessError('Error creating directory "tasks" for storing task code.', e)
        
        # Walk the directory structure and insert available tasks into the registry.
        for root, _, files in os.walk(tasks_dir):
            # Remove svn-entries and entries in an incorrect depth.
            if '.svn' in root or root.count(os.path.sep) != 3:
                continue
            for filename in files:
                # Remove pre-compiled files and __init__.py.
                if filename[-3:] != '.py' or filename == '__init__.py':
                    continue
                # Add the rest.
                self.__tasks.add(root[len(tasks_dir)+1:].replace(os.path.sep,'.') + '.' + filename[:-3])
            
    def has_task(self, task_name):
        """
        Checks whether a given task is available.
        @type task_name: str
        @param task_name: The task identifier.
        @rtype: bool
        @return: Whether or not the task in question is available.
        """
        return task_name in self.__tasks
    
    def install_task(self, task_name, task_code):
        """
        Installs the given task. At this point we know that:
        1) The task is not already installed.
        2) The task code has been validated and found to be "safe".
        3) The task name is valid.
        @type task_name: str
        @param task_name: The task identifier.
        @type task_code: str
        @param task_code: The task code, i.e., the Python code that 
        performs the actual task.
        """
        # Start by creating the file and directories.
        (dir1, dir2, name) = task_name.split('.')
        dir1_path = self._basedir + os.path.sep + 'tasks' + os.path.sep + dir1
        if not os.path.exists(dir1_path):
            os.mkdir(dir1_path)
            open(dir1_path + os.path.sep + '__init__.py', 'w').close()
        dir2_path = self._basedir + os.path.sep + 'tasks' + os.path.sep + dir1 + os.path.sep + dir2
        if not os.path.exists(dir2_path):
            os.mkdir(dir2_path)
            open(dir2_path + os.path.sep + '__init__.py', 'w').close()
            
        # Now the path exists. Create the file and write the code into it.
        target_file = open(dir2_path + os.path.sep + '%s.py'%name, 'w')
        target_file.write(task_code)
        target_file.close()
        
        # Add the task to the registry.
        self.__tasks.add(task_name)
        
    def fetch_task_code(self, task_name):
        """
        Fetches the source code of the named task. It is assumed that 
        1) the task name is valid, and
        2) the task is indeed installed. 
        @type task_name: str
        @param task_name: The name of the task.
        @rtype: str
        @return: The code of the task.
        """
        # Read the task code into memory.
        path = self._basedir + os.path.sep + 'tasks' + os.path.sep + task_name.replace('.', os.path.sep) + '.py'
        infile = open(path)
        code = infile.read()
        infile.close()
        
        # Remove the monkey patching header - if any...
        if code[:20] == '# ---MONKEY_START---':
            mheader_end = code.find('# ---MONKEY_END---') + 18
            if mheader_end != 17: # The header was actually found.
                code = code[mheader_end:]
        
        # Return the code to the caller.
        return code
