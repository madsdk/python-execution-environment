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
This file contains the implementation of the service registry.
"""

import os

class ServiceRegistry:
    """
    The service registry keeps track of installed services within the
    execution environment.
    """
    def __init__(self):
        # Set member vars.
        self.__services = set()

        # Build the service registry by scanning the 'services' directory.
        for root, _, files in os.walk('see' + os.path.sep + 'services'):
            # Remove svn-entries and entries in an incorrect depth.
            if '.svn' in root or root.count(os.path.sep) != 3:
                continue
            for filename in files:
                # Remove pre-compiled files and __init__.py.
                if filename[-3:] != '.py' or filename == '__init__.py':
                    continue
                # Add the rest.
                self.__services.add(root[13:].replace(os.path.sep,'.') + '.' + filename[:-3])
            
    def has_service(self, service_name):
        """
        Checks whether a given service is available.
        @type service_name: str
        @param service_name: The service identifier.
        """
        return service_name in self.__services
    
    def install_service(self, service_name, service_code):
        """
        Installs the given service. At this point we know that:
        1) The service is not already installed.
        2) The service code has been validated and found to be "safe".
        3) The service name is valid.
        @type service_name: str
        @param service_name: The service identifier.
        @type service_code: str
        @param service_code: The service code, i.e., the Python code that 
        performs the actual service.
        """
        # Start by creating the file and directories.
        (dir1, dir2, name) = service_name.split('.')
        dir1_path = 'see' + os.path.sep + 'services' + os.path.sep + dir1
        if not os.path.exists(dir1_path):
            os.mkdir(dir1_path)
            open(dir1_path + os.path.sep + '__init__.py', 'w').close()
        dir2_path = 'see' + os.path.sep + 'services' + os.path.sep + dir1 + os.path.sep + dir2
        if not os.path.exists(dir2_path):
            os.mkdir(dir2_path)
            open(dir2_path + os.path.sep + '__init__.py', 'w').close()
            
        # Now the path exists. Create the file and write the code into it.
        target_file = open(dir2_path + os.path.sep + '%s.py'%name, 'w')
        target_file.write(service_code)
        target_file.close()
        
        # Add the service to the registry.
        self.__services.add(service_name)
        
    def fetch_service_code(self, service_name):
        """
        Fetches the source code of the named service. It is assumed that 
        1) the service name is valid, and
        2) the service is indeed installed. 
        @type service_name: str
        @param service_name: The name of the service.
        @rtype: str
        @return: The code of the service.
        """
        # Read the service code into memory.
        path = 'see' + os.path.sep + 'services' + os.path.sep + service_name.replace('.', os.path.sep) + '.py'
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
