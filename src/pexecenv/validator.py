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

"""The code validator used by Locusts."""

import re

class ValidationError(Exception):
    def __init__(self, msg):
        Exception.__init__(self, msg)
        
class Validator:
    """Validates code to a given security level."""
    def __init__(self, code, seclvl=0):
        """
        Constructor.
        @type code: str
        @param code: The Python code.
        @type seclvl: int
        @param seclvl: The security level to match the code against.
        """
        self.__code = code
        self.__seclvl = seclvl

    IS_COMMENT = re.compile('^[\t ]*\#')
    KEYWORDS = re.compile('(__subclasses__)|(__class__)|(__import__)|(__builtins__)|(__getattr__)|(__getattribute__)|(exec)')
    LEGAL_IMPORTS = ['math', 'PIL', 'StringIO', 'gdata.photos.service', 'smtplib', 'MimeWriter', 'base64']
    RE_IMPORT = re.compile('^[\t ]*import[\t ]+([\w\.]+)(?:[\t ]+as[\t ]+[\w\.]+)?[\t ]*(?:#|$)')
    RE_FROM_IMPORT = re.compile('^[\t ]*from[\t ]+([\w\.]+)[\t ]+import[\t ]+(?:[\w\.]+(?:[\t ]+as[\t ]+[\w\.]+)?[\t ]*,[\t ]*)*[\w\.]+(?:[\t ]+as[\t ]+[\w\.]+)?[\t ]*(?:#|$)')
    
    def validate(self):
        """
        Starts the validation process.
        @raise ValidationError: If the code does not validate. 
        """
        # Run through the code line by line.
        lineno = 0
        for line in self.__code.splitlines():
            lineno += 1
            
            # Weed out comments.
            if Validator.IS_COMMENT.match(line):
                continue
            
            # Check for keyword matches.
            m = Validator.KEYWORDS.search(line)
            if m:
                # Find the matching group.
                for group in m.groups():
                    if group != None:
                        raise ValidationError('Code contains illegal keyword %s on line #%i.'%(group,lineno))
                raise ValidationError('Code contains illegal keyword on line #%i.'%lineno)

            # Check import statements.
            if 'import' in line:
                m = Validator.RE_IMPORT.match(line)
                if not m: m = Validator.RE_FROM_IMPORT.match(line)
                if m:
                    module = m.group(1)
                    if not module in Validator.LEGAL_IMPORTS:
                        raise ValidationError('Code imports: %s'%module)
                else:
                    # Unrecognised import statement?
                    raise ValidationError('Unrecognised (obfuscated?) import statement. %s'%line)
        
            
# DEBUG code below.
if __name__ == '__main__':
    validator = Validator("""
    import math #OK
    import math as bar #OK
    from math import sin #OK
    from math import sin,cos,tan #OK    
    from math import sin as foo #OK
    from math import sin as foo, cos as bar #OK
    #import math, malicious, math #Error

    f.__class__ __builtins__
    """)
    validator.validate()
