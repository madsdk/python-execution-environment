import os

# Re-implementation of the open() function.
def monkey_open(name, mode = 'r', buffering = -1):
    # Validate the path.
    if name.find('..') != -1 or name.find('~') != -1:
        raise IOError('Backtracking is not allowed when opening files.')
    
    # Append 'storage' to the path.
    name = 'storage' + os.path.sep + name

    # Return the opened file object.
    return open(name, mode, buffering)
   
# The standard header that can be prefixed onto untrusted task code.
monkey_header = """# ---MONKEY_START---
import pexecenv.monkey as monkey
open = monkey.monkey_open
def raise_error(e): raise Exception(e)
file = lambda *_: raise_error('Initialization of file objects is prohibited.')
type = lambda *_: raise_error('Usage of the type() function is prohibited.')
eval = lambda *_: raise_error('Usage of the eval() function is prohibited.')
execfile = lambda *_: raise_error('Usage of the execfile() function is prohibited.')
exit = lambda *_: raise_error('Usage of the exit() function is prohibited.')
quit = lambda *_: raise_error('Usage of the quit() function is prohibited.')
getattr = lambda *_: raise_error('Usage of the getattr() function is prohibited.')
globals = lambda *_: raise_error('Usage of the globals() function is prohibited.')
locals = lambda *_: raise_error('Usage of the locals() function is prohibited.')
help = lambda *_: raise_error('Usage of the help() function is prohibited.')
input = lambda *_: raise_error('Usage of the input() function is prohibited.')
raw_input = lambda *_: raise_error('Usage of the raw_input() function is prohibited.')
vars = lambda *_: raise_error('Usage of the vars() function is prohibited.')
compile = lambda *_: raise_error('Usage of the compile() function is prohibited.')
del monkey
# ---MONKEY_END---
"""
