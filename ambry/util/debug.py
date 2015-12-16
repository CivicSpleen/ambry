"""Debugging code. Allows breaking to an interpreter with a signal.

To use, just call the listen() function at some point when your program starts up
(You could even stick it in site.py to have all python programs use it),
 and let it run. At any point, send the process a SIGUSR1 signal, using kill, or in python:

    os.kill(pid, signal.SIGUSR1)

"""

# from:
# http://stackoverflow.com/questions/132058/showing-the-stack-trace-from-a-running-python-application

import code
import traceback
import signal
from six.moves import builtins

def debug_break(sig, frame):
    """Interrupt running process, and provide a python prompt for interactive
    debugging."""
    d = {'_frame': frame}         # Allow access to frame object.
    d.update(frame.f_globals)  # Unless shadowed by global
    d.update(frame.f_locals)

    i = code.InteractiveConsole(d)
    message = "Signal recieved : entering python shell.\nTraceback:\n"
    message += ''.join(traceback.format_stack(frame))
    i.interact(message)


def listen():
    signal.signal(signal.SIGUSR1, debug_break)  # Register handler



def trace(fn): # pragma: no cover
    """ Prints parameteters and return values of the each call of the wrapped function.

    Usage:
        decorate appropriate function or method:
            @trace
            def myf():
                ...
    """
    def wrapped(*args, **kwargs):
        msg = []
        msg.append('Enter {}('.format(fn.__name__))

        if args:
            msg.append(', '.join([str(x) for x in args]))

        if kwargs:
            kwargs_str = ', '.join(['{}={}'.format(k, v) for k, v in list(kwargs.items())])
            if args:
                msg.append(', ')
            msg.append(kwargs_str)
        msg.append(')')
        print(''.join(msg))
        ret = fn(*args, **kwargs)
        print('Return {}'.format(ret))
        return ret
    return wrapped


def patch_file_open(): # pragma: no cover
    """A Monkey patch to log opening and closing of files, which is useful for
    debugging file descriptor exhaustion."""

    openfiles = set()
    oldfile = builtins.file

    class newfile(oldfile):
        def __init__(self, *args, **kwargs):
            self.x = args[0]

            all_fds = count_open_fds()

            print('### {} OPENING {} ( {} total )###'.format(
                len(openfiles), str(self.x), all_fds))
            oldfile.__init__(self, *args, **kwargs)

            openfiles.add(self)

        def close(self):
            print('### {} CLOSING {} ###'.format(len(openfiles), str(self.x)))
            oldfile.close(self)
            openfiles.remove(self)

    def newopen(*args, **kwargs):
        return newfile(*args, **kwargs)

    builtins.file = newfile
    builtins.open = newopen


# patch_file_open()

# From
# http://stackoverflow.com/questions/528281/how-can-i-include-an-yaml-file-inside-another
