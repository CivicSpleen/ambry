""" Debugging code. Allows breaking to an interpreter with a signal.

To use, just call the listen() function at some point when your program starts up
(You could even stick it in site.py to have all python programs use it),
 and let it run. At any point, send the process a SIGUSR1 signal, using kill, or in python:

    os.kill(pid, signal.SIGUSR1)

 """

#Copyright (c) 2014 Clarinova. This file is licensed under the terms of the
#Revised BSD License, included in this distribution as LICENSE.txt


# from: http://stackoverflow.com/questions/132058/showing-the-stack-trace-from-a-running-python-application

import code, traceback, signal

def debug_break(sig, frame):
    """Interrupt running process, and provide a python prompt for
    interactive debugging."""
    d={'_frame':frame}         # Allow access to frame object.
    d.update(frame.f_globals)  # Unless shadowed by global
    d.update(frame.f_locals)

    i = code.InteractiveConsole(d)
    message  = "Signal recieved : entering python shell.\nTraceback:\n"
    message += ''.join(traceback.format_stack(frame))
    i.interact(message)

def listen():
    signal.signal(signal.SIGUSR1, debug_break)  # Register handler