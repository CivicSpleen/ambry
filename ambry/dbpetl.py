"""Extensions to the petl library for manipulating table data in memory.

Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""
import  petl

def follow(table, func, **kwargs):
    """
    Call a function for each non header row
    """   
 
    return FollowView(table, func, **kwargs)



class FollowView(petl.util.RowContainer):
    
    def __init__(self, source, f):
        self.source = source
        self.f = f
        
    def cachetag(self):
        return self.source.cachetag()
    
    def __iter__(self):
     
        for r in self.source:
            self.f(r)
            yield r
   
def mogrify(table, func, **kwargs):
    """
    Call a function for each row and replace the
    row with the one returned by the function
    """   
 
    return MogrifyView(table, func, **kwargs)

class MogrifyView(petl.util.RowContainer):
    
    def __init__(self, source, f):
        self.source = source
        self.f = f
        
    def cachetag(self):
        return self.source.cachetag()
    
    def __iter__(self):
        itr = iter(self.source)
        yield  itr.next() # Header
        for r in itr:
            yield self.f(r)
   

def fromregex(source, *args, **kwargs):
    """
    Extract data from a file, line by line, using a regular expression to break 
    the line into fields. This is useful both for irregular columns, and for
    fixed width lines. 
    E.g.::

        >>> from petl import fromregex, look
        >>> import re
        >>> 
        >>> with open('example1.txt', 'w') as f:
        ...     f.write("111122 foobar333 bingo\n")
        ...     f.write("123489  bcdef765   baz")
        ... 
        >>> regex = re.compile("(\d{4})(\d{2})([\s\w]{7})(\d{3})([\s\w]{6})")
        >>> table1 = fromregex('example1.txt')
        >>> look(table1)
        +--------+------+-----------+-------+----------+
        |        |      |           |       |          |
        +========+======+===========+=======+==========+
        | '1111' | '22' | ' foobar' | '333' | ' bingo' |
        +--------+------+-----------+-------+----------+
        | '1234' | '89' | '  bcdef' | '765' | '   baz' |
        +--------+------+-----------+-------+----------+
        
    There are two ways to add a header. Use the 'header' parameter, or, 
    construct a regular expression with named parameters
    
        >>> regex = re.compile(
        ... "(?P<a>\d{4})(?P<b>\d{2})(?P<c>[\s\w]{7})(?P<d>\d{3})(?P<e>[\s\w]{6})")
        >>> table1 = fromregex('example1.txt')
        >>> look(table1)
        
       +--------+------+-----------+-------+----------+
       | 'a'    | 'c'  | 'b'       | 'e'   | 'd'      |
       +========+======+===========+=======+==========+
       | '1111' | '22' | ' foobar' | '333' | ' bingo' |
       +--------+------+-----------+-------+----------+
       | '1234' | '89' | '  bcdef' | '765' | '   baz' |
       +--------+------+-----------+-------+----------+ 

    Supports transparent reading from URLs, ``.gz`` and ``.bz2`` files.

    .. versionadded:: 0.11
    
    """
    source = petl.io._read_source_from_arg(source)
    return RegexLineView(source, *args, **kwargs)


class RegexLineView(petl.util.RowContainer):
    
    def __init__(self, source, *args, **kwargs):
        self.source = source
        self.args = args
        self.kwargs = kwargs
        self.header = None
    
        if 'regex' in kwargs:
            self.regex = kwargs['regex']

        if 'header' in kwargs:
            self.header = kwargs['header']
        else:
            raise Exception('fromregex() requires a header')
        
        
    def __iter__(self):
           
        yield tuple(self.header)
        
        with self.source.open_() as f:
            for line in f:
                m = self.regex.match(line)
                if m:            
                    yield  m.groups()
                else:
                    raise ValueError("Failed to match regex on line: "+line)
                    
    def cachetag(self):
        try:
            return hash((self.source.checksum(), self.args, tuple(self.kwargs.items())))
        except Exception as e:
            raise Exception(e)



  
#
# Fluentize
#

import sys
from petl.fluent import FluentWrapper, wrap

#
# Add all of the functions in this module into the FluentWrapper as
# methods
#

for n, c in sys.modules[__name__].__dict__.items():
    if callable(c):
        setattr(FluentWrapper, n, wrap(c)) 
        