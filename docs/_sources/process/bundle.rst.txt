
The Bundle Class
================

Topics

- Creating a bundle class
- Column casters
- Ad Hoc operations with exec
- Events

    - Build Events
    - Test Events




Events
******

Build Events
------------

Using build event decorators, bundle methods can be marked to run at an event points

.. code-block:: python 

    @after_run
    def after_run(self):

        for r in self.progress.records:
            print r
            for c in r.children:
                print '    ', c


Test Events
-----------

Test events are similar to buld events, but they are defined in a seperate class, in the vile :file:`test.py`. This class is a subclass of unittest.TestCase, so the class is a real unit test, and the event decorators define when each test case will run, allowing the defintion of tests at each stage of the build. This system is used in testing Ambry, but it can also be used to test bundle builds. 


    

