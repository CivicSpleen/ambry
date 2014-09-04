.. _recipes_build_toplevel:

=============
Bulding
=============

Inserting List data
------------------------

The ValueInserter's :func:`~ambry.database.inserter.ValueInserter.insert` function expects a dict, but you may be reading
rows from a data source. If the order of the fields in the row is the same as the order of the columns, this is easy
to fix with :func:`zip()`


Custom Casters
------------------------

Inserters create caster objects to convert the field in incoming rows to the types declared in the schema. You can
define custom caster types for a column.

For, add a "d_caster" column to the schema.csv file, with the name of a caster class. Then create a class of that
name in the bundle.

Here is a caster type that converts value in an integer column that are bottom coded with '<5' to be zero

.. sourcecode:: python

    class lt5(int):
        '''Convert <5 ( Less than 5 deaths ) to 0'''
        def __new__(self, v):

            if v == "<5":
                v = 0

            return int.__new__(self, v)

Like all types, you must implement the constructor :func:`__new__`, not the initializer ::func:`__init__`. If the caster
type determines that the value is invalid, it can return None, or call

The custom caster can also be a function:

.. sourcecode:: python

    @staticmethod
    def lt5(v):
        if v == "<5":
            v = 0

        return int(500)

In both cases, the casters are members of the bundle.


