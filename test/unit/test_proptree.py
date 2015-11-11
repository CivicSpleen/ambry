# -*- coding: utf-8 -*-
import unittest

from ambry.metadata.proptree import AttrDict, StructuredPropertyTree, DictGroup, ScalarTerm, ListTerm


class AttrDictTest(unittest.TestCase):

    def test_initializes_properties_with_given_arguments(self):
        """ Setting empty property tree key creates group and config in the db. """
        attr_dict = AttrDict(prop1='value1', prop2='value2')
        self.assertTrue(hasattr(attr_dict, 'prop1'))
        self.assertTrue(hasattr(attr_dict, 'prop2'))
        self.assertEqual(attr_dict.prop1, 'value1')
        self.assertEqual(attr_dict.prop2, 'value2')


class ScalarTermTest(unittest.TestCase):

    def test_raises_ValueError_if_value_is_not_listed_in_the_constraint(self):

        # TODO: find the way how to test terms without creating tree.
        class Group1(DictGroup):
            term1 = ScalarTerm(constraint=['value1', 'value2'])

        class Tree(StructuredPropertyTree):
            group1 = Group1()

        tree = Tree()
        try:
            tree.group1.term1 = 'value3'
        except ValueError as exc:
            self.assertIn('is not valid value', str(exc))


class ListTermTest(unittest.TestCase):

    def test_converts_scalar_value_to_list(self):

        # TODO: find the way how to test terms without creating tree.
        class Group1(DictGroup):
            terms1 = ListTerm()

        class Tree(StructuredPropertyTree):
            group1 = Group1()

        tree = Tree()
        tree.group1.terms1 = 'value1'
        self.assertEqual(tree.group1.terms1, ['value1'])

        # test iteration
        tested = False
        for e in tree.group1.terms1:
            self.assertEqual(e, 'value1')
            tested = True
        self.assertTrue(tested)

    def test_replaces_stored_list_on_assign(self):

        # TODO: find the way how to test terms without creating tree.
        class Group1(DictGroup):
            terms1 = ListTerm()

        class Tree(StructuredPropertyTree):
            group1 = Group1()

        tree = Tree()
        tree.group1.terms1 = ['value1']
        self.assertEqual(tree.group1.terms1, ['value1'])

        tree.group1.terms1 = ['value2', 'value3']
        self.assertEqual(tree.group1.terms1, ['value2', 'value3'])
