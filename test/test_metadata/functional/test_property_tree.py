# -*- coding: utf-8 -*-

from ambry.metadata.schema import Top

from ambry.orm import Config

from test.test_base import TestBase


class DatabaseConfigUpdateTest(TestBase):
    """ Tests db update after property tree change. """
    def setUp(self):
        super(DatabaseConfigUpdateTest, self).setUp()
        self.db = self.new_database()
        self.dataset = self.new_db_dataset(self.db, n=0)

    def test_create_group_and_config_in_the_db(self):
        """ Setting empty property tree key creates group and config in the db. """
        db = self.db
        dataset = self.dataset

        top = Top()

        # bind tree to the database
        top.link_config(db.session, dataset)

        # change some fields
        top.names.vid = dataset.vid
        top.about.access = 'public'

        # testing
        query = db.session.query(Config)
        top_config = query.filter_by(d_vid=dataset.vid, type='metadata', parent=None).first()

        # top config does not have key or value
        self.assertIsNone(top_config.key, '')
        self.assertEqual(top_config.value, {})

        #
        # names and about groups does not have values
        #
        names_group = query.filter_by(d_vid=dataset.vid, type='metadata', group='names').first()
        self.assertEqual(names_group.key, 'names')
        self.assertEqual(names_group.value, {})
        self.assertEqual(names_group.parent, top_config)

        about_group = query.filter_by(d_vid=dataset.vid, type='metadata', group='about').first()
        self.assertEqual(about_group.key, 'about')
        self.assertEqual(about_group.value, {})
        self.assertEqual(about_group.parent, top_config)

        #
        # Configs have proper parents and values.
        #
        vid_config = query.filter_by(d_vid=dataset.vid, type='metadata', key='vid').first()
        self.assertEqual(vid_config.key, 'vid')
        self.assertEqual(vid_config.value, dataset.vid)
        self.assertEqual(vid_config.parent, names_group)

    def test_change_database_values(self):
        """ Changing existing property tree key value changes config instance. """
        db = self.db
        dataset = self.dataset

        top = Top()

        # bind tree to the database
        top.link_config(db.session, dataset)

        # change some fields
        top.names.vid = dataset.vid

        # testing
        query = db.session.query(Config)
        vid_config = query.filter_by(d_vid=dataset.vid, type='metadata', key='vid').first()
        self.assertEqual(vid_config.value, dataset.vid)

        top.names.vid = 'vid-1'
        vid_config = query.filter_by(type='metadata', key='vid').first()
        self.assertEqual(vid_config.value, 'vid-1')


class BuildPropertyTreeFromDatabaseTest(TestBase):

    def setUp(self):
        super(self.__class__, self).setUp()
        self.db = self.new_database()
        self.dataset = self.new_db_dataset(self.db, n=0)

    # helpers
    def _create_db_tree(self):
        """ Creates tree in the database.

            .names.vid = dataset.vid
        """
        db = self.db
        dataset = self.dataset
        top_config = Config(
            d_vid=dataset.vid, parent=None, type='metadata')
        db.session.add(top_config)
        db.session.commit()
        names_config = Config(
            d_vid=dataset.vid, key='names', group='names',
            parent=top_config, type='metadata')
        db.session.add(names_config)
        db.session.commit()

        vid_value_config = Config(
            d_vid=dataset.vid, key='vid', value=dataset.vid,
            parent=names_config, type='metadata')
        db.session.add(vid_value_config)
        db.session.commit()

    # tests
    def test_build_tree_from_database(self):
        """ Populates property tree with values from database. """

        # create appropriate tree in the database
        self._create_db_tree()

        # build from db
        top = Top()
        top.build_from_db(self.dataset)
        self.assertEqual(top.names.vid, self.dataset.vid)

    def test_change_tree_build_from_database(self):

        # create appropriate tree in the database
        self._create_db_tree()

        # build from db
        top = Top()
        top.build_from_db(self.dataset)

        # change and test
        assert top.is_bound()
        vid_value_config = self.db.session.query(Config).filter_by(key='vid', type='metadata').one()
        self.assertNotEqual(vid_value_config.value, 'vid-2')
        top.names.vid = 'vid-2'
        vid_value_config = self.db.session.query(Config).filter_by(key='vid', type='metadata').one()
        self.assertEqual(vid_value_config.value, 'vid-2')
