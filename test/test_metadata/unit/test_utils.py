# -*- coding: utf-8 -*-
import unittest

from ambry.metadata.schema import Top
from ambry.metadata.utils import db_save, db_retrieve

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from ambry.orm import Base, Config


class DbSaveTest(unittest.TestCase):

    def test_saves_top_level_config(self):

        engine = create_engine('sqlite://')
        # create all tables.
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)
        session = Session()

        top = Top()
        top.names.vid = 'ds0001'
        db_save(session, top)

        # testing saved
        saved_config = session.query(Config).filter_by(d_vid='ds0001').first()
        self.assertIsNone(saved_config.key)
        self.assertTrue(saved_config.value == {})
        self.assertIsNone(saved_config.parent_id)

    def test_saves_top_children(self):
        engine = create_engine('sqlite://')

        # create all tables.
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)
        session = Session()

        top = Top()
        top.names.vid = 'ds0001'
        db_save(session, top)

        # testing saved
        top_instance = session.query(Config)\
            .filter_by(d_vid='ds0001', parent_id=None)\
            .one()

        top_children = session.query(Config)\
            .filter_by(d_vid='ds0001', parent_id=top_instance.id)\
            .all()

        saved_keys = [x.key for x in top_children]
        assert top._members
        self.assertEquals(len(saved_keys), len(top._members))
        for key, value in top._members.iteritems():
            self.assertIn(key, saved_keys)

    def test_saves_about_children(self):
        engine = create_engine('sqlite://')

        # create all tables.
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)
        session = Session()

        top = Top()
        top.names.vid = 'ds0001'
        top.about.access = 'restricted'
        top.about.tags = ['tag1', 'tag2']

        db_save(session, top)

        # testing saved
        top_instance = session.query(Config)\
            .filter_by(d_vid='ds0001', parent_id=None)\
            .one()

        about_instance = session.query(Config)\
            .filter_by(parent_id=top_instance.id, key='about').one()

        about_children = session.query(Config)\
            .filter_by(parent_id=about_instance.id)\
            .all()

        saved_keys = [x.key for x in about_children]
        assert top.about._members
        self.assertEquals(len(saved_keys), len(top.about._members))
        for key, value in top.about._members.iteritems():
            self.assertIn(key, saved_keys)

        # test saved tags
        tags_instance = session.query(Config)\
            .filter_by(parent_id=about_instance.id, key='tags').one()
        self.assertEquals(tags_instance.value, ['tag1', 'tag2'])


class DbRetrieveTest(unittest.TestCase):
    def test_returns_structure_property_tree(self):

        engine = create_engine('sqlite://')

        # create all tables.
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)
        session = Session()

        # create properties in the db.
        new_config = Config(
            d_vid='ds001', group='names',
            key='vid', value='ds001', type='type')
        session.add(new_config)
        session.commit()
        ret = db_retrieve(session, 'ds001')

        # testing saved
        self.assertIsInstance(ret, Top)

    def test_populates_about_section(self):
        engine = create_engine('sqlite://')

        # create all tables.
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)
        session = Session()

        # create properties in the db.
        root = Config(d_vid='ds001', parent_id=None)
        session.add(root)
        session.commit()

        about = Config(d_vid='ds001', parent_id=root.id, key='about', group='about')
        session.add(about)
        session.commit()

        access = Config(d_vid='ds001', parent_id=about.id, key='access', value='restricted')
        tags = Config(d_vid='ds001', parent_id=about.id, key='tags', value=['tag1', 'tag2'])
        session.add(access)
        session.add(tags)
        session.commit()

        # testing
        ret = db_retrieve(session, 'ds001')
        self.assertEquals(ret.about.access, 'restricted')
