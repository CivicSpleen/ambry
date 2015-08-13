"""
"""

from ambry.bundle import Bundle


class Bundle(Bundle):
    """ """

    @staticmethod
    def not_float_is_none(v):
        try:
            return float(v)
        except:
            return None

    def post_schema(self):

        self.table('altname').column('bar').caster = 'not_float_is_none'
        self.table('altname').column('baz').caster = 'not_float_is_none'
        self.table('simple').column('float').caster = 'not_float_is_none'

        self.commit()

        return self.post_phase('meta')


