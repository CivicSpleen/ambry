
def default_bundle_config():
    '''Return the default bundle config file as an AttrDict'''

    import os
    from ambry.util import AttrDict

    config = AttrDict()
    f = os.path.join(
        os.path.dirname(
            os.path.realpath(__file__)),
        'bundle.yaml')

    config.update_yaml(f)

    return config
