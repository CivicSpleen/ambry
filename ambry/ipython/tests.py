"""Test of how libraries interact with IPYthon"""


def test_logging():
    import logging

    logger = logging.getLogger('ipython')
    logger.propagate = False

    for i in range(1, 3):
        logger.info('Logging message {}'.format(i))
