'''Script for source run to print the umber of dependencies that a source bundle has'''


def run(args, bundle_dir, bundle):

    print '========= {} =========='.format(bundle_dir)

    if bundle.metadata.build.sources:

        s = bundle.metadata.sources
        for k, v in bundle.metadata.build.sources.items():
            s[k]['url'] = v

        bundle.update_configuration()
