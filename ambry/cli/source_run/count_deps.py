"""Script for source run to print the umber of dependencies that a source
bundle has."""


def run(args, bundle_dir, bundle):

    # if bundle.is_built:
    #    return

    deps = bundle.config.build.get('dependencies', {})

    print(len(deps), bundle_dir)
