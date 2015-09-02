"""Script for source run to print the umber of dependencies that a source
bundle has."""


def run(args, bundle_dir, bundle):

    # if bundle.is_built:
    #    return

    deps = bundle.metadata.dependencies

    if len(deps) == 0:
        print(bundle.identity.fqname)
