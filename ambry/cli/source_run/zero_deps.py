'''Script for source run to print the umber of dependencies that a source bundle has'''

def run(args, bundle_dir, bundle, repo):

    import sys

    #if bundle.is_built:
    #    return

    deps = bundle.config.build.get('dependencies',{})

    if len(deps) == 0:
        print bundle_dir
