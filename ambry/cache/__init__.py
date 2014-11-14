
# This used to have the cache implemetation, but that was moved out to the ckcache module

def new_cache(config, root_dir='no_root_dir'):
    import ckcache
    return ckcache.new_cache(config, root_dir)

def parse_cache_string(cstr, root_dir='no_root_dir'):
    import ckcache

    return ckcache.parse_cache_string(cstr, root_dir)