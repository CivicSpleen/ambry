


def display_list(l, list_type='ul'):
    """Convert a list of strings into an HTML list"""
    from IPython.core.display import display_html

    o = ["<{}>".format(list_type)]

    for i in l:
        o.append("<li>{}</li>".format(str(i)))

    o.append("</{}>".format(list_type))

    return  display_html('\n'.join(o), raw=True)