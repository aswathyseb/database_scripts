"""
This script reads data from NCBI taxonomy databases
and flattens them out in a dictionary.
"""

from collections import defaultdict
import csv


def add_node_attrs(data):
    # For every node get its node position among its siblings.
    # ie, 1st cgild or 2nd child and so on.
    # This needs to be called before getting path and depth
    data = get_node_pos(data)

    # Get node depth and full path upto and including the node.
    data = get_depth_path(data)
    return data


def get_node_pos(data):
    """
    For each node, get its position among siblings.
    ie, if its 1st child, 2nd child and so on.
    :param data:
    :return:
    """

    roots = []

    for node, attrs in data.items():

        def make_str(x):
            return str(x).zfill(4)

        parent = attrs['parent']
        children = attrs['children']

        # Add numchild to data
        numchild = len(children) if children else 0
        data[node]['numchild'] = numchild

        parent = parent if parent != "root" else None

        for idx, child in enumerate(children):

            # add all roots to a list, so that they can have different origin in path
            # Needed if there are multiple roots in the tree.
            roots.append(node) if parent is None and node not in roots else roots

            # Get the node position. ie,
            # determines if the node is 1st child or 2nd child and so on,
            # For root node child_pos denote the index of roots.

            if parent:

                node_pos = idx + 1
                str(node_pos).zfill(4)
                data[child]['node_pos'] = make_str(node_pos)
            else:

                # Make parent None.
                data[node]['parent'] = None

                # Get the position of root.
                root_pos = roots.index(node) + 1

                # Get the child position for each of its children.
                node_pos = idx + 1
                data[node]['node_pos'] = make_str(root_pos)
                data[child]['node_pos'] = make_str(node_pos)

    return data


def get_depth_path(data):
    """
    Get depth of the node and the full path upto the node.
    """
    for node, attrs in data.items():
        path = get_path(node, data)
        depth = get_depth(node, data)

        attrs['depth'] = depth
        attrs['path'] = path
        data[node] = attrs
    return data


def get_depth(node, data, depth=1):
    """
    Recursively get depth.
    """
    attrs = data[node]
    parent = attrs['parent']

    if parent is None:
        return depth
    else:
        depth += 1
        return get_depth(parent, data, depth)


def get_path(node, data, path=''):
    """
    Recursively get path
    """
    attrs = data[node]

    parent = attrs['parent']
    node_pos = attrs['node_pos']

    if parent is None:
        path = node_pos + path
        return path

    else:
        path = node_pos + path
        return get_path(parent, data, path)


def add_keys_to_dict(store, **kwargs):
    d = {}

    for key in kwargs:
        if key == "children":
            d[key] = []
        else:
            d[key] = ""

    for k in store:
        store[k].update(d)
        store[k]['taxid'] = k

    return store


def update_children(data, children):
    for node, attrs in data.items():
        if node in children:
            attrs['children'].extend(children[node])
            data[node] = attrs
    return data


def read_test_data(fname):
    stream = csv.reader(open(fname), delimiter="\t")
    children = defaultdict(list)
    collect = defaultdict(dict)

    for row in stream:
        d = {}
        taxid, parent = row

        if not parent or taxid == parent:
            parent = 'root'

        d['taxid'] = taxid
        d['parent'] = parent
        d['children'] = []

        collect[taxid] = d
        children[parent].append(taxid)

        # update children
    store = update_children(collect, children)
    return store


def read_divisions(fname):
    """
    Reads NCBI divisions.dmp file and returns its contents as a dictionary.
    """
    store = defaultdict(dict)

    stream = csv.reader(open(fname), delimiter="|")

    for row in stream:
        div_id, code, name = row[:3]
        div_id, code, name = div_id.strip(), code.strip(), name.strip()

        d = {'div_id': div_id, 'code': code, 'name': name}

        store[div_id] = d

    return store


def read_nodes(fname):
    """
    Reads nodes.dmp file and extracts node, its rank, parent and its children
    Returns a dictionary of dictionaries.
    """

    store = defaultdict(dict)
    children = defaultdict(list)
    stream = csv.reader(open(fname), delimiter="|")

    for row in stream:

        d = dict()
        taxid, parent, rank, embl_code, div_id = row[:5]
        taxid, parent, rank = taxid.strip(), parent.strip(), rank.strip()
        embl_code, div_id = embl_code.strip(), div_id.strip()

        if not parent or taxid == parent:
            parent = 'root'

        children[parent].append(taxid)
        d['taxid'] = int(taxid)
        d['rank'] = rank
        d['parent'] = parent
        d['children'] = []
        d['div_id'] = int(div_id)
        store[taxid] = d

    # update children
    store = update_children(store, children)

    store = add_node_attrs(store)

    return store


def read_names(fname):
    stream = csv.reader(open(fname), delimiter="|")
    store = []

    for row in stream:
        row = [x.strip() for x in row]
        taxid, name_txt, uniq_name, name_class = row[:4]

        d = {'taxid': taxid, 'name_txt': name_txt, 'uniq_name': uniq_name, 'name_class': name_class}
        store.append(d)

    return store


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Process some integers.')

    parser.add_argument('--fname', type=str, help="""Add the contents of file into database.\n
    It is a two column file with node_id as the first column and parent_id as the second column.""")

    parser.add_argument('--nodes', type=str, help='Add the contents of nodes.dmp into database')

    parser.add_argument('--names', type=str, help='Add the contents of names.dmp into database')

    parser.add_argument('--test', action='store_true',
                        help='Run a test query using all three tree representations, and print results.')

    parser.add_argument('--divisions', type=str, help='Add the contents of divisions.dmp into database')

    args = parser.parse_args()

    fname = args.fname
    nodes = args.nodes
    names = args.names
    division = args.divisions
    test = args.test

    # Add to database after migrations are done.


    if nodes:
        data = read_nodes(fname=nodes)

    elif names:
        data = read_names(fname=names)

    elif fname:
        data = read_test_data(fname=fname)

    elif division:
        data = read_divisions(division)

    else:
        pass
