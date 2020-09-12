import django
import time, os, pprint
from collections import defaultdict

from django.conf import settings
from django.core.management import call_command

DATABASE_NAME = "employee.db"

settings.configure(
    DEBUG=True,
    # Set the installed app
    INSTALLED_APPS=(
        'django.contrib.contenttypes',
        'taxonomy_modeling',
    ),

    # Configure the database
    DATABASES={
        "default": {
            "ENGINE": "django.db.backends.sqlite3",
            'NAME': DATABASE_NAME,
        }

    }
)

# Required to use.
django.setup()

# Import all local models once Django has been setup

from taxonomy_modeling.models import MPtree, NStree, ALtree
import csv

LIMIT = 500


def add_to_db(fname):
    """
    Add contents of file to database
    """
    data = create_data(MPtree, fname, batch_size=LIMIT)
    1/0

    # For every node, get its name, parent, no.of children,
    # its position among sibling, path and depth.

    data = make_relations(data)

    fileds = [ 'path', 'depth', 'numchild']
    update_data(MPtree, fileds, data, batch_size=LIMIT)

    return


def create_data(model, fname, batch_size=LIMIT):
    """
    Insert nodes into database without any relationships.
    Returns a dictionary of dictionaries with id and children for each node.
    """

    store = defaultdict(list)
    stream = csv.reader(open(fname), delimiter="\t")
    gen = gen_data(stream=stream, store=store)
    nodes = model.objects.bulk_create(objs=gen, batch_size=batch_size)
    print(f"{len(nodes)} nodes inserted.")

    return store


def gen_data(stream, store=defaultdict(list)):
    for idx, row in enumerate(stream):
        name, parent = row

        if not parent or name == parent:
            parent = 'root'

        store[parent].append(name)
        yield MPtree(name=name, depth=1, path=idx + 1, numchild=0)


def modify_data(data):
    """
    Takes a dictionary of lists with parents as keys and children as values.
    Returns a dictionary of dictionaries with node as the key.
    For every node, its name, parent, its position among children, number of children
    are returned.

    """

    store = {}
    roots = []
    for parent, children in data.items():
        parent = parent if parent != "root" else None

        def get_numchild(node, data):
            k = {}
            numchild = len(data[node]) if node in data else 0
            k['numchild'] = numchild
            k['name'] = node
            return k

        for idx, child in enumerate(children):
            d = get_numchild(child, data)
            d['parent'] = parent

            roots.append(child) if parent is None else roots

            # determines if its 1st child or 2nd child and so on,
            # for root node child_pos denote the index of roots.

            node_pos = idx + 1 if parent else roots.index(child) + 1
            d['node_pos'] = str(node_pos).zfill(4)

            #print(parent, child, node_pos)
            store[child] = d

    return store


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


def make_relations(data):
    info = modify_data(data)


    # for k, v in info.items():
    #     parent = v['parent']
    #     #siblings = info[parent]['numchild']
    #     print("\t".join([k, v['node_pos'], parent, str(siblings)]))
    # 1/0

    for node, attrs in info.items():
        path = get_path(node, info)
        depth = get_depth(node, info)

        attrs['depth'] = depth
        attrs['path'] = path
        info[node] = attrs

    return info


def update_data(model, fields, data, batch_size):

    # for k, v in data.items():
    #     print(k, v['path'])
    # 1/0

    gen = gen_update(model, data)
    model.objects.bulk_update(objs=gen, fields=fields, batch_size=batch_size)
    print(f"{model.__name__}  is updated successfully." )


def gen_update(model, data):

    nodes = [ node for node in model.objects.all()]

    for  node in nodes:
        name = node.name
        attrs = data[name]

        node.depth = attrs['depth']
        node.numchild = attrs['numchild']
        node.path = attrs['path']
        yield node


def printer(funct):
    t0 = time.time()

    objs = funct()

    for o in objs:
         foo = o.name
         print(foo)

    t1 = time.time()
    final = t1 - t0

    print(f"{funct.__name__}:", "{0:.3f} seconds".format(final))
    print(len(objs), "Total objects")
    print()


def test_tree(bulk_data):
    parent = None
    stack = [(parent, node) for node in bulk_data[::-1]]
    print(stack)
    while stack:
        parent, node_struct = stack.pop()
        node_data = node_struct['data'].copy()
        print("***", parent)
        print(node_struct)
        print(node_data)
        1 / 0


def run_queries(modelname, node_name):
    """
        Run simple queries to test performance of different tree representations.
    """

    # Select all descendants of a given node.
    name = node_name
    node = modelname.objects.filter(name=name)[0]

    # Functions to test.
    print(f"Testing {modelname.__name__}\n")
    children = node.get_children
    desc = node.get_descendants
    #anc = node.get_ancestors

    # Print time
    printer(children)
    printer(desc)
    #printer(anc)
    return


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('--makemigrations', action='store_true',
                        help='Used to create migration files when models are changed in app.')
    parser.add_argument('--migrate', action='store_true',
                        help='Apply migrations to database')

    parser.add_argument('--fname', type=str, help='Add the contents of file into database')

    parser.add_argument('--test', action='store_true',
                        help='Run a test query using all three tree representations, and print results.')

    args = parser.parse_args()

    makemig = args.makemigrations
    migrate = args.migrate

    fname = args.fname
    test = args.test

    # Make any migrations neccessary first.
    if makemig:
        call_command('makemigrations', 'taxonomy_modeling')

    # Apply any migrations that might have been made
    if migrate:
        call_command('migrate', 'taxonomy_modeling')

    # Add to database after migrations are done.
    if fname:
        add_to_db(fname=fname)

    # Test queries once database is populated.
    if test:
        node = "9605"
        run_queries(MPtree, node)
        #print("---------------------")
        #run_queries(NStree, node)
        #print("---------------------")
        #run_queries(ALtree, node)
