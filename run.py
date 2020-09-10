import django
import time, os, pprint
from collections import defaultdict


from django.conf import settings
from django.core.management import call_command

DATABASE_NAME = "taxa1.db"

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

LIMIT =10000

def add_to_db(fname):
    """
    Add contents of file to database
    """
    data = create_data(MPtree, fname, batch_size=LIMIT)
    1/0
    # data = make_relations_dict(data)
    #
    # fileds =  ['parent', 'depth', 'numchild', 'path']
    # update_data(MPtree,fileds,data, batch_size=LIMIT )


    #populate_db(MPtree, fname)
    #populate_db(NStree, fname)
    #populate_db(ALtree, fname)
    return


def create_data(model,fname, batch_size=LIMIT):
    """
    Insert nodes into database without any relationships.
    Returns a dictionary of dictionaries with id and children for each node.
    """

    store = defaultdict(list)
    stream = csv.reader(open(fname), delimiter="\t")
    gen = gen_data(stream=stream, store=store)
    nodes =model.objects.bulk_create(objs=gen, batch_size=batch_size)
    print(f"{len(nodes)} nodes inserted.")

    return store

def gen_data(stream, store = {}):

    for idx,row in enumerate(stream):
        name, parent = row

        if not parent or name == parent:
            parent = 'root'

        store[parent].append(name)
        yield MPtree(name=name, parent= parent, depth=1, path=idx+1, numchild=0)


def update_data(model, fields, data, batch_size):
    gen = gen_update(model, data)
    model.objects.bulk_update(objs=gen, fields=fields, batch_size=batch_size)


def make_relations_dict(data):
    """
    Create a dictionary where each node has its parent and num of children.
    """
    def get_parents(data):
        p = {}
        for parent, children in data.items():
            if len(children) == 1:
                name = children[0]
                p[name] = parent

            else:
                for child in children:
                    p[child] = parent
        return p

    # get the parent of each node.
    relations = get_parents(data)

    # ge the numchild for each node.
    for node, parent in relations.items():
        k = {}
        numchild = len(data[node]) if node in data else 0
        k['numchild'] = numchild
        k['parent'] = parent
        relations[node] = k
        
    return relations

def make_dict(**kwargs):
    d={}
    #print(parent, depth, path, numchild)
    d['path'] = kwargs.get('path')
    d['depth'] = kwargs.get('depth')
    d['numchild'] = kwargs.get('numchild')
    d['parent'] = kwargs.get('parent')
    return d


def gen_update(model, data):
    attrs = dict()

    nodes = {node.id :node for node in model.objects.all()}

    for id, node in nodes.items():
        name = node.name
        # get numchild
        numchild = data[name]['numchild']
        parent = data[name]['parent']

        print(id, name, parent, numchild)

        if parent == "root":
            depth, path = 1, '0001'
            attrs[name] = make_dict(parent=parent, depth=depth, numchild=numchild, path=path)
        else:
            path = int(attrs[parent]['path']) + 1
            depth =  attrs[parent]['depth'] + 1
            attrs[node] = make_dict(parent=parent, depth=depth, numchild=numchild, path= path)

        node.parent = parent
        node.depth = depth
        node.numchild = numchild
        node.path = path
        yield node


def read_data(fname):
    """
    Reads a csv file and returns it as a list of dictionaries.
    """
    stream = csv.reader(open(fname), delimiter="\t")

    store = list()
    for idx, row in enumerate(stream):
        name, parent = row

        if not parent or name == parent:
            parent = None

        d = dict()
        # d['id'], d['name'], d['parent'] = idx + 1, name, parent
        d['name'], d['parent'] = name, parent
        store.append(d)

    return store


def remove_empty(obj):
    """
    Input is a list of dictionaries where one key is 'children'
    If the value of key 'children' is [], then it will be removed recursively.
    The function returns the filtered list of dictionaries.
    """
    for i in obj:
        if not i['children']:
            del i['children']
        else:
            remove_empty(i['children'])
    return obj


def make_data_struct(data):
    """
    Makes treebeard specific data structure for bulk load.
    from a list of dictionaries.
    """

    data_map = {}
    for dat in data:
        d = {'name': dat['name']}
        # d = dat['name']
        data_map[dat['name']] = {'data': d, 'children': []}

    data_tree = []
    for dat in data:

        if dat['parent'] == dat['name'] or dat['parent'] is None:
            data_tree.append(data_map[dat['name']])

        else:
            parent = data_map[dat['parent']]
            parent['children'].append(data_map[dat['name']])

    # remove empty list
    data_tree = remove_empty(data_tree)
    return data_tree


def printer(funct):
    t0 = time.time()

    objs = funct()

    # for o in objs:
    #     foo = o.name
    #     print(foo)

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
         print("***",parent)
         print(node_struct)
         print(node_data)
         1/0



def populate_db(model, fname):
    # Read data as a list of dictionaries.
    data = read_data(fname)

    # Create a nested dictionary with keys 'data' and 'children' as required by treebeard load_bulk()
    data_tree = make_data_struct(data)
    test_tree(data_tree)


    # load data
    print(f"Populating {model.__name__}.")
    t0 = time.time()
    model.load_bulk(bulk_data=data_tree, parent=None)
    #relns_dict= bulk_create()
    #update_tree(relns_dict)
    t1 = time.time()
    final = t1 - t0

    #print(f"{model.__name__} is now populated.")
    print("Done. Time taken : {0:.3f} seconds.".format(final))
    return


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
    anc = node.get_ancestors

    # Print time
    printer(children)
    printer(desc)
    printer(anc)
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
        node = "12333"
        run_queries(MPtree, node)
        print("---------------------")
        run_queries(NStree, node)
        print("---------------------")
        run_queries(ALtree, node)
