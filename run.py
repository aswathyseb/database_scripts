import django
import time

from django.conf import settings
from django.core.management import call_command

DATABASE_NAME = "taxa.db"

settings.configure(
    DEBUG=True,
    # Set the installed app
    INSTALLED_APPS=(
        'django.contrib.contenttypes',
        'taxa',
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

from taxa.models import *
from read import read_nodes, read_divisions, read_names

LIMIT = 500


def add_to_db(fname='', nodes='', names='', division=''):
    """
    Add contents of file to database
    """
    t0 = time.time()

    # Add taxa categories to database (from divisions.dmp)
    cdict = read_divisions(division)

    create_divisions(Division, cdict, batch_size=LIMIT)

    divisions = {c.division_id: c for c in Division.objects.all()}

    node_data = read_nodes(fname=nodes)

    # for k, v in node_data.items():
    #     print(k, v)
    # 1/0

    t1 = time.time()
    t = t1 - t0
    print("\nTime for reading data :  {0:.3f} seconds".format(t))

    create_node(Node, node_data, batch_size=LIMIT, divisions=divisions)

    nodes = {n.tax_id: n for n in Node.objects.all()}

    name_data = read_names(fname=names)

    create_names(Name, name_data, batch_size=LIMIT, nodes=nodes)

    return


def create_divisions(model, data, batch_size=LIMIT):
    gen_div = gen_divisions(data)
    model.objects.bulk_create(objs=gen_div, batch_size=batch_size)
    return


def gen_divisions(data):
    for id, attrs in data.items():
        div_id = data[id]['div_id']
        code = data[id]['code']
        name = data[id]['name']
        yield Division(division_id=div_id, code=code, name=name)


def create_names(model, data, batch_size=LIMIT, nodes={}):
    gen_nam = gen_names(data, nodes)

    print(f"\nStarting to create Names at time {time.ctime()}")
    t0 = time.time()

    model.objects.bulk_create(objs=gen_nam, batch_size=batch_size)

    t1 = time.time()
    t = t1 - t0
    print("\nTime taken is {0:.3f} seconds".format(t))
    return


def gen_names(data, nodes):
    node, name_txt, uniq_name, name_class = "", "", "", ""

    for item in data:
        taxid = item['taxid']
        name_txt = item['name_txt']
        uniq_name = item['uniq_name']
        name_class = item['name_class']
        node = nodes[int(taxid)]
        yield Name(node=node, name_txt=name_txt,
                   unique_name=uniq_name, name_class=name_class)


def create_node(model, data, batch_size=LIMIT, divisions={}):
    """
    Insert nodes into database without any relationships.
    Returns a dictionary of dictionaries with id and children for each node.
    """
    gen = gen_node(data, divisions)

    t0 = time.time()
    print(f"\nStarting to create Nodes at time {time.ctime()}")

    model.objects.bulk_create(objs=gen, batch_size=batch_size)

    t1 = time.time()
    print(f"Completed data insert to Nodes  at time {time.ctime()}")

    t = t1 - t0
    print("\nTime taken is {0:.3f} seconds".format(t))

    return


def gen_node(data, divisions):
    for node, attrs in data.items():
        taxid = attrs['taxid']
        numchild = attrs['numchild']
        # parent = attrs['parent']
        path = attrs['path']
        depth = attrs['depth']
        rank = attrs['rank']
        division = divisions[attrs['div_id']]

        yield Node(tax_id=taxid, depth=depth, path=path, numchild=numchild,
                   division=division, rank=rank)


def printer(funct):
    t0 = time.time()

    objs = funct()

    for o in objs:
        foo = o.tax_id
        print(foo)

    t1 = time.time()
    final = t1 - t0

    print(f"{funct.__name__}:", "{0:.3f} seconds".format(final))
    print(len(objs), "Total objects")
    print()


def get_desc(taxid):
    node = Node.objects.filter(tax_id=taxid).first()
    path = node.path
    depth = node.depth
    return Node.objects.filter(path__startswith=path, depth__gte=depth).order_by('path')


def get_desc_name(taxid):
    node = Node.objects.filter(tax_id=taxid).first()
    path = node.path
    depth = node.depth
    names = Name.objects.filter(name_class='scientific name', node__path__startswith=path, node__depth__gte=depth)
    return names


def run_queries(taxid):
    # # Get all desc
    # t0 = time.time()
    # desc = get_desc(taxid)
    # print(f'{len(desc)} objects.')
    # for d in desc:
    #     print(d.tax_id)
    # t1 = time.time()
    # t = t1 - t0
    # print("Time taken", " {0:.3f} seconds".format(t))
    #
    # print("-------------------")
    # # Get all desc with name.

    t0 = time.time()
    desc_name = get_desc_name(taxid)
    print(f'{len(desc_name)} objects.')
    for d in desc_name:
        print(d.node.tax_id, d.name_txt)
    t1 = time.time()
    t = t1 - t0
    print("Time taken", " {0:.3f} seconds".format(t))

    return


def run_queries1(modelname, node):
    """
        Run simple queries to test performance of different tree representations.
    """

    # Select all descendants of a given node.
    taxid = node
    node = modelname.objects.filter(tax_id=taxid).first()

    # Get parent
    # parent_path=node.path[:len(node.path)- 4]
    # parent_node = modelname.objects.filter(path=parent_path).first()
    # print(parent_node.taxid)


    # Functions to test.
    # children = node.get_children
    # desc = node.get_descendants
    desc = get_desc(modelname, node)
    # anc = node.get_ancestors

    # Print time
    # printer(children)
    # printer(desc)
    # 1/0
    # printer(anc)
    t0 = time.time()

    for d in desc:
        print(d.tax_id)
    t1 = time.time()
    t = t1 - t0
    print(f"{len(desc)} objects\n.Time taken is {t}")

    return


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Process some integers.')

    parser.add_argument('--makemigrations', action='store_true',
                        help='Used to create migration files when models are changed in app.')

    parser.add_argument('--migrate', action='store_true',
                        help='Apply migrations to database')

    parser.add_argument('--add', action='store_true',
                        help="""Add data to the database. See --nodes_file, --names_file, 
                            divisions_file to add data.""")

    parser.add_argument('--taxid', type=str, help='Input a taxid to test')

    parser.add_argument('--fname', type=str, help="""Add the contents of file into database.\n
    It is a two column file with node_id as the first column and parent_id as the second column.""")

    parser.add_argument('--nodes', type=str, help='Add the contents of nodes.dmp into database')

    parser.add_argument('--names', type=str, help='Add the contents of names.dmp into database')

    parser.add_argument('--test', action='store_true',
                        help='Run a test query using all three tree representations, and print results.')

    parser.add_argument('--divisions', type=str, help='Add the contents of divisions.dmp into database')

    args = parser.parse_args()

    makemig = args.makemigrations
    migrate = args.migrate

    fname = args.fname
    nodes = args.nodes
    names = args.names
    division = args.divisions
    test = args.test
    add = args.add
    taxid = args.taxid

    # Make any migrations neccessary first.
    if makemig:
        call_command('makemigrations', 'taxa')

    # Apply any migrations that might have been made
    if migrate:
        call_command('migrate', 'taxa')

    # Add to database after migrations are done.
    if add:

        if nodes and names and division:
            add_to_db(nodes=nodes, names=names, division=division)

        elif nodes and names:
            add_to_db(nodes=nodes, names=names)

        elif nodes:
            add_to_db(nodes=nodes)

        elif names:
            add_to_db(names=names)

        elif fname:
            add_to_db(fname=fname)

        elif division:
            add_to_db(division=division)

        else:
            print("No data given.")

    # Test queries once database is populated.
    if test:
        taxid = taxid.strip() if taxid else "9605"
        run_queries(taxid)

        # run_queries(Node, taxid)
        # run_queries(taxid)
        # print("---------------------")
        # run_queries(NStree, node)
        # print("---------------------")
        # run_queries(ALtree, node)
