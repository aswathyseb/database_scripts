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

from taxonomy_modeling.models import MPtree
from read import read_taxa

LIMIT = 500


def add_to_db(fname='', nodes='', names=''):
    """
    Add contents of file to database
    """

    t0 = time.time()
    data = read_taxa(fname=fname, nodes=nodes, names=names,)

    #for k, v in data.items():
    #     print(k, v)

    t1 = time.time()
    t = t1 - t0
    print("\nTime for reading data :  {0:.3f} seconds".format(t))
    create_data(MPtree, data, batch_size=LIMIT)

    return


def create_data(model, data, batch_size=LIMIT):
    """
    Insert nodes into database without any relationships.
    Returns a dictionary of dictionaries with id and children for each node.
    """
    gen = gen_data(data)

    t0 = time.time()
    print(f"\nStarting to create data at time {time.ctime()}")

    nodes = model.objects.bulk_create(objs=gen, batch_size=batch_size)

    t1 = time.time()
    print(f"Completed data insert at time {time.ctime()}")

    t = t1 - t0
    print("\nTime taken is {0:.3f} seconds".format(t))

    return nodes


def gen_data(data):

    for node, attrs in data.items():
        taxid = attrs['taxid']
        numchild = attrs['numchild']
        path = attrs['path']
        depth = attrs['depth']

        yield MPtree(taxid=taxid, depth=depth, path=path, numchild=numchild)


def printer(funct):
    t0 = time.time()

    objs = funct()

    for o in objs:
        foo = o.taxid
        print(foo)

    t1 = time.time()
    final = t1 - t0

    print(f"{funct.__name__}:", "{0:.3f} seconds".format(final))
    print(len(objs), "Total objects")
    print()


def run_queries(modelname, node):
    """
        Run simple queries to test performance of different tree representations.
    """

    # Select all descendants of a given node.
    taxid = node
    node = modelname.objects.filter(taxid=taxid)[0]

    # Functions to test.
    #print(f"Testing {modelname.__name__}\n")
    #children = node.get_children
    desc = node.get_descendants
    # anc = node.get_ancestors

    # Print time
    #printer(children)
    #print("------------")
    printer(desc)
    # printer(anc)
    return


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Process some integers.')

    parser.add_argument('--makemigrations', action='store_true',
                        help='Used to create migration files when models are changed in app.')

    parser.add_argument('--migrate', action='store_true',
                        help='Apply migrations to database')

    parser.add_argument('--test', action='store_true',
                        help='Run a test query and print results.')

    parser.add_argument('--add', action='store_true',
                        help="""Add data to the database. See --nodes_file, --names_file, 
                        divisions_file to add data.""")

    parser.add_argument('--fname', type=str, help="""Add the contents of file into database.\n
    It is a two column file with node_id as the first column and parent_id as the second column.""")

    parser.add_argument('--nodes', type=str, help='Add the contents of nodes.dmp into database')

    parser.add_argument('--names', type=str, help='Add the contents of names.dmp into database')

    parser.add_argument('--divisions', type=str, help='Add the contents of divisions.dmp into database')


    args = parser.parse_args()

    makemig = args.makemigrations
    migrate = args.migrate
    add = args.add

    fname = args.fname
    nodes = args.nodes
    names = args.names
    division = args.divisions
    test = args.test

    # Make any migrations neccessary first.
    if makemig:
        call_command('makemigrations', 'taxonomy_modeling')

    # Apply any migrations that might have been made
    if migrate:
        call_command('migrate', 'taxonomy_modeling')


    # Add to database after migrations are done.
    if add:
        if nodes and names:
            add_to_db(nodes=nodes, names=names)

        elif nodes:
            add_to_db(nodes=nodes)

        elif names:
            add_to_db(names=names)

        elif fname:
            add_to_db(fname=fname)

        else:
            print("No data given")

    # Test queries once database is populated.
    if test:
        node = "10239" #"9605"
        run_queries(MPtree, node)
        # print("---------------------")
        # run_queries(NStree, node)
        # print("---------------------")
        # run_queries(ALtree, node)
