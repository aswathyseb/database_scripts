import django
import time, sys
from functools import wraps

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


def time_it(func):
    @wraps(func)
    def timer(*args, **kwargs):
        start = time.time()
        try:
            return func(*args, **kwargs)
        finally:
            end = time.time()
            diff = int(round((end - start), 1)) or 0.1
            print(f"{func.__name__} execution time: {diff} seconds")

    return timer


def add_to_db(nodes='', names='', division=''):
    """
    Add contents of file to database
    """

    # Read taxa divisions from divisions.dmp.
    div_data = read_divisions(division)

    # Create divisions table.
    divisions = create_divisions(div_data, batch_size=LIMIT)

    # Read nodes from nodes.dmp.
    node_data = read_nodes(fname=nodes)

    # Create nodes table.
    nodes = create_node(node_data, batch_size=LIMIT, divisions=divisions)

    # Reads names from names.dmp.
    name_data = read_names(fname=names)

    # Create names table.
    names = create_names(name_data, batch_size=LIMIT)

    # Get the node object corresponding to name for the creation of synonym table.
    syn_data = []

    for item in name_data:
        taxid = item['taxid']
        uname = item['uniq_name']
        # node = nodes[int(taxid)]
        node = nodes[taxid]
        name = names[uname]
        syn_data.append([node, name])

    # Create synonym table.
    create_synonyms(syn_data, batch_size=LIMIT)

    print("Done!")

    return


def create_divisions(data, batch_size=LIMIT):
    gen_div = gen_divisions(data)
    objs = Division.objects.bulk_create(objs=gen_div, batch_size=batch_size)

    # create a  dictionary of objects for easy lookup
    divisions = {c.division_id: c for c in objs}

    return divisions


def gen_divisions(data):
    for id, attrs in data.items():
        div_id = data[id]['div_id']
        code = data[id]['code']
        name = data[id]['name']
        yield Division(division_id=div_id, code=code, name=name)


@time_it
def create_synonyms(data, batch_size=LIMIT):
    gen_syn = gen_synonyms(data)

    objs = Synonym.objects.bulk_create(objs=gen_syn, batch_size=batch_size)
    return objs


def gen_synonyms(data_list):
    for item in data_list:
        node, name = item
        yield Synonym(node=node, name=name)


@time_it
def create_names(data, batch_size=LIMIT):
    # Generate names.
    gen_nam = gen_names(data)

    Name.objects.bulk_create(objs=gen_nam, batch_size=batch_size)

    #  Name.objects.all() is used instead of the bulk_create returned list of objects
    # because an AutoField primary key will not be set in bulk_create returned objects
    # if the database is sqlite3. This is not the case with postgresql.

    names = {n.unique_name: n for n in Name.objects.all()}

    return names


def gen_names(data):
    for item in data:
        # taxid = item['taxid']
        uniq_name = item['uniq_name']
        name_class = item['name_class']
        # node = nodes[int(taxid)]
        name = Name(unique_name=uniq_name, name_class=name_class)
        # collect.append([node, name])
        # collect.append([taxid, name])

        yield name


@time_it
def create_node(data, batch_size=LIMIT, divisions={}):
    """
    Insert nodes into database without any relationships.
    Returns a dictionary of dictionaries with id and children for each node.
    """
    # Generate node.
    gen = gen_node(data, divisions)

    objs = Node.objects.bulk_create(objs=gen, batch_size=batch_size)

    # Create a dictionary for easy lookup
    nodes = {n.tax_id: n for n in objs}

    return nodes


def gen_node(data, divisions):
    for node, attrs in data.items():
        taxid = attrs['taxid']
        numchild = attrs['numchild']
        path = attrs['path']
        depth = attrs['depth']
        rank = attrs['rank']
        # division = divisions[attrs['div_id']]
        division = divisions[str(attrs['div_id'])]

        yield Node(tax_id=taxid, depth=depth, path=path, numchild=numchild,
                   division=division, rank=rank)


def list_values(ids):
    """
    Query that returns taxid, rank and scientific-name for all descendants of each taxid.
    Input is comma separated string of taxids
    """

    # Get the arguments

    taxids = ids.split(",")

    # Extract node.path and node.depth for each taxid.
    nodes = Node.objects.filter(tax_id__in=taxids).values('path', 'depth')

    for node in nodes:
        # Get the path and depth of each node.
        path = node['path']
        depth = node['depth']

        # Get all descendants and their scientific name.
        names = Synonym.objects.filter(node__path__startswith=path, node__depth__gte=depth,
                                       name__name_class="scientific name").order_by('node__path').values('node__tax_id',
                                                                                                         'node__rank',
                                                                                                         'name__unique_name')
        yield names


@time_it
def list_names(ids):
    """
    Input is a comma separated list of taxids.
    Prints all descendant taxids and their names.
    """
    objs = list_values(ids)

    for obj in objs:
        for o in obj:
            print(o['node__tax_id'], o['name__unique_name'])
        print()


@time_it
def list_all(ids):
    """
    Input is a comma separated list of taxids.
    Prints all descendant taxids and their names.
    """
    objs = list_values(ids)

    for obj in objs:
        for o in obj:
            print(o['node__tax_id'], o['node__rank'], o['name__unique_name'])
        print()


@time_it
def list_ranks(ids):
    taxids = ids.split(",")

    nodes = Node.objects.filter(tax_id__in=taxids).values('path', 'depth')

    for node in nodes:
        # Get the path and depth of each node.
        path = node['path']
        depth = node['depth']
        vals = Node.objects.filter(path__startswith=path, depth__gte=depth).order_by('path').values('tax_id', 'rank')

        for v in vals:
            print(v['tax_id'], v['rank'])


@time_it
def list_ids(ids):
    taxids = ids.split(",")

    nodes = Node.objects.filter(tax_id__in=taxids).values('path', 'depth')

    for node in nodes:
        # Get the path and depth of each node.
        path = node['path']
        depth = node['depth']
        vals = Node.objects.filter(path__startswith=path, depth__gte=depth).order_by('path').values('tax_id')

        for v in vals:
            print(v['tax_id'])


def list_commands(args):
    if (args.show_name or args.show_rank) and not (args.ids):
        print("\nids are required")
        sys.exit()

    if args.show_name and args.show_rank and args.ids:
        list_all(args.ids)
        return

    if args.show_name and args.ids:
        list_names(args.ids)
        return

    if args.show_rank and args.ids:
        list_ranks(args.ids)
        return

    if args.ids:
        list_ids(args.ids)

    return


def db_commands(args):
    # Make any migrations necessary first.
    if args.makemigrations:
        call_command('makemigrations', 'taxa')

    # Apply any migrations that might have been made
    if args.migrate:
        call_command('migrate', 'taxa')

    # Add to database after migrations are done.
    if args.add and not (args.nodes and args.names and args.divisions):
        print("\n--nodes, --names, --divisions must be specified with add.")
        sys.exit()

    if args.add and args.nodes and args.names and args.divisions:
        add_to_db(nodes=args.nodes, names=args.names, division=args.divisions)

    return


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(usage="python run.py <command>")

    # General sub-command parser object
    subparsers = parser.add_subparsers(title='subcommands', description='valid subcommands', dest="cmd")

    # Specific sub-command parsers
    parser_db = subparsers.add_parser('database', help="Database operations")
    parser_list = subparsers.add_parser('list', help="List taxon tree of given taxids")

    # Assign the execution functions
    parser_list.set_defaults(func=list_commands)
    parser_db.set_defaults(func=db_commands)

    # Add database command-specific options
    parser_db.add_argument('--makemigrations', action='store_true',
                           help='Used to create migration files when models are changed in app.')

    parser_db.add_argument('--migrate', action='store_true',
                           help='Apply migrations to database')

    parser_db.add_argument('--add', action='store_true',
                           help="""Add data to the database. --nodes, --names and --divisions must be specified along with this.""")

    parser_db.add_argument('--nodes', type=str, help='Add the contents of nodes.dmp into database')

    parser_db.add_argument('--names', type=str, help='Add the contents of names.dmp into database')

    parser_db.add_argument('--divisions', type=str, help='Add the contents of divisions.dmp into database')

    # Add list command-specific options
    parser_list.add_argument('--ids', type=str, help="""Input taxid. Multiple values should be separated by comma.""")

    parser_list.add_argument('-n', '--show_name', action='store_true', help='Output scientific name.')
    parser_list.add_argument('-r', '--show_rank', action='store_true', help='Output rank.')

    # Parse arguments
    args = parser.parse_args()

    # Invoke the function
    args.func(args)
