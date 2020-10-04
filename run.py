import django
import time, sys

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


def add_to_db(nodes='', names='', division=''):
    """
    Add contents of file to database
    """
    t0 = time.time()

    # Add taxa categories to database (from divisions.dmp)
    div_data = read_divisions(division)

    divisions = create_divisions(div_data, batch_size=LIMIT)

    # Add nodes from nodes.dmp to database.
    node_data = read_nodes(fname=nodes)

    t1 = time.time()
    t = t1 - t0
    print("\nTime for reading data :  {0:.3f} seconds".format(t))

    nodes = create_node(node_data, batch_size=LIMIT, divisions=divisions)

    # Add names from names.dmp to database.
    name_data = read_names(fname=names)

    names = create_names(name_data, batch_size=LIMIT)

    syn_data = []

    for item in name_data:
        taxid = item['taxid']
        uname = item['uniq_name']
        # node = nodes[int(taxid)]
        node = nodes[taxid]
        name = names[uname]
        syn_data.append([node, name])

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


def create_synonyms(data, batch_size=LIMIT):
    gen_syn = gen_synonyms(data)

    print(f"\nStarting to create Synonyms at time {time.ctime()}")
    t0 = time.time()

    objs = Synonym.objects.bulk_create(objs=gen_syn, batch_size=batch_size)

    t1 = time.time()
    t = t1 - t0
    print("\nTime taken is {0:.3f} seconds".format(t))
    return objs


def gen_synonyms(data_list):
    for item in data_list:
        node, name = item
        yield Synonym(node=node, name=name)


def create_names(data, batch_size=LIMIT):
    # gen_nam = gen_names(data, nodes, collect)
    gen_nam = gen_names(data)

    print(f"\nStarting to create Names at time {time.ctime()}")
    t0 = time.time()

    Name.objects.bulk_create(objs=gen_nam, batch_size=batch_size)

    t1 = time.time()
    t = t1 - t0
    print("\nTime taken is {0:.3f} seconds".format(t))

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


def create_node(data, batch_size=LIMIT, divisions={}):
    """
    Insert nodes into database without any relationships.
    Returns a dictionary of dictionaries with id and children for each node.
    """
    gen = gen_node(data, divisions)

    t0 = time.time()
    print(f"\nStarting to create Nodes at time {time.ctime()}")

    objs = Node.objects.bulk_create(objs=gen, batch_size=batch_size)

    t1 = time.time()
    print(f"Completed data insert to Nodes  at time {time.ctime()}")

    t = t1 - t0
    print("\nTime taken is {0:.3f} seconds".format(t))

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


def get_desc_scientific_name(taxid):
    node = Node.objects.filter(tax_id=taxid).first()
    path = node.path
    depth = node.depth

    names = Synonym.objects.filter(node__path__startswith=path, node__depth__gte=depth,
                                   name__name_class="scientific name").order_by('node__path')

    return names


def list_ids(ids):
    """
    Input ids is a string of taxids separated by comma.
    Lists all descendant taxids.
    """
    taxids = ids.split(",")

    for taxid in taxids:
        desc = get_desc(taxid)
        for d in desc:
            print(d.tax_id)
        print()


def list_names(ids):
    """
    Input ids is a string of taxids separated by comma.
    Lists all descendant taxids and their scientific names.
    """
    taxids = ids.split(",")

    for taxid in taxids:
        desc = get_desc_scientific_name(taxid)
        for d in desc:
            print(d.node.tax_id, d.name.unique_name)
        print()


def list_ranks(ids):
    """
    Input ids is a string of taxids separated by comma.
    Lists all descendant taxids and their ranks in the lineage.
    """
    taxids = ids.split(",")

    for taxid in taxids:
        desc = get_desc(taxid)
        for d in desc:
            print(d.tax_id, d.rank)
        print()


def list_all(ids):
    """
    Input ids is a string of taxids separated by comma.
    Lists all descendant taxids, their ranks in the lineage and their scientific names.
    """
    taxids = ids.split(",")

    for taxid in taxids:
        desc = get_desc_scientific_name(taxid)
        for d in desc:
            print(d.node.tax_id, d.node.rank, d.name.unique_name)
        print()


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
