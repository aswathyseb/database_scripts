import django

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
from taxonomy_modeling.models import NStree
import csv


def add_to_db(fname):
    """
    Add contents of file to database
    """
    populate_db(MPtree, fname)
    populate_db(NStree, fname)
    return


def test_queries():
    """
    Run simple queries to test performance of different tree representations.
    """

    print("Queries with MPtree")
    run_queries(MPtree, "Chuck")

    print("\n\nQueries with NStree")
    queries_NStree(NStree, "Chuck")

    return


def populate_db(model, fname):
    get = lambda node_id: model.objects.get(pk=node_id)

    stream = csv.DictReader(open(fname), delimiter="\t")

    for row in stream:

        print("Row is ", row)

        # Add root
        if row['boss'] == 'NULL':
            root = model.add_root(name=row['name'])
            print("root is", root)
        else:
            boss = model.objects.filter(name=row['boss'])
            for b in boss:
                print("Boss is ", get(b.pk))
                node = get(b.pk).add_child(name=row['name'])
                print("Node is ", node)

    return


def populate_MPtree(fname):
    get = lambda node_id: MPtree.objects.get(pk=node_id)

    stream = csv.DictReader(open(fname), delimiter="\t")

    for row in stream:

        print("Row is ", row)

        # Add root
        if row['boss'] == 'NULL':
            root = MPtree.add_root(name=row['name'])
            print("root is", root)
        else:
            boss = MPtree.objects.filter(name=row['boss'])
            for b in boss:
                print("Boss is ", get(b.pk))
                node = get(b.pk).add_child(name=row['name'])
                print("Node is ", node)

    return


def run_queries(modelname, node_name):
    """
        Run simple queries to test performance of different tree representations.
    """

    # Select all descendants of a given node.
    name = node_name
    node = modelname.objects.filter(name=name)[0]

    print(f"Children of {name}")
    children = node.get_children()
    for c in children:
        print(c.name)

    print("----------------------")
    print(f"Descendents of {name}")
    desc = node.get_descendants()
    for d in desc:
        print(d.name)

    print("----------------------")

    # Select all ancestors of a given node.
    anc = node.get_ancestors()
    print(f"All ancestors of {name}")
    for a in anc:
        print(a.name)

    print("----------------------")
    # Get parents of a given node.
    p = node.get_parent()
    print(f"Parent of {name}")
    print(p.name)

    return


def queries_NStree(modelname, node_name):
    """
        Run simple queries to test performance of different tree representations.
        """
    # Select all descendants of a given node.
    name = node_name
    node = modelname.objects.filter(name=name)[0]

    print(name, node)
    left = node.lft
    right = node.rgt
    print(f"Left and right are {left} and {right}")

    print("--------------------")

    run_queries(modelname, node_name)

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
        test_queries()
