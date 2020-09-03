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

from taxonomy_modeling.models import MPtree, NStree, ALtree
import csv


def add_to_db(fname):
    """
    Add contents of file to database
    """
    populate_db(MPtree, fname)
    populate_db(NStree, fname)
    populate_db(ALtree, fname)
    return


def printer(funct):
    t0 = time.time()

    objs = funct()

    #for o in objs:
    #    foo = o.name

    t1 = time.time()
    final = t1 - t0

    print(f"{funct.__name__}:", "{0:.3f} microseconds".format(final))
    print(len(objs), "Total objects")
    print()


def populate_db(model, fname):
    get = lambda node_id: model.objects.get(pk=node_id)

    stream = csv.DictReader(open(fname), delimiter="\t")

    for row in stream:

        # Add root
        if row['boss'] == 'NULL':
            root = model.add_root(name=row['name'])
        else:
            boss = model.objects.filter(name=row['boss'])
            for b in boss:
                node = get(b.pk).add_child(name=row['name'])

    print(f"{model.__name__} is now populated")
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
        run_queries(MPtree, "Chuck")
        print("---------------------")
        run_queries(NStree, "Chuck")
        print("---------------------")
        run_queries(ALtree, "Chuck")

