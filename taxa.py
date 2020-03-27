from peewee import *
import os, csv
import itertools
import plac, sys

dbname = "test.db"
db = SqliteDatabase(dbname)

LIMIT = 2000
SIZE = 1000


class Nodes(Model):
    tax_id = IntegerField(primary_key=True)
    parent_id = IntegerField()
    rank = CharField()
    embl_code = CharField()
    division_id = CharField()

    class Meta:
        database = db


class Names(Model):
    tax_id = ForeignKeyField(Nodes, backref="names")
    name = CharField()

    class Meta:
        database = db


class Accessions(Model):
    acc = CharField(primary_key=True)
    acc_version = CharField()
    tax_id = ForeignKeyField(Nodes, backref="accession")
    gi_id = IntegerField()

    class Meta:
        database = db


def create():
    # Create Index
    idx1 = Names.index(Names.tax_id)
    Names.add_index(idx1)
    idx2 = Names.index(Names.name)
    Names.add_index(idx2)
    #
    idx3 = Accessions.index(Accessions.acc)
    Accessions.add_index(idx3)
    idx4 = Accessions.index(Accessions.tax_id)
    Accessions.add_index(idx4)

    db.connect()
    db.create_tables([Nodes, Names, Accessions])
    return


def path(*args):
    return os.path.join(*args)


def strip(t):
    return t.strip()


def parse(fname, delimiter="\t"):
    stream = csv.reader(open(fname, "rt"), delimiter=delimiter)
    stream = map(lambda x: list(map(strip, x)), stream)
    return stream


def parse_nodes():
    fname = path(".", "data", "test.dmp")
    stream = parse(fname, "|")
    stream = itertools.islice(stream, LIMIT)

    def generate():
        for row in stream:
            tax_id, parent_id, = int(row[0]), int(row[1])
            rank, embl_code, division_id = row[2], row[3], row[4]
            entry = dict(tax_id=tax_id, parent_id=parent_id, rank=rank,
                         embl_code=embl_code, division_id=division_id)
            yield entry

    source = generate()

    print("*** reading nodes.dmp")
    load(Nodes, source, size=SIZE)


def load(klass, source, size):
    with db.atomic():
        num = 0
        for step, batch in enumerate(chunked(source, size)):
            num += len(batch)
            print(f"\r*** loading {num:,d} rows", end='')
            klass.insert_many(batch).execute()
    print("")


def parse_names():
    fname = path(".", "data", "names.dmp")
    stream = parse(fname, "|")
    stream = filter(lambda x: x[3] == "scientific name", stream)
    stream = itertools.islice(stream, LIMIT)

    def generate():
        for row in stream:
            tax_id, name = int(row[0]), row[1]
            entry = dict(tax_id=tax_id, name=name)
            yield entry

    source = generate()

    print("*** reading names.dmp")
    load(Names, source, size=SIZE)


def parse_accession():
    fname = path(".", "data", "acc.txt")
    stream = parse(fname, "\t")
    stream = itertools.islice(stream, LIMIT)

    def generate():
        next(stream)  # skip header
        for row in stream:
            acc, acc_version = row[0], row[1]
            tax_id, gi_id = int(row[2]), int(row[3])
            entry = dict(acc=acc, acc_version=acc_version,
                         tax_id=tax_id, gi_id=gi_id)
            yield entry

    source = generate()

    print("*** reading acc.txt")
    load(Accessions, source, size=SIZE)


def extract_acc_children(tax_id):
    """
    Extracts sequence accessions including the children of the given tax_id.
    """
    # tax_id = 1279
    query = (Names
             .select(Names.name, Nodes.parent_id, Nodes.tax_id, Accessions.acc)
             .join(Nodes)
             .where((Nodes.parent_id == tax_id) | (Nodes.tax_id == tax_id))
             .join(Accessions))

    if not query.exists():
        print(f"No children or No accessions found for {tax_id}")
        sys.exit()

    cursor = db.execute(query)

    header = ['name', 'parent', 'taxid', 'accession']
    print("\t".join(header))

    for name, parent_id, tax_id, acc in cursor:
        print("\t".join([name, str(parent_id), str(tax_id), str(acc)]))


def extract_acc(tax_id):
    """
    Extracts sequence accessions corresponding to a taxid.
    """
    query = Accessions.select(Accessions.acc, Accessions.tax_id).where(Accessions.tax_id == tax_id)

    if not query.exists():
        print(f"No accessions found for {tax_id}")
        sys.exit()

    cursor = db.execute(query)

    header = ['acession', 'taxid']
    print("\t".join(header))

    for acc, tid in cursor:
        print("\t".join([str(acc), str(tid)]))


def create_populate():
    create()
    parse_nodes()
    parse_names()
    parse_accession()


# Plac annotations:
#
# (help, kind, abbrev, type, choices, metavar)
#

@plac.annotations(
    taxid=("Input taxid"),
    child=("Specify to extract accessions including the children of taxid", "flag", "c"),
)
def run(taxid, child=False):
    "Prints all accessions corresponding to taxid"

    if not os.path.exists(dbname):
        create_populate()

    if taxid and child:
        extract_acc_children(tax_id=taxid)
        return
    if taxid:
        extract_acc(tax_id=taxid)
        return


if __name__ == "__main__":
    # create_populate()
    # extract_acc(tax_id=1282)
    # extract_acc_children(tax_id=1279)

    plac.call(run)
