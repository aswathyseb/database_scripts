from peewee import *
import os, csv
import itertools
import plac, sys

dbname = "test.db"
db = SqliteDatabase(dbname)

LIMIT = 2000
SIZE = 1000


class BaseModel(Model):
    class Meta:
        database = db


class Nodes(BaseModel):
    tax_id = IntegerField(primary_key=True)
    parent_id = IntegerField()
    rank = CharField()
    embl_code = CharField()
    division_id = CharField()


class Names(BaseModel):
    name = CharField()
    taxa = ForeignKeyField(Nodes)


class Accessions(BaseModel):
    acc = CharField(primary_key=True)
    acc_version = CharField()
    gi_id = IntegerField()
    taxa = ForeignKeyField(Nodes)


def create():
    # Create Index
    Nodes.add_index(Nodes.rank)
    Names.add_index(Names.name)
    Accessions.add_index(Accessions.acc)

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


def load(klass, source, size):
    with db.atomic():
        num = 0
        for step, batch in enumerate(chunked(source, size)):
            num += len(batch)
            print(f"\r*** loading {num:,d} rows", end='')
            klass.insert_many(batch).execute()
    print("")


def parse_nodes(fname):
    fname = path(".", "data", fname)
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


def parse_names(fname):
    fname = path(".", "data", fname)
    stream = parse(fname, "|")
    stream = filter(lambda x: x[3] == "scientific name", stream)
    stream = itertools.islice(stream, LIMIT)

    def generate():
        for row in stream:
            tax_id, name = int(row[0]), row[1]
            entry = dict(taxa=tax_id, name=name)
            yield entry

    source = generate()

    print("*** reading names.dmp")
    load(Names, source, size=SIZE)


def parse_accession(fname):
    fname = path(".", "data", fname)
    stream = parse(fname, "\t")
    stream = itertools.islice(stream, LIMIT)

    def generate():
        next(stream)  # skip header
        for row in stream:
            acc, acc_version = row[0], row[1]
            tax_id, gi_id = int(row[2]), int(row[3])
            entry = dict(acc=acc, acc_version=acc_version,
                         taxa=tax_id, gi_id=gi_id)
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


def extract_names(tax_id):

    # Get Name, taxid
    query = (Names
             .select(Nodes, Names)
             .join(Nodes)
             .where(Names.taxa.tax_id == tax_id)

             )
    if not query.exists():
        print(f"No accessions found for {tax_id}")
        sys.exit()

    for c in query:
        print(c.name, c.taxa.tax_id)


def extract_acc(tax_id):
    """
    Extracts sequence accessions corresponding to a taxid.
    """

    query = (Accessions
             .select(Nodes, Accessions)
             .join( Nodes)
             .where(Accessions.taxa.tax_id == tax_id)
             )

    if not query.exists():
        print(f"No accessions found for {tax_id}")
        sys.exit()

    header = ['acession', 'taxid']
    print("\t".join(header))

    for c in query:
        tid, acc = str(c.taxa.tax_id), str(c.acc)
        print("\t".join([str(acc), str(tid)]))


def create_populate():
    nodes = "test_nodes.dmp"
    names = "test_names.dmp"
    acc = "acc.txt"

    create()
    parse_nodes(nodes)
    parse_names(names)
    parse_accession(acc)


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
    #create_populate()
    #extract_acc(tax_id=1282)
    #extract_acc_children(tax_id=1279)

    plac.call(run)
