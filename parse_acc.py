import csv, os, sys

# Script extract `LIMIT` accessions for each taxid.
LIMIT = 10


def path(*args):
    return os.path.join(*args)


def strip(x):
    return x.strip()


def parse(fname, idfile):
    # read ids
    ids = open(idfile).readlines()
    ids = list(map(strip, ids))

    # extract ids from fname
    # fname = path(".", "data", "nucl_gb.accession2taxid")
    seen, done = set(), set()
    count = 0
    stream = csv.reader(open(fname), delimiter="\t")

    for row in stream:
        taxid = row[2]

        if taxid not in ids:
            continue

        if taxid in ids and (count <= LIMIT and taxid not in done):
            print("\t".join(row))
            count += 1

        if count == LIMIT:
            count = 0
            done.add(taxid)

        seen.add(taxid)

        if len(seen) == LIMIT and count == LIMIT:
            break


if __name__ == "__main__":
    fname = "nucl_gb.accession2taxid"
    idfile = sys.argv[1] # ids.txt
    parse(fname=fname, idfile=idfile)
