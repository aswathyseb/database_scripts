#!/usr/bin/env bash

DATA=./data
cd $DATA

# Taxid 1279 and its children nodes
cat nodes.dmp | grep -w  "1279"  | head -5 >test_nodes.dmp

# Get ids
cat test_nodes.dmp |cut -d "|" -f 1 |sed 's/[[:space:]]//' >ids.txt

# Get 10 accessions corresponding to each id.
echo "Getting accessions ..."
python ../parse_acc.py ids.txt >acc.txt

echo "Making test nodes and names files ..."

# Add extra ids to nodes and names file for fact checking.
cat nodes.dmp | head -100| tail -5 >>test_nodes.dmp

# Names corresponding to the nodes in nodes file.
cat test_nodes.dmp |cut -d "|" -f 1 |sed 's/[[:space:]]//' >ids.txt
for id in $(cat ids.txt); do grep -w "^$id"  names.dmp | grep "scientific name" ; done >>test_names.dmp
