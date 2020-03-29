#!/usr/bin/env bash

#
#  This script downloads NCBI taxonomy and accession files.
#
set -uex

# Taxonomy URL
TAXA_URL=https://ftp.ncbi.nlm.nih.gov/pub/taxonomy/taxdump.tar.gz

# Accession file URL.
ACC_URL=https://ftp.ncbi.nlm.nih.gov/pub/taxonomy/accession2taxid/nucl_gb.accession2taxid.gz

# Data directory
DATA=./data

# Create data directory
mkdir -p $DATA

# Change into data directory
cd $DATA

# Download taxonomy files.
wget $TAXA_URL

# Unzip taxonomy files.
tar -zxvf taxdump.tar.gz

# Download accession file
wget $ACC_URL

# Unzip accession file
gunzip nucl_gb.accession2taxid.gz








