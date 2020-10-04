## Taxonomy Modeling

Scripts here uses django-treebeard library to model data.

The main script to create the database and run queries is 'run.py'

To see the available subcommands 

	python run.py -h

To create migration files and database
    
	python run.py database --makemigrations --migrate 
 
To create and populate the database with taxonomy files(names.dmp, nodes.dmp and division.dmp)

	python run.py database --add --nodes data/nodes.dmp --names data/names.dmp --divisions data/division.dmp

To list taxon tree of given taxids aling with scientific name and rank
	
	python run.py list -n -r --ids 9605,239934



