## Taxonomy Modeling

Scripts here uses django-treebeard library to model data.


    # Create migration files and database
    python run.py --makemigrations --migrate 
 
    # Populate the database
    python run.py --add --fname employee.txt

    # Test query performaces
    python run.py --test
