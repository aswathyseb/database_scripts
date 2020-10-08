from django.db import models

from treebeard.mp_tree import MP_Node


class Division(models.Model):
    """
    This table represents the division/category that a node belongs to.
    It is populated using divisions.dmp file.
    """

    division_id = models.IntegerField(primary_key=True)
    code = models.CharField(max_length=3)
    name = models.CharField(max_length=50)


class Name(models.Model):
    """"
    Tables denotes the different types of names associated with a taxid.
    It is populated using names.dmp file.
    """

    unique_name = models.CharField(max_length=50)
    name_class = models.CharField(max_length=50)


class Node(MP_Node):
    """
    Table represents  a single node in am MPtree tree.
    In addition to the fields below path, depth, numchild are also available for each node.
    It is populated with nodes.dmp file.
    """

    # path = models.CharField(max_length=255, unique=True)
    # depth = models.PositiveIntegerField()
    # numchild = models.PositiveIntegerField(default=0)

    tax_id = models.IntegerField(primary_key=True)
    rank = models.CharField(max_length=50)
    division = models.ForeignKey(Division, on_delete=models.CASCADE)


class Synonym(models.Model):
    """
    This table connects Nodes and Names.
    """

    node = models.ForeignKey(Node, on_delete=models.CASCADE)
    name = models.ForeignKey(Name, on_delete=models.CASCADE)
