from django.db import models

from treebeard.mp_tree import MP_Node
from treebeard.ns_tree import NS_Node

class MPtree(MP_Node):
    name = models.CharField(max_length=30)

    def __unicode__(self):
        return 'MPtree: %s' % self.name


class NStree(NS_Node):
    name = models.CharField(max_length=30)
    #lft = models.IntegerField()
    #rgt = models.IntegerField()

    def __unicode__(self):
        return 'NStree: %s' % self.name