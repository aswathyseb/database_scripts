from django.db import models

from treebeard.mp_tree import MP_Node
from treebeard.ns_tree import NS_Node
from treebeard.al_tree import AL_Node

class MPtree(MP_Node):
    #taxid = models.IntegerField()
    taxid = models.CharField(max_length=30)

    # class Meta:
    #     indexes = [
    #         models.Index(fields=['taxid','path']),
    #     ]

    def __unicode__(self):
        return 'MPtree: %s' % self.taxid

#
# class NStree(NS_Node):
#     name = models.CharField(max_length=30)
#
#     def __unicode__(self):
#         return 'NStree: %s' % self.name
#
# class ALtree(AL_Node):
#     name = models.CharField(max_length=30)
#     parent = models.ForeignKey('self',
#                                        related_name='children_set',
#                                        null=True,
#                                        db_index=True,
#                                        on_delete=models.CASCADE)
#     sib_order = models.PositiveIntegerField()
#     desc = models.CharField(max_length=255)
#
#     def __unicode__(self):
#         return 'ALtree: %s' % self.parent
