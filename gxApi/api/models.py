from tortoise import fields
from tortoise.models import Model

class User(Model):
    id = fields.IntField(pk=True,auto_increment=True)
    username = fields.CharField(max_length=16)
    password = fields.CharField(max_length=16)
    datasources: fields.ReverseRelation["DataSource"]
class DataSource(Model):
    id = fields.IntField(pk=True,auto_increment=True)
    name = fields.CharField(max_length=16)
    user = fields.ForeignKeyField("models.User",related_name="datasources")
    url = fields.CharField(max_length=255)
    class Meta:
        unique_together = (("name", "user"),)

    dataassets: fields.ReverseRelation["DataAsset"]
class DataAsset(Model):
    id=fields.IntField(pk=True,auto_increment=True)
    name = fields.CharField(max_length=16)
    datasource = fields.ForeignKeyField("models.DataSource",related_name="dataassets")
    class Meta:
        unique_together = (("name", "datasource"),)