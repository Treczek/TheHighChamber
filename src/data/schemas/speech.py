import mongoengine as me

from src.data.schemas.politician import Politician


class Speech(me.Document):

    date = me.DateTimeField()
    politician = me.EmbeddedDocument(Politician, required=True)
    text = me.StringField(required=True)

    meta = {
        'db_alias': 'core',
        'collection': 'speeches'
    }
