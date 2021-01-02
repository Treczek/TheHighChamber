import mongoengine as me


class Speech(me.EmbeddedDocument):

    date = me.DateTimeField()
    raw_text = me.StringField(required=True)

    meta = {
        'db_alias': 'core',
        'collection': 'speeches'
    }
