import mongoengine as me
from hashlib import blake2b


class Speech(me.EmbeddedDocument):

    hash = me.StringField(required=True)

    politician_id = me.StringField()
    politician_name = me.StringField()

    date = me.DateTimeField()
    raw_text = me.StringField(required=True)

    def generate_id(self):
        bk = blake2b(digest_size=8)
        bk.update(self.raw_text.encode('utf-8'))
        return str(bk.hexdigest())
