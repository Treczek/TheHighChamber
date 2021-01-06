import mongoengine as me
from .speech import Speech
from hashlib import blake2b


class Politician(me.Document):

    hash = me.StringField(required=True)

    name = me.StringField(required=True)
    sex = me.StringField(required=True)

    election_list = me.StringField()
    election_area = me.StringField()
    political_group = me.StringField()
    education = me.StringField()
    school = me.StringField()
    profession = me.StringField()
    place_of_birth = me.StringField()
    academic_degree = me.StringField()

    election_date = me.DateTimeField()
    oath_date = me.DateTimeField()
    date_of_birth = me.DateTimeField(required=True)
    age = me.IntField()

    number_of_votes = me.IntField()
    election_area_n = me.IntField()

    parliment_member = me.ListField()

    email = me.EmailField()

    speeches = me.EmbeddedDocumentListField(Speech)

    meta = {
        'db_alias': 'core',
        'collection': 'politicians'
    }

    def generate_id(self):
        bk = blake2b(digest_size=8)
        bk.update((self.name + str(self.date_of_birth)).encode('utf-8'))
        return str(bk.hexdigest())
