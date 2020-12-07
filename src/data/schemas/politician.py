import mongoengine as me


class Politician(me.Document):

    name = me.StringField(required=True)
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

    number_of_votes = me.IntField()
    election_area_n = me.IntField()

    previous_parliment_member = me.ListField()

    email = me.EmailField()

    speeches = me.ReferenceField('Speech')

    meta = {
        'db_alias': 'core',
        'collection': 'politicians'
    }

