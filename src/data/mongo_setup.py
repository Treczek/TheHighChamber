import mongoengine


def global_init():
    mongoengine.register_connection(alias="core", name="THC", host='mongodb://localhost:27017')