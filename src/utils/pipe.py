"""
Object that will handle sequential transformation of data
"""


class Pipe:

    def __init__(self, list_of_transformers):
        assert all([hasattr(transformer, "transform")
                    for transformer in list_of_transformers])
        self.transformers = list_of_transformers

    def __call__(self, obj_to_transform):
        for transformer in self.transformers:
            obj_to_transform = transformer.transform(obj_to_transform)

        return obj_to_transform
