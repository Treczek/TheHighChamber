"""
Object that will handle sequential transformation of data
"""


class Pipe:

    def __init__(list_of_transformers):
        assert all([hassatr(transformer, "transform")
                    for transformer in list_of_transformers])
        self.transformers = list_of_transformers

    def __call__(obj_to_transform, return_type=dict):
        for transformer in self.transformers:
            obj_to_transform = transformer.transform(obj_to_transform)

        assert type(obj_to_transform) is return_type

        return obj_to_transform
