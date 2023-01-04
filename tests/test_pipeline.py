import random
from unittest import TestCase

from private_utils.pipeline import Pipeline


class TestPipeline(TestCase):
    def setUp(self) -> None:
        self.pipeline = Pipeline()

        foo = lambda: None
        bar = lambda: None
        clk = lambda: None
        fun = lambda: None
        aze = lambda: None

        self.register_collection = [(self.pipeline.register(inputs='A', outputs='B'), foo),
                                    (self.pipeline.register(inputs=('A',), outputs='C'), bar),
                                    (self.pipeline.register(inputs=('B', 'C'), outputs='E'), clk),
                                    (self.pipeline.register(inputs='B', outputs='D'), fun),
                                    (self.pipeline.register(inputs='D', outputs='G'), aze)]

    def test(self):
        random.shuffle(self.register_collection)
        for decorator, callable in self.register_collection:
            decorator(callable)

        self.pipeline.execute()
