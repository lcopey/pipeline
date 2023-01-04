from private_utils.pipeline import Pipeline

pipeline = Pipeline()


@pipeline.register(inputs='D', outputs='G')
def aze():
    pass


@pipeline.register(inputs='A', outputs='B')
def foo():
    pass


@pipeline.register(inputs=('B', 'C'), outputs='E')
def clk():
    pass


@pipeline.register(inputs='B', outputs='D')
def fun():
    pass


@pipeline.register(inputs=('A',), outputs='C')
def bar():
    pass


if __name__ == '__main__':
    pipeline.execute()
