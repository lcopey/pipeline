from collections import defaultdict
from typing import Iterable

__all__ = ['Pipeline', 'PipelineItem']


def _is_iterable(arg):
    if not isinstance(arg, Iterable) or isinstance(arg, str):
        return arg,
    return arg


class PipelineItem:
    def __init__(self, inputs, outputs, fun):
        self.inputs = _is_iterable(inputs)
        self.outputs = _is_iterable(outputs)
        self.fun = fun
        self.priority = 0
        self.id = id(fun)
        # self.id = fun.__name__

    def __repr__(self):
        return f'{self.priority}: {self.inputs} -- {self.fun} --> {self.outputs}'


class Pipeline:
    def __init__(self):
        self.items = {}
        self._function_per_outputs = defaultdict(list)
        self._function_per_inputs = defaultdict(list)

    def __repr__(self):
        return '\n'.join([item.__repr__() for item in self.items.values()])

    def _predecessors(self, item_id):
        # return item connected to the inputs (means input of item found in outputs)
        item = self.items[item_id]
        predecessors = []
        for input in item.inputs:
            predecessors.extend(self._function_per_outputs[input])

        return predecessors

    def _successors(self, item_id):
        # return item connected to the outputs (means output of item found in the inputs)
        item = self.items[item_id]
        successors = []
        for output in item.outputs:
            successors.extend(self._function_per_inputs[output])

        return successors

    def _adjust_ranking(self, item_id, priority, mode='forward'):
        self.items[item_id].priority = priority
        func = self._successors if mode == 'forward' else self._predecessors
        offset = 1 if mode == 'forward' else -1

        for neighbor_id in func(item_id):
            self._adjust_ranking(neighbor_id, priority + offset, mode=mode)

    def _sort(self):
        for item_id in self.items.keys():
            current_priority = self.items[item_id].priority
            self._adjust_ranking(item_id, current_priority, mode='forward')
            self._adjust_ranking(item_id, current_priority, mode='backward')

        rank = list(self.items.items())
        rank.sort(key=lambda x: x[1].priority)
        self.items = dict(rank)

    def _register_internals(self, item):
        item_id = item.id
        self.items[item_id] = item

        for input in item.inputs:
            self._function_per_inputs[input].append(item_id)

        for output in item.outputs:
            self._function_per_outputs[output].append(item_id)

    def register(self, inputs: Iterable, outputs: Iterable):
        def inner(fun):
            item = PipelineItem(inputs=inputs, outputs=outputs, fun=fun)
            print(f'Registering {item}')
            self._register_internals(item)
            self._sort()
            print()

            return fun

        return inner

    def execute(self, start={'A'}):
        values_so_far = start
        print(f'Start with {values_so_far}')
        for item_id, item in self.items.items():
            missing_values = [input for input in item.inputs if input not in values_so_far]

            print(item)
            if not missing_values:
                values_so_far = values_so_far.union(item.outputs)
            else:
                print(f'{missing_values} are missing')
                raise
