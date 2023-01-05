from dataclasses import dataclass, field
from collections import defaultdict
from typing import Iterable, Callable, Dict, OrderedDict

__all__ = ["Graph", "Edge"]


def _is_iterable(arg):
    if not isinstance(arg, Iterable) or isinstance(arg, str):
        return (arg,)
    return arg


@dataclass
class Edge:
    inputs: Iterable
    outputs: Iterable
    function: Callable
    priority: int = 0
    id: int = field(init=False)

    def __post_init__(self):
        self.inputs = _is_iterable(self.inputs)
        self.outputs = _is_iterable(self.outputs)
        self.id = id(self.function)

    def __repr__(self):
        return f"{self.priority}: {self.inputs} -- {self.function} --> {self.outputs}"


class Graph:
    def __init__(self):
        self.edges: OrderedDict[int, Edge] = {}
        self._edges_per_outputs = defaultdict(list)
        self._edges_per_inputs = defaultdict(list)

    def __repr__(self):
        return "\n".join([item.__repr__() for item in self.edges.values()])

    def _predecessors(self, item_id: int)-> Iterable[int]:
        """Returns the predecessors of the corresponding edge. 

        Args:
            item_id (int): edge identifier to consider

        Returns:
            Iterable[int]: Iterable of edge identifier connected to the provided edge id inputs.
        """        
        edge = self.edges[item_id]
        predecessors = []
        for in_node in edge.inputs:
            predecessors.extend(self._edges_per_outputs[in_node])

        return predecessors

    def successors(self, edge_id: int)->Iterable[int]:
        """Returns the successors of the corresponding edge. 

        Args:
            edge_id (int): edge identifier to consider

        Returns:
            Iterable[int]: Iterable of edge identifier connected to the provided edge id outputs.
        """
        edge = self.edges[edge_id]
        successors = []
        for out_node in edge.outputs:
            successors.extend(self._edges_per_inputs[out_node])

        return successors

    def _adjust_ranking(self, item_id: int, priority, mode="forward"):
        self.edges[item_id].priority = priority
        func = self.successors if mode == "forward" else self._predecessors
        offset = 1 if mode == "forward" else -1

        for neighbor_id in func(item_id):
            self._adjust_ranking(neighbor_id, priority + offset, mode=mode)

    def _sort(self):
        for item_id in self.edges.keys():
            current_priority = self.edges[item_id].priority
            self._adjust_ranking(item_id, current_priority, mode="forward")
            self._adjust_ranking(item_id, current_priority, mode="backward")

        rank = list(self.edges.items())
        rank.sort(key=lambda x: x[1].priority)
        self.edges = dict(rank)

    def _register_internals(self, edge):
        """Register internal variables

        - Add the edge
        - Add the edge to lists accessible per node
        Args:
            edge (Edge): instance of Edge
        """        
        item_id = edge.id
        self.edges[item_id] = edge

        for in_node in edge.inputs:
            self._edges_per_inputs[in_node].append(item_id)

        for out_node in edge.outputs:
            self._edges_per_outputs[out_node].append(item_id)

    def register(self, inputs: Iterable, outputs: Iterable):
        """Decorator registering the decorated function into the graph instance.

        Args:
            inputs (Iterable): node names as inputs
            outputs (Iterable): node names as outputs
        """        
        def inner(function):
            item = Edge(inputs=inputs, outputs=outputs, function=function)
            self._register_internals(item)
            self._sort()

            return function

        return inner

    def __iter__(self):
        for edge in self.edges.values():
            yield edge.function

    def test_execute(self, start={"A"}):
        values_so_far = start
        print(f"Start with {values_so_far}")
        for item_id, item in self.edges.items():
            missing_values = [
                input for input in item.inputs if input not in values_so_far
            ]

            print(item)
            if not missing_values:
                values_so_far = values_so_far.union(item.outputs)
            else:
                print(f"{missing_values} are missing")
                raise
