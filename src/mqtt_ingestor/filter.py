from mqtt_ingestor.model import DocumentPayload

import importlib
from typing import Callable, cast

DocumentPayloadFilter = Callable[[DocumentPayload], bool]


def load_filter(spec: str) -> DocumentPayloadFilter:
    """
    Load a filter callback from a string like:
       "mypkg.filters.chain2:filter"
       "mypkg.filters.chain2"  # defaults to filter()

    Returns a function(doc) -> bool
    """

    if ":" in spec:
        module_path, func_name = spec.split(":", 1)
    else:
        module_path, func_name = spec, "filter"

    module = importlib.import_module(module_path)

    if not hasattr(module, func_name):
        raise ValueError(
            f"Filter function '{func_name}' not found in module '{module_path}'"
        )

    callback = getattr(module, func_name)

    if not callable(callback):
        raise TypeError(f"'{func_name}' in '{module_path}' is not callable")

    return cast(DocumentPayloadFilter, callback)
