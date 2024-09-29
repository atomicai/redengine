def asdict_without_none(obj):
    """
    Convert a dataclass to a dictionary, omitting fields with None values.
    """
    return {k: v for k, v in asdict(obj).items() if v is not None}
