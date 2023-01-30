from importlib.metadata import version

__version__ = version("dls-normsql")
del version

__all__ = ["__version__"]
