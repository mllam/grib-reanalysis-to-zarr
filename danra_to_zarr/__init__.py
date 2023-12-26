# get __version__ from package
import importlib.metadata

__version__ = importlib.metadata.version(__name__)
