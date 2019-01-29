__version__ = '1.1.1'

# Runtime patch the suds library
from .suds_patch import _PropertyAppender
from suds.mx import appender as _appender
_appender.PropertyAppender = _PropertyAppender

from .suds_patch import _bodycontent
from suds.bindings import document as _document
_document.Document.bodycontent = _bodycontent
# end runtime patching of suds

from .client import ET_Client

# Import all the wrapper objects
from .objects import *
