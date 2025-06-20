class ClientError(Exception):
    """class representing Generic Http error."""

    message = None

    def __init__(self, message=None, response=None):
        super().__init__(message or self.message)
        self.response = response

class IncompatibleFieldSelectionError(ClientError):
    """Raised if the query contains incorrect fields or column names"""

class MarketingCloudError(ClientError):
    """Base exception for Marketing Cloud API errors"""

class MarketingCloudPermissionFailure(MarketingCloudError):
    """Custom Exception raised for permission related faults"""

class AuthenticationError(MarketingCloudError):
    """Raised for auth related faults."""

