class DivPayoutError(Exception):
    """Basic exception for errors raised by the worker"""
    def __init__(self, msg=None):
        if msg is None:
            # Set some default useful error message
            msg = "An error occured while extracting dividend" \
                  " payouts from Seeking Alpha'S website"
        super(DivPayoutError, self).__init__(msg)

class ParsingError(DivPayoutError):
    """When a string could not be parsed as expected"""
    def __init__(self, str, re, msg=None):
        if msg is None:
            # Set some default useful error message
            msg = "String could not be parsed as expected"
        super(ParsingError, self).__init__(
            msg + ", string={}. regexp={}".format(str, re))
        self.string = str
        self.re = re
