from pynamodb.exceptions import PynamoDBException


class InvalidParamException(PynamoDBException):
    """
    Invalid param error for model, attributes
    """
    msg = "Invalid Parameter Error"
