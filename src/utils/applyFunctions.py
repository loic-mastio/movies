# applyFunctions.py
from collections import OrderedDict

'''Lambda functions for pandas's apply function'''
def sortGenre(string: str) -> str:
    '''Sort genre by ASC, remove None values'''
    if string is None:
        return ""
    string = string.split("|")
    try:
        string.remove("None")
    except:
        None
    string.sort()
    return '|'.join(string)


def cleanDuplicateGenre(string: str) ->str:
    '''Remove multiples values in string with | delimiter'''
    if string is None:
        return ""
    return '|'.join(OrderedDict.fromkeys(string.split("|")))


def columnLiked(note: int) -> int:
    '''Return 0 if value is less than 7, 1 otherwise'''
    if note < 7:
        return 0
    else:
        return 1
