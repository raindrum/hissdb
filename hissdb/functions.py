"""
This module translates SQLite functions into Expression objects
that you can include in Statements.

For function documentation, see
https://sqlite.org/lang_corefunc.html#glob
"""

# internal imports
from .expression import Expression as expr

def acos(x) -> expr:
    return expr(x, func='ACOS')

def acosh(x) -> expr:
    return expr(x, func='ACOSH')

def asin(x) -> expr:
    return expr(x, func='ASIN')

def asinh(x) -> expr:
    return expr(x, func='ASINH')

def atan(x) -> expr:
    return expr(x, func='ATAN')

def atan2(x, y) -> expr:
    return expr(x, y, func='ATAN2')

def atanh(x) -> expr:
    return expr(x, func='ATANH')

def avg(x, distinct: bool = False) -> expr:
    return expr(x, func='AVG', prefix='DISTINCT' if distinct else None)

def ceil(x) -> expr:
    return expr(x, func='CEIL')

def changes() -> expr:
    return expr(func='CHANGES')

def char(*args) -> expr:
    return expr(*args, func='CHAR')

def coalesce(*args) -> expr:
    return expr(*args, func='COALESCE')

def cos(x) -> expr:
    return expr(x, func='COS')

def cosh(x) -> expr:
    return expr(x, func='COSH')
    
def count(*args, distinct: bool = False) -> expr:
    args = args or ['*']
    return expr(
        *args,
        func = 'COUNT',
        prefix = ('DISTINCT' if distinct else None),
    )

def date(time_value, *modifiers) -> expr:
    return expr(time_value, *modifiers, func='DATE')

def datetime(time_value, *modifiers) -> expr:
    return expr(time_value, *modifiers, func='DATETIME')

def degrees(x) -> expr:
    return expr(x, func='DEGREES')

def exp(x) -> expr:
    return expr(x, func='EXP')

def floor(x) -> expr:
    return expr(x, func='FLOOR')
    
def glob(*args) -> expr:
    return expr(*args, func='GLOB')

def group_concat(x, y = None, distinct: bool = False) -> expr:
    prefix = 'DISTINCT' if distinct else None
    if y:
        return expr(x, y, func='GROUP_CONCAT', prefix=prefix)
    else:
        return expr(x, func='GROUP_CONCAT', prefix=prefix)

def hex(x) -> expr:
    return expr(x, func='HEX')

def ifnull(x, y) -> expr:
    return expr(x, y, func='IFNULL')

def iif(x, y, z) -> expr:
    return expr(x, y, z, func='IIF')

def instr(x, y) -> expr:
    return expr(x, y, func='INSTR')

def julianday(time_value, *modifiers) -> expr:
    return expr(time_value, *modifiers, func='JULIANDAY')

def length(x) -> expr:
    return expr(x, func='LENGTH')

def like(x, y, z = None) -> expr:
    if z:
        return expr(x, y, z, func='LIKE')
    else:
        return expr(x, y, func='LIKE')

def likelihood(x, y) -> expr:
    return expr(x, y, func='LIKELIHOOD')

def likely(x) -> expr:
    return expr(x, func='LIKELY')

def ln(x) -> expr:
    return expr(x, func='LN')

def load_extension(x, y = None) -> expr:
    if y:
        return expr(x, y, func='LOAD_EXTENSION')
    else:
        return expr(x, func='LOAD_EXTENSION')

def log(x, y = None) -> expr:
    if y:
        return expr(x, y, func='LOG')
    else:
        return expr(x, func='LOG')

def log10(x) -> expr:
    return expr(x, func='LOG10')

def log2(x) -> expr:
    return expr(x, func='LOG2')

def lower(x) -> expr:
    return expr(x, func='LOWER')

def ltrim(x, y = None) -> expr:
    if y:
        return expr(x, y, func='LTRIM')
    else:
        return expr(x, func='LTRIM')

def max(*args, distinct: bool = False) -> expr:
    return expr(
        *args,
        func='MAX',
        prefix='DISTINCT' if distinct else None
    )

def min(*args, distinct: bool = False) -> expr:
    return expr(
        *args,
        func='MIN',
        prefix='DISTINCT' if distinct else None
    )

def mod(x) -> expr:
    return expr(x, func='MOD')

def nullif(x, y) -> expr:
    return expr(x, y, func='NULLIF')

def pi() -> expr:
    return expr(func='PI')

def pow(x, y) -> expr:
    return expr(x, y, func='POW')

def power(x, y) -> expr:
    return expr(x, y, func='POWER')

def printf(format_str, *placeholders) -> expr:
    return expr(format_str, *placeholders, func='PRINTF')

def quote(x) -> expr:
    return expr(x, func='QUOTE')

def radians(x) -> expr:
    return expr(x, func='RADIANS')

def random() -> expr:
    return expr(func='RANDOM')

def randomblob(n) -> expr:
    return expr(n, func='RANDOMBLOB')

def replace(x, y, z) -> expr:
    return expr(x, y, z, func='REPLACE')

def round(x, y = None) -> expr:
    if y:
        return expr(x, y, func='ROUND')
    else:
        return expr(x, func='ROUND')

def rtrim(x, y = None) -> expr:
    if y:
        return expr(x, y, func='RTRIM')
    else:
        return expr(x, func='RTRIM')

def sign(x) -> expr:
    return expr(x, func='SIGN')

def sin(x) -> expr:
    return expr(x, func='SIN')

def sinh(x) -> expr:
    return expr(x, func='SINH')

def soundex(x) -> expr:
    return expr(x, func='SOUNDEX')

def sqlite_compileoption_get(n) -> expr:
    return expr(n, func='SQLITE_COMPILEOPTION_GET')

def sqlite_compileoption_used(x) -> expr:
    return expr(x, func='SQLITE_COMPILEOPTION_USED')

def sqlite_offset(x) -> expr:
    return expr(x, func='SQLITE_OFFSET')

def sqlite_source_id() -> expr:
    return expr(func='SQLITE_SOURCE_ID')

def sqlite_version() -> expr:
    return expr(func='SQLITE_VERSION')

def strftime(format_str, time_value, *modifiers) -> expr:
    return expr(format_str, time_value, *modifiers, func='STRFTIME')

def substr(x, y, z = None) -> expr:
    if z:
        return expr(x, y, z, func='SUBSTR')
    else:
        return expr(x, y, func='SUBSTR')

def sum(x, distinct: bool = False) -> expr:
    return expr(x, func='SUM', prefix='DISTINCT' if distinct else None)

def tan(x) -> expr:
    return expr(x, func='TAN')

def tanh(x) -> expr:
    return expr(x, func='TANH')

def time(time_value, *modifiers) -> expr:
    return expr(time_value, *modifiers, func='TIME')

def total(x, distinct: bool = False) -> expr:
    return expr(
        x, func='TOTAL', prefix='DISTINCT' if distinct else None
     )

def total_changes() -> expr:
    return expr(func='SQLITE_VERSION')

def trim(x, y = None) -> expr:
    if y:
        return expr(x, y, func='TRIM')
    else:
        return expr(x, func='TRIM')

def trunc(x) -> expr:
    return expr(x, func='TRUNC')

def typeof(x) -> expr:
    return expr(x, func='TYPEOF')

def unicode(x) -> expr:
    return expr(x, func='UNICODE')

def unlikely(x) -> expr:
    return expr(x, func='UNLIKELY')

def upper(x) -> expr:
    return expr(x, func='UPPER')

def zeroblob(x) -> expr:
    return expr(x, func='ZEROBLOB')
