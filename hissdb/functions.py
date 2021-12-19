"""
This module translates SQLite functions into Expression objects
that you can include in Statements.

For function documentation, see
https://sqlite.org/lang_corefunc.html#glob
"""

from .expression import Expression as Expr

def acos(x) -> Expr:
    return Expr(x, func='ACOS')

def acosh(x) -> Expr:
    return Expr(x, func='ACOSH')

def asin(x) -> Expr:
    return Expr(x, func='ASIN')

def asinh(x) -> Expr:
    return Expr(x, func='ASINH')

def atan(x) -> Expr:
    return Expr(x, func='ATAN')

def atan2(x, y) -> Expr:
    return Expr(x, y, func='ATAN2')

def atanh(x) -> Expr:
    return Expr(x, func='ATANH')

def avg(x, distinct: bool = False) -> Expr:
    return Expr(x, func='AVG', prefix='DISTINCT' if distinct else None)

def ceil(x) -> Expr:
    return Expr(x, func='CEIL')

def changes() -> Expr:
    return Expr(func='CHANGES')

def char(*args) -> Expr:
    return Expr(*args, func='CHAR')

def coalesce(*args) -> Expr:
    return Expr(*args, func='COALESCE')

def cos(x) -> Expr:
    return Expr(x, func='COS')

def cosh(x) -> Expr:
    return Expr(x, func='COSH')
    
def count(*args, distinct: bool = False) -> Expr:
    args = args or ['*']
    return Expr(
        *args,
        func = 'COUNT',
        prefix = ('DISTINCT' if distinct else None),
    )

def date(time_value, *modifiers) -> Expr:
    return Expr(time_value, *modifiers, func='DATE')

def datetime(time_value, *modifiers) -> Expr:
    return Expr(time_value, *modifiers, func='DATETIME')

def degrees(x) -> Expr:
    return Expr(x, func='DEGREES')

def exists(x) -> Expr:
    return Expr(x, func='EXISTS')

def exp(x) -> Expr:
    return Expr(x, func='EXP')

def floor(x) -> Expr:
    return Expr(x, func='FLOOR')
    
def glob(*args) -> Expr:
    return Expr(*args, func='GLOB')

def group_concat(x, y = None, distinct: bool = False) -> Expr:
    prefix = 'DISTINCT' if distinct else None
    if y:
        return Expr(x, y, func='GROUP_CONCAT', prefix=prefix)
    else:
        return Expr(x, func='GROUP_CONCAT', prefix=prefix)

def hex(x) -> Expr:
    return Expr(x, func='HEX')

def ifnull(x, y) -> Expr:
    return Expr(x, y, func='IFNULL')

def iif(x, y, z) -> Expr:
    return Expr(x, y, z, func='IIF')

def instr(x, y) -> Expr:
    return Expr(x, y, func='INSTR')

def julianday(time_value, *modifiers) -> Expr:
    return Expr(time_value, *modifiers, func='JULIANDAY')

def length(x) -> Expr:
    return Expr(x, func='LENGTH')

def like(x, y, z = None) -> Expr:
    if z:
        return Expr(x, y, z, func='LIKE')
    else:
        return Expr(x, y, func='LIKE')

def likelihood(x, y) -> Expr:
    return Expr(x, y, func='LIKELIHOOD')

def likely(x) -> Expr:
    return Expr(x, func='LIKELY')

def ln(x) -> Expr:
    return Expr(x, func='LN')

def load_extension(x, y = None) -> Expr:
    if y:
        return Expr(x, y, func='LOAD_EXTENSION')
    else:
        return Expr(x, func='LOAD_EXTENSION')

def log(x, y = None) -> Expr:
    if y:
        return Expr(x, y, func='LOG')
    else:
        return Expr(x, func='LOG')

def log10(x) -> Expr:
    return Expr(x, func='LOG10')

def log2(x) -> Expr:
    return Expr(x, func='LOG2')

def lower(x) -> Expr:
    return Expr(x, func='LOWER')

def ltrim(x, y = None) -> Expr:
    if y:
        return Expr(x, y, func='LTRIM')
    else:
        return Expr(x, func='LTRIM')

def max(*args, distinct: bool = False) -> Expr:
    return Expr(
        *args,
        func='MAX',
        prefix='DISTINCT' if distinct else None
    )

def min(*args, distinct: bool = False) -> Expr:
    return Expr(
        *args,
        func='MIN',
        prefix='DISTINCT' if distinct else None
    )

def mod(x) -> Expr:
    return Expr(x, func='MOD')

def nullif(x, y) -> Expr:
    return Expr(x, y, func='NULLIF')

def pi() -> Expr:
    return Expr(func='PI')

def pow(x, y) -> Expr:
    return Expr(x, y, func='POW')

def power(x, y) -> Expr:
    return Expr(x, y, func='POWER')

def printf(format_str, *placeholders) -> Expr:
    return Expr(format_str, *placeholders, func='PRINTF')

def quote(x) -> Expr:
    return Expr(x, func='QUOTE')

def radians(x) -> Expr:
    return Expr(x, func='RADIANS')

def random() -> Expr:
    return Expr(func='RANDOM')

def randomblob(n) -> Expr:
    return Expr(n, func='RANDOMBLOB')

def replace(x, y, z) -> Expr:
    return Expr(x, y, z, func='REPLACE')

def round(x, y = None) -> Expr:
    if y:
        return Expr(x, y, func='ROUND')
    else:
        return Expr(x, func='ROUND')

def rtrim(x, y = None) -> Expr:
    if y:
        return Expr(x, y, func='RTRIM')
    else:
        return Expr(x, func='RTRIM')

def sign(x) -> Expr:
    return Expr(x, func='SIGN')

def sin(x) -> Expr:
    return Expr(x, func='SIN')

def sinh(x) -> Expr:
    return Expr(x, func='SINH')

def soundex(x) -> Expr:
    return Expr(x, func='SOUNDEX')

def sqlite_compileoption_get(n) -> Expr:
    return Expr(n, func='SQLITE_COMPILEOPTION_GET')

def sqlite_compileoption_used(x) -> Expr:
    return Expr(x, func='SQLITE_COMPILEOPTION_USED')

def sqlite_offset(x) -> Expr:
    return Expr(x, func='SQLITE_OFFSET')

def sqlite_source_id() -> Expr:
    return Expr(func='SQLITE_SOURCE_ID')

def sqlite_version() -> Expr:
    return Expr(func='SQLITE_VERSION')

def strftime(format_str, time_value, *modifiers) -> Expr:
    return Expr(format_str, time_value, *modifiers, func='STRFTIME')

def substr(x, y, z = None) -> Expr:
    if z:
        return Expr(x, y, z, func='SUBSTR')
    else:
        return Expr(x, y, func='SUBSTR')

def sum(x, distinct: bool = False) -> Expr:
    return Expr(x, func='SUM', prefix='DISTINCT' if distinct else None)

def tan(x) -> Expr:
    return Expr(x, func='TAN')

def tanh(x) -> Expr:
    return Expr(x, func='TANH')

def time(time_value, *modifiers) -> Expr:
    return Expr(time_value, *modifiers, func='TIME')

def total(x, distinct: bool = False) -> Expr:
    return Expr(
        x, func='TOTAL', prefix='DISTINCT' if distinct else None
     )

def total_changes() -> Expr:
    return Expr(func='SQLITE_VERSION')

def trim(x, y = None) -> Expr:
    if y:
        return Expr(x, y, func='TRIM')
    else:
        return Expr(x, func='TRIM')

def trunc(x) -> Expr:
    return Expr(x, func='TRUNC')

def typeof(x) -> Expr:
    return Expr(x, func='TYPEOF')

def unicode(x) -> Expr:
    return Expr(x, func='UNICODE')

def unlikely(x) -> Expr:
    return Expr(x, func='UNLIKELY')

def upper(x) -> Expr:
    return Expr(x, func='UPPER')

def zeroblob(x) -> Expr:
    return Expr(x, func='ZEROBLOB')
