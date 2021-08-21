# python standard imports
from functools import cached_property
from copy import copy

MAX_PLACEHOLDER = 9999999
CURRENT_PLACEHOLDER = 0
FUNCTIONS = {}

class Expression:
    """
    Expressions are the building blocks of Statements. They provide two
    key features: First, they parameterize input values to avoid SQL
    injection; and second, they provide a set of methods and overloaded
    logical operators (e.g. '==', '&', or '+') that allow users to
    build compound Expressions with Python syntax.
    
    Attributes:
        tokens: the results of parameterizing each of the provided args.
            Any item that is not an Expression, Table, or Column object
            will be converted to a placeholder unless it is in the
            _literals whitelist.
        placeholders: a dictionary of parameters that would need to be
            provided to sqlite3.execute() if this expression were a
            Statement of its own
        necessary_tables: a list of Tables this expression references
    """    
    # strings that will not be converted to placeholders
    _literals = [
        '=', '==', '%', '>', '>=', '<', '<=', '!=', '!<', '!>', '~', '<>',
        '&', '||', '+', '-', '/', '*', '>>', '<<', 'LIKE', 'NOT', 'NOT LIKE',
        'NOT IN', 'SELECT', 'FROM', 'WHERE', 'IN', 'BETWEEN', 'NOT BETWEEN',
        'GLOB', 'EXISTS', 'NOT EXISTS', 'UNIQUE', 'NULL', 'NOT NULL', 'AND',
        'OR', 'AS', '(', ')', 'DISTINCT', 'ALL', 'ASC', 'DESC',
    ]
    
    def __init__(self, *args, func: str = None, prefix: str = None):
        """
        Arguments:
            args: the list of words or other values, in order, in the
                expression. Each one will be converted to a placeholder
                unless it is an Expression, Column, or Table object or
                is otherwise in the _literals whitelist. When the
                expression is converted to a string, the args (or their
                placeholders) will be joined with spaces.
            func: the SQL function, if any, that the expression
                implements. If a func is provided, then the rendered
                expression will be enclosed in parentheses, preceded by
                the func string, and each arg will be separated by
                commas instead of spaces, to make them into function
                arguments rather than just words.
            prefix: only relevant if func is provided. The prefix will
                be rendered just after the opening parentheses, before
                the first arg, with no separating comma. This is meant
                to allow for syntax like the "DISTINCT" argument for
                aggregate functions.
        """
        self.placeholders = {}
        self._necessary_tables = []
        self.args = args
        self.tokens = []
        self.func = func
        self.prefix = prefix
        for arg in args:
            # 
            if issubclass(arg.__class__, __class__):
                self.placeholders.update(arg.placeholders)
                self._necessary_tables += arg._necessary_tables
            elif arg.__class__.__module__ == 'hissdb.column':
                self._necessary_tables.append(arg._table)
            elif arg.__class__.__module__ == 'hissdb.table':
                self._necessary_tables.append(arg)
            
            elif arg is None:
                arg = 'NULL'
            elif type(arg) not in (int, float, str):
                raise SyntaxError(
                    f'Couldn\'t include "{arg}" in expression; '
                    f'no support for objects of type: {type(arg)}'
                )
            elif arg not in self._literals:
                placeholder = next_placeholder()
                self.placeholders[placeholder[1:]] = arg
                arg = placeholder
            
            self.tokens.append(arg)
            
        self._necessary_tables = list(set(self._necessary_tables))
    
    
    def __str__(self):
        """
        Text of the expression that will be inserted into a SQL
        statement, with placeholders to avoid injection
        """
        joiner = ', ' if self.func else ' '
        output = joiner.join([str(t) for t in self.tokens])
        output = output.replace('( ', '(').replace(' )', ')')
        if self.prefix:
            output = f'{self.prefix} {output}'
        if self.func:
            return f'{self.func}({output})'
        else:
            return output


    def __repr__(self):
        return (
            'Expression('
            + ', '.join([repr(t) for t in self.args])
            + (f", func='{self.func}'" if self.func else '')
            + ')'
        )
    
    @cached_property
    def _db(self):
        "Find the Database that this expression relates to"
        for token in [t for t in self.tokens if hasattr(t, '_db')]:
            if token._db:
                return token._db
    
    def render(self):
        "Text of the expression with placeholders filled in"
        text = str(self)
        for k, v in self.placeholders.items():
            if type(v) is str:
                text = text.replace(k, f"'{v}'")
            elif type(v) is int:
                text = text.replace(k, str(v))
            text = text.replace(k, str(v))
        return text
    
    
    # BITWISE OPERATORS
    
    def __and__(self, other):
        return __class__(self, 'AND', other)
    
    def __or__(self, other):
        return __class__('(', self, ')', 'OR', '(', other, ')')
    
    def __rshift__(self, other):
        return __class__(self, '>>', other)
    
    def __lshift__(self, other):
        return __class__(self, '<<', other)

    def __invert__(self):
        """
        Return an expression that is True if and only if this one is not
        True. This works by replacing operators with their inverses
        (e.g. replacing '>' with '<=').
        
        When the expression to be inverted contains two sub-expressions
        joined with AND or OR, the sub-expressions are both inverted,
        and the AND is replaced with OR, or vice versa.
        """
        args = list(copy(self.args))
        func = copy(self.func)
        if func:
            operator = func
        elif len(args) == 3 and type(args[1]) is str:
            operator = args[1]
        elif len(args) == 7 and args[3] == 'OR':
            operator = args[3]
            args = [args[1], args[3], args[5]]
        
        else:
            operator = None
        
        opposites = (
            ('LIKE', 'NOT LIKE'),
            ('IN', 'NOT IN'),
            ('BETWEEN', 'NOT BETWEEN'),
            ('IS', 'IS NOT'),
            ('EXISTS', 'NOT EXISTS'),
            ('<>', '='),
            ('==', '<>'),
            ('<', '>='),
            ('>', '<='),
            ('AND', 'OR'),  # also inverts sub-expressions, see below
        )
        for a, b in opposites:
            if operator not in (a, b):
                continue
            new_op = a if operator == b else b
            if func:
                func = new_op
            else:
                args[1] = new_op
            break
        else:
            raise NotImplementedError(
                f"Unsure how to invert expression '{str(self)}'"
            )
        
        if operator == 'OR':
            return ~args[0] & ~args[2]
        elif operator == 'AND':
            return ~args[0] | ~args[2]
        else:
            return __class__(*args, func=func)
    
    
    # COMPARISONS
    
    def __eq__(self, other):
        return __class__(self, '=', other)
    
    def __ne__(self, other):
        return __class__(self, '<>', other)
    
    def __gt__(self, other):
        return __class__(self, '>', other)
        
    def __lt__(self, other):
        return __class__(self, '<', other)
        
    def __ge__(self, other):
        return __class__(self, '>=', other)
        
    def __le__(self, other):
        return __class__(self, '<=', other)
    
    
    # ARITHMETIC OPERATORS
    
    def __add__(self, other):
        """
        Add two numbers, concatenate two strings, or throw
        an error.
        """
        othertype_ = type_(other)
        selftype_ = type_(self)
        
        if selftype_ in [int, float] and othertype_ in [int, float]:
            return __class__(self, '+', other)
        elif selftype_ is str and othertype_ is str:
            return __class__(self, '||', other)
        else:
            raise SyntaxError(
                'Addition is not supported between '
                f'{selftype_} value "{self}" and {othertype_} '
                f'value "{other}"'
            )
    
    def __sub__(self, other):
        return __class__(self, '-', other)
    
    def __mul__(self, other):
        return __class__(self, '*', other)
    
    def __div__(self, other):
        return __class__(self, '/', other)
    
    def __mod__(self, other):
        "LIKE operator for strings, modulo operator otherwise"
        if type_(self) is str:
            return __class__(self, 'LIKE', other)
        else:
            return __class__(self, '%', other)
    
    def __abs__(self):
        "SQLite ABS() function"
        return __class__(self, func='ABS')
    
    def max(self, distinct: bool = False):
        "SQLite MAX() function"
        return __class__(
            self, func='MAX', prefix='DISTINCT' if distinct else None
        )
        
    def min(self, distinct: bool = False):
        "SQLite MIN() function"
        return __class__(self, func='MIN')
    
    def avg(self, distinct: bool = False):
        "SQLite AVG() function"
        return __class__(
            self, func='AVG', prefix='DISTINCT' if distinct else None
        )
    
    def round(self):
        "SQLite ROUND() function"
        return __class__(self, func='ROUND')
    
    def ceil(self):
        "SQLite CEIL() function"
        return __class__(self, func='CEIL')
        
    def floor(self):
        "SQLite FLOOR() function"
        return __class__(self, func='FLOOR')
    
    def ln(self):
        "SQLite LN() function"
        return __class__(self, func='LN')
    
    def sqrt(self):
        "SQLite SQRT() function"
        return __class__(self, func='SQRT')
    
    def exp(self):
        "SQLite EXP() function"
        return __class__(self, func='EXP')
    
    def pow(self, exponent: int):
        "SQLite POWER() function"
        return __class__(self, exponent, func='POWER')
    
    # ROW OPERATORS
    
    def count(self, distinct: bool = False):
        "SQLite COUNT() function"
        prefix = 'DISTINCT' if distinct else None
        return __class__(self, func='COUNT', prefix=prefix)
    
    def exists(self):
        "SQLite EXISTS() function"
        return __class__(self, func='EXISTS')
    
    def in_list(self, vals: list):
        if type(vals) in [list, tuple, set]:
            return __class__(self, 'IN', '(', *vals, ')')
        else:
            return __class__(self, 'IN', vals)
    
    # STRING OPERATORS
    
    def startswith(self, other):
        return __class__(self, 'LIKE', f'{other}%')
    
    def endswith(self, other):
        return __class__(self, 'LIKE', f'%{other}')
    
    def replace(self, find, repl):
        return __class__(self, find, repl, func='REPLACE')
    
    def length(self):
        return __class__(self, func='LENGTH')
    
    def lower(self):
        return __class__(self, func='LOWER')
    
    def upper(self):
        return __class__(self, func='UPPER')
    
    def substr(self, start: int, length: int):
        return __class__(self, start, length, func='SUBSTR')
    
    def strip(self, character: str = None):
        if character:
            return __class__(self, character, func='TRIM')
        else:
            return __class__(self, func='TRIM')
    
    def lstrip(self, character: str = None):
        if character:
            return __class__(self, character, func='LTRIM')
        else:
            return __class__(self, func='LTRIM')
    
    def rstrip(self, character: str = None):
        if character:
            return __class__(self, character, func='rtrim')
        else:
            return __class__(self, func='LTRIM')
    
    def index(self, substr: str):
        if type_(self) is str:
            return __class__(self, substr, func='INSTR')
        else:
            raise NotImplementedError
    
    # MISC. CONVENIENCES
    
    @property
    def desc(self):
        "Shortcut for use in ORDER BY clauses"
        return __class__(self, 'DESC')


########################################################################
# utility functions
########################################################################

def next_placeholder():
    global CURRENT_PLACEHOLDER
    if CURRENT_PLACEHOLDER > MAX_PLACEHOLDER:
        CURRENT_PLACEHOLDER = 1
    else:
        CURRENT_PLACEHOLDER += 1
    return f':{CURRENT_PLACEHOLDER}'

def type_(value):
    """
    Approximate the Python datatype that the given column,
    expression, or function is likely to return.
    """
    if hasattr(value, '__module__') and value.__module__ == 'hissdb.column':
        type_str = value.type
        if type_str == 'INTEGER':
            return int
        elif type_str == 'REAL':
            return float
        elif type_str == 'TEXT':
            return str
        elif type_str == 'BLOB':
            return bytes
        else:
            raise NotImplementedError(
                f'Unsure how to parse SQLite datatype "{type_str}"'
            )
    
    elif type(value) is Expression:
        if (
            value.func is None
            and len(value.tokens) == 3
            and type(value.tokens[1]) is str
        ):
            operator = value.tokens[1]
            if operator in ['/']:
                return float
            elif operator == '%':
                return int
            elif operator in [
                'LIKE', 'AND', 'OR', '=', '==', '>',
                '>=', '<', '<=', 'IN', 'BETWEEN', 'IN',
            ]:
                return bool
            elif operator in ['*', '+', '-']:
                if float in [type_(tokens[0]), type_(tokens[2])]:
                    return float
                else:
                    return int
            else:
                return type_(value.args[0])
        elif value.func in ['COUNT', 'LENGTH', 'RANDOM']:
            return int
        elif value.func == ['AVG', 'CEIL', 'FLOOR', 'ROUND']:
            return float
        elif value.func in [
            'UPPER', 'LOWER', 'SUBSTR', 'LTRIM',
            'RTRIM', 'TRIM', 'REPLACE', 'TYPEOF',
        ]:
            return str
        elif value.func in [
            'MAX', 'MIN', 'SUM', 'ABS', 
        ]:
            return type_(value.args[0])
        elif value.func in ['EXISTS']:
            return bool
            
        else:
            raise NotImplementedError(
                f'Unsure what type is outputted by a "{value.func}" function'
            )
    else:
        return type(value)

def function(name, *args, **kwargs):
    global FUNCTIONS
    if not FUNCTIONS:
        import functions
        FUNCTIONS = var(functions)
    return FUNCTIONS[name](*args, **kwargs)
