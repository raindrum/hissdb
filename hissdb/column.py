# python standard imports
from functools import cached_property
from sqlite3 import Cursor

# internal imports
from .expression import Expression

class Column(Expression):
    """
    A Column is a reference to a column in a SQLite database. Because it
    is also an Expression, many of its logical operators are overridden
    so that you can build SQL Statements via Python logic like this:
        
        john_does = db.people.select(where=
            db.people.first_name == 'John'
            & db.people.last_name == 'Doe'
        )
    
    For more information on this, see the Expression documentation.
    
    Attributes:
        cid: column index number
        name: name of the column
        type: string containing the SQL datatype of this column
        notnull: int representing whether the column disallows null vals
        dflt_value: the column's default value
        pk: int representing whether the column is a primary key
    """
    
    _pragma_cols = ['cid', 'name', 'type', 'notnull', 'dflt_value', 'pk',]
    placeholders = {}
    
    def __init__(
        self,
        constraints: str = None,
        table = None,
        name: str = None,
    ):
        """
        Column object constructor.
        
        Arguments:
            constraints: a SQL expression defining this column, like
                'TEXT NOT NULL' or 'INTEGER PRIMARY KEY'
            table: the table containing this column. If not provided, it
                will be set when the column is assigned to a table with
                Table.__setattr__ or Table.__setitem__.
            name: the name of this column. If not provided, it will be
                set when this column is assigned to a table.
        
        """
        self._name = name
        self._constraints = constraints
        
        if table:
            table[name] = self
    
    def select(self, where: Expression = None, **kwargs) -> Cursor:
        """
        Convenience method to execute a Select statement targeting only
        this column, and return the resulting Cursor object.
        """
        return self._table.select(
            cols = [self],
            where = where,
            **kwargs,
        )
    
    def fetchone(self, where: Expression = None, **kwargs):
        """
        Convenience method to execute a Select statement targeting only
        this column, and return the single resulting value (rather than
        a tuple with one item in it).
        """
        val = self.select(where, **kwargs).fetchone()
        return val[0] if val else None
    
    def fetchall(self, where: Expression = None, **kwargs):
        """
        Convenience method to execute a Select statement targeting only
        this column, and return a list of the resulting values (rather
        than a list of one-item tuples).
        """
        vals = self.select(where, **kwargs).fetchall()
        return [val[0] for val in vals]
    
    def update(self,
        new_value: Expression,
        where: Expression = None,
        **kwargs
    ):
        """
        Convenience method to execute an Update statement setting the
        value of this column, and return the number of rows modified.
        """
        return self._table.update(
            updates={self: new_value},
            where = where,
            **kwargs
        )
    
    def __str__(self):
        return f'{self._table}.{self._name}'
    
    def __repr__(self):
        return f"{repr(self._table)}['{self._name}']"
    
    def __hash__(self):
        return hash(str(self))
    
    def __getattr__(self, attr: str):
        if attr in self._pragma_cols:
            return self._info[self._pragma_cols.index(attr)]
    
    @property
    def _necessary_tables(self):
        return [self._table]
    
    @cached_property
    def _info(self):
        col_names = [r[1] for r in self._table._info]
        col_index = col_names.index(self._name)
        return self._table._info[col_index]
    
    @cached_property
    def _foreign_key(self):
        if self._name in self._table._foreign_keys:
            return self._table._foreign_keys[self]
        else:
            return None
