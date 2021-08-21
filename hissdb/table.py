# python standard imports
from functools import cached_property
from copy import copy
from sqlite3 import Cursor
from re import finditer

# internal imports# python standard imports
from functools import cached_property
from copy import copy
from sqlite3 import Cursor
from re import finditer, sub, match

# internal imports
from .column import Column
from .statements import Insert, Select, Update, Delete, InsertMany
from .expression import Expression
from .functions import count

class Table:
    def __init__(
        self,
        columns: dict[str, Column] = {},
        foreign_keys: dict[str, Column] = {},
        primary_key: tuple[Column] = (),
        **kwargs,
    ):
        self._columns = {}
        kwargs.update(columns)
        for k, v in kwargs.items():
            col = Column(name = k, constraints = v)
            col._table = self
            self._columns[k] = col
        
        self._foreign_keys = foreign_keys
        self._primary_key = primary_key
        
        # these values are set when the table is assigned to a db
        self._name = None
        self._db = None
    
    
    @classmethod
    def _from_schema(cls, schema: str):
        # normalize schema to something parseable with small regex
        schema = sub('\s+', ' ', schema)
        schema = schema.replace('"', '').replace("'", '')
        schema = schema.replace('( ', '(').replace(' )', ')')
        
        # read column definitions and constraints from the schema
        name = schema.split(' ', 3)[2]
        columns, foreign_keys, = {}, {}
        primary_key = None
        for clause in schema.split('(', 1)[1][:-1].split(', '):
            if clause.startswith('FOREIGN KEY'):
                m = match(
                    r'FOREIGN KEY ?\(?(.+?)\)? REFERENCES (.+?) ?\((.+?)\)',
                    clause
                )
                key_name = m.group(1)
                foreign_table = m.group(2)
                foreign_key = m.group(3)
                foreign_keys[key_name] = f'{foreign_table}({foreign_key})'
            
            elif clause.startswith('PRIMARY KEY'):
                keys_str = schema.split('(', 1)[1][:-1].strip("'\"")
                primary_key = tuple(keys_str.split(', '))
            
            else:
                parts = clause.split(' ', 1)
                col_name = parts[0]
                constraints = parts[1] if len(parts) > 1 else None
                columns[col_name] = constraints
        
        table = cls(
            foreign_keys = foreign_keys,
            primary_key = primary_key,
            **columns
        )
        table._name = name
        return table
    
    
    def __getitem__(self, item):
        result = self._columns.get(item)
        if result:
            return result
        raise AttributeError(
            f"{self} has no column named '{item}'"
        )
    
    
    def __getattr__(self, attr):
        return self[attr]
    
    
    def __setitem__(self, item, value):
        value._table = self
        if not value._name:
            value._name = item
        self._db.execute(
            f'ALTER TABLE {self} ADD COLUMN {value._name}'
            + (f' {value._constraints}' if value._constraints else '')
        )
        self._columns[item] = value
        self._clear_cache()
    
    
    def __setattr__(self, attr, value):
        if type(value) is Column:
            self[attr] = value
        else:
            super().__setattr__(attr, value)
    
    
    def __delattr__(self, attr):
        if attr in self.__dict__:
            super().__delattr__(attr)
        else:
            del self[attr]
    
    
    def __delitem__(self, item):
        self._columns.pop(item)
        self._db.execute(f'ALTER TABLE {self} DROP COLUMN {item}')
        self._clear_cache()
    
    
    def __contains__(self, item):
        return item in self._columns
    
    
    def __str__(self):
        return self._name
    
    
    def __repr__(self):
        return f"{repr(self._db)}['{self._name}']"
    

    def count(self, where: Expression = None, **kwargs) -> int:
        """
        Get the number of rows in the table, optionally restricted to
        those that meet the given criteria.
        """
        return self.select(
            count(),
            where,
            **kwargs,
        ).fetchone()[0]
    
    
    def fetchone(self, cols = '*', where = None, **kwargs):
        """
        Make and execute a Select statement from
        this table, and return the first result.
        """
        return self.select(cols, where, **kwargs).fetchone()
    
    
    def fetchall(self, cols = '*', where = None, **kwargs):
        """
        Make and execute a Select statement from
        this table, and return a list of all results.
        """
        return self.select(cols, where, **kwargs).fetchall()
    
    
    def insert(self, row: dict = {}, **kwargs) -> int:
        """
        Make and execute an Insert statement into this table, and return
        the index of the of the new row.
        """
        return Insert(table = self, row = row, **kwargs).execute().lastrowid
    
    
    def insertmany(
        self,
        cols: tuple[Column],
        rows: list[tuple],
        or_: str = None,
        **kwargs
    ) -> int:
        """
        Make and execute an InsertMany statement, and return the number
        of rows added.
        """
        return InsertMany(
            table = self,
            cols = cols,
            rows = rows,
            or_ = or_,
            **kwargs
        ).execute().rowcount
    
    
    def update(self,
        updates: dict[Column, Expression] = {},
        where: Expression = None,
        **kwargs,
    ) -> int:
        """
        Make and execute an Update statement from this table,
        and return the number of rows modified.
        """ 
        return Update(
            table = self,
            updates = updates,
            where = where,
            **kwargs,
        ).execute().rowcount
    
    
    def delete(self, where: Expression = None, **kwargs) -> int:
        """
        Make and execute a Delete statement from this table, and return
        the number of rows deleted.
        """
        return Delete(
            table = self,
            where = where,
            **kwargs
        ).execute().rowcount
    
    
    def select(self,
        cols: list[Column] = '*',
        where: Expression = None,
        **kwargs,
    ) -> Cursor:
        """
        Make and execute a Select statement from this table, and return
        the resulting SQLite3 Cursor object.
        """
        statement = Select(
            table = self,
            cols = cols,
            where = where,
            **kwargs,
        )
        return statement.execute()
    
    
    @cached_property
    def _info(self):
        return self._db.connection.execute(
            f"PRAGMA TABLE_INFO({self._name})"
        ).fetchall()
    
    
    @cached_property
    def _schema(self):
        """
        If this table is in a database, get the schema from there.
        Otherwise, generate a schema from the list of columns, etc.
        """
        if self._db: # load schema from DB
            cur = self._db.connection.execute(
                "SELECT sql FROM sqlite_schema WHERE tbl_name = ?",
                (self._name,)
            )
            return cur.fetchone()[0]
        
        # if no DB, derive schema from values provided in __init__
        cols = []
        foreign_keys = []
        for col in self._columns.values():
            if col._constraints:
                cols.append(f'{col._name} {col._constraints}')
            else:
                cols.append(col._name)
        
        for key, value in self._foreign_keys.items():
            if type(value) is str:
                if '.' in value: # e.g. users.id
                    parts = value.split('.')
                elif '(' in value: # e.g. users(id)
                    parts = value.split('(')
                    parts[1] = parts[1][:-1] # strip closing paren
                else:
                    raise SyntaxError(
                        f'Unrecognized column reference "{value}"'
                    )
                col_ref = f'{parts[0]}({parts[1]})'
            else:
                col_ref = (
                    f'{value._table._name}'
                    f'({value._name})'
                )
            foreign_keys.append(
                f'FOREIGN KEY ({key}) REFERENCES {col_ref}'
            )
        
        clauses = cols + foreign_keys
        
        if self._primary_key:
            primary_key_cols = [str(col) for col in self._primary_key]
            clauses.append(f'PRIMARY KEY ({", ".join(primary_key_cols)})')
        
        return f'CREATE TABLE {self._name} ({", ".join(clauses)})'
    
    
    @cached_property
    def _foreign_keys(self):
        results = {}
        matches = finditer(
            (
                fr'FOREIGN KEY \((.+?)\) '
                r'REFERENCES ([^\s]+?) ?\((.+?)\)'
            ),
            self._schema
        )
        for match in matches:
            table = match.group(2)
            column = match.group(3)
            results[self[match.group(1)]] = self._db[table][column]
        return results
    
    
    def _clear_cache(self):
        for prop in ['_foreign_keys', '_schema', '_info']:
            if prop in self.__dict__:
                self.__dict__.pop(prop)
