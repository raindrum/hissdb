# python standard imports
from functools import cached_property
from pathlib import Path
import sqlite3

# internal imports
from .table import Table
from .column import Column
from .expression import Expression

class Database:
    """
    A pretty face for a sqlite3 database, with fancy access methods.
    
    If you have a Database called 'db', you can generally access any
    table in it via 'db.TABLE_NAME'. If the table name conflicts with
    an existing property or method, you can instead use 'db[TABLE_NAME]'
    as a fallback.
    """
    def __init__(
        self,
        path: str,
        autoconnect: bool = True,
        autocommit: bool = True,
        verbose: bool = False,
    ):
        """
        Database constructor.
        
        Arguments:
            path: the file path for a new or existing database, or
                ':memory:' for an in-memory database with no
                corresponding file
            autoconnect: whether to autoconnect to the database whenever
                a query requires it. If this is False, you must manually
                run the connect() method or keep all database operations
                inside a 'with' statement.
            autocommit: whether to write all changes to the database at
                the close of a 'with' statement. Note that even if this
                is True, you must manually run the commit() method if
                you make changes *outside* of a context manager.
            verbose: whether to print each SQL statement to the console
                as it is executed
        """
        self._path = Path(path)
        self._verbose = verbose
        self._autoconnect = autoconnect
        self._autocommit = autocommit
        self._connection = None
        self._tables = {}
        
        if self._path.exists():
            self.connect()
            cur = self.execute('SELECT sql FROM sqlite_schema')
            schema_rows = cur.fetchall()
            for schema_row in schema_rows:
                if not schema_row[0]:
                    continue
                table = Table._from_schema(schema_row[0])
                self._tables[table._name] = table
                table._db = self
            if not self._autoconnect:
                self.disconnect()
    
    
    def __setattr__(self, attr: str, value):
        """
        If value is a Table object, do CREATE TABLE. Otherwise,
        do normal __setattr__ behavior.
        """
        if type(value) is not Table:
            super().__setattr__(attr, value)
            return
        
        self.__setitem__(attr, value)
    
    
    def __setitem__(self, item, value):
        assert type(value) is Table
        if item in self._tables:
            del self[item]
        value._name = item
        self.execute(value._schema)
        self._tables[item] = value
        value._db = self
    
    
    def __delattr__(self, attr):
        if type(getattr(self, attr)) is Table:
            self.execute(f'DROP TABLE {attr}')
            self._tables.pop(attr)
        else:
            super().__delattr__(attr)
    
    
    def __delitem__(self, item):
        "Remove a Table from the database"
        self.execute(f'DROP TABLE {item}')
        self._tables.pop(item)
   
    
    def __getattr__(self, attr: str):
        if '_tables' in self.__dict__ and attr in self._tables:
            return self[attr]
        else:
            raise AttributeError(
                f'{self} does not have any property or '
                f'table with the name "{attr}"'
            )
    
    
    def __getitem__(self, item) -> Table:
        if '.' in item:
            table, col = item.split('.')
            return self[table][col]
        elif '(' in item:
            table, col = item.split('(')
            return self[table][col[:-1]]
        else:
            return self._tables[item]
    
    
    def __contains__(self, item) -> bool:
        return item in self._tables
    
    
    def __repr__(self):
        return f"{__class__.__name__}('{self._path}')"
    
    
    def __enter__(self):
        """
        Context manager to handle connections to the database. If
        self._autocommit is True, then any changes are automatically
        committed at the close of the the 'with' statement.
        """
        self.connect(reuse_existing = True)
        return self
    
    
    def __exit__(self, exception_type, exception_value, traceback):
        if exception_type:
            self.disconnect(commit = False)
        else:
            self.disconnect()
    
    
    def create_table(self,
        name: str,
        columns: dict[str, str] = {},
        foreign_keys: dict[str, str] = {},
        primary_key: tuple[str] = (),
        if_not_exist: bool = False,
        **kwargs,
    ) -> Table:
        """
        Add a table to the database.
        
        Arguments:
            name: the name of the new table
            columns: a dict where each key is the name of a column, and
                each value contains the column constraints, e.g.
                {'id': 'INTEGER PRIMARY KEY', 'title': 'TEXT NOT NULL'}.
            foreign_keys: a dict where each key is the name of a column
                in this table, and each value represents a column in
                another table. Values can be specified as Column objects
                or as strings like 'users(id)' or 'users.id'.
            primary_key: optional tuple with the names of multiple
                columns that should be combined to form the primary key.
        """
        if if_not_exist and name in self._tables:
            return self._tables[name]
        table = Table(columns, foreign_keys, primary_key, **kwargs)
        self[name] = table
        return table
    
    
    def drop_table(self, name: str):
        """
        Delete the given table and its contents.
        Same as 'del self[name]'
        """
        del self[name]
    
    
    @property
    def _columns(self) -> dict[str, Column]:
        cols = {}
        for table in self._tables:
            cols.update(table._columns)
        return cols
    
    
    @property
    def connection(self):
        if self._connection:
            return self._connection
        elif self._autoconnect:
            return self.connect()
        else:
            raise AttributeError(
                f'{self} is not connected, and autoconnect is disabled,'
                f' so you must first run {self}.connect() before you '
                f'can perform this action'
            )
    
    
    def connect(self, reuse_existing: bool = True):
        """
        Connect to the database. Returns a sqlite3 connection object,
        but you should not need to use it.
        """
        if self._connection and reuse_existing:
            return self._connection
        else:
            self._connection = sqlite3.connect(self._path)
            return self._connection
    
    
    def commit(self):
        "Save recent changes to the database"
        self.connection.commit()
    
    
    def rollback(self):
        "Undo all changes since the last commit"
        self.connection.rollback()    
    
    
    def disconnect(self, commit: bool = 'AUTO'):
        """
        Close the connection to the database. If commit is False, roll
        back any changes. If commit is True, commit them.
        
        If commit is 'AUTO', only commit if self._autocommit is True, but
        don't rollback either way.
        """
        if commit == True or (commit == 'AUTO' and self._autocommit):
            self.commit()
        elif commit == False:
            self.rollback()
        self.connection.close()
        self._connection = None
    
    
    def execute(self,
        statement: str,
        placeholders: dict = {},
        many: bool = False,
    ):
        """
        Feed the given statement and placeholders to the execute() or
        executemany() method of this database's SQLite3 connection. If
        self._verbose is True, also print the executed statement.
        """
        if hasattr(statement, 'placeholders'):
            placeholders = copy(placeholders)
            placeholders.update(statement.placeholders)
        if many:
            func = self.connection.executemany
        else:
            func = self.connection.execute
        if self._verbose:
            print(
                f"'{statement}'"
                + (f',\n{placeholders}' if placeholders else '')
            )
        return func(statement, placeholders)
