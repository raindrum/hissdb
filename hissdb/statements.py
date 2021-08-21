# python standard imports
from copy import copy
from sqlite3 import Cursor

# internal imports
from .expression import Expression
from .column import Column

class BaseStatement(Expression):
    "base class that all Statements inherit from"
    def __init__(self,
        table,
        where: Expression = None,
        join: dict = {},
        order_by: tuple[Expression] = None,
        limit: int = None,
        offset: int = None,
        autojoin: bool = True,
        **kwargs,
    ):
        """
        BaseStatement class constructor. There should be no need for a
        user to use this directly rather than a subclass, but all
        subclasses accept the following arguments in their constructors.
        
        Arguments:
            table: the Table object this statement relates to
            where: an Expression object representing a WHERE clause
                in SQL.
            join: a dictionary where each key is a Column object to join
                on the condition that the corresponding Expression is
                met.
            order_by: a Column, an Expression object representing a
                virtual column, or a tuple containing multiple of either
                of those things. To sort descending, you can use the
                'desc' property of any Column or Expression, e.g.
                "order_by=db.users.first_name.desc"
            limit: an int or an Expression representing an int, setting
                the maximum number of rows to select or modify
            offset: an int or an Expression representing an int, which
                sets which row to on.
            autojoin: whether HissDB should use the database's foreign
                key relationships to automatically join any tables that
                the statement requires. Defaults to True.
        """
        self.table = table
        self.where: Expression = where
        self.join = join
        self.order_by: tuple[Expression] = None
        self.limit: int = limit
        self.offset: int = offset
        self.autojoin: bool = autojoin
        self.unknown_kwargs = kwargs
        
        if order_by and type(order_by) not in [list, tuple, set]:
            self.order_by = tuple(order_by)
        else:
            self.order_by = order_by
    
    def execute(self) -> Cursor:
        return self._db.execute(
            statement = str(self),
            placeholders = self.placeholders
        )
    
    def __str__(self):
        return '\n'.join(self.clauses)
    
    def __repr__(self):
        return (
            f'{self._db}.execute('
            f"'{self}', {self.placeholders})"
        )
    
    
    def __call__(self) -> Cursor:
        return self.execute()
    
    
    @property
    def _db(self):
        return self.table._db
    
    @property
    def clauses(self) -> list[str]:
        if self.autojoin:
            joins = implicit_join(
                start_table = self.table,
                target_tables = self._necessary_tables,
                prior_joins = self.join,
            )
        else:
            joins = self.join
        
        return list(filter(lambda x: bool(x), [
            f'FROM {self.table}',
            *[f'JOIN {k} ON {v}' for k,v in joins.items()],
            (f'WHERE {self.where}' if self.where else ''),
            (f'LIMIT {self.limit}' if self.limit else ''),
            (f'OFFSET {self.offset}' if self.offset else ''),
            (f'ORDER BY {", ".join(self.order_by)}' if self.order_by else ''),
        ]))
    
    @property
    def _necessary_tables(self) -> list:
        necessary_tables = []
        for val in self.__dict__.values():
            if not val:
                pass
            elif val.__class__.__module__ == 'hissdb.table':
                necessary_tables.append(val)
            elif hasattr(val, '_necessary_tables'):
                necessary_tables += val._necessary_tables
            elif type(val) is list and hasattr(val[0], '_necessary_tables'):
                for item in val:
                    necessary_tables += item._necessary_tables
            elif type(val) is dict:
                for item in [*val.keys(), *val.values()]:
                    if not hasattr(item, '_necessary_tables'):
                        continue
                    necessary_tables += item._necessary_tables
        return list(set(necessary_tables))
    
    @property
    def placeholders(self) -> dict:
        placeholders = {}
        for key, val in self.__dict__.items():
            if not val:
                pass
            elif hasattr(val, 'placeholders'):
                placeholders.update(val.placeholders)
            elif type(val) is list and hasattr(val[0], 'placeholders'):
                for item in val:
                    placeholders.update(item.placeholders)
            elif (
                type(val) is list and 
                len(val) > 0 and
                hasattr(val[0], 'placeholders')
            ):
                for item in val:
                    placeholders.update(item.placeholders)
            elif (
                type(val) is dict and
                len(val) > 0 and
                hasattr(list(val.values())[0], 'placeholders')
            ):
                for item in val.values():
                    placeholders.update(item.placeholders)
        return placeholders
    
    def _resolve_column(self, col_name: str) -> Column:
        if type(col_name) is str:
            if '.' in col_name:
                table, col = col_name.split('.')
                return self._db[table][col]
            else:
                return self.table[col_name]
        return col_name


class Insert(BaseStatement):
    "SQL statement to insert a single row into a table"
    def __init__(self, table, row: dict = {}, or_: str = None, **kwargs):
        """
        Insert statement constructor. Any unknown keyword arguments will
        be added to the row dict.
        
        Arguments:
            table: Table object to insert the row into
            row: dict of values representing the row to insert
            or_: what to do when the insert statement fails due to a
                table constraint. Options are 'ABORT', 'FAIL', 'IGNORE',
                'REPLACE', and 'ROLLBACK'.
        """
        super().__init__(table=table, **kwargs)
        self.or_ = or_
        if self.unknown_kwargs:
            row = copy(row)
            row.update(self.unknown_kwargs)
        self.row = {k: Expression(v) for k,v in row.items()}
    
    
    @property
    def placeholders(self):
        placeholders = {}
        for val in self.row.values():
            if hasattr(val, 'placeholders'):
                placeholders.update(val.placeholders)
        return placeholders
    
    
    @property
    def clauses(self):
        return [
            'INSERT'
            + (f' OR {self.or_}' if self.or_ else '')
            + f' INTO {self.table} ({", ".join(self.row.keys())})'
            + f' VALUES ({", ".join([str(v) for v in self.row.values()])})'
        ] + super().clauses[1:] # skip the FROM clause



class InsertMany(BaseStatement):
    "SQL statement to efficiently insert a list or generator of rows"
    def __init__(self,
        table,
        cols: tuple[Column],
        rows: list[tuple],
        or_: str = None,
        **kwargs
    ):
        """
        InsertMany statement constructor.
        
        Arguments:
            cols: tuple of Column objects (or strings representing them)
                corresponding to the columns for which values will
                be provided.
            rows: list or generator containing each row to insert. A row
                is a tuple whose values each represent the corresponding
                value in cols.
            or_: what to do when the insert statement fails due to a
                table constraint. Options are 'ABORT', 'FAIL', 'IGNORE',
                'REPLACE', and 'ROLLBACK'.
        """
        super().__init__(table = table, **kwargs)
        self.or_ = or_
        self.cols = [self._resolve_column(col) for col in cols]
        self.rows = rows
    

    def execute(self) -> Cursor:
        return self._db.execute(
            statement = str(self),
            placeholders = self.rows,
            many=True
        )
    
    @property
    def clauses(self):
        return [
            'INSERT'
            + (f' OR {self.or_}' if self.or_ else '')
            +f' INTO {self.table} ({", ".join([c._name for c in self.cols])})'
            + f' VALUES ({", ".join(["?" for col in self.cols])})'
        ] + super().clauses[1:] # skip the FROM clause
        



class Delete(BaseStatement):
    "SQL statement to delete some or all rows meeting given criteria"
    @property
    def clauses(self):
        clauses = super().clauses
        clauses[0] = 'DELETE ' + clauses[0] # makes 'DELETE FROM ...'
        return clauses



class Select(BaseStatement):
    "SQL statement to return some or all rows meeting given criteria"
    def __init__(self,
        table,
        cols: list[Column] = '*',
        where: Expression = None,
        group_by: list[Column] = None,
        having: Expression = None,
        **kwargs
    ):
        """
        Select statement constructor.
        
        Any unknown keyword arguments are interpreted as WHERE
        conditions constraining the value of a column in the given
        table. For instance, 'first_name="Jerry"' is equivalent to
        'where=[table].first_name == "Jerry"'
        
        Arguments:
            table: the Table object from which to select values
            cols: list of Column objects, or Expressions representing
                virtual columns, that the statement should select. For
                ease of use, columns can also be referenced via names
                rather than Column objects.
            where: Expression constraining which rows to select
            group_by: equivalent to SQL 'GROUP BY' clause
            having: equivalent to SQL 'HAVING' clause
        """
        super().__init__(table = table, where = where, **kwargs)
        self.raw_columns = cols
        for key, val in self.unknown_kwargs.items():
            new_criteria = Expression(self.table._columns[key], '=', val)
            if self.where:
                self.where = self.where & new_criteria
            else:
                self.where = new_criteria
        
        if self.raw_columns == '*' or type(self.raw_columns) is Expression:
            self.columns = self.raw_columns
        else:
            self.columns = [self._resolve_column(c) for c in self.raw_columns]
        
        if group_by:
            if type(group_by) in [list, set, tuple]:
                self.group_by = [self._resolve_column(c) for c in group_by]
            else:
                self.group_by = self._resolve_column(group_by)
        else:
            self.group_by = None
        self.having = having
    
    @property
    def clauses(self):
        if type(self.columns) in [list, tuple, set]:
            select = f'SELECT {", ".join([str(c) for c in self.columns])}'
        else:
            select = f'SELECT {self.columns}'
        clauses = [select] + super().clauses
        
        if self.group_by:
            if self.group_by:
                if type(self.group_by) in [list, tuple, set]:
                    group_by_clause = f'GROUP BY ({", ".join(self.group_by)})'
                else:
                    group_by_clause = f'GROUP BY {self.group_by}'
            
            # find the WHERE clause, if present, to append before it
            group_by_clause = f'GROUP BY {self.group_by}'
            for i, clause in enumerate(clauses):
                if clause.startswith('WHERE'):
                    clauses.insert(i, group_by_clause)
                    break
            else:
                clauses.append(group_by_clause)
            
        if self.having:
            clauses.append(f'HAVING {self.having}')
        
        return clauses


class Update(BaseStatement):
    def __init__(self, updates: dict[Column, Expression] = {}, **kwargs):
        self.raw_updates = updates
        super().__init__(**kwargs)
    
    @property
    def updates(self):
        updates = {}
        for key, val in self.raw_updates.items():
            updates[self._resolve_column(key)] = val
        if self.unknown_kwargs:
            kwargs = {}
            for k, v in self.unknown_kwargs.items():
                kwargs[self._resolve_column(k)] = v
            updates.update(kwargs)
        return updates
    
    @property
    def clauses(self):
        update_strs = [f'{k._name} = {v}' for k, v in self.updates.items()]
        clauses = [
            f'UPDATE {self.table}',
            f'SET {", ".join(update_strs)}'
        ] + super().clauses
        for i, clause in enumerate(clauses):
            if clause.startswith('FROM'):
                clauses[i] += ' AS hissdb_placeholder'
                break
        return clauses


########################################################################
# utility functions
########################################################################

def implicit_join(
    start_table: list,
    target_tables: list,
    prior_joins: dict = {},
) -> dict:
    """
    Return a dictionary of joins necessary to bridge the gap from the
    provided tables to the target tables, assuming that it is possible
    to do so via some combination of foreign keys. Otherwise, raise a
    SyntaxError.
    """
    joins = copy(prior_joins)
    available_tables = [start_table] + list(prior_joins.keys())
    
    i = 0
    while i < len(target_tables):
        necessary_table = target_tables[i]
        if necessary_table in available_tables:
            i += 1
            continue
        for a in available_tables:
            forward_joins = _implicit_join_helper(a, necessary_table, joins)
            if necessary_table in forward_joins:
                new_joins = forward_joins
            else:
                new_joins = _implicit_join_helper(
                    a, necessary_table, joins, True
                )
            if necessary_table in new_joins:
                joins = new_joins
                available_tables.append(necessary_table)
                i += 1
                break
        else:
            raise SyntaxError(
                f'You must manually join table "{necessary_table}" for '
                'this statement, because there is no obvious way to join '
                'it via foreign keys.'
            )
    return joins


def _implicit_join_helper(
    start_table,
    target_table,
    prior_joins: dict = {},
    reverse: bool = False,
) -> dict:
    if reverse:
        foreign_keys = target_table._foreign_keys
    else:
        foreign_keys = start_table._foreign_keys
    if not foreign_keys:
        return prior_joins
    for key, foreign_key in foreign_keys.items():
        if type(key) is str:
            if reverse:
                key = target_table[key]
            else:
                key = start_table[key]
        if type(foreign_key) is str:
            foreign_key = start_table._db[foreign_key]
        
        if (
            (not reverse and foreign_key._table not in prior_joins)
            or (reverse and key._table not in prior_joins)
        ):
            new_joins = copy(prior_joins)
            expr = Expression(key, '=', foreign_key)
            if reverse:
                new_joins[key._table] = expr
            else:
                new_joins[foreign_key._table] = expr
                
        else:
            new_joins = prior_joins
        if (
            (not reverse and foreign_key._table == target_table)
            or (reverse and key._table == target_table)
        ):
            return new_joins
        else:
            recursion = _implicit_join_helper(
                foreign_key._table,
                target_table,
                new_joins,
                reverse,
            )
            if target_table in recursion:
                return recursion
    else:
        return prior_joins
