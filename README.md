<a href="https://github.com/raindrum/hissdb/issues"><img src="https://img.shields.io/github/issues/raindrum/hissdb" alt="GitHub issues" /></a> <a href="https://github.com/raindrum/hissdb/blob/main/LICENSE.md"><img src="https://img.shields.io/github/license/raindrum/hissdb" alt="GitHub license" /></a> <a href="https://pypi.org/project/hissdb/"><img src="https://img.shields.io/pypi/v/hissdb" alt="PyPI" /></a>

HissDB is a SQLite3 frontend with a focus on ease-of-use and tight integration with Python syntax.

# Installation

```bash
python3 -m pip install hissdb
```

# Usage

## Database Setup

```python
from hissdb import Database

# make a new db or load one from an existing path
db = Database('test_location.db')

# add some tables to the database
users = db.create_table(
	id = 'INTEGER PRIMARY KEY',
    first_name = 'TEXT',
    last_name = 'TEXT',
    age = 'INTEGER',
)
posts = db.create_table(
	user_id = 'INTEGER NOT NULL',
    text = 'TEXT',
    date = 'INTEGER',
    foreign_keys = {'user_id': users.id},
)
```

## Writing Data

```python
# inserting a row returns the new rowid
jane_id = users.insert(first_name = 'Jane', last_name = 'Doe')
john_id = users.insert(first_name = 'John', last_name = 'Doe')

posts.insert(
    user_id = john_id,
    date = 20210817,
    text = "I'm John Doe and this is my first post!"
)

# you can also insert many rows at once using a list or generator
posts.insertmany(
    cols = ['user_id', 'date', 'text'],
    rows = [
    	(jane_id, 20210814, "First!"),
    	(jane_id, 20210816, "The weather is nice today."),
    	(jane_id, 20210817, "Do you ever post on the internet just so there's content?"),
	],
)

# you can update data based on matching criteria.
# for instance, let's add a signature to each of Jane's posts
posts.update(
    text = posts.text + ' - ' + users.first_name,
    where = users.id == jane_id,
)

# finally, we must write the changes to the file
db.commit()
```

## Reading Data

```python
# get all users
names = users.fetchall(cols=['first_name', 'last_name'])
assert names == [('Jane', 'Doe'), ('John', 'Doe')]

# get a single column
first_names = users.first_name.fetchall()
assert first_names == ['Jane', 'John']


# easily write WHERE queries
does = users.id.fetchall(where=users.last_name == 'Doe')
# an even simpler equivalent:
does = users.id.fetchall(last_name='Doe')
assert does = [1, 2]

# a few methods like startswith() have been translated to SQL expressions
users.insert(first_name='Dave', last_name='Guy')
non_j_name = users.first_name.fetchone(~users.first_name.startswith('J'))
assert non_j_name == 'Dave'

# you can construct all kinds of queries
full_names = users.fetchall(cols=(users.first_name + ' ' + users.last_name))
assert full_names == [('Jane Doe',), ('John Doe',), ('Dave Guy',)]
```

For more sample code using HissDB, see the [tests](https://github.com/raindrum/hissdb/blob/main/tests/test_run.py).

If you're looking for more detailed documentation, check out the [library reference](https://raindrum.github.io/hissdb/library).
