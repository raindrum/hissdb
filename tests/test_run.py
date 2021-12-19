from hissdb import Database, Table, Column, Select
from hissdb.functions import count

db = jane_id = john_id = posts = users = None

def test_setup_db():
    global db, users, posts
    db = Database(':memory:', verbose=True)
    
    users = db.create_table(
	    'users',
	    id = 'INTEGER PRIMARY KEY',
        first_name = 'TEXT',
        last_name = 'TEXT',
        age = 'INTEGER',
    )
    posts = db.create_table(
	    'posts',
	    user_id = 'INTEGER NOT NULL',
        text = 'TEXT',
        date = 'INTEGER',
        foreign_keys = {'user_id': db.users.id},
    )

def test_insert():
    global jane_id, john_id
    jane_id = db.users.insert(first_name = 'Jane', last_name = 'Doe')
    john_id = db.users.insert(first_name = 'John', last_name = 'Doe')
    assert jane_id == 1
    assert john_id == 2
    
    posts.insert(
        user_id = john_id,
        date = 20210817,
        text = "I'm John Doe and this is my first post!",
    )

def test_insertmany():
    users.insertmany(
        cols = ('first_name', 'last_name', 'age'),
        rows = [
            ('Michael', 'Smith', 36),
            ('Pichael', 'Smith', 37),
        ]
    )
    row_count = posts.insertmany(
        ['user_id', 'date', 'text'],
        [
        	(jane_id, 20210814, "First!"),
        	(jane_id, 20210816, "The weather is nice today."),
        	(jane_id, 20210817, "Do you ever post on the internet just to make more content?"),
	    ],
    )
    assert row_count == 3

def test_fetch():
    first_names = users.first_name.fetchall()
    assert first_names == ['Jane', 'John', 'Michael', 'Pichael']

def test_fetch_where():
    ichaels = users.first_name.fetchall(
        users.first_name.endswith('ichael')
    )
    assert ichaels == ['Michael', 'Pichael']

def test_implicit_join():
    # this test starts with the posts table and joins the users table
    # via the post table's own foreign key, posts.user_id
    posts_by_does = posts.text.fetchall(
        users.last_name == 'Doe'
    )
    assert len(posts_by_does) == 4

def test_reverse_implicit_join():
    # this test starts with the users table and joins the posts table
    # using the post table's foreign key, posts.user_id
    first_posters = users.first_name.fetchall(
        posts.text.startswith('first')
    )
    assert first_posters == ['Jane']

def test_update_rows():
    # retroactively sign Jane Doe's posts with her name
    row_count = posts.update(
        text = posts.text + ' - ' + users.first_name + ' ' + users.last_name,
        where = users.id == jane_id
    )
    assert row_count == 3
    for post in db.posts.text.fetchall(user_id=jane_id):
        assert post.endswith(' - Jane Doe')
    
    # undo the changes
    posts.update(
        text = posts.text.replace(' - Jane Doe', ''),
        where = posts.user_id == jane_id,
    )

def test_composite_column():
    row = posts.fetchone(cols = (
        posts.text,
        users.first_name + ' ' + users.last_name,
        posts.date - 1,
    ))
    assert row == (
        "I'm John Doe and this is my first post!",
        'John Doe',
        20210816,
    )

def test_multiple_conditions():
    cond1 = posts.text % '%is%'
    cond2 = users.first_name == 'Jane'
    
    meets_both = posts.text.fetchall(cond1 & cond2)
    assert len(meets_both) == 1
    
    meets_either = posts.text.fetchall(cond1 | cond2)
    assert len(meets_either) == 4

def test_invert_conditions():
    exclamation = posts.text.endswith('!')
    question = posts.text.endswith('?')
    
    nonquestions = posts.text.fetchall(~question)
    assert len(nonquestions) == 3
    
    statements1 = posts.text.fetchall(~question | ~exclamation)
    statements2 = posts.text.fetchall(~(question & exclamation))
    assert len(statements1) == 4
    assert statements2 == statements1

def test_counting():
    # count all users with 'e' in their full name
    assert users.count(
        (users.first_name + ' ' + users.last_name) % '%e%'
    ) == 4

def test_edit_table():
    posts['spiciness'] = Column(constraints='INTEGER')
    posts.update(spiciness=posts.date % 10)
    assert posts.spiciness.fetchone() > 6
    del posts.spiciness
    assert not hasattr(posts, 'spiciness')

def test_string_repl():
    users.update(
        first_name = users.first_name.replace('P', 'M')
    )
    assert users.count(first_name='Michael') == 2

def test_abs_val():
    users.age.update(-25, where=users.first_name == 'John')
    assert users.age.fetchone(first_name='John') == -25
    users.age.update(abs(users.age))
    assert users.age.fetchone(first_name='John') == 25

def test_avg_val():
    avg_age = users.fetchone(users.age.avg().floor())[0]
    assert avg_age == 32

def test_external_func():
    from hissdb.functions import group_concat
    ages_str = users.fetchone(group_concat(users.age, ', '))[0]
    assert ages_str == '25, 36, 37'

def test_group_by():
    # see how many posts each user has made
    from hissdb.functions import count
    post_counts = posts.fetchall(
        cols = (
            users.first_name + ' ' + users.last_name,
            count(posts.date)
        ),
        group_by = posts.user_id,
    )
    assert post_counts == [('Jane Doe', 3), ('John Doe', 1)]

def test_group_having():
    repeat_posters = posts.user_id.fetchall(
        group_by = posts.user_id,
        having = count(posts.text) > 1,
    )
    assert repeat_posters == [jane_id]

def test_intersect():
    repeat_poster = posts.user_id.select(
        group_by = posts.user_id,
        having = count(posts.text) > 1,
    )
    e_in_firstname = users.id.select(
        where = users.first_name % '%e%'
    )
    combined_search = repeat_poster & e_in_firstname
    results = combined_search.execute().fetchall()
    assert results == [(jane_id,)]
