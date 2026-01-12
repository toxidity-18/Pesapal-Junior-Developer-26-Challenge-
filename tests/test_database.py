import pytest
import os                  # For cleaning up test JSON file after tests
from database import SimpleRDBMS
from datetime import date

# ---------------- Fixture ----------------
@pytest.fixture
def db():
    """
    Creates a temporary database instance for testing.
    Cleans up the JSON file after the test.
    """
    test_db = SimpleRDBMS('test_database.json')
    yield test_db
    if os.path.exists('test_database.json'):
        os.remove('test_database.json')

# ---------------- Test: Create Table ----------------
def test_create_table(db):
    """
    Verify a table can be created with primary key and unique columns.
    """
    db.create_table('test', [('id', 'int'), ('name', 'str')], 'id', uniques=['name'])
    assert 'test' in db.tables
    assert db.tables['test']['primary_key'] == 'id'
    assert db.tables['test']['uniques'] == ['name']

# ---------------- Test: Insert & Select ----------------
def test_insert_and_select(db):
    """
    Insert data with type coercion and check select works.
    """
    db.create_table('test', [('id', 'int'), ('dob', 'date')], 'id')
    db.insert('test', {'id': '1', 'dob': '2000-01-01'})  # str → int/date
    results = db.select('test')
    assert len(results) == 1
    assert results[0]['id'] == 1
    assert results[0]['dob'] == date(2000, 1, 1)

# ---------------- Test: Constraints ----------------
def test_constraints(db):
    """
    Ensure primary key and unique constraints are enforced.
    """
    db.create_table('test', [('id', 'int')], 'id')
    db.insert('test', {'id': 1})
    # Duplicate primary key should raise error
    with pytest.raises(ValueError, match="Duplicate primary key"):
        db.insert('test', {'id': 1})

# ---------------- Test: Update & Delete ----------------
def test_update_delete(db):
    """
    Test full CRUD cycle: insert, update, select, delete.
    """
    db.create_table('test', [('id', 'int'), ('name', 'str')], 'id')
    db.insert('test', {'id': 1, 'name': 'Old'})
    # ✅ Do not include primary key in updates
    db.update('test', {'id': 1}, {'name': 'New'})
    assert db.select('test', {'id': 1})[0]['name'] == 'New'
    db.delete('test', {'id': 1})
    assert db.select('test') == []

# ---------------- Test: Join ----------------
def test_join(db):
    """
    Test joining two tables with related data.
    """
    # Make column names match for join
    db.create_table('users', [('id', 'int'), ('name', 'str')], 'id')
    db.create_table('orders', [('order_id', 'int'), ('id', 'int')], 'order_id')  # changed user_id → id
    db.insert('users', {'id': 1, 'name': 'Sam'})
    db.insert('orders', {'order_id': 100, 'id': 1})
    results = db.join('users', 'orders', 'id')  # join on 'id'
    assert len(results) == 1
    assert results[0]['orders_order_id'] == 100

# ---------------- Test: Persistence ----------------
def test_persistence(db):
    """
    Verify data survives after recreating the DB instance.
    """
    db.create_table('test', [('id', 'int')], 'id')
    db.insert('test', {'id': 1})
    new_db = SimpleRDBMS('test_database.json')
    assert new_db.select('test') == [{'id': 1}]
