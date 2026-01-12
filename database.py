# database.py
from datetime import date        # Used to handle date values 
import json                     # Used to save/load data from a file


class SimpleRDBMS:
    def __init__(self, db_file='database.json'):
        # Holds all tables in memory
        self.tables = {}

        # File where data is saved
        self.db_file = db_file

        # Load existing data if file exists
        self._load_from_file()

    def _load_from_file(self):
        # Load database data from JSON file
        try:
            with open(self.db_file, 'r') as f:
                data = json.load(f)
                self.tables = data.get('tables', {})

                # Rebuild indexes since they are not stored in the file
                for table_name in self.tables:
                    self._rebuild_indexes(table_name)
        except FileNotFoundError:
            # No file yet → start with empty database
            pass
        except json.JSONDecodeError:
            raise ValueError("Database file is corrupted")

    def _save_to_file(self):
        # Save database to JSON file (indexes are skipped)
        with open(self.db_file, 'w') as f:
            json.dump(
                {
                    'tables': {
                        name: {k: v for k, v in table.items() if k != 'indexes'}
                        for name, table in self.tables.items()
                    }
                },
                f
            )

    def create_table(self, table_name, columns, primary_key, uniques=None):
        # Create a new table with schema and constraints
        if uniques is None:
            uniques = []

        if table_name in self.tables:
            raise ValueError("Table already exists")

        column_names = [col[0] for col in columns]
        if primary_key not in column_names:
            raise ValueError("Primary key must be a column")

        self.tables[table_name] = {
            'columns': columns,
            'rows': [],
            'primary_key': primary_key,
            'uniques': uniques,
            'indexes': {}
        }

        # Automatically index primary key
        self.create_index(table_name, primary_key)
        self._save_to_file()

    def _validate_data(self, table, data):
        # Check column names and data types
        columns = {name: dtype for name, dtype in table['columns']}

        for col, val in data.items():
            if col not in columns:
                raise ValueError(f"Invalid column: {col}")

            if columns[col] == 'int':
                data[col] = int(val)
            elif columns[col] == 'str':
                data[col] = str(val)
            elif columns[col] == 'date':
                if isinstance(val, str):
                    data[col] = date.fromisoformat(val)
                elif not isinstance(val, date):
                    raise ValueError(f"{col} must be a date")

        # Primary key must be present
        if table['primary_key'] not in data:
            raise ValueError("Primary key is required")

    def insert(self, table_name, data):
        # Add a new row to a table
        table = self.tables.get(table_name)
        if not table:
            raise ValueError("Table not found")

        self._validate_data(table, data)

        pk = table['primary_key']

        # Check primary key uniqueness
        for row in table['rows']:
            if row[pk] == data[pk]:
                raise ValueError("Duplicate primary key")

        # Check unique columns
        for col in table['uniques']:
            for row in table['rows']:
                if row.get(col) == data.get(col):
                    raise ValueError(f"Duplicate value in {col}")

        table['rows'].append(data)
        self._update_indexes(table_name, len(table['rows']) - 1)
        self._save_to_file()

    def select(self, table_name, where=None):
        # Get rows from a table
        table = self.tables.get(table_name)
        if not table:
            raise ValueError("Table not found")

        if where is None:
            return [row.copy() for row in table['rows']]

        # Filter rows based on conditions
        return [
            row.copy()
            for row in table['rows']
            if all(row.get(k) == v for k, v in where.items())
        ]

    def update(self, table_name, where, updates):
        # Update a row using primary key
        table = self.tables.get(table_name)
        if not table:
            raise ValueError("Table not found")

        pk = table['primary_key']
        if pk not in where:
            raise ValueError("Primary key required for update")

        for idx, row in enumerate(table['rows']):
            if row[pk] == where[pk]:
                self._validate_data(table, updates)

                for key, val in updates.items():
                    if key == pk:
                        raise ValueError("Cannot update primary key")
                    row[key] = val

                self._update_indexes(table_name, idx, rebuild=True)
                self._save_to_file()
                return

        raise ValueError("Row not found")

    def delete(self, table_name, where):
        # Delete a row using primary key
        table = self.tables.get(table_name)
        if not table:
            raise ValueError("Table not found")

        pk = table['primary_key']
        if pk not in where:
            raise ValueError("Primary key required for delete")

        for idx, row in enumerate(table['rows']):
            if row[pk] == where[pk]:
                del table['rows'][idx]
                self._rebuild_indexes(table_name)
                self._save_to_file()
                return

        raise ValueError("Row not found")

    def create_index(self, table_name, column):
        # Create an index on a column
        table = self.tables.get(table_name)
        if not table:
            raise ValueError("Table not found")

        table['indexes'][column] = {}
        self._rebuild_indexes(table_name, [column])

    def _update_indexes(self, table_name, row_idx, rebuild=False):
        # Update index entries when a row is added or changed
        table = self.tables[table_name]
        row = table['rows'][row_idx]

        for col in table['indexes']:
            if rebuild:
                self._rebuild_indexes(table_name, [col])
            else:
                val = row.get(col)
                table['indexes'][col].setdefault(val, []).append(row_idx)

    def _rebuild_indexes(self, table_name, columns=None):
        # Rebuild indexes from scratch
        table = self.tables[table_name]
        if columns is None:
            columns = table['indexes'].keys()

        for col in columns:
            table['indexes'][col] = {}
            for i, row in enumerate(table['rows']):
                table['indexes'][col].setdefault(row.get(col), []).append(i)

    def join(self, table_name1, table_name2, on_column):
        # Simple inner join between two tables
        t1 = self.tables.get(table_name1)
        t2 = self.tables.get(table_name2)

        if not t1 or not t2:
            raise ValueError("Table not found")

        results = []
        for r1 in t1['rows']:
            for r2 in t2['rows']:
                if r1.get(on_column) == r2.get(on_column):
                    row = r1.copy()
                    for k, v in r2.items():
                        row[f"{table_name2}_{k}"] = v
                    results.append(row)

        return results
