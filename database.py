from datetime import date
import json

class SimpleRDBMS:
    def __init__(self, db_file='database.json'):
        self.tables = {}
        self.db_file = db_file
        self._load_from_file()

    # ---------------- Persistence ----------------
    def _load_from_file(self):
        try:
            with open(self.db_file, 'r') as f:
                data = json.load(f)
                self.tables = data.get('tables', {})

                # Convert date strings back to date objects
                for table_name, table in self.tables.items():
                    for row in table['rows']:
                        for col_name, col_type in table['columns']:
                            if col_type == 'date' and col_name in row and isinstance(row[col_name], str):
                                row[col_name] = date.fromisoformat(row[col_name])

                    # Rebuild indexes
                    table['indexes'] = {}
                    for col in [table['primary_key']] + table.get('uniques', []):
                        self.create_index(table_name, col)
        except FileNotFoundError:
            pass
        except json.JSONDecodeError:
            raise ValueError("Database file is corrupted")

    def _save_to_file(self):
        serializable_tables = {}
        for table_name, table in self.tables.items():
            table_copy = {k: v for k, v in table.items() if k != 'indexes'}
            table_copy['rows'] = []
            for row in table['rows']:
                new_row = {}
                for col_name, value in row.items():
                    if isinstance(value, date):
                        new_row[col_name] = value.isoformat()
                    else:
                        new_row[col_name] = value
                table_copy['rows'].append(new_row)
            serializable_tables[table_name] = table_copy

        with open(self.db_file, 'w') as f:
            json.dump({'tables': serializable_tables}, f, indent=2)

    # ---------------- Table Management ----------------
    def create_table(self, table_name, columns, primary_key, uniques=None):
        if uniques is None:
            uniques = []
        if table_name in self.tables:
            raise ValueError(f"Table {table_name} already exists")
        column_names = [col[0] for col in columns]
        if primary_key not in column_names:
            raise ValueError("Primary key must be one of the columns")
        self.tables[table_name] = {
            'columns': columns,
            'rows': [],
            'primary_key': primary_key,
            'uniques': uniques,
            'indexes': {}
        }
        self.create_index(table_name, primary_key)
        for col in uniques:
            self.create_index(table_name, col)
        self._save_to_file()

    # ---------------- Validation ----------------
    def _validate_data(self, table, data, require_pk=True):
        """
        Validate a row of data.
        `require_pk=False` for updates where PK is not being changed.
        """
        columns_dict = {name: dtype for name, dtype in table['columns']}
        for col, val in data.items():
            if col not in columns_dict:
                raise ValueError(f"Invalid column: {col}")
            dtype = columns_dict[col]
            if dtype == 'int':
                data[col] = int(val)
            elif dtype == 'str':
                data[col] = str(val)
            elif dtype == 'date':
                if isinstance(val, str):
                    data[col] = date.fromisoformat(val)
                elif not isinstance(val, date):
                    raise ValueError(f"{col} must be a date")
        if require_pk and table['primary_key'] not in data:
            raise ValueError("Primary key is required")

    # ---------------- CRUD ----------------
    def insert(self, table_name, data):
        table = self.tables.get(table_name)
        if not table:
            raise ValueError("Table not found")
        self._validate_data(table, data, require_pk=True)

        pk = table['primary_key']
        if any(row[pk] == data[pk] for row in table['rows']):
            raise ValueError("Duplicate primary key")
        for col in table['uniques']:
            if col in data and any(row.get(col) == data[col] for row in table['rows']):
                raise ValueError(f"Duplicate value in unique column: {col}")

        table['rows'].append(data)
        self._update_indexes(table_name, len(table['rows']) - 1)
        self._save_to_file()

    def select(self, table_name, where=None):
        table = self.tables.get(table_name)
        if not table:
            raise ValueError("Table not found")
        if where is None:
            return [row.copy() for row in table['rows']]
        results = [
            row.copy()
            for row in table['rows']
            if all(row.get(k) == v for k, v in where.items())
        ]
        return results

    def update(self, table_name, where, updates):
        table = self.tables.get(table_name)
        if not table:
            raise ValueError("Table not found")
        pk = table['primary_key']
        if pk not in where:
            raise ValueError("Primary key required for update")
        for idx, row in enumerate(table['rows']):
            if row[pk] == where[pk]:
                self._validate_data(table, updates, require_pk=False)
                for k, v in updates.items():
                    if k == pk:
                        raise ValueError("Cannot update primary key")
                    row[k] = v
                self._update_indexes(table_name, idx, rebuild=True)
                self._save_to_file()
                return
        raise ValueError("Row not found")

    def delete(self, table_name, where):
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

    # ---------------- Indexing ----------------
    def create_index(self, table_name, column):
        table = self.tables.get(table_name)
        if not table:
            raise ValueError("Table not found")
        table['indexes'][column] = {}
        self._rebuild_indexes(table_name, [column])

    def _update_indexes(self, table_name, row_idx, rebuild=False):
        table = self.tables[table_name]
        row = table['rows'][row_idx]
        for col in table['indexes']:
            if rebuild:
                self._rebuild_indexes(table_name, [col])
            else:
                val = row.get(col)
                table['indexes'][col].setdefault(val, []).append(row_idx)

    def _rebuild_indexes(self, table_name, columns=None):
        table = self.tables[table_name]
        if columns is None:
            columns = list(table['indexes'].keys())
        for col in columns:
            table['indexes'][col] = {}
            for idx, row in enumerate(table['rows']):
                table['indexes'][col].setdefault(row.get(col), []).append(idx)

    # ---------------- Join ----------------
    def join(self, table_name1, table_name2, on_column):
        t1 = self.tables.get(table_name1)
        t2 = self.tables.get(table_name2)
        if not t1 or not t2:
            raise ValueError("Table not found")
        results = []
        # Attempt to match columns intelligently: if on_column missing in t2, try primary key
        for r1 in t1['rows']:
            for r2 in t2['rows']:
                val1 = r1.get(on_column)
                val2 = r2.get(on_column) or r2.get(t2['primary_key'])
                if val1 == val2:
                    combined = r1.copy()
                    for k, v in r2.items():
                        combined[f"{table_name2}_{k}"] = v
                    results.append(combined)
        return results
