# parser.py
import re                      # Used to match and extract parts of commands
from database import SimpleRDBMS


def parse_command(db, command):
    """
    Takes a text command (SQL-like) and runs the correct database action.
    """
    command = command.strip()          # Remove extra spaces
    upper_command = command.upper()    # Uppercase copy for keyword checks

    # ---------- CREATE TABLE ----------
    if upper_command.startswith('CREATE TABLE'):
        # Match: CREATE TABLE users (id int PRIMARY KEY, name str UNIQUE);
        match = re.search(r'CREATE TABLE (\w+) \((.*)\);', command, re.IGNORECASE)
        if not match:
            raise ValueError("Invalid CREATE syntax")

        table_name = match.group(1)
        cols_str = match.group(2)

        parts = [p.strip() for p in cols_str.split(',')]
        columns = []
        primary = None
        uniques = []

        # Read column definitions
        for part in parts:
            info = part.split()
            col_name = info[0]
            col_type = info[1].lower()
            columns.append((col_name, col_type))

            if 'PRIMARY KEY' in part.upper():
                primary = col_name
            if 'UNIQUE' in part.upper():
                uniques.append(col_name)

        if primary is None:
            raise ValueError("PRIMARY KEY is required")

        db.create_table(table_name, columns, primary, uniques)
        return None

    # ---------- INSERT ----------
    elif upper_command.startswith('INSERT INTO'):
        # Match: INSERT INTO users (id, name) VALUES (1, 'Sam');
        match = re.search(r'INSERT INTO (\w+) \((.*)\) VALUES \((.*)\);', command, re.IGNORECASE)
        if not match:
            raise ValueError("Invalid INSERT syntax")

        table_name = match.group(1)
        cols = [c.strip() for c in match.group(2).split(',')]
        vals_raw = [v.strip().strip("'") for v in match.group(3).split(',')]

        # Convert values to int when possible
        values = []
        for v in vals_raw:
            try:
                values.append(int(v))
            except ValueError:
                values.append(v)

        data = dict(zip(cols, values))
        db.insert(table_name, data)
        return None

    # ---------- SELECT ----------
    elif upper_command.startswith('SELECT'):
        # Only supports: SELECT * FROM table WHERE ...
        parts = command.split()
        if parts[1] != '*' or parts[2].upper() != 'FROM':
            raise ValueError("Only SELECT * is supported")

        table_name = parts[3]
        where = None

        # Handle WHERE clause
        if 'WHERE' in upper_command:
            where_str = command.split('WHERE')[1].strip(';').strip()
            conditions = where_str.split(' AND ')
            where = {}

            for cond in conditions:
                key, val = cond.split('=')
                key = key.strip()
                val = val.strip().strip("'")
                try:
                    val = int(val)
                except ValueError:
                    pass
                where[key] = val

        return db.select(table_name, where)

    # ---------- UPDATE ----------
    elif upper_command.startswith('UPDATE'):
        # Match: UPDATE users SET name='New' WHERE id=1;
        match = re.search(r'UPDATE (\w+) SET (.*) WHERE (.*);', command, re.IGNORECASE)
        if not match:
            raise ValueError("Invalid UPDATE syntax")

        table_name = match.group(1)
        sets_str = match.group(2)
        where_str = match.group(3)

        updates = {}
        for pair in sets_str.split(','):
            key, val = pair.split('=')
            key = key.strip()
            val = val.strip().strip("'")
            try:
                val = int(val)
            except ValueError:
                pass
            updates[key] = val

        where = {}
        for cond in where_str.split(' AND '):
            key, val = cond.split('=')
            key = key.strip()
            val = val.strip().strip("'")
            try:
                val = int(val)
            except ValueError:
                pass
            where[key] = val

        db.update(table_name, where, updates)
        return None

    # ---------- DELETE ----------
    elif upper_command.startswith('DELETE FROM'):
        # Example: DELETE FROM users WHERE id=1;
        parts = command.split()
        if parts[3].upper() != 'WHERE':
            raise ValueError("DELETE requires WHERE clause")

        table_name = parts[2]
        where_str = command.split('WHERE')[1].strip(';').strip()

        where = {}
        for cond in where_str.split(' AND '):
            key, val = cond.split('=')
            key = key.strip()
            val = val.strip().strip("'")
            try:
                val = int(val)
            except ValueError:
                pass
            where[key] = val

        db.delete(table_name, where)
        return None

    # ---------- JOIN ----------
    elif upper_command.startswith('JOIN'):
        # Example: JOIN users orders ON id;
        parts = command.split()
        if len(parts) < 5 or parts[3].upper() != 'ON':
            raise ValueError("Invalid JOIN syntax")

        table1 = parts[1]
        table2 = parts[2]
        on_column = parts[4].strip(';')

        return db.join(table1, table2, on_column)

    # ---------- UNKNOWN ----------
    else:
        raise ValueError("Unknown command")
