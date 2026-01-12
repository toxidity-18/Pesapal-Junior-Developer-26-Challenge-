# repl.py
from parser import parse_command
from database import SimpleRDBMS

# Create the database instance (loads data from database.json if it exists)
db = SimpleRDBMS()

print("Simple RDBMS REPL. Type SQL-like commands or 'exit' to quit.")

# Run forever until the user types 'exit'
while True:
    try:
        # Read a command from the user
        command = input("SQL> ")

        # Exit condition
        if command.lower().strip() == 'exit':
            break

        # Parse and execute the command
        result = parse_command(db, command)

        # Print results for SELECT or JOIN
        if result:
            print("Results:")
            for row in result:
                print(row)

    # Handle expected user errors (bad syntax, missing tables, etc.)
    except ValueError as e:
        print(f"Value Error: {e}")

    # Catch any unexpected crash to keep REPL running
    except Exception as e:
        print(f"Unexpected Error: {e}")
