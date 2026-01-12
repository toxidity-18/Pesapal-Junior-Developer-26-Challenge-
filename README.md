# Pesapal-Junior-Developer-26-Challenge : Simple Persistent RDBMS in Python

## Overview
 Pesapal Junior Dev '26 challenge. I implemented a simple RDBMS from scratch, supporting table creation with data types (int, str, date), CRUD operations, primary/unique keys, basic indexing, and joins. It includes an SQL-like parser with REPL and a trivial Flask web app (todo list) for demo. To stand out, I added JSON persistence (data saves to file), unit tests with pytest, and enhanced WHERE parsing with AND support.

Why this approach? The prompt emphasizes determination and clear thinking—I iterated from my first attempt by adding persistence for practicality and tests for reliability, showing I can think beyond basics.

## Features
- **Persistence**: Data saved to JSON—survives restarts (unorthodox addition for usability).
- **Data Types & Constraints**: int/str/date with validation; primary/unique keys prevent duplicates.
- **CRUD & Joins**: Basic operations plus inner joins.
- **Indexing**: Dict-based for faster lookups (rebuilt on changes).
- **SQL-like Interface**: Parser handles commands; REPL for interactive testing.
- **Web Demo**: Flask todo app with add/view/edit/delete.
- **Tests**: Pytest suite covering core functions.

## Setup & Running
1. Clone repo: `git clone https://github.com/toxidity-18/Pesapal-Junior-Developer-26-Challenge-`
2. Install deps: `pip install -r requirements.txt`
3. Run REPL: `python repl.py` (try commands like `CREATE TABLE users (id int PRIMARY KEY);`)
4. Run Web App: `python app.py` (visit http://127.0.0.1:5000/)
5. Run Tests: `pytest` (verifies functionality)

## Design Decisions & Implementation Notes
- **Storage (database.py)**: Used dicts/lists for simplicity (easy to implement/understand as a beginner). How: Tables as dict with 'rows' list of dicts. Why: Mimics relational structure without external DBs.
- **Persistence**: How: _save_to_file after mutations, load on init. Why: Makes it more real-world—data doesn't reset. Trade-off: File I/O overhead, but negligible for challenge.
- **Validation/Constraints**: How: _validate_data coerces types, checks uniques in insert. Why: Prevents bad data early, showing attention to integrity.
- **Parser (parser.py)**: How: Regex for extraction, split for WHERE AND. Why: Flexible yet simple; enhanced AND to handle multi-filters, adding value.
- **REPL (repl.py)**: How: Input loop with try-except. Why: User-friendly for testing, catches errors gracefully.
- **Web App (app.py)**: How: Flask routes call RDBMS methods. Why: Trivial demo as required—todo list shows CRUD in action.
- **Tests (test_database.py)**: How: Pytest fixtures for isolation. Why: Automates verification, proves robustness—few applicants might include this.
- **Learnings/Reflections**: Parsing regex was challenging—iterated via commits. Added persistence after realizing in-memory limits demos. Used AI for ideas but coded/debugged myself.

## Credits
- This project was built while learning Python and Flask, with extensive use of AI tools (Grok and ChatGPT) for guidance, examples, and debugging support.
- All final code was executed, tested, modified, and validated by me to ensure I understood how each part works.
- Additional references include official Python and Flask documentation and online tutorials.


## Future Improvements
- Add more SQL (e.g., ORDER BY).
- Optimize indexes in select.
- Handle larger data with better storage.

