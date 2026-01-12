# app.py
from flask import Flask, request, render_template, redirect, url_for
from database import SimpleRDBMS

# Create Flask app
app = Flask(__name__)

# Create database instance (loads saved data if available)
db = SimpleRDBMS()

# Create the todos table if it does not exist
if 'todos' not in db.tables:
    db.create_table(
        'todos',
        [('id', 'int'), ('task', 'str')],
        'id'
    )

@app.route('/', methods=['GET', 'POST'])
def index():
    # Handle form submission (add todo)
    if request.method == 'POST':
        task = request.form.get('task')
        if task:
            # Generate next ID
            next_id = len(db.select('todos')) + 1
            db.insert('todos', {'id': next_id, 'task': task})

    # Get all todos
    todos = db.select('todos')
    return render_template('index.html', todos=todos)

@app.route('/update/<int:id>', methods=['GET', 'POST'])
def update(id):
    # Update a todo item
    if request.method == 'POST':
        new_task = request.form.get('task')
        if new_task:
            db.update('todos', {'id': id}, {'task': new_task})
        return redirect(url_for('index'))

    # Show update form with current todo
    todos = db.select('todos', {'id': id})
    todo = todos[0] if todos else None
    return render_template('update.html', todo=todo)

@app.route('/delete/<int:id>')
def delete(id):
    # Delete a todo item
    db.delete('todos', {'id': id})
    return redirect(url_for('index'))

# Run the app
if __name__ == '__main__':
    app.run(debug=True)
