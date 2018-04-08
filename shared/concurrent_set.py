import sqlite3


class ConcurrentSet:
    """Implement a thread safe set"""

    def __init__(self):
        self.connection = sqlite3.connect(':memory:')
        self.db = self.connection.cursor()
        self.db.execute('CREATE TABLE items(item text primary key)')

    def insert(self, new_item):
        self.db.execute('INSERT OR REPLACE INTO items(item) VALUES (?)', (new_item,))
        self.connection.commit()

    def remove(self, item):
        self.db.execute('DELETE FROM items WHERE item = ?', (item,))
        self.connection.commit()

    def contains(self, item):
        self.db.execute('SELECT item FROM items WHERE item = ?', (item,))
        return self.db.fetchone() is not None

    def get_items(self):
        self.db.execute('SELECT item from items')
        ret_val = []
        items = self.db.fetchall()
        for item in items:
            ret_val.append(item[0])
        return ret_val
