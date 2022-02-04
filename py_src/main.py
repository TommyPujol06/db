import json
import os


def flush_db(database, path="people.db"):
    with open(path, "wb") as db:
        db.write(len(database).to_bytes(2, "big"))

    with open(path, "a") as db:
        db.write(json.dumps(database))


def load_db(path="people.db"):
    if not os.path.exists(path):
        return []

    with open(path, "rb") as db:
        size = int.from_bytes(db.read(2), "big")

    with open(path, "r") as db:
        db.seek(2)
        database = json.loads(db.read())
        return database


def search_db(database, key, value):
    for entry in database:
        if key in entry and entry[key] == value:
            return entry

    return None


def update_db(entry, key, value):
    entry[key] = value


database = load_db()


def demo_insert():
    n = int(input("Number of entries: "))
    for _ in range(n):
        name = input("Name: ")
        age = input("Age: ")

        data = {"name": name, "age": age}
        database.append(data)


def demo_search():
    key = input("Key to search: ")
    value = input("Value to match against: ")
    res = search_db(database, key, value)
    print(res)
    return res


def demo_update():
    key = input("Key to update: ")
    old = input("Old value: ")
    entry = search_db(database, key, old)
    if not entry:
        print("Old value not found")
        return

    value = input("New value: ")
    update_db(entry, key, value)


commands = {
    "add": demo_insert,
    "search": demo_search,
    "update": demo_update,
}

while True:
    try:
        command = input("Command: ").lower()
    except KeyboardInterrupt:
        flush_db(database)
        break

    if command not in commands:
        print("\nAvailable commands:\n", *commands.keys(), sep="\n")
        continue

    commands[command]()

flush_db(database)
