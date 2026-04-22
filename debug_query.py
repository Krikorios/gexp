import sqlite3
conn = sqlite3.connect('data/realestate.db')
conn.row_factory = sqlite3.Row

# All documents
rows = conn.execute('''
    SELECT d.id, d.person_id, d.request_number, d.search_scope, d.page_info,
           d.page_number, d.pdf_group_id, d.status,
           p.first_name, p.family_name
    FROM documents d
    LEFT JOIN persons p ON p.id = d.person_id
    ORDER BY d.id
''').fetchall()

print("=== ALL documents ===")
for r in rows:
    d = dict(r)
    print("id=%s status=%s person_id=%s req=%r scope=%r page_info=%r pdf=%r name=%s %s" % (
        d['id'], d['status'], d['person_id'], d['request_number'],
        d['search_scope'], d['page_info'], d['pdf_group_id'],
        d['first_name'], d['family_name']))

# Persons
print("\n=== Persons ===")
persons = conn.execute('SELECT id, first_name, father_name, family_name FROM persons ORDER BY id').fetchall()
for p in persons:
    d = dict(p)
    print("id=%s %s %s %s" % (d['id'], d['first_name'], d['father_name'], d['family_name']))

# Potential duplicate persons
print("\n=== Potential duplicate persons ===")
dups = conn.execute('''
    SELECT p1.id AS pid1, p2.id AS pid2, p1.first_name, p1.family_name
    FROM persons p1
    JOIN persons p2 ON p1.id < p2.id
        AND p1.first_name = p2.first_name
        AND COALESCE(p1.family_name,'') = COALESCE(p2.family_name,'')
''').fetchall()
for d in dups:
    print(dict(d))
