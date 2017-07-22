import sqlite3
import csv
conn = sqlite3.connect('seattlelib.sqlite')
cur=conn.cursor()

cur.executescript('''
DROP TABLE IF EXISTS material;
DROP TABLE IF EXISTS title;
DROP TABLE IF EXISTS checkout;
DROP TABLE IF EXISTS creator;

CREATE TABLE material (
material_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
material_name TEXT UNIQUE
);

CREATE TABLE title (
title_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
title TEXT UNIQUE,
creator_id INTEGER,
subjects TEXT,
publisher TEXT,
pub_year TEXT
);

CREATE TABLE creator (
creator_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
creator_name TEXT UNIQUE
);

CREATE TABLE checkout (
checkout_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
checkout INTEGER,
title_id INTEGER,
material_id INTEGER,
year INTEGER,
month INTEGER
);
''')

countmod=1000000
fname = 'Checkouts_by_title.csv'
print(fname)
with open(fname,'r') as file:
    fh=csv.reader(file,delimiter=',')
    next(fh,None) # skip first header line
    count=1
    
    for line in fh:
#         print(line[2].strip())
        mat=line[2].strip()
        year=int(line[3].strip())
        month=int(line[4].strip())
        checkout=int(line[5].strip())
        title=line[6].strip()
        creator=line[7].strip()
        subjects=line[8].strip()
        publisher=line[9].strip()
        pub_year=line[10].strip() 
#         id=int(line[0].strip())
#         aisle=line[1].strip()
#         print(id,aisle)
        cur.execute('''INSERT OR IGNORE INTO material (material_name)
                VALUES( ? )''', (mat, ))
        cur.execute('SELECT material_id FROM material WHERE material_name = ? ', (mat, ))
        material_id = cur.fetchone()[0]
        
        cur.execute('''INSERT OR IGNORE INTO creator (creator_name)
                VALUES( ? )''', (creator, ))
        cur.execute('SELECT creator_id FROM creator WHERE creator_name = ? ', (creator, ))
        creator_id = cur.fetchone()[0]        
        
        cur.execute(''' INSERT OR IGNORE INTO title (title, creator_id, subjects, publisher, pub_year)
                VALUES( ?, ?, ?, ?, ? )''', (title, creator_id, subjects, publisher, pub_year))
        cur.execute('SELECT title_id FROM title WHERE title = ? ', (title, ))
        title_id = cur.fetchone()[0]
        
        checkout_id=count
        cur.execute(''' INSERT OR REPLACE INTO checkout (checkout_id, checkout, title_id, material_id, year, month)
                VALUES( ?, ?, ?, ?, ?, ? )''',(checkout_id, checkout, title_id, material_id, year, month))
#                 
        if count%countmod == 0:
            conn.commit()
            print('commit')
        count+=1
#         
conn.commit() #commits last chunk if there are any leftovers
print('finished')
cur.close()