#the purpose of the following script is to copy the data of an mssql db to a sqlite db
import pyodbc
import sqlite3

#establish connection to sqlite database / create it if does not exist
conSQLite = sqlite3.connect('MenuListSqlite.sqlite')
SqliteCursor = conSQLite.cursor()

#create tables if they dont exist
SqliteCursor.execute(
    '''CREATE TABLE IF NOT EXISTS ItemCategory 
    (
        Id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
        Name TEXT NOT NULL
    ) STRICT;
    '''
)    
SqliteCursor.execute(
    '''CREATE TABLE IF NOT EXISTS Item
    (
        Id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
        ItemCategory INTEGER NOT NULL ,
        Name TEXT NOT NULL,
        FOREIGN KEY(ItemCategory) REFERENCES ItemCategory(Id)
    ) STRICT;
    '''
)
SqliteCursor.execute(
    '''CREATE TABLE IF NOT EXISTS ShoppingList 
    (
        Id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, 
        Date TEXT NOT NULL,
        Comments TEXT
    ) STRICT;
    '''
)
SqliteCursor.execute(
    '''CREATE TABLE IF NOT EXISTS ShoppingListDetails
    (
        Id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
        ShoppingListId INTEGER NOT NULL,
        RelatedObjectType INTEGER NOT NULL,
        RelatedObjectId INTEGER NOT NULL,
        Remarks TEXT,
        FOREIGN KEY(ShoppingListId) REFERENCES ShoppingList(Id)
    ) STRICT;
    '''
)
SqliteCursor.execute(
    '''CREATE TABLE IF NOT EXISTS IngredientCategory
    (
        Id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
        Name Text NOT NULL
    ) STRICT;
    '''
)
SqliteCursor.execute(
    '''CREATE TABLE IF NOT EXISTS Ingredient
    (
        Id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
        IngredientCategory INTEGER NOT NULL,
        Name TEXT NOT NULL,
        FOREIGN KEY(IngredientCategory) REFERENCES IngredientCategory(Id)
    ) STRICT;
    '''
)
SqliteCursor.execute(
    '''CREATE TABLE IF NOT EXISTS PlateCategory
    (
        Id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
        Name TEXT NOT NULL
    ) STRICT;
    '''
)
SqliteCursor.execute(
    '''CREATE TABLE IF NOT EXISTS Plate
    (
        Id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
        PlateCategory INTEGER NOT NULL,
        Name TEXT NOT NULL,
        Recipe TEXT,
        FOREIGN KEY(PlateCategory) REFERENCES PlateCategory(Id)
    ) STRICT;
    '''
)
SqliteCursor.execute(
    '''CREATE TABLE IF NOT EXISTS Menu
    (
        Id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
        Date TEXT NOT NULL,
        Plate INTEGER NOT NULL,
        FOREIGN KEY(PLATE) REFERENCES PLATE(Id)
    ) STRICT;
    '''
)
SqliteCursor.execute(
    '''CREATE TABLE IF NOT EXISTS PlateIngredients
    (
        Id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
        PlateId INTEGER NOT NULL,
        IngredientId INTEGER NOT NULL,
        FOREIGN KEY(PlateId) REFERENCES Plate(Id) ON DELETE CASCADE,
        FOREIGN KEY(IngredientId) REFERENCES Ingredient(Id)
    ) STRICT;
    '''
)

#establish connection to mssql database  
conMS = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                      "Server=localhost;"
                      "Database=MenuListDB;"
                      "Trusted_Connection=yes;")                    

conMS.setdecoding(pyodbc.SQL_CHAR, encoding='utf-8')
conMS.setdecoding(pyodbc.SQL_WCHAR, encoding='utf-8')
conMS.setencoding(encoding='utf-8')

Mscursor = conMS.cursor()

#we will now read from mssql db and insert into sqlite db in the appropriate order

#ItemCategory
Mscursor.execute('SELECT Id, Name FROM ItemsCategory')
rows = Mscursor.fetchall()
try:
    SqliteCursor.executemany("INSERT INTO ItemCategory VALUES (?,?)", rows)
except sqlite3.Error as er:
    print('er: ', er)

#Item
Mscursor.execute('SELECT Id, ItemCategory, Name FROM Items')
rows = Mscursor.fetchall()
try:
    SqliteCursor.executemany("INSERT INTO Item VALUES (?,?,?)", rows)
except sqlite3.Error as er:
    print('er: ', er)

#ShoppingList
Mscursor.execute('SELECT Id, CONVERT(datetime, date) AS Date, Comments FROM ShoppingList')
rows = Mscursor.fetchall()
try:
    for row in rows:
        dt = str(row[1].strftime('%Y-%m-%d'))
        SqliteCursor.execute("INSERT INTO ShoppingList VALUES (?,?,?)", [row[0], dt, row[2]])
except sqlite3.Error as er:
    print('er: ', er)

#ShoppingListDetails
Mscursor.execute('SELECT Id, ShoppingListId, RelatedObjectType, RelatedObjectId, Remarks FROM ShoppingListDetails')
rows = Mscursor.fetchall()
try:
    SqliteCursor.executemany("INSERT INTO ShoppingListDetails VALUES (?,?,?,?,?)", rows)
except sqlite3.Error as er:
    print('er: ', er)

#IngredientCategory
Mscursor.execute('SELECT Id, Name FROM IngredientsCategory')
rows = Mscursor.fetchall()
try:
    SqliteCursor.executemany("INSERT INTO IngredientCategory VALUES (?,?)", rows)
except sqlite3.Error as er:
    print('er: ', er)

#Ingredient
Mscursor.execute('SELECT Id, IngredientCategory, Name FROM Ingredients')
rows = Mscursor.fetchall()
try:
    SqliteCursor.executemany("INSERT INTO Ingredient VALUES (?,?,?)", rows)
except sqlite3.Error as er:
    print('er: ', er)

#PlateCategory
Mscursor.execute('SELECT Id, Name FROM PlateCategory')
rows = Mscursor.fetchall()
try:
    SqliteCursor.executemany("INSERT INTO PlateCategory VALUES (?,?)", rows)
except sqlite3.Error as er:
    print('er: ', er)

#Plate
Mscursor.execute('SELECT Id, PlateCategory, Name, Recipe FROM Plates')
rows = Mscursor.fetchall()
try:
    SqliteCursor.executemany("INSERT INTO Plate VALUES (?,?,?,?)", rows)
except sqlite3.Error as er:
    print('er: ', er)

#Menu
Mscursor.execute('SELECT Id, CONVERT(datetime, date) AS Date, Plate FROM Menu')
rows = Mscursor.fetchall()
for row in rows:
    try:
        dt = row[1].strftime('%Y-%m-%d')
        SqliteCursor.execute("INSERT INTO Menu VALUES (?,?,?)", [row[0], dt, row[2]])
    except sqlite3.Error as er:
        print('er: ', er)

#PlateIngredients
Mscursor.execute('SELECT Id, PlateId, IngredientId FROM PlateIngredients')
rows = Mscursor.fetchall()
try:
    SqliteCursor.executemany("INSERT INTO PlateIngredients VALUES (?,?,?)", rows)
except sqlite3.Error as er:
    print('er: ', er)

#print the plate tables from the sqlite db
SqliteCursor.execute('SELECT * FROM Menu')
rows = SqliteCursor.fetchall()
for row in rows:
    print(row)

Mscursor.close()
conMS.close()

SqliteCursor.close()
conSQLite.close()