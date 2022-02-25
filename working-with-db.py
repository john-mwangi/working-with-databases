# %% [markdown]
# # Objectives
# Working with databases in Python:
# 1. Using pandas
# 3. Listing database tables in a database
# 2. Making credentials secure
# 4. Using SQL Alchemy
# 5. View SQL commands
#
# Broadly, you will either use either pandas or SQL Alchemy.

# %% [markdown]
# # Packages

# %%
import pandas as pd
import keyring

from sqlalchemy import inspect, create_engine
from sqlalchemy import text
from sqlalchemy import MetaData, select, Table
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import Column, String, Integer
from sqlalchemy.ext.declarative import declarative_base

# %% [markdown]
# # Database URLs
# Reference: https://docs.sqlalchemy.org/en/14/core/engines.html#database-urls

# %%
db_string = "postgresql://postgres:john@localhost:5432/chinook"

# %% [markdown]
# * the DBMS is postgresql
# * username is postgres
# * password is password
# * IP address is localhost (127.0.0.1)
# * port is 5432
# * the database name is chinook.

# %% [markdown]
# # Using Pandas
# This is the simplest and most straightforward. It reads databases into memory.

# %%
album_df = pd.read_sql(con=db_string, sql="album")
album_df

# %%
len(album_df.ArtistId.unique())
len(album_df.AlbumId.unique())

# %%
album_df = album_df.assign(
    num_artists=len(album_df.ArtistId.unique()),
    num_albums=len(album_df.AlbumId.unique()),
)

album_df

# %%
album_df.to_sql(name="album_df", con=db_string, if_exists="replace")

# %% [markdown]
# # Listing tables
# This requires `inspect` from SQLAlchemy. Because we're starting to use SQLAlchemy, we need to first create an engine.

# %%
engine = create_engine(db_string)
engine

# %%
inspector = inspect(subject=engine)
inspector

# %%
inspector.get_table_names()[:5]

# %% [markdown]
# `nullable=False` means that the column does not accept a NULL value.

# %%
inspector.get_columns("album")

# %%
inspector.get_columns("album_df")[:2]

# %% [markdown]
# # Securing credentials
# `service_name` is like a group.

# %%
keyring.set_password(
    service_name="postdb_demo", username="postgres", password="john"
)

# %%
pwd = keyring.get_password(service_name="postdb_demo", username="postgres")
pwd

# %%
pd.read_sql(
    con=f"postgresql://postgres:{pwd}@localhost:5432/chinook", sql="album"
)

# %% [markdown]
# # Using SQL Alchemy
# When working with SQL Alchemy connections, it is important to use context managers so that the connection can be released after the statement is executed.
# ## Using text
# This is the most basic one where we craft SQL statements manually.

# %%
stmt = text("SELECT * FROM album")

# %%
with engine.connect() as conn:
    res_txt = conn.execute(statement=stmt).all()

res_txt[:10]

# %% [markdown]
# ## Using select
# This is a good one for executing select statements on an SQL Alchemy `Table` object. It is important to note the following:
# * `select` prepares a SELECT statement that needs to be **executed** and results **fetched**
# * It operates on a `Table` object
# * In `Table` objects, columns are accessed via a `c` attribute.

# %%
metadata_sel = MetaData()

with engine.connect() as conn:
    album_sel_tbl = Table("album", metadata_sel, autoload_with=conn)

album_sel_tbl

# %% [markdown]
# This creates a SELECT statement that then be executed.

# %%
stmt2 = select(album_sel_tbl)
stmt3 = select(album_sel_tbl).where(album_sel_tbl.c.ArtistId == 10)
print(stmt3)

# %%
with engine.connect() as conn:
    res2 = conn.execute(statement=stmt2).fetchall()

res2[:10]

# %% [markdown]
# Lets insert a few values into the `Table` object and save it to the database.

# %%
for c in album_sel_tbl.c:
    print(c)

# %%
res2[-5:]

# %%
insert_stmt2 = album_sel_tbl.insert().values(
    AlbumId=349, Title="New Insert from Select", ArtistId=99
)
insert_stmt2

# %%
with engine.connect() as conn:
    conn.execute(statement=insert_stmt2)

# %% [markdown]
# ## Using ORM
# This is an object oriented way of handling tables in a database. The tables become classes and the columns become attributes.
#
# First, we check how to map existing tables as classes.

# %%
ABase = automap_base()

ABase.prepare(engine=engine, reflect=True)

list(ABase.classes)

# %% [markdown]
# After preparing the classes, you will now need to `query` the actual database table. At this point, it is important to know the following:
# * Since you are interacting with an actual database, you will need something to manage the connection. For this, you will need a `Session` object.
# * `Session` objects don't need a context manager.
# * To `query` contents of a database table, a connection is required and so is a `Session` object.
# * To insert values into a database table, a connection is required and so is a `Session` object.

# %%
session = Session(engine)
Album = ABase.classes.album

query1 = session.query(Album)
query2 = session.query(Album).filter(Album.ArtistId == 10)
print(query2)

# %%
for row in query1[:10]:
    print(row.AlbumId, row.Title, row.ArtistId)

# %%
[(row.AlbumId, row.Title, row.ArtistId) for row in query1[-10:]]

# %% [markdown]
# Since we're dealing with classes, we insert values as class instances then add the class instances to the session object.

# %%
orm_insert1 = Album(AlbumId=350, Title="New Insert from ORM", ArtistId=99)
orm_insert2 = Album(AlbumId=351, Title="New Insert from ORM", ArtistId=99)

# %%
session.add_all([orm_insert1, orm_insert2])
session.commit()

# %%
pd.read_sql(sql="album", con=db_string).tail()

# %% [markdown]
# ## Creating tables
# This can be done either using the `Table` class or using `declarative_base` class.
#
# If creating many tables at once, use `metadata.create_all`

# %% [markdown]
# ### Using Table
# To create a table, access the `create` attribute of the `Table` object. To update a table, use `conn.execute` to execute an insert statement/object.

# %%
metadata_tbl = MetaData()

students_tbl = Table(
    "students_tbl",
    metadata_tbl,
    Column(name="id", type_=Integer, primary_key=True),
    Column(name="email", type_=String(50), nullable=False),
    Column(name="fullname", type_=String(50)),
)

students_tbl

# %%
with engine.connect() as conn:
    students_tbl.create(bind=conn)

# %%
students_insert = students_tbl.insert().values(
    [
        {"id": 1, "email": "abc@xyz.com", "fullname": "name1"},
        {"id": 2, "email": "xyz@abc.com", "fullname": "name2"},
    ]
)

students_insert

# %%
with engine.begin() as conn:
    conn.execute(students_insert)

# %%
pd.read_sql(con=db_string, sql="students_tbl")

# %% [markdown]
# ### Using declarative_base
# To create a new table, create a new class that inherits from `declarative_base`. To update a table, use `session.add` to add instances of the class.

# %%
DBase = declarative_base()

# %%
class StudentsBase(DBase):
    __tablename__ = "students_dec"
    __table_args__ = {"extend_existing": True}

    id = Column(type_=Integer, primary_key=True)
    email = Column(type_=String(50), nullable=False)
    fullname = Column(type_=String(50), nullable=True)


# %%
with engine.connect() as conn:
    DBase.metadata.create_all(conn)

# %%
session = Session(engine)

# %%
student_inst = StudentsBase(id=1, email="email@email.com", fullname="name")

session.add(student_inst)
session.commit()

# %%
student_objs = [
    StudentsBase(id=2, email="abc@xyz.com", fullname="name1"),
    StudentsBase(id=3, email="xyz@abc.com", fullname="name2"),
]

session.add_all(instances=student_objs)
session.commit()

# %% [markdown]
# This seems to be the simplest way of adding multiple rows in ORM.

# %%
ids = [5, 6]
emails = ["abc@xyz.com", "xyz@abc.com"]
names = ["name1", "name2"]

for id, email, name in zip(ids, emails, names):
    inst = StudentsBase(id=id, email=email, fullname=name)
    session.add(inst)
    session.commit()

# %%
pd.read_sql(sql="students_dec", con=db_string)
