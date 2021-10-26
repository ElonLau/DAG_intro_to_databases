# Basics of Relational Databases


# Import create_engine
from sqlalchemy import create_engine

# Create an engine that connects to the census.sqlite file: engine
engine = create_engine('sqlite:///census.sqlite')

# Print table names
print(engine.table_names())

"""
<script.py> output:
    ['census', 'state_fact']

"""

# -----------------------------------------

# Import create_engine, MetaData, and Table
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy import MetaData

# Create engine: engine
engine = create_engine('sqlite:///census.sqlite')

# Create a metadata object: metadata
metadata = MetaData()

# Reflect census table from the engine: census
census = Table('census', metadata, autoload=True, autoload_with=engine)

# Print census table metadata
print(repr(census))

"""

<script.py> output:
    Table('census', MetaData(bind=None), Column('state', VARCHAR(length=30), table=<census>),
    Column('sex', VARCHAR(length=1), table=<census>), Column('age', INTEGER(), table=<census>),
    Column('pop2000', INTEGER(), table=<census>), Column('pop2008', INTEGER(), table=<census>), schema=None)

"""

# ---------------------------------------------------------------------------

from sqlalchemy import create_engine, MetaData, Table

engine = create_engine('sqlite:///census.sqlite')

metadata = MetaData()

# Reflect the census table from the engine: census
census = Table('census', metadata, autoload=True, autoload_with=engine)

# Print the column names
print(census.columns.keys())

# Print full metadata of census
print(repr(metadata.tables['census']))

"""

<script.py> output:
    ['state', 'sex', 'age', 'pop2000', 'pop2008']
    Table('census', MetaData(bind=None), Column('state', VARCHAR(length=30), table=<census>),
    Column('sex', VARCHAR(length=1), table=<census>), Column('age', INTEGER(), table=<census>),
    Column('pop2000', INTEGER(), table=<census>), Column('pop2008', INTEGER(), table=<census>), schema=None)

"""

# ---------------------------------------------------------------------------

from sqlalchemy import create_engine
engine = create_engine('sqlite:///census.sqlite')

# Create a connection on engine
connection = engine.connect()

# Build select statement for census table: stmt
stmt = 'SELECT * from census'

# Execute the statement and fetch the results: results
results = connection.execute(stmt).fetchall()

# Print results
print(results)

"""

<script.py> output:
    [('Illinois', 'M', 0, 89600, 95012), ('Illinois', 'M', 1, 88445, 91829), ..............

"""



# ---------------------------------------------------------------------------

# Import select
from sqlalchemy import select

# Reflect census table via engine: census
census = Table('census', metadata, autoload=True, autoload_with=engine)

# Build select statement for census table: stmt
stmt = select([census])

# Print the emitted statement to see the SQL string
print(stmt)

# Execute the statement on connection and fetch 10 records: result
results = connection.execute(stmt).fetchmany(size=10)

# Execute the statement and print the results
print(results)

"""
<script.py> output:
    SELECT census.state, census.sex, census.age, census.pop2000, census.pop2008
    FROM census
    [('Illinois', 'M', 0, 89600, 95012), ('Illinois', 'M', 1, 88445, 91829),
    ('Illinois', 'M', 2, 88729, 89547), ('Illinois', 'M', 3, 88868, 90037),
    ('Illinois', 'M', 4, 91947, 91111), ('Illinois', 'M', 5, 93894, 89802),
    ('Illinois', 'M', 6, 93676, 88931), ('Illinois', 'M', 7, 94818, 90940), (
    'Illinois', 'M', 8, 95035, 86943), ('Illinois', 'M', 9, 96436, 86055)]
"""

# ---------------------------------------------------------------------------

# Get the first row of the results by using an index: first_row
first_row = results[0]

# Print the first row of the results
print(first_row)

# Print the first column of the first row by accessing it by its index
print(first_row[0])

# Print the 'state' column of the first row by using its name
print(first_row['state'])

"""
<script.py> output:
    ('Illinois', 'M', 0, 89600, 95012)
    Illinois
    Illinois

"""


# -----------------------------------------------------------------------------

# Import create_engine function
from sqlalchemy import create_engine

# Create an engine to the census database
engine = create_engine('postgresql+psycopg2://student:datacamp@postgresql.csrrinzqubik.us-east-1.rds.amazonaws.com:5432/census')

# Use the .table_names() method on the engine to print the table names
print(engine.table_names())


"""

<script.py> output:
    ['census', 'new_data', 'census1', 'data', 'data1', 'employees', 'employees3', 'employees_2', 'nyc_jobs',
    'final_orders', 'state_fact', 'orders', 'users', 'vrska']

"""

# -----------------------------------------------------------------------------

# Create a select query: stmt
stmt = select([census])

# Add a where clause to filter the results to only those for New York : stmt_filtered
stmt = stmt.where(census.columns.state == 'New York')

# Execute the query to retrieve all the data returned: results
results = connection.execute(stmt).fetchall()

# Loop over the results and print the age, sex, and pop2000
for result in results:
    print(result.age, result.sex, result.pop2000)

"""

<script.py> output:
    0 M 126237
    1 M 124008
    2 M 124725
    3 M 126697
    4 M 131357
    5 M 133095
    6 M 134203
    7 M 137986
    8 M 139455
    9 M 142454
    10 M 145621
    11 M 138746
    12 M 135565
    13 M 132288
    14 M 132388
    15 M 131959
    16 M 130189
    17 M 132566
    18 M 132672
    19 M 133654
    20 M 132121
    21 M 126166
    22 M 123215
    23 M 121282
    24 M 118953
    25 M 123151
    26 M 118727
    27 M 122359
    28 M 128651
    29 M 140687
    30 M 149558
    31 M 139477
    32 M 138911
    33 M 139031
    34 M 145440
    35 M 156168
    36 M 153840
    37 M 152078
    38 M 150765
    39 M 152606
    40 M 159345
    41 M 148628
    42 M 147892
    43 M 144195
    44 M 139354
    45 M 141953
    46 M 131875
    47 M 128767
    48 M 125406
    49 M 124155
    50 M 125955
    51 M 118542
    52 M 118532
    53 M 124418
    54 M 95025
    55 M 92652
    56 M 90096
    57 M 95340
    58 M 83273
    59 M 77213
    60 M 77054
    61 M 72212
    62 M 70967
    63 M 66461
    64 M 64361
    65 M 64385
    66 M 58819
    67 M 58176
    68 M 57310
    69 M 57057
    70 M 57761
    71 M 53775
    72 M 53568
    73 M 51263
    74 M 48440
    75 M 46702
    76 M 43508
    77 M 40730
    78 M 37950
    79 M 35774
    80 M 32453
    81 M 26803
    82 M 25041
    83 M 21687
    84 M 18873
    85 M 88366
    0 F 120355
    1 F 118219
    2 F 119577
    3 F 121029
    4 F 125247
    5 F 128227
    6 F 128428
    7 F 131161
    8 F 133646
    9 F 135746
    10 F 138287
    11 F 131904
    12 F 129028
    13 F 126571
    14 F 125682
    15 F 125409
    16 F 122770
    17 F 123978
    18 F 125307
    19 F 127956
    20 F 129184
    21 F 124575
    22 F 123701
    23 F 124108
    24 F 122624
    25 F 127474
    26 F 123033
    27 F 128125
    28 F 134795
    29 F 146832
    30 F 152973
    31 F 144001
    32 F 143930
    33 F 144653
    34 F 151147
    35 F 159228
    36 F 159999
    37 F 157911
    38 F 156103
    39 F 159284
    40 F 163331
    41 F 155353
    42 F 153688
    43 F 151615
    44 F 146774
    45 F 148318
    46 F 139802
    47 F 138062
    48 F 134107
    49 F 134399
    50 F 136630
    51 F 130843
    52 F 130196
    53 F 136064
    54 F 106579
    55 F 104847
    56 F 101857
    57 F 108406
    58 F 94346
    59 F 88584
    60 F 88932
    61 F 82899
    62 F 82172
    63 F 77171
    64 F 76032
    65 F 76498
    66 F 70465
    67 F 71088
    68 F 70847
    69 F 71377
    70 F 74378
    71 F 70611
    72 F 70513
    73 F 69156
    74 F 68042
    75 F 68410
    76 F 64971
    77 F 61287
    78 F 58911
    79 F 56865
    80 F 54553
    81 F 46381
    82 F 45599
    83 F 40525
    84 F 37436
    85 F 226378

"""

# -----------------------------------------------------------------------------

# Define a list of states for which we want results
states = ['New York', 'California', 'Texas']

# Create a query for the census table: stmt
stmt = select([census])

# Append a where clause to match all the states in_ the list states
stmt = stmt.where(census.columns.state.in_(states))

# Loop over the ResultProxy and print the state and its population in 2000
for result in connection.execute(stmt):
    print(result.state, result.pop2000)

"""

<script.py> output:
    New York 126237
    New York 124008
    New York 124725
    New York 126697
    New York 131357
    New York 133095

"""

# -----------------------------------------------------------------------------

# Import and_
from sqlalchemy import and_

# Build a query for the census table: stmt
stmt = select([census])

# Append a where clause to select only non-male records from California using and_
stmt = stmt.where(
    # The state of California with a non-male sex
    and_(census.columns.state == 'California',
         census.columns.sex != 'M'
         )
)

# Loop over the ResultProxy printing the age and sex
for result in connection.execute(stmt):
    print(result.age, result.sex)


"""
<script.py> output:
    0 F
    1 F
    2 F
    3 F
    4 F
    5 F
"""

# -----------------------------------------------------------------------------
# Build a query to select the state column: stmt
stmt = select([census.columns.state])

# Order stmt by the state column
stmt = stmt.order_by(census.columns.state)

# Execute the query and store the results: results
results = connection.execute(stmt).fetchall()

# Print the first 10 results
print(results[:10])


"""
<script.py> output:
    [('Alabama',), ('Alabama',), ('Alabama',), ('Alabama',), ('Alabama',), ('Alabama',),
    ('Alabama',), ('Alabama',), ('Alabama',), ('Alabama',)]

"""

# -----------------------------------------------------------------------------

# Build a query to select state and age: stmt
stmt = select([census.columns.state,census.columns.age])

# Append order by to ascend by state and descend by age
stmt = stmt.order_by(census.columns.state,desc(census.columns.age))

# Execute the statement and store all the records: results
results = connection.execute(stmt).fetchall()

# Print the first 20 results
print(results[:20])

"""
<script.py> output:
    [('Alabama', 85), ('Alabama', 85), ('Alabama', 84), ('Alabama', 84), ('Alabama', 83),
     ('Alabama', 83), ('Alabama', 82), ('Alabama', 82), ('Alabama', 81), ('Alabama', 81),
     ('Alabama', 80), ('Alabama', 80), ('Alabama', 79), ('Alabama', 79), ('Alabama', 78),
     ('Alabama', 78), ('Alabama', 77), ('Alabama', 77), ('Alabama', 76), ('Alabama', 76)]

"""

# -----------------------------------------------------------------------------

# Build a query to count the distinct states values: stmt
stmt = select([func.count(census.columns.state.distinct())])

# Execute the query and store the scalar result: distinct_state_count
distinct_state_count = connection.execute(stmt).scalar()

# Print the distinct_state_count
print(distinct_state_count)

"""
<script.py> output:
    51
"""


# -----------------------------------------------------------------------------

# Import func
from sqlalchemy import func

# Build a query to select the state and count of ages by state: stmt
stmt = select([census.columns.state, func.count(census.columns.age)])

# Group stmt by state
stmt = stmt.group_by(census.columns.state)

# Execute the statement and store all the records: results
results = connection.execute(stmt).fetchall()

# Print results
print(results)

# Print the keys/column names of the results returned
print(results[0].keys())

"""
<script.py> output:
    [('Alabama', 172), ('Alaska', 172), ('Arizona', 172), ('Arkansas', 172), ('California', 172),
     ('Colorado', 172), ('Connecticut', 172), ('Delaware', 172), ('District of Columbia', 172),
     ('Florida', 172), ('Georgia', 172), ('Hawaii', 172), ('Idaho', 172), ('Illinois', 172),
     ('Indiana', 172), ('Iowa', 172), ('Kansas', 172), ('Kentucky', 172), ('Louisiana', 172),
     ('Maine', 172), ('Maryland', 172), ('Massachusetts', 172), ('Michigan', 172), ('Minnesota', 172),
     ('Mississippi', 172), ('Missouri', 172), ('Montana', 172), ('Nebraska', 172), ('Nevada', 172),
     ('New Hampshire', 172), ('New Jersey', 172), ('New Mexico', 172), ('New York', 172), ('North Carolina', 172),
     ('North Dakota', 172), ('Ohio', 172), ('Oklahoma', 172), ('Oregon', 172), ('Pennsylvania', 172), ('Rhode Island', 172),
      ('South Carolina', 172), ('South Dakota', 172), ('Tennessee', 172), ('Texas', 172),
      ('Utah', 172), ('Vermont', 172), ('Virginia', 172), ('Washington', 172), ('West Virginia', 172),
      ('Wisconsin', 172), ('Wyoming', 172)]
    ['state', 'count_1']

"""

# -----------------------------------------------------------------------------

# import pandas
import pandas as pd

# Create a DataFrame from the results: df
df = pd.DataFrame(results)

# Set column names
df.columns = results[0].keys()

# Print the DataFrame
print(df)

"""
<script.py> output:
            state  population
    0  California    36609002
    1       Texas    24214127
    2    New York    19465159
    3     Florida    18257662
    4    Illinois    12867077

"""

# -----------------------------------------------------------------------------

# Import pyplot as plt from matplotlib
import matplotlib.pyplot as plt

# Create a DataFrame from the results: df
df = pd.DataFrame(results)

# Set Column names
df.columns = results[0].keys()

# Print the DataFrame
print(df)

# Plot the DataFrame
df.plot.bar()
plt.show()

"""
<script.py> output:
            state  population
    0  California    36609002
    1       Texas    24214127
    2    New York    19465159
    3     Florida    18257662
    4    Illinois    12867077

"""

# -----------------------------------------------------------------------------
