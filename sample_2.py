
# Advanced SQLAlchemy Queries
# Connecting to MySQL databases

# Import create_engine function
from sqlalchemy import create_engine

# Create an engine to the census database
engine = create_engine('mysql+pymysql://student:datacamp@courses.csrrinzqubik.us-east-1.rds.amazonaws.com:3306/census')

# Print the table names
print(engine.table_names())

"""
<script.py> output:
    ['census', 'state_fact']
"""

# ------------------------------------------------------------------------------

# Build query to return state names by population difference from 2008 to 2000: stmt
stmt = select([census.columns.state, (census.columns.pop2008-census.columns.pop2000).label('pop_change')])

# Append group by for the state: stmt_grouped
stmt_grouped = stmt.group_by(census.columns.state)

# Append order by for pop_change descendingly: stmt_ordered
stmt_ordered = stmt_grouped.order_by(desc('pop_change'))

# Return only 5 results: stmt_top5
stmt_top5 = stmt_ordered.limit(5)

# Use connection to execute stmt_top5 and fetch all results
results = connection.execute(stmt_top5).fetchall()

# Print the state and population change for each record
for result in results:
    print('{}:{}'.format(result.state, result.pop_change))


"""

<script.py> output:
    California:105705
    Florida:100984
    Texas:51901
    New York:47098
    Pennsylvania:42387

"""

# ------------------------------------------------------------------------------

# import case, cast and Float from sqlalchemy
from sqlalchemy import case, cast,Float

# Build an expression to calculate female population in 2000
female_pop2000 = func.sum(
    case([
        (census.columns.sex == 'F', census.columns.pop2000)
    ], else_=0))

# Cast an expression to calculate total population in 2000 to Float
total_pop2000 = cast(func.sum(census.columns.pop2000), Float)

# Build a query to calculate the percentage of women in 2000: stmt
stmt = select([female_pop2000 / total_pop2000 * 100])

# Execute the query and store the scalar result: percent_female
percent_female = connection.execute(stmt).scalar()

# Print the percentage
print(percent_female)


"""
<script.py> output:
    51.0946743229
"""

# ------------------------------------------------------------------------------

# Build a statement to join census and state_fact tables: stmt
stmt = select([census.columns.pop2000, state_fact.columns.abbreviation])

# Execute the statement and get the first result: result
result = connection.execute(stmt).first()

# Loop over the keys in the result object and print the key and value
for key in result.keys():
    print(key, getattr(result, key))

"""
<script.py> output:
    pop2000 89600
    abbreviation IL
"""

# ------------------------------------------------------------------------------

# Build a statement to select the census and state_fact tables: stmt
stmt = select([census, state_fact])

# Add a select_from clause that wraps a join for the census and state_fact
# tables where the census state column and state_fact name column match
stmt_join = stmt.select_from(
    census.join(state_fact, census.columns.state == state_fact.columns.name))

# Execute the statement and get the first result: result
result = connection.execute(stmt_join).first()

# Loop over the keys in the result object and print the key and value
for key in result.keys():
    print(key, getattr(result, key))

"""

<script.py> output:
    state Illinois
    sex M
    age 0
    pop2000 89600
    pop2008 95012
    id 13
    name Illinois
    abbreviation IL
    country USA
    type state
    sort 10
    status current
    occupied occupied
    notes
    fips_state 17
    assoc_press Ill.
    standard_federal_region V
    census_region 2
    census_region_name Midwest
    census_division 3
    census_division_name East North Central
    circuit_court 7

"""

# ------------------------------------------------------------------------------

# Build a statement to select the state, sum of 2008 population and census
# division name: stmt
stmt = select([
    census.columns.state,
    func.sum(census.columns.pop2008),
    state_fact.columns.census_division_name
])

# Append select_from to join the census and state_fact tables by the census state and state_fact name columns
stmt_joined = stmt.select_from(
    census.join(state_fact, census.columns.state == state_fact.columns.name)
)

# Append a group by for the state_fact name column
stmt_grouped = stmt_joined.group_by(state_fact.columns.name)

# Execute the statement and get the results: results
results = connection.execute(stmt_grouped).fetchall()

# Loop over the results object and print each record.
for record in results:
    print(record)


"""
<script.py> output:
    ('Alabama', 4649367, 'East South Central')
    ('Alaska', 664546, 'Pacific')
    ('Arizona', 6480767, 'Mountain')
    ('Arkansas', 2848432, 'West South Central')
    ('California', 36609002, 'Pacific')
    ('Colorado', 4912947, 'Mountain')
    ('Connecticut', 3493783, 'New England')
    ('Delaware', 869221, 'South Atlantic')
    ('Florida', 18257662, 'South Atlantic')
    ('Georgia', 9622508, 'South Atlantic')
    ('Hawaii', 1250676, 'Pacific')
    ('Idaho', 1518914, 'Mountain')
    ('Illinois', 12867077, 'East North Central')
    ('Indiana', 6373299, 'East North Central')
    ('Iowa', 3000490, 'West North Central')
    ('Kansas', 2782245, 'West North Central')
    ('Kentucky', 4254964, 'East South Central')
    ('Louisiana', 4395797, 'West South Central')
    ('Maine', 1312972, 'New England')
    ('Maryland', 5604174, 'South Atlantic')
    ('Massachusetts', 6492024, 'New England')
    ('Michigan', 9998854, 'East North Central')
    ('Minnesota', 5215815, 'West North Central')
    ('Mississippi', 2922355, 'East South Central')
    ('Missouri', 5891974, 'West North Central')
    ('Montana', 963802, 'Mountain')
    ('Nebraska', 1776757, 'West North Central')
    ('Nevada', 2579387, 'Mountain')
    ('New Hampshire', 1314533, 'New England')
    ('New Jersey', 8670204, 'Mid-Atlantic')
    ('New Mexico', 1974993, 'Mountain')
    ('New York', 19465159, 'Mid-Atlantic')
    ('North Carolina', 9121606, 'South Atlantic')
    ('North Dakota', 634282, 'West North Central')
    ('Ohio', 11476782, 'East North Central')
    ('Oklahoma', 3620620, 'West South Central')
    ('Oregon', 3786824, 'Pacific')
    ('Pennsylvania', 12440129, 'Mid-Atlantic')
    ('Rhode Island', 1046535, 'New England')
    ('South Carolina', 4438870, 'South Atlantic')
    ('South Dakota', 800997, 'West North Central')
    ('Tennessee', 6202407, 'East South Central')
    ('Texas', 24214127, 'West South Central')
    ('Utah', 2730919, 'Mountain')
    ('Vermont', 620602, 'New England')
    ('Virginia', 7648902, 'South Atlantic')
    ('Washington', 6502019, 'Pacific')
    ('West Virginia', 1812879, 'South Atlantic')
    ('Wisconsin', 5625013, 'East North Central')
    ('Wyoming', 529490, 'Mountain')

"""

# ------------------------------------------------------------------------------

# Make an alias of the employees table: managers
managers = employees.alias()

# Build a query to select names of managers and their employees: stmt
stmt = select(
    [managers.columns.name.label('manager'),
     employees.columns.name.label('employee')]
)

# Match managers id with employees mgr: stmt_matched
stmt_matched = stmt.where(managers.columns.id == employees.columns.mgr)

# Order the statement by the managers name: stmt_ordered
stmt_ordered = stmt_matched.order_by(managers.columns.name)

# Execute statement: results
results = connection.execute(stmt_ordered).fetchall()

# Print records
for record in results:
    print(record)


"""
<script.py> output:
    ('FILLMORE', 'GRANT')
    ('FILLMORE', 'ADAMS')
    ('FILLMORE', 'MONROE')
    ('GARFIELD', 'JOHNSON')
    ('GARFIELD', 'LINCOLN')
    ('GARFIELD', 'POLK')
    ('GARFIELD', 'WASHINGTON')
    ('HARDING', 'TAFT')
    ('HARDING', 'HOOVER')
    ('JACKSON', 'HARDING')
    ('JACKSON', 'GARFIELD')
    ('JACKSON', 'FILLMORE')
    ('JACKSON', 'ROOSEVELT')

"""

# ------------------------------------------------------------------------------

# Start a while loop checking for more results
while more_results:
    # Fetch the first 50 results from the ResultProxy: partial_results
    partial_results = results_proxy.fetchmany(50)

    # if empty list, set more_results to False
    if partial_results == []:
        more_results = False

    # Loop over the fetched records and increment the count for the state
    for row in partial_results:
        if row.state in state_count:
            state_count[row.state] += 1
        else:
            state_count[row.state] = 1

# Close the ResultProxy, and thus the connection
results_proxy.close()


# Print the count by state
print(state_count)

"""
<script.py> output:
    {'Illinois': 172, 'New Jersey': 172, 'District of Columbia': 172, 'North Dakota': 75, 'Florida': 172,
    'Maryland': 49, 'Idaho': 172, 'Massachusetts': 16}
"""

# ------------------------------------------------------------------------------

# Import Table, Column, String, Integer, Float, Boolean from sqlalchemy
from sqlalchemy import Table, Column, String, Integer, Float, Boolean

# Define a new table with a name, count, amount, and valid column: data
data = Table('data', metadata,
             Column('name' , String(255)),
             Column('count', Integer()),
             Column('amount',Float()),
             Column('valid' , Boolean())
)

# Use the metadata to create the table
metadata.create_all(engine)

# Print table details
print(repr(data))

"""
<script.py> output:
    Table('data', MetaData(bind=None), Column('name', String(length=255), table=<data>), Column('count', Integer(), table=<data>),
    Column('amount', Float(), table=<data>), Column('valid', Boolean(), table=<data>), schema=None)

"""


# ------------------------------------------------------------------------------

# Import insert and select from sqlalchemy
from sqlalchemy import insert,select

# Build an insert statement to insert a record into the data table: insert_stmt
insert_stmt = insert(data).values(name='Anna', count = 1, amount = 1000.00, valid = True)

# Execute the insert statement via the connection: results
results = connection.execute(insert_stmt)

# Print result rowcount
print(results.rowcount) # 1

# Build a select statement to validate the insert: select_stmt
select_stmt = select([data]).where(data.columns.name == 'Anna')

# Print the result of executing the query.
print(connection.execute(select_stmt).first()) # ('Anna', 1, 1000.0, True)


"""
<script.py> output:
    1
    ('Anna', 1, 1000.0, True)

"""

# ------------------------------------------------------------------------------

# Build a list of dictionaries: values_list
values_list = [
    {'name': 'Anna', 'count': 1, 'amount': 1000.00, 'valid': True},
    {'name' : 'Taylor', 'count' : 1, 'amount' : 750.00, 'valid': False}
]

# Build an insert statement for the data table: stmt
stmt = data.insert()

# Execute stmt with the values_list: results
results = connection.execute(stmt, values_list)

# Print rowcount
print(results.rowcount)


"""
<script.py> output:
    2
"""


# ------------------------------------------------------------------------------

# Loading a CSV into a table
# import pandas
import pandas as pd

# read census.csv into a DataFrame : census_df
census_df = pd.read_csv("census.csv", header=None)

# rename the columns of the census DataFrame
census_df.columns = ['state', 'sex', 'age', 'pop2000', 'pop2008']

# append the data from census_df to the "census" table via connection
census_df.to_sql(name="census", con=connection, if_exists="append", index=False)

"""


"""


# ------------------------------------------------------------------------------


select_stmt = select([state_fact]).where(state_fact.columns.name == 'New York')
results = connection.execute(select_stmt).fetchall()
print(results)
print(results[0]['fips_state'])

update_stmt = update(state_fact).values(fips_state = 36)
update_stmt = update_stmt.where(state_fact.columns.name == 'New York')
update_results = connection.execute(update_stmt)

# Execute select_stmt again and fetch the new results
new_results = connection.execute(select_stmt).fetchall()

# Print the new_results
print(new_results)

# Print the FIPS code for the first row of the new_results
print(new_results[0].fips_state)


"""
<script.py> output:
    [('32', 'New York', 'NY', 'USA', 'state', '10', 'current', 'occupied', '', '0', 'N.Y.', 'II', '1', 'Northeast', '2',
    'Mid-Atlantic', '2')]
    0
    [('32', 'New York', 'NY', 'USA', 'state', '10', 'current', 'occupied', '', '36', 'N.Y.', 'II', '1', 'Northeast', '2',
     'Mid-Atlantic', '2')]
    36

"""


# ------------------------------------------------------------------------------

# Build a statement to update the notes to 'The Wild West': stmt
stmt = update(state_fact).values(notes='The Wild West')

# Append a where clause to match the West census region records: stmt_west
stmt_west = stmt.where(state_fact.columns.census_region_name == 'West')

# Execute the statement: results
results = connection.execute(stmt_west)

# Print rowcount
print(results.rowcount)

"""
<script.py> output:
    13
"""

# ------------------------------------------------------------------------------
# Build a statement to select name from state_fact: fips_stmt
fips_stmt = select([state_fact.columns.name])

# Append a where clause to match the fips_state to flat_census fips_code: fips_stmt
fips_stmt = fips_stmt.where(
    state_fact.columns.fips_state == flat_census.columns.fips_code)

# Build an update statement to set the name to fips_stmt_where: update_stmt
update_stmt = update(flat_census).values(state_name=fips_stmt)

# Execute update_stmt: results
results = connection.execute(update_stmt)

# Print rowcount
print(results.rowcount)

"""
<script.py> output:
    51

"""

# ------------------------------------------------------------------------------

# Import delete, select
from sqlalchemy import delete, select

# Build a statement to empty the census table: stmt
delete_stmt = delete(census)

# Execute the statement: results
results = connection.execute(delete_stmt)

# Print affected rowcount
print(results.rowcount)

# Build a statement to select all records from the census table : select_stmt
select_stmt = select([census])

# Print the results of executing the statement to verify there are no rows
print(connection.execute(select_stmt).fetchall())

"""
<script.py> output:
    8772
    []

"""

# ------------------------------------------------------------------------------

# Build a statement to count records using the sex column for Men ('M') age 36: count_stmt
count_stmt = select([func.count(census.columns.sex)]).where(
    and_(census.columns.sex == 'M',
         census.columns.age == 36)
)

# Execute the select statement and use the scalar() fetch method to save the record count
to_delete = connection.execute(count_stmt).scalar()

# Build a statement to delete records from the census table: delete_stmt
delete_stmt = delete(census)

# Append a where clause to target Men ('M') age 36: delete_stmt
delete_stmt = delete_stmt.where(
    and_(census.columns.sex == 'M',
         census.columns.age == 36)
)

# Execute the statement: results
results = connection.execute(delete_stmt)

# Print affected rowcount and to_delete record count, make sure they match
print(results.rowcount, to_delete)

"""
<script.py> output:
    51 51

"""

# ------------------------------------------------------------------------------

# Drop the state_fact table
state_fact.drop(engine)

# Check to see if state_fact exists
print(state_fact.exists(engine))

# Drop all tables
metadata.drop_all(engine)

# Check to see if census exists
print(census.exists(engine))


"""
<script.py> output:
    False
    False
"""
