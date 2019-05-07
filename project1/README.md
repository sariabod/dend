Purpose of Project
------------------

Create an ETL pipeline to parse song and log data and store them into a an sql database to be queried in order gain meaningful analytics for stake holders.

Steps:

1. Design and deploy a database schema focusing on app events as the base (Fact Table) and use descriptive data about the event as dimension tables.
2. Parse song and log data and insert them into the fact/dimension tables.
3. Make sure duplicate items such as user and artists are updated instead of duplicated.

Schema:
This database schema was created to minimize duplcate datak and allow various aggregations based on "NextSong" logs.


Sample Queries and Results:



