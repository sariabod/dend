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

1. Most songplays by level:
SELECT sp.level, count(*) from songplays sp group by sp.level

| Level | Count |
| ----- | ----- |
| free  | 1305  |
| paid  | 5681  |


2. What hour has the most song plays:
SELECT t.hour, count(*) from songplays sp
left join time t on
t.start_time = sp.start_time
group by t.hour
order by count DESC

| Hour |  Count |
|------|--------|
| 16   | 601    |
| 17   | 517    |
| 18   | 515    |
| 15   | 498    |
| 14   | 448    |
| 11   | 423    |
| 12   | 377    |
| 13   | 375    |


3. Most active users
SELECT u.first_name, u.last_name, count(*) from songplays sp 
left join users u on
u.user_id = sp.user_id
group by u.user_id
order by count DESC

| First      |  Last     |  Count |
|------------|-----------|--------|
| Chloe      | Cuevas    | 691    |
| Tegan      | Levine    | 671    |
| Kate       | Harrell   | 557    |
| Lily       | Koch      | 537    |
| Aleena     | Kirby     | 413    |
| Jacqueline | Lynch     | 346    |
| Layla      | Griffin   | 321    |
| Jacob      | Klein     | 289    |
| Mohammad   | Rodriguez | 277    |



