# Investigation

I followed the instructions from file [analyst_challenge_instructions.sql](analyst_challenge_instructions.sql):

1. total views accrued within each category
2. graph of daily views for an 'average' video within each category
3. how many videos within each category have < 1k views, 1-10K, 10-100K, 100k-1M, 1M+ views
   (additionally please provide any other insights you find relevant)

Additionally I implemented the following report:

1. hourly views within each category for last 7 days 

First I loaded data manually and did few checks, see [checks.sql](sql/checks.sql) file for more details.

Findings:

- Input files are very poorly formatted CSV files
  - see chapter [Solution description/Load](#load) for more details
- There is strict referential integrity between meta and history
- Value of a fact can be reduced, e.g. number of views
- There can be many rows for certain video(GID) in one day
- All videos are created(uploaded) in 2018-06-01 day, history contains the following 7 days (including creation day)
- There are cca 1/2 of rows in history without change of any fact
- There are no rows on the edge of each day
  - Question is, if facts diff (comparing to previous row) should be calculated into previous or following day
  - See chapter [Follow-ups/Timeseries](#timeseries) for more details

# Solution description

I decided to implement an ETL to prepare data to be easily queryable by any report (analytic) query with best performance.

I use Vertica database, because:

- It is best performing DB engine (not only) for analytical queries
- I am experienced user / admin
- It can be easily started on localhost in Docker

I wanted to achieve the following objectives:

- Separate SQL code
- Configurability
- Multi-threading
- Executable on localhost

So I implemented a [python tool](pex_test_solution.py) with [configuration](pex_test_solution.yaml) 
and prepared [Dockerfile](Dockerfile) / [docker-compose](docker-compose.yaml) for Vertica.

# User documentation

Install python3, if necessary, and additional python modules:

```bash
# Install python3 and:
sudo pip install configparser pathlib PyYAML vertica-python

```

See also chapter [Encapsulate the tool into docker container](#encapsulate-the-tool-into-docker-container)

Download Vertica RPM. Registration is needed, so I uploaded it into the G-drive, where presentation of results is stored as well.

[Vertica 9.2.1 community edition](https://drive.google.com/open?id=1ra0S97MVgCkUFJt-Yxmdg_B4W0E4_doJ)

Clone repository containing the tool and start Vertica container:

```bash
# Run Vertica
git clone git@github.com:jaceksan/pex-test.git
```

Download CSV files containing data from the drive shared by you, into directory "data":

- [analyst_challenge_data_meta.csv](https://drive.google.com/open?id=1jvl5t2q4LwDHqSTL2ZWHvtU9tsqZq3z4)
- [analyst_challenge_data_history.csv](https://drive.google.com/open?id=1JWTn91IBTxDBxFJlShCo9Qscka1hnax4)

## Run the tool

```bash
# Run Vertica
cd pex-test
docker-compose up
# Vertica stays running until CTRL+C is pressed (or kill signal is sent from outside)

# From another shell session
cd pex-test
# Run the tool (ETL + reports)
./pex_test_solution.py

# Run the tool from certain phase (in this example run only reports)
./pex_test_solution.py -ph report --skip-init-db

# Display full help
./pex_test_solution.py --help
```

It is possible to tune [configuration](pex_test_solution.yaml), e.g. amount of memory available for queries, parallelism, ...

## Results

Results (csv files) are generated into results/$hostname_from_config/$query_label.csv.

Results and be copied into a sheet, then use "split to columns" feature.


## Investigation

You can either install vertica client RPM or any SQL client, e.g. Dbeaver, and setup connection to Vertica.

(Not only) All report (SELECT) queries are labelled (/*+ label(<label text>) */, so they can be tracked inside Vertica, e.g.:
```sql92
select *
from query_requests
where request_label = 'report_task_3'
order by start_timestamp desc limit 10;
```

## Performance 

Whole ETL including reports execution is running below 30 seconds on 16GB RAM / 4 cores laptop.

I executed the tool on 4-node cluster and it scales with number of nodes pretty well.

Only report queries are running circa 1 second (parallel 4), no single query duration exceeds 300 ms. 

# Solution design

The tool reads YAML config and executes phase of SQL pipeline in required order.

The tool creates schema and setup DB session before starting SQL pipeline.

The tool can execute queries in parallel (multi-threading), if required in the config.

The tool distinguish various types of queries and can execute custom actions for each type:

- COPY (load) - analyze statistics and constraints
- SELECT - fetch results and store them into csv file
- DML - execute commit 
- DDL - remove label, it is not supported for DDLs in Vertica

The tool reports progress during execution.
Finally it reports stats for each executed report to STDOUT.
Results of SELECTs are stored in csv files.

## Model

- [model.sql](sql/model.sql)

Initial data model + tables used by ETL.

## Load

- [load.sql](sql/load.sql)

Very poorly formatted CSV files as a input.
For history I had to use FILLER mechanism and translate "" into valid INTEGER.

## ETL

- [denorm.sql](sql/denorm.sql)
- [pre_agg.sql](sql/pre_agg.sql)

Denormalize model to get rid of JOINs in reports.
Pre-aggregate by day to improve performance of any daily report.

Separation of denormalization and pre-aggregation is necessary, because there is no way, how to optimize both JOIN and AGG use cases -
they use different key columns.
See also clauses ORDER BY / SEGMENTED BY in model.sql - they are kind of indexes determined for the related use cases.

In denorm.sql I calculate diffs between current and previous rows to exclude rows, which do not contain change of any fact -
reducing rows to circa 1/2.

It should be done incrementally, see chapter [Follow-ups/Incremental loads](#incremental-loads).

## Reports

- [reports.sql](sql/reports.sql)

Reports solving tasks from instructions.

Each report uses most optimal table filled by the ETL part.

# Presentation of results

- [Google spreadsheet with results](https://docs.google.com/spreadsheets/d/1-ZPGfndSkD0uY5qyJ3G3Ixtgmkf9FPqFFhlq3ECtyRs/edit#gid=0)

There are four sheets solving the related tasks from [analyst challenge instructions.sql](analyst_challenge_instructions.sql) + 1 additional (task 4).

# Follow-ups

## Support more video providers

I suggest to split gid column into 2 columns - provider_id and video_id.
Just I am not sure, if this is doable in case of other providers.

It would be great to co-locate data of more providers in unified data model.
Some attributes could be empty, columnar DB engine would absorb it well.
Just relaxing data types could be a challenge. 

## Additional facts / attributes

Current data is really minimalistic. 

I am not sure, if in this industry there can be more facts.

I am sure, there can be much more attributes (dimensions), e.g.:

- user
  - name
  - age
  - address (geo location)
- video
  - geo location (video uploaded from, video recorded in)
  - device id / manufacturer
  - resolution / fps
- category
  - more categories
  - category trees
  
## New reports examples

- More metrics
  - min, max, avg, median, percentiles (80, 90, 95, ...)
- report by additional attributes 
- Correlation between facts (views, likes, ...) by relevant attributes 
- Trending videos / users
  - in last hour, day, week


## Incremental loads

I would extend current solution by incremental loading into separated persistent history tables.
Basically all current tables would serve as temporary storage for current increment.
Each result would be merged into full history by using MERGE (optimized in Vertica) statement.
I would consider to limit the history, where to MERGE, e.g. to 1 year.

## Timeseries

There are no records for edge of days.

Example:

| gid           | updated_at          | views |
| ------------- |:-------------------:| -----:|
| 1             | 2019-01-01 23:00:00 | 10    |
| 1             | 2019-01-02 01:00:00 | 20    |

Does this mean, that diff of views (10) should be summed up into day 2019-01-01 or 2019-01-02?

We could use TIMESERIES clause (Vertica specific) and linear interpolation to generate rows for each edge of the day.
Now I sum up diff between current and previous row into the following day.

The same could be applied to reports by hour.

## Partitioning

- archiving / purging data
- partition pruning

Best practice is to partition table by day / month and move partition older than X into archive tables (fast DDL operation).
It helps to MERGE (see chapter [Incremental loads](#incremental-loads)) to do not degrade in time.
Newest feature is so called hierarchical partitioning (by day up to 1 month, by month up to 1 year, ...), which brings additional (Vertica specific) benefits.

## Optimize projections

Projections in Vertica are quite similar to indexes

## Secondary projections

To satisfy additional reports (see also chapter [New reports examples](#new-reports-examples)), it would be wise to create additional projections.

The key is to find minimal number of sets of columns, which are used for reporting (aggregation, windowing functions), to minimize number of projections.
Obviously materializing more projections means significant overhead during data ingestion.
On the other hand incremental load helps to reduce the overhead.

All projections must contain MERGE key, if MERGE is used, so MERGE is still optimized.

## Encapsulate the tool into docker container

Instead of installing python3 and additional modules into host environment, we should create second container for the tool.
We would need additional Dockerfile, setup.py and extend docker-compose.yaml.

As a workaround you can use virtualenv.
