# JDBC Drivers

This directory contains JDBC drivers needed for Spark to connect to external databases.

## PostgreSQL JDBC Driver

The PostgreSQL JDBC driver is required for importing data from PostgreSQL into Spark SQL.

### Download

Download the PostgreSQL JDBC driver (version 42.7.1 recommended):

```bash
cd jars
wget https://jdbc.postgresql.org/download/postgresql-42.7.1.jar
```

Or manually download from:
- https://jdbc.postgresql.org/download/

Place the JAR file as:
```
jars/postgresql-42.7.1.jar
```

### Alternative Method

You can also get it from the spark-eval-groupagg repository if you have it:
```bash
cp ../spark-eval-groupagg/jars/postgresql-*.jar ./postgresql-42.7.1.jar
```

## After Adding the Driver

After placing the JAR file, rebuild the Docker containers:

```bash
docker-compose build backend
docker-compose up -d
```

The Spark session will automatically detect and load the JDBC driver.
