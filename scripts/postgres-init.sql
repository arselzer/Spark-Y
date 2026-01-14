-- PostgreSQL initialization script
-- Creates multiple users and databases for the Yannasparkis application

-- Create imdb user (if not exists)
DO
$do$
BEGIN
   IF NOT EXISTS (
      SELECT FROM pg_catalog.pg_roles WHERE rolname = 'imdb'
   ) THEN
      CREATE USER imdb WITH PASSWORD 'imdb';
   END IF;
END
$do$;

-- Create imdb database (if not exists)
SELECT 'CREATE DATABASE imdb'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'imdb')\gexec

-- Grant privileges to imdb user on imdb database
GRANT ALL PRIVILEGES ON DATABASE imdb TO imdb;

-- Connect to imdb database to set up schema permissions
\c imdb

-- Grant schema permissions to imdb user
GRANT ALL ON SCHEMA public TO imdb;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO imdb;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO imdb;

-- Set default privileges for future tables
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO imdb;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO imdb;

-- Create a sample database for testing
SELECT 'CREATE DATABASE sample'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'sample')\gexec

GRANT ALL PRIVILEGES ON DATABASE sample TO imdb;

-- Log completion
\echo 'PostgreSQL initialization complete'
\echo 'Created users: postgres (admin), imdb (user)'
\echo 'Created databases: imdb, sample'
