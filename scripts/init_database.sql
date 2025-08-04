-- Drop and create database (must be connected to an existing database like 'postgres')
DROP DATABASE IF EXISTS datawarehouse;
CREATE DATABASE datawarehouse;

--open a new query window connected to datawarehouse

-- Drop schemas if they exist
DROP SCHEMA IF EXISTS bronze CASCADE;
DROP SCHEMA IF EXISTS silver CASCADE;
DROP SCHEMA IF EXISTS gold CASCADE;

-- Create schemas
CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;
