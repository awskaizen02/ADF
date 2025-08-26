-- What is Data factory

-- Data Factory is an Azure service which is used for Data movement and Transformation Activities.
-- You can setup rules for data movement, transformation 
-- You can setup triggers when the activity should execute.
-- You can monitor the data once the activities are performed. 
-- You can view the failure logs under this datafactory incase of activity failure

-- PipeLine--
-- A pipeline is a logical grouping of activities that together perform a task. 
-- Ex:- a pipeline could contain a set of activities that ingest and clean log data, and then kick off a mapping data flow to analyze the log data.
-- The pipeline allows you to manage the activities as a set instead of each one individually.
-- You deploy and schedule the pipeline instead of the activities independently.


CREATE TABLE TBL_1 (ID INT, NAME VARCHAR (20))
CREATE TABLE TBL_2 (ID INT, NAME VARCHAR (20))

CREATE TABLE TBL_3 (ID INT, NAME VARCHAR (20), CITY VARCHAR(20))
CREATE TABLE TBL_4 (ID INT, NAME VARCHAR (20), CITY VARCHAR(20))

CREATE TABLE TBL_EMP (EMP_ID INT, EMP_NAME VARCHAR (20))

INSERT INTO TBL_1 VALUES (1,'RAM') ,(2,'SHANKAR')

INSERT INTO TBL_3 VALUES (3,'KUMAR','CHENNAI'),(4,'ANBU','CHENNAI')

SELECT * FROM TBL_1
SELECT * FROM TBL_2
SELECT * FROM TBL_3
SELECT * FROM TBL_4
SELECT * FROM TBL_EMP

TRUNCATE TABLE TBL_EMP.

TRUNCATE TABLE TBL_1
TRUNCATE TABLE TBL_2

INSERT INTO TBL_1 VALUES (1,'RAM'),(2,'SHANKAR')
INSERT INTO TBL_2 VALUES (1,'AA'),(2,'BB')