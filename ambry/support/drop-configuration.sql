/* ---------------------------------------------------------------------- */
/* Script generated with: DeZign for Databases v6.3.4                     */
/* Target DBMS:           PostgreSQL 9                                    */
/* Project file:          configuration-pg.dez                            */
/* Project name:                                                          */
/* Author:                                                                */
/* Script type:           Database drop script                            */
/* Created on:            2013-07-06 13:23                                */
/* ---------------------------------------------------------------------- */


/* ---------------------------------------------------------------------- */
/* Drop foreign key constraints                                           */
/* ---------------------------------------------------------------------- */

ALTER TABLE tables DROP CONSTRAINT datasets_tables;

ALTER TABLE columns DROP CONSTRAINT tables_columns;

ALTER TABLE config DROP CONSTRAINT datasets_config;

ALTER TABLE partitions DROP CONSTRAINT datasets_partitions;

ALTER TABLE partitions DROP CONSTRAINT tables_partitions;

/* ---------------------------------------------------------------------- */
/* Drop table "partitions"                                                */
/* ---------------------------------------------------------------------- */

/* Drop constraints */

ALTER TABLE partitions DROP CONSTRAINT PK_partitions;

ALTER TABLE partitions DROP CONSTRAINT TUC_partitions_1;

/* Drop table */

DROP TABLE partitions;

/* ---------------------------------------------------------------------- */
/* Drop table "columns"                                                   */
/* ---------------------------------------------------------------------- */

/* Drop constraints */

ALTER TABLE columns DROP CONSTRAINT PK_columns;

ALTER TABLE columns DROP CONSTRAINT TUC_columns_1;

ALTER TABLE columns DROP CONSTRAINT TUC_columns_2;

/* Drop table */

DROP TABLE columns;

/* ---------------------------------------------------------------------- */
/* Drop table "tables"                                                    */
/* ---------------------------------------------------------------------- */

/* Drop constraints */

ALTER TABLE tables DROP CONSTRAINT PK_tables;

ALTER TABLE tables DROP CONSTRAINT TUC_tables_1;

ALTER TABLE tables DROP CONSTRAINT TUC_tables_2;

/* Drop table */

DROP TABLE tables;

/* ---------------------------------------------------------------------- */
/* Drop table "files"                                                     */
/* ---------------------------------------------------------------------- */

/* Drop constraints */

ALTER TABLE files DROP CONSTRAINT PK_files;

/* Drop table */

DROP TABLE files;

/* ---------------------------------------------------------------------- */
/* Drop table "config"                                                    */
/* ---------------------------------------------------------------------- */

/* Drop constraints */

ALTER TABLE config DROP CONSTRAINT PK_config;

/* Drop table */

DROP TABLE config;

/* ---------------------------------------------------------------------- */
/* Drop table "datasets"                                                  */
/* ---------------------------------------------------------------------- */

/* Drop constraints */

ALTER TABLE datasets DROP CONSTRAINT PK_datasets;

/* Drop table */

DROP TABLE datasets;
