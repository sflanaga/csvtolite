# csvtolite
Import csv files into sqlite3 tables

Created as I use requirements for many different fields (in the thousands) and want 
to automate things not so easy to do in Excel, etc.

```
csvtolite -d test.db -r '(....).*csv' -v -f wide_sm.csv
```

Imports csv files wide_sm.csv into existing sqlite3 database file test.db, 
creates a table "wide" taken from the -r option's first sub group.

If the table already exists it will cross check the number of fields in the csv file
with the that table and reuse it.

Currently only supports the `text` sqlite3 field type.

`--headeron` option will use the header of the csv file as the table schema for the field names.

Work in progress.

TODO:

- DONE: tablename option vs automatic regex one
- post sql file - good with in memory database
- statistics / progress ticker
- DONE: ignore field count
  - test lesser/greater field count input and option to still execute one or the other
  - lesser not default but greater is ok by default
- add different stdio and related options
- automatically determine types of the files in the csv file based on sampling, etc.
- DONE: easy just added [] around sql statements - support wacky field names [xynz$#@]
- overwrite table option

Related project:
- sqlite3 extension to allow regex matches and regex substitutions.
  - think these exist (maybe) but good exercise
