# MySQL2PostgreSQL

A classic wheel being reinvented. The existing solutions I found all had caveats and could not quite handle all the data. So I wrote my own script to convert a MySQL backup file into files that can recreate the database in PostgreSQL

```bash
$ ./mysql2pg mysql_backup.sql
```

Take a backup from your MySQL system and feed it as input to this. It will create a `schema.sql` file allowing you to build the database in PostgreSQL. For each table in the backup there will be a file named `data_for_*.sql` which is a bunch of inserts to allow you to repopulate the database

## Warning

This was written for my very specific use case and solves it perfectly

YMMV
