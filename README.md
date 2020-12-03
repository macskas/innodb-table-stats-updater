# innodb-table-stats-updater
If your innodb_stats_on_metadata OFF, you might have to update innodb_table_stats manually.

*For personal use atm. Connecting to localhost, with socket. You can change it ofc, look for DSN. I might add cli arguments later for changing it.*

## Dependencies:
 - perl
 - DBI module with mysql support (debian: libclass-dbi-mysql-perl)


## params
```
root@server:~$ perl manual_metadata_update.pl
      -d    - database_name
      -t    - table_name
      -n    - dry_run(no updates)
```
