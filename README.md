# eventindex-h2o
> Hadoop to Oracle importer for the EventIndex project @ ATLAS, CERN

H2O consists of a set of simple programs run by cron jobs.

```sh
$ acrontab -e
00 08-23 * * * hadoop-server /afs/cern.ch/user/a/atlasdba/oei/h2o_cron_runner.sh
00 01 * * * hadoop-server /afs/cern.ch/user/a/atlasdba/oei/h2o_cron_runner_retry_failed.sh
00 09 * * MON hadoop-server /afs/cern.ch/user/a/atlasdba/oei/delete_logs.sh
#00 02 * * * hadoop-server /afs/cern.ch/user/a/atlasdba/oei/h2o_cron_runner_skip_existing.sh
```

It requires a properties file containing the DB datails:

```sh
$ emacs data.properties
schema=schema_name
password=password
tns=/path/to/tns/
dbURL=dbURL
```

Specific details and instructions can be found @ [CERN Twiki](https://twiki.cern.ch/twiki/bin/viewauth/AtlasComputing/ScriptsDepotContent#EIO_H2O_importer)
