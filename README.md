hbasectl
========


   * HbaseCtl is a tool to perfrom some basic hbase administrative tasks
   * Currently supported tasks are "compact", "split" , "list" and "status"
   * Runs compactions and splits concurrently for multiple tables/regions. 
   * Coordinate using zookeeper
   
ToDo
-----
   - [ ] Monitor RegionServer load and factor that while scheduling splits/compaction
   - [ ] Introduce a config for basic settings
   - [ ] Enable kerberos ticket renewal thread for headless accounts 

Notes
-----
   * Add your Hbase jars to **CLASS_PATH**
   * Require Hbase version &gt;= 0.94.9


Usage
-----

   *  hbasectl --help 

```
Usage: hbasectl [options] [subcommand [options]]
Hbase Ctl script

Global Options are
    -v, --[no-]verbose               Show/Disable stderr outputs from hbase and zookeeper
    -h, --help                       Print Help

Commands are:
 compact : compact an hbase table/regions
 split : split an hbase table/regions
 list_regions : list the regions of a table
 status : show the status of region or table

See 'hbasectl help COMMAND' for more information on a specific command.


```

   * hbasectl help compact 

```
Usage: compact [options]
compact  hbase table/regions
    -f, --file FILE_WITH_REGIONS     Input file with the list of table/regions to compact
    -g, --cluster CLUSTER            The cluster to run the compaction
    -p, --percent CHUNK              Percentage of regions to compact simultaneously
    -c, --concurrency CONCURRENCY    Number of concurrent threads to run
    -n, --[no-]dryrun                Lists the actions the command will take without actually performing them
    -r, --region REGION              Compact a region
    -t, --table TABLE                Compact a table
    -T, --type MAJOR_OR_MINOR        Major or Minor compaction. Default major
```

   * hbasectl help split

```
Usage: split [options]
split  hbase table/regions
    -f, --file FILE_WITH_REGIONS     Input file with the list of table/regions to split
    -g, --cluster CLUSTER            The cluster to run the split
    -p, --percent CHUNK              Percentage of regions to split simultaneously
    -c, --concurrency CONCURRENCY    Number of concurrent threads to run
    -n, --[no-]dryrun                Lists the actions the command will take without actually performing them
    -r, --region REGION              Split a region
    -t, --table TABLE                Split a table
    
```

   * hbasectl help list_regions

```
Usage: list_regions [options]
list the regions of a table
    -t, --table TABLE_NAME           Table  to list the regions

```

   * hbasectl help status

```
Usage: status [options]
show the status of region or table
    -r, --region REGION              Region name to show the status
    -t, --table TABLE_NAME           Table to show the status

```

Examples
--------

TODO

   
 
