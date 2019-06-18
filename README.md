# mybinlog

MySQL binary log parser.  

Support v4 Format Desription Event and binlog version.  
I might support MySQL >= 5.5 or more higher version...

# How to use

(Stil developing phase now...)


help
```
Usage of mybinlog
  -j	Do print event headers as JSON
  -p	Do print events
```

For now, you can check event count.

```
~/mybinlog $ go run *.go
samples/binlog_bk.001
Read [ samples/binlog_bk.001 ] ...
totalCount:  4321122
successCount:  4127382
errorCount:  193740
unknownCount:  0
-- Event count --
              QUERY_EVENT: 4126805
         WRITE_ROWS_EVENT: 192988
                XID_EVENT: 575
          TABLE_MAP_EVENT: 575
        UPDATE_ROWS_EVENT: 8
 FORMAT_DESCRIPTION_EVENT: 1
             ROTATE_EVENT: 1
        DELETE_ROWS_EVENT: 169
```

print header and body(partially parsed)

```
go run  *.go -p samples/binlog.000004 | head -n 30
Read [ samples/binlog.000004 ] ...
2019-06-13 01:14:40 --------
  EventType   : FORMAT_DESCRIPTION_EVENT
  ServerID    : 1
  Eventlength : 119
  NextPosition: 123
  Flags       : [1 0]

FormatDescriptionEvent
{4 5.7.22-log 1970-01-01 09:00:00 +0900 JST 19 [56 13 0 8 0 18 0 4 4 4 4 18 0 0 95 0 4 26 8 0 0 0 8 8 8 2 0 0 0 10 10 10 42 42 0 18 52 0 1 29 80 6 172]}
----

2019-06-13 01:15:26 --------
  EventType   : QUERY_EVENT
  ServerID    : 1
  Eventlength : 161
  NextPosition: 380
  Flags       : [0 0]

QueryEvent
{3082 0 6 0 35 {[] []  0 0 0 0 0   0 0 []} sample create table t1 (
id int not null auto_increment,
c1 int not null,
primary key(id))}
----

2019-06-13 01:15:46 --------
  EventType   : QUERY_EVENT
  ServerID    : 1
  Eventlength : 83
signal: broken pipe
```




## Reference 

- https://dev.mysql.com/doc/internals/en/event-data-for-specific-event-types.html
- `mysql-server/sql/log_event.h`

