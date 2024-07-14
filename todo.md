# Todo
- [x] Add a configuration to max oplog size
- [ ] add method to move old file to oplog-number and start fresh

* get_log_file_append_mode
> Has to change to return the new oplog append more
[src/lib/disk_ops.rs:644 ]
* clean_op_log_metadata_files
> Must clean all oplog files
> May be able to invalidate only a part of the oplog
[src/lib/disk_ops.rs:665 ]

* get_log_file_read_mode
> Is this all we need to read from other?
[src/lib/disk_ops.rs:670 ]

* get_op_log_size
> Will have to consider all the 10 files
[src/lib/disk_ops.rs:728 ]
* read_operations_since

> Consider all 10 files
[src/lib/disk_ops.rs:740 ]

* Lock while changing to a new file ... (Performance implications)
[src/lib/replication_ops.rs:421 ]





## How does the replication works on NunDb

There is a single thread to replicate ... to ensure sequence

N threads sending data 
    * ....

1 single thread replicating  (300k ops per second)
    * Pull
