# <img src="./logo-nundb.png" width="50" />  NunDB

## What is NunDB

NunDB is an open source real-time database, made to be fast, light and easy to use.

NunDB is written to be memory-safe and horizontal(soon) scalable.

We believe we can keep NunDB simple, and at the same time, it can power lots of different apps and use cases.

## Examples Use cases 

Checkout our examples of integration and see what we can do for your app

* [Vue.js + NunDB jsfiddle](https://jsfiddle.net/2op63ctk/)
* [React.js Realtime Todo app](https://github.com/mateusfreira/nun-db-js/tree/master/examples/react)


# Installations

## Docker 

Running NunDB using Docker is likely the fastest method. The simplest steps to run NunDB with Docker involve running a container, as shown in the following example.

```bash
#Change user-name to the admin you want, user-pwd to the admin pwd you want, change /tmp to the directory where you want to store your data in the host computer... for dev /tmp works fine
docker run --env NUN_USER=user-name --env NUN_PWD=user-pwd --env NUN_DBS_DIR="/data" --mount type=bind,source="/tmp",target=/data -it --rm -p 3013:3013 -p 3012:3012 -p 3014:3014 --name nun-test mateusfreira/nun-db
```

## Docker-compose

```yaml

version: '3'
services:
  nun-db:
    image: "mateusfreira/nun-db"
    ports:
      - "3012:3012" # Web socket
      - "3013:3013" # Http
      - "3014:3014" # Socket
    environment:
      - NUN_DBS_DIR=/nun_data 
      - NUN_USER=mateus
      - NUN_PWD=mateus
    volumes:
        - /tmp/data/nun_db/:/nun_data
```

Note that the "/tmp/data/nun_db/" is a directory in the machine where the nun-db is running so you may need to create the directory for that use the command `mkdir /tmp/data/nun_db/`.


### Create the sample DB:

```

# First connect to the running container
docker exec -it nun-test  /bin/sh

# Then execute the command to create the database
nun-db -u user-name -p user-pwd create-db -d sample -t sample-pwd

# You should see something like
Response "valid auth\n;create-db success\n"

```

Once done, you now have NunDB running in your docker and exposing all the ports for use: HTTP (3013), WebSocket (3012), and Socket (3014)  you are ready to use.



# How-tos
* [Real-time Medical Image Collaboration POC Made Easy with OHIF and NunDB](https://mateusfreira.github.io/@mateusfreira-real-time-medical-image-collaboration-ohif-nun-db/)

* [How to create users with different permission levels in NunDB](https://mateusfreira.github.io/@mateusfreira-2023-09-17-how-to-create-users-in-nun-db/)

* [How to Building a Trello-like React/Redux App with NunDB with offline and conflict resolution features](https://mateusfreira.github.io/@mateusfreira-trelo-clone-offline-first-with-nun-db-dealing-with-conflicts/)

* [How-to Make Redux TodoMVC Example a real-time multiuser with NunDB in 10 steps](https://mateusfreira.github.io/@mateusfreira-2021-06-30-how-to-making-redux-todomvc-example-a-real-time-multiuser-with-nun-db/)

* [How to create your simple version of google analytics real-time using NunDB](https://mateusfreira.github.io/@mateusfreira-create-a-simple-verison-of-google-analytics-realtime-using-nun-db/)

* [NunDb How to backup one or all databases](https://mateusfreira.github.io/@mateusfreira-nundb-how-to-backups-all-databases-with-one-command/)

* [How to snapshot NunDB databases from memory to disk](https://mateusfreira.github.io/@mateusfreira-nundb-how-to-backups-all-databases-with-one-command/)
```bash
# TLDR version
nun-db --user $NUN_USER  -p $NUN_PWD --host "https://http.nundb.org" exec "use-db $DB_NAME $DB_TOKEN; snapshot"
```
* [Introduction to manage conflicts in NunDB](https://mateusfreira.github.io/@mateusfreira-dealing-with-conflicts-in-nun-db/)

## Technical documentations

[A fast-to-sync/search and space-optimized replication algorithm written in rust, The NunDB data replication model](https://mateusfreira.github.io/@mateusfreira-a-fast-to-sync-search-and-space-optimized-replication-algorithm-written-in-rust-the-Nun-db-data-replication-model/)

[Leader election in rust the journey towards implementing NunDB leader election](https://mateusfreira.github.io/@mateusfreira-leader-election-rust-the-journey-towards-nun-db-leader-election-implementation/)

[Integration tests in rust a multi-process test example](https://mateusfreira.github.io/@mateusfreira-integration-tests-for-rust-apps-testing-command-line-tools-in-rust/)

[Writing a prometheus exporter in rust from idea to grafana chart](https://mateusfreira.github.io/@mateusfreira-writing-a-prometheus-exporter-in-rust-from-idea-to-grafana-chart/)

[Why does NunDB we have a Debug command?](https://mateusfreira.github.io/@mateusfreira-the-nun-db-debug-command/)

[Code Coverage for Rust Projects with GitHub Actions](https://blog.freira.dev/@mateusfreira-2024-06-15-code-coverage-for-rust-projects-with-github-actions/)
[![codecov](https://codecov.io/gh/mateusfreira/nun-db/graph/badge.svg?token=yPiTap1MPJ)](https://codecov.io/gh/mateusfreira/nun-db)

## Diagram

```bash

                                      .------------------------------------------.
                                      |                     |                    |              .---------------.
                                      |       http_ops      |                    |------------->|  Disck        |      
                                      |                     |   disk_ops         |------------->|               |      
                                      |_____________        |                    |              .---------------.
                                      |            |        ._________________.__|                                     
                                      |            |        |                 |  |                                     
                                      |            |        | replication_ops |  |                                     
                                      |   tcp_ops   \_______+--------+--------+  |                                     
                                      |             |       |        |           |                                     
.----------------.                    |             |       |        |           |      .---------------. 
|   Frontends/   | ---text----------> |_____________|parse <.> core  | db_ops    |----->|   Memory      | 
|  Clients       | <---text---------- |             |       |        |           |<-----| Repository    | 
.----------------.                    |             |       |        |           |      |               | 
                                      |             \_______|_______/________.   |      ._______________.
                                      |    ws_ops           |                |   |                                    
                                      |                     |   election_ops |   |                                    
                                      |_____________________._______________/____|                                    
                                      |                                          |                                    
                                      |                                          |                                    
                                      |                 monitoring               |                                    
                                      |                                          |                                    
                                      |                                          |                                    
                                      .------------------------------------------.                                    

```

## Connectors 
* Http
    Port: 3013
* Socket
    Port: 3014
    
* Web Socket
    Port: 3012



## Having any issue or needs help?

Open an issue I will follow as soon as possible.

## Want to use it in production ?

Talk to me @mateusfreira I will help you get it to the scale you need.

## Nun Query language (NQL)

### Auth
#### Context
- [ ] Require admin auth
- [ ] Require db auth
- [ ] Replicate? How? ()

e.g:
```
auth $user $pwd
```

### UseDb
#### Context
- [ ] Require admin auth
- [ ] Require db auth
- [ ] Replicate? How? ()

e.g:
```
use-db $db-name $db-pwd
```
#### Alias
`use`

e.g:
```
use $db-name $db-pwd
```

e.g: 
```
use-db $db-bame $user-name $token
```

### CreateDb
#### Context
- [x] Require admin auth
- [ ] Require db auth
- [x] Replicate? How? (create-db)
- [ ] Register Oplog? How? (Update)
Creates a new database

e.g.,

```
# create simple db
create-db test test-pwd;
response: 
empty
```

```
# create db with arbiter conlifct resolution strategy
create-db test test-pwd arbiter;
response:
empty
```

### Get
#### Context
- [ ] Require admin auth
- [x] Require db auth
- [ ] Replicate? How? ()

### Set
#### Context
- [ ] Require admin auth
- [x] Require db auth
- [x] Replicate? How? (replicate)

### SetSafe
Changes the value of a key, guarantees consistency by version
Soon this will be the default way to set values to a key, since we are moving to a leader less replication model.
#### Context
- [ ] Require admin auth
- [x] Require db auth
- [x] Replicate? How? (replicate)
Examples:
```
set-safe $key $version $value
set-safe name 10 Mateus # Sets key name to mateus if version is equal or minor than 
```


### Remove
#### Context
- [ ] Require admin auth
- [x] Require db auth
- [x] Replicate? How? (replicate)

### Snapshot $reclaim_space(true|false) $db_names_pipe_separated (Empty means will snapshot the current database only)
#### Context
- [x] Require admin auth
- [x] Require db auth
- [x] Replicate? How? (replicate-snapshot)

E.g:

##### Snapshot the databases to disk, faster method will store only the difference will use more disk space.
```
snapshot false db1|db2|db3|db4
```

##### Snapshot the database to disk, slower method will store all data again to disk and reorganize the values file
```
snapshot true db1|db2|db3|db4
```
##### Snapshot the database to disk only the current database
```
snapshot true
```

### UnWatch
#### Context
- [ ] Require admin auth
- [x] Require db auth
- [ ] Replicate? How? 

### UnWatchAll
#### Context
- [ ] Require admin auth
- [x] Require db auth
- [ ] Replicate? How? 


### Watch
#### Context
- [ ] Require admin auth
- [x] Require db auth
- [ ] Replicate? How? 

### Keys
Return the list of keys for the database.
#### Context
- [ ] Require admin auth
- [x] Require db auth
- [ ] Replicate? How?

e.gs: 
* Return all keys
`keys`
* Return keys starting with name
`keys name*`
* Return keys ending with name
`keys *name`
* Return keys containing with name
`keys name`

#### Alias
`ls`


### SetPrimary
#### Context
- [x] Require admin auth
- [ ] Require db auth
- [ ] Replicate? How? 

### ElectionWin
#### Context
- [x] Require admin auth
- [ ] Require db auth
- [ ] Replicate? How? 

### Join
#### Context
- [x] Require admin auth
- [ ] Require db auth
- [x] Replicate? How? (replicate-join)

### ReplicateSince
#### Context
- [x] Require admin auth
- [ ] Require db auth
- [ ] Replicate? How? 

### Increment
#### Context
- [ ] Require admin auth
- [ ] Require db auth
- [x] Replicate? How? (replicate-increment)
- [x] Register Oplog? How? (Update)

Increments an integer key value, if the key does not exist start the value with 0, if the key is integer returns an error
e.gs: 
Increments in 1
```
increment visits
```

Increments by 10

```
increment visits 10
```

### Acknowledge
#### Context
- [x] Require admin auth
- [ ] Require db auth
- [ ] Replicate? How? (replicate-increment)
- [ ] Register Oplog? How? (Update)

Internally used to acknowledge messages processed by the replicas

e.g
```
  ack 1 replica1:181
```

### ClusterState
#### Context
- [x] Require admin auth
- [ ] Require db auth
- [ ] Replicate? How? (replicate-increment)
- [ ] Register Oplog? How? (Update)

Returns the cluster state, useful for debugging or admin purposes

e.gs
```
# request
cluster-state;
response: 
cluster-state  127.0.0.1:3017:Primary, 127.0.0.1:3018:Secoundary,
```

### MetricsState
#### Context
- [x] Require admin auth
- [ ] Require db auth
- [ ] Replicate? How? (replicate-increment)
- [ ] Register Oplog? How? (Update)

Returns the oplog and query metrics state, useful for debugging or admin proposes

e.gs
```
# request
metrics-state;
response: 
metrics-state pending_ops: 0, op_log_file_size: 0, op_log_count: 0,replication_time_moving_avg: 0.0, get_query_time_moving_avg: 0.0
```

### Debug
#### Context
- [x] Require admin auth
- [ ] Require db auth
- [ ] Replicate? How? (replicate-increment)
- [ ] Register Oplog? How? (Update)

The debug command holds admin queries for NunDB, such as, checking the messages that are pending replication from a specific node in the cluster.

e.gs
```
debug pending-ops
# result
pending-ops #list-of-pending-ops#

debug pending-conflicts
# result (Must be connected to a database with use *** ***)
pending-conflicts #list-of-pending-conflicts#

debug list-dbs
# result (List the name and the strategy of the dbs one on each line)
dbs-list
sample : none
vue : none
test : arbiter
analitcs-blog : none
$admin : newer

debug force-election
# Forces run an election immediately, which is useful if some node in the cluster is not responsive or to debug latency problems between nodes. Elections in NunDB are predictable and, unless the primary node is slow, the primary should not change even if you force an election.
```

### Arbiter
#### Context
- [ ] Require admin auth
- [x] Require db auth
- [ ] Replicate? How? ()
- [ ] Register Oplog? How? (no)

With the arbiter command one client register it self as an arbiter for conflicts, this client must be connected to the primary database.
Internally this command is equivalent to watching the key $conflict.

e.gs
```
arbiter
#result
ok|error
```

### CreateUser
#### Context
- [x] Require admin auth
- [x] Require db auth
- [x] Replicate? How? (Set command to security key)
- [x] Register Oplog? How? (As key value)

Creates a user for the selected user
e.gs
```
create-user $user $token
```


### SetPermission
#### Context
- [x] Require admin auth
- [x] Require db auth
- [x] Replicate? How? (Set command to security key)
- [x] Register Oplog? How? (As key value)

Creates a user for the selected user
e.gs
```
set-permissions $user_name $permissions $key_pattern|$permissions $key_pattern;

set-permissions foo rw test-*|rwi count;
# Grant permission to user called foo to read and write keys starting with test-, and read write and increment count key.
# Permissions latter
# r = Read,
# w = Write,
# i = Increment,
# x = Remove,

```


## Special keys

All special keys will have a `$` symbol in the first letter of the name.

### $connections

Count the number of connections to a database.

```
$connections
```

### $conflicts
* Key used to register client as arbiter for conflict resolution

## Secure keys
* All keys prefixed with `$$` will be considered secure by NunDB and will only allow database admin authentication to `set`, `get`, or `remove` them. These are useful if admins want to store information that should not be leaked to any client. 
* The key `$$token` cannot be removed even with admin credentials.



## Configurations
###  NUN_ELECTION_TIMEOUT
* Configurations are available to define the timeout period for elections to wait until they are acknowledged from all nodes. It is important to note that you should rarely change this variable since doing so could make elections slower. The value of this variable should be at least twice the latency value to ensure that the election process runs smoothly.

### NUN_MAX_OPLOG_SIZE
* Defines the max op log size in bytes, default value is 1G. NunDb keeps up to 10 uplog files each one containing at max 10% of the max size in bytes of the Oplog max size. When there are more than 10 oplog files the oldest one will be removed in the next time the declutter disk runs. Declutter run on a scheduler evert every 300s (5 minutes), that can be changed by the environment var NUN_DECLUTTER_INTERVAL that defines the interval secound. That means when cleaning up space 10% of the operations are cleaned at a time.


### NUN_DECLUTTER_INTERVAL
* Defines the interval in seconds that the declutter process will run, default value is 300s (5 minutes).
