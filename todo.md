- [x] Get Keys
- [x] Data Replication
     - Lazy Centralized Protocols using algorithm Single Master with limited Transparency 
     - Write only on master, reads from anywhere (One point writes mutiple points reads)
     * Main goal Delivery content change in the front ent at the time it changes (fast)
     - Efficient leader election in complete networks (Complete netwotk election) -> https://ieeexplore.ieee.org/document/1386052?reload=true
     * Bully algorithm
        Messages:
            ElectionMessage id
            Alive id ->
            Victory ->  SetPrimary
        Implementation
            An election ir run
                Node sends a new Election message.
                If it receives a message with a higer id than it has it set it self as secoundary
                It it has the highest process ID it sends a victory message
                If no Answer after 1s it bacame the leader and seds Victory message
                If P receives an Election message from another process with a lower ID it sends an Answer message back and starts the election process at the beginning, by sending an Election message to higher-numbered processes.
                If P receives a Coordinator message, it treats the sender as the coordinator.
     - [x] Add Join command
     - [x] Wire from secoundary
     - [x] Primary disconnection
     - [x] not recovering the keys file
     - [x] Implement replication transaction (Log based)
     - [x] Implement recovery message 
     - [x] Implement self election 
     - [x] Create a database from secundary
     - [x] Op log file reading as database
     - [x] Implement full sync (if op log fail)
     - [x] Fix primary disconnections election problem
     - [x] Add command to estimate op log size -> Create a issue to it
     - [ ] Stop using timestamp register the last used oplog on the secoundary
     - [ ] Document db creation and deletion lock while restoring a replica set...
     - [ ] Document how to backup the admin datatabase
     - [ ] Document how the all election process works
     - [ ] Document only snapshoted dbs are restored from disaster??? Should we change it?
     - [ ] Update library to use the cluster (Js)
     - [ ] Compare performance with old version (argo + https://k6.io/open-source)
     - [ ] What if oplog file became too big? We need a command to clean oplog file
     - [ ] Some times election falling in ./tests/test-fail-primary-dbs.sh all
     - [ ] Implement ping command
- [x] Read https://jepsen.io/analyses/redis-raft-1b3fbf6
- [x] Add cli interface
- [x] Remove the need to admin auth to use an database  
- [x] Send and errro if the DB does not exits
- [x] Add un-watch command 
- [x] add secret token to create database 
- [x] Read https://www.microsoft.com/en-us/research/uploads/prod/2018/03/faster-sigmod18.pdf
- [x] Reduce the size of the docker image
- [ ] Implement read only user.
    - [ ] Implement user registration and permission to read only user..
    - [ ] User that  has no access to read all keys
    - [ ] Key level access per user.
[./user-management.md]
- [ ] Implement key value security
- [x] Clean unused dataset
- [ ] Add example https://mateusfreira.github.io/nun-db-js/examples/data-analysis/
- [ ] Implement leader less replication
    - [ ] [LeaderlessReplication.md]
```
nun-db --user $NUN_USER  -p $NUN_PWD --host "https://http.nundb.org" exec "use-db data-analysis-demo $key; keys" |  tr "," "\n" | grep -v -E "1624735236495_ds|1624735710952_ds"
```


# Main goal 

* Delivery content change in the frontend at the time it changes (fast)

# What may change over time

* Query language (I am not sure if NQL will go or I will use some kind of GraphQA)
* Hash map to Tree or key lock Hash map
* Security layer
* Client protocols

## Chaos
* Sends a message to all replicas to join
[src/lib/replication_ops.rs:731]

* Start initial election
[src/bin/main.rs:150]


## TODO On primary change check sync state
* When receiving and set primary send the state to all replicas to verify it they are all in sync if not a new election is trigged...




## Today
- [x]  Add a command to force election to run (in debug)
- [x]  Debug elections happening from Primary to Secoundary
- [x] There should be a way to debug the nodes connections to other cluster
- [x] Test force election
- [x] [src/lib/election_ops.rs:23] There should be a way to wait for the response from all replicas but there shold be a timeoutsrc/lib/election_ops.rs:23
    [src/lib/disk_ops.rs:656 ] This will be defined in the DB level
    [src/lib/election_ops.rs:16 ] When sending a messagen from there... we will get the id and will know when it was ack from the replicas
- [x]  Create a method in the dbs to replicate message and return the op_id
- [x] Replicate back from the sender in rp message instead of to all
* Create 2 threads one for reading one for writing the writing is already working and the reading needs to work
[src/lib/process_request.rs:495 ]
- [ ] Improve the code in 
[src/lib/replication_ops.rs:908 ]


## Sat Feb 17 13:11:37 2024
- [ ] Rust ws client for Nun-db with TDD
> Redis https://docs.rs/redis/latest/redis/
```rust
let client = redis::Client::open("redis://127.0.0.1/")?;
let mut con = client.get_connection()?;
let mut pubsub = con.as_pubsub();
pubsub.subscribe("channel_1")?;
pubsub.subscribe("channel_2")?;

loop {
    let msg = pubsub.get_message()?;
    let payload : String = msg.get_payload()?;
    println!("channel '{}': {}", msg.get_channel_name(), payload);
}
```
> Firebase
```rust
let firebase = Firebase::new("https://myfirebase.firebaseio.com").at("users").unwrap();
let stream = firebase.with_realtime_events().unwrap();
stream
.listen( | event_type, data| {
println ! ("Type: {:?} Data: {:?}", event_type, data);
}, | err| println!("{:?}", err), false).await;
```
- [x] Auth response should not be only ok
- [x] Hold the same sonnection for multiple methods calls
- [x] Find a name for the tmp class
- [x] Finish the connect implementation
- [x] Implement ws mock for unit test ...
- [x] Fix print to logger messages
    [src/lib/replication_ops.rs:128 ]



## Disk re-factory
### Reducing the Nun-db disk interface

So it turns out that S3 storage is becaming common for database storage.
First step to implement nun-db s3 storage is to simplify the disk interface
Analising the real interface I notice there is only 3 methods that will need change and really belong to the implementation, snapshot_all_pendding_dbs, load_keys_map_from_disk, load_all_dbs_from_disk all others can stay the same.

To organize I will create sub structure inside disk, since I don't think at this point we want to change oplog becuase of performance,  we need oplog to be fast in NunDb so we can process a lot of data, but we don't need the snapshot to be fast, great separation to implement s3.
* First step lets introduce this new Class To concentrate all Oplog opps
* Next analise how the other ones can be implemented in s3, probably implementing one step down the interface 
    * Lets start with the snapshot_all_pendding_dbs
        * This method is subdivided in 2 other method calls snapshot_keys and storage_data_disk, both must be re-implemented
        * Both methods are over used and are probably the build block I need to change to migrate to s3
        * Actually only the storage_data_disk is over used... that is to save the DB from memory to disk and we must adapt it to save to s3, bt first move it to

#### Will have to change
- [ ][src/lib/disk_ops.rs:639] -> Usd in db ops for snapshot re-think
pub fn snapshot_all_pendding_dbs(dbs: &Arc<Databases>) {
- [x][src/lib/disk_ops.rs:95]  -> Used external to read the keys

pub fn load_keys_map_from_disk() -> HashMap<String, u64> {// will not be needed
- [ ][src/lib/disk_ops.rs:257] -> Used in the database staruo to read the databases 
pub fn load_all_dbs_from_disk(dbs: &Arc<Databases>) {

- [ ] write_keys_map_to_disk Will bot be needed
[src/lib/disk_ops.rs:231]
* ADR: We won't be storing oplog, nor global key map into s3 for performance reasons. To be able to process +300k/s we need ops to be light, if the keys map depends on the s3 call it will take at least 50ms and will block next ops. Therefore we will keep it locally and it can be deleted on every pods restarts. Each node will have its own keys ids and it don't be a single id for the full cluster, we accept that as better than slow opps.


#### Oplog related (Won't change)
- [x][src/lib/disk_ops.rs:282] -> Use in test in replication ops consider other alternatives
pub fn get_op_log_file_name() -> String {// may not change at all
- [x][src/lib/disk_ops.rs:685] -> Used in many places
pub fn get_log_file_append_mode() -> BufWriter<File> {
- [x][src/lib/disk_ops.rs:723] -> Used in startup and  many tests
pub fn clean_op_log_metadata_files() {
- [x][src/lib/disk_ops.rs:763] -> Used in replication
pub fn try_write_op_log(
- [x][src/lib/disk_ops.rs:786] -> Used in replication
pub fn write_op_log(
- [x][src/lib/disk_ops.rs:808] -> Used in replication
pub fn last_op_time() -> u64 {

## Today
- [ ] Multi key files
```text
Key -> Hash
        Value addr -> int64 
        Value file -> number

Jose  -> 12389 % 10 = 9
            db_name_9.nun.keys

Values:
        Append Only
```
- [ ] S3 NunDb blog post
- [ ] Single file for Key and Value partitioned



[src/lib/storage/s3.rs:46 ] -> Change it to a single file for each partition
[V ]
[k ]
[ ]
[ ]
