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
