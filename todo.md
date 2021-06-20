- [x] Get Keys
- [ ] Data Replication
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
     - [ ] Stop using timestamp
     - [ ] Document db creation and deletion lock while restoring a replica set...
     - [ ] Document how to backup the admin datatabase
     - [ ] Document how the all election process works
     - [ ] Document only snapshoted dbs are restored from disaster??? Should we change it?
     - [ ] Update library to use the cluster (Js)
     - [ ] Compare performance with old version (argo + https://k6.io/open-source)
     - [ ] What if oplog file became too big? We need a command to clean oplog file
     - [ ] Some times election falling in ./tests/test-fail-primary-dbs.sh all
     - [ ] Implement ping command
     - [x] Add command to estimate op log size -> Create a issue to it
- [ ] Read https://jepsen.io/analyses/redis-raft-1b3fbf6
- [x] Add cli interface
- [x] Remove the need to admin auth to use an database  
- [x] Send and errro if the DB does not exits
- [x] Add un-watch command 
- [x] add secret token to create datbase 
- [ ] Read https://www.microsoft.com/en-us/research/uploads/prod/2018/03/faster-sigmod18.pdf
- [x] Reduce the size of the docker image



```
                                      .------------------------------------------.
                                      |                     |                    |              .---------------.
                                      |       http_ops      |                    |------------->|  Disck        |      
                                      |                     |   disk_ops         |------------->|               |      
                                      |_____________        |                    |              .---------------.
                                      |            |        .____________________|                                     
                                      |            |        |                    |                                     
                                      |            |        |                    |                                     
                                      |   tcp_ops   \_______|_______             |                                     
                                      |             |       |       |            |                                     
.----------------.                    |             |       |       |            |      .---------------. 
|   Frontends/   | ---text----------> |_____________|parse <.> core | db_ops     |----->|   Memory      | 
|  Clients       | <---text---------- |             |       |       |            |<-----| Repository    | 
.----------------.                    |             |       |       |            |      |               | 
                                      |             \_______|_______|            |      ._______________.
                                      |    ws_ops           |                    |                                    
                                      |                     |                    |                                    
                                      |_____________________.____________________|                                    
                                      |                                          |                                    
                                      |                                          |                                    
                                      |                 monitoring               |                                    
                                      |                                          |                                    
                                      |                                          |                                    
                                      .------------------------------------------.                                    
```

# Main goal 

* Delivery content change in the front ent at the time it changes (fast)

# What may change over time

* Query language (I am not sure if NQL will go or I will use some kind of GraphQA)
* Hash map to Tree or key lock Hash map
* Security layer
* Client protocols
