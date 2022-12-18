# Change 1

1. Add opp id to replication
2. rp op id rest_of_message (May lead to bigger refactory I will reavaluate)
3. May be possible to deploy here
4. Process ack op id
5. May be possible to deploy here
4. Process ck op id
6. ACK message from replication
- [x] Done

# Change 2 

3. Add version to value on disk (To detect conflicts)

## How will we store versions on disk?
* We already store keys on a separated file so we would not need to store it...
* Keys files is only used for replication so it is not a good idea.
* To store key and value in the same disk will use the double of the space needed...(Today it is already like that)

```

 +-------------++---------------------++---------------------+
 |  key        ||         version     ||      value          |                     
 | length:rest || 4 bytes             ||   length:rest       |                 
 +-------------++---------------------++---------------------+     

 ```

I did not like this process too much I would need to store all the data all the time ... and what I need is.

```
# keys file
 +-------------++---------------------++---------------------+
 |  key        ||         version     ||      value_addr     |
 | length:rest || 4 bytes             ||      8 bytes        |
 +-------------++---------------------++---------------------+
 ## If key address is -1 means the key is deleted

# Values file

 +-------------++---------------------+
 |  value      ||         status      |
 | length:rest || 4 bytes             |
 +-------------++---------------------+

 ```

 The second approach allow us to use an append only strategy to the values file and allow us to update only the keys that changed, with an in place update to mark values as deleted and appending the new value super fast. (Test this speed)

 ... this needs to be faster than the current solution... 100ms to store 10k records and 50ms read the same from disk to ready in memory

 ...Performance was much better than previous implementation.

 Time to think in the migration time

 Migration will be done at snapshot time ... unit test will cover ...

 Hard brave dev mode fail ... TDD saved the day :)

 Code is a bit messy time to refactoring
 
 Update in place implemented

Can store like that ... won't be fast to search


Time line... 
```text

+--------+       +--------+        +---+        +------+                                                 
|   C1   +-------|Deploy  +--------|-C2+--------|Deploy|                                                         
|   Done |       |  Done  |        |   |        |      |                                                 
+--------+       +--------+        +---+        +------+                                                 

```


# Todo
- [ ] Replica set sending the set does not need to process the replication!!! (works because if rejects the new version but it is unnecessary  travel)
- [ ] Measure clock difference from replica-sets (Is it needed?) to implement oldest change wins




## 1. Process to resolve from arbiter to Secondary (Working as expected)
```text
                          +-----------------------+                                                                                                                                                 
+------------+ 1. resolve |                       |                                                                                                                                                          
|   Arbiter  |------------|  Secoundary 1 server  |       +---------------+                                                                                                                         
+------------+            +---+----------------+--+       |               |                                                                                                                                   
                              |                |      +---+ Primary server| -> resolve the conflict in the DB and memory                                                                                                                                    
                   +---------------+           +------+   +---------+-----+                                                                                                                                   
                   | Secoundary 2  |       2 resolve                |                                                                                                                                         
                   |               +--------------------------------+                                                                                                                                         
                   +---------------+          3 replicate set value version                                                                                                                                                                 
                                              4 replicate set $$conflitc_{opp_id} version                                                                                                                                                                 
                                                                                                                                                                                                              
```
1. Arbiter sends resolve the conflict pushes the change as a resolve command                                                                                                                                                                                                               
1.1  If server is secondary sends the same message to primary [src/lib/process_request.rs:441]
1.1  If server is primary resolve resolve it in memory [src/lib/process_request.rs:434]
2. Secondary sends message to Primary to resolve
2.1 replicates to other nodes as a set value [src/lib/replication_ops.rs:95]
3. Primary sends replicate set value version to all nodes [src/lib/replication_ops.rs:107]
4. Primary sends replicate set $$conflitc_{opp_id} to all nodes to resolve the conflict [src/lib/replication_ops.rs:111]

## 2. Process to resolve from arbiter to Primary ( Simples case working as expected

## 3. Change in 2 nodes at the same time and Arbiter is connected to the 3rd node (Works because of replication)
                                             +---------+ 1.2 replicate name 1 jose
```text                                      |         |
                          +------------------+----+    |                           +-----------+                                                                                                    
+------------+            |                       |    |                           |  Client 2 |                                                                                                                     
|   Arbiter  |------------|  Secoundary 1 server  |    |  +---------------+        +----+------+                                                                                                    
+------------+            +---+----------------+--+    |  |               +-------------+                                                                                                                     
                              |                        +--+ Primary server|     1. set-safe name 1 jose                                                                                                                                                     
                   +---------------+   1.2 $conflict >    +---------+-----+                                                                                                                                   
                   | Secoundary 2  |   2.1 replicate name 1 mary>   |            In memory 2.2 Conflict no arbiter error                                                                                                                              
                   |               +--------------------------------+ 100ms                                                                                                                                        
                   +---+-----------+  < 1.1 replicate name 1 jose                                                                                                                                                                                                      
                       |              < 2.3 error $conflict 1 name mary jose
                       |  
                       |  
                       |  
                       |  
                       |  
                       |  2 set-safe name 1 mary                                                                                                                                                                                                                               
          +----------+ |                                                                                                                                                                                                                                  
          | Client 1 +-+                                                                                                                                                                                                                                          
          +----------+                                                                                                                                                                                        
```

### Options to solve. (No longer needed)
3.1 - Do not allow the arbiter to connect to secondary. Cons: May introduce complexity, Need to notify client if primary changes.
3.2 - "Ask" in the network who has one arbiter connected, if no server has one. Cons: Hard to conciliate the messages if no arbiter is connected anyware.

## 4. Change in 2 nodes at the same time and no Arbiter is connected to the 3rd node (Not working)

### Options to solve.
4.1 Reject the set command if there is no arbiter!!
4.1.1 What if the arbiter is connected in another instance??
4.2 If there is no arbiter make it set direct to primary (Very bad with latency)
4.3 *Make all values in Nun-db "eventual" consistent and notify the clients that there may be conflict with one of their keys*
4.4 In case one db is "arbitered" there must be one arbiter connected.

# Create conflict queue
- [x] Use keys with conflict to keys * to get pending conflict queue.
- [x] Replicate conflict keys
- [x] Create db with conflict strategy
- [x] If try to set value to a key with pending conflict the set gets to the conflict queue
- [ ] Database is shutdown while conflict is not solved yet
