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



Can store like that ... won't be fast to search


Time line... 
```text

+--------+       +--------+        +---+        +------+                                                 
|   C1   +-------|Deploy  +--------|-C2+--------|Deploy|                                                         
|   Done |       |  Done  |        |   |        |      |                                                 
+--------+       +--------+        +---+        +------+                                                 

```
