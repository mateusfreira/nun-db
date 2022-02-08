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


 +-------------++---------------------++---------------------+
 |  key        ||         version     ||      value          |                     
 | length:rest || 4 bytes             ||   length:rest       |                 
 +-------------++---------------------++---------------------+     
Can store like that ... won't be fast to search
                                                                         


Time line... 
```text

+--------+       +--------+        +---+        +------+                                                 
|   C1   +-------|Deploy  +--------|-C2+--------|Deploy|                                                         
|   Done |       |  Done  |        |   |        |      |                                                 
+--------+       +--------+        +---+        +------+                                                 

```
