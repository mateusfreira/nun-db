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
                   



Time line... 
```text

+--------+       +--------+        +---+        +------+                                                 
|   C1   +-------|Deploy  +--------|-C2+--------|Deploy|                                                         
|   Done |       |  Done  |        |   |        |      |                                                 
+--------+       +--------+        +---+        +------+                                                 

```
