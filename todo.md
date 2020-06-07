# Command line tools 
* Start nun db 
* Create a database
- host
- db_name
- db_token
* Fix paramters
- admin
- admin_pwd
* Config
    

- [x]  Send and errro if the DB does not exits
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
* Delivery contant change in the front ent at the time it changes (fast)

# What may change over time
* Query language (I am not sure if NQL will go or I will use some kind of GraphQA)
* Hash map to Tree or key lock Hash map
* Security layer
* Client protocols
