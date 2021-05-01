# <img src="./logo-nundb.png" width="50" />  NunDB

## What is Nun DB

Nun DB is an open source real time database, made to be fast, light and easy to use.

Nun DB is written to be memory safe and horizotal(soon) scalable.

We believe we can keep Nun DB simple and at the same time it can power lots of different apps and use cases.

## Examples Use cases 

Checkout our examples of integration and see what we can do for your app

* Vue.js + Nun DB
* React.js Realtime Todo app 


# Instalations

## Docker 

Running Nun-db from docker is probably the faster way to do it, the simples steps to run nun-db with
docker is bu running and container using all the default ports like the next example shows.

```
docker run --env NUN_USER=user-name --env NUN_PWD=user-pwd -it --rm -p 3013:3013 -p 3012:3012 -p 3014:3014 --name nun-test mateusfreira/nun-db

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
      - "3014:3014" # Socker
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

Done you now have nun-db running in your docker and exposing all the ports to be use http (3013), web-socket(3012), socket(3014)  you are ready to use.

## Nun Query language (NQL)

### Auth
#### Context
- [ ] Require admin auth
- [ ] Require db auth
- [ ] Replicate? How? ()

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

### Remove
#### Context
- [ ] Require admin auth
- [x] Require db auth
- [x] Replicate? How? (replicate)

### Snapshot
#### Context
- [x] Require admin auth
- [x] Require db auth
- [x] Replicate? How? (replicate-snapshot)

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
#### Context
- [ ] Require admin auth
- [x] Require db auth
- [ ] Replicate? How? 

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

## Connectors 
* Http
    Port: 3013
* Socket
    Port: 3014
    
* Web Socket
    Port: 3012

## Special keys

All special keys will have a `$` symbol in the first letter of the name.

### $connections 

Count the number of connections to a databse.

```
$connections
```

## Diagram

```bash

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



## Having any issue or needs help?

Open an issue I will follow as soon as possible.

## Want to use it in production ?

Talk to me @mateusfreira I will help you get it to the scale you need.

