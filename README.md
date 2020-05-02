# Nun DB 

## Experimentation Repository (I do not recommend it for production YET)

## What is Nun DB

Nun DB is an open source real time database, made to be fast, light and easy to use.

Nun DB is written to be memory safe and horizotal(soon) scalable.

We believe we can keep Nun DB simple and at the same time it can power lots of different apps and use cases.

## Examples Use cases 

Checkout our examples of integration and see what we can do for your app

* Vue.js + Nun DB
* React.js Realtime Todo app 


## How to run
* Docker 

Running Nun-db from docker is probably the faster way to do it, the simples steps to run nun-db with
docker is bu running and container using all the default ports like the next example shows.

```
docker run -it --rm -p 3013:3013 -p 3012:3012 -p 3014:3014 --name nun-test mateusfreira/nun-db

```

Create the sample DB:

```
curl -X "POST" "http://localhost:3013" -d "auth mateus mateus; create-db sample sample-pwd;"
# You should see something like
Valid auth
;create-db success
#That means success
```
Done you now have nun-db running in your docker and exposing all the ports to be use http (3013), web-socket(3012), socket(3014)  you are ready to use.

## Nun Query language (NQL)

### Get
### Set
### Watch
### UnWatch
### Auth
### Snapshot

## Connectors 
* Http
    Port: 3013
* Socket
    Port: 3014
    
* Web Socket
    Port: 3012

## Diagram

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
