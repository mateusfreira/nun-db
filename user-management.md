# User management
* Each user will be a key prefixed with $$user_${user_name}
* Token to access that user will be the value of the key
* That token should be stored encrypted
* Permission will the stored as a key $$permission_${user_name} and as a string ... maybe use the rw linux like

## Questions?
### Will users be global or per database? 
* Per-database so we can easier manage a cluster with many customers
