# User management
* Each user will be a key prefixed with $$user_${user_name}
* Token to access that user will be the value of the key
* That token should be stored encrypted
* Permission will the stored as a key $$permission_${user_name} and as a string ... maybe use the rw linux like

## Questions?
### Will users be global or per database? 
* Per-database so we can easier manage a cluster with many customers

### How will we decide if a user has or not access to the keys?
* There will be 2 kinds of base permission, allow, deny. Allow user will by default have no access and will have to have a list of keys they have access too. On the other hand deny user will have access to all keys and one can limit their access.
* The permission will be stored in the key `$$permission_${user_name}` as the first value e.g `allow *` or `deny test*`
* e.g `set-permissions $user_name $type` command
* e.g `set-permissions jose deny test`



## Open problems
- [ ] Permission to became arbiter
- [ ] Read and write different permission
