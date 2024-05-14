# Queue Administration

- [Design](../README.md)

## Purpose
Migrate Queue Administration Tasks from Ingest to the Merritt Admin Tool

## Potential Enhancements to Enable Shift of Admin Functionality

### Read from Zookeeper from Admin Tool

- This is very do-able.
- Current ingest queue uses java property serialization.  This may be difficult for ruby code to read.
- Proposal: modify the Ingest Queue Item to be serialized as JSON instead

### Write to Zookeeper from Admin Tool

- This is very do-able.
- Assumes binary data can be written back as-is from ruby
- Proposal: modify the Ingest Queue Item to be serialized as JSON instead

### Publish Ingest Profiles as an Artifact

- Ingest service will pull profiles from a deployed artifact (zip file) rather than cloning git
- Admin tool code will pull profiles from a deployed artifact (zip file) rather than requesting data from ingest
- See https://github.com/CDLUC3/mrt-doc-private/issues/80

### ~Mount ZFS to Lambda~

- This is not recommended
- Conceptually, this could allow the remaining set of admin functions to be performed entirely from Lambda

## Migration Levels
- m1: support mrt-zk migration (ingest and inventory)
- m2: collection locks and ingest queue lock through zookeeper
- m3: support mrt-zk migration (access)
- m4: profiles accessible to lambda
- m5: refactor collection creation

## Existing Ingest Admin Endpoints

|service|admin endpoint|future loc|feature needed | comment|
|-|-|-|-|-| 
|ingest|/state| NA | |  /admin/state duplicates /state |
|ingest|/help| NA | | /admin/help duplicates /state |
|ingest|POST reset| ?? | | |
|ingest m1|/locks| admin| read zookeeper from admin| |
|ingest m1|/queues| admin | read zookeeper from admin | NA|
|ingest m1|/queues-acc| admin | read zookeeper from admin | NA |
|ingest m1|/queues-inv| admin | read zookeeper from admin | |
|ingest m1|/queue| admin | read zookeeper from admin| ?|
|ingest m1|/queue/{queue}| admin | read zookeeper from admin | Job.list_all<br>Job.list_all_legacy |
|ingest m1|/queue-acc/{queue}| admin | read zookeeper from admin | Assembly.list_all_legacy|
|ingest m1|/queue-inv/{queue}| admin | read zookeeper from admin | Job.list_all_legacy_inv|
|ingest m1|/lock/{lock}| admin |read  zookeeper from admin | ObjectLocks.list_all |
|ingest m1|POST /requeue/{queue}/{id}/{fromState}| admin | write zookeeper from admin | job.set_status(zk, job.status.state_change(:State)) |
|ingest m1|POST /deleteq/{queue}/{id}/{fromState}| admin | write zookeeper from admin  | job.delete(zk)|
|ingest m1|POST /cleanupq/{queue}| admin | write zookeeper from admin  | Job.cleanup |
|ingest m1|POST /{action: hold or release}/{queue}/{id}| admin | write zookeeper from admin  | job.set_status(zk, job.status.state_change(:State)) |
|ingest m1|POST /release-all/{queue}/{profile}| admin | write zookeeper from admin  | Collection.release_jobs|
|ingest m4|{profilePath}| admin | profiles as artifact | |
|ingest m4|/profiles-full| admin| profiles as artifact | this endpoint also returns data related to collection locks (m2)|
|ingest m4|/profile/{profile}| admin | profiles as artifact| |
|ingest m4|/profile/admin/{env}/{type}/{profile}| admin| profiles as artifact | |
|ingest|/bids/{batchAge}| ingest | mount zfs to lambda | keep in ingest |
|ingest|/bid/{batchID}| ingest | mount zfs to lambda | keep in ingest|
|ingest|/bid/{batchID}/{batchAge}| ingest | mount zfs to lamda | keep in ingest|
|ingest|/jid-erc/{batchID}/{jobID}| ingest| mount zfs to lambda | keep in ingest|
|ingest|/jid-file/{batchID}/{jobID}| ingest | mount zfs to lambda| keep in ingest|
|ingest|/jid-manifest/{batchID}/{jobID}| ingest | mount zfs to lambda|  keep in ingest|
|ingest m2|POST /submission/{request: freeze or thaw}/{collection}| admin | implement hold/freeze in ZK | Collection.hold <br/>Collection.release |
|ingest m2|POST /submissions/{request: freeze or thaw}| admin | implement hold/freeze in ZK | Job.hold <br/> Job.release|
|ingest m4|POST /profile/{type}| admin? | | Is this simply a template edit?  If so, could the admin tool do this?|
|access m3|POST /flag/set/access/#{qobj}|admin|write zookeeper from admin |Access.hold|
|access m3|POST /flag/clear/access/#{qobj}|admin|write zookeeper from admin |Access.relese|
