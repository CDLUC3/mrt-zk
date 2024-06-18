# Queue Data

- [Design](../README.md)

## Queue Data



### Space Considerations
> ZooKeeper was not designed to be a general database or large object store. Instead, it manages coordination data. This data can come in the form of configuration, status information, rendezvous, etc. A common property of the various forms of coordination data is that they are relatively small: measured in kilobytes. The ZooKeeper client and the server implementations have sanity checks to ensure that znodes have less than 1M of data, but the data should be much less than that on average. [^1]
[^1]: https://zookeeper.apache.org/doc/r3.3.3/zookeeperProgrammers.html#Data+Access

#### Final vs Volatile data fields
- As we write to zookeeper, should be distinguish our static fields (submitter, file name) from the volatile fields (status, space_needed, last update)?

## Batches and Jobs

| Zookeeper Node Path | Node Data Type | Fields | Created By | Modified By | Comment |
| - | - | - | - | - | - |
| /batches/BID/lock | none | - | Pending, Reporting | - | **Ephemeral node** to lock a batch, deleted by the thread that creates the node |
| /batches/BID/submission | json | profile_name<br/>submitter<br/>payload_filename<br/><br/>erc_what<br/>erc_who<br/>erc_when<br/>erc_where<br/>type<br/>submission_mode | creation | none | |
| /batches/BID/status | json | status<br/>last_modified<br/>message | creation | all jobs done | message is optional| 
| /batches/BID/status-report | json | failed_jobs | failure | failure | last status report sent to user | 
| /batches/BID/states/STATE/JID | none | - | | | STATE = batch-processing / batch-failed / batch-completed / batch-deleted<br/>Create watcher to watch for states/processing to be empty| 
| /batches/BID/lock | none | - | Several states | - | **Ephemeral node** to lock a job, deleted by the thread that creates the node |
| /batch-uuids/UUID | string | - | Pending | none | batchID uuid is minted by ingest, used for lookup |
| /jobs/JID/bid | string | batch_id | creation | none | |
| /jobs/JID/configuration | json | profile_name<br/>submitter<br/>payload_url<br/>payload_type<br/>response_type<br/>local_id | creation | none | |
| /jobs/JID/status | json | status<br/>last_successful_status<br/>last_modification_date<br/>retry_count<br/>message | creation | none | message is optional|
| /jobs/JID/priority | int | - | creation | estimating | |
| /jobs/JID/space_needed | long | - | creation | estimating | |
| /jobs/JID/identifiers | json | primary_id<br/>local_id: [] | creation | processing | |
| /jobs/JID/metadata | json | erc_what<br/>erc_who<br/>erc_when<br/>erc_where | creation | ? | |
| /jobs/JID/inventory | json | manifest_url<br/>mode | ingest | inventory | mode will be used when we implement "fix" options |
| /jobs/states/STATE/PP-JID | none | - | | | PP = priority <br/>STATE = pending / held / estimating / provisioning / downloading / processing / recording / notify / failed / completed |


## Locks

| Zookeeper Node Path | Node Data Type | Fields | Created By | Modified By | Comment |
| - | - | - | - | - | - |
| /locks/queue/ingest | none | - | Admin | Admin | Previously file-system based|
| /locks/queue/accessSmall | none | - | Admin | Admin | |
| /locks/queue/accessLarge | none | - | Admin | Admin | |
| /locks/storage/{ark} | none | - | Ingest | Ingest | slashes are replaced with _ |
| /locks/inventory/{ark} | none | - | Inventory | Inventory | slashes are replaced with _ |
| /locks/collections/{mnemonic} | none | - | Admin | Admin | |

## Access Queue

| Zookeeper Node Path | Node Data Type | Fields | Created By | Modified By | Comment |
| - | - | - | - | - | - |
| /access/small/ID/token<br>/access/large/ID/token | json | token<br>delivery-node<br>cloud-content-byte<br>status<br>url<br>anticipated-availability-time| Access | Access | |
| /access/small/ID/status<br>/access/large/ID/status | json | status<br/>last_modified<br/>message | creation | all jobs done | message is optional| 

## Batch and Job State Transition

- Processing /jobs/states/StateX/PP-JID
- Job finishes StateX
- Update /jobs/JID/status data
  - last_successful_status = StateX
  - status = StateY
  - last_modification_date = now
- Delete /jobs/states/StateX/PP-JID
- Create /jobs/states/StateY/PP-JID
  - Note: The prior state might have altered the priority
- If StateY == Completed
  - Delete /batches/BID/states/processing/JID
  - Create /batches/BID/states/completed/JID
- If StateY == Failed
  - Delete /batches/BID/states/processing/JID
  - Create /batches/BID/states/failed/JID
- If /batches/BID/states/processing is empty, watcher will trigger batch notification


---
## Legacy Zookeeper Data Structure

### Record Data
- Ingest currently serializes java properties
- Inventory currently serializes XML data

### Record Keys
The ingest service currently packs a priority value into the path name for the zookeeper record.
- /ingest/mrtQ-02100000000003
- (document the component parts here)
- Question: priority may become a more dynamic property in the future
  - We could have a baseline priority in the pathname (for sorting) and an actual priority in the payload
  - We could also explore renaming a path dynamically when a priority change is appropriate

### Record Sorting

#### Current Implementation
In Merritt's current zookeeper implementation, record headers contain binary data.
- Status: 1 byte status field with each byte representing a different queue state
- Time: 8 byte long representing the number of seconds since 1970
