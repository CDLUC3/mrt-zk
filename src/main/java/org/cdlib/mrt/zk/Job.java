package org.cdlib.mrt.zk;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 * Class to manage a Merritt Ingest Job in the Job Queue.
 * @see <a href="https://github.com/CDLUC3/mrt-zk/blob/main/design/transition.md">State Transition Design</a>
 */
public class Job extends QueueItem {
  public static int PRIORITY = 5;
  private String bid;
  private int retryCount = 0;
  private int priority = PRIORITY;
  private long spaceNeeded = 0;
  private String jobStatePath = null;
  private String batchStatePath = null;
  private JSONObject identifiers = new JSONObject();
  private JSONObject metadata = new JSONObject();
  private JSONObject inventory = new JSONObject();

  public Job(String id) {
    super(id);
  }

  public Job(String id, String bid) {
    super(id);
    this.bid = bid;
  }

  public Job(String id, String bid, JSONObject data) {
    super(id, data);
    this.bid = bid;
  }

  public String bid() {
    return bid;
  }

  public String jobStatePath() {
    return jobStatePath;
  }

  public String batchStatePath() {
    return batchStatePath;
  }

  public int retryCount() {
    return retryCount;
  }

  public int priority() {
    return priority;
  }

  public long spaceNeeded() {
    return spaceNeeded;
  }

  public String dir() {
    return QueueItem.ZkPaths.Job.path;
  }

  public String prefix() {
    return QueueItem.ZkPrefixes.Job.prefix;
  }

  public static String prefixPath() {
    return String.format("%s/%s", QueueItem.ZkPaths.Job.path, QueueItem.ZkPrefixes.Job.prefix);
  };

  public static IngestState initStatus() {
    return JobState.Pending;
  }


  public IngestState resolveStatus(String s){
    return JobState.valueOf(s);
  }

  @Override
  public void loadProperties(ZooKeeper client) throws MerrittZKNodeInvalid, KeeperException, InterruptedException {
    data = optJsonProperty(client, ZKKey.JOB_CONFIGURATION);
    bid = stringProperty(client, ZKKey.JOB_BID);
    priority = intProperty(client, ZKKey.JOB_PRIORITY);
    spaceNeeded = longProperty(client, ZKKey.JOB_SPACE_NEEDED);
    identifiers = optJsonProperty(client, ZKKey.JOB_IDENTIFIERS);
    metadata = optJsonProperty(client, ZKKey.JOB_METADATA);
    inventory = optJsonProperty(client, ZKKey.JOB_INVENTORY);
    setJobStatePath(client);
    setBatchStatePath(client);
  }

  public IngestState[] states() {
    return BatchState.values();
  }

  public static JSONObject createJobMetadata(String who, String what, String when, String where) {
    JSONObject json = new JSONObject();
    json.put(MerrittJsonKey.ErcWho.key(), who);
    json.put(MerrittJsonKey.ErcWhat.key(), what);
    json.put(MerrittJsonKey.ErcWhen.key(), when);
    json.put(MerrittJsonKey.ErcWhere.key(), where);
    return json;     
  }

  public String ercWho() {
    return jsonStringProperty(metadata, MerrittJsonKey.ErcWho, "");
  }

  public String ercWhat() {
    return jsonStringProperty(metadata, MerrittJsonKey.ErcWhat, "");
  }

  public String ercWhen() {
    return jsonStringProperty(metadata, MerrittJsonKey.ErcWhen, "");
  }

  public String ercWhere() {
    return jsonStringProperty(metadata, MerrittJsonKey.ErcWhere, "");
  }

  public static JSONObject createJobIdentifiers(String primary, String local) {
    JSONObject json = new JSONObject();
    json.put(MerrittJsonKey.PrimaryId.key(), primary);
    JSONArray arr = new JSONArray();
    for(String s: local.split(";")) {
      if (!s.isEmpty()) {
        arr.put(s);
      }
    }
    json.put(MerrittJsonKey.LocalId.key(), arr);
    return json;     
  }

  public String primaryId() {
    return jsonStringProperty(identifiers, MerrittJsonKey.PrimaryId, "");
  }

  public String localId() {
    JSONArray arr = (JSONArray)jsonDataProperty(identifiers, MerrittJsonKey.LocalId, new JSONArray());
    String[] str = new String[arr.length()];
    for(int i=0; i < arr.length(); i++) {
      str[i] = arr.getString(i);
    }
    return String.join(";", str);
  }

  public static JSONObject createJobConfiguration(String profile, String submitter, String payloadUrl, String payloadType, String responseType) {
    JSONObject json = new JSONObject();
    json.put(MerrittJsonKey.ProfileName.key(), profile);
    json.put(MerrittJsonKey.Submitter.key(), submitter);
    json.put(MerrittJsonKey.JobPayloadUrl.key(), payloadUrl);
    json.put(MerrittJsonKey.JobPayloadType.key(), payloadType);
    json.put(MerrittJsonKey.JobResponseType.key(), responseType);
    return json;     
  }

  public String profileName() {
    return jsonStringProperty(data(), MerrittJsonKey.ProfileName, "");
  }

  public String submitter() {
    return jsonStringProperty(data(), MerrittJsonKey.Submitter, "");
  }

  public String payloadUrl() {
    return jsonStringProperty(data(), MerrittJsonKey.JobPayloadUrl, "");
  }

  public String payloadType() {
    return jsonStringProperty(data(), MerrittJsonKey.JobPayloadType, "");
  }

  public String responseType() {
    return jsonStringProperty(data(), MerrittJsonKey.JobResponseType, "");
  }

  public String jid() {
    return jsonStringProperty(data(), MerrittJsonKey.JID, "");
  }

  public static Job createJob(ZooKeeper client, String bid, JSONObject configuration) throws MerrittZKNodeInvalid, KeeperException, InterruptedException, MerrittStateError {
    return createJob(client, bid, PRIORITY, configuration, new JSONObject(), new JSONObject());
  }
  public static Job createJob(ZooKeeper client, String bid, int priority, JSONObject configuration) throws MerrittZKNodeInvalid, KeeperException, InterruptedException, MerrittStateError {
    return createJob(client, bid, priority, configuration, new JSONObject(), new JSONObject());
  }
  public static Job createJob(ZooKeeper client, String bid, int priority, JSONObject configuration, JSONObject identifiers) throws MerrittZKNodeInvalid, KeeperException, InterruptedException, MerrittStateError {
    return createJob(client, bid, priority, configuration, identifiers, new JSONObject());
  }
  public static Job createJob(ZooKeeper client, String bid, int priority, JSONObject configuration, JSONObject identifiers, JSONObject metadata) throws MerrittZKNodeInvalid, KeeperException, InterruptedException, MerrittStateError {
    String id = QueueItemHelper.createId(client, Job.prefixPath());
    Job job = new Job(id, bid, configuration);
    if (!job.lock(client)) {
      return null;
    }
    try {
      job.createData(client, ZKKey.JOB_BID, bid);
      job.createData(client, ZKKey.JOB_PRIORITY, job.priority);
      job.createData(client, ZKKey.JOB_SPACE_NEEDED, job.spaceNeeded);
      job.createData(client, ZKKey.JOB_CONFIGURATION, configuration);
    
      if (!identifiers.isEmpty()) {
        job.setIdentifiers(client, identifiers);
      }
      if (!metadata.isEmpty()) {
        job.setMetadata(client, metadata);
      }
      job.setStatusWithPriority(client, Job.initStatus(), priority);
      job.setBatchStatePath(client);
      job.setJobStatePath(client);
      return job;
    } finally {
      job.unlock(client);
    }
  }

  public JSONObject statusObject(IngestState status) {
    JSONObject jobj = super.statusObject(status);
    jobj.put(MerrittJsonKey.LastSuccessfulStatus.key(), JSONObject.NULL);
    jobj.put(MerrittJsonKey.RetryCount.key(), retryCount);
    return jobj;
  }

  public void loadStatus(ZooKeeper client, JSONObject js) throws MerrittZKNodeInvalid {
    super.loadStatus(client, js);
    retryCount = js.getInt(MerrittJsonKey.RetryCount.key());
  }

  private void setPriority(ZooKeeper client, int priority) throws KeeperException, InterruptedException, MerrittZKNodeInvalid {
    if (priority == this.priority) {
      return;
    }
    this.priority = priority;
    setData(client, ZKKey.JOB_PRIORITY, priority);
  }

  public void setSpaceNeeded(ZooKeeper client, long spaceNeeded) throws MerrittZKNodeInvalid, KeeperException, InterruptedException {
    if (spaceNeeded == this.spaceNeeded) {
      return;
    }
    this.spaceNeeded = spaceNeeded;
    setData(client, ZKKey.JOB_SPACE_NEEDED, spaceNeeded);
  }

  public void setIdentifiers(ZooKeeper client, JSONObject identifiers) throws MerrittZKNodeInvalid, KeeperException, InterruptedException {
    if (this.identifiers.similar(identifiers)) {
      return;
    }
    this.identifiers = identifiers;
    createOrSetData(client, ZKKey.JOB_IDENTIFIERS, identifiers);
  }

  public void setMetadata(ZooKeeper client, JSONObject metadata) throws MerrittZKNodeInvalid, KeeperException, InterruptedException {
    if (this.metadata.similar(metadata)) {
      return;
    }
    this.metadata = metadata;
    createOrSetData(client, ZKKey.JOB_METADATA, metadata);
  }

  public void setInventory(ZooKeeper client, String manifest_url, String mode) throws MerrittZKNodeInvalid, KeeperException, InterruptedException {
    inventory.put(MerrittJsonKey.InventoryManifestUrl.key(), manifest_url);
    inventory.put(MerrittJsonKey.InventoryMode.key(), mode);
    createOrSetData(client, ZKKey.JOB_INVENTORY, inventory);
  }

  public String inventoryManifestUrl() {
    return jsonStringProperty(inventory, MerrittJsonKey.InventoryManifestUrl, "");
  }

  public String inventoryMode() {
    return jsonStringProperty(inventory, MerrittJsonKey.InventoryMode, "");
  }

  public void setStatusWithRetry(ZooKeeper client, IngestState status) throws MerrittZKNodeInvalid, KeeperException, InterruptedException, MerrittStateError {
    retryCount += 1;
    setStatus(client, status);
  }

  public void setStatusWithPriority(ZooKeeper client, IngestState status, int priority) throws MerrittZKNodeInvalid, KeeperException, InterruptedException, MerrittStateError {
    setPriority(client, priority);
    setStatus(client, status);
  }

  public void setStatusTrigger(ZooKeeper client) throws MerrittZKNodeInvalid, KeeperException, InterruptedException {
    setJobStatePath(client);
    setBatchStatePath(client);
  }

  public String batchStateSubpath() {
    if (this.status() == JobState.Failed) {
      return BatchJobStates.Failed.path;
    } else if (this.status() == JobState.Completed) {
      return BatchJobStates.Completed.path;
    } else if (this.status() == JobState.Deleted) {
      return BatchJobStates.Deleted.path;
    }
    return BatchJobStates.Processing.path;
  }

  public void setBatchStatePath(ZooKeeper client) throws MerrittZKNodeInvalid, KeeperException, InterruptedException {
    String bs = String.format("%s/%s/states/%s/%s", QueueItem.ZkPaths.Batch.path, bid, batchStateSubpath(), id());
    if (bs == batchStatePath) {
      return;
    }

    if (batchStatePath != null) {
      QueueItemHelper.delete(client, batchStatePath);
    }
    batchStatePath = bs;
    if (!QueueItemHelper.exists(client, bs)) {
      String p = Paths.get(bs).getParent().toString();
      if (!QueueItemHelper.exists(client, p)) {
        QueueItemHelper.createIfNeeded(client, p);
      }
      QueueItemHelper.createIfNeeded(client, bs);
    }
  }

  public void setJobStatePath(ZooKeeper client) throws MerrittZKNodeInvalid, InterruptedException, KeeperException {
    String js = String.format("%s/states/%s/%02d-%s", QueueItem.ZkPaths.Job.path, status().name().toLowerCase(), priority, id());
    if (js == jobStatePath) {
      return;
    }
    if (jobStatePath != null) {
      QueueItemHelper.delete(client, jobStatePath);
    }
    jobStatePath = js;
    if (!QueueItemHelper.exists(client, js)) {
      String p = Paths.get(js).getParent().toString();
      if (!QueueItemHelper.exists(client, p)) {
        QueueItemHelper.create(client, p, QueueItemHelper.empty);
      }
      QueueItemHelper.create(client, js, QueueItemHelper.empty);
    }
  }

  public void delete(ZooKeeper client) throws MerrittStateError, MerrittZKNodeInvalid, InterruptedException, KeeperException {
    if (!this.status().isDeletable()) {
      throw new MerrittStateError(String.format("Delete invalid for %s", path()));
    }
    QueueItemHelper.delete(client, jobStatePath());
    QueueItemHelper.delete(client, batchStatePath());
    QueueItemHelper.deleteAll(client, path());
  }

  public static Job acquireJob(ZooKeeper client, IngestState state) throws MerrittZKNodeInvalid, KeeperException, InterruptedException {
    String p = String.format("%s/states/%s", QueueItem.ZkPaths.Job.path, state.name().toLowerCase());
    if (!QueueItemHelper.exists(client, p)) {
      return null;
    }
    List<String> jobs = client.getChildren(p, false);
    jobs.sort(String::compareTo);
    for(String cp: jobs) {
      Job j = new Job(cp.substring(3));
      if (j.lock(client)) {
        j.load(client);
        return j;
      }
    }  
    return null;
  }

  public static void initNodes(ZooKeeper client) throws KeeperException, InterruptedException {
    QueueItemHelper.createIfNeeded(client, QueueItem.ZkPaths.Batch.path);
    QueueItemHelper.createIfNeeded(client, QueueItem.ZkPaths.BatchUuids.path);
    QueueItemHelper.createIfNeeded(client, QueueItem.ZkPaths.Job.path);
    QueueItemHelper.createIfNeeded(client, QueueItem.ZkPaths.JobStates.path);
    for(JobState js: JobState.values()) {
      String s = String.format("%s/%s", QueueItem.ZkPaths.JobStates.path, js.name().toLowerCase()); 
      QueueItemHelper.createIfNeeded(client, s);
    }
  }

  public static List<Job> listJobs(ZooKeeper client, IngestState state) throws KeeperException, InterruptedException, MerrittZKNodeInvalid {
    ArrayList<Job> jobs = new ArrayList<>();
    List<String> jids = client.getChildren(ZkPaths.Job.path, false);
    jids.sort(String::compareTo);
    for(String jid: jids) {
      if (jid.equals("states")) {
        continue;
      }
      Job job = new Job(jid);
      job.load(client);
      if (state == null || state == job.status()) {
        jobs.add(job);
      }
    }
    return jobs;
  }

}
