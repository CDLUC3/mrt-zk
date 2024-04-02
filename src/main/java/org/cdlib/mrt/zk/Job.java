package org.cdlib.mrt.zk;

import java.nio.file.Paths;
import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.json.JSONObject;

/**
 * Class to manage a Merritt Ingest Job in the Job Queue.
 * @see <a href="https://github.com/CDLUC3/mrt-zk/blob/main/design/transition.md">State Transition Design</a>
 */
public class Job extends QueueItem {
  public static final String DIR = "/jobs";
  public static final String PREFIX = "jid";
  private String bid;
  private int retryCount = 0;
  private int priority = 5;
  private long spaceNeeded = 0;
  private String jobStatePath = null;
  private String batchStatePath = null;

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
    return Job.DIR;
  }

  public String prefix() {
    return Job.PREFIX;
  }

  public static String prefixPath() {
    return String.format("%s/%s", Job.DIR, Job.PREFIX);
  };

  public static IngestState initStatus() {
    return JobState.Pending;
  }


  public IngestState resolveStatus(String s){
    return JobState.valueOf(s);
  }

  @Override
  public void loadProperties(ZooKeeper client) throws MerrittZKNodeInvalid, KeeperException, InterruptedException {
    bid = stringProperty(client, ZKKey.JOB_BID);
    priority = intProperty(client, ZKKey.JOB_PRIORITY);
    spaceNeeded = longProperty(client, ZKKey.JOB_SPACE_NEEDED);
    setJobStatePath(client);
    setBatchStatePath(client);
  }

  public IngestState[] states() {
    return BatchState.values();
  }

  public static Job createJob(ZooKeeper client, String bid, JSONObject configuration) throws MerrittZKNodeInvalid, KeeperException, InterruptedException {
    String id = QueueItemHelper.createId(client, Job.prefixPath());
    Job job = new Job(id, bid, configuration);
    job.createData(client, ZKKey.JOB_BID, bid);
    job.createData(client, ZKKey.JOB_PRIORITY, job.priority);
    job.createData(client, ZKKey.JOB_SPACE_NEEDED, job.spaceNeeded);
    job.createData(client, ZKKey.JOB_CONFIGURATION, configuration);
    job.setStatus(client, Job.initStatus());
    job.setBatchStatePath(client);
    job.setJobStatePath(client);
    return job;
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

  public void setPriority(ZooKeeper client, int priority) throws KeeperException, InterruptedException, MerrittZKNodeInvalid {
    if (priority == this.priority) {
      return;
    }
    this.priority = priority;
    setData(client, ZKKey.JOB_PRIORITY, priority);
    setJobStatePath(client);
  }

  public void setSpaceNeeded(ZooKeeper client, long spaceNeeded) throws MerrittZKNodeInvalid, KeeperException, InterruptedException {
    if (spaceNeeded == this.spaceNeeded) {
      return;
    }
    this.spaceNeeded = spaceNeeded;
    setData(client, ZKKey.JOB_SPACE_NEEDED, spaceNeeded);
  }

  public void setStatusWithRetry(ZooKeeper client, IngestState status) throws MerrittZKNodeInvalid, KeeperException, InterruptedException {
    retryCount += 1;
    setStatus(client, status);
  }

  public void setStatusTrigger(ZooKeeper client) throws MerrittZKNodeInvalid, KeeperException, InterruptedException {
    setJobStatePath(client);
    setBatchStatePath(client);
  }

  public String batchStateSubpath() {
    if (this.status() == JobState.Failed) {
      return "batch-failed";
    } else if (this.status() == JobState.Completed) {
      return "batch-completed";
    }
    return "batch-processing";
  }

  public void setBatchStatePath(ZooKeeper client) throws MerrittZKNodeInvalid, KeeperException, InterruptedException {
    String bs = String.format("%s/%s/states/%s/%s", Batch.DIR, bid, batchStateSubpath(), id());
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
        QueueItemHelper.create(client, p, QueueItemHelper.empty);
      }
      QueueItemHelper.create(client, bs, QueueItemHelper.empty);
    }
  }

  public void setJobStatePath(ZooKeeper client) throws MerrittZKNodeInvalid, InterruptedException, KeeperException {
    String js = String.format("%s/states/%s/%02d-%s", Job.DIR, status().name().toLowerCase(), priority, id());
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
    String p = String.format("%s/states/%s", Job.DIR, state.name().toLowerCase());
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

}
