package org.cdlib.mrt.zk;

import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.json.JSONObject;

/**
 * Class to manage a Merritt Ingest Batch in the Batch Queue.
 * @see <a href="https://github.com/CDLUC3/mrt-zk/blob/main/design/transition.md">State Transition Design</a>
 */
public class Batch extends QueueItem {
  private boolean hasFailure = false;

  /**
   * @param id Unique id assigned to a batch.  This id is generated by creating a sequential batch folder.
   */
  public Batch(String id) {
    super(id);
  }

  /**
   * @param id Unique id assigned to a batch.  This id is generated by creating a sequential batch folder.
   * @param data JSON representation of an ingest initiated by a Merritt depostor.
   */
  public Batch(String id, JSONObject data) {
    super(id, data);
  }

  /**
   * Indicate that a batch contains a failed job in the Job Queue.
   * @return true if a failed job exists for the batch
   */
  public boolean hasFailure() {
    return this.hasFailure;
  }

  public String dir() {
    return QueueItem.ZkPaths.Batch.path;
  }
  public String prefix() {
    return QueueItem.ZkPrefixes.Batch.prefix;
  }
  public static String prefixPath() {
    return String.format("%s/%s", QueueItem.ZkPaths.Batch.path, QueueItem.ZkPrefixes.Batch.prefix);
  };
  public static IngestState initStatus() {
    return BatchState.Pending;
  }

  public void loadHasFailure(ZooKeeper client) throws KeeperException, InterruptedException {
    this.hasFailure = false;
    String p = String.format("%s/states/%s", path(), BatchJobStates.Failed.path);
    if (QueueItemHelper.exists(client, p)) {
      if (!client.getChildren(p, false).isEmpty()) {
        this.hasFailure = true;
      }
    }
  }

  public IngestState resolveStatus(String s){
    return BatchState.valueOf(s);
  }

  public String batchUuid() {
    return jsonStringProperty(data(), MerrittJsonKey.BatchId, "");
  }

  @Override
  public void loadProperties(ZooKeeper client) throws KeeperException, InterruptedException, MerrittZKNodeInvalid {
    data = optJsonProperty(client, ZKKey.BATCH_SUBMISSION);
    loadHasFailure(client);
  }

  public IngestState[] states() {
    return BatchState.values();
  }

  public static String batchUuidPath(String uuid) {
    return String.format("%s/%s", ZkPaths.BatchUuids.path, uuid);
  }

  public static Batch createBatch(ZooKeeper client, JSONObject submission) throws KeeperException, InterruptedException, MerrittZKNodeInvalid {
    String id = QueueItemHelper.createId(client, Batch.prefixPath());
    Batch batch = new Batch(id, submission);
    String uuid = jsonStringProperty(submission, MerrittJsonKey.BatchId, "");
    if (!uuid.isEmpty()) {
      QueueItemHelper.create(client, batchUuidPath(uuid), batch.id().getBytes());
    }
    batch.createData(client, ZKKey.BATCH_SUBMISSION, submission);
    batch.setStatus(client, Batch.initStatus()); 
    return batch;
  }

  public void delete(ZooKeeper client) throws MerrittStateError, MerrittZKNodeInvalid, KeeperException, InterruptedException {
    String[] dirs = {BatchJobStates.Processing.path, BatchJobStates.Failed.path, BatchJobStates.Completed.path};
    if (!this.status().isDeletable()) {
      throw new MerrittStateError(String.format("Delete invalid for %s", path()));
    }
    for(String state: dirs) {
      String p = String.format("%s/states/%s", path(), state);
      if (QueueItemHelper.exists(client, p)) {
        for(String cp: client.getChildren(p, false)) {
          new Job(cp, id()).load(client).delete(client);
        }
      }
    }

    if (!batchUuid().isEmpty()) {
      QueueItemHelper.delete(client, batchUuidPath(batchUuid()));
    }

    QueueItemHelper.deleteAll(client, path());
  }

  public static Batch acquirePendingBatch(ZooKeeper client) throws MerrittZKNodeInvalid, KeeperException, InterruptedException {
    List<String> batches = client.getChildren(QueueItem.ZkPaths.Batch.path, false);
    batches.sort(String::compareTo);
    for(String cp: batches) {
      String p = String.format("%s/%s/states", QueueItem.ZkPaths.Batch.path, cp);
      if (!QueueItemHelper.exists(client, p)) {
        Batch b = new Batch(cp);
        if (b.lock(client)) {
          b.load(client);
          b.createData(client, ZKKey.STATES, null);
          return b;
        }
      }
    }  
    return null;
  }
  public static Batch acquireCompletedBatch(ZooKeeper client) throws MerrittZKNodeInvalid, KeeperException, InterruptedException {
    List<String> batches = client.getChildren(QueueItem.ZkPaths.Batch.path, false);
    batches.sort(String::compareTo);
    for(String cp: batches) {
      String p = String.format("%s/%s/states/%s", QueueItem.ZkPaths.Batch.path, cp, BatchJobStates.Processing.path);
      if (QueueItemHelper.exists(client, p)) {
        if (client.getChildren(p, false).isEmpty()) {
          Batch b = new Batch(cp);
          if (b.lock(client)) {
            b.load(client);
            b.setStatus(client, BatchState.Reporting);
            return b;
          }
        }
      }
    }  
    return null;
  }  

  public List<Job> getProcessingJobs(ZooKeeper client) throws KeeperException, InterruptedException {
    return getJobs(client, BatchJobStates.Processing);
  }

  public List<Job> getCompletedJobs(ZooKeeper client) throws KeeperException, InterruptedException {
    return getJobs(client, BatchJobStates.Completed);
  }
  public List<Job> getFailedJobs(ZooKeeper client) throws KeeperException, InterruptedException {
    return getJobs(client, BatchJobStates.Failed);
  }
  public List<Job> getJobs(ZooKeeper client, BatchJobStates state) throws KeeperException, InterruptedException {
    ArrayList<Job> jobs = new ArrayList<>();
    String p = String.format("%s/states/%s", path(), state.path);
    if (QueueItemHelper.exists(client, p)) {
      for(String cp: client.getChildren(p, false)) {
        jobs.add(new Job(cp, id()));
      }
    }
    System.out.println("TBTBTB "+ state.path + " " + jobs.size());
    return jobs;
  }

  public static Batch findByUuid(ZooKeeper client, String uuid) throws KeeperException, InterruptedException, MerrittZKNodeInvalid {
    if (uuid.isEmpty()) {
      return null;
    }
    String p = batchUuidPath(uuid);
    if (!QueueItemHelper.exists(client, p)) {
      return null;
    }
    String bid = QueueItemHelper.pathToString(client, p);
    Batch b = new Batch(bid);
    b.load(client);
    return b;
  }

  public static void main(String[] argv){
    System.out.println("Batch States");
  }
}
