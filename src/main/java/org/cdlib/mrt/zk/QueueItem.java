package org.cdlib.mrt.zk;

import org.json.JSONException;
import org.json.JSONObject;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

/**
 * Base Class for Common functions for Merritt Ingest Batches and Merritt Ingest Jobs.
 */
abstract public class QueueItem {
  /**
   * Standardized path names for Merritt Zookeeper nodes
   */
  public static enum ZkPaths {
    Access("/access"),
    AccessSmall("/access/small"),
    AccessLarge("/access/large"),
    Batch("/batches"),
    BatchUuids("/batch-uuids"),
    Job("/jobs"),
    JobStates("/jobs/states"),
    Locks("/locks"),
    LocksQueue("/locks/queue"),
    LocksQueueIngest("/locks/queue/ingest"),
    LocksQueueAccessSmall("/locks/queue/accessSmall"),
    LocksQueueAccessLarge("/locks/queue/accessLarge"),
    LocksLocalID("/locks/localid"),
    LocksStorage("/locks/storage"),
    LocksInventory("/locks/inventory"),
    LocksCollections("/locks/collections");
    public String path;
    ZkPaths(String path) {
      this.path = path;
    }
  }

    /**
   * Standardized path names for Merritt Zookeeper nodes
   */
  public static enum BatchJobStates {
    Processing("batch-processing"),
    Completed("batch-completed"),
    Deleted("batch-deleted"),
    Failed("batch-failed");
    public String path;
    BatchJobStates(String path) {
      this.path = path;
    }
  }
  /**
   * Standardized prefix names for Merritt Zookeeper sequential nodes
   */
  public static enum ZkPrefixes {
    Access("qid"),
    Batch("bid"),
    Job("jid");
    public String prefix;
    ZkPrefixes(String prefix) {
      this.prefix = prefix;
    }
  }

  private String id;
  protected JSONObject data;
  private IngestState status;

  public QueueItem(String id){
    this(id, (JSONObject)null);
  }

  public QueueItem(String id, JSONObject data) {
    this.id = id;
    this.data = data;
  }

  public String id() {
    return id;
  }

  public JSONObject data() {
    return data;
  }

  public static Object jsonDataProperty(JSONObject obj, MerrittJsonKey key, Object dval) {
    String k = key.key();
    if (obj == null) {
      return dval;
    } else if (obj.has(k)) {
      return obj.get(k);
    } else {
      return dval;
    }
  }
  public static String jsonStringProperty(JSONObject obj, MerrittJsonKey key, String dval) {
    String k = key.key();
    if (obj == null) {
      return dval;
    } else if (obj.has(k)) {
      return obj.getString(k);
    } else {
      return dval;
    }
  }

  public static int jsonIntProperty(JSONObject obj, MerrittJsonKey key, int dval) {
    String k = key.key();
    if (obj == null) {
      return dval;
    } else if (obj.has(k)) {
      return obj.getInt(k);
    } else {
      return dval;
    }
  }

  public static long jsonLongProperty(JSONObject obj, MerrittJsonKey key, long dval) {
    String k = key.key();
    if (obj == null) {
      return dval;
    } else if (obj.has(k)) {
      return obj.getLong(k);
    } else {
      return dval;
    }
  }

  abstract public IngestState[] states();
  public IngestState status() {
    return this.status;
  }

  abstract public String dir();
  abstract public String prefix();
  public String path() {
    return String.format("%s/%s", dir(), id());
  };

  public String makePath(ZKKey key) {
    if (key == ZKKey.ROOT) {
      return path();
    }
    return path() + "/" + key.key();
  }

  public QueueItem load(ZooKeeper client) throws MerrittZKNodeInvalid, KeeperException, InterruptedException {
    if (!QueueItemHelper.exists(client, path())) {
      throw new MerrittZKNodeInvalid(String.format("Missing Node %s", path()));
    }
    loadStatus(client, jsonProperty(client, ZKKey.STATUS));
    loadProperties(client);
    return this;
  }

  public void loadStatus(ZooKeeper client, JSONObject js) throws MerrittZKNodeInvalid {
    String s = js.getString(MerrittJsonKey.Status.key());
    this.status = resolveStatus(s);
  }

  public abstract IngestState resolveStatus(String s);

  public void loadProperties(ZooKeeper client) throws MerrittZKNodeInvalid, KeeperException, InterruptedException {
  }

  public String optStringProperty(ZooKeeper client, ZKKey key) throws KeeperException, InterruptedException {
    String p = makePath(key);
    if (!QueueItemHelper.exists(client, p)) {
      return "";
    }
    return QueueItemHelper.pathToString(client, p);
  }

  public String stringProperty(ZooKeeper client, ZKKey key) throws MerrittZKNodeInvalid, KeeperException, InterruptedException {
    String p = makePath(key);
    if (!QueueItemHelper.exists(client, p)) {
      throw new MerrittZKNodeInvalid("Path not found for " + p);
    }
    return QueueItemHelper.pathToString(client, p);
  }

  public JSONObject jsonProperty(ZooKeeper client, ZKKey key) throws MerrittZKNodeInvalid, KeeperException, InterruptedException {
    try {
      return new JSONObject(stringProperty(client, key));
    } catch(JSONException e) {
      throw new MerrittZKNodeInvalid("Improperly formatted object for " + makePath(key));
    }
  }

  public JSONObject optJsonProperty(ZooKeeper client, ZKKey key) throws MerrittZKNodeInvalid, KeeperException, InterruptedException {
    try {
      String s = optStringProperty(client, key);
      if (s.isEmpty()) {
        return new JSONObject();
      }
      return new JSONObject(s);
    } catch(JSONException e) {
      throw new MerrittZKNodeInvalid("Improperly formatted object for " + makePath(key));
    }
  }

  public int intProperty(ZooKeeper client, ZKKey key) throws MerrittZKNodeInvalid, KeeperException, InterruptedException{
    try {
      return Integer.parseInt(stringProperty(client, key));
    } catch(NumberFormatException e) {
      throw new MerrittZKNodeInvalid("Improperly formatted number for " + makePath(key));
    }
  }

  public long longProperty(ZooKeeper client, ZKKey key) throws MerrittZKNodeInvalid, KeeperException, InterruptedException{
    try {
      return Long.parseLong(stringProperty(client, key));
    } catch(NumberFormatException e) {
      throw new MerrittZKNodeInvalid("Improperly formatted number for " + makePath(key));
    }
  }

  public void setData(ZooKeeper client, ZKKey key, Object data) throws KeeperException, InterruptedException {
    String p = makePath(key);
    QueueItemHelper.setData(client, p, QueueItemHelper.serializeAsBytes(data));
  }

  public void createData(ZooKeeper client, ZKKey key, Object data) throws KeeperException, InterruptedException {
    String p = makePath(key);
    QueueItemHelper.create(client, p, QueueItemHelper.serializeAsBytes(data));
  }

  public void createOrSetData(ZooKeeper client, ZKKey key, Object data) throws KeeperException, InterruptedException {
    String p = makePath(key);
    if (QueueItemHelper.exists(client, p)) {
      QueueItemHelper.setData(client, p, QueueItemHelper.serializeAsBytes(data));
    } else {
      QueueItemHelper.create(client, p, QueueItemHelper.serializeAsBytes(data));
    }
  }

  public JSONObject statusObject(JSONObject statj, IngestState status) {
    JSONObject jobj = statj == null ? new JSONObject() : new JSONObject(statj.toString());
    jobj.put(MerrittJsonKey.Status.key(), status.name());
    jobj.put(MerrittJsonKey.LastModified.key(), QueueItemHelper.now());
    return jobj;
  }

  public void setStatus(ZooKeeper client, IngestState status) throws KeeperException, InterruptedException, MerrittZKNodeInvalid, MerrittStateError {
    setStatus(client, status, "");
  }

  public void setStatus(ZooKeeper client, IngestState status, String message) throws KeeperException, InterruptedException, MerrittZKNodeInvalid, MerrittStateError {
    if (status == null) {
      throw new MerrittStateError("Status cannot be set to null");
    }
    if (status != this.status) {
      if (this.status != null) {
        if (this.status.stateChange(status) == null) {
          throw new MerrittStateError(String.format("State change (%s->%s) is not allowed", this.status.name(), status.name()));
        }  
      }
      String statpath = makePath(ZKKey.STATUS);
      JSONObject oldjson = QueueItemHelper.exists(client, statpath) ? jsonProperty(client, ZKKey.STATUS) : new JSONObject();
      JSONObject json = statusObject(oldjson, status);
      if (message == null) {
        message = "";
      }
      json.put(MerrittJsonKey.Message.key(), message);

      byte[] data = QueueItemHelper.asBytes(QueueItemHelper.serialize(json));
      if (this.status == null) {
        QueueItemHelper.create(client, statpath, data);
      } else {
        QueueItemHelper.setData(client, statpath, data);
      }
      this.status = status;
    }
    setStatusTrigger(client);
  }

  public void setStatusTrigger(ZooKeeper client) throws MerrittZKNodeInvalid, KeeperException, InterruptedException {
  }

  public boolean lock(ZooKeeper client) throws InterruptedException {
    String statpath = makePath(ZKKey.LOCK);
    try {
      if (client.exists(statpath, null) != null) return false;
      if (QueueItemHelper.createEphemeral(client, statpath, QueueItemHelper.empty).equals(statpath)) 
      	return true;  
      else 
	return false;
    } catch(KeeperException e) {
      return false;
    }
  }

  public boolean unlock(ZooKeeper client) throws InterruptedException, KeeperException {
    String statpath = makePath(ZKKey.LOCK);
    QueueItemHelper.delete(client, statpath);
    return true;
  }


  public abstract void delete(ZooKeeper client) throws MerrittStateError, MerrittZKNodeInvalid, InterruptedException, KeeperException;
}
