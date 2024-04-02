package org.cdlib.mrt.zk;

import org.json.JSONException;
import org.json.JSONObject;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

/**
 * Base Class for Common functions for Merritt Ingest Batches and Merritt Ingest Jobs.
 */
abstract public class QueueItem {
  private String id;
  private JSONObject data;
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

  public JSONObject statusObject(IngestState status) {
    JSONObject jobj = new JSONObject();
    jobj.put(MerrittJsonKey.Status.key(), status.name());
    jobj.put(MerrittJsonKey.LastModified.key(), QueueItemHelper.now());
    return jobj;
  }

  public void setStatus(ZooKeeper client, IngestState status) throws KeeperException, InterruptedException, MerrittZKNodeInvalid {
    if (status == this.status) {
      return;
    }
    String statpath = makePath(ZKKey.STATUS);
    byte[] data = QueueItemHelper.asBytes(QueueItemHelper.serialize(statusObject(status)));
    if (this.status == null) {
      QueueItemHelper.create(client, statpath, data);
    } else {
      QueueItemHelper.setData(client, statpath, data);
    }
    this.status = status;
    setStatusTrigger(client);
  }

  public void setStatusTrigger(ZooKeeper client) throws MerrittZKNodeInvalid, KeeperException, InterruptedException {
  }

  public boolean lock(ZooKeeper client) throws KeeperException, InterruptedException {
    String statpath = makePath(ZKKey.LOCK);
    QueueItemHelper.createEphemeral(client, statpath, QueueItemHelper.empty);
    return true;
  }

  public boolean unlock(ZooKeeper client) throws InterruptedException, KeeperException {
    String statpath = makePath(ZKKey.LOCK);
    QueueItemHelper.delete(client, statpath);
    return true;
  }


  public abstract void delete(ZooKeeper client) throws MerrittStateError, MerrittZKNodeInvalid, InterruptedException, KeeperException;
}
