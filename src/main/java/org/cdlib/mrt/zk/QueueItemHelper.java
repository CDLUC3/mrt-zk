package org.cdlib.mrt.zk;

import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;

/**
 * The static methods in this class also provides a simplified interface for common ZooKeeper API calls.
 */
public class QueueItemHelper {
  public static final List<ACL> acl = ZooDefs.Ids.OPEN_ACL_UNSAFE;
  public static final byte[] empty = new byte[0]; 

  public static boolean exists(ZooKeeper client, String p) throws KeeperException, InterruptedException {
    return client.exists(p, false) != null;
  }

  public static String pathToString(ZooKeeper client, String path) throws KeeperException, InterruptedException {
    byte[] b = client.getData(path, false, null);
    if (b == null) {
      return "";
    }
    return new String(b, StandardCharsets.UTF_8);
  }

  public static String serialize(Object data){
    if (data == null) {
      return null;
    }
    String s = data.toString();
    if (s.isEmpty()) {
      return null;
    }
    return s;
  }

  public static byte[] asBytes(String s) {
    if (s != null) {
      if (!s.isEmpty()) {
        return s.getBytes(StandardCharsets.UTF_8);
      }
    }
    return new byte[0];
  }

  public static byte[] serializeAsBytes(Object data){
    return asBytes(serialize(data));
  }

  public static String createId(ZooKeeper client, String prefix) throws KeeperException, InterruptedException {
    String path = QueueItemHelper.createSequential(client, prefix, empty);
    return Paths.get(path).getFileName().toString();
  }

  public static String now() {
    return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss Z").format(new Date());
  }

  public static String create(ZooKeeper client, String path, byte[] data) throws KeeperException, InterruptedException {
    //System.out.println("Creating "+path);
    return client.create(path, data, QueueItemHelper.acl, CreateMode.PERSISTENT);
  }

  public static String createSequential(ZooKeeper client, String path, byte[] data) throws KeeperException, InterruptedException {
    //System.out.println("Creating seq "+path);
    return client.create(path, data, QueueItemHelper.acl, CreateMode.PERSISTENT_SEQUENTIAL);
  }
  public static String createEphemeral(ZooKeeper client, String path, byte[] data) throws KeeperException, InterruptedException {
    //System.out.println("Creating eph "+path);
    return client.create(path, data, QueueItemHelper.acl, CreateMode.EPHEMERAL);
  }

  public static void setData(ZooKeeper client, String path, byte[] data) throws KeeperException, InterruptedException {
    client.setData(path, data, -1);
  }

  public static void delete(ZooKeeper client, String p) throws InterruptedException, KeeperException {
    if (p == null) {
      return;
    }
    if (p.isEmpty()) {
      return;
    }
    if (exists(client, p)){
      client.delete(p, -1);
    }
  }

  public static void deleteAll(ZooKeeper client, String p) throws InterruptedException, KeeperException {
    if (p == null) {
      return;
    }
    if (p.isEmpty()) {
      return;
    }
    if (exists(client, p)){
      for(String cp: client.getChildren(p, false)) {
        deleteAll(client, String.format("%s/%s", p, cp));
      }
      client.delete(p, -1);
    }
  }

  public static void createIfNeeded(ZooKeeper client, String path) throws KeeperException, InterruptedException {
    if (!exists(client, path)) {
      create(client, path, QueueItemHelper.empty);
    }
  }

  public static void createIfNeededForgiving(ZooKeeper client, String path) throws KeeperException, InterruptedException {
    for(int i = 0; i <= 5; i++) {
      try {
        createIfNeeded(client, path);
        return;
      } catch(KeeperException.NodeExistsException e) {
        if (i == 5) {
          throw e;
        }
      }
    }
  }

}
