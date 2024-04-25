package org.cdlib.mrt.zk;

import java.nio.file.Paths;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

/*
 * | /locks/queue/ingest | none | - | Admin | Admin | Previously file-system based|
| /locks/queue/accessSmall | none | - | Admin | Admin | |
| /locks/queue/accessLarge | none | - | Admin | Admin | |
| /locks/storage/{ark} | none | - | Ingest | Ingest | current path: ? |
| /locks/inventory/{ark} | none | - | Inventory | Inventory | current path: ? |
| /locks/collections/{mnemonic} | none | - | Admin | Admin | |
package org.cdlib.mrt.zk;

 */
public class MerrittLocks {
  public static final String LOCKS = "/locks";
  public static final String LOCKS_QUEUE = "/locks/queue";
  public static final String LOCKS_QUEUE_INGEST = "/locks/queue/ingest";
  public static final String LOCKS_QUEUE_ACCESS_SMALL = "/locks/queue/accessSmall";
  public static final String LOCKS_QUEUE_ACCESS_LARGE = "/locks/queue/accessLarge";
  public static final String LOCKS_STORAGE = "/locks/storage";
  public static final String LOCKS_INVENTORY = "/locks/inventory";
  public static final String LOCKS_COLLECTIONS = "/locks/collections";

  private static void createIfNeeded(ZooKeeper client, String path) throws KeeperException, InterruptedException {
    if (!QueueItemHelper.exists(client, path)) {
      QueueItemHelper.create(client, path, QueueItemHelper.empty);
    }
  }

  public static void initLocks(ZooKeeper client) throws KeeperException, InterruptedException {
    createIfNeeded(client, LOCKS);
    createIfNeeded(client, LOCKS_QUEUE);
    createIfNeeded(client, LOCKS_STORAGE);
    createIfNeeded(client, LOCKS_INVENTORY);
    createIfNeeded(client, LOCKS_COLLECTIONS);
  }

  private static boolean createLock(ZooKeeper client, String path) {
    try {
      QueueItemHelper.create(client, path, QueueItemHelper.empty);
      return true;
    } catch(KeeperException|InterruptedException e) {
      return false;
    }
  }

  private static boolean createEphemeralLock(ZooKeeper client, String path) {
    try {
      QueueItemHelper.createEphemeral(client, path, QueueItemHelper.empty);
      return true;
    } catch(KeeperException|InterruptedException e) {
      return false;
    }
  }

  public static boolean lockIngestQueue(ZooKeeper client) {
    return createLock(client, LOCKS_QUEUE_INGEST);
  }
  public static void unlockIngestQueue(ZooKeeper client) throws InterruptedException, KeeperException {
    QueueItemHelper.delete(client, LOCKS_QUEUE_INGEST);
  }

  public static boolean lockLargeAccessQueue(ZooKeeper client) {
    return createLock(client, LOCKS_QUEUE_ACCESS_LARGE);
  }
  public static void unlockLargeAccessQueue(ZooKeeper client) throws InterruptedException, KeeperException {
    QueueItemHelper.delete(client, LOCKS_QUEUE_ACCESS_LARGE);
  }

  public static boolean lockSmallAccessQueue(ZooKeeper client) {
    return createLock(client, LOCKS_QUEUE_ACCESS_SMALL);
  }
  public static void unlockSmallAccessQueue(ZooKeeper client) throws InterruptedException, KeeperException {
    QueueItemHelper.delete(client, LOCKS_QUEUE_ACCESS_SMALL);
  }

  public static boolean lockCollection(ZooKeeper client, String mnemonic) {
    return createLock(client, Paths.get(LOCKS_COLLECTIONS, mnemonic).toString());
  }

  public static void unlockCollection(ZooKeeper client, String mnemonic) throws InterruptedException, KeeperException {
    QueueItemHelper.delete(client, Paths.get(LOCKS_COLLECTIONS, mnemonic).toString());
  }

  public static boolean lockObjectStorage(ZooKeeper client, String ark) {
    return createEphemeralLock(client, Paths.get(LOCKS_STORAGE, ark.replaceAll("\\/", "_")).toString());
  }
  public static void unlockObjectStorage(ZooKeeper client, String ark) throws InterruptedException, KeeperException {
    QueueItemHelper.delete(client, Paths.get(LOCKS_STORAGE, ark.replaceAll("\\/", "_")).toString());
  }
  public static boolean lockObjectInventory(ZooKeeper client, String ark) {
    return createEphemeralLock(client, Paths.get(LOCKS_INVENTORY, ark.replaceAll("\\/", "_")).toString());
  }
  public static void unlockObjectInventory(ZooKeeper client, String ark) throws InterruptedException, KeeperException {
    QueueItemHelper.delete(client, Paths.get(LOCKS_INVENTORY, ark.replaceAll("\\/", "_")).toString());
  }
}
