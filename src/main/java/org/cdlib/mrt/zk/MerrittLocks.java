package org.cdlib.mrt.zk;

import java.nio.file.Paths;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

/**
 * Static methods to set and release Merritt Locks.
 * Object-level locks will be ephemeral.
 * Queue and collection level locks will be persistent.
 */
public class MerrittLocks {

  public static void initLocks(ZooKeeper client) throws KeeperException, InterruptedException {
    QueueItemHelper.createIfNeeded(client, QueueItem.ZkPaths.Locks.path);
    QueueItemHelper.createIfNeeded(client, QueueItem.ZkPaths.LocksQueue.path);
    QueueItemHelper.createIfNeeded(client, QueueItem.ZkPaths.LocksLocalID.path);
    QueueItemHelper.createIfNeeded(client, QueueItem.ZkPaths.LocksStorage.path);
    QueueItemHelper.createIfNeeded(client, QueueItem.ZkPaths.LocksInventory.path);
    QueueItemHelper.createIfNeeded(client, QueueItem.ZkPaths.LocksCollections.path);
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
    return createLock(client, QueueItem.ZkPaths.LocksQueueIngest.path);
  }
  public static void unlockIngestQueue(ZooKeeper client) throws InterruptedException, KeeperException {
    QueueItemHelper.delete(client, QueueItem.ZkPaths.LocksQueueIngest.path);
  }
  public static boolean checkLockIngestQueue(ZooKeeper client) throws KeeperException, InterruptedException {
    return QueueItemHelper.exists(client, QueueItem.ZkPaths.LocksQueueIngest.path);
  }

  public static boolean lockLargeAccessQueue(ZooKeeper client) {
    return createLock(client, QueueItem.ZkPaths.LocksQueueAccessLarge.path);
  }
  public static void unlockLargeAccessQueue(ZooKeeper client) throws InterruptedException, KeeperException {
    QueueItemHelper.delete(client, QueueItem.ZkPaths.LocksQueueAccessLarge.path);
  }
  public static boolean checkLockLargeAccessQueue(ZooKeeper client) throws KeeperException, InterruptedException {
    return QueueItemHelper.exists(client, QueueItem.ZkPaths.LocksQueueAccessLarge.path);
  }

  public static boolean lockSmallAccessQueue(ZooKeeper client) {
    return createLock(client, QueueItem.ZkPaths.LocksQueueAccessSmall.path);
  }
  public static void unlockSmallAccessQueue(ZooKeeper client) throws InterruptedException, KeeperException {
    QueueItemHelper.delete(client, QueueItem.ZkPaths.LocksQueueAccessSmall.path);
  }
  public static boolean checkLockSmallAccessQueue(ZooKeeper client) throws KeeperException, InterruptedException {
    return QueueItemHelper.exists(client, QueueItem.ZkPaths.LocksQueueAccessSmall.path);
  }

  public static boolean lockCollection(ZooKeeper client, String mnemonic) {
    return createLock(client, Paths.get(QueueItem.ZkPaths.LocksCollections.path, mnemonic).toString());
  }

  public static void unlockCollection(ZooKeeper client, String mnemonic) throws InterruptedException, KeeperException {
    QueueItemHelper.delete(client, Paths.get(QueueItem.ZkPaths.LocksCollections.path, mnemonic).toString());
  }
  public static boolean checkLockCollection(ZooKeeper client, String mnemonic) throws KeeperException, InterruptedException {
    return QueueItemHelper.exists(client, Paths.get(QueueItem.ZkPaths.LocksCollections.path, mnemonic).toString());
  }

  public static boolean lockObjectLocalID(ZooKeeper client, String lid) {
    return createEphemeralLock(client, Paths.get(QueueItem.ZkPaths.LocksLocalID.path, lid.replaceAll(":?/", "_")).toString());
  }
  public static void unlockObjectLocalID(ZooKeeper client, String lid) throws InterruptedException, KeeperException {
    QueueItemHelper.delete(client, Paths.get(QueueItem.ZkPaths.LocksLocalID.path, lid.replaceAll(":?/", "_")).toString());
  }
  public static boolean checkLockObjectLocalID(ZooKeeper client, String lid) throws KeeperException, InterruptedException {
    return QueueItemHelper.exists(client, Paths.get(QueueItem.ZkPaths.LocksLocalID.path, lid.replaceAll(":?/", "_")).toString());
  }

  public static boolean lockObjectStorage(ZooKeeper client, String ark) {
    return createLock(client, Paths.get(QueueItem.ZkPaths.LocksStorage.path, ark.replaceAll(":?/", "_")).toString());
  }
  public static void unlockObjectStorage(ZooKeeper client, String ark) throws InterruptedException, KeeperException {
    QueueItemHelper.delete(client, Paths.get(QueueItem.ZkPaths.LocksStorage.path, ark.replaceAll(":?/", "_")).toString());
  }
  public static boolean checkLockObjectStorage(ZooKeeper client, String ark) throws KeeperException, InterruptedException {
    return QueueItemHelper.exists(client, Paths.get(QueueItem.ZkPaths.LocksStorage.path, ark.replaceAll(":?/", "_")).toString());
  }

  public static boolean lockObjectInventory(ZooKeeper client, String ark) {
    return createEphemeralLock(client, Paths.get(QueueItem.ZkPaths.LocksInventory.path, ark.replaceAll(":?/", "_")).toString());
  }
  public static void unlockObjectInventory(ZooKeeper client, String ark) throws InterruptedException, KeeperException {
    QueueItemHelper.delete(client, Paths.get(QueueItem.ZkPaths.LocksInventory.path, ark.replaceAll(":?/", "_")).toString());
  }
  public static boolean checkLockObjectInventory(ZooKeeper client, String ark) throws KeeperException, InterruptedException {
    return QueueItemHelper.exists(client, Paths.get(QueueItem.ZkPaths.LocksInventory.path, ark.replaceAll(":?/", "_")).toString());
  }
}
