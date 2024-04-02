package org.cdlib.mrt.zk;

/**
 * Exception thown when attempting to modify the data payload for a ZooKeeper node. 
 * This exception is thrown when modifying a node that does not exist or modifying a node with an incompatible data type.
 */
public class MerrittZKNodeInvalid extends Exception {
  public MerrittZKNodeInvalid(String message) {
    super(message);
  }
}
