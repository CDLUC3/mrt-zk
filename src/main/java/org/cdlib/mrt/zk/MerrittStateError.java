package org.cdlib.mrt.zk;

/**
 * Exception indicates that an illegal operation was invoked on an object with an incompatible state (ie attempting to delete a Job or Batch that is not in a deleteable state)
 */ 
public class MerrittStateError extends Exception {
  public MerrittStateError(String message) {
    super(message);
  }
}
