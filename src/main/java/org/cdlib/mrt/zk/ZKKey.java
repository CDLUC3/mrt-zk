package org.cdlib.mrt.zk;

/**
 * Defines relative pathnames to ZooKeeper nodes for a Batch or a Job.
 * @see <a href="https://github.com/CDLUC3/mrt-zk/blob/main/design/data.md">ZooKeeper Node Design</a>
 */
public enum ZKKey {
  /**
   * JSON node uses to store current state for a Job or a Batch
   * 
   * <pre>
   * status: Pending
   * last_modified: now
   * </pre>
   * 
   */
  STATUS("status"),
  /**
   * Empty Ephemeral node that indicates that a Batch or Job has been locked by a consumer daemon
   */
  LOCK("lock"),
  /**
   * Parent node of state-specific nodes for a Batch or for a Job.  
   * 
   * The state-specific nodes will contain nodes that reference job ids.
   */
  STATES("states"),
  /**
   * Read-only JSON node containing the parameters that initiated a submission.
   */
  BATCH_SUBMISSION("submission"),
  /**
   * JSON node summarizing a report sent to the depositor detaining completed and failed jobs for a batch.
   */
  BATCH_STATUS_REPORT("status-report"),
  /** 
   * Read-only JSON node containing the parameters for a specific job.
   */
  JOB_CONFIGURATION("configuration"),
  /**
   * JSON node containing the primary id and local id for a job.
   */
  JOB_IDENTIFIERS("identifiers"),
  /**
   * Integer node containing the priority assigned to the job.
   */
  JOB_PRIORITY("priority"),
  /** 
   * Long node containing the bytes of cloud storage to be used by the job.  
   * This value should be set to 0 if the value is unknown.
   */
  JOB_SPACE_NEEDED("space_needed"),
  /**
   * String node containing the batch id for the job
   */
  JOB_BID("bid");

  private String key;

  ZKKey(String s) {
    key = s;
  }

  public String key() {
    return key;
  }
}