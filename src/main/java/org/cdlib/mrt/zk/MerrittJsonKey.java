package org.cdlib.mrt.zk;

/**
 * Lookup key names for properties stored as JSON within ZooKeeper nodes.
 * @see <a href="https://github.com/CDLUC3/mrt-doc/blob/main/design/queue-2023/data.md">ZooKeeper Node Design</a>
 */
public enum MerrittJsonKey {
  Status("status"),
  LastModified("last_modified"),
  RetryCount("retry_count"),
  LastSuccessfulStatus("last_successful_status");
  private String key;
  MerrittJsonKey(String s) {
    this.key = s;
  }
  public String key() {
    return this.key;
  }

}
