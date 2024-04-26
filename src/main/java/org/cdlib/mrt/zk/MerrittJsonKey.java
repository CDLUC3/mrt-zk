package org.cdlib.mrt.zk;

/**
 * Lookup key names for properties stored as JSON within ZooKeeper nodes.
 * @see <a href="https://github.com/CDLUC3/mrt-zk/blob/main/design/data.md">ZooKeeper Node Design</a>
 */
public enum MerrittJsonKey {
  Status("status"),
  LastModified("last_modified"),
  RetryCount("retry_count"),
  LastSuccessfulStatus("last_successful_status"),
  ErcWho("erc_who"),
  ErcWhat("erc_what"),
  ErcWhere("erc_where"),
  ErcWhen("erc_when"),
  PrimaryId("primary_id"),
  LocalId("local_id"),
  ProfileName("profile_name"),
  Submitter("submitter"),
  BatchPayloadFileName("payload_file_name"),
  BatchType("batch_type"),
  SubmissionMode("submission_mode"),
  JobPayloadUrl("payload_url"),
  JobPayloadType("payload_type"),
  JobResponseType("response_type"),
  AccessToken("token"),
  AccessDeliveryNode("delivery-node"),
  AccessCloudContentByte("cloud-content-byte"),
  AccessTokenStatus("status"),
  AccessUrl("url"),
  AccessAnticipatedAvailabilityTime("anticipated-availability-time");

  private String key;
  MerrittJsonKey(String s) {
    this.key = s;
  }
  public String key() {
    return this.key;
  }

}
