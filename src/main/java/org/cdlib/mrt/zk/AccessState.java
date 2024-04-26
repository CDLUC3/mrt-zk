package org.cdlib.mrt.zk;

import java.util.Arrays;
import java.util.List;

/**
 * <h2>Access Assembly State Transitions</h2>
 * @see <a href="https://github.com/CDLUC3/mrt-zk/blob/main/design/states.md">State Transition Design</a>
 * @see <a href="https://github.com/CDLUC3/merritt-tinker/blob/main/state-transition/states.yml">State Definition Yaml</a>
 */
public enum AccessState implements IngestState{
  /** 
   * Assembly is ready to be processed
   */
  Pending {
    public List<IngestState> nextStates() {
      return Arrays.asList(AccessState.Processing);
    }
  },
  /**
   * Payload is analyzed. If the payload is a manifest, it will be downloaded. Jobs are created in the job queue.
   */
  Processing {
    public List<IngestState> nextStates() {
      return Arrays.asList(AccessState.Completed, AccessState.Failed);
    }
    public IngestState success() {
      return AccessState.Completed;
    }
    public IngestState fail() {
      return AccessState.Failed;
    }
  },
  /**
   * At least one job FAILED
   */
  Failed{
    public List<IngestState> nextStates() {
      return Arrays.asList(AccessState.Processing, AccessState.Deleted);
    }
  },
  /**
   * All jobs COMPLETED
   */
  Completed,
  Deleted;

  public List<IngestState> nextStates() {
    return Arrays.asList();
  };
  public IngestState stateChange(IngestState next) {
    if (stateChangeAllowed(next)){
      return next;
    }
    return null;
  }
}
