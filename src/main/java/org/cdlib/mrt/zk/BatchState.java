package org.cdlib.mrt.zk;

import java.util.Arrays;
import java.util.List;

/**
 * <h2>Batch State Transitions</h2>
 * @see <a href="https://github.com/CDLUC3/mrt-doc/blob/main/design/queue-2023/states.md">State Transition Design</a>
 * @see <a href="https://github.com/CDLUC3/merritt-tinker/blob/main/state-transition/states.yml">State Definition Yaml</a>
 */
public enum BatchState implements IngestState{
  /** 
   * Batch is ready to be processed
   */
  Pending {
    public List<IngestState> nextStates() {
      return Arrays.asList(BatchState.Held, BatchState.Processing);
    }
  },
  /**
   * Collection is HELD. The hold must be released before the batch can proceed.
   */
  Held {
    public List<IngestState> nextStates() {
      return Arrays.asList(BatchState.Pending, BatchState.Deleted);
    }
  },
  /**
   * Payload is analyzed. If the payload is a manifest, it will be downloaded. Jobs are created in the job queue.
   */
  Processing {
    public List<IngestState> nextStates() {
      return Arrays.asList(BatchState.Reporting);
    }
    public IngestState success() {
      return BatchState.Reporting;
    }
    public IngestState fail() {
      return BatchState.Reporting;
    }
  },
  /**
   * All jobs have COMPLETED or FAILED, a summary e-mail is sent to the depositor.
   */
  Reporting {
    public List<IngestState> nextStates() {
      return Arrays.asList(BatchState.Failed, BatchState.Completed);
    }
    public IngestState success() {
      return BatchState.Completed;
    }
    public IngestState fail() {
      return BatchState.Failed;
    }
  },
  /**
   * At least one job FAILED
   */
  Failed{
    public List<IngestState> nextStates() {
      return Arrays.asList(BatchState.UpdateReporting, BatchState.Deleted);
    }
  },
  /**
   * Determine if any previously FAILED jobs are not complete. If so, notify the depositor by email.
   */
  UpdateReporting {
    public List<IngestState> nextStates() {
      return Arrays.asList(BatchState.Failed, BatchState.Completed);
    }
    public IngestState success() {
      return BatchState.Completed;
    }
    public IngestState fail() {
      return BatchState.Failed;
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
