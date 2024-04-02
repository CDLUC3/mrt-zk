package org.cdlib.mrt.zk;

import java.util.Arrays;
import java.util.List;

/**
 * <h2>Job State Transitions</h2>
 * @see <a href="https://github.com/CDLUC3/mrt-doc/blob/main/design/queue-2023/states.md">State Transition Design</a>
 * @see <a href="https://github.com/CDLUC3/merritt-tinker/blob/main/state-transition/states.yml">State Definition Yaml</a>
 */
public enum JobState implements IngestState {
  /**
   * Job is waiting to be acquired by the queue
   */
  Pending {
    public List<IngestState> nextStates() {
      return Arrays.asList(JobState.Held, JobState.Estimating);
    }
  },
  /**
   * Since Job was queued, the collection has been put into a HELD state. The job will require an administrateive action to release it after the hold is released.
   */
  Held {
    public List<IngestState> nextStates() {
      return Arrays.asList(JobState.Pending, JobState.Deleted);
    }
  },
  /**
   * Perform HEAD requests for all content to estimate the size of the ingest. 
   * If HEAD requests are not supported, no value is updated and the job should proceed with limited information. 
   * This could potentially affect priority for the job
   */
  Estimating {
    public List<IngestState> nextStates() {
      return Arrays.asList(JobState.Provisioning);
    }
    public JobState success() {
      return JobState.Provisioning;
    }
  },
  /**
   * Once dynamic provisioning is implemented (ie zfs provisioning), wait for dedicated file system to be provisioned.
   * 
   * If not dedicated file system is specified, use default working storage.
   * 
   * if working storage is more than 80% full, then wait
   * 
   * otherwise, use default working storage
   */
  Provisioning {
    public List<IngestState> nextStates() {
      return Arrays.asList(JobState.Downloading);
    }
    public JobState success() {
      return JobState.Downloading;
    }
  },
  /**
   * One or more downloads is in progress. This can be a multi-threaded step. Threads are not managed in the queue.
   */
  Downloading {
    public List<IngestState> nextStates() {
      return Arrays.asList(JobState.Failed, JobState.Processing);
    }
    public JobState success() {
      return JobState.Processing;
    }
    public JobState fail() {
      return JobState.Failed;
    }
  },
  /**
   * All downloads complete; perform Merritt Ingest (validate checksum, mint, create system files, notify storage)
   */
  Processing {
    public List<IngestState> nextStates() {
      return Arrays.asList(JobState.Failed, JobState.Recording);
    }
    public JobState success() {
      return JobState.Recording;
    }
    public JobState fail() {
      return JobState.Failed;
    }
  },
  /**
   * Storage is complete; ready for Inventory. The Inventory service will operate on this step.
   */
  Recording {
    public List<IngestState> nextStates() {
      return Arrays.asList(JobState.Failed, JobState.Notify);
    }
    public JobState success() {
      return JobState.Notify;
    }
    public JobState fail() {
      return JobState.Failed;
    }
  },
  /**
   * Invoke callback if needed Notify batch handler that the job is complete
   */
  Notify {
    public List<IngestState> nextStates() {
      return Arrays.asList(JobState.Failed, JobState.Completed);
    }
    public JobState success() {
      return JobState.Completed;
    }
    public JobState fail() {
      return JobState.Failed;
    }
  },
  /**
   * The queue will track the last successful step so that the job can be resumed at the appropriate step.
   */
  Failed {
    public List<IngestState> nextStates() {
      return Arrays.asList(
        JobState.Deleted, JobState.Downloading, JobState.Processing, JobState.Recording, JobState.Notify
      );
    }
  },
  /**
   * Storage and inventory are complete, cleanup job folder
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
