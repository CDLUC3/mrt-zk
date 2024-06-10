/**
 * <h2>Merritt Queue Design</h2>
 * 
 * The Merritt Ingest Queue will be refactored in 2024 to enable a number of goals.
 * <ul>
 * <li>Match ingest workload to available resources (compute, memory, working storage)</li>
 * <li>dynamically provision resources to match demand</li>
 * <li>dynamically manage running thread count based on processing load</li>
 * <li>Hold jobs based on temporary holds (collection lock, storage node lock, queue hold)</li>
 * <li>Graceful resumption of processing in progress</li>
 * <li>Allow processing to be resumed on any ingest host.  The previous implementation managed state in memory which prevented this capability</li>
 * <li>Accurate notification of ingest completion (including inventory recording)</li>
 * <li>Send accurate summary email on completion of a batch regardless of any interruption that occurred while processing</li>
 * </ul>
 * 
 * <h2>Batch Queue vs Job Queue</h2>
 * The work of the Merritt Ingest Service takes place at a <em>Job</em> level.
 * Merritt Depositors initiate submissions at a <em>Batch</em> level.  
 * The primary function of the <em>Batch Queue</em> is to provide notification to a depositor once all jobs for a batch have completed.
 *  
 * <h2>Use of ZooKeeper</h2>
 * Merritt Utilizes ZooKeeper for the following features
 * <ul>
 *   <li>Creation/validation of distributed (ephemeral node) locks</li>
 *   <li>Creation of unique node names across the distributed node structure (sequential nodes)</li>
 *   <li>Manage data across the distributed node structure to allow any worker to acquire a job/batch (persistent nodes)</li>
 * </ul>
 * 
 * <p>The ZooKeeper documentation advises keeping the payload of shared data relatively small.</p>
 * 
 * <p>The Merritt ZooKeeper design sames read-only data as JSON objects.</p>
 * 
 * <p>More volatile (read/write) fields are saved as Int, Long, String and very small JSON objects.</p>
 * 
 * <h2>Code Examples</h2>
 * 
 * <h3>Create Batch</h3>
 * 
 * <pre>
 * ZooKeeper zk = new ZooKeeper("localhost:8084", 100, null);
 * JSONObject batchSub = new JSONObject("...");
 * Batch batch = Batch.createBatch(zk, batchSub);
 * </pre>
 * 
 * <h3>Consumer Daemon Acquires Batch and Creates Jobs</h3>
 * 
 * <pre>
 * // An ephemeral lock is created when the batch is acquired
 * // The ephemeral lock will be released when the ZooKeeper connection is closed
 * try(ZooKeeper zk = new ZooKeeper("localhost:8084", 100, null)) {
 *   Batch batch = Batch.acquirePendingBatch(zk)
 *   JSONObject jobConfig = Job.createJobConfiguration(...);
 *   Job j = Job.createJob(zk, batch.id(), jobConfig);
 * }
 * </pre>
 *
 * <h3>Consumer Daemon Acquires Pending Job and Moves Job to Estimating</h3>
 * 
 * <pre>
 * try(ZooKeeper zk = new ZooKeeper("localhost:8084", 100, null)) {
 *   Job jj = Job.acquireJob(zk, JobState.Pending);
 *   jj.setStatus(zk, jj.status().stateChange(JobState.Estimating));
 * }
 * </pre>
 * 
 * <h3>Consumer Daemon Acquires Estimating Job and Updates Priority</h3>
 * 
 * <pre>
 * try(ZooKeeper zk = new ZooKeeper("localhost:8084", 100, null)) {
 *   Job jj = Job.acquireJob(zk, JobState.Estimating);
 *   jj.setPriority(3);
 *   jj.setStatus(zk, jj.status().success()));
 * }
 * </pre>
 * 
 * <h3>Acquire Completed Batch, Perform Reporting</h3>
 * 
 * <pre>
 * try(ZooKeeper zk = new ZooKeeper("localhost:8084", 100, null)) {
 *   Batch batch = Batch.acquireBatchForReporting(zk);
 *   //Notify depositor of job status
 *   batch.setStatus(zk, batch.status().success()));
 *   //Admin thread will perform batch.delete(zk);
 * }
 * </pre>
 * 
* <h3>Create Assembly Request</h3>
 * 
 * <pre>
 * ZooKeeper zk = new ZooKeeper("localhost:8084", 100, null);
 * JSONObject tokenData = new JSONObject("...");
 * JSONObject tokenData = Access.createTokenData(...)
 * Access access = Access.createAssembly(zk, Access.Queues.small, tokenData);
 * </pre>
 * 
 * <h3>Consumer Daemon Acquires Assembly Request</h3>
 * 
 * <pre>
 * // An ephemeral lock is created when the batch is acquired
 * // The ephemeral lock will be released when the ZooKeeper connection is closed
 * try(ZooKeeper zk = new ZooKeeper("localhost:8084", 100, null)) {
 *   Access access = Batch.acquirePendingAssembly(zk, Access.Queues.small);
 *   //Do stuff
 *   access.setStatus(zk, AccessState.Processing);
 * }
 * </pre>
 *  
 * @see <a href="https://github.com/CDLUC3/mrt-zk/blob/main/README.md">Design Document</a>
 */
package org.cdlib.mrt.zk;
