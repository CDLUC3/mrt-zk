# frozen_string_literal: true

require 'zk'
require 'json'
require 'yaml'
require_relative 'merritt_zk_state'
require_relative 'merritt_zk_job_state'
require_relative 'merritt_zk_batch_state'
require_relative 'merritt_zk_access_state'
require_relative 'merritt_zk_queue_item'
require_relative 'merritt_zk_job'
require_relative 'merritt_zk_batch'
require_relative 'merritt_zk_access'
require_relative 'merritt_zk_locks'

##
# == Merritt Queue Design
#
# The Merritt Ingest Queue will be refactored in 2024 to enable a number of goals.
# - Match ingest workload to available resources (compute, memory, working storage)
# - dynamically provision resources to match demand
# - dynamically manage running thread count based on processing load
# - Hold jobs based on temporary holds (collection lock, storage node lock, queue hold)
# - Graceful resumption of processing in progress
# - Allow processing to be resumed on any ingest host.
#   The previous implementation managed state in memory which prevented this capability
# - Accurate notification of ingest completion (including inventory recording)
# - Send accurate summary email on completion of a batch regardless of any interruption that occurred while processing
#
# == Batch Queue vs Job Queue
# The work of the Merritt Ingest Service takes place at a <em>Job</em> level.
# Merritt Depositors initiate submissions at a <em>Batch</em> level.
# The primary function of the <em>Batch Queue</em> is to provide notification to a depositor once
# all jobs for a batch have completed.
#
# == Use of ZooKeeper
# Merritt Utilizes ZooKeeper for the following features
# - Creation/validation of distributed (ephemeral node) locks
# - Creation of unique node names across the distributed node structure (sequential nodes)
# - Manage data across the distributed node structure to allow any worker to acquire a job/batch (persistent nodes)
#
# The ZooKeeper documentation advises keeping the payload of shared data relatively small.
#
# The Merritt ZooKeeper design sames read-only data as JSON objects.
#
# More volatile (read/write) fields are saved as Int, Long, String and very small JSON objects.
#
# == Code Examples
#
# === Create Batch
#
#     zk = ZK.new('localhost:8084')
#     batchSub = {}
#     Batch batch = MerrittZK::Batch.create_batch(@zk, {foo: 'bar'})
#
# === Consumer Daemon Acquires Batch and Creates Jobs
#
#     zk = ZK.new('localhost:8084')
#     batch = MerrittZK::Batch.acquire_pending_batch(zk)
#     job = MerrittZK::Job.create_job(zk, batch.id, {foo: 'bar})
#     zk.close
#
# === Consumer Daemon Acquires Pending Job and Moves Job to Estimating
#
#     zk = ZK.new('localhost:8084')
#     jj = MerrittZK::Job.acquire_job(zk, MerrittZK::JobState::Pending)
#     jj.set_status(zk, jj.status.state_change(:Estimating))
#     zk.close
#
# === Consumer Daemon Acquires Estimating Job and Updates Priority
#
#     zk = ZK.new('localhost:8084')
#     jj = MerrittZK::Job.acquire_job(zk, MerrittZK::JobState::Estimating)
#     jj.set_priority(zk, 3)
#     zk.close
#
# === Acquire Completed Batch, Perform Reporting
#
#     zk = ZK.new('localhost:8084')
#     batch = MerrittZK::Batch.acquire_batch_for_reporting_batch(zk)
#     # perform reporting on jobs
#     batch.set_status(zk, batch.status.success)
#     # An admin thread will perform batch.delete(zk)
#     zk.close
#
# == See Also
# @see https://github.com/CDLUC3/mrt-zk/blob/main/README.md

module MerrittZK
  class MerrittZKNodeInvalid < StandardError
  end

  class MerrittStateError < StandardError
  end
end
