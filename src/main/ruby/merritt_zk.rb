require 'zk'
require 'json'
require 'yaml'

##
# == Merritt Queue Design
# 
# The Merritt Ingest Queue will be refactored in 2024 to enable a number of goals.
# - Match ingest workload to available resources (compute, memory, working storage)
# - dynamically provision resources to match demand
# - dynamically manage running thread count based on processing load
# - Hold jobs based on temporary holds (collection lock, storage node lock, queue hold)
# - Graceful resumption of processing in progress
# - Allow processing to be resumed on any ingest host.  The previous implementation managed state in memory which prevented this capability
# - Accurate notification of ingest completion (including inventory recording)
# - Send accurate summary email on completion of a batch regardless of any interruption that occurred while processing
#
# == Batch Queue vs Job Queue
# The work of the Merritt Ingest Service takes place at a <em>Job</em> level.
# Merritt Depositors initiate submissions at a <em>Batch</em> level.  
# The primary function of the <em>Batch Queue</em> is to provide notification to a depositor once all jobs for a batch have completed.
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
#     jj = MerrittZK::Job.acquire_job(zk, MerrittZK::JobState.Pending)
#     jj.set_status(zk, jj.status.state_change(:Estimating))
#     zk.close
# 
# === Consumer Daemon Acquires Estimating Job and Updates Priority
# 
#     zk = ZK.new('localhost:8084')
#     jj = MerrittZK::Job.acquire_job(zk, MerrittZK::JobState.Estimating)
#     jj.set_priority(zk, 3)
#     zk.close
# 
# === Acquire Completed Batch, Perform Reporting
# 
#     zk = ZK.new('localhost:8084')
#     batch = MerrittZK::Batch.acquire_completed_batch(zk)
#     # perform reporting on jobs
#     batch.set_status(zk, batch.status.success)
#     # An admin thread will perform batch.delete(zk)
#     zk.close
# 
# == See Also
# @see https://github.com/CDLUC3/mrt-doc/blob/main/design/queue-2023/README.md

module MerrittZK
  ## 
  # Class details
  class IngestState
    def initialize(status, next_status)
      next_status = {} if next_status.nil?
      @status = status
      @next_status = next_status.keys
      @success_state = nil
      @fail_state = nil
      next_status.each do |k, v|
        next if v.nil?
        @success_state = k if v.fetch(:success, false)
        @fail_state = k if v.fetch(:fail, false)
      end
    end

    def self.state_yaml
      JSON.parse(
        YAML.safe_load(
          File.read('../../../states.yml'), 
          aliases: true
        ).to_json, 
        symbolize_names: true
      )
    end

    def status
      @status
    end

    def name
      @status.to_s
    end

    def next_status
      @next_status
    end

    def deletable?
      @next_status.empty?
    end

    def success_lookup(states)
      states.fetch(success_state, nil)
    end

    def fail_lookup(states)
      states.fetch(fail_state, nil)
    end

    def state_change_allowed(state)
      next_status.include?(state)
    end

    def state_lookup(states, state)
      return states.fetch(state, nil) if state_change_allowed(state)
    end

    def to_s
      "#{@status}: #{@next_status}"
    end

    private 

    def success_state
      @success_state
    end

    def fail_state
      @fail_state
    end

  end

  class JobState < IngestState
    @@states = {}
    @@state_list = []

    IngestState.state_yaml.fetch(:job_states, {}).each do |k, v|
      @@states[k] = JobState.new(k, v)
      @@state_list.append(@@states[k])
    end

    private_class_method :new

    def self.states
      @@states
    end

    def self.init
      self.Pending
    end

    def state_change(state)
      state_lookup(@@states, state)
    end

    def success
      success_lookup(@@states)
    end

    def fail
      fail_lookup(@@states)
    end

    def self.Pending
      @@states[:Pending]
    end

    def self.Held
      @@states[:Held]
    end

    def self.Estimating
      @@states[:Estimating]
    end

    def self.Provisioning
      @@states[:Provisioning]
    end

    def self.Downloading
      @@states[:Downloading]
    end

    def self.Processing
      @@states[:Processing]
    end

    def self.Recording
      @@states[:Recording]
    end

    def self.Notify
      @@states[:Notify]
    end

    def self.Failed
      @@states[:Failed]
    end

    def self.Deleted
      @@states[:Deleted]
    end

    def self.Completed
      @@states[:Completed]
    end
  end

  class BatchState < IngestState
    @@states = {}
    @@state_list = []

    IngestState.state_yaml.fetch(:batch_states, {}).each do |k, v|
      @@states[k] = BatchState.new(k, v)
      @@state_list.append(@@states[k])
    end

    private_class_method :new

    def self.states
      @@states
    end

    def self.init
      self.Pending
    end

    def self.Pending
      @@states[:Pending]
    end

    def self.Held
      @@states[:Held]
    end

    def self.Processing
      @@states[:Processing]
    end

    def self.Reporting
      @@states[:Reporting]
    end

    def self.UpdateReporting
      @@states[:UpdateReporting]
    end

    def self.Failed
      @@states[:Failed]
    end

    def self.Deleted
      @@states[:Deleted]
    end

    def self.Completed
      @@states[:Completed]
    end

    def state_change(state)
      state_lookup(@@states, state)
    end

    def success
      success_lookup(@@states)
    end

    def fail
      fail_lookup(@@states)
    end
  end

  class MerrittZKNodeInvalid < StandardError
    def initialize(message)
      super(message)
    end
  end

  class MerrittStateError < StandardError
    def initialize(message)
      super(message)
    end
  end

  class QueueItem

    def initialize(id, data: nil)
      @id = id
      @data = data
      @status = nil
    end

    attr_reader :id, :status

    def states
      {}
    end

    def load(zk)
      raise MerrittZKNodeInvalid.new("Missing Node #{path}") unless zk.exists?(path)
      load_status(zk, json_property(zk, 'status'))
      load_properties(zk)
      self
    end

    def load_status(zk, js)
      s = js.fetch('status', 'na').to_sym
      @status = states.fetch(s, nil)
    end

    def load_properties(zk)
      # Job will override
    end

    def string_property(zk, key)
      p = "#{path}/#{key}"
      arr = zk.get(p)
      raise MerrittZKNodeInvalid.new("Node Object for (#{p}) missing") if arr.nil?
      arr[0]
    end

    def json_property(zk, key)
      begin
        JSON.parse(string_property(zk, key))
      rescue
        raise MerrittZKNodeInvalid.new("Node Object for (#{p}) does not contain valid json")
      end
    end

    def int_property(zk, key)
      string_property(zk, key).to_i
    end

    def set_data(zk, key, data)
      p = "#{path}/#{key}"
      d = QueueItem.serialize(data)
      if zk.exists?(p)
        zk.set(p, d)
      else
        zk.create(p, data: d)
      end
    end

    def path
      "na"
    end

    def status_path
      "#{path}/status"
    end

    def self.serialize(v)
      return nil if v.nil?
      return v.to_s if v.is_a?(Integer)
      return v unless v.is_a?(Hash)
      return nil if v.empty?
      v.to_json
    end

    def self.create_id(zk, prefix)
      path = zk.create(prefix, data: nil, mode: :persistent_sequential, ignore: :no_node)
      path.split('/')[-1]
    end

    def status_object(status)
      {
        status: status.name, 
        last_modified: Time.now.to_s
      }
    end

    def set_status(zk, status)
      return if status == @status
      data = QueueItem.serialize(status_object(status))
      if @status.nil?
        zk.create(status_path, data)
      else
        zk.set(status_path, data)
      end
      @status = status
    end

    def lock(zk)
      zk.create("#{path}/lock", data: nil, mode: :ephemeral)
    end

    def unlock(zk)
      zk.delete("#{path}/lock")
    end

  end

  class Batch < QueueItem
    @@dir = '/batches'
    @@prefix = 'bid'
    @@init_status = BatchState.init

    def initialize(id, data: nil)
      super(id, data: data)
      @has_failure = false
    end

    attr_reader :has_failure

    def load_has_failure(zk)
      @has_failure = false
      p = "#{path}/states/batch-failed"
      if zk.exists?(p)
        unless zk.children(p).empty?
          @has_failure = true
        end
      end
    end

    def load_properties(zk)
      load_has_failure(zk)
    end

    def states
      BatchState.states
    end

    def self.dir
      "#{@@dir}"
    end

    def self.prefix_path
      "#{@@dir}/#{@@prefix}"
    end

    def path
      "#{@@dir}/#{@id}"
    end

    def self.create_batch(zk, submission)
      id = QueueItem.create_id(zk, prefix_path)
      batch = Batch.new(id, data: submission)
      batch.set_data(zk, "submission", submission)
      batch.set_status(zk, @@init_status)
      batch
    end

    def self.acquire_pending_batch(zk)
      zk.children(@@dir).sort.each do |cp|
        next if zk.exists?("#{@@dir}/#{cp}/states")
        b = Batch.new(cp)
        b.load(zk)
        begin
          if b.lock(zk)
            b.set_data(zk, 'states', nil)
            return b
          end
        rescue  ZK::Exceptions::NodeExists => e
        end
      end
      nil
    end

    def self.acquire_complete_batch(zk)
      zk.children(@@dir).sort.each do |cp|
        next unless zk.exists?("#{@@dir}/#{cp}/states/batch-processing")
        next unless zk.children("#{@@dir}/#{cp}/states/batch-processing").empty?
        b = Batch.new(cp)
        b.load(zk)
        begin
          if b.lock(zk)
            b.set_status(zk, BatchState.Reporting)
            return b
          end
        rescue  ZK::Exceptions::NodeExists => e
        end
      end
      nil
    end

    def delete(zk)
      raise MerrittZK::MerrittStateError.new("Delete invalid #{path}") unless @status.deletable?
      ['batch-processing', 'batch-failed', 'batch-completed'].each do |state|
        p = "#{path}/states/#{state}"
        next unless zk.exists?(p)
        zk.children(p).each do |cp|
          MerrittZK::Job.new(cp).load(zk).delete(zk)
        end
      end

      unless path.nil? || path.empty?
        # puts "DELETE #{path}"
        zk.rm_rf(path)
      end
    end

  end

  class Job < QueueItem
    @@dir = '/jobs'
    @@prefix = 'jid'
    @@init_status = JobState.init

    def initialize(id, bid: nil, data: nil)
      super(id, data: data)
      @bid = bid
      @priority = 5
      @space_needed = 0
      @job_state_path = nil
      @batch_state_path = nil
      @retry_count = 0
    end

    def load_status(zk, js)
      super(zk, js)
      @retry_count = js.fetch('retry_count', 0)
    end

    def load_properties(zk)
      @bid = string_property(zk, 'bid')
      @priority = int_property(zk, 'priority')
      @space_needed = int_property(zk, 'space_needed')
      set_job_state_path(zk)
      set_batch_state_path(zk)
    end

    attr_reader :bid, :priority, :space_needed, :jobstate

    def set_priority(zk, priority)
      return if priority == @priority
      @priority = priority
      set_data(zk, 'priority', priority)
      set_job_state_path(zk)
    end

    def set_space_needed(zk, space_needed)
      return if space_needed == @space_needed
      @space_needed = space_needed
      set_data(zk, 'space_needed', space_needed)
    end

    def set_status(zk, status, job_retry: false)
      @retry_count = @retry_count + 1 if job_retry
      super(zk, status)
      set_job_state_path(zk)
      set_batch_state_path(zk)
    end

    def batch_state_subpath
      return 'batch-failed' if @status.status == :Failed
      return 'batch-completed' if @status.status == :Completed
      'batch-processing'
    end

    def set_batch_state_path(zk)
      bs = sprintf('%s/%s/states/%s/%s', Batch.dir, @bid, batch_state_subpath, id)
      return if bs == @batch_state_path
      if @batch_state_path
        zk.delete(@batch_state_path)
      end
      @batch_state_path = bs 
      unless zk.exists?(@batch_state_path)
        p = File.dirname(@batch_state_path)
        zk.create(p, data: nil) unless zk.exists?(p)
        zk.create(@batch_state_path, data: nil)
      end
    end

    def set_job_state_path(zk)
      js = sprintf('%s/states/%s/%02d-%s', @@dir, status.name.downcase, priority, id)
      bs = sprintf('%s/%s/states/%s/%s', Batch.dir, @bid, batch_state_subpath, id)
      return if js == @job_state_path
      if @job_state_path
        zk.delete(@job_state_path)
      end
      @job_state_path = js 
      unless zk.exists?(@job_state_path)
        p = File.dirname(@job_state_path)
        zk.create(p, data: nil) unless zk.exists?(p)
        zk.create(@job_state_path, data: nil)
      end
    end

    def states
      JobState.states
    end

    def self.prefix_path
      "#{@@dir}/#{@@prefix}"
    end

    def path
      "#{@@dir}/#{@id}"
    end

    def self.create_job(zk, bid, data)
      id = QueueItem.create_id(zk, prefix_path)
      job = Job.new(id, bid: bid, data: data)
      job.set_data(zk, 'bid', bid)
      job.set_data(zk, 'priority', job.priority)
      job.set_data(zk, 'space_needed', job.space_needed)
      job.set_data(zk, 'configuration', data)
      job.set_status(zk, @@init_status)
      job.set_job_state_path(zk)
      job.set_batch_state_path(zk)
      job
    end

    def status_object(status)
      {
        status: status.name, 
        last_successful_status: nil, 
        last_modified: Time.now.to_s,
        retry_count: @retry_count
      }
    end

    def self.acquire_job(zk, state)
      p = "#{@@dir}/states/#{state.name.downcase}"
      return nil unless zk.exists?(p)
      zk.children(p).sort.each do |cp|
        j = Job.new(cp[3..]).load(zk)
        begin
          return j if j.lock(zk)
        rescue  ZK::Exceptions::NodeExists => e
        end
      end
      nil
    end

    def delete(zk)
      raise MerrittZK::MerrittStateError.new("Delete invalid #{path}") unless @status.deletable?
      unless @job_state_path.nil?
        # puts "DELETE #{@job_state_path}"
        zk.rm_rf(@job_state_path)
      end
      unless @batch_state_path.nil?
        # puts "DELETE #{@batch_state_path}"
        zk.rm_rf(@batch_state_path)
      end
      unless path.nil? || path.empty?
        # puts "DELETE #{path}"
        zk.rm_rf(path)
      end
    end

  end

end