# frozen_string_literal: true

require 'zk'
require 'json'
require 'yaml'

module MerrittZK
  ##
  # Merritt Ingest Job Queue item
  class Job < QueueItem
    DIR = '/jobs'
    PREFIX = 'jid'

    def initialize(id, bid: nil, data: nil, identifiers: {}, metadata: {})
      super(id, data: data)
      @bid = bid
      @priority = 5
      @space_needed = 0
      @job_state_path = nil
      @batch_state_path = nil
      @retry_count = 0
      @identifiers = identifiers
      @metadata = metadata
      @inventory = {}
    end

    attr_reader :job_state_path, :batch_state_path, :bid, :priority, :space_needed

    def load_status(zk, js)
      super
      @retry_count = js.fetch(:retry_count, 0)
    end

    # for the admin tool
    def load_optimized(zk)
      raise MerrittZKNodeInvalid, "Missing Node #{path}" unless zk.exists?(path)

      load_status(zk, json_property(zk, ZkKeys::STATUS))
      @data = json_property(zk, ZkKeys::CONFIGURATION)
      @bid = string_property(zk, ZkKeys::BID)
      @space_needed = int_property(zk, ZkKeys::SPACE_NEEEDED)
      self
    end

    def load_properties(zk, set_status_flag)
      @data = json_property(zk, ZkKeys::CONFIGURATION)
      @bid = string_property(zk, ZkKeys::BID)
      @priority = int_property(zk, ZkKeys::PRIORITY)
      @space_needed = int_property(zk, ZkKeys::SPACE_NEEEDED)
      @identifiers = json_property(zk, ZkKeys::IDENTIFIERS) if zk.exists?("#{path}/#{ZkKeys::IDENTIFIERS}")
      @metadata = json_property(zk, ZkKeys::METADATA) if zk.exists?("#{path}/#{ZkKeys::METADATA}")
      @inventory = json_property(zk, ZkKeys::INVENTORY) if zk.exists?("#{path}/#{ZkKeys::INVENTORY}")

      return unless set_status_flag

      set_job_state_path(zk)
      set_batch_state_path(zk)
    end

    def set_priority(zk, priority)
      return if priority == @priority

      @priority = priority
      set_data(zk, ZkKeys::PRIORITY, priority)
    end

    def set_space_needed(zk, space_needed)
      return if space_needed == @space_needed

      @space_needed = space_needed
      set_data(zk, ZkKeys::SPACE_NEEEDED, space_needed)
    end

    def set_status(zk, status, message = '', job_retry: false)
      @retry_count += 1 if job_retry
      super(zk, status, message)
      set_job_state_path(zk)
      set_batch_state_path(zk)
    end

    def set_status_with_priority(zk, status, priority)
      set_priority(zk, priority)
      set_status(zk, status)
    end

    def batch_state_subpath
      return 'batch-failed' if @status.status == :Failed
      return 'batch-completed' if @status.status == :Completed
      return 'batch-deleted' if @status.status == :Deleted

      'batch-processing'
    end

    def set_batch_state_path(zk)
      bs = format('%s/%s/states/%s/%s', Batch.dir, @bid, batch_state_subpath, id)
      return if bs == @batch_state_path

      zk.delete(@batch_state_path) if @batch_state_path
      @batch_state_path = bs
      return if zk.exists?(@batch_state_path)

      p = File.dirname(@batch_state_path)
      pp = File.dirname(p)
      zk.create(pp, data: nil) unless zk.exists?(pp)
      zk.create(p, data: nil) unless zk.exists?(p)
      zk.create(@batch_state_path, data: nil)
    end

    def set_job_state_path(zk)
      js = format('%s/states/%s/%02d-%s', DIR, status.name.downcase, priority, id)
      return if js == @job_state_path

      zk.delete(@job_state_path) if @job_state_path
      @job_state_path = js
      return if zk.exists?(@job_state_path)

      p = File.dirname(@job_state_path)
      zk.create(p, data: nil) unless zk.exists?(p)
      zk.create(@job_state_path, data: nil)
    end

    def states
      JobState.states
    end

    def self.prefix_path
      "#{DIR}/#{PREFIX}"
    end

    def path
      "#{DIR}/#{@id}"
    end

    def self.create_job(zk, bid, data, priority: 5, identifiers: {}, metadata: {})
      id = QueueItem.create_id(zk, prefix_path)
      job = Job.new(id, bid: bid, data: data, identifiers: identifiers, metadata: metadata)
      job.set_data(zk, ZkKeys::BID, bid)
      job.set_data(zk, ZkKeys::PRIORITY, job.priority)
      job.set_data(zk, ZkKeys::SPACE_NEEEDED, job.space_needed)
      job.set_data(zk, ZkKeys::CONFIGURATION, data)
      job.set_data(zk, ZkKeys::IDENTIFIERS, identifiers) unless identifiers.empty?
      job.set_data(zk, ZkKeys::METADATA, metadata) unless metadata.empty?
      job.set_status_with_priority(zk, JobState.init, priority)
      job.set_job_state_path(zk)
      job.set_batch_state_path(zk)
      job
    end

    def status_object(oldstat, status)
      oldstatus = oldstat.nil? ? nil : oldstat[:status]
      jobj = super

      jobj[:last_successful_status] = nil unless jobj.key?(:last_successful_status)

      if oldstatus.nil?
        # no action 1
      elsif status.nil?
        # no action 2
      elsif [MerrittZK::JobState::Failed, MerrittZK::JobState::Deleted].include?(status)
        # no action 3
      elsif %w[Failed Deleted].include?(oldstatus)
        # no action 3.2
      elsif status.name == oldstatus
        # no action 4
      else
        jobj[:last_successful_status] = oldstatus
      end
      jobj[:retry_count] = @retry_count
      jobj
    end

    def self.acquire_job(zk, state)
      p = "#{DIR}/states/#{state.name.downcase}"
      return nil unless zk.exists?(p)

      zk.children(p).sort.each do |cp|
        j = Job.new(cp[3..]).load(zk)
        begin
          if j.lock(zk)
            j.load(zk)
            return j
          end
        rescue ZK::Exceptions::NodeExists
          # no action
        end
      end
      nil
    end

    def delete(zk)
      raise MerrittZK::MerrittStateError, "Delete invalid #{path}" unless @status.deletable?

      unless @job_state_path.nil?
        # puts "DELETE #{@job_state_path}"
        zk.rm_rf(@job_state_path)
      end
      unless @batch_state_path.nil?
        # puts "DELETE #{@batch_state_path}"
        zk.rm_rf(@batch_state_path)
      end
      return if path.nil? || path.empty?

      # puts "DELETE #{path}"
      zk.rm_rf(path)
    end

    ##
    # List jobs as a json object that will be consumed by the admin tool.
    def self.list_jobs_as_json(zk)
      jobs = []
      zk.children(DIR).sort.each do |cp|
        next if cp == ZkKeys::STATES

        begin
          job = Job.new(cp)
          job.load_optimized(zk)
          jobjson = job.data
          jobjson[:id] = job.id
          jobjson[:bid] = job.bid
          jobjson[:status] = job.status_name
          jobs.append(jobjson)
        rescue StandardError => e
          puts "List Job #{cp} exception: #{e}"
        end
      end
      jobs
    end

    def submitter
      data_prop('submitter', '')
    end

    def submission_date
      data_prop('submissionDate', 'foo')
    end

    def creator
      data_prop('creator', '')
    end

    def profile
      data_prop('profile', '')
    end

    def response_form
      data_prop('responseForm', '')
    end

    def filename
      data_prop('filename', '')
    end

    def udpate
      data_prop('update', false)
    end

    def type
      data_prop('type', '')
    end

    def title
      data_prop('title', '')
    end
  end
end
