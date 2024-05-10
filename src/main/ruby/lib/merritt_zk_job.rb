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
      @retry_count = js.fetch(:retry_count, 0)
    end

    def load_properties(zk)
      @data = json_property(zk, ZkKeys::CONFIGURATION)
      @bid = string_property(zk, ZkKeys::BID)
      @priority = int_property(zk, ZkKeys::PRIORITY)
      @space_needed = int_property(zk, ZkKeys::SPACE_NEEEDED)
      set_job_state_path(zk)
      set_batch_state_path(zk)
    end

    attr_reader :bid, :priority, :space_needed

    def set_priority(zk, priority)
      return if priority == @priority

      @priority = priority
      set_data(zk, ZkKeys::PRIORITY, priority)
      set_job_state_path(zk)
    end

    def set_space_needed(zk, space_needed)
      return if space_needed == @space_needed

      @space_needed = space_needed
      set_data(zk, ZkKeys::SPACE_NEEEDED, space_needed)
    end

    def set_status(zk, status, job_retry: false)
      @retry_count += 1 if job_retry
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

    def self.create_job(zk, bid, data)
      id = QueueItem.create_id(zk, prefix_path)
      job = Job.new(id, bid: bid, data: data)
      job.set_data(zk, ZkKeys::BID, bid)
      job.set_data(zk, ZkKeys::PRIORITY, job.priority)
      job.set_data(zk, ZkKeys::SPACE_NEEEDED, job.space_needed)
      job.set_data(zk, ZkKeys::CONFIGURATION, data)
      job.set_status(zk, JobState.init)
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
      p = "#{DIR}/states/#{state.name.downcase}"
      return nil unless zk.exists?(p)

      zk.children(p).sort.each do |cp|
        j = Job.new(cp[3..]).load(zk)
        begin
          return j if j.lock(zk)
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

    def self.list_jobs(zk)
      jobs = []
      zk.children(DIR).sort.each do |cp|
        next if cp == ZkKeys::STATES

        job = Job.new(cp).load(zk)
        jobjson = job.data
        jobjson[:id] = job.id
        jobjson[:bid] = job.bid
        jobs.append(jobjson)
      end
      jobs
    end

    def submitter
      data_prop('submitter', '')
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

  ##
  # Legacy Merritt Ingest Job record.
  # This class will be removed after the migration is complete
  class LegacyIngestJob < LegacyItem
    DIR = '/ingest'
    def dir
      DIR
    end

    def submitter
      @payload.fetch(:submitter, '')
    end

    def creator
      @payload.fetch(:creator, '')
    end

    def profile
      @payload.fetch(:profile, '')
    end

    def response_form
      @payload.fetch(:responseForm, '')
    end

    def filename
      @payload.fetch(:filename, '')
    end

    def udpate
      @payload.fetch(:update, false)
    end

    def type
      @payload.fetch(:type, '')
    end

    def title
      @payload.fetch(:title, '')
    end

    def bid
      @payload.fetch(:bid, '')
    end

    def priority
      @payload.fetch(:priority, 0)
    end

    def space_needed
      @payload.fetch(:space_needed, 0)
    end

    def self.list_jobs(zk)
      jobs = []
      return jobs unless zk.exists?(DIR)

      zk.children(DIR).sort.each do |cp|
        lj = LegacyIngestJob.new(cp)
        lj.load(zk)
        jobs.append(lj.payload_object)
      end
      jobs
    end
  end

  ##
  # Legacy Merritt Inventory Job record.
  # This class will be removed after the migration is complete
  class LegacyInventoryJob < LegacyItem
    DIR = '/mrt.inventory.full'
    def dir
      DIR
    end

    def json?
      false
    end

    def payload_object
      payload = super
      m = /(http:[^<]*)/.match(payload[:payload])
      payload[:queueNode] = DIR
      payload[:manifestURL] = m[1]
      payload
    end

    def self.list_jobs(zk)
      jobs = []
      return jobs unless zk.exists?(DIR)

      zk.children(DIR).sort.each do |cp|
        lj = LegacyInventoryJob.new(cp)
        lj.load(zk)
        jobs.append(lj.payload_object)
      end
      jobs
    end
  end

  ##
  # Legacy Merritt Access Job record.
  # This class will be removed after the migration is complete
  class LegacyAccessJob < LegacyItem
    def initialize(dir, cp)
      @dir = dir
      super(cp)
    end

    attr_reader :dir

    def json?
      true
    end

    def payload_object
      payload = super
      payload[:queueNode] = dir
      payload
    end

    def self.list_jobs(zk)
      jobs = []
      if zk.exists?(LargeLegacyAccessJob::DIR)
        zk.children(LargeLegacyAccessJob::DIR).sort.each do |cp|
          lj = LargeLegacyAccessJob.new(cp)
          lj.load(zk)
          jobs.append(lj.payload_object)
        end
      end

      if zk.exists?(SmallLegacyAccessJob::DIR)
        zk.children(SmallLegacyAccessJob::DIR).sort.each do |cp|
          lj = SmallLegacyAccessJob.new(cp)
          lj.load(zk)
          jobs.append(lj.payload_object)
        end
      end
      jobs
    end
  end

  ##
  # Legacy Merritt Small Access Job record.
  # This class will be removed after the migration is complete
  class SmallLegacyAccessJob < LegacyAccessJob
    DIR = '/accessSmall.1'
    def initialize(cp)
      super(DIR, cp)
    end
  end

  ##
  # Legacy Merritt Large Access Job record.
  # This class will be removed after the migration is complete
  class LargeLegacyAccessJob < LegacyAccessJob
    DIR = '/accessLarge.1'
    def initialize
      super(DIR, cp)
    end
  end
end
