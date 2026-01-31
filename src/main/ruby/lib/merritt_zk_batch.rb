# frozen_string_literal: true

require 'zk'
require 'json'
require 'yaml'

module MerrittZK
  ##
  # Merritt Batch Queue Items
  class Batch < QueueItem
    BATCH_UUIDS = '/batch-uuids'
    DIR = '/batches'
    PREFIX = 'bid'

    def initialize(id, data: nil)
      super
      @has_failure = false
    end

    attr_reader :has_failure

    def load_has_failure(zk)
      @has_failure = false
      p = "#{path}/states/batch-failed"
      return unless zk.exists?(p)
      return if zk.children(p).empty?

      @has_failure = true
    end

    def load_properties(zk, _set_status_flag)
      @data = json_property(zk, ZkKeys::SUBMISSION)
      load_has_failure(zk)
    end

    def states
      BatchState.states
    end

    def self.dir
      DIR.to_s
    end

    def self.prefix_path
      "#{DIR}/#{PREFIX}"
    end

    def path
      "#{DIR}/#{@id}"
    end

    def self.batch_uuid_path(uuid)
      "#{BATCH_UUIDS}/#{uuid}"
    end

    def batch_uuid
      return '' if @data.nil?

      @data.fetch(:batchID, '')
    end

    def self.create_batch(zk, submission)
      id = QueueItem.create_id(zk, prefix_path)
      batch = Batch.new(id, data: submission)
      uuid = submission.fetch(:batchID, '')
      zk.create(batch_uuid_path(uuid), id) unless uuid.empty?
      batch.set_data(zk, ZkKeys::SUBMISSION, submission)
      batch.set_status(zk, BatchState.init)
      batch
    end

    def self.acquire_pending_batch(zk)
      zk.children(DIR).sort.each do |cp|
        next if zk.exists?("#{DIR}/#{cp}/#{ZkKeys::STATES}")

        b = Batch.new(cp)
        b.load(zk, set_status_flag: false)
        begin
          if b.lock(zk)
            b.set_data(zk, ZkKeys::STATES, nil)
            b.set_status(zk, BatchState::Processing)
            return b
          end
        rescue ZK::Exceptions::NodeExists
          # no action
        end
      end
      nil
    end

    def self.acquire_batch_for_reporting_batch(zk)
      zk.children(DIR).sort.each do |cp|
        next unless zk.exists?("#{DIR}/#{cp}/states/batch-processing")
        next unless zk.children("#{DIR}/#{cp}/states/batch-processing").empty?

        b = Batch.new(cp)
        b.load(zk, set_status_flag: false)
        begin
          next if [BatchState::Completed, BatchState::Failed].include?(b.status)

          if b.lock(zk)
            b.set_status(zk, BatchState::Reporting)
            return b
          end
        rescue ZK::Exceptions::NodeExists
          # no action
        end
      end
      nil
    end

    def self.delete_completed_batches(zk)
      ids = []
      zk.children(DIR).sort.each do |cp|
        if zk.exists?("#{DIR}/#{cp}/states/batch-processing")
          next unless zk.children("#{DIR}/#{cp}/states/batch-processing").empty?
        end

        b = Batch.new(cp)
        b.load(zk, set_status_flag: false)
        begin
          next unless [BatchState::Completed, BatchState::Deleted].include?(b.status)

          b.delete(zk)
          ids << b.id
        rescue ZK::Exceptions::NodeExists
          # no action
        end
      end
      ids
    end

    def get_completed_jobs(zk)
      get_jobs(zk, 'batch-completed')
    end

    def get_deleted_jobs(zk)
      get_jobs(zk, 'batch-deleted')
    end

    def get_failed_jobs(zk)
      get_jobs(zk, 'batch-failed')
    end

    def get_processing_jobs(zk)
      get_jobs(zk, 'batch-processing')
    end

    def get_jobs(zk, state)
      jobs = []
      p = "#{path}/states/#{state}"
      if zk.exists?(p)
        zk.children(p).each do |cp|
          jobs << Job.new(cp, bid: id)
        end
      end
      jobs
    end

    def self.find_batch_by_uuid(zk, uuid)
      return if uuid.empty?

      p = batch_uuid_path(uuid)
      return unless zk.exists?(p)

      arr = zk.get(p)
      return if arr.nil?

      bid = arr[0]
      return if bid.empty?

      Batch.new(bid)
    end

    def delete(zk)
      raise MerrittZK::MerrittStateError, "Delete invalid #{path}" unless @status.deletable?

      %w[batch-processing batch-failed batch-completed batch-deleted].each do |state|
        p = "#{path}/states/#{state}"
        next unless zk.exists?(p)

        zk.children(p).each do |cp|
          MerrittZK::Job.new(cp).load(zk).delete(zk)
        rescue StandardError => e
          puts "Delete Job #{cp} exception: #{e}"
        end
      end

      load(zk) if @data.nil?
      zk.delete(Batch.batch_uuid_path(batch_uuid)) unless batch_uuid.empty?

      return if path.nil? || path.empty?

      # puts "DELETE #{path}"
      zk.rm_rf(path)
    end

    def self.list_batches_as_json(zk)
      batches = []
      zk.children(DIR).sort.each do |cp|
        batch = Batch.new(cp)
        batch.load(zk, set_status_flag: false)
        batchjson = batch.data
        batchjson[:id] = batch.id
        batchjson[:status] = batch.status_name
        batchjson[:jobCountFailed] = batch.get_failed_jobs(zk).length
        batchjson[:jobCountDeleted] = batch.get_deleted_jobs(zk).length
        batchjson[:jobCountCompleted] = batch.get_completed_jobs(zk).length
        batchjson[:jobCountProcessing] = batch.get_processing_jobs(zk).length
        batchjson[:jobCount] = batch.get_processing_jobs(zk).length +
                               batch.get_failed_jobs(zk).length + batch.get_deleted_jobs(zk).length +
                               batch.get_completed_jobs(zk).length
        batches.append(batchjson)
      rescue StandardError => e
        puts "List Batch #{cp} exception: #{e}"
      end
      batches
    end
  end
end
