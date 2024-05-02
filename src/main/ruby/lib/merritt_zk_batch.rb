require 'zk'
require 'json'
require 'yaml'

module MerrittZK

  class Batch < QueueItem
    BATCH_UUIDS = '/batch-uuids'
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
      @data = json_property(zk, 'submission')
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

    def self.batch_uuid_path(uuid)
      "#{BATCH_UUIDS}/#{uuid}"
    end

    def batch_uuid
      return "" if @data.nil?
      @data.fetch(:batchID, "")
    end

    def self.create_batch(zk, submission)
      id = QueueItem.create_id(zk, prefix_path)
      batch = Batch.new(id, data: submission)
      uuid = submission.fetch(:batchID, '')
      zk.create(self.batch_uuid_path(uuid), id) unless uuid.empty?
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

    def get_completed_jobs(zk)
      get_jobs(zk, 'batch-completed')
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
      if (zk.exists?(p))
        zk.children(p).each do |cp|
          jobs << Job.new(cp, bid: id)
        end
      end
      jobs
    end

    def self.find_batch_by_uuid(zk, uuid)
      return if uuid.empty?
      p = self.batch_uuid_path(uuid)
      return unless zk.exists?(p)
      arr = zk.get(p)
      return if arr.nil?
      bid = arr[0]
      return if bid.empty?
      Batch.new(bid)
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

      load(zk) if @data.nil?
      zk.delete(Batch.batch_uuid_path(batch_uuid)) unless batch_uuid.empty?

      unless path.nil? || path.empty?
        # puts "DELETE #{path}"
        zk.rm_rf(path)
      end
    end

  end

end
