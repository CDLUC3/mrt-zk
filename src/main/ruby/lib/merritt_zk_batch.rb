require 'zk'
require 'json'
require 'yaml'

module MerrittZK

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

end