# frozen_string_literal: true

require 'zk'
require 'json'
require 'yaml'

module MerrittZK
  ##
  # Merritt Object Assembly Queue Item
  class Access < QueueItem
    DIR = '/access'
    PREFIX = 'qid'
    SMALL = 'small'
    LARGE = 'large'

    def initialize(queue_name, id, data: nil)
      super(id, data: data)
      @queue_name = queue_name
    end

    def load_properties(zk, _set_status_flag)
      @data = json_property(zk, ZkKeys::TOKEN)
    end

    def states
      AccessState.states
    end

    def self.dir(queue_name)
      "#{DIR}/#{queue_name}"
    end

    def self.prefix_path(queue_name)
      "#{dir(queue_name)}/#{PREFIX}"
    end

    def path
      "#{Access.dir(@queue_name)}/#{@id}"
    end

    def self.create_assembly(zk, queue_name, token)
      id = QueueItem.create_id(zk, prefix_path(queue_name))
      access = Access.new(queue_name, id, data: token)
      access.set_data(zk, ZkKeys::TOKEN, token)
      access.set_status(zk, AccessState.init)
      access
    end

    def self.acquire_pending_assembly(zk, queue_name)
      zk.children(Access.dir(queue_name)).sort.each do |cp|
        a = Access.new(queue_name, cp)
        a.load(zk)
        next unless a.status == AccessState::Pending

        begin
          return a if a.lock(zk)
        rescue ZK::Exceptions::NodeExists
          # no action
        end
      end
      nil
    end

    def delete(zk)
      raise MerrittZK::MerrittStateError, "Delete invalid #{path}" unless @status.deletable?

      return if path.nil? || path.empty?

      # puts "DELETE #{path}"
      zk.rm_rf(path)
    end

    ##
    # List jobs as a json object that will be consumed by the admin tool.
    def self.list_jobs_as_json(zk)
      jobs = []
      [SMALL, LARGE].each do |queue|
        zk.children("#{DIR}/#{queue}").sort.each do |cp|
          job = Access.new(queue, cp)
          job.load(zk, set_status_flag: false)
          jobjson = job.data
          jobjson[:id] = cp
          jobjson[:queueNode] = Access.dir(queue)
          jobjson[:path] = job.path
          jobjson[:queueStatus] = jobjson[:status]
          jobjson[:status] = job.status.status
          jobjson[:date] = job.json_property(zk, ZkKeys::STATUS).fetch(:last_modified, '')
          jobs.append(jobjson)
        rescue StandardError => e
          puts "List Access #{cp} exception: #{e}"
        end
      end
      jobs
    end
  end
end
