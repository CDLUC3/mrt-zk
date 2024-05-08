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

    def load_properties(zk)
      @data = json_property(zk, 'token')
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
      access.set_data(zk, 'token', token)
      access.set_status(zk, AccessState.init)
      access
    end

    def self.acquire_pending_assembly(zk, queue_name)
      zk.children(Access.dir(queue_name)).sort.each do |cp|
        a = Access.new(queue_name, cp)
        a.load(zk)
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

    def self.list_jobs(zk)
      jobs = []
      [SMALL, LARGE].each do |queue|
        zk.children("#{DIR}/#{queue}").sort.each do |cp|
          job = Access.new(queue, cp).load(zk)
          jobjson = job.data
          jobjson[:id] = cp
          jobjson[:queueNode] = Access.dir(queue)
          jobjson[:path] = job.path
          jobs.append(jobjson)
        end
      end
      jobs
    end
  end
end
