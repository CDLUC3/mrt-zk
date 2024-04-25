require 'zk'
require 'json'
require 'yaml'

module MerrittZK

  class Access < QueueItem
    @@dir = '/access'
    @@prefix = 'qid'
    @@init_status = AccessState.init

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
      "#{@@dir}/#{queue_name}"
    end

    def self.prefix_path(queue_name)
      "#{self.dir(queue_name)}/#{@@prefix}"
    end

    def path
      "#{Access.dir(@queue_name)}/#{@id}"
    end

    def self.create_assembly(zk, queue_name, token)
      id = QueueItem.create_id(zk, prefix_path(queue_name))
      access = Access.new(queue_name, id, data: token)
      access.set_data(zk, "token", token)
      access.set_status(zk, @@init_status)
      access
    end

    def self.acquire_pending_assembly(zk, queue_name)
      zk.children(Access.dir(queue_name)).sort.each do |cp|
        a = Access.new(queue_name, cp)
        a.load(zk)
        begin
          if a.lock(zk)
            return a
          end
        rescue  ZK::Exceptions::NodeExists => e
        end
      end
      nil
    end


    def delete(zk)
      raise MerrittZK::MerrittStateError.new("Delete invalid #{path}") unless @status.deletable?
      unless path.nil? || path.empty?
        # puts "DELETE #{path}"
        zk.rm_rf(path)
      end
    end

  end

end