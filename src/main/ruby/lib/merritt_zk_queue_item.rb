# frozen_string_literal: true

require 'zk'
require 'json'
require 'yaml'

module MerrittZK
  ##
  # Constants for accessing data nodes in ZK
  class ZkKeys
    TOKEN = 'token'
    SUBMISSION = 'submission'
    STATES = 'states'
    PRIORITY = 'priority'
    SPACE_NEEEDED = 'space_needed'
    BID = 'bid'
    CONFIGURATION = 'configuration'
    INVENTORY = 'inventory'
    STATUS = 'status'
    LOCK = 'lock'
    IDENTIFIERS = 'identifiers'
    METADATA = 'metadata'
  end

  ##
  # Base class for Merritt Queue Items that follow similar conventions
  class QueueItem
    def initialize(id, data: nil)
      @id = id
      @data = data
      @status = nil
    end

    attr_reader :id, :status, :data

    def status_name
      status.name
    end

    def states
      {}
    end

    def load(zk, set_status_flag: true)
      raise MerrittZKNodeInvalid, "Missing Node #{path}" unless zk.exists?(path)

      load_status(zk, json_property(zk, ZkKeys::STATUS))
      load_properties(zk, set_status_flag)
      self
    end

    def load_status(_zk, js)
      s = js.fetch(:status, 'na').to_sym
      @status = states.fetch(s, nil)
    end

    def load_properties(zk, _set_status_flag)
      # Job will override
    end

    def string_property(zk, key)
      p = "#{path}/#{key}"
      arr = zk.get(p)
      raise MerrittZKNodeInvalid, "Node Object for (#{p}) missing" if arr.nil?

      arr[0]
    end

    def json_property(zk, key)
      s = string_property(zk, key)
      begin
        JSON.parse(s, symbolize_names: true)
      rescue StandardError
        raise MerrittZKNodeInvalid, "Node Object for (#{p}) does not contain valid json: #{s}"
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
      'na'
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

    def status_object(oldstat, status)
      defs = {
        last_modified: Time.now.to_s
      }
      stat = oldstat.nil? ? defs : oldstat
      stat[:status] = status.name
      stat
    end

    def set_status(zk, status, message = '')
      return if status == @status

      oldjson = nil
      oldjson = json_property(zk, ZkKeys::STATUS) if zk.exists?("#{path}/#{ZkKeys::STATUS}")

      json = status_object(oldjson, status)
      json[:message] = message
      data = QueueItem.serialize(json)
      if @status.nil?
        zk.create(status_path, data)
      else
        zk.set(status_path, data)
      end
      @status = status
    end

    def lock(zk)
      zk.create("#{path}/#{ZkKeys::LOCK}", data: nil, mode: :ephemeral)
    end

    def unlock(zk)
      zk.delete("#{path}/#{ZkKeys::LOCK}")
    end

    def data_prop(_prop, defval)
      return defval if @data.nil?

      @data.fetch(defval, '')
    end
  end
end
