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

    def load(zk)
      raise MerrittZKNodeInvalid, "Missing Node #{path}" unless zk.exists?(path)

      load_status(zk, json_property(zk, ZkKeys::STATUS))
      load_properties(zk)
      self
    end

    def load_status(_zk, js)
      s = js.fetch(:status, 'na').to_sym
      @status = states.fetch(s, nil)
    end

    def load_properties(zk)
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

    def status_object(status)
      {
        status: status.name,
        last_modified: Time.now.to_s
      }
    end

    def set_status(zk, status, message = '')
      return if status == @status

      json = status_object(status)
      json[:message] = message unless message.empty?
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

  ##
  # Base class for Legacy Merritt ZooKeeper Queue Items.
  # This class will be removed after the migration is successfully completed
  class LegacyItem
    DIR = '/na'
    STATUS_VALS = %w[Pending Consumed Deleted Failed Completed Held].freeze
    def dir
      DIR
    end

    def path
      "#{dir}/#{id}"
    end

    def status_vals
      STATUS_VALS
    end

    def initialize(id)
      @id = id
      @data = {}
      @bytes = []
      @payload = {}
    end

    def load(zk)
      arr = zk.get("#{dir}/#{@id}")
      return if arr.nil?

      payload = arr[0]
      @bytes = payload.nil? ? [] : payload.bytes
      @payload = payload_object
    end

    def status_byte
      @bytes.empty? ? 0 : @bytes[0]
    end

    def status_name
      return 'NA' if status_byte > status_vals.length

      status_vals[status_byte]
    end

    def json?
      true
    end

    def time
      return nil if @bytes.length < 9

      # https://stackoverflow.com/a/68855488/3846548
      t = @bytes[1..8].inject(0) { |m, b| (m << 8) + b }
      Time.at(t / 1000)
    end

    def payload_text
      return '' if @bytes.length < 10

      @bytes[9..].pack('c*')
    end

    def payload_object
      json = { payload: payload_text }
      json = JSON.parse(payload_text, symbolize_names: true) if json?
      json[:queueNode] = dir
      json[:id] = @id
      json[:date] = time.to_s
      json[:status] = status_name
      json[:path] = path
      json
    end

    attr_reader :id
  end
end
