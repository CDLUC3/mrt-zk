require 'zk'
require 'json'
require 'yaml'

module MerrittZK

  class QueueItem

    def initialize(id, data: nil)
      @id = id
      @data = data
      @status = nil
    end

    attr_reader :id, :status

    def status_name
      status.name
    end

    def states
      {}
    end

    def load(zk)
      raise MerrittZKNodeInvalid.new("Missing Node #{path}") unless zk.exists?(path)
      load_status(zk, json_property(zk, 'status'))
      load_properties(zk)
      self
    end

    def load_status(zk, js)
      s = js.fetch('status', 'na').to_sym
      @status = states.fetch(s, nil)
    end

    def load_properties(zk)
      # Job will override
    end

    def string_property(zk, key)
      p = "#{path}/#{key}"
      arr = zk.get(p)
      raise MerrittZKNodeInvalid.new("Node Object for (#{p}) missing") if arr.nil?
      arr[0]
    end

    def json_property(zk, key)
      s = string_property(zk, key)
      begin
        JSON.parse(s)
      rescue
        raise MerrittZKNodeInvalid.new("Node Object for (#{p}) does not contain valid json: #{s}")
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
      "na"
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

    def set_status(zk, status)
      return if status == @status
      data = QueueItem.serialize(status_object(status))
      if @status.nil?
        zk.create(status_path, data)
      else
        zk.set(status_path, data)
      end
      @status = status
    end

    def lock(zk)
      zk.create("#{path}/lock", data: nil, mode: :ephemeral)
    end

    def unlock(zk)
      zk.delete("#{path}/lock")
    end

    def data_prop(prop, defval)
      return defval if @data.nil?
      @data.fetch(defval, '')
    end
  end

  class LegacyItem
    DIR = '/na'
    def dir
      DIR
    end

    def status_vals
      []
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

    def status_name
      return 'NA' if status_byte > status_vals.length
      status_vals[status_byte]
    end

    def is_json
      true
    end
  
    def time
      return nil if @bytes.length < 9
        # https://stackoverflow.com/a/68855488/3846548
        t = @bytes[1..8].inject(0) {|m, b| (m << 8) + b }
      Time.at(t/1000)
    end
  
    def payload_text
      return "" if @bytes.length < 10
      @bytes[9..].pack('c*')
    end

    def payload_object
      if @is_json
        json = JSON.parse(payload_text)
      else
        json = {
          payload: payload_text
        }
      end
      json['queueNode'] = dir
      json['id'] = @id
      json['date'] = time
      json['status'] = status_name
      json
    end

    attr_reader :id

  end

end