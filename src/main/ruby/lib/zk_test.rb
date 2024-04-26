require 'zk'
require 'json'
require 'yaml'

class MyZooTest
  def initialize
    @connstr = 'localhost:8084'
    @zk = ZK.new(@connstr)
    @tests = JSON.parse(
      YAML.safe_load(File.read('../../../test-cases.yml'), aliases: true).to_json,
      symbolize_names: true
    )
  end

  def zk_new
    ZK.new(@connstr)
  end

  def zk
    @zk
  end

  def tests
    @tests
  end

  def make_path(path, cp)
    return "/#{cp}" if path == '/'
    "#{path}/#{cp}"
  end

  def init
    @zk.create('/jobs', data: nil)
    @zk.create('/jobs/states', data: nil)
    @zk.create('/batches', data: nil)
    @zk.create('/access', data: nil)
    @zk.create('/access/small', data: nil)
    @zk.create('/access/large', data: nil)
    MerrittZK::Locks.init_locks(@zk)
  end

  def delete_all
    @zk.children('/').each do |cp|
      next if cp == 'zookeeper'
      @zk.rm_rf("/#{cp}")
    end
  end

  def list_all
    puts '-----'
    puts JSON.pretty_generate(list)
    puts '-----'
    puts
  end

  def skip_listing(path, data)
    # skip root path
    return true if path == '/'
    return true if path == '/batches'
    return true if path == '/jobs/states'
    return true if path == '/access'
    return true if path == '/access/small'
    return true if path == '/access/large'
    return true if path == '/locks/queue'
    return true if path == '/locks/storage'
    return true if path == '/locks/inventory'
    return true if path == '/locks/collections'
    if @zk.children(path).empty?
      # skip job states with no jobs
      return true if File.dirname(path) == '/jobs/states'
      # skip nodes with children, list children
      return false
    end
    # skip nodes with empty data
    return true if data.nil? || data.empty?
    false
  end

  def list(obj: {}, path: '/')
    unless path == '/zookeeper'
      data = @zk.get(path)[0]
      begin
        data = JSON.parse(data)
      rescue
      end
      obj[path.to_sym] = data unless skip_listing(path, data)
      begin
        @zk.children(path).each do |cp|
          list(obj: obj, path: make_path(path, cp))
        end
      rescue StandardError => e 
        puts "FAIL #{e.message}"
      end
    end
    obj
  end

  def serialize(v)
    return nil if v.nil?
    return v.to_json if v.is_a?(Hash)
    v.to_s
  end

  def set_config(data)
    return if data.nil?
    data.each do |k, v|
      @zk.mkdir_p(File.dirname(k.to_s))
      @zk.create(k.to_s, data: serialize(v))
    end
  end

  def input(name)
    tests.fetch(name, {}).fetch(:input, {})
  end

  def output(name)
    s = tests.fetch(name, {}).fetch(:output, {})
    JSON.parse(s.to_json)
  end

  def load_test(name)
    set_config(input(name))
  end

  def verify_output(name, remap: {})
    s = list.to_json
    remap.each do |k, v|
      s.gsub!(v, k.to_s)
    end
    curzk = JSON.parse(s)
    jout = output(name)
    return true if curzk == jout
    puts "---"
    curzk.keys.each do |k|
      next if jout.key?(k) && curzk[k] == jout[k]
      puts "#{k}:"
      puts "\tcur zk: [#{curzk[k]}]"
      puts "\tcur zk: nil" if curzk[k].nil?
      puts "\tyaml out: [#{jout[k]}]"
      puts "\tyaml out: nil" if jout[k].nil?
    end
    (jout.keys - curzk.keys).each do |k|
      puts "#{k}: missing from cur zk"
    end
    (curzk.keys - jout.keys).each do |k|
      puts "#{k}: missing from yaml out"
    end
    puts "---"
    false
  end
end