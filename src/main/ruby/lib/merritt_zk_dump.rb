# frozen_string_literal: true

require 'zk'
require 'json'
require 'yaml'

module MerrittZK
  ##
  # Merritt Node Dump
  class NodeDump
    def initialize(zk, myparams)
      @zk = zk
      @zkpath = myparams.fetch('zkpath', '/')
      @mode = myparams.fetch('mode', 'data')
      @listing = []
      @listing.push("Node State for [#{@zkpath}] as of #{Time.now}")
      dump_node(@zkpath)
    end

    attr_reader :listing

    def standard_node(n)
      n =~ %r{^/(access|batch-uuids|batches|jobs|locks|migration)(/|$)}
    end

    def system_node(n)
      n =~ %r{^/zookeeper(/|$)}
    end

    def show_data(n)
      puts "Show #{n}"
      d = get_data(n)
      if d.is_a?(Hash)
        df = JSON.pretty_generate(d)
        df = df.encode('UTF-8', invalid: :replace, undef: :replace, replace: '?')
        df = JSON.parse(df)
      elsif d.is_a?(String)
        df = d.encode('UTF-8', invalid: :replace, undef: :replace, replace: '?')
      else
        df = d.to_s
      end
      rec = {}
      rec[n] = df
      @listing.push(rec)
    rescue StandardError => e
      rec = {}
      rec[n] = e.to_s
      @listing.push(rec)
    end

    def get_data(n)
      d = @zk.get(n)[0]
      return '' if d.nil?

      begin
        JSON.parse(d.encode('UTF-8', invalid: :replace, undef: :replace, replace: '?'), symbolize_names: true)
      rescue JSON::ParserError
        d
      rescue StandardError => e
        "#{e.class}:#{e}: #{d}"
      end
    end

    def report_node(n)
      if standard_node(n)
        show_data(n) if @mode == 'data'
        show_test(n) if @mode == 'test'
      else
        @listing.push("#{n} Unsupported")
      end
    end

    def dump_node(n = '/')
      return unless @zk.exists?(n)
      return if system_node(n)

      report_node(n)
      arr = @zk.children(n)
      return if arr.empty?

      arr.sort.each do |cp|
        p = "#{n}/#{cp}".gsub(%r{/+}, '/')
        dump_node(p)
      end
    end
  end
end
