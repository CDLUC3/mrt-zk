# frozen_string_literal: true

require 'zk'
require 'json'
require 'yaml'

module MerrittZK
  ##
  # Merritt Node Dump
  class NodeDump
    AGE_FAIL = 3600
    AGE_ALERT = 120

    def initialize(zk, myparams)
      @zk = zk
      @zkpath = myparams.fetch('zkpath', myparams.fetch(:zkpath, '/'))
      @mode = myparams.fetch('mode', myparams.fetch(:mode, 'data'))
      @listing = []
      @test_results = []
      @job_states_count = {}
      @listing.push({ Status: "Node State for [#{@zkpath}] as of #{Time.now}.  Mode: #{@mode}" })
      dump_node(@zkpath)
      return unless @mode == 'test'

      @job_states_count.each_value do |states|
        next unless states.length > 1

        states.each do |s|
          @test_results.append([s, node_datetime(s), s, 'Duplicate JID State', 'FAIL'])
      end
      @test_results.each do |rec|
        lrec = {}
        lrec[rec[0]] = rec
        @listing.push(lrec)
      end
    end

    attr_reader :listing

    def standard_node(n)
      n =~ %r{^/(access|batch-uuids|batches|jobs|locks|migration)(/|$)}
    end

    def system_node(n)
      n =~ %r{^/zookeeper(/|$)}
    end

    def show_data(n)
      d = get_data(n)
      if d.is_a?(Hash)
        df = JSON.pretty_generate(d)
        df = df.encode('UTF-8', invalid: :replace, undef: :replace, replace: '?')
        df = JSON.parse(df)
      elsif d.is_a?(String)
        df = d.encode('UTF-8', invalid: :replace, undef: :replace, replace: '?')
      else
        df = d
      end
      rec = {}
      rec[n] = df
      @listing.push(rec)
    rescue StandardError => e
      rec = {}
      rec[n] = e.to_s
      @listing.push(rec)
    end

    def get_data(n, defval = '')
      return defval unless @zk.exists?(n)

      d = @zk.get(n)[0]
      return defval if d.nil?

      begin
        JSON.parse(d.encode('UTF-8', invalid: :replace, undef: :replace, replace: '?'), symbolize_names: true)
      rescue JSON::ParserError
        d
      rescue StandardError => e
        "#{e.class}:#{e}: #{d}"
      end
    end

    def report_node(n)
      return if n == '/'

      unless standard_node(n)
        puts "Node #{n} Unsupported"
        return
      end

      if @mode == 'data'
        show_data(n)
      elsif @mode == 'test'
        show_test(n)
      else
        @listing.push(n)
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

    def node_datetime(n)
      return 'na' unless @zk.exists?(n)

      ctime = @zk.stat(n).ctime
      ctime.nil? ? 'na' : Time.at(ctime / 1000).strftime('%Y-%m-%d %H:%M:%S')
    end

    def node_stat(n)
      node_age(n, AGE_FAIL) ? 'WARN' : 'FAIL'
    end

    def node_age(n, age)
      return false unless @zk.exists?(n)

      ctime = @zk.stat(n).ctime
      return false if ctime.nil?

      Time.now - Time.at(ctime / 1000) < age
    end

    def test_node(path, deleteable, n)
      return if @zk.exists?(n)

      result = { path: path, test: "Test: #{n} should exist", status: node_stat(path) }
      @test_results.append([
        result[:path], node_datetime(path), deleteable ? result[:path] : '', result[:test],
        result[:status]
      ])
    end

    def test_node_age(path, age, deleteable, n)
      return unless @zk.exists?(n)
      return if node_age(n, age)

      result = { path: path, test: "Test: #{n} should exist", status: 'FAIL' }
      @test_results.append([
        result[:path], node_datetime(path), deleteable ? result[:path] : '', result[:test],
        result[:status]
      ])
    end

    def test_has_children(path, deleteable, n)
      return if @zk.exists?(n) && !@zk.children(n).empty?

      result = { path: path, test: "Test: #{n} should have children", status: node_stat(path) }
      @test_results.append([
        result[:path], node_datetime(path), deleteable ? result[:path] : '', result[:test],
        result[:status]
      ])
    end

    def test_not_node(path, deleteable, n)
      return unless @zk.exists?(n)

      result = { path: path, test: "Test: #{n} should NOT exist", status: node_stat(path) }
      @test_results.append([
        result[:path], node_datetime(path), deleteable ? n : '', result[:test],
        result[:status]
      ])
    end

    def show_test(n)
      rx1 = %r{^/batches/bid[0-9]+/states/batch-.*/(jid[0-9]+)$}
      rx2 = %r{^/jobs/(jid[0-9]+)/bid$}
      rx3 = %r{^/jobs/(jid[0-9]+)$}
      rx4 = %r{^/jobs/states/[^/]*/[0-9][0-9]-(jid[0-9]+)$}
      rx5 = %r{^/batches/bid[0-9]+/states$}
      rx6 = %r{^/batches/bid[0-9]+/lock$}
      rx7 = %r{^/jobs/(jid[0-9]+)/lock$}

      case n
      when %r{^/batch-uuids/(.*)}
        d = get_data(n)
        test_node(n, true, "/batches/#{d}")
      when %r{^/batches/bid[0-9]+/submission}
        d = get_data(n).fetch(:batchID, 'na')
        test_node(n, false, "/batch-uuids/#{d}")
      when rx1
        jid = rx1.match(n)[1]
        test_node(n, true, "/jobs/#{jid}")
      when rx2
        jid = rx2.match(n)[1]
        bid = get_data(n)
        test_node(n, false, "/batches/#{bid}")
        snode = "/jobs/#{jid}/status"
        test_node(n, true, snode)
        if @zk.exists?(snode)
          d = get_data(snode, {})
          status = d.fetch(:status, 'na').downcase
          bstatus = case status
                    when 'deleted'
                      'batch-deleted'
                    when 'completed'
                      'batch-completed'
                    when 'failed'
                      'batch-failed'
                    else
                      'batch-processing'
                    end
          test_node(n, false, "/batches/#{bid}/states/#{bstatus}/#{jid}")
          %w[batch-deleted batch-completed batch-failed batch-processing].each do |ts|
            next if ts == bstatus

            test_not_node(n, true, "/batches/#{bid}/states/#{ts}/#{jid}")
          end
        end
      when rx3
        jid = rx3.match(n)[1]
        snode = "/jobs/#{jid}/status"
        test_node(n, true, snode)
        if @zk.exists?(snode)
          d = get_data(snode, {})
          status = d.fetch(:status, 'na').downcase
          priority = get_data("#{n}/priority", 0)
          test_node(n, false, "/jobs/states/#{status}/#{format('%02d', priority)}-#{jid}")
        end
      when rx4
        jid = rx4.match(n)[1]
        test_node(n, true, "/jobs/#{jid}")
        @job_states_count[jid] = [] unless @job_states_count.key?(jid)
        @job_states_count[jid].append(n)
      when rx5
        test_has_children(n, true, n)
      when rx6
        test_node_age(n, AGE_ALERT, true, n)
      when rx7
        test_node_age(n, AGE_ALERT, true, n)
      end
    end
  end
end
