# frozen_string_literal: true

require 'zk'
require 'json'
require 'yaml'

require 'spec_helper'
require_relative '../lib/merritt_zk'
require_relative '../lib/zk_test'

RSpec.describe 'ZK input/ouput tests' do
  before(:all) do
    @zkt = MyZooTest.new
    @zk = @zkt.zk
  end

  before(:each) do |x|
    @zkt.delete_all
    @zkt.init
    @remap = { now: /\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d .\d\d\d\d/ }
    # Note that the IT test name matches a key in the test-cases.yml file
    @zkt.load_test(x.description.to_sym)
  end

  after(:each) do |x|
    # Note that the IT test name matches a key in the test-cases.yml file
    unless @zkt.output(x.description.to_sym).nil?
      expect(@zkt.verify_output(x.description.to_sym, remap: @remap)).to be(true)
    end
  end

  describe 'Test read/write from ZK' do
    # Note that the IT test name matches a key in the test-cases.yml file
    it :test_read_write_from_zk do |x|
      # no action
    end

    it :test_sequential_node_mapping do |_x|
      @remap['/foo0'] = @zk.create('/foo', data: nil, mode: :persistent_sequential)
      @remap['/foo1'] = @zk.create('/foo', data: nil, mode: :persistent_sequential)
      @remap['/foo2'] = @zk.create('/foo', data: nil, mode: :persistent_sequential)
    end
  end

  describe 'Test ephemeral node behavior' do
    it :ephemeral_node_remaining do |_x|
      @zk.create('/foo', data: nil, mode: :ephemeral)
    end

    it :ephemeral_node_destroyed do |_x|
      tzk = @zkt.zk_new
      tzk.create('/foo', data: nil, mode: :ephemeral)
      tzk.close
    end
  end

  def make_batch_json(s = 'bar', u = 'bid-uuid')
    {
      foo: s,
      batchID: u
    }
  end

  describe 'Test Batch Creation' do
    it :create_batch do |_x|
      b = MerrittZK::Batch.create_batch(@zk, make_batch_json)
      @remap['bid0'] = b.id
    end

    it :create_and_load_batch do |_x|
      b = MerrittZK::Batch.create_batch(@zk, make_batch_json)
      @remap['bid0'] = b.id
      b2 = MerrittZK::Batch.new(b.id).load(@zk)
      expect(b2.status.status).to eq(:Pending)
    end

    it :load_non_existent_batch do |_x|
      expect do
        MerrittZK::Batch.new(111).load(@zk)
      end.to raise_error(MerrittZK::MerrittZKNodeInvalid, /.*/)
    end
  end

  describe 'Test Batch Locking' do
    it :batch_with_lock_unlock do |_x|
      b = MerrittZK::Batch.create_batch(@zk, make_batch_json)
      @remap['bid0'] = b.id
      b.lock(@zk)
      b.unlock(@zk)
    end

    it :batch_with_ephemeral_released_lock do |_x|
      b = MerrittZK::Batch.create_batch(@zk, make_batch_json)
      @remap['bid0'] = b.id
      tzk = @zkt.zk_new
      b.lock(tzk)
      tzk.close
    end

    it :batch_with_unreleased_lock do |_x|
      b = MerrittZK::Batch.create_batch(@zk, make_batch_json)
      @remap['bid0'] = b.id
      b.lock(@zk)
    end

    it :batch_acquire do |_x|
      b = MerrittZK::Batch.create_batch(@zk, make_batch_json)
      b2 = MerrittZK::Batch.create_batch(@zk, make_batch_json('bar2', 'bid-uuid2'))
      @remap['bid0'] = b.id
      @remap['bid1'] = b2.id
      bb = MerrittZK::Batch.acquire_pending_batch(@zk)
      expect(bb).to_not be_nil
      expect(bb.status.status).to eq(:Processing)
      bb = MerrittZK::Batch.acquire_pending_batch(@zk)
      expect(bb).to_not be_nil
      bb = MerrittZK::Batch.acquire_pending_batch(@zk)
      expect(bb).to be_nil
    end
  end

  describe 'Test Batch State Changes' do
    it :modify_batch_state do |_x|
      b = MerrittZK::Batch.create_batch(@zk, make_batch_json)
      @remap['bid0'] = b.id
      b.set_status(@zk, b.status.state_change(:Processing))
    end
  end

  describe 'Test Job Creation' do
    it :create_job do |_x|
      b = MerrittZK::Batch.create_batch(@zk, make_batch_json)
      @remap['bid0'] = b.id
      bb = MerrittZK::Batch.acquire_pending_batch(@zk)
      expect(b.id).to eq(bb.id)
      j = MerrittZK::Job.create_job(@zk, bb.id, { job: 'quack' })
      @remap['jid0'] = j.id
      expect(j.bid).to eq(bb.id)

      puts 'Data dump'
      params = { zkpath: '/', mode: 'data' }
      puts MerrittZK::NodeDump.new(@zk, params).listing
      puts 'Node List'
      params = { zkpath: '/', mode: 'node' }
      puts MerrittZK::NodeDump.new(@zk, params).listing
      puts 'Node Test'
      params = { zkpath: '/', mode: 'test' }
      puts MerrittZK::NodeDump.new(@zk, params).listing
    end

    it :create_job_state_change do |_x|
      b = MerrittZK::Batch.create_batch(@zk, make_batch_json)
      @remap['bid0'] = b.id
      bb = MerrittZK::Batch.acquire_pending_batch(@zk)
      expect(b.id).to eq(bb.id)
      j = MerrittZK::Job.create_job(@zk, bb.id, { job: 'quack' })
      @remap['jid0'] = j.id
      expect(j.bid).to eq(bb.id)
      j.set_status(@zk, j.status.state_change(:Estimating))
    end

    it :load_job_state_change do |_x|
      b = MerrittZK::Batch.create_batch(@zk, make_batch_json)
      @remap['bid0'] = b.id
      bb = MerrittZK::Batch.acquire_pending_batch(@zk)
      expect(b.id).to eq(bb.id)
      j = MerrittZK::Job.create_job(@zk, bb.id, { job: 'quack' })
      @remap['jid0'] = j.id
      expect(j.bid).to eq(bb.id)
      jj = MerrittZK::Job.new(j.id).load(@zk)
      jj.set_status(@zk, jj.status.state_change(:Estimating))
    end

    it :acquire_pending_job do |_x|
      b = MerrittZK::Batch.create_batch(@zk, make_batch_json)
      @remap['bid0'] = b.id
      bb = MerrittZK::Batch.acquire_pending_batch(@zk)
      expect(b.id).to eq(bb.id)
      j = MerrittZK::Job.create_job(@zk, bb.id, { job: 'quack' })
      @remap['jid0'] = j.id
      expect(j.bid).to eq(bb.id)
      bb.unlock(@zk)

      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState::Pending)
      expect(jj).to_not be_nil
      jj.set_status(@zk, jj.status.state_change(:Estimating))
      jj2 = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState::Pending)
      expect(jj2).to be_nil
    end

    it :acquire_lowest_priority_job do |_x|
      b = MerrittZK::Batch.create_batch(@zk, make_batch_json)
      @remap['bid0'] = b.id
      bb = MerrittZK::Batch.acquire_pending_batch(@zk)
      expect(b.id).to eq(bb.id)

      j = MerrittZK::Job.create_job(@zk, bb.id, { job: 'quack1' })
      @remap['jid0'] = j.id

      j = MerrittZK::Job.create_job(@zk, bb.id, { job: 'quack2' })
      @remap['jid1'] = j.id
      j.set_status_with_priority(@zk, MerrittZK::JobState::Pending, 2)

      j = MerrittZK::Job.create_job(@zk, bb.id, { job: 'quack3' })
      @remap['jid2'] = j.id

      bb.unlock(@zk)

      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState::Pending)
      expect(jj.id).to eq(@remap['jid1'])
      jj.set_status(@zk, jj.status.state_change(:Estimating))
    end

    it :job_happy_path do |_x|
      b = MerrittZK::Batch.create_batch(@zk, make_batch_json)
      @remap['bid0'] = b.id
      bb = MerrittZK::Batch.acquire_pending_batch(@zk)
      expect(b.id).to eq(bb.id)

      j = MerrittZK::Job.create_job(@zk, bb.id, { job: 'quack1' })
      @remap['jid0'] = j.id

      j = MerrittZK::Job.create_job(@zk, bb.id, { job: 'quack2' })
      @remap['jid1'] = j.id
      j.set_status_with_priority(@zk, MerrittZK::JobState::Pending, 2)

      j = MerrittZK::Job.create_job(@zk, bb.id, { job: 'quack3' })
      @remap['jid2'] = j.id

      bb.unlock(@zk)

      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState::Pending)
      expect(jj.id).to eq(@remap['jid1'])
      jj.set_status(@zk, jj.status.state_change(:Estimating))
      jj.unlock(@zk)

      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState::Estimating)
      expect(jj).to_not be_nil
      expect(jj.id).to eq(@remap['jid1'])
      jj.set_status(@zk, jj.status.success)
      jj.unlock(@zk)
      expect(jj.status.status).to eq(:Provisioning)

      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState::Provisioning)
      expect(jj).to_not be_nil
      expect(jj.id).to eq(@remap['jid1'])
      jj.set_status(@zk, jj.status.success)
      jj.unlock(@zk)
      expect(jj.status.status).to eq(:Downloading)

      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState::Downloading)
      expect(jj).to_not be_nil
      expect(jj.id).to eq(@remap['jid1'])
      jj.set_status(@zk, jj.status.success)
      jj.unlock(@zk)
      expect(jj.status.status).to eq(:Processing)

      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState::Processing)
      expect(jj).to_not be_nil
      expect(jj.id).to eq(@remap['jid1'])
      jj.set_status(@zk, jj.status.success)
      jj.set_data(@zk, MerrittZK::ZkKeys::INVENTORY, { manifest_url: 'http://storage.manifest.url', mode: 'tbd' })
      jj.unlock(@zk)
      expect(jj.status.status).to eq(:Recording)

      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState::Recording)
      expect(jj).to_not be_nil
      expect(jj.id).to eq(@remap['jid1'])
      inv = jj.json_property(@zk, MerrittZK::ZkKeys::INVENTORY)
      expect(inv.fetch(:manifest_url, '')).to eq('http://storage.manifest.url')
      expect(inv.fetch(:mode, '')).to eq('tbd')
      jj.set_status(@zk, jj.status.success)
      jj.unlock(@zk)
      expect(jj.status.status).to eq(:Notify)

      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState::Notify)
      expect(jj).to_not be_nil
      expect(jj.id).to eq(@remap['jid1'])
      jj.set_status(@zk, jj.status.success)
      jj.unlock(@zk)
      expect(jj.status.status).to eq(:Completed)
      expect(jj.status.deletable?).to be(true)

      arr = bb.get_processing_jobs(@zk)
      expect(arr.length).to eq(2)
      arr = bb.get_completed_jobs(@zk)
      expect(arr.length).to eq(1)
      expect(arr[0].id).to eq(@remap['jid1'])
      arr = bb.get_failed_jobs(@zk)
      expect(arr.length).to eq(0)

      # Only for Ruby interface
      arr = MerrittZK::Job.list_jobs_as_json(@zk)
      expect(arr.length).to eq(3)
    end

    it :batch_happy_path do |_x|
      b = MerrittZK::Batch.create_batch(@zk, make_batch_json)
      @remap['bid0'] = b.id
      bb = MerrittZK::Batch.acquire_pending_batch(@zk)
      expect(b.id).to eq(bb.id)

      j = MerrittZK::Job.create_job(@zk, bb.id, { job: 'quack2' })
      @remap['jid1'] = j.id
      j.set_status_with_priority(@zk, MerrittZK::JobState::Pending, 2)

      bb.unlock(@zk)

      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState::Pending)
      expect(jj.id).to eq(@remap['jid1'])
      jj.set_status(@zk, jj.status.state_change(:Estimating))
      jj.unlock(@zk)

      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState::Estimating)
      expect(jj).to_not be_nil
      expect(jj.id).to eq(@remap['jid1'])
      jj.set_status(@zk, jj.status.success)
      jj.unlock(@zk)
      expect(jj.status.status).to eq(:Provisioning)

      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState::Provisioning)
      expect(jj).to_not be_nil
      expect(jj.id).to eq(@remap['jid1'])
      jj.set_status(@zk, jj.status.success)
      jj.unlock(@zk)
      expect(jj.status.status).to eq(:Downloading)

      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState::Downloading)
      expect(jj).to_not be_nil
      expect(jj.id).to eq(@remap['jid1'])
      jj.set_status(@zk, jj.status.success)
      jj.unlock(@zk)
      expect(jj.status.status).to eq(:Processing)

      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState::Processing)
      expect(jj).to_not be_nil
      expect(jj.id).to eq(@remap['jid1'])
      jj.set_status(@zk, jj.status.success)
      jj.unlock(@zk)
      expect(jj.status.status).to eq(:Recording)

      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState::Recording)
      expect(jj).to_not be_nil
      expect(jj.id).to eq(@remap['jid1'])
      jj.set_status(@zk, jj.status.success)
      jj.unlock(@zk)
      expect(jj.status.status).to eq(:Notify)

      bbb = MerrittZK::Batch.acquire_batch_for_reporting_batch(@zk)
      expect(bbb).to be_nil

      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState::Notify)
      expect(jj).to_not be_nil
      expect(jj.id).to eq(@remap['jid1'])
      jj.set_status(@zk, jj.status.success)
      jj.unlock(@zk)
      expect(jj.status.status).to eq(:Completed)

      bbb = MerrittZK::Batch.acquire_batch_for_reporting_batch(@zk)
      expect(bbb).to_not be_nil
      expect(bbb.status.status).to be(:Reporting)
      expect(bbb.has_failure).to be(false)
      bbb.set_status(@zk, bbb.status.success)
      bbb.unlock(@zk)

      expect(bbb.status.status).to eq(:Completed)
      expect(bbb.status.deletable?).to be(true)

      bbbx = MerrittZK::Batch.acquire_batch_for_reporting_batch(@zk)
      expect(bbbx).to be_nil

      bbbb = MerrittZK::Batch.new(bbb.id).load(@zk)
      expect(bbbb.status.status).to eq(:Completed)
      expect(bbbb.has_failure).to be(false)
    end

    it :batch_failure do |_x|
      b = MerrittZK::Batch.create_batch(@zk, make_batch_json)
      @remap['bid0'] = b.id
      bb = MerrittZK::Batch.acquire_pending_batch(@zk)
      expect(b.id).to eq(bb.id)

      j = MerrittZK::Job.create_job(@zk, bb.id, { job: 'quack2' })
      @remap['jid1'] = j.id
      j.set_status_with_priority(@zk, MerrittZK::JobState::Pending, 2)

      bb.unlock(@zk)

      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState::Pending)
      expect(jj.id).to eq(@remap['jid1'])
      jj.set_status(@zk, jj.status.state_change(:Estimating))
      jj.unlock(@zk)

      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState::Estimating)
      expect(jj).to_not be_nil
      expect(jj.id).to eq(@remap['jid1'])
      jj.set_status(@zk, jj.status.success)
      jj.unlock(@zk)
      expect(jj.status.status).to eq(:Provisioning)

      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState::Provisioning)
      expect(jj).to_not be_nil
      expect(jj.id).to eq(@remap['jid1'])
      jj.set_status(@zk, jj.status.success)
      jj.unlock(@zk)
      expect(jj.status.status).to eq(:Downloading)

      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState::Downloading)
      expect(jj).to_not be_nil
      expect(jj.id).to eq(@remap['jid1'])
      jj.set_status(@zk, jj.status.success)
      jj.unlock(@zk)
      expect(jj.status.status).to eq(:Processing)

      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState::Processing)
      expect(jj).to_not be_nil
      expect(jj.id).to eq(@remap['jid1'])
      jj.set_status(@zk, jj.status.success)
      jj.unlock(@zk)
      expect(jj.status.status).to eq(:Recording)

      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState::Recording)
      expect(jj).to_not be_nil
      expect(jj.id).to eq(@remap['jid1'])
      jj.set_status(@zk, jj.status.success)
      jj.unlock(@zk)
      expect(jj.status.status).to eq(:Notify)

      bbb = MerrittZK::Batch.acquire_batch_for_reporting_batch(@zk)
      expect(bbb).to be_nil

      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState::Notify)
      expect(jj).to_not be_nil
      expect(jj.id).to eq(@remap['jid1'])
      jj.set_status(@zk, jj.status.fail, 'Sample Failure Message')
      jj.unlock(@zk)
      expect(jj.status.status).to eq(:Failed)

      bbb = MerrittZK::Batch.acquire_batch_for_reporting_batch(@zk)
      expect(bbb).to_not be_nil
      expect(bbb.status.status).to be(:Reporting)
      expect(bbb.has_failure).to be(true)
      bbb.set_status(@zk, bbb.status.fail)
      bbb.unlock(@zk)

      expect(bbb.status.status).to eq(:Failed)

      bbbb = MerrittZK::Batch.new(bbb.id).load(@zk)
      expect(bbbb.status.status).to eq(:Failed)
      expect(bbbb.has_failure).to be(true)

      arr = bbbb.get_processing_jobs(@zk)
      expect(arr.length).to eq(0)
      arr = bbbb.get_completed_jobs(@zk)
      expect(arr.length).to eq(0)
      arr = bbbb.get_failed_jobs(@zk)
      expect(arr.length).to eq(1)
      expect(arr[0].id).to eq(@remap['jid1'])

      # Only for Ruby interface
      arr = MerrittZK::Batch.list_batches_as_json(@zk)
      expect(arr.length).to eq(1)

      jj.set_status(@zk, MerrittZK::JobState::Deleted)

      bbbb.set_status(@zk, MerrittZK::BatchState::Deleted)
      expect(bbbb.status.status).to eq(:Deleted)
      expect(bbbb.status.deletable?).to be(true)
    end

    it :batch_recovery do |_x|
      b = MerrittZK::Batch.create_batch(@zk, make_batch_json)
      @remap['bid0'] = b.id
      bb = MerrittZK::Batch.acquire_pending_batch(@zk)
      expect(b.id).to eq(bb.id)

      j = MerrittZK::Job.create_job(@zk, bb.id, { job: 'quack2' })
      @remap['jid1'] = j.id
      j.set_status_with_priority(@zk, MerrittZK::JobState::Pending, 2)

      bb.unlock(@zk)

      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState::Pending)
      expect(jj.id).to eq(@remap['jid1'])
      jj.set_status(@zk, jj.status.state_change(:Estimating))
      jj.unlock(@zk)

      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState::Estimating)
      expect(jj).to_not be_nil
      expect(jj.id).to eq(@remap['jid1'])
      jj.set_status(@zk, jj.status.success)
      jj.unlock(@zk)
      expect(jj.status.status).to eq(:Provisioning)

      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState::Provisioning)
      expect(jj).to_not be_nil
      expect(jj.id).to eq(@remap['jid1'])
      jj.set_status(@zk, jj.status.success)
      jj.unlock(@zk)
      expect(jj.status.status).to eq(:Downloading)

      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState::Downloading)
      expect(jj).to_not be_nil
      expect(jj.id).to eq(@remap['jid1'])
      jj.set_status(@zk, jj.status.success)
      jj.unlock(@zk)
      expect(jj.status.status).to eq(:Processing)

      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState::Processing)
      expect(jj).to_not be_nil
      expect(jj.id).to eq(@remap['jid1'])
      jj.set_status(@zk, jj.status.success)
      jj.unlock(@zk)
      expect(jj.status.status).to eq(:Recording)

      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState::Recording)
      expect(jj).to_not be_nil
      expect(jj.id).to eq(@remap['jid1'])
      jj.set_status(@zk, jj.status.success)
      jj.unlock(@zk)
      expect(jj.status.status).to eq(:Notify)

      bbb = MerrittZK::Batch.acquire_batch_for_reporting_batch(@zk)
      expect(bbb).to be_nil

      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState::Notify)
      expect(jj).to_not be_nil
      expect(jj.id).to eq(@remap['jid1'])
      jj.set_status(@zk, jj.status.fail)
      jj.unlock(@zk)
      expect(jj.status.status).to eq(:Failed)

      bbb = MerrittZK::Batch.acquire_batch_for_reporting_batch(@zk)
      expect(bbb).to_not be_nil
      expect(bbb.status.status).to be(:Reporting)
      expect(bbb.has_failure).to be(true)
      bbb.set_status(@zk, bbb.status.fail)
      bbb.unlock(@zk)

      expect(bbb.status.status).to eq(:Failed)

      jjj = MerrittZK::Job.new(jj.id).load(@zk)
      jjj.set_status(@zk, MerrittZK::JobState::Notify, job_retry: true)

      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState::Notify)
      expect(jj).to_not be_nil
      expect(jj.id).to eq(@remap['jid1'])
      jj.set_status(@zk, jj.status.success)
      jj.unlock(@zk)
      expect(jj.status.status).to eq(:Completed)

      bbbb = MerrittZK::Batch.new(bbb.id).load(@zk)
      expect(bbbb.status.status).to eq(:Failed)
      expect(bbbb.has_failure).to be(false)

      bbbb.set_status(@zk, MerrittZK::BatchState::UpdateReporting)
      expect(bbbb.status.deletable?).to be(false)

      bbbb.set_status(@zk, bbbb.status.success)
      expect(bbbb.status.status).to eq(:Completed)
      expect(bbbb.status.deletable?).to be(true)
    end

    it :job_happy_path_with_delete do |_x|
      b = MerrittZK::Batch.create_batch(@zk, make_batch_json)
      @remap['bid0'] = b.id
      bb = MerrittZK::Batch.acquire_pending_batch(@zk)
      expect(b.id).to eq(bb.id)

      j = MerrittZK::Job.create_job(@zk, bb.id, { job: 'quack1' })
      @remap['jid0'] = j.id

      j = MerrittZK::Job.create_job(@zk, bb.id, { job: 'quack2' })
      @remap['jid1'] = j.id
      j.set_status_with_priority(@zk, MerrittZK::JobState::Pending, 2)

      j = MerrittZK::Job.create_job(@zk, bb.id, { job: 'quack3' })
      @remap['jid2'] = j.id

      bb.unlock(@zk)

      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState::Pending)
      expect(jj.id).to eq(@remap['jid1'])
      jj.set_status(@zk, jj.status.state_change(:Estimating))
      jj.unlock(@zk)

      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState::Estimating)
      expect(jj).to_not be_nil
      expect(jj.id).to eq(@remap['jid1'])
      jj.set_status(@zk, jj.status.success)
      jj.unlock(@zk)
      expect(jj.status.status).to eq(:Provisioning)

      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState::Provisioning)
      expect(jj).to_not be_nil
      expect(jj.id).to eq(@remap['jid1'])
      jj.set_status(@zk, jj.status.success)
      jj.unlock(@zk)
      expect(jj.status.status).to eq(:Downloading)

      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState::Downloading)
      expect(jj).to_not be_nil
      expect(jj.id).to eq(@remap['jid1'])
      jj.set_status(@zk, jj.status.success)
      jj.unlock(@zk)
      expect(jj.status.status).to eq(:Processing)

      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState::Processing)
      expect(jj).to_not be_nil
      expect(jj.id).to eq(@remap['jid1'])
      jj.set_status(@zk, jj.status.success)
      jj.unlock(@zk)
      expect(jj.status.status).to eq(:Recording)

      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState::Recording)
      expect(jj).to_not be_nil
      expect(jj.id).to eq(@remap['jid1'])
      jj.set_status(@zk, jj.status.success)
      jj.unlock(@zk)
      expect(jj.status.status).to eq(:Notify)

      expect do
        jj.delete(@zk)
      end.to raise_error(MerrittZK::MerrittStateError, /.*/)

      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState::Notify)
      expect(jj).to_not be_nil
      expect(jj.id).to eq(@remap['jid1'])
      jj.set_status(@zk, jj.status.success)
      jj.unlock(@zk)
      expect(jj.status.status).to eq(:Completed)
      expect(jj.status.deletable?).to be(true)

      jj.delete(@zk)
    end

    it :batch_happy_path_with_delete do |_x|
      b = MerrittZK::Batch.create_batch(@zk, make_batch_json)
      @remap['bid0'] = b.id
      bb = MerrittZK::Batch.acquire_pending_batch(@zk)
      expect(b.id).to eq(bb.id)

      j = MerrittZK::Job.create_job(@zk, bb.id, { job: 'quack2' })
      @remap['jid1'] = j.id
      j.set_status_with_priority(@zk, MerrittZK::JobState::Pending, 2)

      bb.unlock(@zk)

      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState::Pending)
      expect(jj.id).to eq(@remap['jid1'])
      jj.set_status(@zk, jj.status.state_change(:Estimating))
      jj.unlock(@zk)

      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState::Estimating)
      expect(jj).to_not be_nil
      expect(jj.id).to eq(@remap['jid1'])
      jj.set_status(@zk, jj.status.success)
      jj.unlock(@zk)
      expect(jj.status.status).to eq(:Provisioning)

      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState::Provisioning)
      expect(jj).to_not be_nil
      expect(jj.id).to eq(@remap['jid1'])
      jj.set_status(@zk, jj.status.success)
      jj.unlock(@zk)
      expect(jj.status.status).to eq(:Downloading)

      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState::Downloading)
      expect(jj).to_not be_nil
      expect(jj.id).to eq(@remap['jid1'])
      jj.set_status(@zk, jj.status.success)
      jj.unlock(@zk)
      expect(jj.status.status).to eq(:Processing)

      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState::Processing)
      expect(jj).to_not be_nil
      expect(jj.id).to eq(@remap['jid1'])
      jj.set_status(@zk, jj.status.success)
      jj.unlock(@zk)
      expect(jj.status.status).to eq(:Recording)

      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState::Recording)
      expect(jj).to_not be_nil
      expect(jj.id).to eq(@remap['jid1'])
      jj.set_status(@zk, jj.status.success)
      jj.unlock(@zk)
      expect(jj.status.status).to eq(:Notify)

      bbb = MerrittZK::Batch.acquire_batch_for_reporting_batch(@zk)
      expect(bbb).to be_nil

      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState::Notify)
      expect(jj).to_not be_nil
      expect(jj.id).to eq(@remap['jid1'])
      jj.set_status(@zk, jj.status.success)
      jj.unlock(@zk)
      expect(jj.status.status).to eq(:Completed)

      bbb = MerrittZK::Batch.acquire_batch_for_reporting_batch(@zk)
      expect(bbb).to_not be_nil
      expect(bbb.status.status).to be(:Reporting)
      expect(bbb.has_failure).to be(false)
      bbb.set_status(@zk, bbb.status.success)
      bbb.unlock(@zk)

      expect(bbb.status.status).to eq(:Completed)
      expect(bbb.status.deletable?).to be(true)

      bbbb = MerrittZK::Batch.new(bbb.id).load(@zk)
      expect(bbbb.status.status).to eq(:Completed)
      expect(bbbb.has_failure).to be(false)

      bbbb.delete(@zk)
    end

    it :batch_happy_path_with_delete_completed do |_x|
      b = MerrittZK::Batch.create_batch(@zk, make_batch_json)
      @remap['bid0'] = b.id
      bb = MerrittZK::Batch.acquire_pending_batch(@zk)
      expect(b.id).to eq(bb.id)

      j = MerrittZK::Job.create_job(@zk, bb.id, { job: 'quack2' })
      @remap['jid1'] = j.id
      j.set_status_with_priority(@zk, MerrittZK::JobState::Pending, 2)

      bb.unlock(@zk)

      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState::Pending)
      expect(jj.id).to eq(@remap['jid1'])
      jj.set_status(@zk, jj.status.state_change(:Estimating))
      jj.unlock(@zk)

      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState::Estimating)
      expect(jj).to_not be_nil
      expect(jj.id).to eq(@remap['jid1'])
      jj.set_status(@zk, jj.status.success)
      jj.unlock(@zk)
      expect(jj.status.status).to eq(:Provisioning)

      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState::Provisioning)
      expect(jj).to_not be_nil
      expect(jj.id).to eq(@remap['jid1'])
      jj.set_status(@zk, jj.status.success)
      jj.unlock(@zk)
      expect(jj.status.status).to eq(:Downloading)

      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState::Downloading)
      expect(jj).to_not be_nil
      expect(jj.id).to eq(@remap['jid1'])
      jj.set_status(@zk, jj.status.success)
      jj.unlock(@zk)
      expect(jj.status.status).to eq(:Processing)

      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState::Processing)
      expect(jj).to_not be_nil
      expect(jj.id).to eq(@remap['jid1'])
      jj.set_status(@zk, jj.status.success)
      jj.unlock(@zk)
      expect(jj.status.status).to eq(:Recording)

      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState::Recording)
      expect(jj).to_not be_nil
      expect(jj.id).to eq(@remap['jid1'])
      jj.set_status(@zk, jj.status.success)
      jj.unlock(@zk)
      expect(jj.status.status).to eq(:Notify)

      bbb = MerrittZK::Batch.acquire_batch_for_reporting_batch(@zk)
      expect(bbb).to be_nil

      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState::Notify)
      expect(jj).to_not be_nil
      expect(jj.id).to eq(@remap['jid1'])
      jj.set_status(@zk, jj.status.success)
      jj.unlock(@zk)
      expect(jj.status.status).to eq(:Completed)

      bbb = MerrittZK::Batch.acquire_batch_for_reporting_batch(@zk)
      expect(bbb).to_not be_nil
      expect(bbb.status.status).to be(:Reporting)
      expect(bbb.has_failure).to be(false)
      bbb.set_status(@zk, bbb.status.success)
      bbb.unlock(@zk)

      expect(bbb.status.status).to eq(:Completed)
      expect(bbb.status.deletable?).to be(true)

      bbbb = MerrittZK::Batch.new(bbb.id).load(@zk)
      expect(bbbb.status.status).to eq(:Completed)
      expect(bbbb.has_failure).to be(false)

      ids = MerrittZK::Batch.delete_completed_batches(@zk)
      expect(ids.include?(bbbb.id)).to be(true)
    end

    it :job_create_config do |_x|
      b = MerrittZK::Batch.create_batch(@zk, make_batch_json)
      @remap['bid0'] = b.id
      bb = MerrittZK::Batch.acquire_pending_batch(@zk)
      expect(b.id).to eq(bb.id)
      jc = { profile_name: 'a', submitter: 'b', payload_url: 'c', payload_type: 'd', response_type: 'e' }
      j = MerrittZK::Job.create_job(@zk, bb.id, jc)
      @remap['jid0'] = j.id
      expect(j.bid).to eq(bb.id)
      bb.unlock(@zk)
    end

    it :job_create_config_ident do |_x|
      b = MerrittZK::Batch.create_batch(@zk, make_batch_json)
      @remap['bid0'] = b.id
      bb = MerrittZK::Batch.acquire_pending_batch(@zk)
      expect(b.id).to eq(bb.id)
      jc = { profile_name: 'a', submitter: 'b', payload_url: 'c', payload_type: 'd', response_type: 'e' }
      ji = { primary_id: 'f', local_id: %w[g h] }
      j = MerrittZK::Job.create_job(@zk, bb.id, jc, identifiers: ji)
      @remap['jid0'] = j.id
      expect(j.bid).to eq(bb.id)
      bb.unlock(@zk)
    end

    it :job_create_config_ident_metadata do |_x|
      b = MerrittZK::Batch.create_batch(@zk, make_batch_json)
      @remap['bid0'] = b.id
      bb = MerrittZK::Batch.acquire_pending_batch(@zk)
      expect(b.id).to eq(bb.id)
      jc = { profile_name: 'a', submitter: 'b', payload_url: 'c', payload_type: 'd', response_type: 'e' }
      ji = { primary_id: 'f', local_id: %w[g h] }
      jm = { erc_who: 'i', erc_what: 'j', erc_when: 'k', erc_where: 'l' }
      j = MerrittZK::Job.create_job(@zk, bb.id, jc, identifiers: ji, metadata: jm)
      @remap['jid0'] = j.id
      expect(j.bid).to eq(bb.id)
      bb.unlock(@zk)
    end
  end

  describe 'Lock tests' do
    it :lock_ingest do |_x|
      MerrittZK::Locks.unlock_ingest_queue(@zk)

      expect(MerrittZK::Locks.check_lock_ingest_queue(@zk)).to be(false)
      expect(MerrittZK::Locks.lock_ingest_queue(@zk)).to be(true)
      expect(MerrittZK::Locks.check_lock_ingest_queue(@zk)).to be(true)
      expect(MerrittZK::Locks.lock_ingest_queue(@zk)).to be(false)
      MerrittZK::Locks.unlock_ingest_queue(@zk)
      expect(MerrittZK::Locks.lock_ingest_queue(@zk)).to be(true)
    end

    it :lock_access do |_x|
      expect(MerrittZK::Locks.check_lock_large_access_queue(@zk)).to be(false)
      expect(MerrittZK::Locks.lock_large_access_queue(@zk)).to be(true)
      expect(MerrittZK::Locks.check_lock_large_access_queue(@zk)).to be(true)
      expect(MerrittZK::Locks.lock_large_access_queue(@zk)).to be(false)
      MerrittZK::Locks.unlock_large_access_queue(@zk)
      expect(MerrittZK::Locks.lock_large_access_queue(@zk)).to be(true)

      expect(MerrittZK::Locks.lock_small_access_queue(@zk)).to be(true)
      expect(MerrittZK::Locks.lock_small_access_queue(@zk)).to be(false)
      MerrittZK::Locks.unlock_small_access_queue(@zk)
      expect(MerrittZK::Locks.lock_small_access_queue(@zk)).to be(true)
    end

    it :lock_collection do |_x|
      expect(MerrittZK::Locks.check_lock_collection(@zk, 'foo')).to be(false)
      MerrittZK::Locks.lock_collection(@zk, 'foo')
      expect(MerrittZK::Locks.check_lock_collection(@zk, 'foo')).to be(true)
      MerrittZK::Locks.unlock_collection(@zk, 'foo')
      expect(MerrittZK::Locks.lock_collection(@zk, 'foo')).to be(true)

      expect(MerrittZK::Locks.lock_collection(@zk, 'bar')).to be(true)
      expect(MerrittZK::Locks.lock_collection(@zk, 'bar')).to be(false)
      MerrittZK::Locks.unlock_collection(@zk, 'bar')
      expect(MerrittZK::Locks.lock_collection(@zk, 'bar')).to be(true)
    end

    it :lock_store do |_x|
      expect(MerrittZK::Locks.check_lock_object_storage(@zk, 'ark:/aaa/111')).to be(false)
      MerrittZK::Locks.lock_object_storage(@zk, 'ark:/aaa/111')
      expect(MerrittZK::Locks.check_lock_object_storage(@zk, 'ark:/aaa/111')).to be(true)
      MerrittZK::Locks.unlock_object_storage(@zk, 'ark:/aaa/111')
      expect(MerrittZK::Locks.check_lock_object_storage(@zk, 'ark:/aaa/111')).to be(false)
      MerrittZK::Locks.lock_object_storage(@zk, 'ark:/aaa/111')
      expect(MerrittZK::Locks.check_lock_object_storage(@zk, 'ark:/aaa/111')).to be(true)

      expect(MerrittZK::Locks.check_lock_object_storage(@zk, 'ark:/bbb/222')).to be(false)
      MerrittZK::Locks.lock_object_storage(@zk, 'ark:/bbb/222')
      expect(MerrittZK::Locks.check_lock_object_storage(@zk, 'ark:/bbb/222')).to be(true)
      MerrittZK::Locks.unlock_object_storage(@zk, 'ark:/bbb/222')
      expect(MerrittZK::Locks.check_lock_object_storage(@zk, 'ark:/bbb/222')).to be(false)
      MerrittZK::Locks.lock_object_storage(@zk, 'ark:/bbb/222')
      expect(MerrittZK::Locks.check_lock_object_storage(@zk, 'ark:/bbb/222')).to be(true)
    end

    it :lock_inventory do |_x|
      expect(MerrittZK::Locks.check_lock_object_inventory(@zk, 'ark:/aaa/111')).to be(false)
      MerrittZK::Locks.lock_object_inventory(@zk, 'ark:/aaa/111')
      expect(MerrittZK::Locks.check_lock_object_inventory(@zk, 'ark:/aaa/111')).to be(true)
      MerrittZK::Locks.unlock_object_inventory(@zk, 'ark:/aaa/111')
      expect(MerrittZK::Locks.check_lock_object_inventory(@zk, 'ark:/aaa/111')).to be(false)
      MerrittZK::Locks.lock_object_inventory(@zk, 'ark:/aaa/111')
      expect(MerrittZK::Locks.check_lock_object_inventory(@zk, 'ark:/aaa/111')).to be(true)

      expect(MerrittZK::Locks.check_lock_object_inventory(@zk, 'ark:/bbb/222')).to be(false)
      MerrittZK::Locks.lock_object_inventory(@zk, 'ark:/bbb/222')
      expect(MerrittZK::Locks.check_lock_object_inventory(@zk, 'ark:/bbb/222')).to be(true)
      MerrittZK::Locks.unlock_object_inventory(@zk, 'ark:/bbb/222')
      expect(MerrittZK::Locks.check_lock_object_inventory(@zk, 'ark:/bbb/222')).to be(false)
      MerrittZK::Locks.lock_object_inventory(@zk, 'ark:/bbb/222')
      expect(MerrittZK::Locks.check_lock_object_inventory(@zk, 'ark:/bbb/222')).to be(true)
    end
  end

  describe 'Test Access Assembly Creation' do
    it :access_happy_path do |_x|
      q = MerrittZK::Access::SMALL
      a = MerrittZK::Access.create_assembly(@zk, q, { token: 'abc' })
      @remap['qid0'] = a.id
      aa = MerrittZK::Access.acquire_pending_assembly(@zk, q)
      expect(aa).to_not be_nil
      expect(a.id).to eq(aa.id)
      expect(aa.status.status).to eq(:Pending)
      aa2 = MerrittZK::Access.acquire_pending_assembly(@zk, q)
      expect(aa2).to be_nil
      aa.set_status(@zk, aa.status.state_change(:Processing))
      expect(aa.status.status).to eq(:Processing)
      aa.unlock(@zk)

      aaa = MerrittZK::Access.new(q, a.id)
      aaa.load(@zk)
      expect(a.id).to eq(aaa.id)

      aaa.set_status(@zk, aaa.status.success)

      expect(aaa.status.status).to eq(:Completed)
      expect(aaa.status.deletable?).to be(true)

      # Only for Ruby interface
      arr = MerrittZK::Access.list_jobs_as_json(@zk)
      expect(arr.length).to eq(1)
    end

    it :access_happy_path_del do |_x|
      q = MerrittZK::Access::SMALL
      a = MerrittZK::Access.create_assembly(@zk, q, { token: 'abc' })
      @remap['qid0'] = a.id
      aa = MerrittZK::Access.acquire_pending_assembly(@zk, q)
      expect(aa).to_not be_nil
      expect(a.id).to eq(aa.id)
      expect(aa.status.status).to eq(:Pending)
      aa2 = MerrittZK::Access.acquire_pending_assembly(@zk, q)
      expect(aa2).to be_nil
      aa.set_status(@zk, aa.status.state_change(:Processing))
      expect(aa.status.status).to eq(:Processing)
      aa.unlock(@zk)

      aaa = MerrittZK::Access.new(q, a.id)
      aaa.load(@zk)
      expect(a.id).to eq(aaa.id)

      aaa.set_status(@zk, aaa.status.success)

      expect(aaa.status.status).to eq(:Completed)
      expect(aaa.status.deletable?).to be(true)
      aaa.delete(@zk)

      # Only for Ruby interface
      arr = MerrittZK::Access.list_jobs_as_json(@zk)
      expect(arr.length).to eq(0)
    end

    it :access_fail_path do |_x|
      q = MerrittZK::Access::SMALL
      a = MerrittZK::Access.create_assembly(@zk, q, { token: 'abc' })
      @remap['qid0'] = a.id
      aa = MerrittZK::Access.acquire_pending_assembly(@zk, q)
      expect(aa).to_not be_nil
      expect(a.id).to eq(aa.id)
      expect(aa.status.status).to eq(:Pending)
      aa2 = MerrittZK::Access.acquire_pending_assembly(@zk, q)
      expect(aa2).to be_nil
      aa.set_status(@zk, aa.status.state_change(:Processing))
      expect(aa.status.status).to eq(:Processing)
      aa.unlock(@zk)

      aaa = MerrittZK::Access.new(q, a.id)
      aaa.load(@zk)
      expect(a.id).to eq(aaa.id)

      aaa.set_status(@zk, aaa.status.fail)

      expect(aaa.status.status).to eq(:Failed)
      aa2 = MerrittZK::Access.acquire_pending_assembly(@zk, q)
      expect(aa2).to be_nil

      aaa.set_status(@zk, aaa.status.state_change(:Deleted))

      expect(aaa.status.status).to eq(:Deleted)
      aa2 = MerrittZK::Access.acquire_pending_assembly(@zk, q)
      expect(aa2).to be_nil

      expect(aaa.status.deletable?).to be(true)
    end
  end

  describe 'Find Batch by UUID' do
    it :find_batch_by_uuid do |_x|
      b = MerrittZK::Batch.create_batch(@zk, make_batch_json)
      @remap['bid0'] = b.id

      bb = MerrittZK::Batch.find_batch_by_uuid(@zk, 'bid-uuid')
      expect(bb.id).to eq(b.id)

      bbb = MerrittZK::Batch.find_batch_by_uuid(@zk, 'bid-uuidx')
      expect(bbb).to be_nil
    end
  end
end
