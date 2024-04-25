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
    @remap = {'now': /\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d .\d\d\d\d/}
    # Note that the IT test name matches a key in the test-cases.yml file
    @zkt.load_test(x.description.to_sym)
  end

  after(:each) do |x|
    # Note that the IT test name matches a key in the test-cases.yml file
    unless @zkt.output(x.description.to_sym).nil?
      unless @zkt.output(x.description.to_sym).empty?
        expect(@zkt.verify_output(x.description.to_sym, remap: @remap)).to be(true)
      end
    end
  end

  describe 'Test read/write from ZK' do
    # Note that the IT test name matches a key in the test-cases.yml file
    it :test_read_write_from_zk do |x|
    end

    it :test_sequential_node_mapping do |x|
      @remap['/foo0'] = @zk.create("/foo", data: nil, mode: :persistent_sequential)
      @remap['/foo1'] = @zk.create("/foo", data: nil, mode: :persistent_sequential)
      @remap['/foo2'] = @zk.create("/foo", data: nil, mode: :persistent_sequential)
    end  
  end

  describe 'Test ephemeral node behavior' do
    it :ephemeral_node_remaining do |x|
      @zk.create("/foo", data: nil, mode: :ephemeral)
    end
  
    it :ephemeral_node_destroyed do |x|
      tzk = @zkt.zk_new
      tzk.create("/foo", data: nil, mode: :ephemeral)
      tzk.close
    end
  end

  describe 'Test Batch Creation' do
    it :create_batch do |x|
      b = MerrittZK::Batch.create_batch(@zk, {foo: 'bar'})
      @remap['bid0'] = b.id
    end

    it :create_and_load_batch do |x|
      b = MerrittZK::Batch.create_batch(@zk, {foo: 'bar'})
      @remap['bid0'] = b.id
      b2 = MerrittZK::Batch.new(b.id).load(@zk)
      expect(b2.status.status).to eq(:Pending)
    end

    it :load_non_existent_batch do |x|
      expect {
        MerrittZK::Batch.new(111).load(@zk)
      }.to raise_error(MerrittZK::MerrittZKNodeInvalid, /.*/)
    end
  end

  describe 'Test Batch Locking' do
    it :batch_with_lock_unlock do |x|
      b = MerrittZK::Batch.create_batch(@zk, {foo: 'bar'})
      @remap['bid0'] = b.id
      b.lock(@zk)
      b.unlock(@zk)
    end
  
    it :batch_with_ephemeral_released_lock do |x|
      b = MerrittZK::Batch.create_batch(@zk, {foo: 'bar'})
      @remap['bid0'] = b.id
      tzk = @zkt.zk_new
      b.lock(tzk)
      tzk.close
    end
  
    it :batch_with_unreleased_lock do |x|
      b = MerrittZK::Batch.create_batch(@zk, {foo: 'bar'})
      @remap['bid0'] = b.id
      b.lock(@zk)
    end

    it :batch_acquire do |x|
      b = MerrittZK::Batch.create_batch(@zk, {foo: 'bar'})
      b2 = MerrittZK::Batch.create_batch(@zk, {foo: 'bar2'})
      @remap['bid0'] = b.id
      @remap['bid1'] = b2.id
      bb = MerrittZK::Batch.acquire_pending_batch(@zk)
      bb2 = MerrittZK::Batch.acquire_pending_batch(@zk)
      expect(MerrittZK::Batch.acquire_pending_batch(@zk)).to be_nil
    end
  end

  describe 'Test Batch State Changes' do
    it :modify_batch_state do |x|
      b = MerrittZK::Batch.create_batch(@zk, {foo: 'bar'})
      @remap['bid0'] = b.id
      b.set_status(@zk, b.status.state_change(:Processing))
    end
  end

  describe 'Test Job Creation' do
    it :create_job do |x|
      b = MerrittZK::Batch.create_batch(@zk, {foo: 'bar'})
      @remap['bid0'] = b.id
      bb = MerrittZK::Batch.acquire_pending_batch(@zk)
      expect(b.id).to eq(bb.id)
      j = MerrittZK::Job.create_job(@zk, bb.id, {job: 'quack'})
      @remap['jid0'] = j.id
      expect(j.bid).to eq(bb.id)
    end

    it :create_job_state_change do |x|
      b = MerrittZK::Batch.create_batch(@zk, {foo: 'bar'})
      @remap['bid0'] = b.id
      bb = MerrittZK::Batch.acquire_pending_batch(@zk)
      expect(b.id).to eq(bb.id)
      j = MerrittZK::Job.create_job(@zk, bb.id, {job: 'quack'})
      @remap['jid0'] = j.id
      expect(j.bid).to eq(bb.id)
      j.set_status(@zk, j.status.state_change(:Estimating))
    end

    it :load_job_state_change do |x|
      b = MerrittZK::Batch.create_batch(@zk, {foo: 'bar'})
      @remap['bid0'] = b.id
      bb = MerrittZK::Batch.acquire_pending_batch(@zk)
      expect(b.id).to eq(bb.id)
      j = MerrittZK::Job.create_job(@zk, bb.id, {job: 'quack'})
      @remap['jid0'] = j.id
      expect(j.bid).to eq(bb.id)
      jj = MerrittZK::Job.new(j.id).load(@zk)
      jj.set_status(@zk, jj.status.state_change(:Estimating))
    end

    it :acquire_pending_job do |x|
      b = MerrittZK::Batch.create_batch(@zk, {foo: 'bar'})
      @remap['bid0'] = b.id
      bb = MerrittZK::Batch.acquire_pending_batch(@zk)
      expect(b.id).to eq(bb.id)
      j = MerrittZK::Job.create_job(@zk, bb.id, {job: 'quack'})
      @remap['jid0'] = j.id
      expect(j.bid).to eq(bb.id)
      bb.unlock(@zk)

      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState.Pending)
      jj.set_status(@zk, jj.status.state_change(:Estimating))
    end

    it :acquire_lowest_priority_job do |x|
      b = MerrittZK::Batch.create_batch(@zk, {foo: 'bar'})
      @remap['bid0'] = b.id
      bb = MerrittZK::Batch.acquire_pending_batch(@zk)
      expect(b.id).to eq(bb.id)

      j = MerrittZK::Job.create_job(@zk, bb.id, {job: 'quack1'})
      @remap['jid0'] = j.id
      
      j = MerrittZK::Job.create_job(@zk, bb.id, {job: 'quack2'})
      @remap['jid1'] = j.id
      j.set_priority(@zk, 2)
      
      j = MerrittZK::Job.create_job(@zk, bb.id, {job: 'quack3'})
      @remap['jid2'] = j.id
      
      bb.unlock(@zk)

      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState.Pending)
      expect(jj.id).to eq(@remap['jid1'])
      jj.set_status(@zk, jj.status.state_change(:Estimating))
    end

    it :job_happy_path do |x|
      b = MerrittZK::Batch.create_batch(@zk, {foo: 'bar'})
      @remap['bid0'] = b.id
      bb = MerrittZK::Batch.acquire_pending_batch(@zk)
      expect(b.id).to eq(bb.id)

      j = MerrittZK::Job.create_job(@zk, bb.id, {job: 'quack1'})
      @remap['jid0'] = j.id
      
      j = MerrittZK::Job.create_job(@zk, bb.id, {job: 'quack2'})
      @remap['jid1'] = j.id
      j.set_priority(@zk, 2)
      
      j = MerrittZK::Job.create_job(@zk, bb.id, {job: 'quack3'})
      @remap['jid2'] = j.id
      
      bb.unlock(@zk)

      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState.Pending)
      expect(jj.id).to eq(@remap['jid1'])
      jj.set_status(@zk, jj.status.state_change(:Estimating))
      jj.unlock(@zk)

      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState.Estimating)
      expect(jj).to_not be_nil
      expect(jj.id).to eq(@remap['jid1'])
      jj.set_status(@zk, jj.status.success())
      jj.unlock(@zk)
      expect(jj.status.status).to eq(:Provisioning)

      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState.Provisioning)
      expect(jj).to_not be_nil
      expect(jj.id).to eq(@remap['jid1'])
      jj.set_status(@zk, jj.status.success())
      jj.unlock(@zk)
      expect(jj.status.status).to eq(:Downloading)

      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState.Downloading)
      expect(jj).to_not be_nil
      expect(jj.id).to eq(@remap['jid1'])
      jj.set_status(@zk, jj.status.success())
      jj.unlock(@zk)
      expect(jj.status.status).to eq(:Processing)

      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState.Processing)
      expect(jj).to_not be_nil
      expect(jj.id).to eq(@remap['jid1'])
      jj.set_status(@zk, jj.status.success())
      jj.unlock(@zk)
      expect(jj.status.status).to eq(:Recording)

      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState.Recording)
      expect(jj).to_not be_nil
      expect(jj.id).to eq(@remap['jid1'])
      jj.set_status(@zk, jj.status.success())
      jj.unlock(@zk)
      expect(jj.status.status).to eq(:Notify)

      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState.Notify)
      expect(jj).to_not be_nil
      expect(jj.id).to eq(@remap['jid1'])
      jj.set_status(@zk, jj.status.success())
      jj.unlock(@zk)
      expect(jj.status.status).to eq(:Completed)
      expect(jj.status.deletable?).to be(true)
    end

    it :batch_happy_path do |x|
      b = MerrittZK::Batch.create_batch(@zk, {foo: 'bar'})
      @remap['bid0'] = b.id
      bb = MerrittZK::Batch.acquire_pending_batch(@zk)
      expect(b.id).to eq(bb.id)

      j = MerrittZK::Job.create_job(@zk, bb.id, {job: 'quack2'})
      @remap['jid1'] = j.id
      j.set_priority(@zk, 2)
      
      bb.unlock(@zk)

      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState.Pending)
      expect(jj.id).to eq(@remap['jid1'])
      jj.set_status(@zk, jj.status.state_change(:Estimating))
      jj.unlock(@zk)

      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState.Estimating)
      expect(jj).to_not be_nil
      expect(jj.id).to eq(@remap['jid1'])
      jj.set_status(@zk, jj.status.success())
      jj.unlock(@zk)
      expect(jj.status.status).to eq(:Provisioning)

      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState.Provisioning)
      expect(jj).to_not be_nil
      expect(jj.id).to eq(@remap['jid1'])
      jj.set_status(@zk, jj.status.success())
      jj.unlock(@zk)
      expect(jj.status.status).to eq(:Downloading)

      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState.Downloading)
      expect(jj).to_not be_nil
      expect(jj.id).to eq(@remap['jid1'])
      jj.set_status(@zk, jj.status.success())
      jj.unlock(@zk)
      expect(jj.status.status).to eq(:Processing)

      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState.Processing)
      expect(jj).to_not be_nil
      expect(jj.id).to eq(@remap['jid1'])
      jj.set_status(@zk, jj.status.success())
      jj.unlock(@zk)
      expect(jj.status.status).to eq(:Recording)

      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState.Recording)
      expect(jj).to_not be_nil
      expect(jj.id).to eq(@remap['jid1'])
      jj.set_status(@zk, jj.status.success())
      jj.unlock(@zk)
      expect(jj.status.status).to eq(:Notify)

      bbb = MerrittZK::Batch.acquire_complete_batch(@zk)
      expect(bbb).to be_nil

      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState.Notify)
      expect(jj).to_not be_nil
      expect(jj.id).to eq(@remap['jid1'])
      jj.set_status(@zk, jj.status.success())
      jj.unlock(@zk)
      expect(jj.status.status).to eq(:Completed)

      bbb = MerrittZK::Batch.acquire_complete_batch(@zk)
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
    end

    it :batch_failure do |x|
      b = MerrittZK::Batch.create_batch(@zk, {foo: 'bar'})
      @remap['bid0'] = b.id
      bb = MerrittZK::Batch.acquire_pending_batch(@zk)
      expect(b.id).to eq(bb.id)

      j = MerrittZK::Job.create_job(@zk, bb.id, {job: 'quack2'})
      @remap['jid1'] = j.id
      j.set_priority(@zk, 2)
      
      bb.unlock(@zk)

      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState.Pending)
      expect(jj.id).to eq(@remap['jid1'])
      jj.set_status(@zk, jj.status.state_change(:Estimating))
      jj.unlock(@zk)

      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState.Estimating)
      expect(jj).to_not be_nil
      expect(jj.id).to eq(@remap['jid1'])
      jj.set_status(@zk, jj.status.success())
      jj.unlock(@zk)
      expect(jj.status.status).to eq(:Provisioning)

      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState.Provisioning)
      expect(jj).to_not be_nil
      expect(jj.id).to eq(@remap['jid1'])
      jj.set_status(@zk, jj.status.success())
      jj.unlock(@zk)
      expect(jj.status.status).to eq(:Downloading)

      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState.Downloading)
      expect(jj).to_not be_nil
      expect(jj.id).to eq(@remap['jid1'])
      jj.set_status(@zk, jj.status.success())
      jj.unlock(@zk)
      expect(jj.status.status).to eq(:Processing)

      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState.Processing)
      expect(jj).to_not be_nil
      expect(jj.id).to eq(@remap['jid1'])
      jj.set_status(@zk, jj.status.success())
      jj.unlock(@zk)
      expect(jj.status.status).to eq(:Recording)

      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState.Recording)
      expect(jj).to_not be_nil
      expect(jj.id).to eq(@remap['jid1'])
      jj.set_status(@zk, jj.status.success())
      jj.unlock(@zk)
      expect(jj.status.status).to eq(:Notify)

      bbb = MerrittZK::Batch.acquire_complete_batch(@zk)
      expect(bbb).to be_nil

      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState.Notify)
      expect(jj).to_not be_nil
      expect(jj.id).to eq(@remap['jid1'])
      jj.set_status(@zk, jj.status.fail())
      jj.unlock(@zk)
      expect(jj.status.status).to eq(:Failed)

      bbb = MerrittZK::Batch.acquire_complete_batch(@zk)
      expect(bbb).to_not be_nil
      expect(bbb.status.status).to be(:Reporting)
      expect(bbb.has_failure).to be(true)
      bbb.set_status(@zk, bbb.status.fail)
      bbb.unlock(@zk)

      expect(bbb.status.status).to eq(:Failed)

      bbbb = MerrittZK::Batch.new(bbb.id).load(@zk)
      expect(bbbb.status.status).to eq(:Failed)
      expect(bbbb.has_failure).to be(true)

      bbbb.set_status(@zk, MerrittZK::BatchState.Deleted)
      expect(bbbb.status.status).to eq(:Deleted)
      expect(bbbb.status.deletable?).to be(true)
    end

    it :batch_recovery do |x|
      b = MerrittZK::Batch.create_batch(@zk, {foo: 'bar'})
      @remap['bid0'] = b.id
      bb = MerrittZK::Batch.acquire_pending_batch(@zk)
      expect(b.id).to eq(bb.id)

      j = MerrittZK::Job.create_job(@zk, bb.id, {job: 'quack2'})
      @remap['jid1'] = j.id
      j.set_priority(@zk, 2)
      
      bb.unlock(@zk)

      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState.Pending)
      expect(jj.id).to eq(@remap['jid1'])
      jj.set_status(@zk, jj.status.state_change(:Estimating))
      jj.unlock(@zk)

      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState.Estimating)
      expect(jj).to_not be_nil
      expect(jj.id).to eq(@remap['jid1'])
      jj.set_status(@zk, jj.status.success())
      jj.unlock(@zk)
      expect(jj.status.status).to eq(:Provisioning)

      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState.Provisioning)
      expect(jj).to_not be_nil
      expect(jj.id).to eq(@remap['jid1'])
      jj.set_status(@zk, jj.status.success())
      jj.unlock(@zk)
      expect(jj.status.status).to eq(:Downloading)

      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState.Downloading)
      expect(jj).to_not be_nil
      expect(jj.id).to eq(@remap['jid1'])
      jj.set_status(@zk, jj.status.success())
      jj.unlock(@zk)
      expect(jj.status.status).to eq(:Processing)

      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState.Processing)
      expect(jj).to_not be_nil
      expect(jj.id).to eq(@remap['jid1'])
      jj.set_status(@zk, jj.status.success())
      jj.unlock(@zk)
      expect(jj.status.status).to eq(:Recording)

      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState.Recording)
      expect(jj).to_not be_nil
      expect(jj.id).to eq(@remap['jid1'])
      jj.set_status(@zk, jj.status.success())
      jj.unlock(@zk)
      expect(jj.status.status).to eq(:Notify)

      bbb = MerrittZK::Batch.acquire_complete_batch(@zk)
      expect(bbb).to be_nil

      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState.Notify)
      expect(jj).to_not be_nil
      expect(jj.id).to eq(@remap['jid1'])
      jj.set_status(@zk, jj.status.fail())
      jj.unlock(@zk)
      expect(jj.status.status).to eq(:Failed)

      bbb = MerrittZK::Batch.acquire_complete_batch(@zk)
      expect(bbb).to_not be_nil
      expect(bbb.status.status).to be(:Reporting)
      expect(bbb.has_failure).to be(true)
      bbb.set_status(@zk, bbb.status.fail)
      bbb.unlock(@zk)

      expect(bbb.status.status).to eq(:Failed)

      jjj = MerrittZK::Job.new(jj.id).load(@zk)
      jjj.set_status(@zk, MerrittZK::JobState.Notify, job_retry: true)

      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState.Notify)
      expect(jj).to_not be_nil
      expect(jj.id).to eq(@remap['jid1'])
      jj.set_status(@zk, jj.status.success())
      jj.unlock(@zk)
      expect(jj.status.status).to eq(:Completed)

      bbbb = MerrittZK::Batch.new(bbb.id).load(@zk)
      expect(bbbb.status.status).to eq(:Failed)
      expect(bbbb.has_failure).to be(false)

      bbbb.set_status(@zk, MerrittZK::BatchState.UpdateReporting)
      expect(bbbb.status.deletable?).to be(false)

      bbbb.set_status(@zk, bbbb.status.success)
      expect(bbbb.status.status).to eq(:Completed)
      expect(bbbb.status.deletable?).to be(true)
    end

    it :job_happy_path_with_delete do |x|
      b = MerrittZK::Batch.create_batch(@zk, {foo: 'bar'})
      @remap['bid0'] = b.id
      bb = MerrittZK::Batch.acquire_pending_batch(@zk)
      expect(b.id).to eq(bb.id)
  
      j = MerrittZK::Job.create_job(@zk, bb.id, {job: 'quack1'})
      @remap['jid0'] = j.id
      
      j = MerrittZK::Job.create_job(@zk, bb.id, {job: 'quack2'})
      @remap['jid1'] = j.id
      j.set_priority(@zk, 2)
      
      j = MerrittZK::Job.create_job(@zk, bb.id, {job: 'quack3'})
      @remap['jid2'] = j.id
      
      bb.unlock(@zk)
  
      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState.Pending)
      expect(jj.id).to eq(@remap['jid1'])
      jj.set_status(@zk, jj.status.state_change(:Estimating))
      jj.unlock(@zk)
  
      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState.Estimating)
      expect(jj).to_not be_nil
      expect(jj.id).to eq(@remap['jid1'])
      jj.set_status(@zk, jj.status.success())
      jj.unlock(@zk)
      expect(jj.status.status).to eq(:Provisioning)
  
      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState.Provisioning)
      expect(jj).to_not be_nil
      expect(jj.id).to eq(@remap['jid1'])
      jj.set_status(@zk, jj.status.success())
      jj.unlock(@zk)
      expect(jj.status.status).to eq(:Downloading)
  
      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState.Downloading)
      expect(jj).to_not be_nil
      expect(jj.id).to eq(@remap['jid1'])
      jj.set_status(@zk, jj.status.success())
      jj.unlock(@zk)
      expect(jj.status.status).to eq(:Processing)
  
      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState.Processing)
      expect(jj).to_not be_nil
      expect(jj.id).to eq(@remap['jid1'])
      jj.set_status(@zk, jj.status.success())
      jj.unlock(@zk)
      expect(jj.status.status).to eq(:Recording)
  
      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState.Recording)
      expect(jj).to_not be_nil
      expect(jj.id).to eq(@remap['jid1'])
      jj.set_status(@zk, jj.status.success())
      jj.unlock(@zk)
      expect(jj.status.status).to eq(:Notify)
  
      expect {
        jj.delete(@zk)
      }.to raise_error(MerrittZK::MerrittStateError, /.*/)
 
      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState.Notify)
      expect(jj).to_not be_nil
      expect(jj.id).to eq(@remap['jid1'])
      jj.set_status(@zk, jj.status.success())
      jj.unlock(@zk)
      expect(jj.status.status).to eq(:Completed)
      expect(jj.status.deletable?).to be(true)
  
      jj.delete(@zk)
    end

    it :batch_happy_path_with_delete do |x|
      b = MerrittZK::Batch.create_batch(@zk, {foo: 'bar'})
      @remap['bid0'] = b.id
      bb = MerrittZK::Batch.acquire_pending_batch(@zk)
      expect(b.id).to eq(bb.id)

      j = MerrittZK::Job.create_job(@zk, bb.id, {job: 'quack2'})
      @remap['jid1'] = j.id
      j.set_priority(@zk, 2)
      
      bb.unlock(@zk)

      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState.Pending)
      expect(jj.id).to eq(@remap['jid1'])
      jj.set_status(@zk, jj.status.state_change(:Estimating))
      jj.unlock(@zk)

      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState.Estimating)
      expect(jj).to_not be_nil
      expect(jj.id).to eq(@remap['jid1'])
      jj.set_status(@zk, jj.status.success())
      jj.unlock(@zk)
      expect(jj.status.status).to eq(:Provisioning)

      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState.Provisioning)
      expect(jj).to_not be_nil
      expect(jj.id).to eq(@remap['jid1'])
      jj.set_status(@zk, jj.status.success())
      jj.unlock(@zk)
      expect(jj.status.status).to eq(:Downloading)

      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState.Downloading)
      expect(jj).to_not be_nil
      expect(jj.id).to eq(@remap['jid1'])
      jj.set_status(@zk, jj.status.success())
      jj.unlock(@zk)
      expect(jj.status.status).to eq(:Processing)

      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState.Processing)
      expect(jj).to_not be_nil
      expect(jj.id).to eq(@remap['jid1'])
      jj.set_status(@zk, jj.status.success())
      jj.unlock(@zk)
      expect(jj.status.status).to eq(:Recording)

      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState.Recording)
      expect(jj).to_not be_nil
      expect(jj.id).to eq(@remap['jid1'])
      jj.set_status(@zk, jj.status.success())
      jj.unlock(@zk)
      expect(jj.status.status).to eq(:Notify)

      bbb = MerrittZK::Batch.acquire_complete_batch(@zk)
      expect(bbb).to be_nil

      jj = MerrittZK::Job.acquire_job(@zk, MerrittZK::JobState.Notify)
      expect(jj).to_not be_nil
      expect(jj.id).to eq(@remap['jid1'])
      jj.set_status(@zk, jj.status.success())
      jj.unlock(@zk)
      expect(jj.status.status).to eq(:Completed)

      bbb = MerrittZK::Batch.acquire_complete_batch(@zk)
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

  end

  describe 'Test Access Assembly Creation' do
    it :access_happy_path do |x|
      q = 'small'
      a = MerrittZK::Access.create_assembly(@zk, q, {token: 'abc'})
      @remap['qid0'] = a.id
      aa = MerrittZK::Access.acquire_pending_assembly(@zk, q)
      expect(a.id).to eq(aa.id)  
      expect(aa.status.status).to eq(:Pending)
      aa.set_status(@zk, aa.status.state_change(:Processing))
      expect(aa.status.status).to eq(:Processing)
      aa.unlock(@zk)
  
      aaa = MerrittZK::Access.new(q, a.id)
      aaa.load(@zk)
      expect(a.id).to eq(aaa.id)  
  
      aaa.set_status(@zk, aaa.status.success)
  
      expect(aaa.status.status).to eq(:Completed)
      expect(aaa.status.deletable?).to be(true)
    end
  end

end