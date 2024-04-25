require 'spec_helper'
require_relative '../lib/merritt_zk_state'
require_relative '../lib/merritt_zk_job_state'
require_relative '../lib/merritt_zk_batch_state'
require_relative '../lib/merritt_zk_access_state'

RSpec.describe 'state transition tests' do
  before(:each) do
    @batch_state = MerrittZK::BatchState.init
    @job_state = MerrittZK::JobState.init
    @access_state = MerrittZK::AccessState.init
  end

  describe 'Batch and Job States' do
    it 'Check Initial State' do
      expect(@batch_state.name).to eq('Pending')
      expect(@job_state.name).to eq('Pending')
      expect(@access_state.name).to eq('Pending')
    end

    it 'Check Batch Pending to Processing' do
      expect(@batch_state.success).to be_nil
      expect(@batch_state.state_change_allowed(:Reporting)).to be(false)
      expect(@batch_state.state_change_allowed(:Held)).to be(true)
      expect(@batch_state.state_change_allowed(:Processing)).to be(true)
      state = @batch_state.state_change(:Processing)
      expect(state).to_not be_nil
      expect(state.status).to eq(:Processing)
    end

    it 'Check Batch Pending to Held to Processing' do
      expect(@batch_state.success).to be_nil
      state = @batch_state.state_change(:Held)
      expect(state).to_not be_nil
      expect(state.status).to eq(:Held)
      state = state.state_change(:Pending)
      expect(state).to_not be_nil
      expect(state.status).to eq(:Pending)
      state = state.state_change(:Processing)
      expect(state).to_not be_nil
      expect(state.status).to eq(:Processing)
    end

    it 'Check Job Pending to Estimating' do
      expect(@job_state.success).to be_nil
      expect(@job_state.state_change_allowed(:Processing)).to be(false)
      expect(@job_state.state_change_allowed(:Held)).to be(true)
      expect(@job_state.state_change_allowed(:Estimating)).to be(true)
      state = @job_state.state_change(:Estimating)
      expect(state).to_not be_nil
      expect(state.status).to eq(:Estimating)
    end

    it 'Check Job Pending to Held to Estimating' do
      expect(@job_state.success).to be_nil
      state = @job_state.state_change(:Held)
      expect(state).to_not be_nil
      expect(state.status).to eq(:Held)
      state = state.state_change(:Pending)
      expect(state).to_not be_nil
      expect(state.status).to eq(:Pending)
      state = state.state_change(:Estimating)
      expect(state).to_not be_nil
      expect(state.status).to eq(:Estimating)
    end

    it 'Check Batch Happy Path' do
      state = @batch_state.state_change(:Processing)
      state = state.success
      expect(state).to_not be_nil
      expect(state.status).to eq(:Reporting)
      state = state.success
      expect(state).to_not be_nil
      expect(state.status).to eq(:Completed)
      expect(state.deletable?).to be(true)
    end

    it 'Check Access Happy Path' do
      state = @access_state.state_change(:Processing)
      state = state.success
      expect(state).to_not be_nil
      expect(state.status).to eq(:Completed)
      expect(state.deletable?).to be(true)
    end

    it 'Check Job Happy Path' do
      expect(@job_state.success).to be_nil
      state = @job_state.state_change(:Estimating)
      state = state.success
      expect(state).to_not be_nil

      expect(state.status).to eq(:Provisioning)
      state = state.success
      expect(state).to_not be_nil

      expect(state.status).to eq(:Downloading)
      state = state.success
      expect(state).to_not be_nil

      expect(state.status).to eq(:Processing)
      state = state.success
      expect(state).to_not be_nil

      expect(state.status).to eq(:Recording)
      state = state.success
      expect(state).to_not be_nil

      expect(state.status).to eq(:Notify)
      state = state.success
      expect(state).to_not be_nil

      expect(state.status).to eq(:Completed)
      expect(state.deletable?).to be(true)
    end

    it 'Check Batch Fail Path' do
      state = @batch_state.state_change(:Processing)
      state = state.success
      expect(state).to_not be_nil
      expect(state.status).to eq(:Reporting)
      state = state.fail
      expect(state).to_not be_nil
      expect(state.status).to eq(:Failed)
      state = state.state_change(:Deleted)
      expect(state.status).to eq(:Deleted)
      expect(state.deletable?).to be(true)
    end

    it 'Check Job Fail Paths' do
      expect(@job_state.success).to be_nil

      state = @job_state.state_change(:Estimating)
      expect(state.fail).to be_nil
      state = state.success
      expect(state).to_not be_nil

      expect(state.status).to eq(:Provisioning)
      expect(state.fail).to be_nil
      state = state.success
      expect(state).to_not be_nil

      expect(state.status).to eq(:Downloading)
      expect(state.fail).to_not be_nil
      state = state.success
      expect(state).to_not be_nil

      expect(state.status).to eq(:Processing)
      expect(state.fail).to_not be_nil
      state = state.success
      expect(state).to_not be_nil

      expect(state.status).to eq(:Recording)
      expect(state.fail).to_not be_nil
      state = state.success
      expect(state).to_not be_nil

      expect(state.status).to eq(:Notify)
      expect(state.fail).to_not be_nil
      state = state.fail
      expect(state).to_not be_nil

      expect(state.status).to eq(:Failed)
      state = state.state_change(:Deleted)
      expect(state).to_not be_nil

      expect(state.status).to eq(:Deleted)
      expect(state.deletable?).to be(true)
    end

    it 'Check Batch Recovery Path' do
      state = @batch_state.state_change(:Processing)
      state = state.success
      expect(state).to_not be_nil
      expect(state.status).to eq(:Reporting)
      state = state.fail
      expect(state).to_not be_nil
      expect(state.status).to eq(:Failed)
      state = state.state_change(:UpdateReporting)
      expect(state.status).to eq(:UpdateReporting)
      state = state.success
      expect(state).to_not be_nil
      expect(state.status).to eq(:Completed)
      expect(state.deletable?).to be(true)
    end

    it 'Check Job Recovery Path' do
      expect(@job_state.success).to be_nil

      state = @job_state.state_change(:Estimating)
      state = state.success
      expect(state).to_not be_nil

      expect(state.status).to eq(:Provisioning)
      state = state.success
      expect(state).to_not be_nil

      expect(state.status).to eq(:Downloading)
      state = state.fail
      expect(state).to_not be_nil

      expect(state.status).to eq(:Failed)
      state = state.state_change(:Downloading)
      expect(state).to_not be_nil

      expect(state.status).to eq(:Downloading)
      state = state.success
      expect(state).to_not be_nil

      expect(state.status).to eq(:Processing)
      state = state.success
      expect(state).to_not be_nil

      expect(state.status).to eq(:Recording)
      state = state.success
      expect(state).to_not be_nil

      expect(state.status).to eq(:Notify)
      state = state.success
      expect(state).to_not be_nil

      expect(state.status).to eq(:Completed)
      expect(state.deletable?).to be(true)
    end
  end

end