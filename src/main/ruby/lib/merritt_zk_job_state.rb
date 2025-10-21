# frozen_string_literal: true

require 'zk'
require 'json'
require 'yaml'

module MerrittZK
  ##
  # State transition for Merritt Ingest Jobs
  class JobState < IngestState
    @states = {}
    @state_list = []

    IngestState.state_yaml.fetch(:job_states, {}).each do |k, v|
      @states[k] = JobState.new(k, v)
      @state_list.append(@states[k])
    end

    Pending = @states[:Pending]
    Held = @states[:Held]
    Estimating = @states[:Estimating]
    Provisioning = @states[:Provisioning]
    Downloading = @states[:Downloading]
    Processing = @states[:Processing]
    Recording = @states[:Recording]
    Notify = @states[:Notify]
    Failed = @states[:Failed]
    Deleted = @states[:Deleted]
    Completed = @states[:Completed]
    Storing = @states[:Storing]

    private_class_method :new

    class << self
      attr_reader :states
    end

    def self.init
      Pending
    end

    def state_change(state)
      state_lookup(JobState.states, state)
    end

    def success
      success_lookup(JobState.states)
    end

    def fail
      fail_lookup(JobState.states)
    end
  end
end
