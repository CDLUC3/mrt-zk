# frozen_string_literal: true

require 'zk'
require 'json'
require 'yaml'

module MerrittZK
  ##
  # States for a Merritt Ingest Batch
  class BatchState < IngestState
    @states = {}
    @state_list = []

    IngestState.state_yaml.fetch(:batch_states, {}).each do |k, v|
      @states[k] = BatchState.new(k, v)
      @state_list.append(@states[k])
    end

    Pending = @states[:Pending]
    Held = @states[:Held]
    Processing = @states[:Processing]
    Reporting = @states[:Reporting]
    UpdateReporting = @states[:UpdateReporting]
    Failed = @states[:Failed]
    Deleted = @states[:Deleted]
    Completed = @states[:Completed]

    private_class_method :new

    class << self
      attr_reader :states
    end

    def self.init
      Pending
    end

    def state_change(state)
      state_lookup(BatchState.states, state)
    end

    def success
      success_lookup(BatchState.states)
    end

    def fail
      fail_lookup(BatchState.states)
    end
  end
end
