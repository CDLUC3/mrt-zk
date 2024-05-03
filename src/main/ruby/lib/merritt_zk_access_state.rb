# frozen_string_literal: true

require 'zk'
require 'json'
require 'yaml'
require 'singleton'

module MerrittZK
  ##
  # Class details
  class AccessState < IngestState
    @states = {}
    @state_list = []

    IngestState.state_yaml.fetch(:access_states, {}).each do |k, v|
      @states[k] = AccessState.new(k, v)
      @state_list.append(@states[k])
    end

    private_class_method :new

    Pending = @states[:Pending]
    Processing = @states[:Processing]
    Failed = @states[:Failed]
    Deleted = @states[:Deleted]
    Completed = @states[:Completed]

    class << self
      attr_reader :states
    end

    def self.init
      Pending
    end

    def state_change(state)
      state_lookup(AccessState.states, state)
    end

    def success
      success_lookup(AccessState.states)
    end

    def fail
      fail_lookup(AccessState.states)
    end
  end
end
