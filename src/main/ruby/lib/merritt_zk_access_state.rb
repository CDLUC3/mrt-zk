require 'zk'
require 'json'
require 'yaml'

module MerrittZK
  ## 
  # Class details
  class AccessState < IngestState
    @@states = {}
    @@state_list = []

    IngestState.state_yaml.fetch(:access_states, {}).each do |k, v|
      @@states[k] = AccessState.new(k, v)
      @@state_list.append(@@states[k])
    end

    private_class_method :new

    def self.states
      @@states
    end

    def self.init
      self.Pending
    end

    def self.Pending
      @@states[:Pending]
    end

    def self.Processing
      @@states[:Processing]
    end

    def self.Failed
      @@states[:Failed]
    end

    def self.Deleted
      @@states[:Deleted]
    end

    def self.Completed
      @@states[:Completed]
    end

    def state_change(state)
      state_lookup(@@states, state)
    end

    def success
      success_lookup(@@states)
    end

    def fail
      fail_lookup(@@states)
    end
  end
end