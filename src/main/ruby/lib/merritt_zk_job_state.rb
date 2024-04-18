require 'zk'
require 'json'
require 'yaml'

module MerrittZK

  class JobState < IngestState
    @@states = {}
    @@state_list = []

    IngestState.state_yaml.fetch(:job_states, {}).each do |k, v|
      @@states[k] = JobState.new(k, v)
      @@state_list.append(@@states[k])
    end

    private_class_method :new

    def self.states
      @@states
    end

    def self.init
      self.Pending
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

    def self.Pending
      @@states[:Pending]
    end

    def self.Held
      @@states[:Held]
    end

    def self.Estimating
      @@states[:Estimating]
    end

    def self.Provisioning
      @@states[:Provisioning]
    end

    def self.Downloading
      @@states[:Downloading]
    end

    def self.Processing
      @@states[:Processing]
    end

    def self.Recording
      @@states[:Recording]
    end

    def self.Notify
      @@states[:Notify]
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
  end

end