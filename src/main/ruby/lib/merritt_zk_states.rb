require 'zk'
require 'json'
require 'yaml'

module MerrittZK
  ## 
  # Class details
  class IngestState
    def initialize(status, next_status)
      next_status = {} if next_status.nil?
      @status = status
      @next_status = next_status.keys
      @success_state = nil
      @fail_state = nil
      next_status.each do |k, v|
        next if v.nil?
        @success_state = k if v.fetch(:success, false)
        @fail_state = k if v.fetch(:fail, false)
      end
    end

    def self.state_yaml
      JSON.parse(
        YAML.safe_load(
          File.read(File.join(File.dirname(__FILE__), '../../../../states.yml')), 
          aliases: true
        ).to_json, 
        symbolize_names: true
      )
    end

    def status
      @status
    end

    def name
      @status.to_s
    end

    def next_status
      @next_status
    end

    def deletable?
      @next_status.empty?
    end

    def success_lookup(states)
      states.fetch(success_state, nil)
    end

    def fail_lookup(states)
      states.fetch(fail_state, nil)
    end

    def state_change_allowed(state)
      next_status.include?(state)
    end

    def state_lookup(states, state)
      return states.fetch(state, nil) if state_change_allowed(state)
    end

    def to_s
      "#{@status}: #{@next_status}"
    end

    private 

    def success_state
      @success_state
    end

    def fail_state
      @fail_state
    end

  end

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

  class BatchState < IngestState
    @@states = {}
    @@state_list = []

    IngestState.state_yaml.fetch(:batch_states, {}).each do |k, v|
      @@states[k] = BatchState.new(k, v)
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

    def self.Held
      @@states[:Held]
    end

    def self.Processing
      @@states[:Processing]
    end

    def self.Reporting
      @@states[:Reporting]
    end

    def self.UpdateReporting
      @@states[:UpdateReporting]
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