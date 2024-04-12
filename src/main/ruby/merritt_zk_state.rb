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
          File.read('../../../states.yml'), 
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

end