# frozen_string_literal: true

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
        YAML.safe_load_file(File.join(File.dirname(__FILE__), '../../../../states.yml'), aliases: true).to_json,
        symbolize_names: true
      )
    end

    attr_reader :status, :next_status

    def name
      @status.to_s
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
      states.fetch(state, nil) if state_change_allowed(state)
    end

    def to_s
      "#{@status}: #{@next_status}"
    end

    private

    attr_reader :success_state, :fail_state
  end
end
