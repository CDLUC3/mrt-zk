# frozen_string_literal: true

require 'zk'
require 'json'
require 'yaml'

module MerrittZK
  ##
  # Helper class for setting and releasing Merritt ZooKeeper locks
  class Locks
    LOCKS = '/locks'
    LOCKS_QUEUE = '/locks/queue'
    LOCKS_QUEUE_INGEST = '/locks/queue/ingest'
    LOCKS_QUEUE_ACCESS_SMALL = '/locks/queue/accessSmall'
    LOCKS_QUEUE_ACCESS_LARGE = '/locks/queue/accessLarge'
    LOCKS_STORAGE = '/locks/storage'
    LOCKS_INVENTORY = '/locks/inventory'
    LOCKS_COLLECTION = '/locks/collections'

    def self.create_if_needed(zk, path)
      zk.create(path, data: nil) unless zk.exists?(path)
    end

    def self.init_locks(zk)
      create_if_needed(zk, LOCKS)
      create_if_needed(zk, LOCKS_QUEUE)
      create_if_needed(zk, LOCKS_STORAGE)
      create_if_needed(zk, LOCKS_INVENTORY)
      create_if_needed(zk, LOCKS_COLLECTION)
    end

    def self.create_lock(zk, path)
      zk.create(path, data: nil)
      true
    rescue StandardError
      false
    end

    def self.create_ephemeral_lock(zk, path)
      zk.create(path, data: nil, mode: :ephemeral)
      true
    rescue StandardError
      false
    end

    def self.lock_ingest_queue(zk)
      create_lock(zk, LOCKS_QUEUE_INGEST)
    end

    def self.check_lock_ingest_queue(zk)
      zk.exists?(LOCKS_QUEUE_INGEST)
    end

    def self.unlock_ingest_queue(zk)
      zk.delete(LOCKS_QUEUE_INGEST)
    rescue StandardError
      # no action
    end

    def self.lock_large_access_queue(zk)
      create_lock(zk, LOCKS_QUEUE_ACCESS_LARGE)
    end

    def self.check_lock_large_access_queue(zk)
      zk.exists?(LOCKS_QUEUE_ACCESS_LARGE)
    end

    def self.unlock_large_access_queue(zk)
      zk.delete(LOCKS_QUEUE_ACCESS_LARGE)
    rescue StandardError
      # no action
    end

    def self.lock_small_access_queue(zk)
      create_lock(zk, LOCKS_QUEUE_ACCESS_SMALL)
    end

    def self.check_lock_small_access_queue(zk)
      zk.exists?(LOCKS_QUEUE_ACCESS_SMALL)
    end

    def self.unlock_small_access_queue(zk)
      zk.delete(LOCKS_QUEUE_ACCESS_SMALL)
    rescue StandardError
      # no action
    end

    def self.lock_collection(zk, mnemonic)
      create_lock(zk, "#{LOCKS_COLLECTION}/#{mnemonic}")
    end

    def self.check_lock_collection(zk, mnemonic)
      zk.exists?("#{LOCKS_COLLECTION}/#{mnemonic}")
    end

    def self.unlock_collection(zk, mnemonic)
      zk.delete("#{LOCKS_COLLECTION}/#{mnemonic}")
    rescue StandardError
      # no action
    end

    def self.lock_object_storage(zk, ark)
      create_ephemeral_lock(zk, "#{LOCKS_STORAGE}/#{ark.gsub(':?/', '_')}")
    end

    def self.check_lock_object_storage(zk, ark)
      zk.exists?("#{LOCKS_STORAGE}/#{ark.gsub(':?/', '_')}")
    end

    def self.unlock_object_storage(zk, ark)
      zk.delete("#{LOCKS_STORAGE}/#{ark.gsub(':?/', '_')}")
    rescue StandardError
      # no action
    end

    def self.lock_object_inventory(zk, ark)
      create_ephemeral_lock(zk, "#{LOCKS_INVENTORY}/#{ark.gsub(':?/', '_')}")
    end

    def self.check_lock_object_inventory(zk, ark)
      zk.exists?("#{LOCKS_INVENTORY}/#{ark.gsub(':?/', '_')}")
    end

    def self.unlock_object_inventory(zk, ark)
      zk.delete("#{LOCKS_INVENTORY}/#{ark.gsub(':?/', '_')}")
    rescue StandardError
      # no action
    end
  end
end
