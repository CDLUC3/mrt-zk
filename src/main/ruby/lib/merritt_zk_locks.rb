require 'zk'
require 'json'
require 'yaml'

module MerrittZK

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
      self.create_if_needed(zk, LOCKS)
      self.create_if_needed(zk, LOCKS_QUEUE)
      self.create_if_needed(zk, LOCKS_STORAGE)
      self.create_if_needed(zk, LOCKS_INVENTORY)
      self.create_if_needed(zk, LOCKS_COLLECTION)
    end

    def self.create_lock(zk, path)
      begin
        zk.create(path, data: nil)
        true
      rescue
        false
      end
    end

    def self.create_ephemeral_lock(zk, path)
      begin
        zk.create(path, data: nil, mode: :ephemeral)
        true
      rescue
        false
      end
    end

    def self.lock_ingest_queue(zk)
      return self.create_lock(zk, LOCKS_QUEUE_INGEST)
    end
  
    def self.unlock_ingest_queue(zk)
      zk.delete(LOCKS_QUEUE_INGEST)
      rescue
    end
  
    def self.lock_large_access_queue(zk)
      return self.create_lock(zk, LOCKS_QUEUE_ACCESS_LARGE)
    end
  
    def self.unlock_large_access_queue(zk)
      return zk.delete(LOCKS_QUEUE_ACCESS_LARGE)
    end
  
    def self.lock_small_access_queue(zk)
      return self.create_lock(zk, LOCKS_QUEUE_ACCESS_SMALL)
    end
  
    def self.unlock_small_access_queue(zk)
      return zk.delete(LOCKS_QUEUE_ACCESS_SMALL)
    end
  
    def self.lock_collection(zk, mnemonic)
      return self.create_lock(zk, "#{LOCKS_COLLECTION}/#{mnemonic}")
    end
  
    def self.unlock_collection(zk, mnemonic)
      return zk.delete("#{LOCKS_COLLECTION}/#{mnemonic}")
    end
  
    def self.lock_object_storage(zk, ark)
      return self.create_ephemeral_lock(zk, "#{LOCKS_STORAGE}/#{ark.gsub(/\//, '_')}")
    end
  
    def self.unlock_object_storage(zk, ark)
      return zk.delete("#{LOCKS_STORAGE}/#{ark.gsub(/\//, '_')}")
    end
  
    def self.lock_object_inventory(zk, ark)
      return self.create_ephemeral_lock(zk, "#{LOCKS_INVENTORY}/#{ark.gsub(/\//, '_')}")
    end
  
    def self.unlock_object_inventory(zk, ark)
      return zk.delete("#{LOCKS_INVENTORY}/#{ark.gsub(/\//, '_')}")
    end
  end

end