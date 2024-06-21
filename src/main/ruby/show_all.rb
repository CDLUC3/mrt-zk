# frozen_string_literal: true

# test the ability to read legacy ingest queue ZK nodes
require_relative 'lib/merritt_zk'
require 'zk'

def get_payload(p, cp, d)
  # %w[/ingest /accessLarge.1 /accessSmall.1 /mrt.inventory.full].include?(p)
  # %w[/mrt.lock].include?(p)
  return '' if d.nil?

  if %w[/ingest].include?(p)
    JSON.parse(d.bytes[9..].pack('c*'))
  elsif %w[/accessLarge.1 /accessSmall.1 /mrt.inventory.full].include?(p)
    d.bytes[9..].pack('c*')
  elsif %w[/mrt.lock].include?(p) && cp =~ /(ark|access$)/
    d.bytes[8..].pack('c*')
  else
    begin
      return JSON.parse(d, symbolize_names: true)
    rescue StandardError
      # no action
    end
    d
  end
end

def edit_get_bytes(zk, path)
  return [] unless zk.exists?(path)

  zk.get(path)[0].bytes
end

def edit_get_payload(zk, path)
  edit_get_bytes(zk, path)[9..].pack('c*')
end

def edit_write_payload(zk, path, payload)
  bytes = edit_get_bytes(zk, path)
  return if bytes.length < 10

  zk.set(path, (bytes[0..8] + payload.bytes).pack('CCCCCCCCCc*'))
end

def edit_write_status(zk, path, status)
  return if status.negative? || status > 10

  bytes = edit_get_bytes(zk, path)
  bytes[0] = status
  zk.set(path, bytes.pack('CCCCCCCCCc*'))
end

def show_node(zk, p, cpath)
  puts cpath
  d = get_payload(p, cpath, zk.get(cpath)[0])
  if d.is_a?(Hash)
    puts JSON.pretty_generate(d)
  else
    puts d
  end
  puts

  zk.children(cpath).sort.each do |ccp|
    show_node(zk, p, "#{cpath}/#{ccp}")
  end
end

def show(zk, arr)
  arr.each do |p|
    next unless zk.exists?(p)

    puts p
    next if zk.children(p).empty?

    puts '---------'
    zk.children(p).sort.each do |cp|
      show_node(zk, p, "#{p}/#{cp}")
    end
  rescue StandardError => e
    puts e
  end
end

# run the following from the target env
# export ZKCONN=`get_ssm_value_by_name inventory/zoo/queueService`
zk = ZK.new(ENV.fetch('ZKCONN', 'localhost:8084'))
LIST = %w[
  /batches /batch-uuids /jobs /jobs/states /access /access/small /access/large
  /locks /locks/queue /locks/storage /locks/inventory /locks/collections
  /migration /migration/m1 /migration/m3
].freeze

puts '===> LEGACY'

show(zk, %w[/accessSmall.1 /accessLarge.1 /mrt.inventory.full /mrt.lock /ingest])

if ARGV.include?('-migrate')
  LIST.each do |p|
    zk.rm_rf(p)
    zk.create(p, data: nil)
  end

  batches = {}
  MerrittZK::LegacyIngestJob.list_jobs_as_json(zk).each do |j|
    buuid = j.fetch(:batchID, '')
    b = batches[buuid]
    if b.nil?
      batch = MerrittZK::Batch.create_batch(zk, { migrated: true, batchID: buuid })
      b = batch.id
      batches[buuid] = b
    end
    job = MerrittZK::Job.create_job(zk, b, j)
    status = j.fetch(:status, '')
    case status
    when 'Pending'
      # no action
    when 'Consumed'
      job.set_status(zk, MerrittZK::JobState::Estimating)
    when 'Completed'
      job.set_status(zk, MerrittZK::JobState::Estimating)
      job.set_status(zk, MerrittZK::JobState::Provisioning)
      job.set_status(zk, MerrittZK::JobState::Downloading)
      job.set_status(zk, MerrittZK::JobState::Processing)
      job.set_status(zk, MerrittZK::JobState::Recording)
      job.set_status(zk, MerrittZK::JobState::Notify)
      job.set_status(zk, MerrittZK::JobState::Completed)
    when 'Failed'
      job.set_status(zk, MerrittZK::JobState::Estimating)
      job.set_status(zk, MerrittZK::JobState::Failed)
    when 'Deleted'
      job.set_status(zk, MerrittZK::JobState::Held)
      job.set_status(zk, MerrittZK::JobState::Deleted)
    when 'Held'
      job.set_status(zk, MerrittZK::JobState::Held)
    end
  end

  batches.each_key do |bid|
    batch = MerrittZK::Batch.find_batch_by_uuid(zk, bid)
    batch.load(zk)

    if batch.get_processing_jobs(zk).length.positive?
      batch.set_status(zk, MerrittZK::BatchState::Processing)
    elsif batch.get_failed_jobs(zk).length.positive?
      batch.set_status(zk, MerrittZK::BatchState::Processing)
      batch.set_status(zk, MerrittZK::BatchState::Reporting)
      batch.set_status(zk, MerrittZK::BatchState::Failed)
    elsif batch.get_completed_jobs(zk).length.positive?
      batch.set_status(zk, MerrittZK::BatchState::Processing)
      batch.set_status(zk, MerrittZK::BatchState::Reporting)
      batch.set_status(zk, MerrittZK::BatchState::Completed)
    end
  end

  MerrittZK::LegacyAccessJob.list_jobs_as_json(zk).each do |j|
    job = if j.fetch(:queueNode, '') == MerrittZK::LargeLegacyAccessJob::DIR
            MerrittZK::Access.create_assembly(zk, MerrittZK::Access::LARGE, j)
          else
            MerrittZK::Access.create_assembly(zk, MerrittZK::Access::SMALL, j)
          end
    status = j.fetch(:status, '')
    case status
    when 'Pending'
      # no action
    when 'Consumed'
      job.set_status(zk, MerrittZK::AccessState::Processing)
    when 'Completed'
      job.set_status(zk, MerrittZK::AccessState::Processing)
      job.set_status(zk, MerrittZK::AccessState::Completed)
    when 'Failed'
      job.set_status(zk, MerrittZK::AccessState::Processing)
      job.set_status(zk, MerrittZK::AccessState::Failed)
    when 'Deleted'
      job.set_status(zk, MerrittZK::AccessState::Processing)
      job.set_status(zk, MerrittZK::AccessState::Failed)
      job.set_status(zk, MerrittZK::AccessState::Deleted)
    end
  end
end

if ARGV.include?('-inv')
  puts '===> INV'
  path = '/mrt.inventory.full/mrtQ-000000025100'
  edit_get_payload(zk, path)
  # Failed = 3
  edit_write_status(zk, path, 3)
end

if ARGV.include?('-debug')
  puts '===> DEBUG'
  # MerrittZK::Job.list_jobs_as_json(zk).each do |j|
  #   puts j.fetch(:path, '')
  #   puts JSON.pretty_generate(j)
  # end
  # exit
  MerrittZK::Access.list_jobs_as_json(zk).each do |j|
    puts j.fetch(:path, '')
    puts JSON.pretty_generate(j)
  end
  exit
end

if ARGV.include?('-clear')
  LIST.each do |p|
    zk.rm_rf(p)
  end
end

if ARGV.include?('-locks')
  zk.create('/mrt.lock', data: nil) unless zk.exists?('/mrt.lock')
  zk.create('/mrt.lock/ark-aaa-bbb', data: nil) unless zk.exists?('/mrt.lock/ark-aaa-bbb')
  zk.create('/locks/storage/ark-aaa-ccc', data: nil) unless zk.exists?('/locks/storage/ark-aaa-ccc')
end

if ARGV.include?('-m1')
  LIST.each do |p|
    zk.create(p, data: nil) unless zk.exists?(p)
  end
  zk.rm_rf('/migration')
  zk.create('/migration', data: nil)
  zk.create('/migration/m1', data: nil)
end

if ARGV.include?('-m3')
  LIST.each do |p|
    zk.create(p, data: nil) unless zk.exists?(p)
  end
  zk.rm_rf('/migration')
  zk.create('/migration', data: nil)
  zk.create('/migration/m3', data: nil)
end

if ARGV.include?('-m13')
  LIST.each do |p|
    zk.create(p, data: nil) unless zk.exists?(p)
  end
  zk.rm_rf('/migration')
  zk.create('/migration', data: nil)
  zk.create('/migration/m1', data: nil)
  zk.create('/migration/m3', data: nil)
end

zk.rm_rf('/migration') if ARGV.include?('-m0')

puts '===> MIGRATED'

show(zk, %w[/batches /batch-uuids /jobs /locks /access /migration])
