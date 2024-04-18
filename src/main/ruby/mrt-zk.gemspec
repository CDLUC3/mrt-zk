# frozen_string_literal: true

Gem::Specification.new do |spec|
  spec.name        = 'mrt-zk'
  spec.version     = '1.0.1'
  spec.platform    = Gem::Platform::RUBY
  spec.authors     = ['Terry Brady']
  spec.email       = ['terrence.brady@ucop.edu']

  spec.summary     = 'Merritt Zookeeper Library'
  spec.description = 'Provides an interface to Merritt Zookeeper Nodes'
  spec.homepage    = 'https://github.com/CDLUC3/mrt-zk'
  spec.license     = 'MIT'

  spec.files         = Dir['lib/*.rb', 'Gemfile*']
  spec.require_paths = ['lib']
  spec.required_ruby_version = '>= 3.0'

  spec.add_runtime_dependency('zookeeper', '~> 1.5.5')
  spec.add_runtime_dependency('zk', '~> 1.10.0')

end