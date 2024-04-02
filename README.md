# State Transition code

Goal: implement an API in Java and Ruby for the [Ingest Queue Redesign](https://github.com/CDLUC3/mrt-doc/blob/main/design/queue-2023/transition.md)

Equivalent test cases will be written in java and ruby using the test cases defined in [test-cases.yml](test-cases.yml)

## Start a local ZK instance (for integration tests)

```
docker-compose up -d
```

## Java Code -- Purpose

Ensure that the following enums implement the state transitions defined in [states.yml](states.yml)
- [BatchState](src/main/java/org/cdlib/mrt/zk/BatchState.java)
- [JobState](src/main/java/org/cdlib/mrt/zk/JobState.java)

## Java Unit Tests

- [State Transition Tests](src/test/java/org/cdlib/mrt/zk/StateTest.java)
- [ZooKeeper Node Tests](src/test/java/org/cdlib/mrt/zk/ZKTestTest.java)

```
mvn clean install
```

## Publish Javadocs

```
mvn clean javadoc:javadoc
```

## Ruby Code 

### State Transition Classes - Like the java Enums but uses the yaml file directly
- [merritt_zk.rb](src/main/ruby/merritt_zk.rb#L6-L125)

### State Transition Unit Tests
- [spec/state_spec.rb](src/main/ruby/spec/states_spec.rb)

```
cd src/main/ruby
bundle install
bundle exec rspec spec/states_spec.rb
```

### ZK Queue API Library (under construction)
- [merritt_zk.rb](src/main/ruby/merritt_zk.rb#L127-L381)

### ZK Queue API Integration Tests (requires a running zk on localhost - under construction)
- [spec/zk_spec.rb](src/main/ruby/spec/zk_spec.rb)

```
cd src/main/ruby
bundle install
bundle exec rspec spec/zk_spec.rb
```

### Generate ruby docs
```
cd src/main/ruby
bundle exec rdoc merritt_zk.rb 
```