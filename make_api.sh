#!/bin/sh
rm -rf api
mkdir -p api/java
mvn clean javadoc:javadoc
cp -r target/site/apidocs/* api/java
mkdir -p api/ruby
cd src/main/ruby
bundle exec rdoc merritt_zk.rb
cd ../../..
cp -r src/main/ruby/doc/* api/ruby
