require File.dirname(__FILE__) + "/spec_helper"

describe "HbaseAdapter::Connection" do
  before do
    @connection = HbaseAdapter::Connection.new(:host => HBASE_CONNECTION_PARAMS[0], :port => HBASE_CONNECTION_PARAMS[1])
  end
  
  it "doesn't connect by default" do
    @connection.should_not be_connected
  end
  
  it "connects" do
    @connection.connect!
    @connection.client.should_not be_nil
    @connection.should be_connected
  end
  
  it "connects automatically when client is invoked" do
    @connection.client.should_not be_nil
    @connection.should be_connected
  end
  
  it "gets table names" do
    @connection.table_names.should_not be_empty
  end
  
  it "gets table objects" do
    @connection.tables.should_not be_empty
    @connection.tables.each {|t| t.should be_a_kind_of(HbaseAdapter::Table)}
  end
end