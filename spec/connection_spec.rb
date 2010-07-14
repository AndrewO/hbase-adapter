require File.dirname(__FILE__) + "/spec_helper"

describe "Hbaser::Connection" do
  it "doesn't connect by default" do
    connection = Hbaser::Connection.new(:host => HBASE_CONNECTION_PARAMS[0], :port => HBASE_CONNECTION_PARAMS[1])
    connection.should_not be_connected
  end
end