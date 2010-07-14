require File.dirname(__FILE__) + "/spec_helper"

describe "Time (with extensions)" do
  it "converts itself to a 64-bit int suitable for using over Thrift" do
    t = Time.mktime(2010, 7, 28, 12, 59, 59, 1234)
    t.to_i64.should == 128033639900123
  end
end