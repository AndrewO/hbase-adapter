class Time
  def to_i64
    (self.to_f * 100000).to_i
  end
end