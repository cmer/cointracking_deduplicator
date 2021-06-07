#!/usr/bin/env ruby

EXCHANGE = "Binance"
SOURCE_PATH = File.expand_path("~/CoinTracking.csv")
DEDUP_MODE = :loose # :strict
TIME_ROUNDING = 60 * 2 # 2 minute -- for loose mode

require 'csv'
require 'digest/md5'
require 'time'

class CointrackingDeduplicator
  OUTPUT_HEADER = ["Type", "Buy", "Cur.", "Sell", "Cur.", "Fee", "Cur.", "Exchange", "Group", "Comment", "Date"]

  def initialize(exchange, source_path)
    @exchange = exchange
    @source_path = source_path
  end

  def to_s
    load_data
    puts OUTPUT_HEADER.to_csv

    @output_data.each do |row|
      puts row.to_csv
    end
  end

  private

  def load_data
    @source_data = []
    @output_hashes = []
    @output_data = []
    @duplicates = []

    CSV.foreach(@source_path) do |row|
      next unless row[7] == @exchange

      hash = case DEDUP_MODE
      when :loose
        loose_hash_row(row)
      when :strict
        strict_hash_row(row)
      else
        raise "Unknown DEDUP_MODE: #{DEDUP_MODE}"
      end

      if !is_duplicate?(hash)
        @output_data << row
        @output_hashes << hash
      else
        @duplicates << row
      end
    end
  end

  def is_duplicate?(hash)
    @output_hashes.include?(hash)
  end

  def hash_row(row, columns, round_time: false)
    values = []
    columns.each do |col|
      if col == 10 && round_time
        values << Time.parse(row[col]).round(TIME_ROUNDING).to_s
      else
        values << row[col]
      end
    end
    Digest::MD5.hexdigest(values.join("-"))
  end

  def strict_hash_row(row)
    cols = [0,1,2,3,4,7,8,9,10]
    hash_row(row, cols)
  end

  def loose_hash_row(row)
    cols = [0,1,2,3,4,7,10]
    hash_row(row, cols, round_time: true)
  end
end

class Time
  def round(sec=1)
    down = self - (self.to_i % sec)
    up = down + sec

    difference_down = self - down
    difference_up = up - self

    if (difference_down < difference_up)
      return down
    else
      return up
    end
  end
end

CointrackingDeduplicator.new(EXCHANGE, SOURCE_PATH).to_s
