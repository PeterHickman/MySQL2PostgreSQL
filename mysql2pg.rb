#!/usr/bin/env ruby
# encoding: UTF-8

# Parses the MySQL dump and create the schema definition that can be used
# for PostgreSQL
#
# 1. Needs to handle sequences

SINGLE_QUOTE = "'"

NUMBER_PER_INSERT = 100

class CreateSchema
  attr_reader :data_types

  DROP_STARTS_WITH = [
    'INSERT',
    '/',
    'LOCK TABLES',
    'UNLOCK TABLES',
    'DROP TABLE',
  ]

  TRANSLATE = {
    'int(11)' => 'INTEGER',
    'int(2)' => 'INTEGER',
    'tinyint(1)' => 'BOOLEAN',
    'double' => 'REAL',
    'datetime' => 'TIMESTAMP',
    'varchar(120)' => 'TEXT',
    'varchar(40)' => 'TEXT',
  }

  def initialize
    @output = []
    @data_types = {}
  end

  def write(filename)
    f = File.open(filename, 'w')
    f.puts @output.join("\n")
    f.close
  end

  def parse(filename)
    @output.clear
    @data_types.clear

    keys = []
    rows = []
    table = nil

    in_create = false;

    File.open(filename, 'r').each do |line|
      skip = false

      DROP_STARTS_WITH.each do |pattern|
        if line.index(pattern) == 0
          skip = true
          break
        end
      end

      next if skip

      ##
      # Special cleanups
      ##
      line.gsub!(' ENGINE=InnoDB DEFAULT CHARSET=latin1', '')
      line.gsub!(' ON UPDATE CURRENT_TIMESTAMP', '')
      line.gsub!("enum('pre-match','in-play')", 'TEXT')
      line.gsub!("enum('1st half','full time')", 'TEXT')

      line = line.gsub('`', '').split(/\s+/).map { |i| TRANSLATE[i] || i }.join(' ')

      if line.index('CREATE') == 0
        in_create = true
        table = line.split(/\s+/)[2]
      elsif line.index(');') == 0
        create_table(table, rows)
        rows.clear
        in_create = false
      end

      if in_create
        if line.include?('KEY')
          keys << line.gsub(/,$/, '').strip
        else
          rows << line.gsub(/,$/, '')
        end
      else
        @output << line
        if keys.any?
          create_keys(table, keys)
          keys.clear
        end
      end
    end
  end

  private

  def create_table(name, rows)
    @output << "DROP TABLE IF EXISTS #{name};"
    @output << rows.shift
    @output << rows.join(",\n")

    @data_types[name] = []

    rows.each do |row|
      x = row.downcase.strip.split(/\s+/)
      @data_types[name] << x[1]
    end
  end

  def key_name(table, text)
    table + '_' + text[1..-2].tr(',','_') + '_idx'
  end

  def create_keys(name, keys)
    r = []
    l = []

    keys.each do |key|
      x = key.split(/\s+/)
      kname = key_name(name, x.last)

      next if r.include?(kname)
      r << kname

      case x[0]
      when 'PRIMARY'
        l << "CREATE UNIQUE INDEX #{kname} ON #{name} #{x.last};"
      when 'UNIQUE'
        l << "CREATE UNIQUE INDEX #{kname} ON #{name} #{x.last};"
      when 'KEY'
        l << "CREATE INDEX #{kname} ON #{name} #{x.last};"
      end
    end

    l.uniq.each do |x|
      @output << x
    end
  end
end

class CreateInput
  def parse(input, data_types)
    o = nil
    current_name = nil

    File.open(input, 'r').each do |line|
      next unless line.index('INSERT') == 0

      lhs, rhs = line.chomp.gsub('`', '').split(' VALUES (')

      name = lhs.split(/\s+/).last

      if name != current_name
        o.close if o
        o = File.open("data_for_#{name}.sql", 'w')
        current_name = name
        puts "Writing the data for the #{name} table"
      end

      parts = []

      rhs.gsub(/\);$/,'').split('),(').each do |values|
        x = split(values)

        x.each_with_index do |element, index|
          next if element == 'NULL' || element == nil

          case data_types[name][index]
          when 'boolean'
            x[index] = element == '0' ? false : true
          when 'text'
            x[index] = 'e' + element.gsub(/\\\'/, "''")
          else
            # Do nothing
          end
        end
        parts << x.join(',')
      end

      while parts.any?
        x = parts.shift(NUMBER_PER_INSERT)
	o.puts "INSERT INTO #{name} VALUES (#{x.join('),(')});"
      end
    end

    o.close
  end

  private

  def split(line)
    ##
    # Because of (My)SQL's text quoting rules we cannot use the
    # csv library to split this line apart (which works in 99.99%
    # of the cases). We need to write our own
    #
    # There are a few cases that this does not cover. Or perhaps I
    # should just write proper parser :)
    ##

    r = []
    partial = false

    line.split(',').each do |part|
      if part == SINGLE_QUOTE
        ##
        # This edge case was "inspired" by strings with leading or
        # trailing commas
        ##
        if partial
          r[-1] << ','
          r[-1] << part
        else
          r << part
        end
        partial = ! partial
      elsif part[0] == SINGLE_QUOTE
        partial = true unless part[-1] == SINGLE_QUOTE
        r << part
      elsif partial
        partial = false if part[-1] == SINGLE_QUOTE
        r[-1] << ','
        r[-1] << part
      else
        r << part
      end
    end

    r
  end
end

cs = CreateSchema.new
ci = CreateInput.new

ARGV.each do |filename|
  cs.parse(filename)
  cs.write('schema.sql')

  ci.parse(filename, cs.data_types)
end

