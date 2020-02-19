require 'yaml'
require 'date'
require 'optparse'
require 'open3'
require 'fileutils'

##
# sync a directory to guarantee it's on disk. have to recurse to the root
# to guarantee sync for newly created directories.
def fdirsync(d)
  while d != '/'
    Dir.open(d) do |dh|
      io = IO.for_fd(dh.fileno)
      io.fsync
    end
    d = File.dirname(d)
  end
end


def valid_lsn(s)
  return false unless s.is_a?(String)
  !(%r{^[0-9A-Z]+/[0-9A-Z]{8}$} =~ s).nil?
end


##
# State encapsulates the minimum set of state information: the last sequence
# number number that we used, the timestamp of the last run, and the Postgres
# LSN (Log Sequence Number).
#
# The **sequence number** is the number of the last file that was written, so
# if it's 123456789 then we last wrote 123/456/789.osc.gz, and the next file
# to be written will be 123/456/790.osc.gz
#
# The timestamp is just the timestamp of the last run. This isn't currently
# used in the replication algorithm itself, but it's really very useful to
# have to look at for debugging.
#
# The LSN is the position in the Postgres WAL log of the last commit entry we
# saw. It is a measure of where in the Postgres stream of changes we are. The
# value isn't directly used (Postgres keeps track of it in the replication
# slot for us), but it's useful for debugging and (perhaps) making sure we
# don't see duplicate results.
State = Struct.new(:sequenceNumber, :timestamp, :lsn) do
  def self.from_file(fh)
    data = Hash.new

    # parse state.txt's simple text format; each line is either a hash-denoted
    # comment or a k=v line without whitespace.
    fh.each_line do |line|
      unless line.start_with?('#')
        k, v = line.chomp.split('=')
        data[k] = v
      end
    end

    # on error, the state file has a tendency to be empty. this is techically
    # valid as per the format above, but we require the presence of at least
    # a minimum set of keys.
    self.members.each do |k|
      return :state_file_corrupt unless data.key?(k.to_s)
    end

    # Ruby's .to_i returns zero on failure, so check that the sequence number is
    # a sensible value. NOTE: this means we'll have to manually create a
    # sequence number zero entry if we _want_ to start from zero. but we might
    # as well start from 1 in practice.
    sequence = data['sequenceNumber'].to_i
    return :state_file_corrupt if sequence < 1

    # TODO: more validation on the last LSN - although we don't use it (Postgres
    # should keep track of it in the replication slot) it's probably good to
    # have it around in case of failure.
    lsn = data['lsn']
    if lsn.nil? || !valid_lsn(lsn)
      raise StandardError.new('State file has nil or missing LSN')
    end

    # the timestamp, for reasons lost to the mists of time, escapes the colons
    # in the timestamp. so we copy the same bug...
    begin
      timestamp = DateTime.iso8601(data['timestamp'].delete('\\'))
    rescue StandardError
      return :state_file_corrupt
    end

    State.new(sequence, timestamp, lsn)
  end

  def to_file(fh)
    fh.write(to_h.map { |k, v| "#{k}=#{v}\n" }.join)
  end
end


class DoubleBuffer
  def initialize(klass, backup_file)
    @klass = klass
    @backup_file = backup_file
  end

  def from_file(fh)
    instance = @klass.from_file(fh)

    # if the file wasn't read correctly, and we couldn't get a good state
    # from it, then try the backup file. if that's OK then we can use that.
    # note that we _keep_ the lock on the main state file - we shouldn't
    # touch any of the state files without that lock!
    unless instance.is_a?(@klass)
      # try reading from backup file
      bak_instance = File.open(@backup_file, File::RDONLY) { |bak|
        @klass.from_file(bak)
      }

      # only use the backup if it read correctly, otherwise just use the
      # original (presumably error).
      instance = bak_instance if bak_instance.is_a?(@klass)
    end

    instance
  end

  def to_file(instance, fh)
    data_dir = File.dirname(File.absolute_path(@backup_file))

    # first, write double-buffer file. if we crash while writing the locked
    # state file, we can recover from this file.
    File.open(@backup_file, File::RDWR | File::CREAT | File::TRUNC) do |bak|
      instance.to_file(bak)
    end

    # second, sync the file and the directories it's in. this makes sure that
    # the data is really on disk.
    File.open(@backup_file, File::RDWR) { |bak| bak.fsync }
    fdirsync(data_dir)

    # third, rewrite the state file.
    fh.truncate(0)
    instance.to_file(fh)
    fh.fsync

    # finally, sync the directories again, to make really sure :-)
    fdirsync(data_dir)
  end
end


class DBTException < StandardError
end


class Database
  def initialize(config_file, output_dir, bin_dir)
    @config_file = config_file
    @output_dir = output_dir
    @bin_dir = bin_dir
  end

  def write_diff(state)
    timestamp = DateTime.now
    sequence = state.sequenceNumber + 1

    # peek into the database and get a list of changes
    lsn, tmpfile = peek(state)

    # if no changes, then short circuit - no need to write an empty file?
    return state if lsn == state.lsn

    # create the OSC diff file (in /tmp)
    tmpdiff = create_diff(tmpfile)

    # copy the diff and create a new numbered state file
    new_state = State.new(sequence, timestamp, lsn)
    copy_diff(new_state, tmpdiff)

    # return the new state
    new_state
  end

  def catchup(state)
    run('osmdbt-catchup', '-l', state.lsn)
    state
  end

  private

  def peek(old_state)
    output = run('osmdbt-get-log', '--ignore-earlier-than', old_state.lsn,
                 '--empty-is-ok')

    lsn = nil
    tmpfile = nil
    no_changes = false
    output.lines.each do |line|
      m = %r{LSN is ([0-9A-Z/]*)}.match(line)
      lsn = m[1] if m

      m = %r{Writing log to '([^']*)'}.match(line)
      tmpfile = m[1] if m

      m = %r{No changes found}.match(line)
      no_changes = true if m
    end

    return [old_state.lsn, nil] if no_changes

    if lsn.nil? || tmpfile.nil?
      raise DBTException.new("Failed to peek, expecting LSN and tmpfile in output, but only saw:\n#{output}")
    end

    unless valid_lsn(lsn)
      raise DBTException.new("Failed to peek, expecting LSN but got #{lsn.inspect} from output:\n#{output}")
    end

    unless File.exist?(tmpfile)
      raise DBTException.new("Expecting log file in #{tmpfile.inspect}, but no such file exists.")
    end

    [lsn, tmpfile]
  end

  def create_diff(tmpfile)
    output = run('osmdbt-create-diff', '-f', tmpfile)

    tmpdiff = nil
    output.lines.each do |line|
      m = %r{Opening output file '([^']*).new'}.match(line)
      tmpdiff = m[1] if m
    end

    if tmpdiff.nil?
      raise DBTException.new("Failed to create-diff, expecting tmpdiff in output, but only saw:\n#{output}")
    end

    unless File.exist?(tmpdiff)
      raise DBTException.new('Output file should be #{tmpdiff.inspect}, but no such file exists.')
    end

    tmpdiff
  end

  def copy_diff(state, tmpdiff)
    # make sure output directory actually exists
    dir = sprintf('%s/%03d/%03d', @output_dir,
                  state.sequenceNumber / 1000_000,
                  (state.sequenceNumber / 1000) % 1000)
    FileUtils.mkdir_p(dir)
    file_stem = sprintf('%03d', state.sequenceNumber % 1000)
    state_file = "#{dir}/#{file_stem}.state.txt"
    diff_file = "#{dir}/#{file_stem}.osc.gz"

    # output state file
    File.open(state_file, File::RDWR | File::CREAT) do |fh|
      state.to_file(fh)
      fh.fsync
    end

    # move diff into place
    FileUtils.mv(tmpdiff, diff_file)

    # sync the directory, just in case
    fdirsync(dir)
  end

  def run(cmd, *args)
    output, status = Open3.capture2e("#{@bin_dir}/#{cmd}", '--config', @config_file, *args)
    unless status.success?
      raise DBTException.new("Tried to run #{cmd.inspect} --config #{@config_file.inspect} #{args.inspect}, but got error: #{status.to_s}. Output:\n#{output}")
    end
    output
  end
end


##
# exec_transaction locks the state file, parses it and provides it to the
# database. the database should then update the state and return it, and
# exec_transaction writes it safely back to disk.
def exec_transaction(state_file, database)
  backup_file = state_file + '.bak'
  double_buf = DoubleBuffer.new(State, backup_file)

  File.open(state_file, File::RDWR) do |fh|
    # rather than blocking and waiting for the file to become available, we
    # should just return with an error, which probably isn't really a big
    # problem unless there's a _lot_ of them and it causes replication to
    # lag behind.
    fh.flock(File::LOCK_EX | File::LOCK_NB) || (return :state_file_in_use)

    # read state from file, or from backup file if it's available.
    state = double_buf.from_file(fh)
    return state unless state.is_a?(State)

    # get the changes from the database and write the OSC file, returning
    # the new state (or an error)
    new_state = database.write_diff(state)
    return new_state unless new_state.is_a?(State)

    # write the backup and new state
    double_buf.to_file(new_state, fh)

    # catchup, let the database know that we've successfully written the changes
    # and it should advance the replication slot position.
    database.catchup(new_state)
  end
end


def time_ago_desc(sec)
  if sec < 60
    "#{sec.to_i} seconds"
  elsif sec < (60 * 60)
    "#{(sec / 60).to_i} minutes"
  elsif sec < (24 * 60 * 60)
    "#{(sec / (60 * 60)).to_i} hours"
  elsif sec < (7 * 24 * 60 * 60)
    "#{(sec / (24 * 60 * 60)).to_i} days"
  else
    "#{(sec / (7 * 24 * 60 * 60)).to_i} weeks"
  end
end


def check_last_update_time(state_file, stale_timeout)
  state_time = File.mtime(state_file)
  state_updated_ago = Time.now - state_time
  if state_updated_ago >= stale_timeout
    desc = time_ago_desc(state_updated_ago)
    abort("ERROR: state file last updated #{desc} ago, at #{state_time}. Is update process hung?")
  end
end


if __FILE__ == $0
  config_file = nil

  OptionParser.new do |opts|
    opts.banner = 'Usage: replicate.rb CONFIG.YML'

    opts.on('-c', '--config CONFIG', 'Configuration file') do |cfg|
      config_file = cfg
    end
  end.parse!

  config = YAML.load_file(config_file)
  state_file = config['base-dir'] + '/state.txt'
  osmdbt_config = config['osmdbt-config']

  database = Database.new(osmdbt_config, config['base-dir'], config['osmdbt-bin'])
  status = exec_transaction(state_file, database)

  if status == :state_file_in_use
    # if the last update was a long time ago, then there might be a problem, so
    # be louder about the failure. otherwise it might just indicate a fairly
    # large diff that takes more time to extract from the database and write.
    check_last_update_time(state_file)

  elsif !status.is_a?(State)
    # generic unhandled error!
    abort("ERROR: Unknown status: #{status.inspect}")
  end
end
