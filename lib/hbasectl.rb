include Java
require 'logger'
require 'socket'
java_import 'org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest'
java_import 'org.apache.hadoop.hbase.executor.EventHandler'
java_import 'org.apache.hadoop.hbase.HConstants'

module HbaseCtl

  module Constants
    ZOO_PERM_READ   = 0
    ZOO_PERM_WRITE  = 1
    ZOO_PERM_CREATE = 2
    ZOO_PERM_DELETE = 4
    ZOO_PERM_ADMIN  = 8
    ZOO_PERM_ALL    = ZOO_PERM_READ | ZOO_PERM_WRITE | ZOO_PERM_CREATE | ZOO_PERM_DELETE | ZOO_PERM_ADMIN

    MODE_PERSISTENT = 0
    MODE_PERSISTENT_SEQUENTIAL = 2
    MODE_EPHEMERAL = 1
    MODE_EPHEMERAL_SEQUENTIAL = 3

    COMPACTION_STATE_NONE = CompactionRequest::CompactionState::NONE
    COMPACTION_STATE_MAJOR = CompactionRequest::CompactionState::MAJOR
    COMPACTION_STATE_MINOR = CompactionRequest::CompactionState::MINOR
    COMPACTION_STATE_MAJOR_AND_MINOR = CompactionRequest::CompactionState::MAJOR_AND_MINOR

    RS_REGION_SPLIT = EventHandler::EventType::RS_ZK_REGION_SPLIT
    RS_REGION_SPLITTING = EventHandler::EventType::RS_ZK_REGION_SPLITTING
    RS_REGION_SPLIT_NONE = 100

    # info
    CATALOG_FAMILY_STR = HConstants::CATALOG_FAMILY_STR
    # info in bytes
    CATALOG_FAMILY = HConstants::CATALOG_FAMILY

    # regioninfo
    REGIONINFO_QUALIFIER = HConstants::REGIONINFO_QUALIFIER
    # regioninfo in bytes
    REGIONINFO_QUALIFIER_STR = String.from_java_bytes(REGIONINFO_QUALIFIER)
    # info:regioninfo
    REGIONINFO_FAMILY = "#{CATALOG_FAMILY_STR}:#{REGIONINFO_QUALIFIER_STR}"

    # splitA
    SPLITA_QUALIFIER = HConstants::SPLITA_QUALIFIER
    # splitA in bytes
    SPLITA_QUALIFIER_STR = String.from_java_bytes(SPLITA_QUALIFIER)
    # info:splitA
    SPLITA_FAMILY = "#{CATALOG_FAMILY_STR}:#{SPLITA_QUALIFIER_STR}"

    # splitB
    SPLITB_QUALIFIER = HConstants::SPLITB_QUALIFIER
    # splitB in bytes
    SPLITB_QUALIFIER_STR = String.from_java_bytes(SPLITB_QUALIFIER)
    # info:splitB
    SPLITB_FAMILY = "#{CATALOG_FAMILY_STR}:#{SPLITB_QUALIFIER_STR}"



  end

  class << self
    attr_accessor :debug, :logger , :hostname, :dryrun, :runas
    attr_reader :version, :current_user

    version = '1.0'

    unless HbaseCtl.debug
      org.apache.log4j.Logger.getLogger('org.apache.zookeeper').set_level(org.apache.log4j.Level::OFF)
      org.apache.log4j.Logger.getLogger('org.apache.hadoop.hbase').set_level(org.apache.log4j.Level::OFF)
      org.apache.log4j.Logger.getLogger('org.apache.hadoop').set_level(org.apache.log4j.Level::OFF)
    end

    def log(str)
      str << " DRUN RUN" if HbaseCtl.dryrun && str === String
      logger.debug { str }
    end

    def renew_ticket
      keytab = "/homes/#{@runas}/#{@runas}.keytab"
      principal = "#{@runas}@DOMAIN.COM"
      unless File.exists?(keytab)
        HbaseCtl.log("#{@hostname}:#$0:#{__method__}:# #{keytab} not found")
        raise RuntimeError.new("#{@hostname}:#$0:#{__method__}:# #{keytab} not found")
      end
      unless system("klist -s")
        HbaseCtl.log("#{@hostname}:#$0:#{__method__}:# renewing kerberos ticket")
        unless system("kinit -kt #{keytab} #{principal}")
          HbaseCtl.log("#{@hostname}:#$0:#{__method__}:# kinit failed")
          return false
        else
          return true
        end
      else
        return true
      end

    end

    def validate_user
      if @current_user != @runas
        HbaseCtl.log("#{@hostname}:#$0:#{__method__}:# #$0 should be invoked by #{@runas}")
        return false
      end
      HbaseCtl.log("#{@hostname}:#$0:#{__method__}:# #$0 started by #{@runas}")
      return true
    end

    

  end # module methods end

  @logger ||= ::Logger.new(STDOUT)
  @hostname ||= ::Socket.gethostname
  @dryrun ||= false
  @runas ||= "hbase"
  @current_user = Etc.getpwuid(Process.uid).name

  class CommandFailed < StandardError

    # The command that failed as a string
    attr_reader :command

    # The integer exitstatus
    attr_reader :exitstatus

    # Everything output on the command's stderr as a string
    attr_reader :err

    def initialize(command,exitstatus=nil, err='')
      if exitstatus
	@command = command
	@exitstatus = exitstatus
	@err = err

	message = "Command failed [#{exitstatus}] : #{command}"
	message << "\n\n" << err unless err.nil? || err.empty?
	super message
      else
        super command
      end
    end

  end # class CommandFailed end
  
  class ZookeeperException < CommandFailed

  end


end #module end


