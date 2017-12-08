  #!/usr/bin/ruby -w

require 'rubygems'
require 'mysql2'
require 'rest-client'
require 'json'
require 'colorize'

def dbconnect(host,user,pwd,db)
  @cnn = Mysql2::Client.new(:host => "#{host}", :username => "#{user}",:password => "#{pwd}",:database => "#{db}")
end

def querydb(seqel)
  @rs = @cnn.query("#{seqel}",)
end

def connect_to_mysqldb(h,u,p,dbname)
  dbconnect("#{h}","#{u}","#{p}","#{dbname}")
end

def compare_data_with_couchdb

end

def get_source_data(h,u,p,dbname,cdbusr,cdbpwd,cdb)
  connect_to_mysqldb(h,u,p,dbname)
  puts 'Loading mysql data ....'

  source_data = querydb("select value from national_patient_identifiers n left join people p on n.person_id = p.id where n.voided = 0 and data is not null")
    mysql_npids = source_data.map {|y|y['value']}
  
  puts "Create view for query couchdb"
    system("curl -X PUT http://#{cdbusr}:#{cdbpwd}@#{h}:5984/#{cdb}/_design/identifiers --data-binary @identifiers.json")    
                                                                                                                                                     
  puts 'Loading couchdb data ....'
    begin
  	  doc = RestClient.get("http://#{h}:5984/#{cdb}/_design/identifiers/_view/get_all_identifiers")
    rescue RestClient::ExceptionWithResponse
	 end
 puts 'Parsing couchdb data ...'
    d = JSON.parse(doc)
    puts 'Filtering couchdb data ...'
    puts 'Filtering all Primary NPIDs'
    primary_npids = d['rows'].map {|y|y['value']['id']}
    primary_npids = primary_npids.select{|r|r.size == 6}
    puts 'Filtering all legacy NPIDs'
    legacy_npids = d['rows'].map {|s|s['value']['identifiers']} 
    legacy_npids = legacy_npids.flatten
    legacy_npids = legacy_npids.select{|z|z.size == 6}
    puts 'Combine legacy NPIDs with Primary'
    couchdb_npids  = primary_npids + legacy_npids

    puts 'Calulating Primary ids that are also need to be stripped'
    need_to_strip_ids = primary_npids & legacy_npids

  records_not_found = File.new('log/dde_not_found.log', 'w')
  tested_npids = File.new('log/dde_tested_npids.log', 'w')
  need_to_strip = File.new('log/dde_need_to_strip.log', 'w')

  puts 'Computing records not present in couchdb ...' 
  not_found_npids = mysql_npids - couchdb_npids
  #write results to File
  puts 'Writing results to File'
  tested_npids.syswrite("#{mysql_npids}")
  records_not_found.syswrite("#{not_found_npids}")
  need_to_strip.syswrite("#{need_to_strip_ids}")

  puts "Checked #{mysql_npids.length} records : records not found #{not_found_npids.length} : records that need to be striped #{need_to_strip_ids.length}"
end
#Start program
#Get parameters from terminal
h = ARGV[0]
u = ARGV[1]
p = ARGV[2]
dbname = ARGV[3]
cdbusr = ARGV[4]
cdbpwd = ARGV[5]
cdb = ARGV[6]

if h.nil? || u.nil? || p.nil? || dbname.nil? || cdb.nil? || cdbusr.nil? || cdbpwd.nil? then
  puts 'Please execute command as "ruby test_dde3_migrated_data_v1.0.rb host_ip_address dde1_db_username dde1_db_password dde1_database_name couchdbname" '.colorize(:red)
  exit
end

get_source_data(h,u,p,dbname,cdbusr,cdbpwd,cdb)