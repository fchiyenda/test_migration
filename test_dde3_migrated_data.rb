#!/usr/bin/ruby -w

require 'rubygems'
require 'mysql2'
require 'rest-client'
require 'json'

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

def get_source_data(h,u,p,dbname,cdb)
  connect_to_mysqldb(h,u,p,dbname)
  puts 'Loading mysql data ....'

  source_data = querydb("select value from national_patient_identifiers n left join people p on n.person_id = p.id where n.voided = 0 and data is not null")
    mysql_npids = source_data.map {|y|y['value']}
  
  puts "Create view for query couchdb"
    system("curl -X PUT http://#{h}:5984/#{cdb}/_design/identifiers --data-binary @identifiers.json")    
                                                                                                                                                     
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
    puts 'Filtering all legacy NPIDs'
    primary_npids = primary_npids.select{|r|r.size == 6}
    legacy_npids = d['rows'].map {|s|s['value']['identifiers']} 
    legacy_npids = legacy_npids.flatten
    legacy_npids = legacy_npids.select{|z|z.size == 6}
    puts 'Combine legacy NPIDs with Primary'
    couchdb_npids  = primary_npids + legacy_npids

  records_not_found = File.new('log/dde_not_found.log', 'w')
  tested_npids = File.new('log/dde_tested_npids.log', 'w')
=begin
  source_data.each do |row|
    npid = row['value']
	puts "Testing #{npid} ...record num #{i}"
	tested_npids.syswrite("#{npid}\n")
	unless couchdb_npids.include?("#{npid}") then
	  puts couchdb_npids.include?("#{npid}")
	  puts "#{npid} not found"
	  records_not_found.syswrite("#{npid}\n")
	  f += 1
    end
    i += 1
  end
=end
  puts 'Computing records not present in couchdb ...' 
  not_found_npids = mysql_npids - couchdb_npids
  #write results to File
  puts 'Writing results to File'
  tested_npids.syswrite("#{mysql_npids}")
  records_not_found.syswrite("#{not_found_npids}")

  puts "Checked #{mysql_npids.length} records : records not found #{not_found_npids.length} "
end
#Start program
#Get parameters from terminal
h = ARGV[0]
u = ARGV[1]
p = ARGV[2]
dbname = ARGV[3]
cdb = ARGV[4]

if h.nil? || u.nil? || p.nil? || dbname.nil? || cdb.nil? then
  puts 'Please execute command as "ruby test_dde3_migrated_data_v1.0.rb host_ip_address dde1_db_username dde1_db_password dde1_database_name couchdbname" '
  exit
end

get_source_data(h,u,p,dbname,cdb)