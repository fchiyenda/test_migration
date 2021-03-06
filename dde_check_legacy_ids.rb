#!/usr/bin/ruby -w

require 'rubygems'
require 'mysql2'
require 'rest-client'
require 'json'
require 'colorize'

def dbconnect(host,user,pwd)
  @cnn = Mysql2::Client.new(:host => "#{host}", :username => "#{user}",:password => "#{pwd}")
end

def querydb(seqel)
  @rs = @cnn.query("#{seqel}")
end

def connect_to_mysqldb(h,u,p)
  dbconnect("#{h}","#{u}","#{p}")
end


def check_legacy_idz(h,u,p,dde_db,app_db,cdb)
  connect_to_mysqldb(h,u,p)
  #get all ids
  #Create temporary table t1
  puts 'Drop temporary table t1'
  querydb("drop table if exists #{dde_db}.t1")
  puts 'Create temporary table t1'
  querydb("create temporary table #{app_db}.t1 as (select patient_id,value,p.data as data from #{dde_db}.national_patient_identifiers n join #{dde_db}.people p on n.person_id = p.id join #{app_db}.patient_identifier i on n.value = i.identifier where p.data is not null group by value having not count(value) > 1)")

  puts 'Getting All Legacy Idz'
  person_ids = querydb("select t.value value,group_concat(p.identifier) legacy, data from #{app_db}.t1 t join #{app_db}.patient_identifier p on t.patient_id = p.patient_id where p.voided = 0 and p.identifier_type <> 3 group by p.patient_id")
  #create file to record ids
  	non_matched_legacy = File.new('log/legacy.log','w')
    i = 1
    f = 0

  person_ids.each do |row|
  	puts "Checking Legacy ids for #{row['value']}"
  	npid = row['value']

  	#Get equivalent record in couchdb
    begin
        doc = RestClient.get("http://#{h}:5984/#{cdb}/#{npid}")
    rescue RestClient::ExceptionWithResponse
	    puts "#{npid} not found!"
      i += 1
  	  next 
    end
	  couchdb_data = JSON.parse(doc)
	  puts couchdb_data['patient']['identifiers'].inspect	
      larry = row['legacy'].split(',').map{ |s|s.to_s}     
      puts larry.inspect
      not_present_legacy_npids = larry.to_a - couchdb_data['patient']['identifiers'].to_a
      if not_present_legacy_npids.size != 0 then
      	puts "Some legacy ids did not match for #{npid}" 
      	non_matched_legacy.syswrite("#{npid} did not match #{not_present_legacy_npids}\n")
        f += 1
      end
    i += 1
  end
  puts "Checked #{i-1} records : records with not matched legacy ids #{f} " 
end
	
#Get parameters from terminal
h = ARGV[0]
u = ARGV[1]
p = ARGV[2]
dde_db = ARGV[3]
app_db = ARGV[4]
cdb = ARGV[5]

if h.nil? || u.nil? || p.nil? || dde_db.nil? || app_db.nil? || cdb.nil? then
  puts 'Please execute command as "ruby test_dde3_migrated_data_v1.0.rb host_ip_address username password dde1_db app_db couchdb_name"'.colorize(:red)
  exit
end

check_legacy_idz(h,u,p,dde_db,app_db,cdb)
