#!/usr/bin/ruby -w

require 'rubygems'
require 'mysql2'
require 'rest-client'
require 'json'
#require 'elasticsearch'

def dbconnect(host,user,pwd)
  @cnn = Mysql2::Client.new(:host => "#{host}", :username => "#{user}",:password => "#{pwd}")
end

def querydb(seqel)
  @rs = @cnn.query("#{seqel}")
end

def connect_to_mysqldb(h,u,p)
  dbconnect("#{h}","#{u}","#{p}")
end


def check_legacy_idz(h,u,p)
  connect_to_mysqldb(h,u,p)
  #get all ids
  #Create temporary table t1
  puts 'Drop temporary table t1'
  querydb('drop table if exists dde_proxy.t1')
  puts 'Create temporary table t1'
  querydb('create temporary table kch_reg.t1 as (select patient_id,value,p.data as data from dde_proxy.national_patient_identifiers n join dde_proxy.people p on n.person_id = p.id join kch_reg.patient_identifier i on n.value = i.identifier where p.data is not null)')

  puts 'Getting All Legacy Idz'
  person_ids = querydb('select t.value value,group_concat(p.identifier) legacy, data from kch_reg.t1 t join kch_reg.patient_identifier p on t.patient_id = p.patient_id where p.voided = 0 and p.identifier_type <> 3 group by p.patient_id limit 10')
  #create file to record ids
  	non_matched_legacy = File.new('legacy.log','w')
    i = 1
    f = 0

  person_ids.each do |row|
  	puts "Checking Legacy ids for #{row['value']}"
  	npid = row['value']

  	#Get equivalent record in couchdb
    begin
	    doc = RestClient.get("http://#{h}:5984/dde_person_production/#{npid}")
	  rescue RestClient::ExceptionWithResponse
	    puts "#{npid} not found!"
      i += 1
  	  next 
    end
	  couchdb_data = JSON.parse(doc)
	  puts couchdb_data['patient']['identifiers'].inspect	
      larry = row['legacy'].split(",").map { |s| s.to_s}     
      puts larry.inspect
      if larry.to_a.sort != couchdb_data['patient']['identifiers'].to_a.sort then
      	puts "Some legacy ids did not match for #{npid}"
      	non_matched_legacy.syswrite("#{npid} \n")
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
#dbname = ARGV[3]

if h.nil? || u.nil? || p.nil? then
  puts 'Please execute command as "ruby test_dde3_migrated_data_v1.0.rb host_ip_address dde1_db_username"'
  exit
end

check_legacy_idz(h,u,p)
