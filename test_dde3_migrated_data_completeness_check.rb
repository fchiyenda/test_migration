#!/usr/bin/ruby -w

require 'rubygems'
require 'mysql2'
require 'rest-client'
require 'json'
#require 'elasticsearch'

def dbconnect(host,user,pwd,db)
  @cnn = Mysql2::Client.new(:host => "#{host}", :username => "#{user}",:password => "#{pwd}",:database => "#{db}")
end

def querydb(seqel)
  @rs = @cnn.query("#{seqel}")
end

def connect_to_mysqldb(h,u,p,dbname)
  dbconnect("#{h}","#{u}","#{p}","#{dbname}")
end

def compare_data_with_couchdb

end

def get_source_data(h,u,p,dbname,couchdb)
  connect_to_mysqldb(h,u,p,dbname)
  puts 'Loading mysql data ....'

  source_data = querydb("select value, data from national_patient_identifiers n left join people p on n.person_id = p.id where n.voided = 0 and data is not null limit 100")

  total_records = source_data.count

  i = 0.0
  n = 0
  f = 0

  log = File.new("log/dde_completeness_migration.log", "w")
  records_not_found = File.new('log/dde_completeness_not_found.log', 'w')
  tested_npids = File.new('log/dde_completeness_tested_npids.log', 'w')

  group_problems_found = []
  group_npids = Hash.new { |hash, key| hash[key] = []} #Prepare hash to store arrays

  source_data.each do |row|
		npid = row['value']
		if row['data'] != nil then
			mysql_client_data = JSON.parse(row['data'])
			#puts "Testing #{npid} ...record num #{i}"
			tested_npids.syswrite("'#{npid}',")
		else
			next
		end

	begin
	  doc = RestClient.get("http://#{h}:5984/#{couchdb}/#{npid}")
	rescue RestClient::ExceptionWithResponse
	  #puts "#{npid} not found!"
  	  records_not_found.syswrite("'#{npid}',")
  	  f += 1
  	  i += 1
  	  printf("\rPercentage Complete: %.1f%",(i/total_records*100))
      next 
	end

	couchdb_data = JSON.parse(doc)


		# Remove non required fields
		couchdb_data['names'].delete('family_name_code')
		couchdb_data['names'].delete('given_name_code')
		couchdb_data['names'].delete('middle_name')
		couchdb_data['names'].delete('maiden_name')

		#Rename keys for mapping for mysql data
		mysql_client_data['addresses']['home_ta'] = mysql_client_data['addresses'].delete('county_district')
		mysql_client_data['addresses']['current_district'] = mysql_client_data['addresses'].delete('state_province')
		mysql_client_data['addresses']['current_village'] = mysql_client_data['addresses'].delete('city_village')
		mysql_client_data['addresses']['home_district'] = mysql_client_data['addresses'].delete('address2')
		mysql_client_data['addresses']['home_village'] = mysql_client_data['addresses'].delete('neighborhood_cell')
		mysql_client_data['addresses']['closest_landmark'] = mysql_client_data['addresses'].delete('address1')


		#Clean mysql data
		if mysql_client_data['birthdate_estimated'].to_s == '1' then
			mysql_client_data['birthdate_estimated'] = true
		elsif mysql_client_data['birthdate_estimated'].to_s == '0' then
			mysql_client_data['birthdate_estimated'] = false
		end

		#Clean couchdb data
		if couchdb_data['birthdate_estimated'].to_s == '1' then
			couchdb_data['birthdate_estimated'] = true
		elsif couchdb_data['birthdate_estimated'].to_s == '0' then
			couchdb_data['birthdate_estimated'] = false
		end

		#Check First level Names,Cellphone Number and Gender
  mysql_client_data.each do |key,value|
	  #puts mysql_client_data[key]
	  #puts couchdb_data[key]
    if mysql_client_data[key].to_s.strip != couchdb_data[key].to_s.strip then #If data does not match check further
	  case key 
	  when 'identifiers','addresses','attributes','names'
		mysql_client_data[key].each do |k,v|
		  if couchdb_data[key][k].to_s.strip != v.to_s.strip then
		  	#puts "#{k} did not match for #{npid}"
            group_problems_found << k
            group_npids[k].push npid
            #log.syswrite("#{k} did not match for #{npid} \n")
		    n += 1
		  end
	    end
	  when 'patient' #Separated because of array complication
	    if couchdb_data[key].nil? then
	  	  #puts "#{key} did not match for #{npid}"
	  	  group_problems_found << key
	  	  group_npids[key].push npid
		  #log.syswrite("#{key} did not match for #{npid} \n")
		  n += 1
	  	else
	  	  mysql_client_data[key].each do |k,v|
	  	    if couchdb_data[key][k][0].to_s.strip != v.to_s.strip then
		  	  #puts "#{k} did not match for #{npid}"
		  	  group_problems_found << k
		  	  group_npids[k].push npid 
		      #log.syswrite("#{k} did not match for #{npid} \n")
		      n += 1
		    end
	      end
	    end
      else
		#puts "#{key} did not match for #{npid}"
		group_problems_found << key
		group_npids[key].push npid
		#log.syswrite("#{key} did not match for #{npid} \n")
	    n += 1
	  end
    end
  end
  i += 1
  printf("\rPercentage Complete: %.1f%", (i/total_records*100)) 
end
 log.syswrite("Migration Summary \nNumber of Records Categorized by problems: \n")
 #log.syswrite(group_problems_found.uniq.map{|x| [x,group_problems_found.count(x)]}.to_h)
 b = group_npids.map{|x,y|[x => y.size ]}
 log.syswrite(JSON.pretty_generate(b))
 log.syswrite("\n \n \n NPIDs affected grouped by Field: \n \n \n")
 log.syswrite(JSON.pretty_generate(group_npids))
 puts "\n Checked #{i.to_i} records : found #{n} problems : records not found #{f}  "
end

#Start program

#Get parameters from terminal
h = ARGV[0]
u = ARGV[1]
p = ARGV[2]
dbname = ARGV[3]
couchdb = ARGV[4]

if h.nil? || u.nil? || p.nil? || dbname.nil? || couchdb.nil? then
  puts 'Please execute command as "ruby test_dde3_migrated_data_completeness_check.rb host_ip_address dde1_db_username dde1_db_password dde1_database_name" couchdb '
  exit
end

get_source_data(h,u,p,dbname,couchdb)