require 'rubygems'
require 'rest-client'
require 'json'
require 'colorize'
require 'mysql2'

def dbconnect(host,user,pwd,db)
  @cnn = Mysql2::Client.new(:host => "#{host}", :username => "#{user}",:password => "#{pwd}",:database => "#{db}")
end

def querydb(seqel)
  @rs = @cnn.query("#{seqel}",)
end

def connect_to_mysqldb(h,u,p,dbname)
  dbconnect("#{h}","#{u}","#{p}","#{dbname}")
end

def check_id_for_observations(person_id)
	obs = querydb("Select * from obs where person_id = '#{person_id}' and voided = 0")
	return true if obs.count >= 1
end

def verify_data_with_blanks(h,u,p,mysqldb,couchdb_host,couchdb,site_code)

	i = 0
	n = 100000
	lmt = 100000
	probs = []
	doc_with_probs = []
	with_observations = []
	blanks_log = File.new("log/blanks.log", 'w')
 
 #Get total number of documents
 puts "Getting Total number of records"
  count = RestClient.get("http://#{couchdb_host}:5984/#{couchdb}/_all_docs?skip=0&limit=2")
  cnt = JSON.parse(count)

  while i <= cnt['total_rows'] do
  	puts "processing #{i} to #{n}"
		docs = RestClient.get("http://#{couchdb_host}:5984/#{couchdb}/_all_docs?include_docs=true&skip=#{i}&limit=#{lmt}")
		
		data = JSON.parse(docs)

	  data['rows'].each do |person|
	  	
	   	if person['doc']['_id'].length == 6
	   		puts (person['doc']['birthdate'] == "" && person['doc']['names']['given_name'] == "" && person['doc']['names']['family_name'] == "" && person['doc']['assigned_site'] == "#{site_code}").inspect
	   		if person['doc']['birthdate'] == "" && person['doc']['names']['given_name'] == "" && person['doc']['names']['family_name'] == "" && person['doc']['assigned_site'] == site_code
	   			probs << person['doc']['_id']

					#Get equivalent from mysql
					connect_to_mysqldb(h,u,p,mysqldb)
	  				puts 'Get mysql equivalent ....'

	  			source_data = querydb("select identifier,patient_id,pn.given_name,pn.family_name,pn.date_created,pn.voided,p.gender,p.birthdate from patient_identifier pi left join person_name pn on pi.patient_id = pn.person_id left join person p on pi.patient_id = p.person_id where pi.identifier = '#{person['doc']['_id']}'")
	  			source_data.each do |row|
	  				patient_id = row['patient_id']
	  				row.each do |key,value|	 
	  				  if key == 'given_name' || key == 'family_name'
		  					if row[key] != person['doc']['names'][key]
		  						blanks_log.syswrite("#{key} did not match for #{person['doc']['_id']}")
		  						doc_with_probs << person['doc']['_id']
		  						puts "#{key} did not match #{person['doc']['_id']}" 
		  					end
		  				elsif key == 'birthdate' || key == 'gender'
		  					if row[key] != person['doc'][key]
		  						blanks_log.syswrite("#{key} did not match for #{person['doc']['_id']} \n")
		  						 doc_with_probs << person['doc']['_id']
		  						puts "#{key} did not match #{person['doc']['_id']}"
			  				end
			  			end
			  		end
			  		if check_id_for_observations(patient_id) == true
			  			with_observations << person['doc']['_id']
			  		end
	  			end
				end
			end
		end
		i += 100000
		n += 100000
	end
	blanks_log.syswrite("\n\n\n NPIDs with problems: \n\n\n #{probs.uniq} \n\n\n NPIDs which do not match source data: \n\n\n #{doc_with_probs.uniq} \n\n\n NPIDs with observations: \n\n\n #{with_observations.uniq}")
  puts "NPIDs with missing mandatory fields: #{probs.uniq.count}, NPIDs with non matched mandatory fields #{doc_with_probs.uniq.count}, NPIDs with observations #{with_observations.uniq.count}"
end

#program starts here
h = ARGV[0]
u = ARGV[1]
p = ARGV[2]
mysqldb = ARGV[3]
couchdb_host = ARGV[4]
couchdb = ARGV[5]
site_code = ARGV[6]

if h.nil? || u.nil? || p.nil? || couchdb.nil? || mysqldb.nil? || couchdb_host.nil? || couchdb.nil? || site_code.nil? 
  puts 'Please execute command as "ruby verify_data_with_blanks.rb host_ip_address mysql_db_username mysql_db_password mysql_database_name couchdb_ip_address couch_database_name site_code" '.colorize(:red)
  exit
end

verify_data_with_blanks(h,u,p,mysqldb,couchdb_host,couchdb,site_code)