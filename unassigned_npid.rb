  #!/usr/bin/ruby -w

require 'rubygems'
require 'mysql2'
require 'rest-client'
require 'json'
require 'colorize'
require 'elasticsearch'


def get_source_data(h,cdbusr,cdbpwd,cdb)

  log = File.new('log/unassigned.log', 'w')
  unassigned_npids = []
  assigned_npids = []
  tested_assigned = []
   
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
    
    puts 'Check for unassigned primary_npids with person data'
      client = Elasticsearch::Client.new url: "http://#{h}:9200"
      #puts 'Checking in couchdb'
      #couchdb_npid = RestClient.get("http://#{h}:5984/#{cdb.gsub('_person','')}/_all_docs?include_docs=true&limit=2")
      #couchdb_npid = JSON.parse(couchdb_npid)

      #raise couchdb_npid.inspect


      primary_npids.each do |npid|
        puts "Checking #{npid}"
          tested_assigned << npid
          es_client = client.search index:'dde',type:'npids', body:{query:{match:{ national_id: npid}}}
          npid_decimal_value = es_client['hits']['hits'][0]['_id']

         #puts 'Checked if it is marked as assigned'
          if es_client['hits']['hits'][0]['_source'].has_key?('assigned')
            if es_client['hits']['hits'][0]['_source']['assigned'] == true
               #puts 'OK!'
               assigned_npids << npid
            else 
               #puts 'Something is wrong'
               log.syswrite("NPID: #{npid} Decimal value: #{npid_decimal_value}")
               unassigned_npids << npid
            end
          else
            #puts 'Something is wrong'
            log.syswrite("NPID: #{npid}  : Decimal value: #{npid_decimal_value} \n")
            unassigned_npids << npid
          end
          printf("\rPercentage complete: %.1f record %.d of %.d",(tested_assigned.length/primary_npids.length.to_f*100.0),tested_assigned.length,primary_npids.length)
      end


  puts "Assigned NPIDs: #{assigned_npids.length} : Unassigned #{unassigned_npids.length}"
end
#Start program
#Get parameters from terminal
h = ARGV[0]
cdbusr = ARGV[1]
cdbpwd = ARGV[2]
cdb = ARGV[3]

if h.nil? || cdb.nil? || cdbusr.nil? || cdbpwd.nil? then
  puts 'Please execute command as "ruby test_dde3_migrated_data_v1.0.rb host_ip_address dde1_db_username dde1_db_password dde1_database_name couchdbname" '.colorize(:red)
  exit
end

get_source_data(h,cdbusr,cdbpwd,cdb)