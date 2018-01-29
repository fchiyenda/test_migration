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
  unassigned_not_found = []
   
  puts "Create view for querying couchdb"
    system("curl -X PUT http://#{cdbusr}:#{cdbpwd}@#{h}:5984/#{cdb}/_design/identifiers --data-binary @identifiers.json")    
                                                                                                                                                     
  puts 'Loading couchdb data ....'
    begin
  	  doc = RestClient.get("http://#{h}:5984/#{cdb}/_design/identifiers/_view/get_all_identifiers/")
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

    all_npids = primary_npids + legacy_npids
    
    puts 'Check for unassigned primary_npids with person data'
      client = Elasticsearch::Client.new url: "http://#{h}:9200"
      #puts 'Checking in couchdb'
      #couchdb_npid = RestClient.get("http://#{h}:5984/#{cdb.gsub('_person','')}/_all_docs?include_docs=true&limit=2")
      #couchdb_npid = JSON.parse(couchdb_npid)

      #raise couchdb_npid.inspect


      legacy_npids.uniq.each do |npid|
        puts "Checking #{npid}"
          tested_assigned << npid
          es_client = client.search index:'dde',type:'npids', body:{query:{match:{ national_id: npid}}}

    if es_client['hits']['total'] == 0 #If NPID not found in NPID DB
      #No record found in database
      unassigned_not_found << npid
    else
      npid_decimal_value = es_client['hits']['hits'][0]['_id']

      #puts 'Checked if it is marked as assigned'
      if es_client['hits']['hits'][0]['_source'].has_key?('assigned')
         if es_client['hits']['hits'][0]['_source']['assigned'] == true
               #puts 'OK!'
               assigned_npids << "NPID: #{npid}  : Decimal value: #{npid_decimal_value}"
            else 
               #puts 'Something is wrong'
               unassigned_npids << "NPID: #{npid}  : Decimal value: #{npid_decimal_value}"
            end
          else
            #puts 'Something is wrong'
            unassigned_npids << "NPID: #{npid}  : Decimal value: #{npid_decimal_value}"
          end
          printf("\rPercentage complete: %.1f record %.d of %.d",(tested_assigned.length/all_npids.uniq.length.to_f*100.0),tested_assigned.length,all_npids.uniq.length)
      end
    end

  puts "Writing results to log"
  
  log.syswrite("NPIDs with Demographics but with no NPID record in NPID database: \n\n #{unassigned_not_found} \n\n\n\n\n NPIDs that has Demographics but NPID is not flagged as assigned: #{unassigned_npids} \n\n\n\n\n NPIDS that are okey: #{assigned_npids} \n\n\n\n\n NPIDS tested: #{tested_assigned}")

  puts "Assigned NPIDs: #{assigned_npids.length} : Unassigned #{unassigned_npids.length} : Unassigned not found: #{unassigned_not_found.length} : NPIDs tested: #{tested_assigned.length}"
end


#Start program
#Get parameters from terminal
h = ARGV[0]
cdbusr = ARGV[1]
cdbpwd = ARGV[2]
cdb = ARGV[3]

if h.nil? || cdb.nil? || cdbusr.nil? || cdbpwd.nil? then
  puts 'Please execute command as "ruby unassigned_npid.rb.rb host_ip_address couchdb_username couchdb_password couchdb_person_database_name" '.colorize(:red)
  exit
end

get_source_data(h,cdbusr,cdbpwd,cdb)