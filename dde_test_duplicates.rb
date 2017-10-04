#!/usr/bin/ruby -w

require 'elasticsearch'
require 'rubygems'
require 'mysql2'
require 'rest-client'



def dbconnect(host,user,pwd,db)
  @cnn = Mysql2::Client.new(:host => "#{host}", :username => "#{user}",:password => "#{pwd}",:database => "#{db}")
end

def querydb(seqel)
  @rs = @cnn.query("#{seqel}")
end

def connect_to_mysqldb(h,u,p,dbname)
  dbconnect("#{h}","#{u}","#{p}","#{dbname}")
end


def check_duplicates(h,u,p,db)
  connect_to_mysqldb(h,u,p,db)
  puts "Getting duplicate data from database"
  mysql_data = querydb('select value, data from national_patient_identifiers n left join people p on n.person_id = p.id where n.voided = 0 group by value having count(n.value) > 1')

  client = Elasticsearch::Client.new url: "http://#{h}:9200"
  n = 0
  i = 0
  j = 0
  k = 0
  
  npids_not_found = []
  npids_1_record_found = []
  npids_2_or_more_records_found = []

  mysql_data.each do |row|
    npid = row['value']
    puts "Testing #{npid} ..record #{i}"
    es_client = client.search index: 'dde', body:{query:{match:{ _all: npid}}}
    puts es_client['hits']['total']

    case es_client['hits']['total']

    when 1
      puts "No Record Found"
      n += 1
      npids_not_found << "#{npid}"
    when 2
      puts "One Record Found"
      j += 1
      npids_1_record_found << "#{npid}"
    end
    if es_client['hits']['total'] > 2 then
      puts "Two or more records Found"
      k += 1
      npids_2_or_more_records_found << "#{npid}"
    end 
    i += 1
  end
  log = File.new('log/duplicate.log', 'w')
  log.syswrite("Records not Found \n #{npids_not_found} \n\n One Record Found \n #{npids_1_record_found} \n\n Two or more records found \n #{npids_2_or_more_records_found}")
  puts "Records checked #{i}: Records not found #{n}: Record found with one record #{j}: Records Found >2 #{k}"

end
#Start program
h = ARGV[0]
u = ARGV[1]
p = ARGV[2]
db = ARGV[3]

if h.nil? || u.nil? || p.nil? || db.nil? then
  puts 'Please execute command as "ruby test_dde3_migrated_data_v1.0.rb host_ip_address dde1_db_username dde1_db_password dde1_database_name"'
  exit
end

check_duplicates(h,u,p,db)

