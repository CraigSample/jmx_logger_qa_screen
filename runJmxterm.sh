#!/usr/bin/expect -f
		set CHILD_PID [spawn java -jar libs/jmxterm-1.0.0-uber.jar -n -v silent -l service:jmx:rmi:///jndi/rmi://127.0.0.1:7199/jmxrmi]
		send "domain org.apache.cassandra.metrics\r"
		while { true } {
		        set now [clock seconds]
		        set date [clock format $now -format {%D %T}]
		        set date
		        puts $date
		        send "get -s -b keyspace=keyspace1,name=LiveSSTableCount,scope=standard1,type=ColumnFamily Value && get -s -b keyspace=keyspace1,name=LiveSSTableCount,scope=counter1,type=ColumnFamily Value && get -s -b keyspace=keyspace1,name=AllMemtablesLiveDataSize,scope=standard1,type=ColumnFamily Value && get -s -b keyspace=keyspace1,name=AllMemtablesLiveDataSize,scope=counter1,type=ColumnFamily Value && get -s -b type=ClientRequest,scope=Read,name=Latency 95thPercentile && get -s -b type=ClientRequest,scope=Write,name=Latency 95thPercentile\r"
		        expect sleep 1
		}

send "quit \n"
expect eof
