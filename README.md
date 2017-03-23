# Gobblin-Work


Gobblin is a universal data ingestion framework for extracting, transforming, and loading large volume of data from a variety of data sources, e.g., databases, rest APIs, FTP/SFTP servers, filers, etc., onto Hadoop. Gobblin handles the common routine tasks required for all data ingestion ETLs, including job/task scheduling, task partitioning, error handling, state management, data quality checking, data publishing, etc. Gobblin ingests data from different data sources in the same execution framework, and manages metadata of different sources all in one place. This, combined with other features such as auto scalability, fault tolerance, data quality assurance, extensibility, and the ability of handling data model evolution, makes Gobblin an easy-to-use, self-serving, and efficient data ingestion framework.

Topics Covered :     
----------------

1. Pushing Json data from a Json file to HDFS as Avro files using Gobblin.(Json To Avro)                              
2. Pushing Json data through Kafka to Gobblin as saving as Avro files in HDFS.       



	This Gobblin project uses the following technologies:
	----------------------------------------------------
    * ZooKeeper    
    * Kafka 0.8.2     
    * Hadoop 2.6.0      
    * Gobblin 0.8.0      
    * Maven 3.3.9        

	Goblin Set-up:              
	--------------
#Step 1:         
	Download the latest Gobblin release from the Release Page
	or     
	wget https://github.com/linkedin/gobblin/releases/download/gobblin_0.8.0/gobblin-distribution-0.8.0.tar.gz    
	
 #Step 2 :     
 
	Extract gobblin-distribution-0.8.0.tar.gz to directory /opt/gobblin (mkdir /opt/gobblin/)
	tar -zxvf gobblin-distribution-0.8.0.tar.gz
	
#Step 3:                
	cd /opt/gobblin/gobblin-dist                 
	create following folder :          
	mkdir job_work         
	mkdir job_conf          
	mkdir logs        
	
#Step 4 :           

vi ~/.bash_profile          

	export GOBBLIN_JOB_CONFIG_DIR=/opt/gobblin/gobblin-dist/job_conf                   
	export GOBBLIN_WORK_DIR=/opt/gobblin/gobblin-dist/job_work                     
	export HADOOP_HOME=/opt/hadoop-2.6.0                           
	export PATH=$PATH:$HADOOP_HOME/bin                      
	export KAFKA_HOME=opt/kafka_2.10-0.8.2.0                          
	export PATH=$PATH:$KAFKA_HOME/bin                  
source ~/.bash_profile

------------------------------------------------------------------------------------------------------------------------------------     

You can reach me for any suggestions/clarifications on  : revanthkumar95@gmail.com                                              
Feel free to share any insights or constructive criticism. Cheers!!                                                           
#Happy Gobbling!!!..  
