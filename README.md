# Pinscher: Alarm Processing for Slow Control Systems #

**Author: J. Grigat **

Last Update: 2021-03-08
University of Freiburg

## Brief ##

Pinscher is an open source software based on Apache Storm that can be used as part of a slow control system to process incoming data and check it for different alarm types as well as to write it to permanent storage. It is meant to be used together with Doberman version >=5.0 (https://github.com/AG-Schumann/Doberman). Values are read from a queue realized with Apache Kafka. Alarms are logged to a MongoDB database. All data points are written to an InfluxDB database for permanent storage. 

 
## Installation ##

Installation guide tested for Ubuntu 18.04 LTS
0. If you plan on using Pinscher together with Doberman, follow Doberman's installation guide (https://github.com/AG-Schumann/Doberman) first. Then, you can skip the following two steps and continue with step 3.
1. Install and create a MongoDB server (These steps are for a local database, it is also possible to separate Doberman and the database. Follow   https://docs.mongodb.com/manual/tutorial/install-mongodb-on-ubuntu/).
2. Install and create a InfluxDB server (These steps are for a local database, it is also possible to separate Doberman and the database. Follow https://docs.influxdata.com/influxdb/v1.8/introduction/install/)
3. Install Apache Kafka (A useful installation guide: https://www.digitalocean.com/community/tutorials/how-to-install-apache-kafka-on-ubuntu-18-04)
4. Set up an Apache Storm 2.0.0 cluster (https://storm.apache.org/releases/2.0.0/Setting-up-a-Storm-cluster.html)
5. Setup the configuration database for the experiment (to be added)
6. Download the pinscher.jar file
7. Submit the topology for the specific experiment: 
`$STORM_PATH/bin/storm jar pinscher.jar <MONGO_URI> <EXPERIMENT_NAME>`
