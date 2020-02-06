# Greyhound: Alarm Processing for Slow Control Systems #

**Author: J. Grigat **

Last Update: 2020-02-04
University of Freiburg

## Brief ##

Greyhound is an open source software based on Apache Storm that can be used as part of a slow control system to process incoming data and check it for different alarm types as well as to write it to permanent storage. It is meant to be used together with Doberman 5.0 (https://github.com/AG-Schumann/Doberman/tree/overhaul_mid), but it will work with any program that writes data to Apache Kafka in the form `[<host_name>,]<reading_name>,<value>` where `reading_name` is a unique description of the measurement. `host_name` is an optional parameter to designate the machine that sent the data. If a host name is given, reading names may be used multiple times. `value` corresponds to the numerical value of the measurement in the format of a floating point number. 

## Prerequisities ##
 MongoDB
 InfluxDB
 Kafka
## Installation ##

Installation guide tested for Ubuntu 18.04 LTS

* Install git if not pre-installed already, e.g.`sudo apt-get install git` (apt-get needs to be up to date: `sudo apt-get update`)

1. Set up an Apache Storm 2.0.0 cluster (https://storm.apache.org/releases/2.0.0/Setting-up-a-Storm-cluster.html)
2. Install Apache Maven: `sudo apt install maven'
3. Download this repository to a directory (e.g. `git clone https://github.com/AG-Schumann/Greyhound.git`).
4. In the directory run `mvn assembly:assembly` to create a the Greyhound jar-file.
5. Make sure your Config Database contains the connection information for Kafka and Influxdb.
    *  Connect to your MongoDB instance
    *  `use <experiment_name>_settings`
    *  `db.experiment_config.insert({name: "kafka", "bootstrap_servers" : "<BOOTSTRAP IP>:<BOOTSTRAP PORT>"}`
    *  `db.experiment_config.insert({name: "influx", "server" : "<INFLUX_IP>:<INFLUX PORT>"})`
5. Submit the topology for a specific experiment: 
`$STORM_PATH/bin/storm jar ./target/Greyhound-<VERSION>-jar-with-dependencies.jar <MONGO_URI> <EXPERIMENT_NAME>`
