# Pinscher: Alarm Processing for Slow Control Systems #

**Author: J. Grigat **

Last Update: 2020-02-04
University of Freiburg

## Brief ##

Pinscher is an open source software based on Apache Storm that can be used as part of a slow control system to process incoming data and check it for different alarm types as well as to write it to permanent storage. It is meant to be used together with Doberman version >=5.0 (https://github.com/AG-Schumann/Doberman).
## Prerequisities ##
 MongoDB
 InfluxDB
 Kafka
## Installation ##

Installation guide tested for Ubuntu 18.04 LTS

1. Set up an Apache Storm 2.0.0 cluster (https://storm.apache.org/releases/2.0.0/Setting-up-a-Storm-cluster.html)
2. Setup the configuration database for the experiment (to be added)
3. Download the pinscher.jar file
4. Submit the topology for the specific experiment: 
`$STORM_PATH/bin/storm jar pinscher.jar <MONGO_URI> <EXPERIMENT_NAME>`
