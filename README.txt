## Installation

1) Install and configure StarCluster (e.g., `sudo apt-get install starcluster`)
2) Clone the Myria-EC2 repository (`git clone https://github.com/uwescience/myria-ec2.git`)
3) Install Myria-EC2 (`sudo python setup.py install`)
4) When prompted, enter your EC2 credentials

## Configuring Cluster Configuration

The cluster configuration file is located (by default) in `~/.starcluster/myriacluster.config`.  

The first few lines are most useful to modify the cluster configuration.  The most relevant options are:

```
[cluster myriacluster]
CLUSTER_SIZE = 2                  # How many instances to deploy?
NODE_INSTANCE_TYPE = t1.micro     # What instance type?
SPOT_BID = 0.0035                 # Change your spot instance bid prices if requesting a larger instance type!
``` 

See the commented lines in the configuration file for a complete list of options.
