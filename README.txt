1) Install and configure StarCluster (e.g., sudo apt-get install starcluster)

  a) Create a default config by executing "starcluster help" and selecting option #2

  b) Add your AWS credentials to the ~/.starcluster/config file

		(i) Add Access Key and Secret Access key from AWS credentials file

		(ii) User ID is the 12 digit ID found in the IAM Management Console

  c) Create a UWDB/EC2 key by executing "starcluster createkey UNIQUE_KEYNAME -o ~/.ssh/uwdb.rsa" (if you already have a EC2 keypair, you can select it by modifying ~/.starcluster/myriacluster.config).  Replace UNIQUE_KEYNAME with any keyname that is unique to you.  If you use the same key name as someone else, you will overwrite their key, because we are all using the same AWS account.

  d) In ~/.starcluster/config, change the lines:
		[key mykey]
		KEY_LOCATION=~/.ssh/mykey.rsa
	
  	 to:
		[key UNIQUE_KEYNAME]
		KEY_LOCATION=~/.ssh/uwdb.rsa

  e) Also in ~/.starcluster/config, change:
		KEYNAME = mykey
	
	to:
		KEYNAME = UNIQUE_KEYNAME

2) Edit cluster settings: 
  a) Add the following line to the [global] section of ~/.starcluster/config:
	INCLUDE=~/.starcluster/myriacluster.config

  b) (optional) add the commit id of myria to the [plugin myriaplugin] section
  of myriacluster.config, eg:
  MYRIA_COMMIT = <the commit hash code you want>


3) Copy myriacluster.config to ~/.starcluster/

4) Copy myriaplugin.py to ~/.starcluster/plugins/

5) Run starcluster:

  a) Start a cluster:
       starcluster start -c myriacluster myriacluster

  b) Alternatively, specify a cluster of size n instances:
       starcluster start -c myriacluster --cluster-size n myriacluster

  c) Terminate a cluster:
       starcluster terminate myriacluster

  d) StarCluster supports restarting and stopping clusters as well ("starcluster restart myriacluster" and "starcluster stop myriacluster" respectively)

  c) Modify cluster settings in ~/.starcluster/myriaconfig (e.g., change instance sizes, add spot instances).  Or, use the command line (see http://star.mit.edu/cluster/docs/latest/manual/shell_completion.html)
