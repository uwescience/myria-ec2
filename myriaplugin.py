import os
import time
import random
import string
from postgresplugin import PostgresInstaller, DEFAULT_PATH_FORMAT
from starcluster.clustersetup import DefaultClusterSetup
from starcluster.logger import log

DEFAULT_MYRIA_POSTGRES_PORT = 5401
DEFAULT_MYRIA_CONSTANTS_PATH = 'src/edu/washington/escience/myria/MyriaConstants.java'

# myria deployment configuration template
myria_config = """
# Deployment configuration
[deployment]
name = {name}
path = {path}
rest_port = {port}
dbms = {dbms}
database_password = {database_password}
{heap}

# Compute nodes configuration
[master]
0 = {master_alias}:{master_port}

[workers]
{workers}
"""

class MyriaInstaller(DefaultClusterSetup):

    def __init__(self, 
                 name='MyriaEC2',
                 path='/mnt/myria_ec2_deployment',
                 dbms='postgresql',
                 heap=None,
                 rest_port=8753,
                 master_port=8001,
                 worker_port=9001,
                 required_packages=['git', 'openjdk-7-jre', 'openjdk-7-jdk', 
                                    'libxml2-dev', 'libxslt1-dev', 'python-dev'],
                 additional_packages=[],
                 repository='https://github.com/uwescience/myria.git',
                 install_directory='~/myria',
                 myria_commit=None,
                 jvm_version="java-1.7.0-openjdk",
                 database_name='myria',

                 postgres_port=5401,
                 postgres_version="9.1",
                 postgres_path="/mnt/postgresdata",
                 postgres_name=None,
                 postgres_username="uwdb",
                 postgres_password="".join(random.sample(string.lowercase+string.digits, 10))):
        super(MyriaInstaller, self).__init__()

        self.packages = required_packages + additional_packages
        self.repository = repository
        self.directory = install_directory
        self.name = name
        self.path = path
        self.dbms = dbms
        self.heap = heap
        self.rest_port = rest_port
        self.master_port = master_port
        self.worker_port = worker_port
        self.database_name = database_name
        self.myria_commit = myria_commit
        self.jvm_version = jvm_version

        self.deploy_dir = "{}/myriadeploy".format(install_directory)
        self.postgres = {'port': postgres_port, 'version': postgres_version, 
                         'path': postgres_path, 'name': postgres_name, 
                         'username': postgres_username, 'password': postgres_password}

    def _set_up_node(self, node):
        log.info("Begin configuring {}".format(node.alias))

        log.info('*   Removing source deb http://www.cs.wisc.edu/condor/debian/development lenny contrib')
        node.ssh.execute('sed -i "s/deb http:\/\/www.cs.wisc.edu\/condor\/debian\/development lenny contrib/#deb http:\/\/www.cs.wisc.edu\/condor\/debian\/development lenny contrib/g" /etc/apt/sources.list')

        node.apt_command('update')
        node.package_install(' '.join(self.packages))
        node.ssh.execute(
            'sudo update-java-alternatives -s {}'.format(self.jvm_version))

        if self.dbms == "postgresql":
            self.configure_postgres(node)

    def run(self, nodes, master, user, user_shell, volumes):
        log.info('Beginning Myria configuration')

        # init nodes in parallel
        for node in nodes:
            self.pool.simple_job(
                self._set_up_node, (node), jobid=node.alias)
        self.pool.wait(len(nodes))

        # get, compile and deploy myria from master
        log.info('Begin repository clone on {}'.format(master.alias))
        master.ssh.execute(
            'rm -rf {dir} ; git clone {} {dir}'.format(
                self.repository, dir=self.directory))
        log.info("commit version: {}".format(self.myria_commit))
        if self.myria_commit:
            master.ssh.execute(
                'cd {dir} && git checkout {commit}'.format(
                    dir=self.directory, commit=self.myria_commit))

        if self.dbms == "postgresql":
          self.configure_myria_postgres_port(master, self.postgres['port'])

        log.info('Begin build on {}'.format(master.alias))
        master.ssh.execute('cd {} && ./gradlew clean'.format(self.directory))
        master.ssh.execute('cd {} && ./gradlew eclipseClasspath'.format(
            self.directory))
        master.ssh.execute('cd {} && ./gradlew jar'.format(self.directory))

        log.info(
            'Begin write deployment configuration on {}'.format(master.alias))

        slave_nodes = filter(lambda node: not node.is_master(), nodes)
        with master.ssh.remote_file('deployment.cfg.ec2', 'w') as descriptor:
            descriptor.write(
                self.get_configuration(master, slave_nodes))

        enter_deploy = "cd {}".format(self.deploy_dir)
        log.info('Begin Myria cluster setup on {}'.format(master.alias))
        master.ssh.execute(
            '{} && sudo ./setup_cluster.py ~/deployment.cfg.ec2'.format(
                enter_deploy))

        time.sleep(20)

        log.info('Begin Myria cluster launch on {}'.format(master.alias))
        master.ssh.execute(
            '{} && sudo ./launch_cluster.sh ~/deployment.cfg.ec2'.format(
                enter_deploy))

        log.info('Installing Myria-Python on {}'.format(master.alias))
        master.ssh.execute(
            'git clone https://github.com/uwescience/myria-python.git ~/myria-python &&'
            'cd ~/myria-python &&'
            'python setup.py install')

        log.info('End Myria configuration')

    def get_configuration(self, master, nodes):
        return myria_config.format(
            path=self.path, name=self.name,
            dbms=self.dbms, database_password=self.postgres['password'],
            port=self.rest_port,
            heap='heap = ' + self.heap if self.heap else '',
            master_alias=master.dns_name,
            master_port=self.master_port,
            workers='\n'.join('{} = {}:{}::{}'.format(
                id + 1, node.dns_name, self.worker_port, self.database_name)
                for id, node in enumerate(nodes)))

    def configure_postgres(self, node):
        database = self.database_name
        username = self.postgres['username']
        password = self.postgres['password']
        version = self.postgres['version']
        path = DEFAULT_PATH_FORMAT.format(version=version)
        port = self.postgres['port']

        if username != 'uwdb':
          log.info("WARNING: Myria requires a postgres user named 'uwdb'")

        log.info('Begin Postgres configuration on {}'.format(node.alias))
        PostgresInstaller.create_user(node, username, password, path, port)
        PostgresInstaller.create_database(node, database, path, port)
        PostgresInstaller.grant_all(node, database, username, path, port)
        PostgresInstaller.set_listeners(node, '*', version=version)
        PostgresInstaller.add_host_authentication(node, 
                                                  'host all all 0.0.0.0/0 md5', 
                                                  version=version)
        PostgresInstaller.restart(node)

    def configure_myria_postgres_port(self, node, port):
        if port != DEFAULT_MYRIA_POSTGRES_PORT:
          log.info("HACK: Updating Myria's postgres port to {}".format(port))
          node.ssh.execute(r'sed -i "s/STORAGE_POSTGRESQL_PORT\s*=\s*[0-9]\+/STORAGE_POSTGRESQL_PORT = {port}/ig" {path}'.format(
              port=port,
              path=os.path.join(self.directory, DEFAULT_MYRIA_CONSTANTS_PATH)))