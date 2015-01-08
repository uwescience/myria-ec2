import time
import random
import string
from postgresplugin import PostgresInstaller, DEFAULT_PATH_FORMAT
from starcluster.clustersetup import DefaultClusterSetup
from starcluster.logger import log

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
                 path='/var/myria_ec2_deployment',
                 dbms='postgresql',
                 heap=None,
                 rest_port=8753,
                 master_port=8001,
                 worker_port=9001,
                 required_packages=['git', 'openjdk-7-jre', 'openjdk-7-jdk'],
                 additional_packages=[],
                 repository='https://github.com/uwescience/myria.git',
                 install_directory='~/myria',
                 myria_commit=None,
                 jvm_version="java-1.7.0-openjdk",
                 database_name='myria',

                 postgres_port=5401,
                 postgres_version="9.1",
                 postgres_path="/var/postgresdata",
                 postgres_name=None,
                 postgres_username="postgresadmin",
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
        node.apt_command('update')
        node.package_install(' '.join(self.packages))
        node.ssh.execute(
            'sudo update-java-alternatives -s {}'.format(self.jvm_version))

    def run(self, nodes, master, user, user_shell, volumes):
        log.info('Beginning Myria configuration')

        # init java and postgres in parallel
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

        if self.dbms == "postgresql":
            self.configure_postgres(node)

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

        PostgresInstaller.create_user(node, username, password, path, port)
        PostgresInstaller.create_database(node, database, path, port)
        PostgresInstaller.grant_all(node, database, username, path, port)