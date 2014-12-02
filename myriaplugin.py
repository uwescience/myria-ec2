import time
import random
import string
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

# command templates
sql_create_user = "CREATE USER uwdb WITH PASSWORD"
sql_grant_right = "GRANT ALL PRIVILEGES ON DATABASE"
pwd_strbase = string.lowercase+string.digits


class MyriaInstaller(DefaultClusterSetup):

    def __init__(self, name='MyriaEC2',
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
                 postgres_port="5401",
                 postgres_version="9.1",
                 postgres_user="uwdb",
                 postgres_data="/var/postgresdata",
                 database_name="myriadb",
                 database_password="".join(random.sample(pwd_strbase, 10)),
                 myria_commit=None,
                 jvm_version="java-1.7.0-openjdk-i386"):
        if not database_password:
            log.error("Database password must be provided in config file.")
            raise ValueError("No database password in configuration file")
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
        self.postgres_port = postgres_port
        self.postgres_version = postgres_version
        self.postgres_user = postgres_user
        self.postgres_data = postgres_data
        self.database_name = database_name
        self.database_password = database_password
        self.myria_commit = myria_commit
        self.jvm_version = jvm_version

        #generated properties
        self.postgres_log = "{}/server.log".format(postgres_data)
        self.postgres_path = "/usr/lib/postgresql/{version}/bin".format(
            version=postgres_version)
        self.postgres_conf = '/etc/postgresql/{}/main/postgresql.conf'.format(
            postgres_version)
        self.postgres_option = "-c config_file={conf} -p {port}".format(
            conf=self.postgres_conf, port=postgres_port)
        self.deploy_dir = "{}/myriadeploy".format(install_directory)

    def _set_up_node(self, node):
        """
        Set up postgres in each slave node.
        """

        cd = "cd {}".format(self.postgres_data)

        start_pg = """
        sudo -u postgres {pg_path}/pg_ctl -D {data} -o "{opt}" -l {log} start;
        """.format(
            pg_path=self.postgres_path, data=self.postgres_data,
            opt=self.postgres_option, log=self.postgres_log)

        create_user = """
        sudo -u postgres {pg_path}/psql -p {port} -c "{create_user} \'{pwd}\'"
        """.format(
            pg_path=self.postgres_path, port=self.postgres_port,
            create_user=sql_create_user, pwd=self.database_password)

        create_db = """
        sudo -u postgres {pg_path}/psql -p {port} -c "CREATE DATABASE {db}"
        """.format(pg_path=self.postgres_path, port=self.postgres_port,
                   db=self.database_name)

        grant_right = """
        sudo -u postgres {pg_path}/psql -p {port} -c "{grant} {db} TO {user}"
        """.format(
            pg_path=self.postgres_path, port=self.postgres_port,
            grant=sql_grant_right, db=self.database_name,
            user=self.postgres_user)

        log.info("Begin configuring {}".format(node.alias))
        node.apt_command('update')
        node.package_install(' '.join(self.packages))
        node.ssh.execute(
            'sudo update-java-alternatives -s {}'.format(self.jvm_version))

        if not node.is_master():
            log.info("Setting postgres on {}".format(node.alias))
            node.apt_install("postgresql-{}".format(self.postgres_version))
            node.ssh.execute("sudo service postgresql stop")
            node.ssh.execute("sudo mkdir -p {}".format(self.postgres_data))
            node.ssh.execute("sudo chown postgres {}".format(
                self.postgres_data))
            node.ssh.execute(start_pg)
            # sleep 5 seconds wait for postgres start
            time.sleep(5)

            log.info("Creating user and database {db} on {node}".format(
                db=self.database_name, node=node.alias))
            node.ssh.execute(';'.join([cd, create_user]))
            node.ssh.execute(';'.join([cd, create_db]))

            log.info("Granting all right to {user} on {node}".format(
                user=self.postgres_user, node=node.alias))
            node.ssh.execute(';'.join([cd, grant_right]))

    def run(self, nodes, master, user, user_shell, volumes):
        # set slave nodes
        self.slave_nodes = filter(lambda x: not x.is_master(), nodes)

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
        slave_nodes = filter(lambda x: not x.is_master(), nodes)
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

        log.info('End Myria configuration')

    def get_configuration(self, master, nodes):
        return myria_config.format(
            path=self.path, name=self.name,
            dbms=self.dbms, database_password=self.database_password,
            port=self.rest_port,
            heap='heap = ' + self.heap if self.heap else '',
            master_alias=master.dns_name,
            master_port=self.master_port,
            workers='\n'.join('{} = {}:{}::{}'.format(
                id + 1, node.dns_name, self.worker_port, self.database_name)
                for id, node in enumerate(nodes)))
