import time
import os
from starcluster.clustersetup import DefaultClusterSetup
from starcluster.logger import log

DEFAULT_VERSION = 9.1
DEFAULT_PORT = 5432
DEFAULT_DATA_PATH = '/mnt/postgresdata'
DEFAULT_PATH_FORMAT = '/usr/lib/postgresql/{version}/bin'
DEFAULT_PATH = DEFAULT_PATH_FORMAT.format(version=DEFAULT_VERSION)

MOUNT_ENABLED = False

class PostgresInstaller(DefaultClusterSetup):
    def __init__(self,
                 port=DEFAULT_PORT,
                 version=DEFAULT_VERSION,
                 database_path=DEFAULT_DATA_PATH,
                 install_on_master=True,
                 new_ebs_enable=False,
                 new_ebs_type=None,
                 new_ebs_region=None,
                 new_ebs_availability_zone=None,
                 new_ebs_size=None,
                 mount_existing_ebs_enable=False,
                 mount_existing_ebs_list=None,
                 mount_existing_ebs_region=None):
        super(PostgresInstaller, self).__init__()

        self.port = port
        self.version = version
        self.install_on_master = install_on_master

        #ebs storage options
        self.new_ebs = {'enabled': True if new_ebs_enable=="TRUE" else False,
                        'size': new_ebs_size, 'type': new_ebs_type,
                        'region':new_ebs_region, 'availability_zone': new_ebs_availability_zone}

        global MOUNT_ENABLED
        MOUNT_ENABLED = True if mount_existing_ebs_enable=="TRUE" else False
        self.mount_existing_ebs = {'enabled': MOUNT_ENABLED,
                                    'list_volumes': mount_existing_ebs_list,
                                    'region': mount_existing_ebs_region}

        self.database_path = database_path if not (self.mount_existing_ebs['enabled'] or self.new_ebs['enabled']) else '/data_mount'

        # Generated properties
        self.log = "{}/server.log".format(database_path)
        self.path = DEFAULT_PATH_FORMAT.format(version=version)
        self.conf = '/etc/postgresql/{}/main/postgresql.conf'.format(version)
        self.options = "-c config_file={} -p {}".format(self.conf, port)

    def _set_up_node(self, node):
        log.info("Begin configuration {}".format(node.alias))

        if not node.is_master() or self.install_on_master:
            log.info("Setting up postgres on {}".format(node.alias))

            if self.new_ebs['enabled'] and not node.is_master():
                log.info("Enabling new EBS")
                node.ssh.execute('mkdir /data_mount')

                log.info('create new EBS volume for ' + str(node.alias))
                
                create_drive_command = "aws ec2 create-volume --size {} --region {} --availability-zone {} --volume-type {}".format(self.new_ebs['size'], self.new_ebs['region'], self.new_ebs['availability_zone'], self.new_ebs['type'])

                output_create_drive = os.popen(create_drive_command).read()
                volume_name = output_create_drive.split('\t')[7]

                time.sleep(10)

                log.info('attaching an EBS volume to ' + str(node.alias))

                attach_volume_command = "aws ec2 attach-volume --region {} --volume-id {} --instance-id {} --device /dev/sdc".format(self.new_ebs['region'], volume_name, node.id)
                os.system(attach_volume_command)

                time.sleep(20)

                #mounting new drive
                node.ssh.execute('sudo mkfs -t ext4 /dev/xvdc;')
                node.ssh.execute('sudo mount /dev/xvdc /data_mount;')

            if self.mount_existing_ebs['enabled'] and not node.is_master():
                log.info("Mounting existing EBS drives")
                node.ssh.execute('mkdir /data_mount')

                listOfVolumes = self.mount_existing_ebs['list_volumes'].split(',')
                log.info('attaching an EBS volume to ' + str(node.alias))

                workerNum = int(node.alias[-2:])

                attach_volume_command = "aws ec2 attach-volume --region {} --volume-id {} --instance-id {} --device /dev/sdc".format(self.mount_existing_ebs['region'],listOfVolumes[workerNum-1], node.id)
                log.info(attach_volume_command)

                os.system(attach_volume_command)

                time.sleep(20)

                node.ssh.execute('sudo mount /dev/xvdc /data_mount;')

            node.ssh.execute('sudo add-apt-repository -r "deb http://www.cs.wisc.edu/condor/debian/development lenny contrib"')
            node.apt_command('update')
            node.apt_install("postgresql-{}".format(self.version))

            self.set_port(node, self.port, version=self.version)
            self.set_data_path(node, data_path=self.database_path, version=self.version, restart=False)

            start_pg = """
                sudo -u postgres {pg_path}/pg_ctl -D {data} -o "{opt}" -l {log} start;
                """.format(
                           pg_path=self.path, data=self.database_path,
                           opt=self.options, log=self.log)
            #extra
            log.info(start_pg)
            node.ssh.execute(start_pg)

            # sleep 5 seconds wait for postgres start
            time.sleep(5)

        log.info("End configuration {}".format(node.alias))

    def run(self, nodes, master, user, user_shell, volumes):
        log.info('Beginning Postgres configuration')

        # init java and postgres in parallel
        for node in nodes:
            self.pool.simple_job(
                self._set_up_node, node, jobid=node.alias)
        self.pool.wait(len(nodes))

        log.info('End Postgres configuration')

    @staticmethod
    def create_user(node, user, password, path=DEFAULT_PATH, port=DEFAULT_PORT):
        if MOUNT_ENABLED and not node.is_master() :
            sql = "ALTER USER {user} WITH PASSWORD \'{password}\'".format(user=user, password=password)
            create_command = """sudo -u postgres {path}/psql -p {port} -c "{sql}" """.format(path=path, port=port, sql=sql)
            return PostgresInstaller._execute(node, create_command, path)
        else:
            sql = "CREATE USER {user} WITH PASSWORD \'{password}\'".format(user=user, password=password)
            conditional_command = """sudo -u postgres psql -tAc "SELECT 1 FROM pg_roles WHERE rolname='{user}'" | grep -q 1""".format(user=user)
            create_command = """sudo -u postgres {path}/psql -p {port} -c "{sql}" """.format(path=path, port=port, sql=sql)
            command = conditional_command + '||' + create_command
            return PostgresInstaller._execute(node, command, path)

    @staticmethod
    def grant_all(node, name, user, path=DEFAULT_PATH, port=DEFAULT_PORT):
        sql = "GRANT ALL PRIVILEGES ON DATABASE {database} TO {user}".format(database=name, user=user)
        command = """sudo -u postgres {pg_path}/psql -p {port} -c "{sql}"
                  """.format(pg_path=path, port=port, sql=sql)
        return PostgresInstaller._execute(node, command, path)

    @staticmethod
    def create_database(node, name, path=DEFAULT_PATH, port=DEFAULT_PORT):
        conditional_command = """sudo -u postgres psql -tAc "SELECT 1 FROM pg_database WHERE datname = '{name}'" | grep -q 1""".format(name=name)
        create_command = """sudo -u postgres {pg_path}/psql -p {port} -c "CREATE DATABASE {db}" """.format(pg_path=path, port=port, db=name)
        command = conditional_command + '||' + create_command
        if not MOUNT_ENABLED or node.is_master():
            PostgresInstaller._execute(node, command, path)

    @staticmethod
    def set_listeners(node, listeners, path='/etc/postgresql/{version}/main/postgresql.conf', version=DEFAULT_VERSION):
        node.ssh.execute(r'sed -i "s/^\s*\#\?\s*listen_addresses\s*=\s*''.*\?''/listen_addresses = \'{listeners}\'/ig" {path}'.format(
            listeners=listeners,
            path=path.format(version=version)))

    @staticmethod
    def set_port(node, port, path='/etc/postgresql/{version}/main/postgresql.conf', version=DEFAULT_VERSION):
        node.ssh.execute(r'sed -i "s/^\s*port\s*=\s*[0-9]\+/port = {port}/ig" {path}'.format(
            port=port,
            path=path.format(version=version)))

    @staticmethod
    def add_host_authentication(node, authentication, path='/etc/postgresql/{version}/main/pg_hba.conf', version=DEFAULT_VERSION):
        with node.ssh.remote_file(path.format(version=version), 'a') as descriptor:
            descriptor.write(authentication + '\n')

    @staticmethod
    def set_data_path(node, config_path='/etc/postgresql/{version}/main/postgresql.conf',
                            data_path=DEFAULT_DATA_PATH,
                            version=DEFAULT_VERSION,
                            restart=True):
        PostgresInstaller.stop(node)

        if not MOUNT_ENABLED or node.is_master():
            node.ssh.execute("sudo mkdir -m 700 -p {}".format(data_path))
            node.ssh.execute(r"""sudo cp -rp `grep -Po "data_directory\\s*=\\s*'\K[^']*(?=')" {config_path}`/* {data_path}""".format(
                config_path=config_path.format(version=version),
                data_path=data_path))

        # Just in case directory already existed
        node.ssh.execute("sudo chmod 700 {}".format(data_path))
        node.ssh.execute("sudo chown -R postgres {}".format(data_path))
        node.ssh.execute("sudo chgrp -R postgres {}".format(data_path))

        # Change data directory in .config
        node.ssh.execute(r"""sed -i "s+^\\s*data_directory\\s*=\\s*'[^']*'+data_directory = '{data_path}'+g" {config_path}""".format(
            data_path=data_path,
            config_path=config_path.format(version=version)))

        PostgresInstaller.start(node)

    @staticmethod
    def restart(node):
        node.ssh.execute('sudo service postgresql restart')

    @staticmethod
    def start(node):
        node.ssh.execute('sudo service postgresql start')

    @staticmethod
    def stop(node):
        node.ssh.execute('sudo service postgresql stop')

    @staticmethod
    def restart(node):
        node.ssh.execute('sudo service postgresql restart')

    @staticmethod
    def initialize_database(node, database_path, path=DEFAULT_PATH):
        command = "sudo -u postgres {pg_path}/initdb -D {data_path}".format(pg_path=path, data_path=database_path)
        if not MOUNT_ENABLED or node.is_master():
            node.ssh.execute(command)

    @staticmethod
    def _execute(node, command, path=DEFAULT_PATH):
        cd = "cd {}".format(path)
        log.info(';'.join([cd, command]))
        return node.ssh.execute(';'.join([cd, command]))