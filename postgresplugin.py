import time
from starcluster.clustersetup import DefaultClusterSetup
from starcluster.logger import log

DEFAULT_VERSION = 9.1
DEFAULT_PORT = 5401
DEFAULT_DATA_PATH = '/var/postgresdata'
DEFAULT_PATH_FORMAT = '/usr/lib/postgresql/{version}/bin'
DEFAULT_PATH = DEFAULT_PATH_FORMAT.format(version=DEFAULT_VERSION)

class PostgresInstaller(DefaultClusterSetup):
    def __init__(self, 
                 port=DEFAULT_PORT,
                 version=DEFAULT_VERSION,
                 database_path=DEFAULT_DATA_PATH,
                 install_on_master=True):
        super(PostgresInstaller, self).__init__()

        self.port = port
        self.version = version
        self.database_path = database_path
        self.install_on_master = install_on_master

        #generated properties
        self.log = "{}/server.log".format(database_path)
        self.path = DEFAULT_PATH_FORMAT.format(version=version)
        self.conf = '/etc/postgresql/{}/main/postgresql.conf'.format(version)
        self.options = "-c config_file={} -p {}".format(self.conf, port)

    def _set_up_node(self, node):
        log.info("Begin configuration {}".format(node.alias))

        if not node.is_master() or self.install_on_master:
            log.info("Setting up postgres on {}".format(node.alias))

            start_pg = """
            sudo -u postgres {pg_path}/pg_ctl -D {data} -o "{opt}" -l {log} start;
            """.format(
                pg_path=self.path, data=self.database_path,
                opt=self.options, log=self.log)

            node.ssh.execute('sudo add-apt-repository -r "deb http://www.cs.wisc.edu/condor/debian/development lenny contrib"')
            node.apt_command('update')
            node.apt_install("postgresql-{}".format(self.version))
            node.ssh.execute("sudo service postgresql stop")
            node.ssh.execute("sudo mkdir -p {}".format(self.database_path))
            node.ssh.execute("sudo chown postgres {}".format(self.database_path))
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
        sql = "CREATE USER {user} WITH PASSWORD \'{password}\'".format(user=user, password=password)
        command = """sudo -u postgres {path}/psql -p {port} -c "{sql}"
               """.format(path=path, port=port, sql=sql)
        return PostgresInstaller._execute(node, command, path)

    @staticmethod
    def grant_all(node, name, user, path=DEFAULT_PATH, port=DEFAULT_PORT):
        sql = "GRANT ALL PRIVILEGES ON DATABASE {database} TO {user}".format(database=name, user=user)
        command = """sudo -u postgres {pg_path}/psql -p {port} -c "{sql}"
                  """.format(pg_path=path, port=port, sql=sql)
        return PostgresInstaller._execute(node, command, path)

    @staticmethod
    def create_database(node, name, path=DEFAULT_PATH, port=DEFAULT_PORT):
        command = """sudo -u postgres {pg_path}/psql -p {port} -c "CREATE DATABASE {db}"
                    """.format(pg_path=path, port=port, db=name)
        return PostgresInstaller._execute(node, command, path)

    @staticmethod
    def _execute(node, command, path=DEFAULT_PATH):
        cd = "cd {}".format(path)
        return node.ssh.execute(';'.join([cd, command]))