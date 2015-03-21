import json
from starcluster.clustersetup import DefaultClusterSetup
from starcluster.logger import log
from myria import MyriaConnection, MyriaSchema, MyriaRelation, MyriaQuery

DEPLOYMENT_PATH = '/root/deployment.cfg.ec2'
DEFAULT_TIMEOUT = 3600


class MyriaIngest(DefaultClusterSetup):

    def __init__(self, name, schema,
                 uris, workers=None,
                 scan_type=None, scan_parameters=None,
                 insert_type=None, insert_parameters=None,
                 hostname='localhost', port=8753, ssl=False,
                 wait_for_completion=True,
                 timeout=DEFAULT_TIMEOUT):
        super(MyriaIngest, self).__init__()

        self.hostname = hostname
        self.port = port
        self.ssl = ssl

        self.name = name
        self.schema = MyriaSchema(json.loads(schema))
        self.wait_for_completion = wait_for_completion
        self.timeout = timeout

        self.scan_type = scan_type
        self.scan_parameters = scan_parameters
        self.insert_type = insert_type
        self.insert_parameters = insert_parameters

        uris = map(str.strip, uris.splitlines())
        ids = map(int, workers.re.findall(r"\d+", workers)) \
            if workers else xrange(1, len(uris)+1)
        self.work = zip(ids, uris)

    def run(self, nodes, master, user, user_shell, volumes):
        for worker, uri in self.work:
            log.info("Worker #%d ingesting %s", worker, uri)

        with master.ssh.remote_file(DEPLOYMENT_PATH, 'r') as descriptor:
            connection = MyriaConnection(deployment=descriptor, ssl=self.ssl)
            log.info("MyriaConnection URI: " + connection._url_start)
            relation = MyriaRelation(self.name,
                                     schema=self.schema,
                                     connection=connection)
            query = MyriaQuery.parallel_import(
                relation, self.work,
                scan_type=self.scan_type,
                scan_parameters=self.scan_parameters,
                insert_type=self.insert_type,
                insert_parameters=self.insert_parameters,
                timeout=self.timeout)
            log.info("Ingesting as query %d", query.query_id)

            if self.wait_for_completion:
                query.wait_for_completion()
                log.info("Ingest complete (%d, %s)",
                         query.query_id, query.status)
