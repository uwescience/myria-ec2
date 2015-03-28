import json
from starcluster.clustersetup import DefaultClusterSetup
from starcluster.logger import log
from myria import MyriaConnection, MyriaSchema, MyriaRelation, MyriaQuery
from myriaplugin import MyriaInstaller

DEPLOYMENT_PATH = '/root/deployment.cfg.ec2'
DEFAULT_TIMEOUT = 3600


class MyriaIngest(DefaultClusterSetup):

    def __init__(self, name, schema,
                 uris, workers=None,
                 scan_type=None, scan_parameters=None,
                 insert_type=None, insert_parameters=None,
                 hostname='localhost', port=8753, ssl=False,
                 overwrite_on_restart=False,
                 wait_for_completion=True,
                 timeout=DEFAULT_TIMEOUT):
        super(MyriaIngest, self).__init__()

        self.hostname = hostname
        self.port = port
        self.ssl = ssl

        self.name = name
        self.schema = MyriaSchema(json.loads(schema))
        self.wait_for_completion = wait_for_completion
        self.overwrite_on_restart = overwrite_on_restart
        self.timeout = timeout

        self.scan_type = scan_type
        self.scan_parameters = json.loads(scan_parameters) \
            if scan_parameters else None
        self.insert_type = insert_type
        self.insert_parameters = json.loads(insert_parameters) \
            if insert_parameters else None

        uris = [uri.strip() for uri in uris.splitlines()]
        ids = [int(w) for w in workers.re.findall(r"\d+", workers)] \
            if workers else xrange(1, len(uris)+1)
        self.work = zip(ids, uris)

    def run(self, nodes, master, user, user_shell, volumes):
        with master.ssh.remote_file(DEPLOYMENT_PATH, 'r') as descriptor:
            connection = MyriaConnection(hostname=master.dns_name,
                                         deployment=descriptor,
                                         ssl=self.ssl)
            relation = MyriaRelation(self.name,
                                     schema=self.schema,
                                     connection=connection)

            if not relation.is_persisted or self.overwrite_on_restart:
                for worker, uri in self.work:
                    log.info("Worker #%d ingesting %s", worker, uri)

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

                MyriaInstaller.web_restart(master)

    def on_restart(self, nodes, master, user, user_shell, volumes):
        pass

    def on_shutdown(self, nodes, master, user, user_shell, volumes):
        pass
