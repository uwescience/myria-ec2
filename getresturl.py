#!/usr/bin/python

import starcluster.config as config


def get_instance_by_tag(ec2, tag, key='Name'):
    return next(
        (instance for instance in ec2.get_all_instances()
            if instance.tags[key] == tag))

if __name__ == '__main__':
    configuration_file = None
    configuration = config.get_config(configuration_file)

    plugin = configuration.plugins['myriaplugin']
    port = plugin.get('REST_PORT', 8753)

    instance = get_instance_by_tag(
        config.get_easy_ec2(configuration_file), 'myriacluster-master')

    print 'http://{}:{}'.format(instance.dns_name, port)
