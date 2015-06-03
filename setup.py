import os
import pwd
import getpass
import re
from setuptools import setup
from setuptools.command.install import install

plugin_names = ['myriaplugin.py', 'postgresplugin.py']
config_name = 'myriacluster.config'
config_path = '~/.starcluster'

user_token = '#your userid'
key_token = '#your_aws_access_key_id'
secret_token = '#your_secret_access_key'

class RegisterPluginDecorator(install):
    def run(self):
        install.do_egg_install(self)

        root_configuration = self.ensure_configuration()
        self.ensure_credentials(root_configuration)
        self.ensure_include(config_name, root_configuration)
        self.ensure_key(config_name, root_configuration)

    def ensure_configuration(self):
        try:
            from starcluster import config, exception
            configuration = config.StarClusterConfig()
            configuration.config # Ensure config exists

            return configuration.cfg_file
        except exception.ConfigNotFound as e:
            e.create_config()
            self.update_ownership(e.cfg, self.get_username())
            return e.cfg
        except ImportError:
            print 'Unable to import starcluster; cannot verify configuration file exists'

    def ensure_credentials(self, filename):
        config = self.get_config(filename)

        if user_token in config:
          user = raw_input('AWS User ID (from the IAM Management Console): ')
          if user: self.set_property(filename, user_token, user)

        if key_token in config:
          key = raw_input('AWS API Access Key ID: ')
          if key: self.set_property(filename, key_token, key)

        if secret_token in config:
          secret = raw_input('AWS API Secret Key: ')
          if secret: self.set_property(filename, secret_token, secret)


    def ensure_key(self, config_name, root_configuration):
        keyname = self.get_username() + 'Key'
        keyfile = '~/.ssh/{}.rsa'.format(keyname)

        self.set_keyname(os.path.join(os.path.dirname(root_configuration), config_name), keyname)

        if True or self.has_property(root_configuration, '[key mykey]'):
            try:
                from starcluster import config, exception
                from boto.exception import EC2ResponseError

                print 'Creating EC2 key ' + keyfile

                self.set_property(root_configuration, 'KEY_LOCATION=~/.ssh/mykey.rsa', 
                                                      'KEY_LOCATION={}'.format(keyfile))
                self.set_property(root_configuration, '[key mykey]', 
                                                      '[key {}]'.format(keyname))
                self.set_property(root_configuration, 'KEYNAME = mykey', 
                                                      'KEYNAME = {}'.format(keyname))
                config = config.StarClusterConfig()
                config.load()
                config.get_easy_ec2().create_keypair(keyname, output_file=os.path.expanduser(keyfile))

                self.update_ownership(os.path.expanduser(keyfile), self.get_username())               

            except exception.KeyPairAlreadyExists:
                print 'Key pair "{}" already exists on EC2 -- name must be unique'.format(keyname)
                print '*** Must manually create using "starcluster createkey {} -o {}"'.format(
                          keyname, keyfile)
            except EC2ResponseError:
                print 'Unable to create key pair -- incorrect API credentials?'
                print '*** Must manually create using "starcluster createkey {} -o {}"'.format(
                          keyname, keyfile)

            except ImportError:
                print 'Unable to import starcluster; skipping key generation'



    def ensure_include(self, name, filename):
        config = self.get_config(filename)

        if not self.already_included(name, config):
            print 'Adding include to ' + filename
            self.add_include(config, filename)
        else:
            print 'Root configuration already appears to include ' + config_name

    @staticmethod
    def get_config(path):
        with open(os.path.expanduser(path), 'r') as file:
            return file.read()

    @staticmethod
    def already_included(config_name, config):
        return re.search(r'^INCLUDE\w*=\w*.*' + re.escape(os.path.join(config_path, config_name)),
                         config, 
                         flags=re.IGNORECASE | re.MULTILINE)

    @staticmethod
    def add_include(config, path):
        insertion = 'INCLUDE={}'.format(os.path.join(config_path, config_name))

        # No global section
        if not '[global]' in config:
            config = '[global]\n{}\n{}'.format(insertion, config)
        # Global section with existing INCLUDE pair
        elif re.search(r'^INCLUDE\w*=\w*', config, flags=re.IGNORECASE | re.MULTILINE):
            config = re.sub(r'^INCLUDE\w*=\w*', insertion + ',', config, flags=re.IGNORECASE | re.MULTILINE)
        # Global section, no INCLUDE pair
        else:
            config = config.replace('[global]', '[global]\n' + insertion)

        with open(os.path.expanduser(path), 'w') as file:
            file.write(config)

    @staticmethod
    def set_keyname(path, name):
        RegisterPluginDecorator.set_property(path, 'KEYNAME = AWSKey', 
                                                   'KEYNAME = {}'.format(name))

    @staticmethod
    def update_ownership(filename, username):
        entry = pwd.getpwnam(username)
        os.chown(filename, entry.pw_uid, entry.pw_gid)

    @staticmethod
    def set_property(path, expression, value):
        config = RegisterPluginDecorator.get_config(path)
        config = config.replace(expression, value)
        with open(os.path.expanduser(path), 'w') as file:
            file.write(config)

    @staticmethod
    def has_property(path, value):
        config = RegisterPluginDecorator.get_config(path)
        return value in config

    @staticmethod
    def get_username():
        return os.getenv("SUDO_USER") or getpass.getuser()

setup(
    name='Myria-EC2',
    version=1.0,
    url='https://github.com/uwescience/myria-ec2',
    author='Brandon Haynes',
    author_email='bhaynes@cs.washington.edu',
    description=('A Myria source installation plugin for Starcluster on Amazon EC2.'),
    license='BSD',
    include_package_data=True,
    packages=[],
    scripts=[],
    data_files=[(os.path.expanduser('~/.starcluster/plugins'), plugin_names),
                (os.path.expanduser('~/.starcluster'), [config_name])],
    install_requires=['StarCluster >= 0.95.6'],
    cmdclass={'install': RegisterPluginDecorator},
)
