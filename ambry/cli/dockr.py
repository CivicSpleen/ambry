from __future__ import absolute_import
from six import iterkeys, iteritems

from ..cli import prt, fatal, warn, err


def docker_parser(cmd):
    config_p = cmd.add_parser('docker', help='Install and manipulate docker containers')
    config_p.set_defaults(command='docker')

    asp = config_p.add_subparsers(title='Docker commands', help='Docker commands')

    sp = asp.add_parser('init', help="Initialilze a new data volume and database")
    sp.set_defaults(subcommand='init')
    sp.add_argument('-n', '--new', default=False, action='store_true',
                    help="Initialize a new database and volume, and report the new DSN")
    sp.add_argument('-p', '--public', default=False, action='store_true',
                    help="Map the database port to the host")
    sp.add_argument('-m', '--message', help="Add a message to the record for this container cluster")
    sp.add_argument('-g', '--groupname', help="Set the username / group name, rather than selecting one randomly")

    sp = asp.add_parser('shell', help='Run a shell in a container')
    sp.set_defaults(subcommand='shell')
    sp.add_argument('-k', '--kill', default=False, action='store_true',
                    help="Kill a running shell before starting a new one")

    sp = asp.add_parser('tunnel', help='Run an ssh tunnel to the current database container')
    sp.set_defaults(subcommand='tunnel')
    sp.add_argument('-i', '--identity', help="Specify an identity file for loggin into the docker host")
    sp.add_argument('-k', '--kill', default=False, action='store_true',
                    help="Kill a running tunnel before starting a new one")
    sp.add_argument('ssh_key_file', type=str, nargs=1, help='Path to an ssh key file')

    sp = asp.add_parser('list', help='List docker entries in the accounts file')
    sp.set_defaults(subcommand='list')

    sp = asp.add_parser('kill', help='Destroy all of the containers associated with a username')
    sp.set_defaults(subcommand='kill')
    sp.add_argument('groupname', type=str, nargs='*', help='Group name of set of containers')

    sp = asp.add_parser('ui', help='Run a shell in an ambryui')
    sp.set_defaults(subcommand='ui')
    sp.add_argument('-k', '--kill', default=False, action='store_true',
                    help="Kill a running shell before starting a new one")

    sp = asp.add_parser('info', help='Print information about a docker group')
    sp.set_defaults(subcommand='info')
    sp.add_argument('-d', '--dsn', default=False, action='store_true',
                    help="Display the database DSN")
    sp.add_argument('groupname', type=str, nargs=1, help='Group name of set of containers')

def docker_command(args, rc):
    from ..library import new_library
    from . import global_logger

    try:
        l = new_library(rc)
        l.logger = global_logger
    except Exception as e:
        l = None

    globals()['docker_' + args.subcommand](args, l, rc)


def get_docker_file(rc):
    """Get the path for a .ambry-docker file, parallel to the .ambry.yaml file"""
    from os.path import dirname, join

    loaded = rc['loaded'][0][0]

    return join(dirname(loaded),'.ambry-docker.yaml')

def get_df_entry(rc,name):
    from ambry.util import AttrDict

    d = AttrDict.from_yaml(get_docker_file(rc))

    return d[name]

def set_df_entry(rc, name, entry):
    from ambry.util import AttrDict
    import os.path

    if os.path.exists(get_docker_file(rc)):
        try:
            d = AttrDict.from_yaml(get_docker_file(rc))
        except TypeError:
            # Empty file, I guess.
            d = AttrDict()
    else:
        d = AttrDict()

    d[name] = entry

    with open(get_docker_file(rc), 'wb') as f:
        d.dump(f)

def remove_df_entry(rc, name):
    from ambry.util import AttrDict
    import os.path

    if os.path.exists(get_docker_file(rc)):
        d = AttrDict.from_yaml(get_docker_file(rc))
    else:
        d = AttrDict()

    if name in d:
        del d[name]


    with open(get_docker_file(rc), 'wb') as f:
        d.dump(f)

def docker_init(args, l, rc):
    """Initialize a new docker volumes and database container, and report the database DSNs"""

    from docker.errors import NotFound, NullResource
    import string
    import random
    from ambry.util import parse_url_to_dict
    from docker.utils import kwargs_from_env
    from . import fatal, docker_client

    client = docker_client()

    def id_generator(size=12, chars=string.ascii_lowercase + string.digits):
        return ''.join(random.choice(chars) for _ in range(size))

    # Check if the postgres image exists.

    postgres_image = 'civicknowledge/postgres'

    try:
        inspect = client.inspect_image(postgres_image)
    except NotFound:
        fatal(('Database image {i} not in docker. Run \'python setup.py docker -D\' or '
               ' \'docker pull {i}\'').format(i=postgres_image))

    volumes_image = 'civicknowledge/volumes'

    try:
        inspect = client.inspect_image(volumes_image)
    except NotFound:
        prt('Pulling image for volumns container: {}'.format(volumes_image))
        client.pull(volumes_image)


    # Assume that the database host IP is also the docker host IP. This usually be true
    # externally to the docker host, and internally, we'll alter the host:port to
    # 'db' anyway.
    db_host_ip = parse_url_to_dict(kwargs_from_env()['base_url'])['netloc'].split(':',1)[0]

    try:
        d = parse_url_to_dict(l.database.dsn)
    except AttributeError:
        d = {'query':''}

    if 'docker' not in d['query'] or args.new:
        groupname = id_generator()
        password = id_generator()
        database = groupname
    else:
        groupname = d['username']
        password = d['password']
        database = d['path'].strip('/')

    # Override the username if one was provided
    if args.groupname:
        groupname =  database = args.groupname

    volumes_c = 'ambry_volumes_{}'.format(groupname)
    db_c = 'ambry_db_{}'.format(groupname)

    #
    # Create the volume container
    #

    try:
        inspect = client.inspect_container(volumes_c)
        prt('Found volume container {}'.format(volumes_c))
    except NotFound:
        prt('Creating volume container {}'.format(volumes_c))

        r = client.create_container(
            name=volumes_c,
            image=volumes_image,
            labels={
                'civick.ambry.group': groupname,
                'civick.ambry.message': args.message,
                'civick.ambry.role': 'volumes'
            },
            volumes=['/var/ambry', '/var/backups'],
            host_config = client.create_host_config()
        )

    #
    # Create the database container
    #

    try:
        inspect = client.inspect_container(db_c)
        prt('Found db container {}'.format(db_c))
    except NotFound:
        prt('Creating db container {}'.format(db_c))

        if args.public:
            port_bindings = {5432: ('0.0.0.0',)}
        else:
            port_bindings = None

        kwargs = dict(
            name=db_c,
            image=postgres_image,
            labels={
                'civick.ambry.group': groupname,
                'civick.ambry.message': args.message,
                'civick.ambry.role': 'db'

            },
            volumes=['/var/ambry', '/var/backups'],
            ports=[5432],
            environment={
                'ENCODING': 'UTF8',
                'BACKUP_ENABLED': 'true',
                'BACKUP_FREQUENCY': 'daily',
                'BACKUP_EMAIL': 'eric@busboom.org',
                'USER': groupname,
                'PASSWORD': password,
                'SCHEMA': database,
                'POSTGIS': 'true'
            },
            host_config=client.create_host_config(
                volumes_from=[volumes_c],
                port_bindings=port_bindings
            )
        )

        r = client.create_container(**kwargs)

        client.start(r['Id'])

        inspect = client.inspect_container(r['Id'])

    try:
        port =  inspect['NetworkSettings']['Ports']['5432/tcp'][0]['HostPort']
    except (TypeError, KeyError):
        port = None

    if port:
        dsn = 'postgres://{username}:{password}@{host}:{port}/{database}?docker'.format(
                username=groupname, password=password, database=database, host=db_host_ip, port=port)

    else:
        dsn = 'postgres://{username}:{password}@{host}:{port}/{database}?docker'.format(
            username=groupname, password=password, database=database, host='localhost', port='5432')
        warn("No public port; you'll need to set up a tunnel for external access")

    if l and l.database.dsn != dsn:
        prt("Set the library.database configuration to this DSN:")
        prt(dsn)


    set_df_entry(rc, groupname, dict(
        username=groupname,
        password=password,
        database=database,
        db_port=int(port) if port else None,
        host=db_host_ip,
        docker_url=client.base_url,
        volumes_name=volumes_c,
        db_name=db_c,
        dsn=dsn,
        message=args.message
    ))

def check_ambry_image(client, image):
    from docker.errors import NotFound, NullResource
    try:
        _ = client.inspect_image(image)
    except NotFound:
        fatal(('Database image {i} not in docker. Run \'python setup.py docker {{opt}}\' or '
               ' \'docker pull {i}\'').format(i=image))

def docker_shell(args, l, rc):
    """Run a shell in an Ambry builder image, on the current docker host"""

    from . import docker_client, get_docker_links
    from docker.errors import NotFound, NullResource
    import os

    client = docker_client()

    username, dsn, volumes_c, db_c, envs = get_docker_links(rc)

    shell_name = 'ambry_shell_{}'.format(username)

    # Check if the  image exists.

    image = 'civicknowledge/ambry'

    check_ambry_image(client, image)

    try:
        inspect = client.inspect_container(shell_name)
        running = inspect['State']['Running']
        exists = True
    except NotFound as e:
        running = False
        exists = False

    # If no one is using is, clear it out.
    if exists and not running:
        prt('Container {} exists but is not running; recreate it from latest image'.format(shell_name))
        client.remove_container(shell_name)
        exists = False

    if not running:

        kwargs = dict(
            name=shell_name,
            image=image,
            labels={
                'civick.ambry.group': username,
                'civick.ambry.role': 'shell'
            },
            detach=False,
            tty=True,
            stdin_open=True,
            environment=envs,
            host_config=client.create_host_config(
                volumes_from=[volumes_c],
                links={
                    db_c: 'db'
                }
            ),
            command='/bin/bash'
        )

        prt('Starting container with image {} '.format(image))

        r = client.create_container(**kwargs)

        while True:
            try:
                inspect = client.inspect_container(r['Id'])
                break
            except NotFound:
                prt('Waiting for container to be created')

        prt('Starting {}'.format(inspect['Id']))
        os.execlp('docker', 'docker', 'start', '-a', '-i', inspect['Id'])

    else:

        prt("Exec new shell on running container")
        os.execlp('docker', 'docker', 'exec', '-t', '-i', inspect['Id'], '/bin/bash')


def docker_tunnel(args, l, rc):
    """Run a shell in an Ambry builder image, on the current docker host"""

    from . import docker_client, get_docker_links
    from docker.errors import NotFound, NullResource
    from docker.utils import kwargs_from_env
    from ambry.util import parse_url_to_dict
    import os
    from . import fatal

    args.ssh_key_file = args.ssh_key_file.pop(0)

    if not os.path.exists(args.ssh_key_file):
        fatal('The tunnel argument must be the path to a public ssh key')

    client = docker_client()

    groupname, dsn, volumes_c, db_c, envs = get_docker_links(rc)

    shell_name = 'ambry_tunnel_{}'.format(groupname)

    # Check if the  image exists.

    image = 'civicknowledge/tunnel'

    check_ambry_image(client, image)

    try:
        inspect = client.inspect_container(shell_name)
        running = inspect['State']['Running']
        exists = True
    except NotFound as e:
        running = False
        exists = False

    if args.kill and running:
        client.remove_container(shell_name, force=True)
        running = False

    if running:
        fatal('Container {} is running. Kill it with -k'.format(shell_name))

    kwargs = dict(
        name=shell_name,
        image=image,
        labels={
            'civick.ambry.group': groupname,
            'civick.ambry.role': 'tunnel'
        },
        detach=False,
        tty=False,
        stdin_open=False,
        environment=envs,
        host_config=client.create_host_config(
            links={
                db_c: 'db'
            },
            port_bindings={22: ('0.0.0.0',)}
        ),
        command="/usr/sbin/sshd -D"

    )

    prt('Starting tunnel container with image {} '.format(image))

    r = client.create_container(**kwargs)

    client.start(r['Id'])

    inspect = client.inspect_container(r['Id'])

    port =  inspect['NetworkSettings']['Ports']['22/tcp'][0]['HostPort']

    host, _ = parse_url_to_dict(kwargs_from_env()['base_url'])['netloc'].split(':',1)

    # Now, insert the SSH key

    with open(args.ssh_key_file) as f:
        key = f.read()

    ex = client.exec_create(container=r['Id'],
                            cmd=['sh', '/bin/loadkey',key])

    client.exec_start(ex['Id'])

    p = start_tunnel(host, port)
    prt("Tunnel is running as pid: {}".format(p.pid))
    p.wait()


def start_tunnel(host, port):
    import subprocess

    options = ['-o','"CheckHostIP no"',
               '-o','"StrictHostKeyChecking no"',
               '-o','"UserKnownHostsFile /dev/null"']


    cmd = ['ssh','-N', '-p', port, '-L', '{}:{}:{}'.format(5432,'db',5432)] + options  + ['root@{}'.format(host)]

    prt('Running: '+' '.join(cmd))

    p = subprocess.Popen(' '.join(cmd), shell=True)
    return p


def docker_kill(args, l, rc):
    from operator import itemgetter
    from docker.utils import kwargs_from_env
    from . import docker_client, get_docker_links

    client = docker_client()

    for groupname in args.groupname:

        for c in client.containers(all=True):
            name = c['Names'][0].strip('/')
            if groupname in name:
                prt("Removing: {}".format(name))
                client.remove_container(container=c['Id'], v=True, force=True)
                try:
                    remove_df_entry(rc, groupname)
                except KeyError:
                    pass


def docker_ui(args, l, rc, attach=True):
    """Run a shell in an Ambry builder image, on the current docker host"""

    from . import docker_client, get_docker_links
    from docker.errors import NotFound, NullResource
    import os

    client = docker_client()

    username, dsn, volumes_c, db_c, envs = get_docker_links(rc)

    shell_name = 'ambry_ui_{}'.format(username)

    # Check if the  image exists.

    image = 'civicknowledge/ambryui'

    check_ambry_image(client, image)

    try:
        inspect = client.inspect_container(shell_name)
        running = inspect['State']['Running']
        exists = True
    except NotFound as e:
        running = False
        exists = False

    # If no one is using is, clear it out.
    if exists and (not running or args.kill):
        prt('Killing container {}'.format(shell_name))
        client.remove_container(shell_name, force = True)
        exists = False
        running = False

    if not running:

        vh_root = rc.get('docker', {}).get('ui_domain', None)
        if vh_root:
            envs['VIRTUAL_HOST'] = '{}.{}'.format(username, vh_root)


        kwargs = dict(
            name=shell_name,
            image=image,
            labels={
                'civick.ambry.group': username,
                'civick.ambry.role': 'ui',
                'civick.ambry.virt_host': envs.get('VIRTUAL_HOST')
            },
            detach=False,
            tty=True,
            stdin_open=True,
            environment=envs,
            host_config=client.create_host_config(
                volumes_from=[volumes_c],
                links={
                    db_c: 'db',
                },
                port_bindings={80: ('0.0.0.0',)}
            )
        )


        r = client.create_container(**kwargs)

        while True:
            try:
                inspect = client.inspect_container(r['Id'])
                break
            except NotFound:
                prt('Waiting for container to be created')

        client.start(r['Id'])

        inspect = client.inspect_container(r['Id'])

        try:
            port = inspect['NetworkSettings']['Ports']['80/tcp'][0]['HostPort']
        except:
            port = None
            print inspect['NetworkSettings']['Ports']

        prt('Starting ui container')
        prt('   Name {}'.format(shell_name))
        prt('   Virtual host {} '.format(envs['VIRTUAL_HOST']))
        prt('   Host port: {}'.format(port))


    else:
        prt('Container {} is already running'.format(shell_name))

def docker_list(args, l, rc):
    from operator import itemgetter
    from docker.utils import kwargs_from_env
    from . import docker_client, get_docker_links
    from collections import defaultdict
    from ambry.util import parse_url_to_dict

    client = docker_client()
    prt("Listing Ambry containers for : {}",client.base_url)

    host = parse_url_to_dict(client.base_url)['hostname']

    fields = ' '.split
    rows = []

    entries = defaultdict(dict)

    for c in client.containers(all=True):
        if 'civick.ambry.role' in c['Labels']:
            group = c['Labels'].get('civick.ambry.group')
            role = c['Labels'].get('civick.ambry.role')
            entries[group]['group'] = group

            if role == 'db':
                entries[group]['message'] = c['Labels'].get('civick.ambry.message')


            vhost = c['Labels'].get('civick.ambry.virt_host')

            entries[group][role] = {'name': c['Names'][0],
                                    'role': role,
                                    'message': c['Labels'].get('civick.ambry.message'),
                                    'vhost': "http://{}".format(vhost) if vhost else None,
                                    'id': c['Id'],
                                    'ports': None}

            if c['Ports'] and c['Ports'][0].get('PublicPort'):
                ports = c['Ports'][0]
                host = ports['IP'] if ports.get('IP') and ports.get('IP') != '0.0.0.0' else host
                entries[group][role]['ports'] = "{}:{}".format(host, ports['PublicPort'])

    rows = []
    headers = 'Group Role Name Ports Notes'.split()

    message_map = {
        'db': 'dsn',
        'ui': 'vhost'
    }

    for key in sorted(entries.keys()):
        e = entries[key]
        group = e['group']
        rows.append([group, None, None, None, e['message'] ])

        df = get_df_entry(rc, group)

        for role in sorted([k for k,v in e.items() if isinstance(v, dict)]):
            m = e[role]
            if role == 'ui':
                message = m['vhost']
            elif role == 'db' and df:
                message = df['dsn']
            else:
                message = None
            rows.append(['', role, m['name'], m['ports'], message])


    from tabulate import tabulate

    print tabulate(rows, headers)

def docker_info(args, l, rc):

    groupname = args.groupname.pop(0)

    if args.dsn:
        try:
            df = get_df_entry(rc, groupname)
            prt(df['dsn'])
        except KeyError:
            # Meant for use in shell scripts, so jsut reutrn an error return code
            import sys
            sys.exit(1)
