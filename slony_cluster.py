#!/usr/bin/python
# -*- coding: utf-8 -*-

DOCUMENTATION = '''
---
module: slony_cluster
author: Alexandr Kurilin
version_added: "1.9"
short_description: Manage a Slony-I cluster
requirements: [psycopg2, slonik]
description:
    - Manage a Slony-I cluster assuming one master and one slave
'''

EXAMPLES = '''
# Foo
- slony_cluster: name=replication
'''

try:
    import psycopg2
    import psycopg2.extras
except ImportError:
    postgresqldb_found = False
else:
    postgresqldb_found = True

# ===========================================
# Postgres / slonik support methods.
#

# def execute_command(module, cmd, use_unsafe_shell=False, data=None):
#     # if module.syslogging:
#     #     syslog.openlog('ansible-%s' % os.path.basename(__file__))
#     #     syslog.syslog(syslog.LOG_NOTICE, 'Command %s' % '|'.join(cmd))
#     return module.run_command(module, cmd, use_unsafe_shell=use_unsafe_shell, data=data)

def schema_exists(cursor, cluster_name):
    query = "SELECT * FROM pg_catalog.pg_namespace WHERE nspname=%(schema)s"
    cursor.execute(query, {'schema': "_" + cluster_name})
    return cursor.rowcount == 1

# TODO: this should do drop node first, wait and then proceed with uninstall
# but it's not working for some reason, investigate
def remove_cluster(module, host, db, replication_user, cluster_name, password, port):
    cmd = """
    slonik <<_EOF_
    cluster name = %s;
    node 1 admin conninfo='host=%s dbname=%s user=%s port=%s password=%s';
    uninstall node (id = 1);
_EOF_
    """ % (cluster_name, host, db, replication_user, port, password)

    return module.run_command(cmd, use_unsafe_shell=True)

def init_cluster(module, host, db, cluster_name, replication_user, password, port, origin_id):
    cmd = """slonik <<_EOF_
    # INIT CLUSTER
    cluster name = %s;
    node 1 admin conninfo='host=%s dbname=%s user=%s port=%s password=%s';
    init cluster (id = %s, comment = 'Node 1 - %s@%s');
_EOF_""" % (cluster_name,
            host, db, replication_user, port, password,
            origin_id, db, host)

    return module.run_command(cmd, use_unsafe_shell=True)

# ===========================================
# Module execution.
#

def main():
    module = AnsibleModule(
        argument_spec=dict(
            port=dict(default="5432"),
            cluster_name=dict(default="replication"),
            replication_user=dict(default="postgres"),
            password=dict(default=""),
            db=dict(required=True),
            host=dict(required=True),
            origin_id=dict(default=1),
            state=dict(default="present", choices=["absent", "present"]),
        ),
        supports_check_mode = False
    )

    if not postgresqldb_found:
        module.fail_json(msg="the python psycopg2 module is required")

    port = module.params["port"]
    state = module.params["state"]
    cluster_name = module.params["cluster_name"]
    replication_user = module.params["replication_user"]
    password = module.params["password"]
    host = module.params["host"]
    db = module.params["db"]
    origin_id = module.params["origin_id"]
    changed = False

    # To use defaults values, keyword arguments must be absent, so
    # check which values are empty and don't include in the **kw
    # dictionary
    params_map = {
        "password":"password",
        "port":"port"
    }

    kw = dict( (params_map[k], v) for (k, v) in module.params.iteritems()
              if k in params_map and v != '' )

    try:
        # TODO: this probably gets overwritten by contents of kw which can lead
        # to a total poopshow
        db_connection_master = psycopg2.connect(
                database=db,
                host=host,
                user=replication_user,
                **kw)
        cursor_master = db_connection_master.cursor(
                cursor_factory=psycopg2.extras.DictCursor)

    # TODO: want to be clearer about which DB connection failed to init
    except Exception, e:
        module.fail_json(msg="unable to connect to database: %s" % e)

    result = {}

    if state == "absent":
        master_initialized = schema_exists(cursor_master, cluster_name)
        if master_initialized:
            (rc, out, err) = remove_cluster(module, host, db, replication_user, cluster_name, password, port)
            result['changed'] = True
            if rc != 0:
                module.fail_json(stdout=out,msg=err, rc=rc)
            # TODO: remove for prod
            # result['stdout'] = out
        else:
            result['changed'] = False

    if state == "present":
        master_initialized = schema_exists(cursor_master, cluster_name)
        if master_initialized:
            result['changed'] = False
        else:
            (rc, out, err) = init_cluster(module, host, db, cluster_name, replication_user, password, port, origin_id)
            # TODO: remove for prod
            # result['stdout'] = out
            if rc != 0:
                module.fail_json(stdout=out, msg=err, rc=rc)
            result['changed'] = True

    module.exit_json(**result)

from ansible.module_utils.basic import *
main()
