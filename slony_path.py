#!/usr/bin/python
# -*- coding: utf-8 -*-

DOCUMENTATION = '''
---
module: slony_path
author: Alexandr Kurilin
version_added: "1.9"
short_description: Create / delete a slony path
requirements: [psycopg2, slonik]
description:
    - Manage a Slony-I path
'''

EXAMPLES = '''
# Foo
- slony_path: name=TODO
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

def path_exists(cursor, cluster_name, server_id, client_id):
    query = "SELECT 1 FROM _{0}.sl_path WHERE pa_server = %s AND pa_client = %s".format(cluster_name)
    cursor.execute(query, (int(server_id), int(client_id)))
    return cursor.rowcount == 1

# NB: this one creates TWO paths at once for the sake of sanity. If really necessary we can look into
# decoupling this into two separate calls
def store_path(module, cluster_name, master_conninfo, slave_conninfo, server_id, client_id):
    cmd = """
    slonik <<_EOF_
    cluster name = %s;
    node %s admin conninfo='%s';
    node %s admin conninfo='%s';
    store path (server=%s, client=%s, conninfo='%s');
    store path (server=%s, client=%s, conninfo='%s');
_EOF_
    """ % (cluster_name,
           server_id, master_conninfo,
           client_id, slave_conninfo,
           client_id, server_id, slave_conninfo,
           server_id, client_id, master_conninfo,
           )

    return module.run_command(cmd, use_unsafe_shell=True)

# Drops ONE path at a time
def drop_path(module, cluster_name, master_conninfo, slave_conninfo, master_node_id, slave_node_id, server_id, client_id):
    cmd = """
    slonik <<_EOF_
    cluster name = %s;
    node %s admin conninfo='%s';
    node %s admin conninfo='%s';
    drop path (server=%s, client=%s);
_EOF_
    """ % (cluster_name,
           master_node_id, master_conninfo,
           slave_node_id, slave_conninfo,
           server_id, client_id,
           )
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
            master_db=dict(required=True),
            slave_db=dict(required=True),
            master_host=dict(required=True),
            slave_host=dict(required=True),
            server_id=dict(required=True),
            client_id=dict(required=True),
            state=dict(default="present", choices=["absent", "present"]),
        ),
        supports_check_mode = False
    )

    if not postgresqldb_found:
        module.fail_json(msg="the python psycopg2 module is required")

    port = module.params["port"]
    cluster_name = module.params["cluster_name"]
    replication_user = module.params["replication_user"]
    password = module.params["password"]
    master_db = module.params["master_db"]
    slave_db = module.params["slave_db"]
    master_host = module.params["master_host"]
    slave_host = module.params["slave_host"]
    server_id = module.params["server_id"]
    client_id = module.params["client_id"]
    state = module.params["state"]
    changed = False

    master_conninfo = "host=%s dbname=%s user=%s port=%s password=%s" % (master_host, master_db, replication_user, port, password)
    slave_conninfo = "host=%s dbname=%s user=%s port=%s password=%s" % (slave_host, slave_db, replication_user, port, password)

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
        db_connection_master = psycopg2.connect(master_conninfo)
        db_connection_slave = psycopg2.connect(slave_conninfo)
        db_connection_master.set_isolation_level(0)
        db_connection_slave.set_isolation_level(0)
        master_cursor = db_connection_master.cursor(cursor_factory=psycopg2.extras.DictCursor)
        slave_cursor = db_connection_slave.cursor(cursor_factory=psycopg2.extras.DictCursor)
    except Exception, e:
        module.fail_json(msg="unable to connect to database: %s" % e)

    result = {}

    # The order of server_id and client_is is very important here, don't mess it up
    # Master must see slave as server on master's schema
    # Slave must see master as server on slave's schema
    path_is_present_on_master = path_exists(master_cursor, cluster_name, client_id, server_id)
    path_is_present_on_slave = path_exists(slave_cursor, cluster_name, server_id, client_id)
    path_is_present = path_is_present_on_master and path_is_present_on_slave

    if path_is_present_on_master != path_is_present_on_slave:
        module.fail_json(msg="Path is configured on part of the cluster, the cluster config is in a broken state")

    elif state == "absent":
        if path_is_present:
            # drop both paths. This can't be done in a single slonik operation without locking up the
            # tool
            (rc1, out, err) = drop_path(module, cluster_name, master_conninfo, slave_conninfo, server_id, client_id, server_id, client_id)
            (rc2, out, err) = drop_path(module, cluster_name, master_conninfo, slave_conninfo, server_id, client_id, client_id, server_id)
            result['changed'] = True
            if rc1 != 0 and rc2 != 0:
                module.fail_json(stdout=out,msg=err, rc=rc)
        else:
            result['changed'] = False

    elif state == "present":
        if path_is_present:
            result['changed'] = False
        else:
            (rc, out, err) = store_path(module, cluster_name, master_conninfo, slave_conninfo, server_id, client_id)
            if rc != 0:
                module.fail_json(stdout=out, msg=err, rc=rc)
            result['changed'] = True

    else:
        module.fail_json(msg="The impossible happened")


    module.exit_json(**result)

from ansible.module_utils.basic import *
main()
