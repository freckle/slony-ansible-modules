#!/usr/bin/python
# -*- coding: utf-8 -*-

DOCUMENTATION = '''
---
module: slony_node
author: Alexandr Kurilin
version_added: "1.9"
short_description: Create / delete a slony node
requirements: [psycopg2, slonik]
description:
    - Manage a Slony-I node
'''

EXAMPLES = '''
# Foo
- slony_node: name=TODO
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

def schema_exists(cursor, cluster_name):
    query = "SELECT * FROM pg_catalog.pg_namespace WHERE nspname=%(schema)s"
    cursor.execute(query, {'schema': "_" + cluster_name})
    return cursor.rowcount == 1

def store_node(module, cluster_name, master_conninfo, slave_conninfo, node_id, event_node_id):
    cmd = """
    slonik <<_EOF_
    cluster name = %s;
    node %s admin conninfo='%s';
    node %s admin conninfo='%s';
    store node (id=%s, comment='', event node=%s);
_EOF_
    """ % (cluster_name,
           event_node_id, master_conninfo,
           node_id, slave_conninfo,
           node_id, event_node_id)

    return module.run_command(cmd, use_unsafe_shell=True)

def drop_node(module, cluster_name, master_conninfo, slave_conninfo, node_id, event_node_id):
    cmd = """
    slonik <<_EOF_
    cluster name = %s;
    node %s admin conninfo='%s';
    node %s admin conninfo='%s';
    drop node (id=%s, event node=%s);
    uninstall node (id=%s);
_EOF_
    """ % (cluster_name,
           event_node_id, master_conninfo,
           node_id, slave_conninfo,
           node_id, event_node_id,
           node_id)
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
            node_id=dict(required=True),
            event_node_id=dict(required=True),
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
    node_id = module.params["node_id"]
    event_node_id = module.params["event_node_id"]
    state = module.params["state"]
    changed = False

#     node 1 admin conninfo='host=%s dbname=%s user=%s port=%s password=%s';
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
        master_cursor = db_connection_master.cursor(cursor_factory=psycopg2.extras.DictCursor)
        slave_cursor = db_connection_slave.cursor(cursor_factory=psycopg2.extras.DictCursor)
    except Exception, e:
        module.fail_json(msg="unable to connect to database: %s" % e)

    result = {}

    # NB: has to be run against the slave node
    schema_is_present = schema_exists(slave_cursor, cluster_name)

    if state == "absent":
        if schema_is_present:
            (rc, out, err) = drop_node(module, cluster_name, master_conninfo, slave_conninfo, node_id, event_node_id)
            result['changed'] = True
            if rc != 0:
                module.fail_json(stdout=out,msg=err, rc=rc)
        else:
            result['changed'] = False

    if state == "present":
        if schema_is_present:
            result['changed'] = False
        else:
            (rc, out, err) = store_node(module, cluster_name, master_conninfo, slave_conninfo, node_id, event_node_id)
            if rc != 0:
                module.fail_json(stdout=out, msg=err, rc=rc)
            result['changed'] = True

    module.exit_json(**result)

from ansible.module_utils.basic import *
main()
