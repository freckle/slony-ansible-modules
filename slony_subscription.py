#!/usr/bin/python
# -*- coding: utf-8 -*-

DOCUMENTATION = '''
---
module: slony_subscription
author: Alexandr Kurilin
version_added: "1.9"
short_description: Create / delete a slony subscription
requirements: [psycopg2, slonik]
description:
    - Manage a Slony-I subscription
'''

EXAMPLES = '''
# Foo
- slony_subscription: name=TODO
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

def subscription_exists(cursor, cluster_name, set_id, provider_id, receiver_id):
    query = """SELECT 1 FROM _{0}.sl_subscribe
               WHERE sub_set = %s
               AND sub_provider = %s
               AND sub_receiver = %s
               """.format(cluster_name)
    cursor.execute(query, (int(set_id), int(provider_id), int(receiver_id)))
    return cursor.rowcount == 1

# defaults FORWARD to YES
def subscribe_set(module, cluster_name, master_conninfo, slave_conninfo, set_id, provider_id, receiver_id):
    cmd = """
    slonik <<_EOF_
    cluster name = %s;
    node %s admin conninfo='%s';
    node %s admin conninfo='%s';
    subscribe set (id=%s, provider=%s, receiver=%s, forward=YES);
_EOF_
    """ % (cluster_name,
           provider_id, master_conninfo,
           receiver_id, slave_conninfo,
           set_id, provider_id, receiver_id,
           )

    return module.run_command(cmd, use_unsafe_shell=True)

def unsubscribe_set(module, cluster_name, master_conninfo, slave_conninfo, set_id, provider_id, receiver_id):
    cmd = """
    slonik <<_EOF_
    cluster name = %s;
    node %s admin conninfo='%s';
    node %s admin conninfo='%s';
    unsubscribe set (id=%s, receiver=%s);
_EOF_
    """ % (cluster_name,
           provider_id, master_conninfo,
           receiver_id, slave_conninfo,
           set_id, receiver_id,
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
            set_id=dict(required=True),
            provider_id=dict(required=True),
            receiver_id=dict(required=True),
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
    set_id = module.params["set_id"]
    provider_id = module.params["provider_id"]
    receiver_id = module.params["receiver_id"]
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

    sub_is_present = subscription_exists(master_cursor, cluster_name, set_id, provider_id, receiver_id)

    if state == "absent":
        if sub_is_present:
            (rc, out, err) = unsubscribe_set(module, cluster_name, master_conninfo, slave_conninfo, set_id, provider_id, receiver_id)
            result['changed'] = True
            if rc != 0:
                module.fail_json(stdout=out,msg=err, rc=rc)
        else:
            result['changed'] = False

    elif state == "present":
        if sub_is_present:
            result['changed'] = False
        else:
            (rc, out, err) = subscribe_set(module, cluster_name, master_conninfo, slave_conninfo, set_id, provider_id, receiver_id)
            if rc != 0:
                module.fail_json(stdout=out, msg=err, rc=rc)
            result['changed'] = True

    else:
        module.fail_json(msg="The impossible happened")


    module.exit_json(**result)

from ansible.module_utils.basic import *
main()
