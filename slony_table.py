#!/usr/bin/python
# -*- coding: utf-8 -*-

DOCUMENTATION = '''
---
module: slony_table
author: Alexandr Kurilin
version_added: "1.9"
short_description: Add or drop tables and sequences from/to a slony replication set
requirements: [psycopg2, slonik, jinja2]
description:
    - Adds or removes Slony-I tables and sequences in a replication set
'''

EXAMPLES = '''
# Foo
- slony_table: name=TODO
'''

import jinja2

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

def set_is_subscribed(cursor, cluster_name, set_id):
    query = "SELECT 1 FROM _{0}.sl_subscribe WHERE sub_set = %s".format(cluster_name)
    cursor.execute(query, (int(set_id),))
    return cursor.rowcount == 1

def replicated_tables(cursor, cluster_name, set_id):
    query = """SELECT tab_id,tab_relname,tab_nspname,tab_set
               FROM _{0}.sl_table
               WHERE tab_set = %s""".format(cluster_name)
    cursor.execute(query, (int(set_id),))
    return cursor.fetchall()

def replicated_sequences(cursor, cluster_name, set_id):
    query = """SELECT seq_id,seq_relname,seq_nspname,seq_set
               FROM _{0}.sl_sequence
               WHERE seq_set = %s""".format(cluster_name)
    cursor.execute(query, (int(set_id),))
    return cursor.fetchall()

def create_table(module, host, db, replication_user, cluster_name, password, port, set_id, origin_id, table_id, fqname, comment):
    cmd = """
    slonik <<_EOF_
    cluster name = %s;
    node 1 admin conninfo='host=%s dbname=%s user=%s port=%s password=%s';
    set add table (set id=%s, origin=%s, id=%s, fully qualified name = '%s', comment='%s');
_EOF_
    """ % (cluster_name, host, db, replication_user, port, password, set_id, origin_id, table_id, fqname, comment)

    return module.run_command(cmd, use_unsafe_shell=True)

def drop_table(module, host, db, replication_user, cluster_name, password, port, origin_id, table_id):
    cmd = """
    slonik <<_EOF_
    cluster name = %s;
    node 1 admin conninfo='host=%s dbname=%s user=%s port=%s password=%s';
    set drop table (origin=%s, id=%s);
_EOF_
    """ % (cluster_name, host, db, replication_user, port, password, origin_id, table_id)

    return module.run_command(cmd, use_unsafe_shell=True)

def create_sequence(module, host, db, replication_user, cluster_name, password, port, set_id, origin_id, sequence_id, fqname, comment):
    cmd = """
    slonik <<_EOF_
    cluster name = %s;
    node 1 admin conninfo='host=%s dbname=%s user=%s port=%s password=%s';
    set add sequence (set id=%s, origin=%s, id=%s, fully qualified name='%s', comment='%s');
_EOF_
    """ % (cluster_name, host, db, replication_user, port, password, set_id, origin_id, sequence_id, fqname, comment)

    return module.run_command(cmd, use_unsafe_shell=True)

def drop_sequence(module, host, db, replication_user, cluster_name, password, port, origin_id, sequence_id):
    cmd = """
    slonik <<_EOF_
    cluster name = %s;
    node 1 admin conninfo='host=%s dbname=%s user=%s port=%s password=%s';
    set drop sequence (origin=%s, id=%s);
_EOF_
    """ % (cluster_name, host, db, replication_user, port, password, origin_id, sequence_id)

    return module.run_command(cmd, use_unsafe_shell=True)

# merge new tables tables and sequences into existing replication set
def merge_tables_seqs(module, master_conninfo, slave_conninfo, cluster_name, set_id, origin_id, provider_id, receiver_id, new_tables, new_sequences):
    cmd = """
    slonik <<_EOF_
    cluster name = {{ cluster_name }};
    node {{ origin_id }} admin conninfo='{{ master_conninfo }}';
    node {{ receiver_id }} admin conninfo='{{ slave_conninfo }}';
    create set (id = 99, origin = {{ origin_id }}, comment='temporary replication set to be merged');
    {% for sequence in sequences %}
    set add sequence (set id=99, origin={{ origin_id }}, id={{ sequence.id }}, fully qualified name = '{{ sequence.fqname }}', comment='{{ sequence.comment }}');
    {% endfor %}
    {% for table in tables %}
    set add table (set id=99, origin={{ origin_id }}, id={{ table.id }}, fully qualified name = '{{ table.fqname }}', comment='{{ table.comment }}');
    {% endfor %}
    subscribe set(id=99, provider={{ provider_id }}, receiver={{ receiver_id }});
    merge set(id={{ set_id }}, add id=99, origin={{ origin_id }});
_EOF_"""
    template = jinja2.Template(cmd)
    rendered = template.render(cluster_name=cluster_name,
                               master_conninfo=master_conninfo,
                               slave_conninfo=slave_conninfo,
                               origin_id=origin_id,
                               provider_id=provider_id,
                               receiver_id=receiver_id,
                               set_id=set_id,
                               sequences=new_sequences,
                               tables=new_tables)
    return module.run_command(rendered, use_unsafe_shell=True)


# ===========================================
# Module execution.
#

def main():
    module = AnsibleModule(
        argument_spec=dict(
            port            = dict(default="5432"),
            cluster_name    = dict(default="replication"),
            replication_user= dict(default="postgres"),
            password        = dict(default=""),
            master_db       = dict(required=True),
            master_host     = dict(required=True),
            slave_db        = dict(required=True),
            slave_host      = dict(required=True),
            set_id          = dict(required=True),
            origin_id       = dict(required=True),
            receiver_id     = dict(required=True),
            tables          = dict(required=True, type='list'),
            sequences       = dict(required=False, type='list'),
        ),
        supports_check_mode = False
    )

    if not postgresqldb_found:
        module.fail_json(msg="the python psycopg2 module is required")

    port             = module.params["port"]
    cluster_name     = module.params["cluster_name"]
    replication_user = module.params["replication_user"]
    password         = module.params["password"]
    master_db        = module.params["master_db"]
    master_host      = module.params["master_host"]
    slave_db         = module.params["slave_db"]
    slave_host       = module.params["slave_host"]
    set_id           = module.params["set_id"]
    origin_id        = module.params["origin_id"]
    receiver_id      = module.params["receiver_id"]
    tables           = module.params["tables"]
    sequences        = module.params["sequences"]

    changed          = False

    master_conninfo  = "host=%s dbname=%s user=%s port=%s password=%s" % (master_host, master_db, replication_user, port, password)
    slave_conninfo   = "host=%s dbname=%s user=%s port=%s password=%s" % (slave_host, slave_db, replication_user, port, password)

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
        db_connection_master = psycopg2.connect(master_conninfo)
        master_cursor        = db_connection_master.cursor(cursor_factory=psycopg2.extras.DictCursor)

    except Exception, e:
        module.fail_json(msg="unable to connect to database: %s" % e)

    # NB: a connection to the slave node isn't required for most of this
    # module's operations. It is only necessary in the already-subscribed
    # set merge scenario. In other cases, we can ignore this connection failing
    # to be established.
    try:
        db_connection_slave  = psycopg2.connect(slave_conninfo)
        slave_cursor         = db_connection_slave.cursor(cursor_factory=psycopg2.extras.DictCursor)
        slave_reachable = True
    except Exception, e:
        slave_reachable = False




    present_tables = replicated_tables(master_cursor, cluster_name, set_id)
    present_sequences = replicated_sequences(master_cursor, cluster_name, set_id)

    present_table_ids = frozenset(map(lambda x: x[0], present_tables))
    present_sequence_ids = frozenset(map(lambda x: x[0], present_sequences))

    # print tables

    arg_table_ids = frozenset(map(lambda x: x['id'], tables))
    arg_sequence_ids = frozenset(map(lambda x: x['id'], sequences))

    result = {}
    result['changed'] = False

    #
    # Take care of removing tables from replication set that are no longer in the config
    #
    table_ids_to_remove = present_table_ids - arg_table_ids
    sequence_ids_to_remove = present_sequence_ids - arg_sequence_ids

    # drop no longer replicated tables from the set
    for tid in table_ids_to_remove:
            (rc, out, err) = drop_table(module, master_host, master_db, replication_user, cluster_name, password, port, origin_id, tid)
            result['changed'] = True
            if rc != 0:
                module.fail_json(stdout=out,msg=err, rc=rc)

    # droip no longer replicated sequences from the set
    for sid in sequence_ids_to_remove:
            (rc, out, err) = drop_sequence(module, master_host, master_db, replication_user, cluster_name, password, port, origin_id, sid)
            result['changed'] = True
            if rc != 0:
                module.fail_json(stdout=out,msg=err, rc=rc)

    #
    # Take care of adding new tables to the replication set
    #
    sis = set_is_subscribed(master_cursor, cluster_name, set_id)

    # TODO: what if arg ids is a subset of present ids?
    new_table_ids = arg_table_ids - present_table_ids
    new_sequence_ids = arg_sequence_ids - present_sequence_ids
    must_add = len(new_table_ids) > 0 or len(new_sequence_ids) > 0
    if sis and must_add:

        # fail in the case where the slave is unreachable and we need to update
        # a currently subscribed set, which requires slonik to run against
        # all of the participating nodes
        if not slave_reachable:
            module.fail_json(msg="Cannot merge sets if the slave is unreachable")

        #
        # merge into existing subscription
        #
        new_tables = [table for tid in new_table_ids for table in tables if tid == table['id']]
        new_sequences = [sequence for sid in new_sequence_ids for sequence in sequences if sid == sequence['id']]

        (rc, out, err) = merge_tables_seqs(module, master_conninfo, slave_conninfo, cluster_name, set_id, origin_id, origin_id, receiver_id, new_tables, new_sequences)
        if rc != 0:
            module.fail_json(stdout=out, msg=err, rc=rc)
    elif must_add:
        #
        # add to set, no existing subscription
        #
        for tid in new_table_ids:
            table = next(t for t in tables if t['id'] == tid)
            (rc, out, err) = create_table(
                    module,
                    master_host,
                    master_db,
                    replication_user,
                    cluster_name,
                    password,
                    port,
                    set_id,
                    origin_id,
                    table['id'],
                    table['fqname'],
                    table['comment'])
            if rc != 0:
                module.fail_json(stdout=out, msg=err, rc=rc)
        for sid in new_sequence_ids:
            sequence = next(s for s in sequences if s['id'] == sid)
            (rc, out, err) = create_sequence(
                    module,
                    master_host,
                    master_db,
                    replication_user,
                    cluster_name,
                    password,
                    port,
                    set_id,
                    origin_id,
                    sequence['id'],
                    sequence['fqname'],
                    sequence['comment'])
            if rc != 0:
                module.fail_json(stdout=out, msg=err, rc=rc)

        result['changed'] = True

    module.exit_json(**result)

from ansible.module_utils.basic import *
main()
