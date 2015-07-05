#!/usr/bin/python
# -*- coding: utf-8 -*-

DOCUMENTATION = '''
---
module: slony_table
author: Alexandr Kurilin
version_added: "1.9"
short_description: Create / delete a slony table in a set
requirements: [psycopg2, slonik]
description:
    - Manage a Slony-I table in a set
'''

EXAMPLES = '''
# Foo
- slony_table: name=TODO
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

# Don't SQL inject yourself
# def table_exists(cursor, cluster_name, set_id, table_id):
#     query = "SELECT 1 FROM _{0}.sl_table WHERE tab_id = %s AND tab_set = %s".format(cluster_name)
#     cursor.execute(query, (int(table_id), int(set_id)))
#     return cursor.rowcount == 1

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

# Don't SQL inject yourself
def sequence_exists(cursor, cluster_name, set_id, sequence_id):
    query = """SELECT 1 FROM _{0}.sl_sequence
               WHERE seq_id = %s
               AND seq_set = %s""".format(cluster_name)
    cursor.execute(query, (int(sequence_id), int(set_id)))
    return cursor.rowcount == 1

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

# ===========================================
# Module execution.
#

def main():
    module = AnsibleModule(
        argument_spec=dict(
            port            =dict(default="5432"),
            cluster_name    =dict(default="replication"),
            replication_user=dict(default="postgres"),
            password        =dict(default=""),
            db              =dict(required=True),
            host            =dict(required=True),
            set_id          =dict(required=True),
            origin_id       =dict(required=True),
            tables          =dict(required=True, type='list'),
            sequences       =dict(required=False, type='list'),
            state           =dict(default="present", choices=["absent", "present"]),
        ),
        supports_check_mode = False
    )

    if not postgresqldb_found:
        module.fail_json(msg="the python psycopg2 module is required")

    port             = module.params["port"]
    cluster_name     = module.params["cluster_name"]
    replication_user = module.params["replication_user"]
    password         = module.params["password"]
    db               = module.params["db"]
    host             = module.params["host"]
    set_id           = module.params["set_id"]
    origin_id        = module.params["set_id"]
    tables           = module.params["tables"]
    sequences        = module.params["sequences"]
    state            = module.params["state"]

    changed          = False

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
        cursor = db_connection_master.cursor(
                cursor_factory=psycopg2.extras.DictCursor)

    except Exception, e:
        module.fail_json(msg="unable to connect to database: %s" % e)

    present_tables = replicated_tables(cursor, cluster_name, set_id)
    present_sequences = replicated_tables(cursor, cluster_name, set_id)

    present_table_ids = frozenset(map(lambda x: x[0], present_tables))
    present_sequence_ids = frozenset(map(lambda x: x[0], present_sequences))

    # print tables

    arg_table_ids = frozenset(map(lambda x: x['id'], tables))
    arg_sequence_ids = frozenset(map(lambda x: x['id'], sequences))

    result = {}

    # the trick with absent is making sure the tables with given fqid name and id
    # are no longer present in the database for given set id.
    # It's possible the same tables exist with a different id, so it's not super
    # obvious what to do in that case
    if state == "absent":
        raise Exception('Not yet implemented')
        # table_is_present = table_exists(cursor, cluster_name, set_id, table_id)
        # if table_is_present:
        #     (rc, out, err) = drop_table(module, host, db, replication_user, cluster_name, password, port, origin_id, table_id)
        #     result['changed'] = True
        #     if rc != 0:
        #         module.fail_json(stdout=out,msg=err, rc=rc)
        # else:
        #     result['changed'] = False

    # we have two cases here: either tables are being added to a set that
    # doesn't have subscribers yet, OR tables are being added to an already
    # subscribed-to set, in which case we have to follow a special merge sets
    # flow
    if state == "present":
        sis = set_is_subscribed(cursor, cluster_name, set_id)

        # TODO: what if arg ids is a subset of present ids?
        new_table_ids = arg_table_ids - present_table_ids
        new_sequence_ids = arg_sequence_ids - present_sequence_ids

        must_add = len(new_table_ids) > 0 or len(new_sequence_ids) > 0

        if sis and must_add:
            # merge into existing subscription
            raise Exception('Not yet implemented')
        elif must_add:
            # add to set, no existing subscription
            for tid in new_table_ids:
                table = next(t for t in tables if t['id'] == tid)
                (rc, out, err) = create_table(
                        module,
                        host,
                        db,
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
                        host,
                        db,
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
        else:
            # nothing to add
            result['changed'] = False


    module.exit_json(**result)

from ansible.module_utils.basic import *
main()
