#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# The MIT License (MIT)

# Copyright (c) 2015 Alexandr Kurilin <alex@frontrowed.com>

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

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
def table_exists(cursor, cluster_name, set_id, table_id):
    query = "SELECT 1 FROM _{0}.sl_table WHERE tab_id = %s AND tab_set = %s".format(cluster_name)
    cursor.execute(query, (int(table_id), int(set_id)))
    return cursor.rowcount == 1

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
            set_id=dict(required=True),
            origin_id=dict(required=True),
            table_id=dict(required=True),
            fqname=dict(required=True),
            comment=dict(default=""),
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
    db = module.params["db"]
    host = module.params["host"]
    set_id = module.params["set_id"]
    origin_id = module.params["set_id"]
    table_id = module.params["table_id"]
    fqname = module.params["fqname"]
    comment = module.params["comment"]
    state = module.params["state"]
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
        cursor = db_connection_master.cursor(
                cursor_factory=psycopg2.extras.DictCursor)

    except Exception, e:
        module.fail_json(msg="unable to connect to database: %s" % e)

    result = {}

    if state == "absent":
        table_is_present = table_exists(cursor, cluster_name, set_id, table_id)
        if table_is_present:
            (rc, out, err) = drop_table(module, host, db, replication_user, cluster_name, password, port, origin_id, table_id)
            result['changed'] = True
            if rc != 0:
                module.fail_json(stdout=out,msg=err, rc=rc)
        else:
            result['changed'] = False

    if state == "present":
        table_is_present = table_exists(cursor, cluster_name, set_id, table_id)
        if table_is_present:
            result['changed'] = False
        else:
            (rc, out, err) = create_table(module, host, db, replication_user, cluster_name, password, port, set_id, origin_id, table_id, fqname, comment)
            if rc != 0:
                module.fail_json(stdout=out, msg=err, rc=rc)
            result['changed'] = True

    module.exit_json(**result)

from ansible.module_utils.basic import *
main()
