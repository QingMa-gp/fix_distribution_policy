# -*- coding: utf-8 -*-

import argparse
import re
from multiprocessing import Process
import signal
import time
import pg as pygresql
from pg import DB
import sys

procs = []


def sig_handler(sig, arg):
    global procs
    for proc in procs:
        try:
            proc.terminate()
            proc.join()
        except Exception as e:
            sys.stderr.write("Error while terminating process: %s\n" % str(e))
    sys.stderr.write("terminated by signal %s\n" % sig)
    sys.exit(127)


class ChangePolicy(object):

    def __init__(self, dbname, port, host, user):
        self.dbname = dbname
        self.port = int(port)
        self.host = host
        self.user = user
        self.pt = re.compile(r'[(](.*)[)]')

    def get_db_conn(self):
        db = DB(dbname=self.dbname,
                port=self.port,
                host=self.host,
                user=self.user)

        db.query("set gp_use_legacy_hashops = off;")
        r = db.query("show gp_use_legacy_hashops ;").getresult()
        if r[0][0] == "on":
            db.close()
            sys.stderr.write("gp_use_legacy_hashops cannot be on for this script.\n")
            sys.exit(127)

        return db

    def get_regular_tables(self, is_legacy=True):
        db = self.get_db_conn()

        predict = 'like' if is_legacy else 'not like'
        sql = """
        select
          pn.nspname || '.' || pc.relname as relname,
          pg_get_table_distributedby(pc.oid) distby
        from pg_class pc,
             pg_namespace pn,
             gp_distribution_policy gdp
        where pc.oid = gdp.localoid and
              pn.oid = pc.relnamespace and
              (not pc.relhassubclass) and
              pc.oid not in (select parchildrelid from pg_partition_rule) and
              pg_get_table_distributedby(pc.oid) %s '%%cdbhash%%'
        """ % predict
        r = db.query(sql).getresult()
        db.close()
        return r

    def get_root_partition_tables(self, is_legacy=True):
        db = self.get_db_conn()

        predict = 'like' if is_legacy else 'not like'
        sql = """
        select
          pn.nspname || '.' || pc.relname as relname,
          pg_get_table_distributedby(pc.oid) distby
        from pg_class pc,
             pg_namespace pn,
             gp_distribution_policy gdp
        where pc.oid = gdp.localoid and
              pn.oid = pc.relnamespace and
              pc.relhassubclass and
              pc.oid not in (select parchildrelid from pg_partition_rule) and
              pg_get_table_distributedby(pc.oid) %s '%%cdbhash%%'
        """ % predict
        r = db.query(sql).getresult()
        db.close()
        return r

    def remove_ops_ifany(self, distby):
        # DISTRIBUTED BY (a cdbhash_int4_ops, b cdbhash_int4_ops)
        t = self.pt.findall(distby)[0]
        cols =  ", ".join([s.strip().split()[0].strip()
                           for s in t.split(',')])
        return "distributed by (%s)" % cols

    def handle_one_table(self, name, distby):
        new_distby = self.remove_ops_ifany(distby)
        sql = """
        alter table %s set with (reorganize=true) %s;
        """ % (name, new_distby)
        return sql.strip()

    def dump_table_info(self, db, name, is_normal=True):
        if is_normal:
            sql = "select pg_relation_size('{name}'::regclass);"
            r = db.query(sql.format(name=name)).getresult()
            return "normal table, size %s" % r[0][0]
        else:
            sql_size = """
              with recursive cte(nlevel, table_oid) as (
                select 0, '{name}'::regclass::oid
                union all
                select nlevel+1, pi.inhrelid
                from cte, pg_inherits pi
                where cte.table_oid = pi.inhparent
               )
               select sum(pg_relation_size(table_oid))
               from cte where nlevel = (select max(nlevel) from cte);
            """
            r = db.query(sql_size.format(name=name))
            size = r.getresult()[0][0]
            sql_nleafs = """
              with recursive cte(nlevel, table_oid) as (
                select 0, '{name}'::regclass::oid
                union all
                select nlevel+1, pi.inhrelid
                from cte, pg_inherits pi
                where cte.table_oid = pi.inhparent
               )
               select count(1)
               from cte where nlevel = (select max(nlevel) from cte);
            """
            r = db.query(sql_nleafs.format(name=name))
            nleafs = r.getresult()[0][0]
            return "partition table, %s leafs, size %s" % (nleafs, size)

    def dump(self, fn):
        db = self.get_db_conn()
        f = open(fn, "w")
        # regular
        regular = self.get_regular_tables()
        for name, distby in regular:
            print>>f, "-- ", self.dump_table_info(db, name)
            print>>f, self.handle_one_table(name, distby)
            print>>f

        print >>f, "--------------------------------"

        parts = self.get_root_partition_tables()
        for name, distby in parts:
            print>>f, "-- ", self.dump_table_info(db, name, False)
            print>>f, self.handle_one_table(name, distby)
            print>>f

        f.close()


class ConcurrentRun(object):

    def __init__(self, dbname, port, host, user, script_file, nproc):
        self.dbname = dbname
        self.port = int(port)
        self.host = host
        self.user = user
        self.script_file = script_file
        self.nproc = nproc

    def get_db_conn(self):
        db = DB(dbname=self.dbname,
                port=self.port,
                host=self.host,
                user=self.user)
        return db

    def parse_inputfile(self):
        self.sqls = []
        with open(self.script_file) as f:
            for line in f:
                sql = line.strip()
                if (sql.startswith("alter table") and
                        sql.endswith(";") and
                        sql.count(";") == 1):
                    self.sqls.append(sql)

    def run(self):
        self.parse_inputfile()
        global procs
        procs = []
        for i in range(self.nproc):
            proc = Process(target=ConcurrentRun.alter,
                           args=[self.sqls, i, self.nproc,
                                 self.dbname, self.port, self.host, self.user])
            procs.append(proc)
        for proc in procs:
            proc.start()
        for proc in procs:
            proc.join()

    @staticmethod
    def alter(sqls, idx, nproc, dbname, port, host, user):
        import logging
        logging.basicConfig(level=logging.DEBUG, stream=sys.stdout,
                            format="%(asctime)s - %(levelname)s - %(message)s")
        logger = logging.getLogger()

        logger.info("worker[%d]: begin: " % idx)
        logger.info("worker[%d]: connect to <%s> ..." % (idx, dbname))
        db = DB(dbname=dbname,
                port=port,
                host=host,
                user=user)
        total_queries = len(sqls)
        start = time.time()
        for i, sql in enumerate(sqls):
            if (i % nproc) == idx:
                logger.info("worker[%d]: execute alter command \"%s\" ... " % (idx, sql))
                db.query(sql)
                end = time.time()
                total_time = end - start
                logger.info("Current worker progress: %d out of %d queries completed in %.3f seconds." % (i//nproc+1, total_queries//nproc, total_time))
        db.close()
        logger.info("worker[%d]: finish." % idx)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog='fix_distribution_policy')
    parser.add_argument('--host', type=str, help='Greenplum Database hostname')
    parser.add_argument('--port', type=int, help='Greenplum Database port')
    parser.add_argument('--dbname', type=str, help='Greenplum Database database name')
    parser.add_argument('--user', type=str, help='Greenplum Database user name')

    subparsers = parser.add_subparsers(help='sub-command help', dest='cmd')
    parser_gen = subparsers.add_parser('gen', help='generate alter table cmds')
    parser_run = subparsers.add_parser('run', help='run the alter table cmds')

    parser_gen.add_argument('--out', type=str, help='outfile path for the alter table commands')
    parser_run.add_argument('--nproc', type=int, default=1, help='the concurrent proces to run the alter table commands')
    parser_run.add_argument('--input', type=str, help='the file contains all alter table commands')

    args = parser.parse_args()

    if args.cmd == 'gen':
        cp = ChangePolicy(args.dbname, args.port, args.host, args.user)
        cp.dump(args.out)
    elif args.cmd == "run":
        signal.signal(signal.SIGTERM, sig_handler)
        signal.signal(signal.SIGINT, sig_handler)
        cr = ConcurrentRun(args.dbname, args.port, args.host, args.user,
                           args.input, args.nproc)
        cr.run()
    else:
        sys.stderr.write("unknown subcommand!")
        sys.exit(127)
