fix_distribution_policy
======================

**Find the tables with distribution keys opclass from legacy, and update them to the default one.**

1. use `fix_distribution_policy.py gen` to generate alter table commands.
2. use `fix_distribution_policy.py run` to run the alter table commands.


## Usage

```bash
gpadmin@gpdev:~/z/fix_distribution_policy$ python2 fix_distribution_policy.py -h
usage: fix_distribution_policy [-h] [--host HOST] [--port PORT]
                               [--dbname DBNAME] [--user USER]
                               {gen,run} ...

positional arguments:
  {gen,run}        sub-command help
    gen            generate alter table cmds
    run            run the alter table cmds

optional arguments:
  -h, --help       show this help message and exit
  --host HOST      Greenplum Database hostname
  --port PORT      Greenplum Database port
  --dbname DBNAME  Greenplum Database database name
  --user USER      Greenplum Database user name

gpadmin@gpdev:~/z/fix_distribution_policy$ python2 fix_distribution_policy.py gen -h
usage: fix_distribution_policy gen [-h] [--out OUT] [--dump_legacy_ops]

optional arguments:
  -h, --help         show this help message and exit
  --out OUT          outfile path for the alter table commands
  --dump_legacy_ops  dump all tables with legacy distkey ops

gpadmin@gpdev:~/z/fix_distribution_policy$ python2 fix_distribution_policy.py run -h
usage: fix_distribution_policy run [-h] [--nproc NPROC] [--input INPUT]

optional arguments:
  -h, --help     show this help message and exit
  --nproc NPROC  the concurrent proces to run the alter table commands
  --input INPUT  the file contains all alter table commands
```


## Example

First run `test.sql` to generate some data.

```
gpadmin@gpdev:~/z/fix_distribution_policy$ python2 fix_distribution_policy.py --host localhost --port 6000 --dbname gpadmin  --user gpadmin  gen --out ./out --dump_legacy_ops
gpadmin@gpdev:~/z/fix_distribution_policy$ cat out
-- dump legacy ops is True
--  normal table, size 0
alter table public.t_old set with (reorganize=true) distributed by (a, b);

--  normal table, size 0
alter table public.t1_old set with (reorganize=true) distributed by (a, b);

--------------------------------
--  partition table, 11 leafs, size 5799936
alter table public.rank_old set with (reorganize=true) distributed by (id);

gpadmin@gpdev:~/z/fix_distribution_policy$ python2 fix_distribution_policy.py --host localhost --port 6000 --dbname gpadmin  --user gpadmin  gen --out ./out
gpadmin@gpdev:~/z/fix_distribution_policy$ cat out
-- dump legacy ops is False
--  normal table, size 0
alter table public.t_new set with (reorganize=true) distributed by (a, b);

--  normal table, size 0
alter table public.t1_new set with (reorganize=true) distributed by (a, b);

--------------------------------
--  partition table, 11 leafs, size 5734400
alter table public.rank_new set with (reorganize=true) distributed by (id);

gpadmin@gpdev:~/z/fix_distribution_policy$ python2 fix_distribution_policy.py --host localhost --port 6000 --dbname gpadmin  --user gpadmin  run --nproc 2 --input ./out
2023-07-19 14:07:04,136 - INFO - worker[0]: begin:
2023-07-19 14:07:04,136 - INFO - worker[0]: connect to <gpadmin> ...
2023-07-19 14:07:04,136 - INFO - worker[1]: begin:
2023-07-19 14:07:04,137 - INFO - worker[1]: connect to <gpadmin> ...
2023-07-19 14:07:04,142 - INFO - worker[0]: execute alter command "alter table public.t_new set with (reorganize=true) distributed by (a, b);" ...
2023-07-19 14:07:04,143 - INFO - worker[1]: execute alter command "alter table public.t1_new set with (reorganize=true) distributed by (a, b);" ...
2023-07-19 14:07:04,172 - INFO - Current worker progress: 1 out of 1 queries completed in 0.029 seconds.
2023-07-19 14:07:04,172 - INFO - worker[0]: execute alter command "alter table public.rank_new set with (reorganize=true) distributed by (id);" ...
2023-07-19 14:07:04,172 - INFO - Current worker progress: 1 out of 1 queries completed in 0.029 seconds.
2023-07-19 14:07:04,172 - INFO - worker[1]: finish.
2023-07-19 14:07:04,284 - INFO - Current worker progress: 2 out of 1 queries completed in 0.142 seconds.
2023-07-19 14:07:04,284 - INFO - worker[0]: finish.
```