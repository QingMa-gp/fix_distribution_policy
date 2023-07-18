# Overview
1. use `fix_distribution_policy.py gen`to generate alter table commands.
2. use `fix_distribution_policy.py run`to run the alter table commands.
---
# Usage
```bash
 gpadmin@centos {8:00}~/workspace/fix_distribution_policy:main ✗ ➭ python2 fix_distribution_policy.py -h    
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
gpadmin@centos {8:01}~/workspace/fix_distribution_policy:main ✗ ➭ python2 fix_distribution_policy.py gen -h
usage: fix_distribution_policy gen [-h] [--out OUT]

optional arguments:
  -h, --help  show this help message and exit
  --out OUT   outfile path for the alter table commands
gpadmin@centos {8:01}~/workspace/fix_distribution_policy:main ✗ ➭ python2 fix_distribution_policy.py run -h
usage: fix_distribution_policy run [-h] [--nproc NPROC] [--input INPUT]

optional arguments:
  -h, --help     show this help message and exit
  --nproc NPROC  the concurrent proces to run the alter table commands
  --input INPUT  the file contains all alter table commands

```
---
# Example
```bash
gpadmin@centos {7:48}~/workspace/fix_distribution_policy:main ✗ ➭ python2 fix_distribution_policy.py --host localhost --port 6000 --dbname postgres --user gpadmin gen --out ./alter_cmds.out

gpadmin@centos {7:58}~/workspace/fix_distribution_policy:main ✗ ➭ cat alter_cmds.out 
--------------------------------
--  partition table, 11 leafs, size 6029312
alter table public.bar set with (reorganize=true) distributed by (id);

--  partition table, 11 leafs, size 6029312
alter table public.rank set with (reorganize=true) distributed by (id);

--  partition table, 11 leafs, size 6094848
alter table public.foo set with (reorganize=true) distributed by (id);

gpadmin@centos {7:59}~/workspace/fix_distribution_policy:main ✗ ➭ python2 fix_distribution_policy.py --host localhost --port 6000 --dbname postgres --user gpadmin run --nproc 2 --input ./alter_cmds.out 
2023-07-18 07:59:32,818 - INFO - worker[0]: begin: 
2023-07-18 07:59:32,818 - INFO - worker[0]: connect to <postgres> ...
2023-07-18 07:59:32,818 - INFO - worker[1]: begin: 
2023-07-18 07:59:32,818 - INFO - worker[1]: connect to <postgres> ...
2023-07-18 07:59:32,826 - INFO - worker[0]: execute alter command "alter table public.bar set with (reorganize=true) distributed by (id);" ... 
2023-07-18 07:59:32,826 - INFO - worker[1]: execute alter command "alter table public.rank set with (reorganize=true) distributed by (id);" ... 
2023-07-18 07:59:33,078 - INFO - worker[0]: execute alter command "alter table public.foo set with (reorganize=true) distributed by (id);" ... 
2023-07-18 07:59:33,080 - INFO - worker[1]: finish.
2023-07-18 07:59:33,281 - INFO - worker[0]: finish.
```
