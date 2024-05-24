#!/usr/bin/env python3
import os
from glob import glob
from tqdm import tqdm

dag_name = "make_sources_grl.dag"
icetray = "/cvmfs/icecube.opensciencegrid.org/py3-v4.3.0/icetray-env icetray/v1.10.0"
scriptdir = os.path.expandvars("$PWD")
outdir = os.path.join(os.path.expandvars("$PWD"), "output")

paths = ["/data/exp/IceCube/2010/filtered/level2pass2a/*/Run00??????/",
         "/data/exp/IceCube/2011/filtered/level2pass2a/*/Run00??????/",
         "/data/exp/IceCube/2012/filtered/level2pass2a/*/Run00??????/",
         "/data/exp/IceCube/2013/filtered/level2pass2a/*/Run00??????/",
         "/data/exp/IceCube/2014/filtered/level2pass2a/*/Run00??????/",
         "/data/exp/IceCube/2015/filtered/level2pass2a/*/Run00??????/",
         "/data/exp/IceCube/2016/filtered/level2pass2a/*/Run00??????/",
         "/data/exp/IceCube/2017/filtered/level2pass2a/*/Run00??????/",

         "/data/exp/IceCube/2017/filtered/level2/*/Run00??????/",
         "/data/exp/IceCube/2018/filtered/level2/*/Run00??????/",
         "/data/exp/IceCube/2019/filtered/level2/*/Run00??????/",
         "/data/exp/IceCube/2020/filtered/level2/*/Run00??????/",
         "/data/exp/IceCube/2021/filtered/level2/*/Run00??????/",
         "/data/exp/IceCube/2022/filtered/level2/*/Run00??????/",
         "/data/exp/IceCube/2023/filtered/level2/*/Run00??????/"]


dag_contents = ""
for path in paths:
    rundirs = glob(path)
    print(path, len(rundirs))

    for rundir in rundirs:
        run = rundir.split("/")[-1]
        
        dag_contents += f"JOB {run} submit.sub\n"
        dag_contents += f"VARS {run} cmd=\""
        dag_contents += (f"{icetray} python3 {scriptdir}/build_gaps_grl.py "
                         f" --path {rundir} "
                         f" --output_dir {outdir} "
                         "\"\n")
open(dag_name, "w").write(dag_contents)
    
