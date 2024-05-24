#!/usr/bin/env python
from glob import glob
import json, sys, numpy as np, tarfile, os
from os import listdir, mkdir
from os.path import join, basename, exists
from argparse import ArgumentParser

from icecube import dataclasses, dataio
from icecube.dataclasses import I3Time, I3EventHeader
from icecube.dataio import I3File

usage = "usage: %prog [options]"
parser = ArgumentParser(usage)
parser.add_argument("--path", 
                    default="", required=True,
                    help="What run directory to run over")
parser.add_argument("--run", 
                    default="*", 
                    help=("If given, only look for files with this run in the name."
                          " Otherwise, take all of the files."))
parser.add_argument("--output_dir", 
                    default="./",
                    help="Name of the output directory for per-run grls.")
args = parser.parse_args()


# read the gaps files to find the start/end times for the good selections
def read_from_gaps(run, filenumbers, filelist=[], is_tar=True, contents=[], i3filelist=[]):
    start_times = []
    end_times = []
    for fnum in filenumbers:
        subrun_text = '{:08d}_gaps.txt'.format(fnum)
        gaps_file = [f for f in contents if subrun_text in f]

        if len(gaps_file) == 0:
            # ----------------
            # If this doesn't exist, we get to enjoy some pain.
            # ----------------
            print("\tMissing gaps file for run {} subrun {}".format(run, fnum))
            print("\tFalling back on L2 file")
            i3file = [f for f in i3filelist if '{:08d}.i3'.format(fnum) in f]
            i3file = dataio.I3File(i3file[0], 'r')
            time = np.inf
            while i3file.more():
                frame = i3file.pop_frame()
                if not 'I3EventHeader' in frame: continue
                header = frame['I3EventHeader']

            start_times.append(header.start_time.mod_julian_day_double)
            end_times.append(header.end_time.mod_julian_day_double)
        else:
            # ----------------
            # Otherwise, life is relatively easy. We just read the gaps file.
            # The format for these files is like this:
            #   Run: 115985
            #   First Event of File: 23007693 2010 131181339339644591
            #   Gap Detected: 1.10 23179753 131182116942247695 23182031 131182127922598317
            #   Last Event of File: 23182045 2010 131182128415663331
            #   File Livetime: 77.81            
            # ----------------
            if is_tar:
                lines = filelist.extractfile(gaps_file[0]).readlines()
            else:
                lines = open(gaps_file[0]).readlines()

            year = int(lines[1].split()[-2])
            # Get the start time of the file
            line = lines[1].split()
            start = int(line[-1]) # Note: in DAQ time units, so 0.1 ns
            i3time = I3Time(year, start)
            start_times.append(i3time.mod_julian_day_double)

            # check for gaps detected inside the file
            for line in lines:
                if 'Gap Detected:' not in line.decode(): continue
                pieces = line.decode().split()
                if len(pieces) < 6: continue

                start = int(pieces[6]) # Note: in DAQ time units, so 0.1 ns
                end = int(pieces[4])   # Note: in DAQ time units, so 0.1 ns
                start_times.append(I3Time(year, start).mod_julian_day_double)
                end_times.append(I3Time(year, end).mod_julian_day_double)

            # Get the end time of the file
            line = lines[-2].split()
            t = int(line[-1]) # Note: in DAQ time units, so 0.1 ns
            i3time = I3Time(year, t)
            end_times.append(i3time.mod_julian_day_double)

    return np.array(start_times), np.array(end_times)


# ---------------------------------
# Get the list of files in the path
# ---------------------------------
if 'level2' not in args.path.lower():
    print("ERROR: This script only makes sense when run over L2 files!")
    raise RuntimeError

# Is this a directory?
if os.path.isdir(args.path):
    i3files = sorted(glob(join(args.path, "*.i3*")))
else:
    i3files = sorted(glob(args.path))

# Rip out any interlopers...
i3files = [_ for _ in i3files if 'GCD' not in _]

# ---------------------------------
# Try to detect if this is a single run
# ---------------------------------
runs = {f[f.index("Run")+3:][:8] for f in i3files}
if len(runs) > 1:
    print(f"More than one run identified in {args.path}!"
          " If you want to process this folder, you need to"
          " pass the path in as --path \"/path/to/stuff/*Run0012411*\"")
    raise RuntimeError

args.run = int(list(runs)[0])

# ---------------------------------
# Figure out which season this is.
# We'll use this in the output name later.
# ---------------------------------
season = os.path.basename(i3files[0]).split("_")[1]
print(season)

# ---------------------------------
# And get the subruns
# ---------------------------------
subruns = []
for f in i3files:
    if 'subrun' in f.lower():
        subrun = int(basename(f).split("_")[-1].replace("Subrun","")[:-7])
    else:
        subrun = int(basename(f).split("_")[-1].replace("Part","")[:-7])
    subruns.append(subrun)    
        
# if there's no subruns... I dunno, skip?
if len(subruns) == 0: 
    print("\t\tRun {} has no subruns?".format(run))
    raise RuntimeError

# And check for any obvious missing files
subruns = sorted(subruns)
missing_subruns = [i for i in range(max(subruns)+1) if i not in subruns]

print("\tPath {} appears to be missing {}/{} files".format(args.path, len(missing_subruns), max(subruns)+1))
print("\t\tMissing subruns:", missing_subruns)

# ---------------------------------
# Find the gaps files for the start/stops
# ---------------------------------
year = int(args.path.split("/")[4])
if (year < 2018):
    tar_bases = ["/data/exp/IceCube/{year}/filtered/level2pass2/*/Run{run:08d}_GapsTxt.tar",
                 "/data/exp/IceCube/{year}/filtered/level2pass2/*/Run{run:08d}/Run{run:08d}_GapsTxt.tar"]
    txt_base = "/data/exp/IceCube/{year}/filtered/level2pass2/*/Run{run:08d}/*_gaps.txt"
else:
    tar_bases = ["/data/exp/IceCube/{year}/filtered/level2/*/Run{run:08d}_GapsTxt.tar",
                 "/data/exp/IceCube/{year}/filtered/level2/*/Run{run:08d}/Run{run:08d}_GapsTxt.tar"]
    txt_base = "/data/exp/IceCube/{year}/filtered/level2/*/Run{run:08d}/*_gaps.txt"

gaps_tarfile = (glob(tar_bases[0].format(year=year, run=args.run)) +\
                glob(tar_bases[1].format(year=year, run=args.run)))
gaps_txtfiles = sorted(glob(txt_base.format(year=year, run=args.run)))

# ---------------------------------
# Poke at the tarfiles to see if they will work
# ---------------------------------
if not len(gaps_tarfile) > 0:
    gaps_files = gaps_txtfiles
    tar_contents = gaps_files
    is_tar = False
else:
    try:
        gaps_files = tarfile.open(gaps_tarfile[0])
        tar_contents = gaps_files.getnames()
        is_tar = True
    except:
        print("\tTar file {} is broken?".format(gaps_tarfile[0]))
        print("Falling back on gaps files...")
        gaps_files = gaps_txtfiles
        tar_contents = gaps_files
        is_tar = False

# ---------------------------------
# Read the results
# ---------------------------------
start_times, end_times = read_from_gaps(args.run, subruns, gaps_files, is_tar, tar_contents, i3files)

# ---------------------------------
# remove gaps shorter than 1 seconds
# ---------------------------------
diffs = start_times[1:] - end_times[:-1]
start_times = np.append(start_times[0], start_times[1:][diffs >= 1/86400.])
end_times = np.append(end_times[:-1][diffs >= 1/86400.], end_times[-1])

# ---------------------------------
# Warn if there's splitting and write it out
# ---------------------------------
if (len(end_times) > 1):
    print("\tFound gaps in run {}. Run broken into {} pieces.".format(runs, len(end_times)))

dtype = [('run', float), ('start', np.float64), ('stop', np.float64), ('livetime', np.float64), ('events', np.int32)]
output = np.zeros(len(start_times), dtype=dtype)
output['run'] = args.run
output['start'] = start_times
output['stop'] = end_times
output['livetime'] = output['stop'] - output['start']
output['events'] = -1

output_dir = join(args.output_dir, season)
if not os.path.exists(output_dir):
    os.mkdir(output_dir)
output_name = join(args.output_dir, season, f"NuSources_GRL_{args.run}.npy")
np.save(output_name, output)
