import os
import glob
import subprocess


def subset_grib2(indir):
    files = glob.glob(f'{indir}/graphcastgfs.*')
    files.sort()

    outdir = os.path.join(indir, 'north_america')
    os.makedirs(outdir, exist_ok=True)

    lonMin, lonMax, latMin, latMax = 260, 300, 5, 60
    for grbfile in files:
        outfile = f"{outdir}/{grbfile.split('/')[-1]}"
        command = ['wgrib2', grbfile, '-small_grib', f'{lonMin}:{lonMax}', f'{latMin}:{latMax}', outfile]
        subprocess.run(command, check=True)
