# coding=utf-8
from _datatools import get_files_list
import pandas as pd

# Parameters ###########################################################################################################
DATA_PATH = r'd:\Desktop\present_work\01_ammonia\02_prelim\03_Summer2018\01_footprint\South\day'
INIT = '18'
EXT = '.grd'
########################################################################################################################
grid_files = get_files_list(path=DATA_PATH, file_ext=EXT, file_init=INIT)
for grid_file in grid_files:
    grid_data = pd.read_csv(grid_file['path'], skiprows=[0, 1, 2, 3, 4], sep='\s+', header=None, index_col=False)
    fc_descend = grid_data.stack().dropna().sort_values(ascending=False).reset_index(drop=True)
    fcsum_max = fc_descend.sum()
    fcsum = 0
    i = 0
    fcsum_level = [.5, .7, .8, .9, 1]
    for fc in fc_descend:
        fcsum += fc
        if fcsum / fcsum_max > fcsum_level[i]:
            fcsum_level[i] = fc
            i += 1
        if i == 4:
            break
    lvl = open(grid_file + '.lvl', mode='w')
    level = f"""LVL3
'Level Flags LColor LStyle LWidth FVersion FFGColor FBGColor FPattern OffsetX OffsetY ScaleX ScaleY Angle Coverage
{fcsum_level[3]:8.6f} 0 "Blue" "Solid" 0 1 "R0 G255 B0 A38" "White" "Solid" 0 0 1 1 0 0
{fcsum_level[2]:8.6f} 0 "Green" "Solid" 0 1 "R255 G255 B0 A77" "White" "Solid" 0 0 1 1 0 0
{fcsum_level[1]:8.6f} 0 "Yellow" "Solid" 0 1 "R255 G255 B0 A115" "White" "Solid" 0 0 1 1 0 0
{fcsum_level[0]:8.6f} 0 "Orange" "Solid" 0 1 "R255 G0 B0 A153" "White" "Solid" 0 0 1 1 0 0
"""
    lvl.write(level)
    lvl.close()
