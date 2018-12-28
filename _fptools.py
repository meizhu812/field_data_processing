import os
from accessories import Timer
from pandas import DataFrame, read_csv


def grid_file_grouping(grid_files: list, key_name: str, key_loc: slice, grid_groups: dict):
    timer_group = Timer()
    timer_group.start('Grouping...', 'Grouping')
    for grid_file in grid_files:
        grid_file[key_name] = grid_file['path'].split('\\')[-1][key_loc]  # TODO
        for grid_group in grid_groups:
            if not grid_file[key_name] in grid_group:
                grid_groups[grid_file[key_name]] = []  # create new group
            grid_groups[grid_file[key_name]].append(grid_file['path'])
    timer_group.stop()
    return grid_groups


def grid_average(grid_groups, output_dir: str):
    timer_average = Timer()
    timer_average.start('Averaging groups...', 'Averaging')
    for grid_group in grid_groups:
        average_grid = DataFrame
        i = 0
        for grid_file in grid_groups[grid_group]:
            if i == 0:
                average_grid = read_csv(grid_file, skiprows=[0, 1, 2, 3, 4], sep='\s+', header=None, index_col=False)
            else:
                average_grid += read_csv(grid_file, skiprows=[0, 1, 2, 3, 4], sep='\s+', header=None,
                                         index_col=False)
            i += 1
        average_grid /= i
        os.makedirs(output_dir, exist_ok=True)
        out_path = output_dir + '\\' + grid_group + '.grd'
        average_grid.to_csv(out_path, sep=' ', header=False, index=False)
        with open(out_path, 'r+') as f:
            content = f.read()
            f.seek(0, 0)
            f.write('DSAA\n150 150\n0 0.7500001\n0 0.7500001\n0 1.000000\n' + content)
    timer_average.stop()
