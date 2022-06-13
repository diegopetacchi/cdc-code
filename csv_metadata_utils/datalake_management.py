from .file_utils import read_csv, move_file, copy_file
from .utils import get_normal_path, get_working_path, mkdir, get_history_path, is_nan
import os


def move_to_working_dir(source, file_path, load_id, delimiter=',', remove_breaks=False, escape='\\',
                        dates_from_julian_to_gregorian=None):
    res = []
    file_path = get_normal_path(file_path)
    target_path = get_working_path(file_path, source, load_id, False)
    print(file_path, target_path)
    mkdir(target_path)
    mkdir(target_path + '_orig')
    for f in os.listdir(file_path):
        move_file(file_path + "/" + f, target_path + "_orig/" + f)
        if remove_breaks is False and is_nan(dates_from_julian_to_gregorian):
            copy_file(target_path + "_orig/" + f, target_path + "/" + f)
        else:
            df = read_csv(target_path + "_orig/" + f, delimiter, escape=escape, remove_breaks=remove_breaks,
                          dates_from_julian_to_gregorian=dates_from_julian_to_gregorian)
            df.to_csv(target_path + '/' + f, sep=delimiter, header=True, index=False,
                      escapechar=escape,
                      doublequote=False)
        res.append(file_path + "/" + f)
    return target_path


def move_to_history_dir(source, file_path, load_id, cdc=None):
    file_path_working = get_working_path(file_path, source, load_id, False)
    target_path = get_history_path(file_path, source, load_id, False)
    print("target_path: ", target_path)
    if os.path.isdir(file_path_working):
        mkdir(target_path)
        for f in os.listdir(file_path_working):
            if cdc:
                move_file(file_path_working + "/" + f, target_path + "/" + f)
            else:
                move_file(file_path_working + "_orig/" + f, target_path + "/" + f)
    return target_path
