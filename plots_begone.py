#!/usr/bin/env python3

import asyncio
import argparse
import os
import pathlib
import datetime
import sys
import shutil
import platform
import random
import aionotify
import traceback

parser = argparse.ArgumentParser(
    description='A simple script that allows you to maintain old plots while re-plotting and keeping a certain number of '
                'disks available to be written to.'
)
parser.add_argument(
    '-d',
    '--plot-directories',
    required=True,
    nargs='+',
    type=pathlib.Path,
    help='Specify plot directories that you would want to watch, it can be defined multiple times like: -d /mnt/dir1 '
         '/mnt/dir2 OR it can be defined like this: -d /mnt/plot_directories/*. Directories specified must be mounted.',
)
parser.add_argument(
    '-e',
    '--plot-extension',
    default='.plot',
    help='Specify plot extension. Use like -e .plot or -e .some_other_format, defaults to .plot',
)
parser.add_argument(
    '-c',
    '--plot-cutoff-date',
    required=True,
    type=datetime.datetime.fromisoformat,
    help='Specify date in format "YYYY-MM-DD" before which all plots will be considered "OLD PLOTS" based on plot '
         'creation date. Use like -c 2023-05-05 or -c 2023-06-01',
)
parser.add_argument(
    '-r',
    '--required-drives',
    required=True,
    type=int,
    help='How many drives do you require to be available for writing. Use like -r 15 or -r 5',
)
parser.add_argument(
    '-s',
    '--new-plot-size',
    required=True,
    type=float,
    help='New plot size in GiB. Use like -s 55 or -s 55.56',
)

args = parser.parse_args()

def init():
    directories = parse_directories(args.plot_directories)
    directories_with_stats = get_directory_stats(directories)
    random.shuffle(directories_with_stats)
    chosen_directories = directories_with_stats[0:args.required_drives]

    del directories_with_stats[0:args.required_drives]

    for directory in chosen_directories:
        if not directory_has_enough_space(directory):
            print('Removing: ' + os.fspath(directory['plots']['old_plots'].pop(0)['path']))
            os.remove(directory['plots']['old_plots'].pop(0)['path'])

    return [directories, directories_with_stats, chosen_directories]


def directory_has_enough_space(directory: dict) -> bool:
    return directory['available_space'] >= args.new_plot_size * pow(1024, 3)


def parse_directories(plot_directories: list[pathlib.Path]) -> list[pathlib.Path]:
    directories: list[pathlib.Path] = []

    for plot_directory in plot_directories:
        parts = plot_directory.parts

        if parts[-1] != '*':
            if not valid_directory(plot_directory):
                print(f"Path does not exist, its not a directory or its not mounted: {os.fspath(plot_directory)}", file=sys.stderr)
                continue

            directories.append(plot_directory)

            continue

        parent = plot_directory.parent

        if not valid_directory(parent, False):
            print(f"Path does not exist, its not a directory: {os.fspath(parent)}", file=sys.stderr)

            continue

        all_items = sorted(parent.glob('*'))

        for item in all_items:
            if not valid_directory(item):
                continue

            directories.append(item)

    if len(directories) == 0:
        print(f"There are no valid directories to watch, exiting.", sys.stderr)
        exit(1)

    return directories


def valid_directory(directory: pathlib.Path, check_mount: bool = True) -> bool:
    if not check_mount:
        return directory.exists() and directory.is_dir()

    return directory.exists() and directory.is_dir() and directory.is_mount()


def get_directory_stats(directories: list[pathlib.Path]) -> list[dict]:
    all_stats = []

    for directory in directories:
        directory_stats = {
            'available_space': shutil.disk_usage(directory).free,
            'total_space': shutil.disk_usage(directory).total,
            'plots': classify_plots(list(directory.glob('*' + args.plot_extension))),
            'path': directory
        }

        if len(directory_stats['plots']['old_plots']) == 0:
            continue

        all_stats.append(directory_stats)

    return all_stats


def classify_plots(paths: list[pathlib.Path]) -> dict[str, list[list[dict]]]:
    plots = {
        'old_plots': [],
        'new_plots': []
    }

    paths = sorted(paths, key=lambda p: creation_date(p))

    for path in paths:
        timestamp = creation_date(path)
        date_created = datetime.datetime.fromtimestamp(timestamp)

        if date_created < args.plot_cutoff_date:
            plots['old_plots'].append({
                'path': path,
                'date_created': date_created,
                'timestamp': timestamp

            })
        else:
            plots['new_plots'].append({
                'path': path,
                'date_created': date_created,
                'timestamp': timestamp
            })

    return plots


def creation_date(path: pathlib.Path) -> float:
    if platform.system() == 'Windows':
        return os.path.getctime(path)
    else:
        stat = os.stat(path)
        try:
            return stat.st_birthtime
        except AttributeError:
            # We're probably on Linux. No easy way to get creation dates here,
            # so we'll settle for when its content was last modified.
            return stat.st_mtime


async def watch_plots(directories, plot_event, loop):
    watcher = aionotify.Watcher()

    for directory in directories:
        if not directory.exists():
            print('Path does not exist: ' + os.fspath(directory))

            continue

        print('Watching: ', os.fspath(directory))

        watcher.watch(
            alias=os.fspath(directory),
            path=os.fspath(directory),
            flags=aionotify.Flags.MOVED_TO,
        )

    await watcher.setup(loop)

    print('Keeping watch ...')

    while True:
        event = await watcher.get_event()

        if event.name.endswith(".plot"):
            print('New plot: ' + os.path.join(pathlib.Path(event.alias), event.name))
            await plot_event.put(event.alias)


async def keep_free(directories, chosen_directories, plot_event, loop):
    print('Keeping free ...')

    indexed_directories = {}

    for directory in chosen_directories:
        indexed_directories[os.fspath(directory['path'])] = directory

    while True:
        try:
            plot_dir = await plot_event.get()

            if plot_dir not in indexed_directories:
                print('Plot dir not indexed skipping.')
                continue

            indexed_directories[plot_dir]['available_space'] = shutil.disk_usage(indexed_directories[plot_dir]['path']).free

            if directory_has_enough_space(indexed_directories[plot_dir]):
                print('Plot dir has enough space')
                continue

            if len(indexed_directories[plot_dir]['plots']['old_plots']) == 0:
                print('Plot dir does not have old plots ... Replacing ...')
                del indexed_directories[plot_dir]

                replacement_dir = directories.pop(0)

                print('Replaced with dir: ' + os.fspath(replacement_dir['path']))

                replacement_dir['available_space'] = shutil.disk_usage(replacement_dir['path']).free

                if not directory_has_enough_space(replacement_dir):
                    plot_path = replacement_dir['plots']['old_plots'].pop(0)['path']

                    print("Removing: " + os.fspath(plot_path))
                    os.remove(plot_path)

                    replacement_dir['available_space'] = shutil.disk_usage(replacement_dir['path']).free

                indexed_directories[os.fspath(replacement_dir['path'])] = replacement_dir

                continue

            plot_path = indexed_directories[plot_dir]['plots']['old_plots'].pop(0)['path'];

            print("Removing: " + os.fspath(plot_path))
            os.remove(plot_path)

            indexed_directories[plot_dir]['available_space'] = shutil.disk_usage(indexed_directories[plot_dir]['path']).free

        except Exception as e:
            traceback.print_exc()


async def main(loop):
    print('Running ...')

    [directories, directories_with_stats, chosen_directories] = init()

    plot_events = asyncio.Queue()
    futures = [
        watch_plots(directories, plot_events, loop),
        keep_free(directories_with_stats, chosen_directories, plot_events, loop)
    ]

    await asyncio.gather(*futures)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()

    try:
        loop.run_until_complete(main(loop))
    except KeyboardInterrupt:
        pass
