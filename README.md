# PLOTS BEGONE

## Purpose
Allows you to plot to disks with old plots by deleting old plots only when its necessary to free up space for a new plot.

## Install
Install requirements using pip and then just run ./plots_begone.py --help to get the list of available parameters and what they do.

In short:

1. Scans all provided dirs
2. Gathers stats on the dirs, available and free space, number of old plots (specified by -c parameter), number of new plots
3. Shufles the dir list
4. Selects N number of drives to keep free (specified by -r parameter) and indexes them
5. Indexed drive free space is examined and if there is not enough to store a single new plot it will delete the oldest "old" plot
6. Watches all directories
7. If a new plot is detected on the indexed directory and if there is not enough space for the new plot it removes the oldest "old plot"
8. If dir has no old plots left it removes it from the indexed list and adds the next directory to it
9. If newly added directory does not have enough space for a plot it removes one plot from it before indexing
10. Plots created on non-indexed directories will trigger an event but no action will be taken
