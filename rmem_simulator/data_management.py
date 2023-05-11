import os
from datetime import date, datetime
import json
import git
import gzip

DATA_DIR="data"
STATISTICS_FILE="statistics.json.gz"
INFO_FILE="info.md"

def check_and_create_data_dir(dirname=""):

    if dirname == "":
        dirname = DATA_DIR
    """Check if the data directory exists, and if not, create it."""
    if not os.path.exists(dirname):
        os.makedirs(dirname)

    #verify that the directory exists
    if not os.path.exists(dirname):
        raise Exception("Could not create the data directory.")

    print("Data directory exists.")

def create_info_file(directory):
    repo = git.Repo(search_parent_directories=True)
    sha = repo.head.object.hexsha
    timestamp = datetime.now()
    info_filename = os.path.join(directory, INFO_FILE)
    with open(info_filename, "w") as f:
        f.write("# Git commit: {}\n".format(sha))
        f.write("# Timestamp: {}\n".format(timestamp))

def current_experiment_dir():
    today = date.today()
    d1 = today.strftime("%d-%m-%Y")
    data_files = os.listdir(DATA_DIR)
    todays_files = [f for f in data_files if f.startswith(d1)]
    suffix = "{:03d}".format(len(todays_files))
    current_directory = os.path.join(DATA_DIR, d1 + "_" + suffix)
    return current_directory

def create_experiment_dir(dirname=""):
    print("TODO make a function that archives all the directories older than 1 day")
    check_and_create_data_dir()
    """Create a new directory for the current experiment."""
    current_directory = current_experiment_dir()
    os.makedirs(current_directory)
    return current_directory

def make_dir_latest(directory):
    """Make the given directory the latest directory."""
    latest_dir = os.path.join(DATA_DIR, "latest")
    if os.path.exists(latest_dir):
        os.remove(latest_dir)
    print("Creating a symbolic link to the latest directory.")
    directory = os.path.abspath(directory)
    latest_dir = os.path.abspath(latest_dir)
    print(directory, latest_dir)
    os.symlink(directory, latest_dir)

def save_statistics(statistics, dirname=""):
    # print(statistics)
    stats = json.dumps(statistics, indent=4)
    if dirname == "":
        exp_dir = create_experiment_dir()
    else:
        os.makedirs(dirname, exist_ok=True)
        exp_dir=dirname
    create_info_file(exp_dir)
    stat_file_name = os.path.join(exp_dir, STATISTICS_FILE)
    with gzip.open(stat_file_name, "w") as f:
        f.write(stats.encode('utf-8'))
        f.close()
    if dirname == "":
        make_dir_latest(exp_dir)

def load_statistics(dirname=""):
    if dirname == "":
        dirname = "latest"
        filename = os.path.join(DATA_DIR, dirname, STATISTICS_FILE)
        dirname = os.path.join(DATA_DIR, dirname)
    else:
        filename = os.path.join(dirname, STATISTICS_FILE)

    with gzip.open(filename, "r") as f:
        stats = json.load(f)
    return stats, dirname


