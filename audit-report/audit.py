# audit-report.py
#
# The purpose of this script is to generate a csv of meta data for all jobs in a Domino project 
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, version 3.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

import os
import requests
import json
import csv
import pandas as pd
import datetime
import logging
import sys
import configparser

from joblib import Parallel
from joblib import delayed

from api import DominoAPISession

def read_config(filename):
    """Reads a configuration file, which set parameters for the generated report. The configuration
    file includes the following sections:

      * [column_order] - specifies how the columns should be ordered in the report
      * [columns_to_expand] - what fields from the JSON should be expanded (flattened) into separate columns
      * [columns_to_datetime] - which columns should be reformatted from POSIX timestamps to appropriate datetime format

    Parameters
    ----------
    filename : str    
        Absolute file name of the config file. 
        Each section of the config file should include only column names. For example:

        ```
        [columns_to_expand]
        stageTime
        startedBy

        [columns_to_datetime]
        stageTime-submissionTime
        stageTime-runStartTime
        stageTime-completedTime
        ```
    """
    config = configparser.ConfigParser(allow_no_value=True)
    
    if os.path.isfile(filename):
        config.read(filename)
        logging.info("Successfully read sections {} from the config file.".format(config.sections()))
    else:
        logging.warning("Specified configuration file {} doesn't exist. Will use default values.".format(filename))

    return config


def get_column_order(config):
    """Creates a list, specifying the order of columns in the report.
    The order is taken from the config file. If no config file has been set or the config file
    doesn't contain a [column_order] section, a default ordering is used instead.

    Parameters
    ----------
    config : configparser.ConfigParser
        ConfigParser object, containing the configuration from the config file
    
    See also
    --------
    read_config(filename) - reads the configuration file and provides the input for this function
    """

    if config.has_section("column_order"):
        column_order = config["column_order"]
    else:
        logging.info("No column order provided via config file. Will use default values.")
        column_order = column_order = ["projectName", "number", "title", "startedBy-username", 
                                       "jobRunCommand", "statuses-executionStatus", "stageTime-submissionTime", 
                                       "stageTime-runStartTime", "stageTime-completedTime", "hardwareTier", 
                                       "environment-environmentName", "environment-revisionNumber", "changes", 
                                       "tags", "commitDetails-inputCommitId", "commitDetails-outputCommitId", 
                                       "statuses-isCompleted", "statuses-isArchived", "statuses-isScheduled", 
                                       "goals", "comments", "dominoStats", "mainRepoGitRef", "dependentRepositories", 
                                       "dependentDatasetMounts", "dependentProjects", "dependentExternalVolumeMounts", 
                                       "startState-importedProjectArtifacts", "environment-environmentRevisionId", 
                                       "endState-commitId", "startedBy-id", "hardwareTierId", "id"]

    return column_order


def get_columns_to_expand(config):
    """Gets the list of columns that need to be expanded (flattened from JSON elements). This information is taken from 
    the configuration file or, if no config file has been specified, a pre-defined set of values is returned.

    Parameters
    ----------
    config : configparser.ConfigParser
        ConfigParser object, containing the configuration from the config file
    
    See also
    --------
    read_config(filename) - reads the configuration file and provides the input for this function
    """
    if config.has_section("columns_to_expand"):
        columns_to_expand = config["columns_to_expand"]
    else:
        columns_to_expand = ["stageTime", "startedBy", "commitDetails", "statuses", "environment", "startState", "endState"]

    return columns_to_expand


def get_columns_to_datetime(config):
    """Gets the list of columns that need to be converted to datetime string. This information is taken from 
    the configuration file or, if no config file has been specified, a pre-defined set of values is returned.

    Parameters
    ----------
    config : configparser.ConfigParser
        ConfigParser object, containing the configuration from the config file
    
    See also
    --------
    read_config(filename) - reads the configuration file and provides the input for this function
    convert_datetime(time_str) - converts a POSIX timestamp to locale appropriate time representation
    """
    if config.has_section("columns_to_datetime"):
        columns_to_datetime = config["columns_to_datetime"]
    else:
        columns_to_datetime = ["stageTime-submissionTime", "stageTime-runStartTime", "stageTime-completedTime"]

    return columns_to_datetime


def get_jobs():
    """Pulls all executed job ids from the current project. This function returns a list:str with all job
    ids returned by the Domino API run_lists() call.
    """
    api = DominoAPISession.instance()
    ids = []

    for run_info in api.runs_list()["data"]:
        job_id = run_info["id"]
        if job_id:
            ids.append(job_id)
      
    return ids


def get_goals():
    """Gets all project-associated goals and returns them as a list:str object.
    """
    api = DominoAPISession.instance()
    url = api._routes.host + "/v4/projectManagement/" + api.project_id + "/goals"
    result = api.request_manager.get(url).json()

    goals = {}
    for goal in result:
         goals[goal["id"]] = goal["title"]

    return goals


def get_job_data(job_id):
    """Compiles a job_details object for a specific job. It includes:
    
    * Runtime execution details
    * Comments associated with the job run
    * Artifacts produced
    * General metadata like run command, stage time, job title etc.

    Parameters
    ----------
    job_id:str
        job id to fetch all the details for

    See also
    --------
    get_jobs() - returns a list:str with all job ids in the current project
    """
    api = DominoAPISession.instance()

    endpoints = [f"/v4/jobs/{job_id}",
                 f"/v4/jobs/{job_id}/runtimeExecutionDetails",
                 f"/v4/jobs/{job_id}/comments",
                 f"/v4/jobs/job/{job_id}/artifactsInfo"]
    
    job_details = {}

    for endpoint in endpoints:
        url = api._routes.host + endpoint
        result = api.request_manager.get(url).json()
        if result is not None:
            job_details.update(result)
            
    return job_details


def aggregate_job_data(job_ids, parallelize=True):
    """Collects job details for all jobs attached to the project.
    This function receives a list of all job ids and then pulls the details for each individual id by 
    calling get_job_data(job_id).

    If the parallelize argument is set to True, the function will try to execute the details
    collection in parallel by sending parallel Domino API calls to the Domino instance. This can
    substantially reduce the job details collection time.

    Parameters
    ----------
    job_ids : list:str
        a list of job run ids
    parallelize : boolean
        wether to parallelize the collection of job data or run it sequentially

    See also
    --------
    get_jobs() - returns a list:str with all job ids in the current project
    get_job_data(job_id) - compiles a job_details object with all the relevant metadata for a specific job
    """
    api = DominoAPISession.instance()

    jobs = {}

    if parallelize:
        logging.info("Using {:d} CPU cores...".format(os.cpu_count()))
        result = Parallel(n_jobs=os.cpu_count())(delayed(get_job_data)(job_id) for job_id in job_ids)
        
        for job in result:
            jobs[job["id"]] = job

    else:
        for job_id in job_ids:
            job = get_job_data(job_id)
            jobs[job["id"]] = job

    return jobs

def expand(field, job_id, jobs):
    """Expands a job (JSON) field.

    Parameters
    ----------
    field  : str
        field to expand (i.e. report column name)
    job_id : str
        ID of the specific job, which field/column is being expanded
    jobs   : dict
        collection of all jobs. The expected format matches the output of aggregate_job_data(), more specifically
        job IDs are used as dict keys and all accompanying details are stored as values in JSON format
    
    """
    for sub_field in jobs[job_id][field]:
        jobs[job_id]["{}-{}".format(field, sub_field)] = jobs[job_id][field].get(sub_field)

    jobs[job_id].pop(field)

def convert_datetime(time_str):
    """Converts a POSIX timestamp to locale appropriate time representation.

    Parameters
    ----------
    time_str : str
        POSIX timestamp
    
    """
    return datetime.datetime.fromtimestamp(time_str / 1e3, tz=datetime.timezone.utc).strftime("%F %X:%f %Z")

def clean_comments(job):
    """Cleans up a comment section.
    This function removes unneeded entries like commentId,fullName etc. and streamlines the structure of the comment section
    by grouping all data into three keys: comment-username, comment-timestamp, and comment-value.

    Parameters
    ----------
    job : dict
        The expected format matches the output of aggregate_job_data(), more specifically job IDs are used as dict keys
        and all accompanying details are stored as values in JSON format

    See Also
    --------
    aggregate_job_data : the function that collects details for each individual job, and which output should be piped to
    clean_comments.
    """

    comments = []
    for c in job["comments"]:

        comment = {
            "comment-username": c["commenter"]["username"],
            "comment-timestamp": convert_datetime(c["created"]),
            "comment-value": c["commentBody"]["value"]
        }

        comments.append(comment)
    
    return comments

def merge_goals(job_id, jobs, goals):
    """Appends any job-associated goals to a job entry.

    Parameters
    ----------
    job_id : str
        ID of the specific job, which field/column is being expanded
    jobs   : dict
        collection of all jobs. The expected format matches the output of aggregate_job_data(), more specifically
        job IDs are used as dict keys and all accompanying details are stored as values in JSON format
    goals  : dict
        a dictionary with associated goals in the format of {goal_id1: goal1, goal_id2: goal2, ...}
    """
    goal_names = []
    if len(jobs[job_id]["goalIds"]) > 0:
        for goal_id in jobs[job_id]["goalIds"]:
            goal_names.append(goals[goal_id])
    
    return goal_names


def reorder_columns(job, jobs, column_order):
    """Reorders a job columns (fields) according to a specified order.

    Parameters
    ----------
    job_id : str
        ID of the specific job, which field/column is being expanded
    jobs   : dict
        collection of all jobs. The expected format matches the output of aggregate_job_data(), more specifically
        job IDs are used as dict keys and all accompanying details are stored as values in JSON format
    column_order : list[str]
        a list specifying the preferred column order
    """
    result = {}

    for c in column_order:
        result[c] = jobs[job].get(c)
    return result

def clean_jobs(jobs, columns_to_expand, column_order, columns_to_datetime):

    api = DominoAPISession.instance()
    project_name = api._routes._project_name

    goals = get_goals()
 
    for job in jobs:
        
        for c in (c for c in list(jobs[job]) if c in columns_to_expand):
            expand(c, job, jobs)
        
        for c in (c for c in jobs[job] if c in columns_to_datetime):
            jobs[job][c] = convert_datetime(jobs[job][c])

        if "comments" in jobs[job].keys():
            jobs[job]["comments"] = clean_comments(jobs[job])
        
        jobs[job]["goals"] = merge_goals(job, jobs, goals)
        jobs[job].pop("goalIds")

        jobs[job]["projectName"] = project_name
        jobs[job] = reorder_columns(job, jobs, column_order)
    
    return jobs
    

def main():

    # Set up logging
    DOMINO_LOG_LEVEL = os.getenv("DOMINO_LOG_LEVEL", "INFO").upper()
    logging_level = logging.getLevelName(DOMINO_LOG_LEVEL)
    logging.basicConfig(level=logging_level)
    log = logging.getLogger(__name__)

    # Connect to Domino
    api = DominoAPISession.instance()
    logging.info("Generating audit report for project {}...".format(api._routes._project_name))

    config = read_config("/Users/manchev/Desktop/opensce/OpenSCE/audit-report/config.ini")
    column_order = get_column_order(config)
    columns_to_expand = get_columns_to_expand(config)
    columns_to_datetime = get_columns_to_datetime(config)

    start_time = datetime.datetime.now()

    job_ids = get_jobs()
    logging.info("Found {:d} jobs to report. Aggregating job metadata...".format(len(job_ids)))
    jobs_raw = aggregate_job_data(job_ids[0:10], parallelize=True)

    logging.info("Cleaning data...")
    jobs_cleaned = clean_jobs(jobs_raw, columns_to_expand, column_order, columns_to_datetime)
    
  
    file_name = api._routes._project_name + "_audit_report_" \
                       + datetime.datetime.now(tz=datetime.timezone.utc).strftime("%Y-%m-%d_%X%Z") \
                       + ".csv"
    output_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), file_name)

    logging.info("Saving audit report to: {}".format(output_file))
    
    df = pd.DataFrame.from_dict(jobs_cleaned, orient="index")
    df.to_csv(output_file, header=True, index=False)

    elapsed_time = datetime.datetime.now() - start_time
    logging.info("Audit report generated in {} seconds.".format(str(round(elapsed_time.total_seconds(),1))))

if __name__ == "__main__":
    main()