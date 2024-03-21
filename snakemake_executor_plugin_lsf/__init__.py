__author__ = "Brian Fulton-Howard, David Lähnemann, Johannes Köster, Christian Meesters"
__copyright__ = (
    "Copyright 2023, Brian Fulton-Howard, ",
    "David Lähnemann, ",
    "Johannes Köster, ",
    "Christian Meesters",
)
__email__ = "johannes.koester@uni-due.de"
__license__ = "MIT"

import os
import re
import subprocess
import time
from typing import List, Generator
from collections import Counter
import uuid
from snakemake_interface_executor_plugins.executors.base import SubmittedJobInfo
from snakemake_interface_executor_plugins.executors.remote import RemoteExecutor
from snakemake_interface_executor_plugins.settings import CommonSettings
from snakemake_interface_executor_plugins.jobs import (
    JobExecutorInterface,
)
from snakemake_interface_common.exceptions import WorkflowError


# Required:
# Specify common settings shared by various executors.
common_settings = CommonSettings(
    # define whether your executor plugin executes locally
    # or remotely. In virtually all cases, it will be remote execution
    # (cluster, cloud, etc.). Only Snakemake's standard execution
    # plugins (snakemake-executor-plugin-dryrun, snakemake-executor-plugin-local)
    # are expected to specify False here.
    non_local_exec=True,
    # Define whether your executor plugin implies that there is no shared
    # filesystem (True) or not (False).
    # This is e.g. the case for cloud execution.
    implies_no_shared_fs=False,
    job_deploy_sources=False,
    pass_default_storage_provider_args=True,
    pass_default_resources_args=True,
    pass_envvar_declarations_to_cmd=False,
    auto_deploy_default_storage_provider=False,
    # wait a bit until bjobs has job info available
    init_seconds_before_status_checks=20,
    pass_group_args=True,
)


# Required:
# Implementation of your executor
class Executor(RemoteExecutor):
    def __post_init__(self):
        self.run_uuid = str(uuid.uuid4())
        self._fallback_project_arg = None
        self._fallback_queue = None
        self.lsf_config = self.get_lsf_config()

    def additional_general_args(self):
        # we need to set -j to 1 here, because the behaviour
        # of snakemake is to submit all jobs at once, otherwise.
        # However, the LSF Executor is supposed to submit jobs
        # one after another, so we need to set -j to 1 for the
        # JobStep Executor, which in turn handles the launch of
        # LSF jobsteps.
        return "--executor lsf-jobstep --jobs 1"

    def run_job(self, job: JobExecutorInterface):
        # Implement here how to run a job.
        # You can access the job's resources, etc.
        # via the job object.
        # After submitting the job, you have to call
        # self.report_job_submission(job_info).
        # with job_info being of type
        # snakemake_interface_executor_plugins.executors.base.SubmittedJobInfo.

        log_folder = f"group_{job.name}" if job.is_group() else f"rule_{job.name}"

        wildcard_dict = job.wildcards_dict
        if wildcard_dict:
            wildcard_dict_noslash = {
                k: v.replace("/", "___") for k, v in wildcard_dict.items()
            }
            wildcard_str = "..".join(
                [f"{k}={v}" for k, v in wildcard_dict_noslash.items()]
            )
            wildcard_str_job = ",".join(
                [f"{k}={v}" for k, v in wildcard_dict_noslash.items()]
            )
            jobname = f"Snakemake_{log_folder}:{wildcard_str_job}___({self.run_uuid})"
        else:
            jobname = f"Snakemake_{log_folder}___({self.run_uuid})"
            wildcard_str = "unique"

        lsf_logfile = os.path.abspath(
            f".snakemake/lsf_logs/{log_folder}/{wildcard_str}/{self.run_uuid}.log"
        )

        os.makedirs(os.path.dirname(lsf_logfile), exist_ok=True)

        # generic part of a submission string:
        # we use a run_uuid in the job-name, to allow `--name`-based
        # filtering in the job status checks
        call = f"bsub -J '{jobname}' -o {lsf_logfile} -e {lsf_logfile} -env all"

        call += self.get_project_arg(job)
        call += self.get_queue_arg(job)

        if job.resources.get("runtime"):
            call += f" -W {job.resources.runtime}"
        elif job.resources.get("walltime"):
            call += f" -W {job.resources.walltime}"
        elif job.resources.get("time_min"):
            if not type(job.resources.time_min) in [int, float]:
                self.logger.error("time_min must be a number")
            if not job.resources.time_min % 1 == 0:
                self.logger.error("time_min must be a whole number")
            call += f" -W {job.resources.time_min}"
        else:
            self.logger.warning(
                "No wall time information given. This might or might not "
                "work on your cluster. "
                "If not, specify the resource runtime in your rule or as a reasonable "
                "default via --default-resources."
            )

        cpus_per_task = job.threads
        if job.resources.get("cpus_per_task"):
            if not isinstance(cpus_per_task, int):
                raise WorkflowError(
                    f"cpus_per_task must be an integer, but is {cpus_per_task}"
                )
            cpus_per_task = job.resources.cpus_per_task
        # ensure that at least 1 cpu is requested
        # because 0 is not allowed by LSF
        cpus_per_task = max(1, cpus_per_task)
        call += f" -n {cpus_per_task}"

        # if job.resources.get("constraint"):
        #    call += f" -C {job.resources.constraint}"

        conv_fcts = {"K": 1024, "M": 1, "G": 1 / 1024, "T": 1 / (1024**2)}
        mem_unit = self.lsf_config.get("LSF_UNIT_FOR_LIMITS", "MB")
        conv_fct = conv_fcts[mem_unit[0]]
        if job.resources.get("mem_mb_per_cpu"):
            mem_ = job.resources.mem_mb_per_cpu * conv_fct
        elif job.resources.get("mem_mb"):
            mem_ = job.resources.mem_mb * conv_fct / cpus_per_task
        else:
            self.logger.warning(
                "No job memory information ('mem_mb' or 'mem_mb_per_cpu') is given "
                "- submitting without. This might or might not work on your cluster."
            )
        if self.lsf_config["LSF_MEMFMT"] == "perjob":
            mem_ *= cpus_per_task
        call += f" -R rusage[mem={mem_}]"

        # MPI job
        if job.resources.get("mpi", False):
            if job.resources.get("ptile", False):
                call += f" -R span[ptile={job.resources.get('ptile', 1)}]"
        else:
            call += " -R span[hosts=1]"

        if job.resources.get("lsf_extra"):
            call += f" {job.resources.lsf_extra}"

        exec_job = self.format_job_exec(job)

        # ensure that workdir is set correctly
        call += f" -cwd {self.workflow.workdir_init}"
        # and finally the job to execute with all the snakemake parameters
        # TODO do I need an equivalent to --wrap?
        call += f' "{exec_job}"'

        self.logger.debug(f"bsub call: {call}")
        try:
            out = subprocess.check_output(
                call, shell=True, text=True, stderr=subprocess.STDOUT
            ).strip()
        except subprocess.CalledProcessError as e:
            raise WorkflowError(
                f"LSF job submission failed. The error message was {e.output}"
            )
        lsf_jobid = out.split(" ")[1][1:-1]
        lsf_logfile = lsf_logfile.replace("%J", lsf_jobid)
        self.logger.info(
            f"Job {job.jobid} has been submitted with LSF jobid {lsf_jobid} "
            f"(log: {lsf_logfile})."
        )
        self.report_job_submission(
            SubmittedJobInfo(
                job, external_jobid=lsf_jobid, aux={"lsf_logfile": lsf_logfile}
            )
        )

    async def check_active_jobs(
        self, active_jobs: List[SubmittedJobInfo]
    ) -> Generator[SubmittedJobInfo, None, None]:
        # Check the status of active jobs.

        # You have to iterate over the given list active_jobs.
        # For jobs that have finished successfully, you have to call
        # self.report_job_success(job).
        # For jobs that have errored, you have to call
        # self.report_job_error(job).
        # Jobs that are still running have to be yielded.
        #
        # For queries to the remote middleware, please use
        # self.status_rate_limiter like this:
        #
        # async with self.status_rate_limiter:
        #    # query remote middleware here
        fail_stati = ("SSUSP", "EXIT", "USUSP")
        # Cap sleeping time between querying the status of all active jobs:
        max_sleep_time = 180

        job_query_durations = []

        status_attempts = 6

        active_jobs_ids = {job_info.external_jobid for job_info in active_jobs}
        active_jobs_seen = set()

        self.logger.debug("Checking job status")

        for i in range(status_attempts):
            async with self.status_rate_limiter:
                (status_of_jobs, job_query_duration) = await self.job_stati_bjobs()
                job_query_durations.append(job_query_duration)
                self.logger.debug(f"status_of_jobs after bjobs is: {status_of_jobs}")
                # only take jobs that are still active
                active_jobs_ids_with_current_status = (
                    set(status_of_jobs.keys()) & active_jobs_ids
                )
                active_jobs_seen = (
                    active_jobs_seen | active_jobs_ids_with_current_status
                )
                missing_status_ever = active_jobs_ids - active_jobs_seen
                if missing_status_ever and i > 2:
                    (
                        status_of_jobs_lsbevt,
                        job_query_duration,
                    ) = await self.job_stati_lsbevents()
                    job_query_durations.append(job_query_duration)
                    status_of_jobs.update(status_of_jobs_lsbevt)
                    self.logger.debug(
                        f"status_of_jobs after LSB_EVENTS is: {status_of_jobs}"
                    )
                    active_jobs_ids_with_current_status = (
                        set(status_of_jobs.keys()) & active_jobs_ids
                    )
                    active_jobs_seen = (
                        active_jobs_seen | active_jobs_ids_with_current_status
                    )
                missing_status = active_jobs_seen - active_jobs_ids_with_current_status
                missing_status_ever = active_jobs_ids - active_jobs_seen

                self.logger.debug(f"active_jobs_seen are: {active_jobs_seen}")
                if not missing_status and not missing_status_ever:
                    break
            if i >= status_attempts - 1:
                self.logger.warning(
                    f"Unable to get the status of all active_jobs that should be "
                    f"in LSF, even after {status_attempts} attempts.\n"
                    f"The jobs with the following job ids were previously seen "
                    "but are no longer reported by bjobs or in LSB_EVENTS:\n"
                    f"{missing_status}\n"
                    f"Please double-check with your LSF cluster administrator, that "
                    "job accounting is properly set up.\n"
                )

        any_finished = False
        for j in active_jobs:
            if j.external_jobid not in status_of_jobs:
                yield j
                continue
            status = status_of_jobs[j.external_jobid]
            if status == "DONE":
                self.report_job_success(j)
                any_finished = True
                active_jobs_seen.remove(j.external_jobid)
            elif status == "UNKWN":
                # the job probably does not exist anymore, but 'sacct' did not work
                # so we assume it is finished
                self.report_job_success(j)
                any_finished = True
                active_jobs_seen.remove(j.external_jobid)
            elif status in fail_stati:
                msg = (
                    f"LSF-job '{j.external_jobid}' failed, LSF status is: "
                    f"'{status}'"
                )
                self.report_job_error(j, msg=msg, aux_logs=[j.aux["lsf_logfile"]])
                active_jobs_seen.remove(j.external_jobid)
            else:  # still running?
                yield j

        if not any_finished:
            self.next_seconds_between_status_checks = min(
                self.next_seconds_between_status_checks + 10, max_sleep_time
            )
        else:
            self.next_seconds_between_status_checks = None

    def cancel_jobs(self, active_jobs: List[SubmittedJobInfo]):
        # Cancel all active jobs.
        # This method is called when Snakemake is interrupted.
        if active_jobs:
            # TODO chunk jobids in order to avoid too long command lines
            jobids = " ".join([job_info.external_jobid for job_info in active_jobs])
            try:
                # timeout set to 60, because a scheduler cycle usually is
                # about 30 sec, but can be longer in extreme cases.
                # Under 'normal' circumstances, 'bkill' is executed in
                # virtually no time.
                subprocess.check_output(
                    f"bkill {jobids}",
                    text=True,
                    shell=True,
                    timeout=60,
                    stderr=subprocess.PIPE,
                )
            except subprocess.TimeoutExpired:
                self.logger.warning("Unable to cancel jobs within a minute.")

    async def job_stati_bjobs(self):
        """
        Obtain LSF job status of all submitted jobs from bjobs
        """
        uuid = self.run_uuid

        statuses_all = []

        try:
            running_cmd = f"bjobs -noheader -o 'jobid stat' -aJ '*{uuid}*'"
            time_before_query = time.time()
            running = subprocess.check_output(
                running_cmd, shell=True, text=True, stderr=subprocess.PIPE
            )
            query_duration = time.time() - time_before_query
            self.logger.debug(
                "The job status for running jobs was queried "
                f"with command: {running_cmd}\n"
                f"It took: {query_duration} seconds\n"
                f"The output is:\n'{running}'\n"
            )
            if running:
                statuses_all += [tuple(x.split()) for x in running.strip().split("\n")]
        except subprocess.CalledProcessError as e:
            self.logger.error(
                f"The running job status query failed with command: {running_cmd}\n"
                f"Error message: {e.stderr.strip()}\n"
            )
            pass

        res = {x[0]: x[1] for x in statuses_all}

        return (res, query_duration)

    async def job_stati_lsbevents(self):
        """
        Obtain LSF job status of all submitted jobs from LSB_EVENTS
        """
        uuid = self.run_uuid

        statuses = {
            "0": "NULL",  # State null
            "1": "PEND",  # Job is pending (it has not been dispatched yet).
            "2": "PSUSP",  # Pending job suspended by owner or LSF sysadmin.
            "4": "RUN",  # Job is running.
            "8": "SSUSP",  # Running suspended by the system. *
            "16": "USUSP",  # Running job suspended by owner or LSF sysadmin.
            "32": "EXIT",  # Job terminated with a non-zero status. **
            "64": "DONE",  # Job has terminated with status 0.
            "128": "PDONE",  # Post job process done successfully.
            "256": "PERR",  # Post job process has an error.
            "512": "WAIT",  # Chunk job waiting its turn to exec.
            "32768": "RUNKWN",  # Stat unknown (remote cluster contact lost).
            "65536": "UNKWN",  # Stat unknown (local cluster contact lost). ***
            "131072": "PROV",  # Job is provisional. ****
        }

        #   * because execution host was overloaded or queue run window closed.
        #   ** may have been aborted due to an execution error
        #      or killed by owner or LSF sysadmin.
        #  *** The server batch daemon (sbatchd) on the host on which the job
        #      is processed has lost contact with the master batch daemon
        #      (mbatchd).
        # **** This state shows that the job is dispatched to a standby
        #      power-saved host, and this host is being waken up or started up.

        awk_code = f"""
        awk '
            BEGIN {{
                FPAT = "([^ ]+)|(\\"[^\\"]+\\")"
            }}
            $1 == "\\"JOB_NEW\\"" && $5 == {os.geteuid()} && $42 ~ "{uuid}" {{
                a[$4]
                next
            }}
            $4 in a && $1 == "\\"JOB_STATUS\\"" && $5 != 192 {{
                print $4, $5
            }}
        ' {self.lsf_config['LSB_EVENTS']}
        """

        statuses_all = []

        try:
            time_before_query = time.time()
            finished = subprocess.check_output(
                awk_code, shell=True, text=True, stderr=subprocess.PIPE
            )
            query_duration = time.time() - time_before_query
            self.logger.debug(
                f"The job status for completed jobs was queried.\n"
                f"It took: {query_duration} seconds\n"
                f"The output is:\n'{finished}'\n"
            )
            if finished:
                codes = [tuple(x.split()) for x in finished.strip().split("\n")]
                statuses_all += [(x, statuses[y]) for x, y in codes]
        except subprocess.CalledProcessError as e:
            self.logger.error(
                f"The finished job status query failed with command: {awk_code}\n"
                f"Error message: {e.stderr.strip()}\n"
            )
            pass

        res = {x[0]: x[1] for x in statuses_all}

        return (res, query_duration)

    def get_project_arg(self, job: JobExecutorInterface):
        """
        checks whether the desired project is valid,
        returns a default project, if applicable
        else raises an error - implicetly.
        """
        if job.resources.get("lsf_project"):
            # here, we check whether the given or guessed project is valid
            # if not, a WorkflowError is raised
            # self.test_project(job.resources.lsf_project)
            # disabled because I do not know how to do this
            return f" -P {job.resources.lsf_project}"
        else:
            if self._fallback_project_arg is None:
                self.logger.warning("No LSF project given, trying to guess.")
                project = self.get_project()
                if project:
                    self.logger.warning(f"Guessed LSF project: {project}")
                    self._fallback_project_arg = f" -P {project}"
                else:
                    self.logger.warning(
                        "Unable to guess LSF project. Trying to proceed without."
                    )
                    self._fallback_project_arg = ""  # no project specific args for bsub
            return self._fallback_project_arg

    def get_queue_arg(self, job: JobExecutorInterface):
        """
        checks whether the desired queue is valid,
        returns a default queue, if applicable
        else raises an error - implicetly.
        """
        if job.resources.get("lsf_queue"):
            queue = job.resources.lsf_queue
        else:
            if self._fallback_queue is None:
                self._fallback_queue = self.get_default_queue(job)
            queue = self._fallback_queue
        if queue:
            return f" -q {queue}"
        else:
            return ""

    def get_project(self):
        """
        tries to deduce the project from recent jobs,
        returns None, if none is found
        """
        cmd = "bhist -l -a | grep Project"
        try:
            bhist_out = subprocess.check_output(
                cmd, shell=True, text=True, stderr=subprocess.PIPE
            )
            projects = re.findall(r"Project <([^<>]+)>", bhist_out)
            counter = Counter(projects)
            return counter.most_common(1)[0][0]
        except subprocess.CalledProcessError as e:
            self.logger.warning(
                f"No project was given, not able to get a LSF project from bhist: "
                f"{e.stderr}"
            )
            return None

    def get_default_queue(self, job):
        """
        if no queue is given, checks whether a fallback onto a default
        queue is possible
        """
        if "DEFAULT_QUEUE" in self.lsf_config:
            return self.lsf_config["DEFAULT_QUEUE"]
        self.logger.warning(
            f"No queue was given for rule '{job}', and unable to find "
            "a default queue."
            " Trying to submit without queue information."
            " You may want to invoke snakemake with --default-resources "
            "'lsf_queue=<your default queue>'."
        )
        return ""

    @staticmethod
    def get_lsf_config():
        lsf_config_raw = subprocess.run(
            "badmin showconf mbd", shell=True, capture_output=True, text=True
        )

        lsf_config_lines = lsf_config_raw.stdout.strip().split("\n")
        lsf_config_tuples = [tuple(x.strip().split(" = ")) for x in lsf_config_lines]
        lsf_config = {x[0]: x[1] for x in lsf_config_tuples[1:]}
        clusters = subprocess.run(
            "lsclusters", shell=True, capture_output=True, text=True
        )
        lsf_config["LSF_CLUSTER"] = clusters.stdout.split("\n")[1].split()[0]
        lsf_config["LSB_EVENTS"] = (
            f"{lsf_config['LSB_SHAREDIR']}/{lsf_config['LSF_CLUSTER']}"
            + "/logdir/lsb.events"
        )
        lsb_params_file = (
            f"{lsf_config['LSF_CONFDIR']}/lsbatch/"
            f"{lsf_config['LSF_CLUSTER']}/configdir/lsb.params"
        )
        with open(lsb_params_file, "r") as file:
            for line in file:
                if "=" in line and not line.strip().startswith("#"):
                    key, value = line.strip().split("=", 1)
                    if key.strip() == "DEFAULT_QUEUE":
                        lsf_config["DEFAULT_QUEUE"] = value.split("#")[0].strip()
                        break

        lsf_config["LSF_MEMFMT"] = os.environ.get(
            "SNAKEMAKE_LSF_MEMFMT", "percpu"
        ).lower()

        return lsf_config


def walltime_lsf_to_generic(w):
    """
    convert old LSF walltime format to new generic format
    """
    s = 0
    if type(w) in [int, float]:
        # convert int minutes to hours minutes and seconds
        return w
    elif type(w) is str:
        if re.match(r"^\d+(ms|[smhdw])$", w):
            return w
        elif re.match(r"^\d+:\d+$", w):
            # convert "HH:MM" to hours minutes and seconds
            h, m = map(float, w.split(":"))
        elif re.match(r"^\d+:\d+:\d+$", w):
            # convert "HH:MM:SS" to hours minutes and seconds
            h, m, s = map(float, w.split(":"))
        elif re.match(r"^\d+:\d+\.\d+$", w):
            # convert "HH:MM.XX" to hours minutes and seconds
            h, m = map(float, w.split(":"))
            s = (m % 1) * 60
            m = round(m)
        elif re.match(r"^\d+\.\d+$", w):
            return float(w)
        elif re.match(r"^\d+$", w):
            return int(w)
        else:
            raise ValueError(f"Invalid walltime format: {w}")
    h = int(h)
    m = int(m)
    s = int(s)
    return (h * 60) + m + (s / 60)


def generalize_lsf(rules, runtime=True, memory="perthread_to_perjob"):
    """
    Convert LSF specific resources to generic resources
    """
    re_mem = re.compile(r"^([0-9.]+)(B|KB|MB|GB|TB|PB|KiB|MiB|GiB|TiB|PiB)$")
    for k in rules._rules.keys():
        res_ = rules._rules[k].rule.resources
        if runtime:
            if "walltime" in res_.keys():
                runtime_ = walltime_lsf_to_generic(res_["walltime"])
                del rules._rules[k].rule.resources["walltime"]
            elif "time_min" in res_.keys():
                runtime_ = walltime_lsf_to_generic(res_["time_min"])
                del rules._rules[k].rule.resources["time_min"]
            elif "runtime" in res_.keys():
                runtime_ = walltime_lsf_to_generic(res_["runtime"])
            rules._rules[k].rule.resources["runtime"] = runtime_
        if memory == "perthread_to_perjob":
            if "mem_mb" in res_.keys():
                mem_ = float(res_["mem_mb"]) * res_["_cores"]
                if mem_ % 1 == 0:
                    rules._rules[k].rule.resources["mem_mb"] = int(mem_)
                else:
                    rules._rules[k].rule.resources["mem_mb"] = mem_
            elif "mem" in res_.keys():
                mem_ = re_mem.match(res_["mem"])
                if mem_:
                    mem = float(mem_[1]) * res_["_cores"]
                else:
                    raise ValueError(
                        f"Invalid memory format: {res_['mem']} in rule {k}"
                    )
                if mem % 1 == 0:
                    mem = int(mem)
                rules._rules[k].rule.resources["mem"] = f"{mem}{mem_[2]}"
        elif memory == "rename_mem_mb_per_cpu":
            if "mem_mb" in res_.keys():
                rules._rules[k].rule.resources["mem_mb_per_cpu"] = res_["mem_mb"]
                del rules._rules[k].rule.resources["mem_mb"]
            elif "mem" in res_.keys():
                raise ValueError(
                    f"Cannot rename resource from 'mem' to 'mem_mb_per_cpu' in rule {k}"
                )
