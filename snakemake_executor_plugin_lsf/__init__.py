__author__ = "David Lähnemann, Johannes Köster, Christian Meesters, Brian Fulton-Howard"
__copyright__ = "Copyright 2023, David Lähnemann, Johannes Köster, Christian Meesters, Brian Fulton-Howard"
__email__ = "johannes.koester@uni-due.de"
__license__ = "MIT"

import csv
from io import StringIO
import os
import subprocess
import time
from typing import List, Generator
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
    pass_default_storage_provider_args=True,
    pass_default_resources_args=True,
    pass_envvar_declarations_to_cmd=False,
    auto_deploy_default_storage_provider=False,
    # wait a bit until lsf has job info available
    init_seconds_before_status_checks=20,
)


# Required:
# Implementation of your executor
class Executor(RemoteExecutor):
    def __post_init__(self):
        self.run_uuid = str(uuid.uuid4())
        self._fallback_project_arg = None
        self._fallback_queue = None
        self.lsf_config = get_lsf_config()

    def additional_general_args(self):
        # we need to set -j to 1 here, because the behaviour
        # of snakemake is to submit all jobs at once, otherwise.
        # However, the LSF Executor is supposed to submit jobs
        # one after another, so we need to set -j to 1 for the
        # JobStep Executor, which in turn handles the launch of
        # LSF jobsteps.
        return "--executor slurm-jobstep --jobs 1"

    def run_job(self, job: JobExecutorInterface):
        # Implement here how to run a job.
        # You can access the job's resources, etc.
        # via the job object.
        # After submitting the job, you have to call
        # self.report_job_submission(job_info).
        # with job_info being of type
        # snakemake_interface_executor_plugins.executors.base.SubmittedJobInfo.

        jobid = job.jobid

        log_folder = f"group_{job.name}" if job.is_group() else f"rule_{job.name}"

        lsf_logfile = os.path.abspath(f".snakemake/lsf_logs/{log_folder}/%j.log")

        os.makedirs(os.path.dirname(lsf_logfile), exist_ok=True)

        # generic part of a submission string:
        # we use a run_uuid as the job-name, to allow `--name`-based
        # filtering in the job status checks
        call = f"bsub -J {self.run_uuid} -o {lsf_logfile} -e {lsf_logfile} -env all"

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
        # because 0 is not allowed by slurm
        cpus_per_task = max(1, cpus_per_task)
        call += f" -n {cpus_per_task}"

        # if job.resources.get("constraint"):
        #    call += f" -C {job.resources.constraint}"
        #TODO base this on the block size in self.lsf_config["LSF_UNIT_FOR_LIMITS"]
        if job.resources.get("mem_mb_per_cpu"):
            call += f" -R rusage[mem={job.resources.mem_mb_per_cpu}]"
        elif job.resources.get("mem_mb"):
            mem_mb_per_cpu = job.resources.mem_mb / cpus_per_task
            call += f" -R rusage[mem={mem_mb_per_cpu}]"
        else:
            self.logger.warning(
                "No job memory information ('mem_mb' or 'mem_mb_per_cpu') is given "
                "- submitting without. This might or might not work on your cluster."
            )

        # MPI job
        if job.resources.get("mpi", False):
            if job.resources.get("ptile", False):
                call += f" -R span[ptile={job.resources.get('ptile', 1)}]"
        else:
            call += f" -R span[hosts=1]"


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

        lsf_jobid = out.split(" ")[-1]
        lsf_logfile = lsf_logfile.replace("%j", lsf_jobid)
        self.logger.info(
            f"Job {jobid} has been submitted with LSF jobid {slurm_jobid} "
            f"(log: {lsf_logfile})."
        )
        self.report_job_submission(
            SubmittedJobInfo(
                job, external_jobid=slurm_jobid, aux={"lsf_logfile": lsf_logfile}
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
        fail_stati = (
            "SSUSP",
            "EXIT",
            "USUSP"
        )
        # Cap sleeping time between querying the status of all active jobs:
        # If `AccountingStorageType`` for `sacct` is set to `accounting_storage/none`,
        # sacct will query `slurmctld` (instead of `slurmdbd`) and this in turn can
        # rely on default config, see: https://stackoverflow.com/a/46667605
        # This config defaults to `MinJobAge=300`, which implies that jobs will be
        # removed from `slurmctld` within 6 minutes of finishing. So we're conservative
        # here, with half that time
        max_sleep_time = 180

        job_query_durations = []

        status_attempts = 5

        active_jobs_ids = {job_info.external_jobid for job_info in active_jobs}
        active_jobs_seen = set()

        for i in range(status_attempts):
            async with self.status_rate_limiter:
                (status_of_jobs, job_query_duration) = await self.job_stati()
                job_query_durations.append(job_query_duration)
                self.logger.debug(f"status_of_jobs after bjobs is: {status_of_jobs}")
                # only take jobs that are still active
                active_jobs_ids_with_current_status = (
                    set(status_of_jobs.keys()) & active_jobs_ids
                )
                active_jobs_seen = (
                    active_jobs_seen
                    | active_jobs_ids_with_current_status
                )
                self.logger.debug(
                    f"active_jobs_seen are: {active_jobs_seen}"
                )
                missing_status = (
                    active_jobs_seen
                    - active_jobs_ids_with_current_status
                )
                if not missing_status:
                    break
            if i >= status_attempts - 1:
                self.logger.warning(
                    f"Unable to get the status of all active_jobs that should be "
                    f"in LSF, even after {status_attempts} attempts.\n"
                    f"The jobs with the following  job ids were previously seen "
                    "by bhist, but bhist doesn't report them any more:\n"
                    f"{missing_status}\n"
                    f"Please double-check with your LSF cluster administrator, that "
                    "job accounting is properly set up.\n"
                )

        any_finished = False
        for j in active_jobs:
            # the job probably didn't make it into slurmdbd yet, so
            # `sacct` doesn't return it
            if j.external_jobid not in status_of_jobs:
                # but the job should still be queueing or running and
                # appear in slurmdbd (and thus `sacct` output) later
                yield j
                continue
            status = status_of_jobs[j.external_jobid]
            if status == "DONE":
                self.report_job_success(j)
                any_finished = True
                active_jobs_seen_by_bhist.remove(j.external_jobid)
            elif status == "UNKWN":
                # the job probably does not exist anymore, but 'sacct' did not work
                # so we assume it is finished
                self.report_job_success(j)
                any_finished = True
                active_jobs_seen_by_bhist.remove(j.external_jobid)
            elif status in fail_stati:
                msg = (
                    f"LSF-job '{j.external_jobid}' failed, LSF status is: "
                    f"'{status}'"
                )
                self.report_job_error(j, msg=msg, aux_logs=[j.aux["lsf_logfile"]])
                active_jobs_seen_by_bhist.remove(j.external_jobid)
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

    async def job_stati(self):
        """
        Obtain LSF job status of all submitted jobs
        """
        uuid = self.run_uuid

        statuses = {
            "0": 'NULL',  # State null
            "1": 'PEND',  # The job is pending, i.e., it has not been dispatched yet.
            "2": 'PSUSP',  # The pending job was suspended by its owner or the LSF system administrator.
            "4": 'RUN',  # The job is running.
            "8": 'SSUSP',  # The running job was suspended by the system because an execution host was overloaded or the queue run window closed.
            "16": 'USUSP',  # The running job was suspended by its owner or the LSF system administrator.
            "32": 'EXIT',  # The job has terminated with a non-zero status - it may have been aborted due to an error in its execution or killed by its owner or by the LSF system administrator.
            "64": 'DONE',  # The job has terminated with status 0.
            "128": 'PDONE',  # Post job process done successfully.
            "256": 'PERR',  # Post job process has an error.
            "512": 'WAIT',  # Chunk job waiting its turn to exec.
            "32768": 'RUNKWN',  # Flag: Job status is UNKWN caused by losing contact with a remote cluster.
            "65536": 'UNKWN',  # The server batch daemon (sbatchd) on the host on which the job is processed has lost contact with the master batch daemon (mbatchd).
            "131072": 'PROV'  # This state shows that the job is dispatched to a standby power saved host, and this host is being waken up or started up.
        }
        awk_code = f'''
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
        '''

        statuses_all = []

        try:
            time_before_query = time.time()
            finished = subprocess.check_output(
                awk_code, shell=True, text=True, stderr=subprocess.PIPE)
            query_duration = time.time() - time_before_query
            self.logger.debug(
                f"The job status for completed jobs was queried.\n"
                f"It took: {query_duration} seconds\n"
                f"The output is:\n'{finished}'\n"
            )
            if finished:
                codes = [tuple(x.split()) for x in finished.strip().split('\n')]
                statuses_all += [(x, statuses[y]) for x, y in codes]
        except subprocess.CalledProcessError as e:
            self.logger.error(
                f"The finished job status query failed with command: {awk_code}\n"
                f"Error message: {e.stderr.strip()}\n"
            )
            pass

        try:
            running_cmd = f"bjobs -noheader -o 'jobid stat' -J '*{uuid}*'"
            time_before_query = time.time()
            running = subprocess.check_output(
                running_cmd, shell=True, text=True, stderr=subprocess.PIPE)
            query_duration = time.time() - time_before_query
            self.logger.debug(
                f"The job status for running jobs was queried with command: {running_cmd}\n"
                f"It took: {query_duration} seconds\n"
                f"The output is:\n'{running}'\n"
            )
            if running:
                statuses_all += [tuple(x.split()) for x in running.strip().split('\n')]
        except subprocess.CalledProcessError as e:
            self.logger.error(
                f"The running job status query failed with command: {running_cmd}\n"
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
                    self._fallback_project_arg = (
                        ""  # no project specific args for bsub
                    )
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
            projects = re.findall(r'Project <([^<>]+)>', bhist_out)
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
        lsf_config_raw = subprocess.run(f"badmin showconf mbd",
                                        shell=True, capture_output=True,
                                        text=True)
        
        lsf_config_lines = lsf_config_raw.stdout.strip().split("\n")
        lsf_config_tuples = [tuple(x.strip().split(" = ")) for x in lsf_config_lines]
        lsf_config = {x[0]: x[1] for x in lsf_config_tuples[1:]}
        clusters = subprocess.run("lsclusters", shell=True,
                                  capture_output=True, text=True)
        lsf_config['LSF_CLUSTER'] = clusters.stdout.split("\n")[1].split()[0]
        lsf_config['LSB_EVENTS'] = (f"{lsf_config['LSB_SHAREDIR']}/{lsf_config['LSF_CLUSTER']}" +
                                    "/logdir/lsb.events")
        lsb_params_file = f"{lsf_config['LSF_CONFDIR']}/lsbatch/{lsf_config['LSF_CLUSTER']}/configdir/lsb.params"
        with open(lsb_params_file, 'r') as file:
            for line in file:
                if '=' in line:
                    key, value = line.strip().split('=', 1)
                    if key.strip() == "DEFAULT_QUEUE":
                        lsf_config["DEFAULT_QUEUE"] = value.split("#")[0].strip()
                        break
        return lsf_config