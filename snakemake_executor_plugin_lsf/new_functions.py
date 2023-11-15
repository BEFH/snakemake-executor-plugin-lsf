
import subprocess
import os
import re

def get_lsf_status(regex):
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
      $1 == "\\"JOB_NEW\\"" && $5 == {os.geteuid()} && $42 ~ "{regex}" {{
          a[$4]
          next
      }}
      $4 in a && $1 == "\\"JOB_STATUS\\"" && $5 != 192 {{
          print $4, $5
      }}
  ' /hpc/lsf/work/chimera/logdir/lsb.events
  '''
  
  # Run the command in the shell using subprocess
  finished = subprocess.run(awk_code, shell=True, capture_output=True, text=True)
  running =  subprocess.run(f"bjobs -noheader -o 'jobid stat' -J '*{regex}*'",
                            shell=True, capture_output=True, text=True)
  
  statuses_all = []
  if running.stdout:
      statuses_all += [tuple(x.split()) for x in running.stdout.strip().split('\n')]
  if finished.stdout:
      codes = [tuple(x.split()) for x in finished.stdout.strip().split('\n')]
      statuses_all += [(x, statuses[y]) for x, y in codes]

  return statuses_all
  
get_lsf_status("test")
get_lsf_status("vscode")

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
    return lsf_config

def job_stati(uuid):
    """
    Obtain LSF job status of all submitted jobs
    """
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
    ' /hpc/lsf/work/chimera/logdir/lsb.events
    '''
    statuses_all = []
    try:
        time_before_query = time.time()
        finished = subprocess.check_output(
            awk_code, shell=True, text=True, stderr=subprocess.PIPE)
        query_duration = time.time() - time_before_query
        print(
            f"The job status for completed jobs was queried.\n"
            f"It took: {query_duration} seconds\n"
            f"The output is:\n'{finished}'\n"
        )
        if finished:
            codes = [tuple(x.split()) for x in finished.strip().split('\n')]
            statuses_all += [(x, statuses[y]) for x, y in codes]
    except subprocess.CalledProcessError as e:
        print(
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
        print(
            f"The job status for running jobs was queried with command: {running_cmd}\n"
            f"It took: {query_duration} seconds\n"
            f"The output is:\n'{running}'\n"
        )
        if running:
            statuses_all += [tuple(x.split()) for x in running.strip().split('\n')]
    except subprocess.CalledProcessError as e:
        print(
            f"The running job status query failed with command: {running_cmd}\n"
            f"Error message: {e.stderr.strip()}\n"
        )
        pass
    
    res = {x[0]: x[1] for x in statuses_all}
    return (res, query_duration)