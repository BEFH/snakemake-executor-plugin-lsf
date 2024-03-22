# Snakemake executor plugin: LSF

[LSF](https://www.ibm.com/docs/en/spectrum-lsf/) is common high performance
computing batch system.

## Specifying Project and Queue

LSF clusters can have mandatory resource indicators for
accounting and scheduling, [Project]{.title-ref} and
[Queue]{.title-ref}, respectivily. These resources are usually
omitted from Snakemake workflows in order to keep the workflow
definition independent from the platform. However, it is also possible
to specify them inside of the workflow as resources in the rule
definition (see `snakefiles-resources`{.interpreted-text role="ref"}).

To specify them at the command line, define them as default resources:

``` console
$ snakemake --executor lsf --default-resources lsf_project=<your LSF project> lsf_queue=<your LSF queue>
```

If individual rules require e.g. a different queue, you can override
the default per rule:

``` console
$ snakemake --executor lsf --default-resources lsf_project=<your LSF project> lsf_queue=<your LSF queue> --set-resources <somerule>:lsf_queue=<some other queue>
```

Usually, it is advisable to persist such settings via a
[configuration profile](https://snakemake.readthedocs.io/en/latest/executing/cli.html#profiles), which
can be provided system-wide, per user, and in addition per workflow.

This is an example of the relevant profile settings:

```yaml
jobs: '<max concurrent jobs>'
executor: lsf
default-resources:
  - 'lsf_project=<your LSF project>'
  - 'lsf_queue=<your LSF queue>'
```

## Ordinary SMP jobs

Most jobs will be carried out by programs which are either single core
scripts or threaded programs, hence SMP ([shared memory
programs](https://en.wikipedia.org/wiki/Shared_memory)) in nature. Any
given threads and `mem_mb` requirements will be passed to LSF:

``` python
rule a:
    input: ...
    output: ...
    threads: 8
    resources:
        mem_mb=14000
```

This will give jobs from this rule 14GB of memory and 8 CPU cores. It is
advisable to use resonable default resources, such that you don\'t need
to specify them for every rule. Snakemake already has reasonable
defaults built in, which are automatically activated when using any non-local executor
(hence also with lsf). Use mem_mb_per_cpu to give the standard LSF type memory per CPU

## MPI jobs {#cluster-lsf-mpi}

Snakemake\'s LSF backend also supports MPI jobs, see
`snakefiles-mpi`{.interpreted-text role="ref"} for details.

``` python
rule calc_pi:
  output:
      "pi.calc",
  log:
      "logs/calc_pi.log",
  threads: 40
  resources:
      tasks=10,
      mpi='mpirun,
  shell:
      "{resources.mpi} -np {resources.tasks} calc-pi-mpi > {output} 2> {log}"
```

``` console
$ snakemake --set-resources calc_pi:mpi="mpiexec" ...
```

## Advanced Resource Specifications

A workflow rule may support a number of
[resource specifications](https://snakemake.readthedocs.io/en/latest/snakefiles/rules.html#resources).
For a LSF cluster, a mapping between Snakemake and LSF needs to be performed.

You can use the following specifications:

| LSF                                | Snakemake        | Description                            |
|------------------------------------|------------------|----------------------------------------|
| `-q`                               | `lsf_queue`      | the queue a rule/job is to use         |
| `--W`                              | `walltime`       | the walltime per job in minutes        |
| `--constraint`                     | `constraint`     | may hold features on some clusters     |
| `-R "rusage[mem=<memory_amount>]"` | `mem`, `mem_mb`  | memory a cluster node must provide     |
|                                    |                  | (`mem`: string with unit, `mem_mb`: i) |
| `-R "rusage[mem=<memory_amount>]"` | `mem_mb_per_cpu` | memory per reserved CPU                |
| omit `-R span[hosts=1]`            | `mpi`            | Allow splitting across nodes for MPI   |
| `-R span[ptile=<ptile>]`           | `ptile`          | Processors per host. Reqires `mpi`     |
| Other `bsub` arguments             | `lsf_extra`      | Other args to pass to `bsub` (str)     |


Each of these can be part of a rule, e.g.:

``` python
rule:
    input: ...
    output: ...
    resources:
        partition: <partition name>
        walltime: <some number>
```

`walltime` and `runtime` are synonyms.

Please note: as `--mem` and `--mem-per-cpu` are mutually exclusive,
their corresponding resource flags `mem`/`mem_mb` and
`mem_mb_per_cpu` are mutually exclusive, too. You can only reserve
memory a compute node has to provide or the memory required per CPU
(LSF does not make any distintion between real CPU cores and those
provided by hyperthreads). The executor will convert the provided options
based on cluster config.

## Additional custom job configuration

There are various `bsub` options not directly supported via the resource
definitions shown above. You may use the `lsf_extra` resource to specify
additional flags to `bsub`:

``` python
rule myrule:
    input: ...
    output: ...
    resources:
        lsf_extra="-R a100 -gpu num=2"
```

Again, rather use a [profile](https://snakemake.readthedocs.io/en/latest/executing/cli.html#profiles) to specify such resources.

## Clusters that use per-job memory requests instead of per-core

By default, this plugin converts the specified memory request into the per-core request expected by most LSF clusters.
So `threads: 4` and `mem_mb=128` will result in `-R rusage[mem=32]`. If the request should be per-job on your cluster
(i.e. `-R rusage[mem=<mem_mb>]`) then set the environment variable `SNAKEMAKE_LSF_MEMFMT` to `perjob`.

The executor automatically detects the request unit from cluster configuration, so if your cluster does not use MB,
you do not need to do anything.

