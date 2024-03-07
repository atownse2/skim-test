To create the environment you can do:

```bash
conda create -f environment.yml
```

To do a test run do:
```bash
conda activate skim-test-env
python skim.py -d GJets -t # Run on a small subset
```
In test mode, the default is to run over 3 files per 2 datasets with 2 steps of 50 events per file. These parameters can be modified to scale up.

To run on the full GJets datasets (~500GB) with DaskVine do:
```bash
python skim.py -d GJets -dv
```
This will by default cache the preprocessing step.

If wanted you can run over all the data (~2.5TB) instead by passing `--do_all` instead of `-d dataset_tag`.

To package the environment do:
```bash
conda activate skim-test-env
poncho_package_create $CONDA_PREFIX skim-test-env.tar.gz
```

Start the factory with:

```bash
vine_factory -T condor -C factory.json --python-env skim-test-env.tar.gz --scratch-dir=/tmp/vine-factory-$uid
```

To run interactively with one worker do:
```bash
vine_worker -d all --cores 4 --memory 36000 -M triphoton-manager
```
This will output the logs to the `/tmp` directory of the machine the worker is running on.

To make the main plots do:
```bash
vine_graph_log -T png <PERFORMANCE_LOG_PATH>
```

To make the disk accumulation plots do:
```bash
python vine_plot_compose.py <TRANSACTION_LOGP_PATH> --worker-cache --sublabels --out <OUTPUT_FILENAME>
```