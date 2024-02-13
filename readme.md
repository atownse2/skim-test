To create the environment you can do:

```bash
conda create -f environment.yml
conda activate skim-test-env
conda remove coffea
pip install git+https://github.com/CoffeaTeam/coffea.git@use_merge_union_of_records
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
vine_factory -T condor -C factory.json --python-env skim-test-env.tar.gz
```
