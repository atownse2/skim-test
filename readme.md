To create the environment you can do:

```bash
conda create -f environment.yml
conda activate skim-test-env
conda remove coffea
pip install git+https://github.com/CoffeaTeam/coffea.git@use_merge_union_of_records
```

To run the script you can do
```bash
conda activate skim-test-env
python skim.py -d GJets -t # Run a test job with all of the GJets datasets (~500GB)
```
This will by default cache both the preprocessing and task graph steps. It will not use the cached steps in test mode.

To package the environment do:
```bash
conda activate skim-test-env
poncho_package_create $CONDA_PREFIX skim-test-env.tar.gz
```

Start the factory with:

```bash
vine_factory -T condor -C factory.json --python-env skim-test-env.tar.gz
```

Execute skimming workflow with DaskVine:
```bash
python skim.py -d GJets -b
```