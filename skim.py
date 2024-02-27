import os

import json
# import cloudpickle as pickle

import dask
from ndcctools.taskvine import DaskVine

import awkward as ak
import dask_awkward as dak

top_dir = os.path.dirname(os.path.abspath(__file__))

# Get filenames from DAS and copy them to local directory
all_datasets = [
    # GJets 2016 APV
    "/GJets_HT-40To100_TuneCP5_13TeV-madgraphMLM-pythia8/RunIISummer20UL16NanoAODAPVv9-106X_mcRun2_asymptotic_preVFP_v11-v1/NANOAODSIM",
    "/GJets_HT-100To200_TuneCP5_13TeV-madgraphMLM-pythia8/RunIISummer20UL16NanoAODAPVv9-4cores5k_106X_mcRun2_asymptotic_preVFP_v11-v1/NANOAODSIM",
    "/GJets_HT-200To400_TuneCP5_13TeV-madgraphMLM-pythia8/RunIISummer20UL16NanoAODAPVv9-106X_mcRun2_asymptotic_preVFP_v11-v1/NANOAODSIM",
    "/GJets_HT-400To600_TuneCP5_13TeV-madgraphMLM-pythia8/RunIISummer20UL16NanoAODAPVv9-106X_mcRun2_asymptotic_preVFP_v11-v1/NANOAODSIM",
    "/GJets_HT-600ToInf_TuneCP5_13TeV-madgraphMLM-pythia8/RunIISummer20UL16NanoAODAPVv9-106X_mcRun2_asymptotic_preVFP_v11-v1/NANOAODSIM",
    # GJets 2016
    "/GJets_HT-40To100_TuneCP5_13TeV-madgraphMLM-pythia8/RunIISummer20UL16NanoAODv9-106X_mcRun2_asymptotic_v17-v1/NANOAODSIM",
    "/GJets_HT-100To200_TuneCP5_13TeV-madgraphMLM-pythia8/RunIISummer20UL16NanoAODv9-4cores5k_106X_mcRun2_asymptotic_v17-v1/NANOAODSIM",
    "/GJets_HT-200To400_TuneCP5_13TeV-madgraphMLM-pythia8/RunIISummer20UL16NanoAODv9-106X_mcRun2_asymptotic_v17-v1/NANOAODSIM",
    "/GJets_HT-400To600_TuneCP5_13TeV-madgraphMLM-pythia8/RunIISummer20UL16NanoAODv9-106X_mcRun2_asymptotic_v17-v1/NANOAODSIM",
    "/GJets_HT-600ToInf_TuneCP5_13TeV-madgraphMLM-pythia8/RunIISummer20UL16NanoAODv9-106X_mcRun2_asymptotic_v17-v1/NANOAODSIM",
    # GJets 2017
    "/GJets_HT-40To100_TuneCP5_13TeV-madgraphMLM-pythia8/RunIISummer20UL17NanoAODv9-106X_mc2017_realistic_v9-v1/NANOAODSIM",
    "/GJets_HT-100To200_TuneCP5_13TeV-madgraphMLM-pythia8/RunIISummer20UL17NanoAODv9-4cores5k_106X_mc2017_realistic_v9-v1/NANOAODSIM",
    "/GJets_HT-200To400_TuneCP5_13TeV-madgraphMLM-pythia8/RunIISummer20UL17NanoAODv9-106X_mc2017_realistic_v9-v1/NANOAODSIM",
    "/GJets_HT-400To600_TuneCP5_13TeV-madgraphMLM-pythia8/RunIISummer20UL17NanoAODv9-106X_mc2017_realistic_v9-v1/NANOAODSIM",
    "/GJets_HT-600ToInf_TuneCP5_13TeV-madgraphMLM-pythia8/RunIISummer20UL17NanoAODv9-106X_mc2017_realistic_v9-v1/NANOAODSIM",
    # GJets 2018
    "/GJets_HT-40To100_TuneCP5_13TeV-madgraphMLM-pythia8/RunIISummer20UL18NanoAODv9-106X_upgrade2018_realistic_v16_L1v1-v1/NANOAODSIM",
    "/GJets_HT-100To200_TuneCP5_13TeV-madgraphMLM-pythia8/RunIISummer20UL18NanoAODv9-4cores5k_106X_upgrade2018_realistic_v16_L1v1-v1/NANOAODSIM",
    "/GJets_HT-200To400_TuneCP5_13TeV-madgraphMLM-pythia8/RunIISummer20UL18NanoAODv9-106X_upgrade2018_realistic_v16_L1v1-v1/NANOAODSIM",
    "/GJets_HT-400To600_TuneCP5_13TeV-madgraphMLM-pythia8/RunIISummer20UL18NanoAODv9-106X_upgrade2018_realistic_v16_L1v1-v1/NANOAODSIM",
    "/GJets_HT-600ToInf_TuneCP5_13TeV-madgraphMLM-pythia8/RunIISummer20UL18NanoAODv9-106X_upgrade2018_realistic_v16_L1v1-v1/NANOAODSIM",
    # # Data 2016
    "/DoubleEG/Run2016B-ver1_HIPM_UL2016_MiniAODv2_NanoAODv9-v2/NANOAOD",
    "/DoubleEG/Run2016B-ver2_HIPM_UL2016_MiniAODv2_NanoAODv9-v3/NANOAOD",
    "/DoubleEG/Run2016C-HIPM_UL2016_MiniAODv2_NanoAODv9-v2/NANOAOD",
    "/DoubleEG/Run2016D-HIPM_UL2016_MiniAODv2_NanoAODv9-v2/NANOAOD",
    "/DoubleEG/Run2016E-HIPM_UL2016_MiniAODv2_NanoAODv9-v2/NANOAOD",
    "/DoubleEG/Run2016F-HIPM_UL2016_MiniAODv2_NanoAODv9-v2/NANOAOD",
    "/DoubleEG/Run2016F-UL2016_MiniAODv2_NanoAODv9-v1/NANOAOD",
    "/DoubleEG/Run2016G-UL2016_MiniAODv2_NanoAODv9-v1/NANOAOD",
    "/DoubleEG/Run2016H-UL2016_MiniAODv2_NanoAODv9-v1/NANOAOD",
    # Data 2017
    "/DoubleEG/Run2017B-UL2017_MiniAODv2_NanoAODv9-v1/NANOAOD",
    "/DoubleEG/Run2017C-UL2017_MiniAODv2_NanoAODv9-v1/NANOAOD",
    "/DoubleEG/Run2017D-UL2017_MiniAODv2_NanoAODv9-v1/NANOAOD",
    "/DoubleEG/Run2017E-UL2017_MiniAODv2_NanoAODv9-v1/NANOAOD",
    "/DoubleEG/Run2017F-UL2017_MiniAODv2_NanoAODv9-v1/NANOAOD",
    # Data 2018
    "/EGamma/Run2018A-UL2018_MiniAODv2_NanoAODv9-v1/NANOAOD",
    "/EGamma/Run2018B-UL2018_MiniAODv2_NanoAODv9-v1/NANOAOD",
    "/EGamma/Run2018C-UL2018_MiniAODv2_NanoAODv9-v1/NANOAOD",
    "/EGamma/Run2018D-UL2018_MiniAODv2_NanoAODv9-v3/NANOAOD",
]

data_tags = ['DoubleEG', 'EGamma']

years = ["2016", "2017", "2018"]

mc_years_dict = {"2016":"RunIISummer20UL16",
                 "2017":"RunIISummer20UL17",
                 "2018":"RunIISummer20UL18"}

def get_year(dataset):
    if any(tag in dataset for tag in data_tags):
        for year in years:
            if year in dataset:
                return year
    else:
        for year, mc_year in mc_years_dict.items():
            if mc_year in dataset:
                return year
    raise ValueError(f"Dataset {dataset} not recognized as data or MC")

triggers= {"2016": ['DoublePhoton60'],
           "2017": [
               'DoublePhoton70', 
                "TriplePhoton_20_20_20_CaloIdLV2_R9IdVL"
                ],
           "2018": [
               'DoublePhoton70',
               "TriplePhoton_20_20_20_CaloIdLV2_R9IdVL"
               ]}

vast_base = "/project01/ndcms/atownse2/RSTriPhoton"

def dataset_dir(dataset, base_dir, data_format):

    if any(tag in dataset for tag in data_tags):
        tag = "data"
    else:
        tag = "mc"
    
    outdir = f"{base_dir}/{tag}/{data_format}"
    if not os.path.exists(outdir):
        os.makedirs(outdir)
    
    return outdir

def get_filesets(datasets, base_dir, data_format, test=False, n_test_files=None):

    filesets = {}
    for dataset in datasets:
        filesets[dataset] = {}
        data_dir = dataset_dir(dataset, base_dir, data_format)
        files = [f for f in os.listdir(data_dir) if dataset in f]
        if test: files = files[:n_test_files]
        filesets[dataset]['files'] = {
            f"{data_dir}/{f}": {
                'object_path': "Events",
                } for f in files}
        filesets[dataset]['metadata'] = {'year': get_year(dataset)}

    return filesets

def do_preprocessing(
    datasets,
    step_size,
    input_base_dir=vast_base,
    input_format="NanoAODv9",
    reprocess=False,
    test=False,
    n_test_files=None,
    n_test_steps=None,
    scheduler=None):

    if test:
        print(f"Running in test mode with step size {step_size}")
        if n_test_files is not None:
            print(f"Only processing {n_test_files} files for each dataset")
        if n_test_steps is not None:
            print(f"Only processing {n_test_steps} steps for each file")

    def cache_path(dataset):
        cache_dir = f"{top_dir}/cache/preprocessing"
        if not os.path.exists(cache_dir):
            os.makedirs(cache_dir)

        if test:
            fname = f"{dataset}_{step_size}_{n_test_files}files.json"
        else:
            fname = f"{dataset}_{step_size}.json"
        return f"{cache_dir}/{fname}"

    need_to_preprocess = []
    filesets = {}
    for dataset in datasets:
        preprocessing_cache = cache_path(dataset)
        if not os.path.exists(preprocessing_cache) or reprocess:
            need_to_preprocess.append(dataset)
        else:
            filesets[dataset] = json.load(open(preprocessing_cache))

    if need_to_preprocess:
        print(f'Preprocessing {len(need_to_preprocess)} datasets with chunk size {step_size}')
        filesets = get_filesets(need_to_preprocess, input_base_dir, input_format, test=test, n_test_files=n_test_files)
        from coffea.dataset_tools import preprocess
        available_fileset, _ = preprocess(
            filesets,
            step_size=step_size,
            skip_bad_files=True,
            save_form=True,
            scheduler=scheduler,
        )
        for dataset, info in available_fileset.items():
            filesets[dataset] = info
            json.dump(
                info,
                open(cache_path(dataset), 'w'),
                indent=4, separators=(',', ':'))

    if test and n_test_steps is not None:
        from coffea.dataset_tools import max_chunks
        filesets = max_chunks(filesets, n_test_steps)

    from coffea.dataset_tools import filter_files
    return filter_files(filesets)

def skim_dataset(dataset, dataset_info, test=False):
    from coffea.nanoevents import NanoEventsFactory, NanoAODSchema
    from coffea.util import decompress_form

    events = NanoEventsFactory.from_root(
        dataset_info['files'],
        schemaclass=NanoAODSchema,
        known_base_form=ak.forms.from_json(decompress_form(dataset_info['form']))
    ).events()

    year = get_year(dataset)
    pass_trigger = dak.zeros_like(events.run, dtype='bool')
    for trigger in triggers[year]:
        pass_trigger = pass_trigger | events.HLT[trigger]

    good_photons = (events.Photon.cutBased >= 1) & (events.Photon.pt > 20)
    has_3_photons = dak.num(events.Photon[good_photons]) >= 3

    return events[pass_trigger&has_3_photons]

def compute_and_write(events, outpath):
    ak.to_parquet(dask.compute(events)[0], outpath)

def create_graph(filesets, output_base_dir=vast_base):
    graph = {}
    for dataset, info in filesets.items():
        events = skim_dataset(dataset, info)
        outpath = f"{dataset_dir(dataset, output_base_dir, 'skim')}/{dataset}.parquet"
        graph[dataset] = dask.delayed(compute_and_write)(events, outpath)

    return graph

def optimize_graph(graph):
    """A graph here is a dictionary of delayed object"""
    from dask.optimization import cull, fuse, inline
    from dask.delayed import Delayed
    new_graph = {}
    for key, delayed_obj in graph.items():
        dsk, _ = cull(delayed_obj.dask, delayed_obj.__dask_keys__())
        dsk, _ = fuse(dsk, ave_width=4)
        dsk = inline(dsk, delayed_obj.__dask_keys__())
        dsk, _ = cull(dsk, delayed_obj.__dask_keys__())
        new_graph[key] = Delayed(delayed_obj.__dask_keys__(), dsk)
    
    return new_graph

def compute_graph(graph, scheduler=None):
    if scheduler is not None:
        dask.compute(
            graph,
            scheduler=scheduler,
            resources={"cores": 1},
            resources_mode=None,
            lazy_transfers=True)
    else:
        dask.compute(graph)

if __name__ == "__main__":
    import argparse
    import time

    t_start = time.time()
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--dataset_tag', '-d', type=str, default=None, help='dataset tag')
    parser.add_argument('--do_all', '-a', action='store_true', help='process all datasets')
    parser.add_argument('--step_size', '-s', type=int, default=250000, help='Split input files into chunks of this number of events for processing')
    parser.add_argument('--reprocess', '-r', action='store_true', help='reprocess datasets')
    parser.add_argument('--recreate_graph', action='store_true', help='recreate task graph')
    parser.add_argument('--optimize', action='store_true', help='optimize task graph')
    parser.add_argument('--outdir_base', '-o', type=str, default="/project01/ndcms/atownse2/RSTriPhoton/", help='output directory')
    parser.add_argument('--use_dask_vine', '-dv', action='store_true', help='use DaskVine')
    parser.add_argument('--test', '-t', action='store_true', help='test mode')
    parser.add_argument('--test_step_size', type=int, default=50, help='chunk size for test mode')
    parser.add_argument('--n_test_datasets', type=int, default=2, help='number of datasets to process in test mode (-1=all)')
    parser.add_argument('--n_test_files', type=int, default=3, help='number of files to process in test mode (-1=all)')
    parser.add_argument('--n_test_steps', type=int, default=2, help='number of chunks to process in test mode (-1=all)')
    args = parser.parse_args()

    dataset_tag = args.dataset_tag
    do_all = args.do_all
    step_size = args.step_size
    reprocess = args.reprocess
    recreate_graph = args.recreate_graph
    optimize = args.optimize
    outdir_base = args.outdir_base
    use_dask_vine = args.use_dask_vine

    test = args.test
    n_test_datasets = None
    n_test_files = None
    n_test_steps = None

    if test:
        outdir_base = f"{outdir_base}/test"
        step_size = args.test_step_size
        n_test_datasets = args.n_test_datasets if args.n_test_datasets>0 else None
        n_test_files = args.n_test_files if args.n_test_files>0 else None
        n_test_steps = args.n_test_steps if args.n_test_steps>0 else None

    if not os.path.exists(outdir_base):
        os.makedirs(outdir_base)
    
    scheduler = None
    if use_dask_vine:
        print('Executing with DaskVine')
        m = DaskVine(9125, name="triphoton-manager", run_info_path="/project01/ndcms/atownse2/RSTriPhoton/vine-run-info")
        scheduler = m.get

    # Get datasets
    assert not (dataset_tag is not None and do_all), "Cannot specify dataset tag and use --do_all"
    dataset_name = lambda d: "_".join(d.split("/")[1:-1])
    if dataset_tag is not None:
        datasets = [dataset_name(d) for d in all_datasets if dataset_tag in d]
    elif do_all:
        dataset_tag = "all"
        datasets = [dataset_name(d) for d in all_datasets]
    else:
        raise ValueError("Must specify dataset tag or use --do_all")

    if test:
        datasets = datasets[:n_test_datasets]

    # Create output directories and remove files if they exist
    for dataset in datasets:
        outdir = dataset_dir(dataset, outdir_base, "skim")
        outfile = f"{outdir}/{dataset}.parquet"
        if os.path.exists(outfile): os.remove(outfile)

    # Get filesets
    t1 = time.time()
    filesets = do_preprocessing(
        datasets, step_size,
        reprocess=reprocess,
        scheduler=scheduler,
        test=test,
        n_test_files=n_test_files,
        n_test_steps=n_test_steps)
    print(f'Preprocessing executed in {(time.time()-t1)/60} minutes.')

    # Create task graph
    t1 = time.time()
    print('Creating task graph')
    graph = create_graph(filesets, outdir_base)
    print(f'Task graph created in {(time.time()-t1)/60} minutes.')

    if optimize:
        graph = optimize_graph(graph)

    # Compute and write outputs
    t1 = time.time()
    print('Computing and writing outputs')
    compute_graph(graph, scheduler=scheduler)
    print(f'Outputs computed and written in {(time.time()-t1)/60} minutes.')

    print(f'Skimming {dataset_tag} datasets executed in {(time.time()-t_start)/60} minutes.')