import sys
import os
import shutil

import cloudpickle as pickle
import json

import dask
from ndcctools.taskvine import DaskVine

import dask_awkward as dak
import awkward as ak

top_dir = os.path.dirname(os.path.abspath(__file__))
output_dir = "/project01/ndcms/atownse2/RSTriPhoton/skims"
# output_dir = "/afs/crc.nd.edu/user/a/atownse2/Public/skim-test/outputs"

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

def get_dataset_name(dataset):
    return "_".join(dataset.split("/")[1:-1])

def get_storage(dataset, storage_dir='vast'):
    dType = None
    if "EGamma" in dataset or "DoubleEG" in dataset:
        dType = "data"
    elif "GJets" in dataset:
        dType = "GJets"
    else:
        raise ValueError("Dataset {} not recognized".format(dataset))
    
    if storage_dir == 'vast':
        pre = '/project01/ndcms'
    elif storage_dir == 'hadoop':
        raise NotImplementedError("Hadoop storage not implemented, need to copy files over")
        pre = 'root://ndcms.crc.nd.edu//store/user'
    else:
        raise ValueError(f"Storage directory {storage_dir} not recognized")

    storage = f'{pre}/atownse2/RSTriPhoton/{dType}/NanoAODv9'

    if not os.path.exists(storage):
        os.makedirs(storage)
    return storage

def get_filesets(datasets, test=False):
    filesets = {}
    for dataset in datasets:
        if "/" in dataset:
            dataset = get_dataset_name(dataset)
        filesets[dataset] = {}
        data_dir = get_storage(dataset)
        files = [f for f in os.listdir(data_dir) if dataset in f]
        filesets[dataset]['files'] = {
            f"{data_dir}/{f}": {
                'object_path': "Events",
                } for f in files[:3 if test else None]}
        filesets[dataset]['metadata'] = {'year': get_year(dataset)}

    return filesets

def do_preprocessing(
    datasets,
    step_size,
    reprocess=False,
    test=False,
    n_test_files=None,
    n_test_steps=None,
    scheduler=None):

    if test: assert n_test_files is not None and n_test_steps is not None

    def cache_path(dataset):
        cache_dir = f"{top_dir}/cache/preprocessing"
        if not os.path.exists(cache_dir):
            os.makedirs(cache_dir)

        if test:
            fname = f"{dataset}_{step_size}_{n_test_files}files_{n_test_steps}steps.json"
        else:
            fname = f"{dataset}_{step_size}.json"
        return f"{cache_dir}/{fname}"

    need_to_preprocess = []
    available_filesets = {}
    for dataset in datasets:
        preprocessing_cache = cache_path(dataset)
        if not os.path.exists(preprocessing_cache) or reprocess:
            need_to_preprocess.append(dataset)
        else:
            available_filesets[dataset] = json.load(open(preprocessing_cache))

    if need_to_preprocess:
        print(f'Preprocessing {len(need_to_preprocess)} datasets with chunk size {step_size}')
        filesets = get_filesets(need_to_preprocess, test=test)
        from coffea.dataset_tools import preprocess
        available_fileset, _ = preprocess(
            filesets,
            step_size=step_size,
            skip_bad_files=True,
            save_form=True,
            scheduler=scheduler,
        )
        for dataset_name, dataset_info in available_fileset.items():
            available_filesets[dataset_name] = dataset_info
            json.dump(
                available_fileset[dataset_name],
                open(cache_path(dataset_name), 'w'),
                indent=4, separators=(',', ':'))

    if test:
        print('Only processing one chunk of each dataset')
        from coffea.dataset_tools import max_chunks
        available_filesets = max_chunks(available_filesets, 1)

    from coffea.dataset_tools import filter_files
    return filter_files(available_filesets)

# def get_task_graph(filesets, cache_tag, recreate=False, test=False):
#     graph_cache = f"{top_dir}/cache/graphs/{cache_tag}.pkl"
#     if test: graph_cache = f"{top_dir}/cache/graphs/{cache_tag}_test.pkl"
#     if not os.path.exists(os.path.dirname(graph_cache)):
#         os.makedirs(os.path.dirname(graph_cache))
    
#     if not os.path.exists(graph_cache) or recreate:
#         print('Task graph does not exist, creating it now')
#         graph = {dataset_name: skim_dataset(
#             dataset_name, dataset_info, test=test) for dataset_name, dataset_info in filesets.items()}
#         with open(graph_cache, 'wb') as f:
#             pickle.dump(graph, f)

#     else:
#         with open(graph_cache, 'rb') as f:
#             graph = pickle.load(f)
    
#     return graph

def skim_datasets(graph, scheduler=None):
    if scheduler is not None:
        print("Submitting jobs")
        outputs = dask.compute(
            graph,
            scheduler=scheduler,
            resources={"cores": 1},
            resources_mode=None,
            lazy_transfers=True
        )
    else:
        print("Running skims")
        outputs = dask.compute(
            graph,
        )

def skim_dataset(dataset_name, dataset_info, test=False):
    from coffea.nanoevents import NanoEventsFactory, NanoAODSchema
    from coffea.util import decompress_form

    events = NanoEventsFactory.from_root(
        dataset_info['files'],
        schemaclass=NanoAODSchema,
        known_base_form=ak.forms.from_json(decompress_form(dataset_info['form']))
    ).events()

    year = get_year(dataset_name)
    pass_trigger = dak.zeros_like(events.run, dtype='bool')
    for trigger in triggers[year]:
        pass_trigger = pass_trigger | events.HLT[trigger]

    good_photons = (events.Photon.cutBased >= 1) & (events.Photon.pt > 20)
    has_3_photons = dak.num(events.Photon[good_photons]) >= 3

    events = events[pass_trigger&has_3_photons]

    return events
    # return events.to_parquet(outdir, compute=False)

def compute_and_write(delayed_obj, outpath):
    ak.to_parquet(dask.compute(delayed_obj)[0], outpath)

def skim_and_write(filesets, outdir, scheduler=None):
    to_compute = {}
    for dataset_name, dataset_info in filesets.items():
        events = skim_dataset(dataset_name, dataset_info)
        outpath = f"{outdir}/{dataset_name}.parquet"
        to_compute[dataset_name] = dask.delayed(compute_and_write)(events, outpath)
    
    if scheduler is not None:
        dask.compute(
            to_compute,
            scheduler=scheduler,
            resources={"cores": 1},
            resources_mode=None,
            lazy_transfers=True)
    else:
        dask.compute(to_compute)


# def merge_output(dataset, outdir):
#     import dask_awkward as ak

#     batchdir = f"{get_skim_dir(batch=True)}/{dataset}"
#     if not os.path.exists(batchdir):
#         print(f"Skims do not exist for {dataset}")
#         return

#     outfile = f"{get_skim_dir()}/{dataset}.parquet"
#     if os.path.exists(outfile):
#         os.remove(outfile)

#     outputs=None
#     for file in os.listdir(batchdir):
#         if outputs is None:
#             outputs = ak.from_parquet(f"{batchdir}/{file}")
#         else:
#             outputs = ak.concatenate([outputs, ak.from_parquet(f"{batchdir}/{file}")])

#     return ak.to_parquet(outputs, outfile, compute=False)

# def merge_outputs(datasets, scheduler=None):
#     print('Merging outputs')
#     to_compute = [merge_output(d) for d in datasets]
    
#     dask.compute(
#         to_compute,
#         scheduler=scheduler,
#         resources={"cores": 1},
#         resources_mode=None,
#         lazy_transfers=True
#     )

#     # Clean up batch directory
#     for dataset in datasets:
#         batchdir = f"{get_skim_dir(batch=True)}/{dataset}"
#         if os.path.exists(batchdir):
#             shutil.rmtree(batchdir)

if __name__ == "__main__":
    import argparse
    import time
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--dataset_tag', '-d', type=str, default=None, help='dataset tag')
    parser.add_argument('--do_all', '-a', action='store_true', help='process all datasets')
    parser.add_argument('--step_size', '-s', type=int, default=50000, help='Split input files into chunks of this number of events for processing')
    parser.add_argument('--reprocess', '-r', action='store_true', help='reprocess datasets')
    parser.add_argument('--recreate_graph', action='store_true', help='recreate task graph')
    parser.add_argument('--batch', '-b', action='store_true', help='run in batch mode')
    parser.add_argument('--test', '-t', action='store_true', help='test mode')
    parser.add_argument('--n_test_datasets', type=int, default=2, help='number of datasets to process in test mode')
    parser.add_argument('--n_test_files', type=int, default=3, help='number of files to process in test mode')
    parser.add_argument('--n_test_steps', type=int, default=2, help='number of chunks to process in test mode')
    args = parser.parse_args()

    t_start = time.time()

    # outdir = "/afs/crc.nd.edu/user/a/atownse2/Public/skim-test/outputs"
    outdir = "/project01/ndcms/atownse2/RSTriPhoton/skims"

    # all_datasets = [get_dataset_name(d) for d in all_datasets]
    if args.dataset_tag is not None:
        datasets = [get_dataset_name(d) for d in all_datasets if args.dataset_tag in d]
    elif args.do_all:
        datasets = [get_dataset_name(d) for d in all_datasets]
    else:
        raise ValueError("Must specify dataset tag or use --do_all")

    if args.test:
        print('Running in test mode')
        args.step_size = 50
        args.reprocess = True
        args.recreate_graph = True
        datasets = datasets[:args.n_test_datasets]

    dataset_tag = None
    if args.dataset_tag is not None:
        dataset_tag = args.dataset_tag
    elif args.do_all:
        dataset_tag = "all"

    if args.batch:
        print('Executing with DaskVine')
        m = DaskVine(9125, name="triphoton-manager", run_info_path="/project01/ndcms/atownse2/RSTriPhoton/vine-run-info")
        scheduler = m.get
    else:
        scheduler = None

    t1 = time.time()
    filesets = do_preprocessing(
        datasets, args.step_size,
        reprocess=args.reprocess,
        test=args.test,
        n_test_files=args.n_test_files,
        n_test_steps=args.n_test_steps,
        scheduler=scheduler)
    print(f'Preprocessing executed in {(time.time()-t1)/60} minutes.')

    # Remove output directories if they already exist
    for dataset_name, dataset_info in filesets.items():
        outfile = f"{outdir}/{dataset_name}.parquet"
        if os.path.exists(outfile):
            os.remove(outfile)

    t1 = time.time()
    print('Skimming datasets')
    skim_and_write(filesets, outdir, scheduler=scheduler)
    print(f'Skimming executed in {(time.time()-t1)/60} minutes.')

    # t1 = time.time()
    # print('Getting task graph')
    # cache_filetag = f"{dataset_tag}_{args.step_size}"
    # to_compute = get_task_graph(filesets, cache_filetag, recreate=args.recreate_graph, test=args.test)
    # print(f'Got task graph in {(time.time()-t1)/60} minutes.')

    # t1 = time.time()
    # print('Skimming datasets')
    # skim_datasets(to_compute, scheduler=scheduler)
    # print(f'Skimming executed in {(time.time()-t1)/60} minutes.')

    # t1 = time.time()
    # print('Merging outputs')
    # merge_outputs(datasets, scheduler=scheduler)
    # print(f'Merging outputs executed in {(time.time()-t1)/60} minutes.')

    print(f'Skimming {dataset_tag} datasets executed in {(time.time()-t_start)/60} minutes.')