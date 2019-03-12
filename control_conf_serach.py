import time
import os
import sys
import textwrap


import pandas as pd
import numpy as np
from multiprocessing.pool import ThreadPool


sys.path.append("/home/koerstz/git/tQMC/QMC")
from qmconf import QMConf


def qsub_prep(batchname, script_path, cpus, mem):
    """ """

    pwd = os.getcwd()

    qsub_file = '''\
    #!/bin/sh 
    #SBATCH --job-name={0}
    #SBATCH --cpus-per-task={1}
    #SBATCH --mem={2}
    #SBATCH --ntasks=1
    #SBATCH --error={3}/{0}.stderr
    #SBATCH --output={3}/{0}.stdout
    #SBATCH --time=10000:00:00
    #SBATCH --partition=coms
    #SBATCH --no-requeue

    
    # create sctrach folder
    mkdir -p /scratch/$SLURM_JOB_ID
    cd /scratch/$SLURM_JOB_ID
    

    # copy batch file
    cp {3}/{0}.csv .


    # run python code
    /home/koerstz/anaconda3/envs/rdkit-env/bin/python {4} {0}.csv {1}
    

    # copy data back
    #tar -czf {0}_output.tar.gz *xyz 
    #cp {0}_output.tar.gz *pkl {3}
    
    cp *pkl {3}

    # delete scratch
    rm -rf /scratch/$SLURM_JOB_ID
    '''.format(batchname, cpus, mem, pwd, script_path)
    
    
    with open(batchname + "_qsub.tmp", 'w') as qsub:
        qsub.write(textwrap.dedent(qsub_file))
    
    return batchname + "_qsub.tmp"



def control(batch_info):
    """ check that it terminates """
    
    sbatch, script, cps, mem = batch_info
    qsub_name = qsub_prep(sbatch, script, cps, mem)

    
    batch_id = os.popen("sbatch " + qsub_name).read()
    batch_id = batch_id.strip().split()[-1]

    
    while True:
        output = os.popen("squeue -u koerstz").read()
        
        if batch_id in output:
            time.sleep(5)
            continue 
        
        else:
            break

    # create data frame:
    df = pd.read_pickle(sbatch + ".pkl")
    
    return df


if __name__ == "__main__":
    
    # input params
    cpus = 8
    mem = "40GB"
     
    nodes = 10
    chunk_size = 1

    script = '/home/koerstz/projects/mogens_dha_vhf/v3.0/full_automatization/mogens_conf_search.py'
   

    data_file = sys.argv[1]

    ##########################################################################
    #
    ##########################################################################

    # import data
    data = pd.read_csv(data_file) 

    # split data into chunks
    chunked_data = [data[i:i+chunk_size] for i in range(0, data.shape[0], chunk_size)]
    
    
    chunk_names = list()
    for idx, chunk in enumerate(chunked_data):
        chunk_name = "smiles_batch-{}".format(idx)

        chunk.to_csv(chunk_name + ".csv", index=False)
        chunk_names.append((chunk_name, script, cpus, mem))
        
    # run calculations on nodes
    with ThreadPool(nodes) as pool:
        dfs = pool.map(control, chunk_names)
    

    # collect all data
    out_data = pd.concat(dfs, axis=0)
    out_data.to_csv("all_data.csv", index=False)
    out_data.to_pickle("all_data.pkl")



