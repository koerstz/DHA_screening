import sys
import numpy as np
import pandas as pd

from multiprocessing import Pool

from rdkit import Chem
from rdkit.Chem import AllChem

from remap_product import reorder_product

sys.path.append("/home/koerstz/git/tQMC/QMC")
from qmmol import QMMol
from qmconf import QMConf
from calculator.xtb import xTB
from calculator.orca import ORCA
from calculator.gaussian import Gaussian

from conformers.create_conformers import RotatableBonds


def reactant2product(reac_smi):
    """ create prodruct from reactant """
    
    smarts = "[C:1]12=[C:2][C:3]=[C:4][C:5]=[C:6][C:7]1[C:8][C:9]=[C:10]2>>[C:8]=[C:9][C:10]=[C:1]1[C:2]=[C:3][C:4]=[C:5][C:6]=[C:7]1"
    __rxn__ = AllChem.ReactionFromSmarts(smarts)

    # create reactant mol
    reac_mol =  Chem.MolFromSmiles(reac_smi)
    
    prod_mol = __rxn__.RunReactants((reac_mol,))[0][0]
    prod_smi = Chem.MolToSmiles(prod_mol)
    
    reac_smi = Chem.MolToSmiles(Chem.MolFromSmiles(reac_smi))

    return reac_smi, prod_smi


def gs_conformer_search(name, rdkit_conf, chrg, mult, cpus):
    """ ground state conformer search """
    
    charged = True # hard coded for mogens

    # create conformers
    qmmol = QMMol()
    qmmol.add_conformer(rdkit_conf, fmt='rdkit', label=name, 
                        charged_fragments=charged, set_initial=True)

    rot_bonds = len(RotatableBonds(qmmol.initial_conformer.get_rdkit_mol()))
    num_confs = 5 + 3*rot_bonds
    qmmol.create_random_conformers(threads=cpus, num_confs=num_confs) 

                                      
    xtb_params = {'method': 'gfn2',
                  'opt': 'opt',
                  'cpus': 1}

    qmmol.calc = xTB(parameters=xtb_params)
    qmmol.optimize(num_procs=cpus, keep_files=False)
    
    # Get most stable conformer. If most stable conformer
    # not identical to initial conf try second lowest.
    initial_smi = Chem.MolToSmiles(Chem.RemoveHs(qmmol.initial_conformer.get_rdkit_mol()))
    
    low_energy_conf = qmmol.nlowest(1)[0]
    conf_smi = Chem.MolToSmiles(Chem.RemoveHs(low_energy_conf.get_rdkit_mol()))
    
    i = 1
    while initial_smi != conf_smi:
        low_energy_conf = qmmol.nlowest(i+1)[-1]
        conf_smi = Chem.MolToSmiles(Chem.RemoveHs(low_energy_conf.get_rdkit_mol()))
        i += 1
        
        if len(qmmol.conformers) < i:
            sys.exit('no conformers match the initial input')

    return low_energy_conf


def remap_atom_order():
    """ During the creation of the molecule the atom order
    of reactant and product mismatch. This function maps 
    product atom numbers to reactants.
    """
    pass

    # TODO copy reorder code into this. It's works.
    # but i don't wan't multiple files.


def gs_mogens(name, smi, chrg, mult, cps):
    """GS conformers search given a smiles string  """
    
    reac_smi, prod_smi = reactant2product(smi)
    
    reac_mol = Chem.AddHs(Chem.MolFromSmiles(reac_smi))
    prod_mol = reorder_product(reac_mol, Chem.AddHs(Chem.MolFromSmiles(prod_smi)))
    
    for i, comp in enumerate([(reac_mol, "_r"), (prod_mol, "_p")]):
        mol, p_r = comp
        
        AllChem.EmbedMolecule(mol)
        rdkit_conf = mol.GetConformer()

        # create mol for QMMol using rdkit
        n = name + p_r
        
        if p_r == '_r':
            reac_qmconf = gs_conformer_search(n, rdkit_conf, chrg, mult, cps)
        
        if p_r == '_p':
            prod_qmconf = gs_conformer_search(n, rdkit_conf, chrg, mult, cps)

    storage = (prod_qmconf.results['energy'] - reac_qmconf.results['energy'])*627.503

    return reac_qmconf, prod_qmconf, storage


def ts_search(gs_dict):
    """ Perform ts scan of the bond getting broken"""
     
    reactant = gs_dict['reac']

    charged = True # hard coded for mogens

    # find atoms to move during scan.
    smarts_bond = Chem.MolFromSmarts('[CX4;H0;R]-[CX4;H1;R]')
    reactant_rdkit_mol = reactant.get_rdkit_mol()

    atom_idx = reactant_rdkit_mol.GetSubstructMatch(smarts_bond)
    
    orca_tsscan = {'method': 'pm3',
                    'basis': '',
                    'opt': 'opt',
                    'geom scan': 'B {} {} = 1.5, 3.5, 12'.format(*atom_idx),
                    'mem': '8GB',
                    'cpus': 1}
    
    # run ts guess run
    ts_qmmol = QMMol()
    name = reactant.label.split('_')[0] + '_ts'

    ts_qmmol.add_conformer(reactant.write_xyz(to_file=False), fmt='xyz', 
                           label=name, charged_fragments=charged, 
                           set_initial=True)

    ts_qmmol.calc = ORCA(parameters=orca_tsscan)

    ts_conf = ts_qmmol.conformers[0]
    ts_conf.conf_calculate(quantities=['ts_guess', 'ts_guess_energy'])
    

    # update ts_qmmol, hack since i can't set calc on conf.
    # please fix this.

    # Run real TS optimization
    ts_param = {'method': 'pm3',
                'basis': '',
                'opt': 'ts,calcall,noeigentest',
                'freq': 'freq',
                'nproc': 1,
                'mem': '8GB'}
    
    ts_qmmol.calc = Gaussian(parameters=ts_param)
    ts_conf = ts_qmmol.conformers[0]
    ts_conf.conf_calculate(quantities=['energy', 'frequencies', 'intensities', 'normal_coordinates', 'structure'], keep_files=True)
    

    ts_conf = ts_qmmol.conformers[0]

    gs_dict['ts'] = ts_conf
    gs_dict['ts_energy'] = ts_conf.results['energy']
    gs_dict['correct_ts'] = ts_test(ts_conf)

    return gs_dict


def ts_test(test_qmconf):
    """ Automatically test TS if it correct """

    correct_ts = True # if correct TS. Change to false if wrong TS.

    # first test, if the lowest frequency is not imaginary 
    # (i.e. negative) return false.
    if test_qmconf.frequencies[0] > 0:
        correct_ts = False 
    
    # second test, displace the imaginary frequency and see if "bond"
    # distance changes.
    normal_coordinates = np.asarray(test_qmconf.normal_coordinates[0], dtype=float)
    initial_coordinates = np.asarray(test_qmconf.structure)
    
    # find the bond that is broken. Multiple paths can possible match
    # the smarts patteren, therefore i find the atompairs with
    # the smallest distance.
    test_rdkit_mol = test_qmconf.get_rdkit_mol()
    test_rdkit_conf = test_rdkit_mol.GetConformer()

    smarts = '[C;$(*C#N)]~C~C~C~[C;r7]'
    patt = Chem.MolFromSmarts(smarts)

    test_atom_pairs = test_rdkit_mol.GetSubstructMatches(patt)

    dist0 = 9999.0
    for atom_pair in test_atom_pairs:
        atom1, atom2 = atom_pair[0], atom_pair[-1]

        atom1_pos = np.asarray(test_rdkit_conf.GetAtomPosition(atom1))
        atom2_pos = np.asarray(test_rdkit_conf.GetAtomPosition(atom2))

        dist = np.linalg.norm(atom1_pos - atom2_pos)

        if dist < dist0:
            atom_nums = (atom1, atom2)
            dist0 = dist

    initial_distance = dist0

    # make displacement along imaginary normal coordinate
    new_coords = initial_coordinates + 0.5*normal_coordinates

    atom1_new_coords = new_coords[atom_nums[0]]
    atom2_new_coords = new_coords[atom_nums[1]]

    displaced_dist =  np.linalg.norm(atom1_new_coords - atom2_new_coords)
    
    # if change less than 0.15 wrong TS.
    if abs(displaced_dist - initial_distance) < 0.15:
        correct_ts = False

    return correct_ts
        


if __name__ == '__main__':
    
    import pandas as pd
    import sys
    

    cpus = 8

    data = pd.read_csv(sys.argv[1])
    
    # find storage energy
    compound_list = list()
    for idx, compound in data.iterrows():
        
        reac_qmconf, prod_qmconf, storage = gs_mogens(compound.comp_name,
                                                      compound.smiles,
                                                      compound.charge,
                                                      compound.multiplicity,
                                                      cpus)
        
        compound_list.append({'reac': reac_qmconf,
                              'prod': prod_qmconf, 
                              'storage': storage})
    
    ## find ts
    with Pool(cpus) as pool:
        structures = pool.map(ts_search, compound_list)
    
    structures = compound_list
    
    data = pd.DataFrame(structures)
    data.to_pickle(sys.argv[1].split('.')[0] + '.pkl')
