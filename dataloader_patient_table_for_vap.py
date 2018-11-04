import os
import pandas as pd
import numpy as np
import variable_references as var

#enviroment variables
data_path = "/cluster/work/grlab/clinical/Inselspital/DataReleases/01-19-2017/InselSpital/"
input_dir = "paO2_prediction"
output_dir = "paO2_prediction"
general_data_file = os.path.join(data_path, input_dir, "generaldata.h5")
merged_data_file = os.path.join(data_path, input_dir, "merged_pivot.h5")
output_data_file = os.path.join(data_path, output_dir, "vap_ventilated_pivot.h5")

#Read General File for Patient IDs
df_gen = pd.read_hdf(general_data_file)

#Loop trough all patients and add them to final table after concatiating all tables and joining different columns of same base concepts
for patientID in df_gen[df_gen.AdmissionTime > '01.01.2008'].PatientID:
	print(patientID)

	#concatinate
	df_merged_pat = pd.read_hdf(merged_data_file, where='PatientID==' + str(patientID))
	
	#derived values for filtering
	df_merged_pat[var.MechanicalVentilation] = False | (df_merged_pat[var.etCO2] > 1) | (df_merged_pat[var.etCO2].shift(1) > 1) | (df_merged_pat[var.etCO2].shift(2) > 1) | (df_merged_pat[var.etCO2].shift(3) > 1) | (df_merged_pat[var.etCO2].shift(-1) > 1) | (df_merged_pat[var.etCO2].shift(-2) > 1) | (df_merged_pat[var.etCO2].shift(-3) > 1)  

	#use only timepoints with fiO2 or peep measurements
	df_merged_pat = df_merged_pat.loc[np.logical_not(df_merged_pat[var.fiO2].isnull() & df_merged_pat[var.peep].isnull() & df_merged_pat[var.Leuk].isnull() & df_merged_pat[var.TempCore].isnull() )]
	df_merged_pat.reset_index(inplace=True)

	columns_of_interest = ['PatientID', 'DateTime', var.fiO2, var.peep, var.MechanicalVentilation, var.Leuk, var.TempCore]
	df_small = df_merged_pat[columns_of_interest]

	if len(df_small[df_small[var.MechanicalVentilation]]) > 0:
		df_small.to_hdf(output_data_file, 'pivoted', append=True, complevel=5, complib='blosc:lz4', data_columns=['PatientID', 'VariableID'], format='table')