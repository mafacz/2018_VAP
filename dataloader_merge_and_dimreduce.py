import os
import pandas as pd
import numpy as np

#enviroment variables
data_path = "/cluster/work/grlab/clinical/Inselspital/DataReleases/01-19-2017/InselSpital/"
input_dir = "derived_mafacz/VAP"
output_dir = "derived_mafacz/VAP"
general_data_file = os.path.join(data_path, "1_hdf5_consent/180704", "generaldata.h5")
mon_data_file = os.path.join(data_path, input_dir, "monvals_pivot.h5")
der_data_file = os.path.join(data_path, input_dir, "derv_pivot.h5")
lab_data_file = os.path.join(data_path, input_dir, "labres_pivot.h5")
observ_data_file = os.path.join(data_path, input_dir, "observ_pivot.h5")
output_data_file = os.path.join(data_path, output_dir, "merged_pivot.h5")
output_data_file_5 = os.path.join(data_path, output_dir, "merged_pivot_five_percent.h5")

#Read General File for Patient IDs
df_gen = pd.read_hdf(general_data_file)

#Loop trough all patients and add them to final table after concatiating all tables and joining different columns of same base concepts
i = 0
for patientID in df_gen.PatientID:
	i = i + 1
	print(patientID)
	#concatinate
	df_mon_pat = pd.read_hdf(mon_data_file, where='PatientID==' + str(patientID))
	df_der_pat = pd.read_hdf(der_data_file, where='PatientID==' + str(patientID))
	df_lab_pat = pd.read_hdf(lab_data_file, where='PatientID==' + str(patientID))
	df_obs_pat = pd.read_hdf(observ_data_file, where='PatientID==' + str(patientID))	

	df_concat = pd.concat([df_mon_pat, df_der_pat, df_lab_pat, df_obs_pat], axis=1)
	df_concat.index.names = ['DateTime', 'PatientID']
	
	#reduce dimensionality
	df_concat["SpO2"] = df_concat[['4000', '8280']].mean(axis=1)
	df_concat["etCO2"] = df_concat[['2200', '8290', '30010009']].mean(axis=1)
	df_concat["Temp"] = df_concat[['410', '7100', '400', '7120']].mean(axis=1)

	if len(df_concat.index) > 0:
		df_concat.to_hdf(output_data_file, 'pivoted', append=True, complevel=5, complib='blosc:lz4', data_columns=['PatientID'], format='table')

	if len(df_concat.index) > 0 and i % 20 == 0:
		df_concat.to_hdf(output_data_file_5, 'pivoted', append=True, complevel=5, complib='blosc:lz4', data_columns=['PatientID'], format='table')

	
	

