import os
import pandas as pd
import numpy as np

#settings
variables_of_interest = ['30010009']

#enviroment variables
data_path = "/cluster/work/grlab/clinical/Inselspital/DataReleases/01-19-2017/InselSpital/"
input_dir = "1_hdf5_consent/180704"
output_dir = "derived_mafacz/VAP"
general_data_file = os.path.join(data_path, input_dir, "generaldata.h5")
derv_data_file = os.path.join(data_path, input_dir, "dervals.h5")
output_data_file = os.path.join(data_path, output_dir, "derv_pivot.h5")

#Read General File for Patient Ids
df_gen = pd.read_hdf(general_data_file)

#Loop trough all patients and add them to preprocessed table
for patientID in df_gen.PatientID:
	print(patientID)
	df_derv_pat = pd.read_hdf(derv_data_file, where='PatientID==' + str(patientID))
	df_derv_pat = df_derv_pat[df_derv_pat.Value.notnull()]
	df_derv_pat = df_derv_pat[df_derv_pat.VariableID.isin(variables_of_interest)]
	#pivot table and change to 1Min interval
	df_derv_pat = df_derv_pat.pivot_table(index='Datetime', columns='VariableID', values='Value')
	df_derv_pat = df_derv_pat.rename(columns={x:str(x) for x in df_derv_pat.columns})
	df_derv_pat = df_derv_pat.resample('1Min').mean()
	#add PatientID column
	df_derv_pat['PatientID'] = patientID
	df_derv_pat = df_derv_pat.set_index('PatientID', append=True)
	#add columns of interest which are missing for this patient with nan as value
	missing_columns = set(variables_of_interest) - set(df_derv_pat.columns)
	for col in missing_columns:
		df_derv_pat[col] = np.nan
	
	if len(df_derv_pat) > 0:
		df_derv_pat.to_hdf(output_data_file, 'pivoted', append=True, complevel=5, complib='blosc:lz4', 
                data_columns=['PatientID', 'VariableID'], format='table')
