import os
import pandas as pd
import numpy as np

#settings
variables_of_interest = ['15001555', '15001525', '15001552', '15002241', '15002241','10002308','10002502', '10002523']

#enviroment variables
data_path = "/cluster/work/grlab/clinical/Inselspital/DataReleases/01-19-2017/InselSpital/"
input_dir = "1_hdf5_consent/180704"
output_dir = "derived_mafacz/VAP"
general_data_file = os.path.join(data_path, input_dir, "generaldata.h5")
observ_data_file = os.path.join(data_path, input_dir, "observrec.h5")
output_data_file = os.path.join(data_path, output_dir, "observ_pivot.h5")

#Read General File for Patient IDss
df_gen = pd.read_hdf(general_data_file)

#Loop trough all patients and add them to preprocessed table
for patientID in df_gen.PatientID:
	print(patientID)
	df_observ_pat = pd.read_hdf(observ_data_file, where='PatientID==' + str(patientID))
	df_observ_pat = df_observ_pat[df_observ_pat.Value.notnull()]
	df_observ_pat = df_observ_pat[df_observ_pat.VariableID.isin(variables_of_interest)]
        #pivot table and change to 1Min interval
	df_observ_pat = df_observ_pat.pivot_table(index='DateTime', columns='VariableID', values='Value')
	df_observ_pat = df_observ_pat.rename(columns={x:str(x) for x in df_observ_pat.columns})
	df_observ_pat = df_observ_pat.resample('1Min').mean()
	#add PatientID column
	df_observ_pat['PatientID'] = patientID
	df_observ_pat = df_observ_pat.set_index('PatientID', append=True)
	#add columns of interest which are missing for this patient with nan as value
	missing_columns = set(variables_of_interest) - set(df_observ_pat.columns)
	for col in missing_columns:
		df_observ_pat[col] = np.nan
	
	if len(df_observ_pat) > 0:
		df_observ_pat.to_hdf(output_data_file, 'pivoted', append=True, complevel=5, complib='blosc:lz4', 
                data_columns=['PatientID', 'VariableID'], format='table')

	
	
