import os
import pandas as pd
import numpy as np

#settings
variables_of_interest = ['20000700']

#enviroment variables
data_path = "/cluster/work/grlab/clinical/Inselspital/DataReleases/01-19-2017/InselSpital/"
input_dir = "1_hdf5_consent/180704"
output_dir = "derived_mafacz/VAP"
general_data_file = os.path.join(data_path, input_dir, "generaldata.h5")
lab_data_file = os.path.join(data_path, input_dir, "labres.h5")
output_data_file = os.path.join(data_path, output_dir, "labres_pivot.h5")

#Read General File for Patient IDs
df_gen = pd.read_hdf(general_data_file)

#Loop trough all patients and add them to preprocessed table
for patientID in df_gen.PatientID:
	print(patientID)
	df_lab_pat = pd.read_hdf(lab_data_file, where='PatientID==' + str(patientID))
	df_lab_pat = df_lab_pat[df_lab_pat.Value.notnull()]
	df_lab_pat = df_lab_pat[df_lab_pat.VariableID.isin(variables_of_interest)]
        #pivot table and change to 1Min interval
	df_lab_pat = df_lab_pat.pivot_table(index='SampleTime', columns='VariableID', values='Value') #dupplicate values->mean
	df_lab_pat = df_lab_pat.rename(columns={x:str(x) for x in df_lab_pat.columns})
	df_lab_pat = df_lab_pat.resample('1Min').mean()
	#add PatientID column
	df_lab_pat['PatientID'] = patientID
	df_lab_pat = df_lab_pat.set_index('PatientID', append=True)
	#add columns of interest which are missing for this patient with nan as value
	missing_columns = set(variables_of_interest) - set(df_lab_pat.columns)
	for col in missing_columns:
		df_lab_pat[col] = np.nan
	
	if len(df_lab_pat) > 0:
		df_lab_pat.to_hdf(output_data_file, 'pivoted', append=True, complevel=5, complib='blosc:lz4', 
                data_columns=['PatientID', 'VariableID'], format='table')
	
	
