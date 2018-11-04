import os
import pandas as pd
import numpy as np

#settings
variables_of_interest = ['200', '410', '7100', '400', '2200', '8290', '300', '310', '5685', '2010', '2600', '2610', '3845', '2410', '2400', '3110', '3200', '3000',
	 '320', '7150', '420', '7120', '7110', '7130', '7140', '7150', '430', '3950', '341', '2510','2520','2530', '2910', '2900','3501', '3730']

#enviroment variables
data_path = "/cluster/work/grlab/clinical/Inselspital/DataReleases/01-19-2017/InselSpital/"
input_dir = "1_hdf5_consent/180704"
output_dir = "derived_mafacz/VAP"
general_data_file = os.path.join(data_path, input_dir, "generaldata.h5")
mon_data_file = os.path.join(data_path, input_dir, "monvals.h5")
output_data_file = os.path.join(data_path, output_dir, "monvals_pivot.h5")

#Read General File for Patient IDs
df_gen = pd.read_hdf(general_data_file)

#Loop trough all patients and add them to preprocessed table
for patientID in df_gen.PatientID:
	print(patientID)
	df_mon_pat = pd.read_hdf(mon_data_file, where='PatientID==' + str(patientID))
	df_mon_pat = df_mon_pat[df_mon_pat.Value.notnull()]
	df_mon_pat = df_mon_pat[df_mon_pat.VariableID.isin(variables_of_interest)]
	#pivot table and change to 1Min interval
	#df_mon_pat.Value = df_mon_pat.Value.astype('float64')
	df_mon_pat = df_mon_pat.pivot_table(index='Datetime', columns='VariableID', values='Value') #dupplicate values -> mean
	df_mon_pat = df_mon_pat.resample('1Min').mean()
	df_mon_pat = df_mon_pat.rename(columns={x:str(x) for x in df_mon_pat.columns})
	#add PatientID column
	df_mon_pat['PatientID'] = patientID
	df_mon_pat = df_mon_pat.set_index('PatientID', append=True)
	#add columns of interest which are missing for this patient with nan as value
	missing_columns = set(variables_of_interest) - set(df_mon_pat.columns)
	for col in missing_columns:
		df_mon_pat[col] = np.nan

	if len(df_mon_pat) > 0:
		df_mon_pat.to_hdf(output_data_file, 'pivoted', append=True, complevel=5, complib='blosc:lz4', 
			data_columns=['PatientID', 'VariableID'], format='table')

	
	
