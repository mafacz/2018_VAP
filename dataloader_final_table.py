import os
import pandas as pd
import numpy as np
import variable_references as var

#enviroment variables
data_path = "/cluster/work/grlab/clinical/Inselspital/DataReleases/01-19-2017/InselSpital/"
input_dir = "paO2_prediction"
output_dir = "paO2_prediction"
general_data_file = os.path.join(data_path, "1_hdf5_consent/180704", "generaldata.h5")
merged_data_file = os.path.join(data_path, input_dir, "merged_pivot.h5")
output_data_file = os.path.join(data_path, output_dir, "final_pivot.h5")

#Read General File for Patient IDs
df_gen = pd.read_hdf(general_data_file)

def AllEqual(values):
    return (len(np.unique(values[np.logical_not(np.isnan(values))])) == 1)

def CheckAndStoreRollingEquality(min_period, datacolumn_source, datacolumn_target, dataframe):
    dataframe[datacolumn_target] = dataframe[datacolumn_source].fillna(method='pad').rolling(min_period, min_periods=min_period).apply(AllEqual).fillna(0)

def StoreRollingMean(period_min, datacolumn_source, dataframe):
    dataframe[str(datacolumn_source) + '_mean'] = dataframe[datacolumn_source].rolling(period_min, min_periods=1).mean()

def StoreRollingCenterMean(period_min, datacolumn_source, dataframe):
    dataframe[str(datacolumn_source) + '_mean'] = dataframe[datacolumn_source].rolling(period_min*2, min_periods=1, center=True).mean()

def AddFilledColumn(min_period, dataframe, columnName):
    dataframe[str(columnName) + '_filled'] = dataframe[columnName].fillna(method='ffill', limit=min_period)

#Loop trough all patients and add them to final table after concatiating all tables and joining different columns of same base concepts
excludeUnrealisticCount = 0
excludeVBGACount = 0
for patientID in df_gen[df_gen.AdmissionTime > '2008.1.1'].PatientID:
	print(patientID)
	#concatinate
	df_merged_pat = pd.read_hdf(merged_data_file, where='PatientID==' + str(patientID))
	# import pdb; pdb.set_trace()

	#Check if ventilation settings are constant within last 15min
	CheckAndStoreRollingEquality(15, var.fiO2, 'fiO2_15min_const', df_merged_pat)
	CheckAndStoreRollingEquality(15, var.peep, "peep_15min_const", df_merged_pat)
	CheckAndStoreRollingEquality(15, var.TV, "tv_15min_const", df_merged_pat)
	CheckAndStoreRollingEquality(15, var.RRset, "RR_15min_const", df_merged_pat)
	CheckAndStoreRollingEquality(15, var.VentMode, "VentMode_15min_const", df_merged_pat)
	df_merged_pat['Ventilation_15min_const'] = (df_merged_pat['VentMode_15min_const'] == 1) & (df_merged_pat['fiO2_15min_const'] == 1) & (df_merged_pat['peep_15min_const'] == 1) & (df_merged_pat['tv_15min_const'] == 1) & (df_merged_pat['RR_15min_const'] == 1)
	df_merged_pat.drop(['fiO2_15min_const', 'peep_15min_const', 'tv_15min_const', 'RR_15min_const', 'VentMode_15min_const'], inplace=True, axis=1)	

	#Forewardfill columns
	AddFilledColumn(15, df_merged_pat, var.fiO2)
	AddFilledColumn(15, df_merged_pat, var.SvO2)
	AddFilledColumn(2, df_merged_pat, var.SpO2)
	
	#Mean measurements
	StoreRollingMean(10, var.SpO2, df_merged_pat)
	StoreRollingCenterMean(15, var.TempCore, df_merged_pat)
	StoreRollingMean(10, var.etCO2, df_merged_pat)

	#TempCore measurement windowing (TempCoreMean of 15min if available otherwise 4h)
	df_merged_pat['temp_ffilled'] = df_merged_pat[var.TempCore].fillna(method='ffill', limit=240)
	df_merged_pat['temp_bfilled'] = df_merged_pat[var.TempCore].fillna(method='bfill', limit=240)
	df_merged_pat[var.TempCore_4h] = df_merged_pat[var.TempCore_mean]
	noTempRows = df_merged_pat[var.TempCore_4h].isnull()
	df_merged_pat.loc[noTempRows, var.TempCore_4h] = np.nanmean(df_merged_pat.loc[noTempRows, ['temp_ffilled','temp_bfilled']], axis=1)
	df_merged_pat.drop(['temp_ffilled','temp_bfilled'], axis=1, inplace=True)

	#use only timepoints with paO2 measurements
	df_merged_pat = df_merged_pat.loc[np.logical_not(df_merged_pat[var.apO2].isnull())]

	countBeforeUnrealistic = len(df_merged_pat)
	#filter for realistic values
	if len(df_merged_pat) == 0: print ('no abga')
	df_merged_pat = df_merged_pat[(df_merged_pat[var.apH] <= 7.8) & (df_merged_pat[var.apH] > 6.5)]
	if len(df_merged_pat) == 0: print ('ph')
	df_merged_pat = df_merged_pat[(df_merged_pat[var.aSO2] <= 100) & (df_merged_pat[var.aSO2] > 30)]
	if len(df_merged_pat) == 0: print ('aSo2')
	df_merged_pat = df_merged_pat[(df_merged_pat[var.aHCO3] <= 40) & (df_merged_pat[var.aHCO3] > 10)]
	if len(df_merged_pat) == 0: print ('bicarb')
	df_merged_pat = df_merged_pat[(df_merged_pat[var.aHb] <= 200) & (df_merged_pat[var.aHb] > 20)]
	if len(df_merged_pat) == 0: print ('hb')
	df_merged_pat = df_merged_pat[(df_merged_pat[var.apCO2] <= 100) & (df_merged_pat[var.apCO2] > 3)]
	if len(df_merged_pat) == 0: print ('pco2')
	df_merged_pat = df_merged_pat[(df_merged_pat[var.apO2] <= 800) & (df_merged_pat[var.apO2] > 20)]
	if len(df_merged_pat) == 0: print ('po2')
	df_merged_pat = df_merged_pat[(df_merged_pat[var.aMetHb] <= 20) & (df_merged_pat[var.aMetHb] > 0)]
	if len(df_merged_pat) == 0: print ('aMetHb')
	df_merged_pat = df_merged_pat[(df_merged_pat[var.aBE] <= 30) & (df_merged_pat[var.aBE] > -30)]
	if len(df_merged_pat) == 0: print ('abe')
	df_merged_pat = df_merged_pat[(df_merged_pat[var.aCOHb] <= 20) & (df_merged_pat[var.aCOHb] > 0)]
	if len(df_merged_pat) == 0: print ('aCOHb')
	df_merged_pat = df_merged_pat[(df_merged_pat[var.aLac] <= 25) & (df_merged_pat[var.aLac] > 0)]
	if len(df_merged_pat) == 0: print ('alac')
	df_merged_pat = df_merged_pat[(((df_merged_pat[var.SpO2] <= 100) & (df_merged_pat[var.SpO2] >= 30)) | df_merged_pat[var.SpO2].isnull())]
	df_merged_pat = df_merged_pat[(((df_merged_pat[var.SvO2] <= 90) & (df_merged_pat[var.SvO2] >= 30)) | df_merged_pat[var.SvO2].isnull())]
	df_merged_pat = df_merged_pat[(((df_merged_pat[var.TempCore] <= 45) & (df_merged_pat[var.TempCore] >= 25)) | df_merged_pat[var.TempCore].isnull())]
	df_merged_pat = df_merged_pat[(((df_merged_pat[var.etCO2] <= 100) & (df_merged_pat[var.etCO2] >= 10)) | df_merged_pat[var.etCO2].isnull())]
	df_merged_pat = df_merged_pat[(((df_merged_pat[var.fiO2] <= 100) & (df_merged_pat[var.fiO2] >= 20)) | df_merged_pat[var.fiO2].isnull())]
	df_merged_pat = df_merged_pat[(((df_merged_pat[var.peep] <= 25) & (df_merged_pat[var.peep] >= 0)) | df_merged_pat[var.peep].isnull())]
	excludeUnrealisticCount += countBeforeUnrealistic - len(df_merged_pat)	

	#derived values for filtering
	df_merged_pat[var.MechanicalVentilation] = False | (df_merged_pat[var.etCO2] > 1) | (df_merged_pat[var.etCO2].shift(1) > 1) | (df_merged_pat[var.etCO2].shift(2) > 1) | (df_merged_pat[var.etCO2].shift(3) > 1) | (df_merged_pat[var.etCO2].shift(-1) > 1) | (df_merged_pat[var.etCO2].shift(-2) > 1) | (df_merged_pat[var.etCO2].shift(-3) > 1)  
	df_merged_pat[var.vBGA_Filter] = (df_merged_pat[var.SvO2_filled] > 0) & (df_merged_pat[var.aSO2] > 0) & (abs(df_merged_pat[var.SvO2_filled] - df_merged_pat[var.aSO2]) < 10)
	excludeVBGACount += len(df_merged_pat[df_merged_pat[var.vBGA_Filter] == True])
	df_merged_pat = df_merged_pat[df_merged_pat[var.vBGA_Filter] == False]

	#store last bga result with following dataset
	df_merged_pat.reset_index(inplace=True)
	df_merged_pat = df_merged_pat.sort_values(by=['DateTime'])	
	df_merged_pat[var.aSO2_last] = df_merged_pat[var.aSO2].shift(1)
	df_merged_pat[var.apO2_last] = df_merged_pat[var.apO2].shift(1)
	df_merged_pat[var.aHb_last] = df_merged_pat[var.aHb].shift(1)
	df_merged_pat[var.aMetHb_last] = df_merged_pat[var.aMetHb].shift(1)
	df_merged_pat[var.aCOHb_last] = df_merged_pat[var.aCOHb].shift(1)
	df_merged_pat[var.apH_last] = df_merged_pat[var.apH].shift(1)
	df_merged_pat[var.apCO2_last] = df_merged_pat[var.apCO2].shift(1)
	df_merged_pat[var.aBE_last] = df_merged_pat[var.aBE].shift(1)
	df_merged_pat[var.aHCO3_last] = df_merged_pat[var.aHCO3].shift(1)
	df_merged_pat[var.aLac_last] = df_merged_pat[var.aLac].shift(1)
	df_merged_pat[var.LastBGADate] = pd.to_datetime(df_merged_pat.DateTime.shift(1))
	
	if len(df_merged_pat) > 0:
		df_merged_pat.to_hdf(output_data_file, 'pivoted', append=True, complevel=5, complib='blosc:lz4', data_columns=['PatientID'], format='table')
	else:
		print('skip o rows')

print("Unrealisitc ABGAS excluded: " + str(excludeUnrealisticCount))
print("vBGAS excluded: " + str(excludeVBGACount))
	
	

