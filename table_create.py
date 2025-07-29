'''
    BC Provincial Disturbance Analysis for Caribou
    
    Author:  JRemond 
    Date:    05-08-2020

    
    Purpose:   To create disturbance layers for caribou values boundaries to be used as input in R scripts to measure disturbance on the landcape
    
    NOTE:  This script is provided as-is.  It is not meant as a 'plug and play' script - i.e. considerable updating and knowledge of the data 
    and assessment assumptions will be required to re-run this script.  Review of each processing step is recommended before running.   
        
    Arguments:   Read comments to see where to change variables
          
    Dependencies:  You must have values boundaries and habitat layers for Caribou. Have access to the BCGW and BCCE disturbance data as well.
     
    Outputs: Features classes and shapefiles of individual disturbance and cumulative disturbance
'''
import arcpy
import os
import re
import json
import logging
import smtplib
import socket
import pandas as pd
import pandasql
## python -m pip install "pandasql"

def combine_loose_sheets(csv_dir,csv_output_name):
    # list the files in the CSV directory
    csv_files = os.listdir(csv_dir)

    flat_files = []
    
    rangename = csv_output_name.rsplit("_")[0]
    
    # if files end with _flat compile the to flat_files
    for files in csv_files:
        if files.endswith(rangename + '_flat.csv'):
            print(files)
            flat_files.append(files)
        else:
            pass

    #####
    df_flat_files = []

    # for the _flat files read them with pandas and append them to the dataframe list (df_flat_files)
    for flat_files_df in flat_files:
        flatfiles_name = flat_files_df.strip('.csv')

        flatfiles_name = pd.read_csv(os.path.join(csv_dir,flat_files_df))

        df_flat_files.append(flatfiles_name)

    # Concat the files together 
    disturb_flat = pd.concat(df_flat_files)

    disturb_flat = disturb_flat.loc[:, ~disturb_flat.columns.str.startswith('FID')]
    print(disturb_flat)
    ##

    # Export the concat files together to a single flat 
    disturb_flat.to_csv(os.path.join(csv_dir ,f"{csv_output_name}.csv"))

def make_sheet_base(intersect_layer, unique_value, aoi_location, csv_dir):
    
    layer = os.path.join(aoi_location, intersect_layer)

    with arcpy.da.SearchCursor(layer, [unique_value]) as cursor:
        ecotypes = sorted({row[0] for row in cursor})
    print('Values that are being selected: {}'.format(ecotypes))

    arcpy.TableToTable_conversion(layer, csv_dir, 'sheet_base.csv')

def static_grouping(csv_dir, csv_output_name, table_group, final_output):
    
    flat_table = pd.read_csv(os.path.join(csv_dir,f"{csv_output_name}.csv"))
    data_top = flat_table .head()
    print(data_top)
    print(flat_table.columns)

    sheet_base = pd.read_csv(os.path.join(csv_dir ,'sheet_base.csv'))
    sheet_base = sheet_base.drop(columns=['Shape_Length', 'Shape_Area'])

    ### Set up the first one to join to the herd table

    ag_df = flat_table.loc[flat_table.disturbances.str.contains("ag", na=False)]
    ag_df = ag_df.groupby(table_group).sum('Shape_Area')
    ag_df = ag_df.drop(columns=['Unnamed: 0', 'OID_', 'Shape_Length', 'Number_Disturbance', 'ORIG_FID', 'latest_cut', 'latest_pest', 'latest_fire'])
    ag_df = ag_df.Shape_Area.div(10000).rename("Agriculture (Ha)")


    static_table = pd.merge(sheet_base, ag_df,  how="outer", left_on = table_group, right_on = table_group)
    print(static_table)

    #### Loop through all other static disturbances to join to the SMC table that started off on line 62
    disturbance_list = ['air', 'dam', 'mining', 'pipe', 'rail', 'reservoir', 'road', 'seismic', 'transmission', 'urban', 'well']
    for disturbance in disturbance_list:
        disturbance_df = disturbance + '_df'
        disturbance_df = flat_table.loc[flat_table.disturbances.str.contains("{}".format(disturbance), na=False)]
        disturbance_df = disturbance_df.groupby(table_group).sum('Shape_Area')
        disturbance_df = disturbance_df.drop(columns=['Unnamed: 0', 'OID_', 'Shape_Length', 'Number_Disturbance', 'ORIG_FID', 'latest_cut', 'latest_pest', 'latest_fire'])
        disturbance_df = disturbance_df.Shape_Area.div(10000).rename("{} (Ha)".format(disturbance))
        
        static_table = pd.merge(static_table, disturbance_df,  how="outer", left_on = table_group, right_on = table_group)
    
    print(static_table)
    # #### Static Disturbance
    static_df = flat_table.loc[flat_table.types.str.contains("Static", na=False)]
    static_df = static_df.groupby(table_group).sum('Shape_Area')
    static_df = static_df.drop(columns=['Unnamed: 0', 'OID_', 'Shape_Length', 'Number_Disturbance', 'ORIG_FID', 'latest_cut', 'latest_pest', 'latest_fire'])
    static_df = static_df.Shape_Area.div(10000).rename("Static (Ha)")

    static_table = pd.merge(static_table, static_df,  how="outer", left_on = table_group, right_on = table_group)

    #### Loop through all the static disturbance buffers

    disturbance_list.append('ag')

    for disturbance_buffer in disturbance_list:
        disturbance_buffer_df = disturbance_buffer + '_df'
        disturbance_buffer_df = flat_table.loc[flat_table.disturbances_buffer.str.contains("{}".format(disturbance_buffer), na=False)]
        disturbance_buffer_df = disturbance_buffer_df.groupby(table_group).sum('Shape_Area')
        disturbance_buffer_df = disturbance_buffer_df.drop(columns=['Unnamed: 0', 'OID_', 'Shape_Length', 'Number_Disturbance', 'ORIG_FID', 'latest_cut', 'latest_pest', 'latest_fire'])
        disturbance_buffer_df = disturbance_buffer_df.Shape_Area.div(10000).rename("{} Buffer (Ha)".format(disturbance_buffer))
        
        static_table = pd.merge(static_table, disturbance_buffer_df,  how="outer", left_on = table_group, right_on = table_group)
        #print(static_table)

    # Cumulative static (buffer) area
    static_buffer_df = flat_table.loc[flat_table.types_buffer.str.contains("Static", na=False)]
    static_buffer_df = static_buffer_df .groupby(table_group).sum('Shape_Area')
    static_buffer_df = static_buffer_df.drop(columns=['Unnamed: 0', 'OID_', 'Shape_Length', 'Number_Disturbance', 'ORIG_FID', 'latest_cut', 'latest_pest', 'latest_fire'])
    static_buffer_df = static_buffer_df.Shape_Area.div(10000).rename("Static (Buffer) (Ha)")

    static_table = pd.merge(static_table, static_buffer_df,  how="outer", left_on = table_group, right_on = table_group)


    # ### Cutblock (no buffer) Temporal Selections ###############

    #flat_table.loc[flat_table.types_buffer.str.contains("Static", na=False)]
    ###past 40 years
    cutblock_40_df = flat_table[flat_table["latest_cut"] > 1981]
    ###past 80 years
    cutblock_80_df = flat_table[flat_table["latest_cut"] > 1941] 
    ##1981 - 1991
    cutblock_81_91_df = flat_table[flat_table["latest_cut"] > 1981] 
    cutblock_81_91_df = cutblock_81_91_df[cutblock_81_91_df["latest_cut"] < 1991]
    ###1981 - 2001
    cutblock_81_01_df = flat_table[flat_table["latest_cut"] > 1981] 
    cutblock_81_01_df = cutblock_81_01_df[cutblock_81_01_df["latest_cut"] < 2001]
    ###1981 - 2011
    cutblock_81_11_df = flat_table[flat_table["latest_cut"] > 1981] 
    cutblock_81_11_df = cutblock_81_11_df[cutblock_81_11_df["latest_cut"] < 2011] 
    ###1981 - 2021
    cutblock_81_21_df = flat_table[flat_table["latest_cut"] > 1981] 
    cutblock_81_21_df = cutblock_81_21_df[cutblock_81_21_df["latest_cut"] < 2021] 
    
    cutblock_selections = {"cut past 40": cutblock_40_df, "cut past 80": cutblock_80_df, "cut 1981-1991": cutblock_81_91_df, "cut 1981-2001" : cutblock_81_01_df, 
                            "cut 1981-2011" : cutblock_81_11_df, "cut 1981-2021" : cutblock_81_21_df}

    # # cutblock_81_01_df, cutblock_81_11_df, cutblock_81_21_df]
    for cutblock_df_name, cutblock_df_layer in zip(cutblock_selections.keys(), cutblock_selections.values()):
        cutblock_df_layer = cutblock_df_layer.groupby(table_group).sum('Shape_Area')
        cutblock_df_layer = cutblock_df_layer.drop(columns=['Unnamed: 0', 'OID_', 'Shape_Length', 'Number_Disturbance', 'ORIG_FID', 'latest_cut', 'latest_pest', 'latest_fire'])
        cutblock_df_layer = cutblock_df_layer.Shape_Area.div(10000).rename("{} (Ha)".format(cutblock_df_name))
 

        static_table = pd.merge(static_table, cutblock_df_layer,  how="outer", left_on = table_group, right_on = table_group)

    # Cutlbock (BUFFER) Temporal Selections #####################
    ## past 40 years
    cutblock_buffer_40_df = flat_table[flat_table["latest_cut_buffer"] > 1981]
    ## past 80 years
    cutblock_buffer_80_df = flat_table[flat_table["latest_cut_buffer"] > 1941] 
    ## 1981 - 1991
    cutblock_buffer_81_91_df = flat_table[flat_table["latest_cut_buffer"] > 1981] 
    cutblock_buffer_81_91_df = cutblock_buffer_81_91_df[cutblock_buffer_81_91_df["latest_cut_buffer"] < 1991]
    ## 1981 - 2001
    cutblock_buffer_81_01_df = flat_table[flat_table["latest_cut_buffer"] > 1981] 
    cutblock_buffer_81_01_df = cutblock_buffer_81_01_df[cutblock_buffer_81_01_df["latest_cut_buffer"] < 2001]
    ## 1981 - 2011
    cutblock_buffer_81_11_df = flat_table[flat_table["latest_cut_buffer"] > 1981] 
    cutblock_buffer_81_11_df = cutblock_buffer_81_11_df[cutblock_buffer_81_11_df["latest_cut_buffer"] < 2011] 
    ## 1981 - 2021
    cutblock_buffer_81_21_df =flat_table[flat_table["latest_cut_buffer"]> 1981] 
    cutblock_buffer_81_21_df = cutblock_buffer_81_21_df[cutblock_buffer_81_21_df["latest_cut_buffer"] < 2021] 
    
    cutblock_buffer_selections = {"cut past 40 (buffer)": cutblock_buffer_40_df, "cut past 80 (buffer)": cutblock_buffer_80_df, "cut 1981-1991 (buffer)": cutblock_buffer_81_91_df, 
                                "cut 1981-2001 (buffer)" : cutblock_buffer_81_01_df, "cut 1981-2011 (buffer)" : cutblock_buffer_81_11_df, "cut 1981-2021 (buffer)" : cutblock_buffer_81_21_df}

    for cutblock_buffer_df_name, cutblock_buffer_df_layer in zip(cutblock_buffer_selections.keys(), cutblock_buffer_selections.values()):
        cutblock_buffer_df_layer = cutblock_buffer_df_layer.groupby(table_group).sum('Shape_Area')
        cutblock_buffer_df_layer = cutblock_buffer_df_layer.drop(columns=['Unnamed: 0', 'OID_', 'Shape_Length', 'Number_Disturbance', 'ORIG_FID', 'latest_cut', 'latest_pest', 'latest_fire'])
        cutblock_buffer_df_layer = cutblock_buffer_df_layer.Shape_Area.div(10000).rename("{} (Ha)".format(cutblock_buffer_df_name))

        static_table = pd.merge(static_table, cutblock_buffer_df_layer,  how="outer", left_on = table_group, right_on = table_group)
    
    # # ####### PEST SELECTION #########################
    ## past 40 years
    pest_40_df = flat_table[flat_table["latest_pest"] > 1981]
    ## past 80 years
    pest_80_df = flat_table[flat_table["latest_pest"] > 1941] 
    ## 1981 - 1991
    pest_81_91_df = flat_table[flat_table["latest_pest"] > 1981] 
    pest_81_91_df = pest_81_91_df[pest_81_91_df["latest_pest"] < 1991]
    ## 1981 - 2001
    pest_81_01_df = flat_table[flat_table["latest_pest"] > 1981] 
    pest_81_01_df = pest_81_01_df[pest_81_01_df["latest_pest"] < 2001]
    ## 1981 - 2011
    pest_81_11_df = flat_table[flat_table["latest_pest"] > 1981] 
    pest_81_11_df = pest_81_11_df[pest_81_11_df["latest_pest"] < 2011] 
    ## 1981 - 2021
    pest_81_21_df = flat_table[flat_table["latest_pest"] > 1981] 
    pest_81_21_df = pest_81_21_df[pest_81_21_df["latest_pest"] < 2021] 
    
    pest_selections = {"pest past 40": pest_40_df, "pest past 80": pest_80_df, "pest 1981-1991": pest_81_91_df, "pest 1981-2001" : pest_81_01_df, 
                            "pest 1981-2011" : pest_81_11_df, "pest 1981-2021" : pest_81_21_df}
    
    for pest_df_name, pest_df_layer in zip(pest_selections.keys(), pest_selections.values()):
        pest_df_layer = pest_df_layer.groupby(table_group).sum('Shape_Area')
        pest_df_layer = pest_df_layer.drop(columns=['Unnamed: 0', 'OID_', 'Shape_Length', 'Number_Disturbance', 'ORIG_FID', 'latest_cut', 'latest_pest', 'latest_fire'])
        pest_df_layer = pest_df_layer.Shape_Area.div(10000).rename("{} (Ha)".format(pest_df_name))

        static_table = pd.merge(static_table, pest_df_layer,  how="outer", left_on = table_group, right_on = table_group)

    # # ####### FIRE SELECTION #########################
    ## past 40 years
    fire_40_df = flat_table[flat_table["latest_fire"] > 1981]
    ## past 80 years
    fire_80_df = flat_table[flat_table["latest_fire"] > 1941] 
    ## 1981 - 1991
    fire_81_91_df = flat_table[flat_table["latest_fire"] > 1981] 
    fire_81_91_df = fire_81_91_df[fire_81_91_df["latest_fire"] < 1991]
    ## 1981 - 2001
    fire_81_01_df = flat_table[flat_table["latest_fire"] > 1981] 
    fire_81_01_df = fire_81_01_df[fire_81_01_df["latest_fire"] < 2001]
    ## 1981 - 2011
    fire_81_11_df = flat_table[flat_table["latest_fire"] > 1981] 
    fire_81_11_df = fire_81_11_df[fire_81_11_df["latest_fire"] < 2011] 
    ## 1981 - 2021
    fire_81_21_df = flat_table[flat_table["latest_fire"] > 1981] 
    fire_81_21_df = fire_81_21_df[fire_81_21_df["latest_fire"] < 2021] 
    
    fire_selections = {"fire past 40": fire_40_df, "fire past 80": fire_80_df, "fire 1981-1991": fire_81_91_df, "fire 1981-2001" : fire_81_01_df, 
                            "fire 1981-2011" : fire_81_11_df, "fire 1981-2021" : fire_81_21_df}
    
    for fire_df_name, fire_df_layer in zip(fire_selections.keys(), fire_selections.values()):
        fire_df_layer = fire_df_layer.groupby(table_group).sum('Shape_Area')
        fire_df_layer = fire_df_layer.drop(columns=['Unnamed: 0', 'OID_', 'Shape_Length', 'Number_Disturbance', 'ORIG_FID', 'latest_cut', 'latest_pest', 'latest_fire'])
        fire_df_layer = fire_df_layer.Shape_Area.div(10000).rename("{} (Ha)".format(fire_df_name))

        static_table = pd.merge(static_table, fire_df_layer,  how="outer", left_on = table_group, right_on = table_group)

    # Remove duplicate fields
    flat_table = flat_table.drop(columns=['Area_Ha', 'area_ha_1', 'area_ha_12'])
    
    ## cumulative selection - no pest - no buffer
    cumulative_np_81_21 = pandasql.sqldf("SELECT * FROM flat_table WHERE types LIKE '%Static%' OR latest_fire > 1981 OR latest_cut > 1981")
    cumulative_np_81_11 = pandasql.sqldf("SELECT * FROM flat_table WHERE types LIKE '%Static%' OR (latest_fire > 1981 AND latest_fire < 2011) OR (latest_cut > 1981 AND latest_cut < 2011)")
    cumulative_np_81_01 = pandasql.sqldf("SELECT * FROM flat_table WHERE types LIKE '%Static%' OR (latest_fire > 1981 AND latest_fire < 2001) OR (latest_cut > 1981 AND latest_cut < 2001)")
    cumulative_np_81_91 = pandasql.sqldf("SELECT * FROM flat_table WHERE types LIKE '%Static%' OR (latest_fire > 1981 AND latest_fire < 1991) OR (latest_cut > 1981 AND latest_cut < 1991)")

    # # # cumulative selection - no pest - buffer
    cumulative_buffer_np_81_21 = pandasql.sqldf("SELECT * FROM flat_table WHERE types_buffer LIKE '%Static%' OR latest_fire > 1981 OR latest_cut_buffer > 1981")
    cumulative_buffer_np_81_11 = pandasql.sqldf("SELECT * FROM flat_table WHERE types_buffer LIKE '%Static%' OR (latest_fire > 1981 AND latest_fire < 2011) OR (latest_cut_buffer > 1981 AND latest_cut_buffer < 2011)")
    cumulative_buffer_np_81_01 = pandasql.sqldf("SELECT * FROM flat_table WHERE types_buffer LIKE '%Static%' OR (latest_fire > 1981 AND latest_fire < 2001) OR (latest_cut_buffer > 1981 AND latest_cut_buffer < 2001)")
    cumulative_buffer_np_81_91 = pandasql.sqldf("SELECT * FROM flat_table WHERE types_buffer LIKE '%Static%' OR (latest_fire > 1981 AND latest_fire < 1991) OR (latest_cut_buffer > 1981 AND latest_cut_buffer < 1991)")

    # # # cumulative selection - pest - no buffer
    cumulative_p_81_21 = pandasql.sqldf("SELECT * FROM flat_table WHERE types LIKE '%Static%' OR latest_fire > 1981 OR latest_cut > 1981 OR latest_pest > 1981")
    cumulative_p_81_11 = pandasql.sqldf("SELECT * FROM flat_table WHERE types LIKE '%Static%' OR (latest_fire > 1981 AND latest_fire < 2011) OR (latest_cut > 1981 AND latest_cut < 2011) OR (latest_pest > 1981 AND latest_pest < 2011)")
    cumulative_p_81_01 = pandasql.sqldf("SELECT * FROM flat_table WHERE types LIKE '%Static%' OR (latest_fire > 1981 AND latest_fire < 2001) OR (latest_cut > 1981 AND latest_cut < 2001) OR (latest_pest > 1981 AND latest_pest < 2001)")
    cumulative_p_81_91 = pandasql.sqldf("SELECT * FROM flat_table WHERE types LIKE '%Static%' OR (latest_fire > 1981 AND latest_fire < 1991) OR (latest_cut > 1981 AND latest_cut < 1991) OR (latest_pest > 1981 AND latest_pest < 1991)")

    # # # cumulative selection - pest - buffer
    cumulative_buffer_p_81_21 = pandasql.sqldf("SELECT * FROM flat_table WHERE types_buffer LIKE '%Static%' OR latest_fire > 1981 OR latest_cut_buffer > 1981 OR latest_pest > 1981")
    cumulative_buffer_p_81_11 = pandasql.sqldf("SELECT * FROM flat_table WHERE types_buffer LIKE '%Static%' OR (latest_fire > 1981 AND latest_fire < 2011) OR (latest_cut_buffer > 1981 AND latest_cut_buffer < 2011) OR (latest_pest > 1981 AND latest_pest < 2011)")
    cumulative_buffer_p_81_01 = pandasql.sqldf("SELECT * FROM flat_table WHERE types_buffer LIKE '%Static%' OR (latest_fire > 1981 AND latest_fire < 2001) OR (latest_cut_buffer > 1981 AND latest_cut_buffer < 2001) OR (latest_pest > 1981 AND latest_pest < 2001)")
    cumulative_buffer_p_81_91 = pandasql.sqldf("SELECT * FROM flat_table WHERE types_buffer LIKE '%Static%' OR (latest_fire > 1981 AND latest_fire < 1991) OR (latest_cut_buffer > 1981 AND latest_cut_buffer < 1991) OR (latest_pest > 1981 AND latest_pest < 1991)")

    cumulative_selections = {"cumulative no pest no buffer 1981-2021":cumulative_np_81_21, "cumulative no pest no buffer 1981-2011":cumulative_np_81_11, 
                            "cumulative no pest no buffer 1981-2001":cumulative_np_81_01, "cumulative no pest no buffer 1981-1991":cumulative_np_81_91,
                            "cumulative no pest buffer 1981-2021":cumulative_buffer_np_81_21, "cumulative no pest buffer 1981-2011":cumulative_buffer_np_81_11, 
                            "cumulative no pest buffer 1981-2001":cumulative_buffer_np_81_01, "cumulative no pest buffer 1981-1991":cumulative_buffer_np_81_91,
                            "cumulative pest no buffer 1981-2021":cumulative_p_81_21, "cumulative pest no buffer 1981-2011":cumulative_p_81_11, 
                            "cumulative pest no buffer 1981-2001":cumulative_p_81_01, "cumulative pest no buffer 1981-1991":cumulative_p_81_91,
                            "cumulative pest buffer 1981-2021":cumulative_buffer_p_81_21, "cumulative pest buffer 1981-1991":cumulative_buffer_p_81_91,
                            "cumulative pest buffer 1981-2001":cumulative_buffer_p_81_01, "cumulative pest buffer 1981-2011":cumulative_buffer_p_81_11}

    for cumulative_selections_name, cumulative_selections_layer in zip(cumulative_selections.keys(), cumulative_selections.values()):
        cumulative_selections_layer  = cumulative_selections_layer.groupby(table_group).sum('Shape_Area')
        # Might not have latest pest depending on severity cutoff level
        cumulative_selections_layer  = cumulative_selections_layer.drop(columns=['Unnamed: 0', 'OID_', 'Shape_Length', 'Number_Disturbance', 'ORIG_FID'])
        if 'latest_pest' in cumulative_selections_layer.columns:
            cumulative_selections_layer  = cumulative_selections_layer.drop(columns=['latest_pest'])
        if 'latest_fire' in cumulative_selections_layer.columns:
            cumulative_selections_layer  = cumulative_selections_layer.drop(columns=['latest_fire'])
        if 'latest_cut' in cumulative_selections_layer.columns:
            cumulative_selections_layer  = cumulative_selections_layer.drop(columns=['latest_cut'])    
        cumulative_selections_layer  = cumulative_selections_layer.Shape_Area.div(10000).rename("{} (Ha)".format(cumulative_selections_name))

        static_table = pd.merge(static_table, cumulative_selections_layer,  how="outer", left_on = table_group, right_on = table_group)

    print(static_table.head())

    static_table.to_csv(os.path.join(csv_dir, f"{final_output}.csv"))