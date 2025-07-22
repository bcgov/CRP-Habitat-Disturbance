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
def tabletotable(value_update, csv_dir):
    arcpy.TableToTable_conversion("{}_protect_flat".format(value_update), csv_dir, "{}_protect_flat.csv".format(value_update))
def combine_loose_herds(csv_dir, value_update,csv_protect_output):
    # list the files in the CSV directory
    csv_files = os.listdir(csv_dir)

    flat_files = []
    df_flat_files = []
    for files in csv_files:
        if files.endswith('_protect_flat.csv'):
            if files.startswith(value_update):
                print(value_update)
                print(files)
                flat_files.append(files)
            else:
                pass
        else:
            pass

    for flat_files_df in flat_files:

        print(flat_files_df)
        flatfiles_name = flat_files_df.strip('.csv')

        flatfiles_name = pd.read_csv(csv_dir + flat_files_df)

        df_flat_files.append(flatfiles_name)
    
    protect_flat = pd.concat(df_flat_files)
    ###
    protect_flat = protect_flat.loc[:, ~protect_flat.columns.str.startswith('FID')]
    print(protect_flat)
    ##

    protect_flat.to_csv(csv_dir + csv_protect_output + ".csv")
    
def protection_grouping(csv_dir, csv_protect_output, table_group):
    flat = pd.read_csv(csv_dir + csv_protect_output + ".csv")

    herd_base = pd.read_csv(csv_dir + 'sheet_base.csv')
    herd_base = herd_base.drop(columns=['Shape_Length', 'Shape_Area'])

    park_national = flat.loc[flat.designations.str.contains("park_national", na=False)]
    park_national = park_national.groupby(table_group).sum('Shape_Area')
    park_national = park_national.Shape_Area.div(10000).rename("National Parks")

    flat_protections = pd.merge(herd_base, park_national,  how="outer", left_on = table_group, right_on = table_group)
    
    print(flat_protections)

    protection_list = {'park_er' : 'Ecological Reserves' , 'park_provincial' : 'Provincial Parks' , 'park_conservancy' : 'Conservancies' , 'park_protectedarea' : 'Protected Areas' , 'park_recreationarea' : 'Recreation Area' , 
    'private_conservation_lands_admin' : 'Conservation Lands, Administered Lands' , 'wildlife_management_area' : 'Wildlife Management Areas' , 'creston_valley_wma' : 'Creston Valley Wildlife Management Area' , 'national_wildlife_area' : 'National Wildlife Area' , 
    'ngo_fee_simple' : 'NGO Fee Simple Conservation Lands' ,'migratory_bird_sanctuary' : 'Migratory Bird Sanctuary' , 'mineral_reserve' : 'Mineral Reserve Sites' , 'uwr_no_harvest' : 'Ungulate Winter Range, No Harvest' , 'wha_no_harvest' : 'Wildlife Habitat Areas, No Harvest' , 
     'biodiv_mining_tourism_areas' : 'Biodiversity Mining and Tourism Areas' , 'wildland_area' : 'Sea to Sky Wildland Area' , 'muskwa_kechika_special_wildland' : 'Special Wildland RMZ in Muskwa Kechika MA' , 'ogma' : 'Old Growth Management Areas (Legal and Non-Legal)' , 
     'vqo_preserve' : 'VQO Preserves' , 'designated_area' : 'Designated Area' , 'flathead' : 'Flathead Watershed Area' , 'great_bear_grizzly_class1' : 'Class1 Coast Grizzly Bear Habitat (Coast LUP)' , 'nlhaxten_cayoosh_wildland_area' : 'Nlhaxten/Cayoosh Wildland Area' , 
     's_chilcotin_mta' : 'South Chilctoin Mining and Tourism Area' , 'rec_site_med' : 'Forest Rec Sites (Medium)' , 'rec_site_high' : 'Forest Rec Sites (High)' , 'vqo_retain' : 'VQO Retention' , 'muskwa_kechika_special_mgmt' : 'Special Management RMZ in Muskwa Kechika MA' , 
     'uwr_conditional_harvest' : 'Ungulate Winter Range, Conditional Harvest' , 'wha_conditional_harvest' : 'Wildlife Habitat Areas, Conditional harvest' , 'vqo_partretain' : 'VQO Partial Retention' , 'vqo_modify' : 'VQO Modify' , 
     'community_watershed' : 'Community Watersheds' , 'vqo_maxmodify' : 'VQO Maximum Modify' , 'great_bear_grizzly_class2' : 'Class2 Coast Grizzly Bear Habitat' , 'lakes_corridors' : "Lakes South Landscape Corridor",
      'fsw' : 'Fisheries Sensitive Watersheds' , 'great_bear_fisheries_watersheds' : 'Important Fisheries Watersheds (Great Bear Rainforest LUO)' , 'great_bear_ebm_area' : 'GBRO Area (Great Bear Rainforest LUO EBM Areas)' , 
      'lrmp_hg' : 'Haida Gwaii EBM Areas (unprotected areas on Haida Gwaii)' , 'atlin_taku_fra' : 'Atlin-Taku Forest Retention Areas'}

    for protection, protection_name  in zip(protection_list.keys(), protection_list.values()):
        protection_df = protection + '_df'
        protection_df = flat.loc[flat.designations.str.contains("{}".format(protection), na=False)]
        protection_df = protection_df.groupby(table_group).sum('Shape_Area')
        protection_df = protection_df.Shape_Area.div(10000).rename("{}".format(protection_name))

        flat_protections = pd.merge(flat_protections, protection_df,  how="outer", left_on = table_group, right_on = table_group)

        print(flat_protections)
    
    range_out = csv_protect_output.replace('protect','')
    flat_protections.to_csv(csv_dir + range_out + "protections_flat.csv")
    
def protection_classes(csv_dir, csv_protect_output, table_group):

    flat = pd.read_csv(csv_dir + csv_protect_output + ".csv")

    herd_base = pd.read_csv(csv_dir + 'sheet_base.csv')
    herd_base = herd_base.drop(columns=['Shape_Length', 'Shape_Area'])


    park_national = flat.loc[flat['max_forest_restrict'] == 5]
    park_national = park_national.groupby(table_group).sum('Shape_Area')
    park_national = park_national.Shape_Area.div(10000).rename("Forestry - Protected")

    flat_protections = pd.merge(herd_base, park_national,  how="outer", left_on = table_group, right_on = table_group)

    protection_coding = {4: 'Full', 3: 'High', 2: 'Medium', 1: 'Low'}

    for coding, classes in zip(protection_coding.keys(), protection_coding.values()):
            
        classes_df = classes + '_df'
        classes_df = flat.loc[flat['max_forest_restrict'] == coding]
        classes_df = classes_df.groupby(table_group).sum('Shape_Area')
        classes_df = classes_df.Shape_Area.div(10000).rename("Forestry - {}".format(classes))

        flat_protections = pd.merge(flat_protections, classes_df,  how="outer", left_on = table_group, right_on = table_group)

        print(flat_protections)
    
    flat_protections.loc[:,"Forestry Protected (Aggregated)"] = sum(flat_protections[['Forestry - Protected',
                                                                   'Forestry - Full',
                                                                   'Forestry - High']].sum())

    flat_protections.loc[:,"Forestry Managed (Aggregated)"] = sum(flat_protections[['Forestry - Medium',
                                                                   'Forestry - Low']].sum())
    
    protection_coding = {5: 'Protected', 4: 'Full', 3: 'High', 2: 'Medium', 1: 'Low'}

    for coding, classes in zip(protection_coding.keys(), protection_coding.values()):
            
        classes_df = classes + '_df'
        classes_df = flat.loc[flat['max_mine_restriction'] == coding]
        classes_df = classes_df.groupby(table_group).sum('Shape_Area')
        classes_df = classes_df.Shape_Area.div(10000).rename("Mining - {}".format(classes))

        flat_protections = pd.merge(flat_protections, classes_df,  how="outer", left_on = table_group, right_on = table_group)

        print(flat_protections)
        
    flat_protections.loc[:,"Mining Protected (Aggregated)"] = sum(flat_protections[['Mining - Protected',
                                                                   'Mining - Full',
                                                                   'Mining - High']].sum())

    flat_protections.loc[:,"Mining Managed (Aggregated)"] = sum(flat_protections[['Mining - Medium',
                                                                   'Mining - Low']].sum())

    for coding, classes in zip(protection_coding.keys(), protection_coding.values()):
            
        classes_df = classes + '_df'
        classes_df = flat.loc[flat['max_og_restriction'] == coding]
        classes_df = classes_df.groupby(table_group).sum('Shape_Area')
        classes_df = classes_df.Shape_Area.div(10000).rename("Oil & Gas - {}".format(classes))

        flat_protections = pd.merge(flat_protections, classes_df,  how="outer", left_on = table_group, right_on = table_group)

        print(flat_protections)

    flat_protections.loc[:,"Oil & Gas Protected (Aggregated)"] = sum(flat_protections[['Oil & Gas - Protected',
                                                                   'Oil & Gas - Full',
                                                                   'Oil & Gas - High']].sum())

    flat_protections.loc[:,"Oil & Gas Managed (Aggregated)"] = sum(flat_protections[['Oil & Gas - Medium',
                                                                   'Oil & Gas - Low']].sum())

    range_out = csv_protect_output.replace('protect','')
    flat_protections.to_csv(csv_dir + range_out + "flat_groupings.csv")

    print(flat_protections)
