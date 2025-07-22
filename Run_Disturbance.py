import arcpy
import os
import re
import json
import logging
import smtplib
import socket
import pandas as pd
import numpy as np
import pandasql
import getpass
## If you get a warning about pandas not exisitng/installed write this line of code in the termianl and run it
## python -m pip install "pandasql"


from arcpy import env
from disturbance_layer import disturbance_aoi, buffer_disturbance, intersect, delete, interim_clean_up, delete_layers, disturbance_flatten, disturbance_field_mapping, disturbance_cleanup, disturbance_buffer_flatten,disturbance_buffer_field_mapping, disturbance_buffer_cleanup, identity
from table_create import combine_loose_sheets, make_sheet_base, static_grouping
from protection_layer import protect_aoi, gather_protection, flatten_protection, field_mapping, clean_and_join, combine
from protection_table import tabletotable, combine_loose_herds, protection_grouping, protection_classes
from disturbance_protection_combine import combine_disturbance_and_protection, clean_up
configFile = r"config_disturbance_2023.json"

arcpy.env.parallelProcessingFactor = "50%"
arcpy.env.overwriteOutput = True

def readConfig(configFile):#returns dictionary of parameters
    """
    reads the config file to dictionary
    """
    with open(configFile) as json_file:
        try:
            d = json.load(json_file)
        except:
            print ("failed to parse configuration")
        else:
            return d['params']
def layers():
    disturbance_aoi(connPath, connFile, username, password, aoi_location, layer_name, unique_value, roads_file, bcce_file)
    buffer_disturbance()
    intersect(unique_value, aoi_location, layer_name, dissolve_values)
    delete()
    interim_clean_up(dissolve_values)
def spagh_meatball():

    aoi = (aoi_location + layer_name)
    search_word = "{}".format(unique_value)

    with arcpy.da.SearchCursor(aoi, [search_word]) as cursor:
        values_sorted = sorted({row[0] for row in cursor})
    print('Running disturbance on: {}'.format(values_sorted))
    
    for values in values_sorted:
        layer_query = """{0} = '{1}'""".format(unique_value, values)
        layer_select = arcpy.SelectLayerByAttribute_management(aoi, "NEW_SELECTION", layer_query)
        arcpy.CopyFeatures_management(layer_select, 'aoi')

        (print('Selected {}'.format(values)))

        value_update = values.replace(" ", "")
        value_update = value_update.replace("-", "") 
        value_update = value_update.replace(":", "") 
        value_update = value_update.replace("/", "") 


        disturbance_flatten(values, value_update)
        disturbance_field_mapping(values, value_update)
        disturbance_cleanup(values, value_update, keep_list)

        delete_layers()

        disturbance_buffer_flatten(values, value_update)
        disturbance_buffer_field_mapping(values, value_update)
        disturbance_buffer_cleanup(values, value_update, keep_list)

        delete_layers()

        identity(csv_dir, values, value_update, unique_value, intersect_layer, aoi_location)

def table():
    combine_loose_sheets(csv_dir, csv_output_name)
    make_sheet_base(intersect_layer, unique_value, aoi_location, csv_dir)
    static_grouping(csv_dir, csv_output_name, table_group, final_output)
def protection():
    protect_aoi(aoi_location, layer_name, unique_value)
    
    aoi = (aoi_location + layer_name)
    search_word = "{}".format(unique_value)

    with arcpy.da.SearchCursor(aoi, [search_word]) as cursor:
        values_sorted = sorted({row[0] for row in cursor})
    print('Running protection on: {}'.format(values_sorted))

    for values in values_sorted:
        layer_query = """{0} = '{1}'""".format(unique_value, values)
        layer_select = arcpy.SelectLayerByAttribute_management(aoi, "NEW_SELECTION", layer_query)
        arcpy.CopyFeatures_management(layer_select, 'aoi')

        (print('Selected {}'.format(values)))

        value_update = values.replace(" ", "")
        value_update = value_update.replace("-", "") 
        value_update = value_update.replace(":", "") 
        value_update = value_update.replace("/", "") 

        gather_protection(designated_lands, value_update)
        flatten_protection(value_update)
        field_mapping(value_update)
        clean_and_join(value_update, keep_list)
        combine(values, value_update, unique_value, intersect_layer, aoi_location)
def protection_table():
    aoi = (aoi_location + layer_name)
    search_word = "{}".format(unique_value)

    with arcpy.da.SearchCursor(aoi, [search_word]) as cursor:
        values_sorted = sorted({row[0] for row in cursor})
    print('Running protection on: {}'.format(values_sorted))

    for values in values_sorted:
        (print('Selected {}'.format(values)))

        value_update = values.replace(" ", "")
        value_update = value_update.replace("-", "") 
        value_update = value_update.replace(":", "") 
        value_update = value_update.replace("/", "") 

        tabletotable(value_update, csv_dir)
        combine_loose_herds(csv_dir, value_update, csv_protect_output)
        make_sheet_base(intersect_layer, unique_value, aoi_location, csv_dir)
        protection_grouping(csv_dir, csv_protect_output, table_group)
        protection_classes(csv_dir, csv_protect_output, table_group)
########
readConfig(configFile)
cfg = readConfig(configFile)
#####################################
# Modified for lists (running over multiple ranges at once)
workspace = cfg[0]['workspace']
connPath = cfg[0]['connPath']
connFile = cfg[0]['connFile']
username = "BOYANLIU"
password = getpass.getpass()
aoi_location = cfg[0]["aoi_location"]
layer_name_list = cfg[0]["layer_name"]
unique_value = cfg[0]["unique_value"]
dissolve_values = cfg[0]["dissolve_values"]
keep_list = cfg[0]["keep_list"]
csv_dir = cfg[0]["csv_dir"]
intersect_layer_list = cfg[0]["intersect_layer"]
csv_output_name_list = cfg[0]["csv_output_name"]
table_group = cfg[0]["table_group"]
final_output_list = cfg[0]["final_output"]
csv_protect_output_list = cfg[0]["csv_protect_output"]
designated_lands = cfg[0]["designated_lands"]
roads_file = cfg[0]["roads_file"]
bcce_file = cfg[0]["bcce_file"]
######################################
arcpy.env.workspace = workspace
arcpy.env.overwriteOutput = True
######################################
iterate = 0
for layer_name in layer_name_list:

    intersect_layer = intersect_layer_list[iterate]
    csv_output_name = csv_output_name_list[iterate]
    final_output = final_output_list[iterate]
    csv_protect_output = csv_protect_output_list[iterate]

    layers()
    spagh_meatball()
    table()
    protection()
    protection_table()

    iterate += 1
 
#%%% Format the output tables to match past final products
os.chdir(csv_dir)

# Get area of each habitat type
arcpy.env.workspace = aoi_location.replace('\\','/')[:-1]
aoilist = arcpy.ListFeatureClasses()
herd = []
hab = []
ha = []
for aoi in aoilist:
    with arcpy.da.SearchCursor(aoi, ['Herd_Name', "BCHab_code", 'Shape_Area']) as cursor:
        for row in cursor:
            herd.append(row[0])
            hab.append(row[1])
            # Convert to ha
            ha.append(row[2] * 0.0001)
 
herd_update = []
for h in herd:
    value_update = h.replace(" ", "")
    value_update = value_update.replace("-", "") 
    value_update = value_update.replace(":", "") 
    value_update = value_update.replace("/", "") 
    herd_update.append(value_update)

area_df = pd.DataFrame({"Herd": herd_update, "Habitat": hab, "Hectare": ha})

########################################## Disturbance ##########################################
# Write to multiple sheets in the same excel file
writer = pd.ExcelWriter('Disturbance Analysis 2022.xlsx')

disturb_all_df = pd.DataFrame()
disturb_percent_all_df = pd.DataFrame()
for final_output in final_output_list:
    disturb_df = pd.read_csv(final_output + ".csv")

    # Insert total area
    rangename = final_output.rsplit("_",-1)[0]
    ha_val = list(area_df[area_df["Herd"] == rangename]["Hectare"])
    insert_index = disturb_df.columns.get_loc("Herd_Name") + 1
    disturb_df.insert(insert_index,'Area (ha)',ha_val)
    
    disturb_df = disturb_df.transpose()
    
    # Edit row names
    droplist = ['Unnamed: 0', 'OID_', 'HERD_NO', 'HERD_CODE', 'REGION', 'ECO_GROUP',
           'COSEWIC_DU_CODE', 'COSEWIC_DU', 'HERD_PLAN', 'STATUS', 'DATE_LOADED',
           'DATE_APPROVED', 'DATE_RETIRED', 'CENTROID_X', 'CENTROID_Y', "Area_ha",
           'Species','Herd_id', 'Herd_code','Bc_ecotype_grouping', 'Bc_habitat_type', 
           'Elevation', 'Season', 'Du_cosewic_2014', 'Designation_cosewic_2014', 'Version']
    for d in droplist:
        if d in disturb_df.index:
            disturb_df.drop(d, axis=0, inplace=True)   
    for d in disturb_df.index.values:
        disturb_df = disturb_df.rename(index={d: d.capitalize().replace('Cumuatlive', 'Cumulative')\
        .replace('buffer', '500m buffer').replace("past 40", "past 40 years").replace("past 80", "past 80 years")})
        
    for d in droplist:
        if d in disturb_df.index:
            disturb_df.drop(d, axis=0, inplace=True) 
       
    # Case by case
    disturb_df = disturb_df.rename(index={"Bchab_code": "Habitat",\
                            "Herd_name": "Herd Name","Air (ha)": "Airstrip (ha)", \
                            "Pipe (ha)": "Pipeline (ha)","Static (ha)": "Static Total (ha)",\
                            "Air 500m buffer (ha)": "Airstrip 500m buffer (ha)",\
                            "Pipe 500m buffer (ha)": "Pipeline 500m buffer (ha)",\
                            "Ag 500m buffer (ha)": "Agriculture 500m buffer (ha)",\
                            "Static (500m buffer) (ha)": "Static Total 500m buffer (ha)"})
        
    if "Bc_ecotype" in disturb_df.index:
        disturb_df.drop("Bc_ecotype", axis=0, inplace=True) 
        
    # Append to total table
    disturb_all_df = pd.concat([disturb_all_df, disturb_df], axis = 1)
    
    #### Percentage ####
    # Make copy of dataframe so changes don't affect original
    disturb_percent = disturb_df.copy()
    
    for x in list(range(0, len(disturb_percent.columns))):
        total_area = disturb_percent.loc["Area (ha)",x]
        # Find non-null values in data value rows, round to 2 decimal place, change to text and put % at end
        disturb_percent.iloc[3:,x][disturb_percent.iloc[3:,x].notnull()] = \
            (disturb_percent.iloc[3:,x][disturb_percent.iloc[3:,x].notnull()]\
             / total_area * 100).astype(float).round(2).astype(str) + '%'
     
    # Append to total table
    disturb_percent_all_df = pd.concat([disturb_percent_all_df, disturb_percent], axis = 1)    

# Delete extraneous field
if "Area_ha" in disturb_all_df.index:
    disturb_all_df.drop("Area_ha", axis=0, inplace=True) 
if "Area_ha" in disturb_percent_all_df.index:
    disturb_percent_all_df.drop("Area_ha", axis=0, inplace=True) 
   
# Correct range name
disturb_all_df = disturb_all_df.replace(['Klinseza'],'Klinse-za')
disturb_percent_all_df = disturb_percent_all_df.replace(['Klinseza'],'Klinse-za')

# Rename columns to the range names
disturb_all_df.columns = disturb_all_df.iloc[0,:]
disturb_percent_all_df.columns = disturb_percent_all_df.iloc[0,:]

# Filter overall dataset to separate eco groups
# Boreal
Boreal_ranges = ['Calendar','Chinchaga','Maxhamish','Snake-Sahtaneh','Westside Fort Nelson']
disturb_all_df_Boreal = disturb_all_df.loc[:, disturb_all_df.columns.isin(Boreal_ranges)]
disturb_percent_all_df_Boreal = disturb_percent_all_df.loc[:, disturb_percent_all_df.columns.isin(Boreal_ranges)]
# Check for naming inconsistency
for i in list(set(disturb_percent_all_df_Boreal.columns)):
    if i not in Boreal_ranges:
        print(i)
# Northern
Northern_ranges = ["Atlin", "Carcross", "Edziza", "Finlay", "Frog", "Gataga", 
                   "Horseranch", "Level-Kawdy", "Liard Plateau", "Little Rancheria",
                   "Muskwa", "Pink Mountain", "Rabbit", "Spatsizi", "Swan Lake", "Thutade", "Tsenaglode"]
disturb_all_df_Northern = disturb_all_df.loc[:, disturb_all_df.columns.isin(Northern_ranges)]
disturb_percent_all_df_Northern = disturb_percent_all_df.loc[:, disturb_percent_all_df.columns.isin(Northern_ranges)]
# Check for naming inconsistency
for i in list(set(disturb_percent_all_df_Northern.columns)):
    if i not in Northern_ranges:
        print(i)
# SMC North
SMC_North_ranges = ["Charlotte Alplands", "Chase", "Graham", "Itcha-Ilgachuz", 
                    "Rainbows", "Takla", "Telkwa", "Tweedsmuir", "Wolverine"]
disturb_all_df_SMC_North = disturb_all_df.loc[:, disturb_all_df.columns.isin(SMC_North_ranges)]
disturb_percent_all_df_SMC_North = disturb_percent_all_df.loc[:, disturb_percent_all_df.columns.isin(SMC_North_ranges)]
# Check for naming inconsistency
for i in list(set(disturb_percent_all_df_SMC_North.columns)):
    if i not in SMC_North_ranges:
        print(i)
# SMC Central
SMC_Central_ranges = ["Klinse-za", "Narraway", "Quintette", "Redrock-Prairie Creek", "Scott"]
disturb_all_df_SMC_Central = disturb_all_df.loc[:, disturb_all_df.columns.isin(SMC_Central_ranges)]
disturb_percent_all_df_SMC_Central = disturb_percent_all_df.loc[:, disturb_percent_all_df.columns.isin(SMC_Central_ranges)]
# Check for naming inconsistency
for i in list(set(disturb_percent_all_df_SMC_Central.columns)):
    if i not in SMC_Central_ranges:
        print(i)
# SMC South
SMC_South_ranges = ["Barkerville", "Central Rockies", "Central Selkirks", 
                    "Columbia North", "Columbia South", "Frisby Boulder", 
                    "George Mountain", "Groundhog", "Hart Ranges", "Monashee", 
                    "Narrow Lake", "North Cariboo", "Purcell Central", 
                    "Purcells South", "South Selkirks", "Wells Gray North", "Wells Gray South"]
disturb_all_df_SMC_South = disturb_all_df.loc[:, disturb_all_df.columns.isin(SMC_South_ranges)]
disturb_percent_all_df_SMC_South = disturb_percent_all_df.loc[:, disturb_percent_all_df.columns.isin(SMC_South_ranges)]
# Check for naming inconsistency
for i in list(set(disturb_percent_all_df_SMC_South.columns)):
    if i not in SMC_South_ranges:
        print(i)

# Write to excel
disturb_all_df.to_excel(writer, sheet_name = "All Ranges (ha)", header = False)
disturb_percent_all_df.to_excel(writer, sheet_name = "All Ranges (%)", header = False)

disturb_all_df_Boreal.to_excel(writer, sheet_name = "Boreal (ha)", header = False)
disturb_percent_all_df_Boreal.to_excel(writer, sheet_name = "Boreal (%)", header = False)

disturb_all_df_Northern.to_excel(writer, sheet_name = "Northern (ha)", header = False)
disturb_percent_all_df_Northern.to_excel(writer, sheet_name = "Northern  (%)", header = False)

disturb_all_df_SMC_North.to_excel(writer, sheet_name = "SMC - North (ha)", header = False)
disturb_percent_all_df_SMC_North.to_excel(writer, sheet_name = "SMC - North (%)", header = False)

disturb_all_df_SMC_Central.to_excel(writer, sheet_name = "SMC - Central (ha)", header = False)
disturb_percent_all_df_SMC_Central.to_excel(writer, sheet_name = "SMC - Central (%)", header = False)

disturb_all_df_SMC_South.to_excel(writer, sheet_name = "SMC - South (ha)", header = False)
disturb_percent_all_df_SMC_South.to_excel(writer, sheet_name = "SMC - South (%)", header = False)

writer.save()

del(writer)

########################################## Protection ##########################################
# Write to multiple sheets in the same excel file
writer = pd.ExcelWriter('Protection Analysis 2022.xlsx')
    
prot_all_df = pd.DataFrame()
prot_percent_all_df = pd.DataFrame()
for final_output in csv_protect_output_list:
    rangename = final_output.replace('_protect', '')
    prot_df1 = pd.read_csv(rangename + "_protections_flat.csv")

    # Add range area in hectares
    ha_val = list(area_df[area_df["Herd"] == rangename]["Hectare"])
    insert_index = prot_df1.columns.get_loc("Herd_Name") + 1
    prot_df1.insert(insert_index,'Area (ha)',ha_val)
    
    prot_df1 = prot_df1.transpose()
    
    # Edit row names
    droplist = ['Unnamed: 0', 'OID_', 'HERD_NO', 'HERD_CODE', 'REGION', 'ECO_GROUP',
           'COSEWIC_DU_CODE', 'COSEWIC_DU', 'HERD_PLAN', 'STATUS', 'DATE_LOADED',
           'DATE_APPROVED', 'DATE_RETIRED', 'CENTROID_X', 'CENTROID_Y', "Area_ha", "Area_Ha",
           'Species','Herd_id', 'Herd_code','Bc_ecotype_grouping', 'Bc_habitat_type', 
           'Elevation', 'Season', 'Du_cosewic_2014', 'Designation_cosewic_2014', 'Version',
           'Herd_ID', 'Herd_Code', 'BC_Ecotype_Grouping','BC_Habitat_Type', 'Habitat', 
           'DU_COSEWIC_2014','Designation_COSEWIC_2014', 'VERSION']
    for d in droplist:
        if d in prot_df1.index:
            prot_df1.drop(d, axis=0, inplace=True)  
    prot_df1 = prot_df1.rename(index={"BCHab_code": "Habitat",\
                            "HERD_NAME": "Herd Name", 'Herd_Name': "Herd Name"})
    
    # Grouped protection (levels of protection from Forestry, Mining, Oil & Gas) 
    prot_df2 = pd.read_csv(rangename + "_flat_groupings.csv")
    
    prot_df2 = prot_df2.transpose()
    
    # Edit row names
    droplist = ['Unnamed: 0', 'OID_', 'HERD_NO', 'HERD_CODE', 'REGION', 'ECO_GROUP',
           'COSEWIC_DU_CODE', 'COSEWIC_DU', 'HERD_PLAN', 'STATUS', 'DATE_LOADED',
           'DATE_APPROVED', 'DATE_RETIRED', 'CENTROID_X', 'CENTROID_Y', "Area_ha", "Area_Ha",
           'Species','Herd_id', 'Herd_code','Bc_ecotype_grouping', 'Bc_habitat_type', 
           'Elevation', 'Season', 'Du_cosewic_2014', 'Designation_cosewic_2014', 'Version',
           "BCHab_code", "HERD_NAME", "Herd_Name",
           'Herd_ID', 'Herd_Code', 'BC_Ecotype_Grouping','BC_Habitat_Type', 'Habitat', 
           'DU_COSEWIC_2014','Designation_COSEWIC_2014', 'VERSION']
    for d in droplist:
        if d in prot_df2.index:
            prot_df2.drop(d, axis=0, inplace=True)  
    
    # Join together
    prot_df = pd.concat([prot_df1, prot_df2])
    
    # Append to total table
    prot_all_df = pd.concat([prot_all_df, prot_df], axis = 1)
    
    #### Percentage ####
    # Make copy of dataframe so changes don't affect original
    prot_percent = prot_df.copy()
    
    for x in list(range(0, len(prot_percent.columns))):
        total_area = prot_percent.loc["Area (ha)",x]
        # Find non-null values in data value rows, round to 2 decimal place, change to text and put % at end
        prot_percent.iloc[3:,x][prot_percent.iloc[3:,x].notnull()] = \
            (prot_percent.iloc[3:,x][prot_percent.iloc[3:,x].notnull()]\
             / total_area * 100).astype(float).round(2).astype(str) + '%'

    # Append to total table
    prot_percent_all_df = pd.concat([prot_percent_all_df, prot_percent], axis = 1) 
        
# Correct range name
prot_all_df = prot_all_df.replace(['Klinseza'],'Klinse-za')
prot_percent_all_df = prot_percent_all_df.replace(['Klinseza'],'Klinse-za')

# Rename columns to the range names
prot_all_df.columns = prot_all_df.iloc[0,:]
prot_percent_all_df.columns = prot_percent_all_df.iloc[0,:]

# Filter overall dataset to separate eco groups
# Boreal
Boreal_ranges = ['Calendar','Chinchaga','Maxhamish','Snake-Sahtaneh','Westside Fort Nelson']
prot_all_df_Boreal = prot_all_df.loc[:, prot_all_df.columns.isin(Boreal_ranges)]
prot_percent_all_df_Boreal = prot_percent_all_df.loc[:, prot_percent_all_df.columns.isin(Boreal_ranges)]
# Check for naming inconsistency
for i in list(set(prot_percent_all_df_Boreal.columns)):
    if i not in Boreal_ranges:
        print(i)
# Northern
Northern_ranges = ["Atlin", "Carcross", "Edziza", "Finlay", "Frog", "Gataga", 
                   "Horseranch", "Level-Kawdy", "Liard Plateau", "Little Rancheria",
                   "Muskwa", "Pink Mountain", "Rabbit", "Spatsizi", "Swan Lake", "Thutade", "Tsenaglode"]
prot_all_df_Northern = prot_all_df.loc[:, prot_all_df.columns.isin(Northern_ranges)]
prot_percent_all_df_Northern = prot_percent_all_df.loc[:, prot_percent_all_df.columns.isin(Northern_ranges)]
# Check for naming inconsistency
for i in list(set(prot_percent_all_df_Northern.columns)):
    if i not in Northern_ranges:
        print(i)
# SMC North
SMC_North_ranges = ["Charlotte Alplands", "Chase", "Graham", "Itcha-Ilgachuz", 
                    "Rainbows", "Takla", "Telkwa", "Tweedsmuir", "Wolverine"]
prot_all_df_SMC_North = prot_all_df.loc[:, prot_all_df.columns.isin(SMC_North_ranges)]
prot_percent_all_df_SMC_North = prot_percent_all_df.loc[:, prot_percent_all_df.columns.isin(SMC_North_ranges)]
# Check for naming inconsistency
for i in list(set(prot_percent_all_df_SMC_North.columns)):
    if i not in SMC_North_ranges:
        print(i)
# SMC Central
SMC_Central_ranges = ["Klinse-za", "Narraway", "Quintette", "Redrock-Prairie Creek", "Scott"]
prot_all_df_SMC_Central = prot_all_df.loc[:, prot_all_df.columns.isin(SMC_Central_ranges)]
prot_percent_all_df_SMC_Central = prot_percent_all_df.loc[:, prot_percent_all_df.columns.isin(SMC_Central_ranges)]
# Check for naming inconsistency
for i in list(set(prot_percent_all_df_SMC_Central.columns)):
    if i not in SMC_Central_ranges:
        print(i)
# SMC South
SMC_South_ranges = ["Barkerville", "Central Rockies", "Central Selkirks", 
                    "Columbia North", "Columbia South", "Frisby Boulder", 
                    "George Mountain", "Groundhog", "Hart Ranges", "Monashee", 
                    "Narrow Lake", "North Cariboo", "Purcell Central", 
                    "Purcells South", "South Selkirks", "Wells Gray North", "Wells Gray South"]
prot_all_df_SMC_South = prot_all_df.loc[:, prot_all_df.columns.isin(SMC_South_ranges)]
prot_percent_all_df_SMC_South = prot_percent_all_df.loc[:, prot_percent_all_df.columns.isin(SMC_South_ranges)]
# Check for naming inconsistency
for i in list(set(prot_percent_all_df_SMC_South.columns)):
    if i not in SMC_South_ranges:
        print(i)

# Write to excel
prot_all_df.to_excel(writer, sheet_name = "All Ranges (ha)", header = False)
prot_percent_all_df.to_excel(writer, sheet_name = "All Ranges (%)", header = False)

prot_all_df_Boreal.to_excel(writer, sheet_name = "Boreal (ha)", header = False)
prot_percent_all_df_Boreal.to_excel(writer, sheet_name = "Boreal (%)", header = False)

prot_all_df_Northern.to_excel(writer, sheet_name = "Northern (ha)", header = False)
prot_percent_all_df_Northern.to_excel(writer, sheet_name = "Northern  (%)", header = False)

prot_all_df_SMC_North.to_excel(writer, sheet_name = "SMC - North (ha)", header = False)
prot_percent_all_df_SMC_North.to_excel(writer, sheet_name = "SMC - North (%)", header = False)

prot_all_df_SMC_Central.to_excel(writer, sheet_name = "SMC - Central (ha)", header = False)
prot_percent_all_df_SMC_Central.to_excel(writer, sheet_name = "SMC - Central (%)", header = False)

prot_all_df_SMC_South.to_excel(writer, sheet_name = "SMC - South (ha)", header = False)
prot_percent_all_df_SMC_South.to_excel(writer, sheet_name = "SMC - South (%)", header = False)

writer.save()

del(writer)
