

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
import dotenv

root_dir=os.getenv("ROOT_DIR")
workspace= os.path.join(root_dir, os.getenv("OUTPUT_GDB"))
arcpy.env.overwriteOutput = True
arcpy.env.workspace = workspace
def disturbance_aoi(connPath, connFile, username, password, aoi_location, layer_name, unique_value, roads_file, bcce_file,inst):
    if arcpy.Exists(f"{layer_name}_disturbance"):
        print('disturbance aoi finished moving on to next step')
        return
    bcgwConn = os.path.join(connPath, connFile)
    try:
                arcpy.CreateDatabaseConnection_management(out_folder_path=connPath,
                                                        out_name=connFile,
                                                        database_platform='ORACLE',
                                                        instance=inst,
                                                        account_authentication='DATABASE_AUTH',
                                                        username=username,
                                                        password=password,
                                                        save_user_pass='DO_NOT_SAVE_USERNAME')
                print(' new SDE connection')
    except:
        print('Database connection already exists')

    aoi = (os.path.join(aoi_location,layer_name))

    search_word = str(unique_value)

    print(aoi)
    print(search_word)

    with arcpy.da.SearchCursor(aoi, [search_word]) as cursor:
        values_sorted = sorted({row[0] for row in cursor})
    print('Running disturbance on: {}'.format(values_sorted))

    for values in values_sorted:
        layer_query = """{0} = '{1}'""".format(unique_value, values)
        layer_select = arcpy.SelectLayerByAttribute_management(aoi, "NEW_SELECTION", layer_query)
        arcpy.CopyFeatures_management(layer_select, 'aoi')

        (print('Selected {}'.format(values)))

        # # Update values name to allow it to be a naming convention for layer use
        value_update = values.replace(" ", "")
        value_update = value_update.replace("-", "") 
        value_update = value_update.replace(":", "") 
        value_update = value_update.replace("/", "") 

        # #Local disturbance variables from the BCGW:
        rail = (bcgwConn + "\\WHSE_BASEMAPPING.GBA_RAILWAY_TRACKS_SP") #open
        transmission = (bcgwConn + "\\WHSE_BASEMAPPING.GBA_TRANSMISSION_LINES_SP") #open
        pipe = (bcgwConn + "\\WHSE_MINERAL_TENURE.OG_PIPELINE_AREA_PERMIT_SP") #closed
        well = (bcgwConn + "\\WHSE_MINERAL_TENURE.OG_WELL_FACILITY_PERMIT_SP") #closed
        air = (bcgwConn + "\\WHSE_BASEMAPPING.TRIM_EBM_AIRFIELDS") #closed
        dam = (bcgwConn + "\\WHSE_WATER_MANAGEMENT.WRIS_DAMS_PUBLIC_SVW") #open
        reservoir = (bcgwConn + "\\WHSE_WATER_MANAGEMENT.WLS_RESERVOIR_PMT_LICENSEE_SP") #open
        fire_historical = (bcgwConn + "\\WHSE_LAND_AND_NATURAL_RESOURCE.PROT_HISTORICAL_FIRE_POLYS_SP") #open
        fire_current = (bcgwConn + "\\WHSE_LAND_AND_NATURAL_RESOURCE.PROT_CURRENT_FIRE_POLYS_SP") #closed
        cutblock = (bcgwConn + "\\WHSE_FOREST_VEGETATION.VEG_CONSOLIDATED_CUT_BLOCKS_SP") #closed
        pest = (bcgwConn + "\\WHSE_FOREST_VEGETATION.PEST_INFESTATION_POLY") #closed

        # Roads data from the BCCE
        roads = roads_file
        # Human disturbance data from the BCCE
        bcce = bcce_file

        # Setting up the dictionary
        disturbance_dictionary = {"rail": rail, "transmission": transmission, "pipe":pipe, "well":well, "air":air, "dam":dam, "reservoir":reservoir, "fire_historical": fire_historical, "fire_current": fire_current, "cutblock": cutblock ,"roads": roads}
        print("dictionary setup")

        #runs through the dictionary and for each item it select layers that intersect with the AOI (values boundary) and copies them out
        for name,layer in zip(disturbance_dictionary.keys(), disturbance_dictionary.values()):
                arcpy.MakeFeatureLayer_management(layer, "{}_lyr".format(name))
                arcpy.SelectLayerByLocation_management('{}_lyr'.format(name), "INTERSECT", 'aoi')
                arcpy.CopyFeatures_management('{}_lyr'.format(name), '{}_{}'.format(name, value_update))
                print('copied {} {}'.format(name, values))

                #add field of type and disturbance for each disturbance
                arcpy.AddField_management('{}_{}'.format(name, value_update), "type", "TEXT")
                arcpy.AddField_management('{}_{}'.format(name, value_update), "disturbance", "TEXT")
                arcpy.AddField_management('{}_{}'.format(name, value_update), "year", "SHORT")
                arcpy.AddField_management('{}_{}'.format(name, value_update), "severity", "TEXT")

                print('Field added')
                
                # Add in the year to the cutblock layer
                if name.startswith('cut'):
                    arcpy.CalculateField_management('{}_{}'.format(name, value_update), "type", '''"Temporal"''', "PYTHON")
                    
                    # cut_select = "!HARVEST_YEAR!"
                    cut_select = "!HARVEST_START_YEAR_CALENDAR!"
                    arcpy.CalculateField_management('{}_{}'.format(name, value_update), "year", cut_select)
                    print('done cutblock')

                # Add in the year to the fire layer
                elif name.startswith('fire'):
                    arcpy.CalculateField_management('{}_{}'.format(name, value_update), "type", '''"Temporal"''', "PYTHON")
                    
                    fire_select = "!FIRE_YEAR!"
                    arcpy.CalculateField_management('{}_{}'.format(name, value_update), "year", fire_select)

                else:
                    arcpy.CalculateField_management('{}_{}'.format(name, value_update), "type", '''"Static"''', "PYTHON")
                
                arcpy.CalculateField_management('{}_{}'.format(name, value_update), "disturbance", '''"{}"'''.format(name), "PYTHON")
                
        #Setting up queries for BCCE layer selection and the pest query
        urban_qery = """CEF_DISTURB_GROUP = 'Urban'"""
        ag_qery = """CEF_DISTURB_GROUP = 'Agriculture_and_Clearing'"""
        sesimic_qery = """CEF_DISTURB_GROUP = 'OGC_Geophysical'"""
        mining_qery = """CEF_DISTURB_GROUP = 'Mining_and_Extraction'"""
        pest_qery = """PEST_SPECIES_CODE = 'IBM' OR PEST_SPECIES_CODE = 'IBS' """

        bcce_dict = {"urban": bcce, "ag": bcce, "seismic": bcce, "mining": bcce, "pest": pest}
        query_list = [urban_qery, ag_qery, sesimic_qery, mining_qery, pest_qery]

        # Runs through the dictiarony with the queries to run the same process as above for the BCCE layers (and BCGW pest layer) that need to be selected out 
        for name,layer,query in zip(bcce_dict.keys(), bcce_dict.values(), query_list):
            arcpy.MakeFeatureLayer_management(layer, '{}_lyr'.format(name), query)
            arcpy.SelectLayerByLocation_management('{}_lyr'.format(name), "INTERSECT", 'aoi')
            arcpy.CopyFeatures_management('{}_lyr'.format(name), '{}_{}'.format(name, value_update))
            print('copied {} {}'.format(name, values))

            #add field of type and disturbance for each disturbance
            arcpy.AddField_management('{}_{}'.format(name, value_update), "type", "TEXT")
            arcpy.AddField_management('{}_{}'.format(name, value_update), "disturbance", "TEXT")
            arcpy.AddField_management('{}_{}'.format(name, value_update), "year", "SHORT")
            arcpy.AddField_management('{}_{}'.format(name, value_update), "severity", "TEXT")           

            # Inputs the year and severity code into the pest layer 
            if name == "pest":
                arcpy.CalculateField_management('{}_{}'.format(name, value_update), "type", '''"Temporal"''', "PYTHON")

                pest_select = "!CAPTURE_YEAR!"
                arcpy.CalculateField_management('{}_{}'.format(name, value_update), "year", pest_select)

                pest_severity = "!PEST_SEVERITY_CODE!"
                arcpy.CalculateField_management('{}_{}'.format(name, value_update), "severity", pest_severity)
                
# =============================================================================
#                 # Exclude all pest infestions as disturbance except for the very severe class
#                 with arcpy.da.UpdateCursor('{}_{}'.format(name, value_update), ['severity']) as cursor:
#                     for row in cursor:
#                         if row[0] != "V":
#                             cursor.deleteRow()
# =============================================================================
                print('done pest')
            else:
                arcpy.CalculateField_management('{}_{}'.format(name, value_update), "type", '''"Static"''', "PYTHON")
            
            arcpy.CalculateField_management('{}_{}'.format(name, value_update), "disturbance", '''"{}"'''.format(name), "PYTHON")

        print('Done layer collection for {}'.format(values))

        # Creates an empty list for the buffer features to be added to 
        buffer_class = []

        featureclasses = arcpy.ListFeatureClasses()

        # Gathers the linear features that require buffering and applies pre-deteremined buffer
        for feature in featureclasses:
            if feature.startswith('rail'):
                arcpy.Buffer_analysis(feature, '{}_b_{}'.format(feature, value_update), 5, "", "", "ALL")
                buffer_class.append(feature)

                arcpy.AddField_management('{}_b_{}'.format(feature, value_update), "type", "TEXT")
                arcpy.AddField_management('{}_b_{}'.format(feature, value_update), "disturbance", "TEXT")

                arcpy.CalculateField_management('{}_b_{}'.format(feature, value_update), "disturbance", '''"rail"''', "PYTHON")
                arcpy.CalculateField_management('{}_b_{}'.format(feature, value_update), "type", '''"Static"''', "PYTHON")

            elif feature.startswith('dam'):
                arcpy.Buffer_analysis(feature, '{}_b_{}'.format(feature, value_update), 7, "", "", "ALL")

                arcpy.AddField_management('{}_b_{}'.format(feature, value_update), "type", "TEXT")
                arcpy.AddField_management('{}_b_{}'.format(feature, value_update), "disturbance", "TEXT")

                arcpy.CalculateField_management('{}_b_{}'.format(feature, value_update), "disturbance", '''"dam"''', "PYTHON")
                arcpy.CalculateField_management('{}_b_{}'.format(feature, value_update), "type", '''"Static"''', "PYTHON")
                buffer_class.append(feature)

            elif feature.startswith('transmission'):
                arcpy.Buffer_analysis(feature, '{}_b_{}'.format(feature, value_update), 25, "", "", "ALL")

                arcpy.AddField_management('{}_b_{}'.format(feature, value_update), "type", "TEXT")
                arcpy.AddField_management('{}_b_{}'.format(feature, value_update), "disturbance", "TEXT")

                arcpy.CalculateField_management('{}_b_{}'.format(feature, value_update), "disturbance", '''"transmission"''', "PYTHON")
                arcpy.CalculateField_management('{}_b_{}'.format(feature, value_update), "type", '''"Static"''', "PYTHON")

                buffer_class.append(feature)

            elif feature.startswith('road'):
                arcpy.Buffer_analysis(feature, '{}_b_{}'.format(feature, value_update), 25, "", "", "ALL")

                arcpy.AddField_management('{}_b_{}'.format(feature, value_update), "type", "TEXT")
                arcpy.AddField_management('{}_b_{}'.format(feature, value_update), "disturbance", "TEXT")

                arcpy.CalculateField_management('{}_b_{}'.format(feature, value_update), "disturbance", '''"road"''', "PYTHON")
                arcpy.CalculateField_management('{}_b_{}'.format(feature, value_update), "type", '''"Static"''', "PYTHON")

                buffer_class.append(feature)
            else:
                pass

        print(buffer_class)

        for feature in buffer_class:
            arcpy.Delete_management(feature)

        print('Linear buffers complete')

        merge_list = []
        merge_group = arcpy.ListFeatureClasses()

        for merge in merge_group:
            if merge.endswith(value_update):
                merge_list.append(merge)
            if merge.endswith('_b'):
                merge_list.append(merge)
            else:
                pass
        
        print(merge_list)
        arcpy.management.Merge(merge_list, '{}_disturbance_merge'.format(value_update))

        print('Disturbances merged')


        road_count = arcpy.GetCount_management('roads_{0}_b_{0}'.format(value_update))
        seismic_count = arcpy.GetCount_management('seismic_{}'.format(value_update))

        print('road count is {0}, seismic count is {1}'.format(road_count, seismic_count))

        #unique_disturbance = arcpy.da.SearchCursor('{}_disturbance_merge'.format(value_update), ['disturbance'])
        values = [row[0] for row in arcpy.da.SearchCursor('{}_disturbance_merge'.format(value_update), ['disturbance'])]
        uniqueValues = set(values)
        print(uniqueValues)
        if road_count != '0':
            if 'Road' not in uniqueValues:
                print('Roads no merged into layer')
                arcpy.management.Merge(['roads_{0}_b_{0}'.format(value_update), '{}_disturbance_merge'.format(value_update)] , '{}_disturbance_update_merge'.format(value_update))
                arcpy.Delete_management('{}_disturbance_merge'.format(value_update))
            else:
                pass
        else:
            pass

        if seismic_count != '0':
            if 'seismic' not in uniqueValues:
                print('Seismic no merge into layer')
                arcpy.management.Merge(['seismic_{}'.format(value_update), '{}_disturbance_update_merge'.format(value_update)], '{}_disturbance_update_2_merge'.format(value_update))
                arcpy.Delete_management('{}_disturbance_update_merge'.format(value_update))
        else:
            pass


        for delete in merge_list:
            arcpy.Delete_management(delete)

        layer_list_2 = arcpy.ListFeatureClasses()

        for layer in layer_list_2:
            if layer.endswith('_merge'):
                arcpy.management.Dissolve(layer, '{}_disturbance_d'.format(value_update), ["year", "type", "disturbance", "severity"])
                print('disturbances dissolved')

        arcpy.analysis.Clip('{}_disturbance_d'.format(value_update), 'aoi', '{}_disturbance'.format(value_update))
        print('disturbances clipped')

        print('--------------------------------------------------LAYER PROCESS DONE----------------------------------------------')
# buffers out features by 500m for buffer disturbance class
def buffer_disturbance():
    buffer_features = arcpy.ListFeatureClasses()

    for buffer_f in buffer_features:
        if arcpy.Exists(f"{buffer_f}_buffer"):
            print(f"{buffer_f}_buffer already buffered")
            return
        
        if buffer_f.endswith('_disturbance'):
            print(buffer_f)
            # Select all disturbance except fire, pest and reservoir - they don't recieve the 500m buffer 
            buffer_query = """disturbance <> 'fire_historical' AND disturbance <> 'fire_current' And disturbance <> 'pest' And disturbance <> 'reservoir'"""
            buffer_select = arcpy.SelectLayerByAttribute_management(buffer_f, "NEW_SELECTION", buffer_query)

            #Copy out the selected features
            arcpy.CopyFeatures_management(buffer_select, "buffer_select")
            
            #Buffer the layer by 500
            arcpy.Buffer_analysis("buffer_select", "{}_buffer".format(buffer_f), "500 METERS")
            print('buffered')

            arcpy.CalculateField_management("{}_buffer".format(buffer_f), "disturbance", "!disturbance! + ' buffer'", "PYTHON3")   

    print('--------------------------------------------------BUFFER DISTURBANCE DONE----------------------------------------------')       
#Intersects buffer and disturbance layers with values boundary/habitat
def intersect(unique_value, aoi_location, layer_name, dissolve_values):
    intersect_features = arcpy.ListFeatureClasses()
    intersect_str = ("_buffer", "_disturbance")
    intersect_layers = []
    # Picks features that end with _buffer or _disturbance
    for intersect_f in intersect_features:
        if arcpy.Exists(f"{intersect_f}_intersect"):
            print(f"{intersect_f}_intersect already exists")
            continue
        
        if intersect_f.endswith(intersect_str):
            intersect_layers.append(intersect_f)
        else:
            pass
        # If the features starts with the values name that is currently being used
    aoi = os.path.join(aoi_location,layer_name)

    search_word = "{}".format(unique_value)

    with arcpy.da.SearchCursor(aoi, [search_word]) as cursor:
        values_sorted = sorted({row[0] for row in cursor})

    for values in values_sorted:

        if arcpy.Exists("aoi"):
            arcpy.Delete_management("aoi")

        ###    
        layer_query = """{0} = '{1}'""".format(unique_value, values)
        layer_select = arcpy.SelectLayerByAttribute_management(aoi, "NEW_SELECTION", layer_query)
        arcpy.CopyFeatures_management(layer_select, 'aoi')

        value_update = values.replace(" ", "")
        value_update = value_update.replace("-", "") 
        value_update = value_update.replace(":", "") 
        value_update = value_update.replace("/", "") 
        for intersect_f in intersect_layers:
            if intersect_f.startswith(value_update):
                #Intersect the merged disturbance with the habitat layer
                arcpy.analysis.Intersect(["aoi", intersect_f], '{}_intersect'.format(intersect_f))
            else:
                pass
# Deletes all interm layers that aren't the final intersected, flat or final layers
def delete():
    # Deletes all interm layers that aren't the final intersected, flat or final layers
    layer_list = arcpy.ListFeatureClasses()

    delete_list = []

    for layer in layer_list:
        if layer.endswith('_intersect'):
            pass
        elif layer.endswith('_final'):
            pass
        elif layer.endswith('_flat'):
            pass
        else:
            print('delete {}'.format(layer))
            delete_list.append(layer)

    print(delete_list)

    for delete in delete_list:
        arcpy.Delete_management(delete)
# cleans up fields from layer
def interim_clean_up(dissolve_values, lyr):
    if arcpy.Exists(f"{lyr}_disturbance_final"):
        print(f"{lyr}_disturbance_final already exists")
        return
    intersect_features = arcpy.ListFeatureClasses()
    intersect_str = ("_intersect")
    print("working??")
    print(dissolve_values)
    ##
    #if feature ends with _intersect it 
    for intersect_f in intersect_features:
        if intersect_f.endswith(intersect_str):
            print(intersect_f)
            layer_output = intersect_f.replace("intersect", "final")
            print(layer_output)
            arcpy.management.Dissolve(intersect_f, layer_output, dissolve_values)
    print('--------------------------------------------------CLEAN UP DONE----------------------------------------------')
################################################################################
def disturbance_flatten(values, value_update):
    delete_layer = []
    print(value_update)
        
    disturbance_layer =('{}_disturbance_final'.format(value_update))

    print("Starting on {}".format(disturbance_layer))

    # Make the overlapping multipart protection into a singlepart layer
    arcpy.management.MultipartToSinglepart(disturbance_layer, "{}_singlepart".format(disturbance_layer))
    delete_layer.append("{}_singlepart".format(disturbance_layer))

    print('Multi-part to singlepart done')

    # Union together the singlepart
    arcpy.analysis.Union(["{}_singlepart".format(disturbance_layer)], "{}_d_singlepart_union".format(disturbance_layer), cluster_tolerance= "1 METER")
    
    print('Singlepart unioned')

    ##### Spaghetti Layer ##############################
    arcpy.FeatureToPolygon_management("{}_d_singlepart_union".format(disturbance_layer), "{}_singlepart_union_polygon".format(disturbance_layer))
 
    print('Feature to Polygon done')

    arcpy.management.MultipartToSinglepart("{}_singlepart_union_polygon".format(disturbance_layer), "{}_disturb_flat".format(value_update))

    print('Polygon singleparted')

    fieldObjList = arcpy.ListFields("{}_disturb_flat".format(value_update))
    fieldNameList = []

    for field in fieldObjList:
            if not field.required:
                    fieldNameList.append(field.name)
            
    arcpy.DeleteField_management("{}_disturb_flat".format(value_update), fieldNameList)

    print('Spaghetti done')

    ##### Meatball Layer ################################
    arcpy.RepairGeometry_management("{}_disturb_flat".format(value_update))
    arcpy.FeatureToPoint_management("{}_disturb_flat".format(value_update), "{}_disturb_singlepart_union_meatball".format(value_update), point_location = "INSIDE")

    print('Meatballs done')

    fieldObjList = arcpy.ListFields("{}_disturb_singlepart_union_meatball".format(value_update))
    fieldNameList = []

    for field in fieldObjList:
            if not field.required:
                    fieldNameList.append(field.name)
    
    print('Meatballs fields done')
def disturbance_field_mapping(values, value_update):
    print('Disturbance ready for field mapping for {}'.format(values))
    delete_layer = []
    print(values)    
    print(value_update)
    
    #######
    targetFeatures = ("{}_disturb_singlepart_union_meatball".format(value_update))
    joinFeatures = ("{}_disturbance_final_d_singlepart_union".format(value_update))

    outfc = ("{}_disturb_singlepart_union_meatball_spatialjoin1".format(value_update))

    fieldmappings = arcpy.FieldMappings()
    fieldmappings.addTable(targetFeatures)
    fieldmappings.addTable(joinFeatures)

    desigFieldIndex= fieldmappings.findFieldMapIndex("disturbance")
    fieldmap = fieldmappings.getFieldMap(desigFieldIndex)
    field = fieldmap.outputField

    field.name = "disturbances"
    field.aliasName = "Disturbance List"
    field.length = '600'
    fieldmap.outputField = field

    fieldmap.mergeRule = 'Join'
    fieldmap.joinDelimiter = '; '

    fieldmappings.replaceFieldMap(desigFieldIndex, fieldmap)

    arcpy.SpatialJoin_analysis(targetFeatures, joinFeatures, outfc, 'JOIN_ONE_TO_ONE', '#', fieldmappings, 'INTERSECT')

    arcpy.DeleteField_management("{}_disturb_singlepart_union_meatball_spatialjoin1".format(value_update), 'type')
    
    ############

    targetFeatures = ("{}_disturb_singlepart_union_meatball_spatialjoin1".format(value_update))
    joinFeatures = ("{}_disturbance_final_d_singlepart_union".format(value_update))

    outfc = ("{}_disturb_singlepart_union_meatball_spatialjoin2".format(value_update))

    fieldmappings = arcpy.FieldMappings()
    fieldmappings.addTable(targetFeatures)
    fieldmappings.addTable(joinFeatures)

    desigFieldIndex= fieldmappings.findFieldMapIndex("type")
    fieldmap = fieldmappings.getFieldMap(desigFieldIndex)
    field = fieldmap.outputField

    field.name = "types"
    field.aliasName = "Type List"
    field.length = '600'
    fieldmap.outputField = field

    fieldmap.mergeRule = 'Join'
    fieldmap.joinDelimiter = '; '

    fieldmappings.replaceFieldMap(desigFieldIndex, fieldmap)

    arcpy.SpatialJoin_analysis(targetFeatures, joinFeatures, outfc, 'JOIN_ONE_TO_ONE', '#', fieldmappings, 'INTERSECT')

    
    arcpy.DeleteField_management("{}_disturb_singlepart_union_meatball_spatialjoin2".format(value_update), 'year')
    
    ###########

    year = ['fire', 'cutblock', 'pest']

    for disturb in year:
        if disturb == "fire":
            selection = "disturbance = 'fire_historical' OR disturbance = 'fire_current'"
        else:
            selection = "disturbance = '{}'".format(disturb)
        layer = arcpy.SelectLayerByAttribute_management("{}_disturbance_final_d_singlepart_union".format(value_update), "NEW_SELECTION", selection)
        arcpy.CopyFeatures_management(layer, '{}_{}_lyr'.format(value_update, disturb))
        delete_layer.append('{}_{}_lyr'.format(value_update, disturb))

    ###################
    targetFeatures = ("{}_disturb_singlepart_union_meatball_spatialjoin2".format(value_update))
    joinFeatures = ('{}_cutblock_lyr'.format(value_update))

    outfc = ("{}_disturb_singlepart_union_meatball_spatialjoin3".format(value_update))

    fieldmappings = arcpy.FieldMappings()
    fieldmappings.addTable(targetFeatures)
    fieldmappings.addTable(joinFeatures)

    desigFieldIndex= fieldmappings.findFieldMapIndex("year")
    fieldmap = fieldmappings.getFieldMap(desigFieldIndex)
    field = fieldmap.outputField

    field.name = "Cutblock_year"
    field.aliasName = "Cutblock Years"
    field.length = '600'
    field.type = 'Text'
    fieldmap.outputField = field

    fieldmap.mergeRule = 'Join'
    fieldmap.joinDelimiter = '; '

    fieldmappings.replaceFieldMap(desigFieldIndex, fieldmap)

    arcpy.SpatialJoin_analysis(targetFeatures, joinFeatures, outfc, 'JOIN_ONE_TO_ONE', '#', fieldmappings, 'INTERSECT')
    arcpy.DeleteField_management("{}_disturb_singlepart_union_meatball_spatialjoin3".format(value_update), 'year')

    ###################
    targetFeatures = ("{}_disturb_singlepart_union_meatball_spatialjoin3".format(value_update))
    joinFeatures = ('{}_cutblock_lyr'.format(value_update))

    outfc = ("{}_disturb_singlepart_union_meatball_spatialjoin4".format(value_update))

    fieldmappings = arcpy.FieldMappings()
    fieldmappings.addTable(targetFeatures)
    fieldmappings.addTable(joinFeatures)

    desigFieldIndex= fieldmappings.findFieldMapIndex("year")
    fieldmap = fieldmappings.getFieldMap(desigFieldIndex)
    field = fieldmap.outputField

    field.name = "latest_cut"
    field.aliasName = "Latest Cutblock"
    field.length = '600'
    fieldmap.outputField = field

    fieldmap.mergeRule = 'Max'

    fieldmappings.replaceFieldMap(desigFieldIndex, fieldmap)

    arcpy.SpatialJoin_analysis(targetFeatures, joinFeatures, outfc, 'JOIN_ONE_TO_ONE', '#', fieldmappings, 'INTERSECT')
    arcpy.DeleteField_management("{}_disturb_singlepart_union_meatball_spatialjoin3".format(value_update), 'year')

    # ###################
    targetFeatures = ("{}_disturb_singlepart_union_meatball_spatialjoin4".format(value_update))
    joinFeatures = ('{}_pest_lyr'.format(value_update))

    outfc = ("{}_disturb_singlepart_union_meatball_spatialjoin5".format(value_update))

    fieldmappings = arcpy.FieldMappings()
    fieldmappings.addTable(targetFeatures)
    fieldmappings.addTable(joinFeatures)

    desigFieldIndex= fieldmappings.findFieldMapIndex("year")
    fieldmap = fieldmappings.getFieldMap(desigFieldIndex)
    field = fieldmap.outputField

    field.name = "Pest_year"
    field.aliasName = "Pest Years"
    field.length = '600'
    field.type = 'Text'
    fieldmap.outputField = field

    fieldmap.mergeRule = 'Join'
    fieldmap.joinDelimiter = '; '

    fieldmappings.replaceFieldMap(desigFieldIndex, fieldmap)

    arcpy.SpatialJoin_analysis(targetFeatures, joinFeatures, outfc, 'JOIN_ONE_TO_ONE', '#', fieldmappings, 'INTERSECT')
    
    # ###################
    targetFeatures = ("{}_disturb_singlepart_union_meatball_spatialjoin5".format(value_update))
    joinFeatures = ('{}_pest_lyr'.format(value_update))

    outfc = ("{}_disturb_singlepart_union_meatball_spatialjoin6".format(value_update))

    fieldmappings = arcpy.FieldMappings()
    fieldmappings.addTable(targetFeatures)
    fieldmappings.addTable(joinFeatures)

    desigFieldIndex= fieldmappings.findFieldMapIndex("year")
    fieldmap = fieldmappings.getFieldMap(desigFieldIndex)
    field = fieldmap.outputField

    field.name = "latest_pest"
    field.aliasName = "Latest Pest"
    field.length = '600'
    fieldmap.outputField = field

    fieldmap.mergeRule = 'Max'

    fieldmappings.replaceFieldMap(desigFieldIndex, fieldmap)

    arcpy.SpatialJoin_analysis(targetFeatures, joinFeatures, outfc, 'JOIN_ONE_TO_ONE', '#', fieldmappings, 'INTERSECT')
    arcpy.DeleteField_management("{}_disturb_singlepart_union_meatball_spatialjoin6".format(value_update), 'severity')

    ###################
    targetFeatures = ("{}_disturb_singlepart_union_meatball_spatialjoin6".format(value_update))
    joinFeatures = ('{}_pest_lyr'.format(value_update))

    outfc = ("{}_disturb_singlepart_union_meatball_spatialjoin7".format(value_update))

    fieldmappings = arcpy.FieldMappings()
    fieldmappings.addTable(targetFeatures)
    fieldmappings.addTable(joinFeatures)

    desigFieldIndex= fieldmappings.findFieldMapIndex("severity")
    fieldmap = fieldmappings.getFieldMap(desigFieldIndex)
    field = fieldmap.outputField

    field.name = "pest_severity"
    field.aliasName = "Severity Codes"
    field.length = '600'
    field.type = 'Text'
    fieldmap.outputField = field

    fieldmap.mergeRule = 'Join'
    fieldmap.joinDelimiter = '; '


    fieldmappings.replaceFieldMap(desigFieldIndex, fieldmap)

    arcpy.SpatialJoin_analysis(targetFeatures, joinFeatures, outfc, 'JOIN_ONE_TO_ONE', '#', fieldmappings, 'INTERSECT')
    arcpy.DeleteField_management("{}_disturb_singlepart_union_meatball_spatialjoin7".format(value_update), 'year')
    
    ###################
    targetFeatures = ("{}_disturb_singlepart_union_meatball_spatialjoin7".format(value_update))
    joinFeatures = ('{}_fire_lyr'.format(value_update))

    outfc = ("{}_disturb_singlepart_union_meatball_spatialjoin8".format(value_update))

    fieldmappings = arcpy.FieldMappings()
    fieldmappings.addTable(targetFeatures)
    fieldmappings.addTable(joinFeatures)

    desigFieldIndex= fieldmappings.findFieldMapIndex("year")
    fieldmap = fieldmappings.getFieldMap(desigFieldIndex)
    field = fieldmap.outputField

    field.name = "Fire_year"
    field.aliasName = "Wildfire Years"
    field.length = '600'
    field.type = 'Text'
    fieldmap.outputField = field

    fieldmap.mergeRule = 'Join'
    fieldmap.joinDelimiter = '; '

    fieldmappings.replaceFieldMap(desigFieldIndex, fieldmap)

    arcpy.SpatialJoin_analysis(targetFeatures, joinFeatures, outfc, 'JOIN_ONE_TO_ONE', '#', fieldmappings, 'INTERSECT')

    ###################
    targetFeatures = ("{}_disturb_singlepart_union_meatball_spatialjoin8".format(value_update))
    joinFeatures = ('{}_fire_lyr'.format(value_update))

    outfc = ("{}_disturb_singlepart_union_meatball_spatialjoin9".format(value_update))

    fieldmappings = arcpy.FieldMappings()
    fieldmappings.addTable(targetFeatures)
    fieldmappings.addTable(joinFeatures)

    desigFieldIndex= fieldmappings.findFieldMapIndex("year")
    fieldmap = fieldmappings.getFieldMap(desigFieldIndex)
    field = fieldmap.outputField

    field.name = "latest_fire"
    field.aliasName = "Latest Wildfire"
    field.length = '600'
    fieldmap.outputField = field

    fieldmap.mergeRule = 'Max'

    fieldmappings.replaceFieldMap(desigFieldIndex, fieldmap)

    arcpy.SpatialJoin_analysis(targetFeatures, joinFeatures, outfc, 'JOIN_ONE_TO_ONE', '#', fieldmappings, 'INTERSECT')       
def disturbance_cleanup(values, value_update, keep_list):
    print('Cleaning up {} disturbance'.format(values))
    print(value_update)
    

    fc = "{}_disturb_singlepart_union_meatball_spatialjoin9".format(value_update)
    fieldObjList = arcpy.ListFields(fc)
    fieldNameList = []

    for field in fieldObjList:
            if not field.required:
                    fieldNameList.append(field.name)

    print(fieldNameList)

    for keep_field in keep_list:
        fieldNameList.remove(keep_field)
    fieldNameList.remove('Join_Count')
    fieldNameList.remove('disturbances')
    fieldNameList.remove('types')
    fieldNameList.remove('Cutblock_year')
    fieldNameList.remove('Pest_year')
    fieldNameList.remove('Fire_year')
    fieldNameList.remove('latest_cut')
    fieldNameList.remove('latest_fire')
    fieldNameList.remove('latest_pest')
    fieldNameList.remove('pest_severity')
    fieldNameList.remove('ORIG_FID')

    print(fieldNameList)

    arcpy.DeleteField_management(fc, fieldNameList)
    
    arcpy.JoinField_management("{}_disturb_flat".format(value_update), 'OBJECTID', fc, 'ORIG_FID')

    null_selection = ("disturbances IS NULL")
    null = arcpy.SelectLayerByAttribute_management("{}_disturb_flat".format(value_update), "NEW_SELECTION", null_selection)
    arcpy.DeleteFeatures_management(null)

    arcpy.AlterField_management("{}_disturb_flat".format(value_update), 'Join_Count', 'Number_Disturbance', 'Number of Overlapping Disturbances')

    arcpy.AddField_management("{}_disturb_flat".format(value_update), "most_recent_pest", "TEXT", "", "", "", "Most Recent Pest Severity")
    arcpy.CalculateField_management("{}_disturb_flat".format(value_update), "most_recent_pest", '!pest_severity![-1]', "PYTHON3")

    arcpy.AddField_management("{}_disturb_flat".format(value_update), "area_ha", "DOUBLE", "", "", "", "Area Ha")
    arcpy.CalculateField_management("{}_disturb_flat".format(value_update), "area_ha", '!shape.area@HECTARES!', "PYTHON3")

    arcpy.AddField_management("{}_disturb_flat".format(value_update), "analysis_date", "DATE")
    arcpy.CalculateField_management("{}_disturb_flat".format(value_update), "analysis_date", 'datetime.datetime.now()', "PYTHON3")
################################################################################
def disturbance_buffer_flatten(values,value_update):
    print(values)    
    print(value_update)
        
    disturbance_buffer =('{}_disturbance_buffer_final'.format(value_update))

    print("Starting on {}".format(disturbance_buffer))

    # Make the overlapping multipart protection into a singlepart layer
    arcpy.management.MultipartToSinglepart(disturbance_buffer, "{}_disturb_buffer_singlepart".format(value_update))
    print('Multi-part to singlepart done')

    # Union together the singlepart
    arcpy.analysis.Union(["{}_disturb_buffer_singlepart".format(value_update)], "{}_disturb_buffer_singlepart_union".format(value_update), cluster_tolerance= "1 METER")
    print('Singlepart unioned')

    ##### Spaghetti Layer ##############################
    arcpy.FeatureToPolygon_management("{}_disturb_buffer_singlepart_union".format(value_update), "{}_disturb_buffer_singlepart_union_polygon".format(value_update))
    print('Feature to Polygon done')

    arcpy.management.MultipartToSinglepart("{}_disturb_buffer_singlepart_union_polygon".format(value_update), "{}_disturb_buffer_flat".format(value_update))

    print('Polygon singleparted')

    fieldObjList = arcpy.ListFields("{}_disturb_buffer_flat".format(value_update))
    fieldNameList = []

    for field in fieldObjList:
            if not field.required:
                    fieldNameList.append(field.name)
            
    arcpy.DeleteField_management("{}_disturb_buffer_flat".format(value_update), fieldNameList)

    print('Spaghetti done')

    ##### Meatball Layer ################################
    try:
            arcpy.FeatureToPoint_management("{}_disturb_buffer_flat".format(value_update), "{}_disturb_buffer_singlepart_union_meatball".format(value_update), point_location = "INSIDE")
    
    except arcpy.ExecuteError:
            arcpy.RepairGeometry_management("{}_disturb_buffer_flat".format(value_update))
    
            arcpy.FeatureToPoint_management("{}_disturb_buffer_flat".format(value_update), "{}_disturb_buffer_singlepart_union_meatball".format(value_update), point_location = "INSIDE")
    print('Meatballs done')

    fieldObjList = arcpy.ListFields("{}_disturb_buffer_singlepart_union_meatball".format(value_update))
    fieldNameList = []

    for field in fieldObjList:
            if not field.required:
                    fieldNameList.append(field.name)
    
    print('Meatballs fields done')
def disturbance_buffer_field_mapping(values, value_update):
    delete_layer = []
    print(values)    
    print(value_update)
    
    # #######
    targetFeatures = ("{}_disturb_buffer_singlepart_union_meatball".format(value_update))
    joinFeatures = ("{}_disturb_buffer_singlepart_union".format(value_update))

    outfc = ("{}_disturb_buffer_singlepart_union_meatball_spatialjoin1".format(value_update))

    fieldmappings = arcpy.FieldMappings()
    fieldmappings.addTable(targetFeatures)
    fieldmappings.addTable(joinFeatures)

    desigFieldIndex= fieldmappings.findFieldMapIndex("disturbance")
    fieldmap = fieldmappings.getFieldMap(desigFieldIndex)
    field = fieldmap.outputField

    field.name = "disturbances_buffer"
    field.aliasName = "Disturbance List (Buffer)"
    field.length = '600'
    fieldmap.outputField = field

    fieldmap.mergeRule = 'Join'
    fieldmap.joinDelimiter = '; '

    fieldmappings.replaceFieldMap(desigFieldIndex, fieldmap)

    arcpy.SpatialJoin_analysis(targetFeatures, joinFeatures, outfc, 'JOIN_ONE_TO_ONE', '#', fieldmappings, 'INTERSECT')
    delete_layer.append("{}_disturb_buffer_singlepart_union_meatball_spatialjoin1".format(value_update))
    delete_layer.append("{}_disturb_buffer_singlepart_union_meatball".format(value_update))

    arcpy.DeleteField_management("{}_disturb_buffer_singlepart_union_meatball_spatialjoin1".format(value_update), 'type')

    print("done field map 1")
    
    ############

    targetFeatures = ("{}_disturb_buffer_singlepart_union_meatball_spatialjoin1".format(value_update))
    joinFeatures = ("{}_disturb_buffer_singlepart_union".format(value_update))

    outfc = ("{}_disturb_buffer_singlepart_union_meatball_spatialjoin2".format(value_update))

    fieldmappings = arcpy.FieldMappings()
    fieldmappings.addTable(targetFeatures)
    fieldmappings.addTable(joinFeatures)

    desigFieldIndex= fieldmappings.findFieldMapIndex("type")
    fieldmap = fieldmappings.getFieldMap(desigFieldIndex)
    field = fieldmap.outputField

    field.name = "types_buffer"
    field.aliasName = "Type List (Buffer)"
    field.length = '1010'
    fieldmap.outputField = field

    fieldmap.mergeRule = 'Join'
    fieldmap.joinDelimiter = '; '

    fieldmappings.replaceFieldMap(desigFieldIndex, fieldmap)

    arcpy.SpatialJoin_analysis(targetFeatures, joinFeatures, outfc, 'JOIN_ONE_TO_ONE', '#', fieldmappings, 'INTERSECT')
    delete_layer.append("{}_disturb_buffer_singlepart_union_meatball_spatialjoin1".format(value_update))

    arcpy.DeleteField_management("{}_disturb_buffer_singlepart_union_meatball_spatialjoin2".format(value_update), 'year')
    
    print("done field map 2")

    ###########

    year = ['cutblock buffer']

    for disturb in year:
            selection = "disturbance = '{}'".format(disturb)
            layer = arcpy.SelectLayerByAttribute_management("{}_disturb_buffer_singlepart_union".format(value_update), "NEW_SELECTION", selection)
            arcpy.CopyFeatures_management(layer, '{}_cutblock_lyr_buffer'.format(value_update))
            #arcpy.CopyFeatures_management(layer, '{}_cutblock_lyr_buffer'.format(value_update, disturb))
            delete_layer.append('{}_{}_lyr_buffer'.format(value_update, disturb))
    
    ###################
    targetFeatures = ("{}_disturb_buffer_singlepart_union_meatball_spatialjoin2".format(value_update))
    joinFeatures = ('{}_cutblock_lyr_buffer'.format(value_update))

    outfc = ("{}_disturb_buffer_singlepart_union_meatball_spatialjoin3".format(value_update))

    fieldmappings = arcpy.FieldMappings()
    fieldmappings.addTable(targetFeatures)
    fieldmappings.addTable(joinFeatures)

    desigFieldIndex= fieldmappings.findFieldMapIndex("year")
    fieldmap = fieldmappings.getFieldMap(desigFieldIndex)
    field = fieldmap.outputField

    field.name = "Cutblock_year_buffer"
    field.aliasName = "Cutblock Years (Buffer)"
    field.length = '600'
    field.type = 'Text'
    fieldmap.outputField = field

    fieldmap.mergeRule = 'Join'
    fieldmap.joinDelimiter = '; '

    fieldmappings.replaceFieldMap(desigFieldIndex, fieldmap)

    arcpy.SpatialJoin_analysis(targetFeatures, joinFeatures, outfc, 'JOIN_ONE_TO_ONE', '#', fieldmappings, 'INTERSECT')
    delete_layer.append("{}_disturb_buffer_singlepart_union_meatball_spatialjoin3".format(value_update))

    print("done field map 3")

    ###################
    targetFeatures = ("{}_disturb_buffer_singlepart_union_meatball_spatialjoin3".format(value_update))
    joinFeatures = ('{}_cutblock_lyr_buffer'.format(value_update))

    outfc = ("{}_disturb_buffer_singlepart_union_meatball_spatialjoin4".format(value_update))

    fieldmappings = arcpy.FieldMappings()
    fieldmappings.addTable(targetFeatures)
    fieldmappings.addTable(joinFeatures)

    desigFieldIndex= fieldmappings.findFieldMapIndex("year")
    fieldmap = fieldmappings.getFieldMap(desigFieldIndex)
    field = fieldmap.outputField

    field.name = "latest_cut_buffer"
    field.aliasName = "Latest Cutblock (Buffer)"
    field.type = 'Double'
    fieldmap.outputField = field

    fieldmap.mergeRule = 'MAX'

    fieldmappings.replaceFieldMap(desigFieldIndex, fieldmap)

    arcpy.SpatialJoin_analysis(targetFeatures, joinFeatures, outfc, 'JOIN_ONE_TO_ONE', '#', fieldmappings, 'INTERSECT')

    for layer in delete_layer:
            arcpy.Delete_management(layer)      
def disturbance_buffer_cleanup(values, value_update, keep_list):
    print(values)
    print(value_update)
    
    fc = "{}_disturb_buffer_singlepart_union_meatball_spatialjoin4".format(value_update)
    fieldObjList = arcpy.ListFields(fc)
    fieldNameList = []

    for field in fieldObjList:
            if not field.required:
                    fieldNameList.append(field.name)

    print(fieldNameList)
    for keep_field in keep_list:
            fieldNameList.remove(keep_field)
    fieldNameList.remove('Join_Count')
    fieldNameList.remove('disturbances_buffer')
    fieldNameList.remove('types_buffer')
    fieldNameList.remove('Cutblock_year_buffer')
    fieldNameList.remove('latest_cut_buffer')
    fieldNameList.remove('ORIG_FID')

    print(fieldNameList)

    arcpy.DeleteField_management(fc, fieldNameList)
    
    arcpy.JoinField_management("{}_disturb_buffer_flat".format(value_update), 'OBJECTID', fc, 'ORIG_FID')

    arcpy.Delete_management(fc)
    null_selection = ("disturbances_buffer IS NULL")
    null = arcpy.SelectLayerByAttribute_management("{}_disturb_buffer_flat".format(value_update), "NEW_SELECTION", null_selection)
    arcpy.DeleteFeatures_management(null)

    arcpy.AlterField_management("{}_disturb_buffer_flat".format(value_update), 'Join_Count', 'Number_Disturbance_buff', 'Number of Overlapping Disturbances (Buffer)')
    
    arcpy.AddField_management("{}_disturb_buffer_flat".format(value_update), "area_ha", "DOUBLE", "", "", "", "Area Ha")
    arcpy.CalculateField_management("{}_disturb_buffer_flat".format(value_update), "area_ha", '!shape.area@HECTARES!', "PYTHON3")
################################################################################
def delete_layers():
    cleanupfeatures = arcpy.ListFeatureClasses()

    final_str = ("_final", "_flat")
    for intersect_f in cleanupfeatures:
        if intersect_f.endswith(final_str):
            pass
        else:
            arcpy.Delete_management(intersect_f)
def identity(csv_dir, values, value_update, unique_value, intersect_layer, aoi_location):
    # get all that start with values name and end with flat
    print(values)
    print(value_update)

    layer_location = (aoi_location + intersect_layer)
    values_query = """{} = '{}'""".format(unique_value, values)
    values_select = arcpy.SelectLayerByAttribute_management(layer_location, "NEW_SELECTION", values_query)
    arcpy.CopyFeatures_management(values_select, 'aoi')
    
    arcpy.analysis.Identity('aoi', value_update + "_disturb_flat", value_update + '_disturb_identity_1')
    arcpy.analysis.Identity(value_update + '_disturb_identity_1', value_update + "_disturb_buffer_flat", 
                            value_update + '_flat')

    arcpy.TableToTable_conversion("{}_flat".format(value_update), csv_dir, "{}_flat.csv".format(value_update))