'''
    BC Provincial Protection Analysis for Caribou
    
    ArcGIS Version. 10.6
    Author:  Jamiee Remond 
    Date:    05-08-2020

    
    Purpose:   To create disturbance layers for caribou herd boundaries to be used as input in R scripts to measure disturbance on the landcape
    
    NOTE:  This script is provided as-is.  It is not meant as a 'plug and play' script - i.e. considerable updating and knowledge of the data 
    and assessment assumptions will be required to re-run this script.  Review of each processing step is recommended before running.   
        
    Arguments:   Read comments to see where to change variables
          
    Dependencies:  You must have herd boundaries and habitat layers for Caribou. Have access to the BCGW and BCCE disturbance data as well.
     
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
# Function goes through area of interest (AOI) to start the intersection of protection layers
def protect_aoi(aoi_location, layer_name, unique_value):
    aoi = os.path.join(aoi_location,layer_name)

    search_word = "{}".format(unique_value)

    with arcpy.da.SearchCursor(aoi, [search_word]) as cursor:
        values_sorted = sorted({row[0] for row in cursor})
    print('Running protection on: {}'.format(values_sorted))

    print("AOI loaded")
# Function clips the designated lands (protection) layer by each AOI created in the protection function of Run_Disturbance
def gather_protection(designated_lands, value_update):
    arcpy.analysis.Clip(designated_lands, 'aoi', f"{value_update}_designated_lands_clip")

    arcpy.management.Dissolve(f"{value_update}_designated_lands_clip", f"{value_update}_designated_lands", ['designation', 'source_name', 'forest_restriction', 'mine_restriction', 'og_restriction'])
# Using the Spaghetti and Meatballs method (see disturbance) protection overlap relationship is created
def flatten_protection(value_update):
    delete_layer = []
    protection_layer =('{}_designated_lands'.format(value_update))

    print("Starting on {}".format(protection_layer))

    # Make the overlapping multipart protection into a singlepart layer
    arcpy.management.MultipartToSinglepart(protection_layer, "{}_singlepart".format(protection_layer))
    delete_layer.append("{}_singlepart".format(protection_layer))

    print('Multi-part to singlepart done')

    # Union together the singlepart
    arcpy.analysis.Union(["{}_singlepart".format(protection_layer)], "{}_d_singlepart_union".format(protection_layer), cluster_tolerance= "1 METER")
    
    print('Singlepart unioned')

    ##### Spaghetti Layer ##############################
    arcpy.FeatureToPolygon_management("{}_d_singlepart_union".format(protection_layer), "{}_singlepart_union_polygon".format(protection_layer))
 
    print('Feature to Polygon done')

    arcpy.management.MultipartToSinglepart("{}_singlepart_union_polygon".format(protection_layer), "{}_protect_flat".format(value_update))

    print('Polygon singleparted')

    fieldObjList = arcpy.ListFields("{}_protect_flat".format(value_update))
    fieldNameList = []

    for field in fieldObjList:
            if not field.required:
                    fieldNameList.append(field.name)
            
    arcpy.DeleteField_management("{}_protect_flat".format(value_update), fieldNameList)

    print('Spaghetti done')

    ##### Meatball Layer ################################
    arcpy.RepairGeometry_management("{}_protect_flat".format(value_update))
    arcpy.FeatureToPoint_management("{}_protect_flat".format(value_update), "{}_protect_singlepart_union_meatball".format(value_update), point_location = "INSIDE")

    print('Meatballs done')

    fieldObjList = arcpy.ListFields("{}_protect_singlepart_union_meatball".format(value_update))
    fieldNameList = []

    for field in fieldObjList:
            if not field.required:
                    fieldNameList.append(field.name)
    
    print('Meatballs fields done')
# To read more about field mapping - https://pro.arcgis.com/en/pro-app/latest/arcpy/classes/fieldmappings.htm
def field_mapping(value_update):
   #######
    targetFeatures = ("{}_protect_singlepart_union_meatball".format(value_update))
    joinFeatures = ("{}_designated_lands_d_singlepart_union".format(value_update))

    outfc = ("{}_protect_singlepart_union_meatball_spatialjoin1".format(value_update))

    fieldmappings = arcpy.FieldMappings()
    fieldmappings.addTable(targetFeatures)
    fieldmappings.addTable(joinFeatures)

    desigFieldIndex= fieldmappings.findFieldMapIndex("designation")
    fieldmap = fieldmappings.getFieldMap(desigFieldIndex)
    field = fieldmap.outputField

    field.name = "designations"
    field.aliasName = "Designations List"
    field.length = '600'
    fieldmap.outputField = field

    fieldmap.mergeRule = 'Join'
    fieldmap.joinDelimiter = '; '

    fieldmappings.replaceFieldMap(desigFieldIndex, fieldmap)

    arcpy.SpatialJoin_analysis(targetFeatures, joinFeatures, outfc, 'JOIN_ONE_TO_ONE', '#', fieldmappings, 'INTERSECT')

    arcpy.DeleteField_management("{}_protect_singlepart_union_meatball_spatialjoin1".format(value_update), 'source_name')
    
    ############

    targetFeatures = ("{}_protect_singlepart_union_meatball_spatialjoin1".format(value_update))
    joinFeatures = ("{}_designated_lands_d_singlepart_union".format(value_update))

    outfc = ("{}_protect_singlepart_union_meatball_spatialjoin2".format(value_update))

    fieldmappings = arcpy.FieldMappings()
    fieldmappings.addTable(targetFeatures)
    fieldmappings.addTable(joinFeatures)

    desigFieldIndex= fieldmappings.findFieldMapIndex("source_name")
    fieldmap = fieldmappings.getFieldMap(desigFieldIndex)
    field = fieldmap.outputField

    field.name = "sources_list"
    field.aliasName = "Sources List"
    field.length = '600'
    fieldmap.outputField = field

    fieldmap.mergeRule = 'Join'
    fieldmap.joinDelimiter = '; '

    fieldmappings.replaceFieldMap(desigFieldIndex, fieldmap)

    arcpy.SpatialJoin_analysis(targetFeatures, joinFeatures, outfc, 'JOIN_ONE_TO_ONE', '#', fieldmappings, 'INTERSECT')

    arcpy.DeleteField_management("{}_protect_singlepart_union_meatball_spatialjoin2".format(value_update), 'forest_restriction')
    
    ##############################
    targetFeatures = ("{}_protect_singlepart_union_meatball_spatialjoin2".format(value_update))
    joinFeatures = ("{}_designated_lands_d_singlepart_union".format(value_update))

    outfc = ("{}_protect_singlepart_union_meatball_spatialjoin3".format(value_update))

    fieldmappings = arcpy.FieldMappings()
    fieldmappings.addTable(targetFeatures)
    fieldmappings.addTable(joinFeatures)

    desigFieldIndex= fieldmappings.findFieldMapIndex("forest_restriction")
    fieldmap = fieldmappings.getFieldMap(desigFieldIndex)
    field = fieldmap.outputField

    field.name = "forest_restriction_list"
    field.aliasName = "Forest Restrictions"
    field.length = '600'
    field.type = 'Text'
    fieldmap.outputField = field

    fieldmap.mergeRule = 'Join'
    fieldmap.joinDelimiter = '; '

    fieldmappings.replaceFieldMap(desigFieldIndex, fieldmap)

    arcpy.SpatialJoin_analysis(targetFeatures, joinFeatures, outfc, 'JOIN_ONE_TO_ONE', '#', fieldmappings, 'INTERSECT')

    ###################
    targetFeatures = ("{}_protect_singlepart_union_meatball_spatialjoin3".format(value_update))
    joinFeatures = ("{}_designated_lands_d_singlepart_union".format(value_update))

    outfc = ("{}_protect_singlepart_union_meatball_spatialjoin4".format(value_update))

    fieldmappings = arcpy.FieldMappings()
    fieldmappings.addTable(targetFeatures)
    fieldmappings.addTable(joinFeatures)

    desigFieldIndex= fieldmappings.findFieldMapIndex("forest_restriction")
    fieldmap = fieldmappings.getFieldMap(desigFieldIndex)
    field = fieldmap.outputField

    field.name = "max_forest_restrict"
    field.aliasName = "Maxium Forest Restriction"
    field.length = '600'
    fieldmap.outputField = field

    fieldmap.mergeRule = 'Max'

    fieldmappings.replaceFieldMap(desigFieldIndex, fieldmap)

    arcpy.SpatialJoin_analysis(targetFeatures, joinFeatures, outfc, 'JOIN_ONE_TO_ONE', '#', fieldmappings, 'INTERSECT')
    arcpy.DeleteField_management("{}_protect_singlepart_union_meatball_spatialjoin4".format(value_update), 'mine_restriction')

    # ###################
    targetFeatures = ("{}_protect_singlepart_union_meatball_spatialjoin4".format(value_update))
    joinFeatures = ("{}_designated_lands_d_singlepart_union".format(value_update))

    outfc = ("{}_protect_singlepart_union_meatball_spatialjoin5".format(value_update))

    fieldmappings = arcpy.FieldMappings()
    fieldmappings.addTable(targetFeatures)
    fieldmappings.addTable(joinFeatures)

    desigFieldIndex= fieldmappings.findFieldMapIndex("mine_restriction")
    fieldmap = fieldmappings.getFieldMap(desigFieldIndex)
    field = fieldmap.outputField

    field.name = "mine_restriction_list"
    field.aliasName = "Mine Restrictions"
    field.length = '600'
    field.type = 'Text'
    fieldmap.outputField = field

    fieldmap.mergeRule = 'Join'
    fieldmap.joinDelimiter = '; '

    fieldmappings.replaceFieldMap(desigFieldIndex, fieldmap)

    arcpy.SpatialJoin_analysis(targetFeatures, joinFeatures, outfc, 'JOIN_ONE_TO_ONE', '#', fieldmappings, 'INTERSECT')
    
    # ###################
    targetFeatures = ("{}_protect_singlepart_union_meatball_spatialjoin5".format(value_update))
    joinFeatures = ("{}_designated_lands_d_singlepart_union".format(value_update))

    outfc = ("{}_protect_singlepart_union_meatball_spatialjoin6".format(value_update))

    fieldmappings = arcpy.FieldMappings()
    fieldmappings.addTable(targetFeatures)
    fieldmappings.addTable(joinFeatures)

    desigFieldIndex= fieldmappings.findFieldMapIndex("mine_restriction")
    fieldmap = fieldmappings.getFieldMap(desigFieldIndex)
    field = fieldmap.outputField

    field.name = "max_mine_restriction"
    field.aliasName = "Maxium Mine Restriction"
    field.length = '600'
    fieldmap.outputField = field

    fieldmap.mergeRule = 'Max'

    fieldmappings.replaceFieldMap(desigFieldIndex, fieldmap)

    arcpy.SpatialJoin_analysis(targetFeatures, joinFeatures, outfc, 'JOIN_ONE_TO_ONE', '#', fieldmappings, 'INTERSECT')
    arcpy.DeleteField_management("{}_protect_singlepart_union_meatball_spatialjoin6".format(value_update), 'og_restriction')

   # ###################
    targetFeatures = ("{}_protect_singlepart_union_meatball_spatialjoin6".format(value_update))
    joinFeatures = ("{}_designated_lands_d_singlepart_union".format(value_update))

    outfc = ("{}_protect_singlepart_union_meatball_spatialjoin7".format(value_update))

    fieldmappings = arcpy.FieldMappings()
    fieldmappings.addTable(targetFeatures)
    fieldmappings.addTable(joinFeatures)

    desigFieldIndex= fieldmappings.findFieldMapIndex("og_restriction")
    fieldmap = fieldmappings.getFieldMap(desigFieldIndex)
    field = fieldmap.outputField

    field.name = "og_restriction_list"
    field.aliasName = "Oil and Gas Restrictions"
    field.length = '600'
    field.type = 'Text'
    fieldmap.outputField = field

    fieldmap.mergeRule = 'Join'
    fieldmap.joinDelimiter = '; '

    fieldmappings.replaceFieldMap(desigFieldIndex, fieldmap)

    arcpy.SpatialJoin_analysis(targetFeatures, joinFeatures, outfc, 'JOIN_ONE_TO_ONE', '#', fieldmappings, 'INTERSECT')
    
    # ###################
    targetFeatures = ("{}_protect_singlepart_union_meatball_spatialjoin7".format(value_update))
    joinFeatures = ("{}_designated_lands_d_singlepart_union".format(value_update))

    outfc = ("{}_protect_singlepart_union_meatball_spatialjoin8".format(value_update))

    fieldmappings = arcpy.FieldMappings()
    fieldmappings.addTable(targetFeatures)
    fieldmappings.addTable(joinFeatures)

    desigFieldIndex= fieldmappings.findFieldMapIndex("og_restriction")
    fieldmap = fieldmappings.getFieldMap(desigFieldIndex)
    field = fieldmap.outputField

    field.name = "max_og_restriction"
    field.aliasName = "Maxium Oil and Gas Restriction"
    field.length = '600'
    fieldmap.outputField = field

    fieldmap.mergeRule = 'Max'

    fieldmappings.replaceFieldMap(desigFieldIndex, fieldmap)

    arcpy.SpatialJoin_analysis(targetFeatures, joinFeatures, outfc, 'JOIN_ONE_TO_ONE', '#', fieldmappings, 'INTERSECT')
# Final part of spaghetti and meatballs - joining the polygons and attributes back together     
def clean_and_join(value_update, keep_list):
    fc = "{}_protect_singlepart_union_meatball_spatialjoin8".format(value_update)
    fieldObjList = arcpy.ListFields(fc)
    fieldNameList = []

    for field in fieldObjList:
            if not field.required:
                    fieldNameList.append(field.name)

    print(fieldNameList)

    for field in keep_list:
        if field not in fieldNameList:
            pass
        else:
            fieldNameList.remove(field)
    fieldNameList.remove('Join_Count')
    fieldNameList.remove('designations')
    fieldNameList.remove('mine_restriction_list')
    fieldNameList.remove('og_restriction_list')
    fieldNameList.remove('sources_list')
    fieldNameList.remove('forest_restriction_list')
    fieldNameList.remove('max_forest_restrict')
    fieldNameList.remove('max_mine_restriction')
    fieldNameList.remove('max_og_restriction')
    fieldNameList.remove('ORIG_FID')


    print(fieldNameList)

    arcpy.DeleteField_management(fc, fieldNameList)
    
    arcpy.JoinField_management("{}_protect_flat".format(value_update), 'OBJECTID', fc, 'ORIG_FID')

    null_selection = ("designations IS NULL")
    null = arcpy.SelectLayerByAttribute_management("{}_protect_flat".format(value_update), "NEW_SELECTION", null_selection)
    arcpy.DeleteFeatures_management(null)

    arcpy.AlterField_management("{}_protect_flat".format(value_update), 'Join_Count', 'Number_Protection', 'Number of Overlapping Protections')

    # arcpy.AddField_management("{}_protect_flat".format(value_update), "area_ha", "DOUBLE", "", "", "", "Area Ha")
    # arcpy.CalculateField_management("{}_protect_flat".format(value_update), "area_ha", '!shape.area@HECTARES!', "PYTHON3")

    arcpy.AddField_management("{}_protect_flat".format(value_update), "analysis_date", "DATE")
    arcpy.CalculateField_management("{}_protect_flat".format(value_update), "analysis_date", 'datetime.datetime.now()', "PYTHON3")

    cleanupfeatures = arcpy.ListFeatureClasses()

    final_str = ("_flat")
    for intersect_f in cleanupfeatures:
        if intersect_f.endswith(final_str):
            pass
        else:
            arcpy.Delete_management(intersect_f)
    
def combine(values, value_update, unique_value, intersect_layer, aoi_location):

    layer_location = (aoi_location + intersect_layer)
    values_query = """{} = '{}'""".format(unique_value, values)
    values_select = arcpy.SelectLayerByAttribute_management(layer_location, "NEW_SELECTION", values_query)
    arcpy.CopyFeatures_management(values_select, 'aoi')
    features = arcpy.ListFeatureClasses()
    
    protection_layers = []
    for protection in features:
        if protection.endswith('_protect_flat'):
            protection_layers.append(protection)

    for protection in protection_layers:
        if protection.startswith(value_update):
            arcpy.Identity_analysis('aoi', protection, "{}_protect_intersect".format(value_update))
            arcpy.Identity_analysis("{}_protect_intersect".format(value_update),"{}_flat".format(value_update), "{}_final_flat".format(value_update))

            print("Done identity for {}".format(value_update))

            arcpy.Delete_management(protection)
            arcpy.Rename_management("{}_protect_intersect".format(value_update), "{}_protect_flat".format(value_update))