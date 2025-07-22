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
def combine_disturbance_and_protection(value_update):
    
    protection_layers = arcpy.ListFeatureClasses()

    for protection in protection_layers:
        if protection.startswith(value_update):
                print(protection)
                
                herd_select = ("HERD_NAME = '{}'".format(herd))
                selection = arcpy.SelectLayerByAttribute_management(habitat, "NEW_SELECTION", herd_select)
                arcpy.CopyFeatures_management(selection, '{}_habitat'.format(herd_update))

                arcpy.Identity_analysis('{}_habitat'.format(herd_update), protections + protection, "{}_habitat_protection".format(herd_update))
                arcpy.Identity_analysis("{}_habitat_protection".format(herd_update),"{}_flat".format(herd_update), "{}_final_flat".format(herd_update))

                print("Done identity for {}".format(herd_update))
        else:
            pass

def clean_up():
    cleanupfeatures = arcpy.ListFeatureClasses()

    final_str = ("_final", "_flat")
    for intersect_f in cleanupfeatures:
        if intersect_f.endswith(final_str):
            pass
        else:
            arcpy.Delete_management(intersect_f)

