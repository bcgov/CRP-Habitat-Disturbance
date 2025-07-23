#################################################################################
#### Clean habitat linework and herd range boundary data for use in analysis ####
#################################################################################

def prepare_data(working_loc, linework, range_bound, designated_lands_loc, output_location, uname, pword, inst ):
    """
    Clean habitat linework and herd range boundary data for use in analysis
    
    Parameters:
    working_loc (str): Path to the working location directory
    linework (str): Path to the habitat linework
    range_bound (str): Path to the range boundaries
    """
    import arcpy
    import os

    wrkspc_loc = os.path.join(working_loc, "AOI.gdb")
    desginated_loc=os.path.join(working_loc, "updated_designated.gdb")

    if arcpy.exists(os.path.join(desginated_loc,"designations_ogma_update_221013")):
        print('data prep already completed')
        quit

    arcpy.env.parallelProcessingFactor = "50%"
    arcpy.env.overwriteOutput = True
    if not arcpy.Exists(wrkspc_loc):
        print("Creating AOI.gdb in working location")
        arcpy.management.CreateFileGDB(out_folder_path= working_loc , out_name="AOI.gdb" )
    arcpy.env.workspace = wrkspc_loc

    # Habitat linework

    herd_name = []
    with arcpy.da.SearchCursor(linework, ["Herd_Name"]) as cursor:
        for row in cursor:
            herd_name.append(row[0])
    herd_name = list(set(herd_name))

    # Extract to gdb
    for h in herd_name:
        value_update = h.replace(" ", "")
        value_update = value_update.replace("-", "") 
        value_update = value_update.replace(":", "") 
        value_update = value_update.replace("/", "") 
        arcpy.Select_analysis(linework, value_update, "Herd_Name = '{0}'".format(h))

    # Get boundaries for areas without linework
    herd_name_range = []
    with arcpy.da.SearchCursor(range_bound, ["Herd_Name"]) as cursor:
        for row in cursor:
            herd_name_range.append(row[0])
    herd_name_range = list(set(herd_name_range))

    herd_name_noline = sorted([i for i in herd_name_range if i not in herd_name])
    herd_name_noline.remove('Klinse-za')

    # Extract to gdb
    for h in herd_name_noline:
        value_update = h.replace(" ", "")
        value_update = value_update.replace("-", "") 
        value_update = value_update.replace(":", "") 
        value_update = value_update.replace("/", "") 
        arcpy.Select_analysis(range_bound, value_update, "Herd_Name = '{0}'".format(h))
        arcpy.DeleteField_management(value_update, "HERD_NAME")
        arcpy.management.AddField(value_update, "Herd_Name", "TEXT", field_alias = "Herd Name")
        if len(arcpy.ListFields(value_update, "BCHab_code")) == 0:
            arcpy.AddField_management(value_update, "BCHab_code", "TEXT")
        with arcpy.da.UpdateCursor(value_update, ["BCHab_code", "Herd_Name"]) as cursor:
            for row in cursor:
                row[0] = "Boundary"
                row[1] = h 
                cursor.updateRow(row)
        
    # Update range name 
    arcpy.Rename_management("Moberly", "Klinseza")
    with arcpy.da.UpdateCursor("Klinseza", ["Herd_Name"]) as cursor:
        for row in cursor:
            row[0] = "Klinseza"
            cursor.updateRow(row)
            
    # Add area in ha
    fc_list = arcpy.ListFeatureClasses()

    for f in fc_list:
        if len(arcpy.ListFields(f, "Area_Ha")) == 0:
            arcpy.AddField_management(f, "Area_Ha", "FLOAT")
        with arcpy.da.UpdateCursor(f, ["Shape_Area", "Area_Ha"]) as cursor:
            for row in cursor:
                row[1] = row[0] * 0.0001
                cursor.updateRow(row)    

    bcgw_connection =os.path.join(output_location,"BCGW.sde")

    print(bcgw_connection)

    try:
                arcpy.CreateDatabaseConnection_management(out_folder_path=output_location,
                                                        out_name="bcgw.sde",
                                                        database_platform='ORACLE',
                                                        instance=inst,
                                                        account_authentication='DATABASE_AUTH',
                                                        username=uname,
                                                        password=pword,
                                                        save_user_pass='DO_NOT_SAVE_USERNAME')
                print(' new SDE connection')
    except:
        print('Database connection already exists')

    if not arcpy.Exists(desginated_loc):
        arcpy.management.CreateFileGDB(out_folder_path=working_loc, out_name="updated_designated.gdb")
        print('Created updated designated gdb')
    else:
        print('Updated designated gdb already exists')

    arcpy.env.workspace = desginated_loc
    arcpy.management.Copy(bcgw_connection + r"\WHSE_LAND_USE_PLANNING.RMP_OGMA_NON_LEGAL_CURRENT_SVW", "OGMA_nonlegal")

    # Match designated lands schema
    fc_fields = [f.name for f in arcpy.ListFields("OGMA_nonlegal")]

    for f in fc_fields:
        if not f in ["OBJECTID", "GEOMETRY", "GEOMETRY_Length", "GEOMETRY_Area"]:
            arcpy.DeleteField_management("OGMA_nonlegal", f)

    if len(arcpy.ListFields("OGMA_nonlegal", "designation")) == 0:
        arcpy.AddField_management("OGMA_nonlegal", "designation", "TEXT") 
    if len(arcpy.ListFields("OGMA_nonlegal", "source_name")) == 0:
        arcpy.AddField_management("OGMA_nonlegal", "source_name", "TEXT") 
    if len(arcpy.ListFields("OGMA_nonlegal", "forest_restriction")) == 0:
        arcpy.AddField_management("OGMA_nonlegal", "forest_restriction", "SHORT") 
    if len(arcpy.ListFields("OGMA_nonlegal", "mine_restriction")) == 0:
        arcpy.AddField_management("OGMA_nonlegal", "mine_restriction", "SHORT") 
    if len(arcpy.ListFields("OGMA_nonlegal", "og_restriction")) == 0:
        arcpy.AddField_management("OGMA_nonlegal", "og_restriction", "SHORT") 
        
    with arcpy.da.UpdateCursor("OGMA_nonlegal",["designation", "source_name", 
                                                "forest_restriction","mine_restriction",
                                                "og_restriction"]) as cursor:
        for row in cursor:
            row[0] = "ogma"
            row[1] = "ogma_nonlegal"
            row[2] = 3
            row[3] = 0
            row[4] = 2
            cursor.updateRow(row)
    
    arcpy.Merge_management([designated_lands_loc, "OGMA_nonlegal"], "designations_ogma_update_221013")

    with arcpy.da.UpdateCursor("designations_ogma_update_221013",["designation"]) as cursor:
        for row in cursor:
            if row[0] == "ogma_legal":
                row[0] = "ogma"
                cursor.updateRow(row)  
        
        
        
        
    
    
    
    
    
    
    
    
    
