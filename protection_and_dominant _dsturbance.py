#!/usr/bin/env python3
"""
Caribou Protection and Dominant Disturbance Analysis - Python/ArcPy Version
Converted from R script: PRO_DOM_DIST_2025_UPDATE_ALL_DU.R Written by Bevan Ernst, converted by CFOLKERS

This script processes caribou habitat and disturbance data for different ecotypes:
- Boreal
- Northern Mountain Caribou (NMC) 
- Southern Mountain Caribou Northern Group (SMC_NG)

The script performs spatial analysis to determine dominant disturbance types
and calculates protection statistics for caribou herds.
"""

import arcpy
import pandas as pd
import numpy as np
import os
import time
import re
from pathlib import Path
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed
import warnings
from datetime import datetime
from dotenv import load_dotenv

formatted_date = datetime.strftime("%Y-%m-%d")

dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path)

eco_type_value = os.getenv("ECO_TYPE")
layer_name_list = os.getenv("LAYER_NAME").split(",")

# Suppress warnings
warnings.filterwarnings('ignore')

# Set overwrite output to True
arcpy.env.overwriteOutput = True

# Enable spatial analyst extension if available
try:
    if arcpy.CheckExtension("Spatial") == "Available":
        arcpy.CheckOutExtension("Spatial")
except:
    print("Spatial Analyst extension not available")

class CaribouDisturbanceAnalysis:
    """Class to handle caribou disturbance analysis"""
    
    def __init__(self):
        self.dominant_disturbance_colors = {
            "Cutblock": "#8B4513",  # chocolate4
            "Cutblock Buffer": "#F4A460",  # sandybrown
            "> 40 Year Disturbance": "#CD853F",  # darkorange3
            "Fire": "#FF0000",  # red
            "Other Disturbance Buffer": "#ADD8E6",  # lightblue
            "Pest": "#9400D3",  # darkviolet
            "Road": "#000000",  # black
            "Road Buffer": "#C0C0C0",  # gray76
            "Static": "#00008B",  # darkblue
            "Undisturbed": "#FFFF00"  # yellow
        }
        
        self.disturbance_levels = [
            "Cutblock", "Cutblock Buffer", "> 40 Year Disturbance", "Fire",
            "Other Disturbance Buffer", "Pest", "Road", "Road Buffer", 
            "Static", "Undisturbed"
        ]
    
    def check_geometry_type(self, feature_class):
        """Check geometry types in feature class"""
        geometry_types = {}
        with arcpy.da.SearchCursor(feature_class, ["SHAPE@"]) as cursor:
            for row in cursor:
                geom_type = row[0].type
                geometry_types[geom_type] = geometry_types.get(geom_type, 0) + 1
        print(f"Geometry types in {feature_class}: {geometry_types}")
        return geometry_types
    
    def check_geometry_validity(self, feature_class):
        """Check geometry validity"""
        valid_count = 0
        invalid_count = 0
        with arcpy.da.SearchCursor(feature_class, ["SHAPE@"]) as cursor:
            for row in cursor:
                if row[0] is not None:
                    valid_count += 1
                else:
                    invalid_count += 1
        print(f"Valid geometries: {valid_count}, Invalid geometries: {invalid_count}")
        return {"valid": valid_count, "invalid": invalid_count}
    
    def calculate_hectares(self, feature_class, hectares_field="Hectares"):
        """Calculate area in hectares"""
        print(f"Calculating hectares for {feature_class}")
        
        # Add hectares field if it doesn't exist
        field_names = [f.name for f in arcpy.ListFields(feature_class)]
        if hectares_field not in field_names:
            arcpy.AddField_management(feature_class, hectares_field, "DOUBLE")
        
        # Calculate hectares
        with arcpy.da.UpdateCursor(feature_class, ["SHAPE@AREA", hectares_field]) as cursor:
            for row in cursor:
                # Convert square meters to hectares
                hectares = row[0] / 10000.0
                row[1] = hectares
                cursor.updateRow(row)
    
    def get_herd_names(self, ecotype):
        """Get herd names for specific ecotype"""
        #change herd bounds path as needed
        herd_bounds_gdb = "\\\\spatialfiles.bcgov\\Work\\srm\\gss\\initiatives\\caribou_recovery\\projects\\gr_2025_1033_habitat_status\\source_data\\HERD_BOUND_2025_RENAME.gdb"
        
        # Read herd boundaries
        herds_df = []
        with arcpy.da.SearchCursor(herd_bounds_gdb, ["ECOTYPE", "HERD_NAME"]) as cursor:
            for row in cursor:
                if row[0] == ecotype:
                    herd_name = row[1].replace(" ", "").replace("-", "") + "_final_flat"
                    herds_df.append(herd_name)
        
        return herds_df
    
    def process_pest_data(self, feature_class):
        """Process pest severity data"""
        print(f"Processing pest data for {feature_class}")
        
        # Get field names
        field_names = [f.name for f in arcpy.ListFields(feature_class)]
        
        # Find pest-related fields
        pest_severity_fields = [f for f in field_names if "pest_severity" in f.lower()]
        pest_year_fields = [f for f in field_names if "pest_year" in f.lower()]
        
        # Add latest_pest field if it doesn't exist
        if "latest_pest" not in field_names:
            arcpy.AddField_management(feature_class, "latest_pest", "LONG")
        
        # Process pest data
        update_fields = pest_year_fields + ["latest_pest"]
        with arcpy.da.UpdateCursor(feature_class, update_fields) as cursor:
            for row in cursor:
                pest_years = []
                for i, year_val in enumerate(row[:-1]):  # Exclude latest_pest field
                    if year_val is not None and year_val.strip():  # Check if text exists and isn't just whitespace
                        # Split by semicolon and process each year
                        years = [y.strip() for y in year_val.split(';')]
                        # Convert to integers if needed
                        try:
                            numeric_years = [int(y) for y in years if y]
                            if numeric_years:  # If any valid years
                                pest_years.extend(numeric_years)
                        except ValueError:
                            # Handle case where conversion to int fails
                            print(f"Warning: Could not convert value '{year_val}' to integers")
                
                if pest_years:
                    row[-1] = max(pest_years)  # latest_pest
                else:
                    row[-1] = None
                
                cursor.updateRow(row)
    
    def assign_dominant_disturbance(self, feature_class):
        """Assign dominant disturbance type"""
        print(f"Assigning dominant disturbance for {feature_class}")
        
        # Add required fields
        field_names = [f.name for f in arcpy.ListFields(feature_class)]
        
        new_fields = [
            ("latest_temporal", "LONG"),
            ("latest_temporal_type", "TEXT"),
            ("Dominant_Disturbance", "TEXT"),
            ("Dominant_Distubance_Year", "LONG"),
            ("FILL_COLOR", "TEXT")
        ]
        
        for field_name, field_type in new_fields:
            if field_name not in field_names:
                arcpy.AddField_management(feature_class, field_name, field_type)
        
        # Get relevant fields for processing
        process_fields = [
            "latest_cut", "latest_pest", "latest_fire", "Number_Disturbance",
            "Number_Disturbance_buff", "disturbances", "types", "disturbances_buffer",
            "latest_cut_buffer", "latest_temporal", "latest_temporal_type",
            "Dominant_Disturbance", "Dominant_Distubance_Year", "FILL_COLOR"
        ]
        
        # Filter fields that exist
        existing_fields = [f for f in process_fields if f in field_names]
        
        with arcpy.da.UpdateCursor(feature_class, existing_fields) as cursor:
            for row in cursor:
                row_dict = dict(zip(existing_fields, row))
                
                # Calculate latest temporal disturbance
                temporal_values = []
                for field in ["latest_cut", "latest_pest", "latest_fire"]:
                    if field in row_dict and row_dict[field] is not None:
                        temporal_values.append(row_dict[field])
                
                latest_temporal = max(temporal_values) if temporal_values else 0
                row_dict["latest_temporal"] = latest_temporal
                
                # Determine latest temporal type
                latest_temporal_type = ""
                if latest_temporal > 0:
                    if row_dict.get("latest_cut") == latest_temporal:
                        latest_temporal_type = "Cutblock"
                    elif row_dict.get("latest_fire") == latest_temporal:
                        latest_temporal_type = "Fire"
                    elif row_dict.get("latest_pest") == latest_temporal:
                        latest_temporal_type = "Pest"
                
                row_dict["latest_temporal_type"] = latest_temporal_type
                
                # Assign dominant disturbance
                num_dist = row_dict.get("Number_Disturbance", 0) or 0
                num_dist_buff = row_dict.get("Number_Disturbance_buff", 0) or 0
                disturbances = row_dict.get("disturbances", "") or ""
                types = row_dict.get("types", "") or ""
                disturbances_buffer = row_dict.get("disturbances_buffer", "") or ""
                
                dominant_disturbance = "Undefined"
                # print(f"Processing row: {row_dict}")
                print(f"Latest Temporal: {latest_temporal}, Latest Type: {latest_temporal_type}")
                print(f"Number Disturbance: {num_dist}, Number Disturbance Buffer: {num_dist_buff}")
                if num_dist == 0 and num_dist_buff == 0:
                    dominant_disturbance = "Undisturbed"
                elif num_dist > 0 and "road" in disturbances.lower():
                    dominant_disturbance = "Road"
                elif num_dist > 0 and "static" in types.lower():
                    dominant_disturbance = "Static"
                elif num_dist > 0 and "temporal" in types.lower() and latest_temporal > 0:
                    dominant_disturbance = latest_temporal_type
                elif "cutblock" in disturbances_buffer.lower() and num_dist_buff > 0:
                    dominant_disturbance = "Cutblock Buffer"
                elif num_dist_buff > 0 and "road" in disturbances_buffer.lower():
                    dominant_disturbance = "Road Buffer"
                elif num_dist_buff > 0:
                    dominant_disturbance = "Other Disturbance Buffer"
                elif num_dist > 0 and latest_temporal is None:
                    dominant_disturbance = "Undisturbed"
                print(f"Assigned Dominant Disturbance: {dominant_disturbance}")
                
                # Handle > 40 year disturbance
                disturbance_year = latest_temporal if latest_temporal else (row_dict.get("latest_cut_buffer", 0) or 0)
                current_year = datetime.now().year
                year_40= current_year - 40
                if (disturbance_year < year_40 and disturbance_year > 0 and 
                    dominant_disturbance not in ["Road", "Static", "Undisturbed"]):
                    dominant_disturbance = "> 40 Year Disturbance"
                    print(f"Updated to > 40 Year Disturbance based on year: {disturbance_year}")
                row_dict["Dominant_Disturbance"] = dominant_disturbance
                row_dict["Dominant_Distubance_Year"] = disturbance_year if disturbance_year else 0
                row_dict["FILL_COLOR"] = self.dominant_disturbance_colors.get(dominant_disturbance, "#FFFFFF")
                
                # Update row
                updated_row = [row_dict.get(field) for field in existing_fields]
                cursor.updateRow(updated_row)
    
    def calculate_percentages(self, feature_class):
        """Calculate percentages by herd and habitat"""
        print(f"Calculating percentages for {feature_class}")
        
        # Add percentage field
        field_names = [f.name for f in arcpy.ListFields(feature_class)]
        if "Percent" not in field_names:
            arcpy.AddField_management(feature_class, "Percent", "DOUBLE")
        if "HERD_HAB_TOTAL" not in field_names:
            arcpy.AddField_management(feature_class, "HERD_HAB_TOTAL", "DOUBLE")
        
        # Calculate totals by herd and habitat
        herd_hab_totals = {}
        with arcpy.da.SearchCursor(feature_class, ["Herd_Name", "BCHab_code", "Hectares"]) as cursor:
            for row in cursor:
                herd_name, hab_code, hectares = row
                key = (herd_name, hab_code)
                if key not in herd_hab_totals:
                    herd_hab_totals[key] = 0
                herd_hab_totals[key] += hectares or 0
        
        # Update percentages
        with arcpy.da.UpdateCursor(feature_class, 
                                 ["Herd_Name", "BCHab_code", "Hectares", "HERD_HAB_TOTAL", "Percent"]) as cursor:
            for row in cursor:
                herd_name, hab_code, hectares, _, _ = row
                key = (herd_name, hab_code)
                total = herd_hab_totals.get(key, 1)  # Avoid division by zero
                
                row[3] = total  # HERD_HAB_TOTAL
                row[4] = (hectares / total * 100) if hectares and total > 0 else 0  # Percent
                
                cursor.updateRow(row)
    
    def process_ecotype_data(self, ecotype, source_gdb, output_folder):
        """Process data for a specific ecotype"""
        print(f"\n=== Processing {ecotype} ecotype ===")
        
        start_time = time.time()
        
        # Get layer names
        arcpy.env.workspace = source_gdb
        feature_classes = arcpy.ListFeatureClasses()
        final_flat_layers = [fc for fc in feature_classes if "final_flat" in fc]
        
        print(f"Found {len(final_flat_layers)} final_flat layers")
        
        processed_layers = []
        
        for layer in final_flat_layers:
            print(f"\nProcessing layer: {layer}")
            layer_path = os.path.join(source_gdb, layer)
            
            # Check geometry
            self.check_geometry_type(layer_path)
            self.check_geometry_validity(layer_path)
            
            # Process pest data
            self.process_pest_data(layer_path)
            
            # Assign dominant disturbance
            self.assign_dominant_disturbance(layer_path)
            
            # Calculate hectares
            self.calculate_hectares(layer_path)
            
            # Calculate percentages
            self.calculate_percentages(layer_path)
            
            processed_layers.append(layer_path)
        
        # Save processed data
        self.save_processed_data(processed_layers, ecotype, output_folder)
        
        # Perform dissolve operations
        self.perform_dissolve_operations(ecotype, output_folder)
        
        end_time = time.time()
        print(f"\n{ecotype} processing completed in {end_time - start_time:.2f} seconds")
        
        return processed_layers
    
    def save_processed_data(self, processed_layers, ecotype, output_folder):
        """Save processed data to geodatabase"""
        print(f"\nSaving processed data for {ecotype}")
        
        # Create output geodatabase
        temp_gdb_name = f"{ecotype}_DOM_DIST_TEMP.gdb"
        temp_gdb_path = os.path.join(output_folder, temp_gdb_name)
        
        if arcpy.Exists(temp_gdb_path):
            arcpy.Delete_management(temp_gdb_path)
        
        arcpy.CreateFileGDB_management(output_folder, temp_gdb_name.replace('.gdb', ''))
        
        # Copy processed layers to output geodatabase
        for layer_path in processed_layers:
            layer_name = os.path.basename(layer_path)
            
            # Get herd name from layer
            with arcpy.da.SearchCursor(layer_path, ["Herd_Name"]) as cursor:
                herd_name = next(cursor)[0]
                herd_name = herd_name.replace(" ", "_").replace("-", "_")
            
            output_name = f"{herd_name}_DOM_DIST_TEMP"
            output_path = os.path.join(temp_gdb_path, output_name)
            
            print(f"Copying {layer_name} to {output_name}")
            arcpy.CopyFeatures_management(layer_path, output_path)
    
    def perform_dissolve_operations(self, ecotype, output_folder):
        """Perform dissolve operations using ArcPy"""
        print(f"\nPerforming dissolve operations for {ecotype}")
        
        # Input and output geodatabase paths
        temp_gdb_name = f"{ecotype}_DOM_DIST_TEMP.gdb"
        temp_gdb_path = os.path.join(output_folder, temp_gdb_name)
        
        dissolve_gdb_name = f"{layer_name_list}_{ecotype}_PRO_DOM_DIST_DISS_{formatted_date}.gdb"
        dissolve_gdb_path = os.path.join(output_folder, dissolve_gdb_name)
        
        # Create dissolve geodatabase
        if arcpy.Exists(dissolve_gdb_path):
            arcpy.Delete_management(dissolve_gdb_path)
        
        arcpy.CreateFileGDB_management(output_folder, dissolve_gdb_name.replace('.gdb', ''))
        
        # Dissolve fields
        dissolve_fields = [
            "Herd_Name", "BCHab_code", "max_forest_restrict", 
            "max_mine_restriction", "max_og_restriction", 
            "Dominant_Disturbance", "Dominant_Distubance_Year"
        ]
        
        # Get feature classes from temp geodatabase
        arcpy.env.workspace = temp_gdb_path
        feature_classes = arcpy.ListFeatureClasses()
        
        # Filter out layers containing "George" if needed
        feature_classes = [fc for fc in feature_classes if "George" not in fc]
        
        dissolve_start = time.time()
        
        for fc in feature_classes:
            input_path = os.path.join(temp_gdb_path, fc)
            output_name = fc.replace("DOM_DIST_TEMP", "PRO_DOM_DIST_DISS")
            output_path = os.path.join(dissolve_gdb_path, output_name)
            
            print(f"Dissolving {fc} -> {output_name}")
            
            try:
                # Check which dissolve fields exist in the feature class
                existing_fields = [f.name for f in arcpy.ListFields(input_path)]
                valid_dissolve_fields = [f for f in dissolve_fields if f in existing_fields]
                
                if valid_dissolve_fields:
                    arcpy.PairwiseDissolve_analysis(
                        in_features=input_path,
                        out_feature_class=output_path,
                        dissolve_field=valid_dissolve_fields
                    )
                    print(f"Successfully dissolved {fc}")
                else:
                    print(f"Warning: No valid dissolve fields found for {fc}")
                    # Copy without dissolving
                    arcpy.CopyFeatures_management(input_path, output_path)
                    
            except Exception as e:
                print(f"Error dissolving {fc}: {str(e)}")
                # Copy without dissolving as fallback
                try:
                    arcpy.CopyFeatures_management(input_path, output_path)
                    print(f"Copied {fc} without dissolving as fallback")
                except Exception as e2:
                    print(f"Failed to copy {fc}: {str(e2)}")
        
        dissolve_end = time.time()
        print(f"Dissolve operations completed in {dissolve_end - dissolve_start:.2f} seconds")
    
    def run_analysis(self):
        """Run the complete analysis for all ecotypes"""
        print("=== Starting Caribou Protection and Dominant Disturbance Analysis ===")
        
        # Define paths
        output_folder = "X:\\srm\\gss\\initiatives\\caribou_recovery\\projects\\gr_2025_1033_habitat_status\\deliverables\\data"
        
        # Ensure output folder exists
        if os.path.exists(output_folder):
            print(f"Output folder already exists: {output_folder}")
        else:
            os.makedirs(output_folder)
        
        # Define ecotype configurations
        if eco_type_value == "BOREAL":
            ecotype_configs = {
                "name": "BOREAL",
                "source_gdb": "\\\\spatialfiles.bcgov\\Work\\srm\\gss\\initiatives\\caribou_recovery\\projects\\gr_2025_1033_habitat_status\\deliverables\\data\\Boreal_DisturbanceProtection_2025.gdb"
            }
        elif eco_type_value == "NMC":
            ecotype_configs = {
                "name": "NMC", 
                "source_gdb": "\\\\spatialfiles.bcgov\\Work\\srm\\gss\\initiatives\\caribou_recovery\\projects\\gr_2025_1033_habitat_status\\deliverables\\data\\NMC_DisturbanceProtection_2025.gdb"
            }
        elif eco_type_value == "SMC_NG":
            ecotype_configs = {
                "name": "SMC_NG",
                "source_gdb": "\\\\spatialfiles.bcgov\\Work\\srm\\gss\\initiatives\\caribou_recovery\\projects\\gr_2025_1033_habitat_status\\deliverables\\data\\SMC_NG_DisturbanceProtection_2025.gdb"
            }
        
        
        total_start = time.time()
        
        print(f"Processing {len(ecotype_configs)} ecotype configurations...")
        
        for config in ecotype_configs:
            print(f"\nChecking configuration: {config['name']}")
            print(f"Source GDB: {config['source_gdb']}")
            
            try:
                if arcpy.Exists(config["source_gdb"]):
                    print(f"✓ Source geodatabase found: {config['source_gdb']}")
                    self.process_ecotype_data(
                        config["name"], 
                        config["source_gdb"], 
                        output_folder
                    )
                else:
                    print(f"✗ Warning: Source geodatabase not found: {config['source_gdb']}")
                    
            except Exception as e:
                print(f"✗ Error processing {config['name']}: {str(e)}")
                import traceback
                traceback.print_exc()
                continue
        
        total_end = time.time()
        print(f"\n=== Analysis completed in {total_end - total_start:.2f} seconds ===")


def main():
    """Main function to run the analysis"""
    try:
        # Initialize analysis
        analysis = CaribouDisturbanceAnalysis()
        
        # Run the complete analysis
        analysis.run_analysis()
        
        print("\nAnalysis completed successfully!")
        
    except Exception as e:
        print(f"Error in main analysis: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()