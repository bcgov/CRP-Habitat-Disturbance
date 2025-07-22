# CRP-Habitat-Disturbance
To conduct a provincial-level analysis of disturbance within Caribou Herds in British Columbia. <br>

**Data_prep**:<br>
Setup script that organizes the data to match the schema expected in Run_Disturbance, may need to edit this if input data schema changes<br>

**Run_Disturbance:**<br> 
This is the script to run for disturbance analysis. It takes in all the other scripts and functions from the folder to combine them. There are two .json config files in the folder – one is kept as an example of inputs while the other is blank. Copy the blank one and update the file path to your config file. 
As best practice do not edit the original script – keep it here and copy/paste it to your working directory to edit it out as needed. 

The ‘flatten’ part of this script came from this source:
https://www.esri.com/arcgis-blog/products/arcgis-desktop/analytics/more-adventures-in-overlay-counting-overlapping-polygons-with-spaghetti-and-meatballs/

