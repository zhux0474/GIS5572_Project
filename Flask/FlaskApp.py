import os
import string
from flask import Flask, request, render_template, jsonify
import psycopg2
import json
import pandas as pd
from flask import Flask


app = Flask(__name__)
#@app.route("/")
#def hello_world():
  # name = os.environ.get("NAME", "World")
  # return "Howdy!".format(name)
#Gravity model bugs
@app.route('/bug',methods=['get'])
def getgravity_bug():
  connection=psycopg2.connect("dbname='lab3' user='postgres' host='35.192.52.194' password='postgres'")
  cursor=connection.cursor()
  collection_query="""
    SELECT jsonb_build_object(
        'type','FeatureCollection',
        'features',jsonb_agg(features.feature)::jsonb)
        FROM(
        SELECT jsonb_build_object(
        'type','Feature',
        'geometry',ST_AsGeoJSON(shape)::jsonb,
        'properities', json_build_object('gid',gid,'gnis_featu',gnis_featu,'feature_na',feature_na,'ctu_class',ctu_class,'county_nam',county_nam,'population',population,'shape_leng',shape_leng,'station',station,'name',name,'elevation',elevation,'maxtemp',maxtemp,'nlcd_land',nlcd_land,'attractive',attractive,'ghcnd_ca00',ghcnd_ca00,'ghcnd_usc0',ghcnd_usc0,'ghcnd_us_1',ghcnd_us_1,'ghcnd_us_2',ghcnd_us_2,'ghcnd_us_3',ghcnd_us_3,'ghcnd_us_4',ghcnd_us_4,'ghcnd_us_5',ghcnd_us_5,'ghcnd_us_6',ghcnd_us_6,'ghcnd_us_7',ghcnd_us_7,'ghcnd_us_8',ghcnd_us_8,'ghcnd_us_9',ghcnd_us_9,'ghcnd_u_10',ghcnd_u_10,'ghcnd_u_11',ghcnd_u_11,'ghcnd_u_12',ghcnd_u_12,'ghcnd_u_13',ghcnd_u_13,'ghcnd_u_14',ghcnd_u_14,'ghcnd_u_15',ghcnd_u_15,'ghcnd_u_16',ghcnd_u_16,'ghcnd_u_17',ghcnd_u_17,'ghcnd_u_18',ghcnd_u_18,'ghcnd_u_19',ghcnd_u_19,'ghcnd_u_20',ghcnd_u_20,'ghcnd_u_21',ghcnd_u_21,'ghcnd_u_22',ghcnd_u_22,'ghcnd_u_23',ghcnd_u_23,'ghcnd_u_24',ghcnd_u_24,'ghcnd_u_25',ghcnd_u_25,'shape_le_2',shape_le_2,'shape_area',shape_area,'high_dest',high_dest)::jsonb)
        ::jsonb AS feature
        FROM gravitymodel_bugs )features
    """

  feature_collection=pd.read_sql(collection_query,connection)
  feature_collection_dict=feature_collection.iloc[0]['jsonb_build_object']

    #get rid off extract letter "u"
  feature_collection_dict=json.dumps(feature_collection_dict)

    #display the geojson to page
  return str(feature_collection_dict)
#Gravity model population
@app.route('/pop',methods=['get'])
def getgravity_pop():
  connection=psycopg2.connect("dbname='lab3' user='postgres' host='35.192.52.194' password='postgres'")
  cursor=connection.cursor()
  collection_query="""
    SELECT jsonb_build_object(
        'type','FeatureCollection',
        'features',jsonb_agg(features.feature)::jsonb)
        FROM(
        SELECT jsonb_build_object(
        'type','Feature',
        'geometry',ST_AsGeoJSON(shape)::jsonb,
        'properities', json_build_object('gid',gid,'gnis_featu',gnis_featu,'feature_na',feature_na,'ctu_class',ctu_class,'county_nam',county_nam,'population',population,'shape_leng',shape_leng,'station',station,'name',name,'elevation',elevation,'maxtemp',maxtemp,'nlcd_land',nlcd_land,'attractive',attractive,'ghcnd_ca00',ghcnd_ca00,'ghcnd_usc0',ghcnd_usc0,'ghcnd_us_1',ghcnd_us_1,'ghcnd_us_2',ghcnd_us_2,'ghcnd_us_3',ghcnd_us_3,'ghcnd_us_4',ghcnd_us_4,'ghcnd_us_5',ghcnd_us_5,'ghcnd_us_6',ghcnd_us_6,'ghcnd_us_7',ghcnd_us_7,'ghcnd_us_8',ghcnd_us_8,'ghcnd_us_9',ghcnd_us_9,'ghcnd_u_10',ghcnd_u_10,'ghcnd_u_11',ghcnd_u_11,'ghcnd_u_12',ghcnd_u_12,'ghcnd_u_13',ghcnd_u_13,'ghcnd_u_14',ghcnd_u_14,'ghcnd_u_15',ghcnd_u_15,'ghcnd_u_16',ghcnd_u_16,'ghcnd_u_17',ghcnd_u_17,'ghcnd_u_18',ghcnd_u_18,'ghcnd_u_19',ghcnd_u_19,'ghcnd_u_20',ghcnd_u_20,'ghcnd_u_21',ghcnd_u_21,'ghcnd_u_22',ghcnd_u_22,'ghcnd_u_23',ghcnd_u_23,'ghcnd_u_24',ghcnd_u_24,'ghcnd_u_25',ghcnd_u_25,'ghcnd_u_26',ghcnd_u_26,'ghcnd_u_27',ghcnd_u_27,'ghcnd_u_28',ghcnd_u_28,'ghcnd_u_29',ghcnd_u_29,'ghcnd_u_30',ghcnd_u_30,'ghcnd_u_31',ghcnd_u_31,'ghcnd_u_32',ghcnd_u_32,'shape_le_2',shape_le_2,'shape_area',shape_area,'high_dest',high_dest)::jsonb)
        ::jsonb AS feature
        FROM gravitymodel_population )features
    """

  feature_collection=pd.read_sql(collection_query,connection)
  feature_collection_dict=feature_collection.iloc[0]['jsonb_build_object']

    #get rid off extract letter "u"
  feature_collection_dict=json.dumps(feature_collection_dict)

    #display the geojson to page
  return str(feature_collection_dict)

#Gravity model combined attractiveness
@app.route('/combined',methods=['get'])
def getgravity_combined():
  connection=psycopg2.connect("dbname='lab3' user='postgres' host='35.192.52.194' password='postgres'")
  cursor=connection.cursor()
  collection_query="""
    SELECT jsonb_build_object(
        'type','FeatureCollection',
        'features',jsonb_agg(features.feature)::jsonb)
        FROM(
        SELECT jsonb_build_object(
        'type','Feature',
        'geometry',ST_AsGeoJSON(shape)::jsonb,
        'properities', json_build_object('gid',gid,'feature_na',feature_na,'ctu_class',ctu_class,'county_nam',county_nam,'population',population,'station',station,'name',name,'elevation',elevation,'maxtemp',maxtemp,'nlcd_land',nlcd_land,'attractive',attractive,'ghcnd_ca00',ghcnd_ca00,'ghcnd_usc0',ghcnd_usc0,'ghcnd_us_1',ghcnd_us_1,'ghcnd_us_2',ghcnd_us_2,'ghcnd_us_3',ghcnd_us_3,'ghcnd_us_4',ghcnd_us_4,'ghcnd_us_5',ghcnd_us_5,'ghcnd_us_6',ghcnd_us_6,'ghcnd_us_7',ghcnd_us_7,'ghcnd_us_8',ghcnd_us_8,'ghcnd_us_9',ghcnd_us_9,'ghcnd_u_10',ghcnd_u_10,'ghcnd_u_11',ghcnd_u_11,'ghcnd_u_12',ghcnd_u_12,'ghcnd_u_13',ghcnd_u_13,'ghcnd_u_14',ghcnd_u_14,'ghcnd_u_15',ghcnd_u_15,'ghcnd_u_16',ghcnd_u_16,'ghcnd_u_17',ghcnd_u_17,'ghcnd_u_18',ghcnd_u_18,'ghcnd_u_19',ghcnd_u_19,'ghcnd_u_20',ghcnd_u_20,'ghcnd_u_21',ghcnd_u_21,'ghcnd_u_22',ghcnd_u_22,'ghcnd_u_23',ghcnd_u_23,'ghcnd_u_24',ghcnd_u_24,'ghcnd_u_25',ghcnd_u_25,'ghcnd_u_26',ghcnd_u_26,'ghcnd_u_27',ghcnd_u_27,'ghcnd_u_28',ghcnd_u_28,'ghcnd_u_29',ghcnd_u_29,'ghcnd_u_30',ghcnd_u_30,'ghcnd_u_31',ghcnd_u_31,'ghcnd_u_32',ghcnd_u_32,'shape_le_2',shape_le_2,'shape_area',shape_area,'high_dest',high_dest)::jsonb)
        ::jsonb AS feature
        FROM gravitymodel_combined )features
    """
  feature_collection=pd.read_sql(collection_query,connection)
  feature_collection_dict=feature_collection.iloc[0]['jsonb_build_object']

    #get rid off extract letter "u"
  feature_collection_dict=json.dumps(feature_collection_dict)

    #display the geojson to page
  return str(feature_collection_dict)

if __name__ == "__main__":
   app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
