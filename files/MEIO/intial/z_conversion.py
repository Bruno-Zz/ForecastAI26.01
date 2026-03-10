# -*- coding: utf-8 -*-
"""
Created on Mon Jul  3 18:47:25 2023

@author: BrunoZindy
"""
import pandas as pds
import logging;


sql_load_z_conversion="SELECT fill_rate, z FROM ketteq_custom.io_z_conversion"

#logging.basicConfig(filename='c:/temp/z_conversion.log', filemode='w', format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', force=True)

class ZConversion:
    def __init__(self, dbConnection, scenario_name):
        dbConnection    = dbConnection
        #self.df   = pds.read_sql(sql_load_z_conversion, self.dbConnection,  index_col=['z'] );
        self.df   = pds.read_sql(sql_load_z_conversion, dbConnection);
        #self.dict=df.to_dict('index')

       
    def getfillrate (self, Z_input):
        return (self.df [self.df.z>Z_input].fill_rate.min())
