#!/usr/bin/env python

## Program to perform automatic Atlas to entity tag attachments to hive columns
## is cases where Atlas collects hive column lineage information (Atlas > 0.7). Despite having
## the lineage information currently Atlas (0.8) does not propagate the input column tags
## to the output columns. This script solves this flaw.

import urllib2
import base64
import json
import re
import getpass
import sys

atlas_rest_url = 'http://rjk-hdp-m1:21000/api/atlas/v2/'
username = 'admin'
password = getpass.getpass()
tag_filter = 'JOIN'

## Currently the program is written with simple authentication,
base64string = base64.encodestring('%s:%s' % (username, password)).replace('\n', '')

def atlasRestGet(endPoint, params):
    req = urllib2.Request(atlas_rest_url+endPoint+params)
    #   print("Atlas REST GET call : "+atlas_rest_url+endPoint+params)
    req.add_header("Authorization", "Basic %s" % base64string)
    response = urllib2.urlopen(req)
    json_obj = json.load(response)
    response.close()
    return json_obj

def atlasRestPostTag(tag, guid):
    body = '{"classification":{"typeName":"'+tag+'","attributes":{}},"entityGuids":["'+guid+'"]}'
    #   print("Atlas REST POST call body: "+body)
    req = urllib2.Request(atlas_rest_url+'entity/bulk/classification', body, {'Content-Type': 'application/json'})
    req.add_header("Authorization", "Basic %s" % base64string)
    try:
        response = urllib2.urlopen(req)

        if (response.getcode()==204):
            print "Hive column : "+guid+" successfully (re)tagged"
        response.close()
    except urllib2.HTTPError, e:
        print 'Encountered problem while tagging '+guid, e

## First, query all hive_column_lineage records
lineage_query = atlasRestGet('search/dsl','?typeName=hive_column_lineage')

reported_lineages=[]
for lineage in lineage_query['entities']:
    hive_lineage_entry = lineage['guid']

    ## Get verbose information about each lineage record
    lineage_detail = atlasRestGet('entity/bulk','?guid='+hive_lineage_entry)
    #   print(lineage_detail)

    ## For the moment we assume there can only be 1 output per lineage detail
    output_column_guid = lineage_detail['entities'][0]['attributes']['outputs'][0]['guid']

    ## Loop over input columns involved in lineage
    for input_column_guid in lineage_detail['entities'][0]['attributes']['inputs']:

        ## Get verbose information about the source input_column
        input_column_detail = atlasRestGet('entity/bulk','?guid='+input_column_guid['guid'])

        if input_column_detail['entities'][0]['classifications']:

            ## If there are tags on the source column, get verbose information on the destination output_column
            output_column_detail = atlasRestGet('entity/bulk','?guid='+output_column_guid)

            src_tags=[]
            for tag in input_column_detail['entities'][0]['classifications']:
                src_tags.append(tag['typeName'])
                #if tag_filter == tag['typeName']:
                #    print('Tagging : hive_column : '+output_column_detail['entities'][0]['attributes']['name']+' with tag : '+tag['typeName'])
                #    atlasRestPostTag(tag['typeName'],output_column_guid)

            dst_tags=[]
            for tag in output_column_detail['entities'][0]['classifications']:
                dst_tags.append(tag['typeName'])

            lineage = {
                'lineage': hive_lineage_entry,
                'input_column': {'guid' : input_column_guid['guid'],
                                 'name' : input_column_detail['entities'][0]['attributes']['name'],
                                 'tags' : src_tags},
                'output_column': {'guid' : output_column_guid,
                                  'name' : output_column_detail['entities'][0]['attributes']['name'],
                                  'tags' : dst_tags}
            }
            reported_lineages.append(lineage)

print(json.dumps({'lineages': reported_lineages}))