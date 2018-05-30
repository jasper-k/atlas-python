#!/usr/bin/env python

## Program to perform automatic Atlas entity creation for HDFS directories.
## Normally Atlas 0.8 and lower only have hdfs_path entities for hdfs paths tied to Hive tables
## This script allows for automated hdfs path discovery and subsequently register those as
## new Atlas hdfs_path entities in order to be able to attach Atlas tag to them and enforce
## tag based policies

## TO_DO
## create tag if not existant

import urllib2
import base64
import json
import os
class DirectoryTagger(object):

    def __init__(self, props):
        self.props = props
        self.base64string = base64.encodestring('%s:%s' % (props['atlas_username'], props['atlas_password'])).replace('\n', '')


    def lukeHDFSWalker(self, hdfs_path, paths=[]):
        #    print 'Called for hdfs_path: '+base_webhdfs_cmd+hdfs_path+cmd_suffix
        cmd = self.props['base_webhdfs_cmd']+' "'+self.props['webhdfs_url']+hdfs_path+self.props['cmd_suffix']
        print(cmd)
        stdout = os.popen(cmd)
        asJson = json.load(stdout)
        #    print json.dumps(asJson['FileStatuses']['FileStatus'])

        discovered_paths = []
        for entry in asJson['FileStatuses']['FileStatus']:
            if entry['type'] == 'DIRECTORY':
                discovered_path = hdfs_path + entry['pathSuffix'] + '/'
                #            print 'Discovered : %s' % discovered_path
                discovered_paths.append(discovered_path)
                paths = paths + discovered_paths

                #    print discovered_paths
        for new_path in discovered_paths:
            for x in self.lukeHDFSWalker(new_path):
                yield x

        yield hdfs_path, discovered_paths


    def atlasRestPostTag(self, tag, guid):
        body = '{"classification":{"typeName":"'+tag+'","attributes":{}},"entityGuids":["'+guid+'"]}'
        print("Atlas REST POST call body: "+body)
        print(self.props['atlas_rest_url']+'entity/bulk/classification')
        req = urllib2.Request(self.props['atlas_rest_url']+'entity/bulk/classification', body, {'Content-Type': 'application/json'})
        req.add_header("Authorization", "Basic %s" % self.base64string)
        try:
            response = urllib2.urlopen(req)

            if (response.getcode()==204):
                print "HDFS path : "+guid+" successfully (re)tagged"
            response.close()
        except (urllib2.HTTPError, urllib2.URLError) as exc:
            print 'Encountered problem while tagging : %s exit.code : "%s" reason : %s' % (guid, exc.code, exc.read())


    def atlasRestPostEntity(self, path):
        body = {'entity': {'typeName' : "hdfs_path", 'attributes' : {'name' : path, 'qualifiedName' : self.props['hdfs_id']+path, 'path' : path, 'clusterName' : self.props['clustername']}}}
        #print("Atlas REST POST call body: "+json.dumps(body))
        req = urllib2.Request(self.props['atlas_rest_url']+'entity', json.dumps(body), {'Content-Type': 'application/json'})
        req.add_header("Authorization", "Basic %s" % self.base64string)
        try:
            response = urllib2.urlopen(req)
            if (response.getcode()==200):
                response_json = json.load(response)
                print "HDFS entity : "+path+" successfully created/updated"
                #       print response_json
            response.close()
            if 'UPDATE' in response_json['mutatedEntities']:
                return response_json['mutatedEntities']['UPDATE'][0]['guid']
            else:
                return response_json['mutatedEntities']['CREATE'][0]['guid']


        except urllib2.HTTPError, e:
            print 'Encountered problem while creating : %s exit code : "%s" reason : %s' % (path, e.code, e.read())


def loadProperties(filepath, sep='=',comment_char='#'):
    props = {}
    with open(filepath, "rt") as f:
        for line in f:
            l = line.strip()
            if l and not l.startswith(comment_char):
                key_value = l.split(sep)
                key = key_value[0].strip()
                value = sep.join(key_value[1:]).strip().strip('"')
                props[key] = value
    return props


def main():
    props = loadProperties('atlas.properties')

    walker = DirectoryTagger(props)
    tag = props['tag']

    for root, discovered_paths in walker.lukeHDFSWalker(props['crawl_root']):
        for path in discovered_paths:
            print path.rstrip("/")
            walker.atlasRestPostTag(tag, walker.atlasRestPostEntity(path.rstrip("/"))) # To create the discovered dirs AND attach the tag to it
            #       walker.atlasRestPostEntity(path.rstrip("/")) # To only create the discovered dirs

main()