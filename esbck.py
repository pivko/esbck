#!/usr/bin/env python2.7
#############################################
#./esbck.py -h
#
# examples:
#
# Create a new repo
# ./esbck.py -cr -r REPO_NAME -p /mnt/bck
# List repos
# ./esbck.py -lr
#
# List current snapshots
# ./esbck.py -ls -r REPO_NAME
#
# Create a new snapshot
# ./esbck.py -r REPO_NAME -s SNAP_NAME -d 14
#
# michal.piwoni@gmail.com
#############################################

import requests
import sys
import argparse
import time
import json

from datetime import datetime, timedelta

ES_URL='http://127.0.0.1:9200'

def snap_api(index):
    data ={
            'indices': index,
            'ignore_unavailable': 'false',
            'include_global_state': 'true'
          }
    return json.dumps(data)
#
# List snapshots
#
def snap_list(repo):
    #print  "======================================="
    #print "List of the snapshots in the repo: %s  "% repo
    #print  "======================================="
    r = requests.get(url=ES_URL+'/_snapshot/'+repo+'/_all')
    print r.text
#
# Snapshot status
#
def snap_status(repo,snap_name):
    #print "Status of the snapshot:%s" % snap_name
    r = requests.get(url=ES_URL+'/_snapshot/'+repo+'/'+snap_name)
    print r.text
#
# Check latest snapshot timestamp (for nagios)
#
def snap_check(repo,snap_name,warning='129600',error='180000'):
    repos=repo_list()
    if repo not in repos:
       print "ERROR: Could NOT Find Repo: %s" % repo
       sys.exit(2)
    r = requests.get(url=ES_URL+'/_snapshot/'+repo+'/_all')
    #print r.json()
    #print r.text
    snaps=r.json()
    for i in reversed(snaps['snapshots']):
        snapshot=i['snapshot']
        snap_state=i['state']
        snap_start=i['start_time']
        snap_start_millis=i['start_time_in_millis']
        snap_end=i.get('end_time','not_finished_yet')
        now=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        date_form_now=datetime.strptime(now, "%Y-%m-%d %H:%M:%S")
        if snapshot.startswith(snap_name):
            date_strip=snap_start.split('.',1)
            date_form=datetime.strptime(date_strip[0], "%Y-%m-%dT%H:%M:%S")
            diff_full=date_form_now-date_form
            diff_seconds=diff_full.total_seconds()
            #check if last backup younger than delta = 36 hours by default
            if ((int(diff_seconds) <= int(warning)) and snap_state == 'SUCCESS'):
                #print diff_full
                #print diff_seconds
                print 'Last Good Es Snapshot Found is: ' + str(diff_full) + ' old => name: ' +  snapshot + ' start: '+snap_start+' end: '+snap_end + ' state: '+ snap_state
                sys.exit(0)
            elif ((int(diff_seconds) > int(warning)) and snap_state == 'SUCCESS' and int(diff_seconds) < int(error)):
                print 'CHECK the ES backup! Last Good Es Snapshot Found is: ' + str(diff_full) + ' old => name: ' +  snapshot + ' start: '+snap_start+' end: '+snap_end + ' state: '+ snap_state
                sys.exit(1)
            elif ((int(diff_seconds) > int(warning)) and snap_state == 'SUCCESS' and int(diff_seconds) >= int(error)):
                print 'CHECK the ES backup! Last Good Es Snapshot Found is: ' + str(diff_full) + ' old => name: ' +  snapshot + ' start: '+snap_start+' end: '+snap_end + ' state: '+ snap_state
                sys.exit(2)
            else:
                continue
        else:
            continue
            #print 'CHECK the ES backup!, Could NOT Find Any ES Snapshot for: ' + snap_name
            #sys.exit(2)
    #Error
    print 'CHECK the ES backup ! Could NOT Find Any ES Snapshot for: ' + snap_name + ' younger than: ' + error
    sys.exit(2)
#
# snapshot delete
#
def snap_delete(repo,snap_name):
    r = requests.delete(url=ES_URL+'/_snapshot/'+repo+'/'+snap_name)
    if r.status_code == 200:
        print "Removing snapshot - %s"% snap_name
        print "Status: %s"%r.status_code

#
#Create a snapshot
#
def snap(index,repo,snap_name):
    r = requests.put(url=ES_URL+'/_snapshot/'+repo+'/'+snap_name,
                      data=snap_api(index),
                      headers={'Content-Type': 'application/json'})
    if r.status_code == 200:
        print "Creating snapshot - %s"% snap_name
    else:
        print "Could not create a snapshot - %s" % snap_name
        print "Status: %s - %s "% (r.status_code,r.text)
        sys.exit(1)
    print "Status: %s - %s "% (r.status_code,r.text)
#CREATE REPO

###FS========================================================
def repo_fs_api(location, repo_type = "fs",compress = True):
    if repo_type == "fs":
        data = {
                "type": repo_type,
                "settings": {
                    "location": location,
                    "compress": compress
                }
               }
    return json.dumps(data)

def repo_fs(repo,location,repo_type='fs',compress=True):
    r = requests.put(url=ES_URL+'/_snapshot/'+repo,
                      data=repo_fs_api(location,repo_type,compress),
                      headers={'Content-Type': 'application/json'})
    if r.status_code == 200:
        print "Creating repo - %s"% repo
    else:
        print "Could not create a repo - %s" % repo
    print "Status: %s - %s "% (r.status_code,r.text)

###SWIFT=============================================================
def repo_swift_api(repo_type,swift_authmethod,swift_url,swift_username,swift_password,swift_container,compress = False):
    if repo_type == "swift":
        data = {
                "type": repo_type,
                "settings": {
                    "swift_url": swift_url,
                    "swift_container": swift_container,
                    "swift_username": swift_username,
                    "swift_password": swift_password,
                    "swift_authmethod": swift_authmethod,
                    "compress": compress
                }
               }
        return json.dumps(data)


def repo_swift(repo,swift_authmethod,swift_url,swift_username,swift_password,swift_container,repo_type='swift',compress=False):
    r = requests.put(url=ES_URL+'/_snapshot/'+repo,
                      data=repo_swift_api(repo_type,swift_authmethod,swift_url,swift_username,swift_password,swift_container,compress),
                      headers={'Content-Type': 'application/json'})
    if r.status_code == 200:
        print "Creating repo - %s"% repo
    else:
        print "Could not create a repo - %s" % repo
    print "Status: %s - %s "% (r.status_code,r.text)
#====================================================================
###MINIO=============================================================
def repo_minio_api(repo_type,bucket,region,endpoint,access_key,secret_key):
    if repo_type == "minio":
        data = {
                "type": "s3",
                "settings": {
                    "bucket": bucket,
                    "region": region,
                    "endpoint":  endpoint,
                    "access_key": access_key,
                    "secret_key": secret_key,
                    "protocol": "https",
                    "path_style_access": "true"
                }
               }
        return json.dumps(data)


def repo_minio(repo,bucket,region,endpoint,access_key,secret_key,repo_type='minio'):
    r = requests.put(url=ES_URL+'/_snapshot/'+repo,
                      data=repo_minio_api(repo_type,bucket,region,endpoint,access_key,secret_key),
                      headers={'Content-Type': 'application/json'})
    if r.status_code == 200:
        print "Creating repo - %s"% repo
    else:
        print "Could not create a repo - %s" % repo
    print "Status: %s - %s "% (r.status_code,r.text)


#====================================================================
def repo_list():
    #print "=================================="
    #print "List of the existing repositories:"
    #print "=================================="
    r = requests.get(url=ES_URL+'/_snapshot/_all')
    #print r.text
    return r.text

#########################################################
def main():

    parser = argparse.ArgumentParser(description='ELASTICSEARCH SNAPSHOT(BACKUP) SCRIPT')
    parser.add_argument('--es_url', type=str, default='http://127.0.0.1:9200', help='Elasticsearch url, default: http://127.0.0.1:9200')
    parser.add_argument('-i', '--index', type=str, default='_all', help='Index name to backup, default: _all')
    parser.add_argument('-r', '--repo', type=str, help='Repository name registered in ES, default: autorepo')
    parser.add_argument('-s', '--snapshot', type=str, help='Snapshot Name, default: autosnap')
    parser.add_argument('-d', '--days', type=int, default='14', help='How many days the snapshot should be kept, default: 14')
    parser.add_argument('-cr', '--createrepo',action='store_true', help='Whether to create a repo or not, default: false')
    parser.add_argument('-p', '--path', type=str,help='Location of the shared storage mounted on the local disk, ie: /mnt/es_backup')
    parser.add_argument('-rt', '--rtype', type=str,default='fs',help='Repository type: fs, swift, minio =>  default: fs')
    parser.add_argument('-ls', '--listsnap', action='store_true',help='List snapshots in a given repository')
    parser.add_argument('-lr', '--listrepo', action='store_true',help='List snapshots in a given repository')
    parser.add_argument('-ss', '--snapshot_status', action='store_true',help='Checks status of the given snapshot')
    parser.add_argument('-sc', '--snapshot_check', action='store_true',help='Checks the latest snapshot of the given name: for nagios check')
    parser.add_argument('-sd', '--snapshot_delete', action='store_true',help='Delete the given snapshot')
    #SWIFT
    parser.add_argument('-swift_url','--swift_url', type=str,help='SWIFT Url, ie: http://127.0.0.1:8080/auth/v1.0/')
    parser.add_argument('-swift_username','--swift_username', type=str,help='SWIFT Username, ie: tom:tom')
    parser.add_argument('-swift_password','--swift_password', type=str,help='SWIFT Password')
    parser.add_argument('-swift_container','--swift_container', type=str,help='SWIFT Container Name')
    parser.add_argument('-swift_authmethod','--swift_authmethod', type=str,default='TEMPAUTH',help='SWIFT Authmethod, default: TEMPAUTH')
    #MINIO
    parser.add_argument('-minio_bucket','--minio_bucket', type=str,help='MINO Bucket')
    parser.add_argument('-minio_endpoint','--minio_endpoint', type=str,help='MINIO Endpoint')
    parser.add_argument('-minio_region','--minio_region', type=str,help='MINIO Region')
    parser.add_argument('-minio_access_key','--minio_access_key', type=str,help='MINIO Access Key')
    parser.add_argument('-minio_secret_key','--minio_secret_key', type=str,help='MINIO Secret Key')

    args = parser.parse_args()

    if args.es_url:
        global ES_URL
        ES_URL=args.es_url

    if args.listrepo:
        print repo_list()
        sys.exit(0)
    if args.snapshot_status:
       if args.repo and args.snapshot:
            snap_status(args.repo,args.snapshot)
       else:
            print "=============================================================================="
            print "To check the status of the given snapshot you need to provide:"
            print "=> Repo name by: -r"
            print "=> Snapshot name by: -s"
            print "=============================================================================="
       sys.exit(0)
    if args.snapshot_check:
       if args.repo and args.snapshot:
            snap_check(args.repo,args.snapshot)
       else:
            print "=============================================================================="
            print "To check the latest snapshot in the given repository:"
            print "=> Repo name by: -r"
            print "=> Snapshot name by: -s"
            print "=============================================================================="
       sys.exit(0)
    if args.snapshot_delete:
       if args.repo and args.snapshot:
            snap_delete(args.repo,args.snapshot)
       else:
            print "=============================================================================="
            print "To delete the given snapshot you need to provide:"
            print "=> Repo name by: -r"
            print "=> Snapshot name by: -s"
            print "=============================================================================="
       sys.exit(0)
    if args.listsnap:
        if args.repo:
            snap_list(args.repo)
        else:
            print "======================================================="
            print "To list current snashots please provide:"
            print "=> Repo name by: -r"
            print "======================================================="
        sys.exit(0)

    if args.createrepo:
        if args.rtype == 'fs':
            if args.path and args.repo:
                repo_fs(args.repo,args.path,args.rtype)
            else:
                print "==========================================================================="
                print "To create a new FS repo You need to provide a path by: -p and repo name by: -r"
                print "==========================================================================="
                sys.exit(1)
        elif args.rtype == 'swift':
            if args.repo and args.swift_url and args.swift_username and args.swift_password and args.swift_container:
                repo_swift(args.repo,args.swift_authmethod,args.swift_url,args.swift_username,args.swift_password,args.swift_container,args.rtype)
            else:
                print "==========================================================================="
                print "To create a new SWIFT repo with TEMPAUTH auth method You need to provide:"
                print " --repo"
                print " --swift_url"
                print " --swift_username"
                print " --swift_password"
                print " --swift_container"
                print "==========================================================================="
                sys.exit(1)
        elif args.rtype == 'minio':
            if args.repo and args.minio_bucket and args.minio_endpoint and args.minio_region and args.minio_access_key and args.minio_secret_key:
                repo_minio(args.repo,args.minio_bucket,args.minio_region,args.minio_endpoint,args.minio_access_key,args.minio_secret_key,args.rtype)
            else:
                print "==========================================================================="
                print "To create a new MINIO(S3) repo You need to provide:"
                print " --repo"
                print " --minio_bucket"
                print " --minio_endpoint"
                print " --minio_region"
                print " --minio_access_key"
                print " --minio_secret_key"
                print "==========================================================================="
                sys.exit(1)
    else:
        if args.repo and args.snapshot:
            print "===================================================="
            print "Trying to make a snapshot with the following settings:"
            print "REPO: %s" % args.repo
            print "SNAPSHOT: %s" % args.snapshot
            print "INDEX: %s" % args.index
            print "DAYS: %s" % args.days
            print "===================================================="
            x_days_from_today = datetime.now() - timedelta(int(args.days)+1)
            cur_day = format(datetime.now(),'%Y-%m-%d')
            x_days  = format(x_days_from_today,'%Y-%m-%d')

            snapshot_remove=args.snapshot+'-'+x_days
            day_snapshot=args.snapshot+'-'+cur_day



            print "Removing old snapshot"
            snap_delete(args.repo,snapshot_remove)
            time.sleep(120)
            print "Creating new snapshot"
            snap(args.index,args.repo,day_snapshot)
        else:
            print "======================================================================================="
            print "To make a ES snapshot you need to provide at least repo name: -r  and snapshot name by: -s"
            print "======================================================================================="
            sys.exit(1)


if __name__ == '__main__':
    main()
