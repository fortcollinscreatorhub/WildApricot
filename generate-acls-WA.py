__author__ = 'steve@roseundy.net'

import WaApi
import urllib.parse
import json
import argparse
from filelock import FileLock
import os
import time
import sys

apiKey_fname = 'client_secret'

bin_dir = os.path.dirname(__file__)
app_dir = os.path.dirname(bin_dir)
apiKey_fpath = os.path.join(
    app_dir,
    'etc',
    apiKey_fname)
acl_fname_prefix = 'acl-'

def get_apiKey(kpath):
    """Reads Wild Apricot API key from a file

    Returns: api key string
    """
    with open(kpath, 'r') as f:
        apiKey = f.readline().strip()
    return (apiKey)


def get_all_active_members(debug, contactsUrl):
    """Make an API call to Wild Apricot to retrieve
    contact info for all active members.

    Returns: list of contacts
    """
    params = {'$filter': 'member eq true AND Status eq Active',
              '$async': 'false'}
    request_url = contactsUrl + '?' + urllib.parse.urlencode(params)
    if debug: print('Making api call to get contacts')
    return api.execute_request(request_url).Contacts

RFID_list = []

acl_mapping = {'blaser': 'big-laser-cutter',
               'mlaser': 'medium-laser-cutter',
               'slaser': 'small-laser-cutter'}

def map_acl(x):
    """Maps privileges from Wild Apricot member database
    to ACL names used by ACL server

    Returns: mapped ACL name
    """
    x = x.lower()
    if x in acl_mapping:
        return (acl_mapping[x])
    else:
        return (x)
    

def fix_RFID(r):
    """Maps RFID strings to integers

    Returns: integer RFID
    """
    #f = str(r).strip()
    #while f[0] == '0':
    #    f = f[1:]
    return int(r)

def grab_RFID(debug, contact):
    """Given a contact from Wild Apricot member database,
    pulls out list of RFIDs and privileges (ACLs)

    Adds to global RFID_list
    """
    global RFID_list
    priv = ['door'] # everyone gets in the door!
    rfid = ''
    for field in contact.FieldValues:
        if (field.FieldName == 'RFID ID') and (field.Value is not None):
            rfid = field.Value
        if (field.FieldName == 'Privileges'):
            for privilege in field.Value:
                priv.append(map_acl(privilege.Label))
    if rfid == '':
        return
    if ',' in rfid:
        for r in rfid.split(','):
            RFID_list.append({'rfid':fix_RFID(r), 'priv':priv})
            if debug: print ('Appending ACL - rfid:', r, 'priv:', priv)
    else:
        RFID_list.append({'rfid':fix_RFID(rfid), 'priv':priv})
        if debug: print ('Appending ACL - rfid:', rfid, 'priv:', priv)

def dump_RFIDs(debug, acl_dir, ts):
    """Reads stored list of RFID and writes
    ACL files with that data
    """
    # get existing list of acl files
    old_files = os.listdir(acl_dir)
    
    # sort RFIDs and generate list of privileges - we need the list to know what files to write
    privileges = set()
    RFID_list.sort(key=lambda x:x['rfid'])
    for e in RFID_list:
        for p in e['priv']:
            privileges.add(p)

    # open one file per privilege
    File = {}
    for f in privileges:
        fname = acl_fname_prefix + f
        fname_tmp = os.path.join(acl_dir, '.' + fname + '.tmp')
        File[f] = open (fname_tmp, 'w')
        if debug: print('Opened file:',fname_tmp)
        print('# Generated at', ts, file=File[f])
        try:
            old_files.remove(fname)
        except:
            pass

    # go back through list of RFIDs and write them to the appropriate file(s)
    #
    for e in RFID_list:
        for p in e['priv']:
            File[p].write(str(e['rfid'])+'\n')

    # all done writing
    for f in File:
        File[f].close()

    # rename files just written
    for f in privileges:
        fname = os.path.join(acl_dir, acl_fname_prefix + f)
        fname_tmp = os.path.join(acl_dir, '.' + acl_fname_prefix + f + '.tmp')                                 
        os.rename(fname_tmp, fname)
        
    # remove obsolete files
    for fname in old_files:
        if not fname.startswith(acl_fname_prefix):
            continue
        fpath = os.path.join(acl_dir, fname)
        os.unlink(fpath)

####
################ Main ##################
####
if __name__ == '__main__':
    ts = time.strftime('%Y%m%dT%H%M%S')
    parser = argparse.ArgumentParser(
        #parents=[tools.argparser],
        description='Download Access Control info (RFIDs) from Wild Apricot')
    parser.add_argument(
        '--debug', action='store_true', help='Turn on debugging prints')
    parser.add_argument(
        '--auth-only', action='store_true',
        help='Set up authentication, but don\'t download data')
    parser.add_argument(
        'output_dir', nargs='?',
        help='Directory to write RFID lists to')
    args = parser.parse_args()
    if args.debug: print(args)

    if not args.auth_only and not args.output_dir:
            parser.error('output_dir required if --auth-only not specified')

    # Start API and authenticate
    #
    apiKey = get_apiKey(apiKey_fpath)
    
    api = WaApi.WaApiClient("CLIENT_ID", "CLIENT_SECRET")
    
    api.authenticate_with_apikey (apiKey, scope='account_view contacts_view')
    if args.debug: print('Authenticated')

    if args.auth_only:
        sys.exit(0)

    lock_fn = os.path.join(args.output_dir, ".lock")
    with FileLock(lock_fn):
        
        # Grab account details
        #
        accounts = api.execute_request("/v2/accounts")
        account = accounts[0]
        contactsUrl = next(res for res in account.Resources if res.Name == 'Contacts').Url
        if args.debug: print('contactsUrl:', contactsUrl)

        # request contact details on all active members
        #
        contacts = get_all_active_members(args.debug, contactsUrl)
        if args.debug: print ('Retrieved', len(contacts), 'contacts')

        # find the RFIDs and privileges and store them
        #
        for contact in contacts:
            grab_RFID(args.debug, contact)

        # write the RFIDs to one or more files
        #
        dump_RFIDs(args.debug, args.output_dir, ts)
        print('RFID lists generated OK at', ts)
