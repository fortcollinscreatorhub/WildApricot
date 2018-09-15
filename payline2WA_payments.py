import WaApi
import urllib.parse
import csv
import argparse
import os

apiKey_fname = 'client_secret_rw'
bin_dir = os.path.dirname(__file__)
app_dir = os.path.dirname(bin_dir)
apiKey_fpath = os.path.join(
    app_dir,
    'etc',
    apiKey_fname)

email_mapping = {'larry@injectech.us': 'jeffs@injectech.us'}

def get_apiKey(kpath):
    """Reads Wild Apricot API key from a file

    Returns: api key string
    """
    with open(kpath, 'r') as f:
        apiKey = f.readline().strip()
    return (apiKey)

# read csv formatted transactions from file
def load_csv (fn):
    retval = []
    with open(fn, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            row['amount'] = float(row['amount'])
            row['email'] = row['email'].lower()
            if row['email'] in email_mapping:
                row['email'] = email_mapping[row['email']]
            if (row['type'] == 'settle') and (row['status'] == 'complete'):
                retval.append(row)
    return retval
            

# debug print transaction list
widths = {'first_name':15, 'last_name':15, 'email':35}
def print_trans (trans):
    for key in trans[0]:
        if (key == 'status') or (key == 'type'):
            continue
        just = 10
        if (key in widths):
            just = widths[key]
        print (key.ljust(just), end='', sep='')
    print()
    for row in trans:
        for key in row:
            if (key == 'status') or (key == 'type'):
                continue
            just = 10
            if (key in widths):
                just = widths[key]
            if (key == 'amount'):
                print ('%9.2f ' % row[key], end='', sep='')
            else:
                print (row[key].ljust(just), end='', sep='')
        print()

# combine multiple transactions per email address into one
def reduce_trans (trans):
    by_email = {}
    for row in trans:
        if row['email'] not in by_email:
            by_email[row['email']] = row
        else:
            by_email[row['email']]['amount'] += row['amount']
    # now convert back to list
    retval = [];
    for key in by_email:
        if by_email[key]['amount'] > 0:
            retval.append(by_email[key])
    return retval

def get_all_active_members(api, debug, contactsUrl):
    """Make an API call to Wild Apricot to retrieve
    contact info for all active members.

    Returns: list of contacts
    """
    params = {'$filter': 'member eq true AND Status eq Active',
              '$async': 'false'}
    request_url = contactsUrl + '?' + urllib.parse.urlencode(params)
    if debug: print('Making api call to get contacts')
    return api.execute_request(request_url).Contacts

# try to associate a transaction entry with a Wild Apricot contact id
def lookup_ids (api, debug, trans):
    # Grab account details
    #
    accounts = api.execute_request("/v2/accounts")
    account = accounts[0]
    contactsUrl = next(res for res in account.Resources if res.Name == 'Contacts').Url
    if args.debug: print('contactsUrl:', contactsUrl)

    # request contact details on all active members
    #
    contacts = get_all_active_members(api, debug, contactsUrl)
    if args.debug: print ('Retrieved', len(contacts), 'contacts')

    for txn in trans:
        email_found = False
        for contact in contacts:
            if (contact.Email == txn['email']):
                txn['id'] = contact.Id
                if debug:
                    print (txn['email'], '=>', contact.Id)
                email_found = True
                break
        if not email_found:
            if debug:
                print ('*** email', txn['email'], 'not found. Looking for matching name')
            name_found = False
            for contact in contacts:
                if (contact.LastName == txn['last_name']) and (contact.FirstName == txn['first_name']):
                    txn['id'] = contact.Id
                    name_found = True
                    if debug:
                        print (txn['email'], '=>', contact.Id, '(corresponds to', contact.Email, contact.FirstName, contact.LastName, ')')
                    break
            if not name_found:
                print ('Warning: no matching contact for', txn['email'],
                       txn['first_name'], txn['last_name'], 'Transaction record will be ignored')

def get_tenders (api, debug, tendersUrl):
    request_url = tendersUrl
    if debug: print('Making api call to get tenders')
    return api.execute_request(request_url)    

def build_invoice (txn):
    retdata = {'OrderType': 'Legacy subscription payment',
               'Contact': {'Id':txn['id']},
               'OrderDetails': [{'Value':txn['amount'],'Notes':'Payline subscription payment'}]
               }
    return (retdata)

def build_payment (txn, tender_id, invoice_id):
    retdata = {'Value': txn['amount'],
               'Invoices': [{'Id':invoice_id}],
               'Contact': {'Id':txn['id']},
               'Tender': {'Id':tender_id},
               'Comment': 'Entered via API script',
               'PaymentType': 'Payline subscription'}
    return (retdata)
    
# build api call to add an invoice and payment for each transaction
def push_invoices (api, debug, dryrun, trans):
    accounts = api.execute_request("/v2/accounts")
    account = accounts[0]
    invoicesUrl = next(res for res in account.Resources if res.Name == 'Invoices').Url
    if args.debug: print('invoicesUrl:', invoicesUrl)
    tendersUrl = next(res for res in account.Resources if res.Name == 'Tenders').Url
    if args.debug: print('tendersUrl:', tendersUrl)
    paymentsUrl = next(res for res in account.Resources if res.Name == 'Payments').Url
    if args.debug: print('paymentsUrl:', paymentsUrl)

    tenders = get_tenders(api, debug, tendersUrl)
    tender_id = None
    for tender in tenders:
        #if debug: print (tender.Id, tender.Name)
        if tender.Name == "Payline":
            tender_id = tender.Id
            tender_url = tender.Url
    if tender_id != None:
        if debug: print ("Payline Id/Url = ", tender_id, tender_url)
    else:
        print ("Error: unable to find 'Payline' tender")
        return

    for txn in trans:
        if 'id' not in txn:
            continue
        invoice_id = 0
        invoice_data = build_invoice (txn)
        if debug: print ('invoice_data=', invoice_data)
        if not dryrun:
            if debug: print ("Making api call to post invoice")
            invoice_response = api.execute_request(invoicesUrl, invoice_data, raw=True)
            invoice_id = int(invoice_response.read())
            if debug: print ('invoice_id:', invoice_id)
        payment_data = build_payment (txn, tender_id, invoice_id)
        if debug: print ('payment_data=', payment_data)
        if not dryrun:
            if debug: print ("Making api call to post payment")
            payment_response = api.execute_request(paymentsUrl, payment_data)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Load CSV file from Payline and add payment records to Wild Apricot via API')
    parser.add_argument(
        '--debug', action='store_true', help='Turn on debugging prints')
    parser.add_argument(
        '--dryrun', action='store_true', help='Do not run api to insert payment records')
    parser.add_argument(
        'input_file', nargs='?', help='CSV file to read')
    args = parser.parse_args()
    if args.debug: print(args)

    if not args.input_file:
        parser.error('input_file required')

    raw_trans = load_csv (args.input_file)
    if (args.debug):
        print ('\nRaw:')
        print_trans (raw_trans)
        
    final_trans = reduce_trans (raw_trans)
    if (args.debug):
        print ('\nFinal:')
        print_trans (final_trans)

    # time to fire up the API
    apiKey = get_apiKey(apiKey_fpath)
    api = WaApi.WaApiClient("CLIENT_ID", "CLIENT_SECRET")
    api.authenticate_with_apikey (apiKey, scope='account_view contacts_view finances_view finances_edit')
    if args.debug: print('\n*** Authenticated ***')

    lookup_ids (api, args.debug, final_trans)

    push_invoices (api, args.debug, args.dryrun, final_trans)

    
