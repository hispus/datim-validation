# Generates DATIM validation rules

import json
import collections
import os
import re
import requests
import random
import string

global server_root, server_auth, defaultCOCid, config

server_root="http://localhost:8080/api/"
server_auth=("admin","district")

object_caches={}
debugging=False

defaultCOCid = False
deliveryPoints = {}
dataElements = []
validationRules = []
rulesByName = {}
ruleSignatures = []
ruleExpressionSignatures = []
newRules = []
addedRules = []

deByName = {}
deByShortName = {}

random.seed()

def makeUid():
    uid = random.choice(string.ascii_letters)
    for i in range(0, 10):
        uid += random.choice(string.ascii_letters+'0123456789')
    return uid

def loadConfig(file="default_config.json"):
    global server_root, server_auth, config
    config=json.load(open(file,"r"))
    server_root=config['api']['root']
    server_auth=tuple(config['api']['auth'])
    setup()

op_symbols={'less_than_or_equal_to': '<=',
            'exclusive_pair': ':exclusive:'}

def getAll(dhis2type,fields):
    uri=server_root+dhis2type
    req=requests.get(uri,auth=server_auth,params={'paging': False,'fields': fields})
    try:
        jsonout=req.json()
    except:
        print('Error getting '+uri+' returned '+req.text)
        raise Exception("getAll failed at "+uri)
    return jsonout[dhis2type]

def findAll(dhis2type,filter,fields=False):
    params={'filter': filter}
    params['paging']=False
    if fields is not False:
        params['fields']=fields
    elif not 'fields' in params:
        params['fields']="id,name,shortName,description"
    uri=server_root+dhis2type
    req=requests.get(uri,auth=server_auth,params=params)
    try:
        jsonout=req.json()
    except:
        print('Error getting '+uri+' returned '+req.text)
        raise Exception("findAll failed at "+uri)
    return jsonout[dhis2type]

def getObj(dhis2type,arg):
    if type(arg) is str:
        id=arg
    elif type(arg) is unicode:
        id=arg
    elif isinstance(arg,dict) and 'id' in arg:
        id=arg['id']
    else:
        raise "Bad object argument"
    if dhis2type in object_caches:
        cache=object_caches[dhis2type]
    else:
        cache={}
        object_caches[dhis2type]=cache
    if id in cache:
        return cache[id]
    else:
        uri=server_root+dhis2type+"/"+id
        req=requests.get(uri,auth=server_auth)
        try:
            jsonout=req.json()
        except:
            print('Error getting '+uri+' returned '+req.text)
            raise Exception("getObj failed at "+uri)
        cache[id]=jsonout
        return jsonout

def getDeliveryPoints():
    global deliveryPoints
    serviceDeliveryPointCategories = findAll('categories','name:eq:Service Delivery Point',"categoryOptions[id,name,categoryOptionCombos[id,name,categoryOptionCombos[id,name]]")
    for servicePoint in serviceDeliveryPointCategories[0]['categoryOptions']:
        for combo in servicePoint['categoryOptionCombos']:
            if 'Positive' in combo['name']:
                positive = combo['id']
            elif 'Negative' in combo['name']:
                negative = combo['id']
            else:
                all = combo['id']
        deliveryPoints[servicePoint['name']] = {'positive': positive, 'negative': negative}
    deliveryPointCategoryCombos = findAll('dataElements','name:eq:HTC_TST (N, DSD, ServiceDeliveryPoint): HTC received results',"categoryCombo[categoryOptionCombos[id,name]]")
    for optionCombo in deliveryPointCategoryCombos[0]['categoryCombo']['categoryOptionCombos']:
        deliveryPoints[optionCombo['name']]['all'] = optionCombo['id']

def setup():
    global defaultCOCid
    allDataElements = getAll('dataElements',"id,name,shortName,categoryCombo[id,categoryOptionCombos[id,name]],description,dataSets")
    allValidationRules = getAll('validationRules',"id,name,rightSide[expression,dataElements],leftSide[expression,dataElements],operator")
    defaultCOCid = findAll('categoryOptionCombos','name:eq:default',"id")[0]['id']
    getDeliveryPoints()
    for rule in allValidationRules:
        try:
            op=rule['operator']
            ls=rule['leftSide']['dataElements'][0]['id']
            rs=rule['rightSide']['dataElements'][0]['id']
            lsx=rule['leftSide']['expression']
            rsx=rule['rightSide']['expression']
            ruleSignatures.append([ls,op,rs])
            if op == 'greater_than_or_equal_to':
                ruleSignatures.append([rs,'less_than_or_equal_to',ls])
            ruleExpressionSignatures.append([lsx,op,rsx])
            rulesByName[rule['name']]=rule
            validationRules.append(rule)
        except:
            print('Rule '+str(rule)+' is weird')
            continue
    for de in allDataElements:
        if 'dataSets' in de and len(de['dataSets']) > 0:
            dataElements.append(de)
            name=deName(de)
            shortName=deShortName(de)
            deByName[name] = de
            deByName[shortName] = de

# Create a dictionary of dataElements by name
#
def deName(de):
   return unicode.format(de['name'])
def deShortName(de):
   return unicode.format(de['shortName'])

def findDisaggId(elt,disaggName):
    for coc in elt['categoryCombo']['categoryOptionCombos']:
        if coc['name'] == disaggName:
            return coc['id']
    print('Error: could not find disagg "'+disaggName+'" for data element '+elt['id']+' '+elt['name'])
    raise Exception("findDisaggId failed.")

def makeElementExpression(elt,elt2,disaggs,elementName,missing_value_strategy='NEVER_SKIP'):
    eltid=elt['id']
    disaggDesc=''
    if disaggs:
        expression=''
        for disagg in disaggs:
            if expression:
                expression+='+'
                disaggDesc+=' +'
            expression+="#{"+eltid+"."+disagg['id']+"}"
            disaggDesc+=' '+disagg['name']
    else:
        expression="#{"+eltid+"}"
    dataElements = [ { 'id': eltid } ]
    if elt2:
        expression+="+#{"+elt2['id']+"}"
        disaggDesc+=" + "+elt2['name']
        dataElements.append({ 'id': elt2['id'] })
    description='Value of element '+eltid+' ' + (elementName or '('+elt['name']+')'+disaggDesc)
    return { 'expression': expression, 
             'description': description,
             'dataElements': dataElements,
             'missingValueStrategy': missing_value_strategy }

def makeVRULE(ls,secondSource,op,rs,lsDisaggs,rsDisaggs,destElementName,mr_name,use_name,use_description,importance,strategy,ruleType,periodType,instruction):
    lse = makeElementExpression(ls,secondSource,lsDisaggs,None,strategy)
    rse = makeElementExpression(rs,None,rsDisaggs,destElementName,strategy)
    if 'shortName' in ls:
        lname=ls['shortName']
    else:
        lname=ls['name']
    if 'shortName' in rs:
        rname=rs['shortName']
    else:
        rname=rs['name']
    if op in op_symbols:
        opname=op_symbols[op]
    else:
        opname=op
    if use_name:
        name=use_name
    else:
        name=lname+' '+opname+' '+rname
    if mr_name and debugging:
        name=name+' ('+mr_name+')'
    if use_description:
        description=use_description
    else:
        description=name
    if not instruction:
        instruction=description
    return {'leftSide': lse, 'rightSide': rse, 'operator': op,
            'name': name, 'description': description, 'importance': importance,
            'ruleType': ruleType, 'periodType': periodType, 'instruction': instruction, 'id': makeUid()}

def makeServiceDeliveryPointsRules(ls,secondSource,op,rs,importance,strategy,ruleType,periodType):
    global deliveryPoints, ruleExpressionSignatures, addedRules
    for key in deliveryPoints:
        point = deliveryPoints[key]
        lsDisaggs=[{'name': 'Positive', 'id': point['positive']}, {'name': 'Negative', 'id': point['negative']}]
        rsDisaggs=[{'name': 'All', 'id': point['all']}]
        ruleName=ls['name']+', '+key+', Positive + Negative <= All'
        vrule = makeVRULE(ls,secondSource,op,rs,lsDisaggs,rsDisaggs,None,None,ruleName,ruleName,importance,strategy,ruleType,periodType,ruleName)
        sigx=[vrule['leftSide']['expression'],
              vrule['operator'],
              vrule['rightSide']['expression']]
        if sigx not in ruleExpressionSignatures:
            if vrule['name'] not in rulesByName:
                addedRules.append(vrule)
            else:
                print('Service Delivery Point Rule name conflict despite unique sigx '+str(sigx)+'\n\t'+ruleName+'\n\t'+str(rulesByName[ruleName]))
        else:
            print('Rule expression exists for rule:')
            print(vrule)

# Define the patterns for creating validation rules based on data element naming convention
#
rulePatterns = [
    {'source': re.compile('(.+) \(N, (.+), Specimen Sent\)( TARGET|): (.+)'),
     'op': 'less_than_or_equal_to',
     'dest': '\\1 (N, \\2, Screened Positive)\\3: \\4',
     'id': 'MR01'},
    {'source': re.compile('(.+) \(N, (.+), TB Test Type\)( TARGET|): (.+)'),
     'op': 'less_than_or_equal_to',
     'dest': '\\1 (N, \\2, Specimen Sent)\\3: \\4',
     'id': 'MR02'},
    {'source': re.compile('PMTCT_EID_POS_2MO \(N, (.+)\)( TARGET|): Infant Testing'),
     'op': 'less_than_or_equal_to',
     'dest': ['PMTCT_EID (N, \\1, InfantTest)\\2: Infant Testing', 'Infant Test within 2 months of birth'],
     'id': 'MR03'},
    {'source': re.compile('PMTCT_EID_POS_12MO \(N, (.+)\)( TARGET|): Infant Testing'),
     'op': 'less_than_or_equal_to',
     'dest': ['PMTCT_EID (N, \\1, InfantTest)\\2: Infant Testing', 'Infant Test (first)  between 2 and 12'],
     'id': 'MR04'},
    {'source': re.compile('(.+) \((.),\s*(\S+),\s*([^,)]+)\)( TARGET|): Number Registered'),
     'op': 'less_than_or_equal_to',
     'dest': '\\1 (\\2, \\3)\\5: New/Relapsed TB with HIV',
     'name': '\\1 (\\2, \\3, \\4)\\5: Number Registered <= Total',
     'id': 'MR05'},
    {'source': re.compile('(.+) \((.),\s*(\S+),\s*([^,)]+)\)( TARGET|): 12 Months Viral Load'),
     'op': 'less_than_or_equal_to',
     'dest': '\\1 (\\2, \\3)\\5: 12 Months Prior',
     'name': '\\1 (\\2, \\3, \\4)\\5: 12 Months Viral Load <= Total',
     'id': 'MR06'},
    {'source': re.compile('(.+) \((.),\s*(\S+),\s*([^,)]+)\)( TARGET|): 12 Months Viral Load < 1000'),
     'op': 'less_than_or_equal_to',
     'dest': '\\1 (\\2, \\3)\\5: 12 Months Prior',
     'name': '\\1 (\\2, \\3, \\4)\\5: 12 Months Viral Load < 1000 <= Total',
     'id': 'MR07'},
    {'source': re.compile('(.+) \((.),\s*(\S+),\s*([^,)]+)\)( TARGET|): Key Pop Preventive'),
     'op': 'less_than_or_equal_to',
     'dest': '\\1 (\\2, \\3)\\5: Estimated Key Pop',
     'name': '\\1 (\\2, \\3, \\4)\\5: Key Pop Preventive <= Total',
     'id': 'MR08'},
    {'source': re.compile('(.+) \((.),\s*(\S+),\s*([^,)]+)\)( TARGET|): Screened Positive TB Symptoms'),
     'op': 'less_than_or_equal_to',
     'dest': '\\1 (\\2, \\3)\\5: PLHIV Screened',
     'name': '\\1 (\\2, \\3, \\4)\\5: Screened Positive TB Symptoms <= Total',
     'id': 'MR09'},
    {'source': re.compile('(.+) \((.),\s*(\S+), Specimen Sent\)( TARGET|): Specimens Sent'),
     'op': 'less_than_or_equal_to',
     'dest': '\\1 (\\2, Screened Positive)\\4: Screened Positive TB Symptoms',
     'name': '\\1 (\\2, \\3, Specimen Sent)\\4: Specimens Sent <= Screened Positive',
     'id': 'MR10'},
    {'source': re.compile('(.+) \((.),\s*(\S+), TB Test Type\)( TARGET|): Specimens Sent'),
     'op': 'less_than_or_equal_to',
     'dest': '\\1 (\\2, Specimen Sent)\\4: Specimens Sent',
     'name': '\\1 (\\2, \\3, TB Test Type)\\4: Specimens Sent <= Specimens Sent',
     'id': 'MR11'},
    {'source': re.compile('GEND_GBV \(N, (\S+), PEP\)( TARGET|): GBV Care'),
     'op': 'less_than_or_equal_to',
     'dest': ['GEND_GBV (N, \\1, Age/Sex/ViolenceType)\\2: GBV Care',
              'Unknown Age, Female, Sexual Violence (Post-Rape Care)',
              '<10, Female, Sexual Violence (Post-Rape Care)',
              '10-14, Female, Sexual Violence (Post-Rape Care)',
              '15-19, Female, Sexual Violence (Post-Rape Care)',
              '20-24, Female, Sexual Violence (Post-Rape Care)',
              '25-49, Female, Sexual Violence (Post-Rape Care)',
              '50+, Female, Sexual Violence (Post-Rape Care)',
              'Unknown Age, Male, Sexual Violence (Post-Rape Care)',
              '<10, Male, Sexual Violence (Post-Rape Care)',
              '10-14, Male, Sexual Violence (Post-Rape Care)',
              '15-19, Male, Sexual Violence (Post-Rape Care)',
              '20-24, Male, Sexual Violence (Post-Rape Care)',
              '25-49, Male, Sexual Violence (Post-Rape Care)',
              '50+, Male, Sexual Violence (Post-Rape Care)'],
     'name': 'GEND_GBV (N, \\1, PEP)\\2: GBV Care <= GEND_GBV (N, DSD, Age/Sex/ViolenceType=Sexual Violence)',
     'destElementName': 'Sum of GEND_GBV (N, \\1, Age/Sex/ViolenceType=Sexual Violence)\\2: GBV Care',
     'id': 'MR12'},
    {'source': re.compile('(.+) \((.),\s*(\S+),\s*([^,)]+)\)( TARGET|): HTC result positive'),
     'op': 'less_than_or_equal_to',
     'dest': '\\1 (\\2, \\3)\\5: HTC received results',
     'name': '\\1 (\\2, \\3, \\4)\\5: HTC result positive <= Total',
     'id': 'MR13'},
    {'source': re.compile('(.+) \((.),\s*(\S+),\s*([^,)]+)\)( TARGET|): Active Beneficiaries'),
     'op': 'less_than_or_equal_to',
     'dest': '\\1 (\\2, \\3)\\5: Beneficiaries Served',
     'name': '\\1 (\\2, \\3, \\4)\\5: Active Beneficiaries <= Total',
     'id': 'MR14'},
    {'source': re.compile('(.+) \((.),\s*(\S+),\s*([^,)]+)\)( TARGET|): Known Results'),
     'op': 'less_than_or_equal_to',
     'dest': '\\1 (\\2, \\3)\\5: New ANC and L&D clients',
     'name': '\\1 (\\2, \\3, \\4)\\5: Known Results <= Total',
     'id': 'MR15'},
    {'source': re.compile('(.+) \((.),\s*(\S+),\s*([^,)]+)\)( TARGET|): HIV Prevention Program'),
     'op': 'less_than_or_equal_to',
     'dest': '\\1 (\\2, \\3)\\5: New on ART',
     'name': '\\1 (\\2, \\3, \\4)\\5: HIV Prevention Program <= Total',
     'id': 'MR16'},
    {'source': re.compile('(.+) \((.),\s*(\S+),\s*([^,)]+)\)( TARGET|): Number Positive'),
     'op': 'less_than_or_equal_to',
     'dest': '\\1 (\\2, \\3)\\5: TB/HIV on ART',
     'name': '\\1 (\\2, \\3, \\4)\\5: Number Positive <= Total',
     'id': 'MR17'},
    {'source': re.compile('(.+) \((.),\s*(\S+),\s*([^,)]+)\)( TARGET|): Alive at 12 mo. after initiating ART'),
     'op': 'less_than_or_equal_to',
     'dest': '\\1 (\\2, \\3)\\5: Total Initiated ART in 12 mo.',
     'name': '\\1 (\\2, \\3, \\4)\\5: Alive at 12 mo. after initiating ART <= Total',
     'id': 'MR18'},
    {'source': re.compile('(.+) \((.),\s*(\S+),\s*(\S+)\)( TARGET|): Received PrEP'),
     'op': 'less_than_or_equal_to',
     'dest': '\\1 (\\2, \\3)\\5: Newly Enrolled PrEP',
     'name': '\\1 (\\2, \\3)\\5: Received PrEP <= Total',
     'id': 'MR19'},
    {'source': re.compile('GEND_GBV_PEP \((.),\s*(\S+)\)( TARGET|): GBV PEP'),
     'op': 'less_than_or_equal_to',
     'dest': 'GEND_GBV (\\1, \\2)\\3: GBV Care',
     'name': 'GEND_GBV_PEP (\\1, \\2)\\3: GBV PEP <= Total',
     'id': 'MR20'},
    {'source': re.compile('TX_RET \(D, (\S+), Aggregated Age/Sex\) v2( TARGET|): Alive at 12 mo. after initiating ART'),
     'op': 'less_than_or_equal_to',
     'dest': 'TX_RET (D, \\1)\\2: Total Initiated ART in 12 mo.',
     'name': 'TX_RET (D, \\1, Aggregated Age/Sex)\\2: Alive at 12 mo. after initiating ART <= Total',
     'id': 'MR21'},
    {'source': re.compile('(.+) \((.),\s*(\S+),\s*([^,)]+)\)( TARGET|)( v\d+|): (.+|)'),
     'secondSource': { 'match': 'AgeLessThanTen', 'replace': 'AgeAboveTen/Sex' },
     'op': 'less_than_or_equal_to',
     'dest': '\\1 (\\2, \\3)\\5: \\7',
     'name': '\\1 (\\2, \\3, \\4)\\5\\6: \\7 <= Total',
     'redirect': [
         { 'dest': 'KP_PREV (N, DSD):', 'alt': 'KP_PREV (N, DSD, KeyPop) v2: Key Pop Preventive' },
         { 'dest': 'KP_PREV (N, TA):', 'alt': 'KP_PREV (N, TA, KeyPop) v2: Key Pop Preventive' },
         { 'dest': 'OVC_SERV (N, DSD):', 'alt': 'OVC_SERV (N, DSD, ProgramStatus): Beneficiaries Served' },
         { 'dest': 'OVC_SERV (N, TA):', 'alt': 'OVC_SERV (N, TA, ProgramStatus): Beneficiaries Served' },
         { 'dest': 'TX_PVLS (N, DSD):', 'alt': 'TX_PVLS (N, DSD, RoutineTargeted) v2: 12 Months Viral Load < 1000' },
         { 'dest': 'TX_PVLS (N, TA):', 'alt': 'TX_PVLS (N, TA, RoutineTargeted) v2: 12 Months Viral Load < 1000' }
      ],
     'except': [re.compile('OVC_SERV \(N, .*, Age/Sex/Service'),
                re.compile('GEND_GBV \(N, DSD, PEP\)'),
                re.compile('GEND_GBV \(N, TA, PEP\)')],
     'id': 'MR22'},
    {'source': re.compile('TB_SCREENDX \((.),\s*(\S+),\s*([^,)]+)\)( TARGET|): (.+|)'),
     'op': 'less_than_or_equal_to',
     'dest': 'TB_SCREEN (\\1, \\2)\\4: \\5',
     'name': 'TB_SCREENDX (\\1, \\2, \\3)\\4: \\5 <= Total',
     'match': 'exact',
     'id': 'MR23'},
    {'source': re.compile('(.+) \(([^,)]+)\)( TARGET|)( v\d+|)(: .+|)'),
     'op': 'less_than_or_equal_to',
     'dest': '\\1\\3\\4\\5',
     'name': '\\1 (\\2)\\3\\4\\5 <= Total',
     'match': 'exact',
     'id': 'MR24'},
    {'source': re.compile('PMTCT_FO \(N, (\S+), Outcome\) v2( TARGET|): Final Outcomes Exposed Infants'),
     'op': 'less_than_or_equal_to',
     'dest': 'PMTCT_FO (D, \\1)\\2:Exposed Infants',
     'name': 'PMTCT_FO (N, \\1) v2\\2: Final Outcomes Exposed Infants <= Denominator',
     'id': 'MR25'},
    {'source': re.compile('PMTCT_ARV_(SUB|)NAT \(N, (SUB|)NAT, NewExistingArt\)( TARGET|): ARVs'),
     'op': 'less_than_or_equal_to',
     'dest': 'PMTCT_ARV_\\1NAT (D, \\2NAT)\\3: ARVs',
     'name': 'PMTCT_ARV_\\1NAT (N, \\2NAT, NewExistingArt)\\3: ARVs <= Denominator',
     'id': 'MR26'},
    {'source': re.compile('KP_PREV \(N, (\S+), KeyPop\) v2( TARGET|): Key Pop Preventive'),
     'op': 'less_than_or_equal_to',
     'dest': 'KP_PREV (D, \\1, KeyPop) v2\\2: Key Pop Preventive',
     'name': 'KP_PREV (N, \\1, KeyPop)\\1: ARVs <= Denominator',
     'optional': ['KP_PREV'],
     'id': 'MR27'},
    {'source': re.compile('(.+) \(N,\s+([^,)]+)\)( TARGET|): (.+)'),
     'op': 'less_than_or_equal_to',
     'dest': '\\1 (D, \\2)\\3:',
     'name': '\\1 (N, \\2)\\3: \\4 <= Denominator',
     'redirect': [
         { 'dest': 'PMTCT_STAT (D, DSD):', 'alt': 'PMTCT_STAT (D, DSD, Age) v2: New ANC clients' },
         { 'dest': 'PMTCT_STAT (D, TA):', 'alt': 'PMTCT_STAT (D, TA, Age) v2: New ANC clients' },
         { 'dest': 'OVC_SERV (N, DSD):', 'alt': 'OVC_SERV (N, DSD, ProgramStatus): Beneficiaries Served' },
         { 'dest': 'OVC_SERV (N, TA):', 'alt': 'OVC_SERV (N, TA, ProgramStatus): Beneficiaries Served' }
      ],
     'optional': ['KP_PREV', 'PP_PREV'],
     'id': 'MR28'},
    {'source': re.compile('TB_SCREEN \(N, (\S+)\)( TARGET|): PLHIV Screened'),
     'op': 'less_than_or_equal_to',
     'dest': 'TB_SCREENDX (D, \\1)\\2: PLHIV Reported Symptoms',
     'name': 'TB_SCREEN (N, \\1)\\2: PLHIV Screened <= Denominator',
     'id': 'MR29'},
    {'source': re.compile('(.+) \((N|D), (.+), Age(/Sex)(/Result|)\)( TARGET|): (.+)'),
     'op': 'exclusive_pair',
     'dest': '\\1 (\\2, \\3, Age\\4 Aggregated\\5)\\6: \\7',
     'id': 'MR30'},
    {'source': re.compile('(.+) \((N|D), (.+), (.*)/Age/Female(.*)\)( TARGET|): (.+)'),
     'op': 'exclusive_pair',
     'dest': '\\1 (\\2, \\3, \\4/Aggregated Age/Female\\5)\\6: \\7',
     'id': 'MR31'},
    {'source': re.compile('(.+) \((N|D), (.+), (.*/)(AgeLessThanTen|AgeAboveTen/Sex)(/Result)(/Positive|)\)( TARGET|): (.+)'),
     'op': 'exclusive_pair',
     'dest': '\\1 (\\2, \\3, \\4Aggregated Age/Sex\\6)\\7: \\8',
     'id': 'MR32'},
    {'source': re.compile('(.+) \((N|D), (.+), (AgeLessThanTen|AgeAboveTen/Sex)(/Positive)\)( TARGET|): (.+)'),
     'op': 'exclusive_pair',
     'dest': '\\1 (\\2, \\3, Age/Sex Aggregated/Result)\\6: \\7',
     'id': 'MR33'},
    {'source': re.compile('(.+) \(N, NAT, Sex\)( TARGET|): (.+)'),
     'op': 'exclusive_pair',
     'dest': '\\1 (N, NAT, Age/Sex)\\2: \\3',
     'id': 'MR34'},
    {'source': re.compile('(.+) \((N|D), (.+), ServiceDeliveryPoint/Result\)( TARGET|): (.+)'),
     'op': 'less_than_or_equal_to',
     'dest': '\\1 (\\2, \\3, ServiceDeliveryPoint)\\4: \\5',
     'special': 'serviceDeliveryPoint',
     'id': 'MR35'},
    {'source': re.compile('OVC_HIVSTAT \(N, (.+), StatusPosART\)( TARGET|): OVC Disclosed Known HIV Status'),
     'op': 'less_than_or_equal_to',
     'dest': ['OVC_HIVSTAT (N, \\1, ReportedStatus)\\2: OVC Disclosed Known HIV', 'Positive'],
     'id': 'MR36'},
    {'source': re.compile('OVC_HIVSTAT \(N, (.+), StatusNotRep\)( TARGET|): OVC Disclosed Known HIV Status'),
     'op': 'less_than_or_equal_to',
     'dest': ['OVC_HIVSTAT (N, \\1, ReportedStatus)\\2: OVC Disclosed Known HIV', 'Undisclosed to IP'],
     'id': 'MR37'},
    {'source': re.compile('VMMC_CIRC_(SUB|)NAT \(N, (SUB|)NAT\)( TARGET|): Voluntary Circumcised'),
     'op': 'less_than_or_equal_to',
     'dest': ['VMMC_TOTALCIRC_\\1NAT (N, \\2NAT)\\3: Voluntary Circumcised'],
     'id': 'MR38'}
    ]

def getDeStartingWith(destName):
    for key in deByName.keys():
        if len(key) >= len(destName) and key[:len(destName)] == destName:
            return deByName.get(key)
    return None


# Loop through the data elements and create any needed validation rules
#
def main():
    global server_root, server_auth, defaultCOCid, dataElements, validationRules, config, addedRules
    loadConfig("default_config.json")
    sortedDataElements = sorted(dataElements, key=deName)
    stats={}
    processedElements=[]
    matchedElements=[]
    for dataElement in sortedDataElements:
        eltName=unicode.format(dataElement['name'])
        if re.match(r'$(EA_|Eval_SOP|SOP_|SIMS_)', eltName) is None:
            de = deByName[eltName];
            processedElements.append(dataElement)
            matched = False
            for p in rulePatterns:
                ruleid=p['id']
                vrule=False
                importance="MEDIUM"
                ruleType="VALIDATION"
                periodType="Quarterly"
                if eltName.find("TARGET") > 0:
                    periodType="FinancialOct"
                if 'periodType' in p:
                    periodType=p['periodType']
                if 'importance' in p:
                    importance=p['importance']
                if 'ruletype' in p:
                    ruleType=p['ruletype']
                m = p['source'].match(eltName)
                secondSource = None
                if 'secondSource' in p:
                    if p['secondSource']['replace'] in eltName:
                        m = None # Don't process data element now, because it's secondary to another.
                    elif p['secondSource']['match'] in eltName:
                        secondSourceName = eltName.replace(p['secondSource']['match'],p['secondSource']['replace'])
                        secondSource = deByName.get(secondSourceName)
                        print("SecondSource: "+ p['secondSource']['match'] + " -> " + p['secondSource']['replace']+" -> "+secondSourceName)
                if 'except' in p:
                    if type(p['except']) is list:
                        for e in p['except']:
                            if e.match(eltName):
                                m = None
                    elif p['except'].match(eltName):
                        m = None
                if m:
                    if type(p['dest']) is list:
                        destName = m.expand(p['dest'][0])
                        destDisaggNames = p['dest'][1:]
                    else:
                        destName = m.expand(p['dest'])
                        destDisaggNames = False
                    if 'redirect' in p:
                        for redir in p['redirect']:
                            if redir['dest'] in destName:
                                destName = redir['alt']
                    if 'match' in p and p['match']=='exact':
                        dest = deByName.get(destName)
                    else:
                        dest = getDeStartingWith(destName)
                    if 'description' in p:
                        use_description=m.expand(p['description'])
                    else:
                        use_description = False
                    if 'name' in p:
                        use_name=m.expand(p['name'])
                        if secondSource:
                            use_name=use_name.replace( p['secondSource']['match'], p['secondSource']['match']+'+'+p['secondSource']['replace'] )
                    else:
                        use_name = False
                    if 'instruction' in p:
                        instruction=m.expand(p['instruction'])
                    else:
                        instruction=use_description
                    destElementName = ''
                    if 'destElementName' in p:
                        destElementName = m.expand(p['destElementName'])
                    if p['op'] in ('exclusive_pair','complementary_pair'):
                        strategy='SKIP_IF_ALL_VALUES_MISSING'
                    else:
                        strategy='NEVER_SKIP'
                        if 'optional' in p:
                            for opt in p['optional']:
                                if opt in destName:
                                    strategy='SKIP_IF_ANY_VALUE_MISSING'
                    vrule = False
                    if dest:
                        if destDisaggNames:
                            destDisaggs=[]
                            showDisagg=''
                            for disName in destDisaggNames:
                                destDisaggs.append({'name': disName, 'id': findDisaggId(dest,disName)})
                                showDisagg+=' / ' + disName
                            if not use_name:
                                use_name=dataElement['shortName']+' '+p['op']+' '+dest['shortName']+showDisagg
                        else:
                            destDisaggs=False;
                            showDisagg=''
                        print(ruleid+'\t'+de['name'] + ' (' + de['id'] + ')' + '\t:' + p['op'] + ':\t' + dest['name'] + ' (' + dest['id'] + ')' + showDisagg + ' based on ' + destName)
                    else:
                        print(ruleid+'\t'+de['name'] + '(' + de['id'] + ')' + '\t:' + p['op'] + ':\t' + destName + '\t' + 'NOT FOUND')
                    if dest:
                        if 'special' in p and p['special'] == 'serviceDeliveryPoint':
                            makeServiceDeliveryPointsRules(dataElement,secondSource,p['op'],dest,importance,strategy,ruleType,periodType)
                        else:
                            vrule=makeVRULE(dataElement,secondSource,p['op'],dest,None,destDisaggs,destElementName,None,use_name,use_description,importance,strategy,ruleType,periodType,instruction)
                    if ruleid in stats:
                        stats[ruleid]=stats[ruleid]+1
                    else:
                        stats[ruleid]=1
                    if vrule:
                        print('\t'+vrule['name']+' '+vrule['id'])
                        newRules.append(vrule)
                    matched=True
        if matched:
            matchedElements.append(dataElement)
        else:
            print('?? '+eltName)
    validationRuleGroup = {'name': 'All MER Validation Rules', 'id': 'wnFo1vX2IW3', 'validationRules': []}
    for rule in validationRules:
        validationRuleGroup['validationRules'].append({ 'id': rule['id'] })
    for rule in newRules:
        sig=[rule['leftSide']['dataElements'][0]['id'],
             rule['operator'],
             rule['rightSide']['dataElements'][0]['id']]
        if sig not in ruleSignatures:
            if rule['name'] not in rulesByName:
                addedRules.append(rule)
                validationRuleGroup['validationRules'].append({ 'id': rule['id'] })
            else:
                print('Rule name conflict despite unique sig '+str(sig)+'\n\t'+rule['name']+'\n\t'+str(rulesByName[rule['name']]))
    print('Adding '+str(len(addedRules))+'/'+str(len(newRules))+' validation rules to '+
          str(len(rulesByName))+" current rules based on "+
          str(len(matchedElements))+"/"+str(len(processedElements))+
          "/"+str(len(dataElements))+
          " data elements, by meta rules:")
    for p in rulePatterns:
        ruleid=p['id']
        if ruleid in stats:
            print('\t'+ruleid+':\t'+str(stats[ruleid])+' rules generated')
        else:
            print('\t'+ruleid+':\tno rules generated')
    output_file=os.getenv('OUTPUT')
    if not output_file:
        output_file='vrules_import.json'
    output=open (output_file,'w')
    output.write(json.dumps({'validationRules': addedRules, 'validationRuleGroups': [validationRuleGroup]},
                            sort_keys=True,indent=4,
                            separators=(',',':')))
    output.close()

    remove_file='vrules_remove.sh'
    out_remove=open(remove_file,'w')
    os.fchmod(out_remove.fileno(), 0o755) # make the script executable
    out_remove.write('#!/bin/bash\nset -x #echo on\n\n')
    deleteCommand='curl -X DELETE -u '+server_auth[0]+':'+server_auth[1]+' "'+server_root+'validationRules/'
    for r in addedRules:
        out_remove.write(deleteCommand+r['id']+'"\n')
    out_remove.close()

if __name__ == "__main__":
    main()

