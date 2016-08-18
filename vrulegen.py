# Generates DATIM validation rules

import json
import collections
import os
import re
import requests

global server_root, server_auth, defaultCOCid, config

server_root="http://localhost:8080/api/"
server_auth=("admin","district")

object_caches={}
debugging=False

defaultCOCid = False
dataElements = []
validationRules = []
rulesByName = {}
ruleSignatures = []

deByName = {}
deByShortName = {}

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
    elif not params.has_key('fields'):
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
    elif isinstance(arg,dict) and arg.has_key('id'):
        id=arg['id']
    else:
        raise "Bad object argument"
    if object_caches.has_key(dhis2type):
        cache=object_caches[dhis2type]
    else:
        cache={}
        object_caches[dhis2type]=cache
    if cache.has_key(id):
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


def setup():
    global defaultCOCid
    allDataElements = getAll('dataElements',"id,name,shortName,categoryCombo,categoryOptionCombos,description,dataSets")
    allValidationRules = getAll('validationRules',"id,name,rightSide[expression,dataElements],leftSide[expression,dataElements],operator")
    defaultCOCid = findAll('categoryOptionCombos','name:eq:default',"id")[0]['id'];
    for rule in allValidationRules:
        try:
            op=rule['operator']
            ls=rule['leftSide']['expression']
            rs=rule['leftSide']['expression']
            sig=[ls,op,rs]
            ruleSignatures.append(sig)
            rulesByName[rule['name']]=rule
            validationRules.append(rule)
        except:
            print('Rule '+str(rule)+' is weird')
            continue
    for de in allDataElements:
        if de.has_key('dataSets') and len(de['dataSets']) > 0:
            dataElements.append(de)
            name=deName(de)
            shortName=deShortName(de)
            deByName[name] = de
            deByName[shortName] = de

def getDisAggs(element):
    catcomboref=element['categoryCombo']
    catcombo=getObj('categoryCombos',catcomboref)
    return catcombo['categoryOptionCombos']

# Create a dictionary of dataElements by name
#
def deName(de):
   return unicode.format(de['name'])
def deShortName(de):
   return unicode.format(de['shortName'])

def makeElementExpression(elt,missing_value_strategy='NEVER_SKIP'):
    eltid=elt['id']
    expression="#{"+eltid+"}"
    description='Value of element '+eltid+' ('+elt['name']+')'
    return { 'expression': expression, 
             'description': description,
             'dataElements': [ { 'id': eltid } ],
             'missingValueStrategy': missing_value_strategy };
        

def makeVRULE(ls,op,rs,mr_name=False,use_name=False,use_description=False):
    if op in ('exclusive_pair','complementary_pair'):
        mv_strategy='SKIP_IF_ALL_VALUES_MISSING'
    else:
        mv_strategy='NEVER_SKIP'
    lse = makeElementExpression(ls,mv_strategy)
    rse = makeElementExpression(rs,mv_strategy)
    if ls.has_key('shortName'):
        lname=ls['shortName']
    else:
        lname=ls['name']
    if rs.has_key('shortName'):
        rname=rs['shortName']
    else:
        rname=rs['name']
    if op_symbols.has_key(op):
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
    return {'leftSide': lse, 'rightSide': rse, 'operator': op,
            'name': name, 'description': description}

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
     'dest': 'PMTCT_EID (N, \\1, InfantTest)\\2: Infant Testing',
     'id': 'MR03'},
    {'source': re.compile('PMTCT_EID_POS_12MO \(N, (.+)\)( TARGET|): Infant Testing'), 
     'op': 'less_than_or_equal_to', 
     'dest': 'PMTCT_EID (N, \\1, InfantTest)\\2: Infant Testing',
     'id': 'MR04'},
    {'source': re.compile('(.+) \(N,\s*(\S+),\s*([^,)]+)\)( TARGET|): (.+)'), 
     'op': 'less_than_or_equal_to', 
     'dest': '\\1 (N, \\2)\\4: \\5',
     'name': 'Total > disagg for \\1 \\4',
     'id': 'MR05'},
    {'source': re.compile('(.+) \(N,\s+([^,)]+)\)( TARGET|): (.+)'), 
     'op': 'less_than_or_equal_to', 
     'dest': '\\1 (D, \\2)\\3',
     'name': 'Numerator > Denominator for \\1 (\\2) \\3',
     'id': 'MR06'},
    {'source': re.compile('(.+) \((N|D), (.+), Age/Sex(/Result|)\)( TARGET|): (.+)'), 
     'op': 'exclusive_pair', 
     'dest': '\\1 (\\2, \\3, Age/Sex Aggregated\\4)\\6',
     'id': 'MR07'},
    {'source': re.compile('(.+) \((N|D), (.+), Age/Sex(/Result)\)( TARGET|): (.+)'), 
     'op': 'exclusive_pair', 
     'dest': '\\1 (\\2, \\3, Age/Sex Aggregated\\4)\\6',
     'id': 'MR08'},
    {'source': re.compile('(.+) \((N|D), (.+), (AgeLessThanTen|AgeAboveTen/Sex)(/Positive|)\)( TARGET|): (.+)'), 
     'op': 'exclusive_pair', 
     'dest': '\\1 (\\2, \\3, Aggregated Age/Sex\\5)\\6',
     'id': 'MR09'},
    {'source': re.compile('(.+) \((N|D), (.+), (AgeLessThanTen|AgeAboveTen/Sex)(/Positive)\)( TARGET|): (.+)'), 
     'op': 'exclusive_pair', 
     'dest': '\\1 (\\2, \\3, Age/Sex Aggregated/Result)\\6',
     'id': 'MR10'}
    ]

# Loop through the data elements and create any needed validation rules
#
def main():
    global server_root, server_auth, defaultCOCid, dataElements, validationRules, config
    loadConfig("default_config.json")
    sortedDataElements = sorted(dataElements, key=deName)
    stats={}
    newRules = []
    addedRules = []
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
                if p.has_key('periodType'):
                    periodType=p['periodType']
                if p.has_key('importance'):
                    importance=p['importance']
                if p.has_key('ruletype'):
                    ruleType=p['ruletype']
                m = p['source'].match(eltName)
                if m:
                    destName = m.expand(p['dest'])
                    dest = deByName.get(destName, None)
                    if p.has_key('description'):
                        use_description=m.expand(p['description'])
                    else:
                        use_description = False
                    if p.has_key('name'):
                        use_name=m.expand(p['name'])
                    else:
                        use_name = False
                    vrule = False
                    if dest is not None:
                        print(ruleid+'\t'+de['name'] + ' (' + de['id'] + ')' + '\t:' + p['op'] + ':\t' + dest['name'] + ' (' + dest['id'] + ') based on ' + destName)
                    else:
                        print(ruleid+'\t'+de['name'] + '(' + de['id'] + ')' + '\t:' + p['op'] + ':\t' + destName + '\t' + 'NOT FOUND')
                    if dest:
                        vrule=makeVRULE(dataElement,p['op'],dest,ruleid,use_name,use_description)
                    if vrule:
                        vrule['importance']=importance
                        vrule['ruleType']=ruleType
                        vrule['periodType']=periodType
                        if p.has_key('instruction'):
                            vrule['instruction']=m.expand(p['instruction'])
                        else:
                            vrule['instruction']=vrule['description']
                    if stats.has_key(ruleid):
                        stats[ruleid]=stats[ruleid]+1
                    else:
                        stats[ruleid]=1
                    if vrule:
                        newRules.append(vrule)
                    matched=True
        if matched:
            matchedElements.append(dataElement)
        else:
            print('?? '+eltName)
    for rule in newRules:
        sig=(rule['leftSide']['expression'],
             rule['operator'],
             rule['rightSide']['expression'])
        if sig not in ruleSignatures:
            if rule['name'] not in rulesByName:
                addedRules.append(rule)
            else:
                print('Rule name conflict despite unique sig '+str(sig)+'\n\t'+rule['name']+'\n\t'+str(rulesByName[rule['name']]))
    print('Adding '+str(len(addedRules))+'/'+str(len(newRules))+' validation rules to '+
          str(len(ruleSignatures))+" current rules based on "+
          str(len(matchedElements))+"/"+str(len(processedElements))+
          "/"+str(len(dataElements))+
          " data elements, by meta rules:")
    for p in rulePatterns:
        ruleid=p['id']
        if stats.has_key(ruleid):
            print('\t'+ruleid+':\t'+str(stats[ruleid])+' rules generated')
        else:
            print('\t'+ruleid+':\tno rules generated')
    output_file=os.getenv('OUTPUT')
    if not output_file:
        output_file='vrules_import.json'
    output=open (output_file,'w')
    output.write(json.dumps({'validationRules': addedRules}, 
                            sort_keys=True,indent=4,
                            separators=(',',':')))
    output.close()

if __name__ == "__main__":
    main()

