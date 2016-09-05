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

def setup():
    global defaultCOCid
    allDataElements = getAll('dataElements',"id,name,shortName,categoryCombo[id,categoryOptionCombos[id,name]],description,dataSets")
    allValidationRules = getAll('validationRules',"id,name,rightSide[expression,dataElements],leftSide[expression,dataElements],operator")
    defaultCOCid = findAll('categoryOptionCombos','name:eq:default',"id")[0]['id'];
    for rule in allValidationRules:
        try:
            op=rule['operator']
            ls=rule['leftSide']['dataElements']
            rs=rule['rightSide']['dataElements']
            ruleSignatures.append([ls,op,rs])
            if op == 'greater_than_or_equal_to':
                ruleSignatures.append([rs,'less_than_or_equal_to',ls])
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

def makeElementExpression(elt,disaggName,missing_value_strategy='NEVER_SKIP'):
    eltid=elt['id']
    if disaggName:
        expression="#{"+eltid+"."+findDisaggId(elt,disaggName)+"}"
    else:
        expression="#{"+eltid+"}"
    description='Value of element '+eltid+' ('+elt['name']+')'
    return { 'expression': expression, 
             'description': description,
             'dataElements': [ { 'id': eltid } ],
             'missingValueStrategy': missing_value_strategy };
        

def makeVRULE(ls,op,rs,rs_disagg,mr_name=False,use_name=False,use_description=False):
    if op in ('exclusive_pair','complementary_pair'):
        mv_strategy='SKIP_IF_ALL_VALUES_MISSING'
    else:
        mv_strategy='NEVER_SKIP'
    lse = makeElementExpression(ls,None,mv_strategy)
    rse = makeElementExpression(rs,rs_disagg,mv_strategy)
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
        if rs_disagg:
            name+=' / '+rs_disagg
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
     'dest': ['PMTCT_EID (N, \\1, InfantTest)\\2: Infant Testing', 'Infant Test within 2 months of birth'],
     'id': 'MR03'},
    {'source': re.compile('PMTCT_EID_POS_12MO \(N, (.+)\)( TARGET|): Infant Testing'), 
     'op': 'less_than_or_equal_to', 
     'dest': ['PMTCT_EID (N, \\1, InfantTest)\\2: Infant Testing', 'Infant Test (first)  between 2 and 12'],
     'id': 'MR04'},
    {'source': re.compile('(.+) \((.),\s*(\S+),\s*([^,)]+)\)( TARGET|): Number Registered'), 
     'op': 'less_than_or_equal_to', 
     'dest': '\\1 (\\2, \\3)\\5: New/Relapsed TB with HIV',
     'name': '\\1 (\\2, \\3, \\4)\\5 <= Total',
     'id': 'MR05'},
    {'source': re.compile('(.+) \((.),\s*(\S+),\s*([^,)]+)\)( TARGET|): (.+)'), 
     'op': 'less_than_or_equal_to', 
     'dest': '\\1 (\\2, \\3)\\5: \\6',
     'name': '\\1 (\\2, \\3, \\4)\\5 <= Total',
     'id': 'MR05'},
    {'source': re.compile('(.+) \(N,\s+([^,)]+)\)( TARGET|): (.+)'), 
     'op': 'less_than_or_equal_to', 
     'dest': '\\1 (D, \\2)\\3',
     'name': '\\1 (N, \\2)\\3 <= Denominator',
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
                if 'periodType' in p:
                    periodType=p['periodType']
                if 'importance' in p:
                    importance=p['importance']
                if 'ruletype' in p:
                    ruleType=p['ruletype']
                m = p['source'].match(eltName)
                if m:
                    if type(p['dest']) is list:
                        destName = m.expand(p['dest'][0])
                        destDisaggName = p['dest'][1]
                    else:
                        destName = m.expand(p['dest'])
                        destDisaggName = ''
                    dest = deByName.get(destName, None)
                    if 'description' in p:
                        use_description=m.expand(p['description'])
                    else:
                        use_description = False
                    if 'name' in p:
                        use_name=m.expand(p['name'])
                    else:
                        use_name = False
                    vrule = False
                    if dest is not None:
                        if destDisaggName:
                            showDisagg = ' / ' + destDisaggName
                        else:
                            showDisagg = ''
                        print(ruleid+'\t'+de['name'] + ' (' + de['id'] + ')' + '\t:' + p['op'] + ':\t' + dest['name'] + ' (' + dest['id'] + ')' + showDisagg + ' based on ' + destName)
                    else:
                        print(ruleid+'\t'+de['name'] + '(' + de['id'] + ')' + '\t:' + p['op'] + ':\t' + destName + '\t' + 'NOT FOUND')
                    if dest:
                        vrule=makeVRULE(dataElement,p['op'],dest,destDisaggName,ruleid,use_name,use_description)
                    if vrule:
                        vrule['importance']=importance
                        vrule['ruleType']=ruleType
                        vrule['periodType']=periodType
                        if 'instruction' in p:
                            vrule['instruction']=m.expand(p['instruction'])
                        else:
                            vrule['instruction']=vrule['description']
                    if ruleid in stats:
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
        sig=[rule['leftSide']['dataElements'],
             rule['operator'],
             rule['rightSide']['dataElements']]
        if sig not in ruleSignatures:
            if rule['name'] not in rulesByName:
                addedRules.append(rule)
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
    output.write(json.dumps({'validationRules': addedRules}, 
                            sort_keys=True,indent=4,
                            separators=(',',':')))
    output.close()

if __name__ == "__main__":
    main()

