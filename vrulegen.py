# Generates DATIM validation rules

import json
import collections
import os
import re
import requests

server_auth=("haase","C0coa-datim")
server_root="http://localhost:8080/datim_latest/api/"

def readAll(dhis2type,fields):
    req=requests.get(server_root+dhis2type,auth=server_auth,params={'paging': False,'fields': fields})
    jsonout=req.json()
    return jsonout[dhis2type]

def readObj(id,dhis2type):
    req=requests.get(server_root+dhis2type+"/"+id,auth=server_autho)
    jsonout=req.json()
    return jsonout

dataElements = readAll('dataElements',"id,uid,name,shortName,categoryCombo,description")
validationRules = readAll('validationRules',"id,uid,name,rightSide[expression,dataElements],leftSide[expression,dataElements],operator")

addedRules = []
ruleSignatures = []
for rule in validationRules:
    op='nop'
    if rule.has_key('operator'):
        op=rule['operator']
    else:
        print('Rule '+str(rule)+' has no operator')
        continue
    ls=rule['leftSide']
    rs=rule['rightSide']
    lsd=ls['dataElements']
    rsd=rs['dataElements']
    if len(lsd) == 1:
        if len(rsd) == 1:
            lsid=lsd[0]['id']
            rsid=rsd[0]['id']
        else:
            continue
    else:
        continue
    # print("Rule "+rule['id']+' compares '+lsid+' with '+rsid+' based on '+ls['expression']+' and '+rs['expression'])
    if ls['expression'] != '#{'+lsid+'}':
        continue
    elif rs['expression'] != '#{'+rsid+'}':
        continue
    else:
        sig=[lsid,op,rsid]
        ruleSignatures.append(sg)

# Create a dictionary of dataElements by name
#
def deName(de):
   return unicode.format(de['name'])
def deShortName(de):
   return unicode.format(de['shortName'])
deByName = {}
deByShortName = {}

def indexDataElements():
    ambigShortNames = []
    for de in dataElements:
        name=deName(de)
        shortName=deShortName(de)
        deByName[name] = de
        deByName[shortName] = de

def makeVRULE(ls,op,rs):
    sig = [ls['id'],op,rs['id']]
    if sig in ruleSignatures:
        return False
    lse = { 'expression': '#{'+ls['id']+'}', 'dataElements': [ { 'id': ls['id'] } ],
            'missingValueStrategy': 'NEVER_SKIP'}
    rse = { 'expression': '#{'+rs['id']+'}', 'dataElements': [ { 'id': rs['id'] } ],
            'missingValueStrategy': 'NEVER_SKIP'}
    if ls.has_key('description'):
        lse['description']=ls['description']
    else:
        lse['description']=ls['name']
    if rs.has_key('description'):
        rse['description']=rs['description']
    else:
        rse['description']=rs['name']
    return {'leftSide': lse, 'rightSide': rse, 'operator': op}

# Define the patterns for creating validation rules based on data element naming convention
#
rulePatterns = [
    {'source': re.compile('(.+) \(N, (.+), Specimen Sent\)( TARGET|): (.+)'), 
     'op': 'less_than_or_equal_to', 
     'dest': '\\1 (N, \\2, Screened Positive)\\3: \\4',
     'id': 'MR1'},
    {'source': re.compile('(.+) \(N, (.+), TB Test Type\)( TARGET|): (.+)'), 
     'op': 'less_than_or_equal_to', 
     'dest': '\\1 (N, \\2, Specimen Sent)\\3: \\4',
     'id': 'MR2'},
    {'source': re.compile('PMTCT_EID_POS_2MO \(N, (.+)\)( TARGET|): Infant Testing'), 
     'op': 'less_than_or_equal_to', 
     'dest': 'PMTCT_EID (N, \\1, InfantTest)\\2: Infant Testing',
     'id': 'MR3'},
    {'source': re.compile('PMTCT_EID_POS_12MO \(N, (.+)\)( TARGET|): Infant Testing'), 
     'op': 'less_than_or_equal_to', 
     'dest': 'PMTCT_EID (N, \\1, InfantTest)\\2: Infant Testing',
     'id': 'MR4'},
    {'source': re.compile('(.+) \(N, (\S+), (\S+)\)( TARGET|): (.+)'), 
     'op': 'less_than_or_equal_to', 
     'dest': '\\1 (N, \\2)\\4: \\5',
     'name': 'Total > disagg for \\1 \\4',
     'id': 'MR5'},
    {'source': re.compile('(.+) \(N, (\S+)\)( TARGET|): (.+)'), 
     'op': 'less_than_or_equal_to', 
     'dest': '\\1 (D, \\2)\\3',
     'name': 'Numerator > Denominator for \\1 (\\2) \\3',
     'id': 'MR6'},
    {'source': re.compile('(.+) \((N|D), (.+), (AgeLessThanTen|AgeAboveTen/Sex)(/Positive|)\)( TARGET|): (.+)'), 
     'op': 'exclusive_pair', 
     'dest': '\\1 (\\2, \\3, Aggregated Age/Sex\\5)\\6',
     'id': 'MR7'},
    {'source': re.compile('(.+) \((N|D), (.+), (AgeLessThanTen|AgeAboveTen/Sex)(/Positive)\)( TARGET|): (.+)'), 
     'op': 'exclusive_pair', 
     'dest': '\\1 (\\2, \\3, Age/Sex Aggregated/Result)\\6',
     'id': 'MR8'}
    ]

# Loop through the data elements and create any needed validation rules
#
def main():
    indexDataElements()
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
                id=p['id']
                vrule=False
                importance="MEDIUM"
                ruleType="VALIDATION"
                periodType="Monthly"
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
                    vrule = False
                    if dest:
                        vrule=makeVRULE(dataElement,p['op'],dest)
                    if vrule:
                        vrule['importance']=importance
                        vrule['ruleType']=ruleType
                        vrule['periodType']=periodType
                        if p.has_key('description'):
                            vrule['description']=m.expand(p['description'])
                        if p.has_key('instruction'):
                            vrule['instruction']=m.expand(p['instruction'])
                    print(id+'\t'+de['name'] + '\t:' + p['op'] + ':\t' + destName + '\t' + ('NOT FOUND' if dest is None else ''))
                    if stats.has_key(id):
                        stats[id]=stats[id]+1
                    else:
                        stats[id]=1
                    if vrule:
                        addedRules.append(vrule)
                    matched=True
        if matched:
            matchedElements.append(dataElement)
        else:
            print('?? '+eltName)
    output_file=os.getenv('OUTPUT')
    if not output_file:
        output_file='vrules_import.json'
    output=open (output_file,'w')
    output.write(json.dumps({'validationRules': addedRules}, 
                            sort_keys=True,indent=4,
                            separators=(',',':')))
    output.close()
    print('Adding '+str(len(addedRules))+' validation rules to '+
          str(len(ruleSignatures))+" current rules based on "+
          str(len(matchedElements))+"/"+str(len(processedElements))+
          "/"+str(len(dataElements))+
          " data elements, by meta rules:")
    for p in rulePatterns:
        id=p['id']
        if stats.has_key(id):
            print('\t'+id+':\t'+str(stats[id])+' rules')
        else:
            print('\t'+id+':\tno rules')

if __name__ == "__main__":
    main()

