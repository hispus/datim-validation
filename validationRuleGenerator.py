# Generates DATIM validation rules

import json
import collections
import re

print('Reading...')
m = json.load(open('metadata.json'))
# fout = open('validationRules.json', 'w')

# Create two dictionaries that reference the validationRules
# by dataElement ids from their left and right sides
#
leftSides = collections.defaultdict(list)
rightSides = collections.defaultdict(list)
for rule in m['validationRules']:
    for dic, side in [[leftSides, rule['leftSide']], [rightSides, rule['rightSide']]]:
        try:
            for de in side['dataElements']:
                dic[de['id']].append(rule)
        except KeyError:
            pass

# Create a dictionary of dataElements by name
#
deByName = {}
for de in m['dataElements']:
    deByName[str.format(de['name'])] = de
    if re.match(r'BS_SCREEN', de['name']) is not None:
        print("'" + de['name'] + "'")
    if de['name'] == "BS_SCREEN (N, DSD) TARGET: Blood Units Screened":
        print("Match!")

# Create a sorted list of data element names
# (Names are processed below in sorted order only to benefit troubleshooting.)
#
sortedDeNames = sorted(deByName.keys())

# Define the patterns for creating validation rules based on data element naming convention
#
comparisonPatterns = [
    {'source': re.compile('(.+) \(N, (.+), Specimen Sent\)( TARGET)?: (.+)'), 'op': 'less_than_or_equal_to', 'dest': '\\1 (N, \\2, Screened Positive)\\3: \\4'},
    {'source': re.compile('(.+) \(N, (.+), TB Test Type\)( TARGET)?: (.+)'), 'op': 'less_than_or_equal_to', 'dest': '\\1 (N, \\2, Specimen Sent)\\3: \\4'},
    {'source': re.compile('PMTCT_EID_POS_2MO \(N, (.+)\)( TARGET)?: Infant Testing'), 'op': 'less_than_or_equal_to', 'dest': 'PMTCT_EID (N, \\1, InfantTest)\\2: Infant Testing Infant Test within 2 months of birth'},
    {'source': re.compile('PMTCT_EID_POS_12MO (N, (.+))( TARGET)?: Infant Testing'), 'op': 'less_than_or_equal_to', 'dest': 'PMTCT_EID (N, \\1, InfantTest)\\2: Infant Testing Infant Test (first) between 2 and 12'},
    {'source': re.compile('(.+) \(N, (\S+), (\S+)\)( TARGET)?: (.+)'), 'op': 'less_than_or_equal_to', 'dest': '\\1 (N, \\2)\\4: \\5'},
    {'source': re.compile('(.+) \(N, (\S+)\)( TARGET)?: (.+)'), 'op': 'less_than_or_equal_to', 'dest': '\\1 (D, \\2)\\3: \\4'}
    ]

mutuallyExclusivePatterns = [
    ]

# Loop through the data elements and create any needed validation rules
#
for deName in sortedDeNames:
    if re.match(r'$(EA_|Eval_SOP|SOP_|SIMS_)', deName) is None:
        de = deByName[deName];
        for p in comparisonPatterns:
            m = p['source'].match(de['name'])
            if m:
                destName = m.expand(p['dest'])
                dest = deByName.get(destName, None)
                print(de['name'] + ' :' + p['op'] + ': ' + destName + ('' if dest is None else ' NOT FOUND'))
                break
