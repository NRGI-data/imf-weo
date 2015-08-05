import csv
import urllib
import logging
import os
from datetime import date
import shutil
import pycountry

logger = logging.getLogger()
logging.basicConfig(level=logging.INFO)

urls = []

urls.append({'url':'http://www.imf.org/external/pubs/ft/weo/' + str(date.today().year) + '/02/weodata/WEOOct' + str(date.today().year) + 'all.xls', 'year': date.today().year})
urls.append({'url':'http://www.imf.org/external/pubs/ft/weo/' + str(date.today().year) + '/01/weodata/WEOApr' + str(date.today().year) + 'all.xls', 'year': date.today().year})
urls.append({'url':'http://www.imf.org/external/pubs/ft/weo/' + str(date.today().year - 1) + '/02/weodata/WEOOct' + str(date.today().year - 1) + 'all.xls', 'year': date.today().year - 1})

# url = 'http://www.imf.org/external/pubs/ft/weo/2014/01/weodata/WEOApr2014all.xls'
# weirdly it turns out the xls (url) is in fact a tsv file ...
# fp = args.filepath + '/cache/imf-weo-2014-feb.tsv'

def download():
    logger.info('Retrieving source database: %s ...' % urls[0]['url'])
    if not os.path.exists(args.filepath + '/cache'):
        os.makedirs(args.filepath + '/cache')
    urllib.urlretrieve(urls[0]['url'], fp)
    reader = csv.DictReader(open(fp), delimiter='\t')
    if '<br />' in reader.next():
        logger.info('Source not valid, trying earlier source at %s ...' % urls[1]['url'])
        urllib.urlretrieve(urls[1]['url'], fp)
        reader = csv.DictReader(open(fp), delimiter='\t')
        if '<br />' in reader.next():
            logger.info('Source not valid, trying earlier source at %s ...' % urls[2]['url'])
            urllib.urlretrieve(urls[2]['url'], fp)
            reader = csv.DictReader(open(fp), delimiter='\t')

    logger.info('Source database downloaded to: %s' % fp)

def extract():
    logger.info('Starting extraction of data from: %s' % fp)
    reader = csv.DictReader(open(fp), delimiter='\t')
    indicators = {}
    countrys = {}
    values = []

    years = reader.fieldnames[9:-1]

    for count, row in enumerate(reader):
        # last 2 rows are blank/metadata
        # so get out when we hit a blank row
        if not row['Country']:
            break

        indicators[row['WEO Subject Code']] = [
            row['Subject Descriptor'] + ' (%s)' % row['Units'],  
            row['Subject Notes'],
            row['Units'],
            row['Scale']
            ]
        # not sure we really need given iso is standard
        countrys[row['ISO']] = row['Country']

        # need to store notes somewhere with an id ...
        # also need to uniquify the notes ...
        notes = row['Country/Series-specific Notes']
        if row['ISO'] == 'UVK':
            iso = 'XK'
        else:
            iso = pycountry.countries.get(alpha3=row['ISO']).alpha2
        newrow = {
            'iso2c': iso,
            'indicator': row['WEO Subject Code'],
            'year': None,
            'value': None
            }
        for year in years:
            if row[year] != 'n/a':
                tmprow = dict(newrow)
                tmprow['value'] = row[year]
                tmprow['year'] = year
                values.append(tmprow)

        # TODO: indicate whether a value is an estimate using
        # 'Estimates Start After'

        # delete 'Estimates Start After'
    
    outfp = args.filepath + '/data/indicators.csv'
    writer = csv.writer(open(outfp, 'w'))
    indheader = ['id', 'title', 'description', 'units', 'scale']
    writer.writerow(indheader)
    for k in sorted(indicators.keys()):
        writer.writerow( [k] + indicators[k] )

    outfp = args.filepath + '/data/values.csv'
    header = ['iso2c', 'indicator', 'year', 'value']
    writer = csv.DictWriter(open(outfp, 'w'), header)
    writer.writeheader()
    writer.writerows(values)

    logger.info('Completed data extraction to data/ directory')
    shutil.rmtree(args.filepath + '/cache')

def process():
    download()
    extract()

def check_indicators():
    reader = csv.DictReader(open(fp), delimiter='\t')
    header = ['id', 'title', 'description']
    indicators = {}
    for count, row in enumerate(reader):
        id = row['WEO Subject Code']
        notes = row['Subject Notes']
        ind = [
            row['Subject Descriptor'],
            notes,
            row['Units'],
            row['Scale']
            ]
        # check whether indicators differ
        # in their descriptions etc
        if id in indicators:
            if indicators[id][1] != notes:
                print count
                print notes
            if indicators[id][2] != row['Units']:
                print count
                print row['Units']
            if indicators[id][3] != row['Scale']:
                print count
                print row['Scale']
        indicators[id] = ind
    print len(indicators)  
    for k,v in indicators.items():
        print k, '\t\t',  v[0]

# check_indicators()

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(
        description='download and update imf weo datapackage')

     # Output file option
    parser.add_argument('-o', '--output', dest='filepath', action='store',
                        default=None, metavar='filepath',
                        help='define output filepath')
    # # Source file (default is the global cpi_source)
    # parser.add_argument('source', default=cpi_source, nargs='?',
    #                     help='source file to generate output from')
    # Parse the arguments into args
    args = parser.parse_args()

    fp = args.filepath + '/cache/imf-weo-' + str(date.today().year) + '-' + date.today().strftime('%B').lower()[:3] + '.tsv'
    # # extract()
    process()

