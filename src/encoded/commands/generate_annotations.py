import requests
import json
import re


EPILOG = __doc__

_GENE_URL = 'https://www.encodeproject.org/search/?type=Gene&format=json&locations=*&field=geneid&field=name&field=symbol&field=synonyms&field=dbxrefs&field=locations&field=organism.scientific_name&organism.scientific_name=Homo+sapiens&organism.scientific_name=Mus+musculus&limit=all'


def get_annotation():
    return {
        'assembly_name': '',
        'chromosome': '',
        'start': '',
        'end': ''
    }


def rate_limited_request(url):
    response = requests.get(url)
    if int(response.headers.get('X-RateLimit-Remaining')) < 20:
        print('spleeping for about {} seconds'.format(response.headers.get('X-RateLimit-Reset')))
        time.sleep(int(float(response.headers.get('X-RateLimit-Reset'))) + 1)
    return response.json()


def assembly_mapper(location, species, input_assembly, output_assembly):
    # All others
    new_url = _ENSEMBL_URL + 'map/' + species + '/' \
        + input_assembly + '/' + location + '/' + output_assembly \
        + '/?content-type=application/json'
    try:
        new_response = rate_limited_request(new_url)
    except:
        return('', '', '')
    else:
        if not len(new_response['mappings']):
            return('', '', '')
        data = new_response['mappings'][0]['mapped']
        chromosome = data['seq_region_name']
        start = data['start']
        end = data['end']
        return(chromosome, start, end)


def all_annotations(url):
    annotations = []
    response = requests.get(url)
    for gene in response.json()['@graph']:
        doc = {'annotations': []}

        if 'organism' in gene:
            organism = gene['organism'].get('scientific_name')
            if organism == 'Homo sapiens':
                species = ' (homo sapiens)'
            elif organism == 'Mus musculus':
                species = ' (mus musculus)'

            if 'dbxrefs' in gene:
                identifier = [x for x in gene['dbxrefs'] if x.startswith(('HGNC:', 'MGI:'))]
                if len(identifier) != 1:
                    continue
                if len(identifier) == 1:
                    identifier = ''.join(identifier)

                    if 'name' not in gene:
                        continue

                    if 'name' in gene:
                        species_for_payload = re.split('[(|)]', species)[1]
                        doc['payload'] = {'id': identifier, 'species': species_for_payload}
                        doc['id'] = identifier
                        if organism == 'Homo sapiens':
                            doc['suggest'] = {
                            'input': [gene['name'] + species, gene['symbol'] + species, identifier, gene['geneid'] + ' (Gene ID)']
                        }
                        elif organism == 'Mus musculus':
                            doc['suggest'] = {'input': [gene['symbol'] + species, identifier + species]}

                    if 'synonyms' in gene and organism == 'Homo sapiens':
                        synonyms = [s + species for s in gene['synonyms']]
                        doc['suggest']['input'] = doc['suggest']['input'] + synonyms

                    if 'locations' in gene:
                        for location in gene['locations']:
                            annotation = get_annotation()
                            assembly = location['assembly']
                            if assembly  == 'hg19':
                                annotation['assembly_name'] = 'GRCh37'
                            elif assembly == 'mm9':
                                annotation['assembly_name'] = 'GRCm37'
                            elif assembly == 'mm10':
                                annotation['assembly_name'] = 'GRCm38'
                            else:
                                annotation['assembly_name'] = assembly
                            annotation['chromosome'] = location['chromosome'][3:]
                            annotation['start'] = location['start']
                            annotation['end'] = location['end']
                            doc['annotations'].append(annotation)

                    annotations.append({
                        'index': {
                            '_index': 'annotations',
                            '_type': 'default',
                            '_id': doc['id']
                        }
                    })

        # Adding gene synonyms to autocomplete
        if r['Synonyms'] is not None and r['Synonyms'] != '':
            synonyms = [x.strip(' ') + species for x in r['Synonyms'].split(',')]
            doc['suggest']['input'] = doc['suggest']['input'] + synonyms

        url = '{ensembl}lookup/id/{id}?content-type=application/json'.format(
            ensembl=_ENSEMBL_URL,
            id=r['Ensembl Gene ID'])

        try:
            response = rate_limited_request(url)
        except:
            return
        else:
            annotation = get_annotation()
            if 'assembly_name' not in response:
                return
            annotation['assembly_name'] = response['assembly_name']
            annotation['chromosome'] = response['seq_region_name']
            annotation['start'] = response['start']
            annotation['end'] = response['end']
            doc['annotations'].append(annotation)

            # Get GRcH37 annotation
            location = response['seq_region_name'] \
                + ':' + str(response['start']) \
                + '-' + str(response['end'])
            ann = get_annotation()
            ann['assembly_name'] = 'GRCh37'
            ann['chromosome'], ann['start'], ann['end'] = \
                assembly_mapper(location, response['species'],
                                'GRCh38', 'GRCh37')
            doc['annotations'].append(ann)
        annotations.append({
            "index": {
                "_index": "annotations",
                "_id": doc['id']
            }
        })
        annotations.append(doc)

    doc['annotations'].append({
        'assembly_name': 'GRCm38',
        'chromosome': r['Chromosome Name'],
        'start': r['Gene Start (bp)'],
        'end': r['Gene End (bp)']
    })

    mm9_url = '{geneinfo}{ensembl}?fields=genomic_pos_mm9'.format(
        geneinfo=_GENEINFO_URL,
        ensembl=r['Ensembl Gene ID']
    )
    try:
        response = requests.get(mm9_url).json()
    except:
        return
    else:
        if 'genomic_pos_mm9' in response and isinstance(response['genomic_pos_mm9'], dict):
                ann = get_annotation()
                ann['assembly_name'] = 'GRCm37'
                ann['chromosome'] = response['genomic_pos_mm9']['chr']
                ann['start'] = response['genomic_pos_mm9']['start']
                ann['end'] = response['genomic_pos_mm9']['end']
                doc['annotations'].append(ann)
    annotations.append({
        "index": {
            "_index": "annotations",
            "_id": doc['id']
        }
    })
    annotations.append(doc)
    print('mouse {}'.format(time.time()))
    return annotations


def get_rows_from_file(file_name, row_delimiter):
    response = requests.get(file_name)
    rows = response.content.decode('utf-8').split(row_delimiter)
    header = rows[0].split('\t')
    # remove the leading and ending double-quote string 
    # that sometimes is present in source-file fields
    zipped_rows = [dict(zip(header, [r.strip('\"') for r in row.split('\t')])) for row in rows[1:]]
    return zipped_rows


def prepare_for_bulk_indexing(annotations):
    flattened_annotations = []
    for annotation in annotations:
        if annotation:
            for item in annotation:
                flattened_annotations.append(item)
    return flattened_annotations


def human_annotations(human_file):
    """
    Generates JSON from TSV files
    """
    zipped_rows = get_rows_from_file(human_file, '\n')
    # Too many processes causes the http requests causes the remote to respond with error
    pool = mp.Pool(processes=2)
    annotations = pool.map(human_single_annotation, zipped_rows)
    return prepare_for_bulk_indexing(annotations)


def mouse_annotations(mouse_file):
    """
    Updates and get JSON file for mouse annotations
    """
    zipped_rows = get_rows_from_file(mouse_file, '\n')
    # Too many processes causes the http requests causes the remote to respond with error
    pool = mp.Pool(processes=2)
    annotations = pool.map(mouse_single_annotation, zipped_rows)
    return prepare_for_bulk_indexing(annotations)


def other_annotations(file, species, assembly):
    """
    Generates C. elegans and drosophila annotaions
    """
    annotations = []
    response = requests.get(file)
    header = []
    species_for_payload = re.split('[(|)]', species)[1]
    for row in response.content.decode('utf-8').split('\n'):
        # skipping header row
        if len(header) == 0:
            header = row.split('\t')
            continue

        r = dict(zip(header, row.split('\t')))
        if 'Chromosome Name' not in r or 'Ensembl Gene ID' not in r:
            continue

        doc = {'annotations': []}
        annotation = get_annotation()

        doc['suggest'] = {'input': [r['Associated Gene Name'] + species]}
        doc['payload'] = {'id': r['Ensembl Gene ID'],
                          'species': species_for_payload}
        doc['id'] = r['Ensembl Gene ID']
        annotation['assembly_name'] = assembly
        annotation['chromosome'] = r['Chromosome Name']
        annotation['start'] = r['Gene Start (bp)']
        annotation['end'] = r['Gene End (bp)']
        doc['annotations'].append(annotation)
        annotations.append({
            "index": {
                "_index": "annotations",
                "_id": doc['id']
            }
        })
        annotations.append(doc)
    return annotations


def main():
    '''
    Get annotations from portal Gene objects
    This helps to implement autocomplete for region search
    '''
    import argparse
    parser = argparse.ArgumentParser(
        description="Generate annotations JSON file for multiple species",
        epilog=EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    annotations = all_annotations(_GENE_URL)

    # Create annotations JSON file
    with open('annotations_local.json', 'w') as outfile:
        json.dump(annotations, outfile)


if __name__ == '__main__':
    main()
