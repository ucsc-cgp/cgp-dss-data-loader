import uuid

import dcp_analysis_functions as dcp
import json
import requests


def setup_credentials():
    dcp.add_keys('credentials.json')
    dcp.query_summary_counts('topmed-public')


ALIGNED_READS_QUERY = """
query AlignedReads ($projectID: [String], $first: Int, $offset: Int)
    {
     submitted_aligned_reads(project_id: $projectID, first: $first, offset: $offset) {
        id
        file_name
        file_size
        md5sum
        submitter_id
        updated_datetime
        created_datetime
       _links {
          id
          type
        }
      }
    }
"""


def make_query(query, variables):
    response = dcp.query_api(query, variables)
    return response


def get_alignments():
    pages = [{'first': 1, 'offset': i, 'projectID': 'topmed-public'} for i in range(107)]
    return list(map(lambda page: make_query(ALIGNED_READS_QUERY, page), pages))


READ_INDEX_QUERY = """
query ReadIndexes ($id: String)
    {
     aligned_reads_index(id: $id) {
        id
        file_name
        file_size
        md5sum
        submitter_id
        updated_datetime
        created_datetime
      }
    }

"""


def alignments_to_raw_bundles(alignments):
    index_ids = []

    for alignment in alignments:
        for link in alignment['data']['submitted_aligned_reads'][0]['_links']:
            if link['type'] == 'aligned_reads_index':
                index_ids.append(link['id'])

    pages = [{'id': x} for x in index_ids]
    indices = list(map(lambda page: make_query(READ_INDEX_QUERY, page), pages))
    return list(zip(alignments, indices))


INDEXD_URL = 'https://dcp.bionimbus.org/index'


def get_indexd_id_by_checksum(checksum):
    response = requests.get("{}/index/?hash=md5:{}".format(INDEXD_URL, checksum))
    return response.json()['ids'][0]


def fix_bundles(raw_bundles):
    hashes = [bundle[1]['data']['aligned_reads_index'][0]['md5sum'] for bundle in raw_bundles]
    indexd_ids = [get_indexd_id_by_checksum(chk) for chk in hashes]
    return list(zip(raw_bundles, indexd_ids))


def get_indexd_doc(indexd_id):
    response = requests.get("{}/index/{}".format(INDEXD_URL, indexd_id))
    return response.json()


def fixed_bundles_to_indexd_bundles(fixed_bundles):
    return [(x[0][0],
             x[0][1],
             get_indexd_doc(x[1]),
             get_indexd_doc(x[0][0]['data']['submitted_aligned_reads'][0]['id']))
            for x in fixed_bundles]


def bundle_to_flat(bundle):
    dos_base = "https://dos.bionimbus.org/ga4gh/dos/v1/dataobjects"
    alignment = {}
    index = {}
    alignment_metadata = bundle[0]['data']['submitted_aligned_reads'][0]
    index_metadata = bundle[1]['data']['aligned_reads_index'][0]
    alignment_indexd = bundle[3]
    index_indexd = bundle[2]
    alignment['gsurl'] = next(filter(lambda x: 'gs://' in x, alignment_indexd['urls']))
    alignment['s3url'] = next(filter(lambda x: 's3://' in x, alignment_indexd['urls']))
    alignment['name'] = alignment_metadata['file_name']
    alignment['dosurl'] = "{}/{}".format(dos_base, alignment_indexd['did'])
    alignment['updated_datetime'] = alignment_metadata['updated_datetime']
    alignment['created_datetime'] = alignment_metadata['created_datetime']
    alignment['md5sum'] = alignment_metadata['md5sum']
    alignment['size'] = alignment_metadata['file_size']
    alignment['did'] = alignment_indexd['did']
    index['gsurl'] = next(filter(lambda x: 'gs://' in x, index_indexd['urls']))
    index['s3url'] = next(filter(lambda x: 's3://' in x, index_indexd['urls']))
    index['dosurl'] = "{}/{}".format(dos_base, index_indexd['did'])
    index['name'] = index_metadata['file_name']
    index['updated_datetime'] = index_metadata['updated_datetime']
    index['created_datetime'] = index_metadata['created_datetime']
    index['md5sum'] = index_metadata['md5sum']
    index['size'] = index_metadata['file_size']
    index['did'] = index_indexd['did']
    return alignment, index, bundle


def flatten_bundles(indexd_bundles):
    return [bundle_to_flat(b) for b in indexd_bundles]


CORE_METADATA_QUERY = """
query CoreMetadata ($id: String)
    {
     core_metadata(id: $id) {
          description
          data_type
          rights
          format
          creator
          contributor
          submitter_id
          language
          relation
          source
          coverage
          date
          title
          project_id
          subject
     }
    }

"""


def alignments_to_core_metadata(alignments):
    core_metadata_ids = []
    for alignment in alignments:
        for link in alignment['data']['submitted_aligned_reads'][0]['_links']:
            if link['type'] == 'core_metadata':
                core_metadata_ids.append(link['id'])
    pages = [{'id': x} for x in core_metadata_ids]
    return [dcp.query_api(CORE_METADATA_QUERY, page) for page in pages]


def flatten_core_metadata(core_metadata):
    return [x['data']['core_metadata'][0] for x in core_metadata]


READ_GROUP_QUERY = """
query ReadGroupLinks ($id: String)
    {
     read_group(id: $id) {
     _links {
       type
       id
     }
     }
    }

"""


def alignments_to_read_groups(alignments):
    read_group_ids = []
    for alignment in alignments:
        for link in alignment['data']['submitted_aligned_reads'][0]['_links']:
            if link['type'] == 'read_group':
                read_group_ids.append(link['id'])
    return [dcp.query_api(READ_GROUP_QUERY, {'id': x}) for x in read_group_ids]


ALIQUOT_QUERY = """
query Aliquots ($id: String)
    {
     aliquot(id: $id) {
        type
        project_id
        submitter_id
        aliquot_quantity
        aliquot_volume
        amount
        analyte_isolation_batch_id
        analyte_isolation_date
        analyte_isolation_method
        analyte_type
        concentration
        exclude
        exclusion_criteria
        experiment_batch_id
        experiment_date
        experimental_strategy
         _links {
           type
           id
         }
     }
    }

"""


def read_groups_to_aliquots(read_groups):
    aliquot_ids = []
    for read_group in read_groups:
        for link in read_group['data']['read_group'][0]['_links']:
            if link['type'] == 'aliquot':
                aliquot_ids.append(link['id'])
    return [dcp.query_api(ALIQUOT_QUERY, {'id': x}) for x in aliquot_ids]


SAMPLE_QUERY = """
query SampleMetadata ($id: String)
    {
     sample(id: $id) {
        id
        type
        project_id
        submitter_id
        autolysis_score
        biospecimen_anatomic_site
        biospecimen_anatomic_site_detail
        biospecimen_anatomic_site_uberon_id
        biospecimen_anatomic_site_uberon_term
        biospecimen_physical_site
        biospecimen_type
        collection_kit
        collection_site
        composition
        current_weight
        freezing_method
        hours_to_collection
        hours_to_sample_procurement
        internal_notes
        is_ffpe
        method_of_sample_procurement
        oct_embedded
        pathology_notes
        preservation_method
        prosector_notes
        sample_type
        tissue_type
     }
    }

"""


def aliquots_to_samples(aliquots):
    sample_ids = []
    for aliquot in aliquots:
        for link in aliquot['data']['aliquot'][0]['_links']:
            if link['type'] == 'sample':
                sample_ids.append(link['id'])
    return [dcp.query_api(SAMPLE_QUERY, {'id': x}) for x in sample_ids]


def flatten_aliquot(aliquots):
    flattened_aliquots = []
    for aliquot in aliquots:
        aliquot = aliquot['data']['aliquot'][0]
        del aliquot['_links']
        flattened_aliquots.append(aliquot)
    return flattened_aliquots


def flatten_samples(samples):
    flattened_samples = []
    for sample in samples:
        sample = sample['data']['sample'][0]
        flattened_samples.append(sample)
    return flattened_samples


def make_bundles():
    alignments = get_alignments()
    raw_bundles = alignments_to_raw_bundles(alignments)
    fixed_bundles = fix_bundles(raw_bundles)
    indexd_bundles = fixed_bundles_to_indexd_bundles(fixed_bundles)
    flattened_bundles = flatten_bundles(indexd_bundles)
    core_metadata = alignments_to_core_metadata(alignments)
    flattened_core_metadata = flatten_core_metadata(core_metadata)
    read_groups = alignments_to_read_groups(alignments)
    aliquots = read_groups_to_aliquots(read_groups)
    samples = aliquots_to_samples(aliquots)
    flattened_samples = flatten_samples(samples)
    flattened_aliquots = flatten_aliquot(aliquots)
    return list(zip(flattened_core_metadata,
                flattened_aliquots,
                flattened_samples,
                [f[0] for f in flattened_bundles],  # alignment
                [f[1] for f in flattened_bundles])) # index


def write_bundles_to_files(bundles, filename):
    json_bundles = [{'bundle_did': str(uuid.uuid4()),
                     'core_metadata': b[0],
                     'aliquot': b[1],
                     'sample': b[2],
                     'manifest': [b[3], b[4]]}
                    for b in bundles]

    with open(filename, 'w') as outfile:
        json.dump(json_bundles, outfile)


if __name__ == '__main__':
    setup_credentials()
    write_bundles_to_files(make_bundles(), 'topmed-public.json')
