from pprint import pprint

from couchbase.cluster import Cluster, ClusterOptions, QueryOptions
from couchbase.auth import PasswordAuthenticator
import couchbase.subdocument as SD


if __name__ == '__main__':
   try:
       # Cluster specific information
       cloud_endpoint = \
                   'cb.jzx-fxhtrfnpeej.cloud.couchbase.com'
       username = 'sherlock.holmes'
       password = 'P@ssw0rd'
       bucket_name = 'travel-sample'
      
       # connect to cluster
       conn_str = 'couchbases://{}?ssl=no_verify'.format(cloud_endpoint)
       cluster_opts = ClusterOptions(PasswordAuthenticator(username,
                                       password))
       cluster = Cluster(conn_str, cluster_opts)
       bucket = cluster.bucket(bucket_name)
       collection = bucket.default_collection()

       #KV operation data
       test_key = 'testDoc::0'
       test_doc =  {
           'type': 'testDoc',
           'info': 'This is a test',
           'address': {
               'home': {
                   'address1': '8575 Lewis Springs Mountains',
                   'city': 'West Mohammed',
                   'state': 'WI',
                   'zipCode': '40243-4741',
                   'country': 'CA'
                   }
               }
           }
       sd_path = 'address.home.address1'
       sd_update = '8575 Lewis Springs Mountains Blvd'

       #KV - insert
       ins_result = collection.insert(test_key, test_doc)
       print('\nInserted doc w/ key: {}; CAS: {}\n'.format(test_key,
                                                       ins_result.cas))


       #KV - get
       get_result = collection.get(test_key)
       pprint(get_result.content)

       #KV - upsert
       ins_result = collection.upsert(test_key, test_doc)
       print('\nUpserted doc w/ key: {}; CAS: {}\n'.format(test_key,
                                                       ins_result.cas))

       #KV - subdoc - mutate-in
       mti_result = collection.mutate_in(test_key,
                                   [SD.upsert(sd_path, sd_update)])
       print('Mutated sub-doc w/ key: {} and path: {}; CAS: {}\n'.format(test_key,
                                                   sd_path,
                                                   mti_result.cas))

       #KV - subdoc - lookup-in
       lki_result = collection.lookup_in(test_key,
                                   [SD.get(sd_path), SD.exists(sd_path)])
       print('Lookup in result, key: {}, path: {}, value: {}\n'.format(test_key,
                                                   sd_path,
                                                   lki_result.content_as[str](0)))

       #KV - remove
       rm_result = collection.remove(test_key)
       print('Removed doc w/ key {}; CAS: {}\n'.format(test_key,
                                                   rm_result.cas))
       
       #basic query w/ GROUP BY
       query_str = '''
       SELECT
           t.type AS DocType,
           COUNT(1) AS DocCount
       FROM `{}` t
       GROUP BY t.type
       '''.format(bucket_name)
       q_result = cluster.query(query_str)
       for r in q_result.rows():
           pprint(r)
       print()
      
       #sub-query example
       query_str = '''
       SELECT
           name,
           country,
           (SELECT raw avg(s.ratings.Overall)
            FROM t.reviews  as s)[0] AS overall_avg_rating
       FROM `{}` AS t
       WHERE
           t.type = "hotel"
       ORDER BY
           overall_avg_rating DESC
       LIMIT 10;
       '''.format(bucket_name)
       q_result = cluster.query(query_str)
       for r in q_result.rows():
           pprint(r)
       print()

       #positional query params
       query_str = '''
       SELECT COUNT(1) AS DocCount
       FROM `{}` t
       WHERE t.type=$1
       '''.format(bucket_name)
       query_opts = QueryOptions(positional_parameters=['hotel'])
       q_result = cluster.query(query_str, query_opts)
       for r in q_result.rows():
           pprint(r)
       print()

       #named query params
       query_str = '''
       SELECT COUNT(1) AS DocCount
       FROM `{}` t
       WHERE t.type=$doc_type
       '''.format(bucket_name)
       query_opts = QueryOptions(named_parameters={'doc_type':'airline'})
       q_result = cluster.query(query_str, query_opts)
       for r in q_result.rows():
           pprint(r)
       print()

   except Exception as ex:
       import traceback
       traceback.print_exc()

