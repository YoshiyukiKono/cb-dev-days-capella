from couchbase.cluster import Cluster, ClusterOptions
from couchbase.auth import PasswordAuthenticator

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
 
   except Exception as ex:
       import traceback
       traceback.print_exc()

