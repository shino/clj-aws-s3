(ns aws.sdk.s3
  "Functions to access the Amazon S3 storage service."

  (:import com.amazonaws.auth.BasicAWSCredentials
           com.amazonaws.ClientConfiguration
           com.amazonaws.services.s3.AmazonS3Client
           com.amazonaws.AmazonServiceException
           com.amazonaws.Protocol
           com.amazonaws.services.s3.model.AccessControlList
           com.amazonaws.services.s3.model.Bucket
           com.amazonaws.services.s3.model.Grant
           com.amazonaws.services.s3.model.CanonicalGrantee
           com.amazonaws.services.s3.model.EmailAddressGrantee
           com.amazonaws.services.s3.model.GroupGrantee
           com.amazonaws.services.s3.model.ListObjectsRequest
           com.amazonaws.services.s3.model.Owner
           com.amazonaws.services.s3.model.ObjectMetadata
           com.amazonaws.services.s3.model.ObjectListing
           com.amazonaws.services.s3.model.Permission
           com.amazonaws.services.s3.model.PutObjectRequest
           com.amazonaws.services.s3.model.S3Object
           com.amazonaws.services.s3.model.S3ObjectSummary
           java.io.ByteArrayInputStream
           java.io.File
           java.io.InputStream
           java.nio.charset.Charset))

(defmacro set-attr
  "Set an attribute on an object if not nil."
  {:private true}
  [object setter value]
  `(if-let [v# ~value]
     (~setter ~object v#)))


(defprotocol ^{:no-doc true} Mappable
  "Convert a value into a Clojure map."
  (^{:no-doc true} to-map [x] "Return a map of the value."))

(extend-protocol Mappable
  Bucket
  (to-map [bucket]
    {:name          (.getName bucket)
     :creation-date (.getCreationDate bucket)
     :owner         (to-map (.getOwner bucket))})
  Owner
  (to-map [owner]
    {:id           (.getId owner)
     :display-name (.getDisplayName owner)})
  nil
  (to-map [_] nil))

(defn- protocol-from-keyword
  [protocol]
  (cond
    (= protocol :http) (Protocol/HTTP)
    (= protocol :https) (Protocol/HTTPS)))

(defn- create-config
  [{:keys [proxy-host
           proxy-port
           protocol]}
   config]
  (doto config
    (set-attr .setProxyHost proxy-host)
    (set-attr .setProxyPort proxy-port)
    (set-attr .setProtocol (protocol-from-keyword protocol))))

(defn client
  "Create a new client"

  ([access-key access-secret]
   (client access-key access-secret {}))

  ([access-key access-secret {:keys [endpoint] :as options}]
   (let [config (create-config options (ClientConfiguration.))
         creds (BasicAWSCredentials. access-key access-secret)]
     (doto (AmazonS3Client. creds config)
       (set-attr .setEndpoint endpoint)))))

(defn bucket-exists?
  "Returns true if the supplied bucket name already exists in S3."
  [s3-client name]
  (.doesBucketExist s3-client name))

(defn create-bucket
  "Create a new S3 bucket with the supplied name."
  [s3-client name]
  (to-map (.createBucket s3-client name)))

(defn delete-bucket
  "Delete the S3 bucket with the supplied name."
  [s3-client name]
  (.deleteBucket s3-client name))

(defn list-buckets
  "List all the S3 buckets for the supplied client. The buckets will be
  returned as a seq of maps with the following keys:
    :name          - the bucket name
    :creation-date - the date when the bucket was created
    :owner         - the owner of the bucket"
  [s3-client]
  (map to-map (.listBuckets s3-client)))

(defprotocol ^{:no-doc true} ToPutRequest
  "A protocol for constructing a map that represents an S3 put request."
  (^{:no-doc true} put-request [x] "Convert a value into a put request."))

(extend-protocol ToPutRequest
  InputStream
  (put-request [is] {:input-stream is})
  File
  (put-request [f] {:file f})
  String
  (put-request [s]
    {:input-stream     (ByteArrayInputStream. (.getBytes s))
     :content-length   (count s)
     :content-encoding (.name (Charset/defaultCharset))}))

(defn- map->ObjectMetadata
  "Convert a map of object metadata into a ObjectMetadata instance."
  [metadata]
  (doto (ObjectMetadata.)
    (set-attr .setCacheControl         (:cache-control metadata))
    (set-attr .setContentDisposition   (:content-disposition metadata))
    (set-attr .setContentEncoding      (:content-encoding metadata))
    (set-attr .setContentLength        (:content-length metadata))
    (set-attr .setContentMD5           (:content-md5 metadata))
    (set-attr .setContentType          (:content-type metadata))
    (set-attr .setServerSideEncryption (:server-side-encryption metadata))
    (set-attr .setUserMetadata
     (dissoc metadata :cache-control
                      :content-disposition
                      :content-encoding
                      :content-length
                      :content-md5
                      :content-type
                      :server-size-encryption))))

(defn- ->PutObjectRequest
  "Create a PutObjectRequest instance from a bucket name, key and put request
  map."
  [bucket key request]
  (cond
   (:file request)
     (let [put-obj-req (PutObjectRequest. bucket key (:file request))]
       (.setMetadata put-obj-req (map->ObjectMetadata (dissoc request :file)))
       put-obj-req)
   (:input-stream request)
     (PutObjectRequest.
      bucket key
      (:input-stream request)
      (map->ObjectMetadata (dissoc request :input-stream)))))

(defn put-object
  "Put a value into an S3 bucket at the specified key. The value can be
  a String, InputStream or File (or anything that implements the ToPutRequest
  protocol).

  An optional map of metadata may also be supplied that can include any of the
  following keys:
    :cache-control          - the cache-control header (see RFC 2616)
    :content-disposition    - how the content should be downloaded by browsers
    :content-encoding       - the character encoding of the content
    :content-length         - the length of the content in bytes
    :content-md5            - the MD5 sum of the content
    :content-type           - the mime type of the content
    :server-side-encryption - set to AES256 if SSE is required"
  [s3-client bucket key value & [metadata]]
  (->> (merge (put-request value) metadata)
       (->PutObjectRequest bucket key)
       (.putObject s3-client)))

(extend-protocol Mappable
  S3Object
  (to-map [object]
    {:content  (.getObjectContent object)
     :metadata (to-map (.getObjectMetadata object))
     :bucket   (.getBucketName object)
     :key      (.getKey object)})
  ObjectMetadata
  (to-map [metadata]
    {:cache-control          (.getCacheControl metadata)
     :content-disposition    (.getContentDisposition metadata)
     :content-encoding       (.getContentEncoding metadata)
     :content-length         (.getContentLength metadata)
     :content-md5            (.getContentMD5 metadata)
     :content-type           (.getContentType metadata)
     :etag                   (.getETag metadata)
     :last-modified          (.getLastModified metadata)
     :server-side-encryption (.getServerSideEncryption metadata)})
  ObjectListing
  (to-map [listing]
    {:bucket          (.getBucketName listing)
     :objects         (map to-map (.getObjectSummaries listing))
     :prefix          (.getPrefix listing)
     :common-prefixes (seq (.getCommonPrefixes listing))
     :truncated?      (.isTruncated listing)
     :max-keys        (.getMaxKeys listing)
     :marker          (.getMarker listing)
     :next-marker     (.getNextMarker listing)})
  S3ObjectSummary
  (to-map [summary]
    {:metadata {:content-length (.getSize summary)
                :etag           (.getETag summary)
                :last-modified  (.getLastModified summary)}
     :bucket   (.getBucketName summary)
     :key      (.getKey summary)}))

(defn get-object
  "Get an object from an S3 bucket. The object is returned as a map with the
  following keys:
    :content  - an InputStream to the content
    :metadata - a map of the object's metadata
    :bucket   - the name of the bucket
    :key      - the object's key"
  [s3-client bucket key]
  (to-map (.getObject s3-client bucket key)))

(defn get-object-metadata
  "Get an object's metadata from a bucket. The metadata is a map with the
  following keys:
    :cache-control          - the CacheControl HTTP header
    :content-disposition    - the ContentDisposition HTTP header
    :content-encoding       - the character encoding of the content
    :content-length         - the length of the content in bytes
    :content-md5            - the MD5 hash of the content
    :content-type           - the mime-type of the content
    :etag                   - the HTTP ETag header
    :last-modified          - the last modified date
    :server-side-encryption - the server-side encryption algorithm"
  [s3-client bucket key]
  (to-map (.getObjectMetadata s3-client bucket key)))

(defn- map->ListObjectsRequest
  "Create a ListObjectsRequest instance from a map of values."
  [request]
  (doto (ListObjectsRequest.)
    (set-attr .setBucketName (:bucket request))
    (set-attr .setDelimiter  (:delimiter request))
    (set-attr .setMarker     (:marker request))
    (set-attr .setMaxKeys    (:max-keys request))
    (set-attr .setPrefix     (:prefix request))))

(defn list-objects
  "List the objects in an S3 bucket. A optional map of options may be supplied.
  Available options are:
    :delimiter - read only keys up to the next delimiter (such as a '/')
    :marker    - read objects after this key
    :max-keys  - read only this many objects
    :prefix    - read only objects with this prefix

  The object listing will be returned as a map containing the following keys:
    :bucket          - the name of the bucket
    :prefix          - the supplied prefix (or nil if none supplied)
    :objects         - a list of objects
    :common-prefixes - the common prefixes of keys omitted by the delimiter
    :max-keys        - the maximum number of objects to be returned
    :truncated?      - true if the list of objects was truncated
    :marker          - the marker of the listing
    :next-marker     - the next marker of the listing"
  [s3-client bucket & [options]]
  (to-map
   (.listObjects
    s3-client
    (map->ListObjectsRequest (merge {:bucket bucket} options)))))

(defn delete-object
  "Delete an object from an S3 bucket."
  [s3-client bucket key]
  (.deleteObject s3-client bucket key))

(defn object-exists?
  "Returns true if an object exists in the supplied bucket and key."
  [s3-client bucket key]
  (try
    (get-object-metadata s3-client bucket key)
    true
    (catch AmazonServiceException e
      (if (= 404 (.getStatusCode e))
        false
        (throw e)))))

(defn copy-object
  "Copy an existing S3 object to another key."
  ([s3-client bucket src-key dest-key]
     (copy-object s3-client bucket src-key bucket dest-key))
  ([s3-client src-bucket src-key dest-bucket dest-key]
     (.copyObject s3-client src-bucket src-key dest-bucket dest-key)))

(defprotocol ^{:no-doc true} ToClojure
  "Convert an object into an idiomatic Clojure value."
  (^{:no-doc true} to-clojure [x] "Turn the object into a Clojure value."))

(extend-protocol ToClojure
  CanonicalGrantee
  (to-clojure [grantee]
    {:id           (.getIdentifier grantee)
     :display-name (.getDisplayName grantee)})
  EmailAddressGrantee
  (to-clojure [grantee]
    {:email (.getIdentifier grantee)})
  GroupGrantee
  (to-clojure [grantee]
    (condp = grantee
      GroupGrantee/AllUsers           :all-users
      GroupGrantee/AuthenticatedUsers :authenticated-users
      GroupGrantee/LogDelivery        :log-delivery))
  Permission
  (to-clojure [permission]
    (condp = permission
      Permission/FullControl :full-control
      Permission/Read        :read
      Permission/ReadAcp     :read-acp
      Permission/Write       :write
      Permission/WriteAcp    :write-acp)))

(extend-protocol Mappable
  Grant
  (to-map [grant]
    {:grantee    (to-clojure (.getGrantee grant))
     :permission (to-clojure (.getPermission grant))})
  AccessControlList
  (to-map [acl]
    {:grants (set (map to-map (.getGrants acl)))
     :owner  (to-map (.getOwner acl))}))

(defn get-bucket-acl
  "Get the access control list (ACL) for the supplied bucket. The ACL is a map
  containing two keys:
    :owner  - the owner of the ACL
    :grants - a set of access permissions granted

  The grants themselves are maps with keys:
    :grantee    - the individual or group being granted access
    :permission - the type of permission (:read, :write, :read-acp, :write-acp or
                  :full-control)."
  [s3-client bucket]
  (to-map (.getBucketAcl s3-client bucket)))

(defn get-object-acl
  "Get the access control list (ACL) for the supplied object. See get-bucket-acl
  for a detailed description of the return value."
  [s3-client bucket key]
  (to-map (.getObjectAcl s3-client bucket key)))

(defn- permission [perm]
  (case perm
    :full-control Permission/FullControl
    :read         Permission/Read
    :read-acp     Permission/ReadAcp
    :write        Permission/Write
    :write-acp    Permission/WriteAcp))

(defn- grantee [grantee]
  (cond
   (keyword? grantee)
     (case grantee
      :all-users           GroupGrantee/AllUsers
      :authenticated-users GroupGrantee/AuthenticatedUsers
      :log-delivery        GroupGrantee/LogDelivery)
   (:id grantee)
     (CanonicalGrantee. (:id grantee))
   (:email grantee)
     (EmailAddressGrantee. (:email grantee))))

(defn- clear-acl [acl]
  (doseq [grantee (->> (.getGrants acl)
                       (map #(.getGrantee %))
                       (set))]
    (.revokeAllPermissions acl grantee)))

(defn- add-acl-grants [acl grants]
  (doseq [g grants]
    (.grantPermission acl
      (grantee (:grantee g))
      (permission (:permission g)))))

(defn- update-acl [acl funcs]
  (let [grants (:grants (to-map acl))
        update (apply comp (reverse funcs))]
    (clear-acl acl)
    (add-acl-grants acl (update grants))))

(defn update-bucket-acl
  "Update the access control list (ACL) for the named bucket using functions
  that update a set of grants (see get-bucket-acl).

  This function is often used with the grant and revoke functions, e.g.

    (update-bucket-acl s3-client bucket
      (grant :all-users :read)
      (grant {:email \"foo@example.com\"} :full-control)
      (revoke {:email \"bar@example.com\"} :write))"
  [s3-client bucket & funcs]
  (let [acl (.getBucketAcl s3-client bucket)]
    (update-acl acl funcs)
    (.setBucketAcl s3-client bucket acl)))

(defn update-object-acl
  "Updates the access control list (ACL) for the supplied object using functions
  that update a set of grants (see update-bucket-acl for more details)."
  [s3-client bucket key & funcs]
  (let [acl (.getObjectAcl s3-client bucket key)]
    (update-acl acl funcs)
    (.setObjectAcl s3-client bucket key acl)))

(defn grant
  "Returns a function that adds a new grant map to a set of grants.
  See update-bucket-acl."
  [grantee permission]
  #(conj % {:grantee grantee :permission permission}))

(defn revoke
  "Returns a function that removes a grant map from a set of grants.
  See update-bucket-acl."
  [grantee permission]
  #(disj % {:grantee grantee :permission permission}))
