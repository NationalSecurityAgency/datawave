package datawave.core.iterators.uid;

/**
 * A UidMapper maps one uid to another. The mapped uid MUST exist in the same shard and MUST belong to the same data-type. Usually the only way to guarantee
 * this is if the base uid remains the same. If a null uid is returned then it is assumed that the uid mapped to itself.
 *
 * There are two uses thus far:
 *
 * 1) To determine the context of a query such as in the Top Level Document (TLD) query logic. In this case we set the uidMapper on the ShardQueryLogic and it
 * is used to map all of the documents in a tree to the top level document resulting in the entire set of documents being treated as one event.
 *
 * 2) To determine the document to be returned in lieu of a matching document such as in the Parent Document query logic. In this case we set the
 * returnUidMapper on the ShardQueryLogic and it is used to determine the documents to return in place of the matching documents.
 *
 *
 *
 */
public interface UidMapper {
    /**
     * Map the uid to another uid.
     *
     * @param uid
     *            : the uid to map : if false, then map this uid to a uid for which the query is being applied. if true, then the uid is being mapped for the
     *            sake of an end key in which case the mapped uid should map to the last uid that maps to the same uid if this parameter were false.
     * @return The mapped UID. For efficiency sake, return null is there is no mapping (i.e. the original uid is the same as the mapped uid).
     */
    String getUidMapping(String uid);

    /**
     * Map the uid to another uid.
     *
     * @param uid
     *            : map this uid to the first uid (inclusive) that maps to the same uid per getUidMapping(uid).
     * @param inclusive
     *            : if false then we actually want a uid that would be following this one
     * @return The mapped UID. For efficiency sake, return null is there is no mapping (i.e. the original uid is the same as the mapped uid).
     */
    String getStartKeyUidMapping(String uid, boolean inclusive);

    /**
     * Map the uid to another uid.
     *
     * @param uid
     *            : map this uid to the last uid (inclusive) that maps to the same uid per getUidMapping(uid).
     * @param inclusive
     *            : if false then we actually want a uid that would be following this one
     * @return The mapped UID. For efficiency sake, return null is there is no mapping (i.e. the original uid is the same as the mapped uid).
     */
    String getEndKeyUidMapping(String uid, boolean inclusive);
}
