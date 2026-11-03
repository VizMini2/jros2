package us.ihmc.fastddsjava.natives;

/**
 * Native access to the ROS graph seen by a participant's builtin discovery, plus the sample identities
 * needed to pair service replies with their requests. Kept apart from {@link fastddsjava} so the
 * natives can be shipped without changing that class.
 */
public final class fastddsjavaGraph
{
   private fastddsjavaGraph()
   {
   }

   /**
    * Unicast discovery peers (IPv4 addresses) for every participant created from now on, like ROS_STATIC_PEERS.
    * Multicast discovery is kept. An empty array goes back to the profile's defaults.
    */
   public static native void setInitialPeers(String[] peers);

   /**
    * Fast DDS Discovery Servers ("ip" or "ip:port", like ROS_DISCOVERY_SERVER) for every participant created from now
    * on. Participants become SUPER_CLIENTs so they still see the whole ROS graph. An empty array goes back to the
    * profile's discovery.
    */
   public static native void setDiscoveryServers(String[] servers);

   /** Every endpoint alive in the domain, as "W|R \t ddsTopicName \t ddsTypeName \t guidHex". */
   public static native String[] participantGetDiscoveredEndpoints(long fastddsParticipant);

   /** Every participant alive in the domain as "guidPrefixHex \t name \t ip1,ip2" (unicast IPv4, no loopback). */
   public static native String[] participantGetDiscoveredParticipantAddresses(long fastddsParticipant);

   /** DDS participant_name of every participant alive in the domain. */
   public static native String[] participantGetDiscoveredParticipantNames(long fastddsParticipant);

   /**
    * Identity (16 bytes writer GUID + 8 bytes sequence number, big endian) of the last sample written by this
    * writer. Returns false if it has not written anything yet.
    */
   public static native boolean dataWriterGetLastSampleIdentity(long fastddsDataWriter, byte[] identityOut);

   public static native void sampleInfoSampleIdentity(long fastddsSampleInfo, byte[] identityOut);

   /** For a service reply, the identity of the request it answers. */
   public static native void sampleInfoRelatedSampleIdentity(long fastddsSampleInfo, byte[] identityOut);
}
