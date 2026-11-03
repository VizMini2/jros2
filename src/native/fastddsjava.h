#ifndef FASTDDSJAVA_H
#define FASTDDSJAVA_H

#include <cstdint>
#include <functional>
#include <string>
#include <vector>

#include <fastdds/dds/core/status/PublicationMatchedStatus.hpp>
#include <fastdds/dds/core/status/SubscriptionMatchedStatus.hpp>
#include <fastdds/dds/subscriber/SampleInfo.hpp>

struct fastddsjava_TopicDataWrapper
{
   std::vector<uint8_t> data_vector;

   uint8_t* data_ptr();
};

class fastddsjava_TopicDataWrapperType;

class fastddsjava_DataReaderListener;
class fastddsjava_DataWriterListener;

uint32_t fastddsjava_load_xml_profiles_string(const std::string& xml);

void* fastddsjava_create_participant(const std::string& profile_name);
uint32_t fastddsjava_delete_participant(void* participant_);

void* fastddsjava_create_publisher(void* participant_, const std::string& profile_name);
uint32_t fastddsjava_delete_publisher(void* participant_, void* publisher_);

void* fastddsjava_create_subscriber(void* participant_, const std::string& profile_name);
uint32_t fastddsjava_delete_subscriber(void* participant_, void* subscriber_);

fastddsjava_TopicDataWrapperType* fastddsjava_create_topic_data_wrapper_type(const std::string& name, uint16_t encapsulation);
void* fastddsjava_create_typesupport(fastddsjava_TopicDataWrapperType* type);
uint32_t fastddsjava_register_type(void* participant_, void* type_support_);
uint32_t fastddsjava_unregister_type(void* participant_, const std::string& type_name);
void fastddsjava_delete_typesupport(void* type_support_);
std::string fastddsjava_topic_data_wrapper_type_get_name(fastddsjava_TopicDataWrapperType* type);
fastddsjava_TopicDataWrapper* fastddsjava_topic_data_wrapper_type_create_data(fastddsjava_TopicDataWrapperType* type);
void fastddsjava_topic_data_wrapper_type_delete_data(fastddsjava_TopicDataWrapperType* type, fastddsjava_TopicDataWrapper* data);

void* fastddsjava_create_topic(void* participant_, fastddsjava_TopicDataWrapperType* type, const std::string& topic_name,
                               const std::string& profile_name);
uint32_t fastddsjava_delete_topic(void* participant_, void* topic_);

void* fastddsjava_create_datawriter(void* publisher_, void* topic_, const std::string& profile_name);
uint32_t fastddsjava_delete_datawriter(void* publisher_, void* writer_);
uint32_t fastddsjava_datawriter_write(void* writer_, fastddsjava_TopicDataWrapper* data);
uint32_t fastddsjava_datawriter_set_listener(void* writer_, fastddsjava_DataWriterListener* listener = nullptr);
uint32_t fastddsjava_datawriter_get_publication_matched_status(void* writer_, eprosima::fastdds::dds::PublicationMatchedStatus& status);

void* fastddsjava_create_datareader(void* subscriber_, void* topic_, fastddsjava_DataReaderListener* listener,
                                    const std::string& profile_name);
uint32_t fastddsjava_delete_datareader(void* subscriber_, void* reader_);
uint32_t fastddsjava_datareader_read_next_sample(void* reader_, void* data, eprosima::fastdds::dds::SampleInfo* info);
uint32_t fastddsjava_datareader_take_next_custom(void* reader_, void* data, eprosima::fastdds::dds::SampleInfo* info);
uint32_t fastddsjava_datareader_set_listener(void* reader_, fastddsjava_DataReaderListener* listener = nullptr);
uint32_t fastddsjava_datareader_get_unread_count(void* reader_);
uint32_t fastddsjava_datareader_get_subscription_matched_status(void* reader_,
                                                                eprosima::fastdds::dds::SubscriptionMatchedStatus& status);

void fastddsjava_get_participant_guid(void* participant_, uint8_t* guid_out);
void fastddsjava_get_writer_guid(void* writer_, uint8_t* guid_out);
void fastddsjava_get_reader_guid(void* reader_, uint8_t* guid_out);
void fastddsjava_subscription_matched_status_last_publication_guid(
      const eprosima::fastdds::dds::SubscriptionMatchedStatus& status, uint8_t* guid_out);

void fastddsjava_topic_data_resize(fastddsjava_TopicDataWrapper* data, size_t size);
size_t fastddsjava_topic_data_size(fastddsjava_TopicDataWrapper* data);
void fastddsjava_topic_data_write(fastddsjava_TopicDataWrapper* data, const uint8_t* src, size_t length);
void fastddsjava_topic_data_read(fastddsjava_TopicDataWrapper* data, uint8_t* dst, size_t length);

eprosima::fastdds::dds::SampleInfo* fastddsjava_create_sample_info();
void fastddsjava_delete_sample_info(eprosima::fastdds::dds::SampleInfo* info);

fastddsjava_DataReaderListener* fastddsjava_create_datareader_listener();
void fastddsjava_delete_datareader_listener(fastddsjava_DataReaderListener* listener);
void fastddsjava_datareader_listener_set_on_data_available(fastddsjava_DataReaderListener* listener,
                                                           std::function<void()> callback);
void fastddsjava_datareader_listener_set_on_subscription_matched(fastddsjava_DataReaderListener* listener,
                                                                 std::function<void()> callback);

fastddsjava_DataWriterListener* fastddsjava_create_datawriter_listener();
void fastddsjava_delete_datawriter_listener(fastddsjava_DataWriterListener* listener);
void fastddsjava_datawriter_listener_set_on_publication_matched(fastddsjava_DataWriterListener* listener,
                                                                std::function<void()> callback);

// ---- Graph discovery (ros2 topic/service/action list) and request/reply pairing ----

// One discovered remote or local endpoint, as seen by the participant's builtin discovery.
struct fastddsjava_DiscoveredEndpoint
{
   bool is_writer;
   std::string topic_name;
   std::string type_name;
   uint8_t guid[16];
};

// Unicast discovery peers (IPv4 addresses) added to every participant created from now on, like
// ROS_STATIC_PEERS. The default multicast discovery is kept. Useful on Wi-Fi networks that drop multicast.
void fastddsjava_set_initial_peers(const std::vector<std::string>& peers);

// Fast DDS Discovery Servers ("ip:port", like ROS_DISCOVERY_SERVER) for every participant created from now on.
// When set, participants are SUPER_CLIENTs: they receive the discovery data of the whole domain (needed to list
// the ROS graph) and don't use multicast discovery.
void fastddsjava_set_discovery_servers(const std::vector<std::string>& servers);

// Snapshot of every endpoint currently alive in the domain (as seen by this participant).
std::vector<fastddsjava_DiscoveredEndpoint> fastddsjava_participant_get_discovered_endpoints(void* participant_);
// Name of every participant currently alive in the domain (DDS participant_name, "" if none).
std::vector<std::string> fastddsjava_participant_get_discovered_participant_names(void* participant_);
// Every participant alive in the domain as "guidPrefixHex \t name \t ip1,ip2" (its unicast IPv4 discovery
// locators, without loopback). Lets an app remember where robots are and reach them later by unicast.
std::vector<std::string> fastddsjava_participant_get_discovered_participant_addresses(void* participant_);

// Sample identities are 24 bytes: 16 bytes writer GUID + 4 bytes seq high (int32 BE) + 4 bytes seq low (uint32 BE).
// Fills the identity of the last sample written by this writer through fastddsjava_datawriter_write. Returns false if none.
bool fastddsjava_datawriter_get_last_sample_identity(void* writer_, uint8_t* identity_out);
void fastddsjava_sample_info_get_sample_identity(eprosima::fastdds::dds::SampleInfo* info, uint8_t* identity_out);
void fastddsjava_sample_info_get_related_sample_identity(eprosima::fastdds::dds::SampleInfo* info, uint8_t* identity_out);

#endif // FASTDDSJAVA_H
